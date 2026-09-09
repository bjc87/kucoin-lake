from __future__ import annotations

import json
from datetime import date, datetime, timedelta
from pathlib import Path

import duckdb
import pandas as pd

REQUIRED_CANDIDATE_COLUMNS = {
    "day",
    "asof_date",
    "symbol",
    "asof_dv_30d_median",
    "asof_liquidity_rank_30d",
    "asof_dollar_volume",
    "asof_volume",
    "asof_close_price",
}

OUTPUT_COLUMNS = [
    "day",
    "symbol",
    "asof_date",
    "asof_dv_30d_median",
    "asof_liquidity_rank_30d",
    "asof_dollar_volume",
    "asof_volume",
    "asof_close_price",
    "entry_n",
    "exit_n",
    "is_weekend",
]


def _coerce_date(value: str | date, field_name: str) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, pd.Timestamp):
        return value.date()
    if isinstance(value, str):
        try:
            return date.fromisoformat(value)
        except ValueError as exc:
            raise ValueError(
                f"{field_name} must be YYYY-MM-DD or date; got {value!r}."
            ) from exc
    raise TypeError(f"{field_name} must be YYYY-MM-DD or date; got {type(value)!r}.")


def load_candidate_ranks(
    *,
    meta_db_path: str | Path,
    market: str,
    timeframe: str,
    start_date: str | date,
    end_date: str | date,
) -> pd.DataFrame:
    """
    Load candidate liquidity ranks from metadata with a 1-day lookback.

    Rows returned are keyed by `day` (date + 1) with `asof_date` = date.
    This enforces a no-lookahead rule when building the universe.
    """
    meta_db_path = Path(meta_db_path)
    if not meta_db_path.exists():
        raise FileNotFoundError(f"Metadata DB not found: {meta_db_path}")

    start_dt = _coerce_date(start_date, "start_date")
    end_dt = _coerce_date(end_date, "end_date")
    if start_dt > end_dt:
        raise ValueError("start_date must be <= end_date.")

    start_asof = start_dt - timedelta(days=1)
    end_asof = end_dt - timedelta(days=1)

    query = """
        SELECT
            date + INTERVAL 1 DAY AS day,
            date AS asof_date,
            symbol,
            dv_30d_median AS asof_dv_30d_median,
            liquidity_rank_30d AS asof_liquidity_rank_30d,
            dollar_volume AS asof_dollar_volume,
            volume AS asof_volume,
            close_price AS asof_close_price
        FROM md.md_liquidity_daily
        WHERE market = ?
          AND timeframe = ?
          AND date BETWEEN ? AND ?
        ORDER BY day, liquidity_rank_30d, symbol
    """

    con = duckdb.connect(meta_db_path.as_posix(), read_only=True)
    try:
        df = con.execute(
            query,
            [market, timeframe, start_asof, end_asof],
        ).fetch_df()
    finally:
        con.close()

    if df.empty:
        return df

    df["day"] = pd.to_datetime(df["day"]).dt.normalize()
    df["asof_date"] = pd.to_datetime(df["asof_date"]).dt.normalize()
    return df


def apply_hysteresis(
    candidates: pd.DataFrame, *, entry_n: int, exit_n: int
) -> pd.DataFrame:
    """
    Apply entry/exit hysteresis to candidate ranks.

    Rules:
    - Add symbol if asof_liquidity_rank_30d <= entry_n
    - Remove symbol if asof_liquidity_rank_30d > exit_n
    - Otherwise keep previous membership
    - Drop any previously held symbol missing from today's candidates
    """
    if exit_n < entry_n:
        raise ValueError("exit_n must be >= entry_n.")

    missing = REQUIRED_CANDIDATE_COLUMNS.difference(candidates.columns)
    if missing:
        raise ValueError(f"Candidates missing required columns: {sorted(missing)}")

    if candidates.empty:
        return pd.DataFrame(columns=OUTPUT_COLUMNS)

    df = candidates.copy()
    df["day"] = pd.to_datetime(df["day"]).dt.normalize()
    df["asof_date"] = pd.to_datetime(df["asof_date"]).dt.normalize()
    df = df.sort_values(["day", "asof_liquidity_rank_30d", "symbol"])

    membership: set[str] = set()
    output_frames: list[pd.DataFrame] = []

    for day, day_df in df.groupby("day", sort=True):
        day_df = day_df.sort_values(["asof_liquidity_rank_30d", "symbol"])
        symbols_today = set(day_df["symbol"])

        if membership:
            membership.intersection_update(symbols_today)

        to_add = set(
            day_df.loc[day_df["asof_liquidity_rank_30d"] <= entry_n, "symbol"]
        )
        to_remove = set(
            day_df.loc[day_df["asof_liquidity_rank_30d"] > exit_n, "symbol"]
        )

        membership.update(to_add)
        membership.difference_update(to_remove)
        membership.intersection_update(symbols_today)

        if not membership:
            continue

        in_df = day_df[day_df["symbol"].isin(membership)].copy()
        in_df["entry_n"] = entry_n
        in_df["exit_n"] = exit_n
        in_df["is_weekend"] = pd.to_datetime(in_df["day"]).dt.weekday >= 5
        output_frames.append(in_df[OUTPUT_COLUMNS])

    if not output_frames:
        return pd.DataFrame(columns=OUTPUT_COLUMNS)

    out = pd.concat(output_frames, ignore_index=True)
    out = out.sort_values(
        ["day", "asof_liquidity_rank_30d", "symbol"], ignore_index=True
    )
    return out


def build_universe_daily(
    *,
    meta_db_path: str | Path,
    market: str = "futures",
    timeframe: str = "1m",
    start_date: str | date,
    end_date: str | date,
    entry_n: int = 100,
    exit_n: int = 130,
    output_path: str | Path = "research/outputs/universe_daily.parquet",
    overwrite: bool = True,
    emit_all_candidates: bool = False,
) -> pd.DataFrame:
    """
    Build a daily research universe with no-lookahead candidate ranks.

    Returns the universe DataFrame and writes it to Parquet. A JSON sidecar
    (<output_stem>.meta.json) is also written for reproducibility.
    """
    if exit_n < entry_n:
        raise ValueError("exit_n must be >= entry_n.")
    if entry_n <= 0 or exit_n <= 0:
        raise ValueError("entry_n and exit_n must be positive integers.")

    meta_db_path = Path(meta_db_path)
    if not meta_db_path.exists():
        raise FileNotFoundError(f"Metadata DB not found: {meta_db_path}")

    start_dt = _coerce_date(start_date, "start_date")
    end_dt = _coerce_date(end_date, "end_date")
    if start_dt > end_dt:
        raise ValueError("start_date must be <= end_date.")

    output_path = Path(output_path)
    if output_path.exists() and not overwrite:
        raise FileExistsError(f"Output path already exists: {output_path}")

    candidates = load_candidate_ranks(
        meta_db_path=meta_db_path,
        market=market,
        timeframe=timeframe,
        start_date=start_dt,
        end_date=end_dt,
    )

    universe = apply_hysteresis(candidates, entry_n=entry_n, exit_n=exit_n)

    if emit_all_candidates:
        if candidates.empty:
            universe_with_flags = candidates.copy()
            universe_with_flags["in_universe"] = False
            universe_with_flags["entry_n"] = entry_n
            universe_with_flags["exit_n"] = exit_n
            universe_with_flags["is_weekend"] = (
                pd.to_datetime(universe_with_flags["day"]).dt.weekday >= 5
            )
            universe_with_flags = universe_with_flags[
                OUTPUT_COLUMNS + ["in_universe"]
            ]
        else:
            members = universe[["day", "symbol"]].drop_duplicates()
            universe_with_flags = candidates.copy()
            universe_with_flags["entry_n"] = entry_n
            universe_with_flags["exit_n"] = exit_n
            universe_with_flags["is_weekend"] = (
                pd.to_datetime(universe_with_flags["day"]).dt.weekday >= 5
            )
            universe_with_flags = universe_with_flags.merge(
                members.assign(in_universe=True),
                on=["day", "symbol"],
                how="left",
            )
            universe_with_flags["in_universe"] = (
                universe_with_flags["in_universe"].fillna(False).astype(bool)
            )
            universe_with_flags = universe_with_flags[
                OUTPUT_COLUMNS + ["in_universe"]
            ]
            universe_with_flags = universe_with_flags.sort_values(
                ["day", "asof_liquidity_rank_30d", "symbol"], ignore_index=True
            )
        output_df = universe_with_flags
    else:
        output_df = universe

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_df.to_parquet(output_path, index=False)

    meta_path = output_path.with_suffix(".meta.json")
    meta_payload = {
        "market": market,
        "timeframe": timeframe,
        "entry_n": entry_n,
        "exit_n": exit_n,
        "start_date": start_dt.isoformat(),
        "end_date": end_dt.isoformat(),
        "liquidity_source_table": "md.md_liquidity_daily",
        "liquidity_metric": "dv_30d_median",
        "rank_column": "liquidity_rank_30d",
        "asof_rule": "Universe(D) uses liquidity ranks computed on D-1",
        "emit_all_candidates": emit_all_candidates,
    }
    meta_path.write_text(json.dumps(meta_payload, indent=2, sort_keys=True) + "\n")
    return output_df


def summarize_universe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Summarize daily universe membership with adds/drops and turnover.
    """
    if df.empty:
        return pd.DataFrame(
            columns=[
                "day",
                "n_members",
                "adds",
                "drops",
                "turnover_rate",
                "candidate_count",
            ]
        )

    if "day" not in df.columns or "symbol" not in df.columns:
        raise ValueError("Input must contain 'day' and 'symbol' columns.")

    working = df.copy()
    working["day"] = pd.to_datetime(working["day"]).dt.normalize()
    has_in_universe = "in_universe" in working.columns
    candidate_counts = None
    if has_in_universe:
        candidate_counts = working.groupby("day").size().to_dict()

    base = working
    if has_in_universe:
        base = base[base["in_universe"]].copy()

    if base.empty:
        return pd.DataFrame(
            columns=[
                "day",
                "n_members",
                "adds",
                "drops",
                "turnover_rate",
                "candidate_count",
            ]
        )

    base = base[["day", "symbol"]].drop_duplicates()
    base = base.sort_values(["day", "symbol"])

    days = base["day"].drop_duplicates().tolist()
    summary_rows: list[dict] = []
    prev_members: set[str] = set()

    for day in days:
        day_members = set(base.loc[base["day"] == day, "symbol"])
        adds = len(day_members - prev_members)
        drops = len(prev_members - day_members)
        prev_count = len(prev_members)
        turnover = 0.0 if prev_count == 0 else (adds + drops) / prev_count

        summary_rows.append(
            {
                "day": day,
                "n_members": len(day_members),
                "adds": adds,
                "drops": drops,
                "turnover_rate": turnover,
                "candidate_count": candidate_counts.get(day) if has_in_universe else None,
            }
        )
        prev_members = day_members

    return pd.DataFrame(summary_rows)
