from __future__ import annotations

from datetime import date, datetime
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd

from kucoin_lake.manifest import iter_data_parquets


UNIVERSE_REQUIRED_COLUMNS = {
    "day",
    "symbol",
    "asof_date",
    "asof_dv_30d_median",
    "asof_liquidity_rank_30d",
    "asof_dollar_volume",
    "asof_volume",
    "asof_close_price",
    "is_weekend",
}

PANEL_BASE_COLUMNS = [
    "day",
    "symbol",
    "asof_date",
    "is_weekend",
    "asof_dv_30d_median",
    "asof_liquidity_rank_30d",
    "asof_dollar_volume",
    "asof_volume",
    "asof_close_price",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "dollar_volume",
    "log_dollar_volume",
    "vwap",
    "src_rows",
    "max_ts",
    "ret_1d",
    "fwd_ret_1d",
    "fwd_ret_5d",
    "fwd_ret_20d",
    "log_ret_1d",
    "fwd_log_ret_1d",
]


def _coerce_date(value: str | date | None, field_name: str) -> date | None:
    if value is None:
        return None
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


def _load_1d_klines_for_symbols(
    *,
    nas_root: str | Path,
    market: str,
    symbols: list[str],
    start_date: date,
    end_date: date,
) -> pd.DataFrame:
    """
    Load 1d kline rows for a symbol/date window from the NAS lake.
    """
    if not symbols:
        return pd.DataFrame(
            columns=[
                "day",
                "symbol",
                "open",
                "high",
                "low",
                "close",
                "volume",
                "dollar_volume",
                "vwap",
                "src_rows",
                "max_ts",
            ]
        )

    nas_root = Path(nas_root)
    if not nas_root.exists():
        raise FileNotFoundError(f"NAS root not found: {nas_root}")

    files = sorted(
        info.file_path
        for info in iter_data_parquets(
            nas_root,
            market=market,
            datasets=("klines",),
            timeframe_filter="1d",
            symbols=symbols,
            date_start=start_date,
            date_end=end_date,
        )
    )

    if not files:
        raise FileNotFoundError(
            "No 1d kline parquet files found for requested symbols/date range under "
            f"{nas_root / market / 'klines' / 'timeframe=1d'}."
        )

    query = """
        SELECT
            CAST(date AS DATE) AS day,
            symbol,
            open,
            high,
            low,
            close,
            volume,
            dollar_volume,
            vwap,
            src_rows,
            max_ts
        FROM read_parquet($1, hive_partitioning=1)
        WHERE CAST(date AS DATE) BETWEEN ? AND ?
        ORDER BY symbol, day
    """

    con = duckdb.connect(database=":memory:")
    try:
        try:
            df = con.execute(query, [files, start_date, end_date]).fetch_df()
        except duckdb.BinderException as exc:
            raise ValueError(
                "1d kline schema is missing one of required columns: "
                "date, open, high, low, close, volume, dollar_volume, vwap, src_rows, max_ts, "
                "or partition-derived symbol (requires hive-style symbol=... paths)."
            ) from exc
    finally:
        con.close()

    if df.empty:
        raise ValueError(
            "No 1d kline rows loaded for requested symbols/date range after scanning matching files."
        )

    df["day"] = pd.to_datetime(df["day"]).dt.normalize()
    return df


def build_base_panel(
    *,
    universe_path: str | Path,
    nas_root: str | Path,
    market: str = "futures",
    start_date: str | date | None = None,
    end_date: str | date | None = None,
    output_path: str | Path = "research/outputs/panels/base_panel.parquet",
    overwrite: bool = True,
    require_in_universe: bool = True,
) -> pd.DataFrame:
    """
    Build a deterministic base research panel by joining universe rows to 1d klines.
    """
    universe_path = Path(universe_path)
    if not universe_path.exists():
        raise FileNotFoundError(f"Universe parquet not found: {universe_path}")

    nas_root = Path(nas_root)
    if not nas_root.exists():
        raise FileNotFoundError(f"NAS root not found: {nas_root}")

    output_path = Path(output_path)
    if output_path.exists() and not overwrite:
        raise FileExistsError(f"Output path already exists: {output_path}")

    start_dt = _coerce_date(start_date, "start_date")
    end_dt = _coerce_date(end_date, "end_date")
    if start_dt and end_dt and start_dt > end_dt:
        raise ValueError("start_date must be <= end_date.")

    universe = pd.read_parquet(universe_path)
    missing_cols = UNIVERSE_REQUIRED_COLUMNS.difference(universe.columns)
    if missing_cols:
        raise ValueError(
            f"Universe parquet missing required columns: {sorted(missing_cols)}"
        )

    universe["day"] = pd.to_datetime(universe["day"]).dt.normalize()
    universe["asof_date"] = pd.to_datetime(universe["asof_date"]).dt.normalize()

    if "in_universe" in universe.columns and require_in_universe:
        universe = universe[universe["in_universe"]].copy()

    if start_dt is not None:
        universe = universe[universe["day"] >= pd.Timestamp(start_dt)].copy()
    if end_dt is not None:
        universe = universe[universe["day"] <= pd.Timestamp(end_dt)].copy()

    if universe.empty:
        raise ValueError("Universe has no rows after applying filters.")

    keep_columns = [
        "day",
        "symbol",
        "asof_date",
        "asof_dv_30d_median",
        "asof_liquidity_rank_30d",
        "asof_dollar_volume",
        "asof_volume",
        "asof_close_price",
        "is_weekend",
    ]
    if "entry_n" in universe.columns:
        keep_columns.append("entry_n")
    if "exit_n" in universe.columns:
        keep_columns.append("exit_n")

    universe = universe[keep_columns].drop_duplicates(subset=["day", "symbol"])

    load_start = universe["day"].min().date()
    load_end = universe["day"].max().date()
    symbols = sorted(universe["symbol"].dropna().unique().tolist())

    klines = _load_1d_klines_for_symbols(
        nas_root=nas_root,
        market=market,
        symbols=symbols,
        start_date=load_start,
        end_date=load_end,
    )

    panel = universe.merge(klines, how="left", on=["day", "symbol"])

    if panel.empty:
        raise ValueError("No rows after joining universe to 1d klines.")

    panel = panel.sort_values(["symbol", "day"], ignore_index=True)

    panel["log_dollar_volume"] = np.log(panel["dollar_volume"].clip(lower=1))

    by_symbol_close = panel.groupby("symbol", sort=False)["close"]
    panel["ret_1d"] = by_symbol_close.transform(lambda s: s / s.shift(1) - 1)
    panel["fwd_ret_1d"] = by_symbol_close.transform(lambda s: s.shift(-1) / s - 1)
    panel["fwd_ret_5d"] = by_symbol_close.transform(lambda s: s.shift(-5) / s - 1)
    panel["fwd_ret_20d"] = by_symbol_close.transform(lambda s: s.shift(-20) / s - 1)
    panel["log_ret_1d"] = by_symbol_close.transform(lambda s: np.log(s) - np.log(s.shift(1)))
    panel["fwd_log_ret_1d"] = by_symbol_close.transform(
        lambda s: np.log(s.shift(-1)) - np.log(s)
    )

    output_columns = PANEL_BASE_COLUMNS.copy()
    if "entry_n" in panel.columns:
        output_columns.append("entry_n")
    if "exit_n" in panel.columns:
        output_columns.append("exit_n")

    panel = panel[output_columns]
    panel = panel.sort_values(["symbol", "day"], ignore_index=True)

    if panel.empty:
        raise ValueError("Panel is empty after selecting output columns.")

    output_path.parent.mkdir(parents=True, exist_ok=True)
    panel.to_parquet(output_path, index=False)
    return panel


def summarize_panel(df: pd.DataFrame) -> pd.DataFrame:
    """
    Return simple deterministic daily health/target summary for the panel.
    """
    required = {
        "day",
        "symbol",
        "close",
        "ret_1d",
        "fwd_ret_1d",
        "fwd_ret_5d",
        "fwd_ret_20d",
    }
    missing = required.difference(df.columns)
    if missing:
        raise ValueError(f"Panel is missing required columns for summary: {sorted(missing)}")

    if df.empty:
        return pd.DataFrame(
            columns=[
                "day",
                "n_symbols",
                "mean_ret_1d",
                "mean_fwd_ret_1d",
                "mean_fwd_ret_5d",
                "mean_fwd_ret_20d",
                "missing_close_pct",
                "missing_fwd_ret_1d_pct",
            ]
        )

    base = df.copy()
    base["day"] = pd.to_datetime(base["day"]).dt.normalize()

    summary = (
        base.groupby("day", sort=True)
        .agg(
            n_symbols=("symbol", "nunique"),
            mean_ret_1d=("ret_1d", "mean"),
            mean_fwd_ret_1d=("fwd_ret_1d", "mean"),
            mean_fwd_ret_5d=("fwd_ret_5d", "mean"),
            mean_fwd_ret_20d=("fwd_ret_20d", "mean"),
            missing_close_pct=("close", lambda s: s.isna().mean()),
            missing_fwd_ret_1d_pct=("fwd_ret_1d", lambda s: s.isna().mean()),
        )
        .reset_index()
        .sort_values("day", ignore_index=True)
    )
    return summary
