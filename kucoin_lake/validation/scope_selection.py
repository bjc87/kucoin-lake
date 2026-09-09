from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path

import duckdb


def _normalize_date(value: date | str | None) -> date | None:
    if value is None or isinstance(value, date):
        return value
    return date.fromisoformat(value)


def _coerce_positive_int(name: str, value: int | None, *, allow_zero: bool = False) -> int | None:
    if value is None:
        return None
    out = int(value)
    if allow_zero:
        if out < 0:
            raise ValueError(f"{name} must be >= 0")
    elif out <= 0:
        raise ValueError(f"{name} must be > 0")
    return out


def _open_meta_db_read_only(meta_db_path: str | Path) -> duckdb.DuckDBPyConnection:
    db_path = Path(meta_db_path)
    if not db_path.exists():
        raise FileNotFoundError(f"Metadata DB not found: {db_path}")
    con = duckdb.connect(db_path.as_posix(), read_only=True)
    con.execute("SET TimeZone = 'UTC';")
    return con


def select_candidate_superset_symbols(
    meta_db_path: str | Path,
    *,
    market: str = "futures",
    timeframe: str = "1m",
    rank_threshold: int,
    date_start: date | str | None = None,
    date_end: date | str | None = None,
    min_days_present: int | None = None,
) -> list[str]:
    """
    Select a reproducible candidate superset from liquidity metadata.

    A symbol is included if it ever had liquidity_rank_30d <= rank_threshold
    within the selected date window. Optional min_days_present applies to
    the count of qualifying days.
    """
    rank_threshold_int = _coerce_positive_int("rank_threshold", rank_threshold)
    min_days_present_int = _coerce_positive_int("min_days_present", min_days_present)
    date_start_norm = _normalize_date(date_start)
    date_end_norm = _normalize_date(date_end)
    if date_start_norm is not None and date_end_norm is not None and date_start_norm > date_end_norm:
        raise ValueError("date_start must be <= date_end")

    where_sql = """
        market = ?
        AND timeframe = ?
        AND liquidity_rank_30d IS NOT NULL
        AND liquidity_rank_30d <= ?
    """
    params: list[object] = [market, timeframe, rank_threshold_int]
    if date_start_norm is not None:
        where_sql += " AND date >= CAST(? AS DATE)"
        params.append(date_start_norm.isoformat())
    if date_end_norm is not None:
        where_sql += " AND date <= CAST(? AS DATE)"
        params.append(date_end_norm.isoformat())

    outer_filter_sql = ""
    if min_days_present_int is not None:
        outer_filter_sql = "WHERE eligible_days >= ?"
        params.append(min_days_present_int)

    query = f"""
        WITH eligible AS (
            SELECT
                symbol,
                COUNT(*)::BIGINT AS eligible_days
            FROM md.md_liquidity_daily
            WHERE {where_sql}
            GROUP BY symbol
        )
        SELECT symbol
        FROM eligible
        {outer_filter_sql}
        ORDER BY symbol;
    """

    con = _open_meta_db_read_only(meta_db_path)
    try:
        rows = con.execute(query, params).fetchall()
    finally:
        con.close()
    return [str(row[0]) for row in rows]


def select_near_threshold_symbols(
    meta_db_path: str | Path,
    *,
    market: str = "futures",
    timeframe: str = "1m",
    target_universe_size: int,
    rank_band: int,
    date_start: date | str | None = None,
    date_end: date | str | None = None,
    lookback_days: int = 14,
    min_days_present: int | None = None,
) -> list[str]:
    """
    Select symbols near the liquidity rank cutoff for daily append validation.

    Symbols are included when their recent liquidity_rank_30d falls within:
      [max(1, target_universe_size - rank_band), target_universe_size + rank_band]

    If date_start/date_end are omitted, the window defaults to the latest
    metadata day with a lookback of `lookback_days`.
    """
    target_size_int = _coerce_positive_int("target_universe_size", target_universe_size)
    rank_band_int = _coerce_positive_int("rank_band", rank_band, allow_zero=True)
    lookback_days_int = _coerce_positive_int("lookback_days", lookback_days)
    min_days_present_int = _coerce_positive_int("min_days_present", min_days_present)
    date_start_norm = _normalize_date(date_start)
    date_end_norm = _normalize_date(date_end)

    lower_rank = max(1, target_size_int - rank_band_int)
    upper_rank = target_size_int + rank_band_int

    con = _open_meta_db_read_only(meta_db_path)
    try:
        if date_end_norm is None:
            max_date_row = con.execute(
                """
                SELECT MAX(date)
                FROM md.md_liquidity_daily
                WHERE market = ?
                  AND timeframe = ?
                  AND liquidity_rank_30d IS NOT NULL;
                """,
                [market, timeframe],
            ).fetchone()
            if not max_date_row or max_date_row[0] is None:
                return []
            date_end_norm = max_date_row[0]

        if date_start_norm is None:
            date_start_norm = date_end_norm - timedelta(days=lookback_days_int - 1)

        if date_start_norm > date_end_norm:
            raise ValueError("date_start must be <= date_end")

        where_sql = """
            market = ?
            AND timeframe = ?
            AND liquidity_rank_30d IS NOT NULL
            AND liquidity_rank_30d BETWEEN ? AND ?
            AND date BETWEEN CAST(? AS DATE) AND CAST(? AS DATE)
        """
        params: list[object] = [
            market,
            timeframe,
            lower_rank,
            upper_rank,
            date_start_norm.isoformat(),
            date_end_norm.isoformat(),
        ]

        outer_filter_sql = ""
        if min_days_present_int is not None:
            outer_filter_sql = "WHERE near_days >= ?"
            params.append(min_days_present_int)

        query = f"""
            WITH near_rank AS (
                SELECT
                    symbol,
                    COUNT(*)::BIGINT AS near_days
                FROM md.md_liquidity_daily
                WHERE {where_sql}
                GROUP BY symbol
            )
            SELECT symbol
            FROM near_rank
            {outer_filter_sql}
            ORDER BY symbol;
        """
        rows = con.execute(query, params).fetchall()
    finally:
        con.close()

    return [str(row[0]) for row in rows]

