from __future__ import annotations

from datetime import date
from pathlib import Path

import duckdb

from kucoin_lake import api as api_module
from kucoin_lake import metadata as metadata_module


def _write_synthetic_1m_day(
    con: duckdb.DuckDBPyConnection,
    path: Path,
    *,
    symbol: str,
    day: date,
    volume: float,
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    ts = f"{day.isoformat()} 00:00:00+00"
    path_sql = path.as_posix().replace("'", "''")
    con.execute(
        f"""
        COPY (
            SELECT
                0::BIGINT AS time_ms,
                CAST(? AS TIMESTAMP) AS ts,
                1.0::DOUBLE AS open,
                1.0::DOUBLE AS high,
                1.0::DOUBLE AS low,
                1.0::DOUBLE AS close,
                CAST(? AS DOUBLE) AS volume,
                CAST(? AS DATE) AS date,
                CAST(? AS VARCHAR) AS symbol,
                '1m'::VARCHAR AS timeframe
        ) TO '{path_sql}' (FORMAT PARQUET);
        """,
        [ts, volume, day.isoformat(), symbol],
    )


def _seed_two_days(lake_root: Path, *, symbol: str, day1: date, day2: date) -> None:
    con = duckdb.connect()
    try:
        for offset, day in enumerate((day1, day2), start=1):
            path = (
                lake_root
                / "futures"
                / "klines"
                / "timeframe=1m"
                / f"symbol={symbol}"
                / f"date={day.isoformat()}"
                / "data.parquet"
            )
            _write_synthetic_1m_day(con, path, symbol=symbol, day=day, volume=float(offset))
    finally:
        con.close()


def test_missing_derived_completion_from_coverage(tmp_path: Path) -> None:
    lake_root = tmp_path / "lake"
    meta_db = tmp_path / "meta.duckdb"
    symbol = "TESTUSDTM"
    day1 = date(2026, 1, 22)
    day2 = date(2026, 1, 23)

    _seed_two_days(lake_root, symbol=symbol, day1=day1, day2=day2)

    metadata_module.build_or_update_metadata(
        lake_root,
        market="futures",
        datasets=["klines"],
        meta_db_path=meta_db,
        timeframe_filter="1m",
    )

    con = metadata_module.connect_meta_db(meta_db)
    try:
        day1_liq_ts = con.execute(
            """
            SELECT computed_at_utc
            FROM md.md_liquidity_daily
            WHERE market = 'futures' AND timeframe = '1m' AND symbol = ? AND date = ?;
            """,
            [symbol, day1.isoformat()],
        ).fetchone()[0]
        day1_int_ts = con.execute(
            """
            SELECT computed_at_utc
            FROM md.md_kline_integrity_day
            WHERE market = 'futures' AND timeframe = '1m' AND symbol = ? AND date = ?;
            """,
            [symbol, day1.isoformat()],
        ).fetchone()[0]
        con.execute(
            """
            DELETE FROM md.md_liquidity_daily
            WHERE market = 'futures' AND timeframe = '1m' AND symbol = ? AND date = ?;
            """,
            [symbol, day2.isoformat()],
        )
        con.execute(
            """
            DELETE FROM md.md_kline_integrity_day
            WHERE market = 'futures' AND timeframe = '1m' AND symbol = ? AND date = ?;
            """,
            [symbol, day2.isoformat()],
        )
    finally:
        con.close()

    metadata_module.build_or_update_metadata(
        lake_root,
        market="futures",
        datasets=["klines"],
        meta_db_path=meta_db,
        timeframe_filter="1m",
    )

    con = metadata_module.connect_meta_db(meta_db)
    try:
        liq_rows = con.execute(
            """
            SELECT COUNT(*)
            FROM md.md_liquidity_daily
            WHERE market = 'futures' AND timeframe = '1m' AND symbol = ?;
            """,
            [symbol],
        ).fetchone()[0]
        int_rows = con.execute(
            """
            SELECT COUNT(*)
            FROM md.md_kline_integrity_day
            WHERE market = 'futures' AND timeframe = '1m' AND symbol = ?;
            """,
            [symbol],
        ).fetchone()[0]
        day1_liq_ts_after = con.execute(
            """
            SELECT computed_at_utc
            FROM md.md_liquidity_daily
            WHERE market = 'futures' AND timeframe = '1m' AND symbol = ? AND date = ?;
            """,
            [symbol, day1.isoformat()],
        ).fetchone()[0]
        day1_int_ts_after = con.execute(
            """
            SELECT computed_at_utc
            FROM md.md_kline_integrity_day
            WHERE market = 'futures' AND timeframe = '1m' AND symbol = ? AND date = ?;
            """,
            [symbol, day1.isoformat()],
        ).fetchone()[0]
    finally:
        con.close()

    assert liq_rows == 2
    assert int_rows == 2


def test_alignment_missing_keys_trigger(tmp_path: Path) -> None:
    lake_root = tmp_path / "lake"
    meta_db = tmp_path / "meta.duckdb"
    symbol = "TESTUSDTM"
    day1 = date(2026, 1, 22)
    day2 = date(2026, 1, 23)

    _seed_two_days(lake_root, symbol=symbol, day1=day1, day2=day2)

    metadata_module.build_or_update_metadata(
        lake_root,
        market="futures",
        datasets=["klines"],
        meta_db_path=meta_db,
        timeframe_filter="1m",
        symbols=[symbol],
        date_start=day1,
        date_end=day2,
    )

    con = metadata_module.connect_meta_db(meta_db)
    try:
        alignment_rows = con.execute(
            """
            SELECT COUNT(*)
            FROM md.md_alignment_summary
            WHERE market = 'futures' AND timeframe = '1m' AND symbol = ?;
            """,
            [symbol],
        ).fetchone()[0]
        assert alignment_rows == 2
        con.execute(
            """
            DELETE FROM md.md_alignment_summary
            WHERE market = 'futures' AND timeframe = '1m' AND symbol = ? AND date = ?;
            """,
            [symbol, day2.isoformat()],
        )
    finally:
        con.close()

    result = metadata_module.build_or_update_metadata(
        lake_root,
        market="futures",
        datasets=["klines"],
        meta_db_path=meta_db,
        timeframe_filter="1m",
        symbols=[symbol],
        date_start=day1,
        date_end=day2,
    )

    assert result["new_or_changed_files"] == 0

    con = metadata_module.connect_meta_db(meta_db)
    try:
        alignment_rows = con.execute(
            """
            SELECT COUNT(*)
            FROM md.md_alignment_summary
            WHERE market = 'futures' AND timeframe = '1m' AND symbol = ?;
            """,
            [symbol],
        ).fetchone()[0]
        day2_rows = con.execute(
            """
            SELECT COUNT(*)
            FROM md.md_alignment_summary
            WHERE market = 'futures' AND timeframe = '1m' AND symbol = ? AND date = ?;
            """,
            [symbol, day2.isoformat()],
        ).fetchone()[0]
    finally:
        con.close()

    assert alignment_rows == 2
    assert day2_rows == 1
    assert day1_liq_ts_after == day1_liq_ts
    assert day1_int_ts_after == day1_int_ts

    con = metadata_module.connect_meta_db(meta_db)
    try:
        con.execute(
            """
            DELETE FROM md.md_liquidity_daily
            WHERE market = 'futures' AND timeframe = '1m' AND symbol = ? AND date = ?;
            """,
            [symbol, day2.isoformat()],
        )
        con.execute(
            """
            DELETE FROM md.md_kline_integrity_day
            WHERE market = 'futures' AND timeframe = '1m' AND symbol = ? AND date = ?;
            """,
            [symbol, day2.isoformat()],
        )
    finally:
        con.close()

    api_module.backfill_derived_from_coverage(
        lake_root,
        market="futures",
        timeframe_filter="1m",
        meta_db_path=meta_db,
        symbols=[symbol],
        date_start=day1,
        date_end=day2,
        what="both",
    )

    con = metadata_module.connect_meta_db(meta_db)
    try:
        liq_rows = con.execute(
            """
            SELECT COUNT(*)
            FROM md.md_liquidity_daily
            WHERE market = 'futures' AND timeframe = '1m' AND symbol = ?;
            """,
            [symbol],
        ).fetchone()[0]
        int_rows = con.execute(
            """
            SELECT COUNT(*)
            FROM md.md_kline_integrity_day
            WHERE market = 'futures' AND timeframe = '1m' AND symbol = ?;
            """,
            [symbol],
        ).fetchone()[0]
    finally:
        con.close()

    assert liq_rows == 2
    assert int_rows == 2
