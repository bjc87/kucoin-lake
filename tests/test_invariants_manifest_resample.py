from __future__ import annotations

import os
from datetime import date, timedelta
from pathlib import Path

import duckdb
import pytest
from metadata import connect_meta_db, get_new_or_changed_files, iter_data_parquets


def _touch(path: Path) -> None:
    stat = path.stat()
    os.utime(path, ns=(stat.st_atime_ns, stat.st_mtime_ns + 1_000_000))


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


def _seed_synthetic_klines(tmp_lake_root: Path, *, start_day: date, days: int, symbol: str) -> None:
    con = duckdb.connect()
    try:
        for offset in range(days):
            day = start_day + timedelta(days=offset)
            path = (
                tmp_lake_root
                / "futures"
                / "klines"
                / "timeframe=1m"
                / f"symbol={symbol}"
                / f"date={day.isoformat()}"
                / "data.parquet"
            )
            _write_synthetic_1m_day(con, path, symbol=symbol, day=day, volume=float(offset + 1))
    finally:
        con.close()


def test_manifest_diff_scoped_update(
    tmp_lake_root: Path,
    tmp_duckdb_path: Path,
    run_metadata_build,
) -> None:
    run_metadata_build(timeframe_filter="1m", datasets=["klines"])

    target = (
        tmp_lake_root
        / "futures"
        / "klines"
        / "timeframe=1m"
        / "symbol=ZRXUSDTM"
        / "date=2025-12-30"
        / "data.parquet"
    )
    _touch(target)

    files = list(
        iter_data_parquets(
            tmp_lake_root,
            market="futures",
            datasets=["klines"],
            timeframe_filter="1m",
        )
    )

    con = connect_meta_db(tmp_duckdb_path)
    try:
        changed = get_new_or_changed_files(con, files)
        before_count = con.execute(
            """
            SELECT COUNT(*)
            FROM md.md_partition_coverage
            WHERE market = 'futures' AND timeframe = '1m' AND dataset = 'klines';
            """
        ).fetchone()[0]
    finally:
        con.close()

    assert [f.file_rel for f in changed] == [target.relative_to(tmp_lake_root).as_posix()]

    result = run_metadata_build(timeframe_filter="1m", datasets=["klines"])

    con = connect_meta_db(tmp_duckdb_path)
    try:
        after_count = con.execute(
            """
            SELECT COUNT(*)
            FROM md.md_partition_coverage
            WHERE market = 'futures' AND timeframe = '1m' AND dataset = 'klines';
            """
        ).fetchone()[0]
    finally:
        con.close()

    assert result["coverage_mode"] == "incremental"
    assert result["new_or_changed_files"] == 1
    assert before_count == after_count


def test_derived_1d_matches_1m_and_month_partition(tmp_lake_root: Path) -> None:
    con = duckdb.connect()
    try:
        day = "2025-12-30"
        one_minute_path = (
            tmp_lake_root
            / "futures"
            / "klines"
            / "timeframe=1m"
            / "symbol=ZRXUSDTM"
            / f"date={day}"
            / "data.parquet"
        ).as_posix()
        one_day_path = (
            tmp_lake_root
            / "futures"
            / "klines"
            / "timeframe=1d"
            / "symbol=ZRXUSDTM"
            / "month=2025-12"
            / "data.parquet"
        ).as_posix()

        agg = con.execute(
            """
            SELECT
                ARG_MIN(open, ts) AS open,
                MAX(high) AS high,
                MIN(low) AS low,
                ARG_MAX(close, ts) AS close,
                SUM(volume) AS volume
            FROM read_parquet($1);
            """,
            [one_minute_path],
        ).fetchone()

        daily = con.execute(
            """
            SELECT open, high, low, close, volume
            FROM read_parquet($1)
            WHERE date = CAST(? AS DATE);
            """,
            [one_day_path, day],
        ).fetchone()
    finally:
        con.close()

    assert daily is not None
    assert agg[0] == pytest.approx(daily[0])
    assert agg[1] == pytest.approx(daily[1])
    assert agg[2] == pytest.approx(daily[2])
    assert agg[3] == pytest.approx(daily[3])
    assert agg[4] == pytest.approx(daily[4])

    file_rels = [
        f.file_rel
        for f in iter_data_parquets(
            tmp_lake_root,
            market="futures",
            datasets=["klines"],
            timeframe_filter="1d",
        )
    ]
    assert file_rels
    assert all("month=" in rel for rel in file_rels)


def test_liquidity_rolling_lookback_includes_history(
    tmp_lake_root: Path,
    tmp_duckdb_path: Path,
    run_metadata_build,
) -> None:
    symbol = "TESTUSDTM"
    start_day = date(2025, 1, 1)
    _seed_synthetic_klines(tmp_lake_root, start_day=start_day, days=40, symbol=symbol)

    run_metadata_build(timeframe_filter="1m", datasets=["klines"])

    change_day = start_day + timedelta(days=34)
    change_path = (
        tmp_lake_root
        / "futures"
        / "klines"
        / "timeframe=1m"
        / f"symbol={symbol}"
        / f"date={change_day.isoformat()}"
        / "data.parquet"
    )
    _touch(change_path)

    run_metadata_build(timeframe_filter="1m", datasets=["klines"])

    expected_window = list(range(6, 36))
    expected_median = (expected_window[14] + expected_window[15]) / 2

    con = connect_meta_db(tmp_duckdb_path)
    try:
        dv_30d_median = con.execute(
            """
            SELECT dv_30d_median
            FROM md.md_liquidity_daily
            WHERE market = 'futures'
              AND timeframe = '1m'
              AND symbol = ?
              AND date = CAST(? AS DATE);
            """,
            [symbol, change_day.isoformat()],
        ).fetchone()[0]
    finally:
        con.close()

    assert dv_30d_median == pytest.approx(expected_median)
