from __future__ import annotations

import os
from datetime import date, timedelta
from pathlib import Path

import duckdb
from metadata import build_or_update_metadata, connect_meta_db


def _add_ts_column_to_1d_files(tmp_lake_root: Path) -> None:
    con = duckdb.connect()
    try:
        for dataset in ("klines", "mark", "index"):
            path = (
                tmp_lake_root
                / "futures"
                / dataset
                / "timeframe=1d"
                / "symbol=ZRXUSDTM"
                / "month=2025-12"
                / "data.parquet"
            )
            if not path.exists():
                continue
            tmp_path = path.with_suffix(".parquet.tmp")
            con.execute(
                """
                COPY (
                    SELECT *, min_ts AS ts
                    FROM read_parquet($1)
                ) TO $2 (FORMAT PARQUET);
                """,
                [path.as_posix(), tmp_path.as_posix()],
            )
            tmp_path.replace(path)
    finally:
        con.close()


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


def _snapshot_liquidity(con: duckdb.DuckDBPyConnection, timeframe: str) -> dict:
    rows = con.execute(
        """
        SELECT market, timeframe, symbol, date, computed_at_utc
        FROM md.md_liquidity_daily
        WHERE market = 'futures' AND timeframe = $1
        ORDER BY symbol, date;
        """,
        [timeframe],
    ).fetchall()
    return {(row[0], row[1], row[2], row[3]): row[4] for row in rows}


def _snapshot_timeframe_stats(con: duckdb.DuckDBPyConnection, timeframe: str) -> dict:
    coverage = con.execute(
        """
        SELECT COUNT(*), COALESCE(SUM(num_rows), 0)
        FROM md.md_partition_coverage
        WHERE market = 'futures' AND timeframe = $1;
        """,
        [timeframe],
    ).fetchone()
    stats = con.execute(
        """
        SELECT COUNT(*)
        FROM md.md_symbol_dataset_stats
        WHERE market = 'futures' AND timeframe = $1;
        """,
        [timeframe],
    ).fetchone()
    alignment = con.execute(
        """
        SELECT COUNT(*), COALESCE(SUM(CAST(all_core_present AS INTEGER)), 0)
        FROM md.md_alignment_summary
        WHERE market = 'futures' AND timeframe = $1;
        """,
        [timeframe],
    ).fetchone()
    liquidity = con.execute(
        """
        SELECT COUNT(*), COALESCE(SUM(dollar_volume), 0)
        FROM md.md_liquidity_daily
        WHERE market = 'futures' AND timeframe = $1;
        """,
        [timeframe],
    ).fetchone()
    return {
        "coverage": coverage,
        "stats": stats,
        "alignment": alignment,
        "liquidity": liquidity,
    }


def test_funding_policy_timeframe_scoped(tmp_lake_root: Path, tmp_duckdb_path: Path) -> None:
    _add_ts_column_to_1d_files(tmp_lake_root)

    build_or_update_metadata(
        tmp_lake_root,
        market="futures",
        datasets=["klines", "mark", "index", "funding"],
        meta_db_path=tmp_duckdb_path,
        timeframe_filter="1d",
    )

    con = connect_meta_db(tmp_duckdb_path)
    try:
        funding_coverage = con.execute(
            """
            SELECT COUNT(*)
            FROM md.md_partition_coverage
            WHERE market = 'futures' AND dataset = 'funding';
            """
        ).fetchone()[0]
        funding_manifest = con.execute(
            """
            SELECT COUNT(*)
            FROM md.md_file_manifest
            WHERE file_rel LIKE '%/funding/%';
            """
        ).fetchone()[0]
        funding_stats_1d = con.execute(
            """
            SELECT COUNT(*)
            FROM md.md_symbol_dataset_stats
            WHERE market = 'futures' AND dataset = 'funding' AND timeframe = '1d';
            """
        ).fetchone()[0]
    finally:
        con.close()

    assert funding_coverage == 0
    assert funding_manifest == 0
    assert funding_stats_1d == 0

    build_or_update_metadata(
        tmp_lake_root,
        market="futures",
        datasets=["klines", "mark", "index", "funding"],
        meta_db_path=tmp_duckdb_path,
        timeframe_filter="1m",
    )

    con = connect_meta_db(tmp_duckdb_path)
    try:
        funding_coverage_1m = con.execute(
            """
            SELECT COUNT(*)
            FROM md.md_partition_coverage
            WHERE market = 'futures' AND dataset = 'funding' AND timeframe = '';
            """
        ).fetchone()[0]
        funding_manifest_1m = con.execute(
            """
            SELECT COUNT(*)
            FROM md.md_file_manifest
            WHERE file_rel LIKE '%/funding/%';
            """
        ).fetchone()[0]
        funding_stats_1m = con.execute(
            """
            SELECT COUNT(*)
            FROM md.md_symbol_dataset_stats
            WHERE market = 'futures' AND dataset = 'funding' AND timeframe = '1m';
            """
        ).fetchone()[0]
    finally:
        con.close()

    assert funding_coverage_1m > 0
    assert funding_manifest_1m > 0
    assert funding_stats_1m > 0


def test_cross_timeframe_build_does_not_clobber_1m(
    tmp_lake_root: Path,
    tmp_duckdb_path: Path,
) -> None:
    _add_ts_column_to_1d_files(tmp_lake_root)

    build_or_update_metadata(
        tmp_lake_root,
        market="futures",
        datasets=["klines", "mark", "index", "funding"],
        meta_db_path=tmp_duckdb_path,
        timeframe_filter="1m",
    )

    con = connect_meta_db(tmp_duckdb_path)
    try:
        before = _snapshot_timeframe_stats(con, "1m")
    finally:
        con.close()

    build_or_update_metadata(
        tmp_lake_root,
        market="futures",
        datasets=["klines", "mark", "index", "funding"],
        meta_db_path=tmp_duckdb_path,
        timeframe_filter="1d",
    )

    con = connect_meta_db(tmp_duckdb_path)
    try:
        after = _snapshot_timeframe_stats(con, "1m")
    finally:
        con.close()

    assert before == after


def test_idempotent_1m_runs(tmp_lake_root: Path, tmp_duckdb_path: Path) -> None:
    build_or_update_metadata(
        tmp_lake_root,
        market="futures",
        datasets=["klines", "mark", "index", "funding"],
        meta_db_path=tmp_duckdb_path,
        timeframe_filter="1m",
    )

    con = connect_meta_db(tmp_duckdb_path)
    try:
        before = _snapshot_timeframe_stats(con, "1m")
    finally:
        con.close()

    build_or_update_metadata(
        tmp_lake_root,
        market="futures",
        datasets=["klines", "mark", "index", "funding"],
        meta_db_path=tmp_duckdb_path,
        timeframe_filter="1m",
    )

    con = connect_meta_db(tmp_duckdb_path)
    try:
        after = _snapshot_timeframe_stats(con, "1m")
    finally:
        con.close()

    assert before == after


def test_liquidity_noop_when_no_klines_changes(tmp_lake_root: Path, tmp_duckdb_path: Path) -> None:
    _seed_synthetic_klines(tmp_lake_root, start_day=date(2025, 12, 25), days=3, symbol="ZRXUSDTM")

    build_or_update_metadata(
        tmp_lake_root,
        market="futures",
        datasets=["klines", "mark", "index", "funding"],
        meta_db_path=tmp_duckdb_path,
        timeframe_filter="1m",
    )

    con = connect_meta_db(tmp_duckdb_path)
    try:
        before_snapshot = _snapshot_liquidity(con, "1m")
    finally:
        con.close()

    build_or_update_metadata(
        tmp_lake_root,
        market="futures",
        datasets=["klines", "mark", "index", "funding"],
        meta_db_path=tmp_duckdb_path,
        timeframe_filter="1m",
    )

    con = connect_meta_db(tmp_duckdb_path)
    try:
        after_snapshot = _snapshot_liquidity(con, "1m")
    finally:
        con.close()

    assert len(before_snapshot) == len(after_snapshot)
    assert before_snapshot == after_snapshot

    touched = (
        tmp_lake_root
        / "futures"
        / "klines"
        / "timeframe=1m"
        / "symbol=ZRXUSDTM"
        / "date=2025-12-30"
        / "data.parquet"
    )
    _touch(touched)

    build_or_update_metadata(
        tmp_lake_root,
        market="futures",
        datasets=["klines", "mark", "index", "funding"],
        meta_db_path=tmp_duckdb_path,
        timeframe_filter="1m",
    )

    con = connect_meta_db(tmp_duckdb_path)
    try:
        touched_snapshot = _snapshot_liquidity(con, "1m")
    finally:
        con.close()

    assert len(after_snapshot) == len(touched_snapshot)

    change_date = date(2025, 12, 30)
    changed_keys = []
    unchanged_keys = []
    for key, computed_at in touched_snapshot.items():
        if key[3] < change_date:
            unchanged_keys.append(key)
            assert computed_at == after_snapshot[key]
        else:
            changed_keys.append(key)
            assert computed_at != after_snapshot[key]

    assert changed_keys
    assert unchanged_keys
