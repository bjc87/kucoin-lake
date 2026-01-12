from __future__ import annotations

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
