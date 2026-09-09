from __future__ import annotations

from pathlib import Path

import duckdb

from kucoin_lake import api as api_module
from kucoin_lake.validation.metadata import validate_metadata


def _build_fixture_metadata(tmp_lake_root: Path, tmp_duckdb_path: Path) -> None:
    api_module.build_metadata(
        tmp_lake_root,
        market="futures",
        datasets=["klines", "mark", "index", "funding"],
        meta_db_path=tmp_duckdb_path,
        timeframe_filter="1m",
    )


def _check_map(run_result) -> dict[str, object]:
    return {check.check_id: check for check in run_result.checks}


def test_validate_metadata_smoke_happy_path(tmp_lake_root: Path, tmp_duckdb_path: Path, tmp_path: Path) -> None:
    _build_fixture_metadata(tmp_lake_root, tmp_duckdb_path)
    out_dir = tmp_path / "validation-smoke"

    run = api_module.validate(
        check="metadata",
        nas_root=tmp_lake_root,
        meta_db_path=tmp_duckdb_path,
        market="futures",
        datasets=["klines", "mark", "index", "funding"],
        timeframe_filter="1m",
        output_dir=out_dir,
        profile="smoke",
    )

    checks = _check_map(run)
    assert run.status == "PASS"
    assert checks["rebuild.partition_coverage"].status == "PASS"
    assert checks["invariant.integrity_timeframe_only_1m"].status == "PASS"
    assert (out_dir / "run_summary.json").exists()
    assert (out_dir / "checks.jsonl").exists()


def test_validate_metadata_full_detects_mismatch_and_writes_diffs(
    tmp_lake_root: Path,
    tmp_duckdb_path: Path,
    tmp_path: Path,
) -> None:
    _build_fixture_metadata(tmp_lake_root, tmp_duckdb_path)
    out_dir = tmp_path / "validation-full"

    con = duckdb.connect(tmp_duckdb_path.as_posix())
    try:
        key = con.execute(
            """
            SELECT market, timeframe, symbol, date
            FROM md.md_liquidity_daily
            WHERE market = 'futures' AND timeframe = '1m'
            LIMIT 1;
            """
        ).fetchone()
        assert key is not None
        con.execute(
            """
            UPDATE md.md_liquidity_daily
            SET dollar_volume = COALESCE(dollar_volume, 0) + 7.0
            WHERE market = ? AND timeframe = ? AND symbol = ? AND date = ?;
            """,
            list(key),
        )
    finally:
        con.close()

    run = validate_metadata(
        tmp_lake_root,
        meta_db_path=tmp_duckdb_path,
        market="futures",
        datasets=["klines", "mark", "index", "funding"],
        timeframe_filter="1m",
        output_dir=out_dir,
        profile="full",
    )

    checks = _check_map(run)
    assert run.status == "FAIL"
    assert checks["rebuild.liquidity_daily"].status == "FAIL"
    assert checks["rebuild.liquidity_daily"].artifacts is not None
    diff_dir = out_dir / "artifacts" / "rebuild.liquidity_daily"
    assert (diff_dir / "only_in_source.csv").exists()
    assert (diff_dir / "only_in_rebuild.csv").exists()
    assert (diff_dir / "diff_summary.json").exists()


def test_validate_metadata_invariants_fail_on_invalid_states(
    tmp_lake_root: Path,
    tmp_duckdb_path: Path,
    tmp_path: Path,
) -> None:
    _build_fixture_metadata(tmp_lake_root, tmp_duckdb_path)
    out_dir = tmp_path / "validation-invariants"

    con = duckdb.connect(tmp_duckdb_path.as_posix())
    try:
        cov_key = con.execute(
            """
            SELECT market, dataset, symbol, date, timeframe
            FROM md.md_partition_coverage
            WHERE market = 'futures'
              AND timeframe = '1m'
              AND completeness_ratio IS NOT NULL
            LIMIT 1;
            """
        ).fetchone()
        assert cov_key is not None
        con.execute(
            """
            UPDATE md.md_partition_coverage
            SET completeness_ratio = 1.5
            WHERE market = ? AND dataset = ? AND symbol = ? AND date = ? AND timeframe = ?;
            """,
            list(cov_key),
        )

        con.execute(
            """
            INSERT INTO md.md_kline_integrity_day (
                market, symbol, date, timeframe,
                num_rows, distinct_minute_cnt, gap_rows_vs_expected, duplicate_rows,
                high_lt_low_cnt, nonpositive_price_cnt, max_abs_ret_1m, computed_at_utc
            )
            VALUES (
                'futures', 'BADUSDTM', CAST('2099-01-01' AS DATE), '1d',
                0, 0, 0, 0, 0, 0, 0.0, NOW()
            );
            """
        )

        con.execute("CREATE TABLE md._liq_bak AS SELECT * FROM md.md_liquidity_daily;")
        con.execute("DROP TABLE md.md_liquidity_daily;")
        con.execute("CREATE TABLE md.md_liquidity_daily AS SELECT * FROM md._liq_bak LIMIT 0;")
        con.execute("INSERT INTO md.md_liquidity_daily SELECT * FROM md._liq_bak;")
        con.execute("INSERT INTO md.md_liquidity_daily SELECT * FROM md._liq_bak LIMIT 1;")
        con.execute("DROP TABLE md._liq_bak;")
    finally:
        con.close()

    run = validate_metadata(
        tmp_lake_root,
        meta_db_path=tmp_duckdb_path,
        market="futures",
        datasets=["klines", "mark", "index", "funding"],
        timeframe_filter="1m",
        output_dir=out_dir,
        profile="smoke",
    )

    checks = _check_map(run)
    assert run.status == "FAIL"
    assert checks["invariant.coverage_completeness_ratio_bounds"].status == "FAIL"
    assert checks["invariant.integrity_timeframe_only_1m"].status == "FAIL"
    assert checks["invariant.no_duplicate_primary_keys"].status == "FAIL"

