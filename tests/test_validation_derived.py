from __future__ import annotations

from pathlib import Path

import duckdb

from kucoin_lake.validation.derived import validate_derived_1d


def _check_map(run_result) -> dict[str, object]:
    return {check.check_id: check for check in run_result.checks}


def _rewrite_klines_1d_close(path: Path, *, day: str, bump: float) -> None:
    tmp = path.with_suffix(".tmp.parquet")
    path_sql = path.as_posix().replace("'", "''")
    tmp_sql = tmp.as_posix().replace("'", "''")
    day_sql = day.replace("'", "''")
    con = duckdb.connect()
    try:
        con.execute(
            f"""
            COPY (
                SELECT
                    date,
                    open,
                    high,
                    low,
                    CASE WHEN date = CAST('{day_sql}' AS DATE) THEN close + {float(bump)} ELSE close END AS close,
                    volume,
                    dollar_volume,
                    vwap,
                    src_rows,
                    min_ts,
                    max_ts,
                    month,
                    symbol,
                    timeframe
                FROM read_parquet('{path_sql}')
            ) TO '{tmp_sql}' (FORMAT PARQUET);
            """
        )
    finally:
        con.close()
    tmp.replace(path)


def _drop_klines_1d_day(path: Path, *, day: str) -> None:
    tmp = path.with_suffix(".tmp.parquet")
    path_sql = path.as_posix().replace("'", "''")
    tmp_sql = tmp.as_posix().replace("'", "''")
    day_sql = day.replace("'", "''")
    con = duckdb.connect()
    try:
        con.execute(
            f"""
            COPY (
                SELECT *
                FROM read_parquet('{path_sql}')
                WHERE date <> CAST('{day_sql}' AS DATE)
            ) TO '{tmp_sql}' (FORMAT PARQUET);
            """
        )
    finally:
        con.close()
    tmp.replace(path)


def _write_extra_klines_1d_file(path: Path, *, day: str, symbol: str, month: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path_sql = path.as_posix().replace("'", "''")
    day_sql = day.replace("'", "''")
    symbol_sql = symbol.replace("'", "''")
    month_sql = month.replace("'", "''")
    con = duckdb.connect()
    try:
        con.execute(
            f"""
            COPY (
                SELECT
                    CAST('{day_sql}' AS DATE) AS date,
                    1.0::DOUBLE AS open,
                    1.0::DOUBLE AS high,
                    1.0::DOUBLE AS low,
                    1.0::DOUBLE AS close,
                    1.0::DOUBLE AS volume,
                    1.0::DOUBLE AS dollar_volume,
                    1.0::DOUBLE AS vwap,
                    1::BIGINT AS src_rows,
                    CAST('{day_sql} 00:00:00' AS TIMESTAMP) AS min_ts,
                    CAST('{day_sql} 23:59:00' AS TIMESTAMP) AS max_ts,
                    '{month_sql}'::VARCHAR AS month,
                    '{symbol_sql}'::VARCHAR AS symbol,
                    '1d'::VARCHAR AS timeframe
            ) TO '{path_sql}' (FORMAT PARQUET);
            """
        )
    finally:
        con.close()


def test_validate_derived_1d_smoke_happy_path_scoped_dates(tmp_lake_root: Path, tmp_path: Path) -> None:
    out_dir = tmp_path / "validation-derived-smoke-pass"
    run = validate_derived_1d(
        tmp_lake_root,
        market="futures",
        dataset="klines",
        date_start="2025-12-30",
        date_end="2025-12-31",
        output_dir=out_dir,
        profile="smoke",
    )

    checks = _check_map(run)
    assert run.status == "PASS"
    assert checks["derived.partition_shape"].status == "PASS"
    assert checks["derived.unique_symbol_date"].status == "PASS"
    assert checks["derived.aggregate_match"].status == "PASS"
    assert checks["derived.optional_rollup_fields"].status == "PASS"
    assert (out_dir / "run_summary.json").exists()
    assert (out_dir / "checks.jsonl").exists()


def test_validate_derived_1d_detects_missing_output_rows(tmp_lake_root: Path, tmp_path: Path) -> None:
    one_day_path = (
        tmp_lake_root
        / "futures"
        / "klines"
        / "timeframe=1d"
        / "symbol=ZRXUSDTM"
        / "month=2025-12"
        / "data.parquet"
    )
    _drop_klines_1d_day(one_day_path, day="2025-12-30")

    out_dir = tmp_path / "validation-derived-missing"
    run = validate_derived_1d(
        tmp_lake_root,
        market="futures",
        dataset="klines",
        date_start="2025-12-30",
        date_end="2025-12-30",
        output_dir=out_dir,
        profile="smoke",
    )

    checks = _check_map(run)
    agg = checks["derived.aggregate_match"]
    assert run.status == "FAIL"
    assert agg.status == "FAIL"
    assert agg.metrics["missing_day_rows"] == 1
    assert agg.metrics["extra_day_rows"] == 0


def test_validate_derived_1d_detects_extra_output_rows(tmp_lake_root: Path, tmp_path: Path) -> None:
    extra_file = (
        tmp_lake_root
        / "futures"
        / "klines"
        / "timeframe=1d"
        / "symbol=ZRXUSDTM"
        / "month=2025-11"
        / "data.parquet"
    )
    _write_extra_klines_1d_file(
        extra_file,
        day="2025-11-15",
        symbol="ZRXUSDTM",
        month="2025-11",
    )

    out_dir = tmp_path / "validation-derived-extra"
    run = validate_derived_1d(
        tmp_lake_root,
        market="futures",
        dataset="klines",
        date_start="2025-11-15",
        date_end="2025-11-15",
        output_dir=out_dir,
        profile="smoke",
    )

    checks = _check_map(run)
    agg = checks["derived.aggregate_match"]
    assert run.status == "FAIL"
    assert agg.status == "FAIL"
    assert agg.metrics["missing_day_rows"] == 0
    assert agg.metrics["extra_day_rows"] == 1


def test_validate_derived_1d_full_writes_artifacts_on_value_mismatch(
    tmp_lake_root: Path,
    tmp_path: Path,
) -> None:
    one_day_path = (
        tmp_lake_root
        / "futures"
        / "klines"
        / "timeframe=1d"
        / "symbol=ZRXUSDTM"
        / "month=2025-12"
        / "data.parquet"
    )
    _rewrite_klines_1d_close(one_day_path, day="2025-12-30", bump=0.001)

    out_dir = tmp_path / "validation-derived-full"
    run = validate_derived_1d(
        tmp_lake_root,
        market="futures",
        dataset="klines",
        date_start="2025-12-30",
        date_end="2025-12-30",
        output_dir=out_dir,
        profile="full",
    )

    checks = _check_map(run)
    agg = checks["derived.aggregate_match"]
    assert run.status == "FAIL"
    assert agg.status == "FAIL"
    assert agg.artifacts is not None

    artifact_dir = out_dir / "artifacts" / "derived.aggregate_match"
    assert (artifact_dir / "missing_output_rows.csv").exists()
    assert (artifact_dir / "extra_output_rows.csv").exists()
    assert (artifact_dir / "value_mismatches.csv").exists()
    assert (artifact_dir / "summary.json").exists()
