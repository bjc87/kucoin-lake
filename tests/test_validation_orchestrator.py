from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import duckdb

from kucoin_lake import api as api_module
from kucoin_lake.validation.artifacts import write_run_artifacts
from kucoin_lake.validation.models import ValidationCheckResult
from kucoin_lake.validation.orchestrator import orchestrate_validation
from kucoin_lake.validation.runner import build_validation_run_result


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


def _keep_1d_day_range(path: Path, *, day_start: str, day_end: str) -> None:
    tmp = path.with_suffix(".tmp.parquet")
    path_sql = path.as_posix().replace("'", "''")
    tmp_sql = tmp.as_posix().replace("'", "''")
    day_start_sql = day_start.replace("'", "''")
    day_end_sql = day_end.replace("'", "''")
    con = duckdb.connect()
    try:
        con.execute(
            f"""
            COPY (
                SELECT *
                FROM read_parquet('{path_sql}')
                WHERE date BETWEEN CAST('{day_start_sql}' AS DATE) AND CAST('{day_end_sql}' AS DATE)
            ) TO '{tmp_sql}' (FORMAT PARQUET);
            """
        )
    finally:
        con.close()
    tmp.replace(path)


def _rewrite_index_1d_close(path: Path, *, day: str, bump: float) -> None:
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


def _fake_pass_run(validator: str, output_dir: Path):
    run = build_validation_run_result(
        validator=validator,
        checks=[
            ValidationCheckResult(
                check_id=f"{validator}.check",
                category="orchestration",
                status="PASS",
                target=validator,
                message="ok",
            )
        ],
        output_dir=output_dir,
    )
    write_run_artifacts(output_dir, run)
    return run


def test_orchestrate_validation_all_success(tmp_lake_root: Path, tmp_duckdb_path: Path, tmp_path: Path) -> None:
    klines_1d_path = (
        tmp_lake_root
        / "futures"
        / "klines"
        / "timeframe=1d"
        / "symbol=ZRXUSDTM"
        / "month=2025-12"
        / "data.parquet"
    )
    _keep_1d_day_range(klines_1d_path, day_start="2025-12-30", day_end="2025-12-31")
    _build_fixture_metadata(tmp_lake_root, tmp_duckdb_path)
    out_dir = tmp_path / "validation-all-pass"

    run = orchestrate_validation(
        nas_root=tmp_lake_root,
        meta_db_path=tmp_duckdb_path,
        market="futures",
        datasets=["klines", "mark", "index", "funding"],
        derived_datasets=["klines"],
        symbols=["ZRXUSDTM"],
        output_dir=out_dir,
        profile="smoke",
    )

    checks = _check_map(run)
    assert run.status == "PASS"
    assert checks["orchestration.metadata"].status == "PASS"
    assert checks["orchestration.derived_1d_klines"].status == "PASS"
    assert (out_dir / "run_summary.json").exists()
    assert (out_dir / "checks.jsonl").exists()
    assert (out_dir / "child_runs.json").exists()
    assert (out_dir / "metadata" / "run_summary.json").exists()
    assert (out_dir / "derived_1d_klines" / "run_summary.json").exists()


def test_orchestrate_validation_all_fails_when_one_derived_dataset_fails(
    tmp_lake_root: Path,
    tmp_duckdb_path: Path,
    tmp_path: Path,
) -> None:
    mark_1d_path = (
        tmp_lake_root
        / "futures"
        / "mark"
        / "timeframe=1d"
        / "symbol=ZRXUSDTM"
        / "month=2025-12"
        / "data.parquet"
    )
    index_1d_path = (
        tmp_lake_root
        / "futures"
        / "index"
        / "timeframe=1d"
        / "symbol=ZRXUSDTM"
        / "month=2025-12"
        / "data.parquet"
    )
    _keep_1d_day_range(mark_1d_path, day_start="2025-12-22", day_end="2025-12-23")
    _keep_1d_day_range(index_1d_path, day_start="2025-12-22", day_end="2025-12-23")
    _rewrite_index_1d_close(index_1d_path, day="2025-12-22", bump=0.001)
    _build_fixture_metadata(tmp_lake_root, tmp_duckdb_path)

    out_dir = tmp_path / "validation-all-fail"
    run = orchestrate_validation(
        nas_root=tmp_lake_root,
        meta_db_path=tmp_duckdb_path,
        market="futures",
        datasets=["klines", "mark", "index", "funding"],
        derived_datasets=["mark", "index"],
        symbols=["ZRXUSDTM"],
        output_dir=out_dir,
        profile="full",
    )

    checks = _check_map(run)
    assert run.status == "FAIL"
    assert checks["orchestration.metadata"].status == "PASS"
    assert checks["orchestration.derived_1d_mark"].status == "PASS"
    assert checks["orchestration.derived_1d_index"].status == "FAIL"

    index_artifacts = out_dir / "derived_1d_index" / "artifacts" / "derived.aggregate_match"
    assert (index_artifacts / "value_mismatches.csv").exists()


def test_orchestrate_validation_append_window_scopes_checks(
    monkeypatch,
    tmp_lake_root: Path,
    tmp_duckdb_path: Path,
    tmp_path: Path,
) -> None:
    _build_fixture_metadata(tmp_lake_root, tmp_duckdb_path)
    captured_select: dict[str, object] = {}
    captured_metadata: dict[str, object] = {}

    def _fake_select_near_threshold_symbols(**kwargs):
        captured_select.update(kwargs)
        return ["ZRXUSDTM"]

    def _fake_validate_metadata(*args, **kwargs):
        captured_metadata.update(kwargs)
        return _fake_pass_run("metadata", kwargs["output_dir"])

    monkeypatch.setattr(
        "kucoin_lake.validation.orchestrator.select_near_threshold_symbols",
        _fake_select_near_threshold_symbols,
    )
    monkeypatch.setattr(
        "kucoin_lake.validation.orchestrator.validate_metadata",
        _fake_validate_metadata,
    )

    out_dir = tmp_path / "validation-all-append"
    run = orchestrate_validation(
        nas_root=tmp_lake_root,
        meta_db_path=tmp_duckdb_path,
        market="futures",
        datasets=["klines", "mark", "index", "funding"],
        derived_datasets=["klines"],
        append_date_start="2025-12-30",
        append_date_end="2025-12-31",
        output_dir=out_dir,
        profile="full",
        candidate_rank_threshold=150,
    )

    checks = _check_map(run)
    assert run.status == "PASS"
    assert checks["orchestration.scope_selection"].metrics["mode"] == "append_window"
    assert checks["orchestration.scope_selection"].metrics["selection_source"] == "near_threshold"
    assert captured_select["date_start"].isoformat() == "2025-12-30"
    assert captured_select["date_end"].isoformat() == "2025-12-31"
    assert captured_metadata["date_start"] == date(2025, 12, 30)
    assert captured_metadata["date_end"] == date(2025, 12, 31)
    assert captured_metadata["profile"] == "smoke"

    derived_summary = json.loads((out_dir / "derived_1d_klines" / "run_summary.json").read_text(encoding="utf-8"))
    derived_checks = {item["check_id"]: item for item in derived_summary["checks"]}
    assert derived_checks["derived.aggregate_match"]["metrics"]["profile"] == "full"
    assert derived_checks["derived.aggregate_match"]["metrics"]["expected_day_rows"] == 2
