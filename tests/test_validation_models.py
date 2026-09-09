from __future__ import annotations

import json
from datetime import date
from pathlib import Path

from kucoin_lake.validation.artifacts import write_run_artifacts
from kucoin_lake.validation.models import (
    STATUS_ERROR,
    STATUS_FAIL,
    STATUS_PASS,
    STATUS_SKIP,
    STATUS_WARN,
    ValidationArtifact,
    ValidationCheckResult,
)
from kucoin_lake.validation.runner import (
    build_validation_run_result,
    compute_overall_status,
    validation_status_to_exit_code,
)


def _check(status: str, *, check_id: str = "c1") -> ValidationCheckResult:
    return ValidationCheckResult(
        check_id=check_id,
        category="unit",
        status=status,
        target="target",
        message="message",
    )


def test_compute_overall_status_precedence() -> None:
    assert compute_overall_status([_check(STATUS_PASS)]) == STATUS_PASS
    assert compute_overall_status([_check(STATUS_PASS), _check(STATUS_WARN, check_id="c2")]) == STATUS_WARN
    assert compute_overall_status([_check(STATUS_WARN), _check(STATUS_FAIL, check_id="c2")]) == STATUS_FAIL
    assert compute_overall_status([_check(STATUS_FAIL), _check(STATUS_ERROR, check_id="c2")]) == STATUS_ERROR


def test_validation_result_to_dict_and_artifact_serialization(tmp_path: Path) -> None:
    artifact = ValidationArtifact(
        kind="csv",
        path="artifacts/check/rows.csv",
        description="row-level mismatch",
    )
    check = ValidationCheckResult(
        check_id="check.example",
        category="rebuild",
        status=STATUS_WARN,
        target="md.md_liquidity_daily",
        message="Example warning.",
        metrics={"rows": 3, "as_of": date(2026, 1, 1)},
        artifacts=[artifact],
        details={"scope": {"market": "futures"}},
    )
    run = build_validation_run_result(
        validator="metadata",
        checks=[check, _check(STATUS_SKIP, check_id="check.skip")],
        output_dir=tmp_path,
        run_id="run-unit-1",
    )

    payload = run.to_dict()
    assert payload["run_id"] == "run-unit-1"
    assert payload["validator"] == "metadata"
    assert payload["status"] == STATUS_WARN
    assert payload["check_count"] == 2
    assert payload["counts_by_status"] == {
        "PASS": 0,
        "WARN": 1,
        "FAIL": 0,
        "ERROR": 0,
        "SKIP": 1,
    }
    assert payload["checks"][0]["artifacts"][0]["kind"] == "csv"
    assert payload["checks"][0]["metrics"]["as_of"] == "2026-01-01"

    paths = write_run_artifacts(tmp_path, run)
    summary_path = Path(paths["run_summary"])
    checks_path = Path(paths["checks"])
    assert summary_path.exists()
    assert checks_path.exists()

    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    lines = [json.loads(line) for line in checks_path.read_text(encoding="utf-8").splitlines()]
    assert summary["run_id"] == "run-unit-1"
    assert len(lines) == 2
    assert lines[0]["check_id"] == "check.example"


def test_validation_status_to_exit_code_mapping() -> None:
    assert validation_status_to_exit_code(STATUS_PASS) == 0
    assert validation_status_to_exit_code(STATUS_WARN) == 0
    assert validation_status_to_exit_code(STATUS_SKIP) == 0
    assert validation_status_to_exit_code(STATUS_FAIL) == 1
    assert validation_status_to_exit_code(STATUS_ERROR) == 2
    assert validation_status_to_exit_code("UNKNOWN") == 2

