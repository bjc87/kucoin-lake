from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, Sequence
from uuid import uuid4

from kucoin_lake.validation.models import (
    STATUS_ERROR,
    STATUS_FAIL,
    STATUS_PASS,
    STATUS_SKIP,
    STATUS_WARN,
    ValidationCheckResult,
    ValidationRunResult,
)


def compute_overall_status(checks: Sequence[ValidationCheckResult]) -> str:
    statuses = {check.status for check in checks}
    if STATUS_ERROR in statuses:
        return STATUS_ERROR
    if STATUS_FAIL in statuses:
        return STATUS_FAIL
    if STATUS_WARN in statuses:
        return STATUS_WARN
    return STATUS_PASS


def default_run_id(validator: str) -> str:
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"{validator}-{ts}-{uuid4().hex[:8]}"


def build_validation_run_result(
    *,
    validator: str,
    checks: Sequence[ValidationCheckResult],
    output_dir: str | Path,
    run_id: Optional[str] = None,
) -> ValidationRunResult:
    output_dir_str = Path(output_dir).as_posix()
    checks_list = list(checks)
    return ValidationRunResult(
        run_id=run_id or default_run_id(validator),
        validator=validator,
        status=compute_overall_status(checks_list),
        checks=checks_list,
        output_dir=output_dir_str,
    )


def validation_status_to_exit_code(status: str) -> int:
    if status == STATUS_PASS:
        return 0
    if status == STATUS_WARN:
        return 0
    if status == STATUS_SKIP:
        return 0
    if status == STATUS_FAIL:
        return 1
    if status == STATUS_ERROR:
        return 2
    return 2
