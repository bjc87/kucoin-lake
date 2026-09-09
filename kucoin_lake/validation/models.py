from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
from pathlib import Path
from typing import Any, Mapping, Sequence

STATUS_PASS = "PASS"
STATUS_WARN = "WARN"
STATUS_FAIL = "FAIL"
STATUS_ERROR = "ERROR"
STATUS_SKIP = "SKIP"

ALL_STATUSES = (
    STATUS_PASS,
    STATUS_WARN,
    STATUS_FAIL,
    STATUS_ERROR,
    STATUS_SKIP,
)


def _json_friendly(value: Any) -> Any:
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    if isinstance(value, Path):
        return value.as_posix()
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Mapping):
        return {str(k): _json_friendly(v) for k, v in sorted(value.items(), key=lambda item: str(item[0]))}
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return [_json_friendly(v) for v in value]
    return str(value)


@dataclass(frozen=True)
class ValidationArtifact:
    kind: str
    path: str
    description: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "kind": self.kind,
            "path": self.path,
            "description": self.description,
        }


@dataclass(frozen=True)
class ValidationCheckResult:
    check_id: str
    category: str
    status: str
    target: str
    message: str
    metrics: dict[str, Any] | None = None
    artifacts: list[ValidationArtifact] | None = None
    details: dict[str, Any] | None = None

    def to_dict(self) -> dict[str, Any]:
        out: dict[str, Any] = {
            "check_id": self.check_id,
            "category": self.category,
            "status": self.status,
            "target": self.target,
            "message": self.message,
        }
        if self.metrics is not None:
            out["metrics"] = _json_friendly(self.metrics)
        if self.artifacts is not None:
            out["artifacts"] = [artifact.to_dict() for artifact in self.artifacts]
        if self.details is not None:
            out["details"] = _json_friendly(self.details)
        return out


@dataclass(frozen=True)
class ValidationRunResult:
    run_id: str
    validator: str
    status: str
    checks: list[ValidationCheckResult] = field(default_factory=list)
    output_dir: str = ""

    @property
    def check_count(self) -> int:
        return len(self.checks)

    def counts_by_status(self) -> dict[str, int]:
        counts = {status: 0 for status in ALL_STATUSES}
        for check in self.checks:
            if check.status in counts:
                counts[check.status] += 1
        return counts

    @property
    def pass_count(self) -> int:
        return self.counts_by_status()[STATUS_PASS]

    @property
    def warn_count(self) -> int:
        return self.counts_by_status()[STATUS_WARN]

    @property
    def fail_count(self) -> int:
        return self.counts_by_status()[STATUS_FAIL]

    @property
    def error_count(self) -> int:
        return self.counts_by_status()[STATUS_ERROR]

    @property
    def skip_count(self) -> int:
        return self.counts_by_status()[STATUS_SKIP]

    def to_dict(self) -> dict[str, Any]:
        counts = self.counts_by_status()
        return {
            "run_id": self.run_id,
            "validator": self.validator,
            "status": self.status,
            "output_dir": self.output_dir,
            "checks": [check.to_dict() for check in self.checks],
            "check_count": self.check_count,
            "counts_by_status": counts,
        }

