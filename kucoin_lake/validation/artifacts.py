from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd

from kucoin_lake.validation.models import ValidationRunResult


def ensure_output_dir(output_dir: str | Path) -> Path:
    base = Path(output_dir)
    base.mkdir(parents=True, exist_ok=True)
    return base


def artifact_path(output_dir: str | Path, filename: str) -> Path:
    return ensure_output_dir(output_dir) / filename


def write_json_payload(output_dir: str | Path, filename: str, payload: Any) -> Path:
    path = artifact_path(output_dir, filename)
    with path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, sort_keys=True, ensure_ascii=False)
        f.write("\n")
    return path


def write_dataframe_csv(output_dir: str | Path, filename: str, frame: pd.DataFrame) -> Path:
    path = artifact_path(output_dir, filename)
    frame.to_csv(path, index=False)
    return path


def write_dataframe_parquet(output_dir: str | Path, filename: str, frame: pd.DataFrame) -> Path:
    path = artifact_path(output_dir, filename)
    frame.to_parquet(path, index=False)
    return path


def write_checks_jsonl(output_dir: str | Path, run_result: ValidationRunResult) -> Path:
    path = artifact_path(output_dir, "checks.jsonl")
    with path.open("w", encoding="utf-8") as f:
        for check in run_result.checks:
            f.write(json.dumps(check.to_dict(), sort_keys=True, ensure_ascii=False))
            f.write("\n")
    return path


def write_run_summary(output_dir: str | Path, run_result: ValidationRunResult) -> Path:
    return write_json_payload(output_dir, "run_summary.json", run_result.to_dict())


def write_run_artifacts(output_dir: str | Path, run_result: ValidationRunResult) -> dict[str, str]:
    summary_path = write_run_summary(output_dir, run_result)
    checks_path = write_checks_jsonl(output_dir, run_result)
    return {
        "run_summary": summary_path.as_posix(),
        "checks": checks_path.as_posix(),
    }

