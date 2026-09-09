from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Iterable, Optional, Sequence

from kucoin_lake.constants import DEFAULT_FUTURES_DATASETS
from kucoin_lake.validation.artifacts import ensure_output_dir, write_json_payload, write_run_artifacts
from kucoin_lake.validation.derived import validate_derived_1d
from kucoin_lake.validation.metadata import validate_metadata
from kucoin_lake.validation.models import (
    STATUS_PASS,
    STATUS_SKIP,
    ValidationArtifact,
    ValidationCheckResult,
    ValidationRunResult,
)
from kucoin_lake.validation.runner import build_validation_run_result, default_run_id
from kucoin_lake.validation.scope_selection import (
    select_candidate_superset_symbols,
    select_near_threshold_symbols,
)

DEFAULT_DERIVED_DATASETS: tuple[str, ...] = ("klines", "mark", "index")


def _normalize_date(value: date | str | None) -> date | None:
    if value is None or isinstance(value, date):
        return value
    return date.fromisoformat(value)


def _child_artifacts(child: ValidationRunResult) -> list[ValidationArtifact]:
    child_dir = Path(child.output_dir)
    summary_path = child_dir / "run_summary.json"
    checks_path = child_dir / "checks.jsonl"
    artifacts: list[ValidationArtifact] = []
    if summary_path.exists():
        artifacts.append(
            ValidationArtifact(
                kind="json",
                path=summary_path.as_posix(),
                description=f"Summary for child validator {child.validator}.",
            )
        )
    if checks_path.exists():
        artifacts.append(
            ValidationArtifact(
                kind="jsonl",
                path=checks_path.as_posix(),
                description=f"Checks for child validator {child.validator}.",
            )
        )
    return artifacts


def _build_child_check(
    *,
    check_id: str,
    child: ValidationRunResult,
) -> ValidationCheckResult:
    counts = child.counts_by_status()
    return ValidationCheckResult(
        check_id=check_id,
        category="orchestration",
        status=child.status,
        target=child.validator,
        message=f"Child validator completed with status {child.status}.",
        metrics={
            "child_run_id": child.run_id,
            "child_output_dir": child.output_dir,
            "child_check_count": child.check_count,
            "child_counts_by_status": counts,
        },
        artifacts=_child_artifacts(child),
    )


def _build_skipped_child_run(
    *,
    validator: str,
    output_dir: Path,
    reason: str,
    metrics: dict[str, object] | None = None,
) -> ValidationRunResult:
    check = ValidationCheckResult(
        check_id=f"{validator}.skipped",
        category="orchestration",
        status=STATUS_SKIP,
        target=validator,
        message=reason,
        metrics=metrics,
    )
    run = build_validation_run_result(
        validator=validator,
        checks=[check],
        output_dir=output_dir,
    )
    write_run_artifacts(output_dir, run)
    return run


def orchestrate_validation(
    nas_root: str | Path,
    *,
    meta_db_path: str | Path,
    market: str = "futures",
    datasets: Iterable[str] = DEFAULT_FUTURES_DATASETS,
    derived_datasets: Sequence[str] = DEFAULT_DERIVED_DATASETS,
    timeframe_filter: str = "1m",
    symbols: Optional[Sequence[str]] = None,
    date_start: Optional[date | str] = None,
    date_end: Optional[date | str] = None,
    append_date_start: Optional[date | str] = None,
    append_date_end: Optional[date | str] = None,
    candidate_rank_threshold: int = 150,
    output_dir: Optional[str | Path] = None,
    profile: str = "smoke",
    sample_limit: Optional[int] = None,
    keep_temp_db: bool = False,
) -> ValidationRunResult:
    rank_threshold = int(candidate_rank_threshold)
    if rank_threshold <= 0:
        raise ValueError("candidate_rank_threshold must be > 0")

    append_start = _normalize_date(append_date_start)
    append_end = _normalize_date(append_date_end)
    if (append_start is None) != (append_end is None):
        raise ValueError("append_date_start and append_date_end must both be provided for append-window mode")
    if append_start is not None and append_end is not None and append_start > append_end:
        raise ValueError("append_date_start must be <= append_date_end")

    history_start = _normalize_date(date_start)
    history_end = _normalize_date(date_end)
    if history_start is not None and history_end is not None and history_start > history_end:
        raise ValueError("date_start must be <= date_end")

    append_mode = append_start is not None and append_end is not None
    scope_start = append_start if append_mode else history_start
    scope_end = append_end if append_mode else history_end
    symbols_norm = sorted(set(symbols)) if symbols else None
    metadata_datasets = list(dict.fromkeys(datasets))
    derived_datasets_norm = list(dict.fromkeys(derived_datasets))

    run_id = default_run_id("all")
    out_dir = Path(output_dir) if output_dir is not None else (Path(meta_db_path).parent / "validation" / run_id)
    ensure_output_dir(out_dir)

    selection_source: str
    selected_symbols: list[str]
    if symbols_norm is not None:
        selected_symbols = list(symbols_norm)
        selection_source = "explicit_symbols"
    elif append_mode:
        near_band = max(1, rank_threshold - 100)
        selected_symbols = select_near_threshold_symbols(
            meta_db_path=meta_db_path,
            market=market,
            timeframe=timeframe_filter,
            target_universe_size=100,
            rank_band=near_band,
            date_start=scope_start,
            date_end=scope_end,
        )
        selection_source = "near_threshold"
    else:
        selected_symbols = select_candidate_superset_symbols(
            meta_db_path=meta_db_path,
            market=market,
            timeframe=timeframe_filter,
            rank_threshold=rank_threshold,
            date_start=scope_start,
            date_end=scope_end,
        )
        selection_source = "candidate_superset"

    children: list[ValidationRunResult] = []

    metadata_profile = "smoke" if append_mode else profile
    metadata_run = validate_metadata(
        nas_root,
        meta_db_path=meta_db_path,
        market=market,
        datasets=metadata_datasets,
        timeframe_filter=timeframe_filter,
        symbols=symbols_norm,
        date_start=scope_start,
        date_end=scope_end,
        output_dir=out_dir / "metadata",
        profile=metadata_profile,
        keep_temp_db=keep_temp_db,
    )
    children.append(metadata_run)

    for dataset in derived_datasets_norm:
        child_dir = out_dir / f"derived_1d_{dataset}"
        if symbols_norm is None and not selected_symbols:
            child_run = _build_skipped_child_run(
                validator=f"derived_1d.{dataset}",
                output_dir=child_dir,
                reason="No symbols selected for derived validation scope.",
                metrics={"selection_source": selection_source},
            )
        else:
            child_run = validate_derived_1d(
                nas_root,
                market=market,
                dataset=dataset,
                symbols=selected_symbols if selected_symbols else symbols_norm,
                date_start=scope_start,
                date_end=scope_end,
                output_dir=child_dir,
                profile=profile,
                sample_limit=sample_limit,
            )
        children.append(child_run)

    checks: list[ValidationCheckResult] = [
        ValidationCheckResult(
            check_id="orchestration.scope_selection",
            category="orchestration",
            status=STATUS_PASS,
            target="symbol_scope",
            message="Derived validation symbol scope selected.",
            metrics={
                "mode": "append_window" if append_mode else "full_history",
                "selection_source": selection_source,
                "selected_symbol_count": int(len(selected_symbols)),
                "selected_symbols": selected_symbols,
                "candidate_rank_threshold": rank_threshold,
                "scope_date_start": scope_start.isoformat() if scope_start else None,
                "scope_date_end": scope_end.isoformat() if scope_end else None,
            },
        ),
        _build_child_check(check_id="orchestration.metadata", child=metadata_run),
    ]

    for child in children[1:]:
        child_id = child.validator.replace(".", "_")
        checks.append(_build_child_check(check_id=f"orchestration.{child_id}", child=child))

    aggregate = build_validation_run_result(
        validator="all",
        checks=checks,
        output_dir=out_dir,
        run_id=run_id,
    )
    write_run_artifacts(out_dir, aggregate)
    write_json_payload(
        out_dir,
        "child_runs.json",
        {
            "mode": "append_window" if append_mode else "full_history",
            "children": [child.to_dict() for child in children],
        },
    )
    return aggregate

