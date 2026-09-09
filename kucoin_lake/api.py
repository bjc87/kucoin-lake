from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Any, Iterable, Optional, Sequence

from kucoin_lake import fetch as fetch_module
from kucoin_lake import ingest as mirror_module
from kucoin_lake import metadata as metadata_module
from kucoin_lake import resample as resample_module
from kucoin_lake.constants import DEFAULT_FUTURES_DATASETS
from kucoin_lake.validation.derived import validate_derived_1d
from kucoin_lake.validation.metadata import validate_metadata
from kucoin_lake.validation.models import ValidationRunResult
from kucoin_lake.validation.orchestrator import DEFAULT_DERIVED_DATASETS, orchestrate_validation


def build_metadata(
    nas_root: str | Path,
    *,
    market: str = "futures",
    datasets: Iterable[str] = DEFAULT_FUTURES_DATASETS,
    meta_db_path: Optional[str | Path] = None,
    timeframe_filter: str = "1m",
    liquidity_only: bool = False,
    completeness_threshold: float = 0.98,
    incremental_chunk_size: int = 5000,
    symbols: Optional[Sequence[str]] = None,
    date_start: Optional[date | str] = None,
    date_end: Optional[date | str] = None,
) -> dict:
    """
    Notebook-friendly wrapper for kucoin_lake.metadata.build_or_update_metadata.
    """
    if isinstance(date_start, str):
        date_start = date.fromisoformat(date_start)
    if isinstance(date_end, str):
        date_end = date.fromisoformat(date_end)
    return metadata_module.build_or_update_metadata(
        nas_root,
        market=market,
        datasets=datasets,
        meta_db_path=meta_db_path,
        timeframe_filter=timeframe_filter,
        liquidity_only=liquidity_only,
        completeness_threshold=completeness_threshold,
        incremental_chunk_size=incremental_chunk_size,
        symbols=symbols,
        date_start=date_start,
        date_end=date_end,
    )


def validate(
    *,
    check: str = "metadata",
    nas_root: str | Path,
    meta_db_path: str | Path | None = None,
    market: str = "futures",
    datasets: Iterable[str] = DEFAULT_FUTURES_DATASETS,
    dataset: str = "klines",
    derived_datasets: Sequence[str] = DEFAULT_DERIVED_DATASETS,
    timeframe_filter: str = "1m",
    symbols: Optional[Sequence[str]] = None,
    date_start: Optional[date | str] = None,
    date_end: Optional[date | str] = None,
    months: Optional[Sequence[str]] = None,
    candidate_rank_threshold: int = 150,
    append_date_start: Optional[date | str] = None,
    append_date_end: Optional[date | str] = None,
    output_dir: Optional[str | Path] = None,
    profile: str = "smoke",
    sample_limit: Optional[int] = None,
    keep_temp_db: bool = False,
) -> ValidationRunResult:
    if isinstance(date_start, str):
        date_start = date.fromisoformat(date_start)
    if isinstance(date_end, str):
        date_end = date.fromisoformat(date_end)
    if isinstance(append_date_start, str):
        append_date_start = date.fromisoformat(append_date_start)
    if isinstance(append_date_end, str):
        append_date_end = date.fromisoformat(append_date_end)

    if check == "metadata":
        if meta_db_path is None:
            raise ValueError("meta_db_path is required for check='metadata'")
        return validate_metadata(
            nas_root,
            meta_db_path=meta_db_path,
            market=market,
            datasets=datasets,
            timeframe_filter=timeframe_filter,
            symbols=symbols,
            date_start=date_start,
            date_end=date_end,
            output_dir=output_dir,
            profile=profile,
            keep_temp_db=keep_temp_db,
        )

    if check == "derived-1d":
        return validate_derived_1d(
            nas_root,
            market=market,
            dataset=dataset,
            symbols=symbols,
            date_start=date_start,
            date_end=date_end,
            months=months,
            output_dir=output_dir,
            profile=profile,
            sample_limit=sample_limit,
        )

    if check == "all":
        if meta_db_path is None:
            raise ValueError("meta_db_path is required for check='all'")
        return orchestrate_validation(
            nas_root,
            meta_db_path=meta_db_path,
            market=market,
            datasets=datasets,
            derived_datasets=derived_datasets,
            timeframe_filter=timeframe_filter,
            symbols=symbols,
            date_start=date_start,
            date_end=date_end,
            append_date_start=append_date_start,
            append_date_end=append_date_end,
            candidate_rank_threshold=candidate_rank_threshold,
            output_dir=output_dir,
            profile=profile,
            sample_limit=sample_limit,
            keep_temp_db=keep_temp_db,
        )

    raise ValueError("validate() supports check='metadata' | 'derived-1d' | 'all'")


def validate_all(
    *,
    nas_root: str | Path,
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
    return validate(
        check="all",
        nas_root=nas_root,
        meta_db_path=meta_db_path,
        market=market,
        datasets=datasets,
        derived_datasets=derived_datasets,
        timeframe_filter=timeframe_filter,
        symbols=symbols,
        date_start=date_start,
        date_end=date_end,
        append_date_start=append_date_start,
        append_date_end=append_date_end,
        candidate_rank_threshold=candidate_rank_threshold,
        output_dir=output_dir,
        profile=profile,
        sample_limit=sample_limit,
        keep_temp_db=keep_temp_db,
    )


def build_kline_integrity(
    nas_root: str | Path,
    *,
    market: str = "futures",
    meta_db_path: Optional[str | Path] = None,
    timeframe_filter: str = "1m",
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    changed_files: Optional[Sequence[Any]] = None,
    recompute: bool = False,
    incremental_chunk_size: int = 5000,
) -> dict:
    """
    Notebook-friendly wrapper for kucoin_lake.metadata.build_or_update_kline_integrity_day.
    """
    return metadata_module.build_or_update_kline_integrity_day(
        nas_root,
        market=market,
        meta_db_path=meta_db_path,
        timeframe_filter=timeframe_filter,
        start_date=start_date,
        end_date=end_date,
        changed_files=changed_files,
        recompute=recompute,
        incremental_chunk_size=incremental_chunk_size,
    )


def backfill_derived_from_coverage(
    nas_root: str | Path,
    *,
    market: str = "futures",
    timeframe_filter: str = "1m",
    meta_db_path: Optional[str | Path] = None,
    symbols: Optional[Sequence[str]] = None,
    date_start: Optional[date | str] = None,
    date_end: Optional[date | str] = None,
    what: Sequence[str] | str = ("liquidity", "integrity"),
    incremental_chunk_size: int = 5000,
) -> dict:
    """
    Notebook-friendly wrapper for kucoin_lake.metadata.backfill_derived_from_coverage.
    """
    if isinstance(date_start, str):
        date_start = date.fromisoformat(date_start)
    if isinstance(date_end, str):
        date_end = date.fromisoformat(date_end)

    if isinstance(what, str):
        what_norm = what.lower()
        if what_norm == "both":
            what = ("liquidity", "integrity")
        else:
            what = (what_norm,)
    else:
        what = tuple(w.lower() for w in what)

    return metadata_module.backfill_derived_from_coverage(
        nas_root,
        market=market,
        timeframe_filter=timeframe_filter,
        meta_db_path=meta_db_path,
        symbols=symbols,
        date_start=date_start,
        date_end=date_end,
        what=what,
        incremental_chunk_size=incremental_chunk_size,
    )


def resample_1m_to_1d(
    nas_root: str | Path,
    local_staging_dir: Optional[str | Path] = None,
    *,
    market: str = "futures",
    dataset: str = "klines",
    changed_files: Optional[Sequence[Any]] = None,
    test_symbols: Optional[Sequence[str]] = None,
    test_months: Optional[Sequence[str]] = None,
    max_tasks: Optional[int] = None,
    plan_only: bool = False,
    overwrite: bool = True,
    threads: int = 4,
    memory_limit: Optional[str] = None,
    verbose: bool = False,
) -> dict:
    """
    Notebook-friendly wrapper for kucoin_lake.resample.resample_bars (1m -> 1d).
    """
    return resample_module.resample_bars(
        nas_root,
        local_staging_dir,
        market=market,
        dataset=dataset,
        timeframe_src="1m",
        timeframe_dst="1d",
        changed_files=changed_files,
        test_symbols=test_symbols,
        test_months=test_months,
        max_tasks=max_tasks,
        plan_only=plan_only,
        overwrite=overwrite,
        threads=threads,
        memory_limit=memory_limit,
        use_tqdm=verbose,
    )


def ingest_local_downloads_to_lake(
    *,
    startdate: str | None = None,
    enddate: str | None = None,
    assets: Iterable[str] | None = None,
    timeframes: Iterable[str] | None = None,
    include_klines: bool = True,
    include_funding: bool = True,
    include_mark: bool = True,
    include_index: bool = True,
    done_set_mode: str = "scan",
    local_root: Optional[str | Path] = None,
    verbose: bool = False,
) -> dict:
    """
    Notebook-friendly wrapper for archive.nas_parquet_mirror.run_ingest.
    """
    root = Path(local_root) if local_root is not None else mirror_module.LOCAL_ROOT
    return mirror_module.run_ingest(
        startdate=startdate,
        enddate=enddate,
        assets=assets,
        timeframes=timeframes,
        include_klines=include_klines,
        include_funding=include_funding,
        include_mark=include_mark,
        include_index=include_index,
        done_set_mode=done_set_mode,
        local_root=root,
        show_progress=verbose,
        verbose=verbose,
    )


def fetch_futures(
    out_root: str | Path,
    symbols: str | list[str],
    datatype: str | list[str],
    *,
    timeframe: str = "1m",
    start_date: str | None = None,
    end_date: str | None = None,
    days: int | None = None,
    sleep_s: float = 0.02,
    retries: int = 6,
    backoff_s: float = 1.0,
    timeout: tuple[float, float] = (10, 300),
    dry_run: bool = False,
    show_progress: bool = True,
    verbose: bool = False,
) -> dict:
    """
    Notebook-friendly wrapper for kucoin_lake.fetch.fetch_futures.
    """
    return fetch_module.fetch_futures(
        out_root,
        symbols,
        datatype,
        timeframe=timeframe,
        start_date=start_date,
        end_date=end_date,
        days=days,
        sleep_s=sleep_s,
        retries=retries,
        backoff_s=backoff_s,
        timeout=timeout,
        dry_run=dry_run,
        show_progress=show_progress,
        verbose=verbose,
    )
