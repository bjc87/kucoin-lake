from __future__ import annotations

from pathlib import Path
from typing import Any, Iterable, Optional, Sequence

from kucoin_lake import ingest as mirror_module
from kucoin_lake import fetch as fetch_module
from kucoin_lake import metadata as metadata_module
from kucoin_lake import resample as resample_module
from kucoin_lake.constants import DEFAULT_FUTURES_DATASETS


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
) -> dict:
    """
    Notebook-friendly wrapper for kucoin_lake.metadata.build_or_update_metadata.
    """
    return metadata_module.build_or_update_metadata(
        nas_root,
        market=market,
        datasets=datasets,
        meta_db_path=meta_db_path,
        timeframe_filter=timeframe_filter,
        liquidity_only=liquidity_only,
        completeness_threshold=completeness_threshold,
        incremental_chunk_size=incremental_chunk_size,
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
