from __future__ import annotations

from pathlib import Path
from typing import Iterable, Optional, Sequence, Any

from archive import metadata as metadata_module
from archive import nas_parquet_mirror as mirror_module
from archive import resample as resample_module


def build_metadata(
    nas_root: str | Path,
    *,
    market: str = "futures",
    datasets: Iterable[str] = metadata_module.DEFAULT_FUTURES_DATASETS,
    meta_db_path: Optional[str | Path] = None,
    timeframe_filter: str = "1m",
    liquidity_only: bool = False,
    completeness_threshold: float = 0.98,
    incremental_chunk_size: int = 5000,
) -> dict:
    """
    Notebook-friendly wrapper for archive.metadata.build_or_update_metadata.
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
    Notebook-friendly wrapper for archive.resample.resample_bars (1m -> 1d).
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
        local_root=root,
        show_progress=verbose,
        verbose=verbose,
    )
