from __future__ import annotations

import glob
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional, Sequence, Union

import duckdb

from kucoin_lake.ingest import LOCAL_STAGE_ROOT, atomic_copy_to_parquet
from kucoin_lake.paths import parse_lake_partitions

def _month_str(d: str) -> str:
    return d[:7]  # "YYYY-MM-DD" -> "YYYY-MM"


def _parse_symbol_date_from_path(p: str) -> tuple[Optional[str], Optional[str], Optional[str], Optional[str]]:
    parts = parse_lake_partitions(p)
    return (
        parts.get("symbol"),
        parts.get("date"),
        parts.get("timeframe"),
        parts.get("dataset"),
    )


def _to_path_list(changed_files: Optional[Sequence[Any]]) -> list[str]:
    """
    Accept:
      - list[str] paths
      - list[objects] with .file_path
    """
    if not changed_files:
        return []
    out: list[str] = []
    for x in changed_files:
        if isinstance(x, str):
            out.append(x)
        else:
            fp = getattr(x, "file_path", None)
            if fp:
                out.append(fp)
    return out


def _ensure_parent_dir(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def _atomic_replace(src: Path, dst: Path) -> None:
    """
    Atomic-ish replace on the target filesystem.
    On SMB/NAS, rename semantics vary but os.replace is the best practical option.
    """
    _ensure_parent_dir(dst)
    os.replace(src, dst)


# -----------------------------
# Planning tasks
# -----------------------------
@dataclass(frozen=True)
class ResampleTask:
    market: str
    dataset: str
    symbol: str
    month: str  # "YYYY-MM"


def _dataset_root_glob(
    nas_root: Path,
    market: str,
    dataset: str,
    timeframe: str,
) -> str:
    """
    Build input glob for scanning which (symbol,date) exist in the lake.
    - klines has timeframe partition for 1m: /klines/timeframe=1m/symbol=*/date=*/data.parquet
    - mark/index also have timeframe partition: /mark/timeframe=1m/symbol=*/date=*/data.parquet
    """
    return (nas_root / market / dataset / f"timeframe={timeframe}" / "symbol=*" / "date=*" / "data.parquet").as_posix()


def _build_source_globs(
    nas_root: Path,
    market: str,
    dataset: str,
    timeframe_src: str,
    *,
    symbols: Optional[Sequence[str]] = None,
    months: Optional[Sequence[str]] = None,
) -> list[str]:
    base = nas_root / market / dataset / f"timeframe={timeframe_src}"
    globs: list[str] = []
    if symbols and months:
        for sym in symbols:
            for month in months:
                globs.append((base / f"symbol={sym}" / f"date={month}-*" / "data.parquet").as_posix())
        return globs
    if symbols:
        for sym in symbols:
            globs.append((base / f"symbol={sym}" / "date=*" / "data.parquet").as_posix())
        return globs
    if months:
        for month in months:
            globs.append((base / "symbol=*" / f"date={month}-*" / "data.parquet").as_posix())
        return globs
    globs.append((base / "symbol=*" / "date=*" / "data.parquet").as_posix())
    return globs


def _list_source_parquet_files(
    nas_root: Path,
    market: str,
    dataset: str,
    timeframe_src: str,
    *,
    symbols: Optional[Sequence[str]] = None,
    months: Optional[Sequence[str]] = None,
) -> list[str]:
    globs = _build_source_globs(
        nas_root,
        market,
        dataset,
        timeframe_src,
        symbols=symbols,
        months=months,
    )
    matches: set[str] = set()
    for pattern in globs:
        for match in glob.glob(pattern):
            matches.add(Path(match).as_posix())
    return sorted(matches)


def plan_resample_tasks(
    nas_root: Union[str, Path],
    *,
    market: str,
    dataset: str,
    timeframe_src: str,
    changed_files: Optional[Sequence[Any]] = None,
    symbols: Optional[Sequence[str]] = None,
    months: Optional[Sequence[str]] = None,
) -> list[ResampleTask]:
    """
    Plan (symbol, month) tasks to regenerate.

    Incremental:
      - pass changed_files (from your manifest diff); we derive tasks only from changed files that match
        market/dataset/timeframe_src.

    Bulk:
      - pass changed_files None/empty; we scan the src lake and plan all (symbol, month).

    Optional filters:
      - symbols: only these symbols
      - months: only these "YYYY-MM"
    """
    nas_root = Path(nas_root)
    changed_paths = _to_path_list(changed_files)

    tasks_set: set[tuple[str, str]] = set()

    if changed_paths:
        # Incremental: derive symbol/month from changed file paths
        for p in changed_paths:
            sym, d, tf, ds = _parse_symbol_date_from_path(p)
            if ds != dataset:
                continue
            if tf != timeframe_src:
                continue
            if not sym or not d:
                continue
            tasks_set.add((sym, _month_str(d)))
    else:
        if symbols is not None or months is not None:
            file_list = _list_source_parquet_files(
                nas_root,
                market,
                dataset,
                timeframe_src,
                symbols=symbols,
                months=months,
            )
            if not file_list:
                return []
            con = duckdb.connect(":memory:")
            try:
                con.execute("SET TimeZone = 'UTC';")
                rows = con.execute(
                    """
                    SELECT DISTINCT symbol, CAST(date AS VARCHAR) AS d
                    FROM read_parquet($1, hive_partitioning=1, filename=1)
                    WHERE timeframe = $2
                    """,
                    [file_list, timeframe_src],
                ).fetchall()
            finally:
                con.close()
        else:
            # Bulk: scan existing src files via DuckDB hive partitions (cheap)
            glob_pattern = _dataset_root_glob(nas_root, market, dataset, timeframe_src)
            con = duckdb.connect(":memory:")
            try:
                con.execute("SET TimeZone = 'UTC';")
                rows = con.execute(
                    f"""
                    SELECT DISTINCT symbol, CAST(date AS VARCHAR) AS d
                    FROM read_parquet('{glob_pattern}', hive_partitioning=1, filename=0)
                    """
                ).fetchall()
            finally:
                con.close()

        for sym, d in rows:
            if sym and d:
                tasks_set.add((sym, _month_str(str(d))))

    # Apply optional filters
    if symbols is not None:
        allow = set(symbols)
        tasks_set = {t for t in tasks_set if t[0] in allow}
    if months is not None:
        allow_m = set(months)
        tasks_set = {t for t in tasks_set if t[1] in allow_m}

    return [ResampleTask(market=market, dataset=dataset, symbol=s, month=m) for (s, m) in sorted(tasks_set)]


# -----------------------------
# Core writer for one (symbol, month)
# -----------------------------
def _write_resampled_month(
    con: duckdb.DuckDBPyConnection,
    nas_root: Path,
    local_stage_root: Path,
    task: ResampleTask,
    *,
    timeframe_src: str,
    timeframe_dst: str,
    overwrite: bool,
) -> dict:
    out_path = (
        nas_root
        / task.market
        / task.dataset
        / f"timeframe={timeframe_dst}"
        / f"symbol={task.symbol}"
        / f"month={task.month}"
        / "data.parquet"
    )

    if out_path.exists() and not overwrite:
        return {"symbol": task.symbol, "month": task.month, "status": "skipped_exists", "path": out_path.as_posix()}

    in_glob = (
        nas_root
        / task.market
        / task.dataset
        / f"timeframe={timeframe_src}"
        / f"symbol={task.symbol}"
        / f"date={task.month}-*"
        / "data.parquet"
    ).as_posix()

    if task.dataset == "klines":
        select_sql = f"""
        WITH src AS (
          SELECT
            CAST(date AS DATE) AS date,
            CAST(ts AS TIMESTAMP) AS ts,
            CAST(open  AS DOUBLE) AS open,
            CAST(high  AS DOUBLE) AS high,
            CAST(low   AS DOUBLE) AS low,
            CAST(close AS DOUBLE) AS close,
            CAST(volume AS DOUBLE) AS volume
          FROM read_parquet('{in_glob}', hive_partitioning=1)
          WHERE timeframe = '{timeframe_src}'
        )
        SELECT
          date,
          ARG_MIN(open, ts)  AS open,
          MAX(high)          AS high,
          MIN(low)           AS low,
          ARG_MAX(close, ts) AS close,
          SUM(volume)        AS volume,
          SUM(close * volume) AS dollar_volume,
          CASE WHEN SUM(volume) = 0 THEN NULL ELSE SUM(close * volume) / SUM(volume) END AS vwap,
          COUNT(*)           AS src_rows,
          MIN(ts)            AS min_ts,
          MAX(ts)            AS max_ts
        FROM src
        GROUP BY date
        ORDER BY date
        """
    elif task.dataset in ("mark", "index"):
        select_sql = f"""
        WITH src AS (
          SELECT
            CAST(date AS DATE) AS date,
            CAST(ts AS TIMESTAMP) AS ts,
            CAST(open  AS DOUBLE) AS open,
            CAST(high  AS DOUBLE) AS high,
            CAST(low   AS DOUBLE) AS low,
            CAST(close AS DOUBLE) AS close
          FROM read_parquet('{in_glob}', hive_partitioning=1)
          WHERE timeframe = '{timeframe_src}'
        )
        SELECT
          date,
          ARG_MIN(open, ts)  AS open,
          MAX(high)          AS high,
          MIN(low)           AS low,
          ARG_MAX(close, ts) AS close,
          COUNT(*)           AS src_rows,
          MIN(ts)            AS min_ts,
          MAX(ts)            AS max_ts
        FROM src
        GROUP BY date
        ORDER BY date
        """
    else:
        return {"symbol": task.symbol, "month": task.month, "status": "error", "error": f"Unsupported dataset: {task.dataset}"}

    try:
        # ✅ local write -> NAS copy -> atomic rename (your proven pattern)
        atomic_copy_to_parquet(
            con,
            select_sql,
            out_path,
            nas_root=nas_root,
            local_stage_root=local_stage_root,
        )
        return {"symbol": task.symbol, "month": task.month, "status": "written", "path": out_path.as_posix()}
    except Exception as e:
        return {"symbol": task.symbol, "month": task.month, "status": "error", "error": str(e)}


# -----------------------------
# Public API: unified resample_bars
# -----------------------------
def resample_bars(
    nas_root: Union[str, Path],
    local_staging_dir: Optional[str | Path] = None,
    *,
    market: str = "futures",
    dataset: str = "klines",          # "klines" | "mark" | "index"
    timeframe_src: str = "1m",
    timeframe_dst: str = "1d",
    changed_files: Optional[Sequence[Any]] = None,

    # test controls
    test_symbols: Optional[Sequence[str]] = None,
    test_months: Optional[Sequence[str]] = None,
    max_tasks: Optional[int] = None,
    plan_only: bool = False,

    # overwrite policy
    overwrite: bool = True,

    # performance
    threads: int = 4,
    memory_limit: Optional[str] = None,

    # progress
    use_tqdm: bool = True,
) -> dict:
    """
    Unified resampler for KuCoin futures:
      - dataset="klines": resamples OHLCV
      - dataset="mark"/"index": resamples OHLC (no volume)

    Reads:
      {nas_root}/{market}/{dataset}/timeframe={timeframe_src}/symbol=*/date=*/data.parquet

    Writes:
      {nas_root}/{market}/{dataset}/timeframe={timeframe_dst}/symbol={SYM}/month={YYYY-MM}/data.parquet

    Incremental:
      - pass changed_files from your manifest diff; only affected (symbol, month) are regenerated

    Bulk:
      - omit changed_files (or pass empty) to regenerate all (symbol, month) found in src lake
      - use test_* parameters for small validation runs

    Overwrite:
      - overwrite=True (default): deterministic and safest; rewrites affected months
      - overwrite=False: skips months that already exist

    Notes on existing “test outputs”:
      - With overwrite=True, your earlier single-symbol test months will be overwritten automatically during a later full run.
      - No manual deletion needed.
    """
    nas_root = Path(nas_root)
    if local_staging_dir is None:
        local_staging_dir = LOCAL_STAGE_ROOT
    else:
        local_staging_dir = Path(local_staging_dir)
    local_staging_dir.mkdir(parents=True, exist_ok=True)

    if dataset not in ("klines", "mark", "index"):
        raise ValueError("dataset must be one of: 'klines', 'mark', 'index'")

    tasks = plan_resample_tasks(
        nas_root,
        market=market,
        dataset=dataset,
        timeframe_src=timeframe_src,
        changed_files=changed_files,
        symbols=test_symbols,
        months=test_months,
    )

    if max_tasks is not None:
        tasks = tasks[: int(max_tasks)]

    mode = "incremental" if (changed_files and len(_to_path_list(changed_files)) > 0) else "bulk"

    summary: dict = {
        "ok": True,
        "nas_root": nas_root.as_posix(),
        "market": market,
        "dataset": dataset,
        "timeframe_src": timeframe_src,
        "timeframe_dst": timeframe_dst,
        "mode": mode,
        "tasks_planned": len(tasks),
        "overwrite": bool(overwrite),
        "plan_only": bool(plan_only),
        "output_example": (
            nas_root
            / market
            / dataset
            / f"timeframe={timeframe_dst}"
            / "symbol=BTCUSDT"
            / "month=2025-12"
            / "data.parquet"
        ).as_posix(),
    }

    if plan_only or not tasks:
        summary["results"] = []
        return summary

    iterator = tasks
    if use_tqdm:
        try:
            from tqdm.auto import tqdm
            iterator = tqdm(tasks, desc=f"{dataset} {timeframe_src}->{timeframe_dst}", unit="month")
        except Exception:
            iterator = tasks

    con = duckdb.connect(":memory:")
    try:
        con.execute("SET TimeZone = 'UTC';")
        con.execute(f"PRAGMA threads={int(threads)};")
        con.execute("PRAGMA enable_object_cache=true;")
        if memory_limit:
            con.execute(f"PRAGMA memory_limit='{memory_limit}';")

        results = []
        for task in iterator:
            res = _write_resampled_month(
                con,
                nas_root,
                local_staging_dir,
                task,
                timeframe_src=timeframe_src,
                timeframe_dst=timeframe_dst,
                overwrite=overwrite,
            )
            results.append(res)

        summary["results"] = results
        summary["written"] = sum(1 for r in results if r.get("status") == "written")
        summary["skipped_exists"] = sum(1 for r in results if r.get("status") == "skipped_exists")
        summary["errors"] = [r for r in results if r.get("status") == "error"]
        summary["errors_count"] = len(summary["errors"])
        return summary

    finally:
        con.close()
