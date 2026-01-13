from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Iterator, Optional, Sequence

import duckdb

from kucoin_lake.paths import dataset_glob


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


@dataclass(frozen=True)
class FileInfo:
    market: str
    dataset: str
    file_path: str  # absolute
    file_rel: str  # relative to nas_root
    file_size_bytes: int
    mtime_ns: int


def iter_data_parquets(
    nas_root: str | Path,
    *,
    market: str,
    datasets: Iterable[str],
    timeframe_filter: Optional[str] = None,
    datasets_with_timeframe: Optional[set[str]] = None,
) -> Iterator[FileInfo]:
    """
    Iterate Parquet data files under the NAS lake and return FileInfo objects.

    This is designed to be FAST for derived timeframes:
      - If timeframe_filter is provided, datasets that use timeframe partitions are
        scoped to timeframe={timeframe_filter} rather than scanning all timeframes.

    Parameters
    ----------
    nas_root:
        Root of the data lake (e.g. /Volumes/quant_data/kucoin)
    market:
        'futures' (now) and future-proofed for 'spot'
    datasets:
        e.g. ('klines', 'mark', 'index', 'funding')
    timeframe_filter:
        If provided, only scan this timeframe for datasets that have timeframe partitions
        (e.g. '1m', '1d', '1h'). Funding is unaffected (no timeframe dir).
    datasets_with_timeframe:
        Provide your existing FUTURES_DATASETS_WITH_TIMEFRAME as a set here, or leave None
        to use a conservative default for your current futures setup.

    Yields
    ------
    FileInfo
    """
    nas_root = Path(nas_root)

    # Prefer wiring your existing constant in by passing datasets_with_timeframe=...
    # Default here is your current futures setup.
    if datasets_with_timeframe is None:
        datasets_with_timeframe = {"klines", "mark", "index"}

    # Normalize for safety
    datasets_list: Sequence[str] = list(datasets)

    for ds in datasets_list:

        # Datasets without timeframe partitions (e.g. funding)
        if ds not in datasets_with_timeframe:
            # Only include these in 1m builds
            if timeframe_filter != "1m":
                continue
            tf = "*"
        else:
            # Timeframed datasets: scope to the requested timeframe
            tf = timeframe_filter if timeframe_filter else "*"

        glob_pat = dataset_glob(nas_root, market, ds, timeframe=tf)

        # Use pathlib globbing (fast enough, avoids DuckDB).
        # NOTE: this will expand wildcard patterns; it's a filesystem walk.
        for p in nas_root.glob(Path(glob_pat).relative_to(nas_root).as_posix()):
            if not p.is_file():
                continue
            if p.name != "data.parquet":
                continue

            try:
                st = p.stat()
            except FileNotFoundError:
                # File disappeared between glob and stat; ignore.
                continue

            file_rel = p.relative_to(nas_root).as_posix()

            yield FileInfo(
                file_rel=file_rel,
                market=market,
                dataset=ds,
                file_path=p.as_posix(),
                file_size_bytes=int(st.st_size),
                mtime_ns=int(st.st_mtime_ns),
            )


def upsert_manifest(con: duckdb.DuckDBPyConnection, files: Sequence[FileInfo]) -> None:
    if not files:
        return
    now = utc_now_iso()

    con.execute("CREATE TEMP TABLE _stg_manifest AS SELECT * FROM md.md_file_manifest WHERE 1=0;")
    con.executemany(
        """
        INSERT INTO _stg_manifest (
            file_rel, market, dataset, file_path, file_size_bytes, mtime_ns, first_seen_utc, last_seen_utc
        )
        VALUES (?, ?, ?, ?, ?, ?, CAST(? AS TIMESTAMP), CAST(? AS TIMESTAMP));
        """,
        [(f.file_rel, f.market, f.dataset, f.file_path, f.file_size_bytes, f.mtime_ns, now, now) for f in files],
    )

    con.execute(
        """
        MERGE INTO md.md_file_manifest t
        USING _stg_manifest s
        ON t.file_rel = s.file_rel
        WHEN MATCHED THEN UPDATE SET
            market = s.market,
            dataset = s.dataset,
            file_path = s.file_path,
            file_size_bytes = s.file_size_bytes,
            mtime_ns = s.mtime_ns,
            last_seen_utc = s.last_seen_utc
        WHEN NOT MATCHED THEN INSERT (
            file_rel, market, dataset, file_path, file_size_bytes, mtime_ns, first_seen_utc, last_seen_utc
        ) VALUES (
            s.file_rel, s.market, s.dataset, s.file_path, s.file_size_bytes, s.mtime_ns, s.first_seen_utc, s.last_seen_utc
        );
        """
    )
    con.execute("DROP TABLE _stg_manifest;")


def get_new_or_changed_files(con: duckdb.DuckDBPyConnection, files: Sequence[FileInfo]) -> list[FileInfo]:
    if not files:
        return []

    con.execute("CREATE TEMP TABLE _stg_scan(file_rel VARCHAR, file_size_bytes UBIGINT, mtime_ns UBIGINT);")
    con.executemany(
        "INSERT INTO _stg_scan VALUES (?, ?, ?);",
        [(f.file_rel, f.file_size_bytes, f.mtime_ns) for f in files],
    )

    changed = con.execute(
        """
        SELECT s.file_rel
        FROM _stg_scan s
        LEFT JOIN md.md_file_manifest m ON m.file_rel = s.file_rel
        WHERE m.file_rel IS NULL
           OR m.file_size_bytes <> s.file_size_bytes
           OR m.mtime_ns <> s.mtime_ns
        """
    ).fetchall()
    con.execute("DROP TABLE _stg_scan;")

    want = {r[0] for r in changed}
    return [f for f in files if f.file_rel in want]
