from __future__ import annotations

import calendar
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Iterable, Iterator, Optional, Sequence

import duckdb

from kucoin_lake.paths import dataset_glob, iter_dates, month_starts, parse_lake_partitions


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
    symbols: Optional[Sequence[str]] = None,
    date_start: Optional[date] = None,
    date_end: Optional[date] = None,
) -> Iterator[FileInfo]:
    """
    Iterate Parquet data files under the NAS lake and return FileInfo objects.

    This is designed to be FAST for derived timeframes:
      - If timeframe_filter is provided, datasets that use timeframe partitions are
        scoped to timeframe={timeframe_filter} rather than scanning all timeframes.

    Parameters
    ----------
    nas_root:
        Root of the data lake (e.g. /path/to/lake)
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
    symbols:
        Optional allowlist of symbols to scan (matched to symbol= partitions).
    date_start / date_end:
        Optional UTC date window to constrain partition scans. For 1m datasets this
        filters date=YYYY-MM-DD; for derived timeframes this filters month=YYYY-MM.

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

    if date_start and date_end and date_start > date_end:
        raise ValueError("date_start must be <= date_end")

    symbol_list = list(symbols) if symbols else None
    symbol_allow = set(symbol_list) if symbol_list else None

    def _month_overlaps_window(month_str: str) -> bool:
        month_start = date.fromisoformat(f"{month_str}-01")
        last_day = calendar.monthrange(month_start.year, month_start.month)[1]
        month_end = date(month_start.year, month_start.month, last_day)
        if date_start and month_end < date_start:
            return False
        if date_end and month_start > date_end:
            return False
        return True

    for ds in datasets_list:

        # Datasets without timeframe partitions (e.g. funding)
        if ds not in datasets_with_timeframe:
            # Only include these in 1m builds
            if timeframe_filter != "1m":
                continue
            tf = "*"
            partition_key = "date"
        else:
            # Timeframed datasets: scope to the requested timeframe
            tf = timeframe_filter if timeframe_filter else "*"
            if tf == "*" or tf is None:
                partition_key = "mixed"
            else:
                partition_key = "date" if tf == "1m" else "month"

        base = nas_root / market / ds
        if ds in datasets_with_timeframe:
            base = base / f"timeframe={tf}"

        symbol_dirs = [base / "symbol=*"]
        if symbol_list:
            symbol_dirs = [base / f"symbol={sym}" for sym in symbol_list]

        if partition_key == "date":
            if date_start and date_end:
                partitions = [f"date={d.isoformat()}" for d in iter_dates(date_start, date_end)]
            else:
                partitions = ["date=*"]
        elif partition_key == "month":
            if date_start and date_end:
                partitions = [f"month={d.strftime('%Y-%m')}" for d in month_starts(date_start, date_end)]
            else:
                partitions = ["month=*"]
        else:
            partitions = ["**"]

        glob_pats = []
        for sym_dir in symbol_dirs:
            for part in partitions:
                glob_pats.append((sym_dir / part / "data.parquet").as_posix())

        if not glob_pats:
            glob_pat = dataset_glob(nas_root, market, ds, timeframe=tf)
            glob_pats = [glob_pat]

        # Use pathlib globbing (fast enough, avoids DuckDB).
        # NOTE: this will expand wildcard patterns; it's a filesystem walk.
        seen: set[str] = set()
        for glob_pat in glob_pats:
            rel_glob = Path(glob_pat).relative_to(nas_root).as_posix()
            for p in nas_root.glob(rel_glob):
                if not p.is_file():
                    continue
                if p.name != "data.parquet":
                    continue

                file_rel = p.relative_to(nas_root).as_posix()
                if file_rel in seen:
                    continue
                seen.add(file_rel)

                if symbol_allow or date_start or date_end:
                    parts = parse_lake_partitions(file_rel)
                    if symbol_allow and parts.get("symbol") not in symbol_allow:
                        continue
                    if date_start or date_end:
                        if "date" in parts:
                            try:
                                file_date = date.fromisoformat(parts["date"])
                            except ValueError:
                                continue
                            if (date_start and file_date < date_start) or (date_end and file_date > date_end):
                                continue
                        elif "month" in parts:
                            if not _month_overlaps_window(parts["month"]):
                                continue
                        else:
                            continue

                try:
                    st = p.stat()
                except FileNotFoundError:
                    # File disappeared between glob and stat; ignore.
                    continue

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
