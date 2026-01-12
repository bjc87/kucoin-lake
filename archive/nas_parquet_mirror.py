#!/usr/bin/env python3
from __future__ import annotations

import json
import time
import zipfile
import tempfile
from datetime import datetime, timezone
from datetime import date
from typing import Iterable
from pathlib import Path
import shutil

import duckdb

import time
from dataclasses import dataclass

try:
    from tqdm.auto import tqdm
except Exception:  # tqdm not installed
    tqdm = None

# -----------------------------
# CONFIG
# -----------------------------
LOCAL_ROOT = Path("/Users/benchurch/coding/data/kucoin/data")  # contains futures/...
NAS_ROOT = Path("/Volumes/quant_data/kucoin")
LOCAL_STAGE_ROOT = Path("/Users/benchurch/coding/data/kucoin/data/_stage_parquet")
LOCAL_STAGE_ROOT.mkdir(parents=True, exist_ok=True)

KLINES_ROOT  = LOCAL_ROOT / "futures/daily/klines"
FUNDING_ROOT = LOCAL_ROOT / "futures/daily/fundingRates"
MARK_ROOT  = LOCAL_ROOT / "futures/daily/mark"
INDEX_ROOT = LOCAL_ROOT / "futures/daily/index"

# Skip zips likely still downloading/writing
IN_FLIGHT_SUFFIXES = {".part", ".tmp", ".download"}
IN_FLIGHT_GRACE_SECONDS = 10 * 60  # skip if modified within last 10 minutes

# Progress logging
PRINT_EVERY = 200

# Errors log on NAS (or local, if you prefer)
ERRORS_LOG = NAS_ROOT / "_logs" / "nas_parquet_mirror_errors.jsonl"

# -----------------------------
# Run Counting Helpers
# -----------------------------
@dataclass
class RunCounters:
    total: int = 0
    ok_or_done: int = 0
    failed: int = 0
    in_flight: int = 0
    bad_zip: int = 0
    elapsed_s: float = 0.0


def format_summary(name: str, c: RunCounters) -> str:
    mins = c.elapsed_s / 60.0
    return (
        f"{name}: total={c.total}, ok_or_done={c.ok_or_done}, "
        f"failed={c.failed}, inflight={c.in_flight}, badzip={c.bad_zip}, "
        f"elapsed={mins:.1f} min"
    )


# -----------------------------
# Helpers
# -----------------------------

def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")

def log_error(record: dict) -> None:
    ERRORS_LOG.parent.mkdir(parents=True, exist_ok=True)
    record = {**record, "when_utc": utc_now_iso()}
    with open(ERRORS_LOG, "a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")

def is_in_flight(zip_path: Path) -> bool:
    # temp suffix check
    if zip_path.suffix.lower() in IN_FLIGHT_SUFFIXES:
        return True

    # recently modified check
    try:
        age = time.time() - zip_path.stat().st_mtime
        return age < IN_FLIGHT_GRACE_SECONDS
    except FileNotFoundError:
        return True

def zip_is_valid(zip_path: Path) -> tuple[bool, str | None]:
    """
    Returns (valid, reason). If invalid, reason explains why.
    """
    try:
        with zipfile.ZipFile(zip_path) as zf:
            bad = zf.testzip()
            if bad is not None:
                return False, f"zip integrity fail (bad member): {bad}"
        return True, None
    except zipfile.BadZipFile as e:
        return False, f"BadZipFile: {e}"
    except Exception as e:
        return False, f"zip validate error: {type(e).__name__}: {e}"

def extract_single_csv(zip_path: Path, tmp_dir: Path) -> Path:
    with zipfile.ZipFile(zip_path) as zf:
        zf.extractall(tmp_dir)
    csvs = list(tmp_dir.rglob("*.csv"))
    if len(csvs) != 1:
        raise RuntimeError(f"Expected 1 CSV in {zip_path}, found {len(csvs)}: {csvs}")
    return csvs[0]

# def expected_out_path(zip_path: Path) -> Path:
#     """
#     Mirrors LOCAL_ROOT structure under NAS_ROOT, changing .zip -> .parquet
#     """
#     rel = zip_path.relative_to(LOCAL_ROOT)
#     return (NAS_ROOT / rel).with_suffix(".parquet")

def out_path_klines(asset: str, timeframe: str, date_str: str) -> Path:
    # /.../futures/klines/timeframe=1m/symbol=ASSET/date=YYYY-MM-DD/data.parquet
    return (
        NAS_ROOT
        / "futures" / "klines"
        / f"timeframe={timeframe}"
        / f"symbol={asset}"
        / f"date={date_str}"
        / "data.parquet"
    )

def out_path_funding(asset: str, date_str: str) -> Path:
    # /.../futures/funding/symbol=ASSET/date=YYYY-MM-DD/data.parquet
    return (
        NAS_ROOT
        / "futures" / "funding"
        / f"symbol={asset}"
        / f"date={date_str}"
        / "data.parquet"
    )

def out_path_mark(asset: str, timeframe: str, date_str: str) -> Path:
    # /.../futures/mark/timeframe=1m/symbol=ASSET/date=YYYY-MM-DD/data.parquet
    return (
        NAS_ROOT
        / "futures" / "mark"
        / f"timeframe={timeframe}"
        / f"symbol={asset}"
        / f"date={date_str}"
        / "data.parquet"
    )

def out_path_index(asset: str, timeframe: str, date_str: str) -> Path:
    # /.../futures/index/timeframe=1m/symbol=ASSET/date=YYYY-MM-DD/data.parquet
    return (
        NAS_ROOT
        / "futures" / "index"
        / f"timeframe={timeframe}"
        / f"symbol={asset}"
        / f"date={date_str}"
        / "data.parquet"
    )

def parsedate_from_stem(zip_path: Path) -> str:
    # ...-YYYY-MM-DD.zip -> YYYY-MM-DD
    parts = zip_path.stem.split("-")
    if len(parts) < 3:
        raise RuntimeError(f"Cannot parse date from stem: {zip_path.stem}")
    return "-".join(parts[-3:])

def build_done_set(
    datasets: set[str] | None = None,
    nas_root: Path = NAS_ROOT,
) -> set[str]:
    """
    Scan NAS for existing parquet to avoid per-file stat() calls.
    If datasets is provided, only scans those dataset subtrees.
    """
    done: set[str] = set()
    if not nas_root.exists():
        return done

    # If datasets omitted, fall back to full scan (old behavior)
    roots: list[Path]
    if not datasets:
        roots = [nas_root]
    else:
        roots = [nas_root / "futures" / ds for ds in sorted(datasets)]

    for root in roots:
        if not root.exists():
            continue
        for p in root.rglob("*.parquet"):
            try:
                done.add(p.relative_to(nas_root).as_posix())
            except Exception:
                done.add(p.as_posix())
    return done


def atomic_copy_to_parquet(
    con: duckdb.DuckDBPyConnection,
    select_sql: str,
    out_path_nas: Path,
    *,
    nas_root: Path = NAS_ROOT,
    local_stage_root: Path = LOCAL_STAGE_ROOT,
    compression: str = "ZSTD",  # "ZSTD" or "SNAPPY"
) -> None:
    """
    Write Parquet to local stage (locks supported), then copy to NAS and atomic rename.
    Compatible with NAS filesystems that don't support file locks.
    """
    out_path_nas.parent.mkdir(parents=True, exist_ok=True)

    # Mirror NAS-relative path under local stage
    rel = out_path_nas.relative_to(nas_root)
    out_path_local = (local_stage_root / rel)
    out_path_local.parent.mkdir(parents=True, exist_ok=True)

    tmp_local = out_path_local.with_suffix(out_path_local.suffix + ".tmp")
    if tmp_local.exists():
        tmp_local.unlink()

    # 1) write locally (locks OK)
    con.execute(
        f"COPY ({select_sql}) TO '{tmp_local.as_posix()}' "
        f"(FORMAT PARQUET, COMPRESSION {compression});"
    )
    tmp_local.replace(out_path_local)

    # 2) copy to NAS + rename
    tmp_nas = out_path_nas.with_suffix(out_path_nas.suffix + ".tmpcopy")
    if tmp_nas.exists():
        tmp_nas.unlink()

    shutil.copy2(out_path_local, tmp_nas)
    tmp_nas.replace(out_path_nas)

    # optional cleanup
    out_path_local.unlink()

def read_header_fields(csv_path: Path, delim: str) -> list[str]:
    raw = csv_path.read_bytes()[:2048]
    # strip UTF-8 BOM if present
    if raw.startswith(b"\xef\xbb\xbf"):
        raw = raw[3:]
    first_line = raw.splitlines()[0].decode("utf-8", errors="replace")
    return [c.strip() for c in first_line.split(delim)]

def detect_delim(csv_path: Path) -> str:
    raw = csv_path.read_bytes()[:2048]
    if raw.startswith(b"\xef\xbb\xbf"):
        raw = raw[3:]
    first = raw.splitlines()[0]
    # pick best delimiter by count in header line
    candidates = [b",", b"\t", b";", b"|"]
    counts = {c: first.count(c) for c in candidates}
    best = max(counts, key=counts.get)
    return best.decode("utf-8") if counts[best] > 0 else ","


# -----------------------------
# Sharding
# -----------------------------

def _month_starts(start: date, end: date):
    y, m = start.year, start.month
    cur = date(y, m, 1)
    while cur <= end:
        yield cur
        if m == 12:
            y, m = y + 1, 1
        else:
            m += 1
        cur = date(y, m, 1)

def list_local_ohlc_zips_sharded(
    root: Path,
    dataset: str,          # "klines" | "mark" | "index"
    start: date,
    end: date,
    assets: list[str] | None = None,
    timeframes: list[str] | None = None,
) -> list[Path]:
    """
    Local layout:
      root/dataset/SYMBOL/TIMEFRAME/SYMBOL-TIMEFRAME-YYYY-MM-DD.zip
    Uses month globbing to avoid rglob over everything.
    """
    ds_root = root / dataset
    if not ds_root.exists():
        return []

    # Choose symbols
    if assets:
        symbols = assets
    else:
        symbols = [p.name for p in ds_root.iterdir() if p.is_dir()]

    # Choose timeframes
    tfs = timeframes or ["1m"]

    out: list[Path] = []
    for sym in symbols:
        for tf in tfs:
            folder = ds_root / sym / tf
            if not folder.exists():
                continue
            for m in _month_starts(start, end):
                ym = f"{m.year:04d}-{m.month:02d}"
                pat = f"{sym}-{tf}-{ym}-*.zip"
                out.extend(folder.glob(pat))

    # final day filter (glob is month-level)
    filtered = []
    for p in out:
        # filename ends with YYYY-MM-DD.zip
        d = date.fromisoformat(p.name[-14:-4])
        if start <= d <= end:
            filtered.append(p)

    filtered.sort()
    return filtered

def list_local_funding_zips_sharded(
    root: Path,
    start: date,
    end: date,
    assets: list[str] | None = None,
) -> list[Path]:
    """
    Local layout:
      root/fundingRates/SYMBOL/SYMBOL-fundingRates-YYYY-MM-DD.zip
    """
    ds_root = root / "fundingRates"
    if not ds_root.exists():
        return []

    if assets:
        symbols = assets
    else:
        symbols = [p.name for p in ds_root.iterdir() if p.is_dir()]

    out: list[Path] = []
    for sym in symbols:
        folder = ds_root / sym
        if not folder.exists():
            continue
        for m in _month_starts(start, end):
            ym = f"{m.year:04d}-{m.month:02d}"
            pat = f"{sym}-fundingRates-{ym}-*.zip"
            out.extend(folder.glob(pat))

    filtered = []
    for p in out:
        d = date.fromisoformat(p.name[-14:-4])
        if start <= d <= end:
            filtered.append(p)

    filtered.sort()
    return filtered

# -----------------------------
# Converters
# -----------------------------



# -----------------------------
# Converters
# -----------------------------
# def convert_klines_zip(con: duckdb.DuckDBPyConnection, zip_path: Path, done: set[str]) -> bool:
#     asset = zip_path.parent.parent.name
#     timeframe = zip_path.parent.name
#     date_str = parsedate_from_stem(zip_path)

#     out_path = out_path_klines(asset, timeframe, date_str)
#     out_rel = out_path.relative_to(NAS_ROOT).as_posix()
#     if out_rel in done:
#         return True

#     if is_in_flight(zip_path):
#         return False  # skipped

#     ok, reason = zip_is_valid(zip_path)
#     if not ok:
#         log_error({"dataset": "klines", "zip": zip_path.as_posix(), "error": reason})
#         return False

#     asset = zip_path.parent.parent.name   # HIGHUSDTM
#     timeframe = zip_path.parent.name      # 1m
#     date_str = parsedate_from_stem(zip_path)

#     try:
#         with tempfile.TemporaryDirectory() as td:
#             td = Path(td)
#             csv_path = extract_single_csv(zip_path, td)

#             # KuCoin CSVs are tab-delimited; time is scientific notation ms epoch (e.g. 1.70914E+12)
#             select_sql = f"""
#                 SELECT
#                     CAST(CAST("time" AS DOUBLE) AS BIGINT) AS time_ms,
#                     to_timestamp(CAST(CAST("time" AS DOUBLE) AS BIGINT) / 1000.0) AS ts,

#                     CAST("open"   AS DOUBLE) AS open,
#                     CAST("high"   AS DOUBLE) AS high,
#                     CAST("low"    AS DOUBLE) AS low,
#                     CAST("close"  AS DOUBLE) AS close,
#                     CAST("volume" AS DOUBLE) AS volume
#                 FROM read_csv(
#                     '{csv_path.as_posix()}',
#                     delim=',',
#                     header=true,
#                     auto_detect=false,
#                     strict_mode=false,
#                     null_padding=true,
#                     quote='',
#                     escape='',
#                     columns={{
#                         'time':'DOUBLE',
#                         'open':'DOUBLE',
#                         'high':'DOUBLE',
#                         'low':'DOUBLE',
#                         'close':'DOUBLE',
#                         'volume':'DOUBLE'
#                     }}
#                 )
#             """

#             atomic_copy_to_parquet(con, select_sql, out_path)

#         done.add(out_rel)
#         return True

#     except Exception as e:
#         log_error({
#             "dataset": "klines",
#             "zip": zip_path.as_posix(),
#             "out": out_path.as_posix(),
#             "error": f"{type(e).__name__}: {e}",
#         })
#         # Clean up potentially partial output if present (rare due to atomic tmp, but safe)
#         if out_path.exists():
#             try:
#                 out_path.unlink()
#             except Exception:
#                 pass
#         return False

def convert_klines_zip(
    con: duckdb.DuckDBPyConnection,
    zip_path: Path,
    done: set[str],
) -> str:
    """
    Convert one KuCoin klines zip -> parquet.

    Returns one of:
      - "done": parquet already exists (skipped)
      - "skip": input zip not ready / in-flight (skipped)
      - "ok": converted successfully
      - "fail": validation or conversion failed (logged)
    """
    asset = zip_path.parent.parent.name     # e.g. HIGHUSDTM
    timeframe = zip_path.parent.name        # e.g. 1m
    date_str = parsedate_from_stem(zip_path)

    out_path = out_path_klines(asset, timeframe, date_str)
    out_rel = out_path.relative_to(NAS_ROOT).as_posix()

    # Already written
    if out_rel in done:
        return "done"

    # Zip still downloading / partial
    if is_in_flight(zip_path):
        return "skip"

    ok, reason = zip_is_valid(zip_path)
    if not ok:
        log_error({"dataset": "klines", "zip": zip_path.as_posix(), "error": reason})
        return "fail"

    try:
        with tempfile.TemporaryDirectory() as td:
            td_path = Path(td)
            csv_path = extract_single_csv(zip_path, td_path)

            select_sql = f"""
                SELECT
                    CAST(CAST("time" AS DOUBLE) AS BIGINT) AS time_ms,
                    to_timestamp(CAST(CAST("time" AS DOUBLE) AS BIGINT) / 1000.0) AS ts,

                    CAST("open"   AS DOUBLE) AS open,
                    CAST("high"   AS DOUBLE) AS high,
                    CAST("low"    AS DOUBLE) AS low,
                    CAST("close"  AS DOUBLE) AS close,
                    CAST("volume" AS DOUBLE) AS volume
                FROM read_csv(
                    '{csv_path.as_posix()}',
                    delim=',',
                    header=true,
                    auto_detect=false,
                    strict_mode=false,
                    null_padding=true,
                    quote='',
                    escape='',
                    columns={{
                        'time':'DOUBLE',
                        'open':'DOUBLE',
                        'high':'DOUBLE',
                        'low':'DOUBLE',
                        'close':'DOUBLE',
                        'volume':'DOUBLE'
                    }}
                )
            """

            atomic_copy_to_parquet(con, select_sql, out_path)

        done.add(out_rel)
        return "ok"

    except Exception as e:
        log_error(
            {
                "dataset": "klines",
                "zip": zip_path.as_posix(),
                "error": f"{type(e).__name__}: {e}",
            }
        )
        return "fail"

# def convert_funding_zip(con: duckdb.DuckDBPyConnection, zip_path: Path, done: set[str]) -> bool:
#     asset = zip_path.parent.name
#     date_str = parsedate_from_stem(zip_path)

#     out_path = out_path_funding(asset, date_str)
#     out_rel = out_path.relative_to(NAS_ROOT).as_posix()
#     if out_rel in done:
#         return True

#     if is_in_flight(zip_path):
#         return False  # skipped

#     ok, reason = zip_is_valid(zip_path)
#     if not ok:
#         log_error({"dataset": "fundingRates", "zip": zip_path.as_posix(), "error": reason})
#         return False

#     asset = zip_path.parent.name  # HIGHUSDTM
#     date_str = parsedate_from_stem(zip_path)

#     try:
#         with tempfile.TemporaryDirectory() as td:
#             td = Path(td)
#             csv_path = extract_single_csv(zip_path, td)

#             select_sql = f"""
#                 SELECT
#                     CAST(symbol AS VARCHAR) AS symbol,
#                     CAST(CAST("time" AS DOUBLE) AS BIGINT) AS time_ms,
#                     to_timestamp(CAST(CAST("time" AS DOUBLE) AS BIGINT) / 1000.0) AS ts,
#                     CAST(fundingRate AS DOUBLE) AS funding_rate
#                 FROM read_csv(
#                     '{csv_path.as_posix()}',
#                     delim=',',
#                     header=true,
#                     auto_detect=false,
#                     strict_mode=false,
#                     null_padding=true,
#                     quote='',
#                     escape='',
#                     columns={{
#                         'symbol':'VARCHAR',
#                         'time':'DOUBLE',
#                         'fundingRate':'DOUBLE'
#                     }}
#                 )
#             """

#             atomic_copy_to_parquet(con, select_sql, out_path)

#         done.add(out_rel)
#         return True

#     except Exception as e:
#         log_error({
#             "dataset": "fundingRates",
#             "zip": zip_path.as_posix(),
#             "out": out_path.as_posix(),
#             "error": f"{type(e).__name__}: {e}",
#         })
#         if out_path.exists():
#             try:
#                 out_path.unlink()
#             except Exception:
#                 pass
#         return False
    
def _price_column_from_header(fields: list[str], preferred: str | None = None) -> str:
    """
    Choose the right price column name from a CSV header.
    Common candidates: price, markPrice, indexPrice.
    """
    fields_lower = {f.lower(): f for f in fields}

    # If preferred (e.g. "markprice") try that first
    if preferred:
        pref = preferred.lower()
        if pref in fields_lower:
            return fields_lower[pref]

    # Common defaults
    for cand in ["price", "markprice", "indexprice", "value", "close"]:
        if cand in fields_lower:
            return fields_lower[cand]

    raise RuntimeError(f"Could not find a price column in header fields: {fields}")


# def convert_mark_zip(con: duckdb.DuckDBPyConnection, zip_path: Path, done: set[str]) -> bool:
#     # .../mark/{asset}/{timeframe}/{asset}-{timeframe}-{YYYY-MM-DD}.zip
#     if is_in_flight(zip_path):
#         return False

#     ok, reason = zip_is_valid(zip_path)
#     if not ok:
#         log_error({"dataset": "mark", "zip": zip_path.as_posix(), "error": reason})
#         return False

#     asset = zip_path.parent.parent.name
#     timeframe = zip_path.parent.name
#     date_str = parsedate_from_stem(zip_path)

#     out_path = out_path_mark(asset, timeframe, date_str)
#     out_rel = out_path.relative_to(NAS_ROOT).as_posix()
#     if out_rel in done:
#         return True

#     try:
#         with tempfile.TemporaryDirectory() as td:
#             td = Path(td)
#             csv_path = extract_single_csv(zip_path, td)

#             select_sql = f"""
#                 SELECT
#                     CAST(CAST("time" AS DOUBLE) AS BIGINT) AS time_ms,
#                     to_timestamp(CAST(CAST("time" AS DOUBLE) AS BIGINT) / 1000.0) AS ts,
#                     CAST("open"  AS DOUBLE) AS open,
#                     CAST("high"  AS DOUBLE) AS high,
#                     CAST("low"   AS DOUBLE) AS low,
#                     CAST("close" AS DOUBLE) AS close
#                 FROM read_csv(
#                     '{csv_path.as_posix()}',
#                     delim=',',
#                     header=true,
#                     auto_detect=false,
#                     strict_mode=false,
#                     null_padding=true,
#                     quote='',
#                     escape='',
#                     columns={{
#                         'time':'DOUBLE',
#                         'open':'DOUBLE',
#                         'high':'DOUBLE',
#                         'low':'DOUBLE',
#                         'close':'DOUBLE'
#                     }}
#                 )
#             """

#             atomic_copy_to_parquet(con, select_sql, out_path)

#         done.add(out_rel)
#         return True

#     except Exception as e:
#         log_error({
#             "dataset": "mark",
#             "zip": zip_path.as_posix(),
#             "out": out_path.as_posix(),
#             "error": f"{type(e).__name__}: {e}",
#         })
#         if out_path.exists():
#             try:
#                 out_path.unlink()
#             except Exception:
#                 pass
#         return False


# def convert_index_zip(con: duckdb.DuckDBPyConnection, zip_path: Path, done: set[str]) -> bool:
#     # .../index/{asset}/{timeframe}/{asset}-{timeframe}-{YYYY-MM-DD}.zip
#     if is_in_flight(zip_path):
#         return False

#     ok, reason = zip_is_valid(zip_path)
#     if not ok:
#         log_error({"dataset": "index", "zip": zip_path.as_posix(), "error": reason})
#         return False

#     asset = zip_path.parent.parent.name
#     timeframe = zip_path.parent.name
#     date_str = parsedate_from_stem(zip_path)

#     out_path = out_path_index(asset, timeframe, date_str)
#     out_rel = out_path.relative_to(NAS_ROOT).as_posix()
#     if out_rel in done:
#         return True

#     try:
#         with tempfile.TemporaryDirectory() as td:
#             td = Path(td)
#             csv_path = extract_single_csv(zip_path, td)

#             select_sql = f"""
#                 SELECT
#                     CAST(CAST("time" AS DOUBLE) AS BIGINT) AS time_ms,
#                     to_timestamp(CAST(CAST("time" AS DOUBLE) AS BIGINT) / 1000.0) AS ts,
#                     CAST("open"  AS DOUBLE) AS open,
#                     CAST("high"  AS DOUBLE) AS high,
#                     CAST("low"   AS DOUBLE) AS low,
#                     CAST("close" AS DOUBLE) AS close
#                 FROM read_csv(
#                     '{csv_path.as_posix()}',
#                     delim=',',
#                     header=true,
#                     auto_detect=false,
#                     strict_mode=false,
#                     null_padding=true,
#                     quote='',
#                     escape='',
#                     columns={{
#                         'time':'DOUBLE',
#                         'open':'DOUBLE',
#                         'high':'DOUBLE',
#                         'low':'DOUBLE',
#                         'close':'DOUBLE'
#                     }}
#                 )
#             """

#             atomic_copy_to_parquet(con, select_sql, out_path)

#         done.add(out_rel)
#         return True

#     except Exception as e:
#         log_error({
#             "dataset": "index",
#             "zip": zip_path.as_posix(),
#             "out": out_path.as_posix(),
#             "error": f"{type(e).__name__}: {e}",
#         })
#         if out_path.exists():
#             try:
#                 out_path.unlink()
#             except Exception:
#                 pass
#         return False

def convert_funding_zip(
    con: duckdb.DuckDBPyConnection,
    zip_path: Path,
    done: set[str],
) -> str:
    """
    Convert one KuCoin fundingRates zip -> parquet.

    Returns one of:
      - "done": parquet already exists (skipped)
      - "skip": input zip not ready / in-flight (skipped)
      - "ok": converted successfully
      - "fail": validation or conversion failed (logged)
    """
    asset = zip_path.parent.name
    date_str = parsedate_from_stem(zip_path)

    out_path = out_path_funding(asset, date_str)
    out_rel = out_path.relative_to(NAS_ROOT).as_posix()

    if out_rel in done:
        return "done"

    if is_in_flight(zip_path):
        return "skip"

    ok, reason = zip_is_valid(zip_path)
    if not ok:
        log_error({"dataset": "fundingRates", "zip": zip_path.as_posix(), "error": reason})
        return "fail"

    try:
        with tempfile.TemporaryDirectory() as td:
            td_path = Path(td)
            csv_path = extract_single_csv(zip_path, td_path)

            select_sql = f"""
                SELECT
                    CAST(symbol AS VARCHAR) AS symbol,
                    CAST(CAST("time" AS DOUBLE) AS BIGINT) AS time_ms,
                    to_timestamp(CAST(CAST("time" AS DOUBLE) AS BIGINT) / 1000.0) AS ts,
                    CAST(fundingRate AS DOUBLE) AS funding_rate
                FROM read_csv(
                    '{csv_path.as_posix()}',
                    delim=',',
                    header=true,
                    auto_detect=false,
                    strict_mode=false,
                    null_padding=true,
                    quote='',
                    escape='',
                    columns={{
                        'symbol':'VARCHAR',
                        'time':'DOUBLE',
                        'fundingRate':'DOUBLE'
                    }}
                )
            """

            atomic_copy_to_parquet(con, select_sql, out_path)

        done.add(out_rel)
        return "ok"

    except Exception as e:
        log_error(
            {
                "dataset": "fundingRates",
                "zip": zip_path.as_posix(),
                "out": out_path.as_posix(),
                "error": f"{type(e).__name__}: {e}",
            }
        )
        return "fail"

def convert_mark_zip(
    con: duckdb.DuckDBPyConnection,
    zip_path: Path,
    done: set[str],
) -> str:
    """
    Convert one KuCoin mark zip -> parquet.

    Returns: "done" | "skip" | "ok" | "fail"
    """
    asset = zip_path.parent.parent.name
    timeframe = zip_path.parent.name
    date_str = parsedate_from_stem(zip_path)

    out_path = out_path_mark(asset, timeframe, date_str)
    out_rel = out_path.relative_to(NAS_ROOT).as_posix()

    if out_rel in done:
        return "done"

    if is_in_flight(zip_path):
        return "skip"

    ok, reason = zip_is_valid(zip_path)
    if not ok:
        log_error({"dataset": "mark", "zip": zip_path.as_posix(), "error": reason})
        return "fail"

    try:
        with tempfile.TemporaryDirectory() as td:
            td_path = Path(td)
            csv_path = extract_single_csv(zip_path, td_path)

            select_sql = f"""
                SELECT
                    CAST(CAST("time" AS DOUBLE) AS BIGINT) AS time_ms,
                    to_timestamp(CAST(CAST("time" AS DOUBLE) AS BIGINT) / 1000.0) AS ts,
                    CAST("open"  AS DOUBLE) AS open,
                    CAST("high"  AS DOUBLE) AS high,
                    CAST("low"   AS DOUBLE) AS low,
                    CAST("close" AS DOUBLE) AS close,
                FROM read_csv(
                    '{csv_path.as_posix()}',
                    delim=',',
                    header=true,
                    auto_detect=false,
                    strict_mode=false,
                    null_padding=true,
                    quote='',
                    escape='',
                    columns={{
                        'time':'DOUBLE',
                        'open':'DOUBLE',
                        'high':'DOUBLE',
                        'low':'DOUBLE',
                        'close':'DOUBLE'
                    }}
                )
            """

            atomic_copy_to_parquet(con, select_sql, out_path)

        done.add(out_rel)
        return "ok"

    except Exception as e:
        log_error(
            {
                "dataset": "mark",
                "zip": zip_path.as_posix(),
                "out": out_path.as_posix(),
                "error": f"{type(e).__name__}: {e}",
            }
        )
        return "fail"

def convert_index_zip(
    con: duckdb.DuckDBPyConnection,
    zip_path: Path,
    done: set[str],
) -> str:
    """
    Convert one KuCoin index zip -> parquet.

    Returns: "done" | "skip" | "ok" | "fail"
    """
    asset = zip_path.parent.parent.name
    timeframe = zip_path.parent.name
    date_str = parsedate_from_stem(zip_path)

    out_path = out_path_index(asset, timeframe, date_str)
    out_rel = out_path.relative_to(NAS_ROOT).as_posix()

    if out_rel in done:
        return "done"

    if is_in_flight(zip_path):
        return "skip"

    ok, reason = zip_is_valid(zip_path)
    if not ok:
        log_error({"dataset": "index", "zip": zip_path.as_posix(), "error": reason})
        return "fail"

    try:
        with tempfile.TemporaryDirectory() as td:
            td_path = Path(td)
            csv_path = extract_single_csv(zip_path, td_path)

            select_sql = f"""
                SELECT
                    CAST(CAST("time" AS DOUBLE) AS BIGINT) AS time_ms,
                    to_timestamp(CAST(CAST("time" AS DOUBLE) AS BIGINT) / 1000.0) AS ts,
                    CAST("open"  AS DOUBLE) AS open,
                    CAST("high"  AS DOUBLE) AS high,
                    CAST("low"   AS DOUBLE) AS low,
                    CAST("close" AS DOUBLE) AS close,
                FROM read_csv(
                    '{csv_path.as_posix()}',
                    delim=',',
                    header=true,
                    auto_detect=false,
                    strict_mode=false,
                    null_padding=true,
                    quote='',
                    escape='',
                    columns={{
                        'time':'DOUBLE',
                        'open':'DOUBLE',
                        'high':'DOUBLE',
                        'low':'DOUBLE',
                        'close':'DOUBLE'
                    }}
                )
            """

            atomic_copy_to_parquet(con, select_sql, out_path)

        done.add(out_rel)
        return "ok"

    except Exception as e:
        log_error(
            {
                "dataset": "index",
                "zip": zip_path.as_posix(),
                "out": out_path.as_posix(),
                "error": f"{type(e).__name__}: {e}",
            }
        )
        return "fail"


# -----------------------------
# Pilot
# -----------------------------

def list_klines_zips(local_root: Path = LOCAL_ROOT) -> list[Path]:
    return sorted((local_root / "futures/daily/klines").rglob("*.zip"))

def list_funding_zips(local_root: Path = LOCAL_ROOT) -> list[Path]:
    return sorted((local_root / "futures/daily/fundingRates").rglob("*.zip"))

def list_mark_zips(local_root: Path = LOCAL_ROOT) -> list[Path]:
    return sorted((local_root / "futures/daily/mark").rglob("*.zip"))

def list_index_zips(local_root: Path = LOCAL_ROOT) -> list[Path]:
    return sorted((local_root / "futures/daily/index").rglob("*.zip"))

def filter_klines(zips: list[Path], asset: str | None, timeframe: str | None, dates: set[str] | None) -> list[Path]:
    out = []
    for zp in zips:
        # Expect stem: {asset}-{timeframe}-{YYYY-MM-DD}
        stem = zp.stem
        date_str = "-".join(stem.split("-")[-3:])
        if dates and date_str not in dates:
            continue
        if asset and zp.parent.parent.name != asset:
            continue
        if timeframe and zp.parent.name != timeframe:
            continue
        out.append(zp)
    return out

def filter_funding(zips: list[Path], asset: str | None, dates: set[str] | None) -> list[Path]:
    out = []
    for zp in zips:
        stem = zp.stem
        date_str = "-".join(stem.split("-")[-3:])
        if dates and date_str not in dates:
            continue
        if asset and zp.parent.name != asset:
            continue
        out.append(zp)
    return out

def run_pilot(
    asset: str,
    timeframe: str,
    dates: set[str],
    local_root: Path = LOCAL_ROOT,
    nas_root: Path = NAS_ROOT,
    include_klines: bool = True,
    include_funding: bool = True,
) -> dict:
    """
    Runs a small, filtered conversion job intended for validation/pilots.
    Returns a stats dict you can print in Jupyter.
    """
    assert local_root.exists(), f"Missing local_root: {local_root}"
    assert nas_root.parent.exists(), f"NAS not mounted? Missing: {nas_root.parent}"

    # One-time done scan for fast resumability
    done = build_done_set()

    # Collect + filter targets
    klines_zips = list_klines_zips(local_root) if include_klines else []
    funding_zips = list_funding_zips(local_root) if include_funding else []

    if include_klines:
        klines_zips = filter_klines(klines_zips, asset=asset, timeframe=timeframe, dates=dates)
    if include_funding:
        funding_zips = filter_funding(funding_zips, asset=asset, dates=dates)

    stats = {
        "asset": asset,
        "timeframe": timeframe,
        "dates": sorted(dates),
        "klines_targets": len(klines_zips),
        "funding_targets": len(funding_zips),
        "klines_written_or_done": 0,
        "funding_written_or_done": 0,
    }

    con = duckdb.connect()

    try:
        for zp in klines_zips:
            if convert_klines_zip(con, zp, done):
                stats["klines_written_or_done"] += 1
        for zp in funding_zips:
            if convert_funding_zip(con, zp, done):
                stats["funding_written_or_done"] += 1
    finally:
        con.close()

    return stats

def run_with_progress(
    label: str,
    zips: list[Path],
    convert_fn,  # (con, zip_path, done) -> bool
    con: duckdb.DuckDBPyConnection,
    done: set[str],
) -> RunCounters:
    counters = RunCounters(total=len(zips))
    start = time.perf_counter()

    iterator = zips
    use_tqdm = tqdm is not None

    pbar = tqdm(iterator, desc=label, unit="zip") if use_tqdm else iterator

    for zp in pbar:
        t0 = time.perf_counter()

        # We want to classify "False" returns a bit better.
        # Your convert_* returns:
        #   True  = written or already done
        #   False = skipped in-flight OR failed
        # We can cheaply detect in-flight here (same logic you already have)
        if is_in_flight(zp):
            counters.in_flight += 1
            # don't log as failure; just skip
            ok = False
        else:
            # zip integrity check classification (optional but useful)
            ok_zip, reason = zip_is_valid(zp)
            if not ok_zip:
                counters.bad_zip += 1
                log_error({"dataset": label, "zip": zp.as_posix(), "error": reason})
                ok = False
            else:
                ok = convert_fn(con, zp, done)

        if ok:
            counters.ok_or_done += 1
        else:
            # If it wasn't in-flight and wasn't a bad zip, treat as a failure
            if not is_in_flight(zp) and ok_zip:
                counters.failed += 1

        # update elapsed + ETA
        dt = time.perf_counter() - t0
        counters.elapsed_s = time.perf_counter() - start

        if use_tqdm:
            processed = counters.ok_or_done + counters.failed + counters.in_flight + counters.bad_zip
            # throughput based on processed zips (excluding in-flight/badzip still count as processed)
            rate = processed / max(counters.elapsed_s, 1e-9)
            remaining = counters.total - processed
            eta_s = remaining / max(rate, 1e-9)

            pbar.set_postfix({
                "ok/done": counters.ok_or_done,
                "fail": counters.failed,
                "inflight": counters.in_flight,
                "badzip": counters.bad_zip,
                "rate_zps": f"{rate:.2f}",
                "eta_min": f"{eta_s/60:.1f}",
            })

    return counters


def run_full(local_root: Path = LOCAL_ROOT, include_klines: bool = True, include_funding: bool = True) -> dict:
    assert local_root.exists(), f"Missing local_root: {local_root}"
    assert NAS_ROOT.parent.exists(), f"NAS not mounted? Missing: {NAS_ROOT.parent}"

    done = build_done_set()

    klines_zips = list_klines_zips(local_root) if include_klines else []
    funding_zips = list_funding_zips(local_root) if include_funding else []

    con = duckdb.connect()
    try:
        kl = run_with_progress("klines", klines_zips, convert_klines_zip, con, done) if include_klines else RunCounters()
        fu = run_with_progress("funding", funding_zips, convert_funding_zip, con, done) if include_funding else RunCounters()
    finally:
        con.close()

    return {
        "klines": kl.__dict__,
        "funding": fu.__dict__,
        "errors_log": ERRORS_LOG.as_posix(),
    }

def date_str_from_zip_stem(stem: str) -> str:
    # ...-YYYY-MM-DD -> YYYY-MM-DD
    parts = stem.split("-")
    if len(parts) < 3:
        raise ValueError(f"Cannot parse date from stem: {stem}")
    return "-".join(parts[-3:])

def _indate_range(d: str, start: str | None, end: str | None) -> bool:
    dd = date.fromisoformat(d)
    if start is not None and dd < date.fromisoformat(start):
        return False
    if end is not None and dd > date.fromisoformat(end):
        return False
    return True

def _filter_klines_zips(
    zips: list[Path],
    assets: set[str] | None,
    timeframes: set[str] | None,
    startdate: str | None,
    enddate: str | None,
) -> list[Path]:
    out: list[Path] = []
    for zp in zips:
        # folder structure: .../klines/{asset}/{timeframe}/{asset}-{timeframe}-{YYYY-MM-DD}.zip
        asset = zp.parent.parent.name
        tf = zp.parent.name
        d = date_str_from_zip_stem(zp.stem)

        if assets and asset not in assets:
            continue
        if timeframes and tf not in timeframes:
            continue
        if not _indate_range(d, startdate, enddate):
            continue
        out.append(zp)
    return out

def _filter_funding_zips(
    zips: list[Path],
    assets: set[str] | None,
    startdate: str | None,
    enddate: str | None,
) -> list[Path]:
    out: list[Path] = []
    for zp in zips:
        # folder structure: .../fundingRates/{asset}/{asset}-fundingRates-{YYYY-MM-DD}.zip
        asset = zp.parent.name
        d = date_str_from_zip_stem(zp.stem)

        if assets and asset not in assets:
            continue
        if not _indate_range(d, startdate, enddate):
            continue
        out.append(zp)
    return out


# -----------------------------
# Resolve Dates
# -----------------------------

def resolvedates_ingest_strict(
    *,
    startdate: str | None,
    enddate: str | None,
) -> tuple[date, date]:
    """
    Strict resolver for ingestion: ALWAYS returns a bounded (start, end).

    Ingest should be explicit and repeatable, so we require BOTH bounds.
    """
    if not (startdate and enddate):
        raise ValueError("Ingest requires a bounded window: provide BOTH startdate and enddate (YYYY-MM-DD).")

    start = date.fromisoformat(startdate)
    end = date.fromisoformat(enddate)
    if start > end:
        raise ValueError("startdate must be <= enddate")
    return start, end


def build_zip_targets_ingest_strict(
    local_root: str | Path,
    *,
    startdate: str,
    enddate: str,
    assets: list[str] | None = None,
    timeframes: list[str] | None = None,
    include_klines: bool = True,
    include_funding: bool = True,
    include_mark: bool = True,
    include_index: bool = True,
):
    """
    Clean/strict ingestion target builder:
      - No unbounded runs.
      - Uses month-sharded local listing only.
      - No post-filtering required.

    Returns:
      (klines_zips, funding_zips, mark_zips, index_zips, start, end)
    """
    local_root = Path(local_root)
    start, end = resolvedates_ingest_strict(startdate=startdate, enddate=enddate)

    klines_zips = (
        list_local_ohlc_zips_sharded(local_root, "klines", start, end, assets, timeframes)
        if include_klines
        else []
    )
    mark_zips = (
        list_local_ohlc_zips_sharded(local_root, "mark", start, end, assets, timeframes)
        if include_mark
        else []
    )
    index_zips = (
        list_local_ohlc_zips_sharded(local_root, "index", start, end, assets, timeframes)
        if include_index
        else []
    )
    funding_zips = (
        list_local_funding_zips_sharded(local_root, start, end, assets)
        if include_funding
        else []
    )

    return klines_zips, funding_zips, mark_zips, index_zips, start, end

# def run_ingest(
#     *,
#     startdate: str | None = None,
#     enddate: str | None = None,
#     assets: Iterable[str] | None = None,
#     timeframes: Iterable[str] | None = None,   # applies to klines/mark/index
#     include_klines: bool = True,
#     include_funding: bool = True,
#     include_mark: bool = True,
#     include_index: bool = True,
#     local_root: Path = LOCAL_ROOT,
#     show_progress: bool = True,
# ) -> dict:
#     """
#     Jupyter-first ingest API.

#     - Filters by date range (inclusive), assets, and timeframes (klines only).
#     - Safe to rerun: skips anything already written (done-set) and uses atomic writes.
#     - Logs failures to ERRORS_LOG.

#     Dates must be ISO 'YYYY-MM-DD' if provided.
#     """

#     assert local_root.exists(), f"Missing local_root: {local_root}"
#     assert NAS_ROOT.parent.exists(), f"NAS not mounted? Missing: {NAS_ROOT.parent}"

#     assets_set = set(assets) if assets else None
#     tfs_set = set(timeframes) if timeframes else None

#     klines_zips, funding_zips, mark_zips, index_zips, start, end = build_zip_targets_ingest_strict(
#         local_root,
#         startdate=startdate,
#         enddate=enddate,
#         assets=assets,
#         timeframes=timeframes,
#         include_klines=include_klines,
#         include_funding=include_funding,
#         include_mark=include_mark,
#         include_index=include_index,
#     )

#     # Done set for fast resumability
#     datasets = set()
#     if include_klines: datasets.add("klines")
#     if include_funding: datasets.add("funding")
#     if include_mark: datasets.add("mark")
#     if include_index: datasets.add("index")

#     print('Building done set...')
#     done = build_done_set(datasets=datasets)
#     print('done set built')

#     # Progress bar (optional)
#     pbar = None
#     if show_progress:
#         try:
#             from tqdm.auto import tqdm as _tqdm
#             pbar = _tqdm
#         except Exception:
#             pbar = None

#     def _iter(label: str, items: list[Path]):
#         if pbar is None:
#             return items
#         return pbar(items, desc=label, unit="zip")

#     stats = {
#         "filters": {
#             "startdate": startdate,
#             "enddate": enddate,
#             "assets": sorted(assets_set) if assets_set else None,
#             "timeframes": sorted(tfs_set) if tfs_set else None,
#             "include_klines": include_klines,
#             "include_funding": include_funding,
#             "include_mark": include_mark,
#             "include_index": include_index,
#         },
#         "targets": {
#             "klines": len(klines_zips),
#             "funding": len(funding_zips),
#             "mark": len(mark_zips),
#             "index": len(index_zips),
#         },
#         "result": {
#             "klines_ok_or_done": 0,
#             "klines_failed": 0,
#             "funding_ok_or_done": 0,
#             "funding_failed": 0,
#             "mark_ok_or_done": 0,
#             "mark_failed": 0,
#             "index_ok_or_done": 0,
#             "index_failed": 0,
#         },
#         "errors_log": ERRORS_LOG.as_posix(),
#     }

#     con = duckdb.connect()
#     try:
#         if include_klines:
#             for zp in _iter("klines", klines_zips):
#                 ok = convert_klines_zip(con, zp, done)
#                 if ok:
#                     stats["result"]["klines_ok_or_done"] += 1
#                 else:
#                     # convert_* returns False for both "in-flight skip" and failure
#                     # Downloads are finished, so False now usually means failure.
#                     stats["result"]["klines_failed"] += 1

#         if include_funding:
#             for zp in _iter("funding", funding_zips):
#                 ok = convert_funding_zip(con, zp, done)
#                 if ok:
#                     stats["result"]["funding_ok_or_done"] += 1
#                 else:
#                     stats["result"]["funding_failed"] += 1

#         if include_mark:
#             for zp in _iter("mark", mark_zips):
#                 ok = convert_mark_zip(con, zp, done)
#                 if ok:
#                     stats["result"]["mark_ok_or_done"] += 1
#                 else:
#                     stats["result"]["mark_failed"] += 1

#         if include_index:
#             for zp in _iter("index", index_zips):
#                 ok = convert_index_zip(con, zp, done)
#                 if ok:
#                     stats["result"]["index_ok_or_done"] += 1
#                 else:
#                     stats["result"]["index_failed"] += 1

#     finally:
#         con.close()

#     return stats

def run_ingest(
    *,
    startdate: str | None = None,
    enddate: str | None = None,
    assets: Iterable[str] | None = None,
    timeframes: Iterable[str] | None = None,   # applies to klines/mark/index
    include_klines: bool = True,
    include_funding: bool = True,
    include_mark: bool = True,
    include_index: bool = True,
    local_root: Path = LOCAL_ROOT,
    show_progress: bool = True,
    verbose: bool = False,
) -> dict:
    """
    Jupyter-first ingest API.

    - Filters by date range (inclusive), assets, and timeframes.
    - Safe to rerun: skips anything already written (done-set) and uses atomic writes.
    - Logs failures to ERRORS_LOG.

    Dates must be ISO 'YYYY-MM-DD' if provided.
    """
    if not local_root.exists():
        raise FileNotFoundError(f"Missing local_root: {local_root}")
    if not NAS_ROOT.parent.exists():
        raise FileNotFoundError(f"NAS not mounted? Missing: {NAS_ROOT.parent}")

    assets_set = set(assets) if assets else None
    tfs_set = set(timeframes) if timeframes else None

    klines_zips, funding_zips, mark_zips, index_zips, start, end = build_zip_targets_ingest_strict(
        local_root,
        startdate=startdate,
        enddate=enddate,
        assets=assets,
        timeframes=timeframes,
        include_klines=include_klines,
        include_funding=include_funding,
        include_mark=include_mark,
        include_index=include_index,
    )

    datasets = set()
    if include_klines: datasets.add("klines")
    if include_funding: datasets.add("funding")
    if include_mark: datasets.add("mark")
    if include_index: datasets.add("index")

    if verbose:
        print("Building done set...")
    done = build_done_set(datasets=datasets)
    if verbose:
        print(f"Done set built ({len(done):,} parquet files)")

    pbar = None
    if show_progress:
        try:
            from tqdm.auto import tqdm as _tqdm
            pbar = _tqdm
        except Exception:
            pbar = None

    def _iter(label: str, items: list[Path]):
        if pbar is None:
            return items
        return pbar(items, desc=label, unit="zip", leave=False)

    stats = {
        "filters": {
            "startdate": startdate,
            "enddate": enddate,
            "assets": sorted(assets_set) if assets_set else None,
            "timeframes": sorted(tfs_set) if tfs_set else None,
            "include_klines": include_klines,
            "include_funding": include_funding,
            "include_mark": include_mark,
            "include_index": include_index,
        },
        "resolved_window": {"start": start.isoformat(), "end": end.isoformat()},
        "targets": {
            "klines": len(klines_zips),
            "funding": len(funding_zips),
            "mark": len(mark_zips),
            "index": len(index_zips),
        },
        "result": {
            "klines_ok_or_done": 0,
            "klines_skipped": 0,
            "klines_failed": 0,
            "funding_ok_or_done": 0,
            "funding_skipped": 0,
            "funding_failed": 0,
            "mark_ok_or_done": 0,
            "mark_skipped": 0,
            "mark_failed": 0,
            "index_ok_or_done": 0,
            "index_skipped": 0,
            "index_failed": 0,
        },
        "errors_log": ERRORS_LOG.as_posix(),
    }

    con = duckdb.connect()
    try:
        if include_klines:
            for zp in _iter("klines", klines_zips):
                status = convert_klines_zip(con, zp, done)
                if status in ("ok", "done"):
                    stats["result"]["klines_ok_or_done"] += 1
                elif status == "skip":
                    stats["result"]['klines_slipped'] += 1
                else:
                    stats["result"]["klines_failed"] += 1

        if include_funding:
            for zp in _iter("funding", funding_zips):
                status = convert_funding_zip(con, zp, done)
                if status in ("ok", "done"):
                    stats["result"]["funding_ok_or_done"] += 1
                elif status == "skip":
                    stats["result"]["funding_skipped"] += 1
                else:
                    stats["result"]["funding_failed"] += 1

        if include_mark:
            for zp in _iter("mark", mark_zips):
                status = convert_mark_zip(con, zp, done)
                if status in ("ok", "done"):
                    stats["result"]["mark_ok_or_done"] += 1
                elif status == "skip":
                    stats["result"]["mark_skipped"] += 1
                else:
                    stats["result"]["mark_failed"] += 1

        if include_index:
            for zp in _iter("index", index_zips):
                status = convert_index_zip(con, zp, done)
                if status in ("ok", "done"):
                    stats["result"]["index_ok_or_done"] += 1
                elif status == "skip":
                    stats["result"]["index_skipped"] += 1
                else:
                    stats["result"]["index_failed"] += 1

    finally:
        con.close()

    return stats
