from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from datetime import date, datetime, timezone, timedelta
import re
import time
import requests
from urllib.parse import urlencode
from xml.etree import ElementTree as ET
from typing import Iterable, Literal
from requests import Session
from tqdm.auto import tqdm

BASE = "https://historical-data.kucoin.com/"

# -----------------------------
# Normalisation + date helpers
# -----------------------------

def normalize_symbol(sym: str) -> str:
    # XBT -> BTC only at start (avoid AIXBT etc.)
    return ("BTC" + sym[3:]) if sym.startswith("XBT") else sym

_DATE_RE = re.compile(r"(\d{4}-\d{2}-\d{2})\.zip(?:\.CHECKSUM)?$")

def date_from_key(key: str) -> date | None:
    m = _DATE_RE.search(key)
    return date.fromisoformat(m.group(1)) if m else None

def _parse_date(s: str) -> date:
    # expects YYYY-MM-DD
    return date.fromisoformat(s.strip())

def _resolve_date_range(
    *,
    start_date: str | None = None,
    end_date: str | None = None,
    days: int | None = None,
) -> tuple[date, date]:
    """
    Returns (start, end) inclusive bounds.
    - If days provided: start = today - days, end = today
    - Else requires start_date and end_date
    """
    if days is not None:
        end = date.today()
        start = end - timedelta(days=days)
        return start, end

    if not start_date or not end_date:
        raise ValueError("Provide either `days` OR (`start_date` and `end_date`).")

    start = _parse_date(start_date)
    end = _parse_date(end_date)
    if start > end:
        raise ValueError("start_date must be <= end_date")
    return start, end

def month_starts(start: date, end: date) -> Iterable[date]:
    y, m = start.year, start.month
    cur = date(y, m, 1)
    while cur <= end:
        yield cur
        if m == 12:
            y, m = y + 1, 1
        else:
            m += 1
        cur = date(y, m, 1)

def _as_list(x: str | list[str] | tuple[str, ...]) -> list[str]:
    if isinstance(x, (list, tuple)):
        return list(x)
    return [x]

def iter_dates(start: date, end: date) -> Iterable[date]:
    d = start
    while d <= end:
        yield d
        d += timedelta(days=1)

def key_for_futures_zip(symbol: str, dataset: DataType, d: date, timeframe: str = "1m") -> str:
    spec = DATASETS[dataset]
    sym = normalize_symbol(symbol)
    ds = spec.base_dir
    ds_date = d.isoformat()

    if spec.has_timeframe:
        # e.g. klines/index/mark
        return f"data/futures/daily/{ds}/{sym}/{timeframe}/{sym}-{timeframe}-{ds_date}.zip"
    else:
        # fundingRates
        return f"data/futures/daily/{ds}/{sym}/{sym}-fundingRates-{ds_date}.zip"


# -----------------------------
# S3-style listing (single page)
# -----------------------------

def s3_list_page(prefix: str, delimiter: str = "", max_keys: int = 1000) -> dict:
    """
    Returns up to 1000 object keys for a prefix. We avoid pagination entirely by sharding prefixes.
    """
    params = {
        "list-type": "2",
        "prefix": prefix.lstrip("/"),
        "delimiter": delimiter,
        "max-keys": str(max_keys),
    }
    url = BASE + "?" + urlencode(params)
    r = requests.get(url, timeout=30, headers={"User-Agent": "Mozilla/5.0"})
    r.raise_for_status()

    root = ET.fromstring(r.text)

    def local(tag: str) -> str:
        return tag.split("}")[-1]

    files: list[str] = []
    for el in root.iter():
        if local(el.tag) == "Contents":
            key = None
            for child in el:
                if local(child.tag) == "Key":
                    key = child.text
                    break
            if key:
                files.append(key)

    return {"files": files, "url": url}

# -----------------------------
# Datatype config (futures only)
# -----------------------------

DataType = Literal["klines", "funding", "mark", "index"]

@dataclass(frozen=True)
class DatasetSpec:
    name: DataType
    # base directory under data/futures/daily/
    base_dir: str
    # whether it has timeframe folder
    has_timeframe: bool
    # filename prefix (before YYYY-MM-DD), NOT including the trailing YYYY-MM
    # e.g. funding: "{sym}-fundingRates-"
    # e.g. klines:  "{sym}-{tf}-"
    file_prefix_template: str

DATASETS: dict[DataType, DatasetSpec] = {
    "klines": DatasetSpec(
        name="klines",
        base_dir="klines",
        has_timeframe=True,
        file_prefix_template="{sym}-{tf}-",
    ),
    "index": DatasetSpec(
        name="index",
        base_dir="index",
        has_timeframe=True,
        file_prefix_template="{sym}-{tf}-",
    ),
    "mark": DatasetSpec(
        name="mark",
        base_dir="mark",
        has_timeframe=True,
        file_prefix_template="{sym}-{tf}-",
    ),
    "funding": DatasetSpec(
        name="funding",
        base_dir="fundingRates",
        has_timeframe=False,
        file_prefix_template="{sym}-fundingRates-",
    ),
}

# -----------------------------
# Sharded key listing
# -----------------------------

def list_month_keys_futures(
    *,
    symbol: str,
    dataset: DataType,
    year: int,
    month: int,
    timeframe: str = "1m",
) -> list[str]:
    """
    List keys for a single month shard by prefixing with YYYY-MM.
    """
    spec = DATASETS[dataset]
    sym = normalize_symbol(symbol)
    ym = f"{year:04d}-{month:02d}"

    if spec.has_timeframe:
        prefix = (
            f"data/futures/daily/{spec.base_dir}/{sym}/{timeframe}/"
            f"{spec.file_prefix_template.format(sym=sym, tf=timeframe)}{ym}"
        )
    else:
        prefix = (
            f"data/futures/daily/{spec.base_dir}/{sym}/"
            f"{spec.file_prefix_template.format(sym=sym, tf=timeframe)}{ym}"
        )

    page = s3_list_page(prefix, delimiter="", max_keys=1000)
    return [k for k in page["files"] if k.endswith(".zip")]

def list_keys_futures(
    *,
    symbol: str,
    dataset: DataType,
    start: date,
    end: date,
    timeframe: str = "1m",
) -> list[str]:
    out: list[str] = []
    for d in month_starts(start, end):
        out.extend(
            list_month_keys_futures(
                symbol=symbol,
                dataset=dataset,
                year=d.year,
                month=d.month,
                timeframe=timeframe,
            )
        )
    # date filter (inclusive)
    filtered: list[str] = []
    for k in set(out):
        kd = date_from_key(k)
        if kd and start <= kd <= end:
            filtered.append(k)
    filtered.sort()
    return filtered

# -----------------------------
# Download utilities
# -----------------------------

# def download_key_retry(key: str, out_root: Path, retries: int = 6, backoff_s: float = 1.0, timeout=(10, 300)) -> str:
#     url = BASE + key
#     out_path = out_root / key
#     out_path.parent.mkdir(parents=True, exist_ok=True)
#     tmp = out_path.with_suffix(out_path.suffix + ".part")

#     if out_path.exists():
#         return "exists"

#     for attempt in range(1, retries + 1):
#         try:
#             with requests.get(url, stream=True, timeout=timeout, headers={"User-Agent": "Mozilla/5.0"}) as r:
#                 if r.status_code == 404:
#                     return "missing_remote"
#                 r.raise_for_status()
#                 with open(tmp, "wb") as f:
#                     for chunk in r.iter_content(chunk_size=1024 * 1024):
#                         if chunk:
#                             f.write(chunk)
#             tmp.replace(out_path)
#             return "downloaded"
#         except Exception:
#             try:
#                 if tmp.exists():
#                     tmp.unlink()
#             except Exception:
#                 pass
#             if attempt == retries:
#                 raise
#             time.sleep(backoff_s * attempt)

def download_key_retry(
    key: str,
    out_root: Path,
    retries: int = 6,
    backoff_s: float = 1.0,
    timeout=(10, 300),
    session: Session | None = None,
) -> str:
    url = BASE + key
    out_path = out_root / key
    out_path.parent.mkdir(parents=True, exist_ok=True)
    tmp = out_path.with_suffix(out_path.suffix + ".part")

    if out_path.exists():
        return "exists"

    sess = session or requests.Session()

    for attempt in range(1, retries + 1):
        try:
            with sess.get(url, stream=True, timeout=timeout, headers={"User-Agent": "Mozilla/5.0"}) as r:
                sc = r.status_code

                # Permanent / non-retry statuses
                if sc == 404:
                    return "missing_remote"
                if sc in (401, 403):
                    return "forbidden"
                if 400 <= sc < 500 and sc not in (408, 429):
                    # e.g. 400, 409, 410...
                    return f"client_error_{sc}"

                # Retryable statuses
                if sc == 429:
                    # respect Retry-After if provided
                    ra = r.headers.get("Retry-After")
                    if ra:
                        try:
                            time.sleep(float(ra))
                        except Exception:
                            time.sleep(backoff_s * attempt)
                    else:
                        time.sleep(backoff_s * attempt)
                    # try again (do not write body)
                    raise requests.HTTPError("429 rate limited", response=r)

                if 500 <= sc < 600:
                    # server error; retry
                    raise requests.HTTPError(f"{sc} server error", response=r)

                # Otherwise OK
                r.raise_for_status()

                with open(tmp, "wb") as f:
                    for chunk in r.iter_content(chunk_size=1024 * 1024):
                        if chunk:
                            f.write(chunk)

            tmp.replace(out_path)
            return "downloaded"

        except (requests.Timeout, requests.ConnectionError, requests.HTTPError) as e:
            # cleanup partial
            try:
                if tmp.exists():
                    tmp.unlink()
            except Exception:
                pass

            if attempt == retries:
                raise

            time.sleep(backoff_s * attempt)

        except Exception:
            # unexpected error: still cleanup + retry, but keep behaviour
            try:
                if tmp.exists():
                    tmp.unlink()
            except Exception:
                pass

            if attempt == retries:
                raise

            time.sleep(backoff_s * attempt)

def local_existing_keys(out_root: Path, keys: list[str]) -> set[str]:
    return {k for k in keys if (out_root / k).exists()}

def missing_keys(out_root: Path, keys: list[str]) -> list[str]:
    out = []
    for k in keys:
        if not (out_root / k).exists():
            out.append(k)
    return out

# -----------------------------
# Unified Jupyter-friendly API
# -----------------------------

def fetch_futures(
    out_root: str | Path,
    symbols: str | list[str],
    datatype: DataType | list[DataType],
    *,
    timeframe: str = "1m",
    start_date: str | None = None,
    end_date: str | None = None,
    days: int | None = None,
    sleep_s: float = 0.02,
    retries: int = 6,
    backoff_s: float = 1.0,
    timeout=(10, 300),
    dry_run: bool = False,
    show_progress: bool = True,
    verbose: bool = False,
) -> dict:
    """
    Fetch KuCoin historical futures daily zips for given symbols + datatypes over a date window.

    - symbols: "XAIUSDTM" or ["BTCUSDTM", ...]
    - datatype: "klines" | "funding" | "mark" | "index" or list of those
    - window: either days=int OR (start_date, end_date) as "YYYY-MM-DD"
    - avoids re-downloading files that already exist under out_root/<bucket-key>
    - normalises XBT* -> BTC* (only at start of symbol string)
    - caps end-date to yesterday (UTC) because daily bundles lag / today's file often missing
    - progress: one global tqdm bar over total missing-local files
    """
    out_root = Path(out_root)
    out_root.mkdir(parents=True, exist_ok=True)

    syms = sorted(set(normalize_symbol(s) for s in _as_list(symbols)))
    dts: list[DataType] = _as_list(datatype)  # type: ignore

    start, end = _resolve_date_range(start_date=start_date, end_date=end_date, days=days)

    yesterday_utc = datetime.now(timezone.utc).date() - timedelta(days=1)
    if end > yesterday_utc:
        end = yesterday_utc
    if start > end:
        raise ValueError(f"Resolved date window is empty: start={start} end={end}")

    dates = list(iter_dates(start, end))

    summary: dict = {
        "start": start.isoformat(),
        "end": end.isoformat(),
        "timeframe": timeframe,
        "symbols": len(syms),
        "datatypes": dts,
        "planned": 0,
        "missing_local": 0,
        "missing_remote": 0,
        "downloaded": 0,
        "skipped_exists": 0,
        "errors": 0,
        "per_symbol": [],
    }

    # Reuse one session for speed/robustness
    sess = requests.Session()

    # Precompute total missing-local for a single global progress bar
    total_missing = 0
    if show_progress and not dry_run:
        for sym in syms:
            for dt in dts:
                keys = [key_for_futures_zip(sym, dt, d, timeframe=timeframe) for d in dates]
                total_missing += len(missing_keys(out_root, keys))

    pbar = None
    if show_progress and not dry_run:
        pbar = tqdm(
            total=total_missing,
            desc="Downloading KuCoin futures",
            unit="file",
            leave=True,
        )

    def _tick():
        if pbar is None:
            return
        pbar.update(1)
        # update postfix occasionally to avoid overhead
        if pbar.n % 25 == 0:
            pbar.set_postfix(
                downloaded=summary["downloaded"],
                missing_remote=summary["missing_remote"],
                errors=summary["errors"],
            )

    for sym in syms:
        per = {"symbol": sym, "by_type": {}}

        for dt in dts:
            # Generate expected keys directly (no remote listing)
            keys = [key_for_futures_zip(sym, dt, d, timeframe=timeframe) for d in dates]
            missing_local = missing_keys(out_root, keys)

            summary["planned"] += len(keys)
            summary["missing_local"] += len(missing_local)

            per["by_type"][dt] = {
                "keys": len(keys),
                "missing_local": len(missing_local),
                "first": keys[0] if keys else None,
                "last": keys[-1] if keys else None,
                "missing_first": missing_local[0] if missing_local else None,
                "missing_last": missing_local[-1] if missing_local else None,
            }

            if verbose:
                print(f"{sym} {dt}: {len(missing_local)}/{len(keys)} missing locally")

            if dry_run:
                continue

            for k in missing_local:
                try:
                    status = download_key_retry(
                        k,
                        out_root,
                        retries=retries,
                        backoff_s=backoff_s,
                        timeout=timeout,
                        session=sess,
                    )

                    if status == "downloaded":
                        summary["downloaded"] += 1
                    elif status == "missing_remote":
                        summary["missing_remote"] += 1
                    elif status == "exists":
                        summary["skipped_exists"] += 1
                    else:
                        # e.g. forbidden / client_error_4xx if you added those statuses
                        summary["errors"] += 1

                except Exception as e:
                    summary["errors"] += 1
                    # Optional: log for debugging if you have log_error available
                    try:
                        log_error({"dataset": dt, "key": k, "error": f"{type(e).__name__}: {e}"})
                    except Exception:
                        pass

                finally:
                    _tick()
                    if sleep_s:
                        time.sleep(sleep_s)

        summary["per_symbol"].append(per)

    if pbar is not None:
        pbar.set_postfix(
            downloaded=summary["downloaded"],
            missing_remote=summary["missing_remote"],
            errors=summary["errors"],
        )
        pbar.close()

    return summary

# def fetch_futures(
#     out_root: str | Path,
#     symbols: str | list[str],
#     datatype: DataType | list[DataType],
#     *,
#     timeframe: str = "1m",
#     start_date: str | None = None,
#     end_date: str | None = None,
#     days: int | None = None,
#     sleep_s: float = 0.02,
#     retries: int = 6,
#     backoff_s: float = 1.0,
#     timeout=(10, 300),
#     dry_run: bool = False,
# ) -> dict:
#     """
#     Fetch KuCoin historical futures daily zips for given symbols + datatypes over a date window.

#     - symbols: "XAIUSDTM" or ["BTCUSDTM", ...]
#     - datatype: "klines" | "funding" | "mark" | "index" or list of those
#     - window: either days=int OR (start_date, end_date) as "YYYY-MM-DD"
#     - avoids re-downloading files that already exist under out_root/<bucket-key>
#     - normalises XBT* -> BTC* (only at start of symbol string)
#     """
#     out_root = Path(out_root)
#     syms = [normalize_symbol(s) for s in _as_list(symbols)]
#     dts: list[DataType] = _as_list(datatype)  # type: ignore
#     start, end = _resolve_date_range(start_date=start_date, end_date=end_date, days=days)

#     # Clamp end to today (avoid pointless 404s)
#     yesterday = (datetime.now(timezone.utc).date() - timedelta(days=1))
#     if end > yesterday:
#         end = yesterday

#     if start > end:
#         raise ValueError(f"Resolved date window is empty: start={start} end={end}")

#     # Precompute dates once
#     dates = list(iter_dates(start, end))

#     # Optional: dedupe symbols
#     syms = sorted(set(normalize_symbol(s) for s in _as_list(symbols)))

#     summary = {
#         "start": start.isoformat(),
#         "end": end.isoformat(),
#         "timeframe": timeframe,
#         "symbols": len(syms),
#         "datatypes": dts,
#         "planned": 0,
#         "missing_local": 0,
#         "missing_remote": 0,
#         "downloaded": 0,
#         "skipped_exists": 0,
#         "errors": 0,
#         "per_symbol": [],
#     }

#     sess = requests.Session()

#     for sym in syms:
#         per = {"symbol": sym, "by_type": {}}
#         for dt in dts:
#             keys = [key_for_futures_zip(sym, dt, d, timeframe=timeframe) for d in dates]
#             missing_local = missing_keys(out_root, keys)

#             ...
#             per["by_type"][dt] = {
#                 "keys": len(keys),
#                 "missing_local": len(missing_local),
#                 "first": keys[0] if keys else None,
#                 "last": keys[-1] if keys else None,
#                 "missing_first": missing_local[0] if missing_local else None,
#                 "missing_last": missing_local[-1] if missing_local else None,
#             }

#             if dry_run:
#                 continue

#             for k in missing_local:
#                 try:
#                     status = download_key_retry(k, out_root, retries=retries, backoff_s=backoff_s, timeout=timeout, session=sess)
#                     if status == "downloaded":
#                         summary["downloaded"] += 1
#                     elif status == "missing_remote":
#                         summary["missing_remote"] += 1
#                     elif status == "exists":
#                         summary["skipped_exists"] += 1
#                     elif status in ("forbidden",) or status.startswith("client_error_"):
#                         summary["errors"] += 1
#                     else:
#                         summary["errors"] += 1
#                     if sleep_s:
#                         time.sleep(sleep_s)
#                 except Exception as e:
#                     summary["errors"] += 1

#         summary["per_symbol"].append(per)

#     return summary
