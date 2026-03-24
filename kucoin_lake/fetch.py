from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from pathlib import Path
import time
from typing import Callable
from urllib.parse import urlencode
from xml.etree import ElementTree as ET

import requests
from requests import Session
from tqdm.auto import tqdm

from kucoin_lake.ingest import log_error
from kucoin_lake.constants import DATASETS, DataType
from kucoin_lake.paths import (
    date_from_key,
    iter_dates,
    key_for_futures_zip,
    month_starts,
    normalize_symbol,
)

BASE = "https://historical-data.kucoin.com/"

# -----------------------------
# Normalisation + date helpers
# -----------------------------

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


def _as_list(x: str | list[str] | tuple[str, ...]) -> list[str]:
    if isinstance(x, (list, tuple)):
        return list(x)
    return [x]


def _normalize_datatype(dt: str) -> DataType:
    dt_norm = dt.strip()
    if dt_norm.lower() == "fundingrates":
        return "funding"
    if dt_norm in DATASETS:
        return dt_norm  # type: ignore[return-value]
    raise ValueError(
        f"Unknown datatype '{dt}'. Expected one of {sorted(DATASETS)} or 'fundingRates'."
    )


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
    on_shard_error: Callable[[date, Exception], None] | None = None,
    on_shard_done: Callable[[date], None] | None = None,
) -> list[str]:
    out: list[str] = []
    for d in month_starts(start, end):
        try:
            out.extend(
                list_month_keys_futures(
                    symbol=symbol,
                    dataset=dataset,
                    year=d.year,
                    month=d.month,
                    timeframe=timeframe,
                )
            )
        except Exception as exc:
            if on_shard_error is None:
                raise
            on_shard_error(d, exc)
        finally:
            if on_shard_done is not None:
                on_shard_done(d)
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

def download_key_retry(
    key: str,
    out_root: Path,
    retries: int = 6,
    backoff_s: float = 1.0,
    timeout: tuple[float, float] = (10, 300),
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

        except (requests.Timeout, requests.ConnectionError, requests.HTTPError):
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

    raise RuntimeError("Unexpected retry loop termination")


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
    datatype: DataType | list[DataType] | str | list[str],
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
    Fetch KuCoin historical futures daily zips for given symbols + datatypes over a date window.

    - symbols: "XAIUSDTM" or ["BTCUSDTM", ...]
    - datatype: "klines" | "funding" | "mark" | "index" (or "fundingRates") or list of those
    - window: either days=int OR (start_date, end_date) as "YYYY-MM-DD"
    - avoids re-downloading files that already exist under out_root/<bucket-key>
    - normalises XBT* -> BTC* (only at start of symbol string)
    - caps end-date to yesterday (UTC) because daily bundles lag / today's file often missing
    - planning is listing-driven: list remote keys first, then download only remote+missing-local
    - progress: separate listing (planning) and download tqdm bars
    """
    out_root = Path(out_root)
    out_root.mkdir(parents=True, exist_ok=True)

    syms = sorted(set(normalize_symbol(s) for s in _as_list(symbols)))
    dts_raw = _as_list(datatype)  # type: ignore[arg-type]
    dts: list[DataType] = [_normalize_datatype(dt) for dt in dts_raw]

    start, end = _resolve_date_range(start_date=start_date, end_date=end_date, days=days)

    yesterday_utc = datetime.now(timezone.utc).date() - timedelta(days=1)
    if end > yesterday_utc:
        end = yesterday_utc
    if start > end:
        raise ValueError(f"Resolved date window is empty: start={start} end={end}")

    dates = list(iter_dates(start, end))
    listing_months = list(month_starts(start, end))

    summary: dict = {
        "start": start.isoformat(),
        "end": end.isoformat(),
        "timeframe": timeframe,
        "symbols": len(syms),
        "datatypes": dts,
        "requested_keys": 0,
        "remote_listed": 0,
        "planned": 0,
        "missing_local": 0,
        "missing_remote": 0,
        "downloaded": 0,
        "skipped_exists": 0,
        "errors": 0,
        "per_symbol": [],
    }

    planned_downloads: list[tuple[DataType, str]] = []
    listing_total_shards = len(syms) * len(dts) * len(listing_months)

    listing_pbar = None
    if show_progress:
        listing_pbar = tqdm(
            total=listing_total_shards,
            desc="Planning remote listings",
            unit="shard",
            leave=True,
        )

    def _tick_listing(n: int = 1) -> None:
        if listing_pbar is None:
            return
        listing_pbar.update(n)
        if listing_pbar.n % 25 == 0 or listing_pbar.n == listing_pbar.total:
            listing_pbar.set_postfix(
                remote_listed=summary["remote_listed"],
                errors=summary["errors"],
            )

    # Plan from remote listings first.
    for sym in syms:
        per = {"symbol": sym, "by_type": {}}

        for dt in dts:
            requested = [key_for_futures_zip(sym, dt, d, timeframe=timeframe) for d in dates]
            requested_set = set(requested)

            listing_errors = 0
            shards_done_for_pair = 0

            def _on_shard_error(month_start: date, exc: Exception) -> None:
                nonlocal listing_errors
                listing_errors += 1
                summary["errors"] += 1
                if verbose:
                    print(
                        f"{sym} {dt}: listing failed for {month_start:%Y-%m} "
                        f"({type(exc).__name__}: {exc})"
                    )
                try:
                    log_error(
                        {
                            "dataset": dt,
                            "symbol": sym,
                            "month": month_start.strftime("%Y-%m"),
                            "error": f"{type(exc).__name__}: {exc}",
                        }
                    )
                except Exception:
                    pass

            def _on_shard_done(_month_start: date) -> None:
                nonlocal shards_done_for_pair
                shards_done_for_pair += 1
                _tick_listing()

            try:
                remote_listed_raw = list_keys_futures(
                    symbol=sym,
                    dataset=dt,
                    start=start,
                    end=end,
                    timeframe=timeframe,
                    on_shard_error=_on_shard_error,
                    on_shard_done=_on_shard_done,
                )
            except Exception as exc:
                summary["errors"] += 1
                listing_errors += 1
                remote_listed_raw = []
                if verbose:
                    print(f"{sym} {dt}: listing failed ({type(exc).__name__}: {exc})")
                try:
                    log_error(
                        {
                            "dataset": dt,
                            "symbol": sym,
                            "error": f"{type(exc).__name__}: {exc}",
                        }
                    )
                except Exception:
                    pass

            # Keep listing progress exact even if list_keys_futures exits before
            # invoking shard callbacks (e.g. monkeypatched or unexpected failure).
            if shards_done_for_pair < len(listing_months):
                _tick_listing(len(listing_months) - shards_done_for_pair)

            remote_listed = [k for k in remote_listed_raw if k in requested_set]
            remote_listed.sort()

            existing_local = local_existing_keys(out_root, remote_listed)
            missing_local = [k for k in remote_listed if k not in existing_local]
            inferred_missing_remote = len(requested) - len(remote_listed)

            summary["requested_keys"] += len(requested)
            summary["remote_listed"] += len(remote_listed)
            summary["planned"] += len(missing_local)
            summary["missing_local"] += len(missing_local)
            summary["missing_remote"] += inferred_missing_remote
            summary["skipped_exists"] += len(existing_local)

            per["by_type"][dt] = {
                "keys": len(requested),
                "requested_keys": len(requested),
                "remote_listed": len(remote_listed),
                "missing_local": len(missing_local),
                "planned": len(missing_local),
                "first": requested[0] if requested else None,
                "last": requested[-1] if requested else None,
                "missing_first": missing_local[0] if missing_local else None,
                "missing_last": missing_local[-1] if missing_local else None,
            }
            if listing_errors:
                per["by_type"][dt]["listing_errors"] = listing_errors

            if verbose:
                print(
                    f"{sym} {dt}: requested={len(requested)} "
                    f"listed={len(remote_listed)} local_missing={len(missing_local)}"
                )

            planned_downloads.extend((dt, k) for k in missing_local)

        summary["per_symbol"].append(per)

    if listing_pbar is not None:
        listing_pbar.set_postfix(
            remote_listed=summary["remote_listed"],
            errors=summary["errors"],
        )
        listing_pbar.close()

    # Reuse one session for speed/robustness
    sess = requests.Session()

    pbar = None
    if show_progress and not dry_run:
        pbar = tqdm(
            total=summary["planned"],
            desc="Downloading KuCoin futures",
            unit="file",
            leave=True,
        )

    def _tick() -> None:
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

    if not dry_run:
        for dt, k in planned_downloads:
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
                elif status == "exists":
                    summary["skipped_exists"] += 1
                elif status == "missing_remote":
                    # Listed key disappeared between listing and download.
                    summary["errors"] += 1
                else:
                    # e.g. forbidden / client_error_4xx
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

    if pbar is not None:
        pbar.set_postfix(
            downloaded=summary["downloaded"],
            missing_remote=summary["missing_remote"],
            errors=summary["errors"],
        )
        pbar.close()

    return summary
