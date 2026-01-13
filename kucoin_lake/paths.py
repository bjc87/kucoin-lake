from __future__ import annotations

import re
from datetime import date, timedelta
from pathlib import Path
from typing import Iterable

from kucoin_lake.constants import DATASETS, DataType, FUTURES_DATASETS_WITH_TIMEFRAME


_DATE_RE = re.compile(r"(\d{4}-\d{2}-\d{2})\.zip(?:\.CHECKSUM)?$")


def normalize_symbol(sym: str) -> str:
    # XBT -> BTC only at start (avoid AIXBT etc.)
    return ("BTC" + sym[3:]) if sym.startswith("XBT") else sym


def date_from_key(key: str) -> date | None:
    m = _DATE_RE.search(key)
    return date.fromisoformat(m.group(1)) if m else None


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
    # fundingRates
    return f"data/futures/daily/{ds}/{sym}/{sym}-fundingRates-{ds_date}.zip"


def _partition_dir_for_timeframe(tf: str) -> str:
    """
    Filesystem partition key for a given timeframe.
    Convention:
      - raw 1m is partitioned by date=YYYY-MM-DD
      - derived timeframes (1h, 4h, 1d, ...) are partitioned by month=YYYY-MM
    """
    return "date=*" if tf == "1m" else "month=*"


def dataset_glob(nas_root: Path, market: str, dataset: str, timeframe: str = "*") -> str:
    base = Path(nas_root) / market / dataset

    if dataset in FUTURES_DATASETS_WITH_TIMEFRAME:
        if timeframe == "*":
            # Match BOTH partition styles under symbol=*
            # - .../date=YYYY-MM-DD/data.parquet
            # - .../month=YYYY-MM/data.parquet
            return (base / "timeframe=*" / "symbol=*" / "**" / "data.parquet").as_posix()

        part = _partition_dir_for_timeframe(timeframe)
        return (base / f"timeframe={timeframe}" / "symbol=*" / part / "data.parquet").as_posix()

    # datasets without timeframe (e.g. funding)
    return (base / "symbol=*" / "date=*" / "data.parquet").as_posix()
