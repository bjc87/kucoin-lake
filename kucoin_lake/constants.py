from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

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

FUTURES_DATASETS_WITH_TIMEFRAME = {"klines", "mark", "index"}
FUTURES_DATASETS_NO_TIMEFRAME = {"funding"}
DEFAULT_FUTURES_DATASETS = ("klines", "mark", "index", "funding")
