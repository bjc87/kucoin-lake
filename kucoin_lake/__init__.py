"""Public API for kucoin_lake."""

from __future__ import annotations

from typing import TYPE_CHECKING

__all__ = [
    "build_metadata",
    "build_kline_integrity",
    "resample_1m_to_1d",
    "ingest_local_downloads_to_lake",
    "fetch_futures",
]

if TYPE_CHECKING:
    from .api import (
        build_metadata,
        build_kline_integrity,
        fetch_futures,
        ingest_local_downloads_to_lake,
        resample_1m_to_1d,
    )


def __getattr__(name: str):
    if name in __all__:
        from . import api

        return getattr(api, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
