from __future__ import annotations

from datetime import date
from pathlib import Path

from kucoin_lake.manifest import iter_data_parquets
from kucoin_lake.paths import parse_lake_partitions


def _touch(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"")


def test_iter_data_parquets_filters_by_symbol(tmp_lake_root: Path) -> None:
    extra_path = (
        tmp_lake_root
        / "futures"
        / "klines"
        / "timeframe=1m"
        / "symbol=BTCUSDTM"
        / "date=2025-12-30"
        / "data.parquet"
    )
    _touch(extra_path)

    files = list(
        iter_data_parquets(
            tmp_lake_root,
            market="futures",
            datasets=["klines"],
            timeframe_filter="1m",
            symbols=["ZRXUSDTM"],
        )
    )

    assert files
    assert all("symbol=ZRXUSDTM" in f.file_rel for f in files)


def test_iter_data_parquets_filters_by_date_window_1m(tmp_lake_root: Path) -> None:
    files = list(
        iter_data_parquets(
            tmp_lake_root,
            market="futures",
            datasets=["klines"],
            timeframe_filter="1m",
            date_start=date(2025, 12, 31),
            date_end=date(2025, 12, 31),
        )
    )

    assert files
    dates = {parse_lake_partitions(f.file_rel)["date"] for f in files}
    assert dates == {"2025-12-31"}


def test_iter_data_parquets_filters_by_date_window_1d_months(tmp_lake_root: Path) -> None:
    nov_path = (
        tmp_lake_root
        / "futures"
        / "klines"
        / "timeframe=1d"
        / "symbol=ZRXUSDTM"
        / "month=2025-11"
        / "data.parquet"
    )
    jan_path = (
        tmp_lake_root
        / "futures"
        / "klines"
        / "timeframe=1d"
        / "symbol=ZRXUSDTM"
        / "month=2026-01"
        / "data.parquet"
    )
    _touch(nov_path)
    _touch(jan_path)

    files = list(
        iter_data_parquets(
            tmp_lake_root,
            market="futures",
            datasets=["klines"],
            timeframe_filter="1d",
            date_start=date(2025, 12, 5),
            date_end=date(2026, 1, 2),
        )
    )

    months = {parse_lake_partitions(f.file_rel).get("month") for f in files}
    assert months == {"2025-12", "2026-01"}


def test_iter_data_parquets_no_filters_matches_full_scan(tmp_lake_root: Path) -> None:
    extra_path = (
        tmp_lake_root
        / "futures"
        / "klines"
        / "timeframe=1m"
        / "symbol=BTCUSDTM"
        / "date=2025-12-30"
        / "data.parquet"
    )
    _touch(extra_path)

    files = list(
        iter_data_parquets(
            tmp_lake_root,
            market="futures",
            datasets=["klines"],
            timeframe_filter="1m",
        )
    )

    assert files
    entries = {(parse_lake_partitions(f.file_rel)["symbol"], parse_lake_partitions(f.file_rel)["date"]) for f in files}
    assert entries == {
        ("ZRXUSDTM", "2025-12-30"),
        ("ZRXUSDTM", "2025-12-31"),
        ("BTCUSDTM", "2025-12-30"),
    }
