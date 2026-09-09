from __future__ import annotations

from datetime import timezone
from pathlib import Path

import pandas as pd

from kucoin_lake import metadata as metadata_module


def _write_klines_parquet(root: Path, symbol: str, date: str, rows: pd.DataFrame) -> Path:
    data_dir = root / "futures" / "klines" / "timeframe=1m" / f"symbol={symbol}" / f"date={date}"
    data_dir.mkdir(parents=True, exist_ok=True)
    path = data_dir / "data.parquet"
    rows.to_parquet(path, index=False)
    return path


def _build_rows(ts_index: pd.DatetimeIndex, symbol: str) -> pd.DataFrame:
    close = 100 + pd.Series(range(len(ts_index))) * 0.01
    data = pd.DataFrame(
        {
            "ts": ts_index,
            "open": close - 0.01,
            "high": close + 0.02,
            "low": close - 0.02,
            "close": close,
            "volume": 1.0,
            "date": ts_index.tz_convert(timezone.utc).date,
            "symbol": symbol,
            "timeframe": "1m",
        }
    )
    return data


def test_kline_integrity_perfect_day(tmp_path: Path) -> None:
    lake_root = tmp_path / "lake"
    symbol = "TESTUSDTM"
    day = "2025-01-01"
    ts_index = pd.date_range(f"{day} 00:00:00", periods=1440, freq="min", tz="UTC")
    rows = _build_rows(ts_index, symbol)
    parquet_path = _write_klines_parquet(lake_root, symbol, day, rows)

    meta_db = tmp_path / "meta.duckdb"
    result = metadata_module.build_or_update_kline_integrity_day(
        lake_root,
        market="futures",
        meta_db_path=meta_db,
        timeframe_filter="1m",
        changed_files=[parquet_path.as_posix()],
    )
    assert result["ok"] is True

    con = metadata_module.connect_meta_db(meta_db)
    try:
        row = con.execute(
            """
            SELECT num_rows, distinct_minute_cnt, gap_rows_vs_expected, duplicate_rows, max_abs_ret_1m
            FROM md.md_kline_integrity_day
            WHERE market = 'futures' AND symbol = ? AND date = ? AND timeframe = '1m';
            """,
            [symbol, day],
        ).fetchone()
    finally:
        con.close()

    assert row is not None
    num_rows, distinct_cnt, gap, dup, max_abs_ret = row
    assert num_rows == 1440
    assert distinct_cnt == 1440
    assert gap == 0
    assert dup == 0
    assert max_abs_ret is not None


def test_kline_integrity_duplicate_minute(tmp_path: Path) -> None:
    lake_root = tmp_path / "lake"
    symbol = "TESTUSDTM"
    day = "2025-01-02"
    ts_index = pd.date_range(f"{day} 00:00:00", periods=1440, freq="min", tz="UTC")
    rows = _build_rows(ts_index, symbol)
    rows = pd.concat([rows, rows.iloc[[10]]], ignore_index=True)
    parquet_path = _write_klines_parquet(lake_root, symbol, day, rows)

    meta_db = tmp_path / "meta.duckdb"
    metadata_module.build_or_update_kline_integrity_day(
        lake_root,
        market="futures",
        meta_db_path=meta_db,
        timeframe_filter="1m",
        changed_files=[parquet_path.as_posix()],
    )

    con = metadata_module.connect_meta_db(meta_db)
    try:
        row = con.execute(
            """
            SELECT num_rows, distinct_minute_cnt, duplicate_rows, gap_rows_vs_expected, max_abs_ret_1m
            FROM md.md_kline_integrity_day
            WHERE market = 'futures' AND symbol = ? AND date = ? AND timeframe = '1m';
            """,
            [symbol, day],
        ).fetchone()
    finally:
        con.close()

    assert row is not None
    num_rows, distinct_cnt, dup, gap, max_abs_ret = row
    assert num_rows == 1441
    assert distinct_cnt == 1440
    assert dup == 1
    assert gap == 0
    assert max_abs_ret is not None


def test_kline_integrity_missing_minute(tmp_path: Path) -> None:
    lake_root = tmp_path / "lake"
    symbol = "TESTUSDTM"
    day = "2025-01-03"
    ts_index = pd.date_range(f"{day} 00:00:00", periods=1440, freq="min", tz="UTC")
    ts_index = ts_index.delete(5)
    rows = _build_rows(ts_index, symbol)
    parquet_path = _write_klines_parquet(lake_root, symbol, day, rows)

    meta_db = tmp_path / "meta.duckdb"
    metadata_module.build_or_update_kline_integrity_day(
        lake_root,
        market="futures",
        meta_db_path=meta_db,
        timeframe_filter="1m",
        changed_files=[parquet_path.as_posix()],
    )

    con = metadata_module.connect_meta_db(meta_db)
    try:
        row = con.execute(
            """
            SELECT num_rows, distinct_minute_cnt, gap_rows_vs_expected, duplicate_rows, max_abs_ret_1m
            FROM md.md_kline_integrity_day
            WHERE market = 'futures' AND symbol = ? AND date = ? AND timeframe = '1m';
            """,
            [symbol, day],
        ).fetchone()
    finally:
        con.close()

    assert row is not None
    num_rows, distinct_cnt, gap, dup, max_abs_ret = row
    assert num_rows == 1439
    assert distinct_cnt == 1439
    assert gap == 1
    assert dup == 0
    assert max_abs_ret is not None
