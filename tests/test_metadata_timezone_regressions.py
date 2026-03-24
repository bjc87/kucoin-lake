from __future__ import annotations

from pathlib import Path

import duckdb
import pandas as pd

import metadata as metadata_module


def _write_raw_1m_file(
    lake_root: Path,
    *,
    dataset: str,
    symbol: str,
    day: str,
    ts_values: list[str],
) -> None:
    data_dir = (
        lake_root
        / "futures"
        / dataset
        / "timeframe=1m"
        / f"symbol={symbol}"
        / f"date={day}"
    )
    data_dir.mkdir(parents=True, exist_ok=True)
    path = data_dir / "data.parquet"

    ts = pd.to_datetime(ts_values, utc=True)
    if dataset == "klines":
        rows = pd.DataFrame(
            {
                "ts": ts,
                "open": [100.0 + i for i in range(len(ts))],
                "high": [100.5 + i for i in range(len(ts))],
                "low": [99.5 + i for i in range(len(ts))],
                "close": [100.2 + i for i in range(len(ts))],
                "volume": [1.0 for _ in range(len(ts))],
            }
        )
    else:
        rows = pd.DataFrame({"ts": ts, "price": [100.0 + i for i in range(len(ts))]})
    rows.to_parquet(path, index=False)


def _patch_metadata_connections_to_london(monkeypatch) -> None:
    original_connect = metadata_module.connect_meta_db

    def _connect_london(db_path: Path):
        con = original_connect(db_path)
        con.execute("SET TimeZone = 'Europe/London';")
        return con

    monkeypatch.setattr(metadata_module, "connect_meta_db", _connect_london)


def _write_funding_file(
    lake_root: Path,
    *,
    symbol: str,
    day: str,
    ts_values: list[str],
) -> None:
    data_dir = lake_root / "futures" / "funding" / f"symbol={symbol}" / f"date={day}"
    data_dir.mkdir(parents=True, exist_ok=True)
    path = data_dir / "data.parquet"

    ts = pd.to_datetime(ts_values, utc=True)
    rows = pd.DataFrame(
        {
            "symbol": [symbol for _ in range(len(ts))],
            "ts": ts,
            "funding_rate": [0.0001 + (i * 0.00001) for i in range(len(ts))],
        }
    )
    rows.to_parquet(path, index=False)


def test_raw_1m_day_keys_do_not_spill_on_london_session(tmp_path: Path, monkeypatch) -> None:
    _patch_metadata_connections_to_london(monkeypatch)

    lake_root = tmp_path / "lake"
    meta_db = tmp_path / "meta.duckdb"
    symbol = "DSTUSDTM"
    day = "2024-09-29"

    # One UTC partition day spans two Europe/London calendar dates during BST.
    span_ts = ["2024-09-29T00:10:00Z", "2024-09-29T23:50:00Z"]
    for dataset in ("klines", "mark", "index"):
        _write_raw_1m_file(lake_root, dataset=dataset, symbol=symbol, day=day, ts_values=span_ts)

    metadata_module.build_or_update_metadata(
        lake_root,
        market="futures",
        datasets=["klines", "mark", "index"],
        meta_db_path=meta_db,
        timeframe_filter="1m",
    )

    con = metadata_module.connect_meta_db(meta_db)
    try:
        coverage = con.execute(
            """
            SELECT dataset, date::VARCHAR
            FROM md.md_partition_coverage
            WHERE market = 'futures' AND timeframe = '1m'
            ORDER BY dataset, date;
            """
        ).fetchall()
        liquidity_dates = con.execute(
            """
            SELECT date::VARCHAR
            FROM md.md_liquidity_daily
            WHERE market = 'futures' AND timeframe = '1m' AND symbol = ?
            ORDER BY date;
            """,
            [symbol],
        ).fetchall()
        integrity_dates = con.execute(
            """
            SELECT date::VARCHAR
            FROM md.md_kline_integrity_day
            WHERE market = 'futures' AND timeframe = '1m' AND symbol = ?
            ORDER BY date;
            """,
            [symbol],
        ).fetchall()
    finally:
        con.close()

    assert coverage == [("index", day), ("klines", day), ("mark", day)]
    assert liquidity_dates == [(day,)]
    assert integrity_dates == [(day,)]


def test_funding_day_keys_do_not_spill_on_london_session(tmp_path: Path, monkeypatch) -> None:
    _patch_metadata_connections_to_london(monkeypatch)

    lake_root = tmp_path / "lake"
    meta_db = tmp_path / "meta.duckdb"
    symbol = "FUNDUSDTM"
    day = "2024-09-29"

    # One UTC partition day spans two Europe/London calendar dates during BST.
    _write_funding_file(
        lake_root,
        symbol=symbol,
        day=day,
        ts_values=["2024-09-29T00:10:00Z", "2024-09-29T23:50:00Z"],
    )
    _write_raw_1m_file(
        lake_root,
        dataset="klines",
        symbol=symbol,
        day=day,
        ts_values=["2024-09-29T12:00:00Z"],
    )

    result = metadata_module.build_or_update_metadata(
        lake_root,
        market="futures",
        datasets=["funding"],
        meta_db_path=meta_db,
        timeframe_filter="1m",
    )

    con = metadata_module.connect_meta_db(meta_db)
    try:
        coverage = con.execute(
            """
            SELECT date::VARCHAR, num_rows
            FROM md.md_partition_coverage
            WHERE market = 'futures'
              AND dataset = 'funding'
              AND symbol = ?
              AND timeframe = ''
            ORDER BY date;
            """,
            [symbol],
        ).fetchall()
        spill_rows = con.execute(
            """
            SELECT COUNT(*)
            FROM md.md_partition_coverage
            WHERE market = 'futures'
              AND dataset = 'funding'
              AND symbol = ?
              AND timeframe = ''
              AND date <> CAST(? AS DATE);
            """,
            [symbol, day],
        ).fetchone()[0]
    finally:
        con.close()

    assert result["coverage_mode"] == "bulk_first_run"
    assert coverage == [(day, 2)]
    assert spill_rows == 0


def test_incremental_coverage_adjacent_dst_days_small_chunks(tmp_path: Path, monkeypatch) -> None:
    _patch_metadata_connections_to_london(monkeypatch)

    lake_root = tmp_path / "lake"
    meta_db = tmp_path / "meta.duckdb"
    symbol = "BRETTUSDTM"

    _write_raw_1m_file(
        lake_root,
        dataset="klines",
        symbol=symbol,
        day="2024-09-29",
        ts_values=["2024-09-29T23:50:00Z"],
    )
    _write_raw_1m_file(
        lake_root,
        dataset="klines",
        symbol=symbol,
        day="2024-09-30",
        ts_values=["2024-09-30T00:10:00Z"],
    )

    result = metadata_module.build_or_update_metadata(
        lake_root,
        market="futures",
        datasets=["klines"],
        meta_db_path=meta_db,
        timeframe_filter="1m",
        symbols=[symbol],  # scoped first run -> incremental coverage path
        incremental_chunk_size=1,
    )

    con = metadata_module.connect_meta_db(meta_db)
    try:
        rows = con.execute(
            """
            SELECT date::VARCHAR, num_rows
            FROM md.md_partition_coverage
            WHERE market = 'futures'
              AND dataset = 'klines'
              AND symbol = ?
              AND timeframe = '1m'
            ORDER BY date;
            """,
            [symbol],
        ).fetchall()
    finally:
        con.close()

    assert result["coverage_mode"] == "scoped_first_run"
    assert result["new_or_changed_files"] == 2
    assert rows == [("2024-09-29", 1), ("2024-09-30", 1)]


def test_funding_incremental_coverage_adjacent_dst_days_small_chunks(tmp_path: Path, monkeypatch) -> None:
    _patch_metadata_connections_to_london(monkeypatch)

    lake_root = tmp_path / "lake"
    meta_db = tmp_path / "meta.duckdb"
    symbol = "FUNDSTEPUSDTM"

    _write_funding_file(
        lake_root,
        symbol=symbol,
        day="2024-03-30",
        ts_values=["2024-03-30T23:50:00Z"],
    )
    _write_funding_file(
        lake_root,
        symbol=symbol,
        day="2024-03-31",
        ts_values=["2024-03-31T23:50:00Z"],
    )

    result = metadata_module.build_or_update_metadata(
        lake_root,
        market="futures",
        datasets=["funding"],
        meta_db_path=meta_db,
        timeframe_filter="1m",
        symbols=[symbol],  # scoped first run -> incremental coverage path
        incremental_chunk_size=1,
    )

    con = metadata_module.connect_meta_db(meta_db)
    try:
        rows = con.execute(
            """
            SELECT date::VARCHAR, num_rows
            FROM md.md_partition_coverage
            WHERE market = 'futures'
              AND dataset = 'funding'
              AND symbol = ?
              AND timeframe = ''
            ORDER BY date;
            """,
            [symbol],
        ).fetchall()
        counts = con.execute(
            """
            SELECT COUNT(*) AS row_cnt, COUNT(DISTINCT date) AS distinct_day_cnt
            FROM md.md_partition_coverage
            WHERE market = 'futures'
              AND dataset = 'funding'
              AND symbol = ?
              AND timeframe = '';
            """,
            [symbol],
        ).fetchone()
    finally:
        con.close()

    assert result["coverage_mode"] == "scoped_first_run"
    assert result["new_or_changed_files"] == 2
    assert rows == [("2024-03-30", 1), ("2024-03-31", 1)]
    assert counts == (2, 2)


def test_derived_1d_day_expr_still_uses_timestamp_when_date_absent(tmp_path: Path) -> None:
    sample_path = tmp_path / "derived_1d_sample.parquet"
    pd.DataFrame(
        {
            "ts": pd.to_datetime(["2025-12-30T00:00:00Z"], utc=True),
            "open": [1.0],
            "high": [1.0],
            "low": [1.0],
            "close": [1.0],
            "volume": [1.0],
        }
    ).to_parquet(sample_path, index=False)

    con = duckdb.connect()
    try:
        expr = metadata_module._metadata_day_key_expr(
            con,
            sample_path.as_posix(),
            dataset="klines",
            timeframe_filter="1d",
        )
    finally:
        con.close()

    assert expr == "CAST(ts AS DATE)"
