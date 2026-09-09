from __future__ import annotations

from pathlib import Path

import duckdb

from kucoin_lake.validation.scope_selection import (
    select_candidate_superset_symbols,
    select_near_threshold_symbols,
)


def _create_liquidity_table(db_path: Path) -> None:
    con = duckdb.connect(db_path.as_posix())
    try:
        con.execute("CREATE SCHEMA md;")
        con.execute(
            """
            CREATE TABLE md.md_liquidity_daily (
                market VARCHAR,
                timeframe VARCHAR,
                symbol VARCHAR,
                date DATE,
                dollar_volume DOUBLE,
                volume DOUBLE,
                close_price DOUBLE,
                dv_30d_median DOUBLE,
                liquidity_rank_30d INTEGER,
                in_top_100 BOOLEAN,
                computed_at_utc TIMESTAMP
            );
            """
        )
    finally:
        con.close()


def test_select_candidate_superset_symbols_threshold_window_and_min_days(tmp_path: Path) -> None:
    db_path = tmp_path / "meta.duckdb"
    _create_liquidity_table(db_path)

    con = duckdb.connect(db_path.as_posix())
    try:
        con.execute(
            """
            INSERT INTO md.md_liquidity_daily VALUES
            ('futures','1m','A','2024-01-01',1000,10,100,500,120,FALSE,'2024-01-02 00:00:00'),
            ('futures','1m','A','2024-01-02',1000,10,100,500,140,FALSE,'2024-01-03 00:00:00'),
            ('futures','1m','A','2024-01-03',1000,10,100,500,149,FALSE,'2024-01-04 00:00:00'),
            ('futures','1m','B','2024-01-03',900,9,99,450,151,FALSE,'2024-01-04 00:00:00'),
            ('futures','1m','B','2024-01-04',900,9,99,450,130,FALSE,'2024-01-05 00:00:00'),
            ('futures','1m','C','2024-01-02',850,8,98,430,80,TRUE,'2024-01-03 00:00:00'),
            ('futures','1m','D','2024-01-02',850,8,98,430,NULL,FALSE,'2024-01-03 00:00:00');
            """
        )
    finally:
        con.close()

    symbols_all = select_candidate_superset_symbols(
        db_path,
        market="futures",
        timeframe="1m",
        rank_threshold=150,
    )
    assert symbols_all == ["A", "B", "C"]

    symbols_min_days = select_candidate_superset_symbols(
        db_path,
        market="futures",
        timeframe="1m",
        rank_threshold=150,
        min_days_present=2,
    )
    assert symbols_min_days == ["A"]

    symbols_window = select_candidate_superset_symbols(
        db_path,
        market="futures",
        timeframe="1m",
        rank_threshold=150,
        date_start="2024-01-03",
        date_end="2024-01-05",
    )
    assert symbols_window == ["A", "B"]


def test_select_near_threshold_symbols_defaults_to_latest_lookback(tmp_path: Path) -> None:
    db_path = tmp_path / "meta.duckdb"
    _create_liquidity_table(db_path)

    con = duckdb.connect(db_path.as_posix())
    try:
        con.execute(
            """
            INSERT INTO md.md_liquidity_daily VALUES
            ('futures','1m','X','2024-02-04',1000,10,100,500,95,TRUE,'2024-02-05 00:00:00'),
            ('futures','1m','X','2024-02-05',1000,10,100,500,105,FALSE,'2024-02-06 00:00:00'),
            ('futures','1m','Y','2024-02-05',1000,10,100,500,89,TRUE,'2024-02-06 00:00:00'),
            ('futures','1m','Z','2024-02-05',1000,10,100,500,110,FALSE,'2024-02-06 00:00:00'),
            ('futures','1m','Q','2024-02-01',1000,10,100,500,100,TRUE,'2024-02-02 00:00:00');
            """
        )
    finally:
        con.close()

    symbols = select_near_threshold_symbols(
        db_path,
        market="futures",
        timeframe="1m",
        target_universe_size=100,
        rank_band=10,
        lookback_days=2,
        min_days_present=2,
    )
    assert symbols == ["X"]


def test_select_near_threshold_symbols_respects_explicit_window(tmp_path: Path) -> None:
    db_path = tmp_path / "meta.duckdb"
    _create_liquidity_table(db_path)

    con = duckdb.connect(db_path.as_posix())
    try:
        con.execute(
            """
            INSERT INTO md.md_liquidity_daily VALUES
            ('futures','1m','Q','2024-02-01',1000,10,100,500,100,TRUE,'2024-02-02 00:00:00'),
            ('futures','1m','Q','2024-02-02',1000,10,100,500,114,FALSE,'2024-02-03 00:00:00'),
            ('futures','1m','Q','2024-02-03',1000,10,100,500,130,FALSE,'2024-02-04 00:00:00'),
            ('futures','1m','X','2024-02-05',1000,10,100,500,102,FALSE,'2024-02-06 00:00:00');
            """
        )
    finally:
        con.close()

    symbols = select_near_threshold_symbols(
        db_path,
        market="futures",
        timeframe="1m",
        target_universe_size=100,
        rank_band=15,
        date_start="2024-02-01",
        date_end="2024-02-03",
        lookback_days=1,
        min_days_present=2,
    )
    assert symbols == ["Q"]
