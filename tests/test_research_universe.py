from __future__ import annotations

from datetime import datetime
from pathlib import Path

import duckdb
import pandas as pd

from kucoin_lake.research.universe import (
    apply_hysteresis,
    build_universe_daily,
    load_candidate_ranks,
    summarize_universe,
)


def _base_row(**kwargs: object) -> dict:
    row = {
        "day": "2024-01-02",
        "asof_date": "2024-01-01",
        "symbol": "A",
        "asof_dv_30d_median": 10.0,
        "asof_liquidity_rank_30d": 50,
        "asof_dollar_volume": 1000.0,
        "asof_volume": 10.0,
        "asof_close_price": 100.0,
    }
    row.update(kwargs)
    return row


def test_apply_hysteresis_add_remove() -> None:
    candidates = pd.DataFrame(
        [
            _base_row(symbol="A", asof_liquidity_rank_30d=50),
            _base_row(symbol="B", asof_liquidity_rank_30d=120, asof_dv_30d_median=9.0),
            _base_row(symbol="C", asof_liquidity_rank_30d=140, asof_dv_30d_median=8.0),
            _base_row(
                day="2024-01-03",
                asof_date="2024-01-02",
                symbol="A",
                asof_liquidity_rank_30d=150,
                asof_dv_30d_median=7.0,
            ),
            _base_row(
                day="2024-01-03",
                asof_date="2024-01-02",
                symbol="B",
                asof_liquidity_rank_30d=90,
                asof_dv_30d_median=6.0,
            ),
            _base_row(
                day="2024-01-03",
                asof_date="2024-01-02",
                symbol="C",
                asof_liquidity_rank_30d=110,
                asof_dv_30d_median=5.0,
            ),
        ]
    )

    out = apply_hysteresis(candidates, entry_n=100, exit_n=130)
    pairs = list(zip(out["day"].dt.date, out["symbol"]))
    assert pairs == [(datetime(2024, 1, 2).date(), "A"), (datetime(2024, 1, 3).date(), "B")]
    assert out.loc[out["symbol"] == "B", "asof_date"].dt.date.iloc[0] == datetime(
        2024, 1, 2
    ).date()


def test_apply_hysteresis_missing_drop() -> None:
    candidates = pd.DataFrame(
        [
            _base_row(symbol="A", asof_liquidity_rank_30d=50),
            _base_row(
                day="2024-01-03",
                asof_date="2024-01-02",
                symbol="B",
                asof_liquidity_rank_30d=90,
                asof_dv_30d_median=9.0,
            ),
        ]
    )

    out = apply_hysteresis(candidates, entry_n=100, exit_n=130)
    pairs = list(zip(out["day"].dt.date, out["symbol"]))
    assert pairs == [(datetime(2024, 1, 2).date(), "A"), (datetime(2024, 1, 3).date(), "B")]


def test_load_candidate_ranks_shifts_dates(tmp_path: Path) -> None:
    db_path = tmp_path / "meta.duckdb"
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
        con.execute(
            """
            INSERT INTO md.md_liquidity_daily VALUES
            ('futures', '1m', 'A', '2024-01-01', 1000, 10, 100, 500, 1, TRUE, '2024-01-02 00:00:00');
            """
        )
    finally:
        con.close()

    df = load_candidate_ranks(
        meta_db_path=db_path,
        market="futures",
        timeframe="1m",
        start_date="2024-01-02",
        end_date="2024-01-02",
    )

    assert df.shape[0] == 1
    assert df.loc[0, "day"].date() == datetime(2024, 1, 2).date()
    assert df.loc[0, "asof_date"].date() == datetime(2024, 1, 1).date()
    assert df.loc[0, "asof_liquidity_rank_30d"] == 1


def test_build_universe_emit_all_candidates(tmp_path: Path) -> None:
    db_path = tmp_path / "meta.duckdb"
    out_path = tmp_path / "universe.parquet"
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
        con.execute(
            """
            INSERT INTO md.md_liquidity_daily VALUES
            ('futures', '1m', 'A', '2024-01-01', 1000, 10, 100, 500, 1, TRUE, '2024-01-02 00:00:00'),
            ('futures', '1m', 'B', '2024-01-01', 900, 9, 99, 400, 120, FALSE, '2024-01-02 00:00:00');
            """
        )
    finally:
        con.close()

    df = build_universe_daily(
        meta_db_path=db_path,
        market="futures",
        timeframe="1m",
        start_date="2024-01-02",
        end_date="2024-01-02",
        output_path=out_path,
        emit_all_candidates=True,
    )

    assert "in_universe" in df.columns
    assert df.shape[0] == 2
    assert bool(df.loc[df["symbol"] == "A", "in_universe"].iloc[0]) is True
    assert bool(df.loc[df["symbol"] == "B", "in_universe"].iloc[0]) is False


def test_summarize_universe_basic() -> None:
    df = pd.DataFrame(
        [
            {"day": "2024-01-02", "symbol": "A", "in_universe": True},
            {"day": "2024-01-02", "symbol": "B", "in_universe": True},
            {"day": "2024-01-03", "symbol": "B", "in_universe": True},
            {"day": "2024-01-03", "symbol": "C", "in_universe": True},
        ]
    )

    summary = summarize_universe(df)
    assert list(summary["n_members"]) == [2, 2]
    assert list(summary["adds"]) == [2, 1]
    assert list(summary["drops"]) == [0, 1]
    assert summary.loc[1, "turnover_rate"] == 1.0
    assert list(summary["candidate_count"]) == [2, 2]


def test_summarize_universe_candidate_count_missing() -> None:
    df = pd.DataFrame(
        [
            {"day": "2024-01-02", "symbol": "A"},
            {"day": "2024-01-03", "symbol": "A"},
        ]
    )

    summary = summarize_universe(df)
    assert summary["candidate_count"].isna().all()
