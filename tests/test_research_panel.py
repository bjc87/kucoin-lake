from __future__ import annotations

from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd

from kucoin_lake.research.panel import build_base_panel, summarize_panel


def _universe_rows(include_flag: bool = False) -> pd.DataFrame:
    rows: list[dict] = []
    for day_str, asof_str in [
        ("2024-01-02", "2024-01-01"),
        ("2024-01-03", "2024-01-02"),
        ("2024-01-04", "2024-01-03"),
    ]:
        for symbol, rank in [("A", 10), ("B", 20)]:
            row = {
                "day": day_str,
                "symbol": symbol,
                "asof_date": asof_str,
                "asof_dv_30d_median": float(1000 - rank),
                "asof_liquidity_rank_30d": rank,
                "asof_dollar_volume": float(10000 - rank),
                "asof_volume": float(100 - rank),
                "asof_close_price": float(200 - rank),
                "entry_n": 100,
                "exit_n": 130,
                "is_weekend": False,
            }
            if include_flag:
                row["in_universe"] = not (symbol == "B" and day_str == "2024-01-03")
            rows.append(row)
    return pd.DataFrame(rows)


def _write_1d_klines(nas_root: Path) -> None:
    a_path = (
        nas_root
        / "futures"
        / "klines"
        / "timeframe=1d"
        / "symbol=A"
        / "month=2024-01"
        / "data.parquet"
    )
    b_path = (
        nas_root
        / "futures"
        / "klines"
        / "timeframe=1d"
        / "symbol=B"
        / "month=2024-01"
        / "data.parquet"
    )
    a_path.parent.mkdir(parents=True, exist_ok=True)
    b_path.parent.mkdir(parents=True, exist_ok=True)

    pd.DataFrame(
        [
            {
                "date": "2024-01-02",
                "open": 10.0,
                "high": 11.0,
                "low": 9.0,
                "close": 11.0,
                "volume": 100.0,
                "dollar_volume": 1100.0,
                "vwap": 10.5,
                "src_rows": 1440,
                "max_ts": "2024-01-02 23:59:00",
            },
            {
                "date": "2024-01-03",
                "open": 11.0,
                "high": 12.0,
                "low": 10.0,
                "close": 12.0,
                "volume": 90.0,
                "dollar_volume": 1080.0,
                "vwap": 11.3,
                "src_rows": 1440,
                "max_ts": "2024-01-03 23:59:00",
            },
            {
                "date": "2024-01-04",
                "open": 12.0,
                "high": 13.0,
                "low": 11.0,
                "close": 13.0,
                "volume": 80.0,
                "dollar_volume": 1040.0,
                "vwap": 12.2,
                "src_rows": 1440,
                "max_ts": "2024-01-04 23:59:00",
            },
            {
                "date": "2024-01-07",
                "open": 16.0,
                "high": 18.0,
                "low": 15.0,
                "close": 17.0,
                "volume": 75.0,
                "dollar_volume": 1275.0,
                "vwap": 16.5,
                "src_rows": 1440,
                "max_ts": "2024-01-07 23:59:00",
            },
            {
                "date": "2024-01-22",
                "open": 31.0,
                "high": 33.0,
                "low": 30.0,
                "close": 32.0,
                "volume": 70.0,
                "dollar_volume": 2240.0,
                "vwap": 31.5,
                "src_rows": 1440,
                "max_ts": "2024-01-22 23:59:00",
            },
        ]
    ).to_parquet(a_path, index=False)

    pd.DataFrame(
        [
            {
                "date": "2024-01-02",
                "open": 20.0,
                "high": 21.0,
                "low": 19.0,
                "close": 19.0,
                "volume": 150.0,
                "dollar_volume": 2850.0,
                "vwap": 19.5,
                "src_rows": 1440,
                "max_ts": "2024-01-02 23:59:00",
            },
            {
                "date": "2024-01-03",
                "open": 19.0,
                "high": 20.0,
                "low": 18.0,
                "close": 20.0,
                "volume": 120.0,
                "dollar_volume": 2400.0,
                "vwap": 19.3,
                "src_rows": 1440,
                "max_ts": "2024-01-03 23:59:00",
            },
            {
                "date": "2024-01-04",
                "open": 20.0,
                "high": 22.0,
                "low": 19.0,
                "close": 21.0,
                "volume": 115.0,
                "dollar_volume": 2415.0,
                "vwap": 20.4,
                "src_rows": 1440,
                "max_ts": "2024-01-04 23:59:00",
            },
        ]
    ).to_parquet(b_path, index=False)


def test_build_base_panel_join_and_returns(tmp_path: Path) -> None:
    universe_path = tmp_path / "universe.parquet"
    output_path = tmp_path / "base_panel.parquet"
    nas_root = tmp_path / "nas"

    _universe_rows(include_flag=False).to_parquet(universe_path, index=False)
    _write_1d_klines(nas_root)

    panel = build_base_panel(
        universe_path=universe_path,
        nas_root=nas_root,
        market="futures",
        start_date="2024-01-02",
        end_date="2024-01-04",
        output_path=output_path,
    )

    assert panel.shape[0] == 6
    assert panel[["symbol", "day"]].equals(
        panel[["symbol", "day"]].sort_values(["symbol", "day"]).reset_index(drop=True)
    )

    row = panel[(panel["symbol"] == "A") & (panel["day"] == pd.Timestamp("2024-01-03"))].iloc[0]
    assert row["ret_1d"] == (12.0 / 11.0 - 1.0)
    assert row["fwd_ret_1d"] == (13.0 / 12.0 - 1.0)
    assert row["log_ret_1d"] == np.log(12.0) - np.log(11.0)
    assert row["fwd_log_ret_1d"] == np.log(13.0) - np.log(12.0)
    assert row["log_dollar_volume"] == np.log(1080.0)

    first_row = panel[
        (panel["symbol"] == "A") & (panel["day"] == pd.Timestamp("2024-01-02"))
    ].iloc[0]
    assert first_row["fwd_ret_5d"] == (17.0 / 11.0 - 1.0)
    assert first_row["fwd_ret_20d"] == (32.0 / 11.0 - 1.0)

    required_cols = {
        "day",
        "symbol",
        "asof_date",
        "is_weekend",
        "asof_dv_30d_median",
        "asof_liquidity_rank_30d",
        "asof_dollar_volume",
        "asof_volume",
        "asof_close_price",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "dollar_volume",
        "log_dollar_volume",
        "vwap",
        "src_rows",
        "max_ts",
        "ret_1d",
        "fwd_ret_1d",
        "fwd_ret_5d",
        "fwd_ret_20d",
        "log_ret_1d",
        "fwd_log_ret_1d",
    }
    assert required_cols.issubset(set(panel.columns))


def test_build_base_panel_respects_require_in_universe(tmp_path: Path) -> None:
    universe_path = tmp_path / "universe_with_flag.parquet"
    output_path = tmp_path / "base_panel.parquet"
    nas_root = tmp_path / "nas"

    _universe_rows(include_flag=True).to_parquet(universe_path, index=False)
    _write_1d_klines(nas_root)

    panel_required = build_base_panel(
        universe_path=universe_path,
        nas_root=nas_root,
        market="futures",
        output_path=output_path,
        require_in_universe=True,
    )

    panel_all = build_base_panel(
        universe_path=universe_path,
        nas_root=nas_root,
        market="futures",
        output_path=output_path,
        require_in_universe=False,
    )

    assert panel_required.shape[0] == 5
    assert panel_all.shape[0] == 6
    assert panel_required[
        (panel_required["symbol"] == "B") & (panel_required["day"] == pd.Timestamp("2024-01-03"))
    ].empty
    reentry = panel_required[
        (panel_required["symbol"] == "B") & (panel_required["day"] == pd.Timestamp("2024-01-04"))
    ].iloc[0]
    assert reentry["ret_1d"] == (21.0 / 20.0 - 1.0)


def test_build_base_panel_does_not_bridge_missing_calendar_days(tmp_path: Path) -> None:
    universe_path = tmp_path / "universe.parquet"
    output_path = tmp_path / "base_panel.parquet"
    nas_root = tmp_path / "nas"

    universe = _universe_rows(include_flag=False)
    universe = universe[
        (universe["symbol"] == "A") & universe["day"].isin(["2024-01-02", "2024-01-04"])
    ]
    universe.to_parquet(universe_path, index=False)
    _write_1d_klines(nas_root)

    a_path = (
        nas_root
        / "futures"
        / "klines"
        / "timeframe=1d"
        / "symbol=A"
        / "month=2024-01"
        / "data.parquet"
    )
    bars = pd.read_parquet(a_path)
    bars[bars["date"] != "2024-01-03"].to_parquet(a_path, index=False)

    panel = build_base_panel(
        universe_path=universe_path,
        nas_root=nas_root,
        start_date="2024-01-02",
        end_date="2024-01-04",
        output_path=output_path,
    )

    day2 = panel[panel["day"] == pd.Timestamp("2024-01-02")].iloc[0]
    day4 = panel[panel["day"] == pd.Timestamp("2024-01-04")].iloc[0]
    assert np.isnan(day2["fwd_ret_1d"])
    assert np.isnan(day4["ret_1d"])


def test_build_base_panel_uses_forward_bar_beyond_output_window(tmp_path: Path) -> None:
    universe_path = tmp_path / "universe.parquet"
    output_path = tmp_path / "base_panel.parquet"
    nas_root = tmp_path / "nas"

    _universe_rows(include_flag=False).to_parquet(universe_path, index=False)
    _write_1d_klines(nas_root)

    panel = build_base_panel(
        universe_path=universe_path,
        nas_root=nas_root,
        start_date="2024-01-03",
        end_date="2024-01-03",
        output_path=output_path,
    )

    row = panel[(panel["symbol"] == "A") & (panel["day"] == pd.Timestamp("2024-01-03"))].iloc[0]
    assert row["fwd_ret_1d"] == (13.0 / 12.0 - 1.0)


def test_summarize_panel_outputs_expected_columns() -> None:
    panel = pd.DataFrame(
        [
            {
                "day": "2024-01-02",
                "symbol": "A",
                "close": 10.0,
                "ret_1d": np.nan,
                "fwd_ret_1d": 0.1,
                "fwd_ret_5d": np.nan,
                "fwd_ret_20d": np.nan,
            },
            {
                "day": "2024-01-02",
                "symbol": "B",
                "close": 20.0,
                "ret_1d": np.nan,
                "fwd_ret_1d": 0.2,
                "fwd_ret_5d": np.nan,
                "fwd_ret_20d": np.nan,
            },
            {
                "day": "2024-01-03",
                "symbol": "A",
                "close": 11.0,
                "ret_1d": 0.1,
                "fwd_ret_1d": np.nan,
                "fwd_ret_5d": np.nan,
                "fwd_ret_20d": np.nan,
            },
            {
                "day": "2024-01-03",
                "symbol": "B",
                "close": np.nan,
                "ret_1d": 0.05,
                "fwd_ret_1d": np.nan,
                "fwd_ret_5d": np.nan,
                "fwd_ret_20d": np.nan,
            },
        ]
    )

    summary = summarize_panel(panel)

    assert list(summary.columns) == [
        "day",
        "n_symbols",
        "mean_ret_1d",
        "mean_fwd_ret_1d",
        "mean_fwd_ret_5d",
        "mean_fwd_ret_20d",
        "missing_close_pct",
        "missing_fwd_ret_1d_pct",
    ]
    assert summary.loc[0, "n_symbols"] == 2
    assert np.isclose(summary.loc[0, "mean_fwd_ret_1d"], 0.15)
    assert summary.loc[1, "missing_close_pct"] == 0.5
    assert summary.loc[1, "missing_fwd_ret_1d_pct"] == 1.0
    assert summary.loc[0, "day"].date() == datetime(2024, 1, 2).date()
