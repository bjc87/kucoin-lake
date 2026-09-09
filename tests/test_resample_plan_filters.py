from __future__ import annotations

from datetime import date
from pathlib import Path

import duckdb

from kucoin_lake import resample as resample_module


def _write_synthetic_1m_day(
    con: duckdb.DuckDBPyConnection,
    path: Path,
    *,
    symbol: str,
    day: date,
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    ts = f"{day.isoformat()} 00:00:00+00"
    path_sql = path.as_posix().replace("'", "''")
    con.execute(
        f"""
        COPY (
            SELECT
                0::BIGINT AS time_ms,
                CAST(? AS TIMESTAMP) AS ts,
                1.0::DOUBLE AS open,
                1.0::DOUBLE AS high,
                1.0::DOUBLE AS low,
                1.0::DOUBLE AS close,
                1.0::DOUBLE AS volume,
                CAST(? AS DATE) AS date,
                CAST(? AS VARCHAR) AS symbol,
                '1m'::VARCHAR AS timeframe
        ) TO '{path_sql}' (FORMAT PARQUET);
        """,
        [ts, day.isoformat(), symbol],
    )


def _seed_1m_file(tmp_root: Path, *, symbol: str, day: date) -> None:
    con = duckdb.connect()
    try:
        path = (
            tmp_root
            / "futures"
            / "klines"
            / "timeframe=1m"
            / f"symbol={symbol}"
            / f"date={day.isoformat()}"
            / "data.parquet"
        )
        _write_synthetic_1m_day(con, path, symbol=symbol, day=day)
    finally:
        con.close()


def test_list_source_parquet_files_filters_by_symbol_and_month(tmp_path: Path) -> None:
    lake_root = tmp_path / "lake"
    _seed_1m_file(lake_root, symbol="BTCUSDTM", day=date(2025, 12, 1))
    _seed_1m_file(lake_root, symbol="BTCUSDTM", day=date(2025, 12, 2))
    _seed_1m_file(lake_root, symbol="BTCUSDTM", day=date(2025, 11, 30))
    _seed_1m_file(lake_root, symbol="ETHUSDTM", day=date(2025, 12, 1))

    files = resample_module._list_source_parquet_files(
        lake_root,
        market="futures",
        dataset="klines",
        timeframe_src="1m",
        symbols=["BTCUSDTM"],
        months=["2025-12"],
    )

    assert len(files) == 2
    assert all("symbol=BTCUSDTM" in path for path in files)
    assert all("/date=2025-12-" in path for path in files)


def test_resample_summary_is_not_ok_when_a_task_errors(monkeypatch, tmp_path: Path) -> None:
    task = resample_module.ResampleTask(
        market="futures",
        dataset="klines",
        symbol="BTCUSDTM",
        month="2025-12",
    )
    monkeypatch.setattr(resample_module, "plan_resample_tasks", lambda *args, **kwargs: [task])
    monkeypatch.setattr(
        resample_module,
        "_write_resampled_month",
        lambda *args, **kwargs: {
            "symbol": task.symbol,
            "month": task.month,
            "status": "error",
            "error": "synthetic failure",
        },
    )

    summary = resample_module.resample_bars(
        tmp_path / "lake",
        local_staging_dir=tmp_path / "stage",
        use_tqdm=False,
    )

    assert summary["ok"] is False
    assert summary["errors_count"] == 1
    assert summary["errors"] == summary["results"]
