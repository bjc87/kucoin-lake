from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path

import duckdb
import pandas as pd

from kucoin_lake import ingest as ingest_module
from kucoin_lake import resample as resample_module


@dataclass
class _RecordingConnection:
    con: duckdb.DuckDBPyConnection
    saw_set_timezone_utc: bool = False

    def execute(self, query, *args, **kwargs):
        sql = str(query).upper()
        if "SET TIMEZONE" in sql and "UTC" in sql:
            self.saw_set_timezone_utc = True
        return self.con.execute(query, *args, **kwargs)

    def __getattr__(self, name: str):
        return getattr(self.con, name)


def _patch_connect_to_london_with_recording(monkeypatch, target_module):
    real_connect = duckdb.connect
    recordings: list[_RecordingConnection] = []

    def _connect(*args, **kwargs):
        raw = real_connect(*args, **kwargs)
        raw.execute("SET TimeZone = 'Europe/London';")
        wrapped = _RecordingConnection(raw)
        recordings.append(wrapped)
        return wrapped

    monkeypatch.setattr(target_module.duckdb, "connect", _connect)
    return recordings, real_connect


def _write_klines_1m_file(
    lake_root: Path,
    *,
    symbol: str,
    day: str,
    ts_values: list[str],
) -> None:
    path = (
        lake_root
        / "futures"
        / "klines"
        / "timeframe=1m"
        / f"symbol={symbol}"
        / f"date={day}"
        / "data.parquet"
    )
    path.parent.mkdir(parents=True, exist_ok=True)

    ts = pd.to_datetime(ts_values, utc=True)
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
    rows.to_parquet(path, index=False)


def test_run_ingest_sets_utc_session(monkeypatch, tmp_path: Path) -> None:
    recordings, _ = _patch_connect_to_london_with_recording(monkeypatch, ingest_module)

    def _fake_targets(*args, **kwargs):
        day = date(2024, 1, 2)
        return [], [], [], [], day, day

    monkeypatch.setattr(ingest_module, "build_zip_targets_ingest_strict", _fake_targets)
    nas_root = tmp_path / "nas" / "kucoin"
    (tmp_path / "nas").mkdir(parents=True, exist_ok=True)

    ingest_module.run_ingest(
        local_root=tmp_path,
        nas_root=nas_root,
        local_stage_root=tmp_path / "stage",
        startdate="2024-01-02",
        enddate="2024-01-02",
        include_klines=False,
        include_funding=False,
        include_mark=False,
        include_index=False,
        done_set_mode="skip",
        show_progress=False,
    )

    assert recordings
    assert all(rec.saw_set_timezone_utc for rec in recordings)


def test_resample_active_flow_sets_utc_and_preserves_day_partition(monkeypatch, tmp_path: Path) -> None:
    recordings, real_connect = _patch_connect_to_london_with_recording(monkeypatch, resample_module)

    lake_root = tmp_path / "lake"
    symbol = "UTCFLOWUSDTM"
    day = "2025-06-01"

    # 23:55Z is next local day in Europe/London during BST; partition day must remain canonical.
    _write_klines_1m_file(
        lake_root,
        symbol=symbol,
        day=day,
        ts_values=["2025-06-01T00:05:00Z", "2025-06-01T23:55:00Z"],
    )

    result = resample_module.resample_bars(
        lake_root,
        local_staging_dir=tmp_path / "stage",
        market="futures",
        dataset="klines",
        timeframe_src="1m",
        timeframe_dst="1d",
        use_tqdm=False,
    )

    out_path = (
        lake_root
        / "futures"
        / "klines"
        / "timeframe=1d"
        / f"symbol={symbol}"
        / "month=2025-06"
        / "data.parquet"
    )

    con = real_connect(":memory:")
    try:
        out_days = con.execute(
            "SELECT date::VARCHAR FROM read_parquet(?) ORDER BY date;",
            [out_path.as_posix()],
        ).fetchall()
    finally:
        con.close()

    assert result["errors_count"] == 0
    assert result["written"] == 1
    assert len(recordings) >= 2
    assert all(rec.saw_set_timezone_utc for rec in recordings)
    assert out_days == [(day,)]
