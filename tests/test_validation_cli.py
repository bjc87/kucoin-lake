from __future__ import annotations

import sys
import types
from datetime import date
from pathlib import Path

import duckdb
import pytest

if "requests" not in sys.modules:
    requests_stub = types.ModuleType("requests")

    class _Session:
        pass

    requests_stub.Session = _Session
    requests_stub.Timeout = type("Timeout", (Exception,), {})
    requests_stub.ConnectionError = type("ConnectionError", (Exception,), {})
    requests_stub.HTTPError = type("HTTPError", (Exception,), {})

    def _get(*_args, **_kwargs):
        raise RuntimeError("requests.get should not be used in tests")

    requests_stub.get = _get
    sys.modules["requests"] = requests_stub

import kucoin_lake.api as api_module
import kucoin_lake.cli as cli_module


def _build_fixture_metadata(tmp_lake_root: Path, tmp_duckdb_path: Path) -> None:
    api_module.build_metadata(
        tmp_lake_root,
        market="futures",
        datasets=["klines", "mark", "index", "funding"],
        meta_db_path=tmp_duckdb_path,
        timeframe_filter="1m",
    )


def test_cli_validate_metadata_parses_and_dispatches(monkeypatch, tmp_path: Path) -> None:
    captured: dict = {}

    class _RunResult:
        status = "PASS"

        def to_dict(self) -> dict:
            return {"status": self.status}

    def _fake_validate(**kwargs):
        captured.update(kwargs)
        return _RunResult()

    monkeypatch.setattr(cli_module.api, "validate", _fake_validate)

    exit_code = cli_module.main(
        [
            "validate",
            "metadata",
            "--nas-root",
            str(tmp_path / "lake"),
            "--meta-db-path",
            str(tmp_path / "meta.duckdb"),
            "--profile",
            "smoke",
            "--symbols",
            "ETHUSDTM,BTCUSDTM",
            "--symbol",
            "ZRXUSDTM",
            "--symbol",
            "ETHUSDTM",
            "--date-start",
            "2025-12-30",
            "--date-end",
            "2025-12-31",
        ]
    )

    assert exit_code == 0
    assert captured["check"] == "metadata"
    assert captured["profile"] == "smoke"
    assert captured["symbols"] == ["BTCUSDTM", "ETHUSDTM", "ZRXUSDTM"]
    assert captured["date_start"] == date(2025, 12, 30)
    assert captured["date_end"] == date(2025, 12, 31)


def test_cli_validate_derived_parses_and_dispatches(monkeypatch, tmp_path: Path) -> None:
    captured: dict = {}

    class _RunResult:
        status = "PASS"

        def to_dict(self) -> dict:
            return {"status": self.status}

    def _fake_validate(**kwargs):
        captured.update(kwargs)
        return _RunResult()

    monkeypatch.setattr(cli_module.api, "validate", _fake_validate)

    exit_code = cli_module.main(
        [
            "validate",
            "derived-1d",
            "--nas-root",
            str(tmp_path / "lake"),
            "--market",
            "futures",
            "--dataset",
            "klines",
            "--profile",
            "full",
            "--symbols",
            "ETHUSDTM,BTCUSDTM",
            "--symbol",
            "ZRXUSDTM",
            "--month",
            "2025-12",
            "--months",
            "2026-01,2026-02",
            "--sample-limit",
            "5",
            "--date-start",
            "2025-12-30",
            "--date-end",
            "2025-12-31",
        ]
    )

    assert exit_code == 0
    assert captured["check"] == "derived-1d"
    assert captured["dataset"] == "klines"
    assert captured["profile"] == "full"
    assert captured["symbols"] == ["BTCUSDTM", "ETHUSDTM", "ZRXUSDTM"]
    assert captured["months"] == ["2025-12", "2026-01", "2026-02"]
    assert captured["sample_limit"] == 5
    assert captured["date_start"] == date(2025, 12, 30)
    assert captured["date_end"] == date(2025, 12, 31)


def test_cli_validate_all_parses_and_dispatches(monkeypatch, tmp_path: Path) -> None:
    captured: dict = {}

    class _RunResult:
        status = "PASS"

        def to_dict(self) -> dict:
            return {"status": self.status}

    def _fake_validate(**kwargs):
        captured.update(kwargs)
        return _RunResult()

    monkeypatch.setattr(cli_module.api, "validate", _fake_validate)

    exit_code = cli_module.main(
        [
            "validate",
            "all",
            "--nas-root",
            str(tmp_path / "lake"),
            "--meta-db-path",
            str(tmp_path / "meta.duckdb"),
            "--datasets",
            "klines",
            "mark",
            "--derived-datasets",
            "klines",
            "index",
            "--candidate-rank-threshold",
            "180",
            "--append-date-start",
            "2025-12-30",
            "--append-date-end",
            "2025-12-31",
            "--symbols",
            "ETHUSDTM,BTCUSDTM",
            "--symbol",
            "ZRXUSDTM",
            "--profile",
            "smoke",
            "--sample-limit",
            "7",
        ]
    )

    assert exit_code == 0
    assert captured["check"] == "all"
    assert captured["datasets"] == ["klines", "mark"]
    assert captured["derived_datasets"] == ["klines", "index"]
    assert captured["candidate_rank_threshold"] == 180
    assert captured["append_date_start"] == date(2025, 12, 30)
    assert captured["append_date_end"] == date(2025, 12, 31)
    assert captured["symbols"] == ["BTCUSDTM", "ETHUSDTM", "ZRXUSDTM"]
    assert captured["sample_limit"] == 7


def test_cli_validate_metadata_exit_code_success(
    tmp_lake_root: Path,
    tmp_duckdb_path: Path,
    tmp_path: Path,
) -> None:
    _build_fixture_metadata(tmp_lake_root, tmp_duckdb_path)

    exit_code = cli_module.main(
        [
            "validate",
            "metadata",
            "--nas-root",
            str(tmp_lake_root),
            "--meta-db-path",
            str(tmp_duckdb_path),
            "--profile",
            "smoke",
            "--output-dir",
            str(tmp_path / "validation-success"),
        ]
    )

    assert exit_code == 0


def test_cli_validate_metadata_exit_code_fail(
    tmp_lake_root: Path,
    tmp_duckdb_path: Path,
    tmp_path: Path,
) -> None:
    _build_fixture_metadata(tmp_lake_root, tmp_duckdb_path)

    con = duckdb.connect(tmp_duckdb_path.as_posix())
    try:
        key = con.execute(
            """
            SELECT market, timeframe, symbol, date
            FROM md.md_liquidity_daily
            WHERE market = 'futures' AND timeframe = '1m'
            LIMIT 1;
            """
        ).fetchone()
        assert key is not None
        con.execute(
            """
            UPDATE md.md_liquidity_daily
            SET dollar_volume = COALESCE(dollar_volume, 0) + 13.0
            WHERE market = ? AND timeframe = ? AND symbol = ? AND date = ?;
            """,
            list(key),
        )
    finally:
        con.close()

    exit_code = cli_module.main(
        [
            "validate",
            "metadata",
            "--nas-root",
            str(tmp_lake_root),
            "--meta-db-path",
            str(tmp_duckdb_path),
            "--profile",
            "smoke",
            "--output-dir",
            str(tmp_path / "validation-fail"),
        ]
    )

    assert exit_code == 1


def test_cli_validate_metadata_exit_code_error(tmp_lake_root: Path, tmp_path: Path) -> None:
    missing_db = tmp_path / "missing-meta.duckdb"

    exit_code = cli_module.main(
        [
            "validate",
            "metadata",
            "--nas-root",
            str(tmp_lake_root),
            "--meta-db-path",
            str(missing_db),
            "--profile",
            "smoke",
            "--output-dir",
            str(tmp_path / "validation-error"),
        ]
    )

    assert exit_code == 2


@pytest.mark.parametrize(
    ("status", "expected_exit"),
    [
        ("PASS", 0),
        ("FAIL", 1),
        ("ERROR", 2),
    ],
)
def test_cli_validate_derived_exit_codes(monkeypatch, tmp_path: Path, status: str, expected_exit: int) -> None:
    class _RunResult:
        def __init__(self, result_status: str) -> None:
            self.status = result_status

        def to_dict(self) -> dict:
            return {"status": self.status}

    def _fake_validate(**_kwargs):
        return _RunResult(status)

    monkeypatch.setattr(cli_module.api, "validate", _fake_validate)

    exit_code = cli_module.main(
        [
            "validate",
            "derived-1d",
            "--nas-root",
            str(tmp_path / "lake"),
            "--dataset",
            "klines",
        ]
    )

    assert exit_code == expected_exit


@pytest.mark.parametrize(
    ("status", "expected_exit"),
    [
        ("PASS", 0),
        ("FAIL", 1),
        ("ERROR", 2),
    ],
)
def test_cli_validate_all_exit_codes(monkeypatch, tmp_path: Path, status: str, expected_exit: int) -> None:
    class _RunResult:
        def __init__(self, result_status: str) -> None:
            self.status = result_status

        def to_dict(self) -> dict:
            return {"status": self.status}

    def _fake_validate(**_kwargs):
        return _RunResult(status)

    monkeypatch.setattr(cli_module.api, "validate", _fake_validate)

    exit_code = cli_module.main(
        [
            "validate",
            "all",
            "--nas-root",
            str(tmp_path / "lake"),
            "--meta-db-path",
            str(tmp_path / "meta.duckdb"),
        ]
    )

    assert exit_code == expected_exit
