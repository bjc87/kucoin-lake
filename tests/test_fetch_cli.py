from __future__ import annotations

from pathlib import Path
import sys
import types

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


def test_api_fetch_futures_delegates(monkeypatch):
    captured = {}

    def _fake_fetch_futures(*args, **kwargs):
        captured["args"] = args
        captured["kwargs"] = kwargs
        return {"ok": True}

    monkeypatch.setattr(api_module.fetch_module, "fetch_futures", _fake_fetch_futures)

    result = api_module.fetch_futures(
        "/tmp/kucoin",
        symbols=["BTCUSDTM"],
        datatype=["klines"],
        timeframe="1m",
        start_date="2024-01-01",
        end_date="2024-01-02",
        days=None,
        sleep_s=0.1,
        retries=2,
        backoff_s=0.5,
        timeout=(1, 2),
        dry_run=True,
        show_progress=False,
        verbose=True,
    )

    assert result == {"ok": True}
    assert captured["args"] == ("/tmp/kucoin", ["BTCUSDTM"], ["klines"])
    assert captured["kwargs"] == {
        "timeframe": "1m",
        "start_date": "2024-01-01",
        "end_date": "2024-01-02",
        "days": None,
        "sleep_s": 0.1,
        "retries": 2,
        "backoff_s": 0.5,
        "timeout": (1, 2),
        "dry_run": True,
        "show_progress": False,
        "verbose": True,
    }


def test_cli_fetch_futures_parses_and_dispatches(monkeypatch, tmp_path: Path):
    captured = {}

    def _fake_fetch_futures(*args, **kwargs):
        captured["args"] = args
        captured["kwargs"] = kwargs
        return {"ok": True}

    monkeypatch.setattr(cli_module.api, "fetch_futures", _fake_fetch_futures)

    args = cli_module._parse_args(
        [
            "fetch-futures",
            "--out-root",
            str(tmp_path),
            "--symbols",
            "BTCUSDTM",
            "ETHUSDTM",
            "--datatype",
            "klines",
            "funding",
            "--timeframe",
            "5m",
            "--start-date",
            "2024-01-01",
            "--end-date",
            "2024-01-03",
            "--sleep-s",
            "0.2",
            "--retries",
            "3",
            "--backoff-s",
            "0.7",
            "--timeout",
            "4",
            "9",
            "--dry-run",
            "--no-progress",
            "--verbose",
        ]
    )

    result = cli_module._handle_fetch_futures(args)

    assert result == {"ok": True}
    assert captured["args"][0] == str(tmp_path)
    assert captured["kwargs"] == {
        "symbols": ["BTCUSDTM", "ETHUSDTM"],
        "datatype": ["klines", "funding"],
        "timeframe": "5m",
        "start_date": "2024-01-01",
        "end_date": "2024-01-03",
        "days": None,
        "sleep_s": 0.2,
        "retries": 3,
        "backoff_s": 0.7,
        "timeout": (4.0, 9.0),
        "dry_run": True,
        "show_progress": False,
        "verbose": True,
    }
