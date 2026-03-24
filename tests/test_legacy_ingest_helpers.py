from __future__ import annotations

from pathlib import Path

from kucoin_lake import ingest as ingest_module


class _DummyConnection:
    def close(self) -> None:
        return None


def test_run_with_progress_explicit_status_accounting(monkeypatch) -> None:
    monkeypatch.setattr(ingest_module, "tqdm", None)
    monkeypatch.setattr(ingest_module, "is_in_flight", lambda zp: zp.name == "skip.zip")
    monkeypatch.setattr(
        ingest_module,
        "zip_is_valid",
        lambda zp: (False, "bad zip") if zp.name == "fail.zip" else (True, None),
    )

    statuses = {
        "ok.zip": "ok",
        "done.zip": "done",
        "skip.zip": "skip",
        "fail.zip": "fail",
    }

    def _convert(_con, zip_path: Path, _done):
        return statuses[zip_path.name]

    counters = ingest_module.run_with_progress(
        "legacy",
        [Path("ok.zip"), Path("done.zip"), Path("skip.zip"), Path("fail.zip")],
        _convert,
        _DummyConnection(),
        set(),
    )

    assert counters.total == 4
    assert counters.ok_or_done == 2
    assert counters.skipped == 1
    assert counters.failed == 1
    assert counters.in_flight == 1
    assert counters.bad_zip == 1


def test_run_pilot_status_counters_are_truthful(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(ingest_module.duckdb, "connect", lambda: _DummyConnection())
    monkeypatch.setattr(ingest_module, "build_done_set", lambda **kwargs: set())
    monkeypatch.setattr(
        ingest_module,
        "list_klines_zips",
        lambda _local_root: [Path("k-ok.zip"), Path("k-skip.zip"), Path("k-fail.zip")],
    )
    monkeypatch.setattr(
        ingest_module,
        "list_funding_zips",
        lambda _local_root: [Path("f-done.zip"), Path("f-skip.zip"), Path("f-fail.zip")],
    )
    monkeypatch.setattr(ingest_module, "filter_klines", lambda zips, **kwargs: zips)
    monkeypatch.setattr(ingest_module, "filter_funding", lambda zips, **kwargs: zips)
    monkeypatch.setattr(
        ingest_module,
        "convert_klines_zip",
        lambda _con, zp, _done: {"k-ok.zip": "ok", "k-skip.zip": "skip", "k-fail.zip": "fail"}[zp.name],
    )
    monkeypatch.setattr(
        ingest_module,
        "convert_funding_zip",
        lambda _con, zp, _done: {"f-done.zip": "done", "f-skip.zip": "skip", "f-fail.zip": "fail"}[zp.name],
    )

    nas_root = tmp_path / "nas" / "kucoin"
    nas_root.parent.mkdir(parents=True, exist_ok=True)

    stats = ingest_module.run_pilot(
        asset="BTCUSDTM",
        timeframe="1m",
        dates={"2024-01-02"},
        local_root=tmp_path,
        nas_root=nas_root,
        include_klines=True,
        include_funding=True,
    )

    assert stats["klines_targets"] == 3
    assert stats["funding_targets"] == 3
    assert stats["klines_written_or_done"] == 1
    assert stats["klines_skipped"] == 1
    assert stats["klines_failed"] == 1
    assert stats["funding_written_or_done"] == 1
    assert stats["funding_skipped"] == 1
    assert stats["funding_failed"] == 1


def test_run_full_uses_status_based_accounting(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(ingest_module, "tqdm", None)
    monkeypatch.setattr(ingest_module.duckdb, "connect", lambda: _DummyConnection())
    monkeypatch.setattr(ingest_module, "build_done_set", lambda **kwargs: set())
    monkeypatch.setattr(
        ingest_module,
        "list_klines_zips",
        lambda _local_root: [Path("k-ok.zip"), Path("k-skip.zip"), Path("k-fail.zip")],
    )
    monkeypatch.setattr(
        ingest_module,
        "list_funding_zips",
        lambda _local_root: [Path("f-done.zip"), Path("f-skip.zip"), Path("f-fail.zip")],
    )
    monkeypatch.setattr(ingest_module, "is_in_flight", lambda zp: "skip" in zp.name)
    monkeypatch.setattr(
        ingest_module,
        "zip_is_valid",
        lambda zp: (False, "bad zip") if "fail" in zp.name else (True, None),
    )
    monkeypatch.setattr(
        ingest_module,
        "convert_klines_zip",
        lambda _con, zp, _done: {"k-ok.zip": "ok", "k-skip.zip": "skip", "k-fail.zip": "fail"}[zp.name],
    )
    monkeypatch.setattr(
        ingest_module,
        "convert_funding_zip",
        lambda _con, zp, _done: {"f-done.zip": "done", "f-skip.zip": "skip", "f-fail.zip": "fail"}[zp.name],
    )

    nas_root = tmp_path / "nas" / "kucoin"
    nas_root.parent.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(ingest_module, "NAS_ROOT", nas_root)

    result = ingest_module.run_full(local_root=tmp_path, include_klines=True, include_funding=True)

    assert result["klines"]["total"] == 3
    assert result["klines"]["ok_or_done"] == 1
    assert result["klines"]["skipped"] == 1
    assert result["klines"]["failed"] == 1
    assert result["klines"]["in_flight"] == 1
    assert result["klines"]["bad_zip"] == 1

    assert result["funding"]["total"] == 3
    assert result["funding"]["ok_or_done"] == 1
    assert result["funding"]["skipped"] == 1
    assert result["funding"]["failed"] == 1
    assert result["funding"]["in_flight"] == 1
    assert result["funding"]["bad_zip"] == 1
    assert result["errors_log"] == ingest_module.ERRORS_LOG.as_posix()
