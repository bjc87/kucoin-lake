from __future__ import annotations

import os
import subprocess
import sys
from datetime import date
from pathlib import Path

from kucoin_lake import api as api_module
from kucoin_lake import cli as cli_module
from kucoin_lake import ingest as ingest_module


def test_import_does_not_create_storage_directories(tmp_path: Path) -> None:
    repo_root = Path(__file__).resolve().parents[1]
    env = os.environ.copy()
    env["PYTHONPATH"] = str(repo_root)
    env.pop("KUCOIN_LAKE_LOCAL_ROOT", None)
    env.pop("KUCOIN_LAKE_NAS_ROOT", None)
    env.pop("KUCOIN_LAKE_LOCAL_STAGE_ROOT", None)

    subprocess.run(
        [sys.executable, "-c", "import kucoin_lake; import kucoin_lake.api"],
        cwd=tmp_path,
        env=env,
        check=True,
    )

    assert list(tmp_path.iterdir()) == []


def test_ingest_api_delegates_explicit_paths(monkeypatch, tmp_path: Path) -> None:
    captured: dict = {}

    def _fake_run_ingest(**kwargs):
        captured.update(kwargs)
        return {"ok": True}

    monkeypatch.setattr(api_module.mirror_module, "run_ingest", _fake_run_ingest)

    result = api_module.ingest_local_downloads_to_lake(
        local_root=tmp_path / "downloads",
        nas_root=tmp_path / "lake",
        local_staging_dir=tmp_path / "stage",
        startdate="2024-01-01",
        enddate="2024-01-02",
        verbose=True,
    )

    assert result == {"ok": True}
    assert captured["local_root"] == tmp_path / "downloads"
    assert captured["nas_root"] == tmp_path / "lake"
    assert captured["local_stage_root"] == tmp_path / "stage"


def test_ingest_cli_parses_and_dispatches_explicit_paths(monkeypatch, tmp_path: Path) -> None:
    captured: dict = {}

    def _fake_ingest(**kwargs):
        captured.update(kwargs)
        return {"ok": True}

    monkeypatch.setattr(cli_module.api, "ingest_local_downloads_to_lake", _fake_ingest)

    args = cli_module._parse_args(
        [
            "ingest-local-downloads-to-lake",
            "--startdate",
            "2024-01-01",
            "--enddate",
            "2024-01-02",
            "--local-root",
            str(tmp_path / "downloads"),
            "--nas-root",
            str(tmp_path / "lake"),
            "--local-staging-dir",
            str(tmp_path / "stage"),
            "--assets",
            "BTCUSDTM",
            "--timeframes",
            "1m",
            "--verbose",
        ]
    )

    result = cli_module._handle_ingest(args)

    assert result == {"ok": True}
    assert captured["local_root"] == str(tmp_path / "downloads")
    assert captured["nas_root"] == str(tmp_path / "lake")
    assert captured["local_staging_dir"] == str(tmp_path / "stage")
    assert captured["assets"] == ["BTCUSDTM"]
    assert captured["timeframes"] == ["1m"]


def test_run_ingest_passes_explicit_paths_to_converter(monkeypatch, tmp_path: Path) -> None:
    local_root = tmp_path / "downloads"
    nas_root = tmp_path / "lake"
    staging_root = tmp_path / "stage"
    local_root.mkdir()
    captured: dict = {}

    def _fake_targets(*args, **kwargs):
        day = date(2024, 1, 1)
        return [local_root / "BTCUSDTM-1m-2024-01-01.zip"], [], [], [], day, day

    def _fake_convert(con, zip_path, done, **kwargs):
        captured.update(kwargs)
        return "ok"

    monkeypatch.setattr(ingest_module, "build_zip_targets_ingest_strict", _fake_targets)
    monkeypatch.setattr(ingest_module, "convert_klines_zip", _fake_convert)

    result = ingest_module.run_ingest(
        local_root=local_root,
        nas_root=nas_root,
        local_stage_root=staging_root,
        startdate="2024-01-01",
        enddate="2024-01-01",
        include_funding=False,
        include_mark=False,
        include_index=False,
        done_set_mode="skip",
        show_progress=False,
    )

    assert result["result"]["klines_ok_or_done"] == 1
    assert captured["nas_root"] == nas_root
    assert captured["local_stage_root"] == staging_root
    assert captured["errors_log"] == nas_root / "_logs" / "nas_parquet_mirror_errors.jsonl"
