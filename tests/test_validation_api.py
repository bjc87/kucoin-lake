from __future__ import annotations

from datetime import date
from pathlib import Path

from kucoin_lake import api as api_module
from kucoin_lake.validation.models import ValidationCheckResult
from kucoin_lake.validation.runner import build_validation_run_result


def _fake_run(validator: str, output_dir: Path):
    return build_validation_run_result(
        validator=validator,
        checks=[
            ValidationCheckResult(
                check_id=f"{validator}.check",
                category="unit",
                status="PASS",
                target=validator,
                message="ok",
            )
        ],
        output_dir=output_dir,
    )


def test_api_validate_dispatches_derived_1d(monkeypatch, tmp_path: Path) -> None:
    captured: dict = {}

    def _fake_validate_derived(*args, **kwargs):
        captured.update(kwargs)
        return _fake_run("derived_1d.klines", tmp_path / "derived")

    monkeypatch.setattr(api_module, "validate_derived_1d", _fake_validate_derived)

    run = api_module.validate(
        check="derived-1d",
        nas_root=tmp_path / "lake",
        dataset="klines",
        months=["2025-12"],
        date_start="2025-12-30",
        date_end="2025-12-31",
        output_dir=tmp_path / "derived",
        profile="full",
        sample_limit=9,
    )

    assert run.validator == "derived_1d.klines"
    assert captured["dataset"] == "klines"
    assert captured["months"] == ["2025-12"]
    assert captured["date_start"] == date(2025, 12, 30)
    assert captured["date_end"] == date(2025, 12, 31)
    assert captured["profile"] == "full"
    assert captured["sample_limit"] == 9


def test_api_validate_dispatches_all(monkeypatch, tmp_path: Path) -> None:
    captured: dict = {}

    def _fake_orchestrate(*args, **kwargs):
        captured.update(kwargs)
        return _fake_run("all", tmp_path / "all")

    monkeypatch.setattr(api_module, "orchestrate_validation", _fake_orchestrate)

    run = api_module.validate(
        check="all",
        nas_root=tmp_path / "lake",
        meta_db_path=tmp_path / "meta.duckdb",
        derived_datasets=["klines", "index"],
        append_date_start="2025-12-30",
        append_date_end="2025-12-31",
        candidate_rank_threshold=175,
        output_dir=tmp_path / "all",
        profile="smoke",
        sample_limit=7,
    )

    assert run.validator == "all"
    assert captured["derived_datasets"] == ["klines", "index"]
    assert captured["append_date_start"] == date(2025, 12, 30)
    assert captured["append_date_end"] == date(2025, 12, 31)
    assert captured["candidate_rank_threshold"] == 175
    assert captured["sample_limit"] == 7
