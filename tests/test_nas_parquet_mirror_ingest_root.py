from pathlib import Path

from kucoin_lake.ingest import build_zip_targets_ingest_strict


def _touch(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"")


def _seed_daily_layout(root: Path) -> None:
    _touch(root / "klines" / "BTCUSDTM" / "1m" / "BTCUSDTM-1m-2024-01-02.zip")
    _touch(root / "fundingRates" / "BTCUSDTM" / "BTCUSDTM-fundingRates-2024-01-02.zip")


def test_build_zip_targets_ingest_strict_resolves_parent_layout(tmp_path: Path) -> None:
    daily_root = tmp_path / "futures" / "daily"
    _seed_daily_layout(daily_root)

    klines_zips, funding_zips, _, _, _, _ = build_zip_targets_ingest_strict(
        local_root=tmp_path,
        market="futures",
        startdate="2024-01-01",
        enddate="2024-01-02",
    )

    assert len(klines_zips) > 0
    assert len(funding_zips) > 0
    assert all(path.is_relative_to(daily_root) for path in klines_zips + funding_zips)


def test_build_zip_targets_ingest_strict_resolves_daily_root(tmp_path: Path) -> None:
    daily_root = tmp_path / "futures" / "daily"
    _seed_daily_layout(daily_root)

    klines_zips, funding_zips, _, _, _, _ = build_zip_targets_ingest_strict(
        local_root=daily_root,
        market="futures",
        startdate="2024-01-01",
        enddate="2024-01-02",
    )

    assert len(klines_zips) > 0
    assert len(funding_zips) > 0
    assert all(path.is_relative_to(daily_root) for path in klines_zips + funding_zips)


def test_build_zip_targets_ingest_strict_prefers_direct_dataset_dirs(tmp_path: Path) -> None:
    _seed_daily_layout(tmp_path / "futures" / "daily")
    _seed_daily_layout(tmp_path)

    klines_zips, funding_zips, _, _, _, _ = build_zip_targets_ingest_strict(
        local_root=tmp_path,
        market="futures",
        startdate="2024-01-01",
        enddate="2024-01-02",
    )

    assert len(klines_zips) > 0
    assert len(funding_zips) > 0

    for path in klines_zips + funding_zips:
        rel = path.relative_to(tmp_path)
        assert rel.parts[0] in {"klines", "fundingRates"}
