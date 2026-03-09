from __future__ import annotations

from datetime import date
from pathlib import Path

import kucoin_lake.fetch as fetch_module


def _futures_key(symbol: str, dataset: str, d: date, timeframe: str = "1m") -> str:
    return fetch_module.key_for_futures_zip(symbol, dataset, d, timeframe=timeframe)


def test_fetch_futures_listing_plans_only_remote_and_skips_local(
    monkeypatch, tmp_path: Path
) -> None:
    out_root = tmp_path / "downloads"
    symbol = "BTCUSDTM"
    dataset = "klines"

    in_range_a = _futures_key(symbol, dataset, date(2024, 1, 1))
    in_range_b = _futures_key(symbol, dataset, date(2024, 1, 3))
    out_of_range = _futures_key(symbol, dataset, date(2024, 1, 6))

    local_existing_path = out_root / in_range_b
    local_existing_path.parent.mkdir(parents=True, exist_ok=True)
    local_existing_path.touch()

    def _fake_list_keys_futures(**_kwargs):
        # Includes one key outside the requested range; fetch_futures must filter it out.
        return [out_of_range, in_range_b, in_range_a]

    attempted_downloads: list[str] = []

    def _fake_download_key_retry(key: str, *_args, **_kwargs) -> str:
        attempted_downloads.append(key)
        return "downloaded"

    monkeypatch.setattr(fetch_module, "list_keys_futures", _fake_list_keys_futures)
    monkeypatch.setattr(fetch_module, "download_key_retry", _fake_download_key_retry)

    summary = fetch_module.fetch_futures(
        out_root,
        symbols=[symbol],
        datatype=[dataset],
        start_date="2024-01-01",
        end_date="2024-01-05",
        sleep_s=0.0,
        show_progress=False,
    )

    assert attempted_downloads == [in_range_a]
    assert summary["requested_keys"] == 5
    assert summary["remote_listed"] == 2
    assert summary["planned"] == 1
    assert summary["missing_local"] == 1
    assert summary["missing_remote"] == 3
    assert summary["downloaded"] == 1
    assert summary["skipped_exists"] == 1
    assert summary["errors"] == 0

    by_type = summary["per_symbol"][0]["by_type"][dataset]
    assert by_type["requested_keys"] == 5
    assert by_type["remote_listed"] == 2
    assert by_type["missing_local"] == 1
    assert by_type["planned"] == 1
    assert by_type["missing_first"] == in_range_a
    assert by_type["missing_last"] == in_range_a

    assert summary["requested_keys"] == summary["remote_listed"] + summary["missing_remote"]
    assert summary["planned"] == summary["missing_local"]


def test_fetch_futures_empty_remote_listing_has_no_download_attempts(
    monkeypatch, tmp_path: Path
) -> None:
    out_root = tmp_path / "downloads"

    monkeypatch.setattr(fetch_module, "list_keys_futures", lambda **_kwargs: [])

    attempted_downloads: list[str] = []

    def _fake_download_key_retry(key: str, *_args, **_kwargs) -> str:
        attempted_downloads.append(key)
        return "downloaded"

    monkeypatch.setattr(fetch_module, "download_key_retry", _fake_download_key_retry)

    summary = fetch_module.fetch_futures(
        out_root,
        symbols=["BTCUSDTM"],
        datatype=["klines"],
        start_date="2024-01-01",
        end_date="2024-01-03",
        sleep_s=0.0,
        show_progress=False,
    )

    assert attempted_downloads == []
    assert summary["requested_keys"] == 3
    assert summary["remote_listed"] == 0
    assert summary["planned"] == 0
    assert summary["missing_local"] == 0
    assert summary["missing_remote"] == 3
    assert summary["downloaded"] == 0
    assert summary["errors"] == 0


def test_fetch_futures_shard_listing_failure_is_error_without_bruteforce_fallback(
    monkeypatch, tmp_path: Path
) -> None:
    out_root = tmp_path / "downloads"
    symbol = "BTCUSDTM"
    dataset = "klines"
    feb_key = _futures_key(symbol, dataset, date(2024, 2, 1))

    def _fake_list_month_keys_futures(*, year: int, month: int, **_kwargs):
        if year == 2024 and month == 1:
            raise RuntimeError("month listing failure")
        if year == 2024 and month == 2:
            return [feb_key]
        return []

    attempted_downloads: list[str] = []

    def _fake_download_key_retry(key: str, *_args, **_kwargs) -> str:
        attempted_downloads.append(key)
        return "downloaded"

    monkeypatch.setattr(fetch_module, "list_month_keys_futures", _fake_list_month_keys_futures)
    monkeypatch.setattr(fetch_module, "download_key_retry", _fake_download_key_retry)

    summary = fetch_module.fetch_futures(
        out_root,
        symbols=[symbol],
        datatype=[dataset],
        start_date="2024-01-30",
        end_date="2024-02-02",
        sleep_s=0.0,
        show_progress=False,
    )

    assert attempted_downloads == [feb_key]
    assert summary["requested_keys"] == 4
    assert summary["remote_listed"] == 1
    assert summary["planned"] == 1
    assert summary["missing_remote"] == 3
    assert summary["errors"] == 1
    assert summary["per_symbol"][0]["by_type"][dataset]["listing_errors"] == 1


def test_fetch_futures_listing_failure_records_error_and_skips_download(
    monkeypatch, tmp_path: Path
) -> None:
    out_root = tmp_path / "downloads"

    def _fake_list_keys_futures(**_kwargs):
        raise RuntimeError("listing unavailable")

    attempted_downloads: list[str] = []

    def _fake_download_key_retry(key: str, *_args, **_kwargs) -> str:
        attempted_downloads.append(key)
        return "downloaded"

    monkeypatch.setattr(fetch_module, "list_keys_futures", _fake_list_keys_futures)
    monkeypatch.setattr(fetch_module, "download_key_retry", _fake_download_key_retry)

    summary = fetch_module.fetch_futures(
        out_root,
        symbols=["BTCUSDTM"],
        datatype=["klines"],
        start_date="2024-01-01",
        end_date="2024-01-03",
        sleep_s=0.0,
        show_progress=False,
    )

    assert attempted_downloads == []
    assert summary["requested_keys"] == 3
    assert summary["remote_listed"] == 0
    assert summary["planned"] == 0
    assert summary["missing_local"] == 0
    assert summary["missing_remote"] == 3
    assert summary["errors"] == 1


def test_fetch_futures_progress_total_is_planned_downloads(monkeypatch, tmp_path: Path) -> None:
    out_root = tmp_path / "downloads"
    symbol = "BTCUSDTM"
    dataset = "klines"
    key_a = _futures_key(symbol, dataset, date(2024, 1, 1))
    key_b = _futures_key(symbol, dataset, date(2024, 1, 2))

    monkeypatch.setattr(fetch_module, "list_keys_futures", lambda **_kwargs: [key_a, key_b])

    class _FakeTqdm:
        instances: list["_FakeTqdm"] = []

        def __init__(self, total: int, **kwargs):
            self.total = total
            self.desc = kwargs.get("desc")
            self.n = 0
            self.updated = 0
            self.closed = False
            _FakeTqdm.instances.append(self)

        def update(self, n: int = 1) -> None:
            self.n += n
            self.updated += n

        def set_postfix(self, **_kwargs) -> None:
            return

        def close(self) -> None:
            self.closed = True

    monkeypatch.setattr(fetch_module, "tqdm", _FakeTqdm)
    monkeypatch.setattr(fetch_module, "download_key_retry", lambda *_args, **_kwargs: "downloaded")

    summary = fetch_module.fetch_futures(
        out_root,
        symbols=[symbol],
        datatype=[dataset],
        start_date="2024-01-01",
        end_date="2024-01-02",
        sleep_s=0.0,
        show_progress=True,
    )

    assert len(_FakeTqdm.instances) == 2
    listing_pbar = _FakeTqdm.instances[0]
    download_pbar = _FakeTqdm.instances[1]
    assert listing_pbar.desc == "Planning remote listings"
    assert listing_pbar.total == 1
    assert listing_pbar.updated == 1
    assert listing_pbar.closed
    assert download_pbar.desc == "Downloading KuCoin futures"
    assert download_pbar.total == 2
    assert download_pbar.updated == 2
    assert download_pbar.closed
    assert summary["planned"] == 2
    assert summary["downloaded"] == 2


def test_fetch_futures_listing_progress_uses_exact_shard_count_in_dry_run(
    monkeypatch, tmp_path: Path
) -> None:
    out_root = tmp_path / "downloads"
    attempted_downloads: list[str] = []

    def _fake_list_month_keys_futures(**_kwargs):
        return []

    def _fake_download_key_retry(key: str, *_args, **_kwargs) -> str:
        attempted_downloads.append(key)
        return "downloaded"

    class _FakeTqdm:
        instances: list["_FakeTqdm"] = []

        def __init__(self, total: int, **kwargs):
            self.total = total
            self.desc = kwargs.get("desc")
            self.n = 0
            self.updated = 0
            self.closed = False
            _FakeTqdm.instances.append(self)

        def update(self, n: int = 1) -> None:
            self.n += n
            self.updated += n

        def set_postfix(self, **_kwargs) -> None:
            return

        def close(self) -> None:
            self.closed = True

    monkeypatch.setattr(fetch_module, "list_month_keys_futures", _fake_list_month_keys_futures)
    monkeypatch.setattr(fetch_module, "download_key_retry", _fake_download_key_retry)
    monkeypatch.setattr(fetch_module, "tqdm", _FakeTqdm)

    summary = fetch_module.fetch_futures(
        out_root,
        symbols=["BTCUSDTM", "ETHUSDTM"],
        datatype=["klines", "mark"],
        start_date="2024-01-01",
        end_date="2024-02-03",
        dry_run=True,
        show_progress=True,
        sleep_s=0.0,
    )

    assert len(_FakeTqdm.instances) == 1
    listing_pbar = _FakeTqdm.instances[0]
    assert listing_pbar.desc == "Planning remote listings"
    assert listing_pbar.total == 8
    assert listing_pbar.updated == 8
    assert listing_pbar.closed
    assert attempted_downloads == []
    assert summary["planned"] == 0


def test_fetch_futures_listing_failure_still_advances_listing_progress(
    monkeypatch, tmp_path: Path
) -> None:
    out_root = tmp_path / "downloads"

    def _fake_list_month_keys_futures(*, month: int, **_kwargs):
        if month == 1:
            raise RuntimeError("month listing failure")
        return []

    class _FakeTqdm:
        instances: list["_FakeTqdm"] = []

        def __init__(self, total: int, **kwargs):
            self.total = total
            self.desc = kwargs.get("desc")
            self.n = 0
            self.updated = 0
            self.closed = False
            _FakeTqdm.instances.append(self)

        def update(self, n: int = 1) -> None:
            self.n += n
            self.updated += n

        def set_postfix(self, **_kwargs) -> None:
            return

        def close(self) -> None:
            self.closed = True

    monkeypatch.setattr(fetch_module, "list_month_keys_futures", _fake_list_month_keys_futures)
    monkeypatch.setattr(fetch_module, "tqdm", _FakeTqdm)

    summary = fetch_module.fetch_futures(
        out_root,
        symbols=["BTCUSDTM"],
        datatype=["klines"],
        start_date="2024-01-01",
        end_date="2024-02-02",
        dry_run=True,
        show_progress=True,
        sleep_s=0.0,
    )

    assert len(_FakeTqdm.instances) == 1
    listing_pbar = _FakeTqdm.instances[0]
    assert listing_pbar.desc == "Planning remote listings"
    assert listing_pbar.total == 2
    assert listing_pbar.updated == 2
    assert listing_pbar.closed
    assert summary["errors"] == 1
