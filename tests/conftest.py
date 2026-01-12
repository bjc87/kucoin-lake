from __future__ import annotations

import shutil
import sys
from pathlib import Path
from typing import Callable, Iterable

import duckdb
import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
ARCHIVE_PATH = REPO_ROOT / "archive"

if ARCHIVE_PATH.exists():
    sys.path.insert(0, str(ARCHIVE_PATH))

from metadata import build_or_update_metadata  # noqa: E402


@pytest.fixture(scope="session")
def repo_root() -> Path:
    return REPO_ROOT


@pytest.fixture(scope="session")
def fixture_lake_root(repo_root: Path) -> Path:
    return repo_root / "tests" / "fixtures" / "lake"


@pytest.fixture()
def tmp_lake_root(tmp_path: Path, fixture_lake_root: Path) -> Path:
    tmp_lake = tmp_path / "lake"
    shutil.copytree(fixture_lake_root, tmp_lake)
    return tmp_lake


@pytest.fixture()
def tmp_duckdb_path(tmp_path: Path) -> Path:
    return tmp_path / "metadata_test.duckdb"


@pytest.fixture()
def duckdb_conn(tmp_duckdb_path: Path) -> duckdb.DuckDBPyConnection:
    connection = duckdb.connect(tmp_duckdb_path)
    try:
        yield connection
    finally:
        connection.close()


@pytest.fixture()
def run_metadata_build(
    tmp_lake_root: Path, tmp_duckdb_path: Path
) -> Callable[[str, Iterable[str]], dict]:
    def _run(timeframe_filter: str, datasets: Iterable[str]) -> dict:
        return build_or_update_metadata(
            tmp_lake_root,
            market="futures",
            datasets=datasets,
            meta_db_path=tmp_duckdb_path,
            timeframe_filter=timeframe_filter,
        )

    return _run
