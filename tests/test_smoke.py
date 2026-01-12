from __future__ import annotations


def test_smoke_metadata_build_runs(run_metadata_build) -> None:
    run_metadata_build(timeframe_filter="1m", datasets=["klines"])
