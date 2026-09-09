# KuCoin Lake

Python, Parquet, and DuckDB tooling for reproducible KuCoin futures ingestion, data-quality validation, daily aggregation, and point-in-time liquidity universes.

This repository is the data foundation I built for independent cross-sectional crypto research. It converts KuCoin bulk archives into a partitioned research lake, maintains rebuildable metadata, validates stored daily bars against minute inputs, and constructs a D-1 liquidity universe with entry/exit hysteresis.

It is a single-machine research data system, not a trading bot, backtester, or live execution platform.

## Pipeline

```text
KuCoin archives
    -> resumable ZIP downloads
    -> normalized 1m Parquet partitions
    -> monthly 1d Parquet partitions
    -> DuckDB coverage, integrity, and liquidity metadata
    -> validation artifacts
    -> D-1 research universe and return panel
```

The lake is the source data; DuckDB metadata and research outputs can be rebuilt from it.

## What it demonstrates

- Month-sharded remote discovery, retries, partial-download handling, and resumable fetches.
- Atomic local staging and final lake writes for NAS-friendly ingestion.
- Hive-style partitions with daily raw data and monthly derived data.
- Incremental metadata based on file size and modification-time fingerprints.
- UTC session handling, timeframe isolation, idempotence, and gap/duplicate checks.
- Rebuild-based metadata validation and independent 1m-to-1d aggregate comparisons.
- Point-in-time universe membership using D-1 liquidity ranks and entry/exit hysteresis.
- Exact calendar-day research returns that do not bridge missing bars or membership gaps.

## Installation

Python 3.10 or newer is required.

```bash
python -m venv .venv
source .venv/bin/activate
python -m pip install -e ".[dev]"
python -m pytest
```

Display the available commands with:

```bash
kucoin-lake --help
```

## Representative usage

Fetch a small date range:

```bash
kucoin-lake fetch-futures \
  --out-root /path/to/downloads \
  --symbols BTCUSDTM ETHUSDTM \
  --datatype klines funding \
  --start-date 2025-01-01 \
  --end-date 2025-01-07
```

Ingest local archives using explicit download, lake, and local-staging paths:

```bash
kucoin-lake ingest-local-downloads-to-lake \
  --startdate 2025-01-01 \
  --enddate 2025-01-07 \
  --local-root /path/to/downloads \
  --nas-root /path/to/lake \
  --local-staging-dir /path/to/local-stage
```

Build and validate metadata:

```bash
kucoin-lake build-metadata \
  --nas-root /path/to/lake \
  --market futures \
  --timeframe-filter 1m

kucoin-lake validate all \
  --nas-root /path/to/lake \
  --meta-db-path /path/to/metadata.duckdb \
  --profile smoke
```

The package also exposes notebook-friendly entry points:

```python
from kucoin_lake import build_metadata, validate

result = build_metadata(
    "/path/to/lake",
    market="futures",
    timeframe_filter="1m",
)

validation = validate(
    check="all",
    nas_root="/path/to/lake",
    meta_db_path="/path/to/metadata.duckdb",
    profile="smoke",
)
```

## Repository map

- `kucoin_lake/fetch.py`: archive discovery and download.
- `kucoin_lake/ingest.py`: ZIP normalization and atomic Parquet ingestion.
- `kucoin_lake/resample.py`: 1m-to-1d aggregation.
- `kucoin_lake/metadata.py`: manifest, coverage, integrity, and liquidity metadata.
- `kucoin_lake/validation/`: metadata and derived-data validators.
- `kucoin_lake/research/`: D-1 universe and base-panel construction.
- `tests/`: regression tests and a 192 KB recorded KuCoin market-data fixture.
- `docs/`: data contracts, CLI reference, and operational runbooks.

## Research conventions and limitations

- The current implementation supports KuCoin futures, not spot data.
- `dollar_volume` is `sum(close * volume)`. It is a consistent liquidity proxy, but the project does not verify contract multipliers or claim that it is exact USD notional.
- `dv_30d_median` uses up to the latest 30 available daily observations. It is not a strict calendar-day window and has no minimum-history eligibility rule.
- Universe membership for day D uses ranks available as of D-1. Hysteresis reduces turnover but does not impose a fixed universe size.
- Historical symbols present in the lake remain available to the universe builder, but the project has no exchange listing/delisting registry and cannot prove complete freedom from survivorship bias.
- Validation checks reproducibility and internal contracts. It is not independent certification of the source exchange data.
- Deleted Parquet files are not yet reconciled out of metadata automatically.
- The system targets local/NAS research workflows; it has no distributed execution, service monitoring, or production SLA.

The committed fixture files are small recorded market-data samples used to exercise the real partition and schema contracts. Generated synthetic data is used in tests that require specific edge cases.

## Documentation

Start with [the documentation index](docs/README.md), then see:

- [Architecture](docs/architecture.md)
- [Lake layout](docs/data_lake_layout.md)
- [Metadata contracts](docs/metadata_contracts.md)
- [Validation](docs/validation.md)
- [CLI reference](docs/cli.md)
- [Research universe](docs/research/universe.md)
- [Research panel](docs/research/panel.md)

## License

MIT
