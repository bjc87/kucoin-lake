# KuCoin Lake — Documentation Self-Review

Date: 2026-02-07

This report audits the rewritten docs against the acceptance checklist using the required source-of-truth hierarchy.

--------------------------------------------------------------------
## 1) Structural sanity — PASS

Evidence:
- Start-here entry point: `docs/README.md`
> Start here: this file is the entry point for new developers and agents.

- System purpose: `docs/architecture.md`
> `kucoin_lake` is a **research-grade crypto futures data lake** optimized for **daily cross-sectional factor research on 1d bars**.

- Run sequence easy to find: `docs/README.md`
> **Canonical Jupyter Workflow (recommended)**
> fetch_futures()
> ingest_local_downloads_to_lake()
> resample_1m_to_1d()
> build_metadata()

--------------------------------------------------------------------
## 2) Workflow correctness — PASS

Evidence:
- Unified flow statement: `docs/runbooks.md`
> All run types use the **same pipeline steps**. Only scope changes.

- Concrete scope examples: `docs/runbooks.md`
> **Backfill**: bounded symbol/date/dataset/timeframe window
> Run `build_metadata` with a matching scope:
> `--symbols` and `--date-start/--date-end`
> `--timeframe-filter` set to the target timeframe

--------------------------------------------------------------------
## 3) Canonical Jupyter API golden path — PASS

Evidence:
- `docs/README.md`
> fetch_futures()
> ingest_local_downloads_to_lake()
> resample_1m_to_1d()
> build_metadata()

--------------------------------------------------------------------
## 4) CLI accuracy gate — PASS (with TODO)

Evidence:
- CLI entrypoint from `pyproject.toml`
> [project.scripts]
> kucoin-lake = "kucoin_lake.cli:main"

- CLI docs derived from source: `docs/cli.md`
> The commands and options below are derived directly from `kucoin_lake/cli.py`.

- End-to-end CLI example: `docs/cli.md`
> kucoin-lake fetch-futures ...
> kucoin-lake ingest-local-downloads-to-lake ...
> kucoin-lake build-metadata ...

TODO:
- Unable to capture actual `kucoin-lake --help` output because `duckdb` is not installed in this environment. When dependencies are available, run `python -m kucoin_lake.cli --help` and confirm the CLI doc matches.

--------------------------------------------------------------------
## 5) Incremental processing semantics — PASS

Evidence:
- File-scan driven incremental: `docs/architecture.md`
> Incremental updates are **file-scan driven**:
> `iter_data_parquets(...)` scans the lake directly (filesystem) with optional **symbol** and **date** scope.

- Manifest-driven refresh backlog only: `docs/architecture.md`
> There is **no manifest-driven refresh** architecture in production. Any such design is backlog only.

- Confirm no manifest-driven incremental claim remains:
  Searched `docs/` for `manifest-driven` and only found backlog statements in `docs/architecture.md` and `docs/metadata_contracts.md` (plus this review file).

--------------------------------------------------------------------
## 6) Data truth hierarchy — PASS

Evidence:
- `docs/README.md`
> 1. NAS Parquet lake = canonical truth
> 2. DuckDB metadata = rebuildable derivative
> 3. Research datasets = ephemeral

--------------------------------------------------------------------
## 7) Partition semantics correctness — PASS

Evidence:
- Day-grained partitions (`date=`): `docs/data_lake_layout.md`
> futures/{dataset}/timeframe={tf}/symbol={symbol}/date={YYYY-MM-DD}/data.parquet

- Derived 1d uses `month=`: `docs/data_lake_layout.md`
> futures/{dataset}/timeframe=1d/symbol={symbol}/month={YYYY-MM}/data.parquet

- Fixture grounding reference: `docs/data_lake_layout.md`
> `tests/fixtures/lake/futures`

--------------------------------------------------------------------
## 8) Metadata contracts clarity — PASS

Evidence (purpose statements for each table):
- `md_file_manifest`: `docs/metadata_contracts.md`
> **Purpose**: File-level index of Parquet files in the lake.

- `md_partition_coverage`: `docs/metadata_contracts.md`
> **Purpose**: What data exists (by symbol/day/dataset/timeframe) and how complete it is.

- `md_symbol_dataset_stats`: `docs/metadata_contracts.md`
> **Purpose**: Per-symbol/dataset/timeframe stats for sanity checks and range summaries.

- `md_alignment_summary`: `docs/metadata_contracts.md`
> **Purpose**: Cross-dataset alignment at the day level (klines/mark/index/funding).

- `md_liquidity_daily`: `docs/metadata_contracts.md`
> **Purpose**: Daily liquidity metrics for universe construction (dollar volume, ranks).

- `md_kline_integrity_day`: `docs/metadata_contracts.md`
> **Purpose**: Detect silent data issues at the symbol-day level (gaps, duplicates, price anomalies).

--------------------------------------------------------------------
## 9) Fixtures integration — PASS

Evidence:
- `docs/README.md`
> `tests/fixtures/lake/futures` is the canonical miniature lake layout used by tests and examples.

- `docs/architecture.md`
> `tests/fixtures/lake/futures` is the canonical miniature lake example.

- `docs/data_lake_layout.md`
> **Fixture reference** (canonical miniature example): `tests/fixtures/lake/futures`

--------------------------------------------------------------------
## 10) Research enablement test — PASS

Evidence:
- Run sequence quickly: `docs/README.md` and `docs/runbooks.md` (see item 1 and 3 evidence).

- Universe construction entrypoint: `docs/architecture.md`
> Universe construction is **not implemented as code in this package**. It is a research workflow built on metadata tables:
> Start from `md_liquidity_daily` ...
> Use `md_alignment_summary` ...
> Use `md_kline_integrity_day` ...
> Use `md_partition_coverage` ...

- Liquidity ranks provenance: `docs/metadata_contracts.md`
> **Purpose**: Daily liquidity metrics for universe construction (dollar volume, ranks).

- Coverage/alignment meaning: `docs/metadata_contracts.md`
> **Purpose**: What data exists (by symbol/day/dataset/timeframe) and how complete it is.
> **Purpose**: Cross-dataset alignment at the day level (klines/mark/index/funding).

- Ingestion + metadata steps: `docs/runbooks.md` and `docs/cli.md` (pipeline steps and CLI example).

--------------------------------------------------------------------
## 11) Hallucination scan — PASS (with TODO)

Evidence:
- Searched for archive-era commands/modules:
  - `grep -R "archive/" -n docs` found only a negation in `docs/architecture.md`.
- CLI command list aligns to `kucoin_lake/cli.py` via source inspection.

TODO:
- Once CLI help is runnable, re-check that help output matches `docs/cli.md` to rule out drift.

--------------------------------------------------------------------
## Automated Checks

Validation script found (`tools/validate_docs.py` in repo).
