# KuCoin Lake — Architecture

This repository implements a canonical, reusable data ingestion + metadata + resample pipeline for a KuCoin USDT perpetual futures Parquet data lake. The system is designed for **research correctness and reproducibility first**, with **idempotent operations**, **incremental updates**, and **multi-timeframe safety** as hard requirements.

> Current state note
> - The runnable pipeline lives under `archive/` (legacy-but-active scripts).
> - The `kucoin-lake/` package is currently an empty placeholder for a future refactor.

---

## Goals

### Primary goals
- Maintain a canonical Parquet data lake on a NAS using **hive-style partitions**.
- Maintain a DuckDB-backed metadata database (e.g. `metadata_futures.duckdb`) that supports:
  - Incremental, resumable updates
  - Reliable detection of new/changed files
  - Multi-timeframe metadata without destructive cross-timeframe side effects
  - Alignment/coverage/stats rollups scoped by `(market, timeframe)`
- Produce “research-ready” metadata and integrity signals to support:
  - Stable universe selection (future)
  - Correct backtests (no silent corruption)
  - Repeatable resampling and downstream feature generation

### Non-goals (for now)
- Micro-optimizations of DuckDB queries beyond basic correctness and reasonable performance
- Full orchestration (Airflow/Prefect) — this is runnable via Python scripts / notebooks
- Multi-machine concurrency guarantees (assume single-writer patterns; DuckDB file locks avoided by process discipline)

---

## System Overview

### Components (current code)
1. **Download (raw ZIP acquisition)** — `archive/fetch_data.py`
   - Builds KuCoin historical-data keys and downloads ZIPs via HTTP.
   - Stores downloads under `out_root/<bucket-key>`, where the key is `data/futures/daily/...`.

2. **Ingestion (ZIP → Parquet)** — `archive/nas_parquet_mirror.py`
   - Converts ZIP CSVs to canonical Parquet files in the NAS lake.
   - Uses local staging + atomic copy to avoid partial writes on NAS.
   - Skips in-flight ZIPs (recently modified or `.part/.tmp/.download`).

3. **Manifest + metadata builders** — `archive/metadata.py`
   - Scans Parquet files via filesystem globbing.
   - Stores file-level manifest rows (path + size/mtime) in DuckDB.
   - Builds/updates:
     - partition coverage (`md_partition_coverage`)
     - symbol dataset stats (`md_symbol_dataset_stats`)
     - alignment summary (`md_alignment_summary`)
     - liquidity daily (`md_liquidity_daily`)

4. **Resampling (1m → 1d)** — `archive/resample.py`
   - Set-based DuckDB aggregation from 1m to derived 1d.
   - Writes month-partitioned Parquet files to the same dataset namespace.

### Not implemented in code yet
- Dedicated integrity builders (only table DDL exists).
- Universe engineering.
- CLI entrypoints (scripts are currently Jupyter-first or direct Python APIs).

---

## Canonical Data Lake Layout

### Raw canonical lake (futures)
Root:
- `/Volumes/quant_data/kucoin/futures/`

Path template (timeframed datasets):
- `{root}/{dataset}/timeframe={tf}/symbol={symbol}/date={YYYY-MM-DD}/data.parquet`

Examples:
- `/Volumes/quant_data/kucoin/futures/klines/timeframe=1m/symbol=BTCUSDTM/date=2026-01-07/data.parquet`
- `/Volumes/quant_data/kucoin/futures/mark/timeframe=1m/symbol=BTCUSDTM/date=2026-01-07/data.parquet`
- `/Volumes/quant_data/kucoin/futures/index/timeframe=1m/symbol=BTCUSDTM/date=2026-01-07/data.parquet`

### Derived lake (resampled bars)
Derived bars (currently 1d from 1m) are stored alongside datasets as separate timeframe partitions.

Path template (derived timeframed datasets, month-partitioned):
- `{root}/{dataset}/timeframe=1d/symbol={symbol}/month={YYYY-MM}/data.parquet`

Example:
- `/Volumes/quant_data/kucoin/futures/klines/timeframe=1d/symbol=BTCUSDTM/month=2026-01/data.parquet`

**Notes**
- Derived 1d bars are produced by set-based DuckDB aggregation from 1m.
- Output schemas for derived 1d include summary fields (`src_rows`, `min_ts`, `max_ts`, `dollar_volume`, `vwap`).
- Month partitioning is chosen for manageable file counts and efficient scans.

### Datasets
- Timeframed: `klines`, `mark`, `index`
- Non-timeframed: `funding`

---

## Metadata DB and “Grain” (current schema)

Metadata is stored in DuckDB, under schema `md`.

### Implemented tables
- `md_file_manifest`
- `md_partition_coverage`
- `md_symbol_dataset_stats`
- `md_alignment_summary`
- `md_liquidity_daily`
- `md_kline_integrity_day` (DDL only; no builder yet)

### Grain definitions (as implemented)
- **File manifest**: one row per physical Parquet file (keyed by `file_rel`, the path relative to `nas_root`).
- **Partition coverage**: one row per `(market, dataset, symbol, date, timeframe)`.
- **Symbol dataset stats**: one row per `(market, dataset, symbol, timeframe)`.
- **Alignment summary**: one row per `(market, timeframe, symbol, date)`.
- **Liquidity daily**: one row per `(market, timeframe, symbol, date)`.

---

## Core Invariants (Non-Negotiable)

These invariants must hold across futures/spot and across timeframes (1m, 1d, …).

### Invariant 1 — Destructive operations are always scoped
Any `DELETE` / recompute action must be scoped at minimum by:
- `market`
- `timeframe` (for timeframed tables)

**Never** allow “global deletes” that wipe multiple timeframes (unless explicitly intended and extremely rare).

### Invariant 2 — Timeframe is in every relevant primary key
If a table’s results differ by timeframe, then timeframe must be part of:
- primary key (or unique constraint)
- all join keys
- all GROUP BY keys where aggregation occurs

### Invariant 3 — Timeframe scoping happens at the source of truth
Timeframe scoping must be applied:
- during filesystem iteration (candidate files)
- and/or during manifest change detection queries

### Invariant 4 — Funding handling is explicit and consistent
Funding is non-timeframed in storage (`timeframe = ''` in `md_partition_coverage`), but participates in the **logical 1m build** only.

Rules:
- Funding is included only when `timeframe_filter == '1m'`.
- 1d/1h builds must never delete, recompute, or incorporate funding unless explicitly designed.
- Rollups and alignment normalize funding logically to timeframe `1m` when `timeframe_filter == '1m'`.
  - This produces `timeframe='1m'` rows in `md_symbol_dataset_stats` and `md_alignment_summary` for funding.

### Invariant 5 — Idempotence
Running the same build twice with no changes:
- makes no effective metadata changes
- produces stable row counts and stable computed values (except for “computed_at” timestamps)

---

## Execution Model (current APIs)

### Inputs
- `nas_root` (path to the lake root)
- `market` (e.g., `futures`, later `spot`)
- `datasets` (subset or full dataset list)
- `timeframe_filter` (e.g. `1m`, `1d`)
- completeness thresholds, liquidity thresholds, etc.

### Build order (recommended)
1. **Download** ZIP files with `fetch_futures(...)`.
2. **Ingest** ZIPs to Parquet with `run_ingest(...)`.
3. **Discover Parquet candidates** using `iter_data_parquets(...)` scoped by timeframe.
4. **Update manifest** (upsert + diff) with `upsert_manifest(...)` and `get_new_or_changed_files(...)`.
5. **Coverage refresh** (bulk or incremental).
6. **Symbol dataset stats** and **alignment** rollups.
7. **Liquidity daily** with rolling median + ranks.

---

## Partition Semantics (Date vs Month)

The lake uses:
- `date=YYYY-MM-DD` partitions for raw 1m
- `month=YYYY-MM` partitions for derived 1d

Metadata must **not assume** that all data is day-partitioned.

Coverage and liquidity for 1d **derive day keys from timestamps**:
- `CAST(ts AS DATE)`

---

## Module Responsibilities (current files)

### `archive/fetch_data.py`
- Download KuCoin historical ZIPs.
- Construct KuCoin S3-style keys and write to local disk under `out_root/`.

### `archive/nas_parquet_mirror.py`
- ZIP → Parquet conversion.
- Atomic copy to NAS with a local staging area.
- Local ZIP listing and sharded date filtering.

### `archive/metadata.py`
- Manifest table DDL and upsert/diff logic.
- Coverage, stats, alignment, liquidity builders.
- Timeframe scoping and funding inclusion policies.

### `archive/resample.py`
- Resample 1m → 1d (klines, mark, index).
- Month-partitioned output with atomic writes.

---

## Funding Policy (Detailed)

Funding is stored as non-timeframed dataset metadata with `timeframe = ''` in `md_partition_coverage`.

### Inclusion rules
- Funding participates only when building `timeframe_filter == '1m'`.
- Funding does not participate in 1d/1h builds unless explicitly added later.

### Normalization for rollups/alignment (current behavior)
When building `md_symbol_dataset_stats` and `md_alignment_summary` for `timeframe_filter == '1m'`, funding is normalized logically:
- `tf_norm = CASE WHEN dataset='funding' THEN '1m' ELSE timeframe END`

This means:
- Funding coverage remains at `timeframe=''` in `md_partition_coverage`.
- Funding rollups/alignment appear under `timeframe='1m'`.

### Non-destructive behavior
- 1d build deletes and recomputes must never touch rows with `timeframe=''`.
- Funding coverage is updated only during 1m runs.

---

## Safety and Operational Assumptions

- Single-user / single-writer discipline to avoid DuckDB file locking issues on NAS.
- NAS mounted to host; agent tooling (Codex) may operate on repo only.
- Execution is primarily local; CI uses synthetic fixtures (no NAS).
- Research correctness is prioritized over throughput.

---
