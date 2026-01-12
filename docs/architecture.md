# KuCoin Lake — Architecture

This repository implements a canonical, reusable data ingestion + metadata + integrity + universe engineering system for a KuCoin USDT perpetual futures Parquet data lake (and later spot). The system is designed for **research correctness and reproducibility first**, with **idempotent operations**, **incremental updates**, and **multi-timeframe safety** as hard requirements.

---

## Goals

### Primary goals
- Maintain a canonical Parquet data lake on a NAS using **hive-style partitions**.
- Maintain a DuckDB-backed metadata database (e.g. `metadata_futures.duckdb`) that supports:
  - Incremental, resumable updates
  - Reliable detection of new/changed files
  - Multi-timeframe metadata without destructive cross-timeframe side effects
  - Alignment/coverage/stats rollups scoped by `(market, timeframe)` where relevant
- Produce “research-ready” metadata and integrity signals to support:
  - Stable universe selection
  - Correct backtests (no silent corruption)
  - Repeatable resampling and downstream feature generation

### Non-goals (for now)
- Micro-optimizations of DuckDB queries beyond basic correctness and reasonable performance
- Full orchestration (Airflow/Prefect) — this is runnable via CLI/scripts
- Multi-machine concurrency guarantees (assume single-writer patterns; DuckDB file locks avoided by process discipline)

---

## System Overview

### Components
1. **Lake I/O and file discovery**
   - Enumerates Parquet files from the data lake according to canonical partition schemes.
   - Scopes iteration by `market`, dataset and `timeframe_filter` at the source (filesystem iterator).
   - Produces canonical file “candidates” for manifest diffing.

2. **Manifest + file change detection**
   - Maintains `md_file_manifest` capturing file path + partition attributes + fingerprints.
   - Computes the delta (“new or changed files”) from the manifest vs current filesystem state.
   - Provides the list of changed file paths (and their parsed partition metadata) to downstream metadata builders.

3. **Metadata builders**
   - Build/refresh a set of timeframed and non-timeframed metadata tables:
     - Partition coverage (what exists per symbol/dataset/time bucket)
     - Per-symbol dataset stats
     - Alignment summaries
     - Liquidity daily metrics
   - Builders must be **idempotent**, must support incremental updates, and must be **timeframe-safe**.

4. **Integrity hooks**
   - Daily gap/dup checks and other lightweight validation that detects silent corruption early.
   - Stores results in dedicated integrity tables (events/results), rather than raising hard failures by default.

5. **Universe engineering**
   - Uses liquidity and integrity metadata to build stable tradable universes.
   - Implements hysteresis-style membership rules to reduce symbol churn.

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
- Derived 1d OHLCV is validated vs 1m aggregation (OHLCV matches exactly).
- Month partitioning is chosen for manageable file counts and efficient scans.

### Datasets
- Timeframed: `klines`, `mark`, `index`
- Non-timeframed: `funding`

---

## Metadata DB and “Grain”

Metadata is stored in DuckDB, under schema `md`.

### Intended tables (final schema)
- `md_file_manifest`
- `md_partition_coverage`
- `md_symbol_dataset_stats`
- `md_alignment_summary`
- `md_liquidity_daily`
- (recommended) `md_kline_integrity_day`
- (recommended) `md_meta_runs`

### Grain definitions
- **File manifest**: one row per physical Parquet file (by path).
- **Partition coverage**: one row per `(market, timeframe, dataset, symbol, day)` for daily-partitioned data, and an analogous representation for month-partitioned data (see “Partition semantics” below).
- **Symbol dataset stats**: one row per `(market, timeframe, dataset, symbol)` (or per table definition).
- **Alignment summary**: one row per `(market, timeframe, date)` or per `(market, timeframe, date, dataset)` depending on design.
- **Liquidity daily**: one row per `(market, timeframe, symbol, date)`.

---

## Core Invariants (Non-Negotiable)

These invariants must hold across futures/spot and across timeframes (1m, 1h, 1d, …).

### Invariant 1 — Destructive operations are always scoped
Any `DELETE` / recompute action must be scoped at minimum by:
- `market`
- `timeframe` (for timeframed tables)

**Never** allow “global deletes” that wipe multiple timeframes (unless explicitly intended and extremely rare).

Preferred pattern:
- delete only the rows for the impacted `(market, timeframe)` and ideally only impacted partitions.

### Invariant 2 — Timeframe is in every relevant primary key
If a table’s results differ by timeframe, then timeframe must be part of:
- primary key (or unique constraint)
- all join keys
- all GROUP BY keys where aggregation occurs

### Invariant 3 — Timeframe scoping happens at the source of truth
Timeframe scoping must be applied:
- during filesystem iteration (candidate files)
- and/or during manifest change detection queries

If scoping is only applied “after the fact” (Python filtering of changed files), leakage bugs are more likely.

### Invariant 4 — Funding handling is explicit and consistent
Funding is non-timeframed in storage (`timeframe = ''` in metadata), but participates in the **logical 1m build** only.

Rules:
- Funding is included only when `timeframe_filter == '1m'`.
- 1d/1h builds must never delete, recompute, or incorporate funding unless explicitly designed.
- Funding may be *normalized logically* to 1m in rollups/alignment queries via an “effective timeframe” expression; it should not be duplicated physically into `timeframe='1m'` rows unless there is a deliberate design reason.

### Invariant 5 — Idempotence
Running the same build twice with no changes:
- makes no effective metadata changes
- produces stable row counts and stable computed values (except for “computed_at” timestamps)

---

## Execution Model

### Inputs
- `nas_root` (path to the lake root)
- `market` (e.g., `futures`, later `spot`)
- `datasets` (subset or full dataset list)
- `timeframe_filter` (e.g. `1m`, `1d`, later `1h`)
- completeness thresholds, liquidity thresholds, etc.

### Build order (recommended)
1. **Discover candidate Parquet files** using `iter_data_parquets(...)` scoped by timeframe.
2. **Update manifest**:
   - upsert file fingerprints
   - detect new/changed file paths
3. **Coverage refresh**:
   - first run per timeframe: recompute coverage for that timeframe only
   - incremental: update only impacted partitions
4. **Symbol dataset stats**:
   - update only impacted `(symbol, dataset, timeframe)` sets
5. **Alignment summary**:
   - recompute only for impacted date range/timeframe
6. **Liquidity daily**:
   - compute daily dollar volume metrics
   - recompute rolling median + ranks over a range with sufficient lookback
7. **Integrity hooks** (recommended):
   - gap/dup checks for changed symbol-days
   - schema drift checks
8. **Universe updates** (recommended):
   - daily membership with hysteresis
   - exclude integrity-failed symbol-days

---

## Partition Semantics (Date vs Month)

The lake uses:
- `date=YYYY-MM-DD` partitions for raw 1m (and other raw timeframes if chosen)
- `month=YYYY-MM` partitions for derived 1d

Metadata must **not assume** that all data is day-partitioned.

Recommended approach:
- manifest stores partition kind/value (e.g., `partition_key` in {`date`,`month`} and `partition_value`)
- coverage and stats logic must derive:
  - `period_start`
  - `period_end`
  - `period_grain` (day|month)
from the partition metadata

If the initial implementation stores coverage daily for 1d derived bars, it must compute the day key from the timestamp column (e.g., `CAST(ts AS DATE)`), not from a hive partition column that may not exist.

---

## Incremental Updates: Correctness Requirements

### Manifest-based change detection
- For each Parquet file, track a stable fingerprint:
  - minimum: `(size_bytes, mtime)` or `(size_bytes, mtime_ns)`
  - optional: strong hash for higher assurance
- `get_new_or_changed_files(...)` returns only files relevant to the invoked `(market, timeframe_filter)` build.

### Downstream recompute scoping
For a changed file with `(market, dataset, timeframe, symbol, partition)`:
- coverage should update for that partition only (or for the smallest safe superset)
- symbol stats update for that symbol/dataset/timeframe
- alignment and liquidity update for the impacted date range, plus necessary lookback for rolling windows

### Rolling windows (liquidity)
Rolling metrics must include enough pre-history so results for `date_from` are correct.
For a 30-day rolling median:
- include `date_from - 29 days` through `date_to` in the window input
- update only `date_from .. date_to` outputs

---

## Module Responsibilities

### `paths.py`
- Canonical path building helpers for lake locations.
- Dataset glob construction (single canonical helper).
- Safety guards for filesystem operations:
  - explicit `nas_root`
  - refuse risky roots (e.g. `/`, `~`, non-whitelisted prefixes)
  - helper to resolve and validate paths

### `manifest.py`
- Schema and operations for `md_file_manifest`.
- Parse path → partition attributes (market, dataset, timeframe, symbol, date/month).
- Upsert manifest rows.
- Diff current filesystem state vs manifest to return changed/new file list.

### `metadata.py`
- Orchestrates metadata builds by timeframe:
  - coverage
  - stats
  - alignment
  - liquidity
- Must guarantee timeframe scoping on deletes and recomputes.
- Encodes funding inclusion policy for 1m only.

### `resample.py`
- Set-based resampling from 1m to higher bars (e.g. 1d, later 1h).
- Writes derived Parquet files using hive partitions.
- Validates resample correctness (OHLCV matches aggregation constraints).

### `integrity.py`
- Computes lightweight integrity checks:
  - gap detection (missing minutes)
  - duplicates
  - basic schema presence and type expectations
- Writes results to tables like `md_kline_integrity_day`.
- Designed to be incremental: process changed symbol-days only.

### `universe.py`
- Universe selection and stability:
  - liquidity ranks
  - hysteresis rules (enter/exit thresholds)
  - integrity filtering
- Produces daily membership table(s) and/or snapshots.

### `cli.py`
- Thin command wrappers over package entrypoints.
- Commands are explicit and safe:
  - require `--nas-root`
  - require `--market`
  - require `--timeframe`
  - destructive ops require `--yes`
- Never defaults to scanning arbitrary filesystem locations.

---

## Funding Policy (Detailed)

Funding is stored as non-timeframed dataset metadata with `timeframe = ''`.

### Inclusion rules
- Funding participates only when building `timeframe_filter == '1m'`.
- Funding does not participate in 1d/1h builds unless explicitly added later.

### Normalization for alignment/rollups
When a downstream table needs to align funding with other 1m datasets, it must do so logically:
- define an “effective timeframe”:
  - `tf_eff = CASE WHEN dataset='funding' THEN '1m' ELSE timeframe END`
- join/aggregate on `tf_eff` where appropriate

### Non-destructive behavior
- 1d build deletes and recomputes must never touch rows with `timeframe=''`.
- coverage/stats for funding should be updated only during 1m runs.

---

## Safety and Operational Assumptions

- Single-user / single-writer discipline to avoid DuckDB file locking issues on NAS.
- NAS mounted to host; agent tooling (Codex) may operate on repo only.
- Execution is primarily local; CI uses synthetic fixtures (no NAS).
- Research correctness is prioritized over throughput.

---

## “Research-Ready” Additions (Recommended)

### `md_kline_integrity_day`
Purpose:
- Detect silent data corruption at the daily slice granularity (symbol-day):
  - missing minutes (gaps)
  - duplicate minutes
  - unexpected timestamp ranges

Use:
- Exclude bad symbol-days from research
- Monitor ingestion regressions over time

### `md_meta_runs`
Purpose:
- Append-only audit log for every run:
  - parameters (market, timeframe, datasets)
  - counts (scanned files, changed files, updated partitions)
  - status (started/finished/failed)
  - code version (git SHA) if available

Use:
- Debug “why did my stats change?”
- Reproduce research outputs
- Detect recurring failure patterns

---

## Development Approach

### Keep legacy scripts
- Place the current working scripts under `archive/` (or `legacy/`) and treat them as read-only reference.

### Refactor in phases
1. Move and wrap: create package skeleton; keep behavior stable.
2. Port modules one by one with tests.
3. Add integrity + run audit hooks once core invariants are pinned by tests.

### Test tiers
- Unit: fast invariant tests, no NAS.
- Integration: local fixture lake mimicking hive partitions.
- Reality checks: manual pilot runs on NAS with narrow filters.

---
