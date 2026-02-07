# KuCoin Lake — Metadata Contracts

This document defines the **current DuckDB metadata schema semantics** implemented in `kucoin_lake.metadata`.

The focus is **purpose clarity** and **grain** rather than exhaustive schemas. If a detail here conflicts with code, the code wins.

--------------------------------------------------------------------
0. GLOBAL PRINCIPLES
--------------------------------------------------------------------

P0.1 Timeframe safety
- Any table whose outputs differ by timeframe must include timeframe in its primary key.
- Deletes/recomputes are scoped by (market, timeframe).

P0.2 Idempotence
- Re-running with unchanged inputs yields stable values (except timestamps).

P0.3 Incremental correctness
- Incremental rebuilds must update only impacted partitions (or a minimal safe superset).
- Rolling metrics include lookback pre-history.

P0.4 Funding handling
- Funding rows use `timeframe=''` in coverage.
- Funding participates only in logical 1m builds.
- Rollups normalize funding to `timeframe='1m'` when `timeframe_filter == '1m'`.

P0.5 Partition semantics
- Raw 1m uses `date=` partitions.
- Derived 1d uses `month=` partitions.
- Daily keys for derived data are computed from timestamps (not hive date partitions).

P0.6 Incremental scan model
- Incremental updates are **file-scan driven** via `iter_data_parquets(...)`.
- The manifest is internal and derived from filesystem scans.
- There is no external manifest-driven refresh in production (backlog only).

--------------------------------------------------------------------
1. BUILD ENTRYPOINTS
--------------------------------------------------------------------

Primary workflow:
- `build_or_update_metadata(...)` (via `kucoin_lake.api.build_metadata` and CLI `build-metadata`)

Supporting workflows:
- `build_or_update_kline_integrity_day(...)` (via CLI `build-kline-integrity`)
- `backfill_derived_from_coverage(...)` (via CLI `backfill-derived`)

--------------------------------------------------------------------
2. md_file_manifest
--------------------------------------------------------------------

**Purpose**: File-level index of Parquet files in the lake.

**Grain**: one row per physical Parquet file (keyed by `file_rel`).

**Notes**:
- Used to detect new/changed files via size/mtime.
- The manifest does not store partitions; those are derived from file paths.

--------------------------------------------------------------------
3. md_partition_coverage
--------------------------------------------------------------------

**Purpose**: What data exists (by symbol/day/dataset/timeframe) and how complete it is.

**Grain**: `(market, dataset, symbol, date, timeframe)`.

**Notes**:
- For timeframed datasets, coverage is filtered by `timeframe_filter`.
- For funding, `timeframe=''` and only included when `timeframe_filter == '1m'`.
- Completeness uses expected rows for 1m; derived timeframes compute daily keys from timestamps.

--------------------------------------------------------------------
4. md_symbol_dataset_stats
--------------------------------------------------------------------

**Purpose**: Per-symbol/dataset/timeframe stats for sanity checks and range summaries.

**Grain**: `(market, dataset, symbol, timeframe)`.

**Notes**:
- Built from `md_partition_coverage`.
- When `timeframe_filter == '1m'`, funding is normalized to `timeframe='1m'` for stats.

--------------------------------------------------------------------
5. md_alignment_summary
--------------------------------------------------------------------

**Purpose**: Cross-dataset alignment at the day level (klines/mark/index/funding).

**Grain**: `(market, timeframe, symbol, date)`.

**Notes**:
- Built from `md_partition_coverage`.
- Funding only included when `timeframe_filter == '1m'`.

--------------------------------------------------------------------
6. md_liquidity_daily
--------------------------------------------------------------------

**Purpose**: Daily liquidity metrics for universe construction (dollar volume, ranks).

**Grain**: `(market, timeframe, symbol, date)`.

**Notes**:
- Derived from klines only.
- Rolling medians and ranks include lookback pre-history.
- Only computed for `timeframe_filter == '1m'` in current code.

--------------------------------------------------------------------
7. md_kline_integrity_day
--------------------------------------------------------------------

**Purpose**: Detect silent data issues at the symbol-day level (gaps, duplicates, price anomalies).

**Grain**: `(market, symbol, date, timeframe)`.

**Notes**:
- Implemented for `timeframe_filter == '1m'` only.
- Incremental paths use changed files; full rebuilds can use date ranges.

--------------------------------------------------------------------
8. REQUIRED BEHAVIOR OF METADATA BUILDERS
--------------------------------------------------------------------

- **First run**: bulk coverage build for the selected timeframe (unless scoped by symbols/date).
- **Incremental**: update only changed or missing partitions based on file diffs.
- **Liquidity**: recompute only affected days plus rolling lookback window.
- **Integrity**: recompute only changed or missing klines days (1m only).
- **Funding**: present only in 1m logic; never touched by non-1m runs.

--------------------------------------------------------------------
9. VALIDATION CHECKLIST
--------------------------------------------------------------------

After any build:
- No cross-timeframe row changes (1d build must not touch 1m rows)
- Funding policy honored (only 1m builds include funding)
- Rolling windows include lookback pre-history
- Derived data handled via timestamp-derived dates
- Idempotence: re-run with no changes yields same counts and values
