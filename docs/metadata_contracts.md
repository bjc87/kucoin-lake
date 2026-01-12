# KuCoin Lake — Metadata Contracts

This document defines the **current DuckDB metadata schema** (tables, grains, primary keys, and semantics) implemented in `archive/metadata.py`. It is the contract between:

- the filesystem lake layout (hive partitions)
- the metadata builders (coverage, stats, alignment, liquidity)
- integrity checks and research consumers

This contract is designed for:
- multi-timeframe safety (1m, 1d, later 1h, etc. can coexist)
- incremental, resumable updates
- non-destructive behavior across timeframes
- research correctness and auditability

--------------------------------------------------------------------
0. GLOBAL PRINCIPLES
--------------------------------------------------------------------

P0.1 Timeframe safety
- Any table whose outputs differ by timeframe MUST include timeframe in its primary key.
- Any DELETE / recompute operation MUST be scoped by (market, timeframe) for timeframed tables.

P0.2 Idempotence
- Running the same build twice with unchanged inputs should yield identical results (except computed_at fields).

P0.3 Incremental correctness
- A changed Parquet file must result in recomputation only for impacted partitions (or the smallest safe superset).
- Rolling-window metrics must include sufficient pre-history to compute correct values at the boundary.

P0.4 Funding handling
- Funding is stored as dataset='funding' with timeframe='' in `md_partition_coverage`.
- Funding is included only for logical 1m builds.
- 1d or 1h builds MUST NOT delete or recompute funding rows.

P0.5 Partition semantics
- Raw 1m uses date=YYYY-MM-DD partitions.
- Derived 1d uses month=YYYY-MM partitions.
- Metadata logic MUST NOT assume that a hive partition column 'date' always exists.
- For derived data, daily keys must be computed from timestamps (`CAST(ts AS DATE)`) when needed.

--------------------------------------------------------------------
1. SCHEMA AND NAMING
--------------------------------------------------------------------

Schema name:
- md

All tables described below live under md.*.

Common column conventions:
- market     : string (e.g. 'futures', later 'spot')
- timeframe  : string (e.g. '1m', '1d') for timeframed tables; '' for funding rows in coverage
- dataset    : string (e.g. 'klines', 'mark', 'index', 'funding')
- symbol     : string (e.g. 'BTCUSDTM')
- computed_at_utc : TIMESTAMP (UTC) indicating when a row was last computed

Where a table stores dates:
- date is stored as DATE (not string) unless explicitly noted.

--------------------------------------------------------------------
2. md_file_manifest
--------------------------------------------------------------------

Purpose:
- Source-of-truth index of physical Parquet files that exist in the lake.
- Enables detection of new/changed files for incremental metadata updates.

Grain:
- One row per physical Parquet file (unique by `file_rel`).

Columns (implemented):
- file_rel          : string (path relative to nas_root)
- market            : string
- dataset           : string
- file_path         : string (absolute path)
- file_size_bytes   : bigint
- mtime_ns          : bigint
- first_seen_utc    : TIMESTAMP
- last_seen_utc     : TIMESTAMP

Primary key:
- file_rel

Semantics:
- A file is considered “changed” if size/mtime differs from the manifest.
- The manifest does **not** store partition attributes (timeframe, symbol, date). That parsing is derived downstream.

--------------------------------------------------------------------
3. md_partition_coverage
--------------------------------------------------------------------

Purpose:
- Summarize which symbol/dataset partitions exist and how complete they are.
- Acts as the first-layer “what data do I have?” table used by alignment and stats.

Grain:
- One row per (market, dataset, symbol, date, timeframe).

Primary key:
- (market, dataset, symbol, date, timeframe)

Columns (implemented):
- market
- dataset
- symbol
- date
- timeframe
- num_files
- num_rows
- distinct_minute_cnt
- min_ts
- max_ts
- expected_rows
- completeness_ratio
- is_suspect
- computed_at_utc

Completeness definition:
- For 1m klines, completeness is based on expected minute bars per day (if expected_rows is defined).
- For derived 1d bars, completeness uses `CAST(ts AS DATE)` grouping, because month partitions do not carry `date`.

Funding handling:
- Funding coverage is stored with timeframe='' and is updated only during 1m logical builds.

--------------------------------------------------------------------
4. md_symbol_dataset_stats
--------------------------------------------------------------------

Purpose:
- Per-symbol/dataset/timeframe statistics useful for sanity checks and alignment caps.

Grain:
- One row per (market, dataset, symbol, timeframe)

Primary key:
- (market, dataset, symbol, timeframe)

Columns (implemented):
- market
- dataset
- symbol
- timeframe
- min_date
- max_date
- min_ts
- max_ts
- days_present
- days_complete
- pct_days_complete
- avg_rows_per_day
- last_refreshed_utc

Funding handling (current behavior):
- Funding coverage is stored with timeframe='', but stats rollups normalize funding to timeframe='1m' when `timeframe_filter == '1m'`.

--------------------------------------------------------------------
5. md_alignment_summary
--------------------------------------------------------------------

Purpose:
- Summarize cross-dataset alignment and completeness at the day level.
- Detects when datasets disagree on coverage for a given symbol/day/timeframe.

Grain:
- One row per (market, timeframe, symbol, date)

Primary key:
- (market, timeframe, symbol, date)

Columns (implemented):
- market
- timeframe
- symbol
- date
- has_klines
- has_mark
- has_index
- has_funding
- all_core_present
- core_missing_mask
- core_completeness_min
- computed_at_utc

Funding handling (current behavior):
- Funding coverage is normalized to timeframe='1m' when building 1m alignment summaries.
- 1d builds do not include funding.

--------------------------------------------------------------------
6. md_liquidity_daily
--------------------------------------------------------------------

Purpose:
- Provide daily liquidity metrics per symbol/timeframe.
- Compute rolling medians and ranks for universe selection and filters.

Grain:
- One row per (market, timeframe, symbol, date)

Primary key:
- (market, timeframe, symbol, date)

Columns (implemented):
- market
- timeframe
- symbol
- date
- dollar_volume
- volume
- close_price
- dv_30d_median
- liquidity_rank_30d
- in_top_100
- computed_at_utc

Semantics:
- Daily key is derived from timestamps (`CAST(ts AS DATE)`).
- Rolling windows include sufficient lookback (date_from - 29 days for a 30-day median).

--------------------------------------------------------------------
7. md_kline_integrity_day (DDL only)
--------------------------------------------------------------------

Purpose:
- Detect silent data issues at the symbol-day level (gaps, duplicates, anomalous prices).

Grain:
- One row per (market, symbol, date, timeframe)

Primary key:
- (market, symbol, date, timeframe)

Columns (implemented):
- market
- symbol
- date
- timeframe
- num_rows
- distinct_minute_cnt
- gap_rows_vs_expected
- duplicate_rows
- high_lt_low_cnt
- nonpositive_price_cnt
- max_abs_ret_1m
- computed_at_utc

Note:
- The table DDL exists, but the builder is not implemented yet.

--------------------------------------------------------------------
8. CROSS-TABLE RELATIONSHIPS AND JOINS
--------------------------------------------------------------------

Common join keys:
- Timeframed tables: (market, timeframe, symbol, date) or extensions thereof
- Dataset-grained joins add dataset:
  (market, timeframe, dataset, symbol, date)

Never join timeframed tables without timeframe.

Funding join rule:
- Funding rows use timeframe='' in `md_partition_coverage`.
- When building stats/alignment for 1m, funding is normalized to timeframe='1m'.

--------------------------------------------------------------------
9. REQUIRED BEHAVIOR OF METADATA BUILDERS
--------------------------------------------------------------------

For each table family:

- md_file_manifest:
  - upsert new/changed file fingerprints
  - return changed file list scoped to build (market, timeframe_filter, datasets)

- md_partition_coverage:
  - first run for timeframe: recompute for that timeframe only
  - incremental: update only affected symbol-days

- md_symbol_dataset_stats:
  - recompute only for the timeframe filter
  - include funding normalization for 1m only

- md_alignment_summary:
  - recompute only for the timeframe filter
  - include funding normalization for 1m only

- md_liquidity_daily:
  - recompute only for the timeframe filter
  - rolling recompute includes lookback pre-history

--------------------------------------------------------------------
10. VALIDATION CHECKLIST
--------------------------------------------------------------------

After any build:

- No cross-timeframe row changes:
  - 1d build must not modify 1m rows in coverage/stats/alignment/liquidity
- Funding policy honored:
  - funding coverage updated only during logical 1m build
  - funding stats/alignment only normalized during 1m runs
- Rolling windows correct:
  - dv_30d_median not reset at incremental range boundaries
- Derived data daily keys:
  - liquidity/coverage computations do not assume hive date partitions for month-partitioned data
- Idempotence:
  - re-run with no changes produces same counts and values

This document is the canonical reference for how metadata tables behave today.
