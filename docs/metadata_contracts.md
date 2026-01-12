# KuCoin Lake — Metadata Contracts

This document defines the canonical DuckDB metadata schema (tables, grains, primary keys, and semantics) for the KuCoin Parquet lake. It is the contract between:

- the filesystem lake layout (hive partitions)
- the metadata builders (coverage, stats, alignment, liquidity)
- integrity checks and universe engineering

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
- Funding is stored as dataset='funding' with timeframe='' in metadata.
- Funding is included only for logical 1m builds.
- 1d or 1h builds MUST NOT delete or recompute funding rows.

P0.5 Partition semantics
- Raw 1m uses date=YYYY-MM-DD partitions.
- Derived 1d uses month=YYYY-MM partitions.
- Metadata logic MUST NOT assume that a hive partition column 'date' always exists.
- For derived data, daily keys must be computed from timestamps (CAST(ts AS DATE)) when needed.

--------------------------------------------------------------------
1. SCHEMA AND NAMING
--------------------------------------------------------------------

Schema name:
- md

All tables described below live under md.*.

Common column conventions:
- market     : string (e.g. 'futures', later 'spot')
- timeframe  : string (e.g. '1m', '1d') for timeframed tables; '' for funding rows
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
- Stores parsed partition attributes for each file.

Grain:
- One row per physical Parquet file (unique by absolute path).

Required columns (recommended minimum):
- market              : string
- dataset             : string
- timeframe           : string ('' for funding)
- symbol              : string
- partition_key       : string ('date' or 'month')
- partition_value     : string ('YYYY-MM-DD' or 'YYYY-MM')
- path                : string (absolute path)
- size_bytes          : bigint
- mtime_utc           : TIMESTAMP (or numeric epoch; must be consistent)
- fingerprint         : string (optional; e.g. hash or derived fingerprint)
- first_seen_at_utc   : TIMESTAMP
- last_seen_at_utc    : TIMESTAMP

Primary key:
- (market, path)

Uniqueness constraints (logical):
- path must be globally unique
- (market, dataset, timeframe, symbol, partition_key, partition_value) should be unique under “one file per partition”
  but path is the hard key (filesystem truth).

Semantics:
- The manifest is append/update only; it should not be “rebuilt from scratch” unless explicitly requested.
- last_seen_at_utc is updated even if a file has not changed (optional but useful).
- A file is considered “changed” if size/mtime/fingerprint differs from the manifest.

Timeframe handling:
- Funding rows have timeframe = ''.
- Timeframed datasets have timeframe equal to hive partition value (e.g. '1m','1d').

--------------------------------------------------------------------
3. md_partition_coverage
--------------------------------------------------------------------

Purpose:
- Summarize which symbol/dataset partitions exist and how complete they are.
- Acts as the first-layer “what data do I have?” table used by alignment and universe decisions.

Grain (recommended):
- One row per (market, timeframe, dataset, symbol, day)
  where day is a DATE representing the trading/calendar day for that slice.

Primary key:
- (market, timeframe, dataset, symbol, date)

Core columns:
- market        : string
- timeframe     : string ('' only if you intentionally store funding coverage; otherwise keep funding separate)
- dataset        : string
- symbol        : string
- date          : DATE
- n_rows        : bigint
- min_ts        : TIMESTAMP
- max_ts        : TIMESTAMP
- completeness  : DOUBLE (0..1, definition depends on dataset/timeframe)
- computed_at_utc : TIMESTAMP

Completeness definition:
- For 1m klines, completeness is typically based on expected minute bars per day
  (but must tolerate exchange quirks).
- For derived 1d bars, completeness may simply be n_rows>=1 for each day.

Partition caveat for derived data:
- Derived files are month-partitioned; coverage must still be day-grained if used for daily alignment/universe.
- Therefore coverage builders must group by CAST(ts AS DATE) rather than relying on hive 'date'.

Funding handling:
- Funding should only participate in logical 1m builds.
- If you store funding coverage, keep timeframe='' and include it only when timeframe_filter == '1m'.
- 1d/1h builds must not touch funding coverage rows.

--------------------------------------------------------------------
4. md_symbol_dataset_stats
--------------------------------------------------------------------

Purpose:
- Fast per-symbol/dataset/timeframe statistics useful for sanity checks, caps, and alignment.
- Used to compute global min/max, lowest-max caps, etc., per timeframe.

Grain:
- One row per (market, timeframe, dataset, symbol)

Primary key:
- (market, timeframe, dataset, symbol)

Core columns (recommended):
- market          : string
- timeframe       : string ('' for funding if included)
- dataset         : string
- symbol          : string
- n_rows          : bigint
- min_ts          : TIMESTAMP
- max_ts          : TIMESTAMP
- n_days          : integer (optional)
- min_date        : DATE (optional)
- max_date        : DATE (optional)
- computed_at_utc : TIMESTAMP

Semantics:
- Must not mix stats across timeframes.
- Useful “caps” should be computed per (market, timeframe, dataset), e.g.:
  - global_min_ts
  - global_max_ts
  - lowest_max_ts_across_symbols

Funding handling:
- If funding stats exist, they remain at timeframe='' and are included only for 1m logical builds.

Derived data note:
- For month-partitioned derived bars, n_days and min/max date should be derived from timestamps,
  not from hive month alone, if you need day resolution.

--------------------------------------------------------------------
5. md_alignment_summary
--------------------------------------------------------------------

Purpose:
- Summarize cross-dataset alignment and completeness at the day level.
- Detects when datasets disagree on coverage for a given symbol/day/timeframe.
- Helps prevent label leakage/corruption due to mis-joins or missing slices.

Grain (common patterns):
Option A (day-grained, per timeframe, aggregated over symbols):
- One row per (market, timeframe, date)

Option B (day-grained, per timeframe, per symbol):
- One row per (market, timeframe, symbol, date)

Option C (day-grained, per timeframe, per dataset pair):
- One row per (market, timeframe, date, dataset_a, dataset_b)

Pick one and enforce it consistently. The key is: timeframe must be included.

Primary key:
- Must include (market, timeframe, date) at minimum, plus symbol/pairs depending on design.

Core columns (suggested for Option A or B):
- market          : string
- timeframe       : string
- date            : DATE
- symbol          : string (if per-symbol)
- expected_datasets : integer (count)
- present_datasets  : integer (count)
- missing_datasets  : integer (count)
- alignment_score   : DOUBLE (0..1)
- notes             : string (optional)
- computed_at_utc   : TIMESTAMP

Semantics:
- Alignment inputs are typically md_partition_coverage rows for a given timeframe.
- Funding participation should follow funding policy (logical 1m only).
- Alignment computation must not silently include funding in non-1m timeframes.

--------------------------------------------------------------------
6. md_liquidity_daily
--------------------------------------------------------------------

Purpose:
- Provide daily liquidity metrics per symbol and timeframe for universe selection and research filters.
- Compute rolling medians and ranks to produce stable universe candidates.

Grain:
- One row per (market, timeframe, symbol, date)

Primary key:
- (market, timeframe, symbol, date)

Core columns (recommended):
- market             : string
- timeframe          : string
- symbol             : string
- date               : DATE

Raw daily liquidity measures:
- dollar_volume      : DOUBLE (sum close*volume for the day)
- volume             : DOUBLE
- close_price        : DOUBLE (ARG_MAX(close, ts) or last close)

Rolling / rank measures:
- dv_30d_median      : DOUBLE (rolling median of dollar_volume)
- liquidity_rank_30d : INTEGER (rank per day across symbols)
- in_top_100         : BOOLEAN (or configurable top_n)

Operational:
- computed_at_utc    : TIMESTAMP

Semantics:
- Daily key must be derived from timestamp: CAST(ts AS DATE)
  (critical for month-partitioned derived bars).
- Rolling windows must include sufficient pre-history:
  when recomputing for date_from..date_to, the input window must include
  (date_from - 29 days) .. date_to for a 30-day median.

Funding handling:
- Typically liquidity is computed from klines (or mark) only.
- Funding should not be part of liquidity unless explicitly designed.

--------------------------------------------------------------------
7. md_kline_integrity_day (RECOMMENDED)
--------------------------------------------------------------------

Purpose:
- Detect silent data issues at the symbol-day level, particularly for 1m klines:
  - missing minutes (gaps)
  - duplicate minutes
  - non-monotonic timestamps
  - unexpected timestamp range

Grain:
- One row per (market, timeframe, symbol, date, dataset)
  (dataset is usually 'klines', but include it for extensibility)

Primary key:
- (market, timeframe, dataset, symbol, date)

Core columns (suggested):
- market             : string
- timeframe          : string
- dataset            : string
- symbol             : string
- date               : DATE

Integrity signals:
- n_rows             : bigint
- n_unique_minutes   : bigint (for 1m)
- n_duplicate_minutes: bigint
- n_missing_minutes  : bigint
- min_ts             : TIMESTAMP
- max_ts             : TIMESTAMP

Flags:
- has_gaps           : BOOLEAN
- has_dups           : BOOLEAN
- is_suspect         : BOOLEAN (derived from gap/dup thresholds)

Operational:
- computed_at_utc    : TIMESTAMP
- source             : string (optional: 'bulk'/'incremental')

Semantics:
- Intended as “events/results”, not necessarily blocking.
- Universe engineering can exclude suspect days, or the model can drop them.
- Should be incremental: recompute only for symbol-days touched by changed files.

Thresholds:
- Expected minutes per day may vary; do not hardcode 1440 without tolerance.
- Gap/dup thresholds should be configurable.

--------------------------------------------------------------------
8. md_meta_runs (RECOMMENDED)
--------------------------------------------------------------------

Purpose:
- Append-only audit trail for metadata builds.
- Enables debugging of “why did this change?”, reproducibility, and operational confidence.

Grain:
- One row per run.

Primary key:
- run_id (UUID or timestamp-based unique identifier)

Core columns (suggested):
- run_id               : string
- started_at_utc       : TIMESTAMP
- finished_at_utc      : TIMESTAMP (nullable until complete)
- status               : string ('started','success','failed')
- error_message        : string (nullable)

Inputs:
- market               : string
- timeframe_filter     : string
- datasets             : string (e.g. JSON array or CSV)
- nas_root             : string (optional, or omit if sensitive)

Counts:
- scanned_files        : bigint
- changed_files        : bigint
- updated_partitions   : bigint
- updated_symbols      : bigint (optional)

Outputs built:
- built_manifest       : BOOLEAN
- built_coverage       : BOOLEAN
- built_stats          : BOOLEAN
- built_alignment      : BOOLEAN
- built_liquidity      : BOOLEAN
- built_integrity      : BOOLEAN
- built_universe       : BOOLEAN

Code provenance (optional but very useful):
- git_sha              : string
- code_version         : string
- hostname             : string (optional)

Semantics:
- Always insert a 'started' row first, then update to success/failed.
- Must not be deleted during normal operation.
- Enables reproducible research: you can tie produced metadata to a run_id.

--------------------------------------------------------------------
9. CROSS-TABLE RELATIONSHIPS AND JOINS
--------------------------------------------------------------------

Common join keys:
- Timeframed tables: (market, timeframe, symbol, date) or extensions thereof
- Dataset-grained joins add dataset:
  (market, timeframe, dataset, symbol, date)

Never join timeframed tables without timeframe.

Funding join rule:
- Funding rows use timeframe=''.
- If a join requires aligning funding with 1m tables:
  use an effective timeframe expression (tf_eff) in the query:
    tf_eff = CASE WHEN dataset='funding' THEN '1m' ELSE timeframe END
  and ensure this logic is applied consistently and only for 1m builds.

--------------------------------------------------------------------
10. REQUIRED BEHAVIOR OF METADATA BUILDERS
--------------------------------------------------------------------

For each table family:

- md_file_manifest:
  - upsert new/changed file fingerprints
  - return changed file list scoped to build (market, timeframe_filter, datasets)

- md_partition_coverage:
  - first run for timeframe: recompute for that timeframe only
  - incremental: update only affected symbol-days

- md_symbol_dataset_stats:
  - incremental: update affected (symbol, dataset, timeframe)

- md_alignment_summary:
  - incremental: update affected dates (and optionally symbols) for timeframe

- md_liquidity_daily:
  - incremental: update affected date range for timeframe
  - rolling recompute includes lookback pre-history

- md_kline_integrity_day:
  - incremental: update affected symbol-days for timeframe

- md_meta_runs:
  - always write a run record around all builds

--------------------------------------------------------------------
11. VALIDATION CHECKLIST
--------------------------------------------------------------------

After any build:

- No cross-timeframe row changes:
  - 1d build must not modify 1m rows in coverage/stats/alignment/liquidity
- Funding policy honored:
  - funding rows updated only during logical 1m build
- Rolling windows correct:
  - dv_30d_median not reset at incremental range boundaries
- Derived data daily keys:
  - liquidity/coverage computations do not assume hive date partitions for month-partitioned data
- Idempotence:
  - re-run with no changes produces same counts and values

This document is the canonical reference for how metadata tables must behave and how they relate to the lake layout.
