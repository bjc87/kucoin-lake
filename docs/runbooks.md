# KuCoin Lake — Runbooks

This document is an operational guide for running the ingestion, resampling, metadata builds, integrity checks, and universe engineering workflows safely and repeatably.

Design priorities:
- research correctness > micro-optimisation
- idempotent, resumable operations
- multi-timeframe safety (1m + 1d now; 1h later)
- no destructive cross-timeframe side effects
- explicit root paths (never “scan my whole filesystem”)

--------------------------------------------------------------------
0. PREREQUISITES AND ASSUMPTIONS
--------------------------------------------------------------------

Assumptions:
- You run builds locally on a Mac (or other host) with the NAS mounted.
- DuckDB database files live on local disk or NAS (prefer local for fewer locking surprises).
- Single-writer discipline: do not run multiple writers against the same DuckDB file concurrently.

NAS mount assumptions:
- NAS is mounted at:
    /Volumes/quant_data
- Lake roots:
    /Volumes/quant_data/kucoin/futures/
    /Volumes/quant_data/kucoin/spot/   (future)

Local staging (optional):
- Local staging may be used for downloaded zip bundles and temporary extracted files.
- Staging is not the canonical lake; the NAS lake is.

Timezones:
- All timestamps are treated as UTC unless explicitly documented otherwise.
- Daily keys (DATE) are computed as CAST(ts AS DATE) in UTC.

Safety:
- Any command that deletes/recomputes must be scoped by (market, timeframe).
- Destructive actions require an explicit --yes flag (recommended).

--------------------------------------------------------------------
1. QUICK START (MOST COMMON WORKFLOW)
--------------------------------------------------------------------

Goal: update lake + metadata incrementally for 1m futures.

1) Ingest new raw data (download + parquet write) for 1m
2) Update metadata for 1m (manifest + coverage + stats + alignment + liquidity)
3) Run integrity checks for 1m klines on changed symbol-days
4) Update universe for 1m

Recommended cadence:
- daily: run incremental 1m metadata + integrity + universe
- weekly: run broader sanity checks and alignment trend review

--------------------------------------------------------------------
2. FIRST RUN SETUP (NEW MACHINE / NEW DB / NEW MARKET)
--------------------------------------------------------------------

2.1 Create / choose the metadata DuckDB file
- Futures DB example:
    metadata_futures.duckdb
- Spot DB example:
    metadata_spot.duckdb

Recommendation:
- Keep DB on local SSD for speed and fewer NAS file lock issues.
- If DB must be on NAS, enforce single-writer strictly.

2.2 Initialize schemas and tables
- Ensure schema `md` exists.
- Ensure all required tables exist:
    md_file_manifest
    md_partition_coverage
    md_symbol_dataset_stats
    md_alignment_summary
    md_liquidity_daily
  Recommended:
    md_kline_integrity_day
    md_meta_runs

2.3 Perform an initial manifest build
- Scan filesystem for candidate Parquet files (scoped to 1m + funding).
- Upsert all files into md_file_manifest.

2.4 Perform initial metadata build for timeframe=1m
- Run coverage, stats, alignment, liquidity for 1m.
- Funding participates only in logical 1m build.

2.5 Verify initial outputs
- Spot-check row counts:
  - md_file_manifest should have one row per Parquet file
  - md_partition_coverage should have one row per (symbol, dataset, day) for 1m
  - md_liquidity_daily should have one row per (symbol, day) for 1m
- Verify no other timeframe rows were created.

2.6 Optional: first integrity + universe build
- Compute md_kline_integrity_day for 1m (bulk for recent window or full history)
- Compute universe membership (hysteresis) for 1m

--------------------------------------------------------------------
3. ROUTINE INCREMENTAL RUN (DAILY)
--------------------------------------------------------------------

This is the standard “keep metadata up to date” run.

Inputs:
- market = futures
- timeframe_filter = 1m
- datasets = [klines, mark, index] plus funding participation

3.1 Step A: Update manifest and detect changes
- Iterate filesystem Parquet candidates scoped to timeframe_filter
- Upsert manifest rows
- Compute changed/new file paths list for this run

Outputs:
- changed file paths list
- changed partition set (dataset, symbol, date/month)

3.2 Step B: Incremental coverage update
- For each dataset, recompute coverage only for impacted symbol-days.
- No global deletes.
- Funding coverage only if timeframe_filter == 1m.

3.3 Step C: Incremental symbol dataset stats
- Recompute stats only for impacted (symbol, dataset, timeframe).
- Stats must remain scoped by timeframe (no mixing 1m with 1d).

3.4 Step D: Incremental alignment update
- Recompute alignment only for affected days (and symbols if applicable).
- Ensure funding participation policy is honored.

3.5 Step E: Incremental liquidity daily
- Recompute dollar volume for affected days.
- Recompute rolling median + rank for affected date range WITH LOOKBACK.
  Example: for 30-day rolling median, include date_from-29 days.

3.6 Step F: Integrity checks (recommended)
- For changed 1m klines symbol-days:
  - compute gaps and duplicates
  - write md_kline_integrity_day
- Do not block by default; store results.

3.7 Step G: Universe update (recommended)
- Recompute membership for affected dates.
- Apply integrity filters (exclude suspect symbol-days if configured).
- Apply hysteresis membership rules.

3.8 Run audit (recommended)
- Record run parameters and counts in md_meta_runs:
  - scanned_files
  - changed_files
  - updated_partitions
  - status success/failed

--------------------------------------------------------------------
4. BUILDING DERIVED 1D BARS FROM 1M
--------------------------------------------------------------------

Goal: generate derived timeframe data without impacting raw 1m.

Inputs:
- source timeframe: 1m
- target timeframe: 1d
- dataset: typically klines (and optionally mark/index if desired)

4.1 Determine date range to build
- Identify newest 1m day available per symbol.
- Decide whether to build partial current month or only completed days.

Recommendation:
- Build derived 1d only for fully completed days.
- Use a “cap date” of yesterday UTC if today’s data is incomplete.

4.2 Run DuckDB set-based resample
- Read 1m Parquet via hive partitioning
- Aggregate to 1d bars
- Write Parquet to month-partitioned location:
    {dataset}/timeframe=1d/symbol={symbol}/month={YYYY-MM}/data.parquet

4.3 Validate resample correctness
- For a sample of symbol-days:
  - compare derived OHLCV to aggregated 1m OHLCV
  - ensure exact match for open/high/low/close and sums for volume
- Store validation results (optional but recommended).

4.4 Metadata build for timeframe=1d
- Run metadata builders with timeframe_filter=1d:
  - manifest update scoped to 1d
  - coverage/stats/alignment/liquidity for 1d only
- Funding must NOT participate in 1d builds.

Key pitfall:
- Derived data is month-partitioned; liquidity and coverage should compute day from ts, not hive date.

--------------------------------------------------------------------
5. ADDING A NEW TIMEFRAME (E.G. 1H)
--------------------------------------------------------------------

There are two scenarios:
A) 1h is derived from 1m (recommended)
B) 1h is ingested natively (if exchange provides it and you choose to store it)

5.1 Decide the source of truth
- If derived: implement a resample path like 1d but target 1h.
- If native: implement ingestion path writing:
    {dataset}/timeframe=1h/symbol={symbol}/date={YYYY-MM-DD}/data.parquet
  or consider month partitioning if file counts become too large.

5.2 Write derived or ingest data
- Ensure it does not overwrite existing 1m or 1d files.

5.3 Run metadata build for timeframe=1h
- Must be fully scoped by timeframe=1h.
- Must not delete or modify 1m/1d rows.
- Funding must not participate unless explicitly designed (default: no).

5.4 Add integrity hooks
- If timeframe is not 1m, gap checks differ (expected bars/day changes).
- Integrity logic must be parameterised by timeframe.

5.5 Update universe logic (optional)
- Decide whether universe is defined at 1m and reused, or computed per timeframe.
- Default: compute universe per timeframe if you trade/validate per timeframe.

--------------------------------------------------------------------
6. ADDING A NEW MARKET (SPOT)
--------------------------------------------------------------------

6.1 Create a separate market root
- /Volumes/quant_data/kucoin/spot/

6.2 Decide whether to share or separate metadata DB
Recommendation:
- Separate DBs per market (simpler, fewer accidental joins):
    metadata_futures.duckdb
    metadata_spot.duckdb
Alternative:
- Single DB with market in all PKs (works, but requires stronger discipline)

6.3 Ingest spot data
- Mirror the same dataset partition conventions where possible.
- If spot datasets differ, document them in data_lake_layout.md.

6.4 Run initial manifest + metadata build for spot
- Same first-run procedure as futures.
- Ensure no futures rows are affected.

--------------------------------------------------------------------
7. BACKFILLING HISTORY
--------------------------------------------------------------------

Backfills are common and must be safe and resumable.

7.1 Ingest backfill files
- Ensure deterministic/idempotent ingestion (same inputs produce same output paths).
- Avoid partial overwrites by writing to temp then moving into place (optional).

7.2 Run incremental metadata build
- Manifest should detect changed/new partitions.
- Coverage/stats/alignment/liquidity should update only impacted date ranges.

7.3 For very large backfills
- Consider running in chunks:
  - by symbol group
  - by month
- Record each chunk run in md_meta_runs.

7.4 Post-backfill validations
- Recompute alignment summaries for backfilled date range.
- Run integrity checks for backfilled symbol-days.

--------------------------------------------------------------------
8. FAILURE RECOVERY AND RESUMABILITY
--------------------------------------------------------------------

8.1 If a run fails mid-way
- The manifest and/or some metadata tables may have partial updates.
- Resume by re-running the same command; idempotence should converge.

8.2 Ensure rolling windows recompute correctly
- For liquidity rolling metrics, ensure recompute range includes lookback pre-history.
- If unsure, recompute a wider date range (safe superset).

8.3 If you suspect corrupted metadata
- Prefer scoped rebuilds:
  - rebuild coverage for timeframe=1m only
  - rebuild liquidity for last N days only
- Avoid “wipe everything” unless absolutely necessary.

8.4 Verify timeframe isolation
- After recovery, confirm that only the intended timeframe was modified.

--------------------------------------------------------------------
9. OPERATIONAL CHECKS (RECOMMENDED)
--------------------------------------------------------------------

After each daily run:
- md_meta_runs status is success
- changed_files count is plausible
- liquidity ranks present for recent days
- alignment score not materially degraded vs previous days
- integrity flags do not spike unexpectedly

Weekly:
- Compare global min/max timestamps in md_symbol_dataset_stats across datasets for consistency.
- Review alignment trend.
- Sample a few symbols and check raw parquet vs metadata (sanity).

--------------------------------------------------------------------
10. COMMON PITFALLS
--------------------------------------------------------------------

Pitfall: cross-timeframe deletes
- Any DELETE missing timeframe in WHERE can wipe other timeframes.

Pitfall: funding leakage into non-1m builds
- Funding must only participate in logical 1m builds.

Pitfall: derived 1d uses month partition; code assumes hive date exists
- Fix by deriving day from timestamps.

Pitfall: rolling median recompute wrong at boundaries
- Must include lookback pre-history.

Pitfall: scanning too broadly then filtering
- Scoping must happen at iterator / manifest query level.

Pitfall: running concurrent writers against the same DuckDB file
- Enforce single-writer.

--------------------------------------------------------------------
11. SUGGESTED CLI COMMAND SHAPES (PSEUDO)
--------------------------------------------------------------------

The exact CLI will depend on implementation, but the recommended shape is:

- Update manifest only:
  kucoin-lake manifest --market futures --timeframe 1m --datasets klines,mark,index --nas-root /Volumes/quant_data/kucoin/futures

- Full incremental metadata:
  kucoin-lake metadata --market futures --timeframe 1m --datasets klines,mark,index --nas-root /Volumes/quant_data/kucoin/futures

- Resample 1d:
  kucoin-lake resample --market futures --source-timeframe 1m --target-timeframe 1d --dataset klines --nas-root /Volumes/quant_data/kucoin/futures --date-from 2025-01-01 --date-to 2026-01-01

- Integrity checks:
  kucoin-lake integrity --market futures --timeframe 1m --dataset klines --nas-root /Volumes/quant_data/kucoin/futures --mode incremental

- Universe update:
  kucoin-lake universe --market futures --timeframe 1m --nas-root /Volumes/quant_data/kucoin/futures --top-n 100 --hysteresis ...

All destructive operations should require:
  --yes

And all commands should refuse unsafe roots.

--------------------------------------------------------------------
END
--------------------------------------------------------------------
