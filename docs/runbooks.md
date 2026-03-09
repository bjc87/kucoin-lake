# KuCoin Lake — Runbooks

Operational guidance for ingestion, resampling, and metadata workflows.

Design priorities:
- research correctness > micro-optimisation
- idempotent, resumable operations
- multi-timeframe safety
- explicit scope (symbol/date/dataset/timeframe)

--------------------------------------------------------------------
0. ONE OPERATIONAL FLOW (SCOPE VARIES)
--------------------------------------------------------------------

All run types use the **same pipeline steps**. Only scope changes.

Pipeline steps:
1. `fetch_futures()` (optional, if ZIPs not already local)
2. `ingest_local_downloads_to_lake()`
3. `resample_1m_to_1d()` (per dataset, if 1d is needed)
4. `build_metadata()`

Run types:
- **First run**: full scope (all symbols, full history)
- **Daily update**: last N days + changed files
- **Backfill**: bounded symbol/date/dataset/timeframe window
- **Gap fill**: targeted symbol-day from metadata gaps

--------------------------------------------------------------------
1. CANONICAL JUPYTER API SEQUENCE
--------------------------------------------------------------------

Recommended sequence (per dataset resample if needed):
```
fetch_futures()
ingest_local_downloads_to_lake()
resample_1m_to_1d()
build_metadata()
```

--------------------------------------------------------------------
2. FIRST RUN
--------------------------------------------------------------------

**Goal**: initialize lake + metadata for 1m futures.

Steps:
1. Fetch ZIPs for the full desired range.
2. Ingest ZIPs into the NAS lake.
3. (Optional) Resample 1m → 1d per dataset.
4. Build metadata with `timeframe_filter='1m'`.

Notes:
- `fetch_futures()` plans from month-sharded remote listings; it downloads only listed keys and skips already local keys.
- If you also need 1d metadata, run `build_metadata` again with `timeframe_filter='1d'` after resampling.
- Funding participates only in 1m builds.

--------------------------------------------------------------------
3. DAILY UPDATE
--------------------------------------------------------------------

**Goal**: keep lake + metadata current with minimal work.

Steps:
1. Fetch last N days of ZIPs.
2. Ingest bounded date window (typically last N days).
3. Resample affected months per dataset (use `changed_files` or month filters).
4. Run `build_metadata` with `timeframe_filter='1m'` (and `1d` if needed).

Key property:
- Incremental metadata updates are **file-scan driven**; you can scope by symbols and date range to reduce scan time.
- Fetch planning is listing-driven, so sparse historical windows avoid per-day missing-file probe overhead.

--------------------------------------------------------------------
4. BACKFILL (HISTORICAL)
--------------------------------------------------------------------

**Goal**: backfill a bounded history for specific symbols or datasets.

Steps:
1. Fetch ZIPs for the backfill date range.
2. Ingest the same bounded date range.
3. Resample 1d for the backfilled range (per dataset).
4. Run `build_metadata` with a matching scope:
   - `--symbols` and `--date-start/--date-end`
   - `--timeframe-filter` set to the target timeframe

--------------------------------------------------------------------
5. GAP FILL (MISSING DAYS)
--------------------------------------------------------------------

**Goal**: fill missing derived tables using coverage as the driver.

Steps:
1. Ensure `md_partition_coverage` is up to date (via `build_metadata`).
2. Use `backfill-derived` to rebuild missing liquidity/integrity rows **without rescanning the lake**.

--------------------------------------------------------------------
6. CLI TEMPLATES
--------------------------------------------------------------------

Templates use placeholders; see `docs/cli.md` for exact options.

Fetch:
```
kucoin-lake fetch-futures \
  --out-root <path-to-download-root> \
  --symbols <SYM1> <SYM2> \
  --datatype klines mark index funding \
  --timeframe 1m \
  --start-date YYYY-MM-DD --end-date YYYY-MM-DD
```

Ingest:
```
kucoin-lake ingest-local-downloads-to-lake \
  --startdate YYYY-MM-DD \
  --enddate YYYY-MM-DD \
  --assets <SYM1> <SYM2> \
  --timeframes 1m \
  --local-root <path-containing-futures/daily-or-datasets>
```

Resample (per dataset):
```
kucoin-lake resample-1m-to-1d \
  --nas-root <path-to-lake-root> \
  --market futures \
  --dataset klines \
  --threads 4
```

Metadata build:
```
kucoin-lake build-metadata \
  --nas-root <path-to-lake-root> \
  --meta-db-path <path-to-metadata.duckdb> \
  --market futures \
  --datasets klines mark index funding \
  --timeframe-filter 1m
```

--------------------------------------------------------------------
7. COMMON PITFALLS
--------------------------------------------------------------------

- Cross-timeframe deletes (always scope by timeframe)
- Funding leakage into non-1m builds
- Assuming hive `date` for derived 1d (must use timestamps)
- Rolling window recompute without lookback pre-history
- Concurrent writers against the same DuckDB file
