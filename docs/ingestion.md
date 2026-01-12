# KuCoin Lake — Ingestion Contract

This document describes the **current ingestion pipeline** implemented in `archive/fetch_data.py` and `archive/nas_parquet_mirror.py`. It is the authoritative description of how ZIP downloads map into the canonical lake and what invariants ingestion must preserve.

--------------------------------------------------------------------
1. OVERVIEW
--------------------------------------------------------------------

The ingestion pipeline has two stages:

1) **Download ZIPs** from KuCoin’s historical endpoint
   - Implemented in `archive/fetch_data.py` (`fetch_futures(...)`).
   - Downloads ZIPs into a local directory that mirrors KuCoin’s key layout.

2) **Convert ZIP → Parquet**
   - Implemented in `archive/nas_parquet_mirror.py` (`run_ingest(...)`).
   - Parses CSVs in ZIPs, normalizes schemas, and writes Parquet to the NAS lake.
   - Uses local staging + atomic copy to avoid partial writes.

The **canonical contract** remains the lake layout defined in `docs/data_lake_layout.md`.

--------------------------------------------------------------------
2. DOWNLOAD INPUTS (ZIP LAYOUT)
--------------------------------------------------------------------

KuCoin ZIP keys are built as:
- `data/futures/daily/{dataset}/{symbol}/.../{file}.zip`

`fetch_futures(...)` writes ZIPs under:
- `{out_root}/data/futures/daily/...`

Example (klines):
- `data/futures/daily/klines/BTCUSDTM/1m/BTCUSDTM-1m-2026-01-01.zip`

Example (funding):
- `data/futures/daily/fundingRates/BTCUSDTM/BTCUSDTM-fundingRates-2026-01-01.zip`

**Local root expectation for ingestion**:
- `run_ingest(...)` expects `local_root` to be the directory that contains `futures/`.
- For the download example above, `local_root` should point to `{out_root}/data`.

--------------------------------------------------------------------
3. MAPPING: ZIP → CANONICAL LAKE PATH
--------------------------------------------------------------------

For each ZIP file, ingestion writes exactly one Parquet file in the lake.

Mapping rules (current behavior):

- klines ZIP:
  - Local: `klines/{SYMBOL}/1m/{SYMBOL}-1m-YYYY-MM-DD.zip`
  - Lake:  `klines/timeframe=1m/symbol={SYMBOL}/date=YYYY-MM-DD/data.parquet`

- mark ZIP:
  - Local: `mark/{SYMBOL}/1m/{SYMBOL}-1m-YYYY-MM-DD.zip`
  - Lake:  `mark/timeframe=1m/symbol={SYMBOL}/date=YYYY-MM-DD/data.parquet`

- index ZIP:
  - Local: `index/{SYMBOL}/1m/{SYMBOL}-1m-YYYY-MM-DD.zip`
  - Lake:  `index/timeframe=1m/symbol={SYMBOL}/date=YYYY-MM-DD/data.parquet`

- fundingRates ZIP:
  - Local: `fundingRates/{SYMBOL}/{SYMBOL}-fundingRates-YYYY-MM-DD.zip`
  - Lake:  `funding/symbol={SYMBOL}/date=YYYY-MM-DD/data.parquet`

--------------------------------------------------------------------
4. SCHEMA NORMALIZATION
--------------------------------------------------------------------

Ingestion uses DuckDB `read_csv(...)` with explicit column mappings and writes Parquet.

Common behavior:
- `time` is interpreted as a numeric epoch in milliseconds (often stored as scientific notation in the CSV).
- `time` is cast to `BIGINT` as `time_ms` and to a timestamp `ts` (UTC) via `to_timestamp(time_ms / 1000.0)`.
- Columns are cast to `DOUBLE` where appropriate.

Dataset-specific expectations:

- **klines**
  - Expected CSV columns: `time`, `open`, `high`, `low`, `close`, `volume`.
  - Output columns: `time_ms`, `ts`, `open`, `high`, `low`, `close`, `volume`.

- **mark**
  - Expected CSV columns: `time`, `open`, `high`, `low`, `close`.
  - Output columns: `time_ms`, `ts`, `open`, `high`, `low`, `close`.

- **index**
  - Expected CSV columns: `time`, `open`, `high`, `low`, `close`.
  - Output columns: `time_ms`, `ts`, `open`, `high`, `low`, `close`.

- **fundingRates**
  - Expected CSV columns: `symbol`, `time`, `fundingRate`.
  - Output columns: `symbol`, `time_ms`, `ts`, `funding_rate`.

Assumptions:
- Timestamps are interpreted as UTC.
- The CSV delimiter is `,` (comma) in the current converter implementation.

--------------------------------------------------------------------
5. IDEMPOTENCE AND SAFETY RULES
--------------------------------------------------------------------

Idempotence:
- The output path is deterministic per (dataset, symbol, date, timeframe).
- A “done set” of existing Parquet files is built at the start of ingestion to skip already-converted ZIPs.

Safety and consistency:
- Ingestion skips ZIP files that appear “in-flight” (recently modified or `.part/.tmp/.download`).
- ZIPs are validated with `zipfile.ZipFile(...).testzip()` before parsing.
- Parquet writes use local staging then atomic copy to the NAS.
- If a conversion fails, the output file is not written (or is cleaned up) and the error is logged.

--------------------------------------------------------------------
6. FAILURE MODES AND RECOVERY
--------------------------------------------------------------------

Common failure modes:
- **In-flight ZIPs**: skipped and retried on the next run.
- **Bad ZIPs**: logged to `_logs/nas_parquet_mirror_errors.jsonl` and marked failed.
- **Schema drift**: conversion errors if expected CSV columns are missing.

Recovery approach:
- Rerun ingestion for the same date range; idempotent skips will avoid overwriting good files.
- Fix or re-download corrupted ZIPs, then rerun ingestion for those dates/symbols.

--------------------------------------------------------------------
7. INCREMENTAL BEHAVIOR AND PARTIAL RECOMPUTE
--------------------------------------------------------------------

- Ingestion itself is incremental: it only converts missing Parquet files.
- The metadata pipeline detects new/changed Parquet files via manifest diffing (size/mtime).
- Downstream recompute is scoped by changed symbol-days and timeframe.

--------------------------------------------------------------------
8. DERIVED 1D GENERATION (FROM 1M)
--------------------------------------------------------------------

Derived 1d bars are generated by `archive/resample.py`:

- Input: `klines/mark/index` 1m Parquet files (`date=YYYY-MM-DD`).
- Output: `timeframe=1d` Parquet files partitioned by `month=YYYY-MM`.
- The resampler computes daily OHLC (and volume for klines) and writes one Parquet per symbol-month.

Validation expectations:
- Derived 1d OHLCV should exactly match the aggregation of 1m data for the same day.
- Spot-checks are recommended after any schema change or resample logic change.

--------------------------------------------------------------------
9. CONTRACT VS IMPLEMENTATION DETAIL
--------------------------------------------------------------------

Contractual:
- Canonical lake layout and dataset names.
- Idempotence rules for ingestion output paths.
- UTC timestamp normalization.

Implementation details (may evolve):
- ZIP validation logic and retry behavior.
- Local staging directory layout.
- Specific DuckDB `read_csv(...)` options.
