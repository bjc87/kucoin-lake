# KuCoin Lake — Ingestion Contract

This document describes the **current ingestion pipeline** implemented in `kucoin_lake.fetch` and `kucoin_lake.ingest`. It is the authoritative mapping from KuCoin ZIP downloads into the canonical Parquet lake.

--------------------------------------------------------------------
1. OVERVIEW
--------------------------------------------------------------------

The ingestion pipeline has two stages:

1) **Download ZIPs** from KuCoin’s historical endpoint
   - Implemented in `kucoin_lake.fetch.fetch_futures(...)`
   - Writes ZIPs under `{out_root}/data/futures/daily/...`

2) **Convert ZIP → Parquet**
   - Implemented in `kucoin_lake.ingest.run_ingest(...)`
   - Parses CSVs, normalizes schemas, and writes Parquet to the supplied lake root
   - Uses local staging + atomic copy to avoid partial writes

The canonical output contract is the lake layout in `docs/data_lake_layout.md`.

--------------------------------------------------------------------
2. DOWNLOAD INPUTS (ZIP LAYOUT)
--------------------------------------------------------------------

KuCoin ZIP keys are built as:
```
data/futures/daily/{dataset}/{symbol}/.../{file}.zip
```

`fetch_futures(...)` writes ZIPs under:
```
{out_root}/data/futures/daily/...
```

Examples:
- klines: `data/futures/daily/klines/BTCUSDTM/1m/BTCUSDTM-1m-2026-01-01.zip`
- funding: `data/futures/daily/fundingRates/BTCUSDTM/BTCUSDTM-fundingRates-2026-01-01.zip`

--------------------------------------------------------------------
3. FETCH PLANNING + SUMMARY SEMANTICS
--------------------------------------------------------------------

`fetch_futures(...)` plans downloads from **remote object listings** (month-sharded prefixes), not by probing every theoretical day key.

Current behavior:
- Build theoretical requested coverage for each `(symbol, datatype)` in the date window.
- List remotely available keys for that window by month shard.
- Filter to in-range keys and skip keys that already exist locally under `{out_root}/data/futures/daily/...`.
- Download only keys that are both remotely listed and missing locally.

Listing failures:
- If a month shard listing fails, it is counted as an error for that run.
- The fetcher does **not** fall back to brute-force per-day GET probes for missing shards.

Summary counters distinguish coverage vs availability vs work:
- `requested_keys`: theoretical keys in requested date coverage
- `remote_listed`: keys actually listed remotely in range
- `missing_remote`: inferred as `requested_keys - remote_listed`
- `missing_local`: listed keys that are absent locally
- `planned`: actual download candidates after remote listing + local skip filtering
- `downloaded`, `skipped_exists`, `errors`: execution outcomes

`planned` is the progress-bar total and represents real download attempts.

Progress display has two stages when enabled:
- `Planning remote listings` bar: exact shard progress over `(symbol, datatype, month)` listing work (including empty and failed shards).
- `Downloading KuCoin futures` bar: progress over actual planned downloads only.

--------------------------------------------------------------------
4. MAPPING: ZIP → CANONICAL LAKE PATH
--------------------------------------------------------------------

For each ZIP file, ingestion writes exactly one Parquet file in the lake.

Mapping rules (current behavior):
- klines ZIP → `klines/timeframe=1m/symbol={SYMBOL}/date=YYYY-MM-DD/data.parquet`
- mark ZIP → `mark/timeframe=1m/symbol={SYMBOL}/date=YYYY-MM-DD/data.parquet`
- index ZIP → `index/timeframe=1m/symbol={SYMBOL}/date=YYYY-MM-DD/data.parquet`
- fundingRates ZIP → `funding/symbol={SYMBOL}/date=YYYY-MM-DD/data.parquet`

--------------------------------------------------------------------
5. SCHEMA NORMALIZATION
--------------------------------------------------------------------

Ingestion uses DuckDB `read_csv(...)` with explicit column mappings and writes Parquet.

Common behavior:
- `time` is interpreted as milliseconds since epoch
- `time_ms` is stored as BIGINT
- `ts` is `to_timestamp(time_ms / 1000.0)` (UTC)

Dataset-specific expectations:
- `klines`: `time`, `open`, `high`, `low`, `close`, `volume`
- `mark`/`index`: `time`, `open`, `high`, `low`, `close`
- `fundingRates`: `symbol`, `time`, `fundingRate`

--------------------------------------------------------------------
6. IDEMPOTENCE AND SAFETY RULES
--------------------------------------------------------------------

Idempotence:
- Output path is deterministic per (dataset, symbol, date, timeframe)
- A “done set” is built to skip already-converted ZIPs

Safety:
- In-flight ZIPs are skipped (recently modified or `.part/.tmp/.download`)
- ZIPs are validated via `zipfile.ZipFile(...).testzip()`
- Parquet writes use the supplied local staging directory, then an atomic copy to the lake
- Failures are logged to `_logs/nas_parquet_mirror_errors.jsonl`

--------------------------------------------------------------------
7. REQUIRED DATE BOUNDS
--------------------------------------------------------------------

`run_ingest(...)` **requires both** `startdate` and `enddate` (YYYY-MM-DD). Unbounded ingest is intentionally disallowed for repeatability.

The CLI enforces both bounds at argument parsing time.

--------------------------------------------------------------------
8. DONE-SET MODES
--------------------------------------------------------------------

`run_ingest(...)` supports:
- `scan` (default): build done set by scanning the lake (safest, slowest)
- `targets`: build done set from the planned ZIP targets (fast for small runs)
- `skip`: do not build done set (fastest; overwrites existing outputs)

The CLI currently uses the default `scan` mode.

--------------------------------------------------------------------
9. LOCAL ROOT RESOLUTION
--------------------------------------------------------------------

`resolve_local_download_root(...)` accepts multiple shapes:
- a directory that directly contains dataset folders (`klines`, `mark`, `index`, `fundingRates`)
- a directory containing `{market}/daily/` (e.g. `futures/daily/`)
- a parent that contains `futures/daily/`

This allows you to pass either:
- `/path/to/downloads` (contains `futures/daily/...`)
- `/path/to/downloads/futures/daily`

The supported CLI and public API require explicit download, lake, and local-staging paths. Environment-backed generic defaults remain only for legacy module helpers; importing the package does not create directories.

--------------------------------------------------------------------
10. CLI ENTRYPOINTS
--------------------------------------------------------------------

See `docs/cli.md` for exact options. Relevant commands:
- `kucoin-lake fetch-futures`
- `kucoin-lake ingest-local-downloads-to-lake`

--------------------------------------------------------------------
11. CONTRACT VS IMPLEMENTATION DETAIL
--------------------------------------------------------------------

Contractual:
- Canonical lake layout and dataset names
- Idempotence of output paths
- UTC normalization of timestamps

Implementation details (may evolve):
- ZIP validation and retry behavior
- Local staging directory layout
- Specific DuckDB `read_csv(...)` options
