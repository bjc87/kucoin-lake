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
   - Parses CSVs, normalizes schemas, writes Parquet to the NAS lake
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
3. MAPPING: ZIP → CANONICAL LAKE PATH
--------------------------------------------------------------------

For each ZIP file, ingestion writes exactly one Parquet file in the lake.

Mapping rules (current behavior):
- klines ZIP → `klines/timeframe=1m/symbol={SYMBOL}/date=YYYY-MM-DD/data.parquet`
- mark ZIP → `mark/timeframe=1m/symbol={SYMBOL}/date=YYYY-MM-DD/data.parquet`
- index ZIP → `index/timeframe=1m/symbol={SYMBOL}/date=YYYY-MM-DD/data.parquet`
- fundingRates ZIP → `funding/symbol={SYMBOL}/date=YYYY-MM-DD/data.parquet`

--------------------------------------------------------------------
4. SCHEMA NORMALIZATION
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
5. IDEMPOTENCE AND SAFETY RULES
--------------------------------------------------------------------

Idempotence:
- Output path is deterministic per (dataset, symbol, date, timeframe)
- A “done set” is built to skip already-converted ZIPs

Safety:
- In-flight ZIPs are skipped (recently modified or `.part/.tmp/.download`)
- ZIPs are validated via `zipfile.ZipFile(...).testzip()`
- Parquet writes use local staging then atomic copy to NAS
- Failures are logged to `_logs/nas_parquet_mirror_errors.jsonl`

--------------------------------------------------------------------
6. REQUIRED DATE BOUNDS
--------------------------------------------------------------------

`run_ingest(...)` **requires both** `startdate` and `enddate` (YYYY-MM-DD). Unbounded ingest is intentionally disallowed for repeatability.

Note: the CLI does not enforce this at argument parsing time, but the ingestion code will raise if either bound is missing.

--------------------------------------------------------------------
7. DONE-SET MODES
--------------------------------------------------------------------

`run_ingest(...)` supports:
- `scan` (default): build done set by scanning the NAS (safest, slowest)
- `targets`: build done set from the planned ZIP targets (fast for small runs)
- `skip`: do not build done set (fastest; overwrites existing outputs)

The CLI currently uses the default `scan` mode.

--------------------------------------------------------------------
8. LOCAL ROOT RESOLUTION
--------------------------------------------------------------------

`resolve_local_download_root(...)` accepts multiple shapes:
- a directory that directly contains dataset folders (`klines`, `mark`, `index`, `fundingRates`)
- a directory containing `{market}/daily/` (e.g. `futures/daily/`)
- a parent that contains `futures/daily/`

This allows you to pass either:
- `/Users/you/coding/data/kucoin/data` (contains `futures/daily/...`)
- `/Users/you/coding/data/kucoin/data/futures/daily`

--------------------------------------------------------------------
9. CLI ENTRYPOINTS
--------------------------------------------------------------------

See `docs/cli.md` for exact options. Relevant commands:
- `kucoin-lake fetch-futures`
- `kucoin-lake ingest-local-downloads-to-lake`

--------------------------------------------------------------------
10. CONTRACT VS IMPLEMENTATION DETAIL
--------------------------------------------------------------------

Contractual:
- Canonical lake layout and dataset names
- Idempotence of output paths
- UTC normalization of timestamps

Implementation details (may evolve):
- ZIP validation and retry behavior
- Local staging directory layout
- Specific DuckDB `read_csv(...)` options
