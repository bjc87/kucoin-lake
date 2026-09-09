# KuCoin Lake — Local Download Layout

This document describes the **raw KuCoin download layout** prior to ingestion and how it maps into the canonical Parquet lake. It is an **input adapter contract only**.

Downstream systems (manifest, metadata, resampling, research) must rely on the canonical lake layout in `docs/data_lake_layout.md`.

--------------------------------------------------------------------
1. CURRENT LOCAL DIRECTORY STRUCTURE
--------------------------------------------------------------------

Example layout after `fetch_futures(...)`:
```
{out_root}/data/futures/daily/
  fundingRates/
    1INCHUSDTM/
      1INCHUSDTM-fundingRates-2025-12-29.zip
  index/
    0GUSDTM/
      1m/
        0GUSDTM-1m-2025-09-18.zip
  klines/
    0GUSDTM/
      1m/
        0GUSDTM-1m-2025-09-18.zip
  mark/
    0GUSDTM/
      1m/
        0GUSDTM-1m-2025-09-18.zip
```

Key observations:
- Dataset names appear at the top level (klines, mark, index, fundingRates).
- Symbols are directories under datasets.
- Timeframe is a directory for timeframed datasets.
- Date is encoded in the filename.
- Funding uses a different directory pattern (no timeframe directory).
- Files present locally are only those that were remotely listed and requested; missing days are not probed/downloaded one-by-one.

--------------------------------------------------------------------
2. MAPPING: LOCAL DOWNLOADS → CANONICAL LAKE
--------------------------------------------------------------------

Mapping rules:
- `klines/{SYMBOL}/1m/{SYMBOL}-1m-YYYY-MM-DD.zip`
  → `klines/timeframe=1m/symbol={SYMBOL}/date=YYYY-MM-DD/data.parquet`

- `mark/{SYMBOL}/1m/{SYMBOL}-1m-YYYY-MM-DD.zip`
  → `mark/timeframe=1m/symbol={SYMBOL}/date=YYYY-MM-DD/data.parquet`

- `index/{SYMBOL}/1m/{SYMBOL}-1m-YYYY-MM-DD.zip`
  → `index/timeframe=1m/symbol={SYMBOL}/date=YYYY-MM-DD/data.parquet`

- `fundingRates/{SYMBOL}/{SYMBOL}-fundingRates-YYYY-MM-DD.zip`
  → `funding/symbol={SYMBOL}/date=YYYY-MM-DD/data.parquet`

--------------------------------------------------------------------
3. LOCAL ROOT RESOLUTION
--------------------------------------------------------------------

`kucoin_lake.ingest.resolve_local_download_root(...)` accepts:
- a directory that directly contains dataset folders (`klines`, `mark`, `index`, `fundingRates`)
- a directory containing `{market}/daily/` (e.g. `futures/daily/`)
- a parent that contains `futures/daily/`

This allows you to pass either:
- `/path/to/downloads/data`
- `/path/to/downloads/data/futures/daily`

--------------------------------------------------------------------
4. INPUT ADAPTER BOUNDARY
--------------------------------------------------------------------

This layout is **not** a downstream contract. Only the canonical lake layout is.

If KuCoin changes their download format:
- Update ingestion and this document
- Do not change lake layout or downstream logic
