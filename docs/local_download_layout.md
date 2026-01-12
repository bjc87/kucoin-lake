# KuCoin Lake — Local Download Layout

This document describes the structure of **raw KuCoin downloads prior to ingestion** and how they map into the canonical Parquet lake. It exists to document the **input adapter boundary** for this repository.

Important distinction:
- The **local download layout** documented here is an *input format* and may change if the exchange changes formats.
- The **canonical contract** for all downstream systems (manifest, metadata, integrity, resampling, universe, research) is the NAS-style lake layout documented in:
  docs/data_lake_layout.md

In other words:
- Ingestion code MAY depend on this document.
- All other code MUST depend only on the canonical lake layout.

--------------------------------------------------------------------
1. PURPOSE AND SCOPE
--------------------------------------------------------------------

This layout represents:
- ZIP files downloaded directly from KuCoin’s bulk historical endpoints
- Vendor-specific naming conventions
- Directory structures chosen for convenience during download

It does NOT represent:
- A stable contract for downstream logic
- A format guaranteed to remain stable over time
- The shape used by metadata or research logic

The ingestion layer is responsible for translating this layout into the canonical lake layout.

--------------------------------------------------------------------
2. CURRENT LOCAL DIRECTORY STRUCTURE
--------------------------------------------------------------------

Current example layout (after `fetch_futures(...)`):

└── ~/coding/data/kucoin/data/futures/daily
    ├── fundingRates
    │   ├── 1INCHUSDTM
    │   │   ├── 1INCHUSDTM-fundingRates-2025-12-29.zip
    │   │   ├── 1INCHUSDTM-fundingRates-2025-12-30.zip
    │   │   └── 1INCHUSDTM-fundingRates-2025-12-31.zip
    │
    ├── index
    │   ├── 0GUSDTM
    │   │   └── 1m
    │   │       ├── 0GUSDTM-1m-2025-09-18.zip
    │   │       └── 0GUSDTM-1m-2025-09-19.zip
    │
    ├── klines
    │   ├── 0GUSDTM
    │   │   └── 1m
    │   │       ├── 0GUSDTM-1m-2025-09-18.zip
    │   │       └── 0GUSDTM-1m-2025-09-19.zip
    │
    └── mark
        ├── 0GUSDTM
        │   └── 1m
        │       ├── 0GUSDTM-1m-2025-09-18.zip
        │       └── 0GUSDTM-1m-2025-09-19.zip

Key observations:
- Dataset names appear at the top level (klines, mark, index, fundingRates).
- Symbols are directories under datasets.
- Timeframe (e.g. 1m) is a directory for timeframed datasets.
- Date is encoded in the filename.
- Funding uses a different directory pattern (no timeframe directory).

This structure is treated as **input only**.

--------------------------------------------------------------------
3. DATASETS AND SEMANTICS
--------------------------------------------------------------------

Local dataset names:
- klines         : OHLCV candles
- mark           : mark price candles
- index          : index price candles
- fundingRates   : funding rate snapshots

Only the lake dataset names are considered canonical:
- klines
- mark
- index
- funding

The ingestion layer maps:
- fundingRates  → funding

No other code should depend on "fundingRates" as a dataset name.

--------------------------------------------------------------------
4. MAPPING: LOCAL DOWNLOADS → CANONICAL LAKE
--------------------------------------------------------------------

Each ZIP file is parsed, normalized, and written into a deterministic Parquet path in the lake.

Mapping rules:

Local path:
  klines/{SYMBOL}/1m/{SYMBOL}-1m-YYYY-MM-DD.zip
Maps to lake path:
  klines/timeframe=1m/symbol={SYMBOL}/date=YYYY-MM-DD/data.parquet

Local path:
  mark/{SYMBOL}/1m/{SYMBOL}-1m-YYYY-MM-DD.zip
Maps to lake path:
  mark/timeframe=1m/symbol={SYMBOL}/date=YYYY-MM-DD/data.parquet

Local path:
  index/{SYMBOL}/1m/{SYMBOL}-1m-YYYY-MM-DD.zip
Maps to lake path:
  index/timeframe=1m/symbol={SYMBOL}/date=YYYY-MM-DD/data.parquet

Local path:
  fundingRates/{SYMBOL}/{SYMBOL}-fundingRates-YYYY-MM-DD.zip
Maps to lake path:
  funding/symbol={SYMBOL}/date=YYYY-MM-DD/data.parquet

Notes:
- `fetch_futures(...)` downloads under `out_root/data/futures/daily/...` because KuCoin keys are prefixed by `data/`.
  Example: if `out_root=/Users/you/coding/data/kucoin`, downloads land under
  `/Users/you/coding/data/kucoin/data/futures/daily/...`.
- `run_ingest(...)` expects `local_root` to point at the directory that contains `futures/` (typically the `.../data` folder shown above).
- The local directory names are not treated as stable contracts.
- Filenames are parsed only to extract symbol, timeframe, and date.
- The canonical output path is always determined by lake rules, not by local layout.

--------------------------------------------------------------------
5. TIMESTAMPS AND NORMALIZATION
--------------------------------------------------------------------

Assumptions applied during ingestion:

- Timestamps in downloaded files are interpreted as UTC (or explicitly normalized to UTC during parsing).
- ZIP contents are normalized before Parquet writing:
  - column names may be renamed
  - types may be coerced
  - malformed rows may be dropped
- The schema inside raw ZIP files is NOT considered stable.

Only the Parquet schema written into the lake is considered contractual for:
- metadata generation
- integrity checks
- resampling
- universe construction
- research workflows

--------------------------------------------------------------------
6. INPUT ADAPTER BOUNDARY
--------------------------------------------------------------------

This repository contains two conceptual layers:

1) Input adapters (ingestion)
   - Allowed to depend on this document
   - Responsible for handling vendor-specific quirks
   - Responsible for mapping into canonical lake layout

2) Core logic (everything downstream)
   - Must NOT depend on this document
   - Must depend only on the canonical lake layout
   - Includes:
     - manifest iteration
     - metadata builders
     - integrity checks
     - resampling
     - universe engineering
     - research queries

Design intent:
- Vendor instability is isolated to the ingestion layer.
- Downstream systems remain stable even if KuCoin changes download formats.

--------------------------------------------------------------------
7. DESIGN INTENT
--------------------------------------------------------------------

This document is intentionally:

- Descriptive, not prescriptive
- Flexible, not strict
- Focused on documentation rather than contracts

If KuCoin changes their download format:
- This document should be updated.
- The ingestion layer should be updated.
- The lake layout and downstream systems should remain unchanged.

The lake layout is the true contract.
This layout is simply an adapter input description.
