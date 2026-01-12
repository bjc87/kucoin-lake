# KuCoin Lake — Data Lake Layout

This document defines the **canonical on-disk layout** for the KuCoin Parquet data lake.  
It is the contract the current code implements, and downstream systems (manifest, metadata, resampling, research) must align with it.

This layout is designed to support:
- deterministic file discovery
- safe multi-timeframe coexistence (1m, 1d, future 1h, etc.)
- idempotent ingestion and metadata builds
- incremental updates without destructive cross-timeframe effects

--------------------------------------------------------------------
1. ROOT STRUCTURE
--------------------------------------------------------------------

Canonical futures lake root (example):

  /Volumes/quant_data/kucoin/futures/

All examples below assume the working root is:

  futures/

Tree example (current real structure):

  futures/
    funding/
      symbol=ZRXUSDTM/
        date=2025-12-29/data.parquet
        date=2025-12-30/data.parquet

    index/
      timeframe=1m/
        symbol=ZRXUSDTM/
          date=2025-12-22/data.parquet
          date=2025-12-23/data.parquet
      timeframe=1d/
        symbol=ZRXUSDTM/
          month=2025-12/data.parquet

    klines/
      timeframe=1m/
        symbol=ZRXUSDTM/
          date=2025-12-30/data.parquet
          date=2025-12-31/data.parquet
      timeframe=1d/
        symbol=ZRXUSDTM/
          month=2025-12/data.parquet

    mark/
      timeframe=1m/
        symbol=ZRXUSDTM/
          date=2025-12-22/data.parquet
          date=2025-12-23/data.parquet
      timeframe=1d/
        symbol=ZRXUSDTM/
          month=2025-12/data.parquet

This is the canonical shape all code must assume.

--------------------------------------------------------------------
2. DATASETS
--------------------------------------------------------------------

Canonical dataset names:

- klines   : OHLCV candles
- mark     : mark price candles
- index    : index price candles
- funding  : funding rate snapshots

These names are contractual and must not change without migration.

--------------------------------------------------------------------
3. TIMEFRAME SEMANTICS
--------------------------------------------------------------------

Timeframed datasets:
- klines
- mark
- index

These datasets always appear under:

  {dataset}/timeframe={tf}/...

Supported timeframes currently:
- 1m : raw canonical resolution (source of truth)
- 1d : derived resolution (resampled from 1m)

Future timeframes (e.g. 1h, 4h) must follow the same pattern.

Non-timeframed dataset:
- funding

Funding has no timeframe partition and always appears as:

  funding/symbol=.../date=.../data.parquet

In metadata, funding is represented with:
- dataset = 'funding'
- timeframe = '' (empty string)

Funding participates only in logical 1m builds unless explicitly extended.

--------------------------------------------------------------------
4. CANONICAL PATH TEMPLATES
--------------------------------------------------------------------

4.1 Raw timeframed data (daily partitioned)

Template:

  futures/{dataset}/timeframe={tf}/symbol={symbol}/date={YYYY-MM-DD}/data.parquet

Example:

  futures/klines/timeframe=1m/symbol=ZRXUSDTM/date=2025-12-31/data.parquet

Hive partition columns exposed by DuckDB read_parquet(hive_partitioning=1):

- timeframe : string
- symbol    : string
- date      : string (must be cast to DATE when used)

Used by:
- manifest scanning
- coverage
- alignment
- liquidity
- integrity

--------------------------------------------------------------------
4.2 Derived timeframed data (month partitioned)

Derived datasets (currently 1d) use month partitioning to avoid tiny files.

Template:

  futures/{dataset}/timeframe=1d/symbol={symbol}/month={YYYY-MM}/data.parquet

Example:

  futures/klines/timeframe=1d/symbol=ZRXUSDTM/month=2025-12/data.parquet

Hive partition columns:

- timeframe : string
- symbol    : string
- month     : string (YYYY-MM)

Important:
- There is NO hive date partition here.
- Any daily logic (coverage, liquidity, alignment) must derive day keys from timestamps:
  
  CAST(ts AS DATE)

Code must never assume that a hive `date` column exists for derived data.

Derived 1d output columns (current resampler behavior):
- klines: `date`, `open`, `high`, `low`, `close`, `volume`, `dollar_volume`, `vwap`, `src_rows`, `min_ts`, `max_ts`
- mark/index: `date`, `open`, `high`, `low`, `close`, `src_rows`, `min_ts`, `max_ts`

--------------------------------------------------------------------
4.3 Funding (non-timeframed, daily partitioned)

Template:

  futures/funding/symbol={symbol}/date={YYYY-MM-DD}/data.parquet

Example:

  futures/funding/symbol=ZRXUSDTM/date=2025-12-30/data.parquet

Hive partition columns:

- symbol
- date

Metadata representation:
- dataset = 'funding'
- timeframe = '' (empty string)

--------------------------------------------------------------------
5. PARTITION KEYS AND NAMING RULES
--------------------------------------------------------------------

Supported partition keys:

- timeframe={tf}
- symbol={symbol}
- date={YYYY-MM-DD}
- month={YYYY-MM}

Naming rules:
- All keys are lowercase.
- All values use fixed canonical formats.
- One file per partition.
- The filename is always:

  data.parquet

This consistency is deliberate and relied upon by:
- filesystem iteration
- manifest diffing
- metadata correctness
- safety against accidental overwrites

--------------------------------------------------------------------
6. PARQUET SCHEMA CONTRACT (HIGH LEVEL)
--------------------------------------------------------------------

Exact schemas may evolve, but the following are required expectations.

All time series datasets must include:
- a timestamp column (e.g. ts)
  - must be castable to DuckDB TIMESTAMP
  - must represent UTC or be normalized to UTC during ingestion

Typical expectations:

klines:
- ts
- time_ms (ingestion keeps ms epoch as BIGINT)
- open
- high
- low
- close
- volume

mark / index:
- ts
- open
- high
- low
- close

funding:
- symbol
- ts (or funding_time normalized to ts)
- time_ms (ingestion keeps ms epoch as BIGINT)
- funding_rate

Downstream logic must reference columns explicitly and never rely on implicit ordering.

--------------------------------------------------------------------
7. RELATIONSHIP TO METADATA
--------------------------------------------------------------------

The filesystem layout is the **source of truth** for:

- md_file_manifest
- md_partition_coverage
- md_symbol_dataset_stats
- md_alignment_summary
- md_liquidity_daily
- integrity tables

Critical rule:

If two Parquet files live under different timeframe directories,  
they MUST remain fully isolated in metadata unless explicitly joined.

Timeframe safety is enforced by:
- partition parsing
- manifest primary keys
- metadata table primary keys
- scoped deletes and recomputes

--------------------------------------------------------------------
8. EXTENSIBILITY RULES
--------------------------------------------------------------------

When adding:
- a new timeframe (e.g. 1h)
- a new dataset
- a new market (spot)

The following must remain true:

- Existing paths must remain valid.
- Existing data must not be moved.
- Existing metadata must not be destroyed.
- New data must be additive and scoped.
- No global rebuild should be required.

Any change to this layout requires:
- updating this document
- updating metadata_contracts.md if semantics change
- considering migration impact

--------------------------------------------------------------------
9. CANONICALITY STATEMENT
--------------------------------------------------------------------

This layout is:

- The authoritative contract for code
- The shape all tests and fixtures should emulate
- The reference Codex should assume when reasoning about logic

Local download formats, vendor APIs, and temporary staging layouts are NOT contractual.

If something disagrees with this document, update this document to match the code (or adjust the code deliberately).
