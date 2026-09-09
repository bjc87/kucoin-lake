# KuCoin Lake — Data Lake Layout

This document defines the **canonical on-disk layout** for the KuCoin Parquet data lake. It is the contract that `kucoin_lake` code implements.

This layout is designed to support:
- deterministic discovery
- safe multi-timeframe coexistence
- idempotent ingestion + metadata
- incremental updates without cross-timeframe side effects

--------------------------------------------------------------------
1. ROOT STRUCTURE
--------------------------------------------------------------------

Canonical futures lake root (example):
```
/path/to/lake/futures/
```

**Fixture reference** (canonical miniature example):
`tests/fixtures/lake/futures`

--------------------------------------------------------------------
2. DATASETS
--------------------------------------------------------------------

Canonical dataset names:
- `klines`   : OHLCV candles
- `mark`     : mark price candles
- `index`    : index price candles
- `funding`  : funding rate snapshots

--------------------------------------------------------------------
3. TIMEFRAME SEMANTICS
--------------------------------------------------------------------

Timeframed datasets:
- `klines`, `mark`, `index`

These always appear under:
```
{dataset}/timeframe={tf}/...
```

Non-timeframed dataset:
- `funding`

Funding always appears under:
```
funding/symbol=.../date=.../data.parquet
```

In metadata, funding uses `timeframe=''` in `md_partition_coverage` and is only included in logical 1m builds.

--------------------------------------------------------------------
4. CANONICAL PATH TEMPLATES
--------------------------------------------------------------------

4.1 Raw timeframed data (daily partitioned)
```
futures/{dataset}/timeframe={tf}/symbol={symbol}/date={YYYY-MM-DD}/data.parquet
```
Example:
```
futures/klines/timeframe=1m/symbol=ZRXUSDTM/date=2025-12-31/data.parquet
```

4.2 Derived timeframed data (month partitioned)
```
futures/{dataset}/timeframe=1d/symbol={symbol}/month={YYYY-MM}/data.parquet
```
Example:
```
futures/klines/timeframe=1d/symbol=ZRXUSDTM/month=2025-12/data.parquet
```

**Critical rule**: derived 1d datasets switch the **terminal partition** to `month=`. There is no hive `date` partition for 1d data; daily keys are derived from timestamps (`CAST(ts AS DATE)`).

4.3 Funding (non-timeframed, daily partitioned)
```
futures/funding/symbol={symbol}/date={YYYY-MM-DD}/data.parquet
```

--------------------------------------------------------------------
5. PARTITION KEYS AND NAMING RULES
--------------------------------------------------------------------

Supported partition keys:
- `timeframe={tf}`
- `symbol={symbol}`
- `date={YYYY-MM-DD}`
- `month={YYYY-MM}`

Naming rules:
- All keys are lowercase.
- All values use fixed canonical formats.
- One file per partition.
- The filename is always `data.parquet`.

--------------------------------------------------------------------
6. PARQUET SCHEMA EXPECTATIONS (HIGH LEVEL)
--------------------------------------------------------------------

Schemas may evolve, but the following are expected:

`klines`:
- `ts`, `time_ms`, `open`, `high`, `low`, `close`, `volume`

`mark` / `index`:
- `ts`, `time_ms`, `open`, `high`, `low`, `close`

`funding`:
- `symbol`, `ts`, `time_ms`, `funding_rate`

Derived 1d outputs (current resampler behavior):
- `klines`: `date`, `open`, `high`, `low`, `close`, `volume`, `dollar_volume`, `vwap`, `src_rows`, `min_ts`, `max_ts`
- `mark/index`: `date`, `open`, `high`, `low`, `close`, `src_rows`, `min_ts`, `max_ts`

--------------------------------------------------------------------
7. RELATIONSHIP TO METADATA
--------------------------------------------------------------------

The filesystem layout is the **source of truth** for:
- `md_file_manifest`
- `md_partition_coverage`
- `md_symbol_dataset_stats`
- `md_alignment_summary`
- `md_liquidity_daily`
- `md_kline_integrity_day`

If two Parquet files live under different timeframe directories, they must remain isolated in metadata unless explicitly joined.

--------------------------------------------------------------------
8. EXTENSIBILITY RULES
--------------------------------------------------------------------

When adding:
- a new timeframe (e.g. `1h`)
- a new dataset
- a new market (spot)

The following must remain true:
- Existing paths remain valid.
- Existing data is not moved.
- Existing metadata is not destroyed.
- New data is additive and scoped.

--------------------------------------------------------------------
9. CANONICALITY STATEMENT
--------------------------------------------------------------------

This layout is:
- The authoritative contract for code
- The shape all tests and fixtures should emulate
- The reference agents should assume when reasoning about logic

If anything disagrees with this document, update this document to match the code (or deliberately change the code).
