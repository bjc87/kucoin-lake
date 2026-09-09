# KuCoin Lake — CLI Reference

**Grounding**
- CLI surface (commands, flags, required/optional) is verified against `kucoin-lake --help` and per-command `--help`.
- Defaults and behavior are verified against `kucoin_lake/cli.py` and the underlying API modules.
Regenerate commands:
- `kucoin-lake --help`
- `kucoin-lake <cmd> --help`
- `python -m kucoin_lake.cli --help`
- `python -m kucoin_lake.cli <cmd> --help`

**Naming Gotchas**
- Date flag names vary by command: `--start-date/--end-date`, `--date-start/--date-end`, `--startdate/--enddate`. Do not guess; check the command section.

--------------------------------------------------------------------
1. build-metadata
--------------------------------------------------------------------

**Purpose**: Build or update the DuckDB metadata database.

Required (argparse):
- `--nas-root`
- `--meta-db-path`

Options:
- `--market` (default: `futures`)
- `--datasets` (space-separated; default: `klines mark index funding`)
- `--timeframe-filter` (default: `1m`)
- `--liquidity-only` (skip coverage/alignment; compute liquidity only)
- `--completeness-threshold` (default: `0.98`)
- `--incremental-chunk-size` (default: `5000`)
- `--symbols` (comma-separated symbols)
- `--symbol` (repeatable symbol filter, may be passed multiple times)
- `--date-start` (YYYY-MM-DD, UTC)
- `--date-end` (YYYY-MM-DD, UTC)

Notes:
- `--datasets` default comes from `DEFAULT_FUTURES_DATASETS`.
- `--symbols` and repeated `--symbol` values are merged and de-duplicated in the CLI.
- `--date-start` and `--date-end` are parsed with `date.fromisoformat(...)` in the CLI.

--------------------------------------------------------------------
2. build-kline-integrity
--------------------------------------------------------------------

**Purpose**: Build or update daily kline integrity checks (1m only).

Required (argparse):
- `--nas-root`
- `--meta-db-path`

Options:
- `--market` (default: `futures`)
- `--timeframe` (default: `1m`)
- `--start-date`
- `--end-date`
- `--changed-files` (space-separated file paths)
- `--recompute` (force recompute)
- `--incremental-chunk-size` (default: `5000`)

Notes:
- `--start-date` and `--end-date` are passed through as strings (no CLI parsing).

--------------------------------------------------------------------
3. backfill-derived
--------------------------------------------------------------------

**Purpose**: Backfill derived liquidity/integrity from coverage without scanning the lake.

Required (argparse):
- `--nas-root`
- `--meta-db-path`

Options:
- `--market` (default: `futures`)
- `--timeframe` (default: `1m`)
- `--symbols` (comma-separated symbols)
- `--symbol` (repeatable symbol filter)
- `--date-start` (YYYY-MM-DD)
- `--date-end` (YYYY-MM-DD)
- `--what` (`liquidity` | `integrity` | `both`, default: `both`)
- `--incremental-chunk-size` (default: `5000`)

Notes:
- `--symbols` and repeated `--symbol` values are merged and de-duplicated in the CLI.
- `--date-start` and `--date-end` are parsed with `date.fromisoformat(...)` in the CLI.
- `--what both` expands to `liquidity` + `integrity` in the API wrapper.

--------------------------------------------------------------------
4. resample-1m-to-1d
--------------------------------------------------------------------

**Purpose**: Resample 1m bars to 1d bars (per dataset).

Required (argparse):
- `--nas-root`

Options:
- `--local-staging-dir`
- `--market` (default: `futures`)
- `--dataset` (default: `klines`)
- `--changed-files` (space-separated file paths)
- `--test-symbols` (space-separated symbols)
- `--test-months` (space-separated YYYY-MM)
- `--max-tasks`
- `--plan-only` (plan tasks, do not write)
- `--overwrite` / `--no-overwrite` (default overwrite: `True`)
- `--threads` (default: `4`)
- `--memory-limit`
- `--verbose`

Notes:
- Dataset validity is enforced by the API (`klines`, `mark`, `index`); argparse does not enforce choices.
- Safety: default `overwrite=True` will rewrite any existing 1d outputs for affected (symbol, month). Run with `--plan-only` first, and use `--no-overwrite` if you need to preserve existing outputs.
- If `--changed-files` is provided, only impacted (symbol, month) tasks are generated.
- Derived output partitions by `month=YYYY-MM`.

--------------------------------------------------------------------
5. ingest-local-downloads-to-lake
--------------------------------------------------------------------

**Purpose**: Convert local ZIP downloads into the NAS Parquet lake.

Required (argparse):
- None.

Required by API logic:
- `--startdate`
- `--enddate`

Options:
- `--assets` (space-separated symbols)
- `--timeframes` (space-separated timeframes; applies to klines/mark/index)
- `--include-klines` / `--no-klines` (default include)
- `--include-funding` / `--no-funding` (default include)
- `--include-mark` / `--no-mark` (default include)
- `--include-index` / `--no-index` (default include)
- `--local-root` (path to local downloads)
- `--verbose`

Notes:
- The CLI does not enforce date bounds, but the API raises a `ValueError` if either `startdate` or `enddate` is missing.
- Default input root is `kucoin_lake.ingest.LOCAL_ROOT` when `--local-root` is omitted.
- Output NAS root is configured in `kucoin_lake.ingest.NAS_ROOT`.

--------------------------------------------------------------------
6. fetch-futures
--------------------------------------------------------------------

**Purpose**: Fetch KuCoin futures ZIPs to local storage.

Required (argparse):
- `--out-root`
- `--symbols` (space-separated)
- `--datatype` (space-separated)

Options:
- `--timeframe` (default: `1m`)
- `--start-date` / `--end-date`
- `--days` (alternative to start/end)
- `--sleep-s` (default: `0.02`)
- `--retries` (default: `6`)
- `--backoff-s` (default: `1.0`)
- `--timeout` (two floats: connect read; default: `10 300`)
- `--dry-run`
- `--show-progress` / `--no-progress` (default show)
- `--verbose`

Notes:
- `--datatype` values are validated in the API: `klines`, `mark`, `index`, `funding`, plus the alias `fundingRates` (normalized to `funding`).
- API requires either `--days` OR both `--start-date` and `--end-date`.
- Planning is remote-listing-driven (month shards): only remotely listed keys are considered for download, and existing local files are skipped.
- Summary fields separate requested coverage from remote availability: `requested_keys`, `remote_listed`, `missing_remote`, `missing_local`, `planned`.
- Progress is two-stage when enabled: listing/planning shard progress first, then download progress for planned files.

--------------------------------------------------------------------
7. validate metadata
--------------------------------------------------------------------

**Purpose**: Validate metadata reproducibility and metadata contract invariants.

Required (argparse):
- `validate metadata --nas-root`
- `validate metadata --meta-db-path`

Options:
- `--market` (default: `futures`)
- `--datasets` (space-separated; default: `klines mark index funding`)
- `--timeframe-filter` (default: `1m`)
- `--symbols` (comma-separated symbols)
- `--symbol` (repeatable symbol filter, may be passed multiple times)
- `--date-start` (YYYY-MM-DD, UTC)
- `--date-end` (YYYY-MM-DD, UTC)
- `--output-dir`
- `--profile` (`smoke` | `full`, default: `smoke`)
- `--keep-temp-db`

Notes:
- `--symbols` and repeated `--symbol` values are merged and de-duplicated in the CLI.
- `--date-start` and `--date-end` are parsed with `date.fromisoformat(...)` in the CLI.
- `validate metadata` focuses on metadata reproducibility + metadata invariants.

--------------------------------------------------------------------
8. validate derived-1d
--------------------------------------------------------------------

**Purpose**: Validate stored derived `1d` bars against raw `1m` inputs.

Required (argparse):
- `validate derived-1d --nas-root`
- `validate derived-1d --dataset` (`klines` | `mark` | `index`)

Options:
- `--market` (default: `futures`)
- `--symbols` (comma-separated symbols)
- `--symbol` (repeatable symbol filter)
- `--date-start` (YYYY-MM-DD, UTC)
- `--date-end` (YYYY-MM-DD, UTC)
- `--month` (repeatable `YYYY-MM`)
- `--months` (comma-separated `YYYY-MM`)
- `--output-dir`
- `--profile` (`smoke` | `full`, default: `smoke`)
- `--sample-limit` (int)

Notes:
- `--symbols` and repeated `--symbol` values are merged and de-duplicated in the CLI.
- `--month` and comma-separated `--months` values are merged and de-duplicated in the CLI.
- This validator is for `1m -> 1d` checks only (phase-2 scope).

--------------------------------------------------------------------
9. validate all
--------------------------------------------------------------------

**Purpose**: Run the operational trust gate (metadata + derived `1d` orchestration).

Required (argparse):
- `validate all --nas-root`
- `validate all --meta-db-path`

Options:
- `--market` (default: `futures`)
- `--datasets` (space-separated; default: `klines mark index funding`)
- `--derived-datasets` (space-separated; default: `klines mark index`)
- `--timeframe-filter` (default: `1m`)
- `--candidate-rank-threshold` (default: `150`)
- `--append-date-start` (YYYY-MM-DD, UTC)
- `--append-date-end` (YYYY-MM-DD, UTC)
- `--symbols` (comma-separated explicit override)
- `--symbol` (repeatable explicit override)
- `--output-dir`
- `--profile` (`smoke` | `full`, default: `smoke`)
- `--sample-limit` (int)

Notes:
- `--symbols`/`--symbol` explicitly override automatic scope selection.
- Without append bounds, this runs full-history orchestration mode.
- With both append bounds, this runs append-window mode.

--------------------------------------------------------------------
10. VALIDATION EXIT CODES
--------------------------------------------------------------------

Validation commands (`validate metadata`, `validate derived-1d`, `validate all`) use:
- `0`: `PASS`/`WARN`/`SKIP`
- `1`: `FAIL`
- `2`: `ERROR`

--------------------------------------------------------------------
11. BEHAVIOR NOTES
--------------------------------------------------------------------

- Symbol filtering: build-metadata/backfill-derived accept `--symbols` (comma-separated) and `--symbol` (repeatable) and merge them; fetch-futures uses a space-separated list for `--symbols`.
- Date parsing: build-metadata/backfill-derived/validate metadata/validate derived-1d parse `--date-start/--date-end` with `date.fromisoformat(...)`; `validate all` parses append bounds via `--append-date-start/--append-date-end`; build-kline-integrity passes `--start-date/--end-date` through as strings.

--------------------------------------------------------------------
12. OUTPUT
--------------------------------------------------------------------

Most CLI commands return a dict from the underlying API and print it to stdout.
Validation commands return a `ValidationRunResult` and the CLI prints `ValidationRunResult.to_dict()`.
No output is printed if the result is `None`.

--------------------------------------------------------------------
13. EXAMPLES
--------------------------------------------------------------------

End-to-end (fetch -> ingest -> metadata):
```bash
kucoin-lake fetch-futures \
  --out-root /Users/you/coding/data/kucoin \
  --symbols BTCUSDTM \
  --datatype klines funding \
  --days 3 \
  --timeframe 1m

kucoin-lake ingest-local-downloads-to-lake \
  --startdate 2026-01-01 \
  --enddate 2026-01-03 \
  --assets BTCUSDTM \
  --timeframes 1m \
  --local-root /Users/you/coding/data/kucoin/data

kucoin-lake build-metadata \
  --nas-root /Volumes/quant_data/kucoin \
  --meta-db-path /Users/you/coding/data/kucoin/_meta/metadata_futures.duckdb \
  --market futures \
  --datasets klines mark index funding \
  --timeframe-filter 1m

kucoin-lake validate metadata \
  --nas-root /Volumes/quant_data/kucoin \
  --meta-db-path /Users/you/coding/data/kucoin/_meta/metadata_futures.duckdb \
  --market futures \
  --datasets klines mark index funding \
  --timeframe-filter 1m \
  --profile smoke
```

Derived validation (single dataset):
```bash
kucoin-lake validate derived-1d \
  --nas-root /Volumes/quant_data/kucoin \
  --market futures \
  --dataset klines \
  --profile smoke
```

Full trust gate:
```bash
kucoin-lake validate all \
  --nas-root /Volumes/quant_data/kucoin \
  --meta-db-path /Users/you/coding/data/kucoin/_meta/metadata_futures.duckdb \
  --market futures \
  --datasets klines mark index funding \
  --derived-datasets klines mark index \
  --candidate-rank-threshold 150 \
  --profile smoke
```

Daily append trust gate:
```bash
kucoin-lake validate all \
  --nas-root /Volumes/quant_data/kucoin \
  --meta-db-path /Users/you/coding/data/kucoin/_meta/metadata_futures.duckdb \
  --append-date-start 2026-03-31 \
  --append-date-end 2026-03-31 \
  --candidate-rank-threshold 150 \
  --profile full
```

Resample (safe plan first):
```bash
kucoin-lake resample-1m-to-1d \
  --nas-root /Volumes/quant_data/kucoin \
  --dataset klines \
  --plan-only \
  --no-overwrite
```

Backfill derived (single symbol, bounded window):
```bash
kucoin-lake backfill-derived \
  --nas-root /Volumes/quant_data/kucoin \
  --meta-db-path /Users/you/coding/data/kucoin/_meta/metadata_futures.duckdb \
  --symbols BTCUSDTM \
  --date-start 2026-01-01 \
  --date-end 2026-01-31 \
  --what both
```

Build kline integrity (bounded window):
```bash
kucoin-lake build-kline-integrity \
  --nas-root /Volumes/quant_data/kucoin \
  --meta-db-path /Users/you/coding/data/kucoin/_meta/metadata_futures.duckdb \
  --start-date 2026-01-01 \
  --end-date 2026-01-31
```
