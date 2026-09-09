# KuCoin Lake — Architecture

`kucoin_lake` is a single-machine crypto futures data pipeline optimized for daily cross-sectional research on 1d bars. It is not a general-purpose data platform or a live trading system.

--------------------------------------------------------------------
1. SYSTEM OVERVIEW (CURRENT PACKAGE)
--------------------------------------------------------------------

**Canonical runtime**: the `kucoin_lake` package. There is no active `archive/` pipeline.

Core modules:
- `kucoin_lake.fetch` — download KuCoin historical ZIPs
- `kucoin_lake.ingest` — ZIP → Parquet ingestion with atomic writes
- `kucoin_lake.resample` — 1m → 1d resampling (per dataset)
- `kucoin_lake.metadata` — manifest + coverage + alignment + liquidity + integrity
- `kucoin_lake.validation` — package-owned validation runners, checks, and artifacts
- `kucoin_lake.cli` — CLI wrapper around notebook-friendly APIs
- `kucoin_lake.api` — public notebook-friendly entrypoints

**CLI entrypoint**: `kucoin-lake` (defined in `pyproject.toml`).

--------------------------------------------------------------------
2. DATA TRUTH HIERARCHY
--------------------------------------------------------------------

1. **NAS Parquet lake** = canonical truth
2. **DuckDB metadata** = rebuildable derivative
3. **Research datasets** = ephemeral

Any contradictions must be resolved in favor of the Parquet lake.

--------------------------------------------------------------------
3. OPERATIONAL SEMANTICS (SINGLE PIPELINE)
--------------------------------------------------------------------

There is one operational flow. Different run types only change **scope**.

**Pipeline steps**:
1. `fetch_futures()` — download ZIPs (optional if data already local)
2. `ingest_local_downloads_to_lake()` — ZIP → Parquet
3. `resample_1m_to_1d()` — per dataset, only if 1d data is needed
4. `build_metadata()` — manifest + coverage + alignment + liquidity + integrity
5. `validate(check="all")` — operational trust gate (metadata + derived `1d`)

Targeted validation remains available:
- `validate(check="metadata")`
- `validate(check="derived-1d")`

**Run types = same steps, different scope**:
- First run: full scope (all symbols, all dates)
- Daily update: last N days + changed files
- Backfill: constrained symbol/date/dataset/timeframe window
- Gap fill: targeted symbol-day coverage from metadata gaps

--------------------------------------------------------------------
4. INCREMENTAL METADATA (FILE-SCAN DRIVEN)
--------------------------------------------------------------------

Incremental updates are **file-scan driven**:
- `iter_data_parquets(...)` scans the lake directly (filesystem) with optional **symbol** and **date** scope.
- `md_file_manifest` is updated from that scan and used to detect changes via size/mtime.
- There is **no manifest-driven refresh** architecture in production. Any such design is backlog only.

This is intentionally simple and deterministic for research workflows.

--------------------------------------------------------------------
5. PARTITION SEMANTICS (CRITICAL CONTRACT)
--------------------------------------------------------------------

Standard partitions:
- `dataset/`
- `timeframe=`
- `symbol=`
- `date=` (raw 1m)

Derived 1d datasets switch the **terminal partition**:
- `month=` (YYYY-MM)

Example:
- Raw 1m: `.../klines/timeframe=1m/symbol=BTCUSDTM/date=2025-12-31/data.parquet`
- Derived 1d: `.../klines/timeframe=1d/symbol=BTCUSDTM/month=2025-12/data.parquet`

All metadata logic must respect this distinction. For derived data, daily keys are computed from timestamps (e.g. `CAST(ts AS DATE)`), not from hive partitions.

--------------------------------------------------------------------
6. FUNDING DATA HANDLING
--------------------------------------------------------------------

- Funding is stored without a timeframe partition: `funding/symbol=.../date=.../data.parquet`.
- In metadata, funding uses `timeframe=''` in `md_partition_coverage`.
- Funding participates only in **logical 1m** builds.
- Funding is **normalized** to `timeframe='1m'` only in rollups/alignment for 1m.

--------------------------------------------------------------------
7. VALIDATION (PHASE 2)
--------------------------------------------------------------------

Validation is implemented in-package as a first-class capability:
- CLI: `kucoin-lake validate metadata ...`
- CLI: `kucoin-lake validate derived-1d ...`
- CLI: `kucoin-lake validate all ...`
- API: `kucoin_lake.api.validate(check="metadata", ...)`
- API: `kucoin_lake.api.validate(check="derived-1d", ...)`
- API: `kucoin_lake.api.validate(check="all", ...)`

Phase 2 scope:
- metadata reproducibility + metadata invariants
- derived `1m -> 1d` validation for `klines`, `mark`, `index`
- orchestration mode that runs both checks and aggregates status

Operational modes for `validate all`:
- full-history mode (no append window): metadata + derived scope using candidate superset symbols from liquidity ranks
- append-window mode: metadata smoke + derived append checks using near-threshold symbol selection unless symbols are explicit

Validation remains read-only against the source lake and source metadata DB.
It writes artifacts (`run_summary.json`, `checks.jsonl`, child run artifacts, per-check mismatch artifacts in `full`) into an output directory.

Notebooks and ad hoc audits consume validation outputs; they are not the canonical validation implementation.

--------------------------------------------------------------------
8. UNIVERSE CONSTRUCTION (RESEARCH WORKFLOW)
--------------------------------------------------------------------

Universe construction is implemented in `kucoin_lake.research.universe` as a research workflow built on metadata tables:
- Start from `md_liquidity_daily` for liquidity ranks (rolling median + rank).
- Use `md_alignment_summary` to ensure core datasets are present and complete.
- Use `md_kline_integrity_day` to exclude symbol-days with data quality issues.
- Use `md_partition_coverage` to diagnose missing data and gaps.

Membership for day D uses D-1 liquidity ranks, with entry/exit hysteresis to reduce turnover. Validation scope helpers (`select_candidate_superset_symbols`, `select_near_threshold_symbols`) are separate and only define symbol scopes for validation runs.

The implementation retains historical symbols that exist in metadata, but it does not maintain an exchange listing/delisting registry and therefore cannot prove complete freedom from survivorship bias.

--------------------------------------------------------------------
9. FIXTURE GROUNDING
--------------------------------------------------------------------

`tests/fixtures/lake/futures` is a miniature lake of recorded KuCoin market-data samples. Generated synthetic data is used by tests that require controlled edge cases.

--------------------------------------------------------------------
10. NOTE ON CLI HELP
--------------------------------------------------------------------

`kucoin_lake.cli` defines the CLI interface. Run `python -m kucoin_lake.cli --help` or `kucoin-lake --help` in an installed environment to inspect it.
