# KuCoin Lake — Architecture

`kucoin_lake` is a **research-grade crypto futures data lake** optimized for **daily cross-sectional factor research on 1d bars**. It is not a general-purpose data engineering platform. The architecture prioritizes **research iteration speed**, **universe construction**, and **factor validation**.

--------------------------------------------------------------------
1. SYSTEM OVERVIEW (CURRENT PACKAGE)
--------------------------------------------------------------------

**Canonical runtime**: the `kucoin_lake` package. There is no active `archive/` pipeline.

Core modules:
- `kucoin_lake.fetch` — download KuCoin historical ZIPs
- `kucoin_lake.ingest` — ZIP → Parquet ingestion with atomic writes
- `kucoin_lake.resample` — 1m → 1d resampling (per dataset)
- `kucoin_lake.metadata` — manifest + coverage + alignment + liquidity + integrity
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
7. UNIVERSE CONSTRUCTION (RESEARCH WORKFLOW)
--------------------------------------------------------------------

Universe construction is **not implemented as code in this package**. It is a research workflow built on metadata tables:
- Start from `md_liquidity_daily` for liquidity ranks (rolling median + rank).
- Use `md_alignment_summary` to ensure core datasets are present and complete.
- Use `md_kline_integrity_day` to exclude symbol-days with data quality issues.
- Use `md_partition_coverage` to diagnose missing data and gaps.

This is the intended data path for tradable universe construction in notebooks or downstream research code.

--------------------------------------------------------------------
8. FIXTURE GROUNDING
--------------------------------------------------------------------

`tests/fixtures/lake/futures` is the canonical miniature lake example. All documentation and reasoning should be consistent with this fixture layout.

--------------------------------------------------------------------
9. NOTE ON CLI HELP
--------------------------------------------------------------------

`kucoin_lake.cli` defines the complete CLI interface. In this repo environment, running `python -m kucoin_lake.cli --help` fails because `duckdb` is not installed. CLI documentation is therefore derived directly from `kucoin_lake/cli.py` (the argparse definitions).
