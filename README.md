# KuCoin Lake

A canonical, research-grade data lake, metadata, and universe-engineering system for KuCoin futures (and later spot) built around Parquet + DuckDB.

This project exists to support:
- Correct, reproducible quantitative research
- Deterministic data ingestion and transformation
- Multi-timeframe metadata (1m, 1d, later 1h+) without destructive side effects
- Incremental, resumable updates
- Explicit auditability (manifest, integrity checks, run logs)

It is not a trading bot.  
It is a data engineering and research foundation.

--------------------------------------------------------------------
WHAT THIS REPOSITORY DOES
--------------------------------------------------------------------

At a high level, the system handles:

1) Ingestion (input adapters)
   - Parses KuCoin bulk ZIP downloads
   - Normalizes schema and timestamps
   - Writes canonical Parquet files into a deterministic lake layout

2) Canonical Parquet data lake
   - Hive-style partitioning
   - Supports multiple datasets (klines, mark, index, funding)
   - Supports multiple timeframes side-by-side (1m + 1d today, more later)

3) Metadata layer (DuckDB)
   - File manifest (what exists, what changed)
   - Partition coverage
   - Per-symbol dataset stats
   - Alignment summaries
   - Liquidity metrics

4) Integrity hooks (optional but recommended)
   - Gap/duplicate detection for klines
   - Detection of silent data corruption

5) Universe engineering
   - Liquidity-based ranking
   - Stability via hysteresis rules
   - Research-ready universe snapshots

All components are designed to be:
- Idempotent
- Incremental
- Explicitly scoped by market and timeframe
- Safe against cross-timeframe corruption

--------------------------------------------------------------------
REPOSITORY LAYOUT
--------------------------------------------------------------------

kucoin-lake/
  archive/                  # Legacy scripts (reference only, not evolving)
  docs/                     # Canonical documentation and contracts
  kucoin_lake/              # Package code (after refactor)
  tests/                    # Tests and tiny fixture lake
  pyproject.toml
  README.md

Key directories:

archive/
  Snapshot of the original working scripts. Treated as read-only reference.

docs/
  The real specification for how the system behaves:
  - architecture.md
  - data_lake_layout.md
  - metadata_contracts.md
  - runbooks.md
  - decisions.md
  - local_download_layout.md

kucoin_lake/
  The actual implementation (refactored from files in archive/ toward modular structure).

tests/fixtures/lake/
  Tiny synthetic Parquet data that mirrors the canonical lake layout and is used
  for tests and tooling (including Codex).

--------------------------------------------------------------------
DESIGN PHILOSOPHY
--------------------------------------------------------------------

This project is deliberately built around a few hard principles:

- Correctness over performance
- Explicit over implicit
- Contracts over convenience
- Idempotence everywhere
- No silent cross-timeframe effects
- Filesystem is source of truth
- Metadata is queryable and auditable

If something behaves ambiguously, the documentation in docs/ wins.

--------------------------------------------------------------------
INTENDED USAGE (SHAPE ONLY, NOT FINAL API)
--------------------------------------------------------------------

This repository is not yet a polished CLI tool, but the intended usage looks like:

- Ingest raw downloads into the lake
- Run metadata builds for a given market and timeframe
- Run integrity checks
- Build or update a research universe

Example conceptual commands (shape only, not final API):

kucoin-lake ingest   --market futures --source ~/downloads
kucoin-lake metadata --market futures --timeframe 1m
kucoin-lake resample --market futures --from 1m --to 1d
kucoin-lake integrity --market futures --timeframe 1m
kucoin-lake universe --market futures --timeframe 1m

Exact interfaces will evolve.  
The contracts in docs/ are considered stable.

--------------------------------------------------------------------
WHO THIS IS FOR
--------------------------------------------------------------------

This project is designed for:

- Quantitative researchers
- Data engineers working on financial data
- Anyone building serious research pipelines on crypto market data
- Future-me (who will forget half of these decisions without documentation)

It is not designed for:
- Casual trading
- Plug-and-play bots
- High-frequency execution
- General web scraping

--------------------------------------------------------------------
STATUS
--------------------------------------------------------------------

This is an active refactor from a working prototype toward:

- A clean Python package structure
- Test-backed invariants
- Codex-assisted refactoring
- Explicit architectural contracts

Expect iteration.  
Do not expect backwards compatibility with pre-refactor scripts.

--------------------------------------------------------------------
CANONICAL DOCUMENTATION
--------------------------------------------------------------------

If you are trying to understand how the system truly works, start here:

- docs/architecture.md
- docs/data_lake_layout.md
- docs/metadata_contracts.md
- docs/runbooks.md
- docs/decisions.md

These documents define the system more accurately than the current code does during refactor.

--------------------------------------------------------------------
LICENSE
--------------------------------------------------------------------

Private project. Not currently licensed for redistribution.
