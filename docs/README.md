# KuCoin Lake — Docs Index

Start here: this file is the entry point for new developers and agents.

This docs set is aligned to the **current refactored `kucoin_lake` package**. It is the authoritative description of how the system behaves today.

**Data Truth Hierarchy**
1. NAS Parquet lake = canonical truth
2. DuckDB metadata = rebuildable derivative
3. Research datasets = ephemeral

**Canonical Jupyter Workflow (recommended)**
```
fetch_futures()
ingest_local_downloads_to_lake()
resample_1m_to_1d()
build_metadata()
validate(check="all")
```
Resampling is executed **per dataset** (`klines`, `mark`, `index`) as needed.

**Navigation**
- `docs/architecture.md` — System overview, invariants, and operational model
- `docs/cli.md` — CLI commands and options (from `kucoin_lake.cli`)
- `docs/data_lake_layout.md` — Canonical lake contract and partitions
- `docs/ingestion.md` — Fetch + ingest behavior and invariants
- `docs/local_download_layout.md` — Raw download layout (input adapter only)
- `docs/metadata_contracts.md` — Metadata tables and semantics
- `docs/validation.md` — Phase-2 validation surface, trust gates, checks, and artifacts
- `docs/runbooks.md` — Operational trust-gate runbooks (full-history and daily append)
- `docs/decisions.md` — Rationale for design choices

**Fixture Reference**
`tests/fixtures/lake/futures` is the canonical miniature lake layout used by tests and examples. It mirrors the real partition contract and is the recommended grounding example when reasoning about paths and partitions.
