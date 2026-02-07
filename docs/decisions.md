# KuCoin Lake — Decisions & Rationale

This document records deliberate architectural and operational decisions for the **current `kucoin_lake` package**.

--------------------------------------------------------------------
1. OVERARCHING PHILOSOPHY
--------------------------------------------------------------------

Decision: Research correctness over micro-optimisation
Rationale:
- Silent corruption is more expensive than slower queries.
- Early-stage research benefits from correctness, auditability, and clarity.

Decision: Idempotent, resumable operations everywhere
Rationale:
- Ingest and metadata runs will fail occasionally.
- Re-running should converge without manual cleanup.

Decision: Explicit scoping beats implicit cleverness
Rationale:
- All destructive operations are scoped by (market, timeframe).
- Prevents cross-timeframe data loss.

--------------------------------------------------------------------
2. DATA LAKE LAYOUT
--------------------------------------------------------------------

Decision: Hive-style partitioning with one file per partition
Rationale:
- Deterministic discovery
- Easy manifest diffing
- Low risk of partial overwrites

Decision: `date=` for raw 1m data, `month=` for derived 1d data
Rationale:
- Raw data is naturally day-grained.
- Derived 1d avoids tiny files by month partitioning.
- Daily keys are derived from timestamps when needed.

Decision: Keep raw and derived in the same dataset namespace
Rationale:
- Derived bars are functions of raw bars.
- `timeframe=` partition cleanly separates them.

--------------------------------------------------------------------
3. MULTI-TIMEFRAME SAFETY
--------------------------------------------------------------------

Decision: Timeframe is part of all relevant metadata PKs
Rationale:
- Prevents 1m/1d cross-contamination.
- Enables safe scoped deletes.

Decision: No global deletes across timeframes
Rationale:
- A 1d rebuild must never destroy 1m metadata.

--------------------------------------------------------------------
4. FUNDING DATA HANDLING
--------------------------------------------------------------------

Decision: Store funding as non-timeframed dataset (`timeframe=''` in coverage)
Rationale:
- Funding is not naturally barred like OHLCV.

Decision: Funding participates only in logical 1m builds
Rationale:
- Avoids accidental inclusion in 1d/1h builds.

Decision: Normalize funding logically, not physically
Rationale:
- Funding is normalized to `timeframe='1m'` only in rollups/alignment.
- Avoids duplicating rows.

--------------------------------------------------------------------
5. MANIFEST AND CHANGE DETECTION
--------------------------------------------------------------------

Decision: File-scan driven incremental updates
Rationale:
- The filesystem is the source of truth.
- `iter_data_parquets(...)` scans the lake directly and populates the manifest.

Decision: Manifest is internal, not a user-provided driver
Rationale:
- Manifest-driven refresh is backlog only.
- The authoritative state is the lake itself.

Decision: Fingerprints use size + mtime by default
Rationale:
- Fast and sufficient for daily iteration.
- Strong hashes can be added later if needed.

--------------------------------------------------------------------
6. METADATA TABLE DESIGN
--------------------------------------------------------------------

Decision: Separate tables for coverage, stats, alignment, liquidity, integrity
Rationale:
- Clear grain and responsibility per table.
- Easier debugging and safe evolution.

Decision: Liquidity computed daily with rolling windows
Rationale:
- Daily liquidity is the correct granularity for universe selection.
- Rolling median stabilizes rank noise.

Decision: Rolling recomputes include pre-history
Rationale:
- Prevents boundary errors for rolling windows.

--------------------------------------------------------------------
7. EXECUTION MODEL
--------------------------------------------------------------------

Decision: One operational pipeline, different scopes
Rationale:
- First run, daily update, backfill, and gap fill are the same steps.
- Scope is controlled by symbols/date/datasets/timeframe filters.

--------------------------------------------------------------------
8. WHAT IS INTENTIONALLY DEFERRED
--------------------------------------------------------------------

Deferred:
- Distributed orchestration (Airflow/Prefect)
- Cryptographic file hashing
- Real-time ingestion
- Automated alerting on integrity failures

Rationale:
- This is a research platform.
- Correctness and iteration speed come first.

--------------------------------------------------------------------
9. CHANGE POLICY
--------------------------------------------------------------------

Any change that:
- affects lake layout
- alters metadata table grain or PKs
- changes funding participation rules
- introduces cross-timeframe side effects

MUST:
- update this document
- update `docs/architecture.md` and/or `docs/metadata_contracts.md`
- include a migration plan if data must be rebuilt
