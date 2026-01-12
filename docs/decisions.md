# KuCoin Lake — Decisions & Rationale

This document records **deliberate architectural and operational decisions** made during the design of the KuCoin Parquet lake, metadata system, and universe engineering pipeline.

Its purpose is to:
- prevent re-litigating settled decisions
- give future-you (and tools like Codex) clear constraints
- explain *why* things are the way they are, not just *what* they are

Unless explicitly stated, these decisions are considered **final for v1**.

--------------------------------------------------------------------
1. OVERARCHING PHILOSOPHY
--------------------------------------------------------------------

Decision: Research correctness over micro-optimisation  
Rationale:
- Silent data corruption is far more expensive than slow queries.
- Early-stage research benefits more from correctness, auditability, and clarity than from shaving milliseconds.
- Performance tuning can come later once invariants are stable.

Decision: Idempotent, resumable operations everywhere  
Rationale:
- Data ingestion and metadata builds will fail occasionally (network, NAS, code changes).
- Re-running the same command should converge to the same state without manual cleanup.
- Enables safe backfills and incremental updates.

Decision: Explicit scoping beats implicit cleverness  
Rationale:
- All destructive operations are scoped by (market, timeframe).
- All inclusion rules (e.g. funding participation) are explicit.
- Avoids “magical” behavior that becomes dangerous as the system grows.

--------------------------------------------------------------------
2. DATA LAKE LAYOUT
--------------------------------------------------------------------

Decision: Hive-style partitioning with one file per partition  
Rationale:
- Simple, deterministic discovery.
- Easy to diff via manifest.
- Avoids accidental overwrites or partial updates.

Decision: Use date partitions for raw 1m data  
Rationale:
- Natural unit of ingestion and backfill.
- Aligns with most research workflows and daily sanity checks.
- Keeps file sizes manageable.

Decision: Use month partitions for derived 1d data  
Rationale:
- 1d bars would otherwise create too many tiny files.
- Month is a natural grouping for derived daily bars.
- DuckDB can still derive daily keys from timestamps.

Decision: Keep raw and derived data in the same dataset namespace  
Rationale:
- Derived bars are a function of raw bars and belong conceptually to the same dataset.
- Timeframe partition cleanly separates raw vs derived.
- Avoids proliferation of dataset names (e.g. klines_1d).

--------------------------------------------------------------------
3. MULTI-TIMEFRAME SUPPORT
--------------------------------------------------------------------

Decision: Multiple timeframes may coexist in the lake  
Rationale:
- 1m is the canonical source of truth.
- Derived 1d (and later 1h) are needed for research and validation.
- Coexistence avoids repeated resampling and supports cross-timeframe analysis.

Decision: Timeframe must be part of all relevant metadata PKs  
Rationale:
- Prevents accidental blending of 1m and 1d statistics.
- Makes destructive operations safe by construction.
- Scales cleanly to additional timeframes.

Decision: No global deletes across timeframes  
Rationale:
- A 1d rebuild must not destroy 1m metadata.
- Accidental global deletes are one of the highest-risk failure modes.
- Scoping deletes by timeframe is cheap insurance.

--------------------------------------------------------------------
4. FUNDING DATA HANDLING
--------------------------------------------------------------------

Decision: Store funding as non-timeframed dataset (timeframe='')  
Rationale:
- Funding is not naturally “barred” at a timeframe like OHLCV.
- Avoids pretending funding is a 1m bar series when it is not.
- Keeps storage honest to the source data.

Decision: Funding participates only in logical 1m builds  
Rationale:
- Funding is aligned most naturally to raw 1m data.
- Including funding in 1d or higher timeframes would require additional aggregation logic and design decisions.
- Excluding funding from non-1m builds avoids accidental leakage.

Decision: Normalize funding logically, not physically  
Rationale:
- Funding may be aligned to 1m via SQL expressions when needed.
- Avoids duplicating funding rows under timeframe='1m'.
- Prevents double counting and destructive deletes.

--------------------------------------------------------------------
5. MANIFEST AND CHANGE DETECTION
--------------------------------------------------------------------

Decision: Use a file-level manifest table  
Rationale:
- Filesystem state is the ultimate source of truth.
- Detecting new/changed files enables incremental metadata builds.
- Avoids re-scanning and recomputing everything unnecessarily.

Decision: Path is the hard primary key for manifest  
Rationale:
- Paths uniquely identify physical files.
- Partition metadata can be derived from paths.
- Prevents collisions even if partition parsing logic changes.

Decision: Fingerprints are lightweight (mtime + size) by default  
Rationale:
- Fast enough for daily operation.
- Strong hashes can be added later if needed.
- False negatives (missed change) are rare and detectable via integrity checks.

--------------------------------------------------------------------
6. METADATA TABLE DESIGN
--------------------------------------------------------------------

Decision: Separate tables for coverage, stats, alignment, liquidity  
Rationale:
- Each table has a clear grain and responsibility.
- Simplifies reasoning and debugging.
- Avoids “one giant metadata table” that is hard to evolve.

Decision: Coverage is day-grained even for month-partitioned data  
Rationale:
- Alignment and universe decisions are day-based.
- Month partitions are a storage optimization, not a semantic one.
- Day-grained coverage avoids downstream hacks.

Decision: Liquidity is computed daily with rolling windows  
Rationale:
- Daily liquidity is the right granularity for universe selection.
- Rolling medians stabilize rank noise.
- Daily ranks are easy to reason about and audit.

Decision: Rolling metrics must include pre-history on incremental runs  
Rationale:
- Without pre-history, rolling windows produce incorrect boundary values.
- Silent rolling-window errors are extremely dangerous for research.
- Slightly wider recompute ranges are acceptable.

--------------------------------------------------------------------
7. INTEGRITY CHECKS
--------------------------------------------------------------------

Decision: Add explicit integrity tables rather than failing hard  
Rationale:
- Data issues are often localized (symbol-day).
- Blocking the entire pipeline is counterproductive during research.
- Storing integrity events allows flexible downstream handling.

Decision: Focus integrity checks on 1m klines first  
Rationale:
- 1m klines are the foundation for most derived data and features.
- Most silent corruption originates at the base resolution.
- Derived data issues usually trace back to 1m.

Decision: Integrity tables are append/update, not ephemeral  
Rationale:
- Integrity history is useful for diagnosing regressions.
- Enables trend analysis (e.g. gap rates over time).
- Supports exclusion logic in universe engineering.

--------------------------------------------------------------------
8. RUN AUDITING
--------------------------------------------------------------------

Decision: Add an explicit md_meta_runs table  
Rationale:
- Metadata builds change over time as code evolves.
- When results change, you need to know what ran, when, and with what inputs.
- Debugging without a run log is slow and error-prone.

Decision: Record counts and parameters, not raw data  
Rationale:
- Keeps audit logs lightweight.
- Avoids leaking sensitive paths unnecessarily.
- Still sufficient to reason about behavior.

--------------------------------------------------------------------
9. UNIVERSE ENGINEERING
--------------------------------------------------------------------

Decision: Universe selection is downstream of integrity + liquidity  
Rationale:
- Liquidity alone is insufficient if data is corrupted.
- Integrity checks prevent bad data from contaminating research.
- Clear layering improves explainability.

Decision: Use hysteresis to stabilize universe membership  
Rationale:
- Daily rank thresholds are noisy.
- Frequent in/out churn harms research stability and realism.
- Hysteresis produces a more realistic tradable universe.

Decision: Universe may be timeframe-specific  
Rationale:
- Liquidity and tradability can differ by timeframe.
- Avoids assuming 1m universe is valid for 1d or 1h by default.

--------------------------------------------------------------------
10. DEVELOPMENT AND TOOLING
--------------------------------------------------------------------

Decision: Keep working code in `archive/` while a future package refactor is planned  
Rationale:
- The operational pipeline currently lives in `archive/` scripts.
- `kucoin-lake/` is a placeholder for a future refactor once invariants are locked.
- Avoids premature refactors that could change behavior.

Decision: Preserve legacy scripts under archive/  
Rationale:
- Provides a known-good behavioral reference.
- Reduces fear during refactors.
- Allows diffing outputs during migration.

Decision: Codex cloud for refactors, local execution for runs  
Rationale:
- Avoids giving agents access to the full filesystem.
- Keeps destructive execution under human control.
- Still benefits from agent-assisted multi-file refactors.

Decision: No Docker requirement for v1  
Rationale:
- Execution environment is simple and controlled.
- Docker adds friction and NAS-mount complexity.
- Can be added later if reproducibility needs increase.

--------------------------------------------------------------------
11. WHAT IS INTENTIONALLY DEFERRED
--------------------------------------------------------------------

Deferred:
- Distributed execution / orchestration (Airflow, Prefect)
- Strong cryptographic file hashing
- Real-time ingestion
- Automated alerting on integrity failures
- Production-grade SLAs

Rationale:
- This is a research platform.
- Correctness and clarity come first.
- Deferred items can be added once workflows stabilize.

--------------------------------------------------------------------
12. CHANGE POLICY
--------------------------------------------------------------------

Any change that:
- affects lake layout
- alters metadata table grain or PKs
- changes funding participation rules
- introduces cross-timeframe side effects

MUST:
- update this document
- update architecture.md and/or metadata_contracts.md as needed
- include a clear migration plan if data must be rebuilt

This document is the canonical record of design intent for the project.
