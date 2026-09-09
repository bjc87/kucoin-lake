# KuCoin Lake — Validation (Phase 2)

This document defines the implemented validation behavior in `kucoin_lake.validation`.

Validation is a package-owned, contract-focused trust gate for:
- metadata reproducibility/invariants
- derived `1d` bar correctness (`1m -> 1d`)
- orchestration of both checks for operational readiness

--------------------------------------------------------------------
1. SCOPE (PHASE 2)
--------------------------------------------------------------------

Supported validators:
- `validate metadata`
- `validate derived-1d`
- `validate all` (orchestration)

Current derived scope:
- datasets: `klines`, `mark`, `index`
- transformation: `1m -> 1d` only

Out of scope:
- research universe construction logic
- research factor validation logic
- mutation of lake data or source metadata DB during validation

--------------------------------------------------------------------
2. DESIGN PRINCIPLES
--------------------------------------------------------------------

- Validation is read-only with respect to:
  - source NAS lake
  - source metadata DB
- Validation writes artifacts only to the chosen output directory.
- Package validators are canonical; notebook checks are consumers.
- Resample semantics come from `kucoin_lake.resample` (not duplicated rules).

--------------------------------------------------------------------
3. COMMANDS AND API
--------------------------------------------------------------------

CLI:
```bash
kucoin-lake validate metadata ...
kucoin-lake validate derived-1d ...
kucoin-lake validate all ...
```

API:
```python
from kucoin_lake import validate

run_meta = validate(check="metadata", nas_root=lake_root, meta_db_path=meta_db)
run_derived = validate(check="derived-1d", nas_root=lake_root, dataset="klines")
run_all = validate(check="all", nas_root=lake_root, meta_db_path=meta_db)
```

--------------------------------------------------------------------
4. VALIDATE DERIVED-1D
--------------------------------------------------------------------

Purpose:
- verify stored `1d` bars match raw `1m` aggregation semantics for scoped `(symbol, month)` tasks

Planning/source of truth:
- task discovery reuses `plan_resample_tasks(...)`
- aggregation checks use the same OHLC rollup semantics as resampling

Check groups:
- `derived.partition_shape`
- `derived.unique_symbol_date`
- `derived.aggregate_match`
- `derived.optional_rollup_fields`

What is validated:
- output layout uses `month=YYYY-MM` partitioning
- exactly one `1d` row per `(symbol, date)` key (no duplicates/null keys)
- required OHLC(+volume for klines) values match `1m` rollups
- missing output rows are `FAIL`
- extra output rows (no backing `1m` in scope) are `FAIL`
- optional lineage rollup fields (`src_rows`, `min_ts`, `max_ts`) are checked when present

Dataset field coverage:
- `klines`: `open`, `high`, `low`, `close`, `volume`
- `mark`: `open`, `high`, `low`, `close`
- `index`: `open`, `high`, `low`, `close`

Profiles:
- `smoke`:
  - evaluates counts + stable fingerprint metrics + mismatch counts
  - does not write row-level mismatch CSV artifacts
- `full`:
  - same checks as smoke
  - writes per-check mismatch artifacts under `artifacts/<check_id>/` on failures

Outputs:
- always: `run_summary.json`, `checks.jsonl`
- on `full` failures:
  - `artifacts/derived.partition_shape/...`
  - `artifacts/derived.unique_symbol_date/...`
  - `artifacts/derived.aggregate_match/...`
  - `artifacts/derived.optional_rollup_fields/...`

--------------------------------------------------------------------
5. VALIDATE ALL (ORCHESTRATION)
--------------------------------------------------------------------

Purpose:
- run metadata + derived `1d` checks together as an operational trust gate

Child validators:
- metadata validator (`metadata/`)
- one derived validator per requested derived dataset:
  - `derived_1d_klines/`
  - `derived_1d_mark/`
  - `derived_1d_index/`

Scope selection for derived validators:
- if explicit symbols are provided: use explicit symbols
- else in full-history mode: use candidate superset from `md.md_liquidity_daily`
- else in append-window mode: use near-threshold symbols from recent liquidity ranks

Full-history mode:
- active when append window bounds are not provided
- metadata runs using requested profile/scope
- derived symbol scope defaults to `select_candidate_superset_symbols(...)`

Append-window mode:
- active when both `append_date_start` and `append_date_end` are provided
- metadata validator is forced to `smoke` profile
- derived scope defaults to `select_near_threshold_symbols(...)` unless symbols are explicitly provided
- derived checks run for append dates

Candidate-superset logic:
- includes symbols that ever had `liquidity_rank_30d <= candidate_rank_threshold` in window
- source table: `md.md_liquidity_daily`

Near-threshold logic:
- source table: `md.md_liquidity_daily`
- selects symbols near a target universe cutoff (`100`) using rank banding in recent dates
- in orchestrator append mode, band is derived from threshold: `max(1, candidate_rank_threshold - 100)`

Outputs:
- top level:
  - `run_summary.json`
  - `checks.jsonl`
  - `child_runs.json`
- child outputs under stable subdirectories:
  - `metadata/...`
  - `derived_1d_<dataset>/...`

Overall status:
- fails if any child validator fails/errors

--------------------------------------------------------------------
6. OPERATIONAL TRUST CHECKLIST
--------------------------------------------------------------------

Before research refresh (full-history trust gate):
1. Ensure ingest/resample/metadata build is complete.
2. Run `kucoin-lake validate all ...` for full scope.
3. Treat `FAIL`/`ERROR` as blocking.

Daily append trust gate:
1. After daily ingest/resample/metadata update, run `kucoin-lake validate all ...` with append window bounds.
2. Review metadata + derived child statuses.
3. Treat `FAIL`/`ERROR` as blocking for daily research refresh.

--------------------------------------------------------------------
7. STATUS AND EXIT CODES
--------------------------------------------------------------------

Per-check statuses:
- `PASS`, `WARN`, `FAIL`, `ERROR`, `SKIP`

Overall precedence:
- `ERROR` > `FAIL` > `WARN` > `PASS`

CLI exit codes:
- `0`: `PASS`/`WARN`/`SKIP`
- `1`: `FAIL`
- `2`: `ERROR`

--------------------------------------------------------------------
8. BOUNDARIES
--------------------------------------------------------------------

- Validation remains contract-focused.
- Validation scope selection helpers are not research-universe construction.
- Research universe implementation remains a separate workflow (typically notebook/downstream code).
