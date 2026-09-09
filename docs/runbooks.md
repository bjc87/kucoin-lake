# KuCoin Lake — Runbooks

Operational guidance for ingestion, resampling, metadata build, and phase-2 trust gates.

Design priorities:
- research correctness > micro-optimisation
- idempotent, resumable operations
- explicit scope (symbol/date/dataset/timeframe)
- package-owned validation as the trust gate

--------------------------------------------------------------------
0. ONE PIPELINE, TWO TRUST GATES
--------------------------------------------------------------------

Base data pipeline:
1. `fetch_futures()` (optional if ZIPs already local)
2. `ingest_local_downloads_to_lake()`
3. `resample_1m_to_1d()` (per dataset, when `1d` outputs are required)
4. `build_metadata()`

Validation trust gates:
- full-history trust gate: `kucoin-lake validate all` (no append bounds)
- daily append trust gate: `kucoin-lake validate all --append-date-start ... --append-date-end ...`

Use `validate metadata` and `validate derived-1d` for targeted diagnosis, but use `validate all` as the operational gate.

--------------------------------------------------------------------
1. BEFORE STARTING RESEARCH (FULL-HISTORY TRUST GATE)
--------------------------------------------------------------------

Run this once before kicking off a full research refresh.

1. Ensure full ingest/resample/metadata build is complete.
2. Run full trust gate:
```bash
kucoin-lake validate all \
  --nas-root <lake_root> \
  --meta-db-path <metadata.duckdb> \
  --market futures \
  --datasets klines mark index funding \
  --derived-datasets klines mark index \
  --candidate-rank-threshold 150 \
  --profile full
```
Scope behavior:
- if `--symbols`/`--symbol` are provided, those symbols are used
- otherwise derived validation symbols come from candidate-superset selection (`md.md_liquidity_daily`, `liquidity_rank_30d <= candidate_rank_threshold`)

3. Inspect top-level outputs:
   - `<output_dir>/run_summary.json`
   - `<output_dir>/checks.jsonl`
   - `<output_dir>/child_runs.json`
4. If needed, inspect child outputs:
   - `<output_dir>/metadata/...`
   - `<output_dir>/derived_1d_klines/...`
   - `<output_dir>/derived_1d_mark/...`
   - `<output_dir>/derived_1d_index/...`

Blocker policy:
- `FAIL` or `ERROR` in overall `validate all` blocks research refresh.
- Fix and rerun gate before refresh.

--------------------------------------------------------------------
2. DAILY AFTER NEW DATA ARRIVES (APPEND TRUST GATE)
--------------------------------------------------------------------

Run this after daily ingest/resample/metadata updates.

1. Build/refresh the day’s data (`ingest -> resample -> metadata`).
2. Run append-window trust gate:
```bash
kucoin-lake validate all \
  --nas-root <lake_root> \
  --meta-db-path <metadata.duckdb> \
  --append-date-start YYYY-MM-DD \
  --append-date-end YYYY-MM-DD \
  --candidate-rank-threshold 150 \
  --profile smoke
```
3. If append gate fails, rerun with `--profile full` for row-level artifacts.

Append mode behavior implemented today:
- metadata validator runs in `smoke` mode
- derived symbol scope uses near-threshold selection unless explicit symbols are provided
- near-threshold selection is based on recent `md.md_liquidity_daily` ranks around the target cutoff (`100`) using a band derived from `candidate_rank_threshold`

Blocker policy:
- `FAIL` or `ERROR` blocks daily research refresh.

--------------------------------------------------------------------
3. WHEN TO RUN TARGETED VALIDATORS
--------------------------------------------------------------------

Use targeted validators when diagnosing failures or validating one component:

Metadata only:
```bash
kucoin-lake validate metadata \
  --nas-root <lake_root> \
  --meta-db-path <metadata.duckdb> \
  --profile full
```

Derived only (single dataset):
```bash
kucoin-lake validate derived-1d \
  --nas-root <lake_root> \
  --dataset klines \
  --profile full
```

Typical reasons:
- isolate whether failure is metadata vs derived bars
- focus on one dataset or symbol subset
- collect row-level mismatch artifacts quickly

--------------------------------------------------------------------
4. FAILURE TRIAGE (PRACTICAL)
--------------------------------------------------------------------

If `validate all` fails:
1. Check top-level child statuses in `run_summary.json`.
2. Open failing child `run_summary.json` and `checks.jsonl`.
3. For `full` profile, inspect per-check artifacts under `artifacts/<check_id>/`.

Common outcomes:
- metadata child fail: metadata contracts or reproducibility mismatch
- derived child fail: `1d` output differs from `1m` aggregation, missing rows, extra rows, or key/partition issues

--------------------------------------------------------------------
5. BOUNDARIES (DO NOT CONFUSE RESPONSIBILITIES)
--------------------------------------------------------------------

- Validation is contract-focused.
- Candidate superset and near-threshold selection are validation scope helpers.
- Research universe construction is separate and not implemented by validation commands.
