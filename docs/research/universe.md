# Daily Research Universe (v1)

## Purpose
Build an MVP, reproducible daily research universe for cross-sectional factor work on 1d bars. Output is a local Parquet artifact (repo-relative) and is not part of the canonical NAS lake.

## Liquidity Source
Candidates are derived from DuckDB metadata table `md.md_liquidity_daily` with these columns:
- `market`, `timeframe`, `symbol`, `date`
- `dollar_volume`, `volume`, `close_price`
- `dv_30d_median`, `liquidity_rank_30d`, `in_top_100`
- `computed_at_utc`

## No-Lookahead Rule
Universe membership for day D uses liquidity ranks computed as-of D-1 (EOD). The builder queries `md.md_liquidity_daily` rows with `date = D-1` and emits:
- `day = date + 1 day`
- `asof_date = date`

## Entry/Exit Hysteresis
Hysteresis prevents churn in membership:
- `entry_n` default = 100
- `exit_n` default = 130

State update per day:
- Add symbol if `asof_liquidity_rank_30d <= entry_n`
- Remove symbol if `asof_liquidity_rank_30d > exit_n`
- Otherwise keep previous membership
- If a symbol is missing for day D, it is removed immediately

Universe size can drift above/below 100; there is no hard cap in v1.

## Output Schema
One row per `(day, symbol)` in the universe (unless `emit_all_candidates=True`):
- `day` (date)
- `symbol`
- `asof_date` (date)
- `asof_dv_30d_median`
- `asof_liquidity_rank_30d`
- `asof_dollar_volume`
- `asof_volume`
- `asof_close_price`
- `entry_n`
- `exit_n`
- `is_weekend`

When `emit_all_candidates=True`, all candidate rows are emitted and include:
- `in_universe` (boolean)

Default output path: `research/outputs/universe_daily.parquet`

## Helper: `summarize_universe`
`summarize_universe(df)` returns a daily summary with:
- `day`
- `n_members`
- `adds`
- `drops`
- `turnover_rate` = `(adds + drops) / previous_day_members`
- `candidate_count` (only when `in_universe` is present; otherwise null)

If the input includes `in_universe`, only rows with `in_universe=True` are used.

## Metadata Sidecar
When the parquet is written, a JSON sidecar is also written:
- `universe_daily.meta.json`

It records key build parameters (market, timeframe, entry/exit, date range, and the D-1 asof rule).

## Example Jupyter Usage
```python
from pathlib import Path
from kucoin_lake.research.universe import build_universe_daily, summarize_universe

meta_db = Path("/path/to/metadata.duckdb")

df = build_universe_daily(
    meta_db_path=meta_db,
    market="futures",
    timeframe="1m",
    start_date="2024-01-01",
    end_date="2024-03-31",
    entry_n=100,
    exit_n=130,
    output_path="research/outputs/universe_daily.parquet",
    overwrite=True,
    emit_all_candidates=False,
)

summary = summarize_universe(df)
summary.head()
```

## Known Limitations / TODO
- No hard cap or target universe size
- No persistence filters (e.g., minimum days in universe)
- No incremental updates (full rebuild only)
- No additional QC beyond the metadata inputs
- No CLI integration (research-only by design)
