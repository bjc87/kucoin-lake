# Base Research Panel

## Purpose
`build_base_panel` creates a deterministic research panel by joining the research universe to NAS 1d kline bars. This panel is the default starting point for cross-sectional factor research.

Output default:
- `research/outputs/panels/base_panel.parquet`

This output is a research artifact and is not part of the canonical lake.

## Data Sources
- Universe parquet from `kucoin_lake.research.universe`
- NAS 1d klines under:
  - `/Volumes/quant_data/kucoin/<market>/klines/timeframe=1d/symbol=<SYMBOL>/month=YYYY-MM/data.parquet`

## Join Keys
Join uses:
- `day`
- `symbol`

Universe `day` is matched to kline `date` (renamed to `day` in panel output).

## Timestamp Clarification
- `date` is the canonical daily key for economic alignment.
- `max_ts` is QA metadata from resampling lineage and is not the economic bar-end timestamp.
- `max_ts` is kept as metadata only and is not used in return timing.

## Fields
Panel includes:
- Universe timing/diagnostics: `day`, `symbol`, `asof_date`, `is_weekend`, `asof_dv_30d_median`, `asof_liquidity_rank_30d`, `asof_dollar_volume`, `asof_volume`, `asof_close_price`
- Kline fields: `open`, `high`, `low`, `close`, `volume`, `dollar_volume`, `vwap`, `src_rows`, `max_ts`
- Liquidity transform: `log_dollar_volume = np.log(dollar_volume.clip(lower=1))`
- Returns:
  - Primary percent returns: `ret_1d`, `fwd_ret_1d`, `fwd_ret_5d`, `fwd_ret_20d`
  - Secondary log returns: `log_ret_1d`, `fwd_log_ret_1d`
- Optional universe parameters if available: `entry_n`, `exit_n`

Rows are sorted by `symbol, day` before return calculations to avoid cross-symbol leakage.

## Jupyter Usage
```python
from kucoin_lake.research.panel import build_base_panel, summarize_panel

panel = build_base_panel(
    universe_path="research/outputs/universe_daily.parquet",
    nas_root="/Volumes/quant_data/kucoin",
    market="futures",
    start_date="2025-01-01",
    end_date="2025-12-31",
    output_path="research/outputs/panels/base_panel.parquet",
)

summary = summarize_panel(panel)
summary.head()
```

## Summary Helper
`summarize_panel(df)` returns daily aggregates:
- `day`
- `n_symbols`
- `mean_ret_1d`
- `mean_fwd_ret_1d`
- `mean_fwd_ret_5d`
- `mean_fwd_ret_20d`
- `missing_close_pct`
- `missing_fwd_ret_1d_pct`

## Known Limitations / TODO
- Uses only 1d bars and does not include additional microstructure features.
- Forward returns near the sample end are naturally missing.
- First available row per symbol has missing backward return by construction.
- No incremental panel append in this MVP.
