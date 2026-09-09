from __future__ import annotations

import duckdb

from kucoin_lake.constants import FUTURES_DATASETS_WITH_TIMEFRAME


def parquet_columns(con: duckdb.DuckDBPyConnection, sample_file: str) -> set[str]:
    cur = con.execute("SELECT * FROM parquet_schema(?);", [sample_file])
    rows = cur.fetchall()
    colnames = [c[0] for c in cur.description]
    if "name" in colnames:
        idx = colnames.index("name")
    elif "column_name" in colnames:
        idx = colnames.index("column_name")
    else:
        raise ValueError(f"Could not find column name field in parquet_schema: columns={colnames}")
    return {row[idx].lower() for row in rows if row[idx] is not None}


def day_key_expr_from_parquet(con: duckdb.DuckDBPyConnection, sample_file: str) -> str:
    """
    Determine the day key expression for a parquet schema.
    Prefer DATE-typed `date` if present, else CAST(ts AS DATE), else CAST(day AS DATE).
    """
    colnames = parquet_columns(con, sample_file)
    if "date" in colnames:
        return "date"
    if "ts" in colnames:
        return "CAST(ts AS DATE)"
    if "day" in colnames:
        return "CAST(day AS DATE)"
    raise ValueError(f"Could not infer day key for parquet: columns={sorted(colnames)}")


def metadata_day_key_expr(
    con: duckdb.DuckDBPyConnection,
    sample_file: str,
    *,
    dataset: str,
    timeframe_filter: str,
) -> str:
    # Raw 1m futures datasets key metadata by hive date partitions.
    if timeframe_filter == "1m" and dataset in FUTURES_DATASETS_WITH_TIMEFRAME:
        return "CAST(date AS DATE)"
    return day_key_expr_from_parquet(con, sample_file)


def coverage_ts_cols_from_parquet(con: duckdb.DuckDBPyConnection, sample_file: str) -> tuple[str, str, str]:
    colnames = parquet_columns(con, sample_file)
    if "ts" in colnames:
        return "ts", "ts", "ts"
    min_ts = "min_ts" if "min_ts" in colnames else None
    max_ts = "max_ts" if "max_ts" in colnames else None
    distinct_ts = min_ts or max_ts
    if distinct_ts is None:
        raise ValueError(f"Could not infer timestamp columns for parquet: columns={sorted(colnames)}")
    return distinct_ts, (min_ts or distinct_ts), (max_ts or distinct_ts)


def detect_kline_cols(con: duckdb.DuckDBPyConnection, sample_file: str) -> tuple[str, str, str]:
    """
    Detect (time_col, close_col, volume_col) from a sample parquet file.
    We try a few common variants.

    Returns: (time_col, close_col, volume_col)
    Raises if it can't find required columns.
    """
    # DuckDB can describe a parquet file as a relation.
    cols = con.execute(f"DESCRIBE SELECT * FROM read_parquet('{sample_file}');").fetchall()
    colnames = {c[0].lower() for c in cols}

    # Timestamp column (you've used ts everywhere so far)
    ts_candidates = ["ts", "timestamp", "time", "max_ts", "min_ts", "date", "day"]
    close_candidates = ["close", "c"]
    vol_candidates = ["volume", "vol", "qty", "size", "base_volume"]

    time_col = next((c for c in ts_candidates if c in colnames), None)
    close_col = next((c for c in close_candidates if c in colnames), None)
    vol_col = next((c for c in vol_candidates if c in colnames), None)

    if time_col is None:
        raise ValueError(f"Could not find time column in klines parquet: columns={sorted(colnames)}")
    if close_col is None:
        raise ValueError(f"Could not find close column in klines parquet: columns={sorted(colnames)}")
    if vol_col is None:
        raise ValueError(f"Could not find volume column in klines parquet: columns={sorted(colnames)}")

    return time_col, close_col, vol_col


def detect_kline_ohlc_cols(
    con: duckdb.DuckDBPyConnection,
    sample_file: str,
) -> tuple[str, str, str, str, str]:
    cols = con.execute(f"DESCRIBE SELECT * FROM read_parquet('{sample_file}');").fetchall()
    colnames = {c[0].lower() for c in cols}

    ts_candidates = ["ts", "timestamp", "time"]
    open_candidates = ["open", "o"]
    high_candidates = ["high", "h"]
    low_candidates = ["low", "l"]
    close_candidates = ["close", "c"]

    time_col = next((c for c in ts_candidates if c in colnames), None)
    open_col = next((c for c in open_candidates if c in colnames), None)
    high_col = next((c for c in high_candidates if c in colnames), None)
    low_col = next((c for c in low_candidates if c in colnames), None)
    close_col = next((c for c in close_candidates if c in colnames), None)

    if time_col is None:
        raise ValueError(f"Could not find time column in klines parquet: columns={sorted(colnames)}")
    if open_col is None or high_col is None or low_col is None or close_col is None:
        raise ValueError(f"Could not find OHLC columns in klines parquet: columns={sorted(colnames)}")

    return time_col, open_col, high_col, low_col, close_col

