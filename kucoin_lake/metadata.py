from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
import glob as globlib
from pathlib import Path
from typing import Iterable, Optional, Sequence

import duckdb

from kucoin_lake.constants import DEFAULT_FUTURES_DATASETS, FUTURES_DATASETS_WITH_TIMEFRAME
from kucoin_lake.manifest import (
    FileInfo,
    get_new_or_changed_files,
    iter_data_parquets,
    upsert_manifest,
    utc_now_iso,
)
from kucoin_lake.paths import dataset_glob
from kucoin_lake.util import expected_rows_for_day

DEFAULT_META_SUBDIR = "_meta"
DEFAULT_META_DBNAME = "metadata.duckdb"


def default_local_meta_db_path(market: str) -> Path:
    base = Path.home() / "coding" / "data" / "kucoin" / "_meta"
    base.mkdir(parents=True, exist_ok=True)
    return base / f"metadata_{market}.duckdb"


DDL = """
CREATE SCHEMA IF NOT EXISTS md;

-- Market-aware file manifest (future-proof for spot)
CREATE TABLE IF NOT EXISTS md.md_file_manifest (
    file_rel         VARCHAR PRIMARY KEY,
    market           VARCHAR NOT NULL,
    dataset          VARCHAR NOT NULL,
    file_path        VARCHAR NOT NULL,
    file_size_bytes  UBIGINT NOT NULL,
    mtime_ns         UBIGINT NOT NULL,
    first_seen_utc   TIMESTAMP NOT NULL,
    last_seen_utc    TIMESTAMP NOT NULL
);

-- Market-aware coverage at day partition grain
CREATE TABLE IF NOT EXISTS md.md_partition_coverage (
    market              VARCHAR NOT NULL,
    dataset             VARCHAR NOT NULL,
    symbol              VARCHAR NOT NULL,
    date                DATE    NOT NULL,
    timeframe           VARCHAR NOT NULL,

    num_files           UBIGINT,
    num_rows            UBIGINT,
    distinct_minute_cnt UBIGINT,
    min_ts              TIMESTAMP,
    max_ts              TIMESTAMP,

    expected_rows       INTEGER,
    completeness_ratio  DOUBLE,
    is_suspect          BOOLEAN,

    computed_at_utc     TIMESTAMP NOT NULL,

    PRIMARY KEY (market, dataset, symbol, date, timeframe)
);

CREATE TABLE IF NOT EXISTS md.md_kline_integrity_day (
    market                VARCHAR NOT NULL,
    symbol                VARCHAR NOT NULL,
    date                  DATE    NOT NULL,
    timeframe             VARCHAR NOT NULL,

    num_rows              UBIGINT,
    distinct_minute_cnt   UBIGINT,
    gap_rows_vs_expected  BIGINT,
    duplicate_rows        BIGINT,

    high_lt_low_cnt       UBIGINT,
    nonpositive_price_cnt UBIGINT,
    max_abs_ret_1m        DOUBLE,

    computed_at_utc       TIMESTAMP NOT NULL,

    PRIMARY KEY (market, symbol, date, timeframe)
);

CREATE TABLE IF NOT EXISTS md.md_symbol_dataset_stats (
    market             VARCHAR NOT NULL,
    dataset            VARCHAR NOT NULL,
    symbol             VARCHAR NOT NULL,
    timeframe          VARCHAR NOT NULL,

    min_date           DATE,
    max_date           DATE,
    min_ts             TIMESTAMP,
    max_ts             TIMESTAMP,

    days_present       UBIGINT,
    days_complete      UBIGINT,
    pct_days_complete  DOUBLE,

    avg_rows_per_day   DOUBLE,

    last_refreshed_utc TIMESTAMP NOT NULL,

    PRIMARY KEY (market, dataset, symbol, timeframe)
);

CREATE TABLE IF NOT EXISTS md.md_alignment_summary (
    market               VARCHAR NOT NULL,
    timeframe            VARCHAR NOT NULL,
    symbol               VARCHAR NOT NULL,
    date                 DATE    NOT NULL,

    has_klines            BOOLEAN,
    has_mark              BOOLEAN,
    has_index             BOOLEAN,
    has_funding           BOOLEAN,

    all_core_present      BOOLEAN,
    core_missing_mask     VARCHAR,
    core_completeness_min DOUBLE,

    computed_at_utc       TIMESTAMP NOT NULL,

    PRIMARY KEY (market, timeframe, symbol, date)
);

CREATE TABLE IF NOT EXISTS md.md_liquidity_daily (
    market             VARCHAR NOT NULL,
    timeframe          VARCHAR NOT NULL,
    symbol             VARCHAR NOT NULL,
    date               DATE    NOT NULL,

    dollar_volume      DOUBLE,
    volume             DOUBLE,
    close_price        DOUBLE,

    dv_30d_median      DOUBLE,
    liquidity_rank_30d INTEGER,
    in_top_100         BOOLEAN,

    computed_at_utc    TIMESTAMP NOT NULL,

    PRIMARY KEY (market, timeframe, symbol, date)
);
"""


def connect_meta_db(db_path: Path) -> duckdb.DuckDBPyConnection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(db_path.as_posix())
    con.execute("PRAGMA threads=4;")
    con.execute("PRAGMA enable_object_cache=true;")
    con.execute(DDL)
    return con


def is_first_run(con: duckdb.DuckDBPyConnection, market: str) -> bool:
    n = con.execute("SELECT COUNT(*) FROM md.md_partition_coverage WHERE market = ?;", [market]).fetchone()[0]
    return int(n) == 0


def bulk_refresh_coverage_first_run(
    con: duckdb.DuckDBPyConnection,
    nas_root: Path,
    market: str,
    datasets: Iterable[str],
    completeness_threshold: float,
    timeframe_filter: str,
) -> None:
    now = utc_now_iso()

    # funding uses timeframe='' - don't delete funding if not doing 1m data
    if timeframe_filter == "1m":
        con.execute("DELETE FROM md.md_partition_coverage WHERE market = ? AND timeframe IN ('1m','');", [market])
    else:
        con.execute("DELETE FROM md.md_partition_coverage WHERE market = ? AND timeframe = ?;", [market, timeframe_filter])

    datasets = list(datasets)
    datasets_sorted = sorted(datasets, key=lambda d: 0 if d not in FUTURES_DATASETS_WITH_TIMEFRAME else 1)

    for ds in datasets_sorted:
        if ds not in FUTURES_DATASETS_WITH_TIMEFRAME and timeframe_filter != "1m":
            continue
        if ds in FUTURES_DATASETS_WITH_TIMEFRAME:
            glob = dataset_glob(nas_root, market, ds, timeframe=timeframe_filter)
            expected = expected_rows_for_day(timeframe_filter)
            sample_paths = glob and globlib.glob(glob)
            if not sample_paths:
                continue
            day_key_expr = _day_key_expr_from_parquet(con, sample_paths[0])
            distinct_ts_col, min_ts_col, max_ts_col = _coverage_ts_cols_from_parquet(con, sample_paths[0])

            con.execute(f"""
            INSERT INTO md.md_partition_coverage
            SELECT
                '{market}' AS market,
                '{ds}' AS dataset,
                symbol,
                {day_key_expr} AS date,
                '{timeframe_filter}'::VARCHAR AS timeframe,
                COUNT(DISTINCT filename)::UBIGINT AS num_files,
                COUNT(*)::UBIGINT AS num_rows,
                COUNT(DISTINCT date_trunc('minute', {distinct_ts_col}))::UBIGINT AS distinct_minute_cnt,
                MIN({min_ts_col}) AS min_ts,
                MAX({max_ts_col}) AS max_ts,
                {expected if expected is not None else 'NULL'}::INTEGER AS expected_rows,
                CASE WHEN {expected if expected is not None else 'NULL'} IS NULL
                    THEN NULL
                    ELSE COUNT(*)::DOUBLE / {expected}
                END AS completeness_ratio,
                CASE WHEN {expected if expected is not None else 'NULL'} IS NULL
                    THEN NULL
                    ELSE (COUNT(*)::DOUBLE / {expected}) < {completeness_threshold}
                END AS is_suspect,
                CAST('{now}' AS TIMESTAMP) AS computed_at_utc
            FROM read_parquet('{glob}', hive_partitioning=1, filename=1)
            WHERE timeframe = '{timeframe_filter}'
            GROUP BY symbol, {day_key_expr}, timeframe;
            """)
        else:
            glob = dataset_glob(nas_root, market, ds)
            con.execute(
                f"""
                INSERT INTO md.md_partition_coverage
                SELECT
                    '{market}' AS market,
                    '{ds}' AS dataset,
                    symbol,
                    CAST(ts AS DATE) AS day,
                    ''::VARCHAR AS timeframe,
                    COUNT(DISTINCT filename)::UBIGINT AS num_files,
                    COUNT(*)::UBIGINT AS num_rows,
                    COUNT(DISTINCT date_trunc('minute', ts))::UBIGINT AS distinct_minute_cnt,
                    MIN(ts) AS min_ts,
                    MAX(ts) AS max_ts,
                    NULL::INTEGER AS expected_rows,
                    NULL::DOUBLE AS completeness_ratio,
                    NULL::BOOLEAN AS is_suspect,
                    CAST('{now}' AS TIMESTAMP) AS computed_at_utc
                FROM read_parquet('{glob}', hive_partitioning=1, filename=1)
                GROUP BY symbol, day;
                """
            )


def chunked(seq: Sequence[str], n: int):
    for i in range(0, len(seq), n):
        yield seq[i : i + n]


def incremental_refresh_coverage_for_files(
    con: duckdb.DuckDBPyConnection,
    market: str,
    changed_files: Sequence[FileInfo],
    completeness_threshold: float,
    timeframe_filter: str,
    chunk_size: int,
) -> None:
    if not changed_files:
        return

    now = utc_now_iso()
    by_ds: dict[str, list[FileInfo]] = {}
    for f in changed_files:
        by_ds.setdefault(f.dataset, []).append(f)

    for ds, files in by_ds.items():
        paths = [f.file_path for f in files]

        con.execute("CREATE TEMP TABLE _stg_cov AS SELECT * FROM md.md_partition_coverage WHERE 1=0;")

        day_key_expr = None
        distinct_ts_col = None
        min_ts_col = None
        max_ts_col = None
        if ds in FUTURES_DATASETS_WITH_TIMEFRAME and paths:
            day_key_expr = _day_key_expr_from_parquet(con, paths[0])
            distinct_ts_col, min_ts_col, max_ts_col = _coverage_ts_cols_from_parquet(con, paths[0])

        for batch in chunked(paths, chunk_size):
            arr = "[" + ",".join("'" + p.replace("'", "''") + "'" for p in batch) + "]"

            if ds in FUTURES_DATASETS_WITH_TIMEFRAME:
                expected = expected_rows_for_day(timeframe_filter)

                con.execute(
                    f"""
                    INSERT INTO _stg_cov
                    SELECT
                        '{market}' AS market,
                        '{ds}' AS dataset,
                        symbol,
                        {day_key_expr} AS date,
                        '{timeframe_filter}'::VARCHAR AS timeframe,
                        COUNT(DISTINCT filename)::UBIGINT AS num_files,
                        COUNT(*)::UBIGINT AS num_rows,
                        COUNT(DISTINCT date_trunc('minute', {distinct_ts_col}))::UBIGINT AS distinct_minute_cnt,
                        MIN({min_ts_col}) AS min_ts,
                        MAX({max_ts_col}) AS max_ts,
                        {expected if expected is not None else 'NULL'}::INTEGER AS expected_rows,
                        CASE WHEN {expected if expected is not None else 'NULL'} IS NULL
                            THEN NULL
                            ELSE COUNT(*)::DOUBLE / {expected}
                        END AS completeness_ratio,
                        CASE WHEN {expected if expected is not None else 'NULL'} IS NULL
                            THEN NULL
                            ELSE (COUNT(*)::DOUBLE / {expected}) < {completeness_threshold}
                        END AS is_suspect,
                        CAST('{now}' AS TIMESTAMP) AS computed_at_utc
                    FROM read_parquet({arr}, hive_partitioning=1, filename=1)
                    WHERE timeframe = '{timeframe_filter}'
                    GROUP BY symbol, {day_key_expr}, timeframe;
                    """
                )

            else:
                con.execute(
                    f"""
                    INSERT INTO _stg_cov
                    SELECT
                        '{market}' AS market,
                        '{ds}' AS dataset,
                        symbol,
                        CAST(ts AS DATE),
                        ''::VARCHAR AS timeframe,
                        COUNT(DISTINCT filename)::UBIGINT AS num_files,
                        COUNT(*)::UBIGINT AS num_rows,
                        COUNT(DISTINCT date_trunc('minute', ts))::UBIGINT AS distinct_minute_cnt,
                        MIN(ts) AS min_ts,
                        MAX(ts) AS max_ts,
                        NULL::INTEGER AS expected_rows,
                        NULL::DOUBLE AS completeness_ratio,
                        NULL::BOOLEAN AS is_suspect,
                        CAST('{now}' AS TIMESTAMP) AS computed_at_utc
                    FROM read_parquet({arr}, hive_partitioning=1, filename=1)
                    GROUP BY symbol, CAST(ts AS DATE);
                    """
                )

        con.execute(
            """
            MERGE INTO md.md_partition_coverage t
            USING _stg_cov s
            ON t.market = s.market
               AND t.dataset = s.dataset
               AND t.symbol = s.symbol
               AND t.date = s.date
               AND t.timeframe = s.timeframe
            WHEN MATCHED THEN UPDATE SET
                num_files = s.num_files,
                num_rows = s.num_rows,
                distinct_minute_cnt = s.distinct_minute_cnt,
                min_ts = s.min_ts,
                max_ts = s.max_ts,
                expected_rows = s.expected_rows,
                completeness_ratio = s.completeness_ratio,
                is_suspect = s.is_suspect,
                computed_at_utc = s.computed_at_utc
            WHEN NOT MATCHED THEN INSERT (
                market, dataset, symbol, date, timeframe,
                num_files, num_rows, distinct_minute_cnt, min_ts, max_ts,
                expected_rows, completeness_ratio, is_suspect, computed_at_utc
            ) VALUES (
                s.market, s.dataset, s.symbol, s.date, s.timeframe,
                s.num_files, s.num_rows, s.distinct_minute_cnt, s.min_ts, s.max_ts,
                s.expected_rows, s.completeness_ratio, s.is_suspect, s.computed_at_utc
            );
            """
        )
        con.execute("DROP TABLE _stg_cov;")


def recompute_rollups(con: duckdb.DuckDBPyConnection, market: str, *, timeframe_filter: str) -> None:
    now = utc_now_iso()

    # Delete only this timeframe's rollups (non-destructive)
    con.execute(
        "DELETE FROM md.md_symbol_dataset_stats WHERE market = ? AND timeframe = ?;",
        [market, timeframe_filter],
    )

    # For timeframe_filter == '1m', include funding rows stored with timeframe=''
    # by normalizing them into tf_norm='1m' for rollup purposes.
    con.execute(
        f"""
        INSERT INTO md.md_symbol_dataset_stats
        WITH base AS (
            SELECT
                market,
                dataset,
                symbol,
                CASE
                    WHEN '{timeframe_filter}' = '1m' AND dataset = 'funding' THEN '1m'
                    ELSE timeframe
                END AS tf_norm,
                date,
                min_ts,
                max_ts,
                num_rows,
                is_suspect
            FROM md.md_partition_coverage
            WHERE market = '{market}'
              AND (
                    timeframe = '{timeframe_filter}'
                 OR ('{timeframe_filter}' = '1m' AND dataset='funding' AND timeframe = '')
              )
        )
        SELECT
            market,
            dataset,
            symbol,
            tf_norm AS timeframe,
            MIN(date) AS min_date,
            MAX(date) AS max_date,
            MIN(min_ts) AS min_ts,
            MAX(max_ts) AS max_ts,
            COUNT(*)::UBIGINT AS days_present,
            SUM(CASE WHEN is_suspect = FALSE OR is_suspect IS NULL THEN 1 ELSE 0 END)::UBIGINT AS days_complete,
            AVG(CASE WHEN is_suspect IS NULL THEN NULL
                     WHEN is_suspect THEN 0.0 ELSE 1.0 END) AS pct_days_complete,
            AVG(num_rows)::DOUBLE AS avg_rows_per_day,
            CAST('{now}' AS TIMESTAMP) AS last_refreshed_utc
        FROM base
        GROUP BY market, dataset, symbol, tf_norm;
        """
    )

    # Alignment: rebuild only this timeframe (non-destructive)
    con.execute(
        "DELETE FROM md.md_alignment_summary WHERE market = ? AND timeframe = ?;",
        [market, timeframe_filter],
    )

    con.execute(
        f"""
        INSERT INTO md.md_alignment_summary
        WITH base AS (
            SELECT
                market,
                CASE
                    WHEN '{timeframe_filter}' = '1m' AND dataset = 'funding' THEN '1m'
                    ELSE timeframe
                END AS tf_norm,
                symbol,
                date,

                MAX(CASE WHEN dataset='klines' THEN TRUE ELSE FALSE END) AS has_klines,
                MAX(CASE WHEN dataset='mark' THEN TRUE ELSE FALSE END) AS has_mark,
                MAX(CASE WHEN dataset='index' THEN TRUE ELSE FALSE END) AS has_index,
                MAX(CASE WHEN dataset='funding' THEN TRUE ELSE FALSE END) AS has_funding,

                MIN(CASE WHEN dataset IN ('klines','mark','index')
                         THEN completeness_ratio ELSE NULL END) AS core_completeness_min
            FROM md.md_partition_coverage
            WHERE market = '{market}'
              AND (
                    timeframe = '{timeframe_filter}'
                 OR ('{timeframe_filter}' = '1m' AND dataset='funding' AND timeframe = '')
              )
            GROUP BY market, tf_norm, symbol, date
        )
        SELECT
            market,
            tf_norm AS timeframe,
            symbol,
            date,
            has_klines,
            has_mark,
            has_index,
            has_funding,
            (has_klines AND has_mark AND has_index) AS all_core_present,
            TRIM(BOTH ',' FROM
                (CASE WHEN NOT has_klines THEN 'klines,' ELSE '' END) ||
                (CASE WHEN NOT has_mark THEN 'mark,' ELSE '' END) ||
                (CASE WHEN NOT has_index THEN 'index,' ELSE '' END)
            ) AS core_missing_mask,
            core_completeness_min,
            CAST('{now}' AS TIMESTAMP) AS computed_at_utc
        FROM base;
        """
    )


def build_or_update_metadata(
    nas_root: str | Path,
    *,
    market: str = "futures",
    datasets: Iterable[str] = DEFAULT_FUTURES_DATASETS,
    meta_db_path: Optional[str | Path] = None,
    timeframe_filter: str = "1m",
    liquidity_only: bool = False,
    completeness_threshold: float = 0.98,
    incremental_chunk_size: int = 5000,
) -> dict:
    """
    Future-proofed for /spot by introducing 'market' everywhere, but only processes the chosen market.
    """
    nas_root = Path(nas_root)
    if meta_db_path is None:
        meta_db_path = default_local_meta_db_path(market)
    meta_db_path = Path(meta_db_path)

    files = list(
        iter_data_parquets(
            nas_root,
            market=market,
            datasets=datasets,
            timeframe_filter=timeframe_filter,
            datasets_with_timeframe=set(FUTURES_DATASETS_WITH_TIMEFRAME),
        )
    )

    con = connect_meta_db(meta_db_path)
    try:
        first = is_first_run_coverage_for_timeframe(con, market=market, timeframe=timeframe_filter)
        changed = get_new_or_changed_files(con, files)

        upsert_manifest(con, files)

        if not liquidity_only:
            if first:
                bulk_refresh_coverage_first_run(
                    con,
                    nas_root=nas_root,
                    market=market,
                    datasets=datasets,
                    completeness_threshold=completeness_threshold,
                    timeframe_filter=timeframe_filter,
                )
                mode = "bulk_first_run"
                changed_count = len(files)
            else:
                incremental_refresh_coverage_for_files(
                    con,
                    market=market,
                    changed_files=changed,
                    completeness_threshold=completeness_threshold,
                    timeframe_filter=timeframe_filter,
                    chunk_size=incremental_chunk_size,
                )
                mode = "incremental"
                changed_count = len(changed)
        else:
            mode = "liquidity_only"
            changed_count = 0

        liq_summary = build_or_update_liquidity_daily(
            con,
            nas_root=nas_root,
            market=market,
            timeframe_filter=timeframe_filter,
            lookback_days=30,
            top_n=100,
            changed_files=changed,  # pass the same changed files list
            incremental_chunk_size=incremental_chunk_size,
        )

        if not liquidity_only:
            recompute_rollups(con, market=market, timeframe_filter=timeframe_filter)
            if timeframe_filter == "1m":
                integrity_summary = build_or_update_kline_integrity_day(
                    nas_root,
                    market=market,
                    meta_db_path=meta_db_path,
                    timeframe_filter=timeframe_filter,
                    changed_files=changed,
                    recompute=False,
                    incremental_chunk_size=incremental_chunk_size,
                    con=con,
                )
            else:
                integrity_summary = {
                    "ok": True,
                    "market": market,
                    "timeframe_filter": timeframe_filter,
                    "mode": "skipped_non_1m_timeframe",
                }
        else:
            integrity_summary = {
                "ok": True,
                "market": market,
                "timeframe_filter": timeframe_filter,
                "mode": "skipped_liquidity_only",
            }

    finally:
        con.close()

    return {
        "nas_root": nas_root.as_posix(),
        "market": market,
        "datasets": list(datasets),
        "meta_db_path": meta_db_path.as_posix(),
        "first_run": bool(first),
        "files_seen": len(files),
        "new_or_changed_files": int(changed_count),
        "coverage_mode": mode,
        "when_utc": utc_now_iso(),
        "liquidity": liq_summary,
        "kline_integrity": integrity_summary,
    }


def _utc_now_ts_expr() -> str:
    # Use DuckDB's now() for consistency inside SQL, but keep helper for clarity.
    return "now()"


def _safe_ident(name: str) -> str:
    # Minimal quoting helper if needed; not used heavily here.
    return '"' + name.replace('"', '""') + '"'


def _parquet_columns(con: duckdb.DuckDBPyConnection, sample_file: str) -> set[str]:
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


def _day_key_expr_from_parquet(con: duckdb.DuckDBPyConnection, sample_file: str) -> str:
    """
    Determine the day key expression for a parquet schema.
    Prefer DATE-typed `date` if present, else CAST(ts AS DATE), else CAST(day AS DATE).
    """
    colnames = _parquet_columns(con, sample_file)
    if "date" in colnames:
        return "date"
    if "ts" in colnames:
        return "CAST(ts AS DATE)"
    if "day" in colnames:
        return "CAST(day AS DATE)"
    raise ValueError(f"Could not infer day key for parquet: columns={sorted(colnames)}")


def _coverage_ts_cols_from_parquet(con: duckdb.DuckDBPyConnection, sample_file: str) -> tuple[str, str, str]:
    colnames = _parquet_columns(con, sample_file)
    if "ts" in colnames:
        return "ts", "ts", "ts"
    min_ts = "min_ts" if "min_ts" in colnames else None
    max_ts = "max_ts" if "max_ts" in colnames else None
    distinct_ts = min_ts or max_ts
    if distinct_ts is None:
        raise ValueError(f"Could not infer timestamp columns for parquet: columns={sorted(colnames)}")
    return distinct_ts, (min_ts or distinct_ts), (max_ts or distinct_ts)


def _detect_kline_cols(con: duckdb.DuckDBPyConnection, sample_file: str) -> tuple[str, str, str]:
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


def _paths_for_changed_klines(
    files: Sequence[FileInfo | str],
    timeframe_filter: str,
) -> list[str]:
    """
    Keep only changed klines files for the chosen timeframe.
    Assumes file paths look like .../klines/timeframe=1m/...
    """
    out = []
    needle = f"/klines/timeframe={timeframe_filter}/"
    for f in files:
        if isinstance(f, FileInfo):
            if f.dataset != "klines":
                continue
            path = f.file_path
        else:
            path = str(f)
        if needle in path.replace("\\", "/"):
            out.append(path)
    return out


def _detect_kline_ohlc_cols(
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


def build_or_update_kline_integrity_day(
    nas_root: str | Path,
    *,
    market: str = "futures",
    meta_db_path: Optional[str | Path] = None,
    timeframe_filter: str = "1m",
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    changed_files: Optional[Sequence[FileInfo | str]] = None,
    recompute: bool = False,
    incremental_chunk_size: int = 5000,
    con: duckdb.DuckDBPyConnection | None = None,
) -> dict:
    if timeframe_filter != "1m":
        raise ValueError("Only timeframe_filter='1m' is supported for kline integrity checks.")

    nas_root = Path(nas_root)
    if meta_db_path is None and con is None:
        meta_db_path = default_local_meta_db_path(market)

    expected = expected_rows_for_day(timeframe_filter)
    changed_paths: list[str] = []
    if changed_files:
        changed_paths = _paths_for_changed_klines(changed_files, timeframe_filter=timeframe_filter)

    if not changed_paths and start_date is None and end_date is None and not recompute:
        return {
            "ok": True,
            "market": market,
            "timeframe_filter": timeframe_filter,
            "mode": "no_op",
            "reason": "No changed klines files or date range provided; skipping integrity build.",
        }

    owns_connection = False
    if con is None:
        con = connect_meta_db(Path(meta_db_path))
        owns_connection = True

    try:
        if changed_paths:
            sample_file = changed_paths[0]
            source_rel = "read_parquet({arr}, hive_partitioning=1, filename=0)"
        else:
            glob = dataset_glob(nas_root, market, "klines", timeframe=timeframe_filter)
            sample = con.execute(
                f"SELECT filename FROM read_parquet('{glob}', filename=1, hive_partitioning=1) LIMIT 1;"
            ).fetchone()
            if not sample:
                return {
                    "ok": False,
                    "reason": "No klines parquet files found for sampling",
                    "market": market,
                }
            sample_file = sample[0]
            source_rel = f"read_parquet('{glob}', hive_partitioning=1, filename=0)"

        time_col, open_col, high_col, low_col, close_col = _detect_kline_ohlc_cols(con, sample_file)
        day_key_expr = _day_key_expr_from_parquet(con, sample_file)

        con.execute("CREATE TEMP TABLE _stg_kline_integrity AS SELECT * FROM md.md_kline_integrity_day WHERE 1=0;")

        if changed_paths:
            for i in range(0, len(changed_paths), incremental_chunk_size):
                batch = changed_paths[i : i + incremental_chunk_size]
                arr = "[" + ",".join("'" + p.replace("'", "''") + "'" for p in batch) + "]"
                con.execute(
                    f"""
                    INSERT INTO _stg_kline_integrity
                    WITH base AS (
                        SELECT
                            symbol,
                            {day_key_expr} AS day,
                            CAST({time_col} AS TIMESTAMP) AS ts,
                            CAST({open_col} AS DOUBLE) AS open,
                            CAST({high_col} AS DOUBLE) AS high,
                            CAST({low_col} AS DOUBLE) AS low,
                            CAST({close_col} AS DOUBLE) AS close
                        FROM {source_rel.format(arr=arr)}
                        WHERE timeframe = '{timeframe_filter}'
                    ),
                    calc AS (
                        SELECT
                            symbol,
                            day,
                            ts,
                            open,
                            high,
                            low,
                            close,
                            LAG(close) OVER (PARTITION BY symbol, day ORDER BY ts) AS prev_close
                        FROM base
                    )
                    SELECT
                        '{market}' AS market,
                        symbol,
                        day AS date,
                        '{timeframe_filter}' AS timeframe,
                        COUNT(*)::UBIGINT AS num_rows,
                        COUNT(DISTINCT date_trunc('minute', ts))::UBIGINT AS distinct_minute_cnt,
                        {expected} - COUNT(DISTINCT date_trunc('minute', ts)) AS gap_rows_vs_expected,
                        COUNT(*) - COUNT(DISTINCT date_trunc('minute', ts)) AS duplicate_rows,
                        SUM(CASE WHEN high < low THEN 1 ELSE 0 END)::UBIGINT AS high_lt_low_cnt,
                        SUM(CASE WHEN open <= 0 OR high <= 0 OR low <= 0 OR close <= 0 THEN 1 ELSE 0 END)::UBIGINT
                            AS nonpositive_price_cnt,
                        MAX(ABS(close / prev_close - 1)) AS max_abs_ret_1m,
                        {_utc_now_ts_expr()} AS computed_at_utc
                    FROM calc
                    GROUP BY symbol, day;
                    """
                )
        else:
            date_filter = ""
            if start_date:
                date_filter += f" AND {day_key_expr} >= CAST('{start_date}' AS DATE)"
            if end_date:
                date_filter += f" AND {day_key_expr} <= CAST('{end_date}' AS DATE)"
            con.execute(
                f"""
                INSERT INTO _stg_kline_integrity
                WITH base AS (
                    SELECT
                        symbol,
                        {day_key_expr} AS day,
                        CAST({time_col} AS TIMESTAMP) AS ts,
                        CAST({open_col} AS DOUBLE) AS open,
                        CAST({high_col} AS DOUBLE) AS high,
                        CAST({low_col} AS DOUBLE) AS low,
                        CAST({close_col} AS DOUBLE) AS close
                    FROM {source_rel}
                    WHERE timeframe = '{timeframe_filter}'{date_filter}
                ),
                calc AS (
                    SELECT
                        symbol,
                        day,
                        ts,
                        open,
                        high,
                        low,
                        close,
                        LAG(close) OVER (PARTITION BY symbol, day ORDER BY ts) AS prev_close
                    FROM base
                )
                SELECT
                    '{market}' AS market,
                    symbol,
                    day AS date,
                    '{timeframe_filter}' AS timeframe,
                    COUNT(*)::UBIGINT AS num_rows,
                    COUNT(DISTINCT date_trunc('minute', ts))::UBIGINT AS distinct_minute_cnt,
                    {expected} - COUNT(DISTINCT date_trunc('minute', ts)) AS gap_rows_vs_expected,
                    COUNT(*) - COUNT(DISTINCT date_trunc('minute', ts)) AS duplicate_rows,
                    SUM(CASE WHEN high < low THEN 1 ELSE 0 END)::UBIGINT AS high_lt_low_cnt,
                    SUM(CASE WHEN open <= 0 OR high <= 0 OR low <= 0 OR close <= 0 THEN 1 ELSE 0 END)::UBIGINT
                        AS nonpositive_price_cnt,
                    MAX(ABS(close / prev_close - 1)) AS max_abs_ret_1m,
                    {_utc_now_ts_expr()} AS computed_at_utc
                FROM calc
                GROUP BY symbol, day;
                """
            )

        row_count = con.execute("SELECT COUNT(*) FROM _stg_kline_integrity;").fetchone()[0]
        if row_count:
            con.execute(
                """
                DELETE FROM md.md_kline_integrity_day
                WHERE market = ?
                  AND timeframe = ?
                  AND (symbol, date) IN (SELECT symbol, date FROM _stg_kline_integrity);
                """,
                [market, timeframe_filter],
            )
            con.execute("INSERT INTO md.md_kline_integrity_day SELECT * FROM _stg_kline_integrity;")
        con.execute("DROP TABLE _stg_kline_integrity;")

        return {
            "ok": True,
            "market": market,
            "timeframe_filter": timeframe_filter,
            "mode": "incremental" if changed_paths else "date_range",
            "rows_inserted": int(row_count),
        }
    finally:
        if owns_connection and con is not None:
            con.close()


def _compute_base_liquidity_from_paths(
    con: duckdb.DuckDBPyConnection,
    market: str,
    paths: Sequence[str],
    *,
    timeframe_filter: str,
    time_col: str,
    day_key_expr: str,
    close_col: str,
    vol_col: str,
    chunk_size: int = 5000,
) -> None:
    """
    Compute daily base liquidity metrics for the provided klines parquet file paths and upsert into md.md_liquidity_daily:
      - dollar_volume = sum(close * volume)
      - volume = sum(volume)
      - close_price = last close by time column
    """
    if not paths:
        return

    con.execute("CREATE TEMP TABLE _stg_liq AS SELECT * FROM md.md_liquidity_daily WHERE 1=0;")

    for i in range(0, len(paths), chunk_size):
        batch = paths[i : i + chunk_size]
        arr = "[" + ",".join("'" + p.replace("'", "''") + "'" for p in batch) + "]"

        # NOTE: we hardcode timeframe as timeframe_filter (like we did for coverage) and group by symbol,date
        con.execute(
            f"""
            INSERT INTO _stg_liq
            SELECT
                '{market}' AS market,
                '{timeframe_filter}' AS timeframe,
                symbol,
                {day_key_expr} AS date,

                SUM(CAST({close_col} AS DOUBLE) * CAST({vol_col} AS DOUBLE)) AS dollar_volume,
                SUM(CAST({vol_col} AS DOUBLE)) AS volume,
                ARG_MAX(CAST({close_col} AS DOUBLE), CAST({time_col} AS TIMESTAMP)) AS close_price,

                NULL::DOUBLE AS dv_30d_median,
                NULL::INTEGER AS liquidity_rank_30d,
                NULL::BOOLEAN AS in_top_100,

                {_utc_now_ts_expr()} AS computed_at_utc
            FROM read_parquet({arr}, hive_partitioning=1, filename=0)
            WHERE timeframe = '{timeframe_filter}'
            GROUP BY symbol, {day_key_expr};
            """
        )

    # Upsert base columns; keep rolling columns as-is for existing rows (we update them in a separate step)
    con.execute(
        """
        MERGE INTO md.md_liquidity_daily t
        USING _stg_liq s
        ON t.market = s.market AND t.timeframe = s.timeframe AND t.symbol = s.symbol AND t.date = s.date
        WHEN MATCHED THEN UPDATE SET
            dollar_volume = s.dollar_volume,
            volume = s.volume,
            close_price = s.close_price,
            computed_at_utc = s.computed_at_utc
        WHEN NOT MATCHED THEN INSERT (
            market, timeframe, symbol, date,
            dollar_volume, volume, close_price,
            dv_30d_median, liquidity_rank_30d, in_top_100,
            computed_at_utc
        ) VALUES (
            s.market, s.timeframe, s.symbol, s.date,
            s.dollar_volume, s.volume, s.close_price,
            s.dv_30d_median, s.liquidity_rank_30d, s.in_top_100,
            s.computed_at_utc
        );
        """
    )

    con.execute("DROP TABLE _stg_liq;")


def _recompute_rolling_and_rank_for_date_range(
    con: duckdb.DuckDBPyConnection,
    market: str,
    timeframe: str,
    *,
    date_from: str,
    date_to: str,
    lookback_days: int,
    top_n: int,
) -> None:
    # Include sufficient pre-history so the rolling window is correct at date_from.
    con.execute(
        f"""
        WITH base AS (
            SELECT
                market,
                timeframe,
                symbol,
                date,
                dollar_volume
            FROM md.md_liquidity_daily
            WHERE market = '{market}'
              AND timeframe = '{timeframe}'
              AND date BETWEEN
                    (CAST('{date_from}' AS DATE) - INTERVAL '{lookback_days - 1} days')
                AND CAST('{date_to}' AS DATE)
        ),
        calc AS (
            SELECT
                market,
                timeframe,
                symbol,
                date,
                QUANTILE_CONT(dollar_volume, 0.5) OVER (
                    PARTITION BY market, timeframe, symbol
                    ORDER BY date
                    ROWS BETWEEN {lookback_days - 1} PRECEDING AND CURRENT ROW
                ) AS dv_30d_median
            FROM base
        ),
        ranked AS (
            SELECT
                c.market,
                c.timeframe,
                c.symbol,
                c.date,
                c.dv_30d_median,
                RANK() OVER (
                    PARTITION BY c.market, c.timeframe, c.date
                    ORDER BY c.dv_30d_median DESC NULLS LAST
                ) AS liquidity_rank_30d
            FROM calc c
            WHERE c.date BETWEEN CAST('{date_from}' AS DATE) AND CAST('{date_to}' AS DATE)
        )
        UPDATE md.md_liquidity_daily t
        SET
            dv_30d_median = r.dv_30d_median,
            liquidity_rank_30d = r.liquidity_rank_30d,
            in_top_100 = (r.liquidity_rank_30d <= {top_n}),
            computed_at_utc = {_utc_now_ts_expr()}
        FROM ranked r
        WHERE t.market = r.market
          AND t.timeframe = r.timeframe
          AND t.symbol = r.symbol
          AND t.date = r.date;
        """
    )


def is_first_run_coverage_for_timeframe(con: duckdb.DuckDBPyConnection, market: str, timeframe: str) -> bool:
    if timeframe == "1m":
        # 1m core + funding (timeframe='')
        n = con.execute(
            "SELECT COUNT(*) FROM md.md_partition_coverage WHERE market = ? AND timeframe IN ('1m','');",
            [market],
        ).fetchone()[0]
        return int(n) == 0
    n = con.execute(
        "SELECT COUNT(*) FROM md.md_partition_coverage WHERE market = ? AND timeframe = ?;",
        [market, timeframe],
    ).fetchone()[0]
    return int(n) == 0


def is_first_run_liquidity_for_timeframe(con: duckdb.DuckDBPyConnection, market: str, timeframe: str) -> bool:
    n = con.execute(
        "SELECT COUNT(*) FROM md.md_liquidity_daily WHERE market = ? AND timeframe = ?;",
        [market, timeframe],
    ).fetchone()[0]
    return int(n) == 0


def build_or_update_liquidity_daily(
    con: duckdb.DuckDBPyConnection,
    nas_root: Path,
    *,
    market: str = "futures",
    timeframe_filter: str = "1m",
    lookback_days: int = 30,
    top_n: int = 100,
    changed_files: Optional[Sequence[FileInfo]] = None,
    incremental_chunk_size: int = 5000,
) -> dict:
    """
    Build/update md.md_liquidity_daily derived entirely from klines.

    First run: scans all klines/timeframe_filter across the lake (set-based) and builds base liquidity + rolling + ranks.
    Incremental: uses changed klines file paths only, updates base metrics for those symbol-days,
                 then recomputes rolling+rank for the minimal affected date range:
                   [min_changed_date, max_changed_date + (lookback_days - 1)].

    You should pass changed_files from your existing build_or_update_metadata() pipeline to make it truly incremental.
    """
    nas_root = Path(nas_root)

    # Detect columns using one sample file (pick any klines file)
    sample_glob = dataset_glob(nas_root, market, "klines", timeframe=timeframe_filter)
    sample = con.execute(f"SELECT filename FROM read_parquet('{sample_glob}', filename=1, hive_partitioning=1) LIMIT 1;").fetchone()
    if not sample:
        return {"ok": False, "reason": "No klines parquet files found for sampling", "market": market}

    sample_file = sample[0]
    time_col, close_col, vol_col = _detect_kline_cols(con, sample_file)
    day_key_expr = _day_key_expr_from_parquet(con, sample_file)

    # Is this the first run for liquidity?
    first_run = is_first_run_liquidity_for_timeframe(con, market=market, timeframe=timeframe_filter)

    if first_run or not changed_files:
        # BULK: compute base liquidity from ALL klines files for timeframe_filter
        glob = dataset_glob(nas_root, market, "klines", timeframe=timeframe_filter)  # (nas_root / market / "klines" / f"timeframe={timeframe_filter}" / "symbol=*" / "date=*" / "data.parquet").as_posix()

        # Replace everything for this market (liquidity-only) — deterministic rebuild
        con.execute("DELETE FROM md.md_liquidity_daily WHERE market = ? AND timeframe = ?;", [market, timeframe_filter])

        # Base compute (set-based)
        con.execute(
            f"""
            INSERT INTO md.md_liquidity_daily
            SELECT
                '{market}' AS market,
                '{timeframe_filter}' AS timeframe,
                symbol,
                {day_key_expr} AS date,

                SUM(CAST({close_col} AS DOUBLE) * CAST({vol_col} AS DOUBLE)) AS dollar_volume,
                SUM(CAST({vol_col} AS DOUBLE)) AS volume,
                ARG_MAX(CAST({close_col} AS DOUBLE), CAST({time_col} AS TIMESTAMP)) AS close_price,

                NULL::DOUBLE AS dv_30d_median,
                NULL::INTEGER AS liquidity_rank_30d,
                NULL::BOOLEAN AS in_top_100,

                {_utc_now_ts_expr()} AS computed_at_utc
            FROM read_parquet('{glob}', hive_partitioning=1, filename=0)
            WHERE timeframe = '{timeframe_filter}'
            GROUP BY symbol, {day_key_expr};
            """
        )

        # Rolling + rank for entire available range
        dr = con.execute(
            """
            SELECT MIN(date)::VARCHAR, MAX(date)::VARCHAR
            FROM md.md_liquidity_daily
            WHERE market = ?
              AND timeframe = ?;
            """,
            [market, timeframe_filter],
        ).fetchone()

        if dr and dr[0] is not None and dr[1] is not None:
            _recompute_rolling_and_rank_for_date_range(
                con,
                market,
                timeframe_filter,
                date_from=dr[0],
                date_to=dr[1],
                lookback_days=lookback_days,
                top_n=top_n,
            )

        return {
            "ok": True,
            "market": market,
            "mode": "bulk_first_run" if first_run else "bulk_no_changed_files",
            "timeframe_filter": timeframe_filter,
            "lookback_days": lookback_days,
            "top_n": top_n,
        }

    # INCREMENTAL: only changed klines paths
    changed_paths = _paths_for_changed_klines(changed_files, timeframe_filter=timeframe_filter)
    if not changed_paths:
        return {
            "ok": True,
            "market": market,
            "mode": "incremental_no_klines_changes",
            "timeframe_filter": timeframe_filter,
        }

    # Compute base liquidity for only changed symbol-days
    _compute_base_liquidity_from_paths(
        con,
        market,
        changed_paths,
        timeframe_filter=timeframe_filter,
        time_col=time_col,
        day_key_expr=day_key_expr,
        close_col=close_col,
        vol_col=vol_col,
        chunk_size=incremental_chunk_size,
    )

    # Determine affected date range.
    # We recompute rolling+rank for [min_changed_date, max_changed_date + (lookback_days - 1)]
    # because edits on a day can affect the median/rank for subsequent days in the window.
    con.execute(
        f"""
        CREATE TEMP TABLE _chg_dates AS
        SELECT DISTINCT
        {day_key_expr} AS date
        FROM read_parquet($1, hive_partitioning=1);
        """,
        [changed_paths],
    )

    dr = con.execute("SELECT MIN(date)::VARCHAR, MAX(date)::VARCHAR FROM _chg_dates;").fetchone()
    con.execute("DROP TABLE _chg_dates;")

    if not dr or dr[0] is None or dr[1] is None:
        return {"ok": True, "market": market, "mode": "incremental_no_dates"}

    min_d = datetime.fromisoformat(dr[0]).date()
    max_d = datetime.fromisoformat(dr[1]).date()
    date_from = min_d.isoformat()
    date_to = (max_d + timedelta(days=lookback_days - 1)).isoformat()

    _recompute_rolling_and_rank_for_date_range(
        con,
        market,
        timeframe_filter,
        date_from=date_from,
        date_to=date_to,
        lookback_days=lookback_days,
        top_n=top_n,
    )

    return {
        "ok": True,
        "market": market,
        "mode": "incremental",
        "timeframe_filter": timeframe_filter,
        "changed_klines_files": len(changed_paths),
        "rolling_recomputed_from": date_from,
        "rolling_recomputed_to": date_to,
        "lookback_days": lookback_days,
        "top_n": top_n,
    }


def demo_day_key_expr(
    con: duckdb.DuckDBPyConnection,
    *,
    sample_1m: str,
    sample_1d: str,
) -> dict[str, str]:
    """
    Developer sanity helper: show day key expressions for 1m vs 1d schemas.
    Runs a tiny coverage-style query for the 1d sample to validate the expression.
    """
    expr_1m = _day_key_expr_from_parquet(con, sample_1m)
    expr_1d = _day_key_expr_from_parquet(con, sample_1d)

    con.execute(
        f"""
        SELECT
            symbol,
            {expr_1d} AS date
        FROM read_parquet(?, hive_partitioning=1)
        GROUP BY symbol, {expr_1d}
        LIMIT 1;
        """,
        [sample_1d],
    )

    return {"1m": expr_1m, "1d": expr_1d}
