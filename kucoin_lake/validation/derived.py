from __future__ import annotations

import glob
import re
import calendar
from datetime import date
from pathlib import Path
from typing import Optional, Sequence

import duckdb
import pandas as pd
from pandas.api import types as ptypes

from kucoin_lake.paths import month_starts, parse_lake_partitions
from kucoin_lake.resample import ResampleTask, plan_resample_tasks
from kucoin_lake.validation.artifacts import (
    ensure_output_dir,
    write_dataframe_csv,
    write_json_payload,
    write_run_artifacts,
)
from kucoin_lake.validation.models import (
    STATUS_ERROR,
    STATUS_FAIL,
    STATUS_PASS,
    STATUS_SKIP,
    ValidationArtifact,
    ValidationCheckResult,
    ValidationRunResult,
)
from kucoin_lake.validation.runner import build_validation_run_result, default_run_id

_SUPPORTED_DATASETS = {"klines", "mark", "index"}
_MONTH_RE = re.compile(r"^\d{4}-\d{2}$")
_REQUIRED_FIELDS: dict[str, tuple[str, ...]] = {
    "klines": ("open", "high", "low", "close", "volume"),
    "mark": ("open", "high", "low", "close"),
    "index": ("open", "high", "low", "close"),
}
_OPTIONAL_ROLLUP_FIELDS: tuple[str, ...] = ("src_rows", "min_ts", "max_ts")


def _sql_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _normalize_date(value: date | str | None) -> date | None:
    if value is None or isinstance(value, date):
        return value
    return date.fromisoformat(value)


def _normalize_months(months: Sequence[str] | None) -> list[str] | None:
    if months is None:
        return None
    out = sorted({m.strip() for m in months if m and m.strip()})
    for month in out:
        if not _MONTH_RE.match(month):
            raise ValueError(f"Invalid month format '{month}'. Expected YYYY-MM.")
    return out


def _months_for_window(date_start: date, date_end: date) -> list[str]:
    return [m.strftime("%Y-%m") for m in month_starts(date_start, date_end)]


def _month_overlaps_window(month: str, date_start: date | None, date_end: date | None) -> bool:
    month_start = date.fromisoformat(f"{month}-01")
    month_end = date(
        month_start.year,
        month_start.month,
        calendar.monthrange(month_start.year, month_start.month)[1],
    )
    if date_start is not None and month_end < date_start:
        return False
    if date_end is not None and month_start > date_end:
        return False
    return True


def _task_in_scope(
    task: ResampleTask,
    *,
    months: set[str] | None,
    date_start: date | None,
    date_end: date | None,
) -> bool:
    if months is not None and task.month not in months:
        return False
    return _month_overlaps_window(task.month, date_start, date_end)


def _resolve_raw_files_for_tasks(nas_root: Path, tasks: Sequence[ResampleTask]) -> list[str]:
    files: set[str] = set()
    for task in tasks:
        pattern = (
            nas_root
            / task.market
            / task.dataset
            / "timeframe=1m"
            / f"symbol={task.symbol}"
            / f"date={task.month}-*"
            / "data.parquet"
        ).as_posix()
        for path in glob.glob(pattern):
            files.add(Path(path).as_posix())
    return sorted(files)


def _month_candidate_from_partitions(parts: dict[str, str]) -> str | None:
    if "month" in parts:
        return parts["month"]
    day = parts.get("date")
    if day and len(day) >= 7:
        return day[:7]
    return None


def _output_file_in_scope(
    parts: dict[str, str],
    *,
    symbols: set[str] | None,
    months: set[str] | None,
    date_start: date | None,
    date_end: date | None,
) -> bool:
    symbol = parts.get("symbol")
    if symbols is not None and symbol not in symbols:
        return False

    month_candidate = _month_candidate_from_partitions(parts)
    if months is not None and month_candidate not in months:
        return False

    if date_start is None and date_end is None:
        return True

    day_value = parts.get("date")
    if day_value:
        try:
            day = date.fromisoformat(day_value)
        except ValueError:
            return True
        if date_start is not None and day < date_start:
            return False
        if date_end is not None and day > date_end:
            return False
        return True

    month_value = parts.get("month")
    if month_value and _MONTH_RE.match(month_value):
        return _month_overlaps_window(month_value, date_start, date_end)

    return True


def _discover_output_files(
    nas_root: Path,
    *,
    market: str,
    dataset: str,
    symbols: set[str] | None,
    months: set[str] | None,
    date_start: date | None,
    date_end: date | None,
) -> list[Path]:
    dataset_root = nas_root / market / dataset / "timeframe=1d"
    if not dataset_root.exists():
        return []

    out: list[Path] = []
    for path in dataset_root.glob("symbol=*/**/data.parquet"):
        if not path.is_file():
            continue
        rel = path.relative_to(nas_root).as_posix()
        parts = parse_lake_partitions(rel)
        if _output_file_in_scope(parts, symbols=symbols, months=months, date_start=date_start, date_end=date_end):
            out.append(path)
    return sorted(out)


def _date_clause(alias: str, date_start: date | None, date_end: date | None) -> tuple[str, list[str]]:
    clauses: list[str] = []
    params: list[str] = []
    if date_start is not None:
        clauses.append(f"{alias} >= CAST(? AS DATE)")
        params.append(date_start.isoformat())
    if date_end is not None:
        clauses.append(f"{alias} <= CAST(? AS DATE)")
        params.append(date_end.isoformat())
    if not clauses:
        return "", params
    return " AND ".join(clauses), params


def _empty_expected_frame(dataset: str) -> pd.DataFrame:
    cols = ["symbol", "date", "open", "high", "low", "close"]
    if dataset == "klines":
        cols.append("volume")
    cols.extend(list(_OPTIONAL_ROLLUP_FIELDS))
    return pd.DataFrame(columns=cols)


def _relation_columns(con: duckdb.DuckDBPyConnection, parquet_files: Sequence[str]) -> set[str]:
    if not parquet_files:
        return set()
    cur = con.execute(
        "SELECT * FROM read_parquet(?, hive_partitioning=1, filename=1, union_by_name=1) LIMIT 0;",
        [list(parquet_files)],
    )
    return {col[0].lower() for col in cur.description}


def _load_expected_rows(
    con: duckdb.DuckDBPyConnection,
    *,
    dataset: str,
    raw_files: Sequence[str],
    date_start: date | None,
    date_end: date | None,
) -> pd.DataFrame:
    if not raw_files:
        return _empty_expected_frame(dataset)

    fields = [
        "CAST(symbol AS VARCHAR) AS symbol",
        "CAST(date AS DATE) AS date",
        "CAST(ts AS TIMESTAMP) AS ts",
        "CAST(open AS DOUBLE) AS open",
        "CAST(high AS DOUBLE) AS high",
        "CAST(low AS DOUBLE) AS low",
        "CAST(close AS DOUBLE) AS close",
    ]
    if dataset == "klines":
        fields.append("CAST(volume AS DOUBLE) AS volume")

    where_date_sql, where_params = _date_clause("CAST(date AS DATE)", date_start, date_end)
    where_sql = "WHERE timeframe = '1m'"
    if where_date_sql:
        where_sql += f" AND {where_date_sql}"

    agg_fields = [
        "symbol",
        "date",
        "ARG_MIN(open, ts) AS open",
        "MAX(high) AS high",
        "MIN(low) AS low",
        "ARG_MAX(close, ts) AS close",
    ]
    if dataset == "klines":
        agg_fields.append("SUM(volume) AS volume")
    agg_fields.extend(
        [
            "COUNT(*)::BIGINT AS src_rows",
            "MIN(ts) AS min_ts",
            "MAX(ts) AS max_ts",
        ]
    )

    sql = f"""
    WITH src AS (
        SELECT {", ".join(fields)}
        FROM read_parquet(?, hive_partitioning=1, filename=1, union_by_name=1)
        {where_sql}
    )
    SELECT {", ".join(agg_fields)}
    FROM src
    GROUP BY symbol, date
    ORDER BY symbol, date;
    """
    return con.execute(sql, [list(raw_files), *where_params]).df()


def _load_actual_rows(
    con: duckdb.DuckDBPyConnection,
    *,
    dataset: str,
    output_files: Sequence[str],
    available_columns: set[str],
    date_start: date | None,
    date_end: date | None,
) -> pd.DataFrame:
    if not output_files:
        return _empty_expected_frame(dataset)

    def _field_expr(name: str, cast_type: str) -> str:
        if name in available_columns:
            return f"CAST({name} AS {cast_type}) AS {name}"
        return f"NULL::{cast_type} AS {name}"

    select_fields = [
        _field_expr("symbol", "VARCHAR"),
        _field_expr("date", "DATE"),
        _field_expr("open", "DOUBLE"),
        _field_expr("high", "DOUBLE"),
        _field_expr("low", "DOUBLE"),
        _field_expr("close", "DOUBLE"),
    ]
    if dataset == "klines":
        select_fields.append(_field_expr("volume", "DOUBLE"))
    select_fields.extend(
        [
            _field_expr("src_rows", "BIGINT"),
            _field_expr("min_ts", "TIMESTAMP"),
            _field_expr("max_ts", "TIMESTAMP"),
        ]
    )

    where_parts: list[str] = []
    if "timeframe" in available_columns:
        where_parts.append("timeframe = '1d'")
    where_sql = f"WHERE {' AND '.join(where_parts)}" if where_parts else ""

    date_sql, date_params = _date_clause("date", date_start, date_end)
    outer_where = f"WHERE {date_sql}" if date_sql else ""

    sql = f"""
    WITH src AS (
        SELECT {", ".join(select_fields)}
        FROM read_parquet(?, hive_partitioning=1, filename=1, union_by_name=1)
        {where_sql}
    )
    SELECT *
    FROM src
    {outer_where}
    ORDER BY symbol, date;
    """
    return con.execute(sql, [list(output_files), *date_params]).df()


def _series_diff_mask(left: pd.Series, right: pd.Series, *, tolerance: float = 1e-12) -> pd.Series:
    left_na = left.isna()
    right_na = right.isna()
    diff = left_na ^ right_na
    both = ~(left_na | right_na)

    if both.any():
        if ptypes.is_numeric_dtype(left) and ptypes.is_numeric_dtype(right):
            lv = pd.to_numeric(left[both], errors="coerce")
            rv = pd.to_numeric(right[both], errors="coerce")
            val_diff = (lv - rv).abs() > tolerance
        else:
            val_diff = left[both] != right[both]
        diff.loc[both] = val_diff

    return diff.fillna(True)


def _fingerprint_dataframe(
    con: duckdb.DuckDBPyConnection,
    frame: pd.DataFrame,
    *,
    cols: Sequence[str],
) -> dict[str, str | int]:
    if frame.empty:
        return {
            "row_count": 0,
            "hash_sum": "0",
            "hash_min": "0",
            "hash_max": "0",
        }

    view_name = "_fp_rows"
    con.register(view_name, frame[list(cols)])
    try:
        cols_sql = ", ".join(_sql_ident(c) for c in cols)
        row = con.execute(
            f"""
            WITH base AS (
                SELECT hash({cols_sql}) AS row_hash
                FROM {view_name}
            )
            SELECT
                COUNT(*)::BIGINT AS row_count,
                COALESCE(SUM(row_hash::HUGEINT), 0)::VARCHAR AS hash_sum,
                COALESCE(MIN(row_hash), 0)::VARCHAR AS hash_min,
                COALESCE(MAX(row_hash), 0)::VARCHAR AS hash_max
            FROM base;
            """
        ).fetchone()
    finally:
        con.unregister(view_name)

    return {
        "row_count": int(row[0]),
        "hash_sum": str(row[1]),
        "hash_min": str(row[2]),
        "hash_max": str(row[3]),
    }


def _scope_metrics(
    *,
    tasks: Sequence[ResampleTask],
    output_file_rels: Sequence[str],
    symbols_filter: Sequence[str] | None,
    months_filter: Sequence[str] | None,
    tasks_planned: int,
    sample_limit: int | None,
) -> dict[str, int | list[str] | None]:
    symbols = {task.symbol for task in tasks}
    months = {task.month for task in tasks}

    for rel in output_file_rels:
        parts = parse_lake_partitions(rel)
        symbol = parts.get("symbol")
        month = _month_candidate_from_partitions(parts)
        if symbol:
            symbols.add(symbol)
        if month:
            months.add(month)

    if not symbols and symbols_filter:
        symbols.update(symbols_filter)
    if not months and months_filter:
        months.update(months_filter)

    return {
        "tasks_planned": int(tasks_planned),
        "tasks_scoped": int(len(tasks)),
        "sample_limit": sample_limit,
        "scoped_symbol_count": int(len(symbols)),
        "scoped_month_count": int(len(months)),
        "scoped_symbols": sorted(symbols),
        "scoped_months": sorted(months),
    }


def validate_derived_1d(
    nas_root: str | Path,
    *,
    market: str = "futures",
    dataset: str,
    symbols: Optional[Sequence[str]] = None,
    date_start: Optional[date | str] = None,
    date_end: Optional[date | str] = None,
    months: Optional[Sequence[str]] = None,
    output_dir: Optional[str | Path] = None,
    profile: str = "smoke",
    sample_limit: Optional[int] = None,
) -> ValidationRunResult:
    if dataset not in _SUPPORTED_DATASETS:
        raise ValueError("dataset must be one of: 'klines', 'mark', 'index'")
    if profile not in {"smoke", "full"}:
        raise ValueError("profile must be one of: 'smoke', 'full'")

    date_start_norm = _normalize_date(date_start)
    date_end_norm = _normalize_date(date_end)
    if date_start_norm and date_end_norm and date_start_norm > date_end_norm:
        raise ValueError("date_start must be <= date_end")

    months_norm = _normalize_months(months)
    symbols_norm = sorted(set(symbols)) if symbols else None
    sample_limit_norm = int(sample_limit) if sample_limit is not None else None
    if sample_limit_norm is not None and sample_limit_norm < 0:
        raise ValueError("sample_limit must be >= 0")

    nas_root_path = Path(nas_root)
    planning_months = months_norm
    if planning_months is None and date_start_norm is not None and date_end_norm is not None:
        planning_months = _months_for_window(date_start_norm, date_end_norm)

    planned_tasks = plan_resample_tasks(
        nas_root_path,
        market=market,
        dataset=dataset,
        timeframe_src="1m",
        symbols=symbols_norm,
        months=planning_months,
    )
    months_set = set(months_norm) if months_norm else None
    scoped_tasks = [
        task
        for task in planned_tasks
        if _task_in_scope(task, months=months_set, date_start=date_start_norm, date_end=date_end_norm)
    ]
    if sample_limit_norm is not None:
        scoped_tasks = scoped_tasks[:sample_limit_norm]

    if sample_limit_norm is not None and scoped_tasks:
        output_symbols = {task.symbol for task in scoped_tasks}
        output_months = {task.month for task in scoped_tasks}
    else:
        output_symbols = set(symbols_norm) if symbols_norm else None
        if months_norm:
            output_months = set(months_norm)
        elif date_start_norm is not None and date_end_norm is not None:
            output_months = set(_months_for_window(date_start_norm, date_end_norm))
        else:
            output_months = None

    output_files = _discover_output_files(
        nas_root_path,
        market=market,
        dataset=dataset,
        symbols=output_symbols,
        months=output_months,
        date_start=date_start_norm,
        date_end=date_end_norm,
    )
    output_file_paths = [p.as_posix() for p in output_files]
    output_file_rels = [p.relative_to(nas_root_path).as_posix() for p in output_files]
    raw_files = _resolve_raw_files_for_tasks(nas_root_path, scoped_tasks)

    validator_name = f"derived_1d.{dataset}"
    run_id = default_run_id(validator_name)
    output_dir_path = Path(output_dir) if output_dir is not None else (Path.cwd() / "validation" / run_id)
    ensure_output_dir(output_dir_path)

    scope_metrics = _scope_metrics(
        tasks=scoped_tasks,
        output_file_rels=output_file_rels,
        symbols_filter=symbols_norm,
        months_filter=months_norm,
        tasks_planned=len(planned_tasks),
        sample_limit=sample_limit_norm,
    )
    target = f"{market}/{dataset}/1m->1d"

    try:
        con = duckdb.connect(":memory:")
        try:
            con.execute("SET TimeZone = 'UTC';")
            output_cols = _relation_columns(con, output_file_paths)
            expected_df = _load_expected_rows(
                con,
                dataset=dataset,
                raw_files=raw_files,
                date_start=date_start_norm,
                date_end=date_end_norm,
            )
            actual_df = _load_actual_rows(
                con,
                dataset=dataset,
                output_files=output_file_paths,
                available_columns=output_cols,
                date_start=date_start_norm,
                date_end=date_end_norm,
            )
        finally:
            con.close()
    except Exception as exc:
        checks = [
            ValidationCheckResult(
                check_id="derived.partition_shape",
                category="derived",
                status=STATUS_ERROR,
                target=target,
                message="Failed while loading derived 1d scope.",
                details={"error": str(exc)},
                metrics={**scope_metrics, "profile": profile},
            ),
            ValidationCheckResult(
                check_id="derived.unique_symbol_date",
                category="derived",
                status=STATUS_SKIP,
                target=target,
                message="Skipped because loading derived 1d scope failed.",
                metrics={**scope_metrics, "profile": profile},
            ),
            ValidationCheckResult(
                check_id="derived.aggregate_match",
                category="derived",
                status=STATUS_SKIP,
                target=target,
                message="Skipped because loading derived 1d scope failed.",
                metrics={**scope_metrics, "profile": profile},
            ),
            ValidationCheckResult(
                check_id="derived.optional_rollup_fields",
                category="derived",
                status=STATUS_SKIP,
                target=target,
                message="Skipped because loading derived 1d scope failed.",
                metrics={**scope_metrics, "profile": profile},
            ),
        ]
        run_result = build_validation_run_result(
            validator=validator_name,
            checks=checks,
            output_dir=output_dir_path,
            run_id=run_id,
        )
        write_run_artifacts(output_dir_path, run_result)
        return run_result

    invalid_partitions: list[dict[str, str]] = []
    for file_rel in output_file_rels:
        parts = parse_lake_partitions(file_rel)
        has_month = "month" in parts
        has_date = "date" in parts
        if has_month and not has_date:
            continue
        if not has_month and has_date:
            reason = "uses_date_partition_for_1d"
        elif has_month and has_date:
            reason = "contains_both_month_and_date_partitions"
        else:
            reason = "missing_month_partition"
        invalid_partitions.append(
            {
                "file_rel": file_rel,
                "reason": reason,
                "symbol": parts.get("symbol", ""),
                "month": parts.get("month", ""),
                "date": parts.get("date", ""),
            }
        )
    invalid_partitions_df = pd.DataFrame(invalid_partitions)

    partition_artifacts: list[ValidationArtifact] | None = None
    partition_status = STATUS_PASS if invalid_partitions_df.empty else STATUS_FAIL
    partition_message = (
        "All scoped derived 1d outputs use month= partitions."
        if invalid_partitions_df.empty
        else "Found scoped derived 1d outputs that do not use month= partitioning."
    )
    if partition_status == STATUS_FAIL and profile == "full":
        check_dir = ensure_output_dir(output_dir_path / "artifacts" / "derived.partition_shape")
        bad_paths = write_dataframe_csv(check_dir, "invalid_partition_paths.csv", invalid_partitions_df)
        summary_path = write_json_payload(
            check_dir,
            "summary.json",
            {
                "invalid_partition_files": int(len(invalid_partitions_df)),
                "scoped_output_files": int(len(output_file_rels)),
            },
        )
        partition_artifacts = [
            ValidationArtifact(
                kind="csv",
                path=bad_paths.as_posix(),
                description="Scoped output files with non-month partition layout for 1d data.",
            ),
            ValidationArtifact(
                kind="json",
                path=summary_path.as_posix(),
                description="Partition shape mismatch summary.",
            ),
        ]

    key_cols = ["symbol", "date"]
    if actual_df.empty:
        duplicate_keys_df = pd.DataFrame(columns=["symbol", "date", "row_count"])
        null_key_rows = 0
        actual_unique_df = actual_df.copy()
    else:
        duplicate_keys_df = (
            actual_df.groupby(key_cols, dropna=False).size().reset_index(name="row_count").query("row_count > 1")
        )
        null_key_rows = int(actual_df[actual_df["symbol"].isna() | actual_df["date"].isna()].shape[0])
        actual_unique_df = (
            actual_df.sort_values(key_cols, kind="mergesort")
            .drop_duplicates(subset=key_cols, keep="first")
            .reset_index(drop=True)
        )

    duplicate_row_excess = (
        int((duplicate_keys_df["row_count"] - 1).sum()) if not duplicate_keys_df.empty else 0
    )
    unique_status = STATUS_PASS if duplicate_row_excess == 0 and null_key_rows == 0 else STATUS_FAIL
    unique_message = (
        "Derived 1d output has unique symbol-date keys."
        if unique_status == STATUS_PASS
        else "Duplicate or null symbol-date keys detected in derived 1d output."
    )
    unique_artifacts: list[ValidationArtifact] | None = None
    if unique_status == STATUS_FAIL and profile == "full":
        check_dir = ensure_output_dir(output_dir_path / "artifacts" / "derived.unique_symbol_date")
        dup_path = write_dataframe_csv(check_dir, "duplicate_symbol_date_keys.csv", duplicate_keys_df)
        summary_path = write_json_payload(
            check_dir,
            "summary.json",
            {
                "duplicate_keys": int(len(duplicate_keys_df)),
                "duplicate_row_excess": int(duplicate_row_excess),
                "null_key_rows": int(null_key_rows),
            },
        )
        unique_artifacts = [
            ValidationArtifact(
                kind="csv",
                path=dup_path.as_posix(),
                description="Duplicate output keys grouped by symbol and date.",
            ),
            ValidationArtifact(
                kind="json",
                path=summary_path.as_posix(),
                description="Duplicate key summary.",
            ),
        ]

    required_fields = list(_REQUIRED_FIELDS[dataset])
    missing_required_output_cols = sorted(set(required_fields) - set(output_cols))
    merge_cols = ["symbol", "date", *required_fields]
    merged = expected_df[merge_cols].merge(
        actual_unique_df[merge_cols],
        on=["symbol", "date"],
        how="outer",
        suffixes=("_expected", "_actual"),
        indicator=True,
    )

    missing_day_df = merged.loc[merged["_merge"] == "left_only"].copy()
    extra_day_df = merged.loc[merged["_merge"] == "right_only"].copy()
    matched = merged.loc[merged["_merge"] == "both"].copy()

    mismatch_flag_cols: list[str] = []
    for field in required_fields:
        flag_col = f"{field}_mismatch"
        mismatch_flag_cols.append(flag_col)
        matched[flag_col] = _series_diff_mask(
            matched[f"{field}_expected"],
            matched[f"{field}_actual"],
        )

    if mismatch_flag_cols:
        matched["mismatch_columns"] = matched[mismatch_flag_cols].apply(
            lambda row: ",".join(
                field for field, is_mismatch in zip(required_fields, row.tolist()) if bool(is_mismatch)
            ),
            axis=1,
        )
        value_mismatch_df = matched.loc[matched["mismatch_columns"] != ""].copy()
    else:
        matched["mismatch_columns"] = ""
        value_mismatch_df = matched.iloc[0:0].copy()

    with duckdb.connect(":memory:") as fp_con:
        fp_con.execute("SET TimeZone = 'UTC';")
        expected_fp = _fingerprint_dataframe(
            fp_con,
            expected_df,
            cols=["symbol", "date", *required_fields],
        )
        actual_fp = _fingerprint_dataframe(
            fp_con,
            actual_unique_df,
            cols=["symbol", "date", *required_fields],
        )
    fingerprint_match = (
        expected_fp["row_count"] == actual_fp["row_count"]
        and expected_fp["hash_sum"] == actual_fp["hash_sum"]
        and expected_fp["hash_min"] == actual_fp["hash_min"]
        and expected_fp["hash_max"] == actual_fp["hash_max"]
    )

    mismatch_keys = {
        *(tuple(row) for row in missing_day_df[["symbol", "date"]].itertuples(index=False, name=None)),
        *(tuple(row) for row in extra_day_df[["symbol", "date"]].itertuples(index=False, name=None)),
        *(tuple(row) for row in value_mismatch_df[["symbol", "date"]].itertuples(index=False, name=None)),
    }
    mismatch_day_rows = len(mismatch_keys)

    aggregate_fail = (
        len(missing_day_df) > 0
        or len(extra_day_df) > 0
        or len(value_mismatch_df) > 0
        or not fingerprint_match
        or (len(output_file_rels) > 0 and len(missing_required_output_cols) > 0)
    )
    aggregate_status = STATUS_FAIL if aggregate_fail else STATUS_PASS
    aggregate_message = (
        "Derived 1d aggregates match raw 1m rollups."
        if aggregate_status == STATUS_PASS
        else "Derived 1d aggregates do not match raw 1m rollups."
    )
    aggregate_artifacts: list[ValidationArtifact] | None = None
    if aggregate_status == STATUS_FAIL and profile == "full":
        check_dir = ensure_output_dir(output_dir_path / "artifacts" / "derived.aggregate_match")
        missing_path = write_dataframe_csv(check_dir, "missing_output_rows.csv", missing_day_df)
        extra_path = write_dataframe_csv(check_dir, "extra_output_rows.csv", extra_day_df)
        mismatch_cols = [
            "symbol",
            "date",
            "mismatch_columns",
            *[f"{field}_expected" for field in required_fields],
            *[f"{field}_actual" for field in required_fields],
        ]
        value_path = write_dataframe_csv(
            check_dir,
            "value_mismatches.csv",
            value_mismatch_df[mismatch_cols] if not value_mismatch_df.empty else pd.DataFrame(columns=mismatch_cols),
        )
        summary_path = write_json_payload(
            check_dir,
            "summary.json",
            {
                "missing_output_rows": int(len(missing_day_df)),
                "extra_output_rows": int(len(extra_day_df)),
                "value_mismatch_rows": int(len(value_mismatch_df)),
                "mismatched_day_rows": int(mismatch_day_rows),
                "fingerprint_match": bool(fingerprint_match),
                "missing_required_output_columns": missing_required_output_cols,
            },
        )
        aggregate_artifacts = [
            ValidationArtifact(
                kind="csv",
                path=missing_path.as_posix(),
                description="Expected symbol-date rows missing from derived 1d output.",
            ),
            ValidationArtifact(
                kind="csv",
                path=extra_path.as_posix(),
                description="Derived 1d rows that have no backing raw 1m input in scope.",
            ),
            ValidationArtifact(
                kind="csv",
                path=value_path.as_posix(),
                description="Matched symbol-date rows with required field mismatches.",
            ),
            ValidationArtifact(
                kind="json",
                path=summary_path.as_posix(),
                description="Aggregate mismatch summary.",
            ),
        ]

    optional_present = [field for field in _OPTIONAL_ROLLUP_FIELDS if field in output_cols]
    optional_status = STATUS_SKIP
    optional_message = "Optional rollup fields are absent in output; check skipped."
    optional_artifacts: list[ValidationArtifact] | None = None
    optional_metrics: dict[str, int | list[str] | None | str] = {
        **scope_metrics,
        "profile": profile,
        "optional_columns_present": optional_present,
        "compared_day_rows": 0,
        "mismatch_rows": 0,
    }

    if optional_present:
        optional_cols = ["symbol", "date", *optional_present]
        optional_join = expected_df[["symbol", "date", *_OPTIONAL_ROLLUP_FIELDS]].merge(
            actual_unique_df[optional_cols],
            on=["symbol", "date"],
            how="inner",
            suffixes=("_expected", "_actual"),
        )
        optional_flag_cols: list[str] = []
        for field in optional_present:
            flag_col = f"{field}_mismatch"
            optional_flag_cols.append(flag_col)
            optional_join[flag_col] = _series_diff_mask(
                optional_join[f"{field}_expected"],
                optional_join[f"{field}_actual"],
            )

        optional_join["mismatch_columns"] = optional_join[optional_flag_cols].apply(
            lambda row: ",".join(
                field for field, is_mismatch in zip(optional_present, row.tolist()) if bool(is_mismatch)
            ),
            axis=1,
        )
        optional_mismatch_df = optional_join.loc[optional_join["mismatch_columns"] != ""].copy()
        optional_status = STATUS_FAIL if not optional_mismatch_df.empty else STATUS_PASS
        optional_message = (
            "Optional rollup fields match raw 1m rollups for compared rows."
            if optional_status == STATUS_PASS
            else "Optional rollup fields do not match raw 1m rollups."
        )
        optional_metrics.update(
            {
                "compared_day_rows": int(len(optional_join)),
                "mismatch_rows": int(len(optional_mismatch_df)),
            }
        )

        if optional_status == STATUS_FAIL and profile == "full":
            check_dir = ensure_output_dir(output_dir_path / "artifacts" / "derived.optional_rollup_fields")
            optional_cols_out = [
                "symbol",
                "date",
                "mismatch_columns",
                *[f"{field}_expected" for field in optional_present],
                *[f"{field}_actual" for field in optional_present],
            ]
            mismatch_path = write_dataframe_csv(
                check_dir,
                "optional_rollup_mismatches.csv",
                optional_mismatch_df[optional_cols_out]
                if not optional_mismatch_df.empty
                else pd.DataFrame(columns=optional_cols_out),
            )
            summary_path = write_json_payload(
                check_dir,
                "summary.json",
                {
                    "optional_columns_present": optional_present,
                    "compared_day_rows": int(len(optional_join)),
                    "mismatch_rows": int(len(optional_mismatch_df)),
                },
            )
            optional_artifacts = [
                ValidationArtifact(
                    kind="csv",
                    path=mismatch_path.as_posix(),
                    description="Rows where optional lineage rollup fields differ from raw rollups.",
                ),
                ValidationArtifact(
                    kind="json",
                    path=summary_path.as_posix(),
                    description="Optional rollup field mismatch summary.",
                ),
            ]

    checks = [
        ValidationCheckResult(
            check_id="derived.partition_shape",
            category="derived",
            status=partition_status,
            target=target,
            message=partition_message,
            metrics={
                **scope_metrics,
                "profile": profile,
                "scoped_output_files": int(len(output_file_rels)),
                "invalid_partition_files": int(len(invalid_partitions_df)),
            },
            artifacts=partition_artifacts,
        ),
        ValidationCheckResult(
            check_id="derived.unique_symbol_date",
            category="derived",
            status=unique_status,
            target=target,
            message=unique_message,
            metrics={
                **scope_metrics,
                "profile": profile,
                "output_rows": int(len(actual_df)),
                "distinct_symbol_date_rows": int(len(actual_unique_df)),
                "duplicate_keys": int(len(duplicate_keys_df)),
                "duplicate_row_excess": int(duplicate_row_excess),
                "null_key_rows": int(null_key_rows),
            },
            artifacts=unique_artifacts,
        ),
        ValidationCheckResult(
            check_id="derived.aggregate_match",
            category="derived",
            status=aggregate_status,
            target=target,
            message=aggregate_message,
            metrics={
                **scope_metrics,
                "profile": profile,
                "expected_day_rows": int(len(expected_df)),
                "output_day_rows": int(len(actual_unique_df)),
                "missing_day_rows": int(len(missing_day_df)),
                "extra_day_rows": int(len(extra_day_df)),
                "value_mismatch_rows": int(len(value_mismatch_df)),
                "mismatched_day_rows": int(mismatch_day_rows),
                "fingerprint_match": bool(fingerprint_match),
                "expected_hash_sum": expected_fp["hash_sum"],
                "output_hash_sum": actual_fp["hash_sum"],
                "missing_required_output_columns": missing_required_output_cols,
            },
            artifacts=aggregate_artifacts,
        ),
        ValidationCheckResult(
            check_id="derived.optional_rollup_fields",
            category="derived",
            status=optional_status,
            target=target,
            message=optional_message,
            metrics=optional_metrics,
            artifacts=optional_artifacts,
        ),
    ]

    run_result = build_validation_run_result(
        validator=validator_name,
        checks=checks,
        output_dir=output_dir_path,
        run_id=run_id,
    )
    write_run_artifacts(output_dir_path, run_result)
    return run_result
