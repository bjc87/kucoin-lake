from __future__ import annotations

import os
from collections import Counter
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Iterable, Optional, Sequence

import duckdb
import pandas as pd

from kucoin_lake.constants import DEFAULT_FUTURES_DATASETS
from kucoin_lake.manifest import iter_data_parquets
from kucoin_lake.metadata import build_or_update_metadata
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


@dataclass(frozen=True)
class _TableSpec:
    table: str
    check_id: str
    volatile_cols: tuple[str, ...]
    pk_cols: tuple[str, ...]


_TABLE_SPECS: tuple[_TableSpec, ...] = (
    _TableSpec(
        table="md.md_file_manifest",
        check_id="rebuild.file_manifest",
        volatile_cols=("first_seen_utc", "last_seen_utc"),
        pk_cols=("file_rel",),
    ),
    _TableSpec(
        table="md.md_partition_coverage",
        check_id="rebuild.partition_coverage",
        volatile_cols=("computed_at_utc",),
        pk_cols=("market", "dataset", "symbol", "date", "timeframe"),
    ),
    _TableSpec(
        table="md.md_symbol_dataset_stats",
        check_id="rebuild.symbol_dataset_stats",
        volatile_cols=("last_refreshed_utc",),
        pk_cols=("market", "dataset", "symbol", "timeframe"),
    ),
    _TableSpec(
        table="md.md_alignment_summary",
        check_id="rebuild.alignment_summary",
        volatile_cols=("computed_at_utc",),
        pk_cols=("market", "timeframe", "symbol", "date"),
    ),
    _TableSpec(
        table="md.md_liquidity_daily",
        check_id="rebuild.liquidity_daily",
        volatile_cols=("computed_at_utc",),
        pk_cols=("market", "timeframe", "symbol", "date"),
    ),
    _TableSpec(
        table="md.md_kline_integrity_day",
        check_id="rebuild.kline_integrity_day",
        volatile_cols=("computed_at_utc",),
        pk_cols=("market", "symbol", "date", "timeframe"),
    ),
)


def _quote_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _sql_list(values: Sequence[str]) -> str:
    if not values:
        return "(NULL)"
    return "(" + ",".join(_quote_literal(v) for v in values) + ")"


def _sql_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _normalize_date(value: date | str | None) -> date | None:
    if value is None or isinstance(value, date):
        return value
    return date.fromisoformat(value)


def _table_columns(con: duckdb.DuckDBPyConnection, table: str) -> list[str]:
    cur = con.execute(f"SELECT * FROM {table} LIMIT 0;")
    return [col[0] for col in cur.description]


def _manifest_scope_file_rels(
    nas_root: Path,
    *,
    market: str,
    datasets: Sequence[str],
    timeframe_filter: str,
    symbols: Sequence[str] | None,
    date_start: date | None,
    date_end: date | None,
) -> list[str]:
    files = list(
        iter_data_parquets(
            nas_root,
            market=market,
            datasets=datasets,
            timeframe_filter=timeframe_filter,
            symbols=symbols,
            date_start=date_start,
            date_end=date_end,
        )
    )
    return sorted({f.file_rel for f in files})


def _scope_where_clause(
    table: str,
    *,
    columns: Sequence[str],
    market: str,
    datasets: Sequence[str],
    timeframe_filter: str,
    symbols: Sequence[str] | None,
    date_start: date | None,
    date_end: date | None,
    file_rels: Sequence[str],
    alias: str = "t",
) -> str:
    p = f"{alias}."
    clauses: list[str] = []
    colset = set(columns)

    if "market" in colset:
        clauses.append(f"{p}market = {_quote_literal(market)}")
    if "dataset" in colset:
        clauses.append(f"{p}dataset IN {_sql_list(datasets)}")

    if table == "md.md_file_manifest":
        if file_rels:
            clauses.append(f"{p}file_rel IN {_sql_list(file_rels)}")
        else:
            clauses.append("1 = 0")
    elif "timeframe" in colset:
        if table == "md.md_partition_coverage":
            clauses.append(
                "("
                f"{p}timeframe = {_quote_literal(timeframe_filter)} "
                f"OR ({_quote_literal(timeframe_filter)} = '1m' AND {p}dataset = 'funding' AND {p}timeframe = '')"
                ")"
            )
        else:
            clauses.append(f"{p}timeframe = {_quote_literal(timeframe_filter)}")

    if symbols and "symbol" in colset:
        clauses.append(f"{p}symbol IN {_sql_list(sorted(set(symbols)))}")
    if date_start is not None and "date" in colset:
        clauses.append(f"{p}date >= CAST({_quote_literal(date_start.isoformat())} AS DATE)")
    if date_end is not None and "date" in colset:
        clauses.append(f"{p}date <= CAST({_quote_literal(date_end.isoformat())} AS DATE)")

    if not clauses:
        return ""
    return " WHERE " + " AND ".join(clauses)


def _fingerprint_metrics(
    con: duckdb.DuckDBPyConnection,
    *,
    table: str,
    stable_cols: Sequence[str],
    where_clause: str,
) -> dict[str, str | int]:
    if not stable_cols:
        raise ValueError(f"No stable columns to fingerprint for table {table}")
    cols_sql = ", ".join(_sql_ident(c) for c in stable_cols)
    sql = f"""
    WITH base AS (
        SELECT hash({cols_sql}) AS row_hash
        FROM {table} t
        {where_clause}
    )
    SELECT
        COUNT(*)::BIGINT AS row_count,
        COALESCE(SUM(row_hash::HUGEINT), 0)::VARCHAR AS hash_sum,
        COALESCE(MIN(row_hash), 0)::VARCHAR AS hash_min,
        COALESCE(MAX(row_hash), 0)::VARCHAR AS hash_max
    FROM base;
    """
    row = con.execute(sql).fetchone()
    return {
        "row_count": int(row[0]),
        "hash_sum": str(row[1]),
        "hash_min": str(row[2]),
        "hash_max": str(row[3]),
    }


def _load_stable_projection(
    con: duckdb.DuckDBPyConnection,
    *,
    table: str,
    stable_cols: Sequence[str],
    where_clause: str,
) -> pd.DataFrame:
    cols_sql = ", ".join(_sql_ident(c) for c in stable_cols)
    order_sql = ", ".join(_sql_ident(c) for c in stable_cols)
    sql = f"""
    SELECT {cols_sql}
    FROM {table} t
    {where_clause}
    ORDER BY {order_sql};
    """
    return con.execute(sql).df()


def _counter_from_frame(frame: pd.DataFrame) -> Counter[tuple]:
    return Counter(tuple(row) for row in frame.itertuples(index=False, name=None))


def _expand_counter(counter: Counter[tuple], columns: Sequence[str]) -> pd.DataFrame:
    rows: list[tuple] = []
    for row, count in sorted(counter.items(), key=lambda item: item[0]):
        rows.extend([row] * count)
    if rows:
        df = pd.DataFrame(rows, columns=list(columns))
        return df.sort_values(list(columns), kind="mergesort").reset_index(drop=True)
    return pd.DataFrame(columns=list(columns))


def _materialize_diff_artifacts(
    *,
    source_con: duckdb.DuckDBPyConnection,
    rebuild_con: duckdb.DuckDBPyConnection,
    table: str,
    check_id: str,
    stable_cols: Sequence[str],
    source_where: str,
    rebuild_where: str,
    output_dir: Path,
) -> list[ValidationArtifact]:
    src_df = _load_stable_projection(
        source_con,
        table=table,
        stable_cols=stable_cols,
        where_clause=source_where,
    )
    reb_df = _load_stable_projection(
        rebuild_con,
        table=table,
        stable_cols=stable_cols,
        where_clause=rebuild_where,
    )

    src_counter = _counter_from_frame(src_df)
    reb_counter = _counter_from_frame(reb_df)

    only_src = src_counter - reb_counter
    only_reb = reb_counter - src_counter

    check_dir = ensure_output_dir(output_dir / "artifacts" / check_id)

    only_src_df = _expand_counter(only_src, stable_cols)
    only_reb_df = _expand_counter(only_reb, stable_cols)

    only_src_path = write_dataframe_csv(check_dir, "only_in_source.csv", only_src_df)
    only_reb_path = write_dataframe_csv(check_dir, "only_in_rebuild.csv", only_reb_df)
    summary_path = write_json_payload(
        check_dir,
        "diff_summary.json",
        {
            "table": table,
            "check_id": check_id,
            "only_in_source_rows": int(len(only_src_df)),
            "only_in_rebuild_rows": int(len(only_reb_df)),
        },
    )

    return [
        ValidationArtifact(
            kind="csv",
            path=only_src_path.as_posix(),
            description="Rows present in source metadata but missing from rebuilt metadata.",
        ),
        ValidationArtifact(
            kind="csv",
            path=only_reb_path.as_posix(),
            description="Rows present in rebuilt metadata but missing from source metadata.",
        ),
        ValidationArtifact(
            kind="json",
            path=summary_path.as_posix(),
            description="Row-level diff summary for stable projection mismatch.",
        ),
    ]


def _duplicate_row_count(
    con: duckdb.DuckDBPyConnection,
    *,
    table: str,
    key_cols: Sequence[str],
    where_clause: str,
) -> int:
    key_sql = ", ".join(_sql_ident(col) for col in key_cols)
    sql = f"""
    SELECT COALESCE(SUM(cnt - 1), 0)::BIGINT
    FROM (
        SELECT COUNT(*)::BIGINT AS cnt
        FROM {table} t
        {where_clause}
        GROUP BY {key_sql}
        HAVING COUNT(*) > 1
    ) d;
    """
    return int(con.execute(sql).fetchone()[0])


def _cleanup_temp_db(path: Path) -> None:
    for suffix in ("", ".wal"):
        p = Path(path.as_posix() + suffix)
        if p.exists():
            os.remove(p)


def validate_metadata(
    nas_root: str | Path,
    *,
    meta_db_path: str | Path,
    market: str = "futures",
    datasets: Iterable[str] = DEFAULT_FUTURES_DATASETS,
    timeframe_filter: str = "1m",
    symbols: Optional[Sequence[str]] = None,
    date_start: Optional[date | str] = None,
    date_end: Optional[date | str] = None,
    output_dir: Optional[str | Path] = None,
    profile: str = "smoke",
    keep_temp_db: bool = False,
) -> ValidationRunResult:
    if profile not in {"smoke", "full"}:
        raise ValueError("profile must be one of: 'smoke', 'full'")

    nas_root = Path(nas_root)
    source_meta_db = Path(meta_db_path)
    datasets_list = list(dict.fromkeys(datasets))
    symbols_norm = sorted(set(symbols)) if symbols else None
    date_start_norm = _normalize_date(date_start)
    date_end_norm = _normalize_date(date_end)

    run_id = default_run_id("metadata")
    output_dir_path = Path(output_dir) if output_dir is not None else source_meta_db.parent / "validation" / run_id
    ensure_output_dir(output_dir_path)

    temp_db_path = output_dir_path / "temp_metadata_rebuild.duckdb"
    checks: list[ValidationCheckResult] = []

    build_ok = True
    try:
        if temp_db_path.exists():
            _cleanup_temp_db(temp_db_path)
        build_or_update_metadata(
            nas_root,
            market=market,
            datasets=datasets_list,
            meta_db_path=temp_db_path,
            timeframe_filter=timeframe_filter,
            symbols=symbols_norm,
            date_start=date_start_norm,
            date_end=date_end_norm,
        )
        checks.append(
            ValidationCheckResult(
                check_id="rebuild.temp_build",
                category="rebuild",
                status=STATUS_PASS,
                target=temp_db_path.as_posix(),
                message="Temporary metadata rebuild completed successfully.",
                metrics={"profile": profile},
            )
        )
    except Exception as exc:
        build_ok = False
        checks.append(
            ValidationCheckResult(
                check_id="rebuild.temp_build",
                category="rebuild",
                status=STATUS_ERROR,
                target=temp_db_path.as_posix(),
                message="Temporary metadata rebuild failed.",
                details={"error": str(exc)},
                metrics={"profile": profile},
            )
        )

    source_con: duckdb.DuckDBPyConnection | None = None
    rebuild_con: duckdb.DuckDBPyConnection | None = None
    try:
        try:
            source_con = duckdb.connect(source_meta_db.as_posix(), read_only=True)
            source_con.execute("SET TimeZone = 'UTC';")
        except Exception as exc:
            checks.append(
                ValidationCheckResult(
                    check_id="source.open",
                    category="invariants",
                    status=STATUS_ERROR,
                    target=source_meta_db.as_posix(),
                    message="Failed to open source metadata DB in read-only mode.",
                    details={"error": str(exc)},
                )
            )
            run_result = build_validation_run_result(
                validator="metadata",
                checks=checks,
                output_dir=output_dir_path,
                run_id=run_id,
            )
            write_run_artifacts(output_dir_path, run_result)
            return run_result

        if build_ok:
            rebuild_con = duckdb.connect(temp_db_path.as_posix(), read_only=True)
            rebuild_con.execute("SET TimeZone = 'UTC';")

        manifest_file_rels = _manifest_scope_file_rels(
            nas_root,
            market=market,
            datasets=datasets_list,
            timeframe_filter=timeframe_filter,
            symbols=symbols_norm,
            date_start=date_start_norm,
            date_end=date_end_norm,
        )

        for spec in _TABLE_SPECS:
            if not build_ok or rebuild_con is None:
                checks.append(
                    ValidationCheckResult(
                        check_id=spec.check_id,
                        category="rebuild",
                        status=STATUS_SKIP,
                        target=spec.table,
                        message="Skipped because temporary metadata rebuild failed.",
                    )
                )
                continue

            try:
                source_columns = _table_columns(source_con, spec.table)
                rebuild_columns = _table_columns(rebuild_con, spec.table)
                stable_cols = [c for c in source_columns if c not in set(spec.volatile_cols)]

                if source_columns != rebuild_columns:
                    checks.append(
                        ValidationCheckResult(
                            check_id=spec.check_id,
                            category="rebuild",
                            status=STATUS_FAIL,
                            target=spec.table,
                            message="Column mismatch between source and rebuilt metadata tables.",
                            details={
                                "source_columns": source_columns,
                                "rebuild_columns": rebuild_columns,
                            },
                        )
                    )
                    continue

                source_where = _scope_where_clause(
                    spec.table,
                    columns=source_columns,
                    market=market,
                    datasets=datasets_list,
                    timeframe_filter=timeframe_filter,
                    symbols=symbols_norm,
                    date_start=date_start_norm,
                    date_end=date_end_norm,
                    file_rels=manifest_file_rels,
                    alias="t",
                )
                rebuild_where = _scope_where_clause(
                    spec.table,
                    columns=rebuild_columns,
                    market=market,
                    datasets=datasets_list,
                    timeframe_filter=timeframe_filter,
                    symbols=symbols_norm,
                    date_start=date_start_norm,
                    date_end=date_end_norm,
                    file_rels=manifest_file_rels,
                    alias="t",
                )
                source_metrics = _fingerprint_metrics(
                    source_con,
                    table=spec.table,
                    stable_cols=stable_cols,
                    where_clause=source_where,
                )
                rebuild_metrics = _fingerprint_metrics(
                    rebuild_con,
                    table=spec.table,
                    stable_cols=stable_cols,
                    where_clause=rebuild_where,
                )

                match = (
                    source_metrics["row_count"] == rebuild_metrics["row_count"]
                    and source_metrics["hash_sum"] == rebuild_metrics["hash_sum"]
                    and source_metrics["hash_min"] == rebuild_metrics["hash_min"]
                    and source_metrics["hash_max"] == rebuild_metrics["hash_max"]
                )
                status = STATUS_PASS if match else STATUS_FAIL
                message = (
                    "Stable projection matches rebuilt metadata."
                    if match
                    else "Stable projection mismatch between source and rebuilt metadata."
                )

                artifacts: list[ValidationArtifact] | None = None
                details: dict[str, str] | None = None
                if not match and profile == "full":
                    try:
                        artifacts = _materialize_diff_artifacts(
                            source_con=source_con,
                            rebuild_con=rebuild_con,
                            table=spec.table,
                            check_id=spec.check_id,
                            stable_cols=stable_cols,
                            source_where=source_where,
                            rebuild_where=rebuild_where,
                            output_dir=output_dir_path,
                        )
                    except Exception as exc:
                        status = STATUS_ERROR
                        message = "Stable projection mismatch and failed to materialize diff artifacts."
                        details = {"artifact_error": str(exc)}

                checks.append(
                    ValidationCheckResult(
                        check_id=spec.check_id,
                        category="rebuild",
                        status=status,
                        target=spec.table,
                        message=message,
                        metrics={
                            "profile": profile,
                            "source_row_count": source_metrics["row_count"],
                            "rebuild_row_count": rebuild_metrics["row_count"],
                            "source_hash_sum": source_metrics["hash_sum"],
                            "rebuild_hash_sum": rebuild_metrics["hash_sum"],
                        },
                        artifacts=artifacts,
                        details=details,
                    )
                )
            except Exception as exc:
                checks.append(
                    ValidationCheckResult(
                        check_id=spec.check_id,
                        category="rebuild",
                        status=STATUS_ERROR,
                        target=spec.table,
                        message="Failed to compare stable projection.",
                        details={"error": str(exc)},
                    )
                )

        try:
            if timeframe_filter == "1m":
                checks.append(
                    ValidationCheckResult(
                        check_id="invariant.funding_non_1m_absent",
                        category="invariants",
                        status=STATUS_SKIP,
                        target="md.md_partition_coverage",
                        message="Skipped for timeframe_filter='1m' (funding participation expected).",
                    )
                )
            else:
                funding_cov = source_con.execute(
                    """
                    SELECT COUNT(*)
                    FROM md.md_partition_coverage
                    WHERE market = ?
                      AND dataset = 'funding'
                      AND timeframe = ?;
                    """,
                    [market, timeframe_filter],
                ).fetchone()[0]
                funding_stats = source_con.execute(
                    """
                    SELECT COUNT(*)
                    FROM md.md_symbol_dataset_stats
                    WHERE market = ?
                      AND dataset = 'funding'
                      AND timeframe = ?;
                    """,
                    [market, timeframe_filter],
                ).fetchone()[0]
                funding_align = source_con.execute(
                    """
                    SELECT COUNT(*)
                    FROM md.md_alignment_summary
                    WHERE market = ?
                      AND timeframe = ?
                      AND has_funding = TRUE;
                    """,
                    [market, timeframe_filter],
                ).fetchone()[0]
                violations = int(funding_cov) + int(funding_stats) + int(funding_align)
                checks.append(
                    ValidationCheckResult(
                        check_id="invariant.funding_non_1m_absent",
                        category="invariants",
                        status=STATUS_PASS if violations == 0 else STATUS_FAIL,
                        target=f"market={market},timeframe={timeframe_filter}",
                        message=(
                            "Funding does not leak into non-1m outputs."
                            if violations == 0
                            else "Funding rows found in non-1m outputs."
                        ),
                        metrics={
                            "coverage_rows": int(funding_cov),
                            "symbol_stats_rows": int(funding_stats),
                            "alignment_rows": int(funding_align),
                            "violations": int(violations),
                        },
                    )
                )
        except Exception as exc:
            checks.append(
                ValidationCheckResult(
                    check_id="invariant.funding_non_1m_absent",
                    category="invariants",
                    status=STATUS_ERROR,
                    target="md.*",
                    message="Failed funding non-1m invariant check.",
                    details={"error": str(exc)},
                )
            )

        try:
            bad_rows = source_con.execute(
                """
                SELECT COUNT(*)
                FROM md.md_kline_integrity_day
                WHERE market = ?
                  AND timeframe <> '1m';
                """,
                [market],
            ).fetchone()[0]
            checks.append(
                ValidationCheckResult(
                    check_id="invariant.integrity_timeframe_only_1m",
                    category="invariants",
                    status=STATUS_PASS if int(bad_rows) == 0 else STATUS_FAIL,
                    target="md.md_kline_integrity_day",
                    message=(
                        "Kline integrity rows are limited to timeframe='1m'."
                        if int(bad_rows) == 0
                        else "Found kline integrity rows outside timeframe='1m'."
                    ),
                    metrics={"violations": int(bad_rows)},
                )
            )
        except Exception as exc:
            checks.append(
                ValidationCheckResult(
                    check_id="invariant.integrity_timeframe_only_1m",
                    category="invariants",
                    status=STATUS_ERROR,
                    target="md.md_kline_integrity_day",
                    message="Failed kline integrity timeframe invariant check.",
                    details={"error": str(exc)},
                )
            )

        try:
            cov_cols = _table_columns(source_con, "md.md_partition_coverage")
            cov_where = _scope_where_clause(
                "md.md_partition_coverage",
                columns=cov_cols,
                market=market,
                datasets=datasets_list,
                timeframe_filter=timeframe_filter,
                symbols=symbols_norm,
                date_start=date_start_norm,
                date_end=date_end_norm,
                file_rels=manifest_file_rels,
                alias="c",
            )
            bad_ratios = source_con.execute(
                f"""
                SELECT COUNT(*)
                FROM md.md_partition_coverage c
                {cov_where}
                  AND c.completeness_ratio IS NOT NULL
                  AND (c.completeness_ratio < 0 OR c.completeness_ratio > 1);
                """
            ).fetchone()[0]
            checks.append(
                ValidationCheckResult(
                    check_id="invariant.coverage_completeness_ratio_bounds",
                    category="invariants",
                    status=STATUS_PASS if int(bad_ratios) == 0 else STATUS_FAIL,
                    target="md.md_partition_coverage",
                    message=(
                        "Coverage completeness ratios are null or within [0, 1]."
                        if int(bad_ratios) == 0
                        else "Out-of-range coverage completeness ratios detected."
                    ),
                    metrics={"violations": int(bad_ratios)},
                )
            )
        except Exception as exc:
            checks.append(
                ValidationCheckResult(
                    check_id="invariant.coverage_completeness_ratio_bounds",
                    category="invariants",
                    status=STATUS_ERROR,
                    target="md.md_partition_coverage",
                    message="Failed coverage completeness ratio bounds check.",
                    details={"error": str(exc)},
                )
            )

        try:
            align_cols = _table_columns(source_con, "md.md_alignment_summary")
            align_where = _scope_where_clause(
                "md.md_alignment_summary",
                columns=align_cols,
                market=market,
                datasets=datasets_list,
                timeframe_filter=timeframe_filter,
                symbols=symbols_norm,
                date_start=date_start_norm,
                date_end=date_end_norm,
                file_rels=manifest_file_rels,
                alias="a",
            )
            align_missing = source_con.execute(
                f"""
                SELECT COUNT(*)
                FROM md.md_alignment_summary a
                {align_where}
                  AND NOT EXISTS (
                      SELECT 1
                      FROM md.md_partition_coverage c
                      WHERE c.market = a.market
                        AND c.symbol = a.symbol
                        AND c.date = a.date
                        AND (
                            c.timeframe = a.timeframe
                            OR (a.timeframe = '1m' AND c.dataset = 'funding' AND c.timeframe = '')
                        )
                  );
                """
            ).fetchone()[0]
            checks.append(
                ValidationCheckResult(
                    check_id="invariant.alignment_keys_backed_by_coverage",
                    category="invariants",
                    status=STATUS_PASS if int(align_missing) == 0 else STATUS_FAIL,
                    target="md.md_alignment_summary",
                    message=(
                        "Alignment keys are backed by coverage keys."
                        if int(align_missing) == 0
                        else "Alignment keys missing matching coverage keys."
                    ),
                    metrics={"violations": int(align_missing)},
                )
            )
        except Exception as exc:
            checks.append(
                ValidationCheckResult(
                    check_id="invariant.alignment_keys_backed_by_coverage",
                    category="invariants",
                    status=STATUS_ERROR,
                    target="md.md_alignment_summary",
                    message="Failed alignment-to-coverage key backing check.",
                    details={"error": str(exc)},
                )
            )

        try:
            liq_cols = _table_columns(source_con, "md.md_liquidity_daily")
            liq_where = _scope_where_clause(
                "md.md_liquidity_daily",
                columns=liq_cols,
                market=market,
                datasets=datasets_list,
                timeframe_filter=timeframe_filter,
                symbols=symbols_norm,
                date_start=date_start_norm,
                date_end=date_end_norm,
                file_rels=manifest_file_rels,
                alias="l",
            )
            liq_missing = source_con.execute(
                f"""
                SELECT COUNT(*)
                FROM md.md_liquidity_daily l
                {liq_where}
                  AND NOT EXISTS (
                      SELECT 1
                      FROM md.md_partition_coverage c
                      WHERE c.market = l.market
                        AND c.dataset = 'klines'
                        AND c.timeframe = l.timeframe
                        AND c.symbol = l.symbol
                        AND c.date = l.date
                  );
                """
            ).fetchone()[0]
            checks.append(
                ValidationCheckResult(
                    check_id="invariant.liquidity_keys_backed_by_coverage_klines",
                    category="invariants",
                    status=STATUS_PASS if int(liq_missing) == 0 else STATUS_FAIL,
                    target="md.md_liquidity_daily",
                    message=(
                        "Liquidity keys are backed by klines coverage keys."
                        if int(liq_missing) == 0
                        else "Liquidity keys missing matching klines coverage keys."
                    ),
                    metrics={"violations": int(liq_missing)},
                )
            )
        except Exception as exc:
            checks.append(
                ValidationCheckResult(
                    check_id="invariant.liquidity_keys_backed_by_coverage_klines",
                    category="invariants",
                    status=STATUS_ERROR,
                    target="md.md_liquidity_daily",
                    message="Failed liquidity-to-coverage key backing check.",
                    details={"error": str(exc)},
                )
            )

        try:
            duplicates: dict[str, int] = {}
            total_dups = 0
            for spec in _TABLE_SPECS:
                cols = _table_columns(source_con, spec.table)
                where = _scope_where_clause(
                    spec.table,
                    columns=cols,
                    market=market,
                    datasets=datasets_list,
                    timeframe_filter=timeframe_filter,
                    symbols=symbols_norm,
                    date_start=date_start_norm,
                    date_end=date_end_norm,
                    file_rels=manifest_file_rels,
                    alias="t",
                )
                dups = _duplicate_row_count(
                    source_con,
                    table=spec.table,
                    key_cols=spec.pk_cols,
                    where_clause=where,
                )
                duplicates[spec.table] = int(dups)
                total_dups += int(dups)

            checks.append(
                ValidationCheckResult(
                    check_id="invariant.no_duplicate_primary_keys",
                    category="invariants",
                    status=STATUS_PASS if total_dups == 0 else STATUS_FAIL,
                    target="md.*",
                    message=(
                        "No duplicate primary-key rows detected."
                        if total_dups == 0
                        else "Duplicate primary-key rows detected."
                    ),
                    metrics={
                        "total_duplicate_rows": int(total_dups),
                        "duplicates_by_table": duplicates,
                    },
                )
            )
        except Exception as exc:
            checks.append(
                ValidationCheckResult(
                    check_id="invariant.no_duplicate_primary_keys",
                    category="invariants",
                    status=STATUS_ERROR,
                    target="md.*",
                    message="Failed duplicate primary-key invariant check.",
                    details={"error": str(exc)},
                )
            )

    finally:
        if source_con is not None:
            source_con.close()
        if rebuild_con is not None:
            rebuild_con.close()
        if not keep_temp_db:
            _cleanup_temp_db(temp_db_path)

    run_result = build_validation_run_result(
        validator="metadata",
        checks=checks,
        output_dir=output_dir_path,
        run_id=run_id,
    )
    write_run_artifacts(output_dir_path, run_result)
    return run_result

