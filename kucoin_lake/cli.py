from __future__ import annotations

import argparse
from datetime import date
from typing import Iterable, Sequence

from kucoin_lake import api
from kucoin_lake.constants import DEFAULT_FUTURES_DATASETS


def _parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="kucoin-lake",
        description="CLI wrapper around kucoin_lake notebook-friendly APIs.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    build_parser = subparsers.add_parser(
        "build-metadata",
        help="Build or update metadata database.",
    )
    build_parser.add_argument("--nas-root", required=True)
    build_parser.add_argument("--meta-db-path", required=True)
    build_parser.add_argument("--market", default="futures")
    build_parser.add_argument(
        "--datasets",
        nargs="+",
        default=list(DEFAULT_FUTURES_DATASETS),
        help="Datasets to include (space-separated).",
    )
    build_parser.add_argument("--timeframe-filter", default="1m")
    build_parser.add_argument("--liquidity-only", action="store_true")
    build_parser.add_argument("--completeness-threshold", type=float, default=0.98)
    build_parser.add_argument("--incremental-chunk-size", type=int, default=5000)
    build_parser.add_argument(
        "--symbols",
        help="Comma-separated symbols to scan (e.g. BTCUSDTM,ETHUSDTM).",
    )
    build_parser.add_argument(
        "--symbol",
        dest="symbol",
        action="append",
        help="Repeatable symbol filter (may be passed multiple times).",
    )
    build_parser.add_argument("--date-start", help="YYYY-MM-DD (UTC)")
    build_parser.add_argument("--date-end", help="YYYY-MM-DD (UTC)")

    integrity_parser = subparsers.add_parser(
        "build-kline-integrity",
        help="Build or update daily kline integrity checks.",
    )
    integrity_parser.add_argument("--nas-root", required=True)
    integrity_parser.add_argument("--meta-db-path", required=True)
    integrity_parser.add_argument("--market", default="futures")
    integrity_parser.add_argument("--timeframe", default="1m")
    integrity_parser.add_argument("--start-date")
    integrity_parser.add_argument("--end-date")
    integrity_parser.add_argument("--changed-files", nargs="+")
    integrity_parser.add_argument("--recompute", action="store_true")
    integrity_parser.add_argument("--incremental-chunk-size", type=int, default=5000)

    resample_parser = subparsers.add_parser(
        "resample-1m-to-1d",
        help="Resample 1m bars to 1d bars.",
    )
    resample_parser.add_argument("--nas-root", required=True)
    resample_parser.add_argument("--local-staging-dir")
    resample_parser.add_argument("--market", default="futures")
    resample_parser.add_argument("--dataset", default="klines")
    resample_parser.add_argument("--changed-files", nargs="+")
    resample_parser.add_argument("--test-symbols", nargs="+")
    resample_parser.add_argument("--test-months", nargs="+")
    resample_parser.add_argument("--max-tasks", type=int)
    resample_parser.add_argument("--plan-only", action="store_true")
    resample_parser.add_argument(
        "--overwrite",
        dest="overwrite",
        action="store_true",
        default=True,
    )
    resample_parser.add_argument(
        "--no-overwrite",
        dest="overwrite",
        action="store_false",
    )
    resample_parser.add_argument("--threads", type=int, default=4)
    resample_parser.add_argument("--memory-limit")
    resample_parser.add_argument("--verbose", action="store_true")

    ingest_parser = subparsers.add_parser(
        "ingest-local-downloads-to-lake",
        help="Ingest local downloads into the lake.",
    )
    ingest_parser.add_argument("--startdate")
    ingest_parser.add_argument("--enddate")
    ingest_parser.add_argument("--assets", nargs="+")
    ingest_parser.add_argument("--timeframes", nargs="+")
    ingest_parser.add_argument(
        "--include-klines",
        dest="include_klines",
        action="store_true",
        default=True,
    )
    ingest_parser.add_argument(
        "--no-klines",
        dest="include_klines",
        action="store_false",
    )
    ingest_parser.add_argument(
        "--include-funding",
        dest="include_funding",
        action="store_true",
        default=True,
    )
    ingest_parser.add_argument(
        "--no-funding",
        dest="include_funding",
        action="store_false",
    )
    ingest_parser.add_argument(
        "--include-mark",
        dest="include_mark",
        action="store_true",
        default=True,
    )
    ingest_parser.add_argument(
        "--no-mark",
        dest="include_mark",
        action="store_false",
    )
    ingest_parser.add_argument(
        "--include-index",
        dest="include_index",
        action="store_true",
        default=True,
    )
    ingest_parser.add_argument(
        "--no-index",
        dest="include_index",
        action="store_false",
    )
    ingest_parser.add_argument("--local-root")
    ingest_parser.add_argument("--verbose", action="store_true")

    fetch_parser = subparsers.add_parser(
        "fetch-futures",
        help="Fetch KuCoin futures ZIPs to local storage.",
    )
    fetch_parser.add_argument("--out-root", required=True)
    fetch_parser.add_argument("--symbols", nargs="+", required=True)
    fetch_parser.add_argument("--datatype", nargs="+", required=True)
    fetch_parser.add_argument("--timeframe", default="1m")
    fetch_parser.add_argument("--start-date")
    fetch_parser.add_argument("--end-date")
    fetch_parser.add_argument("--days", type=int)
    fetch_parser.add_argument("--sleep-s", type=float, default=0.02)
    fetch_parser.add_argument("--retries", type=int, default=6)
    fetch_parser.add_argument("--backoff-s", type=float, default=1.0)
    fetch_parser.add_argument("--timeout", nargs=2, type=float, metavar=("CONNECT", "READ"), default=(10, 300))
    fetch_parser.add_argument("--dry-run", action="store_true")
    fetch_parser.add_argument(
        "--show-progress",
        dest="show_progress",
        action="store_true",
        default=True,
    )
    fetch_parser.add_argument(
        "--no-progress",
        dest="show_progress",
        action="store_false",
    )
    fetch_parser.add_argument("--verbose", action="store_true")

    return parser.parse_args(argv)


def _print_result(result: dict | None) -> None:
    if result is None:
        return
    print(result)


def _handle_build_metadata(args: argparse.Namespace) -> dict:
    symbols: list[str] = []
    if args.symbols:
        symbols.extend([s.strip() for s in args.symbols.split(",") if s.strip()])
    if args.symbol:
        symbols.extend([s.strip() for s in args.symbol if s.strip()])
    symbol_filter = sorted(set(symbols)) if symbols else None
    date_start = date.fromisoformat(args.date_start) if args.date_start else None
    date_end = date.fromisoformat(args.date_end) if args.date_end else None
    return api.build_metadata(
        args.nas_root,
        market=args.market,
        datasets=cast_iterable(args.datasets),
        meta_db_path=args.meta_db_path,
        timeframe_filter=args.timeframe_filter,
        liquidity_only=args.liquidity_only,
        completeness_threshold=args.completeness_threshold,
        incremental_chunk_size=args.incremental_chunk_size,
        symbols=symbol_filter,
        date_start=date_start,
        date_end=date_end,
    )


def _handle_build_kline_integrity(args: argparse.Namespace) -> dict:
    return api.build_kline_integrity(
        args.nas_root,
        market=args.market,
        meta_db_path=args.meta_db_path,
        timeframe_filter=args.timeframe,
        start_date=args.start_date,
        end_date=args.end_date,
        changed_files=cast_sequence(args.changed_files),
        recompute=args.recompute,
        incremental_chunk_size=args.incremental_chunk_size,
    )


def _handle_resample(args: argparse.Namespace) -> dict:
    return api.resample_1m_to_1d(
        args.nas_root,
        args.local_staging_dir,
        market=args.market,
        dataset=args.dataset,
        changed_files=cast_sequence(args.changed_files),
        test_symbols=cast_sequence(args.test_symbols),
        test_months=cast_sequence(args.test_months),
        max_tasks=args.max_tasks,
        plan_only=args.plan_only,
        overwrite=args.overwrite,
        threads=args.threads,
        memory_limit=args.memory_limit,
        verbose=args.verbose,
    )


def _handle_ingest(args: argparse.Namespace) -> dict:
    return api.ingest_local_downloads_to_lake(
        startdate=args.startdate,
        enddate=args.enddate,
        assets=cast_iterable(args.assets),
        timeframes=cast_iterable(args.timeframes),
        include_klines=args.include_klines,
        include_funding=args.include_funding,
        include_mark=args.include_mark,
        include_index=args.include_index,
        local_root=args.local_root,
        verbose=args.verbose,
    )


def _handle_fetch_futures(args: argparse.Namespace) -> dict:
    return api.fetch_futures(
        args.out_root,
        symbols=cast_sequence(args.symbols) or [],
        datatype=cast_sequence(args.datatype) or [],
        timeframe=args.timeframe,
        start_date=args.start_date,
        end_date=args.end_date,
        days=args.days,
        sleep_s=args.sleep_s,
        retries=args.retries,
        backoff_s=args.backoff_s,
        timeout=tuple(args.timeout),
        dry_run=args.dry_run,
        show_progress=args.show_progress,
        verbose=args.verbose,
    )


def cast_iterable(values: Sequence[str] | None) -> Iterable[str] | None:
    if values is None:
        return None
    return list(values)


def cast_sequence(values: Sequence[str] | None) -> Sequence[str] | None:
    if values is None:
        return None
    return list(values)


def main(argv: Sequence[str] | None = None) -> int:
    args = _parse_args(argv)
    if args.command == "build-metadata":
        result = _handle_build_metadata(args)
    elif args.command == "build-kline-integrity":
        result = _handle_build_kline_integrity(args)
    elif args.command == "resample-1m-to-1d":
        result = _handle_resample(args)
    elif args.command == "ingest-local-downloads-to-lake":
        result = _handle_ingest(args)
    elif args.command == "fetch-futures":
        result = _handle_fetch_futures(args)
    else:
        raise ValueError(f"Unknown command: {args.command}")

    _print_result(result)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
