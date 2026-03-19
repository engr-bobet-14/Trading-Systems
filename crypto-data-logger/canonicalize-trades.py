#!/usr/bin/env python3
"""
Build canonical trades from raw Binance trade tables.

This script is intentionally downstream of raw ingestion. It reads raw
`spot_trades` and `futures_trades`, preserves the raw lake as-is, and writes a
cleaner canonical layer with one row per `(symbol, trade_id)`.
"""

from __future__ import annotations

import argparse
import logging
import os
from collections import defaultdict
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import pyarrow as pa
import pyarrow.parquet as pq


CANONICAL_TRADE_SCHEMA = pa.schema(
    [
        ("symbol", pa.string()),
        ("ts_ms", pa.int64()),
        ("event_time_ms", pa.int64()),
        ("trade_time_ms", pa.int64()),
        ("trade_id", pa.int64()),
        ("price", pa.string()),
        ("quantity", pa.string()),
        ("quote_quantity", pa.string()),
        ("buyer_order_id", pa.int64()),
        ("seller_order_id", pa.int64()),
        ("is_buyer_maker", pa.bool_()),
        ("is_best_match", pa.bool_()),
        ("preferred_source", pa.string()),
        ("has_ws", pa.bool_()),
        ("has_rest", pa.bool_()),
        ("ws_ingest_ts_ms", pa.int64()),
        ("rest_ingest_ts_ms", pa.int64()),
        ("quote_quantity_is_derived", pa.bool_()),
        ("raw_row_count", pa.int64()),
        ("canonicalized_at_ms", pa.int64()),
    ]
)

RAW_TO_CANONICAL_TABLE = {
    "spot_trades": "canonical_spot_trades",
    "futures_trades": "canonical_futures_trades",
}


def as_int(value: Any) -> Optional[int]:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def as_str(value: Any) -> Optional[str]:
    if value is None:
        return None
    return str(value)


def as_bool(value: Any) -> Optional[bool]:
    if value is None or value == "":
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        low = value.strip().lower()
        if low in {"true", "1", "yes", "y"}:
            return True
        if low in {"false", "0", "no", "n"}:
            return False
    if isinstance(value, (int, float)):
        return bool(value)
    return None


def now_utc_ms() -> int:
    import time

    return int(time.time() * 1000)


def parse_csv_list(raw: Optional[str]) -> List[str]:
    if not raw:
        return []
    out: List[str] = []
    seen: set[str] = set()
    for item in raw.split(","):
        value = item.strip().upper()
        if value and value not in seen:
            out.append(value)
            seen.add(value)
    return out


def fsync_file(path: Path) -> None:
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def fsync_directory(path: Path) -> None:
    if not path.exists():
        return
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def score_row(row: Dict[str, Any]) -> Tuple[int, int]:
    populated = sum(1 for value in row.values() if value not in (None, ""))
    ingest_ts_ms = as_int(row.get("ingest_ts_ms")) or 0
    return populated, ingest_ts_ms


def first_non_null(*values: Any) -> Any:
    for value in values:
        if value not in (None, ""):
            return value
    return None


def decimal_product_as_string(price: Optional[str], quantity: Optional[str]) -> Optional[str]:
    if not price or not quantity:
        return None
    try:
        product = Decimal(str(price)) * Decimal(str(quantity))
    except (InvalidOperation, ValueError):
        return None
    text = format(product, "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return text or "0"


@dataclass
class CanonicalTradeAccumulator:
    ws_row: Optional[Dict[str, Any]] = None
    rest_row: Optional[Dict[str, Any]] = None
    fallback_row: Optional[Dict[str, Any]] = None
    raw_row_count: int = 0

    def add_row(self, row: Dict[str, Any]) -> None:
        self.raw_row_count += 1
        source = (as_str(row.get("source")) or "").lower()
        if self.fallback_row is None or score_row(row) >= score_row(self.fallback_row):
            self.fallback_row = row
        if source == "ws":
            if self.ws_row is None or score_row(row) >= score_row(self.ws_row):
                self.ws_row = row
        elif source == "rest":
            if self.rest_row is None or score_row(row) >= score_row(self.rest_row):
                self.rest_row = row

    def finalize(self, canonicalized_at_ms: int, derive_quote_quantity: bool) -> Optional[Dict[str, Any]]:
        base = self.rest_row or self.ws_row or self.fallback_row
        if base is None:
            return None

        symbol = as_str(first_non_null(base.get("symbol"), self.ws_row and self.ws_row.get("symbol")))
        trade_id = as_int(first_non_null(base.get("trade_id"), self.ws_row and self.ws_row.get("trade_id")))
        if not symbol or trade_id is None:
            return None

        rest = self.rest_row or {}
        ws = self.ws_row or {}

        price = as_str(first_non_null(rest.get("price"), ws.get("price"), base.get("price")))
        quantity = as_str(first_non_null(rest.get("quantity"), ws.get("quantity"), base.get("quantity")))
        quote_quantity = as_str(
            first_non_null(rest.get("quote_quantity"), ws.get("quote_quantity"), base.get("quote_quantity"))
        )
        quote_quantity_is_derived = False
        if derive_quote_quantity and not quote_quantity:
            derived_value = decimal_product_as_string(price, quantity)
            if derived_value is not None:
                quote_quantity = derived_value
                quote_quantity_is_derived = True

        trade_time_ms = as_int(first_non_null(rest.get("trade_time_ms"), ws.get("trade_time_ms"), base.get("trade_time_ms")))
        event_time_ms = as_int(first_non_null(ws.get("event_time_ms"), rest.get("event_time_ms"), base.get("event_time_ms")))
        ts_ms = as_int(first_non_null(trade_time_ms, event_time_ms, base.get("ts_ms")))

        return {
            "symbol": symbol,
            "ts_ms": ts_ms,
            "event_time_ms": event_time_ms,
            "trade_time_ms": trade_time_ms,
            "trade_id": trade_id,
            "price": price,
            "quantity": quantity,
            "quote_quantity": quote_quantity,
            "buyer_order_id": as_int(first_non_null(ws.get("buyer_order_id"), rest.get("buyer_order_id"), base.get("buyer_order_id"))),
            "seller_order_id": as_int(first_non_null(ws.get("seller_order_id"), rest.get("seller_order_id"), base.get("seller_order_id"))),
            "is_buyer_maker": as_bool(first_non_null(rest.get("is_buyer_maker"), ws.get("is_buyer_maker"), base.get("is_buyer_maker"))),
            "is_best_match": as_bool(first_non_null(rest.get("is_best_match"), ws.get("is_best_match"), base.get("is_best_match"))),
            "preferred_source": "rest" if self.rest_row is not None else "ws" if self.ws_row is not None else as_str(base.get("source")),
            "has_ws": self.ws_row is not None,
            "has_rest": self.rest_row is not None,
            "ws_ingest_ts_ms": as_int(self.ws_row.get("ingest_ts_ms")) if self.ws_row else None,
            "rest_ingest_ts_ms": as_int(self.rest_row.get("ingest_ts_ms")) if self.rest_row else None,
            "quote_quantity_is_derived": quote_quantity_is_derived,
            "raw_row_count": self.raw_row_count,
            "canonicalized_at_ms": canonicalized_at_ms,
        }


def iter_partition_dirs(root_dir: Path, raw_table: str, symbols: Sequence[str]) -> Iterable[Tuple[str, Path]]:
    table_root = root_dir / raw_table
    if not table_root.exists():
        return []
    symbol_filter = set(symbols)
    out: List[Tuple[str, Path]] = []
    for date_dir in sorted(table_root.glob("date=*")):
        for instrument_dir in sorted(date_dir.glob("instrument=*")):
            symbol = instrument_dir.name.split("=", 1)[-1]
            if symbol_filter and symbol not in symbol_filter:
                continue
            for hour_dir in sorted(instrument_dir.glob("hour=*")):
                out.append((symbol, hour_dir))
    return out


def build_canonical_rows(files: Sequence[Path], derive_quote_quantity: bool) -> List[Dict[str, Any]]:
    if not files:
        return []
    table = pq.read_table([str(path) for path in files])
    accumulators: Dict[Tuple[str, int], CanonicalTradeAccumulator] = {}
    for row in table.to_pylist():
        symbol = as_str(row.get("symbol"))
        trade_id = as_int(row.get("trade_id"))
        if not symbol or trade_id is None:
            continue
        key = (symbol, trade_id)
        accumulator = accumulators.get(key)
        if accumulator is None:
            accumulator = CanonicalTradeAccumulator()
            accumulators[key] = accumulator
        accumulator.add_row(row)

    canonicalized_at_ms = now_utc_ms()
    rows: List[Dict[str, Any]] = []
    for accumulator in accumulators.values():
        row = accumulator.finalize(canonicalized_at_ms=canonicalized_at_ms, derive_quote_quantity=derive_quote_quantity)
        if row is not None:
            rows.append(row)
    rows.sort(key=lambda row: ((as_int(row.get("ts_ms")) or 0), (as_int(row.get("trade_id")) or 0)))
    return rows


def write_canonical_partition(output_file: Path, rows: Sequence[Dict[str, Any]]) -> None:
    output_file.parent.mkdir(parents=True, exist_ok=True)
    tmp_file = output_file.with_name(f".{output_file.name}.{os.getpid()}.tmp")
    arrays = [pa.array([row.get(field.name) for row in rows], type=field.type) for field in CANONICAL_TRADE_SCHEMA]
    table = pa.Table.from_arrays(arrays, schema=CANONICAL_TRADE_SCHEMA)
    pq.write_table(table, tmp_file, compression="snappy")
    fsync_file(tmp_file)
    os.replace(tmp_file, output_file)
    fsync_directory(output_file.parent)


def canonical_output_file(output_root: Path, raw_table: str, partition_dir: Path) -> Path:
    relative = partition_dir.relative_to(output_root / raw_table)
    table_name = RAW_TO_CANONICAL_TABLE[raw_table]
    return output_root / table_name / relative / "canonical.parquet"


def process_market(
    root_dir: Path,
    raw_table: str,
    symbols: Sequence[str],
    derive_quote_quantity: bool,
    logger: logging.Logger,
) -> Tuple[int, int]:
    partition_dirs = list(iter_partition_dirs(root_dir, raw_table, symbols))
    partitions_written = 0
    rows_written = 0
    logger.info("Processing %s partitions for %s", len(partition_dirs), raw_table)
    for _symbol, partition_dir in partition_dirs:
        files = sorted(partition_dir.glob("*.parquet"))
        if not files:
            continue
        rows = build_canonical_rows(files, derive_quote_quantity=derive_quote_quantity)
        if not rows:
            continue
        output_file = canonical_output_file(root_dir, raw_table, partition_dir)
        write_canonical_partition(output_file, rows)
        partitions_written += 1
        rows_written += len(rows)
    return partitions_written, rows_written


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build canonical Binance trades from raw parquet tables")
    parser.add_argument("--root-dir", default="parquet_binance-datasets")
    parser.add_argument("--markets", default="spot,futures", help="Comma-separated: spot,futures")
    parser.add_argument("--symbols", default="", help="Optional comma-separated symbol filter")
    parser.add_argument("--log-level", default="INFO")
    parser.add_argument(
        "--derive-quote-quantity",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Derive quote_quantity as price * quantity when REST did not provide quoteQty.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    logging.basicConfig(
        level=getattr(logging, str(args.log_level).upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(message)s",
    )
    logger = logging.getLogger("canonicalize-trades")
    root_dir = Path(args.root_dir)
    if not root_dir.exists():
        raise SystemExit(f"Root directory not found: {root_dir}")

    markets = parse_csv_list(args.markets)
    valid_markets = {"SPOT", "FUTURES"}
    if not markets:
        markets = ["SPOT", "FUTURES"]
    invalid = [market for market in markets if market not in valid_markets]
    if invalid:
        raise SystemExit(f"Unsupported markets: {', '.join(invalid)}")

    symbols = parse_csv_list(args.symbols)
    totals: Dict[str, Tuple[int, int]] = {}
    for market in markets:
        raw_table = "spot_trades" if market == "SPOT" else "futures_trades"
        partitions_written, rows_written = process_market(
            root_dir=root_dir,
            raw_table=raw_table,
            symbols=symbols,
            derive_quote_quantity=bool(args.derive_quote_quantity),
            logger=logger,
        )
        totals[raw_table] = (partitions_written, rows_written)
        logger.info(
            "Completed %s: partitions_written=%d rows_written=%d",
            raw_table,
            partitions_written,
            rows_written,
        )

    logger.info("Canonicalization finished: %s", totals)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
