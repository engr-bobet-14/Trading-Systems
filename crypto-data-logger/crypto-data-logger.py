#!/usr/bin/env python3
"""
Raw Binance spot + futures ingestion pipeline.

This rewrite stores only normalized raw data and intentionally skips every
derived feature. It uses a fully asynchronous control plane for websocket and
REST ingestion, while offloading Arrow Parquet I/O to a bounded executor so the
event loop stays responsive under heavy load.

Key features:
- Continuous token buckets, per-endpoint min spacing
- Endpoint-scoped, half-open circuits with adaptive decay
- Unified timeout policy + latency buckets
- Strong trades pagination with fromId continuation + dedupe
- Arrow Parquet writer with batched writes, periodic fsync, atomic swap
- Persisted dedupe seeding per shard (reads last few ts_ms)
- Cross-task stats aggregation and graceful shutdown (no os._exit)
- Fully async ingestion with bounded queues and strict write backpressure

Output tables:
- spot_trades
- futures_trades
- spot_l1
- futures_l1
- spot_depth_updates
- futures_depth_updates
- futures_mark_price
- futures_funding_history
"""

from __future__ import annotations

import argparse
import asyncio
import collections
import contextlib
import json
import logging
import math
import os
import random
import signal
import sys
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence, Tuple

import httpx
import pyarrow as pa
import pyarrow.parquet as pq
import structlog
from aiohttp import web
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Gauge, Histogram, generate_latest
from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

try:
    from websockets.asyncio.client import connect as ws_connect
except ImportError:  # pragma: no cover - compatibility fallback
    from websockets import connect as ws_connect


SPOT_REST_BASE = "https://api.binance.com"
FUTURES_REST_BASE = "https://fapi.binance.com"
SPOT_WS_BASE = "wss://stream.binance.com:9443"
FUTURES_WS_BASE = "wss://fstream.binance.com"

DEPTH_LEVEL_TYPE = pa.list_(pa.struct([("price", pa.string()), ("quantity", pa.string())]))

FALLBACK_LIQUID_SYMBOLS = [
    "BTCUSDT",
    "ETHUSDT",
    "SOLUSDT",
    "BNBUSDT",
    "XRPUSDT",
    "ADAUSDT",
]

DEFAULT_REQUESTED_SYMBOLS = [
    "BTCUSDT",
    "ETHUSDT",
    "ADAUSDT",
    "BNBUSDT",
    "DOGEUSDT",
    "MATICUSDT",
    "SOLUSDT",
    "TRXUSDT",
    "XRPUSDT",
    "XMRUSDT",
]

SYMBOL_ALIASES = {
    "MATICUDST": "MATICUSDT",
    "MONERO": "XMRUSDT",
    "MONEROUSDT": "XMRUSDT",
    "XMR": "XMRUSDT",
}

MARKET_SUFFIXES = {
    "spot": ("trade", "bookTicker", "depth@100ms"),
    "futures": ("trade", "bookTicker", "depth@100ms", "markPrice@1s"),
}

TABLE_MARKETS = {
    "spot_trades": "spot",
    "spot_l1": "spot",
    "spot_depth_updates": "spot",
    "futures_trades": "futures",
    "futures_l1": "futures",
    "futures_depth_updates": "futures",
    "futures_mark_price": "futures",
    "futures_funding_history": "futures",
}

TABLE_EXPECTED_RATE_KIND = {
    "spot_trades": "trade",
    "futures_trades": "trade",
    "spot_l1": "l1",
    "futures_l1": "l1",
    "spot_depth_updates": "depth",
    "futures_depth_updates": "depth",
    "futures_mark_price": "mark_price",
    "futures_funding_history": "funding",
}


def now_utc_ms() -> int:
    return int(time.time() * 1000)


def utc_iso_from_ms(ts_ms: int) -> str:
    return datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc).isoformat()


def date_partition_from_ms(ts_ms: int) -> str:
    return datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc).strftime("%Y-%m-%d")


def hour_partition_from_ms(ts_ms: int) -> str:
    return datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc).strftime("%H")


def as_int(value: Any) -> Optional[int]:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def as_float(value: Any) -> Optional[float]:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def as_str(value: Any) -> Optional[str]:
    if value is None:
        return None
    return str(value)


def as_bool(value: Any) -> Optional[bool]:
    if value is None:
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


def latency_bucket_ms(latency_ms: int) -> str:
    if latency_ms < 50:
        return "lt50ms"
    if latency_ms < 100:
        return "lt100ms"
    if latency_ms < 250:
        return "lt250ms"
    if latency_ms < 1000:
        return "lt1000ms"
    return "ge1000ms"


def normalize_symbol(raw_symbol: str) -> str:
    symbol = raw_symbol.strip().upper()
    return SYMBOL_ALIASES.get(symbol, symbol)


def parse_symbol_list(value: Any) -> Optional[List[str]]:
    if value is None:
        return None
    if isinstance(value, str):
        raw = value.strip()
        if not raw or raw.lower() == "auto":
            return None
        items = [item.strip() for item in raw.split(",")]
    elif isinstance(value, (list, tuple, set)):
        items = [str(item).strip() for item in value]
    else:
        raise TypeError(f"Unsupported symbol list value: {type(value)!r}")

    out: List[str] = []
    seen: set[str] = set()
    for item in items:
        if not item:
            continue
        symbol = normalize_symbol(item)
        if symbol and symbol not in seen:
            out.append(symbol)
            seen.add(symbol)
    return out or None


def parse_string_list(value: Any) -> List[str]:
    if value is None:
        return []
    if isinstance(value, str):
        items = [item.strip() for item in value.split(",")]
    elif isinstance(value, (list, tuple, set)):
        items = [str(item).strip() for item in value]
    else:
        raise TypeError(f"Unsupported list value: {type(value)!r}")
    out: List[str] = []
    seen: set[str] = set()
    for item in items:
        normalized = item.upper()
        if normalized and normalized not in seen:
            out.append(normalized)
            seen.add(normalized)
    return out


def normalize_depth_levels(levels: Any) -> List[Dict[str, Optional[str]]]:
    if not isinstance(levels, list):
        return []
    out: List[Dict[str, Optional[str]]] = []
    for level in levels:
        if not isinstance(level, (list, tuple)) or len(level) < 2:
            continue
        out.append(
            {
                "price": as_str(level[0]),
                "quantity": as_str(level[1]),
            }
        )
    return out


def chunked(items: Sequence[str], size: int) -> List[List[str]]:
    size = max(1, size)
    return [list(items[i : i + size]) for i in range(0, len(items), size)]


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


def build_cli_overrides(argv: Optional[Sequence[str]] = None) -> Dict[str, Any]:
    parser = argparse.ArgumentParser(description="Async Binance raw market data logger")
    parser.add_argument("--spot-symbols")
    parser.add_argument("--futures-symbols")
    parser.add_argument("--enable-spot", action=argparse.BooleanOptionalAction, default=None)
    parser.add_argument("--enable-futures", action=argparse.BooleanOptionalAction, default=None)
    parser.add_argument("--out-dir")
    parser.add_argument("--state-dir")
    parser.add_argument("--log-level")
    parser.add_argument("--health-port", type=int)
    parser.add_argument("--queue-maxsize", type=int)
    parser.add_argument("--docker-image")
    parser.add_argument("--print-config", action="store_true")
    args = parser.parse_args(argv)
    overrides = {
        "spot_symbols": args.spot_symbols,
        "futures_symbols": args.futures_symbols,
        "enable_spot": args.enable_spot,
        "enable_futures": args.enable_futures,
        "out_dir": args.out_dir,
        "state_dir": args.state_dir,
        "log_level": args.log_level,
        "health_port": args.health_port,
        "queue_maxsize": args.queue_maxsize,
        "docker_image": args.docker_image,
        "print_config": args.print_config,
    }
    return {key: value for key, value in overrides.items() if value is not None}


class AppSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=False,
    )

    enable_spot: bool = True
    enable_futures: bool = True
    spot_symbols: Optional[List[str]] = Field(default_factory=lambda: list(DEFAULT_REQUESTED_SYMBOLS))
    futures_symbols: Optional[List[str]] = Field(default_factory=lambda: list(DEFAULT_REQUESTED_SYMBOLS))
    fallback_symbols: List[str] = Field(default_factory=lambda: list(FALLBACK_LIQUID_SYMBOLS))
    quote_assets: List[str] = Field(default_factory=lambda: ["USDT"])
    discovery_limit: int = 0

    out_dir: Path = Path("parquet_binance-datasets")
    state_dir: Path = Path("state")
    log_level: str = "INFO"
    print_config: bool = False

    http_timeout_seconds: float = 5.0
    http_connect_timeout_seconds: float = 2.0
    http_pool_timeout_seconds: float = 5.0
    http_keepalive_connections: int = 32
    http_max_connections: int = 128

    queue_maxsize: int = 20000
    parquet_batch_size: int = 512
    segment_max_rows: int = 8192
    segment_max_age_seconds: float = 30.0
    flush_interval_seconds: float = 2.0
    fsync_interval_seconds: float = 15.0
    seed_file_count: int = 4
    seed_row_count: int = 256
    max_open_partition_writers: int = 16
    partition_writer_idle_close_seconds: float = 5.0

    dedupe_window_seconds: int = 900
    seen_capacity: Optional[int] = None
    expected_trade_rows_per_symbol_per_second: float = 25.0
    expected_l1_rows_per_symbol_per_second: float = 10.0
    expected_depth_rows_per_symbol_per_second: float = 10.0
    expected_mark_price_rows_per_symbol_per_second: float = 1.0
    expected_funding_rows_per_symbol_per_second: float = 0.01

    spot_request_weight_per_minute: int = 1100
    futures_request_weight_per_minute: int = 2200
    initial_trade_backfill_trades: int = 1000
    funding_lookback_hours: int = 168

    ws_max_streams_per_connection: int = 120
    ws_recv_timeout_seconds: float = 1.0
    ws_message_queue_size: int = 4096
    ws_ping_interval_seconds_spot: float = 20.0
    ws_ping_timeout_seconds_spot: float = 20.0
    ws_ping_interval_seconds_futures: float = 150.0
    ws_ping_timeout_seconds_futures: float = 60.0

    metrics_host: str = "0.0.0.0"
    health_host: str = "0.0.0.0"
    health_port: int = 8080
    stats_interval_seconds: float = 30.0

    parquet_io_workers: int = 4

    binance_api_key: Optional[str] = None
    binance_api_key_file: Optional[Path] = None

    docker_image: str = "engr-bobet-14/crypto-data-logger:latest"

    @field_validator("spot_symbols", "futures_symbols", mode="before")
    @classmethod
    def _parse_symbols(cls, value: Any) -> Optional[List[str]]:
        return parse_symbol_list(value)

    @field_validator("fallback_symbols", mode="before")
    @classmethod
    def _parse_fallback_symbols(cls, value: Any) -> List[str]:
        parsed = parse_symbol_list(value)
        return parsed or list(FALLBACK_LIQUID_SYMBOLS)

    @field_validator("quote_assets", mode="before")
    @classmethod
    def _parse_quote_assets(cls, value: Any) -> List[str]:
        parsed = parse_string_list(value)
        return parsed or ["USDT"]

    @field_validator("log_level", mode="before")
    @classmethod
    def _normalize_log_level(cls, value: Any) -> str:
        return str(value or "INFO").upper()

    def resolve_api_key(self) -> Optional[str]:
        file_path = self.binance_api_key_file
        if file_path:
            try:
                value = Path(file_path).read_text(encoding="utf-8").strip()
            except FileNotFoundError:
                return self.binance_api_key
            if value:
                return value
        return (self.binance_api_key or "").strip() or None

    def expected_rate_for_table(self, table_name: str) -> float:
        kind = TABLE_EXPECTED_RATE_KIND[table_name]
        if kind == "trade":
            return self.expected_trade_rows_per_symbol_per_second
        if kind == "l1":
            return self.expected_l1_rows_per_symbol_per_second
        if kind == "depth":
            return self.expected_depth_rows_per_symbol_per_second
        if kind == "mark_price":
            return self.expected_mark_price_rows_per_symbol_per_second
        return self.expected_funding_rows_per_symbol_per_second

    def dedupe_capacity_for_table(self, table_name: str, symbol_count: int) -> int:
        if self.seen_capacity is not None:
            return max(1024, int(self.seen_capacity))
        total = math.ceil(max(1, symbol_count) * self.expected_rate_for_table(table_name) * self.dedupe_window_seconds)
        return max(1024, total)


def configure_logging(level: str) -> structlog.stdlib.BoundLogger:
    logging.basicConfig(level=getattr(logging, level.upper(), logging.INFO), format="%(message)s", stream=sys.stdout)
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)
    logging.getLogger("websockets").setLevel(logging.WARNING)
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso", utc=True),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.stdlib.BoundLogger,
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )
    return structlog.get_logger("crypto-data-logger")


@dataclass
class RuntimeState:
    started_at_ms: int = field(default_factory=now_utc_ms)
    shutdown_requested: bool = False
    last_ws_event_ms: Optional[int] = None
    last_rest_event_ms: Optional[int] = None
    last_disk_write_ms: Optional[int] = None
    spot_symbols: List[str] = field(default_factory=list)
    futures_symbols: List[str] = field(default_factory=list)
    websocket_connected: Dict[str, bool] = field(default_factory=dict)

    def mark_ws_event(self, ts_ms: Optional[int] = None) -> None:
        self.last_ws_event_ms = ts_ms or now_utc_ms()

    def mark_rest_event(self, ts_ms: Optional[int] = None) -> None:
        self.last_rest_event_ms = ts_ms or now_utc_ms()

    def mark_disk_write(self, ts_ms: Optional[int] = None) -> None:
        self.last_disk_write_ms = ts_ms or now_utc_ms()

    def health_payload(self, queues: Dict[str, int]) -> Dict[str, Any]:
        now_ms = now_utc_ms()
        return {
            "status": "shutting_down" if self.shutdown_requested else "ok",
            "started_at_ms": self.started_at_ms,
            "started_at_iso": utc_iso_from_ms(self.started_at_ms),
            "uptime_seconds": round((now_ms - self.started_at_ms) / 1000.0, 3),
            "spot_symbol_count": len(self.spot_symbols),
            "futures_symbol_count": len(self.futures_symbols),
            "last_ws_event_age_seconds": None if self.last_ws_event_ms is None else round((now_ms - self.last_ws_event_ms) / 1000.0, 3),
            "last_rest_event_age_seconds": None if self.last_rest_event_ms is None else round((now_ms - self.last_rest_event_ms) / 1000.0, 3),
            "last_disk_write_age_seconds": None if self.last_disk_write_ms is None else round((now_ms - self.last_disk_write_ms) / 1000.0, 3),
            "websocket_connected": dict(self.websocket_connected),
            "queue_sizes": queues,
        }


class Metrics:
    def __init__(self) -> None:
        self.rest_latency_seconds = Histogram(
            "binance_rest_latency_seconds",
            "REST request latency in seconds.",
            labelnames=("endpoint",),
        )
        self.rest_latency_bucket_total = Counter(
            "binance_rest_latency_bucket_total",
            "REST request counts grouped by coarse millisecond bucket.",
            labelnames=("endpoint", "bucket"),
        )
        self.rest_requests_total = Counter(
            "binance_rest_requests_total",
            "REST request outcomes.",
            labelnames=("endpoint", "status"),
        )
        self.rest_retries_total = Counter(
            "binance_rest_retries_total",
            "REST retry attempts.",
            labelnames=("endpoint",),
        )
        self.websocket_messages_total = Counter(
            "binance_websocket_messages_total",
            "Websocket messages accepted.",
            labelnames=("worker",),
        )
        self.websocket_reconnects_total = Counter(
            "binance_websocket_reconnects_total",
            "Websocket reconnect attempts.",
            labelnames=("worker",),
        )
        self.rows_routed_total = Counter(
            "binance_rows_routed_total",
            "Rows routed to table queues.",
            labelnames=("table", "source"),
        )
        self.queue_size = Gauge(
            "binance_table_queue_size",
            "Current queue size for each output table.",
            labelnames=("table",),
        )
        self.queue_put_wait_seconds = Histogram(
            "binance_table_queue_put_wait_seconds",
            "Time spent waiting on a bounded queue put.",
            labelnames=("table",),
        )
        self.queue_backpressure_total = Counter(
            "binance_table_queue_backpressure_total",
            "Times a producer hit a full queue and had to wait.",
            labelnames=("table",),
        )
        self.open_partition_writers = Gauge(
            "binance_open_partition_writers",
            "Currently open partition writers per table.",
            labelnames=("table",),
        )
        self.rate_limit_tokens = Gauge(
            "binance_rate_limit_tokens",
            "Approximate remaining tokens in the token bucket.",
            labelnames=("bucket",),
        )
        self.rate_limit_capacity = Gauge(
            "binance_rate_limit_capacity",
            "Configured token bucket capacity.",
            labelnames=("bucket",),
        )
        self.circuit_state = Gauge(
            "binance_circuit_state",
            "Circuit state by endpoint: 0 closed, 0.5 half-open, 1 open.",
            labelnames=("endpoint",),
        )
        self.disk_write_seconds = Histogram(
            "binance_disk_write_seconds",
            "Time spent in blocking Parquet write operations.",
            labelnames=("table",),
        )
        self.disk_write_rows_total = Counter(
            "binance_disk_write_rows_total",
            "Rows persisted to Parquet.",
            labelnames=("table",),
        )
        self.disk_write_bytes_total = Counter(
            "binance_disk_write_bytes_total",
            "Estimated bytes written to Parquet temp shards.",
            labelnames=("table",),
        )
        self.disk_write_speed_bytes_per_second = Gauge(
            "binance_disk_write_speed_bytes_per_second",
            "Approximate write throughput from the most recent flush.",
            labelnames=("table",),
        )
        self.disk_fsync_total = Counter(
            "binance_disk_fsync_total",
            "Number of fsync operations performed.",
            labelnames=("table",),
        )
        self.deduped_rows_total = Counter(
            "binance_deduped_rows_total",
            "Rows dropped by writer-level dedupe.",
            labelnames=("table",),
        )
        self.invalid_rows_total = Counter(
            "binance_invalid_rows_total",
            "Rows dropped as structurally invalid before persistence.",
            labelnames=("table",),
        )
        self.last_activity_age_seconds = Gauge(
            "binance_last_activity_age_seconds",
            "Age of the last activity timestamp.",
            labelnames=("kind",),
        )

    def observe_rest_latency(self, endpoint: str, latency_ms: int) -> None:
        self.rest_latency_seconds.labels(endpoint=endpoint).observe(latency_ms / 1000.0)
        self.rest_latency_bucket_total.labels(endpoint=endpoint, bucket=latency_bucket_ms(latency_ms)).inc()

    def observe_rate_limit(self, bucket_name: str, tokens: float, capacity: float) -> None:
        self.rate_limit_capacity.labels(bucket=bucket_name).set(capacity)
        self.rate_limit_tokens.labels(bucket=bucket_name).set(max(0.0, tokens))

    def observe_circuit(self, endpoint: str, state: str) -> None:
        value = 0.0
        if state == "half-open":
            value = 0.5
        elif state == "open":
            value = 1.0
        self.circuit_state.labels(endpoint=endpoint).set(value)

    def observe_write(self, table_name: str, rows: int, bytes_written: int, seconds_spent: float, fsync_count: int) -> None:
        if rows:
            self.disk_write_rows_total.labels(table=table_name).inc(rows)
        if bytes_written:
            self.disk_write_bytes_total.labels(table=table_name).inc(bytes_written)
        self.disk_write_seconds.labels(table=table_name).observe(max(0.0, seconds_spent))
        if seconds_spent > 0 and bytes_written > 0:
            self.disk_write_speed_bytes_per_second.labels(table=table_name).set(bytes_written / seconds_spent)
        if fsync_count:
            self.disk_fsync_total.labels(table=table_name).inc(fsync_count)


class MissingApiKeyError(RuntimeError):
    pass


class ShutdownRequested(RuntimeError):
    pass


class BinanceHttpError(RuntimeError):
    def __init__(self, status_code: int, message: str, headers: Optional[Dict[str, Any]] = None):
        super().__init__(message)
        self.status_code = int(status_code)
        self.headers = dict(headers or {})


@dataclass(frozen=True)
class EndpointSpec:
    host_key: str
    path: str
    weight: int
    min_spacing_seconds: float
    timeout_multiplier: float = 1.0
    requires_api_key: bool = False


REST_HOSTS = {
    "spot": SPOT_REST_BASE,
    "futures": FUTURES_REST_BASE,
}

REST_ENDPOINTS: Dict[str, EndpointSpec] = {
    "spot_exchange_info": EndpointSpec("spot", "/api/v3/exchangeInfo", weight=20, min_spacing_seconds=0.1, timeout_multiplier=2.0),
    "futures_exchange_info": EndpointSpec("futures", "/fapi/v1/exchangeInfo", weight=1, min_spacing_seconds=0.1, timeout_multiplier=1.5),
    "spot_recent_trades": EndpointSpec("spot", "/api/v3/trades", weight=25, min_spacing_seconds=0.02),
    "spot_historical_trades": EndpointSpec(
        "spot",
        "/api/v3/historicalTrades",
        weight=25,
        min_spacing_seconds=0.05,
        timeout_multiplier=1.2,
        requires_api_key=True,
    ),
    "futures_recent_trades": EndpointSpec("futures", "/fapi/v1/trades", weight=5, min_spacing_seconds=0.02),
    "futures_historical_trades": EndpointSpec(
        "futures",
        "/fapi/v1/historicalTrades",
        weight=20,
        min_spacing_seconds=0.05,
        timeout_multiplier=1.2,
        requires_api_key=True,
    ),
    "futures_funding_rate": EndpointSpec("futures", "/fapi/v1/fundingRate", weight=1, min_spacing_seconds=0.1),
}


class AsyncTokenBucket:
    def __init__(self, name: str, rpm: int, metrics: Metrics):
        rpm = max(1, int(rpm))
        self.name = name
        self.rate_per_second = float(rpm) / 60.0
        self.capacity = float(rpm)
        self.tokens = float(rpm)
        self.last_refill = time.monotonic()
        self.lock = asyncio.Lock()
        self.metrics = metrics
        self.metrics.observe_rate_limit(self.name, self.tokens, self.capacity)

    async def acquire(self, tokens: float = 1.0) -> None:
        need = max(1.0, float(tokens))
        while True:
            async with self.lock:
                now = time.monotonic()
                elapsed = now - self.last_refill
                if elapsed > 0:
                    self.tokens = min(self.capacity, self.tokens + (elapsed * self.rate_per_second))
                    self.last_refill = now
                if self.tokens >= need:
                    self.tokens -= need
                    self.metrics.observe_rate_limit(self.name, self.tokens, self.capacity)
                    return
                missing = need - self.tokens
                sleep_for = max(0.005, missing / self.rate_per_second)
                self.metrics.observe_rate_limit(self.name, self.tokens, self.capacity)
            await asyncio.sleep(sleep_for)


class AsyncCircuitBreaker:
    def __init__(self, name: str, metrics: Metrics):
        self.name = name
        self.metrics = metrics
        self.state = "closed"
        self.until = 0.0
        self.failures = 0
        self.probe_in_flight = False
        self.lock = asyncio.Lock()
        self.metrics.observe_circuit(self.name, self.state)

    async def wait_until_ready(self, shutdown_event: asyncio.Event) -> None:
        while not shutdown_event.is_set():
            allowed, sleep_for = await self._allow_attempt()
            if allowed:
                return
            await asyncio.sleep(min(0.25, max(0.01, sleep_for)))
        raise ShutdownRequested()

    async def _allow_attempt(self) -> Tuple[bool, float]:
        async with self.lock:
            now = time.monotonic()
            if self.state == "open":
                if now < self.until:
                    self.metrics.observe_circuit(self.name, self.state)
                    return False, self.until - now
                self.state = "half-open"
                if not self.probe_in_flight:
                    self.probe_in_flight = True
                    self.metrics.observe_circuit(self.name, self.state)
                    return True, 0.0
                self.metrics.observe_circuit(self.name, self.state)
                return False, 0.25
            if self.state == "half-open":
                if self.probe_in_flight:
                    self.metrics.observe_circuit(self.name, self.state)
                    return False, 0.25
                self.probe_in_flight = True
                self.metrics.observe_circuit(self.name, self.state)
                return True, 0.0
            self.metrics.observe_circuit(self.name, self.state)
            return True, 0.0

    async def on_success(self) -> None:
        async with self.lock:
            self.state = "closed"
            self.until = 0.0
            self.probe_in_flight = False
            self.failures = max(0, self.failures // 2)
            self.metrics.observe_circuit(self.name, self.state)

    async def on_failure(self, base_seconds: float) -> None:
        async with self.lock:
            self.failures += 1
            decay = min(2.0 ** (self.failures - 1), 8.0)
            self.state = "open"
            self.until = time.monotonic() + (base_seconds * decay)
            self.probe_in_flight = False
            self.metrics.observe_circuit(self.name, self.state)


class CircuitRegistry:
    def __init__(self, metrics: Metrics):
        self.metrics = metrics
        self._circuits: Dict[str, AsyncCircuitBreaker] = {}

    def get(self, name: str) -> AsyncCircuitBreaker:
        circuit = self._circuits.get(name)
        if circuit is None:
            circuit = AsyncCircuitBreaker(name=name, metrics=self.metrics)
            self._circuits[name] = circuit
        return circuit


def is_retryable(exc: Exception) -> bool:
    if isinstance(exc, (httpx.TimeoutException, httpx.NetworkError)):
        return True
    if isinstance(exc, BinanceHttpError):
        return exc.status_code in {418, 429, 500, 502, 503, 504}
    return False


def retry_after_seconds(exc: Exception) -> Optional[float]:
    headers = getattr(exc, "headers", None) or {}
    value = headers.get("Retry-After")
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def backoff_seconds(exc: Exception, attempt: int) -> float:
    if isinstance(exc, BinanceHttpError):
        if exc.status_code == 418:
            base, cap = 10.0, 1800.0
        elif exc.status_code == 429:
            base, cap = 2.0, 120.0
        else:
            base, cap = 0.75, 20.0
    elif isinstance(exc, (httpx.TimeoutException, httpx.NetworkError)):
        base, cap = 0.75, 15.0
    else:
        base, cap = 0.5, 6.0
    override = retry_after_seconds(exc)
    delay = (override if override is not None else (base * (2**attempt))) + random.uniform(0.0, base)
    return min(delay, cap)


async def wait_with_shutdown(shutdown_event: asyncio.Event, seconds: float) -> None:
    try:
        await asyncio.wait_for(shutdown_event.wait(), timeout=max(0.0, seconds))
    except asyncio.TimeoutError:
        return
    raise ShutdownRequested()


class AsyncBinanceTransport:
    def __init__(
        self,
        settings: AppSettings,
        api_key: Optional[str],
        metrics: Metrics,
        circuits: CircuitRegistry,
        shutdown_event: asyncio.Event,
        logger: structlog.stdlib.BoundLogger,
    ):
        self.settings = settings
        self.api_key = api_key or ""
        self.metrics = metrics
        self.circuits = circuits
        self.shutdown_event = shutdown_event
        self.logger = logger.bind(component="transport")
        self.buckets = {
            "spot": AsyncTokenBucket("spot", settings.spot_request_weight_per_minute, metrics),
            "futures": AsyncTokenBucket("futures", settings.futures_request_weight_per_minute, metrics),
        }
        self._min_spacing_locks = {name: asyncio.Lock() for name in REST_ENDPOINTS}
        self._last_call: Dict[str, float] = {}
        self.client = httpx.AsyncClient(
            headers={
                "Accept": "application/json",
                "Connection": "keep-alive",
                "User-Agent": "crypto-data-logger/async-raw-ingestor",
            },
            limits=httpx.Limits(
                max_connections=settings.http_max_connections,
                max_keepalive_connections=settings.http_keepalive_connections,
            ),
            timeout=httpx.Timeout(
                timeout=settings.http_timeout_seconds,
                connect=settings.http_connect_timeout_seconds,
                pool=settings.http_pool_timeout_seconds,
            ),
        )

    async def aclose(self) -> None:
        await self.client.aclose()

    async def _respect_min_spacing(self, endpoint_name: str, min_spacing_seconds: float) -> None:
        if min_spacing_seconds <= 0:
            return
        async with self._min_spacing_locks[endpoint_name]:
            while True:
                now = time.monotonic()
                last = self._last_call.get(endpoint_name, 0.0)
                gap = min_spacing_seconds - (now - last)
                if gap <= 0:
                    self._last_call[endpoint_name] = now
                    return
                await asyncio.sleep(min(gap, 0.25))

    async def request_json(self, endpoint_name: str, params: Optional[Dict[str, Any]] = None, retries: int = 5) -> Any:
        spec = REST_ENDPOINTS[endpoint_name]
        if spec.requires_api_key and not self.api_key:
            raise MissingApiKeyError(f"{endpoint_name} requires BINANCE_API_KEY")

        circuit = self.circuits.get(f"rest.{endpoint_name}")
        headers: Dict[str, str] = {}
        if spec.requires_api_key and self.api_key:
            headers["X-MBX-APIKEY"] = self.api_key
        url = REST_HOSTS[spec.host_key] + spec.path

        for attempt in range(retries):
            if self.shutdown_event.is_set():
                raise ShutdownRequested()

            await circuit.wait_until_ready(self.shutdown_event)
            await self._respect_min_spacing(endpoint_name, spec.min_spacing_seconds)
            await self.buckets[spec.host_key].acquire(spec.weight)

            started = time.perf_counter()
            try:
                response = await self.client.get(
                    url,
                    params=params or {},
                    headers=headers,
                    timeout=httpx.Timeout(
                        timeout=self.settings.http_timeout_seconds * spec.timeout_multiplier,
                        connect=self.settings.http_connect_timeout_seconds,
                        pool=self.settings.http_pool_timeout_seconds,
                    ),
                )
                latency_ms = int((time.perf_counter() - started) * 1000)
                self.metrics.observe_rest_latency(endpoint_name, latency_ms)
                if response.status_code >= 400:
                    raise BinanceHttpError(response.status_code, response.text[:300].strip(), dict(response.headers))
                await circuit.on_success()
                self.metrics.rest_requests_total.labels(endpoint=endpoint_name, status="ok").inc()
                return response.json()
            except Exception as exc:
                self.metrics.rest_requests_total.labels(endpoint=endpoint_name, status=exc.__class__.__name__).inc()
                retryable = is_retryable(exc)
                if not retryable or attempt == (retries - 1):
                    await circuit.on_failure(30.0 if not retryable else 5.0)
                    raise

                delay = backoff_seconds(exc, attempt)
                self.metrics.rest_retries_total.labels(endpoint=endpoint_name).inc()
                await circuit.on_failure(max(5.0, delay if isinstance(exc, BinanceHttpError) else 5.0))
                self.logger.warning(
                    "rest_retry",
                    endpoint=endpoint_name,
                    attempt=attempt + 1,
                    retries=retries,
                    params=params,
                    sleep_seconds=round(delay, 3),
                    error=str(exc),
                )
                await wait_with_shutdown(self.shutdown_event, delay)

        raise RuntimeError(f"Retries exhausted for {endpoint_name}")


@dataclass(frozen=True)
class TableSpec:
    name: str
    schema: pa.Schema
    dedupe_fields: Tuple[str, ...]


TRADE_SCHEMA = pa.schema(
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
        ("source", pa.string()),
        ("ingest_ts_ms", pa.int64()),
    ]
)

L1_SCHEMA = pa.schema(
    [
        ("symbol", pa.string()),
        ("ts_ms", pa.int64()),
        ("event_time_ms", pa.int64()),
        ("transaction_time_ms", pa.int64()),
        ("update_id", pa.int64()),
        ("best_bid_price", pa.string()),
        ("best_bid_quantity", pa.string()),
        ("best_ask_price", pa.string()),
        ("best_ask_quantity", pa.string()),
        ("ingest_ts_ms", pa.int64()),
    ]
)

DEPTH_SCHEMA = pa.schema(
    [
        ("symbol", pa.string()),
        ("ts_ms", pa.int64()),
        ("event_time_ms", pa.int64()),
        ("transaction_time_ms", pa.int64()),
        ("first_update_id", pa.int64()),
        ("final_update_id", pa.int64()),
        ("previous_final_update_id", pa.int64()),
        ("bids", DEPTH_LEVEL_TYPE),
        ("asks", DEPTH_LEVEL_TYPE),
        ("ingest_ts_ms", pa.int64()),
    ]
)

MARK_PRICE_SCHEMA = pa.schema(
    [
        ("symbol", pa.string()),
        ("ts_ms", pa.int64()),
        ("event_time_ms", pa.int64()),
        ("mark_price", pa.string()),
        ("index_price", pa.string()),
        ("estimated_settle_price", pa.string()),
        ("funding_rate", pa.string()),
        ("next_funding_time_ms", pa.int64()),
        ("ingest_ts_ms", pa.int64()),
    ]
)

FUNDING_HISTORY_SCHEMA = pa.schema(
    [
        ("symbol", pa.string()),
        ("ts_ms", pa.int64()),
        ("funding_time_ms", pa.int64()),
        ("funding_rate", pa.string()),
        ("mark_price", pa.string()),
        ("ingest_ts_ms", pa.int64()),
    ]
)


TABLE_SPECS: Dict[str, TableSpec] = {
    "spot_trades": TableSpec("spot_trades", TRADE_SCHEMA, ("trade_id", "source")),
    "futures_trades": TableSpec("futures_trades", TRADE_SCHEMA, ("trade_id", "source")),
    "spot_l1": TableSpec("spot_l1", L1_SCHEMA, ("update_id",)),
    "futures_l1": TableSpec("futures_l1", L1_SCHEMA, ("update_id",)),
    "spot_depth_updates": TableSpec("spot_depth_updates", DEPTH_SCHEMA, ("final_update_id",)),
    "futures_depth_updates": TableSpec("futures_depth_updates", DEPTH_SCHEMA, ("final_update_id",)),
    "futures_mark_price": TableSpec("futures_mark_price", MARK_PRICE_SCHEMA, ("event_time_ms",)),
    "futures_funding_history": TableSpec("futures_funding_history", FUNDING_HISTORY_SCHEMA, ("funding_time_ms",)),
}


def normalize_spot_trade_ws(data: Dict[str, Any], ingest_ts_ms: int) -> Dict[str, Any]:
    event_time_ms = as_int(data.get("E"))
    trade_time_ms = as_int(data.get("T"))
    ts_ms = trade_time_ms or event_time_ms or ingest_ts_ms
    return {
        "symbol": as_str(data.get("s")),
        "ts_ms": ts_ms,
        "event_time_ms": event_time_ms,
        "trade_time_ms": trade_time_ms,
        "trade_id": as_int(data.get("t")),
        "price": as_str(data.get("p")),
        "quantity": as_str(data.get("q")),
        "quote_quantity": None,
        "buyer_order_id": as_int(data.get("b")),
        "seller_order_id": as_int(data.get("a")),
        "is_buyer_maker": as_bool(data.get("m")),
        "is_best_match": None,
        "source": "ws",
        "ingest_ts_ms": ingest_ts_ms,
    }


def normalize_futures_trade_ws(data: Dict[str, Any], ingest_ts_ms: int) -> Dict[str, Any]:
    event_time_ms = as_int(data.get("E"))
    trade_time_ms = as_int(data.get("T"))
    ts_ms = trade_time_ms or event_time_ms or ingest_ts_ms
    return {
        "symbol": as_str(data.get("s")),
        "ts_ms": ts_ms,
        "event_time_ms": event_time_ms,
        "trade_time_ms": trade_time_ms,
        "trade_id": as_int(data.get("t")),
        "price": as_str(data.get("p")),
        "quantity": as_str(data.get("q")),
        "quote_quantity": None,
        "buyer_order_id": as_int(data.get("b")),
        "seller_order_id": as_int(data.get("a")),
        "is_buyer_maker": as_bool(data.get("m")),
        "is_best_match": None,
        "source": "ws",
        "ingest_ts_ms": ingest_ts_ms,
    }


def normalize_spot_trade_rest(data: Dict[str, Any], ingest_ts_ms: int) -> Dict[str, Any]:
    trade_time_ms = as_int(data.get("time"))
    ts_ms = trade_time_ms or ingest_ts_ms
    return {
        "symbol": as_str(data.get("symbol")),
        "ts_ms": ts_ms,
        "event_time_ms": None,
        "trade_time_ms": trade_time_ms,
        "trade_id": as_int(data.get("id")),
        "price": as_str(data.get("price")),
        "quantity": as_str(data.get("qty")),
        "quote_quantity": as_str(data.get("quoteQty")),
        "buyer_order_id": None,
        "seller_order_id": None,
        "is_buyer_maker": as_bool(data.get("isBuyerMaker")),
        "is_best_match": as_bool(data.get("isBestMatch")),
        "source": "rest",
        "ingest_ts_ms": ingest_ts_ms,
    }


def normalize_futures_trade_rest(data: Dict[str, Any], ingest_ts_ms: int, symbol: str) -> Dict[str, Any]:
    trade_time_ms = as_int(data.get("time"))
    ts_ms = trade_time_ms or ingest_ts_ms
    return {
        "symbol": symbol,
        "ts_ms": ts_ms,
        "event_time_ms": None,
        "trade_time_ms": trade_time_ms,
        "trade_id": as_int(data.get("id")),
        "price": as_str(data.get("price")),
        "quantity": as_str(data.get("qty")),
        "quote_quantity": as_str(data.get("quoteQty")),
        "buyer_order_id": None,
        "seller_order_id": None,
        "is_buyer_maker": as_bool(data.get("isBuyerMaker")),
        "is_best_match": None,
        "source": "rest",
        "ingest_ts_ms": ingest_ts_ms,
    }


def normalize_spot_l1_ws(data: Dict[str, Any], ingest_ts_ms: int) -> Dict[str, Any]:
    event_time_ms = as_int(data.get("E"))
    ts_ms = event_time_ms or ingest_ts_ms
    return {
        "symbol": as_str(data.get("s")),
        "ts_ms": ts_ms,
        "event_time_ms": event_time_ms,
        "transaction_time_ms": None,
        "update_id": as_int(data.get("u")),
        "best_bid_price": as_str(data.get("b")),
        "best_bid_quantity": as_str(data.get("B")),
        "best_ask_price": as_str(data.get("a")),
        "best_ask_quantity": as_str(data.get("A")),
        "ingest_ts_ms": ingest_ts_ms,
    }


def normalize_futures_l1_ws(data: Dict[str, Any], ingest_ts_ms: int) -> Dict[str, Any]:
    event_time_ms = as_int(data.get("E"))
    transaction_time_ms = as_int(data.get("T"))
    ts_ms = event_time_ms or transaction_time_ms or ingest_ts_ms
    return {
        "symbol": as_str(data.get("s")),
        "ts_ms": ts_ms,
        "event_time_ms": event_time_ms,
        "transaction_time_ms": transaction_time_ms,
        "update_id": as_int(data.get("u")),
        "best_bid_price": as_str(data.get("b")),
        "best_bid_quantity": as_str(data.get("B")),
        "best_ask_price": as_str(data.get("a")),
        "best_ask_quantity": as_str(data.get("A")),
        "ingest_ts_ms": ingest_ts_ms,
    }


def normalize_spot_depth_ws(data: Dict[str, Any], ingest_ts_ms: int) -> Dict[str, Any]:
    event_time_ms = as_int(data.get("E"))
    ts_ms = event_time_ms or ingest_ts_ms
    return {
        "symbol": as_str(data.get("s")),
        "ts_ms": ts_ms,
        "event_time_ms": event_time_ms,
        "transaction_time_ms": None,
        "first_update_id": as_int(data.get("U")),
        "final_update_id": as_int(data.get("u")),
        "previous_final_update_id": None,
        "bids": normalize_depth_levels(data.get("b")),
        "asks": normalize_depth_levels(data.get("a")),
        "ingest_ts_ms": ingest_ts_ms,
    }


def normalize_futures_depth_ws(data: Dict[str, Any], ingest_ts_ms: int) -> Dict[str, Any]:
    event_time_ms = as_int(data.get("E"))
    transaction_time_ms = as_int(data.get("T"))
    ts_ms = event_time_ms or transaction_time_ms or ingest_ts_ms
    return {
        "symbol": as_str(data.get("s")),
        "ts_ms": ts_ms,
        "event_time_ms": event_time_ms,
        "transaction_time_ms": transaction_time_ms,
        "first_update_id": as_int(data.get("U")),
        "final_update_id": as_int(data.get("u")),
        "previous_final_update_id": as_int(data.get("pu")),
        "bids": normalize_depth_levels(data.get("b")),
        "asks": normalize_depth_levels(data.get("a")),
        "ingest_ts_ms": ingest_ts_ms,
    }


def normalize_futures_mark_price_ws(data: Dict[str, Any], ingest_ts_ms: int) -> Dict[str, Any]:
    event_time_ms = as_int(data.get("E"))
    ts_ms = event_time_ms or ingest_ts_ms
    return {
        "symbol": as_str(data.get("s")),
        "ts_ms": ts_ms,
        "event_time_ms": event_time_ms,
        "mark_price": as_str(data.get("p")),
        "index_price": as_str(data.get("i")),
        "estimated_settle_price": as_str(data.get("P")),
        "funding_rate": as_str(data.get("r")),
        "next_funding_time_ms": as_int(data.get("T")),
        "ingest_ts_ms": ingest_ts_ms,
    }


def normalize_funding_history_rest(data: Dict[str, Any], ingest_ts_ms: int, symbol: str) -> Dict[str, Any]:
    funding_time_ms = as_int(data.get("fundingTime"))
    ts_ms = funding_time_ms or ingest_ts_ms
    return {
        "symbol": symbol,
        "ts_ms": ts_ms,
        "funding_time_ms": funding_time_ms,
        "funding_rate": as_str(data.get("fundingRate")),
        "mark_price": as_str(data.get("markPrice")),
        "ingest_ts_ms": ingest_ts_ms,
    }


def decode_combined_stream_message(raw_message: Any) -> Tuple[Optional[str], Optional[Dict[str, Any]]]:
    if isinstance(raw_message, bytes):
        raw_message = raw_message.decode("utf-8")

    payload = json.loads(raw_message)
    if isinstance(payload, dict) and payload.get("result") is None and "id" in payload:
        return None, None

    if isinstance(payload, dict) and "stream" in payload and "data" in payload:
        return as_str(payload.get("stream")), payload.get("data")

    if isinstance(payload, dict):
        data = payload
        symbol = as_str(data.get("s"))
        event_name = as_str(data.get("e"))
        if symbol and event_name:
            return f"{symbol.lower()}@{event_name.lower()}", data
        return None, data

    raise ValueError("Unexpected websocket payload")


class StateStore:
    def __init__(self, root: Path):
        self.root = root
        self.root.mkdir(parents=True, exist_ok=True)
        self._write_lock = asyncio.Lock()

    def _path(self, state_kind: str, table_name: str, symbol: str) -> Path:
        return self.root / state_kind / table_name / f"{symbol}.json"

    def _load_sync(self, state_kind: str, table_name: str, symbol: str) -> Optional[Dict[str, Any]]:
        path = self._path(state_kind, table_name, symbol)
        if not path.exists():
            return None
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            return None

    async def load(self, state_kind: str, table_name: str, symbol: str) -> Optional[Dict[str, Any]]:
        return await asyncio.to_thread(self._load_sync, state_kind, table_name, symbol)

    def _save_sync(self, state_kind: str, table_name: str, symbol: str, payload: Dict[str, Any]) -> None:
        path = self._path(state_kind, table_name, symbol)
        path.parent.mkdir(parents=True, exist_ok=True)
        temp_path = path.with_suffix(path.suffix + f".{os.getpid()}.tmp")
        body = dict(payload)
        body["updated_at_ms"] = now_utc_ms()
        body["updated_at_iso"] = utc_iso_from_ms(body["updated_at_ms"])
        with temp_path.open("w", encoding="utf-8") as handle:
            handle.write(json.dumps(body, sort_keys=True))
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temp_path, path)
        fsync_directory(path.parent)

    async def save(self, state_kind: str, table_name: str, symbol: str, payload: Dict[str, Any]) -> None:
        async with self._write_lock:
            await asyncio.to_thread(self._save_sync, state_kind, table_name, symbol, payload)


class DatasetInspector:
    def __init__(self, out_dir: Path):
        self.out_dir = out_dir

    def _recent_files_sync(self, table_name: str, symbol: str, limit: int = 6) -> List[Path]:
        pattern = f"{table_name}/date=*/instrument={symbol}/hour=*/*.parquet"
        return sorted(self.out_dir.glob(pattern), reverse=True)[:limit]

    def _tail_rows_sync(
        self,
        table_name: str,
        symbol: str,
        columns: Sequence[str],
        file_limit: int = 6,
        row_limit: int = 256,
    ) -> List[Dict[str, Any]]:
        rows: List[Dict[str, Any]] = []
        for file_path in self._recent_files_sync(table_name, symbol, limit=file_limit):
            try:
                table = pq.read_table(file_path, columns=list(columns))
            except Exception:
                continue
            if table.num_rows == 0:
                continue
            start = max(0, table.num_rows - row_limit)
            rows = table.slice(start).to_pylist() + rows
            if len(rows) >= row_limit:
                rows = rows[-row_limit:]
                break
        return rows[-row_limit:]

    async def infer_trade_next_id(self, table_name: str, symbol: str) -> Optional[int]:
        rows = await asyncio.to_thread(self._tail_rows_sync, table_name, symbol, ("trade_id", "ts_ms"))
        max_trade_id: Optional[int] = None
        for row in rows:
            trade_id = as_int(row.get("trade_id"))
            if trade_id is not None and (max_trade_id is None or trade_id > max_trade_id):
                max_trade_id = trade_id
        return None if max_trade_id is None else max_trade_id + 1

    async def infer_funding_next_time(self, symbol: str) -> Optional[int]:
        rows = await asyncio.to_thread(self._tail_rows_sync, "futures_funding_history", symbol, ("funding_time_ms", "ts_ms"))
        max_time: Optional[int] = None
        for row in rows:
            funding_time_ms = as_int(row.get("funding_time_ms"))
            if funding_time_ms is not None and (max_time is None or funding_time_ms > max_time):
                max_time = funding_time_ms
        return None if max_time is None else max_time + 1


@dataclass
class WriteResult:
    rows_written: int = 0
    bytes_written: int = 0
    fsync_count: int = 0
    files_committed: int = 0
    deduped: int = 0
    invalid_rows: int = 0

    def merge(self, other: "WriteResult") -> None:
        self.rows_written += other.rows_written
        self.bytes_written += other.bytes_written
        self.fsync_count += other.fsync_count
        self.files_committed += other.files_committed
        self.deduped += other.deduped
        self.invalid_rows += other.invalid_rows


class PartitionWriter:
    """
    Writes immutable Parquet segments per table/date/symbol/hour partition.

    Each segment is written to a temporary file, periodically fsynced, then
    atomically renamed to its final Parquet shard name.
    """

    def __init__(
        self,
        spec: TableSpec,
        partition_dir: Path,
        session_id: str,
        batch_size: int,
        segment_max_rows: int,
        segment_max_age_seconds: float,
        flush_interval_seconds: float,
        fsync_interval_seconds: float,
        seed_file_count: int,
        seed_row_count: int,
        seen_capacity: int,
    ):
        self.spec = spec
        self.partition_dir = partition_dir
        self.partition_dir.mkdir(parents=True, exist_ok=True)
        self.session_id = session_id
        self.batch_size = max(1, batch_size)
        self.segment_max_rows = max(self.batch_size, segment_max_rows)
        self.segment_max_age_seconds = max(1.0, segment_max_age_seconds)
        self.flush_interval_seconds = max(0.2, flush_interval_seconds)
        self.fsync_interval_seconds = max(1.0, fsync_interval_seconds)
        self.seed_file_count = max(1, seed_file_count)
        self.seed_row_count = max(1, seed_row_count)
        self.seen_capacity = max(1024, seen_capacity)

        self.writer: Optional[pq.ParquetWriter] = None
        self.current_tmp_path: Optional[Path] = None
        self.current_final_path: Optional[Path] = None
        self.segment_index = 0
        self.segment_opened_ms = 0
        self.rows_in_segment = 0
        self.segment_started_at = time.monotonic()
        self.last_activity_at = time.monotonic()
        self.last_fsync_at = time.monotonic()
        self.buffer: List[Dict[str, Any]] = []

        self.seen_keys: set[Tuple[Any, ...]] = set()
        self.seen_order: collections.deque[Tuple[Any, ...]] = collections.deque()
        self._seed_seen_keys()

    @property
    def is_open(self) -> bool:
        return self.writer is not None

    @property
    def has_buffered_rows(self) -> bool:
        return bool(self.buffer)

    def _seed_seen_keys(self) -> None:
        columns = list(dict.fromkeys(["ts_ms", *self.spec.dedupe_fields]))
        for file_path in sorted(self.partition_dir.glob("*.parquet"), reverse=True)[: self.seed_file_count]:
            try:
                table = pq.read_table(file_path, columns=columns)
            except Exception:
                continue
            if table.num_rows == 0:
                continue
            start = max(0, table.num_rows - self.seed_row_count)
            for row in table.slice(start).to_pylist():
                self._remember_key(tuple(row.get(column) for column in columns))

    def _remember_key(self, key: Tuple[Any, ...]) -> None:
        if key in self.seen_keys:
            return
        self.seen_keys.add(key)
        self.seen_order.append(key)
        while len(self.seen_order) > self.seen_capacity:
            dropped = self.seen_order.popleft()
            self.seen_keys.discard(dropped)

    def _dedupe_key(self, row: Dict[str, Any]) -> Tuple[Any, ...]:
        return tuple(row.get(field_name) for field_name in ("ts_ms", *self.spec.dedupe_fields))

    def _ensure_writer(self) -> None:
        if self.writer is not None:
            return
        self.segment_opened_ms = now_utc_ms()
        self.current_tmp_path = self.partition_dir / (
            f".part-{self.segment_opened_ms:013d}-{self.session_id}-{self.segment_index:06d}.tmp"
        )
        self.current_final_path = self.partition_dir / (
            f"part-{self.segment_opened_ms:013d}-{self.session_id}-{self.segment_index:06d}.parquet"
        )
        self.writer = pq.ParquetWriter(self.current_tmp_path, self.spec.schema, compression="snappy")
        self.segment_started_at = time.monotonic()
        self.last_fsync_at = time.monotonic()

    def _flush_buffer(self) -> WriteResult:
        result = WriteResult()
        if not self.buffer:
            return result
        self._ensure_writer()
        assert self.current_tmp_path is not None
        before_size = self.current_tmp_path.stat().st_size if self.current_tmp_path.exists() else 0
        arrays = [pa.array([row.get(field.name) for row in self.buffer], type=field.type) for field in self.spec.schema]
        table = pa.Table.from_arrays(arrays, schema=self.spec.schema)
        assert self.writer is not None
        self.writer.write_table(table)
        after_size = self.current_tmp_path.stat().st_size if self.current_tmp_path.exists() else before_size
        result.rows_written = table.num_rows
        result.bytes_written = max(0, after_size - before_size)
        self.rows_in_segment += table.num_rows
        self.buffer.clear()
        return result

    def _fsync_open_file(self) -> WriteResult:
        result = self._flush_buffer()
        if self.current_tmp_path is None or not self.current_tmp_path.exists():
            return result
        fsync_file(self.current_tmp_path)
        self.last_fsync_at = time.monotonic()
        result.fsync_count += 1
        return result

    def append_rows(self, rows: Sequence[Dict[str, Any]]) -> WriteResult:
        result = WriteResult()
        for row in rows:
            key = self._dedupe_key(row)
            if key in self.seen_keys:
                result.deduped += 1
                continue
            self._remember_key(key)
            self.buffer.append(row)
            self.last_activity_at = time.monotonic()
            if len(self.buffer) >= self.batch_size:
                result.merge(self._flush_buffer())
            if self.rows_in_segment >= self.segment_max_rows:
                result.merge(self.checkpoint())
        return result

    def maintenance(self) -> WriteResult:
        result = WriteResult()
        now = time.monotonic()
        if self.buffer and (now - self.last_activity_at) >= self.flush_interval_seconds:
            result.merge(self._flush_buffer())
        if self.writer and (now - self.last_fsync_at) >= self.fsync_interval_seconds:
            result.merge(self._fsync_open_file())
        if self.writer and self.rows_in_segment > 0 and (now - self.segment_started_at) >= self.segment_max_age_seconds:
            result.merge(self.checkpoint())
        return result

    def close_if_idle(self, idle_seconds: float) -> WriteResult:
        if idle_seconds <= 0:
            return WriteResult()
        if not self.is_open:
            return WriteResult()
        if (time.monotonic() - self.last_activity_at) < idle_seconds:
            return WriteResult()
        return self.checkpoint()

    def checkpoint(self) -> WriteResult:
        result = WriteResult()
        if self.writer is None and not self.buffer:
            return result

        result.merge(self._flush_buffer())
        if self.writer is None or self.current_tmp_path is None or self.current_final_path is None:
            return result

        self.writer.close()
        self.writer = None
        fsync_file(self.current_tmp_path)
        os.replace(self.current_tmp_path, self.current_final_path)
        fsync_directory(self.partition_dir)
        result.fsync_count += 1
        result.files_committed += 1

        self.current_tmp_path = None
        self.current_final_path = None
        self.segment_opened_ms = 0
        self.rows_in_segment = 0
        self.segment_index += 1
        self.segment_started_at = time.monotonic()
        self.last_fsync_at = time.monotonic()
        return result

    def close(self) -> WriteResult:
        return self.checkpoint()


class SyncTableWriter:
    def __init__(
        self,
        table_name: str,
        out_dir: Path,
        settings: AppSettings,
        symbol_count: int,
    ):
        self.table_name = table_name
        self.spec = TABLE_SPECS[table_name]
        self.out_dir = out_dir
        self.settings = settings
        self.symbol_count = max(1, symbol_count)
        self.session_id = uuid.uuid4().hex[:10]
        self.partitions: Dict[Tuple[str, str, str], PartitionWriter] = {}
        self.open_partition_keys: "collections.OrderedDict[Tuple[str, str, str], None]" = collections.OrderedDict()
        self.seen_capacity_total = settings.dedupe_capacity_for_table(table_name, self.symbol_count)
        self.seen_capacity_per_partition = max(
            1024,
            math.ceil(self.seen_capacity_total / self.symbol_count),
        )

    @property
    def open_writer_count(self) -> int:
        return len(self.open_partition_keys)

    def _partition_key(self, row: Dict[str, Any]) -> Tuple[str, str, str]:
        ts_ms = as_int(row.get("ts_ms")) or as_int(row.get("ingest_ts_ms")) or now_utc_ms()
        symbol = as_str(row.get("symbol")) or "UNKNOWN"
        return symbol, date_partition_from_ms(ts_ms), hour_partition_from_ms(ts_ms)

    def _partition_writer(self, row: Dict[str, Any]) -> PartitionWriter:
        symbol, date_part, hour_part = self._partition_key(row)
        key = (symbol, date_part, hour_part)
        writer = self.partitions.get(key)
        if writer is None:
            partition_dir = self.out_dir / self.table_name / f"date={date_part}" / f"instrument={symbol}" / f"hour={hour_part}"
            writer = PartitionWriter(
                spec=self.spec,
                partition_dir=partition_dir,
                session_id=self.session_id,
                batch_size=self.settings.parquet_batch_size,
                segment_max_rows=self.settings.segment_max_rows,
                segment_max_age_seconds=self.settings.segment_max_age_seconds,
                flush_interval_seconds=self.settings.flush_interval_seconds,
                fsync_interval_seconds=self.settings.fsync_interval_seconds,
                seed_file_count=self.settings.seed_file_count,
                seed_row_count=self.settings.seed_row_count,
                seen_capacity=self.seen_capacity_per_partition,
            )
            self.partitions[key] = writer
        return writer

    def _mark_partition_open(self, key: Tuple[str, str, str], writer: PartitionWriter) -> None:
        if not writer.is_open:
            self.open_partition_keys.pop(key, None)
            return
        self.open_partition_keys.pop(key, None)
        self.open_partition_keys[key] = None

    def _close_partition(self, key: Tuple[str, str, str]) -> WriteResult:
        writer = self.partitions.get(key)
        if writer is None:
            self.open_partition_keys.pop(key, None)
            return WriteResult()
        result = writer.checkpoint()
        self.open_partition_keys.pop(key, None)
        return result

    def _enforce_open_writer_limit(self, exempt_key: Optional[Tuple[str, str, str]] = None) -> WriteResult:
        result = WriteResult()
        max_open = max(1, self.settings.max_open_partition_writers)
        while len(self.open_partition_keys) >= max_open:
            candidate_key = next((key for key in self.open_partition_keys.keys() if key != exempt_key), None)
            if candidate_key is None:
                break
            result.merge(self._close_partition(candidate_key))
        return result

    def append_rows(self, rows: Sequence[Dict[str, Any]]) -> WriteResult:
        result = WriteResult()
        grouped: Dict[Tuple[str, str, str], List[Dict[str, Any]]] = collections.defaultdict(list)
        for row in rows:
            symbol = as_str(row.get("symbol"))
            ts_ms = as_int(row.get("ts_ms"))
            if not symbol or ts_ms is None:
                result.invalid_rows += 1
                continue
            grouped[self._partition_key(row)].append(row)
        for key, group_rows in grouped.items():
            writer = self._partition_writer(group_rows[0])
            if not writer.is_open:
                result.merge(self._enforce_open_writer_limit(exempt_key=key))
            result.merge(writer.append_rows(group_rows))
            self._mark_partition_open(key, writer)
        return result

    def maintenance(self) -> WriteResult:
        result = WriteResult()
        for key, writer in self.partitions.items():
            if writer.has_buffered_rows and not writer.is_open:
                result.merge(self._enforce_open_writer_limit(exempt_key=key))
            result.merge(writer.maintenance())
            result.merge(writer.close_if_idle(self.settings.partition_writer_idle_close_seconds))
            self._mark_partition_open(key, writer)
        return result

    def close(self) -> WriteResult:
        result = WriteResult()
        for key, writer in self.partitions.items():
            result.merge(writer.close())
            self._mark_partition_open(key, writer)
        return result


class AsyncTableRouter:
    def __init__(self, queues: Dict[str, asyncio.Queue], metrics: Metrics, shutdown_event: asyncio.Event):
        self.queues = queues
        self.metrics = metrics
        self.shutdown_event = shutdown_event

    def snapshot_queue_sizes(self) -> Dict[str, int]:
        return {table_name: queue.qsize() for table_name, queue in self.queues.items()}

    async def submit(self, table_name: str, row: Dict[str, Any]) -> None:
        if self.shutdown_event.is_set():
            raise ShutdownRequested()
        queue = self.queues[table_name]
        source = str(row.get("source") or "ws")
        started = time.perf_counter()
        try:
            queue.put_nowait(row)
        except asyncio.QueueFull:
            self.metrics.queue_backpressure_total.labels(table=table_name).inc()
            await queue.put(row)
        waited = time.perf_counter() - started
        self.metrics.queue_put_wait_seconds.labels(table=table_name).observe(waited)
        self.metrics.rows_routed_total.labels(table=table_name, source=source).inc()
        self.metrics.queue_size.labels(table=table_name).set(queue.qsize())


class AsyncTableWriter:
    def __init__(
        self,
        table_name: str,
        queue: asyncio.Queue,
        settings: AppSettings,
        out_dir: Path,
        symbol_count: int,
        metrics: Metrics,
        runtime_state: RuntimeState,
        logger: structlog.stdlib.BoundLogger,
        executor: ThreadPoolExecutor,
        shutdown_event: asyncio.Event,
    ):
        self.table_name = table_name
        self.queue = queue
        self.settings = settings
        self.metrics = metrics
        self.runtime_state = runtime_state
        self.logger = logger.bind(component="writer", table=table_name)
        self.executor = executor
        self.shutdown_event = shutdown_event
        self.sync_writer = SyncTableWriter(table_name=table_name, out_dir=out_dir, settings=settings, symbol_count=symbol_count)

    async def _run_blocking(self, fn: Callable[[], WriteResult]) -> WriteResult:
        loop = asyncio.get_running_loop()
        started = time.perf_counter()
        result = await loop.run_in_executor(self.executor, fn)
        elapsed = time.perf_counter() - started
        self.metrics.observe_write(
            table_name=self.table_name,
            rows=result.rows_written,
            bytes_written=result.bytes_written,
            seconds_spent=elapsed,
            fsync_count=result.fsync_count,
        )
        if result.deduped:
            self.metrics.deduped_rows_total.labels(table=self.table_name).inc(result.deduped)
        if result.invalid_rows:
            self.metrics.invalid_rows_total.labels(table=self.table_name).inc(result.invalid_rows)
        if result.rows_written or result.files_committed:
            self.runtime_state.mark_disk_write()
        self.metrics.open_partition_writers.labels(table=self.table_name).set(self.sync_writer.open_writer_count)
        return result

    async def run(self) -> None:
        batch: List[Dict[str, Any]] = []
        maintenance_interval = min(self.settings.flush_interval_seconds, 1.0)
        next_maintenance_at = time.monotonic() + maintenance_interval
        try:
            while True:
                if self.shutdown_event.is_set() and self.queue.empty() and not batch:
                    break
                try:
                    row = await asyncio.wait_for(self.queue.get(), timeout=self.settings.flush_interval_seconds)
                    batch.append(row)
                    self.metrics.queue_size.labels(table=self.table_name).set(self.queue.qsize())
                except asyncio.TimeoutError:
                    row = None

                if batch and (row is None or len(batch) >= self.settings.parquet_batch_size):
                    await self._run_blocking(lambda rows=list(batch): self.sync_writer.append_rows(rows))
                    batch.clear()

                now = time.monotonic()
                if row is None or now >= next_maintenance_at:
                    await self._run_blocking(self.sync_writer.maintenance)
                    next_maintenance_at = now + maintenance_interval
                self.metrics.queue_size.labels(table=self.table_name).set(self.queue.qsize())
        finally:
            if batch:
                await self._run_blocking(lambda rows=list(batch): self.sync_writer.append_rows(rows))
            await self._run_blocking(self.sync_writer.close)
            self.metrics.queue_size.labels(table=self.table_name).set(self.queue.qsize())
            self.metrics.open_partition_writers.labels(table=self.table_name).set(self.sync_writer.open_writer_count)


async def discover_spot_symbols(
    transport: AsyncBinanceTransport,
    settings: AppSettings,
    logger: structlog.stdlib.BoundLogger,
) -> List[str]:
    payload = await transport.request_json("spot_exchange_info", retries=3)
    symbols: List[str] = []
    seen: set[str] = set()
    for item in payload.get("symbols", []):
        symbol = normalize_symbol(as_str(item.get("symbol")) or "")
        quote_asset = (as_str(item.get("quoteAsset")) or "").upper()
        if not symbol or symbol in seen:
            continue
        if as_str(item.get("status")) != "TRADING":
            continue
        if quote_asset not in settings.quote_assets:
            continue
        if item.get("isSpotTradingAllowed") is False:
            continue
        seen.add(symbol)
        symbols.append(symbol)
    if settings.discovery_limit > 0:
        symbols = symbols[: settings.discovery_limit]
    logger.info("symbols_discovered", market="spot", count=len(symbols))
    return symbols


async def discover_futures_symbols(
    transport: AsyncBinanceTransport,
    settings: AppSettings,
    logger: structlog.stdlib.BoundLogger,
) -> List[str]:
    payload = await transport.request_json("futures_exchange_info", retries=3)
    symbols: List[str] = []
    seen: set[str] = set()
    for item in payload.get("symbols", []):
        symbol = normalize_symbol(as_str(item.get("symbol")) or "")
        quote_asset = (as_str(item.get("quoteAsset")) or "").upper()
        if not symbol or symbol in seen:
            continue
        if as_str(item.get("status")) != "TRADING":
            continue
        if quote_asset not in settings.quote_assets:
            continue
        if as_str(item.get("contractType")) != "PERPETUAL":
            continue
        seen.add(symbol)
        symbols.append(symbol)
    if settings.discovery_limit > 0:
        symbols = symbols[: settings.discovery_limit]
    logger.info("symbols_discovered", market="futures", count=len(symbols))
    return symbols


async def resolve_symbols(
    settings: AppSettings,
    transport: AsyncBinanceTransport,
    logger: structlog.stdlib.BoundLogger,
) -> Tuple[List[str], List[str]]:
    fallback = list(settings.fallback_symbols)
    spot_symbols: List[str] = []
    futures_symbols: List[str] = []

    if settings.enable_spot:
        if settings.spot_symbols:
            spot_symbols = list(settings.spot_symbols)
        else:
            try:
                spot_symbols = await discover_spot_symbols(transport, settings, logger)
            except Exception as exc:
                logger.warning("symbol_discovery_failed", market="spot", error=str(exc), fallback=fallback)
                spot_symbols = list(fallback)

    if settings.enable_futures:
        if settings.futures_symbols:
            futures_symbols = list(settings.futures_symbols)
        else:
            try:
                futures_symbols = await discover_futures_symbols(transport, settings, logger)
            except Exception as exc:
                logger.warning("symbol_discovery_failed", market="futures", error=str(exc), fallback=fallback)
                futures_symbols = list(fallback)

    return spot_symbols, futures_symbols


def build_stream_groups(symbols: Sequence[str], suffixes: Sequence[str], max_streams_per_connection: int) -> List[List[str]]:
    stream_names = [f"{symbol.lower()}@{suffix}" for symbol in symbols for suffix in suffixes]
    return chunked(stream_names, max_streams_per_connection)


def make_spot_message_handler(router: AsyncTableRouter) -> Callable[[str, Dict[str, Any], int], Any]:
    async def handle(stream_name: str, payload: Dict[str, Any], ingest_ts_ms: int) -> None:
        stream = stream_name.lower()
        if stream.endswith("@trade"):
            await router.submit("spot_trades", normalize_spot_trade_ws(payload, ingest_ts_ms))
            return
        if stream.endswith("@bookticker"):
            await router.submit("spot_l1", normalize_spot_l1_ws(payload, ingest_ts_ms))
            return
        if stream.endswith("@depth@100ms"):
            await router.submit("spot_depth_updates", normalize_spot_depth_ws(payload, ingest_ts_ms))
            return

    return handle


def make_futures_message_handler(router: AsyncTableRouter) -> Callable[[str, Dict[str, Any], int], Any]:
    async def handle(stream_name: str, payload: Dict[str, Any], ingest_ts_ms: int) -> None:
        stream = stream_name.lower()
        if stream.endswith("@trade"):
            await router.submit("futures_trades", normalize_futures_trade_ws(payload, ingest_ts_ms))
            return
        if stream.endswith("@bookticker"):
            await router.submit("futures_l1", normalize_futures_l1_ws(payload, ingest_ts_ms))
            return
        if stream.endswith("@depth@100ms"):
            await router.submit("futures_depth_updates", normalize_futures_depth_ws(payload, ingest_ts_ms))
            return
        if stream.endswith("@markprice@1s"):
            await router.submit("futures_mark_price", normalize_futures_mark_price_ws(payload, ingest_ts_ms))
            return

    return handle


class WebsocketStreamWorker:
    def __init__(
        self,
        name: str,
        market: str,
        base_url: str,
        stream_names: Sequence[str],
        message_handler: Callable[[str, Dict[str, Any], int], Any],
        settings: AppSettings,
        metrics: Metrics,
        circuits: CircuitRegistry,
        runtime_state: RuntimeState,
        shutdown_event: asyncio.Event,
        logger: structlog.stdlib.BoundLogger,
    ):
        self.name = name
        self.market = market
        self.base_url = base_url
        self.stream_names = list(stream_names)
        self.message_handler = message_handler
        self.settings = settings
        self.metrics = metrics
        self.circuits = circuits
        self.runtime_state = runtime_state
        self.shutdown_event = shutdown_event
        self.logger = logger.bind(component="websocket", worker=name, market=market)

    async def run(self) -> None:
        circuit = self.circuits.get(f"ws.{self.name}")
        attempt = 0
        ping_interval = (
            self.settings.ws_ping_interval_seconds_spot
            if self.market == "spot"
            else self.settings.ws_ping_interval_seconds_futures
        )
        ping_timeout = (
            self.settings.ws_ping_timeout_seconds_spot
            if self.market == "spot"
            else self.settings.ws_ping_timeout_seconds_futures
        )

        while not self.shutdown_event.is_set():
            await circuit.wait_until_ready(self.shutdown_event)
            url = f"{self.base_url}/stream?streams={'/'.join(self.stream_names)}"
            try:
                async with ws_connect(
                    url,
                    open_timeout=self.settings.http_timeout_seconds,
                    close_timeout=self.settings.http_timeout_seconds,
                    ping_interval=ping_interval,
                    ping_timeout=ping_timeout,
                    max_queue=self.settings.ws_message_queue_size,
                ) as websocket:
                    await circuit.on_success()
                    attempt = 0
                    self.runtime_state.websocket_connected[self.name] = True
                    self.logger.info("ws_connected", stream_count=len(self.stream_names))
                    while not self.shutdown_event.is_set():
                        try:
                            raw_message = await asyncio.wait_for(
                                websocket.recv(),
                                timeout=self.settings.ws_recv_timeout_seconds,
                            )
                        except asyncio.TimeoutError:
                            continue
                        if raw_message is None:
                            break
                        ingest_ts_ms = now_utc_ms()
                        stream_name, payload = decode_combined_stream_message(raw_message)
                        if stream_name and payload is not None:
                            await self.message_handler(stream_name, payload, ingest_ts_ms)
                            self.metrics.websocket_messages_total.labels(worker=self.name).inc()
                            self.runtime_state.mark_ws_event(ingest_ts_ms)
            except ShutdownRequested:
                raise
            except Exception as exc:
                attempt += 1
                await circuit.on_failure(5.0)
                delay = min(30.0, (2 ** min(attempt, 5)) + random.uniform(0.0, 1.0))
                self.metrics.websocket_reconnects_total.labels(worker=self.name).inc()
                self.logger.warning("ws_reconnect", delay_seconds=round(delay, 3), error=str(exc))
                await wait_with_shutdown(self.shutdown_event, delay)
            finally:
                self.runtime_state.websocket_connected[self.name] = False


class TradeBackfillWorker:
    def __init__(
        self,
        market: str,
        symbol: str,
        router: AsyncTableRouter,
        state_store: StateStore,
        inspector: DatasetInspector,
        transport: AsyncBinanceTransport,
        settings: AppSettings,
        runtime_state: RuntimeState,
        shutdown_event: asyncio.Event,
        logger: structlog.stdlib.BoundLogger,
    ):
        self.market = market
        self.symbol = symbol
        self.router = router
        self.state_store = state_store
        self.inspector = inspector
        self.transport = transport
        self.settings = settings
        self.runtime_state = runtime_state
        self.shutdown_event = shutdown_event
        self.logger = logger.bind(component="trade_backfill", market=market, symbol=symbol)
        if market == "spot":
            self.table_name = "spot_trades"
            self.recent_endpoint = "spot_recent_trades"
            self.historical_endpoint = "spot_historical_trades"
            self.rest_normalizer = normalize_spot_trade_rest
            self.batch_limit = 1000
        else:
            self.table_name = "futures_trades"
            self.recent_endpoint = "futures_recent_trades"
            self.historical_endpoint = "futures_historical_trades"
            self.rest_normalizer = normalize_futures_trade_rest
            self.batch_limit = 500
        self.next_trade_id: Optional[int] = None

    async def _bootstrap_next_id(self) -> Optional[int]:
        cached = await self.state_store.load("trade_cursor", self.table_name, self.symbol)
        if cached and as_int(cached.get("next_trade_id")) is not None:
            return int(cached["next_trade_id"])
        inferred = await self.inspector.infer_trade_next_id(self.table_name, self.symbol)
        if inferred is not None:
            return inferred
        recent = await self.transport.request_json(self.recent_endpoint, {"symbol": self.symbol, "limit": 1}, retries=3)
        if not recent:
            return None
        latest_id = as_int(recent[-1].get("id"))
        if latest_id is None:
            return None
        return max(0, latest_id - self.settings.initial_trade_backfill_trades + 1)

    async def _save_next_id(self, next_trade_id: int) -> None:
        await self.state_store.save(
            "trade_cursor",
            self.table_name,
            self.symbol,
            {"next_trade_id": next_trade_id},
        )

    async def _drain_once(self) -> bool:
        next_id = self.next_trade_id
        if next_id is None:
            next_id = await self._bootstrap_next_id()
            if next_id is None:
                return False
            self.next_trade_id = next_id

        progressed_any = False
        while not self.shutdown_event.is_set():
            batch = await self.transport.request_json(
                self.historical_endpoint,
                {"symbol": self.symbol, "fromId": next_id, "limit": self.batch_limit},
                retries=5,
            )
            if not batch:
                return progressed_any

            seen_trade_ids: set[int] = set()
            last_trade_id: Optional[int] = None
            ingest_ts_ms = now_utc_ms()
            for item in batch:
                trade_id = as_int(item.get("id"))
                if trade_id is None or trade_id < next_id or trade_id in seen_trade_ids:
                    continue
                seen_trade_ids.add(trade_id)
                if self.market == "spot":
                    row = self.rest_normalizer(item, ingest_ts_ms)
                    row["symbol"] = self.symbol
                else:
                    row = self.rest_normalizer(item, ingest_ts_ms, self.symbol)
                await self.router.submit(self.table_name, row)
                last_trade_id = trade_id
                progressed_any = True

            self.runtime_state.mark_rest_event(ingest_ts_ms)

            if last_trade_id is None:
                return progressed_any

            next_id = last_trade_id + 1
            self.next_trade_id = next_id
            await self._save_next_id(next_id)
            if len(batch) < self.batch_limit:
                return progressed_any

        return progressed_any

    async def run(self) -> None:
        while not self.shutdown_event.is_set():
            try:
                progressed = await self._drain_once()
            except MissingApiKeyError:
                self.logger.warning("trade_backfill_disabled_no_api_key")
                return
            except ShutdownRequested:
                return
            except Exception as exc:
                self.logger.warning("trade_backfill_error", error=str(exc))
                progressed = False
            if not progressed:
                await wait_with_shutdown(self.shutdown_event, 1.0)


class FundingHistoryWorker:
    def __init__(
        self,
        symbol: str,
        router: AsyncTableRouter,
        state_store: StateStore,
        inspector: DatasetInspector,
        transport: AsyncBinanceTransport,
        settings: AppSettings,
        runtime_state: RuntimeState,
        shutdown_event: asyncio.Event,
        logger: structlog.stdlib.BoundLogger,
    ):
        self.symbol = symbol
        self.router = router
        self.state_store = state_store
        self.inspector = inspector
        self.transport = transport
        self.settings = settings
        self.runtime_state = runtime_state
        self.shutdown_event = shutdown_event
        self.logger = logger.bind(component="funding_history", symbol=symbol)
        self.next_funding_time_ms: Optional[int] = None

    async def _bootstrap_next_time(self) -> int:
        cached = await self.state_store.load("funding_cursor", "futures_funding_history", self.symbol)
        if cached and as_int(cached.get("next_funding_time_ms")) is not None:
            return int(cached["next_funding_time_ms"])
        inferred = await self.inspector.infer_funding_next_time(self.symbol)
        if inferred is not None:
            return inferred
        return now_utc_ms() - (self.settings.funding_lookback_hours * 60 * 60 * 1000)

    async def _save_next_time(self, next_time_ms: int) -> None:
        await self.state_store.save(
            "funding_cursor",
            "futures_funding_history",
            self.symbol,
            {"next_funding_time_ms": next_time_ms},
        )

    async def _drain_once(self) -> bool:
        next_time_ms = self.next_funding_time_ms
        if next_time_ms is None:
            next_time_ms = await self._bootstrap_next_time()
            self.next_funding_time_ms = next_time_ms

        progressed_any = False
        while not self.shutdown_event.is_set():
            batch = await self.transport.request_json(
                "futures_funding_rate",
                {"symbol": self.symbol, "startTime": next_time_ms, "limit": 1000},
                retries=5,
            )
            if not batch:
                return progressed_any

            seen_times: set[int] = set()
            last_time_ms: Optional[int] = None
            ingest_ts_ms = now_utc_ms()
            for item in batch:
                funding_time_ms = as_int(item.get("fundingTime"))
                if funding_time_ms is None or funding_time_ms < next_time_ms or funding_time_ms in seen_times:
                    continue
                seen_times.add(funding_time_ms)
                row = normalize_funding_history_rest(item, ingest_ts_ms, self.symbol)
                await self.router.submit("futures_funding_history", row)
                last_time_ms = funding_time_ms
                progressed_any = True

            self.runtime_state.mark_rest_event(ingest_ts_ms)

            if last_time_ms is None:
                return progressed_any

            next_time_ms = last_time_ms + 1
            self.next_funding_time_ms = next_time_ms
            await self._save_next_time(next_time_ms)
            if len(batch) < 1000:
                return progressed_any
        return progressed_any

    async def run(self) -> None:
        while not self.shutdown_event.is_set():
            try:
                progressed = await self._drain_once()
            except ShutdownRequested:
                return
            except Exception as exc:
                self.logger.warning("funding_history_error", error=str(exc))
                progressed = False
            if not progressed:
                await wait_with_shutdown(self.shutdown_event, 300.0)


class ControlServer:
    def __init__(
        self,
        settings: AppSettings,
        router: AsyncTableRouter,
        runtime_state: RuntimeState,
    ):
        self.settings = settings
        self.router = router
        self.runtime_state = runtime_state
        self.runner: Optional[web.AppRunner] = None
        self.site: Optional[web.TCPSite] = None

    async def start(self) -> None:
        app = web.Application()
        app.router.add_get("/health", self.handle_health)
        app.router.add_get("/metrics", self.handle_metrics)
        self.runner = web.AppRunner(app)
        await self.runner.setup()
        self.site = web.TCPSite(self.runner, host=self.settings.health_host, port=self.settings.health_port)
        await self.site.start()

    async def stop(self) -> None:
        if self.site is not None:
            await self.site.stop()
            self.site = None
        if self.runner is not None:
            await self.runner.cleanup()
            self.runner = None

    async def handle_health(self, _request: web.Request) -> web.Response:
        status = 503 if self.runtime_state.shutdown_requested else 200
        return web.json_response(self.runtime_state.health_payload(self.router.snapshot_queue_sizes()), status=status)

    async def handle_metrics(self, _request: web.Request) -> web.Response:
        return web.Response(body=generate_latest(), headers={"Content-Type": CONTENT_TYPE_LATEST})


async def stats_reporter(
    settings: AppSettings,
    router: AsyncTableRouter,
    runtime_state: RuntimeState,
    metrics: Metrics,
    shutdown_event: asyncio.Event,
    logger: structlog.stdlib.BoundLogger,
) -> None:
    log = logger.bind(component="stats")
    while not shutdown_event.is_set():
        queue_sizes = router.snapshot_queue_sizes()
        for table_name, size in queue_sizes.items():
            metrics.queue_size.labels(table=table_name).set(size)
        now_ms = now_utc_ms()
        if runtime_state.last_ws_event_ms:
            metrics.last_activity_age_seconds.labels(kind="ws").set(max(0.0, (now_ms - runtime_state.last_ws_event_ms) / 1000.0))
        if runtime_state.last_rest_event_ms:
            metrics.last_activity_age_seconds.labels(kind="rest").set(max(0.0, (now_ms - runtime_state.last_rest_event_ms) / 1000.0))
        if runtime_state.last_disk_write_ms:
            metrics.last_activity_age_seconds.labels(kind="disk").set(max(0.0, (now_ms - runtime_state.last_disk_write_ms) / 1000.0))
        log.info(
            "runtime_stats",
            queue_sizes=queue_sizes,
            spot_symbol_count=len(runtime_state.spot_symbols),
            futures_symbol_count=len(runtime_state.futures_symbols),
            websocket_connected=sum(1 for alive in runtime_state.websocket_connected.values() if alive),
        )
        await wait_with_shutdown(shutdown_event, settings.stats_interval_seconds)


def required_tables(spot_symbols: Sequence[str], futures_symbols: Sequence[str]) -> List[str]:
    tables: List[str] = []
    if spot_symbols:
        tables.extend(["spot_trades", "spot_l1", "spot_depth_updates"])
    if futures_symbols:
        tables.extend(
            [
                "futures_trades",
                "futures_l1",
                "futures_depth_updates",
                "futures_mark_price",
                "futures_funding_history",
            ]
        )
    return tables


def symbol_count_for_table(table_name: str, spot_symbols: Sequence[str], futures_symbols: Sequence[str]) -> int:
    return len(spot_symbols) if TABLE_MARKETS[table_name] == "spot" else len(futures_symbols)


def install_signal_handlers(shutdown_event: asyncio.Event, logger: structlog.stdlib.BoundLogger) -> None:
    loop = asyncio.get_running_loop()

    def _request_shutdown(sig_name: str) -> None:
        logger.info("shutdown_signal", signal=sig_name)
        shutdown_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        with contextlib.suppress(NotImplementedError):
            loop.add_signal_handler(sig, _request_shutdown, sig.name)


async def supervise_tasks(
    tasks: Sequence[asyncio.Task],
    shutdown_event: asyncio.Event,
    logger: structlog.stdlib.BoundLogger,
) -> None:
    try:
        while True:
            if shutdown_event.is_set():
                break
            done, _pending = await asyncio.wait(tasks, timeout=1.0, return_when=asyncio.FIRST_EXCEPTION)
            for task in done:
                if task.cancelled():
                    continue
                exc = task.exception()
                if exc is not None:
                    logger.error("background_task_failed", task=task.get_name(), error=str(exc))
                    shutdown_event.set()
                    break
            if all(task.done() for task in tasks):
                break
    finally:
        shutdown_event.set()


async def async_main(settings: AppSettings) -> int:
    logger = configure_logging(settings.log_level)
    api_key = settings.resolve_api_key()
    shutdown_event = asyncio.Event()
    install_signal_handlers(shutdown_event, logger)

    settings.out_dir.mkdir(parents=True, exist_ok=True)
    settings.state_dir.mkdir(parents=True, exist_ok=True)

    runtime_state = RuntimeState()
    metrics = Metrics()
    circuits = CircuitRegistry(metrics)
    transport = AsyncBinanceTransport(settings, api_key, metrics, circuits, shutdown_event, logger)
    control_server: Optional[ControlServer] = None
    executor: Optional[ThreadPoolExecutor] = None
    try:
        spot_symbols, futures_symbols = await resolve_symbols(settings, transport, logger)
        if not spot_symbols and not futures_symbols:
            raise SystemExit("At least one spot or futures symbol is required.")

        runtime_state.spot_symbols = spot_symbols
        runtime_state.futures_symbols = futures_symbols

        tables = required_tables(spot_symbols, futures_symbols)
        queues = {table_name: asyncio.Queue(maxsize=max(1, settings.queue_maxsize)) for table_name in tables}
        router = AsyncTableRouter(queues=queues, metrics=metrics, shutdown_event=shutdown_event)
        state_store = StateStore(settings.state_dir)
        inspector = DatasetInspector(settings.out_dir)
        control_server = ControlServer(settings, router, runtime_state)
        executor = ThreadPoolExecutor(max_workers=max(1, settings.parquet_io_workers), thread_name_prefix="parquet-io")

        if settings.print_config:
            logger.info(
                "resolved_config",
                spot_symbols=spot_symbols,
                futures_symbols=futures_symbols,
                out_dir=str(settings.out_dir),
                state_dir=str(settings.state_dir),
                queue_maxsize=settings.queue_maxsize,
                api_key_present=bool(api_key),
                docker_image=settings.docker_image,
            )

        logger.info(
            "startup",
            mode="async",
            out_dir=str(settings.out_dir),
            state_dir=str(settings.state_dir),
            api_key_present=bool(api_key),
            spot_symbol_count=len(spot_symbols),
            futures_symbol_count=len(futures_symbols),
            spot_symbols=spot_symbols[:20],
            futures_symbols=futures_symbols[:20],
            health_port=settings.health_port,
        )

        for table_name in tables:
            logger.info(
                "writer_config",
                table=table_name,
                symbol_count=symbol_count_for_table(table_name, spot_symbols, futures_symbols),
                seen_capacity=settings.dedupe_capacity_for_table(
                    table_name,
                    symbol_count_for_table(table_name, spot_symbols, futures_symbols),
                ),
            )

        await control_server.start()

        tasks: List[asyncio.Task] = []

        for table_name, queue in queues.items():
            writer = AsyncTableWriter(
                table_name=table_name,
                queue=queue,
                settings=settings,
                out_dir=settings.out_dir,
                symbol_count=symbol_count_for_table(table_name, spot_symbols, futures_symbols),
                metrics=metrics,
                runtime_state=runtime_state,
                logger=logger,
                executor=executor,
                shutdown_event=shutdown_event,
            )
            tasks.append(asyncio.create_task(writer.run(), name=f"writer:{table_name}"))

        if spot_symbols:
            spot_handler = make_spot_message_handler(router)
            for index, stream_group in enumerate(
                build_stream_groups(spot_symbols, MARKET_SUFFIXES["spot"], settings.ws_max_streams_per_connection),
                start=1,
            ):
                worker = WebsocketStreamWorker(
                    name=f"spot-ws-{index}",
                    market="spot",
                    base_url=SPOT_WS_BASE,
                    stream_names=stream_group,
                    message_handler=spot_handler,
                    settings=settings,
                    metrics=metrics,
                    circuits=circuits,
                    runtime_state=runtime_state,
                    shutdown_event=shutdown_event,
                    logger=logger,
                )
                tasks.append(asyncio.create_task(worker.run(), name=f"ws:spot:{index}"))

        if futures_symbols:
            futures_handler = make_futures_message_handler(router)
            for index, stream_group in enumerate(
                build_stream_groups(futures_symbols, MARKET_SUFFIXES["futures"], settings.ws_max_streams_per_connection),
                start=1,
            ):
                worker = WebsocketStreamWorker(
                    name=f"futures-ws-{index}",
                    market="futures",
                    base_url=FUTURES_WS_BASE,
                    stream_names=stream_group,
                    message_handler=futures_handler,
                    settings=settings,
                    metrics=metrics,
                    circuits=circuits,
                    runtime_state=runtime_state,
                    shutdown_event=shutdown_event,
                    logger=logger,
                )
                tasks.append(asyncio.create_task(worker.run(), name=f"ws:futures:{index}"))

        if spot_symbols:
            if api_key:
                for symbol in spot_symbols:
                    worker = TradeBackfillWorker(
                        market="spot",
                        symbol=symbol,
                        router=router,
                        state_store=state_store,
                        inspector=inspector,
                        transport=transport,
                        settings=settings,
                        runtime_state=runtime_state,
                        shutdown_event=shutdown_event,
                        logger=logger,
                    )
                    tasks.append(asyncio.create_task(worker.run(), name=f"backfill:spot:{symbol}"))
            else:
                logger.warning("spot_trade_backfill_disabled_no_api_key")

        if futures_symbols:
            if api_key:
                for symbol in futures_symbols:
                    worker = TradeBackfillWorker(
                        market="futures",
                        symbol=symbol,
                        router=router,
                        state_store=state_store,
                        inspector=inspector,
                        transport=transport,
                        settings=settings,
                        runtime_state=runtime_state,
                        shutdown_event=shutdown_event,
                        logger=logger,
                    )
                    tasks.append(asyncio.create_task(worker.run(), name=f"backfill:futures:{symbol}"))
            else:
                logger.warning("futures_trade_backfill_disabled_no_api_key")

            for symbol in futures_symbols:
                worker = FundingHistoryWorker(
                    symbol=symbol,
                    router=router,
                    state_store=state_store,
                    inspector=inspector,
                    transport=transport,
                    settings=settings,
                    runtime_state=runtime_state,
                    shutdown_event=shutdown_event,
                    logger=logger,
                )
                tasks.append(asyncio.create_task(worker.run(), name=f"funding:{symbol}"))

        tasks.append(
            asyncio.create_task(
                stats_reporter(settings, router, runtime_state, metrics, shutdown_event, logger),
                name="stats",
            )
        )

        supervisor = asyncio.create_task(supervise_tasks(tasks, shutdown_event, logger), name="supervisor")
        await shutdown_event.wait()
        runtime_state.shutdown_requested = True
        await supervisor
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for task, result in zip(tasks, results):
            if isinstance(result, Exception) and not isinstance(result, ShutdownRequested):
                logger.warning("task_exit_with_error", task=task.get_name(), error=str(result))
        return 0
    finally:
        shutdown_event.set()
        runtime_state.shutdown_requested = True
        if control_server is not None:
            with contextlib.suppress(Exception):
                await control_server.stop()
        await transport.aclose()
        if executor is not None:
            with contextlib.suppress(Exception):
                executor.shutdown(wait=True, cancel_futures=False)


def main(argv: Optional[Sequence[str]] = None) -> int:
    overrides = build_cli_overrides(argv)
    settings = AppSettings(**overrides)
    return asyncio.run(async_main(settings))


if __name__ == "__main__":
    raise SystemExit(main())
