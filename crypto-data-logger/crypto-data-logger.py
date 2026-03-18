#!/usr/bin/env python3
"""
Raw Binance spot + futures ingestion pipeline.

This rewrite stores only normalized raw data and intentionally skips every
derived feature. It talks directly to Binance's official REST and websocket
endpoints so the runtime stays lean and avoids unofficial Python wrappers.

Key features:
- Continuous token buckets, per-endpoint min spacing
- Endpoint-scoped, half-open circuits with adaptive decay
- Unified timeout policy + latency buckets
- Strong trades pagination with fromId continuation + dedupe
- Arrow Parquet writer with batched writes, periodic fsync, atomic swap
- Persisted dedupe seeding per shard (reads last few ts_ms)
- Cross-thread stats aggregation and graceful shutdown (no os._exit)
- Multi-threaded execution across websocket ingestion, REST backfill, and table writers

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
import json
import logging
import os
import queue
import random
import signal
import threading
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

import pyarrow as pa
import pyarrow.parquet as pq
import requests
from requests.adapters import HTTPAdapter
from requests.exceptions import ConnectionError as RequestsConnectionError
from requests.exceptions import Timeout as RequestsTimeout

try:
    from websockets.asyncio.client import connect as ws_connect
except ImportError:
    from websockets import connect as ws_connect


BASE_TIMEOUT_SECONDS = 5.0
SHUTDOWN = threading.Event()
DEFAULT_SYMBOLS_INPUT = "BTCUSDT,ETHUSDT,ADAUSDT,BNBUSDT,DOGEUSDT,MATICUDST,SOLUSDT,TRXUSDT,XRPUSDT,MONERO"
SYMBOL_ALIASES = {
    "MATICUDST": "MATICUSDT",
    "MONERO": "XMRUSDT",
    "MONEROUSDT": "XMRUSDT",
    "XMR": "XMRUSDT",
}


def setup_logging(level: str) -> logging.Logger:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s %(threadName)s %(levelname)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    return logging.getLogger("crypto-data-logger")


log = setup_logging(os.getenv("LOG_LEVEL", "INFO"))


# ---------------------- Cross-thread stats ----------------------
_LOCAL_STATS = threading.local()
_GLOBAL_STATS = collections.Counter()
_GLOBAL_STATS_LOCK = threading.Lock()
_LAST_STATS_LOG = [time.monotonic()]


def increment_stat(key: str, count: int = 1) -> None:
    if not hasattr(_LOCAL_STATS, "counters"):
        _LOCAL_STATS.counters = collections.Counter()
    _LOCAL_STATS.counters[key] += count


def aggregate_stats_local() -> None:
    counters = getattr(_LOCAL_STATS, "counters", None)
    if not counters:
        return
    with _GLOBAL_STATS_LOCK:
        _GLOBAL_STATS.update(counters)
    counters.clear()


def log_stats_periodic(period_s: float = 60.0, force: bool = False) -> None:
    now = time.monotonic()
    if not force and (now - _LAST_STATS_LOG[0]) < period_s:
        return
    _LAST_STATS_LOG[0] = now
    aggregate_stats_local()
    with _GLOBAL_STATS_LOCK:
        if not _GLOBAL_STATS:
            return

        def prio(key: str) -> Tuple[int, str]:
            if key.startswith("wrote."):
                return (0, key)
            if key.startswith("queued."):
                return (1, key)
            if key.startswith("ok."):
                return (2, key)
            if key.startswith("err."):
                return (3, key)
            if key.startswith("drop."):
                return (4, key)
            if key.startswith("dedup."):
                return (5, key)
            if key.startswith("lat."):
                return (6, key)
            return (9, key)

        items = sorted(_GLOBAL_STATS.items(), key=lambda kv: prio(kv[0]))
        summary = ", ".join(f"{k}={v}" for k, v in items[:24])
        log.info("STATS %s", summary)


# ---------------------- Helpers ----------------------
def now_utc_ms() -> int:
    return int(time.time() * 1000)


def utc_iso_from_ms(ts_ms: int) -> str:
    return datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc).isoformat()


def date_partition_from_ms(ts_ms: int) -> str:
    return datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc).strftime("%Y-%m-%d")


def hour_partition_from_ms(ts_ms: int) -> str:
    return datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc).strftime("%H")


def sleep_with_shutdown(seconds: float, step: float = 0.25) -> None:
    deadline = time.monotonic() + max(0.0, seconds)
    while not SHUTDOWN.is_set():
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return
        time.sleep(min(step, remaining))


def chunked(items: Sequence[str], size: int) -> List[List[str]]:
    size = max(1, size)
    return [list(items[i : i + size]) for i in range(0, len(items), size)]


def parse_symbols(raw: str) -> List[str]:
    if not raw:
        return []
    seen = set()
    out: List[str] = []
    for item in raw.split(","):
        raw_symbol = item.strip().upper()
        if not raw_symbol:
            continue
        symbol = SYMBOL_ALIASES.get(raw_symbol, raw_symbol)
        if symbol != raw_symbol:
            log.warning("Symbol alias normalized: %s -> %s", raw_symbol, symbol)
        if not symbol or symbol in seen:
            continue
        seen.add(symbol)
        out.append(symbol)
    return out


def as_int(value: Any) -> Optional[int]:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def as_bool(value: Any) -> Optional[bool]:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        low = value.strip().lower()
        if low in {"true", "1", "yes"}:
            return True
        if low in {"false", "0", "no"}:
            return False
    if isinstance(value, (int, float)):
        return bool(value)
    return None


def as_str(value: Any) -> Optional[str]:
    if value is None:
        return None
    return str(value)


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


# ---------------------- Rate limits & circuits ----------------------
class TokenBucket:
    def __init__(self, rpm: int):
        rpm = max(1, int(rpm))
        self.rate_per_second = float(rpm) / 60.0
        self.capacity = float(rpm)
        self.tokens = float(rpm)
        self.last_refill = time.monotonic()
        self.lock = threading.Lock()

    def acquire(self, tokens: float = 1.0) -> None:
        tokens = max(1.0, float(tokens))
        while not SHUTDOWN.is_set():
            sleep_for = 0.0
            with self.lock:
                now = time.monotonic()
                elapsed = now - self.last_refill
                if elapsed > 0:
                    self.tokens = min(self.capacity, self.tokens + elapsed * self.rate_per_second)
                    self.last_refill = now
                if self.tokens >= tokens:
                    self.tokens -= tokens
                    return
                missing = tokens - self.tokens
                sleep_for = max(0.005, missing / self.rate_per_second)
            time.sleep(sleep_for)


class AdaptiveCircuit:
    """
    Endpoint-scoped half-open circuit with exponential decay and a single
    half-open probe at a time.
    """

    def __init__(self):
        self.state = "closed"
        self.until = 0.0
        self.failures = 0
        self.probe_in_flight = False
        self.lock = threading.Lock()

    def allow(self) -> bool:
        with self.lock:
            now = time.monotonic()
            if self.state == "open":
                if now < self.until:
                    return False
                self.state = "half-open"
                if not self.probe_in_flight:
                    self.probe_in_flight = True
                    return True
                return False
            if self.state == "half-open":
                if self.probe_in_flight:
                    return False
                self.probe_in_flight = True
                return True
            return True

    def on_success(self) -> None:
        with self.lock:
            self.state = "closed"
            self.until = 0.0
            self.probe_in_flight = False
            self.failures = max(0, self.failures - 1)

    def on_failure(self, base_seconds: float) -> None:
        with self.lock:
            self.failures += 1
            decay = min(2.0 ** (self.failures - 1), 8.0)
            self.state = "open"
            self.until = time.monotonic() + (base_seconds * decay)
            self.probe_in_flight = False


_CIRCUITS: Dict[str, AdaptiveCircuit] = {}
_CIRCUITS_LOCK = threading.Lock()


def get_circuit(name: str) -> AdaptiveCircuit:
    with _CIRCUITS_LOCK:
        circuit = _CIRCUITS.get(name)
        if circuit is None:
            circuit = AdaptiveCircuit()
            _CIRCUITS[name] = circuit
        return circuit


# ---------------------- REST transport ----------------------
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
    min_spacing_s: float
    timeout_scale: float = 1.0
    requires_api_key: bool = False


REST_HOSTS = {
    "spot": "https://api.binance.com",
    "futures": "https://fapi.binance.com",
}

REST_BUCKETS = {
    "spot": TokenBucket(1100),
    "futures": TokenBucket(2200),
}

REST_ENDPOINTS = {
    "spot_recent_trades": EndpointSpec("spot", "/api/v3/trades", weight=5, min_spacing_s=0.02, timeout_scale=1.0),
    "spot_historical_trades": EndpointSpec(
        "spot",
        "/api/v3/historicalTrades",
        weight=25,
        min_spacing_s=0.05,
        timeout_scale=1.2,
        requires_api_key=True,
    ),
    "futures_recent_trades": EndpointSpec("futures", "/fapi/v1/trades", weight=5, min_spacing_s=0.02, timeout_scale=1.0),
    "futures_historical_trades": EndpointSpec(
        "futures",
        "/fapi/v1/historicalTrades",
        weight=20,
        min_spacing_s=0.05,
        timeout_scale=1.2,
        requires_api_key=True,
    ),
    "futures_funding_rate": EndpointSpec("futures", "/fapi/v1/fundingRate", weight=1, min_spacing_s=0.10, timeout_scale=1.0),
    "spot_time": EndpointSpec("spot", "/api/v3/time", weight=1, min_spacing_s=0.01, timeout_scale=0.8),
    "futures_time": EndpointSpec("futures", "/fapi/v1/time", weight=1, min_spacing_s=0.01, timeout_scale=0.8),
}

_REST_LAST_CALL: Dict[str, float] = {}
_REST_LAST_CALL_LOCK = threading.Lock()


def build_session(pool_size: int = 32) -> requests.Session:
    session = requests.Session()
    adapter = HTTPAdapter(pool_connections=pool_size, pool_maxsize=pool_size, max_retries=0)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.headers.update(
        {
            "Accept": "application/json",
            "Connection": "keep-alive",
            "User-Agent": "raw-crypto-data-logger/1.0",
        }
    )
    return session


def is_retryable(exc: Exception) -> bool:
    if isinstance(exc, (RequestsTimeout, RequestsConnectionError)):
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
    elif isinstance(exc, (RequestsTimeout, RequestsConnectionError)):
        base, cap = 0.75, 15.0
    else:
        base, cap = 0.5, 6.0
    override = retry_after_seconds(exc)
    delay = (override if override is not None else (base * (2 ** attempt))) + random.uniform(0.0, base)
    return min(delay, cap)


class RestClient:
    def __init__(self, api_key: Optional[str], pool_size: int = 32):
        self.api_key = (api_key or "").strip()
        self.session = build_session(pool_size=pool_size)

    def close(self) -> None:
        self.session.close()

    def _respect_min_spacing(self, endpoint_name: str, min_spacing_s: float) -> None:
        while not SHUTDOWN.is_set():
            with _REST_LAST_CALL_LOCK:
                now = time.monotonic()
                last = _REST_LAST_CALL.get(endpoint_name, 0.0)
                gap = min_spacing_s - (now - last)
                if gap <= 0:
                    _REST_LAST_CALL[endpoint_name] = now
                    return
            time.sleep(min(gap, 0.25))

    def request_json(self, endpoint_name: str, params: Optional[Dict[str, Any]] = None, retries: int = 5) -> Any:
        spec = REST_ENDPOINTS[endpoint_name]
        if spec.requires_api_key and not self.api_key:
            raise MissingApiKeyError(f"{endpoint_name} requires BINANCE_API_KEY")

        circuit = get_circuit(f"rest.{endpoint_name}")
        url = REST_HOSTS[spec.host_key] + spec.path
        headers: Dict[str, str] = {}
        if spec.requires_api_key and self.api_key:
            headers["X-MBX-APIKEY"] = self.api_key

        for attempt in range(retries):
            if SHUTDOWN.is_set():
                raise ShutdownRequested()

            while not SHUTDOWN.is_set() and not circuit.allow():
                increment_stat(f"circuit_wait.rest.{endpoint_name}")
                time.sleep(0.25)

            if SHUTDOWN.is_set():
                raise ShutdownRequested()

            self._respect_min_spacing(endpoint_name, spec.min_spacing_s)
            REST_BUCKETS[spec.host_key].acquire(spec.weight)

            try:
                t0 = time.perf_counter()
                response = self.session.get(
                    url,
                    params=params or {},
                    headers=headers,
                    timeout=BASE_TIMEOUT_SECONDS * spec.timeout_scale,
                )
                latency_ms = int((time.perf_counter() - t0) * 1000)
                increment_stat(f"lat.rest.{endpoint_name}.{latency_bucket_ms(latency_ms)}")

                if response.status_code >= 400:
                    raise BinanceHttpError(response.status_code, response.text[:300].strip(), dict(response.headers))

                circuit.on_success()
                increment_stat(f"ok.rest.{endpoint_name}")
                return response.json()

            except Exception as exc:
                status = getattr(exc, "status_code", exc.__class__.__name__)
                increment_stat(f"err.rest.{endpoint_name}.{status}")

                retryable = is_retryable(exc)
                if not retryable or attempt == (retries - 1):
                    circuit.on_failure(30.0 if not retryable else 5.0)
                    raise

                delay = backoff_seconds(exc, attempt)
                circuit.on_failure(max(5.0, delay if isinstance(exc, BinanceHttpError) else 5.0))
                log.warning(
                    "REST retry endpoint=%s attempt=%d/%d params=%s sleep=%.2fs error=%s",
                    endpoint_name,
                    attempt + 1,
                    retries,
                    params,
                    delay,
                    exc,
                )
                sleep_with_shutdown(delay)

        raise RuntimeError(f"unreachable: retries exhausted for {endpoint_name}")


# ---------------------- Output schemas ----------------------
DEPTH_LEVEL_TYPE = pa.list_(pa.struct([("price", pa.string()), ("quantity", pa.string())]))


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
    "spot_trades": TableSpec("spot_trades", TRADE_SCHEMA, ("trade_id",)),
    "futures_trades": TableSpec("futures_trades", TRADE_SCHEMA, ("trade_id",)),
    "spot_l1": TableSpec("spot_l1", L1_SCHEMA, ("update_id",)),
    "futures_l1": TableSpec("futures_l1", L1_SCHEMA, ("update_id",)),
    "spot_depth_updates": TableSpec("spot_depth_updates", DEPTH_SCHEMA, ("final_update_id",)),
    "futures_depth_updates": TableSpec("futures_depth_updates", DEPTH_SCHEMA, ("final_update_id",)),
    "futures_mark_price": TableSpec("futures_mark_price", MARK_PRICE_SCHEMA, ("event_time_ms",)),
    "futures_funding_history": TableSpec("futures_funding_history", FUNDING_HISTORY_SCHEMA, ("funding_time_ms",)),
}


# ---------------------- Normalization ----------------------
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

    raise ValueError("unexpected websocket payload")


# ---------------------- State & dataset inspection ----------------------
class StateStore:
    def __init__(self, root: Path):
        self.root = root
        self.root.mkdir(parents=True, exist_ok=True)
        self.lock = threading.Lock()

    def _path(self, state_kind: str, table_name: str, symbol: str) -> Path:
        return self.root / state_kind / table_name / f"{symbol}.json"

    def load(self, state_kind: str, table_name: str, symbol: str) -> Optional[Dict[str, Any]]:
        path = self._path(state_kind, table_name, symbol)
        if not path.exists():
            return None
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except Exception as exc:
            log.warning("State read failed for %s: %s", path, exc)
            return None

    def save(self, state_kind: str, table_name: str, symbol: str, payload: Dict[str, Any]) -> None:
        path = self._path(state_kind, table_name, symbol)
        path.parent.mkdir(parents=True, exist_ok=True)
        temp_path = path.with_suffix(path.suffix + f".{os.getpid()}.tmp")
        body = dict(payload)
        body["updated_at_ms"] = now_utc_ms()
        body["updated_at_iso"] = utc_iso_from_ms(body["updated_at_ms"])

        with self.lock:
            with temp_path.open("w", encoding="utf-8") as handle:
                handle.write(json.dumps(body, sort_keys=True))
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temp_path, path)
            fsync_directory(path.parent)


class DatasetInspector:
    def __init__(self, out_dir: Path):
        self.out_dir = out_dir

    def recent_files(self, table_name: str, symbol: str, limit: int = 6) -> List[Path]:
        pattern = f"{table_name}/date=*/instrument={symbol}/hour=*/*.parquet"
        return sorted(self.out_dir.glob(pattern), reverse=True)[:limit]

    def tail_rows(
        self,
        table_name: str,
        symbol: str,
        columns: Sequence[str],
        file_limit: int = 6,
        row_limit: int = 256,
    ) -> List[Dict[str, Any]]:
        rows: List[Dict[str, Any]] = []
        for file_path in self.recent_files(table_name, symbol, limit=file_limit):
            try:
                table = pq.read_table(file_path, columns=list(columns))
            except Exception as exc:
                log.warning("Tail read failed for %s: %s", file_path, exc)
                continue
            if table.num_rows == 0:
                continue
            start = max(0, table.num_rows - row_limit)
            rows = table.slice(start).to_pylist() + rows
            if len(rows) >= row_limit:
                rows = rows[-row_limit:]
                break
        return rows[-row_limit:]

    def infer_trade_next_id(self, table_name: str, symbol: str) -> Optional[int]:
        max_trade_id: Optional[int] = None
        for row in self.tail_rows(table_name, symbol, columns=("trade_id", "ts_ms")):
            trade_id = as_int(row.get("trade_id"))
            if trade_id is not None and (max_trade_id is None or trade_id > max_trade_id):
                max_trade_id = trade_id
        if max_trade_id is None:
            return None
        return max_trade_id + 1

    def infer_funding_next_time(self, symbol: str) -> Optional[int]:
        max_time: Optional[int] = None
        for row in self.tail_rows("futures_funding_history", symbol, columns=("funding_time_ms", "ts_ms")):
            funding_time_ms = as_int(row.get("funding_time_ms"))
            if funding_time_ms is not None and (max_time is None or funding_time_ms > max_time):
                max_time = funding_time_ms
        if max_time is None:
            return None
        return max_time + 1


# ---------------------- Arrow Parquet writer ----------------------
class PartitionWriter:
    """
    Writes immutable Parquet segments per table/date/symbol/hour partition.

    Each segment is written to a temporary file, periodically fsynced, then
    atomically renamed to a final parquet shard.
    """

    def __init__(
        self,
        spec: TableSpec,
        partition_dir: Path,
        session_id: str,
        batch_size: int,
        segment_max_rows: int,
        segment_max_age_s: float,
        flush_interval_s: float,
        fsync_interval_s: float,
        seed_file_count: int = 4,
        seed_row_count: int = 256,
    ):
        self.spec = spec
        self.partition_dir = partition_dir
        self.partition_dir.mkdir(parents=True, exist_ok=True)

        self.session_id = session_id
        self.batch_size = max(1, batch_size)
        self.segment_max_rows = max(self.batch_size, segment_max_rows)
        self.segment_max_age_s = max(1.0, segment_max_age_s)
        self.flush_interval_s = max(0.2, flush_interval_s)
        self.fsync_interval_s = max(1.0, fsync_interval_s)
        self.seed_file_count = max(1, seed_file_count)
        self.seed_row_count = max(1, seed_row_count)

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

        self.seen_keys = set()
        self.seen_order = collections.deque()
        self.seen_capacity = max(4096, self.seed_row_count * 8)
        self._seed_seen_keys()

    def _seed_seen_keys(self) -> None:
        columns = list(dict.fromkeys(["ts_ms", *self.spec.dedupe_fields]))
        for file_path in sorted(self.partition_dir.glob("*.parquet"), reverse=True)[: self.seed_file_count]:
            try:
                table = pq.read_table(file_path, columns=columns)
            except Exception as exc:
                log.warning("Dedupe seed read failed for %s: %s", file_path, exc)
                continue
            if table.num_rows == 0:
                continue
            start = max(0, table.num_rows - self.seed_row_count)
            for row in table.slice(start).to_pylist():
                self._remember_key(tuple(row.get(col) for col in columns))

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

    def _flush_buffer(self) -> None:
        if not self.buffer:
            return
        self._ensure_writer()
        arrays = []
        for field in self.spec.schema:
            arrays.append(pa.array([row.get(field.name) for row in self.buffer], type=field.type))
        table = pa.Table.from_arrays(arrays, schema=self.spec.schema)
        self.writer.write_table(table)
        self.rows_in_segment += table.num_rows
        increment_stat(f"wrote.{self.spec.name}", table.num_rows)
        self.buffer.clear()

    def _fsync_open_file(self) -> None:
        if self.current_tmp_path is None or not self.current_tmp_path.exists():
            return
        self._flush_buffer()
        fsync_file(self.current_tmp_path)
        self.last_fsync_at = time.monotonic()

    def append(self, row: Dict[str, Any]) -> bool:
        key = self._dedupe_key(row)
        if key in self.seen_keys:
            increment_stat(f"dedup.{self.spec.name}")
            return False
        self._remember_key(key)
        self.buffer.append(row)
        self.last_activity_at = time.monotonic()

        if len(self.buffer) >= self.batch_size:
            self._flush_buffer()

        if self.rows_in_segment >= self.segment_max_rows:
            self.checkpoint()

        return True

    def maintenance(self) -> None:
        now = time.monotonic()
        if self.buffer and (now - self.last_activity_at) >= self.flush_interval_s:
            self._flush_buffer()
        if self.writer and (now - self.last_fsync_at) >= self.fsync_interval_s:
            self._fsync_open_file()
        if self.writer and self.rows_in_segment > 0 and (now - self.segment_started_at) >= self.segment_max_age_s:
            self.checkpoint()

    def checkpoint(self) -> None:
        if self.writer is None and not self.buffer:
            return

        self._flush_buffer()
        if self.writer is None or self.current_tmp_path is None or self.current_final_path is None:
            return

        self.writer.close()
        self.writer = None
        fsync_file(self.current_tmp_path)
        os.replace(self.current_tmp_path, self.current_final_path)
        fsync_directory(self.partition_dir)
        increment_stat(f"checkpoint.{self.spec.name}")

        self.current_tmp_path = None
        self.current_final_path = None
        self.segment_opened_ms = 0
        self.rows_in_segment = 0
        self.segment_index += 1
        self.segment_started_at = time.monotonic()
        self.last_fsync_at = time.monotonic()

    def close(self) -> None:
        self.checkpoint()


class WriterWorker(threading.Thread):
    def __init__(
        self,
        table_name: str,
        out_dir: Path,
        row_queue: "queue.Queue[Dict[str, Any]]",
        batch_size: int,
        segment_max_rows: int,
        segment_max_age_s: float,
        flush_interval_s: float,
        fsync_interval_s: float,
    ):
        super().__init__(name=f"writer-{table_name}", daemon=False)
        self.table_name = table_name
        self.spec = TABLE_SPECS[table_name]
        self.out_dir = out_dir
        self.row_queue = row_queue
        self.batch_size = batch_size
        self.segment_max_rows = segment_max_rows
        self.segment_max_age_s = segment_max_age_s
        self.flush_interval_s = flush_interval_s
        self.fsync_interval_s = fsync_interval_s
        self.session_id = uuid.uuid4().hex[:10]
        self.partitions: Dict[Tuple[str, str, str], PartitionWriter] = {}

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
                batch_size=self.batch_size,
                segment_max_rows=self.segment_max_rows,
                segment_max_age_s=self.segment_max_age_s,
                flush_interval_s=self.flush_interval_s,
                fsync_interval_s=self.fsync_interval_s,
            )
            self.partitions[key] = writer
        return writer

    def _maintenance(self) -> None:
        for writer in self.partitions.values():
            writer.maintenance()

    def _close_all(self) -> None:
        for writer in self.partitions.values():
            writer.close()

    def run(self) -> None:
        try:
            while not SHUTDOWN.is_set() or not self.row_queue.empty():
                try:
                    row = self.row_queue.get(timeout=1.0)
                except queue.Empty:
                    self._maintenance()
                    aggregate_stats_local()
                    continue

                try:
                    if not row.get("symbol") or row.get("ts_ms") is None:
                        increment_stat(f"drop.{self.table_name}.invalid_row")
                    else:
                        self._partition_writer(row).append(row)
                except Exception as exc:
                    increment_stat(f"drop.{self.table_name}.writer_error")
                    log.exception("[%s] writer error: %s", self.table_name, exc)
                finally:
                    self.row_queue.task_done()

                self._maintenance()
                aggregate_stats_local()
        finally:
            self._close_all()
            aggregate_stats_local()


# ---------------------- Queue router ----------------------
class TableRouter:
    def __init__(self, table_queues: Dict[str, "queue.Queue[Dict[str, Any]]"]):
        self.table_queues = table_queues

    def submit(self, table_name: str, row: Dict[str, Any]) -> None:
        q = self.table_queues[table_name]
        while not SHUTDOWN.is_set():
            try:
                q.put(row, timeout=0.5)
                increment_stat(f"queued.{table_name}")
                return
            except queue.Full:
                increment_stat(f"backpressure.{table_name}")
        raise ShutdownRequested()


# ---------------------- Websocket workers ----------------------
SPOT_WS_BASE = "wss://stream.binance.com:9443"
FUTURES_WS_BASE = "wss://fstream.binance.com"


class WebsocketStreamWorker(threading.Thread):
    def __init__(
        self,
        name: str,
        base_url: str,
        stream_names: Sequence[str],
        message_handler: Callable[[str, Dict[str, Any], int], None],
    ):
        super().__init__(name=name, daemon=False)
        self.base_url = base_url
        self.stream_names = list(stream_names)
        self.message_handler = message_handler
        self.endpoint_name = f"ws.{name}"
        self.messages_processed = 0

    async def _run_forever(self) -> None:
        circuit = get_circuit(self.endpoint_name)
        attempt = 0

        while not SHUTDOWN.is_set():
            while not SHUTDOWN.is_set() and not circuit.allow():
                increment_stat(f"circuit_wait.{self.endpoint_name}")
                await asyncio.sleep(0.25)

            if SHUTDOWN.is_set():
                return

            url = f"{self.base_url}/stream?streams={'/'.join(self.stream_names)}"

            try:
                async with ws_connect(
                    url,
                    open_timeout=BASE_TIMEOUT_SECONDS,
                    close_timeout=BASE_TIMEOUT_SECONDS,
                    ping_interval=20,
                    ping_timeout=20,
                    max_queue=4096,
                ) as websocket:
                    circuit.on_success()
                    attempt = 0
                    increment_stat(f"ok.{self.endpoint_name}.connect")
                    log.info("[%s] connected with %d streams", self.name, len(self.stream_names))

                    while not SHUTDOWN.is_set():
                        try:
                            raw_message = await asyncio.wait_for(websocket.recv(), timeout=1.0)
                        except asyncio.TimeoutError:
                            continue

                        if raw_message is None:
                            break

                        ingest_ts_ms = now_utc_ms()
                        try:
                            stream_name, payload = decode_combined_stream_message(raw_message)
                            if stream_name and payload is not None:
                                self.message_handler(stream_name, payload, ingest_ts_ms)
                                self.messages_processed += 1
                        except Exception as exc:
                            increment_stat(f"drop.{self.endpoint_name}.message")
                            log.warning("[%s] dropped websocket payload: %s", self.name, exc)

                        if self.messages_processed and (self.messages_processed % 500) == 0:
                            aggregate_stats_local()
                            log_stats_periodic()

            except Exception as exc:
                attempt += 1
                increment_stat(f"err.{self.endpoint_name}.{exc.__class__.__name__}")
                circuit.on_failure(5.0)
                delay = min(30.0, (2 ** min(attempt, 5)) + random.uniform(0.0, 1.0))
                log.warning("[%s] reconnect in %.1fs after %s", self.name, delay, exc)
                await asyncio.sleep(delay)

    def run(self) -> None:
        try:
            asyncio.run(self._run_forever())
        finally:
            aggregate_stats_local()


# ---------------------- REST backfill workers ----------------------
class TradeBackfillWorker(threading.Thread):
    def __init__(
        self,
        market: str,
        symbol: str,
        router: TableRouter,
        state_store: StateStore,
        inspector: DatasetInspector,
        api_key: Optional[str],
        initial_backfill_trades: int,
        batch_limit: int = 1000,
        idle_sleep_s: float = 1.0,
    ):
        super().__init__(name=f"{market}-trade-rest-{symbol}", daemon=False)
        self.market = market
        self.symbol = symbol
        self.router = router
        self.state_store = state_store
        self.inspector = inspector
        self.rest = RestClient(api_key=api_key)
        self.initial_backfill_trades = max(1, initial_backfill_trades)
        self.batch_limit = max(1, min(1000, batch_limit))
        self.idle_sleep_s = max(0.25, idle_sleep_s)

        if market == "spot":
            self.table_name = "spot_trades"
            self.recent_endpoint = "spot_recent_trades"
            self.historical_endpoint = "spot_historical_trades"
            self.rest_normalizer = normalize_spot_trade_rest
        else:
            self.table_name = "futures_trades"
            self.recent_endpoint = "futures_recent_trades"
            self.historical_endpoint = "futures_historical_trades"
            self.rest_normalizer = normalize_futures_trade_rest

        self.next_trade_id: Optional[int] = None

    def _bootstrap_next_id(self) -> Optional[int]:
        cached = self.state_store.load("trade_cursor", self.table_name, self.symbol)
        if cached and as_int(cached.get("next_trade_id")) is not None:
            return int(cached["next_trade_id"])

        inferred = self.inspector.infer_trade_next_id(self.table_name, self.symbol)
        if inferred is not None:
            return inferred

        recent = self.rest.request_json(self.recent_endpoint, {"symbol": self.symbol, "limit": 1}, retries=3)
        if not recent:
            return None
        latest_id = as_int(recent[-1].get("id"))
        if latest_id is None:
            return None
        return max(0, latest_id - self.initial_backfill_trades + 1)

    def _save_next_id(self, next_trade_id: int) -> None:
        self.state_store.save(
            "trade_cursor",
            self.table_name,
            self.symbol,
            {
                "next_trade_id": next_trade_id,
            },
        )

    def _drain_symbol(self) -> bool:
        next_id = self.next_trade_id
        if next_id is None:
            next_id = self._bootstrap_next_id()
            if next_id is None:
                return False
            self.next_trade_id = next_id

        progressed_any = False
        while not SHUTDOWN.is_set():
            batch = self.rest.request_json(
                self.historical_endpoint,
                {
                    "symbol": self.symbol,
                    "fromId": next_id,
                    "limit": self.batch_limit,
                },
                retries=5,
            )

            if not batch:
                return progressed_any

            seen_trade_ids = set()
            last_trade_id: Optional[int] = None
            ingest_ts_ms = now_utc_ms()

            for item in batch:
                trade_id = as_int(item.get("id"))
                if trade_id is None or trade_id < next_id or trade_id in seen_trade_ids:
                    increment_stat(f"drop.{self.table_name}.rest_dedupe")
                    continue

                seen_trade_ids.add(trade_id)
                if self.market == "spot":
                    row = self.rest_normalizer(item, ingest_ts_ms)
                    row["symbol"] = self.symbol
                else:
                    row = self.rest_normalizer(item, ingest_ts_ms, self.symbol)
                self.router.submit(self.table_name, row)
                last_trade_id = trade_id
                progressed_any = True

            if last_trade_id is None:
                return progressed_any

            next_id = last_trade_id + 1
            self.next_trade_id = next_id
            self._save_next_id(next_id)
            aggregate_stats_local()

            if len(batch) < self.batch_limit:
                return progressed_any

        return progressed_any

    def run(self) -> None:
        try:
            while not SHUTDOWN.is_set():
                try:
                    any_progress = self._drain_symbol()
                except ShutdownRequested:
                    return
                except Exception as exc:
                    increment_stat(f"err.{self.table_name}.worker")
                    log.warning("[%s] trade backfill failed for %s: %s", self.market, self.symbol, exc)
                    any_progress = False
                aggregate_stats_local()
                log_stats_periodic()
                if not any_progress:
                    sleep_with_shutdown(self.idle_sleep_s)
        finally:
            self.rest.close()
            aggregate_stats_local()


class FundingHistoryWorker(threading.Thread):
    def __init__(
        self,
        symbol: str,
        router: TableRouter,
        state_store: StateStore,
        inspector: DatasetInspector,
        api_key: Optional[str],
        lookback_hours: int,
        batch_limit: int = 1000,
        idle_sleep_s: float = 300.0,
    ):
        super().__init__(name=f"futures-funding-rest-{symbol}", daemon=False)
        self.symbol = symbol
        self.router = router
        self.state_store = state_store
        self.inspector = inspector
        self.rest = RestClient(api_key=api_key)
        self.lookback_hours = max(1, lookback_hours)
        self.batch_limit = max(1, min(1000, batch_limit))
        self.idle_sleep_s = max(5.0, idle_sleep_s)
        self.next_funding_time: Optional[int] = None

    def _bootstrap_next_time(self) -> int:
        cached = self.state_store.load("funding_cursor", "futures_funding_history", self.symbol)
        if cached and as_int(cached.get("next_funding_time_ms")) is not None:
            return int(cached["next_funding_time_ms"])

        inferred = self.inspector.infer_funding_next_time(self.symbol)
        if inferred is not None:
            return inferred

        return now_utc_ms() - (self.lookback_hours * 60 * 60 * 1000)

    def _save_next_time(self, next_time_ms: int) -> None:
        self.state_store.save(
            "funding_cursor",
            "futures_funding_history",
            self.symbol,
            {
                "next_funding_time_ms": next_time_ms,
            },
        )

    def _drain_symbol(self) -> bool:
        next_time_ms = self.next_funding_time
        if next_time_ms is None:
            next_time_ms = self._bootstrap_next_time()
            self.next_funding_time = next_time_ms

        progressed_any = False
        while not SHUTDOWN.is_set():
            batch = self.rest.request_json(
                "futures_funding_rate",
                {
                    "symbol": self.symbol,
                    "startTime": next_time_ms,
                    "limit": self.batch_limit,
                },
                retries=5,
            )

            if not batch:
                return progressed_any

            seen_times = set()
            last_time_ms: Optional[int] = None
            ingest_ts_ms = now_utc_ms()

            for item in batch:
                funding_time_ms = as_int(item.get("fundingTime"))
                if funding_time_ms is None or funding_time_ms < next_time_ms or funding_time_ms in seen_times:
                    increment_stat("drop.futures_funding_history.rest_dedupe")
                    continue

                seen_times.add(funding_time_ms)
                row = normalize_funding_history_rest(item, ingest_ts_ms, self.symbol)
                self.router.submit("futures_funding_history", row)
                last_time_ms = funding_time_ms
                progressed_any = True

            if last_time_ms is None:
                return progressed_any

            next_time_ms = last_time_ms + 1
            self.next_funding_time = next_time_ms
            self._save_next_time(next_time_ms)
            aggregate_stats_local()

            if len(batch) < self.batch_limit:
                return progressed_any

        return progressed_any

    def run(self) -> None:
        try:
            while not SHUTDOWN.is_set():
                try:
                    any_progress = self._drain_symbol()
                except ShutdownRequested:
                    return
                except Exception as exc:
                    increment_stat("err.futures_funding_history.worker")
                    log.warning("[futures] funding history failed for %s: %s", self.symbol, exc)
                    any_progress = False
                aggregate_stats_local()
                log_stats_periodic()
                if not any_progress:
                    sleep_with_shutdown(self.idle_sleep_s)
        finally:
            self.rest.close()
            aggregate_stats_local()


# ---------------------- Stats logger ----------------------
class StatsReporter(threading.Thread):
    def __init__(self, interval_s: float = 60.0):
        super().__init__(name="stats", daemon=False)
        self.interval_s = max(5.0, interval_s)

    def run(self) -> None:
        while not SHUTDOWN.is_set():
            sleep_with_shutdown(self.interval_s)
            log_stats_periodic(force=True)


# ---------------------- Stream routing ----------------------
def build_stream_groups(symbols: Sequence[str], suffixes: Sequence[str], max_streams_per_connection: int) -> List[List[str]]:
    stream_names: List[str] = []
    for symbol in symbols:
        lower = symbol.lower()
        for suffix in suffixes:
            stream_names.append(f"{lower}@{suffix}")
    return chunked(stream_names, max_streams_per_connection)


def make_spot_message_handler(router: TableRouter) -> Callable[[str, Dict[str, Any], int], None]:
    def handle(stream_name: str, payload: Dict[str, Any], ingest_ts_ms: int) -> None:
        stream = stream_name.lower()
        if stream.endswith("@trade"):
            router.submit("spot_trades", normalize_spot_trade_ws(payload, ingest_ts_ms))
            return
        if stream.endswith("@bookticker"):
            router.submit("spot_l1", normalize_spot_l1_ws(payload, ingest_ts_ms))
            return
        if stream.endswith("@depth@100ms"):
            router.submit("spot_depth_updates", normalize_spot_depth_ws(payload, ingest_ts_ms))
            return
        increment_stat("drop.spot_ws.unknown_stream")

    return handle


def make_futures_message_handler(router: TableRouter) -> Callable[[str, Dict[str, Any], int], None]:
    def handle(stream_name: str, payload: Dict[str, Any], ingest_ts_ms: int) -> None:
        stream = stream_name.lower()
        if stream.endswith("@trade"):
            router.submit("futures_trades", normalize_futures_trade_ws(payload, ingest_ts_ms))
            return
        if stream.endswith("@bookticker"):
            router.submit("futures_l1", normalize_futures_l1_ws(payload, ingest_ts_ms))
            return
        if stream.endswith("@depth@100ms"):
            router.submit("futures_depth_updates", normalize_futures_depth_ws(payload, ingest_ts_ms))
            return
        if stream.endswith("@markprice@1s"):
            router.submit("futures_mark_price", normalize_futures_mark_price_ws(payload, ingest_ts_ms))
            return
        increment_stat("drop.futures_ws.unknown_stream")

    return handle


# ---------------------- CLI & app bootstrap ----------------------
def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Binance raw market data logger")
    parser.add_argument("--spot-symbols", default=os.getenv("SPOT_SYMBOLS", DEFAULT_SYMBOLS_INPUT))
    parser.add_argument("--futures-symbols", default=os.getenv("FUTURES_SYMBOLS", DEFAULT_SYMBOLS_INPUT))
    parser.add_argument("--out-dir", default=os.getenv("OUT_DIR", "parquet_binance-datasets"))
    parser.add_argument("--state-dir", default=os.getenv("STATE_DIR", "state"))
    parser.add_argument("--log-level", default=os.getenv("LOG_LEVEL", "INFO"))
    parser.add_argument("--queue-maxsize", type=int, default=int(os.getenv("QUEUE_MAXSIZE", "50000")))
    parser.add_argument("--parquet-batch-size", type=int, default=int(os.getenv("PARQUET_BATCH_SIZE", "512")))
    parser.add_argument("--segment-max-rows", type=int, default=int(os.getenv("SEGMENT_MAX_ROWS", "8192")))
    parser.add_argument("--segment-max-age-s", type=float, default=float(os.getenv("SEGMENT_MAX_AGE_S", "30")))
    parser.add_argument("--flush-interval-s", type=float, default=float(os.getenv("FLUSH_INTERVAL_S", "2")))
    parser.add_argument("--fsync-interval-s", type=float, default=float(os.getenv("FSYNC_INTERVAL_S", "15")))
    parser.add_argument(
        "--ws-max-streams-per-connection",
        type=int,
        default=int(os.getenv("WS_MAX_STREAMS_PER_CONNECTION", "120")),
    )
    parser.add_argument(
        "--initial-trade-backfill-trades",
        type=int,
        default=int(os.getenv("INITIAL_TRADE_BACKFILL_TRADES", "1000")),
    )
    parser.add_argument(
        "--funding-lookback-hours",
        type=int,
        default=int(os.getenv("FUNDING_LOOKBACK_HOURS", "168")),
    )
    parser.add_argument("--stats-interval-s", type=float, default=float(os.getenv("STATS_INTERVAL_S", "60")))
    return parser.parse_args()


def install_signal_handlers() -> None:
    def _handle_shutdown(signum: int, _frame: Any) -> None:
        log.info("Signal %s received. Shutting down gracefully.", signum)
        SHUTDOWN.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            signal.signal(sig, _handle_shutdown)
        except Exception:
            pass


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


def build_writer_workers(
    tables: Sequence[str],
    out_dir: Path,
    queue_maxsize: int,
    parquet_batch_size: int,
    segment_max_rows: int,
    segment_max_age_s: float,
    flush_interval_s: float,
    fsync_interval_s: float,
) -> Tuple[Dict[str, "queue.Queue[Dict[str, Any]]"], List[WriterWorker]]:
    table_queues: Dict[str, "queue.Queue[Dict[str, Any]]"] = {}
    workers: List[WriterWorker] = []
    for table_name in tables:
        row_queue: "queue.Queue[Dict[str, Any]]" = queue.Queue(maxsize=max(1, queue_maxsize))
        table_queues[table_name] = row_queue
        workers.append(
            WriterWorker(
                table_name=table_name,
                out_dir=out_dir,
                row_queue=row_queue,
                batch_size=parquet_batch_size,
                segment_max_rows=segment_max_rows,
                segment_max_age_s=segment_max_age_s,
                flush_interval_s=flush_interval_s,
                fsync_interval_s=fsync_interval_s,
            )
        )
    return table_queues, workers


def main() -> int:
    args = parse_args()
    global log
    log = setup_logging(args.log_level)

    spot_symbols = parse_symbols(args.spot_symbols)
    futures_symbols = parse_symbols(args.futures_symbols)
    if not spot_symbols and not futures_symbols:
        raise SystemExit("At least one spot or futures symbol is required.")

    out_dir = Path(args.out_dir)
    state_dir = Path(args.state_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    state_dir.mkdir(parents=True, exist_ok=True)

    api_key = os.getenv("BINANCE_API_KEY", "").strip() or None
    install_signal_handlers()

    log.info("=" * 72)
    log.info("Binance Raw Data Logger")
    log.info("=" * 72)
    log.info("Spot symbols: %s", ", ".join(spot_symbols) if spot_symbols else "(disabled)")
    log.info("Futures symbols: %s", ", ".join(futures_symbols) if futures_symbols else "(disabled)")
    log.info("Output directory: %s", out_dir)
    log.info("State directory: %s", state_dir)
    log.info("BINANCE_API_KEY present: %s", bool(api_key))
    log.info(
        "Thread model: %d table writers, %d spot symbols, %d futures symbols",
        len(required_tables(spot_symbols, futures_symbols)),
        len(spot_symbols),
        len(futures_symbols),
    )
    log.info("=" * 72)

    tables = required_tables(spot_symbols, futures_symbols)
    table_queues, writer_workers = build_writer_workers(
        tables=tables,
        out_dir=out_dir,
        queue_maxsize=args.queue_maxsize,
        parquet_batch_size=args.parquet_batch_size,
        segment_max_rows=args.segment_max_rows,
        segment_max_age_s=args.segment_max_age_s,
        flush_interval_s=args.flush_interval_s,
        fsync_interval_s=args.fsync_interval_s,
    )

    router = TableRouter(table_queues)
    state_store = StateStore(state_dir)
    inspector = DatasetInspector(out_dir)

    stream_workers: List[threading.Thread] = []
    if spot_symbols:
        spot_handler = make_spot_message_handler(router)
        for idx, stream_group in enumerate(
            build_stream_groups(
                spot_symbols,
                suffixes=("trade", "bookTicker", "depth@100ms"),
                max_streams_per_connection=args.ws_max_streams_per_connection,
            ),
            start=1,
        ):
            stream_workers.append(
                WebsocketStreamWorker(
                    name=f"spot-ws-{idx}",
                    base_url=SPOT_WS_BASE,
                    stream_names=stream_group,
                    message_handler=spot_handler,
                )
            )

    if futures_symbols:
        futures_handler = make_futures_message_handler(router)
        for idx, stream_group in enumerate(
            build_stream_groups(
                futures_symbols,
                suffixes=("trade", "bookTicker", "depth@100ms", "markPrice@1s"),
                max_streams_per_connection=args.ws_max_streams_per_connection,
            ),
            start=1,
        ):
            stream_workers.append(
                WebsocketStreamWorker(
                    name=f"futures-ws-{idx}",
                    base_url=FUTURES_WS_BASE,
                    stream_names=stream_group,
                    message_handler=futures_handler,
                )
            )

    rest_workers: List[threading.Thread] = []
    if spot_symbols and api_key:
        for symbol in spot_symbols:
            rest_workers.append(
                TradeBackfillWorker(
                    market="spot",
                    symbol=symbol,
                    router=router,
                    state_store=state_store,
                    inspector=inspector,
                    api_key=api_key,
                    initial_backfill_trades=args.initial_trade_backfill_trades,
                )
            )
    elif spot_symbols:
        log.warning("[spot] BINANCE_API_KEY not set; websocket trades stay live but historicalTrades backfill is disabled")
    if futures_symbols:
        if api_key:
            for symbol in futures_symbols:
                rest_workers.append(
                    TradeBackfillWorker(
                        market="futures",
                        symbol=symbol,
                        router=router,
                        state_store=state_store,
                        inspector=inspector,
                        api_key=api_key,
                        initial_backfill_trades=args.initial_trade_backfill_trades,
                    )
                )
        else:
            log.warning("[futures] BINANCE_API_KEY not set; websocket trades stay live but historicalTrades backfill is disabled")
        for symbol in futures_symbols:
            rest_workers.append(
                FundingHistoryWorker(
                    symbol=symbol,
                    router=router,
                    state_store=state_store,
                    inspector=inspector,
                    api_key=api_key,
                    lookback_hours=args.funding_lookback_hours,
                )
            )

    stats_worker = StatsReporter(interval_s=args.stats_interval_s)
    all_workers: List[threading.Thread] = [*writer_workers, *stream_workers, *rest_workers, stats_worker]

    try:
        for worker in all_workers:
            worker.start()
            log.info("Started %s", worker.name)

        while any(worker.is_alive() for worker in all_workers):
            for worker in all_workers:
                worker.join(timeout=1.0)
            log_stats_periodic(period_s=args.stats_interval_s)

    except KeyboardInterrupt:
        log.info("Keyboard interrupt received. Requesting shutdown.")

    finally:
        SHUTDOWN.set()
        for worker in all_workers:
            worker.join(timeout=20.0)

        alive = [worker.name for worker in all_workers if worker.is_alive()]
        if alive:
            log.warning("Workers still alive after shutdown wait: %s", ", ".join(alive))

        log_stats_periodic(force=True)
        log.info("Shutdown complete.")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
