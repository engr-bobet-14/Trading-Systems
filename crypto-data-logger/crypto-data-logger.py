#!/usr/bin/env python3
"""
Production-grade Binance Spot+Futures snapshot logger (Parquet, partitioned)

Implements:
- Continuous token buckets, per-endpoint min spacing
- Endpoint-scoped, half-open circuits with adaptive decay
- Unified timeout policy + latency buckets
- Strong trades pagination with fromId continuation + dedupe
- Arrow Parquet writer with batched writes, periodic fsync, atomic swap
- Persisted dedupe seeding per shard (reads last few ts_ms)
- Cross-thread stats aggregation and graceful shutdown (no os._exit)
"""

import collections
import logging
import math
import os
import random
import threading
import time
import signal
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple

import numpy as np
import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from binance.client import Client
from binance.exceptions import BinanceAPIException, BinanceRequestException
from requests.exceptions import Timeout, ConnectionError as ReqConnErr

# Optional fast path for ParquetWriter
try:
    import pyarrow as pa
    import pyarrow.parquet as pq
    HAVE_ARROW = True
except Exception:
    HAVE_ARROW = False

# ---------------------- Global config ----------------------
BASE_TIMEOUT = 5.0  # seconds
SHUTDOWN = threading.Event()

# ---------------------- Logging & Observability ----------------------
def setup_logging() -> logging.Logger:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(threadName)s %(levelname)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    return logging.getLogger(__name__)

log = setup_logging()

# Lock-free stats: thread-local counters with periodic aggregation
_LOCAL_STATS = threading.local()
_GLOBAL_STATS = collections.Counter()
_GLOBAL_STATS_LOCK = threading.Lock()
_LAST_STATS_LOG = [time.monotonic()]

def increment_stat(key: str, count: int = 1):
    """Thread-local stat increment (no locks on hot path)."""
    if not hasattr(_LOCAL_STATS, 'counters'):
        _LOCAL_STATS.counters = collections.Counter()
    _LOCAL_STATS.counters[key] += count

def aggregate_stats_local():
    """Push the current thread's local counters to global."""
    if hasattr(_LOCAL_STATS, 'counters') and _LOCAL_STATS.counters:
        with _GLOBAL_STATS_LOCK:
            _GLOBAL_STATS.update(_LOCAL_STATS.counters)
        _LOCAL_STATS.counters.clear()

def log_stats_periodic(period_s=120):
    """Log aggregated stats every period_s seconds."""
    now = time.monotonic()
    if now - _LAST_STATS_LOG[0] < period_s:
        return
    _LAST_STATS_LOG[0] = now
    # Pull this thread's locals
    aggregate_stats_local()
    with _GLOBAL_STATS_LOCK:
        if not _GLOBAL_STATS:
            return
        # Show a tidy, sorted slice: wrote.*, ok.*, err.*, drop.*
        def prio(k):
            if k.startswith("wrote."): return (0, k)
            if k.startswith("ok."):    return (1, k)
            if k.startswith("err."):   return (2, k)
            if k.startswith("drop."):  return (3, k)
            if k.startswith("lat."):   return (4, k)
            return (9, k)
        items = sorted(_GLOBAL_STATS.items(), key=lambda kv: prio(kv[0]))
        head = ", ".join(f"{k}={v}" for k, v in items[:16])
        log.info("STATS %s", head)

# ---------------------- Time Synchronization ----------------------
def iso_now_utc_ms():
    now = datetime.now(timezone.utc)
    return int(now.timestamp() * 1000), now.isoformat()

class TimeSync:
    """Tracks server-local time offset to avoid timestamp drift."""
    def __init__(self, session: requests.Session, refresh_s: int = 300):
        self.session = session
        self.refresh_s = refresh_s
        self.offset_ms = 0
        self.last_sync = 0.0
        self.lock = threading.Lock()

    def _sync(self):
        try:
            r = self.session.get("https://api.binance.com/api/v3/time", timeout=5)
            r.raise_for_status()
            server_ms = int(r.json()["serverTime"])
            local_ms = int(time.time() * 1000)
            with self.lock:
                self.offset_ms = server_ms - local_ms
                self.last_sync = time.time()
            log.info("Server time sync: offset %+d ms", self.offset_ms)
        except Exception as e:
            log.warning("Failed to sync server time: %s", e)

    def now_ms(self) -> int:
        # tiny race protection: read last_sync under lock
        need_sync = False
        with self.lock:
            need_sync = (time.time() - self.last_sync) > self.refresh_s
        if need_sync:
            self._sync()
        with self.lock:
            return int(time.time() * 1000 + self.offset_ms)

# ---------------------- Math Helpers ----------------------
def vwap_side(levels: List[tuple]) -> float:
    if not levels:
        return float("nan")
    px = np.array([p for p, _ in levels], dtype=float)
    qty = np.array([q for _, q in levels], dtype=float)
    denom = qty.sum()
    return float((px * qty).sum() / denom) if denom > 1e-12 else float("nan")

def slope_price_vs_cumqty(levels: List[tuple]) -> float:
    if not levels or len(levels) < 2:
        return float("nan")
    qtys = np.cumsum([q for _, q in levels])
    if qtys[-1] == 0:
        return float("nan")
    prices = np.array([p for p, _ in levels], dtype=float)
    if np.unique(qtys).size < 2:
        return float("nan")
    return float(np.polyfit(qtys, prices, 1)[0])

# ---------------------- HTTP Session ----------------------
def build_session(pool_size: int = 20) -> requests.Session:
    """Create session with explicit connection pool limits per thread."""
    s = requests.Session()
    retry = Retry(total=0, connect=0, read=0, backoff_factor=0)
    adapter = HTTPAdapter(
        max_retries=retry,
        pool_connections=pool_size,
        pool_maxsize=pool_size,
    )
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    s.headers.update({
        'Connection': 'keep-alive',
        'Accept': 'application/json',
    })
    return s

# ---------------------- Token Buckets & Weights ----------------------
class TokenBucket:
    """Continuous refill token bucket for rate limiting."""
    def __init__(self, rpm: int):
        self.rate = max(1.0, float(rpm)) / 60.0  # tokens per second
        self.capacity = max(1.0, float(rpm))
        self.tokens = self.capacity
        self.last = time.monotonic()
        self.lock = threading.Lock()

    def acquire(self, n: int = 1):
        n = max(1.0, float(n))
        with self.lock:
            while True:
                now = time.monotonic()
                elapsed = now - self.last
                if elapsed > 0:
                    self.tokens = min(self.capacity, self.tokens + elapsed * self.rate)
                    self.last = now
                if self.tokens >= n:
                    self.tokens -= n
                    return
                need = n - self.tokens
                time.sleep(max(0.005, need / self.rate))

# conservative defaults; tune per account/IP limits
BUCKETS: Dict[str, TokenBucket] = {
    "api": TokenBucket(1000),
    "fapi": TokenBucket(1000),
}

# Documented-ish endpoint weights (adapt to your exact account/doc tier)
def weight_get_order_book(limit: int = 100, **_) -> int:
    limit = int(limit or 100)
    if limit <= 100: return 5
    if limit <= 500: return 10
    return 50

def weight_klines(limit: int = 500, startTime=None, endTime=None, **_) -> int:
    w = 1 if int(limit or 500) <= 1000 else 2
    if startTime is not None or endTime is not None:
        w += 1
    return w

WEIGHTS = {
    "get_orderbook_ticker": lambda **_: 1,
    "get_order_book": weight_get_order_book,
    "get_klines": weight_klines,
    "get_aggregate_trades": lambda **_: 1,
    "get_symbol_ticker": lambda **_: 1,
    "futures_mark_price": lambda **_: 1,
}

# Per-endpoint minimum spacing to reduce micro-bursts
MIN_SPACING: Dict[Tuple[str, str], float] = {
    ("api", "get_order_book"): 0.05,
    ("api", "get_klines"): 0.02,
    ("api", "get_aggregate_trades"): 0.02,
    ("api", "get_orderbook_ticker"): 0.01,
    ("api", "get_symbol_ticker"): 0.01,
    ("fapi", "futures_mark_price"): 0.01,
}

# Thread-local last call timestamps
_LAST_CALL_TS = threading.local()
def _get_last_call_dict(host: str) -> dict:
    if not hasattr(_LAST_CALL_TS, 'data'):
        _LAST_CALL_TS.data = {
            "api": collections.defaultdict(lambda: 0.0),
            "fapi": collections.defaultdict(lambda: 0.0),
        }
    return _LAST_CALL_TS.data[host]

def _host_for(func) -> str:
    name = getattr(func, "__name__", "")
    return "fapi" if "futures" in name else "api"

def _tokens_for_call(func, **params) -> int:
    name = getattr(func, "__name__", "")
    w = WEIGHTS.get(name, lambda **_: 1)
    try:
        return int(w(**params))
    except Exception:
        return 1

# ---------------------- Endpoint-Scoped Circuit Breaker ----------------------
class AdaptiveCircuit:
    """Half-open circuit breaker with exponential decay on repeated failures."""
    def __init__(self):
        self.state = "closed"
        self.until = 0.0
        self.failures = 0
        self.lock = threading.Lock()

    def open(self, base_secs: float):
        with self.lock:
            self.failures += 1
            decay = min(2.0 ** (self.failures - 1), 8.0)  # cap at 8x
            duration = base_secs * decay
            self.state = "open"
            self.until = max(self.until, time.time() + duration)
            log.warning("Circuit open for %.1fs (failure #%d, decay=%.1fx)",
                        duration, self.failures, decay)

    def allow(self) -> bool:
        with self.lock:
            if self.state == "open" and time.time() >= self.until:
                self.state = "half"
                return True
            return self.state != "open"

    def on_success(self):
        with self.lock:
            if self.state in ("half", "closed"):
                self.state = "closed"
                self.failures = 0
                self.until = 0.0

    def on_failure(self, min_secs: float = 30.0):
        with self.lock:
            self.failures += 1
            decay = min(2.0 ** (self.failures - 1), 8.0)
            duration = min_secs * decay
            self.state = "open"
            self.until = time.time() + duration

# Circuits per (host, endpoint_name)
_CIRCUITS: Dict[Tuple[str, str], AdaptiveCircuit] = {}
_CIRCUITS_LOCK = threading.Lock()

def _get_circuit(host: str, fname: str) -> AdaptiveCircuit:
    key = (host, fname)
    with _CIRCUITS_LOCK:
        if key not in _CIRCUITS:
            _CIRCUITS[key] = AdaptiveCircuit()
        return _CIRCUITS[key]

# ---------------------- Unified Timeout Policy ----------------------
TIMEOUT_SCALE = {
    "get_order_book": 1.0,
    "get_klines": 1.2,
    "get_aggregate_trades": 1.6,
    "get_orderbook_ticker": 0.8,
    "get_symbol_ticker": 0.8,
    "futures_mark_price": 0.8,
}
def _timeout_for(fname: str) -> float:
    return BASE_TIMEOUT * TIMEOUT_SCALE.get(fname, 1.0)

# ---------------------- Error Classification & Retry ----------------------
RETRYABLE_HTTP = {429, 500, 502, 503, 504}
BAN_HTTP = {418}

def is_retryable(exc: Exception) -> bool:
    if isinstance(exc, (Timeout, ReqConnErr, BinanceRequestException)):
        return True
    if isinstance(exc, BinanceAPIException):
        return exc.status_code in RETRYABLE_HTTP or exc.status_code in BAN_HTTP
    return False

def get_retry_after_seconds(exc: Exception) -> Optional[float]:
    try:
        resp = getattr(exc, 'response', None)
        if resp is None or not hasattr(resp, 'headers'):
            return None
        ra = resp.headers.get('Retry-After')
        return float(ra) if ra else None
    except Exception:
        return None

def _classify_backoff(exc: Exception) -> Tuple[float, float]:
    """Returns (max_cap_seconds, base_delay_seconds)."""
    if isinstance(exc, BinanceAPIException):
        sc = exc.status_code
        if sc == 418:
            return (1800.0, 10.0)
        if sc == 429:
            return (120.0, 2.0)
        if sc in (500, 502, 503, 504):
            return (12.0, 0.75)
    if isinstance(exc, (Timeout, ReqConnErr, BinanceRequestException)):
        return (12.0, 0.75)
    return (6.0, 0.5)

def safe_call(func, *args, retries=5, **kwargs):
    """
    Full-jitter exponential backoff with:
    - Circuit breaker check BEFORE token acquisition
    - Endpoint-scoped circuits
    - Adaptive backoff with decay
    - Unified timeout policy
    - Per-endpoint min spacing
    - Latency buckets
    """
    k = dict(kwargs)
    fname = getattr(func, "__name__", "unknown")
    host = _host_for(func)

    # Apply unified timeout
    rp = k.get('requests_params', {})
    if 'timeout' not in rp:
        rp['timeout'] = _timeout_for(fname)
    k['requests_params'] = rp

    tokens_cost = _tokens_for_call(func, **kwargs)
    circuit = _get_circuit(host, fname)
    last_call_dict = _get_last_call_dict(host)

    last_exc = None
    for i in range(retries):
        # circuit gating (half-open allows single probe)
        while not circuit.allow():
            increment_stat(f"circuit_wait.{host}.{fname}")
            time.sleep(0.25)

        # Min-spacing enforcement
        last = last_call_dict[fname]
        gap = MIN_SPACING.get((host, fname), 0.0) - (time.monotonic() - last)
        if gap > 0:
            time.sleep(gap)

        # Rate limiting
        BUCKETS[host].acquire(tokens_cost)

        try:
            t0 = time.time()
            res = func(*args, **k)
            dt_ms = int((time.time() - t0) * 1000)
            last_call_dict[fname] = time.monotonic()
            circuit.on_success()
            increment_stat(f"ok.{host}.{fname}")
            # latency buckets
            bucket = 50 if dt_ms < 50 else 100 if dt_ms < 100 else 250 if dt_ms < 250 else 1000
            increment_stat(f"lat.{host}.{fname}.lt{bucket}ms")
            return res

        except Exception as exc:
            last_exc = exc
            status = getattr(exc, 'status_code', 'unknown')
            increment_stat(f"err.{host}.{fname}.{status}")

            if not is_retryable(exc):
                log.error("%s non-retryable: %s", fname, exc)
                circuit.on_failure(30.0)
                break

            wait_hdr = get_retry_after_seconds(exc)
            max_cap, base = _classify_backoff(exc)
            expo = base * (2 ** i)
            jitter = random.uniform(0, expo)
            delay = min((wait_hdr if wait_hdr is not None else expo) + jitter, max_cap)

            if isinstance(exc, BinanceAPIException) and getattr(exc, 'status_code', None) in (418, 429):
                circuit.open(delay)

            log.warning("%s retry %d/%d host=%s: %s | sleep %.2fs",
                        fname, i + 1, retries, host, exc, delay)
            time.sleep(delay)

    log.error("%s failed after %d retries (%s)", fname, retries, last_exc)
    return None

# ---------------------- Symbol Validation ----------------------
def validate_symbol_format(symbols: List[str]):
    """Warn about symbols that may not work with both spot and futures APIs."""
    for s in symbols:
        if not s.endswith("USDT"):
            log.warning("Symbol %s may not be compatible with spot/futures APIs", s)

# ---------------------- Binance Data Fetchers ----------------------
def latest_ohlcv_1m(client, symbol):
    """Fetch closed 1-minute candle with monotonicity checks."""
    kl = safe_call(client.get_klines, symbol=symbol,
                   interval=Client.KLINE_INTERVAL_1MINUTE, limit=2)
    if not kl or len(kl) < 2:
        return {}
    k = kl[-2]  # closed candle
    try:
        o, h, l, c = map(float, (k[1], k[2], k[3], k[4]))
        v = float(k[5])
        if not (l <= min(o, c) <= h and l <= max(o, c) <= h and v >= 0):
            log.warning("[%s] Invalid OHLCV: o=%.6f h=%.6f l=%.6f c=%.6f v=%.6f",
                        symbol, o, h, l, c, v)
            return {}
    except (ValueError, IndexError, TypeError) as e:
        log.warning("[%s] OHLCV parse error: %s", symbol, e)
        return {}
    return {
        "ohlc_ts_open": int(k[0]),
        "ohlc_open": o,
        "ohlc_high": h,
        "ohlc_low": l,
        "ohlc_close": c,
        "ohlc_volume": v,
        "ohlc_ts_close": int(k[6]),
        "ohlc_trades": int(k[8]),
        "ohlc_taker_base": float(k[9]),
        "ohlc_taker_quote": float(k[10]),
    }

def l1_quote(client, symbol):
    """Fetch L1 bid/ask with sanity checks."""
    t = safe_call(client.get_orderbook_ticker, symbol=symbol)
    if not t:
        return {}
    try:
        bid, ask = float(t["bidPrice"]), float(t["askPrice"])
        bq, aq = float(t["bidQty"]), float(t["askQty"])
        if bid <= 0 or ask <= 0 or ask < bid:
            log.warning("[%s] Invalid L1: bid=%.6f ask=%.6f", symbol, bid, ask)
            return {}
        mid = 0.5 * (bid + ask)
        spread = ask - bid
        imb = (bq - aq) / (bq + aq) if (bq + aq) > 0 else float("nan")
        return {
            "l1_bid": bid, "l1_ask": ask, "l1_mid": mid, "l1_spread": spread,
            "l1_bid_qty": bq, "l1_ask_qty": aq, "l1_imbalance": imb,
        }
    except (KeyError, ValueError, TypeError) as e:
        log.warning("[%s] L1 parse error: %s", symbol, e)
        return {}

def l2_features(client, symbol, depth):
    """Fetch L2 order book features with filtering."""
    depth = int(max(5, min(depth, 100)))
    ob = safe_call(client.get_order_book, symbol=symbol, limit=depth)
    if not ob:
        return {}
    try:
        bids = [(float(p), float(q)) for p, q in ob["bids"][:depth]]
        asks = [(float(p), float(q)) for p, q in ob["asks"][:depth]]
    except (ValueError, TypeError, KeyError) as e:
        log.warning("[%s] L2 parse error: %s", symbol, e)
        return {}

    bids = [(p, q) for p, q in bids if p > 0 and q > 0]
    asks = [(p, q) for p, q in asks if p > 0 and q > 0]
    if not bids or not asks:
        return {}

    bid_depth = float(sum(q for _, q in bids))
    ask_depth = float(sum(q for _, q in asks))
    denom = bid_depth + ask_depth
    if denom <= 1e-9:
        return {}
    depth_asym = (bid_depth - ask_depth) / denom
    return {
        "l2_bid_depth": bid_depth,
        "l2_ask_depth": ask_depth,
        "l2_depth_asymmetry": depth_asym,
        "l2_bid_vwap": vwap_side(bids),
        "l2_ask_vwap": vwap_side(asks),
        "l2_bid_slope": slope_price_vs_cumqty(bids),
        "l2_ask_slope": slope_price_vs_cumqty(asks),
    }

def trades_features(client, symbol, lookback_minutes, now_ms_fn):
    """
    Fetch aggregate trades with pagination, deduplication, and progress monitoring.
    Aligned to last closed minute to reduce overlap.
    """
    end_ms = (now_ms_fn() // 60_000) * 60_000
    start_ms = end_ms - lookback_minutes * 60_000
    all_trades: List[dict] = []
    pages = 0
    MAX_PAGES = 120
    start_wall = time.monotonic()
    MAX_WALL = 12.0

    page_args: Dict[str, Any] = {"symbol": symbol, "startTime": start_ms, "endTime": end_ms}

    while not SHUTDOWN.is_set():
        batch = safe_call(client.get_aggregate_trades, **page_args)
        if not batch:
            break

        pages += 1
        all_trades.extend(batch)
        last = batch[-1]
        reached_end = last.get("T", 0) >= end_ms
        if reached_end:
            break

        elapsed = time.monotonic() - start_wall
        if elapsed > MAX_WALL * 0.8 and not reached_end:
            log.warning("[%s] Trades pagination slow (%.1fs elapsed); consider reducing lookback",
                        symbol, elapsed)

        if len(batch) == 1000:
            from_id = last["a"] + 1
            page_args = {"symbol": symbol, "fromId": from_id}
            continue

        # short page before end: ensure forward progress at least once
        if len(batch) < 1000 and "fromId" not in page_args:
            from_id = last["a"] + 1
            page_args = {"symbol": symbol, "fromId": from_id}
            continue

        if pages >= MAX_PAGES or elapsed > MAX_WALL:
            break

        # if already using fromId and short page, likely done
        if "fromId" in page_args:
            break

    if not all_trades:
        return {
            "tr_volume_base": 0.0,
            "tr_volume_quote": 0.0,
            "tr_vwap": float("nan"),
            "tr_buy_sell_imbalance": float("nan"),
        }

    # Deduplicate by (aggregate_id, timestamp)
    seen = set()
    uniq = []
    for t in all_trades:
        k = (t.get("a"), t.get("T"))
        if k not in seen:
            seen.add(k)
            uniq.append(t)

    try:
        px = np.array([float(t["p"]) for t in uniq], dtype=float)
        qty = np.array([float(t["q"]) for t in uniq], dtype=float)
        vol_base = float(qty.sum())
        vol_quote = float((qty * px).sum())
        vwap = vol_quote / vol_base if vol_base > 1e-12 else float("nan")
        m = np.array([bool(t["m"]) for t in uniq])
        buy_vol = float(qty[~m].sum())
        sell_vol = float(qty[m].sum())
        imb = (buy_vol - sell_vol) / (buy_vol + sell_vol) if (buy_vol + sell_vol) > 1e-12 else float("nan")
        return {
            "tr_volume_base": vol_base,
            "tr_volume_quote": vol_quote,
            "tr_vwap": vwap,
            "tr_buy_sell_imbalance": imb,
        }
    except (KeyError, ValueError, TypeError) as e:
        log.warning("[%s] Trades parse error: %s", symbol, e)
        return {}

def basis_funding(client, symbol):
    """Fetch spot/perp basis and funding rate."""
    spot_info = safe_call(client.get_symbol_ticker, symbol=symbol)
    mark_info = safe_call(client.futures_mark_price, symbol=symbol)
    if not spot_info or not mark_info:
        return {}
    try:
        spot = float(spot_info["price"])
        mark = float(mark_info["markPrice"])
        if spot <= 0 or mark <= 0:
            log.warning("[%s] Invalid basis: spot=%.6f mark=%.6f", symbol, spot, mark)
            return {}
        funding_rate = float(mark_info.get("lastFundingRate", 0.0) or 0.0)
        next_funding_time = int(mark_info.get("nextFundingTime", 0) or 0)
        basis_abs = mark - spot
        basis_pct = basis_abs / spot if spot > 0 else float("nan")
        return {
            "spot_price": spot,
            "perp_mark_price": mark,
            "basis_abs": basis_abs,
            "basis_pct": basis_pct,
            "funding_rate": funding_rate,
            "next_funding_time_ms": next_funding_time,
        }
    except (KeyError, ValueError, TypeError) as e:
        log.warning("[%s] Basis parse error: %s", symbol, e)
        return {}

# ---------------------- Snapshot Assembly ----------------------
def clean_row(d: Dict[str, Any]) -> Dict[str, Any]:
    """Replace NaN/Inf with None for Parquet compatibility."""
    for k, v in list(d.items()):
        if isinstance(v, float) and (np.isnan(v) or np.isinf(v)):
            d[k] = None
    return d

def too_null_to_write(row: Dict[str, Any], min_non_null_ratio=0.45) -> bool:
    """Check if row has too many nulls to be useful."""
    vals = [v for k, v in row.items() if k not in ("symbol", "ts_ms", "iso_utc")]
    nn = sum(v is not None for v in vals)
    ratio = nn / max(1, len(vals))
    if ratio < min_non_null_ratio:
        log.warning("[%s] High-null snapshot suppressed (%.0f%% non-null)",
                    row.get("symbol"), 100 * ratio)
        return True
    return False

def make_snapshot(client, symbol, l2_depth, trades_lookback_min, now_ms_fn):
    """Assemble snapshot with feature-level error isolation."""
    epoch_ms, iso_utc = iso_now_utc_ms()
    row: Dict[str, Any] = {"symbol": symbol, "ts_ms": epoch_ms, "iso_utc": iso_utc}

    features = [
        ("ohlc", lambda: latest_ohlcv_1m(client, symbol)),
        ("l1",   lambda: l1_quote(client, symbol)),
        ("l2",   lambda: l2_features(client, symbol, l2_depth)),
        ("tr",   lambda: trades_features(client, symbol, trades_lookback_min, now_ms_fn)),
        ("bf",   lambda: basis_funding(client, symbol)),
    ]
    for name, fn in features:
        try:
            d = fn()
            row.update(d)
        except Exception as e:
            log.exception("[%s] Feature '%s' failed: %s", symbol, name, e)
            increment_stat(f"feat_fail.{symbol}.{name}")

    row = clean_row(row)
    return row

# ---------------------- Parquet Writer with Deduplication ----------------------
if HAVE_ARROW:
    SCHEMA = pa.schema([
        ("symbol", pa.string()),
        ("ts_ms", pa.int64()),
        ("iso_utc", pa.string()),
        ("ohlc_ts_open", pa.int64()),
        ("ohlc_open", pa.float64()),
        ("ohlc_high", pa.float64()),
        ("ohlc_low", pa.float64()),
        ("ohlc_close", pa.float64()),
        ("ohlc_volume", pa.float64()),
        ("ohlc_ts_close", pa.int64()),
        ("ohlc_trades", pa.int64()),
        ("ohlc_taker_base", pa.float64()),
        ("ohlc_taker_quote", pa.float64()),
        ("l1_bid", pa.float64()),
        ("l1_ask", pa.float64()),
        ("l1_mid", pa.float64()),
        ("l1_spread", pa.float64()),
        ("l1_bid_qty", pa.float64()),
        ("l1_ask_qty", pa.float64()),
        ("l1_imbalance", pa.float64()),
        ("l2_bid_depth", pa.float64()),
        ("l2_ask_depth", pa.float64()),
        ("l2_depth_asymmetry", pa.float64()),
        ("l2_bid_vwap", pa.float64()),
        ("l2_ask_vwap", pa.float64()),
        ("l2_bid_slope", pa.float64()),
        ("l2_ask_slope", pa.float64()),
        ("tr_volume_base", pa.float64()),
        ("tr_volume_quote", pa.float64()),
        ("tr_vwap", pa.float64()),
        ("tr_buy_sell_imbalance", pa.float64()),
        ("spot_price", pa.float64()),
        ("perp_mark_price", pa.float64()),
        ("basis_abs", pa.float64()),
        ("basis_pct", pa.float64()),
        ("funding_rate", pa.float64()),
        ("next_funding_time_ms", pa.int64()),
    ])

class SimpleFileLock:
    """Dependency-free file lock via atomic create."""
    def __init__(self, target: Path):
        self.lock_path = Path(str(target) + ".lock")
        self.acquired = False

    def acquire(self, timeout: float = 10.0):
        start = time.monotonic()
        while True:
            try:
                fd = os.open(self.lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
                os.close(fd)
                self.acquired = True
                return
            except FileExistsError:
                if time.monotonic() - start > timeout:
                    raise TimeoutError(f"Timeout acquiring lock {self.lock_path}")
                time.sleep(0.05)

    def release(self):
        if self.acquired:
            try:
                self.lock_path.unlink(missing_ok=True)
                self.acquired = False
            except Exception as e:
                log.warning("Lock release error: %s", e)

class ParquetShardWriter:
    """
    Hour-partitioned Parquet writer with:
    - Deduplication by (symbol, ts_ms) (persisted seed via shard tail read)
    - Batched writes to reduce I/O
    - Periodic fsync without full checkpoint
    - Atomic tmp->final swap
    """
    def __init__(self, out_dir: str):
        self.out_dir = Path(out_dir)
        self.current_path: Optional[Path] = None
        self.tmp_path: Optional[Path] = None
        self.writer = None
        self.schema = SCHEMA if HAVE_ARROW else None
        self.lock = None
        self.have_arrow = HAVE_ARROW

        # Write buffering
        self.buffer: List[List[Any]] = []
        self.batch_size = 32  # rows per write to tmp
        self.rows_since_flush = 0
        self.flush_every = 20  # checkpoint (tmp->final swap) every N writes

        # Deduplication
        self.seen_ts = set()

        # Periodic fsync without checkpoint
        self.last_fsync = time.time()
        self.fsync_interval = 30.0  # seconds

    def _target_path(self, symbol: str, ts_ms: int, iso_utc: str) -> Path:
        date_str = iso_utc[:10]
        part_dir = self.out_dir / f"date={date_str}"
        part_dir.mkdir(parents=True, exist_ok=True)
        hour_bucket = int(math.floor(ts_ms / 3_600_000))
        return part_dir / f"{symbol}_{hour_bucket}.parquet"

    def _seed_seen_from_tail(self, symbol: str):
        """Seed dedupe set from last few timestamps in existing shard."""
        if not self.current_path or not self.current_path.exists() or not self.have_arrow:
            return
        try:
            tbl = pq.read_table(self.current_path, columns=["ts_ms"])
            if tbl.num_rows == 0:
                return
            ts = tbl.column(0).to_pandas()
            tail = ts.tail(64)
            for v in tail:
                if pd.notna(v):
                    self.seen_ts.add((symbol, int(v)))
        except Exception:
            # best-effort; ignore if shard missing/corrupt
            pass

    def _rotate_if_needed(self, path: Path, symbol: str):
        if self.current_path is None or path != self.current_path:
            self._close_writer(do_checkpoint=True)
            self.current_path = path
            self.tmp_path = path.with_suffix(f".{os.getpid()}.tmp")
            self.lock = SimpleFileLock(path)
            self.rows_since_flush = 0
            self.buffer.clear()
            self.seen_ts.clear()
            self.last_fsync = time.time()
            if self.have_arrow:
                self.writer = pq.ParquetWriter(self.tmp_path, self.schema, compression="snappy")
            # seed dedupe from tail of existing final shard
            self._seed_seen_from_tail(symbol)

    def _flush_buffer(self):
        if not self.buffer:
            return
        if self.have_arrow:
            arrays = []
            for i, field in enumerate(self.schema):
                arrays.append(pa.array([row[i] for row in self.buffer], type=field.type))
            table = pa.Table.from_arrays(arrays, schema=self.schema)
            if self.writer is None:
                self.writer = pq.ParquetWriter(self.tmp_path, self.schema, compression="snappy")
            self.writer.write_table(table)
            self.buffer.clear()

    def _fsync_tmp_if_overdue(self):
        if not self.tmp_path:
            return
        if time.time() - self.last_fsync > self.fsync_interval:
            try:
                fd = os.open(self.tmp_path, os.O_RDONLY)
                try:
                    os.fsync(fd)
                finally:
                    os.close(fd)
                self.last_fsync = time.time()
            except Exception as e:
                log.debug("fsync warning: %s", e)

    def _checkpoint(self):
        """Flush buffer and atomically replace final file."""
        if not self.tmp_path or not self.current_path:
            return
        self._flush_buffer()
        self.lock.acquire(timeout=15.0)
        try:
            if self.writer:
                self.writer.close()
                self.writer = None
            try:
                fd = os.open(self.tmp_path, os.O_RDONLY)
                try:
                    os.fsync(fd)
                finally:
                    os.close(fd)
            except Exception:
                pass
            self.tmp_path.replace(self.current_path)
            if self.have_arrow:
                self.writer = pq.ParquetWriter(self.tmp_path, self.schema, compression="snappy")
            self.rows_since_flush = 0
            self.last_fsync = time.time()
        finally:
            self.lock.release()

    def _close_writer(self, do_checkpoint: bool):
        self._flush_buffer()
        if self.writer:
            try:
                self.writer.close()
            except Exception as e:
                log.warning("Writer close error: %s", e)
            self.writer = None
        if do_checkpoint and self.tmp_path and self.current_path:
            self.lock.acquire(timeout=15.0)
            try:
                try:
                    fd = os.open(self.tmp_path, os.O_RDONLY)
                    try:
                        os.fsync(fd)
                    finally:
                        os.close(fd)
                except Exception:
                    pass
                self.tmp_path.replace(self.current_path)
            except Exception as e:
                log.error("Final replace failed for %s: %s", self.current_path, e)
            finally:
                self.lock.release()

    def close(self):
        self._close_writer(do_checkpoint=True)

    def append_snapshot(self, snap: Dict[str, Any]):
        """Append snapshot with deduplication."""
        symbol = snap["symbol"]
        ts_ms = snap["ts_ms"]
        iso_utc = snap["iso_utc"]

        target = self._target_path(symbol, ts_ms, iso_utc)
        self._rotate_if_needed(target, symbol)

        # Deduplication check
        ts_key = (symbol, ts_ms)
        if ts_key in self.seen_ts:
            log.debug("[%s] Duplicate timestamp %d skipped", symbol, ts_ms)
            increment_stat(f"dedup.{symbol}")
            return
        self.seen_ts.add(ts_key)

        if self.have_arrow:
            row = [snap.get(name, None) for name in self.schema.names]
            self.buffer.append(row)
            if len(self.buffer) >= self.batch_size:
                self._flush_buffer()
                self._fsync_tmp_if_overdue()
            self.rows_since_flush += 1
            if self.rows_since_flush >= self.flush_every:
                self._checkpoint()
        else:
            # Fallback: read+concat+rewrite (slow)
            df = pd.DataFrame([snap])
            tmp = target.with_suffix(".tmp")
            if target.exists():
                try:
                    old = pd.read_parquet(target)
                    df = pd.concat([old, df], ignore_index=True)
                except Exception as e:
                    log.warning("Corrupted shard %s (%s), overwriting", target, e)
            df.to_parquet(tmp, index=False, compression="snappy")
            lock = SimpleFileLock(target)
            lock.acquire(timeout=15.0)
            try:
                tmp.replace(target)
            finally:
                lock.release()

# Thread-local writer instances
_parquet_writers: Dict[str, ParquetShardWriter] = {}
_parquet_writers_lock = threading.Lock()
def get_parquet_writer(out_dir: str) -> ParquetShardWriter:
    key = threading.current_thread().name
    with _parquet_writers_lock:
        w = _parquet_writers.get(key)
        if w is None:
            w = ParquetShardWriter(out_dir)
            _parquet_writers[key] = w
        return w

def append_snapshot_parquet(client, symbol, out_dir, l2_depth, trades_lookback_min, now_ms_fn):
    """Create and write snapshot."""
    snap = make_snapshot(client, symbol, l2_depth, trades_lookback_min, now_ms_fn)
    if not snap or too_null_to_write(snap):
        increment_stat(f"drop.null.{symbol}")
        aggregate_stats_local()
        return

    writer = get_parquet_writer(out_dir)
    writer.append_snapshot(snap)
    increment_stat(f"wrote.{symbol}")
    aggregate_stats_local()
    log.info("[%s] Snapshot @ %s -> parquet shard", symbol, snap["iso_utc"])
    log_stats_periodic()

# ---------------------- Main Logger Loop ----------------------
def run_logger(symbol, out_dir, count, interval_sec, l2_depth, trades_lookback_min):
    """Main per-symbol logging loop with local cooldown."""
    client = Client()
    client.session = build_session(pool_size=20)
    ts = TimeSync(client.session)

    i = 0
    # Stagger phase by symbol hash to avoid thundering herd
    phase = (abs(hash(symbol)) % interval_sec) / float(interval_sec)
    next_t = time.monotonic() + phase * interval_sec
    consecutive_failures = 0
    LOCAL_COOL_AFTER_FAILS = 5
    LOCAL_COOL_SECONDS = 60

    try:
        while (count == -1 or i < count) and not SHUTDOWN.is_set():
            loop_start = time.monotonic()
            try:
                append_snapshot_parquet(client, symbol, out_dir, l2_depth,
                                        trades_lookback_min, now_ms_fn=ts.now_ms)
                consecutive_failures = 0
            except Exception as e:
                consecutive_failures += 1
                log.error("[%s] ERROR: %s (fail #%d)", symbol, e, consecutive_failures)
                increment_stat(f"loop_fail.{symbol}")
                aggregate_stats_local()

                if consecutive_failures >= LOCAL_COOL_AFTER_FAILS:
                    log.error("[%s] Too many failures; local cooldown for %ds",
                              symbol, LOCAL_COOL_SECONDS)
                    for _ in range(LOCAL_COOL_SECONDS * 2):  # 0.5s chunks to notice shutdown
                        if SHUTDOWN.is_set():
                            break
                        time.sleep(0.5)
                    consecutive_failures = 0
                else:
                    for _ in range(20):  # 10s sleep in 0.5s steps
                        if SHUTDOWN.is_set():
                            break
                        time.sleep(0.5)
                next_t = time.monotonic()
                continue

            i += 1
            elapsed = time.monotonic() - loop_start
            next_t += interval_sec

            # Reset if we're falling behind
            if elapsed > interval_sec * 1.2:
                log.warning("[%s] Slow iteration (%.1fs > %.1fs), resetting schedule",
                            symbol, elapsed, interval_sec)
                next_t = time.monotonic()

            if count != -1 and i >= count:
                break

            # Sleep with jitter, but wake early on shutdown
            sleep_for = max(0.0, next_t - time.monotonic()) + random.uniform(0, 1.0)
            log.info("[%s] (%d/%s) sleeping %.1fs…",
                     symbol, i, count if count != -1 else "∞", sleep_for)
            end_sleep = time.monotonic() + sleep_for
            while time.monotonic() < end_sleep:
                if SHUTDOWN.is_set():
                    break
                time.sleep(0.25)
    finally:
        aggregate_stats_local()

# ---------------------- Graceful Shutdown ----------------------
def close_all_writers():
    """Close all Parquet writers and flush buffers."""
    aggregate_stats_local()  # push last stats
    with _parquet_writers_lock:
        for w in _parquet_writers.values():
            try:
                w.close()
            except Exception as e:
                log.warning("Error closing writer: %s", e)
    log.info("All writers closed")

def _shutdown_handler(signum, frame):
    log.info("Signal %s received. Shutting down gracefully…", signum)
    SHUTDOWN.set()

# ---------------------- Entry Point ----------------------
if __name__ == "__main__":
    # Configuration
    SYMBOLS = [
        "BTCUSDT", "ETHUSDT", "BNBUSDT", "XRPUSDT", "SOLUSDT",
        "ADAUSDT", "MATICUSDT", "TRXUSDT", "DOGEUSDT",
    ]
    OUT_DIR = "parquet_dataset"
    COUNT = -1  # -1 for infinite
    INTERVAL_SEC = 60
    L2_DEPTH = 20
    TRADES_LOOKBACK_MIN = 5

    log.info("=" * 70)
    log.info("Binance Market Data Logger v2")
    log.info("=" * 70)
    log.info("PyArrow available: %s", HAVE_ARROW)
    log.info("Symbols: %d", len(SYMBOLS))
    log.info("Output directory: %s", OUT_DIR)
    log.info("Interval: %ds", INTERVAL_SEC)
    log.info("L2 depth: %d", L2_DEPTH)
    log.info("Trades lookback: %d minutes", TRADES_LOOKBACK_MIN)
    log.info("=" * 70)

    if not HAVE_ARROW:
        log.warning("PyArrow not available - using slow fallback path!")
        log.warning("Install with: pip install pyarrow")

    # Validate symbol format
    validate_symbol_format(SYMBOLS)

    # Setup graceful shutdown
    import atexit
    atexit.register(close_all_writers)
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            signal.signal(sig, _shutdown_handler)
        except Exception:
            pass

    # Launch threads
    threads = []
    try:
        for i, sym in enumerate(SYMBOLS, 1):
            t = threading.Thread(
                target=run_logger,
                name=f"logger-{sym}",
                args=(sym, OUT_DIR, COUNT, INTERVAL_SEC, L2_DEPTH, TRADES_LOOKBACK_MIN),
                daemon=False,
            )
            t.start()
            threads.append(t)
            log.info("[%d/%d] Launched thread for %s", i, len(SYMBOLS), sym)
            time.sleep(1.0)  # stagger startup

        log.info("All threads launched. Press Ctrl+C to stop gracefully.")

        # Main monitoring loop
        while any(t.is_alive() for t in threads):
            for t in threads:
                t.join(timeout=1.0)
            # Periodic global stats print from main too
            log_stats_periodic(period_s=120)

    except KeyboardInterrupt:
        log.info("Keyboard interrupt received. Stopping…")

    finally:
        SHUTDOWN.set()
        log.info("Waiting for threads to finish…")
        for t in threads:
            t.join(timeout=15.0)
        close_all_writers()
        with _GLOBAL_STATS_LOCK:
            if _GLOBAL_STATS:
                log.info("=== FINAL STATISTICS ===")
                for k, v in _GLOBAL_STATS.most_common(30):
                    log.info("  %s: %d", k, v)
        log.info("Shutdown complete.")