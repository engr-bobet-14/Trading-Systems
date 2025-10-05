#!/usr/bin/env python3
"""
Robust Binance Spot+Futures snapshot logger (Parquet, partitioned)
(Rev: implements continuous token bucket, stronger pagination, half-open circuit breaker,
write buffering+fsync, staggered cadences, null-row guardrails, and observability.)

Notes:
- pyarrow strongly recommended (fallback kept but slower).
- Endpoint weights remain illustrative; replace with doc-accurate weights if needed.
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

# ---------------------- Logging & Obs ----------------------

def setup_logging() -> logging.Logger:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(threadName)s %(levelname)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    return logging.getLogger(__name__)

log = setup_logging()
STATS = collections.Counter()
STATS_LOCK = threading.Lock()
LAST_STATS_LOG = [time.monotonic()]

def log_stats_periodic(period_s=120):
    now = time.monotonic()
    if now - LAST_STATS_LOG[0] < period_s:
        return
    LAST_STATS_LOG[0] = now
    with STATS_LOCK:
        if not STATS:
            return
        # emit a compact view
        top = ", ".join(f"{k}={v}" for k, v in list(STATS.items())[:12])
        log.info("STATS %s ...", top)

# ---------------------- Time helpers ----------------------

def iso_now_utc_ms():
    now = datetime.now(timezone.utc)
    return int(now.timestamp() * 1000), now.isoformat()

class TimeSync:
    """Tracks server-local time offset to avoid drift for time-windowed API calls."""
    def __init__(self, session: requests.Session, refresh_s: int = 300):
        self.session = session
        self.refresh_s = refresh_s
        self.offset_ms = 0
        self.last_sync = 0.0

    def _sync(self):
        try:
            r = self.session.get("https://api.binance.com/api/v3/time", timeout=5)
            r.raise_for_status()
            server_ms = int(r.json()["serverTime"])
            local_ms = int(time.time() * 1000)
            self.offset_ms = server_ms - local_ms
            self.last_sync = time.time()
            log.info("Server time sync: offset %+d ms", self.offset_ms)
        except Exception as e:
            log.warning("Failed to sync server time: %s", e)

    def now_ms(self) -> int:
        if time.time() - self.last_sync > self.refresh_s:
            self._sync()
        return int(time.time() * 1000 + self.offset_ms)

# ---------------------- Math helpers ----------------------

def vwap_side(levels: List[tuple]) -> float:
    if not levels:
        return float("nan")
    px = np.array([p for p, _ in levels], dtype=float)
    qty = np.array([q for _, q in levels], dtype=float)
    denom = qty.sum()
    return float((px * qty).sum() / denom) if denom > 1e-12 else float("nan")

def slope_price_vs_cumqty(levels: List[tuple]) -> float:
    if not levels:
        return float("nan")
    qtys = np.cumsum([q for _, q in levels])
    if len(levels) < 2 or qtys[-1] == 0:
        return float("nan")
    prices = np.array([p for p, _ in levels], dtype=float)
    if np.unique(qtys).size < 2:
        return float("nan")
    return float(np.polyfit(qtys, prices, 1)[0])

# ---------------------- HTTP session ----------------------

def build_session() -> requests.Session:
    s = requests.Session()
    retry = Retry(total=0, connect=0, read=0, backoff_factor=0)
    adapter = HTTPAdapter(max_retries=retry, pool_connections=100, pool_maxsize=100)
    s.mount("https://", adapter)
    s.headers.update({'Connection': 'keep-alive', 'Accept': 'application/json'})
    return s

# ---------------------- Token buckets & weights ----------------------

class TokenBucket:
    """Continuous refill token bucket to smooth bursts."""
    def __init__(self, rpm: int):
        self.rate = max(1.0, rpm) / 60.0  # tokens per second
        self.capacity = max(1.0, rpm)
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
                # sleep until enough tokens accumulate
                need = n - self.tokens
                time.sleep(max(0.005, need / self.rate))

# conservative defaults; tune as needed per account/IP limits
BUCKETS: Dict[str, TokenBucket] = {
    "api": TokenBucket(1000),   # api.binance.com (spot)
    "fapi": TokenBucket(1000),  # fapi.binance.com (futures)
}

# Per-endpoint weights (illustrative; replace with doc-accurate numbers if you have them)
def _weight_get_order_book(limit: int = 20, **_) -> int:
    limit = int(limit or 20)
    if limit <= 20:  return 2
    if limit <= 50:  return 10
    return 50  # 100 depth

def _weight_klines(limit: int = 100, **_) -> int:
    # Placeholder example: keep cheap unless very large pages
    return 1 if int(limit or 100) <= 100 else 5

WEIGHTS = {
    "get_orderbook_ticker": lambda **_: 1,
    "get_order_book": _weight_get_order_book,
    "get_klines": _weight_klines,
    "get_aggregate_trades": lambda **_: 1,
    "get_symbol_ticker": lambda **_: 1,
    "futures_mark_price": lambda **_: 1,
}

# light per-endpoint min spacing to reduce micro-bursts
MIN_SPACING: Dict[Tuple[str, str], float] = {
    ("api", "get_order_book"): 0.05,
    ("api", "get_klines"): 0.02,
    ("api", "get_aggregate_trades"): 0.02,
    ("api", "get_orderbook_ticker"): 0.01,
    ("api", "get_symbol_ticker"): 0.01,
    ("fapi", "futures_mark_price"): 0.01,
}
LAST_CALL_TS = {"api": collections.defaultdict(lambda: 0.0),
                "fapi": collections.defaultdict(lambda: 0.0)}

def _host_for(func) -> str:
    # naive: futures endpoints on fapi, others on api
    name = getattr(func, "__name__", "")
    return "fapi" if "futures" in name else "api"

def _tokens_for_call(func, **params) -> int:
    name = getattr(func, "__name__", "")
    w = WEIGHTS.get(name, lambda **_: 1)
    try:
        return int(w(**params))
    except Exception:
        return 1

# ---------------------- Circuit breaker (half-open) ----------------------

class Circuit:
    def __init__(self):
        self.state = "closed"
        self.until = 0.0
        self.lock = threading.Lock()

    def open(self, secs: float):
        with self.lock:
            self.state = "open"
            self.until = max(self.until, time.time() + secs)

    def allow(self) -> bool:
        with self.lock:
            if self.state == "open" and time.time() >= self.until:
                self.state = "half"
                return True
            return self.state != "open"

    def on_success(self):
        with self.lock:
            self.state = "closed"
            self.until = 0.0

    def on_failure(self, min_secs: float = 30.0):
        with self.lock:
            self.state = "open"
            self.until = time.time() + min_secs

CIRCUITS: Dict[str, Circuit] = {"api": Circuit(), "fapi": Circuit()}

# ---------------------- Retry logic ----------------------

RETRYABLE_HTTP = {429, 500, 502, 503, 504}
BAN_HTTP = {418}

def is_retryable(exc: Exception) -> bool:
    if isinstance(exc, (Timeout, ReqConnErr, BinanceRequestException)):
        return True
    if isinstance(exc, BinanceAPIException):
        if exc.status_code in RETRYABLE_HTTP or exc.status_code in BAN_HTTP:
            return True
        return False
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

def _classify_backoff(exc: Exception) -> tuple[float, float]:
    if isinstance(exc, BinanceAPIException):
        sc = exc.status_code
        if sc == 418:
            return (1800.0, 10.0)  # up to 30 min cap
        if sc == 429:
            return (120.0, 2.0)   # higher cap
        if sc in (500, 502, 503, 504):
            return (12.0, 0.75)
    if isinstance(exc, (Timeout, ReqConnErr, BinanceRequestException)):
        return (12.0, 0.75)
    return (6.0, 0.5)

def safe_call(func, *args, retries=5, base_delay=0.75, timeout=10.0, **kwargs):
    """
    Full-jitter exponential backoff + per-host token buckets + host-scoped half-open circuit breaker.
    Applies light per-endpoint minimum spacing to smooth bursts.
    """
    k = dict(kwargs)
    rp = k.get('requests_params', {})
    rp.setdefault('timeout', timeout)
    k['requests_params'] = rp

    host = _host_for(func)
    fname = getattr(func, "__name__", "")
    tokens_cost = _tokens_for_call(func, **kwargs)

    last_exc = None
    for i in range(retries):
        # circuit gating (half-open allows single probe)
        while not CIRCUITS[host].allow():
            time.sleep(0.25)

        # min-spacing enforcement
        last = LAST_CALL_TS[host][fname]
        gap = MIN_SPACING.get((host, fname), 0.0) - (time.monotonic() - last)
        if gap > 0:
            time.sleep(gap)

        # rate limiting
        BUCKETS[host].acquire(tokens_cost)

        try:
            t0 = time.time()
            res = func(*args, **k)
            dt = (time.time() - t0) * 1000
            log.debug("OK %s host=%s weight=%d latency_ms=%.1f", fname, host, tokens_cost, dt)
            LAST_CALL_TS[host][fname] = time.monotonic()
            CIRCUITS[host].on_success()
            with STATS_LOCK:
                STATS[f"ok.{host}.{fname}"] += 1
            return res
        except Exception as exc:
            last_exc = exc
            with STATS_LOCK:
                key = f"err.{host}.{fname}.{getattr(exc, 'status_code', 'x')}"
                STATS[key] += 1

            if not is_retryable(exc):
                log.error("%s non-retryable: %s", fname, exc)
                CIRCUITS[host].on_failure(30.0)
                break

            wait_hdr = get_retry_after_seconds(exc)
            max_cap, base = _classify_backoff(exc)
            expo = base * (2 ** i)
            jitter = random.uniform(0, expo)
            delay = min((wait_hdr if wait_hdr is not None else expo) + jitter, max_cap)

            if isinstance(exc, BinanceAPIException) and exc.status_code in (418, 429):
                CIRCUITS[host].open(delay)

            log.warning("%s retry %d/%d host=%s: %s | sleeping %.2fs",
                        fname, i + 1, retries, host, exc, delay)
            time.sleep(delay)
    log.error("%s failed after %d retries (%s)", fname, retries, last_exc)
    return None

# ---------------------- Binance pulls ----------------------

def latest_ohlcv_1m(client, symbol):
    kl = safe_call(client.get_klines, symbol=symbol,
                   interval=Client.KLINE_INTERVAL_1MINUTE, limit=2, timeout=6.0)
    if not kl or len(kl) < 2:
        return {}
    k = kl[-2]
    # sanity: monotonic & non-negative volumes
    try:
        o, h, l, c = map(float, (k[1], k[2], k[3], k[4]))
        v = float(k[5])
        if not (l <= min(o, c) <= h and l <= max(o, c) <= h and v >= 0):
            return {}
    except Exception:
        return {}
    return {
        "ohlc_ts_open": int(k[0]),
        "ohlc_open": float(k[1]),
        "ohlc_high": float(k[2]),
        "ohlc_low": float(k[3]),
        "ohlc_close": float(k[4]),
        "ohlc_volume": float(k[5]),
        "ohlc_ts_close": int(k[6]),
        "ohlc_trades": int(k[8]),
        "ohlc_taker_base": float(k[9]),
        "ohlc_taker_quote": float(k[10]),
    }

def l1_quote(client, symbol):
    t = safe_call(client.get_orderbook_ticker, symbol=symbol, timeout=4.0)
    if not t:
        return {}
    bid, ask = float(t["bidPrice"]), float(t["askPrice"])
    bq, aq = float(t["bidQty"]), float(t["askQty"])
    if bid <= 0 or ask <= 0 or ask < bid:
        return {}
    mid = 0.5 * (bid + ask)
    spread = ask - bid
    imb = (bq - aq) / (bq + aq) if (bq + aq) > 0 else float("nan")
    return {
        "l1_bid": bid, "l1_ask": ask, "l1_mid": mid, "l1_spread": spread,
        "l1_bid_qty": bq, "l1_ask_qty": aq, "l1_imbalance": imb,
    }

def l2_features(client, symbol, depth):
    depth = int(max(5, min(depth, 100)))
    ob = safe_call(client.get_order_book, symbol=symbol, limit=depth, timeout=4.0)
    if not ob:
        return {}
    bids = [(float(p), float(q)) for p, q in ob["bids"][:depth]]
    asks = [(float(p), float(q)) for p, q in ob["asks"][:depth]]
    if not bids or not asks:
        return {}

    # filter non-positive qty and negative prices
    bids = [(p, q) for p, q in bids if p > 0 and q > 0]
    asks = [(p, q) for p, q in asks if p > 0 and q > 0]
    if not bids or not asks:
        return {}

    bid_depth = float(sum(q for _, q in bids))
    ask_depth = float(sum(q for _, q in asks))
    denom = bid_depth + ask_depth
    depth_asym = (bid_depth - ask_depth) / denom if denom > 1e-12 else float("nan")
    return {
        "l2_bid_depth": bid_depth,
        "l2_ask_depth": ask_depth,
        "l2_depth_asymmetry": depth_asym,
        "l2_bid_vwap": vwap_side(bids),
        "l2_ask_vwap": vwap_side(asks),
        "l2_bid_slope": slope_price_vs_cumqty(bids),
        "l2_ask_slope": slope_price_vs_cumqty(asks),
    }

def trades_features(client, symbol, lookback_minutes, now_ms_fn=lambda: int(time.time() * 1000)):
    """Time-window first page, then continue via fromId until end_ms crossed or guards hit; dedupe by (a, T)."""
    end_ms = (now_ms_fn() // 60_000) * 60_000
    start_ms = end_ms - lookback_minutes * 60_000
    all_trades, from_id = [], None
    pages, MAX_PAGES = 0, 120
    start_wall = time.monotonic()
    MAX_WALL = 12.0

    page_args = {"symbol": symbol, "startTime": start_ms, "endTime": end_ms}

    while True:
        batch = safe_call(client.get_aggregate_trades, timeout=8.0, **page_args)
        if not batch:
            break

        pages += 1
        all_trades.extend(batch)
        last = batch[-1]
        reached_end = last.get("T", 0) >= end_ms

        if reached_end:
            break
        # If exactly 1000, there may be more; continue by fromId
        if len(batch) == 1000:
            from_id = last["a"] + 1
            page_args = {"symbol": symbol, "fromId": from_id}
        else:
            # short page before end could be transient; guard by wall/pages
            if pages >= MAX_PAGES or (time.monotonic() - start_wall) > MAX_WALL:
                break
            # otherwise continue by fromId if we have one
            if "fromId" not in page_args:
                from_id = last["a"] + 1
                page_args = {"symbol": symbol, "fromId": from_id}
            else:
                break

    if not all_trades:
        return {"tr_volume_base": 0.0, "tr_volume_quote": 0.0, "tr_vwap": float("nan"), "tr_buy_sell_imbalance": float("nan")}

    seen, uniq = set(), []
    for t in all_trades:
        k = (t.get("a"), t.get("T"))
        if k not in seen:
            seen.add(k)
            uniq.append(t)
    all_trades = uniq

    px = np.array([float(t["p"]) for t in all_trades], dtype=float)
    qty = np.array([float(t["q"]) for t in all_trades], dtype=float)
    vol_base = float(qty.sum())
    vol_quote = float((qty * px).sum())
    vwap = vol_quote / vol_base if vol_base > 1e-12 else float("nan")
    m = np.array([bool(t["m"]) for t in all_trades])
    buy_vol = float(qty[~m].sum())
    sell_vol = float(qty[m].sum())
    imb = (buy_vol - sell_vol) / (buy_vol + sell_vol) if (buy_vol + sell_vol) > 1e-12 else float("nan")
    return {"tr_volume_base": vol_base, "tr_volume_quote": vol_quote, "tr_vwap": vwap, "tr_buy_sell_imbalance": imb}

def basis_funding(client, symbol):
    spot_info = safe_call(client.get_symbol_ticker, symbol=symbol, timeout=4.0)
    mark_info = safe_call(client.futures_mark_price, symbol=symbol, timeout=4.0)
    if not spot_info or not mark_info:
        return {}
    spot = float(spot_info["price"])
    mark = float(mark_info["markPrice"])
    if spot <= 0 or mark <= 0:
        return {}
    funding_rate = float(mark_info.get("lastFundingRate", 0.0) or 0.0)
    next_funding_time = int(mark_info.get("nextFundingTime", 0) or 0)
    basis_abs = mark - spot
    basis_pct = basis_abs / spot if spot > 0 else float("nan")
    return {"spot_price": spot, "perp_mark_price": mark, "basis_abs": basis_abs,
            "basis_pct": basis_pct, "funding_rate": funding_rate, "next_funding_time_ms": next_funding_time}

# ---------------------- Snapshot + Parquet ----------------------

def clean_row(d: Dict[str, Any]) -> Dict[str, Any]:
    for k, v in list(d.items()):
        if isinstance(v, float) and (np.isnan(v) or np.isinf(v)):
            d[k] = None
    return d

def too_null_to_write(row: Dict[str, Any], min_non_null_ratio=0.45) -> bool:
    vals = [v for k, v in row.items() if k not in ("symbol", "ts_ms", "iso_utc")]
    nn = sum(v is not None for v in vals)
    ratio = nn / max(1, len(vals))
    if ratio < min_non_null_ratio:
        log.warning("[%s] high-null snapshot suppressed (%.0f%% non-null)",
                    row.get("symbol"), 100 * ratio)
        return True
    return False

class SimpleFileLock:
    """Lock via atomic lockfile create; dep-free."""
    def __init__(self, target: Path):
        self.lock_path = Path(str(target) + ".lock")
    def acquire(self, timeout: float = 10.0):
        start = time.monotonic()
        while True:
            try:
                fd = os.open(self.lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
                os.close(fd)
                return
            except FileExistsError:
                if time.monotonic() - start > timeout:
                    raise TimeoutError(f"Timeout acquiring file lock {self.lock_path}")
                time.sleep(0.05)
    def release(self):
        try:
            self.lock_path.unlink(missing_ok=True)
        except Exception:
            pass

# CHANGED: explicit Arrow schema to avoid dtype drift
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

class ParquetShardWriter:
    """
    Keeps a ParquetWriter open for current shard (symbol-hour) and rotates on hour change.
    Arrow path: batch rows in-memory, write in groups, then atomically swap tmp->final.
    Lock only around tmp->final replace (short critical section). fsync before rename.
    """
    def __init__(self, out_dir: str):
        self.out_dir = Path(out_dir)
        self.current_path: Optional[Path] = None
        self.tmp_path: Optional[Path] = None
        self.writer = None
        self.schema = SCHEMA if HAVE_ARROW else None
        self.lock = None
        self.have_arrow = HAVE_ARROW
        # buffering to reduce parquet footer churn
        self.buffer = []
        self.batch_size = 32  # rows per write
        self.rows_since_flush = 0
        self.flush_every = 10  # checkpoints (tmp->final) every N writes

    def _target_path(self, symbol: str, ts_ms: int, iso_utc: str) -> Path:
        date_str = iso_utc[:10]
        part_dir = self.out_dir / f"date={date_str}"
        part_dir.mkdir(parents=True, exist_ok=True)
        hour_bucket = int(math.floor(ts_ms / 3_600_000))
        return part_dir / f"{symbol}_{hour_bucket}.parquet"

    def _rotate_if_needed(self, path: Path):
        if self.current_path is None or path != self.current_path:
            self._close_writer(do_checkpoint=True)
            self.current_path = path
            self.tmp_path = path.with_suffix(f".{os.getpid()}.tmp")
            self.lock = SimpleFileLock(path)
            self.rows_since_flush = 0
            self.buffer.clear()
            if self.have_arrow:
                self.writer = pq.ParquetWriter(self.tmp_path, self.schema, compression="snappy")

    def _flush_buffer(self):
        if not self.buffer:
            return
        if self.have_arrow:
            arrays = []
            for i, name in enumerate(self.schema.names):
                field = self.schema.field(i)
                arrays.append(pa.array([row[i] for row in self.buffer], type=field.type))
            table = pa.Table.from_arrays(arrays, names=self.schema.names)
            if self.writer is None:
                self.writer = pq.ParquetWriter(self.tmp_path, self.schema, compression="snappy")
            self.writer.write_table(table)
            self.buffer.clear()
            self._fsync_tmp()

    def _fsync_tmp(self):
        try:
            fd = os.open(self.tmp_path, os.O_RDONLY)
            try:
                os.fsync(fd)
            finally:
                os.close(fd)
        except Exception:
            pass

    def _checkpoint(self):
        if not self.tmp_path or not self.current_path:
            return
        # ensure buffered rows are flushed first
        self._flush_buffer()
        self.lock.acquire(timeout=15.0)
        try:
            if self.writer:
                self.writer.close()
                self.writer = None
            try:
                self.tmp_path.replace(self.current_path)
            except FileNotFoundError:
                pass
            if self.have_arrow:
                self.writer = pq.ParquetWriter(self.tmp_path, self.schema, compression="snappy")
            self.rows_since_flush = 0
        finally:
            self.lock.release()

    def _close_writer(self, do_checkpoint: bool):
        # final flush
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
                    self.tmp_path.replace(self.current_path)
                except Exception as e:
                    log.error("Replace failed for %s: %s", self.current_path, e)
            finally:
                self.lock.release()

    def close(self):
        self._close_writer(do_checkpoint=True)

    def append_snapshot(self, snap: Dict[str, Any]):
        symbol = snap["symbol"]
        ts_ms = snap["ts_ms"]
        iso_utc = snap["iso_utc"]
        target = self._target_path(symbol, ts_ms, iso_utc)
        self._rotate_if_needed(target)

        if self.have_arrow:
            # pack row respecting schema order
            row = [snap.get(name, None) for name in self.schema.names]
            self.buffer.append(row)
            if len(self.buffer) >= self.batch_size:
                self._flush_buffer()
            self.rows_since_flush += 1
            if self.rows_since_flush >= self.flush_every:
                self._checkpoint()
        else:
            # Fallback: read+concat+rewrite (slower). Lock only around replace.
            df = pd.DataFrame([snap])
            tmp = target.with_suffix(".tmp")
            if target.exists():
                try:
                    old = pd.read_parquet(target)
                    df = pd.concat([old, df], ignore_index=True)
                except Exception as e:
                    log.warning("Corrupted shard %s (%s), overwriting.", target, e)
            df.to_parquet(tmp, index=False, compression="snappy")
            lock = SimpleFileLock(target)
            lock.acquire(timeout=15.0)
            try:
                tmp.replace(target)
            finally:
                lock.release()

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

def make_snapshot(client, symbol, l2_depth, trades_lookback_min, now_ms_fn):
    epoch_ms, iso_utc = iso_now_utc_ms()
    row = {"symbol": symbol, "ts_ms": epoch_ms, "iso_utc": iso_utc}

    # Feature assembly with isolation
    parts = []
    for name, fn in (
        ("ohlc", lambda: latest_ohlcv_1m(client, symbol)),
        ("l1",   lambda: l1_quote(client, symbol)),
        ("l2",   lambda: l2_features(client, symbol, l2_depth)),
        ("tr",   lambda: trades_features(client, symbol, trades_lookback_min, now_ms_fn=now_ms_fn)),
        ("bf",   lambda: basis_funding(client, symbol)),
    ):
        try:
            d = fn()
            parts.append(d)
        except Exception as e:
            log.exception("[%s] feature %s failed: %s", symbol, name, e)

    for d in parts:
        row.update(d)

    row = clean_row(row)
    return row

def append_snapshot_parquet(client, symbol, out_dir, l2_depth, trades_lookback_min, now_ms_fn):
    snap = make_snapshot(client, symbol, l2_depth, trades_lookback_min, now_ms_fn=now_ms_fn)
    if not snap or too_null_to_write(snap):
        with STATS_LOCK:
            STATS[f"drop.null.{symbol}"] += 1
        return
    writer = get_parquet_writer(out_dir)
    writer.append_snapshot(snap)
    with STATS_LOCK:
        STATS[f"wrote.{symbol}"] += 1
    log.info("[%s] Snapshot @ %s -> parquet shard", symbol, snap["iso_utc"])
    log_stats_periodic()

# ---------------------- Main logger loop ----------------------

def run_logger(symbol, out_dir, count, interval_sec, l2_depth, trades_lookback_min):
    client = Client()
    client.session = build_session()
    ts = TimeSync(client.session)

    i = 0
    # stagger phase by symbol hash to avoid bursts
    phase = (abs(hash(symbol)) % interval_sec) / float(interval_sec)
    next_t = time.monotonic() + phase * interval_sec
    consecutive_failures = 0
    LOCAL_COOL_AFTER_FAILS = 5
    LOCAL_COOL_SECONDS = 60  # local thread cooldown only

    while True if count == -1 else i < count:
        loop_start = time.monotonic()
        try:
            append_snapshot_parquet(client, symbol, out_dir, l2_depth, trades_lookback_min, now_ms_fn=ts.now_ms)
            consecutive_failures = 0
        except Exception as e:
            consecutive_failures += 1
            log.error("[%s] ERROR: %s (fail %d)", symbol, e, consecutive_failures)
            if consecutive_failures >= LOCAL_COOL_AFTER_FAILS:
                log.error("[%s] Too many failures; local cooldown for %ds", symbol, LOCAL_COOL_SECONDS)
                time.sleep(LOCAL_COOL_SECONDS)
                consecutive_failures = 0
            else:
                time.sleep(10)
            next_t = time.monotonic()
            continue

        i += 1
        elapsed = time.monotonic() - loop_start
        next_t += interval_sec
        if elapsed > interval_sec * 1.2:
            next_t = time.monotonic()

        if count != -1 and i >= count:
            break

        sleep_for = max(0.0, next_t - time.monotonic()) + random.uniform(0, 1.0)
        log.info("[%s] (%s/%s) sleeping %.1fs …", symbol, i, count if count != -1 else "∞", sleep_for)
        time.sleep(sleep_for)

# ---------------------- Entrypoint + graceful shutdown ----------------------

def close_all_writers():
    with _parquet_writers_lock:
        for w in _parquet_writers.values():
            try:
                w.close()
            except Exception as e:
                log.warning("Error closing writer: %s", e)

def _shutdown_handler(signum, frame):
    log.info("Signal %s received. Shutting down…", signum)
    close_all_writers()

if __name__ == "__main__":
    SYMBOLS = ["BTCUSDT", "ETHUSDT", "BNBUSDT", "XRPUSDT", "SOLUSDT",
               "ADAUSDT", "MATICUSDT", "TRXUSDT", "DOGEUSDT"]
    OUT_DIR = "parquet_dataset"
    COUNT = -1
    INTERVAL_SEC = 60
    L2_DEPTH = 20
    TRADES_LOOKBACK_MIN = 5

    log.info("Launching %d symbol loggers… (pyarrow=%s)", len(SYMBOLS), HAVE_ARROW)
    if not HAVE_ARROW:
        log.warning("pyarrow not found: fallback path is O(N) rewrite and not recommended for production.")

    # graceful shutdown hooks
    import atexit
    atexit.register(close_all_writers)
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            signal.signal(sig, _shutdown_handler)
        except Exception:
            pass

    threads = []
    try:
        for i, sym in enumerate(SYMBOLS, 1):
            t = threading.Thread(
                target=run_logger,
                name=f"logger-{sym}",
                args=(sym, OUT_DIR, COUNT, INTERVAL_SEC, L2_DEPTH, TRADES_LOOKBACK_MIN),
                daemon=False,  # ensure clean shutdown
            )
            t.start()
            threads.append(t)
            log.info("[%d/%d] launched %s", i, len(SYMBOLS), sym)
            time.sleep(1.0)  # modest stagger on launch

        while any(t.is_alive() for t in threads):
            for t in threads:
                t.join(timeout=1.0)
    except KeyboardInterrupt:
        log.info("Stop requested (Ctrl+C). Exiting…")
    finally:
        close_all_writers()