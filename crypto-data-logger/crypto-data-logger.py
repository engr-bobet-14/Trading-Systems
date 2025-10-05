#!/usr/bin/env python3
"""
Partitioned Parquet logger for Binance features (robust version)
Adds retry/backoff, per-thread client, and persistent connections.
"""

import time
import random
from pathlib import Path
from datetime import datetime, timezone
import threading
import numpy as np
import pandas as pd
import requests
from binance.client import Client


# ---------------------- Time/session helpers ----------------------

def iso_now_utc_ms():
    now = datetime.now(timezone.utc)
    return int(now.timestamp() * 1000), now.isoformat()

def vwap_side(levels):
    if not levels:
        return float("nan")
    px = np.array([p for p, _ in levels], dtype=float)
    qty = np.array([q for _, q in levels], dtype=float)
    denom = qty.sum()
    return float((px * qty).sum() / denom) if denom > 0 else float("nan")

def slope_price_vs_cumqty(levels):
    if not levels:
        return float("nan")
    qtys = np.cumsum([q for _, q in levels])
    if len(levels) < 2 or qtys[-1] == 0:
        return float("nan")
    prices = np.array([p for p, _ in levels], dtype=float)
    return float(np.polyfit(qtys, prices, 1)[0])


# ---------------------- Safe API wrapper ----------------------

def safe_call(func, *args, retries=3, delay=3, **kwargs):
    for i in range(retries):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            print(f"[WARN] {func.__name__} failed ({e}) attempt {i+1}/{retries}")
            time.sleep(delay * (2 ** i))
    print(f"[ERROR] {func.__name__} failed after {retries} retries — skipping.")
    return None


# ---------------------- Binance pulls ----------------------

def latest_ohlcv_1m(client, symbol):
    kl = safe_call(client.get_klines, symbol=symbol.upper(),
                   interval=Client.KLINE_INTERVAL_1MINUTE, limit=1)
    if not kl:
        return {}
    k = kl[0]
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
    t = safe_call(client.get_orderbook_ticker, symbol=symbol.upper())
    if not t:
        return {}
    bid, ask = float(t["bidPrice"]), float(t["askPrice"])
    bq, aq = float(t["bidQty"]), float(t["askQty"])
    mid = 0.5 * (bid + ask)
    spread = ask - bid
    imb = (bq - aq) / (bq + aq) if (bq + aq) > 0 else float("nan")
    return {
        "l1_bid": bid, "l1_ask": ask, "l1_mid": mid, "l1_spread": spread,
        "l1_bid_qty": bq, "l1_ask_qty": aq, "l1_imbalance": imb,
    }

def l2_features(client, symbol, depth):
    depth = min(max(depth, 5), 5000)
    ob = safe_call(client.get_order_book, symbol=symbol.upper(), limit=depth)
    if not ob:
        return {}
    bids = [(float(p), float(q)) for p, q in ob["bids"][:depth]]
    asks = [(float(p), float(q)) for p, q in ob["asks"][:depth]]

    bid_depth = float(sum(q for _, q in bids))
    ask_depth = float(sum(q for _, q in asks))
    depth_asym = (bid_depth - ask_depth) / (bid_depth + ask_depth) if (bid_depth + ask_depth) > 0 else float("nan")

    return {
        "l2_bid_depth": bid_depth,
        "l2_ask_depth": ask_depth,
        "l2_depth_asymmetry": depth_asym,
        "l2_bid_vwap": vwap_side(bids),
        "l2_ask_vwap": vwap_side(asks),
        "l2_bid_slope": slope_price_vs_cumqty(bids),
        "l2_ask_slope": slope_price_vs_cumqty(asks),
    }

def trades_features(client, symbol, lookback_minutes):
    end_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    start_ms = end_ms - lookback_minutes * 60_000
    trades = safe_call(client.get_aggregate_trades, symbol=symbol.upper(),
                       startTime=start_ms, endTime=end_ms)
    if not trades:
        return {"tr_volume_base": 0.0, "tr_volume_quote": 0.0,
                "tr_vwap": float("nan"), "tr_buy_sell_imbalance": float("nan")}
    px = np.array([float(t["p"]) for t in trades], dtype=float)
    qty = np.array([float(t["q"]) for t in trades], dtype=float)
    vol_base = float(qty.sum())
    vol_quote = float((qty * px).sum())
    v = float(vol_quote / vol_base) if vol_base > 0 else float("nan")
    m = np.array([bool(t["m"]) for t in trades])
    buy_vol = float(qty[~m].sum())
    sell_vol = float(qty[m].sum())
    imb = float((buy_vol - sell_vol) / (buy_vol + sell_vol)) if (buy_vol + sell_vol) > 0 else float("nan")
    return {
        "tr_volume_base": vol_base,
        "tr_volume_quote": vol_quote,
        "tr_vwap": v,
        "tr_buy_sell_imbalance": imb,
    }

def basis_funding(client, symbol):
    spot_info = safe_call(client.get_symbol_ticker, symbol=symbol.upper())
    mark_info = safe_call(client.futures_mark_price, symbol=symbol.upper())
    if not spot_info or not mark_info:
        return {}
    spot = float(spot_info["price"])
    mark = float(mark_info["markPrice"])
    funding_rate = float(mark_info.get("lastFundingRate", 0.0))
    next_funding_time = int(mark_info.get("nextFundingTime", 0))
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


# ---------------------- Snapshot + Parquet ----------------------

def make_snapshot(client, symbol, l2_depth, trades_lookback_min):
    epoch_ms, iso_utc = iso_now_utc_ms()
    row = {"symbol": symbol.upper(), "ts_ms": epoch_ms, "iso_utc": iso_utc}
    row.update(latest_ohlcv_1m(client, symbol))
    row.update(l1_quote(client, symbol))
    row.update(l2_features(client, symbol, l2_depth))
    row.update(trades_features(client, symbol, trades_lookback_min))
    row.update(basis_funding(client, symbol))
    return row

def append_snapshot_parquet_partitioned(client, symbol, out_dir, l2_depth, trades_lookback_min):
    snap = make_snapshot(client, symbol, l2_depth, trades_lookback_min)
    if not snap:
        return
    df = pd.DataFrame([snap])
    date_str = snap["iso_utc"][:10]
    part_dir = Path(out_dir) / f"date={date_str}"
    part_dir.mkdir(parents=True, exist_ok=True)
    fname = f"{snap['symbol']}_{snap['ts_ms']}.parquet"
    fpath = part_dir / fname
    df.to_parquet(fpath, index=False)
    print(f"[{snap['symbol']}] Snapshot @ {snap['iso_utc']} -> {fpath}")


# ---------------------- Logger Loop ----------------------

def run_logger(symbol, out_dir, count, interval_sec, l2_depth, trades_lookback_min):
    client = Client()
    client.session = requests.Session()       # persistent connection
    client.session.headers.update({'Connection': 'keep-alive'})

    i = 0
    while True if count == -1 else i < count:
        try:
            append_snapshot_parquet_partitioned(client, symbol, out_dir, l2_depth, trades_lookback_min)
        except Exception as e:
            print(f"[{symbol}] ERROR: {e}, retrying in 5s...")
            time.sleep(5)
            continue

        i += 1
        if count != -1 and i >= count:
            break

        elapsed = time.time() % interval_sec
        sleep_for = max(0.0, interval_sec - elapsed)
        sleep_for += random.uniform(0, 1.0)  # jitter
        print(f"[{symbol}] ({i}/{count if count!=-1 else '∞'}) sleeping {sleep_for:.1f}s …")
        time.sleep(sleep_for)


# ---------------------- CLI Entrypoint ----------------------

if __name__ == "__main__":
    symbols = [
        "BTCUSDT", "ETHUSDT", "BNBUSDT", "XRPUSDT",
        "SOLUSDT", "ADAUSDT", "MATICUSDT", "TRXUSDT", "DOGEUSDT"
    ]

    out_dir = "parquet_dataset"
    count = -1
    interval_sec = 60
    l2_depth = 20
    trades_lookback_min = 5

    print(f"Starting {len(symbols)} symbol loggers (threaded)…")

    threads = []
    try:
        for i, sym in enumerate(symbols, 1):
            t = threading.Thread(
                target=run_logger,
                name=f"logger-{sym}",
                kwargs=dict(
                    symbol=sym,
                    out_dir=out_dir,
                    count=count,
                    interval_sec=interval_sec,
                    l2_depth=l2_depth,
                    trades_lookback_min=trades_lookback_min,
                ),
                daemon=True,
            )
            t.start()
            threads.append(t)
            print(f"[{i}/{len(symbols)}] launched {sym}")
            time.sleep(2.0)  # stagger thread starts

        while any(t.is_alive() for t in threads):
            for t in threads:
                t.join(timeout=1.0)
    except KeyboardInterrupt:
        print("\nStop requested (Ctrl+C). Exiting…")