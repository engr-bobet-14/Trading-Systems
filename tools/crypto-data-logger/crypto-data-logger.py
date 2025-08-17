#!/usr/bin/env python3
"""
Partitioned Parquet logger for Binance features using python-binance.

Each snapshot writes ONE Parquet file under:
  <out_dir>/date=YYYY-MM-DD/<SYMBOL>_<ts_ms>.parquet

Snapshot includes:
- ts_ms, iso_utc, day_of_week, is_weekend, session flags (asia/europe/us)
- Latest 1m OHLCV (current bar)
- L1 quote (bid/ask/mid/spread, size imbalance)
- L2 shallow features (top-N): bid/ask depth, asymmetry, side VWAPs, simple slopes
- Trades features over a lookback window (volume_base, volume_quote, VWAP, buy/sell imbalance)
- Spot–Perp basis & funding (USDT-M perpetual)

Run examples:
  # 10 snapshots then stop
  python binance_parquet_logger.py --symbol BTCUSDT --out-dir parquet_dataset --count 10 --interval-sec 60

  # run forever (Ctrl+C to stop)
  python binance_parquet_logger.py --symbol BTCUSDT --out-dir parquet_dataset --count -1 --interval-sec 60
"""

import os
import time
import argparse
from pathlib import Path
from datetime import datetime, timezone

import numpy as np
import pandas as pd
from binance.client import Client

# ---------------------- Time/session helpers ----------------------

def iso_now_utc_ms() -> tuple[int, str]:
    now = datetime.now(timezone.utc)
    return int(now.timestamp() * 1000), now.isoformat()

def sessions_from_ts_ms(ts_ms: int) -> dict:
    dt = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
    hod = dt.hour + dt.minute / 60
    dow = dt.weekday()  # 0=Mon
    return {
        "iso_utc": dt.isoformat(),
        "day_of_week": dow,
        "is_weekend": int(dow >= 5),
        "asia_session": int(0 <= hod < 8),
        "europe_session": int(7 <= hod < 16),
        "us_session": int(13 <= hod < 22),
    }

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

# ---------------------- Binance pulls ----------------------

client = Client()  # public client; no API key required for market data

def latest_ohlcv_1m(symbol: str) -> dict:
    # current (possibly still forming) 1m bar
    kl = client.get_klines(symbol=symbol.upper(), interval=Client.KLINE_INTERVAL_1MINUTE, limit=1)
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

def l1_quote(symbol: str) -> dict:
    t = client.get_orderbook_ticker(symbol=symbol.upper())
    bid, ask = float(t["bidPrice"]), float(t["askPrice"])
    bq, aq = float(t["bidQty"]), float(t["askQty"])
    mid = 0.5 * (bid + ask)
    spread = ask - bid
    imb = (bq - aq) / (bq + aq) if (bq + aq) > 0 else float("nan")
    return {
        "l1_bid": bid, "l1_ask": ask, "l1_mid": mid, "l1_spread": spread,
        "l1_bid_qty": bq, "l1_ask_qty": aq, "l1_imbalance": imb,
    }

def l2_features(symbol: str, depth: int) -> dict:
    depth = min(max(depth, 5), 5000)  # Binance limits
    ob = client.get_order_book(symbol=symbol.upper(), limit=depth)
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

def trades_features(symbol: str, lookback_minutes: int) -> dict:
    end_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    start_ms = end_ms - lookback_minutes * 60_000
    trades = client.get_aggregate_trades(symbol=symbol.upper(), startTime=start_ms, endTime=end_ms)
    if not trades:
        return {"tr_volume_base": 0.0, "tr_volume_quote": 0.0, "tr_vwap": float("nan"), "tr_buy_sell_imbalance": float("nan")}
    px = np.array([float(t["p"]) for t in trades], dtype=float)
    qty = np.array([float(t["q"]) for t in trades], dtype=float)
    vol_base = float(qty.sum())
    vol_quote = float((qty * px).sum())
    v = float(vol_quote / vol_base) if vol_base > 0 else float("nan")
    m = np.array([bool(t["m"]) for t in trades])  # True => buyer is maker (seller aggressed)
    buy_vol = float(qty[~m].sum())
    sell_vol = float(qty[m].sum())
    imb = float((buy_vol - sell_vol) / (buy_vol + sell_vol)) if (buy_vol + sell_vol) > 0 else float("nan")
    return {
        "tr_volume_base": vol_base,
        "tr_volume_quote": vol_quote,
        "tr_vwap": v,
        "tr_buy_sell_imbalance": imb,
    }

def basis_funding(symbol: str) -> dict:
    spot = float(client.get_symbol_ticker(symbol=symbol.upper())["price"])
    mark_info = client.futures_mark_price(symbol=symbol.upper())  # USDT-M perp
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

# ---------------------- Snapshot + Parquet (Partitioned) ----------------------

def make_snapshot(symbol: str, l2_depth: int, trades_lookback_min: int) -> dict:
    ts_ms, _ = iso_now_utc_ms()
    row = {
        "symbol": symbol.upper(),
        "ts_ms": ts_ms,
        **sessions_from_ts_ms(ts_ms),
    }
    row.update(latest_ohlcv_1m(symbol))
    row.update(l1_quote(symbol))
    row.update(l2_features(symbol, l2_depth))
    row.update(trades_features(symbol, trades_lookback_min))
    row.update(basis_funding(symbol))
    return row

def append_snapshot_parquet_partitioned(symbol: str, out_dir: str, l2_depth: int, trades_lookback_min: int):
    """
    Writes one Parquet file per snapshot under:
      <out_dir>/date=YYYY-MM-DD/<SYMBOL>_<ts_ms>.parquet
    """
    snap = make_snapshot(symbol, l2_depth, trades_lookback_min)
    df = pd.DataFrame([snap])

    # Partition by UTC date
    date_str = snap["iso_utc"][:10]  # YYYY-MM-DD
    part_dir = Path(out_dir) / f"date={date_str}"
    part_dir.mkdir(parents=True, exist_ok=True)

    # Unique filename per snapshot (symbol + ms ts helps with multi-symbol runs)
    fname = f"{snap['symbol']}_{snap['ts_ms']}.parquet"
    fpath = part_dir / fname

    df.to_parquet(fpath, index=False)
    print(f"[{snap['symbol']}] Appended snapshot @ {snap['iso_utc']} -> {fpath}")

def run_logger(symbol: str, out_dir: str, count: int, interval_sec: int, l2_depth: int, trades_lookback_min: int):
    """
    If count == -1, runs forever (Ctrl+C to stop). Otherwise writes 'count' snapshots.
    """
    i = 0
    try:
        while True if count == -1 else i < count:
            start_t = time.time()

            append_snapshot_parquet_partitioned(symbol, out_dir, l2_depth, trades_lookback_min)

            i += 1
            if count != -1 and i >= count:
                break

            # Pace the loop
            elapsed = time.time() - start_t
            sleep_for = max(0.0, interval_sec - elapsed)
            total_str = "∞" if count == -1 else str(count)
            print(f"[{symbol.upper()}] ({i}/{total_str}) sleeping {sleep_for:.2f}s …")
            time.sleep(sleep_for)
    except KeyboardInterrupt:
        print(f"\n[{symbol.upper()}] Stopped by user (Ctrl+C).")

# ---------------------- CLI ----------------------

def main():
    ap = argparse.ArgumentParser(description="Partitioned Parquet Binance feature logger")
    ap.add_argument("--symbol", default="BTCUSDT", help="e.g., BTCUSDT/ETHUSDT")
    ap.add_argument("--out-dir", default="parquet_dataset", help="Output directory for partitioned Parquet files")
    ap.add_argument("--count", type=int, default=10, help="How many snapshots (-1 = run forever)")
    ap.add_argument("--interval-sec", type=int, default=60, help="Seconds between snapshots")
    ap.add_argument("--l2-depth", type=int, default=20, help="Top-N depth for L2 metrics")
    ap.add_argument("--trades-lookback-min", type=int, default=5, help="Trade window in minutes")

    args = ap.parse_args()
    run_logger(
        symbol=args.symbol,
        out_dir=args.out_dir,
        count=args.count,
        interval_sec=args.interval_sec,
        l2_depth=args.l2_depth,
        trades_lookback_min=args.trades_lookback_min,
    )

if __name__ == "__main__":
    main()