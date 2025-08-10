from __future__ import annotations
import sys
import os

sys.path.append(os.path.abspath("../../tools/tv-tickers"))
from utils import tv_pricedata

df = tv_pricedata(ticker="BTCUSDT", exchange="BINANCE", timeframe='15m', num_candles=10000000)

"""
SMC MTF Hybrid Module
---------------------
A middle-ground approach:
- Keeps a ready MTF strategy harness (resample -> align HTF -> LTF signals -> backtest -> plot)
- Auto-uses external SMC indicator library if present (via a soft adapter)
- Falls back to built-in implementations so it runs anywhere, offline

Python 3.10+, pandas>=1.5, numpy>=1.23, matplotlib>=3.6
"""
from dataclasses import dataclass
from typing import Optional, Dict, Tuple, List
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

# ---------------------------------------------------------------------
# Optional external SMC library adapter (soft import)
# ---------------------------------------------------------------------
class _ExternalSMC:
    """Thin adapter around the external SMC lib.
    We detect common function names at runtime. If not found, we return None and
    the pipeline will fall back to built-ins.
    """
    def __init__(self):
        self.ok = False
        self.lib = None
        try:
            # Try a few likely import names. Adjust if your local package differs.
            for name in ("smart_money_concepts", "smc", "smartmoneyconcepts"):
                try:
                    self.lib = __import__(name)
                    break
                except Exception:
                    continue
            self.ok = self.lib is not None
        except Exception:
            self.ok = False

    def swings(self, df, left: int, right: int):
        if not self.ok:
            return None
        # try common names
        fn = getattr(self.lib, "swing_highs_lows", None) or getattr(self.lib, "swings", None)
        if fn is None:
            return None
        try:
            out = fn(df.copy(), left=left, right=right)
            # Expect columns: swing_high, swing_low
            if {"swing_high", "swing_low"}.issubset(out.columns):
                return out
        except Exception:
            pass
        return None

    def bos_choch(self, df):
        if not self.ok:
            return None
        fn = getattr(self.lib, "bos_choch", None) or getattr(self.lib, "bos", None)
        if fn is None:
            return None
        try:
            out = fn(df.copy())
            # Expect boolean columns 'bos' and 'choch' (or similar)
            if "bos" in out.columns:
                if "choch" not in out.columns:
                    out["choch"] = False
                return out
        except Exception:
            pass
        return None

    def order_blocks(self, df, use_wicks: bool):
        if not self.ok:
            return None
        fn = getattr(self.lib, "order_blocks", None) or getattr(self.lib, "ob", None)
        if fn is None:
            return None
        try:
            out = fn(df.copy(), use_wicks=use_wicks) if "use_wicks" in fn.__code__.co_varnames else fn(df.copy())
            # Expect: ob_type, ob_low, ob_high, ob_active or similar
            expected = {"ob_type", "ob_low", "ob_high"}
            if expected.issubset(out.columns):
                if "ob_active" not in out.columns:
                    out["ob_active"] = True
                return out
        except Exception:
            pass
        return None

    def fvgs(self, df):
        if not self.ok:
            return None
        fn = getattr(self.lib, "fvg", None) or getattr(self.lib, "fair_value_gaps", None)
        if fn is None:
            return None
        try:
            out = fn(df.copy())
            # Expect: fvg_type, fvg_low, fvg_high, fvg_filled?
            if {"fvg_low", "fvg_high"}.issubset(out.columns):
                if "fvg_type" not in out.columns:
                    # infer type if not supplied
                    out["fvg_type"] = np.where(out["fvg_high"] > out["fvg_low"], "bullish", "bearish")
                if "fvg_filled" not in out.columns:
                    out["fvg_filled"] = False
                return out
        except Exception:
            pass
        return None

    def liquidity(self, df, tol: float):
        if not self.ok:
            return None
        fn = getattr(self.lib, "liquidity", None)
        if fn is None:
            return None
        try:
            out = fn(df.copy(), tol=tol) if "tol" in fn.__code__.co_varnames else fn(df.copy())
            # Expect: equal_high/low, sweep_high/low
            needed = {"equal_high", "equal_low", "sweep_high", "sweep_low"}
            if needed.issubset(out.columns):
                return out
        except Exception:
            pass
        return None

    def sessions(self, df, tz: str, sessions: Dict[str, Tuple[str, str]]):
        if not self.ok:
            return None
        fn = getattr(self.lib, "sessions", None) or getattr(self.lib, "tag_sessions", None)
        if fn is None:
            return None
        try:
            out = fn(df.copy(), tz=tz, sessions=sessions) if "tz" in fn.__code__.co_varnames else fn(df.copy())
            if "session" in out.columns:
                return out
        except Exception:
            pass
        return None

_ext = _ExternalSMC()

# ---------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------
@dataclass
class Config:
    htf: str = "4H"
    ltf: str = "15T"
    swing_left: int = 2
    swing_right: int = 2
    use_wicks_for_ob: bool = False
    equal_level_tol: float = 0.0005
    risk_per_trade: float = 0.005
    rr_target: float = 2.0
    allow_multiple_positions: bool = False
    session_tz: str = "UTC"
    sessions: Dict[str, Tuple[str, str]] = None
    prefer_external_indicators: bool = True  # if available, use them

    def __post_init__(self):
        if self.sessions is None:
            self.sessions = {
                "London": ("08:00", "11:00"),
                "NewYork": ("13:30", "16:00"),
            }

# ---------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------
def _ensure_dt_index(df: pd.DataFrame) -> pd.DataFrame:
    if "datetime" in df.columns:
        df = df.copy()
        df["datetime"] = pd.to_datetime(df["datetime"], utc=False, errors="coerce")
        df = df.set_index("datetime").sort_index()
    if not isinstance(df.index, pd.DatetimeIndex):
        raise ValueError("DataFrame must have a DatetimeIndex or a 'datetime' column.")
    return df

def resample_ohlcv(df: pd.DataFrame, timeframe: str) -> pd.DataFrame:
    df = _ensure_dt_index(df)
    o = df["open"].resample(timeframe, label="right", closed="right").first()
    h = df["high"].resample(timeframe, label="right", closed="right").max()
    l = df["low"].resample(timeframe, label="right", closed="right").min()
    c = df["close"].resample(timeframe, label="right", closed="right").last()
    v = df["volume"].resample(timeframe, label="right", closed="right").sum()
    return pd.concat({"open": o, "high": h, "low": l, "close": c, "volume": v}, axis=1).dropna()

# ---------------------------------------------------------------------
# Built-in (fallback) indicators
# ---------------------------------------------------------------------
def label_swings(df: pd.DataFrame, left: int = 2, right: int = 2) -> pd.DataFrame:
    x = df.copy()
    highs = x["high"].values
    lows = x["low"].values
    n = len(x)
    sh = np.zeros(n, dtype=bool)
    sl = np.zeros(n, dtype=bool)
    for i in range(left, n - right):
        if highs[i] == np.max(highs[i-left:i+right+1]) and np.argmax(highs[i-left:i+right+1]) == left:
            sh[i] = True
        if lows[i] == np.min(lows[i-left:i+right+1]) and np.argmin(lows[i-left:i+right+1]) == left:
            sl[i] = True
    x["swing_high"] = sh
    x["swing_low"] = sl
    return x

def detect_market_structure(df: pd.DataFrame) -> pd.DataFrame:
    x = df.copy()
    x["structure"] = None
    last_high = None
    last_low = None
    for i, row in enumerate(x.itertuples()):
        if row.swing_high:
            x.at[x.index[i], "structure"] = "HH" if (last_high is not None and x["high"].iat[i] > last_high) else "LH"
            last_high = x["high"].iat[i]
        elif row.swing_low:
            x.at[x.index[i], "structure"] = "LL" if (last_low is not None and x["low"].iat[i] < last_low) else "HL"
            last_low = x["low"].iat[i]
    x["trend"] = x["structure"].map(lambda s: "bull" if s in ("HH", "HL") else ("bear" if s in ("LH", "LL") else None)).ffill()
    return x

def detect_bos_choch(df: pd.DataFrame) -> pd.DataFrame:
    x = df.copy()
    x["bos"], x["choch"] = False, False
    swing_high_idx = x.index[x["swing_high"]].tolist()
    swing_low_idx = x.index[x["swing_low"]].tolist()
    prev_sh, prev_sl, prev_trend = None, None, None
    for idx in x.index:
        close_i = x.at[idx, "close"]
        trend_i = x.at[idx, "trend"]
        if idx in swing_high_idx: prev_sh = x.at[idx, "high"]
        if idx in swing_low_idx: prev_sl = x.at[idx, "low"]
        bos = choch = False
        if prev_sh is not None and close_i > prev_sh:
            bos = True
            if prev_trend == "bear": choch = True
        if prev_sl is not None and close_i < prev_sl:
            bos = True
            if prev_trend == "bull": choch = True
        x.at[idx, "bos"] = bos
        x.at[idx, "choch"] = choch
        prev_trend = trend_i if trend_i is not None else prev_trend
    return x

def find_order_blocks(df: pd.DataFrame, use_wicks: bool = False) -> pd.DataFrame:
    x = df.copy()
    x["ob_type"] = None; x["ob_low"] = np.nan; x["ob_high"] = np.nan; x["ob_active"] = False
    last_bullish_ob = None; last_bearish_ob = None
    for i, idx in enumerate(x.index):
        if not bool(x.at[idx, "bos"]): 
            continue
        direction_up = x.at[idx, "close"] >= x.at[idx, "open"]
        prev_slice = x.iloc[max(0, i-10):i]
        if direction_up:
            bears = prev_slice[prev_slice["close"] < prev_slice["open"]]
            if not bears.empty:
                ob = bears.iloc[-1]
                lo, hi = (ob["low"], ob["high"]) if use_wicks else (min(ob["open"], ob["close"]), max(ob["open"], ob["close"]))
                x.at[idx, "ob_type"] = "bullish"; x.at[idx, "ob_low"] = lo; x.at[idx, "ob_high"] = hi; x.at[idx, "ob_active"] = True
                last_bullish_ob = (lo, hi, idx)
        else:
            bulls = prev_slice[prev_slice["close"] > prev_slice["open"]]
            if not bulls.empty:
                ob = bulls.iloc[-1]
                lo, hi = (ob["low"], ob["high"]) if use_wicks else (min(ob["open"], ob["close"]), max(ob["open"], ob["close"]))
                x.at[idx, "ob_type"] = "bearish"; x.at[idx, "ob_low"] = lo; x.at[idx, "ob_high"] = hi; x.at[idx, "ob_active"] = True
                last_bearish_ob = (lo, hi, idx)
    x["bullish_ob_mitigated"] = False
    x["bearish_ob_mitigated"] = False
    return x

def find_fvgs(df: pd.DataFrame) -> pd.DataFrame:
    x = df.copy()
    x["fvg_type"] = None; x["fvg_low"] = np.nan; x["fvg_high"] = np.nan
    h2 = x["high"].shift(2); l2 = x["low"].shift(2)
    cond_bull = x["low"] > h2; cond_bear = x["high"] < l2
    x.loc[cond_bull, "fvg_type"] = "bullish"; x.loc[cond_bear, "fvg_type"] = "bearish"
    x.loc[cond_bull, ["fvg_low","fvg_high"]] = np.c_[h2[cond_bull].values, x.loc[cond_bull,"low"].values]
    x.loc[cond_bear, ["fvg_low","fvg_high"]] = np.c_[x.loc[cond_bear,"high"].values, l2[cond_bear].values]
    x["fvg_filled"] = False
    return x

def detect_liquidity_sweeps(df: pd.DataFrame, tol: float = 0.0005) -> pd.DataFrame:
    x = df.copy()
    x["equal_high"]=False; x["equal_low"]=False; x["sweep_high"]=False; x["sweep_low"]=False
    rel = lambda a,b: np.abs(a-b) / np.where(b!=0, np.abs(b), 1.0)
    prev_h = x["high"].shift(1); prev_l = x["low"].shift(1)
    x.loc[rel(x["high"], prev_h) <= tol, "equal_high"] = True
    x.loc[rel(x["low"], prev_l) <= tol, "equal_low"] = True
    lvl_h = prev_h.where(x["equal_high"]).ffill(); lvl_l = prev_l.where(x["equal_low"]).ffill()
    x.loc[(lvl_h.notna()) & (x["high"] > lvl_h) & (x["close"] < lvl_h), "sweep_high"] = True
    x.loc[(lvl_l.notna()) & (x["low"] < lvl_l) & (x["close"] > lvl_l), "sweep_low"] = True
    return x

def tag_sessions(df: pd.DataFrame, tz: str, sessions: Dict[str, Tuple[str, str]]) -> pd.DataFrame:
    x = df.copy(); idx = x.index
    local = idx.tz_localize("UTC", nonexistent="shift_forward", ambiguous="NaT") if idx.tz is None else idx
    local = local.tz_convert(tz)
    session_col = pd.Series("OFF", index=idx, dtype="object")
    for name,(start,end) in sessions.items():
        sh, sm = map(int, start.split(":")); eh, em = map(int, end.split(":"))
        in_sess = (((local.hour > sh) | ((local.hour==sh) & (local.minute>=sm))) &
                   ((local.hour < eh) | ((local.hour==eh) & (local.minute<=em))))
        session_col.loc[in_sess] = name
    x["session"] = session_col
    return x

# ---------------------------------------------------------------------
# Hybrid decorators (prefer external indicators if available)
# ---------------------------------------------------------------------
def _apply_swings(df: pd.DataFrame, cfg: Config) -> pd.DataFrame:
    if cfg.prefer_external_indicators:
        ext = _ext.swings(df, cfg.swing_left, cfg.swing_right)
        if ext is not None and {"swing_high","swing_low"}.issubset(ext.columns):
            return ext
    return label_swings(df, cfg.swing_left, cfg.swing_right)

def _apply_bos_choch(df: pd.DataFrame, cfg: Config) -> pd.DataFrame:
    if cfg.prefer_external_indicators:
        ext = _ext.bos_choch(df)
        if ext is not None and "bos" in ext.columns:
            # merge columns into df
            x = df.copy()
            for col in ext.columns:
                x[col] = ext[col]
            return x
    return detect_bos_choch(df)

def _apply_ob(df: pd.DataFrame, cfg: Config) -> pd.DataFrame:
    if cfg.prefer_external_indicators:
        ext = _ext.order_blocks(df, cfg.use_wicks_for_ob)
        if ext is not None and {"ob_type","ob_low","ob_high"}.issubset(ext.columns):
            x = df.copy()
            for col in ext.columns:
                x[col] = ext[col]
            if "ob_active" not in x.columns:
                x["ob_active"] = True
            return x
    return find_order_blocks(df, cfg.use_wicks_for_ob)

def _apply_fvg(df: pd.DataFrame, cfg: Config) -> pd.DataFrame:
    if cfg.prefer_external_indicators:
        ext = _ext.fvgs(df)
        if ext is not None and {"fvg_low","fvg_high"}.issubset(ext.columns):
            x = df.copy()
            for col in ext.columns:
                x[col] = ext[col]
            if "fvg_filled" not in x.columns:
                x["fvg_filled"] = False
            return x
    return find_fvgs(df)

def _apply_liquidity(df: pd.DataFrame, cfg: Config) -> pd.DataFrame:
    if cfg.prefer_external_indicators:
        ext = _ext.liquidity(df, cfg.equal_level_tol)
        if ext is not None and {"equal_high","equal_low","sweep_high","sweep_low"}.issubset(ext.columns):
            x = df.copy()
            for col in ext.columns:
                x[col] = ext[col]
            return x
    return detect_liquidity_sweeps(df, cfg.equal_level_tol)

def _apply_sessions(df: pd.DataFrame, cfg: Config) -> pd.DataFrame:
    if cfg.prefer_external_indicators:
        ext = _ext.sessions(df, cfg.session_tz, cfg.sessions)
        if ext is not None and "session" in ext.columns:
            return ext
    return tag_sessions(df, cfg.session_tz, cfg.sessions)

# ---------------------------------------------------------------------
# MTF wiring + signals/backtest/plot
# ---------------------------------------------------------------------
def align_htf_to_ltf(htf: pd.DataFrame, ltf: pd.DataFrame) -> pd.DataFrame:
    cols = [c for c in ["structure","trend","bos","choch","ob_type","ob_low","ob_high"] if c in htf.columns]
    ctx = htf[cols].copy().reset_index().rename(columns={"index":"datetime"})
    ltf_idx = _ensure_dt_index(ltf).index
    aligned = pd.merge_asof(
        left=pd.DataFrame({"datetime": ltf_idx}),
        right=ctx,
        on="datetime",
        direction="backward",
    ).set_index("datetime")
    return aligned.reindex(ltf_idx)

def _latest_zone_before(idx_i, df, col_type, typ):
    if col_type == "ob":
        mask = (df.index < idx_i) & (df.get("ob_type") == typ) & df.get("ob_active", True)
        if not mask.any(): return None
        last = df.loc[mask].iloc[-1]
        return (last["ob_low"], last["ob_high"], last.name)
    elif col_type == "fvg":
        mask = (df.index < idx_i) & (df.get("fvg_type") == typ) & (~df.get("fvg_filled", False))
        if not mask.any(): return None
        last = df.loc[mask].iloc[-1]
        lo, hi = float(last["fvg_low"]), float(last["fvg_high"])
        if np.isnan(lo) or np.isnan(hi): return None
        return (min(lo,hi), max(lo,hi), last.name)
    return None

def generate_signals(ltf: pd.DataFrame, htf_ctx: pd.DataFrame, cfg: Config) -> pd.DataFrame:
    x = ltf.copy()
    ctx = htf_ctx.reindex(x.index)
    x["htf_trend"] = ctx.get("trend")
    x["htf_structure"] = ctx.get("structure")
    records = []
    for idx in x.index:
        if x.at[idx, "session"] == "OFF":
            continue
        htf_trend = x.at[idx, "htf_trend"]
        if htf_trend not in ("bull","bear"):
            continue
        if not bool(x.at[idx, "bos"]):
            continue
        side = "long" if htf_trend=="bull" else "short"
        if side=="long":
            zone = _latest_zone_before(idx, x, "ob", "bullish") or _latest_zone_before(idx, x, "fvg", "bullish")
            if not zone: continue
            lo, hi, _ = zone
            touched = (x.at[idx,"low"] <= hi) and (x.at[idx,"low"] >= lo)
            if not touched: continue
            entry = float(x.at[idx,"close"]); stop = float(lo)
        else:
            zone = _latest_zone_before(idx, x, "ob", "bearish") or _latest_zone_before(idx, x, "fvg", "bearish")
            if not zone: continue
            lo, hi, _ = zone
            touched = (x.at[idx,"high"] >= lo) and (x.at[idx,"high"] <= hi)
            if not touched: continue
            entry = float(x.at[idx,"close"]); stop = float(hi)
        risk = abs(entry-stop)
        if risk<=0: continue
        target = entry + cfg.rr_target*risk if side=="long" else entry - cfg.rr_target*risk
        records.append({
            "datetime": idx, "side": side, "entry": entry, "stop": stop, "target": target,
            "rr": cfg.rr_target, "reason": f"{side.upper()} | HTF:{htf_trend} LTF:BOS zone_touch",
            "htf_state": str(x.at[idx,"htf_structure"]), "session": x.at[idx,"session"]
        })
    return pd.DataFrame.from_records(records).set_index("datetime")

def backtest_signals(ltf: pd.DataFrame, signals: pd.DataFrame, cfg: Config):
    if signals.empty:
        perf = pd.DataFrame(columns=["trade_id","side","entry_px","stop_px","target_px","exit_px","exit_time","R","pnl","equity"])
        curve = pd.DataFrame(index=ltf.index, data={"equity": [1.0]*len(ltf)})
        return perf, curve
    equity = 1.0; in_pos=False; trade_id=0
    trade_recs=[]
    for sig_idx, sig in signals.iterrows():
        if sig_idx not in ltf.index: continue
        sig_loc = ltf.index.get_loc(sig_idx)
        if isinstance(sig_loc, slice): sig_loc = sig_loc.stop-1
        entry_loc = sig_loc+1
        if entry_loc >= len(ltf): continue
        if in_pos and not cfg.allow_multiple_positions: continue
        side = sig["side"]; entry_time = ltf.index[entry_loc]; entry_px = float(ltf["open"].iat[entry_loc])
        stop_px = float(sig["stop"]); target_px = float(sig["target"])
        exit_px=None; exit_time=None
        for j in range(entry_loc, len(ltf)):
            h=float(ltf["high"].iat[j]); l=float(ltf["low"].iat[j]); t=ltf.index[j]
            if side=="long":
                hit_stop = l<=stop_px; hit_target = h>=target_px
            else:
                hit_stop = h>=stop_px; hit_target = l<=target_px
            if hit_stop: exit_px=stop_px; exit_time=t; break
            if hit_target: exit_px=target_px; exit_time=t; break
        if exit_px is None:
            exit_px=float(ltf["close"].iat[-1]); exit_time=ltf.index[-1]
        risk=abs(entry_px-stop_px); R=(exit_px-entry_px)/risk if side=="long" else (entry_px-exit_px)/risk
        pnl_frac = cfg.risk_per_trade * R; equity *= (1.0+pnl_frac)
        trade_recs.append({"trade_id":trade_id,"side":side,"entry_time":entry_time,"entry_px":entry_px,
                           "stop_px":stop_px,"target_px":target_px,"exit_time":exit_time,"exit_px":exit_px,
                           "R":R,"pnl":pnl_frac,"equity":equity,"reason":sig["reason"],"session":sig["session"]})
        trade_id+=1
    perf = pd.DataFrame(trade_recs).set_index("entry_time")
    curve = pd.DataFrame(index=ltf.index, data={"equity":1.0})
    for _,tr in perf.iterrows():
        curve.loc[curve.index >= tr["exit_time"], "equity"] = tr["equity"]
    return perf, curve

def plot_marked_up_chart(ltf: pd.DataFrame, annotations: Dict[str, pd.DataFrame], cfg: Config):
    x = ltf.copy(); sigs = annotations.get("signals", pd.DataFrame())
    plt.figure(figsize=(12,6))
    plt.plot(x.index, x["close"], lw=1.0, label="Close")
    bos_idx = x.index[x.get("bos", False).fillna(False)]
    choch_idx = x.index[x.get("choch", False).fillna(False)]
    plt.scatter(bos_idx, x.loc[bos_idx,"close"], marker="^", s=40, label="BOS")
    plt.scatter(choch_idx, x.loc[choch_idx,"close"], marker="v", s=40, label="CHOCH")
    if {"ob_low","ob_high"}.issubset(x.columns):
        for i,row in x[x.get("ob_active", False).fillna(False)].iterrows():
            lo,hi=row["ob_low"],row["ob_high"]
            if pd.notna(lo) and pd.notna(hi):
                plt.fill_between([i,i],[lo,lo],[hi,hi],alpha=0.15,step="pre")
    if {"fvg_low","fvg_high"}.issubset(x.columns):
        for i,row in x[(~x.get("fvg_filled", False)) & x["fvg_low"].notna()].iterrows():
            lo,hi=row["fvg_low"],row["fvg_high"]; lo,hi=min(lo,hi),max(lo,hi)
            plt.fill_between([i,i],[lo,lo],[hi,hi],alpha=0.1,step="pre")
    if not sigs.empty:
        for t,s in sigs.iterrows():
            if t not in x.index: continue
            plt.scatter([t],[s["entry"]], marker="o", s=30, label="Entry" if "Entry" not in plt.gca().get_legend_handles_labels()[1] else "")
            plt.hlines(s["stop"], xmin=t, xmax=t, linestyles="dashed", label="Stop" if "Stop" not in plt.gca().get_legend_handles_labels()[1] else "")
            plt.hlines(s["target"], xmin=t, xmax=t, linestyles="dotted", label="Target" if "Target" not in plt.gca().get_legend_handles_labels()[1] else "")
    plt.title("SMC Markup (LTF, hybrid indicators)")
    plt.legend(loc="best"); plt.tight_layout(); plt.show()

# Convenience: decorate a timeframe with full feature set
def _decorate(df: pd.DataFrame, cfg: Config) -> pd.DataFrame:
    df = _apply_swings(df, cfg)
    df = detect_market_structure(df)  # structure/trend derived from swings (stable)
    df = _apply_bos_choch(df, cfg)
    df = _apply_ob(df, cfg)
    df = _apply_fvg(df, cfg)
    df = _apply_liquidity(df, cfg)
    df = _apply_sessions(df, cfg)
    return df

# ---------------------------------------------------------------------
# Minimal runnable example (synthetic)
# ---------------------------------------------------------------------
if __name__ == "__main__":
    # 1) Make sure columns exist & datetime is parseable (tz-aware or naive OK)
    df.index = pd.to_datetime(df.index, errors="coerce")

    # 2) Configure (you can change sessions/tz/timeframes)
    cfg = Config(
        htf="4H",
        ltf="15T",
        session_tz="UTC",           # set to your preference, e.g., "Asia/Singapore"
        prefer_external_indicators=True,  # auto-use the GitHub lib if installed
        rr_target=2.0,
        risk_per_trade=0.005,
    )

    # 3) Resample to LTF/HTF (works even if df is already 15m—resample is idempotent)
    ltf = resample_ohlcv(df, cfg.ltf)
    htf = resample_ohlcv(df, cfg.htf)

    # 4) Build features (hybrid: external lib if available, else fallback)
    htf_feat = _decorate(htf, cfg)
    ltf_feat = _decorate(ltf, cfg)

    # 5) Align HTF context down to LTF and generate signals
    htf_ctx = align_htf_to_ltf(htf_feat, ltf_feat)
    signals = generate_signals(ltf_feat, htf_ctx, cfg)

    # 6) Simple backtest
    perf, curve = backtest_signals(ltf_feat, signals, cfg)

    print("Signals (last 10):")
    print(signals.tail(10))
    print("\nTrades (summary):")
    if not perf.empty:
        hit_rate = (perf["R"] > 0).mean()
        avg_R = perf["R"].mean()
        expectancy = avg_R * cfg.risk_per_trade
        max_dd = (curve["equity"].cummax() - curve["equity"]).max()
        print(f"n={len(perf)} | Hit {hit_rate:.1%} | Avg R {avg_R:.2f} | Exp/trade {expectancy:.4f} | MaxDD {max_dd:.3f}")
    else:
        print("No trades produced by the current rules/timeframe/session filter.")

    # 7) Plot
    plot_marked_up_chart(ltf_feat, {"signals": signals}, cfg)