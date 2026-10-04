"""
Bot Scalping v22.1 DEMO — INSTITUTIONAL QUANT ENGINE (Binance Futures)
====================================================================
FIXED:
- WebSocket compatibility fix for python-binance
- Removed os.system('clear') for cloud/Railway compatibility
- Fixed BTC Macro $0.0 display bug with REST fallback
"""

import sys
if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    except Exception:
        pass

import os
import time
import inspect
import math
import threading
import queue
from datetime import datetime, timezone, timedelta
import numpy as np
import pandas as pd
from collections import deque, defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from typing import Optional, Tuple, List, Dict, Any

from dotenv import load_dotenv
from binance.client import Client
from binance import ThreadedWebsocketManager
import ta

load_dotenv()
api_key = os.getenv("API_KEY")
api_secret = os.getenv("API_SECRET")

try:
    client = Client(api_key, api_secret)
except Exception:
    client = Client(api_key, api_secret)

PAPER_TRADING = True
BINANCE_DEMO = False
client.FUTURES_URL = "https://fapi.binance.com/fapi"

WS_MAX_QUEUE_SIZE = 2000
MARKPRICE_FRESH_SEC = 10

# Definisi Zona Waktu Makassar (WITA / UTC+8)
WITA_TZ = timezone(timedelta(hours=8))

def _create_twm():
    kwargs = {"api_key": api_key, "api_secret": api_secret}
    try:
        params = inspect.signature(ThreadedWebsocketManager.__init__).parameters
        if "max_queue_size" in params:
            kwargs["max_queue_size"] = WS_MAX_QUEUE_SIZE
    except Exception:
        pass
    try:
        return ThreadedWebsocketManager(**kwargs)
    except TypeError:
        kwargs.pop("max_queue_size", None)
        return ThreadedWebsocketManager(**kwargs)
    except Exception:
        kwargs.pop("max_queue_size", None)
        return ThreadedWebsocketManager(**kwargs)

twm = _create_twm()

# ═══════════════════════════════════════════════════════════════════════════
# CONFIGURATION & INSTITUTIONAL PARAMETERS
# ═══════════════════════════════════════════════════════════════════════════

LEVERAGE      = 20
ORDER_USDT    = 3.0
MAX_POSITIONS = 1

# Strict Volume & Sideway Filters
MIN_VOLUME_RATIO = 0.85
MIN_ADX_TREND    = 20.0
MIN_ATR_PCT      = 0.003

# Scanning & Concurrency
SCAN_INTERVAL = 2.0
MAX_WORKERS   = 5

# REST API Safety
REST_MIN_INTERVAL = 0.20
REST_403_COOLDOWN = 300.0
REST_429_COOLDOWN = 60.0

# Scoring & Risk
MIN_SCORE                  = 58
ATR_TP_RESTORED_MULTIPLIER = 3.5
ATR_SL_RESTORED_MULTIPLIER = 1.8

MIN_TP_PCT        = 0.025
MAX_TP_PCT        = 0.035
MIN_SL_PCT        = 0.015
MAX_SL_PCT        = 0.025
MAX_HOLD_SECONDS  = 6120

# Institutional Order Book
WALL_RATIO_THRESHOLD  = 2.5
WALL_DEPTH_PCT        = 0.35
WALL_PROXIMITY_PCT    = 0.005
IMBALANCE_STRONG_BULL = 0.25
IMBALANCE_STRONG_BEAR = -0.25
SPOOF_DROP_THRESHOLD  = 0.40

# Macro BTC Correlation
BTC_CRASH_THRESHOLD  = -0.003
BTC_PUMP_THRESHOLD   = 0.003
BTC_WINDOW_SEC       = 8.0
BTC_BREAKER_COOLDOWN = 120.0

# Banned Mechanism Variables
BANNED_SHORT_DURATION = 6120.0

# ═══════════════════════════════════════════════════════════════════════════
# SYMBOLS
# ═══════════════════════════════════════════════════════════════════════════
SYMBOLS = [
    "BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT",
    "ADAUSDT", "DOGEUSDT", "AVAXUSDT", "TRXUSDT", "DOTUSDT",
    "LINKUSDT", "MATICUSDT", "LTCUSDT", "ATOMUSDT", "UNIUSDT",
    "NEARUSDT", "APTUSDT", "ARBUSDT", "OPUSDT", "INJUSDT",
    "SUIUSDT", "SEIUSDT", "FETUSDT", "WLDUSDT", "AAVEUSDT",
    "ORDIUSDT", "TONUSDT", "1000PEPEUSDT", "WIFUSDT", "JUPUSDT",
    "FTMUSDT", "SANDUSDT", "MANAUSDT", "GALAUSDT", "APEUSDT",
    "CRVUSDT", "1000SHIBUSDT", "COMPUSDT", "MKRUSDT", "SNXUSDT",
]
SYMBOLS = list(dict.fromkeys(SYMBOLS))

# ═══════════════════════════════════════════════════════════════════════════
# ORDER BOOK ENGINE
# ═══════════════════════════════════════════════════════════════════════════

class OrderBookEngine:
    def __init__(self):
        self._cache = {}
        self._history = defaultdict(lambda: deque(maxlen=10))
        self._lock = threading.Lock()

    def update(self, symbol: str, bids_raw: list, asks_raw: list, ts: float = None):
        if ts is None: ts = time.time()
        try:
            bids = [(float(p), float(q)) for p, q in bids_raw]
            asks = [(float(p), float(q)) for p, q in asks_raw]
            bids.sort(key=lambda x: x[0], reverse=True)
            asks.sort(key=lambda x: x[0])
            
            bid_vol = sum(q for _, q in bids)
            ask_vol = sum(q for _, q in asks)
            tot_vol = bid_vol + ask_vol
            imbalance = (bid_vol - ask_vol) / (tot_vol + 1e-9)

            best_bid = bids[0][0] if bids else 0.0
            best_ask = asks[0][0] if asks else 0.0

            with self._lock:
                self._cache[symbol] = {
                    "bids": bids, "asks": asks,
                    "bid_vol": bid_vol, "ask_vol": ask_vol,
                    "imbalance": imbalance,
                    "best_bid": best_bid, "best_ask": best_ask,
                    "ts": ts
                }
                self._history[symbol].append((ts, bid_vol, ask_vol, best_bid, best_ask))
        except Exception:
            pass

    def get_book(self, symbol: str) -> Optional[Dict[str, Any]]:
        with self._lock:
            return self._cache.get(symbol)

    def get_imbalance(self, symbol: str) -> float:
        book = self.get_book(symbol)
        return book["imbalance"] if book else 0.0

    def check_walls(self, symbol: str, current_price: float, side: str) -> Tuple[bool, str, float, float, float]:
        book = self.get_book(symbol)
        if not book:
            return False, "NO_DATA", 0.0, 0.0, 0.0

        if side == "LONG":
            asks = book["asks"]
            tot_ask = book["ask_vol"]
            if not asks or tot_ask <= 0: return False, "OK", 0.0, 0.0, 0.0
            avg_ask = tot_ask / len(asks)

            for px, qty in asks:
                if px >= current_price and (px - current_price) / current_price <= WALL_PROXIMITY_PCT:
                    if qty >= WALL_RATIO_THRESHOLD * avg_ask or qty >= WALL_DEPTH_PCT * tot_ask:
                        mult = qty / avg_ask if avg_ask > 0 else 0.0
                        return True, "SELL_WALL", px, qty, mult

        elif side == "SHORT":
            bids = book["bids"]
            tot_bid = book["bid_vol"]
            if not bids or tot_bid <= 0: return False, "OK", 0.0, 0.0, 0.0
            avg_bid = tot_bid / len(bids)

            for px, qty in bids:
                if px <= current_price and (current_price - px) / current_price <= WALL_PROXIMITY_PCT:
                    if qty >= WALL_RATIO_THRESHOLD * avg_bid or qty >= WALL_DEPTH_PCT * tot_bid:
                        mult = qty / avg_bid if avg_bid > 0 else 0.0
                        return True, "BUY_WALL", px, qty, mult

        return False, "OK", 0.0, 0.0, 0.0

    def detect_spoofing(self, symbol: str, side: str) -> Tuple[bool, str]:
        with self._lock:
            hist = list(self._history.get(symbol, []))
        if len(hist) < 3: return False, ""
        
        curr_ts, curr_b_vol, curr_a_vol, _, _ = hist[-1]
        for ts, b_vol, a_vol, _, _ in hist[:-1]:
            if 0.5 <= (curr_ts - ts) <= 2.5:
                if side == "LONG" and b_vol > 0:
                    if curr_b_vol < b_vol * (1 - SPOOF_DROP_THRESHOLD):
                        drop_pct = (1 - curr_b_vol / b_vol) * 100
                        return True, f"Bid liquidity pulled ({drop_pct:.0f}% drop in {curr_ts - ts:.1f}s)"
                elif side == "SHORT" and a_vol > 0:
                    if curr_a_vol < a_vol * (1 - SPOOF_DROP_THRESHOLD):
                        drop_pct = (1 - curr_a_vol / a_vol) * 100
                        return True, f"Ask liquidity pulled ({drop_pct:.0f}% drop in {curr_ts - ts:.1f}s)"
        return False, ""

order_book = OrderBookEngine()

# ═══════════════════════════════════════════════════════════════════════════
# MACRO BTC ENGINE
# ═══════════════════════════════════════════════════════════════════════════

class BTCMacroEngine:
    def __init__(self):
        self.tick_history = deque(maxlen=150)
        self.breaker = {"active": False, "type": "NONE", "until": 0.0, "delta": 0.0, "trigger_ts": 0.0}
        self.last_price = 0.0
        self.lock = threading.Lock()

    def update_tick(self, price: float, ts: float = None):
        if ts is None: ts = time.time()
        with self.lock:
            self.last_price = price
            self.tick_history.append((ts, price))

            cutoff = ts - BTC_WINDOW_SEC
            baseline_price = None
            for t_ts, t_px in self.tick_history:
                if t_ts >= cutoff:
                    baseline_price = t_px
                    break

            if baseline_price and baseline_price > 0:
                delta = (price - baseline_price) / baseline_price
                if delta <= BTC_CRASH_THRESHOLD and not (self.breaker["active"] and self.breaker["type"] == "CRASH"):
                    self.breaker = {
                        "active": True, "type": "CRASH", "until": ts + BTC_BREAKER_COOLDOWN,
                        "delta": delta, "trigger_ts": ts
                    }
                    print(f"\n  🚨 [BTC FLASH CRASH] Drop: {delta*100:+.2f}% | Altcoin LONGs LOCKED {BTC_BREAKER_COOLDOWN:.0f}s!")
                elif delta >= BTC_PUMP_THRESHOLD and not (self.breaker["active"] and self.breaker["type"] == "PUMP"):
                    self.breaker = {
                        "active": True, "type": "PUMP", "until": ts + BTC_BREAKER_COOLDOWN,
                        "delta": delta, "trigger_ts": ts
                    }
                    print(f"\n  🚀 [BTC FLASH PUMP] Surge: {delta*100:+.2f}% | Altcoin SHORTs LOCKED {BTC_BREAKER_COOLDOWN:.0f}s!")

    def check_veto(self, side: str, now: float = None) -> Tuple[bool, str]:
        if now is None: now = time.time()
        with self.lock:
            if self.breaker["active"]:
                if now < self.breaker["until"]:
                    rem = self.breaker["until"] - now
                    b_type = self.breaker["type"]
                    if b_type == "CRASH" and side == "LONG":
                        return True, f"BTC Flash Crash active ({rem:.0f}s left)"
                    elif b_type == "PUMP" and side == "SHORT":
                        return True, f"BTC Flash Pump active ({rem:.0f}s left)"
                else:
                    self.breaker["active"] = False
                    self.breaker["type"] = "NONE"
        return False, "OK"

    def get_status_str(self) -> str:
        with self.lock:
            px = self.last_price
            if px <= 0:
                # Fallback ke REST API jika WS belum mendapatkan harga BTC
                px = price_live("BTCUSDT")
                self.last_price = px

            active = self.breaker["active"] and time.time() < self.breaker["until"]
            if active:
                rem = self.breaker["until"] - time.time()
                return f"BTC: ${px:.1f} | 🚨BREAKER ACTIVE [{self.breaker['type']} ({rem:.0f}s left)]"
            return f"BTC: ${px:.1f} [NORMAL]"

btc_macro = BTCMacroEngine()
_btc_macro = {"regime": "UNKNOWN", "m5": 0.0, "delta_ratio": 0.0, "cvd": 0.0}

# ═══════════════════════════════════════════════════════════════════════════
# ABSORPTION & ORDER FLOW
# ═══════════════════════════════════════════════════════════════════════════

class AbsorptionDetector:
    @staticmethod
    def detect(df: pd.DataFrame) -> Tuple[bool, bool, str]:
        if df is None or len(df) < 25: return False, False, ""
        row = df.iloc[-2]
        
        vol_spike = row.get("vr", 1.0) >= 1.4
        rng = row.get("rng", 1.0)
        low = row.get("low", 0.0)
        high = row.get("high", 0.0)
        close = row.get("close", 0.0)
        delta_ratio = row.get("delta_ratio", 0.0)
        buy_ratio = row.get("br", 0.5)
        lw_ratio = row.get("lower_wick_ratio", 0.0)
        uw_ratio = row.get("upper_wick_ratio", 0.0)

        heavy_seller = (delta_ratio < -0.20) or (buy_ratio < 0.40)
        wick_bull = lw_ratio >= 0.38
        close_held_bull = close >= (low + 0.45 * rng)
        bull_absorb = vol_spike and heavy_seller and (wick_bull or close_held_bull)

        heavy_buyer = (delta_ratio > 0.20) or (buy_ratio > 0.60)
        wick_bear = uw_ratio >= 0.38
        close_held_bear = close <= (high - 0.45 * rng)
        bear_absorb = vol_spike and heavy_buyer and (wick_bear or close_held_bear)

        details = []
        if bull_absorb: details.append("BullAbsorb")
        if bear_absorb: details.append("BearAbsorb")

        return bull_absorb, bear_absorb, " ".join(details)

# ═══════════════════════════════════════════════════════════════════════════
# RISK MANAGEMENT
# ═══════════════════════════════════════════════════════════════════════════

class DynamicRiskManager:
    @staticmethod
    def calculate_levels(entry_price: float, execution_side: str, atr: float) -> Dict[str, float]:
        atr_pct = (atr / entry_price) if entry_price > 0 else 0.015

        tp_pct = max(MIN_TP_PCT, min(MAX_TP_PCT, ATR_TP_RESTORED_MULTIPLIER * atr_pct))
        sl_pct = max(MIN_SL_PCT, min(MAX_SL_PCT, ATR_SL_RESTORED_MULTIPLIER * atr_pct))

        if execution_side == "LONG":
            tp_price = entry_price * (1 + tp_pct)
            sl_price = entry_price * (1 - sl_pct)
        else: # SHORT
            tp_price = entry_price * (1 - tp_pct)
            sl_price = entry_price * (1 + sl_pct)

        return {
            "tp_pct": tp_pct, "sl_pct": sl_pct,
            "tp_price": tp_price, "sl_price": sl_price,
            "atr_pct": atr_pct
        }

# ═══════════════════════════════════════════════════════════════════════════
# MARKET REGIME DETECTION
# ═══════════════════════════════════════════════════════════════════════════

class MarketRegime:
    REGIME_TRENDING_BULL = "TRENDING_BULL"
    REGIME_TRENDING_BEAR = "TRENDING_BEAR"
    REGIME_RANGE         = "RANGE"
    REGIME_VOLATILE      = "VOLATILE"
    REGIME_EXHAUSTION    = "EXHAUSTION"

    @staticmethod
    def detect(df: pd.DataFrame) -> Tuple[str, float, float]:
        if df is None or len(df) < 55: return MarketRegime.REGIME_RANGE, 0, 0
        row, prev = df.iloc[-2], df.iloc[-3]
        close = row["close"]
        e5, e9, e21, e50 = row["e5"], row["e9"], row["e21"], row["e50"]
        atr, atr_prev = row["atr"], prev["atr"]
        adx = row["adx"]
        bull_stack = close > e5 > e9 > e21 > e50
        bear_stack = close < e5 < e9 < e21 < e50
        mild_bull  = close > e9 > e21
        mild_bear  = close < e9 < e21
        strong_trend      = adx > 25
        very_strong_trend = adx > 35
        atr_expand   = (atr / atr_prev) > 1.2 if atr_prev > 0 else False
        atr_collapse = (atr / atr_prev) < 0.8 if atr_prev > 0 else False
        m5, m5_prev = row["m5"], prev["m5"]
        decelerating = (abs(m5) < abs(m5_prev)) if not np.isnan(m5_prev) else False

        if very_strong_trend and bull_stack: return MarketRegime.REGIME_TRENDING_BULL, min(adx, 100), 1.0
        elif very_strong_trend and bear_stack: return MarketRegime.REGIME_TRENDING_BEAR, min(adx, 100), -1.0
        elif strong_trend and (bull_stack or mild_bull): return MarketRegime.REGIME_TRENDING_BULL, min(adx, 80), 0.7
        elif strong_trend and (bear_stack or mild_bear): return MarketRegime.REGIME_TRENDING_BEAR, min(adx, 80), -0.7
        elif atr_expand and adx < 20: return MarketRegime.REGIME_VOLATILE, 50, 0
        elif (atr_collapse and decelerating) or (adx > 20 and adx < 35 and decelerating): return MarketRegime.REGIME_EXHAUSTION, 40, (1 if m5 > 0 else -1)
        else: return MarketRegime.REGIME_RANGE, 30, 0

# ═══════════════════════════════════════════════════════════════════════════
# SCORING ENGINE
# ═══════════════════════════════════════════════════════════════════════════

class SignalWeights:
    def __init__(self):
        self.weights = {
            "ema_bull_stack": 30, "ema_mild_bull": 20, "ema_weak_bull": 12,
            "mom_strong": 25, "mom_moderate": 15,
            "macd_cross_up": 22, "macd_strengthen": 15,
            "orderflow_delta_bull": 25, "orderflow_buy_high": 15,
            "absorption_bull": 35, "orderbook_imbalance_bull": 20,
            "rsi_bull_flow": 15, "rsi_extreme_ob": 10,
            
            "ema_bear_stack": 30, "ema_mild_bear": 20, "ema_weak_bear": 12,
            "mom_strong_neg": 25, "mom_moderate_neg": 15,
            "macd_cross_down": 22, "macd_strengthen_neg": 15,
            "orderflow_delta_bear": 25, "orderflow_sell_high": 15,
            "absorption_bear": 35, "orderbook_imbalance_bear": 20,
            "rsi_bear_flow": 15, "rsi_extreme_os": 10,
        }
        self.history = defaultdict(list)
        self.adaptive_enabled = True

    def record_outcome(self, signals: List[str], won: bool):
        for sig in signals:
            base = sig.split('[')[0].strip()
            if base in self.weights:
                self.history[base].append(1 if won else 0)
                if len(self.history[base]) > 200:
                    self.history[base] = self.history[base][-200:]

    def get_adjusted_weight(self, signal_name: str) -> float:
        if not self.adaptive_enabled: return self.weights.get(signal_name, 10)
        base = signal_name.split('[')[0].strip()
        hist = self.history.get(base, [])
        if len(hist) < 20: return self.weights.get(base, 10)
        return self.weights.get(base, 10) * max(0.5, min(1.5, 0.5 + sum(hist) / len(hist)))

class SignalScorer:
    def __init__(self, signal_weights: SignalWeights):
        self.weights = signal_weights

    def get_signal(self, df: pd.DataFrame, symbol: str = None) -> Tuple[Optional[str], int, List[str], float, str, float]:
        if df is None or len(df) < 55:
            return None, 0, [], 0.0, "UNKNOWN", 0.0
        
        regime, strength, bias = MarketRegime.detect(df)
        long_score, long_sigs = self._score_long(df, symbol)
        short_score, short_sigs = self._score_short(df, symbol)
        atr = df["atr"].iloc[-2]

        bull_absorb, bear_absorb, _ = AbsorptionDetector.detect(df)

        btc_reg = _btc_macro.get("regime", "UNKNOWN")
        if btc_reg == MarketRegime.REGIME_TRENDING_BULL:
            long_score += 10; long_sigs.append("BTC_BullTrend[+10]")
            short_score -= 20
        elif btc_reg == MarketRegime.REGIME_TRENDING_BEAR:
            short_score += 10; short_sigs.append("BTC_BearTrend[+10]")
            long_score -= 20

        if regime == MarketRegime.REGIME_TRENDING_BULL:
            if long_score >= MIN_SCORE: return "LONG", long_score, long_sigs, atr, regime, bias
            return None, max(long_score, short_score), [], atr, regime, bias

        elif regime == MarketRegime.REGIME_TRENDING_BEAR:
            if short_score >= MIN_SCORE: return "SHORT", short_score, short_sigs, atr, regime, bias
            return None, max(long_score, short_score), [], atr, regime, bias

        elif regime in (MarketRegime.REGIME_RANGE, MarketRegime.REGIME_EXHAUSTION):
            if bull_absorb and long_score >= MIN_SCORE:
                return "LONG", long_score, long_sigs, atr, f"{regime}_ABSORB", bias
            if bear_absorb and short_score >= MIN_SCORE:
                return "SHORT", short_score, short_sigs, atr, f"{regime}_ABSORB", bias
            _stats["regime_block"] += 1
            return None, max(long_score, short_score), [], atr, regime, bias

        elif regime == MarketRegime.REGIME_VOLATILE:
            _stats["regime_block"] += 1
            return None, max(long_score, short_score), [], atr, regime, bias

        return None, 0, [], atr, regime, bias

    def _score_long(self, df: pd.DataFrame, symbol: str) -> Tuple[int, List[str]]:
        row, prev, prev2 = df.iloc[-2], df.iloc[-3], df.iloc[-4]
        score, signals = 0, []
        p, e5, e9, e21, e50 = row["close"], row["e5"], row["e9"], row["e21"], row["e50"]

        if p > e5 > e9 > e21 > e50: w = self.weights.get_adjusted_weight("ema_bull_stack"); score += w; signals.append(f"EMA5↑[{w:.0f}]")
        elif p > e5 > e9 > e21: w = self.weights.get_adjusted_weight("ema_mild_bull"); score += w; signals.append(f"EMA4↑[{w:.0f}]")
        elif p > e5 > e9: w = self.weights.get_adjusted_weight("ema_weak_bull"); score += w; signals.append(f"EMA3↑[{w:.0f}]")

        if row["m5"] > 0.003: w = self.weights.get_adjusted_weight("mom_strong"); score += w; signals.append(f"Mom+{row['m5']*100:.1f}%↑[{w:.0f}]")
        elif row["m5"] > 0.0015: w = self.weights.get_adjusted_weight("mom_moderate"); score += w; signals.append(f"Mom+{row['m5']*100:.1f}%↑[{w:.0f}]")

        if prev["mh"] <= 0 and row["mh"] > 0: w = self.weights.get_adjusted_weight("macd_cross_up"); score += w; signals.append(f"MACD_X↑[{w:.0f}]")
        elif row["mh"] > 0 and row["mh"] > prev["mh"] > prev2["mh"]: w = self.weights.get_adjusted_weight("macd_strengthen"); score += w; signals.append(f"MACD↑↑[{w:.0f}]")

        delta_ratio = row.get("delta_ratio", 0.0)
        buy_ratio = row.get("br", 0.5)
        if delta_ratio > 0.20: w = self.weights.get_adjusted_weight("orderflow_delta_bull"); score += w; signals.append(f"ΔBuy+{delta_ratio*100:.0f}%[{w:.0f}]")
        elif buy_ratio > 0.55: w = self.weights.get_adjusted_weight("orderflow_buy_high"); score += w; signals.append(f"TakerBuy{buy_ratio*100:.0f}%[{w:.0f}]")

        bull_abs, _, _ = AbsorptionDetector.detect(df)
        if bull_abs:
            w = self.weights.get_adjusted_weight("absorption_bull"); score += w; signals.append(f"BullAbsorb[{w:.0f}]")

        if symbol:
            imb = order_book.get_imbalance(symbol)
            if imb > IMBALANCE_STRONG_BULL:
                w = self.weights.get_adjusted_weight("orderbook_imbalance_bull"); score += w; signals.append(f"BAI+{imb*100:.0f}%[{w:.0f}]")

        if 48 <= row["rsi"] <= 68: w = self.weights.get_adjusted_weight("rsi_bull_flow"); score += w; signals.append(f"RSI{row['rsi']:.0f}[{w:.0f}]")
        elif row["rsi"] > 68: w = self.weights.get_adjusted_weight("rsi_extreme_ob"); score += w; signals.append(f"RSI{row['rsi']:.0f}OB[{w:.0f}]")

        return score, signals

    def _score_short(self, df: pd.DataFrame, symbol: str) -> Tuple[int, List[str]]:
        row, prev, prev2 = df.iloc[-2], df.iloc[-3], df.iloc[-4]
        score, signals = 0, []
        p, e5, e9, e21, e50 = row["close"], row["e5"], row["e9"], row["e21"], row["e50"]

        if p < e5 < e9 < e21 < e50: w = self.weights.get_adjusted_weight("ema_bear_stack"); score += w; signals.append(f"EMA5↓[{w:.0f}]")
        elif p < e5 < e9 < e21: w = self.weights.get_adjusted_weight("ema_mild_bear"); score += w; signals.append(f"EMA4↓[{w:.0f}]")
        elif p < e5 < e9: w = self.weights.get_adjusted_weight("ema_weak_bear"); score += w; signals.append(f"EMA3↓[{w:.0f}]")

        if row["m5"] < -0.003: w = self.weights.get_adjusted_weight("mom_strong_neg"); score += w; signals.append(f"Mom{row['m5']*100:.1f}%↓[{w:.0f}]")
        elif row["m5"] < -0.0015: w = self.weights.get_adjusted_weight("mom_moderate_neg"); score += w; signals.append(f"Mom{row['m5']*100:.1f}%↓[{w:.0f}]")

        if prev["mh"] >= 0 and row["mh"] < 0: w = self.weights.get_adjusted_weight("macd_cross_down"); score += w; signals.append(f"MACD_X↓[{w:.0f}]")
        elif row["mh"] < 0 and row["mh"] < prev["mh"] < prev2["mh"]: w = self.weights.get_adjusted_weight("macd_strengthen_neg"); score += w; signals.append(f"MACD↓↓[{w:.0f}]")

        delta_ratio = row.get("delta_ratio", 0.0)
        buy_ratio = row.get("br", 0.5)
        if delta_ratio < -0.20: w = self.weights.get_adjusted_weight("orderflow_delta_bear"); score += w; signals.append(f"ΔSell{delta_ratio*100:.0f}%[{w:.0f}]")
        elif buy_ratio < 0.45: w = self.weights.get_adjusted_weight("orderflow_sell_high"); score += w; signals.append(f"TakerSell{(1-buy_ratio)*100:.0f}%[{w:.0f}]")

        _, bear_abs, _ = AbsorptionDetector.detect(df)
        if bear_abs:
            w = self.weights.get_adjusted_weight("absorption_bear"); score += w; signals.append(f"BearAbsorb[{w:.0f}]")

        if symbol:
            imb = order_book.get_imbalance(symbol)
            if imb < IMBALANCE_STRONG_BEAR:
                w = self.weights.get_adjusted_weight("orderbook_imbalance_bear"); score += w; signals.append(f"BAI{imb*100:.0f}%[{w:.0f}]")

        if 32 <= row["rsi"] <= 52: w = self.weights.get_adjusted_weight("rsi_bear_flow"); score += w; signals.append(f"RSI{row['rsi']:.0f}[{w:.0f}]")
        elif row["rsi"] < 32: w = self.weights.get_adjusted_weight("rsi_extreme_os"); score += w; signals.append(f"RSI{row['rsi']:.0f}OS[{w:.0f}]")

        return score, signals

# ═══════════════════════════════════════════════════════════════════════════
# TRADE RECORDS & LEARNING LAYER
# ═══════════════════════════════════════════════════════════════════════════

@dataclass
class TradeRecord:
    symbol:       str
    direction:    str
    entry_price:  float
    exit_price:   float
    pnl:          float
    won:          bool
    regime:       str
    signals:      List[str]
    score:        float
    atr_entry:    float
    hold_seconds: float
    exit_reason:  str
    peak_pct:     float
    timestamp:    float = field(default_factory=time.time)

class LearningLayer:
    def __init__(self, signal_weights: SignalWeights):
        self.signal_weights  = signal_weights
        self.trades          = []
        self.stats_by_regime = defaultdict(lambda: {"wins": 0, "losses": 0, "pnl": 0.0})
        self.stats_by_symbol = defaultdict(lambda: {"wins": 0, "losses": 0})

    def add_trade(self, trade: TradeRecord):
        self.trades.append(trade)
        r = trade.regime
        self.stats_by_regime[r]["wins"]   += 1 if trade.won else 0
        self.stats_by_regime[r]["losses"] += 0 if trade.won else 1
        self.stats_by_regime[r]["pnl"]    += trade.pnl
        self.stats_by_symbol[trade.symbol]["wins"]   += 1 if trade.won else 0
        self.stats_by_symbol[trade.symbol]["losses"] += 0 if trade.won else 1
        self.signal_weights.record_outcome(trade.signals, trade.won)
        if len(self.trades) > 1000: self.trades = self.trades[-500:]

# ═══════════════════════════════════════════════════════════════════════════
# GLOBAL STATE & UTILITIES
# ═══════════════════════════════════════════════════════════════════════════

_executor        = ThreadPoolExecutor(max_workers=MAX_WORKERS)
_ws_mark_price   = {}
_kline_cache     = {}
_kline_lock      = threading.Lock()

_stats = {
    "trades": 0, "wins": 0, "losses": 0, "pnl": 0.0, "best": 0.0, "worst": 0.0, "ath_pnl": 0.0,
    "hard_sl": 0, "tp_exit": 0, "time_limit_exit": 0, "regime_block": 0,
    "wall_veto": 0, "btc_breaker_veto": 0, "spoof_veto": 0, "low_vol_veto": 0,
    "hist": deque(maxlen=200), "start": time.time(),
}

is_logic_inverted = False 
banned_short_until = 0.0

live_positions = {}
cooldown_list  = {}
signal_weights = SignalWeights()
scorer         = SignalScorer(signal_weights)
learning       = LearningLayer(signal_weights)

_last_err_print   = defaultdict(float)
_rest_lock = threading.Lock()
_rest_last_ts = 0.0
_rest_block_until = 0.0
_rest_price_cache = {}

def _log_err(tag, e, cooldown=10):
    now = time.time()
    if now - _last_err_print[tag] > cooldown:
        print(f"  ⚠️ [{tag}] {type(e).__name__}: {e}")
        _last_err_print[tag] = now

def _rest_call(tag, fn, *args, retries=1, **kwargs):
    global _rest_last_ts, _rest_block_until
    last_exc = None
    for attempt in range(retries + 1):
        with _rest_lock:
            wait = max(0.0, _rest_block_until - time.time())
            if wait > 0: time.sleep(wait)
            gap = time.time() - _rest_last_ts
            if gap < REST_MIN_INTERVAL: time.sleep(REST_MIN_INTERVAL - gap)
            _rest_last_ts = time.time()

        try:
            return fn(*args, **kwargs)
        except Exception as e:
            last_exc = e
            msg = str(e).upper()
            now = time.time()
            if "403" in msg or "429" in msg:
                _rest_block_until = max(_rest_block_until, now + REST_429_COOLDOWN)
                break
            if attempt < retries: time.sleep(min(2.0, 0.5 * (2 ** attempt)))

    if last_exc is not None: raise last_exc
    raise RuntimeError(f"REST call failed: {tag}")

get_precision = lambda sym: 2

def qty(symbol, price):
    raw = (ORDER_USDT * LEVERAGE) / price
    return round(raw, get_precision(symbol))

def price_live(symbol):
    cached = _ws_mark_price.get(symbol)
    if cached:
        px, ts = cached
        if px > 0 and (time.time() - ts) < MARKPRICE_FRESH_SEC:
            return px

    now = time.time()
    old = _rest_price_cache.get(symbol)
    if old and (now - old[1]) < 1.0:
        return old[0]

    try:
        res = _rest_call(f"price_live_{symbol}", client.futures_symbol_ticker, symbol=symbol)
        px = float(res["price"])
        _rest_price_cache[symbol] = (px, now)
        return px
    except Exception:
        if old: return old[0]
        return 0.0

# ═══════════════════════════════════════════════════════════════════════════
# TECHNICAL ANALYSIS & DATA ENGINE
# ═══════════════════════════════════════════════════════════════════════════

def fetch_klines(symbol: str, interval: str = "1m", limit: int = 100) -> Optional[pd.DataFrame]:
    with _kline_lock:
        cached = _kline_cache.get(symbol)
        if cached and (time.time() - cached["ts"]) < 10.0:
            return cached["df"].copy()

    try:
        klines = _rest_call(f"klines_{symbol}", client.futures_klines, symbol=symbol, interval=interval, limit=limit)
        if not klines or len(klines) < 55: return None

        df = pd.DataFrame(klines, columns=[
            'open_time', 'open', 'high', 'low', 'close', 'volume',
            'close_time', 'qav', 'num_trades', 'taker_base_vol', 'taker_quote_vol', 'ignore'
        ])
        
        for col in ['open', 'high', 'low', 'close', 'volume', 'qav', 'taker_base_vol', 'taker_quote_vol']:
            df[col] = df[col].astype(float)

        df['e5']  = ta.trend.ema_indicator(df['close'], window=5)
        df['e9']  = ta.trend.ema_indicator(df['close'], window=9)
        df['e21'] = ta.trend.ema_indicator(df['close'], window=21)
        df['e50'] = ta.trend.ema_indicator(df['close'], window=50)

        df['m5']  = df['close'].pct_change(5)
        df['atr'] = ta.volatility.average_true_range(df['high'], df['low'], df['close'], window=14)
        df['adx'] = ta.trend.adx(df['high'], df['low'], df['close'], window=14)
        df['rsi'] = ta.momentum.rsi(df['close'], window=14)

        macd = ta.trend.MACD(df['close'], window_slow=26, window_fast=12, window_sign=9)
        df['mh'] = macd.macd_diff()

        df['vol_ma'] = df['volume'].rolling(window=20).mean()
        df['vr'] = df['volume'] / (df['vol_ma'] + 1e-9)

        df['delta'] = (2 * df['taker_base_vol']) - df['volume']
        df['delta_ratio'] = df['delta'] / (df['volume'] + 1e-9)
        df['br'] = df['taker_base_vol'] / (df['volume'] + 1e-9)

        df['rng'] = df['high'] - df['low'] + 1e-9
        df['lower_wick'] = np.minimum(df['open'], df['close']) - df['low']
        df['upper_wick'] = df['high'] - np.maximum(df['open'], df['close'])
        df['lower_wick_ratio'] = df['lower_wick'] / df['rng']
        df['upper_wick_ratio'] = df['upper_wick'] / df['rng']

        with _kline_lock:
            _kline_cache[symbol] = {"df": df, "ts": time.time()}

        return df
    except Exception as e:
        _log_err(f"fetch_klines_{symbol}", e)
        return None

# ═══════════════════════════════════════════════════════════════════════════
# EXECUTION & MONITORING
# ═══════════════════════════════════════════════════════════════════════════

def execute_trade(symbol: str, raw_side: str, score: int, signals: List[str], atr: float, regime: str):
    global is_logic_inverted, banned_short_until

    now = time.time()
    execution_side = raw_side
    if is_logic_inverted:
        execution_side = "SHORT" if raw_side == "LONG" else "LONG"

    if not is_logic_inverted and execution_side == "SHORT":
        if now < banned_short_until:
            rem = banned_short_until - now
            print(f"  🚫 [BANNED COOLDOWN] {symbol} Analisis SHORT ditolak! Terkena ban tersisa {rem:.0f}s lagi.")
            return

    current_price = price_live(symbol)
    if current_price <= 0: return

    veto, _ = btc_macro.check_veto(execution_side, now)
    if veto: _stats["btc_breaker_veto"] += 1; return

    wall, _, _, _, _ = order_book.check_walls(symbol, current_price, execution_side)
    if wall: _stats["wall_veto"] += 1; return

    spoof, _ = order_book.detect_spoofing(symbol, execution_side)
    if spoof: _stats["spoof_veto"] += 1; return

    levels = DynamicRiskManager.calculate_levels(current_price, execution_side, atr)
    q = qty(symbol, current_price)

    mode_str = "INVERTED 🔄" if is_logic_inverted else "NORMAL 🟢"
    print(f"\n⚡ [ENTRY LOGIC] Mode: {mode_str} | Signal Original: {raw_side} -> Execution: {execution_side}")
    print(f"📈 [ORDER OPENED] {symbol} {execution_side} @ ${current_price:.4f} | Qty: {q} | Score: {score}")

    live_positions[symbol] = {
        "symbol": symbol, "raw_side": raw_side, "execution_side": execution_side,
        "entry_price": current_price, "qty": q, "entry_time": now,
        "tp_price": levels["tp_price"], "sl_price": levels["sl_price"],
        "tp_pct": levels["tp_pct"], "sl_pct": levels["sl_pct"],
        "score": score, "signals": signals, "atr": atr, "regime": regime,
        "peak_pct": 0.0, "mode_at_entry": "INVERTED" if is_logic_inverted else "NORMAL"
    }

def monitor_positions():
    global is_logic_inverted, banned_short_until

    now = time.time()
    for symbol in list(live_positions.keys()):
        pos = live_positions.get(symbol)
        if not pos: continue

        current_price = price_live(symbol)
        if current_price <= 0: continue

        entry_price = pos["entry_price"]
        side = pos["execution_side"]
        entry_mode = pos["mode_at_entry"]
        hold_time = now - pos["entry_time"]

        pnl_pct = ((current_price - entry_price) / entry_price) if side == "LONG" else ((entry_price - current_price) / entry_price)
        pos["peak_pct"] = max(pos["peak_pct"], pnl_pct)

        exit_triggered, exit_reason = False, ""

        if side == "LONG" and current_price >= pos["tp_price"]: exit_triggered, exit_reason = True, "TAKE_PROFIT"
        elif side == "SHORT" and current_price <= pos["tp_price"]: exit_triggered, exit_reason = True, "TAKE_PROFIT"
        elif side == "LONG" and current_price <= pos["sl_price"]: exit_triggered, exit_reason = True, "STOP_LOSS"
        elif side == "SHORT" and current_price >= pos["sl_price"]: exit_triggered, exit_reason = True, "STOP_LOSS"
        elif hold_time >= MAX_HOLD_SECONDS: exit_triggered, exit_reason = True, "TIME_LIMIT"

        if exit_triggered:
            pnl_usdt = pnl_pct * (ORDER_USDT * LEVERAGE)
            won = pnl_usdt > 0

            _stats["trades"] += 1
            _stats["pnl"] += pnl_usdt
            if won:
                _stats["wins"] += 1
                _stats["best"] = max(_stats["best"], pnl_usdt)
            else:
                _stats["losses"] += 1
                _stats["worst"] = min(_stats["worst"], pnl_usdt)
            _stats["ath_pnl"] = max(_stats["ath_pnl"], _stats["pnl"])

            if exit_reason == "STOP_LOSS": _stats["hard_sl"] += 1
            elif exit_reason == "TAKE_PROFIT": _stats["tp_exit"] += 1
            elif exit_reason == "TIME_LIMIT": _stats["time_limit_exit"] += 1

            trade_rec = TradeRecord(
                symbol=symbol, direction=side, entry_price=entry_price, exit_price=current_price,
                pnl=pnl_usdt, won=won, regime=pos["regime"], signals=pos["signals"],
                score=pos["score"], atr_entry=pos["atr"], hold_seconds=hold_time,
                exit_reason=exit_reason, peak_pct=pos["peak_pct"], timestamp=now
            )
            learning.add_trade(trade_rec)
            _stats["hist"].append(trade_rec)

            entry_dt = datetime.fromtimestamp(pos["entry_time"], tz=WITA_TZ).strftime('%H:%M:%S')
            exit_dt  = datetime.fromtimestamp(now, tz=WITA_TZ).strftime('%H:%M:%S WITA')
            print(f"\n🚪 [CLOSED POS] {symbol} {side} | Exit: {exit_reason} | PnL: ${pnl_usdt:+.2f} ({pnl_pct*100:+.2f}%)")
            print(f"⏱️ [TIMESTAMP] Entry: {entry_dt} -> Exit: {exit_dt} | Duration: {hold_time:.0f}s")

            # Dynamic Inversion Mode Switch Logic
            if not won:
                if entry_mode == "NORMAL":
                    if (side == "SHORT" and exit_reason == "TIME_LIMIT") or \
                       (side == "LONG" and exit_reason in ("TIME_LIMIT", "STOP_LOSS")):
                        is_logic_inverted = True
                        print(f"🔄 [MODE TOGGLE] Loss pada Normal Mode ({side} | {exit_reason})! MENGUBAH MODE -> INVERTED MODE 🔄")

                elif entry_mode == "INVERTED":
                    if exit_reason == "TIME_LIMIT":
                        is_logic_inverted = False
                        print(f"🟢 [MODE TOGGLE] Loss Time Limit pada Inverted Mode! MENGUBAH MODE -> NORMAL MODE 🟢")
                        if side == "SHORT":
                            banned_short_until = now + BANNED_SHORT_DURATION
                            print(f"🚫 [BANNED APPLIED] Mode Normal BANNED dari Entry SHORT selama {BANNED_SHORT_DURATION:.0f} detik!")

            del live_positions[symbol]
            cooldown_list[symbol] = now + 60.0

# ═══════════════════════════════════════════════════════════════════════════
# SCANNING & MAIN ENGINE LOOP
# ═══════════════════════════════════════════════════════════════════════════

def scan_symbol(symbol: str):
    if symbol in live_positions or time.time() < cooldown_list.get(symbol, 0):
        return

    df = fetch_klines(symbol, interval="1m", limit=100)
    if df is None or len(df) < 55: return

    last_row = df.iloc[-2]
    if last_row.get("vr", 0.0) < MIN_VOLUME_RATIO or last_row.get("adx", 0.0) < MIN_ADX_TREND or (last_row.get("atr", 0.0) / last_row.get("close", 1.0)) < MIN_ATR_PCT:
        _stats["low_vol_veto"] += 1
        return

    raw_side, score, signals, atr_val, regime, bias = scorer.get_signal(df, symbol)

    if raw_side and len(live_positions) < MAX_POSITIONS:
        execute_trade(symbol, raw_side, score, signals, atr_val, regime)

def print_dashboard():
    # HAPUS os.system('cls'/'clear') untuk mencegah 'TERM environment variable not set' di Cloud Container
    now_wita = datetime.now(WITA_TZ).strftime("%Y-%m-%d %H:%M:%S WITA")
    uptime = time.time() - _stats["start"]
    hours, rem = divmod(uptime, 3600)
    mins, secs = divmod(rem, 60)

    mode_status = "🔄 INVERTED MODE" if is_logic_inverted else "🟢 NORMAL MODE"
    banned_status = f"🚫 SHORT BANNED ({banned_short_until - time.time():.0f}s left)" if time.time() < banned_short_until else "NONE"

    print("\n═══════════════════════════════════════════════════════════════════════════")
    print(f"   🤖 BOT SCALPING v22.1 — INSTITUTIONAL QUANT ENGINE (BINANCE FUTURES)")
    print("═══════════════════════════════════════════════════════════════════════════")
    print(f" 🕒 Current Time : {now_wita} | Uptime: {int(hours)}h {int(mins)}m {int(secs)}s")
    print(f" 🔀 System Mode  : {mode_status} | Ban Status: {banned_status}")
    print(f" 📊 BTC Macro    : {btc_macro.get_status_str()}")
    print(" -------------------------------------------------------------------------")
    print(f" 💰 Total PnL    : ${_stats['pnl']:+.2f} | ATH PnL: ${_stats['ath_pnl']:+.2f}")
    print(f" 🎯 Trades/Win/L : {_stats['trades']} Trades | {_stats['wins']} Wins | {_stats['losses']} Losses")
    print(f" 🏆 Best / Worst : Best: ${_stats['best']:+.2f} | Worst: ${_stats['worst']:+.2f}")
    print(f" 🚪 Exits Break  : TP: {_stats['tp_exit']} | SL: {_stats['hard_sl']} | TimeLimit: {_stats['time_limit_exit']}")
    print(" -------------------------------------------------------------------------")
    print(" 🛑 ACTIVE POSITIONS:")
    if not live_positions:
        print("    (No open position)")
    else:
        for sym, pos in live_positions.items():
            px = price_live(sym)
            side = pos["execution_side"]
            pnl = ((px - pos['entry_price']) if side == "LONG" else (pos['entry_price'] - px)) / pos['entry_price'] * 100
            dur = time.time() - pos["entry_time"]
            print(f"    • {sym} [{side}] Entry: ${pos['entry_price']:.4f} | Mark: ${px:.4f} | PnL: {pnl:+.2f}% | Mode: {pos['mode_at_entry']} | Hold: {dur:.0f}s")
    print("═══════════════════════════════════════════════════════════════════════════")

def main():
    print("🚀 Starting Bot Scalping Engine v22.1...")
    
    # Websocket Handler yang Kompatibel dengan python-binance Versi Baru
    def _ws_mark_price_handler(msg):
        try:
            if isinstance(msg, dict):
                s = msg.get("s")
                p = msg.get("p") or msg.get("c")
                if s and p:
                    _ws_mark_price[s] = (float(p), time.time())
                    if s == "BTCUSDT":
                        btc_macro.update_tick(float(p))
            elif isinstance(msg, list):
                for item in msg:
                    s = item.get("s")
                    p = item.get("p") or item.get("c")
                    if s and p:
                        _ws_mark_price[s] = (float(p), time.time())
                        if s == "BTCUSDT":
                            btc_macro.update_tick(float(p))
        except Exception:
            pass

    try:
        twm.start()
        # Menggunakan method resmi WebSocket python-binance yang stabil
        twm.start_symbol_mark_price_socket(callback=_ws_mark_price_handler, symbol="BTCUSDT")
    except Exception as e:
        print(f"  ⚠️ WebSocket Init Warning: {e}")

    last_dash = 0
    while True:
        try:
            now = time.time()
            monitor_positions()

            if len(live_positions) < MAX_POSITIONS:
                futures = [_executor.submit(scan_symbol, sym) for sym in SYMBOLS]
                for future in as_completed(futures):
                    pass

            if now - last_dash >= 5.0: # Interval dashboard dinaikkan ke 5 detik agar log Cloud bersih
                print_dashboard()
                last_dash = now

            time.sleep(SCAN_INTERVAL)
        except KeyboardInterrupt:
            print("\n🛑 Bot stopped manually by user.")
            break
        except Exception as e:
            _log_err("main_loop", e)
            time.sleep(1.0)

if __name__ == "__main__":
    main()
