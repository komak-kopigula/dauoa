"""
Bot Scalping v22.0 DEMO — INSTITUTIONAL QUANT ENGINE (Binance Futures)
====================================================================
STRICT SIDEWAY & VOLUME FILTER + MAKASSAR TIMEZONE (WITA):
- Volume Filter: Wajib Volume Ratio (VR) >= 0.85 & ADX >= 20 (Cegah Entry Sideway)
- Volatility Filter: ATR % wajib cukup untuk pergerakan harga.
- Dynamic Logic Toggle Mode: Normal <-> Inverted saat Loss (Time Limit / SL).
- Smart Banned System (6120s) pasca Inverted Time Limit Loss.
- Real-time Metrics: ATH PnL, Best Single Win, Worst Single Loss.
- Last 5 Trades History dengan Timestamp Entry & Exit (Zona Waktu WITA / Makassar UTC+8).
- MAX_POSITIONS = 1 | ORDER_USDT = 3.0 USDT.
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
DEPTH_SOCKET_CHUNK = 8
MARK_PRICE_FAST = False

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
#  CONFIGURATION & INSTITUTIONAL PARAMETERS
# ═══════════════════════════════════════════════════════════════════════════

LEVERAGE      = 20
ORDER_USDT    = 3.0
MAX_POSITIONS = 1

# Strict Volume & Sideway Filters
MIN_VOLUME_RATIO = 0.85   # Volume minimal 85% dari rata-rata 20 candle
MIN_ADX_TREND    = 20.0   # ADX minimal 20 (mengabaikan market mati/sideway)
MIN_ATR_PCT      = 0.003  # Volatilitas minimal 0.3% agar ada pergerakan

# Scanning & Concurrency
SCAN_INTERVAL = 2.0
MONITOR_INT   = 0.1
BATCH_SIZE    = 15
MAX_WORKERS   = 5
SLOT_FILL_INT = 0.01

# REST API Safety
REST_MIN_INTERVAL = 0.20
REST_403_COOLDOWN = 300.0
REST_429_COOLDOWN = 60.0
REST_418_COOLDOWN = 900.0
REST_RETRIES = 2

# Scoring & Risk
MIN_SCORE                  = 58   # Batas minimal skor sinyal
ATR_TP_RESTORED_MULTIPLIER = 3.5
ATR_SL_RESTORED_MULTIPLIER = 1.8

MIN_TP_PCT        = 0.025
MAX_TP_PCT        = 0.035
MIN_SL_PCT        = 0.015
MAX_SL_PCT        = 0.025
MAX_HOLD_SECONDS  = 6120   # Batas maksimal tahan posisi (6120 detik)

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

# Kill Switch
DAILY_LOSS   = -20.0
CONSEC_MAX   = 15
CONSEC_PAUSE = 10

LEARNING_WINDOW       = 200
MIN_TRADES_FOR_WEIGHT = 20

# ═══════════════════════════════════════════════════════════════════════════
#  SYMBOLS
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
#  ORDER BOOK ENGINE
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
#  MACRO BTC ENGINE
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
            active = self.breaker["active"] and time.time() < self.breaker["until"]
            if active:
                rem = self.breaker["until"] - time.time()
                return f"BTC: ${px:.1f} | 🚨BREAKER ACTIVE [{self.breaker['type']} ({rem:.0f}s left)]"
            return f"BTC: ${px:.1f} [NORMAL]"

btc_macro = BTCMacroEngine()
_btc_macro = {"regime": "UNKNOWN", "m5": 0.0, "delta_ratio": 0.0, "cvd": 0.0}

# ═══════════════════════════════════════════════════════════════════════════
#  ABSORPTION & ORDER FLOW
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
#  RISK MANAGEMENT
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
#  MARKET REGIME DETECTION
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
#  SCORING ENGINE
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
                if len(self.history[base]) > LEARNING_WINDOW:
                    self.history[base] = self.history[base][-LEARNING_WINDOW:]

    def get_adjusted_weight(self, signal_name: str) -> float:
        if not self.adaptive_enabled: return self.weights.get(signal_name, 10)
        base = signal_name.split('[')[0].strip()
        hist = self.history.get(base, [])
        if len(hist) < MIN_TRADES_FOR_WEIGHT: return self.weights.get(base, 10)
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
#  TRADE RECORDS & LEARNING LAYER
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
    entry_ts:     float = field(default_factory=time.time)
    exit_ts:      float = field(default_factory=time.time)

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
        if trade.won:
            self.stats_by_regime[r].setdefault("peak_sum", 0.0)
            self.stats_by_regime[r]["peak_sum"] += trade.peak_pct
        self.stats_by_symbol[trade.symbol]["wins"]   += 1 if trade.won else 0
        self.stats_by_symbol[trade.symbol]["losses"] += 0 if trade.won else 1
        self.signal_weights.record_outcome(trade.signals, trade.won)
        if len(self.trades) > 1000: self.trades = self.trades[-500:]

    def avg_win(self) -> float:
        wins = [t.pnl for t in self.trades if t.won]
        return sum(wins) / len(wins) if wins else 0.0

    def avg_loss(self) -> float:
        losses = [abs(t.pnl) for t in self.trades if not t.won]
        return sum(losses) / len(losses) if losses else 0.0

# ═══════════════════════════════════════════════════════════════════════════
#  GLOBAL STATE & UTILITIES
# ═══════════════════════════════════════════════════════════════════════════

_precision_cache = {}
_ticker_cache    = {}
_ticker_ts       = 0
_lock            = threading.Lock()
_executor        = ThreadPoolExecutor(max_workers=MAX_WORKERS)
_rescan_q        = queue.Queue()
_hot_syms        = deque(maxlen=30)

_ws_mark_price   = {}
_kline_cache     = {}
_kline_lock      = threading.Lock()
_ws_ticker_cache = {}
_ws_ticker_ts    = 0
_ws_last_msg_ts  = time.time()
WS_STALE_SEC     = 30
MARKPRICE_FRESH_SEC = 10

_macro = {"btc": "UNKNOWN"}
_ks    = {"active": False, "reason": "", "resume": 0, "consec": 0, "daily": 0.0, "day_reset": 0}
_stats = {
    "trades": 0, "wins": 0, "losses": 0, "pnl": 0.0, "best": 0.0, "worst": 0.0, "ath_pnl": 0.0,
    "hard_sl": 0, "tp_exit": 0, "time_limit_exit": 0, "regime_block": 0,
    "wall_veto": 0, "btc_breaker_veto": 0, "spoof_veto": 0, "absorb_entries": 0,
    "low_vol_veto": 0,
    "hist": deque(maxlen=200), "start": time.time(),
}

# Variable Dynamic Logic Toggle Mode
is_logic_inverted = False  # False = Normal, True = Inverted
banned_short_until = 0.0   # Banned time untuk entry Short ketika kembali ke Normal Mode

live_positions = {}
cooldown_list  = {}
trade_log      = []
signal_weights = SignalWeights()
scorer         = SignalScorer(signal_weights)
learning       = LearningLayer(signal_weights)

_last_err_print   = defaultdict(float)
_api_fail_streak = 0
_api_ok_last      = time.time()
_rest_lock = threading.Lock()
_rest_last_ts = 0.0
_rest_block_until = 0.0
_rest_price_cache = {}

def _log_err(tag, e, cooldown=10):
    now = time.time()
    if now - _last_err_print[tag] > cooldown:
        print(f"  ⚠️ [{tag}] {type(e).__name__}: {e}")
        _last_err_print[tag] = now

def _api_ok():
    global _api_fail_streak, _api_ok_last
    _api_fail_streak = 0
    _api_ok_last = time.time()

def _api_fail(tag):
    global _api_fail_streak
    _api_fail_streak += 1

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
            result = fn(*args, **kwargs)
            _api_ok()
            return result
        except Exception as e:
            last_exc = e
            msg = str(e).upper()
            now = time.time()

            if "403" in msg or "REQUEST BLOCKED" in msg:
                _rest_block_until = max(_rest_block_until, now + REST_403_COOLDOWN)
                _api_fail(f"{tag}_403")
                break
            if "429" in msg:
                _rest_block_until = max(_rest_block_until, now + REST_429_COOLDOWN)
                _api_fail(f"{tag}_429")
                break

            _api_fail(tag)
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
    try:
        r = _rest_call("price", client.futures_symbol_ticker, symbol=symbol)
        px = float(r["price"])
        _ws_mark_price[symbol] = (px, time.time())
        return px
    except Exception as e:
        _log_err("price_live", e)
        cached_any = _ws_mark_price.get(symbol)
        return cached_any[0] if cached_any else 0.0

def process_candles(klines: list) -> pd.DataFrame:
    df = pd.DataFrame(klines, columns=[
        "time", "open", "high", "low", "close", "volume",
        "close_time", "qav", "trades", "tbv", "tbq", "ignore"
    ])
    for col in ["open", "high", "low", "close", "volume", "tbv"]:
        df[col] = df[col].astype(float)

    c, h, l, v = df["close"], df["high"], df["low"], df["volume"]
    tbv = df["tbv"]

    df["e5"]  = ta.trend.ema_indicator(c, window=5)
    df["e9"]  = ta.trend.ema_indicator(c, window=9)
    df["e21"] = ta.trend.ema_indicator(c, window=21)
    df["e50"] = ta.trend.ema_indicator(c, window=50)

    m = ta.trend.MACD(c)
    df["mh"] = m.macd_diff()
    df["rsi"] = ta.momentum.rsi(c, window=14)
    df["atr"] = ta.volatility.average_true_range(h, l, c, window=14)
    
    # ADX Filter untuk deteksi Sideway
    adx_ind = ta.trend.ADXIndicator(h, l, c, window=14)
    df["adx"] = adx_ind.adx()

    df["m5"]  = c.pct_change(5)
    v_ma20    = v.rolling(20).mean()
    df["vr"]  = v / (v_ma20 + 1e-9)

    df["br"] = tbv / (v + 1e-9)
    df["delta_vol"] = (2 * tbv) - v
    df["delta_ratio"] = df["delta_vol"] / (v + 1e-9)

    rng = (h - l).replace(0, 1e-9)
    df["rng"] = rng
    df["upper_wick_ratio"] = (h - df[["open", "close"]].max(axis=1)) / rng
    df["lower_wick_ratio"] = (df[["open", "close"]].min(axis=1) - l) / rng

    return df

def fetch_df(symbol: str) -> Optional[pd.DataFrame]:
    with _kline_lock:
        kl = _kline_cache.get(symbol)
        if kl and len(kl) >= 55:
            try: return process_candles(kl)
            except Exception: pass
    try:
        raw = _rest_call("klines", client.futures_klines, symbol=symbol, interval="1m", limit=60)
        formatted = [[
            k[0], float(k[1]), float(k[2]), float(k[3]), float(k[4]), float(k[5]),
            k[6], float(k[7]), k[8], float(k[9]), float(k[10]), k[11]
        ] for k in raw]
        with _kline_lock: _kline_cache[symbol] = formatted
        return process_candles(formatted)
    except Exception as e:
        _log_err("fetch_df", e)
        return None

# ═══════════════════════════════════════════════════════════════════════════
#  TRADING CORE (ENTRY & EXIT)
# ═══════════════════════════════════════════════════════════════════════════

def enter_position(symbol: str, signal_dir: str, score: float, signals: list, atr: float, regime: str, bias: float) -> bool:
    global is_logic_inverted, banned_short_until

    now = time.time()

    # 1. Tentukan Eksekusi Aktual Berdasarkan Mode
    if not is_logic_inverted:
        # Mode NORMAL: Sinyal strategi langsung jadi eksekusi
        execution_side = signal_dir
        mode_str = "NORMAL"
    else:
        # Mode INVERTED: Sinyal strategi diputar
        # Strategi LONG  -> Eksekusi SHORT
        # Strategi SHORT -> Eksekusi LONG
        execution_side = "SHORT" if signal_dir == "LONG" else "LONG"
        mode_str = "INVERTED"

    # 2. Cek Banned Cooldown Khusus Eksekusi SHORT
    if execution_side == "SHORT" and banned_short_until > 0 and now < banned_short_until:
        rem_banned = banned_short_until - now
        print(f"  🚫 [BANNED COOLDOWN] Eksekusi SHORT diblokir. Sisa Waktu Banned: {rem_banned:.0f} detik.")
        return False

    price = price_live(symbol)
    if price <= 0: return False

    # Veto Filters & Order Placement...
    veto, reason = btc_macro.check_veto(execution_side, now)
    if veto:
        _stats["btc_breaker_veto"] += 1
        print(f"  🚫 [{symbol}] BTC BREAKER VETO ({execution_side}): {reason}")
        return False

    has_wall, wall_type, w_price, w_qty, w_mult = order_book.check_walls(symbol, price, execution_side)
    if has_wall:
        _stats["wall_veto"] += 1
        print(f"  🧱 [{symbol}] ORDERBOOK WALL VETO ({execution_side}): {wall_type} at {w_price:.4f} ({w_mult:.1f}x avg)")
        return False

    is_spoof, spoof_reason = order_book.detect_spoofing(symbol, execution_side)
    if is_spoof:
        _stats["spoof_veto"] += 1
        print(f"  🎭 [{symbol}] SPOOFING VETO ({execution_side}): {spoof_reason}")
        return False

    q = qty(symbol, price)
    risk_params = DynamicRiskManager.calculate_levels(price, execution_side, atr)

    print(f"\n  🚀 [ENTRY TRIGGERED] {symbol} | Mode: {mode_str}")
    print(f"     Sinyal Strategi: {signal_dir} ➔ Eksekusi Aktual: {execution_side}")
    print(f"     Price: ${price:.4f} | Qty: {q} | Score: {score}")
    print(f"     TP: ${risk_params['tp_price']:.4f} ({risk_params['tp_pct']*100:.2f}%)")
    print(f"     SL: ${risk_params['sl_price']:.4f} ({risk_params['sl_pct']*100:.2f}%)")

    live_positions[symbol] = {
        "symbol": symbol,
        "side": execution_side,
        "signal_base": signal_dir,
        "entry_price": price,
        "qty": q,
        "entry_time": now,
        "tp_price": risk_params["tp_price"],
        "sl_price": risk_params["sl_price"],
        "tp_pct": risk_params["tp_pct"],
        "sl_pct": risk_params["sl_pct"],
        "score": score,
        "signals": signals,
        "atr": atr,
        "regime": regime,
        "peak_pct": 0.0,
        "mode_at_entry": mode_str
    }
    return True

def monitor_positions():
    global is_logic_inverted, banned_short_until

    if not live_positions: return
    now = time.time()

    for symbol in list(live_positions.keys()):
        pos = live_positions[symbol]
        price = price_live(symbol)
        if price <= 0: continue

        side = pos["side"]
        entry = pos["entry_price"]
        hold_sec = now - pos["entry_time"]

        pnl_pct = (price - entry) / entry if side == "LONG" else (entry - price) / entry
        if pnl_pct > pos["peak_pct"]: pos["peak_pct"] = pnl_pct

        exit_triggered = False
        exit_reason = ""

        if side == "LONG":
            if price >= pos["tp_price"]: exit_triggered, exit_reason = True, "TAKE_PROFIT"
            elif price <= pos["sl_price"]: exit_triggered, exit_reason = True, "HARD_SL"
        else:
            if price <= pos["tp_price"]: exit_triggered, exit_reason = True, "TAKE_PROFIT"
            elif price >= pos["sl_price"]: exit_triggered, exit_reason = True, "HARD_SL"

        if not exit_triggered and hold_sec >= MAX_HOLD_SECONDS:
            exit_triggered, exit_reason = True, "TIME_LIMIT"

        if exit_triggered:
            pnl_usdt = pnl_pct * (pos["qty"] * entry)
            won = pnl_usdt > 0

            # Dynamic Logic Toggle & Banned Rules
            if not won and exit_reason in ["TIME_LIMIT", "HARD_SL"]:
                prev_mode = "INVERTED" if is_logic_inverted else "NORMAL"
                is_logic_inverted = not is_logic_inverted
                new_mode = "INVERTED" if is_logic_inverted else "NORMAL"
                print(f"\n  🔄 [LOGIC TOGGLE] Loss triggered by {exit_reason}! Switching Mode: {prev_mode} -> {new_mode}")

                # Rule Banned ketika beralih kembali ke Normal dari Inverted
                if prev_mode == "INVERTED" and pos["side"] == "SHORT" and exit_reason == "TIME_LIMIT":
                    banned_short_until = now + MAX_HOLD_SECONDS
                    print(f"  ⛔ [SHORT BANNED ACTIVATED] Inverted Short Time-Limit Loss! Short Entry Banned for {MAX_HOLD_SECONDS}s (1.7 Hours).")

            _stats["trades"] += 1
            if won:
                _stats["wins"] += 1
                if exit_reason == "TAKE_PROFIT": _stats["tp_exit"] += 1
            else:
                _stats["losses"] += 1
                if exit_reason == "HARD_SL": _stats["hard_sl"] += 1
                elif exit_reason == "TIME_LIMIT": _stats["time_limit_exit"] += 1

            _stats["pnl"] += pnl_usdt
            if _stats["pnl"] > _stats["ath_pnl"]: _stats["ath_pnl"] = _stats["pnl"]
            if pnl_usdt > _stats["best"]: _stats["best"] = pnl_usdt
            if pnl_usdt < _stats["worst"]: _stats["worst"] = pnl_usdt

            trade_rec = TradeRecord(
                symbol=symbol, direction=side, entry_price=entry, exit_price=price,
                pnl=pnl_usdt, won=won, regime=pos["regime"], signals=pos["signals"],
                score=pos["score"], atr_entry=pos["atr"], hold_seconds=hold_sec,
                exit_reason=exit_reason, peak_pct=pos["peak_pct"],
                entry_ts=pos["entry_time"], exit_ts=now
            )
            learning.add_trade(trade_rec)
            trade_log.append(trade_rec)

            res_symbol = "🟢" if won else "🔴"
            print(f"\n  {res_symbol} [POSITION CLOSED] {symbol} ({side})")
            print(f"     Reason: {exit_reason} | Hold: {hold_sec:.0f}s")
            print(f"     Entry: ${entry:.4f} -> Exit: ${price:.4f}")
            print(f"     PnL: ${pnl_usdt:+.4f} ({pnl_pct*100:+.2f}%)")

            del live_positions[symbol]
            cooldown_list[symbol] = now + 120.0

# ═══════════════════════════════════════════════════════════════════════════
#  WEBSOCKET SUBSCRIBERS
# ═══════════════════════════════════════════════════════════════════════════

def _ws_mark_price_handler(msg):
    global _ws_last_msg_ts
    _ws_last_msg_ts = time.time()
    try:
        data = msg if isinstance(msg, list) else [msg]
        for item in data:
            sym = item.get("s")
            px = float(item.get("p", 0))
            if sym and px > 0:
                _ws_mark_price[sym] = (px, time.time())
                if sym == "BTCUSDT": btc_macro.update_tick(px)
    except Exception: pass

def _ws_kline_handler(msg):
    global _ws_last_msg_ts
    _ws_last_msg_ts = time.time()
    try:
        data = msg.get("data", msg)
        k = data.get("k")
        if not k: return
        sym = k.get("s")
        if not sym: return
        candle = [
            k["t"], float(k["o"]), float(k["h"]), float(k["l"]), float(k["c"]),
            float(k["v"]), k["T"], float(k["q"]), k["n"], float(k["V"]), float(k["Q"]), "0"
        ]
        with _kline_lock:
            if sym not in _kline_cache: _kline_cache[sym] = []
            kl = _kline_cache[sym]
            if kl and kl[-1][0] == candle[0]: kl[-1] = candle
            else:
                kl.append(candle)
                if len(kl) > 120: _kline_cache[sym] = kl[-100:]
    except Exception: pass

def _ws_depth_handler(msg):
    global _ws_last_msg_ts
    _ws_last_msg_ts = time.time()
    try:
        data = msg.get("data", msg)
        sym = data.get("s")
        bids = data.get("b", [])
        asks = data.get("a", [])
        if sym and (bids or asks): order_book.update(sym, bids, asks)
    except Exception: pass

def start_websockets():
    print("  🔌 Starting Binance WebSockets...")
    try:
        twm.start_futures_multiplex_socket(
            callback=_ws_mark_price_handler,
            streams=["!markPrice@arr@1s"]
        )
        kline_streams = [f"{s.lower()}@kline_1m" for s in SYMBOLS]
        for i in range(0, len(kline_streams), 20):
            twm.start_futures_multiplex_socket(
                callback=_ws_kline_handler,
                streams=kline_streams[i:i+20]
            )
        depth_streams = [f"{s.lower()}@depth10@100ms" for s in SYMBOLS[:DEPTH_SOCKET_CHUNK]]
        if depth_streams:
            twm.start_futures_multiplex_socket(
                callback=_ws_depth_handler,
                streams=depth_streams
            )
        print("  ✅ WebSockets Connected Successfully.")
    except Exception as e:
        print(f"  ⚠️ WebSocket Init Warning: {e}")

# ═══════════════════════════════════════════════════════════════════════════
#  SCANNING & WORKER THREADS
# ═══════════════════════════════════════════════════════════════════════════

def scan_symbol(symbol: str):
    if len(live_positions) >= MAX_POSITIONS: return
    if symbol in live_positions: return
    if cooldown_list.get(symbol, 0) > time.time(): return

    df = fetch_df(symbol)
    if df is None or len(df) < 55: return

    # Strict Sideway & Volume Check
    row = df.iloc[-2]
    vr = row.get("vr", 0.0)
    adx = row.get("adx", 0.0)
    atr = row.get("atr", 0.0)
    close = row.get("close", 1.0)
    atr_pct = (atr / close) if close > 0 else 0.0

    if vr < MIN_VOLUME_RATIO or adx < MIN_ADX_TREND or atr_pct < MIN_ATR_PCT:
        _stats["low_vol_veto"] += 1
        return

    sig_dir, score, sigs, atr, regime, bias = scorer.get_signal(df, symbol)
    if sig_dir and score >= MIN_SCORE:
        enter_position(symbol, sig_dir, score, sigs, atr, regime, bias)

def main_scanner_loop():
    while True:
        try:
            if len(live_positions) < MAX_POSITIONS:
                futures = [_executor.submit(scan_symbol, s) for s in SYMBOLS]
                for f in as_completed(futures):
                    try: f.result()
                    except Exception: pass
            time.sleep(SCAN_INTERVAL)
        except Exception as e:
            _log_err("scanner_loop", e)
            time.sleep(1.0)

def main_monitor_loop():
    while True:
        try:
            monitor_positions()
            time.sleep(MONITOR_INT)
        except Exception as e:
            _log_err("monitor_loop", e)
            time.sleep(0.5)

# ═══════════════════════════════════════════════════════════════════════════
#  DASHBOARD DISPLAY
# ═══════════════════════════════════════════════════════════════════════════

def print_dashboard():
    os.system("cls" if os.name == "nt" else "clear")
    now_wita = datetime.now(WITA_TZ)
    uptime = time.time() - _stats["start"]
    up_str = str(timedelta(seconds=int(uptime)))

    mode_status = "🔴 INVERTED" if is_logic_inverted else "🟢 NORMAL"
    banned_str = "OFF"
    if banned_short_until > time.time():
        rem_b = banned_short_until - time.time()
        banned_str = f"ACTIVE ({rem_b:.0f}s sisa)"

    print("══════════════════════════════════════════════════════════════════════════════════")
    print(f" 🤖 BOT SCALPING v22.0 DEMO — INSTITUTIONAL QUANT ENGINE")
    print(f" 🕒 Waktu Makassar (WITA): {now_wita.strftime('%Y-%m-%d %H:%M:%S')} | Uptime: {up_str}")
    print(f" ⚡ Mode Logika: {mode_status} | Short Banned Status: {banned_str}")
    print(f" 📊 {btc_macro.get_status_str()}")
    print("══════════════════════════════════════════════════════════════════════════════════")

    wr = (_stats["wins"] / _stats["trades"] * 100) if _stats["trades"] > 0 else 0.0
    print(f" 📈 PERFORMANCE METRICS:")
    print(f"    Total Trades : {_stats['trades']:<4} | Win Rate     : {wr:.1f}%")
    print(f"    Total PnL    : ${_stats['pnl']:<+8.4f} | ATH PnL      : ${_stats['ath_pnl']:<+8.4f}")
    print(f"    Best Trade   : ${_stats['best']:<+8.4f} | Worst Trade  : ${_stats['worst']:<+8.4f}")
    print(f"    Exit Reason  : TP={_stats['tp_exit']} | SL={_stats['hard_sl']} | TimeLimit={_stats['time_limit_exit']}")
    print(f"    Veto Filter  : Vol/Sideway={_stats['low_vol_veto']} | Wall={_stats['wall_veto']} | BTC Breaker={_stats['btc_breaker_veto']}")
    print("─" * 82)

    print(" 💼 POSISI AKTIF:")
    if not live_positions:
        print("    (Tidak Ada Posisi Aktif)")
    else:
        for sym, pos in live_positions.items():
            px = price_live(sym)
            pnl = (px - pos['entry_price'])/pos['entry_price'] if pos['side'] == 'LONG' else (pos['entry_price'] - px)/pos['entry_price']
            pnl_usdt = pnl * (pos['qty'] * pos['entry_price'])
            hold = time.time() - pos['entry_time']
            print(f"    • {sym:<10} | {pos['side']:<5} | Entry: ${pos['entry_price']:.4f} | Live: ${px:.4f}")
            print(f"      PnL: ${pnl_usdt:+.4f} ({pnl*100:+.2f}%) | Hold: {hold:.0f}s / {MAX_HOLD_SECONDS}s")
            print(f"      TP: ${pos['tp_price']:.4f} | SL: ${pos['sl_price']:.4f} | Mode: {pos['mode_at_entry']}")
    print("─" * 82)

    print(" 📜 5 RIWAYAT TRADE TERAKHIR (MAKASSAR TIMEZONE / WITA):")
    recent_trades = trade_log[-5:] if trade_log else []
    if not recent_trades:
        print("    (Belum ada riwayat trade)")
    else:
        print(f"    {'Symbol':<10} {'Side':<6} {'Entry Time (WITA)':<19} {'Exit Time (WITA)':<19} {'PnL ($)':<9} {'Exit Reason'}")
        print(f"    {'-'*75}")
        for t in reversed(recent_trades):
            entry_dt = datetime.fromtimestamp(t.entry_ts, tz=WITA_TZ).strftime('%H:%M:%S')
            exit_dt  = datetime.fromtimestamp(t.exit_ts, tz=WITA_TZ).strftime('%H:%M:%S')
            res_flag = "🟢" if t.won else "🔴"
            print(f"    {res_flag} {t.symbol:<8} {t.direction:<6} {entry_dt:<19} {exit_dt:<19} ${t.pnl:<+8.4f} {t.exit_reason}")
    print("══════════════════════════════════════════════════════════════════════════════════")

def dashboard_loop():
    while True:
        try:
            print_dashboard()
            time.sleep(1.0)
        except Exception as e:
            _log_err("dashboard_loop", e)
            time.sleep(2.0)

# ═══════════════════════════════════════════════════════════════════════════
#  MAIN ENTRY POINT
# ═══════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    print("🚀 Initializing Bot Scalping v22.0 Quant Engine...")
    start_websockets()

    # Thread Spinups
    t_scan = threading.Thread(target=main_scanner_loop, daemon=True)
    t_mon  = threading.Thread(target=main_monitor_loop, daemon=True)
    t_dash = threading.Thread(target=dashboard_loop, daemon=True)

    t_scan.start()
    t_mon.start()
    t_dash.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n🛑 Stopping Bot Executions Safely...")
        twm.stop()
        sys.exit(0)
