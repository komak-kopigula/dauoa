"""
Bot Scalping v22.0 DEMO — INSTITUTIONAL QUANT ENGINE (Binance Futures)
====================================================================
STRICT SIDEWAY & VOLUME FILTER + MAKASSAR TIMEZONE (WITA):
- Volume Filter: Wajib Volume Ratio (VR) >= 0.85 & ADX >= 20.0 (Cegah Entry Sideway)
- Volatility Filter: ATR % wajib >= 0.3% untuk pergerakan harga.
- Dynamic Logic Toggle Mode: Normal <-> Inverted saat Loss / Time Limit.
- Smart Ban Logic: Jika di Mode Inverted minus kena Time Limit, kembali ke Normal Mode.
  - Sinyal Normal yang BERLAWANAN arah langsung diperbolehkan entry.
  - Sinyal Normal yang SEARAH dengan trade Inverted yang gagal di-BAN selama 6120 detik.
- Real-time Metrics: ATH PnL, Best Single Win, Worst Single Loss.
- Last 5 Trades History dengan Timestamp Entry & Exit (Zona Waktu WITA / Makassar UTC+8).
- MAX_POSITIONS = 1 | ORDER_USDT = 3.0 USDT | LEVERAGE = 20x.
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
from concurrent.futures import ThreadPoolExecutor
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

def format_wita(ts: float) -> str:
    """Mengubah timestamp epoch ke format tanggal & jam Makassar (WITA)."""
    return datetime.fromtimestamp(ts, tz=WITA_TZ).strftime("%Y-%m-%d %H:%M:%S WITA")

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

btc_macro = BTCMacroEngine()
_btc_macro = {"regime": "UNKNOWN", "m5": 0.0, "delta_ratio": 0.0, "cvd": 0.0}

# ═══════════════════════════════════════════════════════════════════════════
#  MARKET REGIME & ABSORPTION
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
        strong_trend      = adx > 20
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

    def get_weight(self, key: str) -> float:
        return float(self.weights.get(key, 10))

class SignalScorer:
    def __init__(self, signal_weights: SignalWeights):
        self.weights = signal_weights

    def get_signal(self, df: pd.DataFrame, symbol: str = None) -> Tuple[Optional[str], int, List[str], float, str, float]:
        if df is None or len(df) < 55:
            return None, 0, [], 0.0, "UNKNOWN", 0.0
        
        # Filter Strict Volume & Sideway Market
        row = df.iloc[-2]
        vr = row.get("vr", 0.0)
        adx = row.get("adx", 0.0)
        atr = row.get("atr", 0.0)
        close = row.get("close", 1.0)
        atr_pct = (atr / close) if close > 0 else 0.0

        if vr < MIN_VOLUME_RATIO or adx < MIN_ADX_TREND or atr_pct < MIN_ATR_PCT:
            _stats["low_vol_veto"] += 1
            return None, 0, ["SIDEWAY/LOW_VOL_FILTER"], atr, "RANGE_SIDEWAY", 0.0

        regime, strength, bias = MarketRegime.detect(df)
        long_score, long_sigs = self._score_long(df, symbol)
        short_score, short_sigs = self._score_short(df, symbol)

        bull_absorb, bear_absorb, _ = AbsorptionDetector.detect(df)

        if regime == MarketRegime.REGIME_TRENDING_BULL:
            if long_score >= MIN_SCORE: return "LONG", long_score, long_sigs, atr, regime, bias
        elif regime == MarketRegime.REGIME_TRENDING_BEAR:
            if short_score >= MIN_SCORE: return "SHORT", short_score, short_sigs, atr, regime, bias
        elif regime in (MarketRegime.REGIME_RANGE, MarketRegime.REGIME_EXHAUSTION):
            if bull_absorb and long_score >= MIN_SCORE:
                return "LONG", long_score, long_sigs, atr, f"{regime}_ABSORB", bias
            if bear_absorb and short_score >= MIN_SCORE:
                return "SHORT", short_score, short_sigs, atr, f"{regime}_ABSORB", bias

        return None, max(long_score, short_score), [], atr, regime, bias

    def _score_long(self, df: pd.DataFrame, symbol: str) -> Tuple[int, List[str]]:
        row, prev, prev2 = df.iloc[-2], df.iloc[-3], df.iloc[-4]
        score, signals = 0, []
        p, e5, e9, e21, e50 = row["close"], row["e5"], row["e9"], row["e21"], row["e50"]

        if p > e5 > e9 > e21 > e50: w = self.weights.get_weight("ema_bull_stack"); score += w; signals.append(f"EMA5↑[{w:.0f}]")
        elif p > e5 > e9 > e21: w = self.weights.get_weight("ema_mild_bull"); score += w; signals.append(f"EMA4↑[{w:.0f}]")

        if row["m5"] > 0.003: w = self.weights.get_weight("mom_strong"); score += w; signals.append(f"Mom+{row['m5']*100:.1f}%↑[{w:.0f}]")
        if prev["mh"] <= 0 and row["mh"] > 0: w = self.weights.get_weight("macd_cross_up"); score += w; signals.append(f"MACD_X↑[{w:.0f}]")

        if 48 <= row["rsi"] <= 68: w = self.weights.get_weight("rsi_bull_flow"); score += w; signals.append(f"RSI{row['rsi']:.0f}[{w:.0f}]")
        return int(score), signals

    def _score_short(self, df: pd.DataFrame, symbol: str) -> Tuple[int, List[str]]:
        row, prev, prev2 = df.iloc[-2], df.iloc[-3], df.iloc[-4]
        score, signals = 0, []
        p, e5, e9, e21, e50 = row["close"], row["e5"], row["e9"], row["e21"], row["e50"]

        if p < e5 < e9 < e21 < e50: w = self.weights.get_weight("ema_bear_stack"); score += w; signals.append(f"EMA5↓[{w:.0f}]")
        elif p < e5 < e9 < e21: w = self.weights.get_weight("ema_mild_bear"); score += w; signals.append(f"EMA4↓[{w:.0f}]")

        if row["m5"] < -0.003: w = self.weights.get_weight("mom_strong_neg"); score += w; signals.append(f"Mom{row['m5']*100:.1f}%↓[{w:.0f}]")
        if prev["mh"] >= 0 and row["mh"] < 0: w = self.weights.get_weight("macd_cross_down"); score += w; signals.append(f"MACD_X↓[{w:.0f}]")

        if 32 <= row["rsi"] <= 52: w = self.weights.get_weight("rsi_bear_flow"); score += w; signals.append(f"RSI{row['rsi']:.0f}[{w:.0f}]")
        return int(score), signals

# ═══════════════════════════════════════════════════════════════════════════
#  GLOBAL STATE, TRADE HISTORY & SMART BAN ENGINE
# ═══════════════════════════════════════════════════════════════════════════

@dataclass
class TradeRecord:
    symbol:       str
    mode:         str       # "Normal" / "Inverted"
    direction:    str       # "LONG" / "SHORT"
    entry_price:  float
    exit_price:   float
    pnl:          float
    won:          bool
    exit_reason:  str
    entry_time:   float
    exit_time:    float = field(default_factory=time.time)

_stats = {
    "trades": 0, "wins": 0, "losses": 0, "pnl": 0.0, "best": 0.0, "worst": 0.0, "ath_pnl": 0.0,
    "hard_sl": 0, "tp_exit": 0, "time_limit_exit": 0, "low_vol_veto": 0,
    "start": time.time(),
}

# Variable Toggle Invert Logika
is_logic_inverted = False 
banned_direction_until = {"LONG": 0.0, "SHORT": 0.0}

# Storing Last 5 Trades
last_5_trades = deque(maxlen=5)

live_positions = {}
signal_weights = SignalWeights()
scorer         = SignalScorer(signal_weights)

_rest_lock = threading.Lock()
_rest_last_ts = 0.0
_rest_block_until = 0.0

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
            time.sleep(0.5)
    if last_exc is not None: raise last_exc

def process_klines(klines) -> Optional[pd.DataFrame]:
    """Mengolah klines candlestick menjadi indikator teknikal."""
    if not klines or len(klines) < 55: return None
    try:
        df = pd.DataFrame(klines, columns=[
            "open_time", "open", "high", "low", "close", "volume",
            "close_time", "quote_vol", "trades", "taker_buy_vol", "taker_buy_quote_vol", "ignore"
        ])
        for col in ["open", "high", "low", "close", "volume", "quote_vol", "taker_buy_vol"]:
            df[col] = df[col].astype(float)

        close = df["close"]
        high  = df["high"]
        low   = df["low"]
        vol   = df["volume"]

        df["e5"]  = ta.trend.ema_indicator(close, window=5)
        df["e9"]  = ta.trend.ema_indicator(close, window=9)
        df["e21"] = ta.trend.ema_indicator(close, window=21)
        df["e50"] = ta.trend.ema_indicator(close, window=50)

        df["rsi"] = ta.momentum.rsi(close, window=14)
        
        macd_ind = ta.trend.MACD(close, window_slow=26, window_fast=12, window_sign=9)
        df["mh"] = macd_ind.macd_diff()

        adx_ind = ta.trend.ADXIndicator(high, low, close, window=14)
        df["adx"] = adx_ind.adx()

        atr_ind = ta.volatility.AverageTrueRange(high, low, close, window=14)
        df["atr"] = atr_ind.average_true_range()

        vol_sma = vol.rolling(20).mean()
        df["vr"] = vol / (vol_sma + 1e-9)

        df["m5"] = (close - close.shift(5)) / (close.shift(5) + 1e-9)
        df["rng"] = high - low
        df["delta_ratio"] = (df["taker_buy_vol"] - (vol - df["taker_buy_vol"])) / (vol + 1e-9)
        df["br"] = df["taker_buy_vol"] / (vol + 1e-9)
        df["lower_wick_ratio"] = (np.minimum(df["open"], close) - low) / (df["rng"] + 1e-9)
        df["upper_wick_ratio"] = (high - np.maximum(df["open"], close)) / (df["rng"] + 1e-9)

        return df.dropna()
    except Exception:
        return None

# ═══════════════════════════════════════════════════════════════════════════
#  POSITION MONITORING & TRADE EXIT HANDLER
# ═══════════════════════════════════════════════════════════════════════════

def close_position_and_log(symbol: str, exit_price: float, exit_reason: str):
    global is_logic_inverted, banned_direction_until

    pos = live_positions.pop(symbol, None)
    if not pos: return

    entry_price = pos["entry_price"]
    exec_side   = pos["exec_side"]
    raw_signal  = pos["raw_signal"]
    mode_str    = pos["mode"]
    entry_time  = pos["entry_time"]
    exit_time   = time.time()

    # Hitung PnL Realized
    if exec_side == "LONG":
        pnl_pct = (exit_price - entry_price) / entry_price
    else:
        pnl_pct = (entry_price - exit_price) / entry_price

    pnl_usdt = pnl_pct * (ORDER_USDT * LEVERAGE)
    won = pnl_usdt > 0

    # Update Global Stats
    _stats["trades"] += 1
    _stats["pnl"] += pnl_usdt
    if won:
        _stats["wins"] += 1
        if pnl_usdt > _stats["best"]: _stats["best"] = pnl_usdt
    else:
        _stats["losses"] += 1
        if pnl_usdt < _stats["worst"]: _stats["worst"] = pnl_usdt

    if _stats["pnl"] > _stats["ath_pnl"]:
        _stats["ath_pnl"] = _stats["pnl"]

    if exit_reason == "TAKE_PROFIT": _stats["tp_exit"] += 1
    elif exit_reason == "HARD_SL": _stats["hard_sl"] += 1
    elif exit_reason == "TIME_LIMIT": _stats["time_limit_exit"] += 1

    # Catat ke Riwayat 5 Trade Terakhir
    trade_rec = TradeRecord(
        symbol=symbol,
        mode=mode_str,
        direction=exec_side,
        entry_price=entry_price,
        exit_price=exit_price,
        pnl=pnl_usdt,
        won=won,
        exit_reason=exit_reason,
        entry_time=entry_time,
        exit_time=exit_time
    )
    last_5_trades.append(trade_rec)

    print(f"\n  🔔 [TRADE CLOSED] {symbol} | Mode: {mode_str} | Side: {exec_side} | PnL: ${pnl_usdt:+.2f} | Reason: {exit_reason}")

    # ═══════════════════════════════════════════════════════════════════════
    #  LOGIKA UTAMA: DYNAMIC TOGGLE MODE & SMART BANNING MECHANISM
    # ═══════════════════════════════════════════════════════════════════════
    if not won or exit_reason == "TIME_LIMIT":
        if not is_logic_inverted:
            # Dari Normal Mode minus / Time Limit -> Switch ke Inverted Mode
            is_logic_inverted = True
            print(f"  🔄 [MODE SWITCH] Mode Normal -> INVERTED MODE (Alasan: {exit_reason}, Loss: ${pnl_usdt:.2f})")
        else:
            # Dari Inverted Mode minus / Time Limit -> Kembali ke Normal Mode
            is_logic_inverted = False
            # Banned direction yang gagal pada Mode Inverted selama 6120 detik
            banned_direction_until[exec_side] = time.time() + MAX_HOLD_SECONDS
            rem_m = MAX_HOLD_SECONDS / 60
            print(f"  🔄 [MODE SWITCH] Mode Inverted -> NORMAL MODE")
            print(f"  ⛔ [BAN TRIGGERED] Direction '{exec_side}' di-BAN selama {MAX_HOLD_SECONDS} detik ({rem_m:.0f} menit) di Normal Mode karena gagal Time Limit pada Mode Inverted!")

def monitor_positions():
    """Thread mandiri untuk memantau SL, TP, dan Time Limit posisi secara real-time."""
    while True:
        try:
            now = time.time()
            for sym in list(live_positions.keys()):
                pos = live_positions.get(sym)
                if not pos: continue

                # Ambil Harga Live
                try:
                    ticker = _rest_call("get_ticker", client.futures_symbol_ticker, symbol=sym)
                    curr_price = float(ticker["price"])
                except Exception:
                    continue

                exec_side = pos["exec_side"]
                tp_price  = pos["tp_price"]
                sl_price  = pos["sl_price"]
                hold_sec  = now - pos["entry_time"]

                # Check TP / SL / Time Limit
                if exec_side == "LONG":
                    if curr_price >= tp_price:
                        close_position_and_log(sym, curr_price, "TAKE_PROFIT")
                    elif curr_price <= sl_price:
                        close_position_and_log(sym, curr_price, "HARD_SL")
                    elif hold_sec >= MAX_HOLD_SECONDS:
                        close_position_and_log(sym, curr_price, "TIME_LIMIT")
                else: # SHORT
                    if curr_price <= tp_price:
                        close_position_and_log(sym, curr_price, "TAKE_PROFIT")
                    elif curr_price >= sl_price:
                        close_position_and_log(sym, curr_price, "HARD_SL")
                    elif hold_sec >= MAX_HOLD_SECONDS:
                        close_position_and_log(sym, curr_price, "TIME_LIMIT")

        except Exception as e:
            pass
        time.sleep(MONITOR_INT)

# ═══════════════════════════════════════════════════════════════════════════
#  DISPLAY DASHBOARD & RIWAYAT 5 TRADE
# ═══════════════════════════════════════════════════════════════════════════

def print_dashboard():
    """Menampilkan status bot, mode aktif, status ban, dan 5 trade terakhir."""
    now = time.time()
    mode_status = "INVERTED MODE 🔄" if is_logic_inverted else "NORMAL MODE ⚡"
    
    # Ban Status String
    ban_info = []
    for side in ["LONG", "SHORT"]:
        if now < banned_direction_until[side]:
            rem_s = banned_direction_until[side] - now
            ban_info.append(f"{side} BANNED ({rem_s:.0f}s left)")
    ban_str = ", ".join(ban_info) if ban_info else "Tidak Ada Ban Active"

    print("\n" + "═"*90)
    print(f" 🤖 BOT SCALPING v22.0 | Mode: [{mode_status}] | Status Ban: [{ban_str}]")
    print(f" 💰 Total PnL: ${_stats['pnl']:+.2f} | ATH PnL: ${_stats['ath_pnl']:+.2f} | Winrate: {(_stats['wins']/max(1,_stats['trades']))*100:.1f}%")
    print(f" 📊 Trades: {_stats['trades']} | Win: {_stats['wins']} | Loss: {_stats['losses']} | Best: ${_stats['best']:+.2f} | Worst: ${_stats['worst']:+.2f}")
    print(f" 🚫 Sideway/Vol Vetoed: {_stats['low_vol_veto']} | Time Limit Exits: {_stats['time_limit_exit']}")
    print("═"*90)

    print(" 📜 LAST 5 TRADES HISTORY (ZONA WAKTU MAKASSAR / WITA UTC+8):")
    print("─"*90)
    if not last_5_trades:
        print("  Belum ada riwayat transaksi yang selesai.")
    else:
        print(f"  {'Symbol':<10} | {'Mode & Side':<16} | {'Entry Time (WITA)':<20} | {'Exit Time (WITA)':<20} | {'PnL ($)':<10} | {'Reason':<10}")
        print("  " + "─"*86)
        for t in list(last_5_trades):
            e_time = format_wita(t.entry_time)
            x_time = format_wita(t.exit_time)
            mode_side = f"{t.mode} {t.direction}"
            pnl_c = f"${t.pnl:+.2f}"
            print(f"  {t.symbol:<10} | {mode_side:<16} | {e_time:<20} | {x_time:<20} | {pnl_c:<10} | {t.exit_reason:<10}")
    print("═"*90 + "\n")

# ═══════════════════════════════════════════════════════════════════════════
#  MAIN BOT LOOP & ENTRY EXECUTION
# ═══════════════════════════════════════════════════════════════════════════

def run_bot():
    print("🚀 Memulai Quant Engine Bot Scalping v22.0...")
    
    # Run Background Position Monitor Thread
    t_monitor = threading.Thread(target=monitor_positions, daemon=True)
    t_monitor.start()

    last_dash_time = 0.0

    while True:
        try:
            now = time.time()
            if now - last_dash_time >= 10.0:
                print_dashboard()
                last_dash_time = now

            # Jika slot posisi penuh, lewati scanning entry
            if len(live_positions) >= MAX_POSITIONS:
                time.sleep(SCAN_INTERVAL)
                continue

            for symbol in SYMBOLS:
                if len(live_positions) >= MAX_POSITIONS:
                    break

                # Ambil Klines
                try:
                    raw_klines = _rest_call("get_klines", client.futures_klines, symbol=symbol, interval="1m", limit=80)
                    df = process_klines(raw_klines)
                except Exception:
                    continue

                if df is None: continue

                # Check Signal
                raw_sig, score, sigs, atr, regime, bias = scorer.get_signal(df, symbol)
                if not raw_sig: continue

                # Tentukan Executed Side berdasarkan Mode Logika
                if not is_logic_inverted:
                    exec_side = raw_sig
                    curr_mode = "Normal"
                else:
                    exec_side = "SHORT" if raw_sig == "LONG" else "LONG"
                    curr_mode = "Inverted"

                # ═══════════════════════════════════════════════════════════
                # CHECK BAN FILTER (KHUSUS MODE NORMAL)
                # ═══════════════════════════════════════════════════════════
                if not is_logic_inverted:
                    if now < banned_direction_until.get(exec_side, 0.0):
                        rem_sec = banned_direction_until[exec_side] - now
                        print(f"  🚫 [ENTRY BANNED] Sinyal Normal '{exec_side}' untuk {symbol} di-BAN! Sisa waktu ban: {rem_sec:.0f} detik.")
                        continue

                # Ambil Harga Entry Live
                ticker = _rest_call("get_ticker", client.futures_symbol_ticker, symbol=symbol)
                entry_price = float(ticker["price"])

                # Hitung Dynamic Risk TP & SL
                atr_pct = atr / entry_price
                tp_pct  = max(MIN_TP_PCT, min(MAX_TP_PCT, ATR_TP_RESTORED_MULTIPLIER * atr_pct))
                sl_pct  = max(MIN_SL_PCT, min(MAX_SL_PCT, ATR_SL_RESTORED_MULTIPLIER * atr_pct))

                if exec_side == "LONG":
                    tp_price = entry_price * (1 + tp_pct)
                    sl_price = entry_price * (1 - sl_pct)
                else: # SHORT
                    tp_price = entry_price * (1 - tp_pct)
                    sl_price = entry_price * (1 + sl_pct)

                # Eksekusi SIMULASI POSISI (PAPER TRADING)
                live_positions[symbol] = {
                    "symbol": symbol,
                    "raw_signal": raw_sig,
                    "exec_side": exec_side,
                    "mode": curr_mode,
                    "entry_price": entry_price,
                    "tp_price": tp_price,
                    "sl_price": sl_price,
                    "entry_time": now
                }

                entry_wita_str = format_wita(now)
                print(f"\n  🎯 [ENTRY EXECUTION] {symbol} | Mode: {curr_mode} | Executed Side: {exec_side} (Raw: {raw_sig})")
                print(f"     Harga Entry: ${entry_price:.4f} | TP: ${tp_price:.4f} (+{tp_pct*100:.2f}%) | SL: ${sl_price:.4f} (-{sl_pct*100:.2f}%)")
                print(f"     Waktu Entry (Makassar): {entry_wita_str}")

                time.sleep(SLOT_FILL_INT)

        except KeyboardInterrupt:
            print("\n  🛑 Bot Dihentikan Oleh Pengguna.")
            break
        except Exception as e:
            time.sleep(2.0)

if __name__ == "__main__":
    run_bot()
