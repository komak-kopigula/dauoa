"""
Bot Scalping v22.0 DEMO — INSTITUTIONAL QUANT ENGINE (Binance Futures)
====================================================================
UPDATED STRATEGY & ANTI-SPAM LOGGING:
- Status posisi & PnL live di-update setiap 5 detik (ringkas 1 baris).
- Dashboard Keseluruhan & 5 Trade History di-print setiap 60 detik (1 menit).
- Double Loss Protection: Pause 15 menit jika rugi berturut-turut (Normal -> Inverted).
- Dynamic Soft Ban: Masa ban 15 menit, bisa dimasuki jika Score >= 80 atau di-reset via MSS.
- Strict Volume & Sideway Filter (VR >= 0.85, ADX >= 20.0, ATR % >= 0.3%).
- Timestamp Zona Waktu Makassar (WITA / UTC+8).
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
from dataclasses import dataclass, field
from typing import Optional, Tuple, List, Dict, Any

from dotenv import load_dotenv
from binance.client import Client
import ta

load_dotenv()
api_key = os.getenv("API_KEY")
api_secret = os.getenv("API_SECRET")

try:
    client = Client(api_key, api_secret)
except Exception:
    client = Client(api_key, api_secret)

PAPER_TRADING = True
client.FUTURES_URL = "https://fapi.binance.com/fapi"

# Definisi Zona Waktu Makassar (WITA / UTC+8)
WITA_TZ = timezone(timedelta(hours=8))

def format_wita(ts: float) -> str:
    """Mengubah timestamp epoch ke format tanggal & jam Makassar (WITA)."""
    return datetime.fromtimestamp(ts, tz=WITA_TZ).strftime("%Y-%m-%d %H:%M:%S WITA")

# ═══════════════════════════════════════════════════════════════════════════
#  CONFIGURATION & STRATEGY PARAMETERS
# ═══════════════════════════════════════════════════════════════════════════

LEVERAGE      = 20
ORDER_USDT    = 3.0
MAX_POSITIONS = 1

# Strict Volume & Sideway Filters
MIN_VOLUME_RATIO = 0.85   # Volume minimal 85% dari rata-rata 20 candle
MIN_ADX_TREND    = 20.0   # ADX minimal 20.0 (Cegah entry market mati)
MIN_ATR_PCT      = 0.003  # Volatilitas minimal 0.3%

# Scoring Thresholds
MIN_SCORE_NORMAL   = 58   # Batas minimal skor sinyal standar
MIN_SCORE_SOFT_BAN = 80   # Batas minimal skor sinyal untuk arah yang di-soft ban

# Durasi Proteksi & Risk Management
MAX_HOLD_SECONDS     = 6120  # Batas maksimal tahan posisi (102 menit)
SOFT_BAN_DURATION    = 900   # Masa Soft Ban arah gagal (15 menit / 15 candle)
NEUTRAL_PAUSE_SEC    = 900   # Masa Pause saat Double Loss (15 menit)

# Multiplier TP/SL berbasis ATR
ATR_TP_RESTORED_MULTIPLIER = 3.5
ATR_SL_RESTORED_MULTIPLIER = 1.8
MIN_TP_PCT        = 0.025
MAX_TP_PCT        = 0.035
MIN_SL_PCT        = 0.015
MAX_SL_PCT        = 0.025

# Interval Logging Console
STATUS_LOG_INTERVAL = 5.0   # Update status ringkas / PnL live per 5 detik
DASH_LOG_INTERVAL   = 60.0  # Summary Dashboard & History per 60 detik (1 menit)
SCAN_INTERVAL       = 2.0
MONITOR_INT         = 0.2
REST_MIN_INTERVAL   = 0.20

# ═══════════════════════════════════════════════════════════════════════════
#  SYMBOLS
# ═══════════════════════════════════════════════════════════════════════════
SYMBOLS = [
    "BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT",
    "ADAUSDT", "DOGEUSDT", "AVAXUSDT", "TRXUSDT", "DOTUSDT",
    "LINKUSDT", "MATICUSDT", "LTCUSDT", "ATOMUSDT", "UNIUSDT",
    "NEARUSDT", "APTUSDT", "ARBUSDT", "OPUSDT", "INJUSDT",
    "SUIUSDT", "SEIUSDT", "FETUSDT", "WLDUSDT", "AAVEUSDT",
]
SYMBOLS = list(dict.fromkeys(SYMBOLS))

# ═══════════════════════════════════════════════════════════════════════════
#  REST API SAFETY & KLINES PROCESSOR
# ═══════════════════════════════════════════════════════════════════════════

_rest_lock = threading.Lock()
_rest_last_ts = 0.0

def _rest_call(tag, fn, *args, retries=1, **kwargs):
    global _rest_last_ts
    last_exc = None
    for attempt in range(retries + 1):
        with _rest_lock:
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
        macd_ind  = ta.trend.MACD(close, window_slow=26, window_fast=12, window_sign=9)
        df["mh"]  = macd_ind.macd_diff()

        adx_ind   = ta.trend.ADXIndicator(high, low, close, window=14)
        df["adx"]  = adx_ind.adx()

        atr_ind   = ta.volatility.AverageTrueRange(high, low, close, window=14)
        df["atr"]  = atr_ind.average_true_range()

        vol_sma   = vol.rolling(20).mean()
        df["vr"]   = vol / (vol_sma + 1e-9)

        df["m5"]   = (close - close.shift(5)) / (close.shift(5) + 1e-9)
        df["rng"]  = high - low

        return df.dropna()
    except Exception:
        return None

# ═══════════════════════════════════════════════════════════════════════════
#  SCORING & MARKET STRUCTURE SHIFT (MSS) ENGINE
# ═══════════════════════════════════════════════════════════════════════════

class SignalScorer:
    def get_signal(self, df: pd.DataFrame) -> Tuple[Optional[str], int, float]:
        if df is None or len(df) < 55: return None, 0, 0.0
        
        row = df.iloc[-2]
        vr, adx, atr, close = row.get("vr", 0.0), row.get("adx", 0.0), row.get("atr", 0.0), row.get("close", 1.0)
        atr_pct = (atr / close) if close > 0 else 0.0

        if vr < MIN_VOLUME_RATIO or adx < MIN_ADX_TREND or atr_pct < MIN_ATR_PCT:
            _stats["low_vol_veto"] += 1
            return None, 0, atr

        long_score = self._score_long(df)
        short_score = self._score_short(df)

        if long_score >= MIN_SCORE_NORMAL and long_score > short_score:
            return "LONG", long_score, atr
        elif short_score >= MIN_SCORE_NORMAL and short_score > long_score:
            return "SHORT", short_score, atr

        return None, max(long_score, short_score), atr

    def _score_long(self, df: pd.DataFrame) -> int:
        row, prev = df.iloc[-2], df.iloc[-3]
        score = 0
        p, e5, e9, e21, e50 = row["close"], row["e5"], row["e9"], row["e21"], row["e50"]

        if p > e5 > e9 > e21 > e50: score += 30
        elif p > e5 > e9 > e21: score += 20

        if row["m5"] > 0.003: score += 25
        if prev["mh"] <= 0 and row["mh"] > 0: score += 22
        if 48 <= row["rsi"] <= 68: score += 15
        return score

    def _score_short(self, df: pd.DataFrame) -> int:
        row, prev = df.iloc[-2], df.iloc[-3]
        score = 0
        p, e5, e9, e21, e50 = row["close"], row["e5"], row["e9"], row["e21"], row["e50"]

        if p < e5 < e9 < e21 < e50: score += 30
        elif p < e5 < e9 < e21: score += 20

        if row["m5"] < -0.003: score += 25
        if prev["mh"] >= 0 and row["mh"] < 0: score += 22
        if 32 <= row["rsi"] <= 52: score += 15
        return score

def check_mss_unban(df: pd.DataFrame, banned_side: str) -> Tuple[bool, str]:
    """Deteksi Market Structure Shift (MSS) untuk batalkan Soft Ban secara prematur."""
    if df is None or len(df) < 25: return False, ""
    row = df.iloc[-2]
    adx = row.get("adx", 0.0)
    close = row.get("close", 0.0)

    if adx < 22.0: return False, "" # Perlu tren yang terkonfirmasi

    recent_high = df["high"].iloc[-22:-2].max()
    recent_low = df["low"].iloc[-22:-2].min()

    if banned_side == "SHORT" and close < recent_low:
        return True, f"Bearish Breakdown (Close < Low 20-candle: ${recent_low:.4f})"
    elif banned_side == "LONG" and close > recent_high:
        return True, f"Bullish Breakout (Close > High 20-candle: ${recent_high:.4f})"

    return False, ""

# ═══════════════════════════════════════════════════════════════════════════
#  GLOBAL STATE & TRADE HISTORY
# ═══════════════════════════════════════════════════════════════════════════

@dataclass
class TradeRecord:
    symbol:       str
    mode:         str
    direction:    str
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

# State Bot & Variable Toggle Mode
bot_state = "NORMAL" # NORMAL | INVERTED | NEUTRAL_PAUSE
is_logic_inverted = False 
neutral_pause_until = 0.0
banned_direction_until = {"LONG": 0.0, "SHORT": 0.0}

last_5_trades = deque(maxlen=5)
live_positions = {}
positions_lock = threading.Lock()
scorer = SignalScorer()

# ═══════════════════════════════════════════════════════════════════════════
#  POSITION MONITORING & TRADE EXIT HANDLER
# ═══════════════════════════════════════════════════════════════════════════

def close_position_and_log(symbol: str, exit_price: float, exit_reason: str):
    global bot_state, is_logic_inverted, neutral_pause_until, banned_direction_until

    with positions_lock:
        pos = live_positions.pop(symbol, None)

    if not pos: return

    entry_price = pos["entry_price"]
    exec_side   = pos["exec_side"]
    mode_str    = pos["mode"]
    entry_time  = pos["entry_time"]
    exit_time   = time.time()

    # Hitung Realized PnL
    if exec_side == "LONG":
        pnl_pct = ((exit_price - entry_price) / entry_price) * LEVERAGE
    else:
        pnl_pct = ((entry_price - exit_price) / entry_price) * LEVERAGE

    pnl_usdt = pnl_pct * ORDER_USDT
    won = pnl_usdt > 0

    # Update Statistics
    _stats["trades"] += 1
    _stats["pnl"] += pnl_usdt
    if won:
        _stats["wins"] += 1
        if pnl_usdt > _stats["best"]: _stats["best"] = pnl_usdt
    else:
        _stats["losses"] += 1
        if pnl_usdt < _stats["worst"]: _stats["worst"] = pnl_usdt

    if _stats["pnl"] > _stats["ath_pnl"]: _stats["ath_pnl"] = _stats["pnl"]

    if exit_reason == "TAKE_PROFIT": _stats["tp_exit"] += 1
    elif exit_reason == "HARD_SL": _stats["hard_sl"] += 1
    elif exit_reason == "TIME_LIMIT": _stats["time_limit_exit"] += 1

    # Simpan ke Last 5 Trades
    last_5_trades.append(TradeRecord(
        symbol=symbol, mode=mode_str, direction=exec_side,
        entry_price=entry_price, exit_price=exit_price, pnl=pnl_usdt,
        won=won, exit_reason=exit_reason, entry_time=entry_time, exit_time=exit_time
    ))

    print(f"\n  🔔 [TRADE CLOSED] {symbol} | Mode: {mode_str} | Side: {exec_side} | PnL: ${pnl_usdt:+.2f} ({pnl_pct*100:+.2f}%) | Reason: {exit_reason}")

    # ═══════════════════════════════════════════════════════════════════════
    # LOGIKA BARU: DOUBLE LOSS PAUSE & DYNAMIC SOFT BAN
    # ═══════════════════════════════════════════════════════════════════════
    if not won or exit_reason == "TIME_LIMIT":
        if bot_state == "NORMAL":
            bot_state = "INVERTED"
            is_logic_inverted = True
            print(f"  🔄 [MODE SWITCH] Mode Normal -> INVERTED MODE (Loss: ${pnl_usdt:+.2f})")
        
        elif bot_state == "INVERTED":
            # DOUBLE LOSS DETECTED! Masuk Neutral Pause untuk cegah Whipsaw Market
            bot_state = "NEUTRAL_PAUSE"
            neutral_pause_until = time.time() + NEUTRAL_PAUSE_SEC
            is_logic_inverted = False
            
            # Terapkan Soft Ban pada arah yang gagal di Inverted Mode selama 15 menit
            banned_direction_until[exec_side] = time.time() + SOFT_BAN_DURATION
            
            print(f"  🚨 [DOUBLE LOSS DETECTED] Loss berturut-turut pada Mode Inverted!")
            print(f"  ⏸️ [NEUTRAL PAUSE] Bot di-PAUSE selama 15 Menit untuk proteksi pasar Whipsaw.")
            print(f"  ⚠️ [SOFT BAN] Arah '{exec_side}' Soft-Banned 15m (Score butuh >= {MIN_SCORE_SOFT_BAN} untuk entry).")
    else:
        # Jika Trade MENANG pada Mode Inverted -> Kembalikan ke Normal Mode
        if bot_state == "INVERTED":
            bot_state = "NORMAL"
            is_logic_inverted = False
            print(f"  ✅ [INVERTED WIN] Profit ${pnl_usdt:+.2f}! Mode dikembalikan ke NORMAL MODE.")

def monitor_positions():
    """Background Thread: Cek SL, TP, & Time Limit posisi real-time."""
    while True:
        try:
            now = time.time()
            with positions_lock:
                active_symbols = list(live_positions.keys())

            for sym in active_symbols:
                with positions_lock:
                    pos = live_positions.get(sym)
                if not pos: continue

                try:
                    ticker = _rest_call("get_ticker", client.futures_symbol_ticker, symbol=sym)
                    curr_price = float(ticker["price"])
                except Exception:
                    continue

                exec_side = pos["exec_side"]
                tp_price  = pos["tp_price"]
                sl_price  = pos["sl_price"]
                hold_sec  = now - pos["entry_time"]

                if exec_side == "LONG":
                    if curr_price >= tp_price: close_position_and_log(sym, curr_price, "TAKE_PROFIT")
                    elif curr_price <= sl_price: close_position_and_log(sym, curr_price, "HARD_SL")
                    elif hold_sec >= MAX_HOLD_SECONDS: close_position_and_log(sym, curr_price, "TIME_LIMIT")
                else: # SHORT
                    if curr_price <= tp_price: close_position_and_log(sym, curr_price, "TAKE_PROFIT")
                    elif curr_price >= sl_price: close_position_and_log(sym, curr_price, "HARD_SL")
                    elif hold_sec >= MAX_HOLD_SECONDS: close_position_and_log(sym, curr_price, "TIME_LIMIT")

        except Exception:
            pass
        time.sleep(MONITOR_INT)

# ═══════════════════════════════════════════════════════════════════════════
#  ANTI-SPAM LOGGING SYSTEM (STATUS 5s & DASHBOARD 1m)
# ═══════════════════════════════════════════════════════════════════════════

def print_quick_status(now: float):
    """Update status ringkas 1 baris per 5 detik."""
    with positions_lock:
        has_pos = len(live_positions) > 0
        pos_items = list(live_positions.items())

    if has_pos:
        for sym, pos in pos_items:
            try:
                ticker = _rest_call("get_ticker", client.futures_symbol_ticker, symbol=sym)
                curr_price = float(ticker["price"])
            except Exception:
                curr_price = pos["entry_price"]

            entry_price = pos["entry_price"]
            exec_side   = pos["exec_side"]
            mode_str    = pos["mode"]
            hold_min    = (now - pos["entry_time"]) / 60.0

            if exec_side == "LONG":
                pnl_pct = ((curr_price - entry_price) / entry_price) * LEVERAGE * 100
            else:
                pnl_pct = ((entry_price - curr_price) / entry_price) * LEVERAGE * 100

            pnl_usdt = (pnl_pct / 100) * ORDER_USDT
            print(f"  📈 [POSISI AKTIF] {sym} | Mode: {mode_str} | Side: {exec_side} | Entry: ${entry_price:.4f} | Live: ${curr_price:.4f} | PnL: {pnl_pct:+.2f}% (${pnl_usdt:+.2f}) | Duration: {hold_min:.1f}m")
    else:
        if bot_state == "NEUTRAL_PAUSE":
            rem_s = max(0.0, neutral_pause_until - now)
            print(f"  ⏸️ [STATUS] Bot NEUTRAL PAUSE | Whipsaw Protection Active | Sisa Pause: {rem_s:.0f}s")
        else:
            mode_str = "INVERTED 🔄" if is_logic_inverted else "NORMAL ⚡"
            bans = []
            for side in ["LONG", "SHORT"]:
                if now < banned_direction_until[side]:
                    rem = banned_direction_until[side] - now
                    bans.append(f"{side} Soft-Banned ({rem:.0f}s)")
            ban_txt = ", ".join(bans) if bans else "None"
            print(f"  🔎 [STATUS] Mode: {mode_str} | Posisi: 0/{MAX_POSITIONS} | Scanning Market... | Soft Ban: {ban_txt}")

def print_dashboard():
    """Menampilkan Dashboard Lengkap & 5 Trade History per 1 menit (60 detik)."""
    now = time.time()
    mode_status = f"{bot_state} MODE" + (" 🔄" if is_logic_inverted else " ⚡")
    
    ban_info = []
    for side in ["LONG", "SHORT"]:
        if now < banned_direction_until[side]:
            rem_s = banned_direction_until[side] - now
            ban_info.append(f"{side} Soft-Banned ({rem_s:.0f}s left)")
    ban_str = ", ".join(ban_info) if ban_info else "Tidak Ada Ban Active"

    print("\n" + "═"*90)
    print(f" 🤖 BOT SCALPING v22.0 | Mode: [{mode_status}] | Status Ban: [{ban_str}]")
    print(f" 💰 Total PnL: ${_stats['pnl']:+.2f} | ATH PnL: ${_stats['ath_pnl']:+.2f} | Winrate: {(_stats['wins']/max(1,_stats['trades']))*100:.1f}%")
    print(f" 📊 Trades: {_stats['trades']} | Win: {_stats['wins']} | Loss: {_stats['losses']} | Best: ${_stats['best']:+.2f} | Worst: ${_stats['worst']:+.2f}")
    print(f" 🚫 Sideway Veto: {_stats['low_vol_veto']} | Time Limit Exits: {_stats['time_limit_exit']}")
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
#  MAIN BOT LOOP
# ═══════════════════════════════════════════════════════════════════════════

def run_bot():
    global bot_state, is_logic_inverted

    print("🚀 Memulai Quant Engine Bot Scalping v22.0...")
    
    t_monitor = threading.Thread(target=monitor_positions, daemon=True)
    t_monitor.start()

    last_dash_time = 0.0
    last_status_time = 0.0

    while True:
        try:
            now = time.time()

            # 1. Output Dashboard Summary per 60 Detik (1 Menit)
            if now - last_dash_time >= DASH_LOG_INTERVAL:
                print_dashboard()
                last_dash_time = now

            # 2. Output Ringkas status / PnL Live per 5 Detik
            if now - last_status_time >= STATUS_LOG_INTERVAL:
                print_quick_status(now)
                last_status_time = now

            # Handler Neutral Pause
            if bot_state == "NEUTRAL_PAUSE":
                if now >= neutral_pause_until:
                    bot_state = "NORMAL"
                    print(f"  ✅ [PAUSE SELESAI] Masa Neutral Pause berakhir. Bot kembali aktif di NORMAL MODE.")
                else:
                    time.sleep(SCAN_INTERVAL)
                    continue

            # Check Kapasitas Posisi
            with positions_lock:
                pos_count = len(live_positions)

            if pos_count >= MAX_POSITIONS:
                time.sleep(SCAN_INTERVAL)
                continue

            # Scanning Markets
            for symbol in SYMBOLS:
                with positions_lock:
                    if len(live_positions) >= MAX_POSITIONS: break

                try:
                    raw_klines = _rest_call("get_klines", client.futures_klines, symbol=symbol, interval="1m", limit=80)
                    df = process_klines(raw_klines)
                except Exception:
                    continue

                if df is None: continue

                # Analisa Sinyal & Skor
                raw_sig, score, atr = scorer.get_signal(df)
                if not raw_sig: continue

                # Tentukan Executed Side
                if not is_logic_inverted:
                    exec_side = raw_sig
                    curr_mode = "Normal"
                else:
                    exec_side = "SHORT" if raw_sig == "LONG" else "LONG"
                    curr_mode = "Inverted"

                # ═══════════════════════════════════════════════════════════
                # CHECK SOFT BAN & MARKET STRUCTURE SHIFT (MSS)
                # ═══════════════════════════════════════════════════════════
                if now < banned_direction_until.get(exec_side, 0.0):
                    # Cek apakah terjadi MSS untuk Unban prematur
                    mss_triggered, mss_reason = check_mss_unban(df, exec_side)
                    if mss_triggered:
                        banned_direction_until[exec_side] = 0.0
                        print(f"  🔓 [MSS UNBAN] Soft Ban '{exec_side}' di-RESET otomatis! Reason: {mss_reason}")
                    else:
                        # Soft Ban: Butuh Score >= 80
                        if score < MIN_SCORE_SOFT_BAN:
                            continue
                        else:
                            print(f"  ⚡ [SOFT BAN PASSED] Skor Tinggi ({score} >= {MIN_SCORE_SOFT_BAN}) menembus Soft Ban '{exec_side}'!")

                # Ambil Harga Live & Hitung Risk
                ticker = _rest_call("get_ticker", client.futures_symbol_ticker, symbol=symbol)
                entry_price = float(ticker["price"])

                atr_pct = atr / entry_price
                tp_pct  = max(MIN_TP_PCT, min(MAX_TP_PCT, ATR_TP_RESTORED_MULTIPLIER * atr_pct))
                sl_pct  = max(MIN_SL_PCT, min(MAX_SL_PCT, ATR_SL_RESTORED_MULTIPLIER * atr_pct))

                if exec_side == "LONG":
                    tp_price = entry_price * (1 + tp_pct)
                    sl_price = entry_price * (1 - sl_pct)
                else:
                    tp_price = entry_price * (1 - tp_pct)
                    sl_price = entry_price * (1 + sl_pct)

                # Simpan Posisi (Simulasi Paper Trading)
                with positions_lock:
                    live_positions[symbol] = {
                        "symbol": symbol, "raw_signal": raw_sig, "exec_side": exec_side,
                        "mode": curr_mode, "entry_price": entry_price, "tp_price": tp_price,
                        "sl_price": sl_price, "entry_time": now
                    }

                print(f"\n  🎯 [ENTRY EXECUTION] {symbol} | Mode: {curr_mode} | Side: {exec_side} (Raw: {raw_sig}) | Score: {score}")
                print(f"     Harga Entry: ${entry_price:.4f} | TP: ${tp_price:.4f} (+{tp_pct*100:.2f}%) | SL: ${sl_price:.4f} (-{sl_pct*100:.2f}%)")
                print(f"     Waktu Entry (Makassar): {format_wita(now)}")

        except KeyboardInterrupt:
            print("\n  🛑 Bot Dihentikan Oleh Pengguna.")
            break
        except Exception:
            time.sleep(2.0)

if __name__ == "__main__":
    run_bot()
