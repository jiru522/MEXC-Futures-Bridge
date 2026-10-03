#!/usr/bin/env python3
# ==============================================================================
# JIRU MEXC FUTURES AUTONOMOUS BRIDGE  (v34)
# ==============================================================================
# TradingView webhook  ->  multi-agent pipeline  ->  CCXT / MEXC USDT-M futures
#
#   Research Agent    validates payload, symbol, allowlist, cooldown, capacity,
#                     Monday stand-down.
#   Analysis Agent    5m/1h candles (MEXC via CCXT, GeckoTerminal fallback):
#                     ATR, 1h trend bias, volume acceleration, volatility spike,
#                     R:R >= MIN_RR gate, round-trip-cost <= 25% of TP1 move gate,
#                     ATR/slippage-scaled risk budget (SOP Sentinel rules).
#   Trader Agent      lower-timeframe validation, contract sizing, market entry,
#                     hard stop-loss trigger, fail-safe emergency close.
#   Position Monitor  background loop: TP1 scale-out, breakeven, post-TP1 runner
#                     trail, TP2 scale-out, 60-minute pre-TP1 time stop.
#   Diagnostic Agent  SQLite ledger: MFE/MAE, slippage, exit reason, trade path,
#                     `analytics` view.
#   Maintainer (2nd   background thread (15-30 min): compact telemetry ->
#   Brain) Agent      OpenRouter free LLM -> bounded updates to dynamic_bias.json,
#                     with a 4-consecutive-loss circuit breaker (6 h suspension).
#
# SAFE BY DEFAULT: TRADING_ENABLED=false and DRY_RUN=true. In that state the bridge
# runs a full PAPER simulation (paper positions, simulated fills from live public
# prices, the whole exit ladder) so the ledger and the 2nd Brain get real data.
# Orders are only sent to MEXC when TRADING_ENABLED=true AND DRY_RUN=false.
#
# Requirements / assumptions:
#   * MEXC USDT-M futures (swap, linear, settle USDT) in ONE-WAY position mode.
#     The bridge only VERIFIES the mode; it never switches it.
#   * Use a trade-only API key (no withdrawals).
#   * All secrets come from environment variables and are never logged or stored.
# ==============================================================================

import os
import re
import json
import time
import math
import hmac
import hashlib
import logging
import sqlite3
import tempfile
import threading
import asyncio
import random
import statistics
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Optional

import ccxt
import requests
from fastapi import FastAPI, HTTPException, Request, BackgroundTasks, Header
from fastapi.responses import HTMLResponse
from pydantic import BaseModel, Field, AliasChoices, ConfigDict

APP_NAME = "JIRU MEXC Futures Autonomous Bridge"
APP_VERSION = "34.2.0"


# ==============================================================================
# Environment helpers
# ==============================================================================
def env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    return raw.strip().lower() in {"1", "true", "yes", "y", "on"}


def env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        return int(raw)
    except ValueError:
        raise RuntimeError(f"Environment variable {name} must be an integer")


def env_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        return float(raw)
    except ValueError:
        raise RuntimeError(f"Environment variable {name} must be a number")


def env_str(name: str, default: str = "") -> str:
    raw = os.getenv(name)
    return default if raw is None else raw.strip()


def clean_symbol_text(value: str) -> str:
    s = str(value).strip().upper()
    if s.startswith("MEXC:"):                    # TradingView: MEXC:BTCUSDT.P
        s = s[5:]
    s = s.replace(".P", "").replace("PERP", "")
    s = s.replace("-SWAP", "").replace("_SWAP", "")
    return s.strip()


# ==============================================================================
# Secrets (environment only) and log redaction
# ==============================================================================
WEBHOOK_SECRET = env_str("TRADINGVIEW_WEBHOOK_SECRET") or env_str("WEBHOOK_SECRET")
EXCHANGE_API_KEY = env_str("EXCHANGE_API_KEY") or env_str("MEXC_API_KEY")
EXCHANGE_API_SECRET = env_str("EXCHANGE_API_SECRET") or env_str("MEXC_API_SECRET")
OPENROUTER_API_KEY = env_str("OPENROUTER_API_KEY")

_SECRET_VALUES = [v for v in (WEBHOOK_SECRET, EXCHANGE_API_KEY, EXCHANGE_API_SECRET, OPENROUTER_API_KEY)
                  if v and len(v) >= 4]


def redact(text: Any) -> str:
    """Remove every configured secret from a string before it is logged or stored."""
    s = str(text)
    for v in _SECRET_VALUES:
        s = s.replace(v, "***")
    return s


class _RedactFilter(logging.Filter):
    """Last line of defence: scrub secrets from every log record, including tracebacks."""

    def filter(self, record: logging.LogRecord) -> bool:
        try:
            msg = record.getMessage()
            red = redact(msg)
            if red != msg:
                record.msg, record.args = red, ()
            if record.exc_info and not record.exc_text:
                record.exc_text = redact(logging.Formatter().formatException(record.exc_info))
        except Exception:
            pass
        return True


LOG_LEVEL = env_str("LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=getattr(logging, LOG_LEVEL, logging.INFO),
                    format="%(asctime)s %(levelname)s %(name)s | %(message)s")
for _h in logging.getLogger().handlers:
    _h.addFilter(_RedactFilter())
logger = logging.getLogger("jiru-mexc-v34")


# ==============================================================================
# Configuration
# ==============================================================================
TRADING_ENABLED = env_bool("TRADING_ENABLED", False)
DRY_RUN = env_bool("DRY_RUN", True)


def is_live() -> bool:
    """True only when real orders may be sent to MEXC."""
    return TRADING_ENABLED and not DRY_RUN


# --- risk / sizing (SOP Sentinel) ---------------------------------------------
DEFAULT_RISK_USD = env_float("DEFAULT_RISK_USD", 10.0)
MAX_RISK_USD = env_float("MAX_RISK_USD", 10.0)             # strict cap, never exceeded
DEFAULT_LEVERAGE = env_int("DEFAULT_LEVERAGE", 10)
MAX_LEVERAGE = env_int("MAX_LEVERAGE", 50)
MARGIN_MODE = env_str("MARGIN_MODE", "isolated").lower()
POSITION_MODE = env_str("MEXC_POSITION_MODE", "oneway").lower()
MARGIN_UTILIZATION = env_float("MAX_MARGIN_UTILIZATION", 0.90)
MIN_STOP_DISTANCE_PCT = env_float("MIN_STOP_DISTANCE_PCT", 0.10)
MAX_PRICE_DEVIATION_PCT = env_float("MAX_PRICE_DEVIATION_PCT", 0.30)
RISK_BUFFER_PCT = env_float("RISK_BUFFER_PCT", 5.0)        # sizing haircut for fees/rounding
MIN_RR = env_float("MIN_RR", 1.5)                          # (TP1-entry)/(entry-SL) gate
PAPER_BALANCE_USDT = env_float("PAPER_BALANCE_USDT", 1000.0)

# --- levels used when the TradingView payload omits them -----------------------
SL_ATR_MULT = env_float("SL_ATR_MULT", 1.5)
TP1_R_MULT = env_float("TP1_R_MULT", 1.5)                  # must be >= MIN_RR to pass the R:R gate
TP2_R_MULT = env_float("TP2_R_MULT", 3.0)

# --- volatility / filters -------------------------------------------------------
ATR_PERIOD = env_int("ATR_PERIOD", 14)
VOL_REF_ATR_PCT = env_float("VOL_REF_ATR_PCT", 0.40)       # 5m ATR% regarded as "normal"
MIN_RISK_SCALAR = env_float("MIN_RISK_SCALAR", 0.40)       # floor of the volatility scalar
VOL_SPIKE_MULT = env_float("VOL_SPIKE_MULT", 3.0)          # last 5m true range > N x ATR => stand down
MIN_STOP_ATR_MULT = env_float("MIN_STOP_ATR_MULT", 0.5)    # stop must be >= N x ATR (not inside noise)
MIN_VOLUME_ACCEL = env_float("MIN_VOLUME_ACCEL", 0.8)      # recent 3x5m volume / prior 12x5m volume
REQUIRE_TREND_ALIGNMENT = env_bool("REQUIRE_TREND_ALIGNMENT", True)  # reject trades against the 1h bias
MONDAY_STANDDOWN = env_bool("MONDAY_STANDDOWN", False)     # UTC Monday: no automated entries
LTF_CHECK_ENABLED = env_bool("LTF_CHECK_ENABLED", True)
LTF_MAX_DROP_PCT = env_float("LTF_MAX_DROP_PCT", 0.15)     # adverse 1m drift tolerated at entry

# --- costs ----------------------------------------------------------------------
TAKER_FEE_PCT = env_float("TAKER_FEE_PCT", 0.02)           # per side
EST_SLIPPAGE_PCT = env_float("EST_SLIPPAGE_PCT", 0.02)     # per side, used until observed drag exists
MAX_COST_TO_TARGET = env_float("MAX_COST_TO_TARGET", 0.25) # round-trip cost / TP1 move
BE_BUFFER_PCT = env_float("BE_BUFFER_PCT", 0.02)           # extra margin above fees for breakeven

# --- exits ----------------------------------------------------------------------
TP1_FRACTION = env_float("TP1_FRACTION", 0.33)             # share of the ORIGINAL position (0.33-0.50)
TP2_FRACTION = env_float("TP2_FRACTION", 0.33)             # share of the ORIGINAL position
BASE_TRAIL_MULT = env_float("BASE_TRAIL_MULT", 1.0)        # trail = N x stop distance behind the extreme
TIME_STOP_MINUTES = env_float("TIME_STOP_MINUTES", 60.0)   # exit if TP1 not reached (0 disables)
MONITOR_INTERVAL_SEC = env_float("MONITOR_INTERVAL_SEC", 5.0)
PATH_SNAPSHOT_SEC = env_float("PATH_SNAPSHOT_SEC", 15.0)
STOP_REPLACE_MIN_INTERVAL_SEC = env_float("STOP_REPLACE_MIN_INTERVAL_SEC", 10.0)
STOP_REPLACE_MIN_STEP_FRAC = env_float("STOP_REPLACE_MIN_STEP_FRAC", 0.10)  # of stop distance
SW_STOP_GRACE_FRAC = env_float("SW_STOP_GRACE_FRAC", 0.10)  # live pre-TP1: exchange trigger gets this head start
TRIGGER_PRICE_TYPE = env_str("TRIGGER_PRICE_TYPE", "mark").lower()

# --- portfolio / execution ------------------------------------------------------
MAX_OPEN_POSITIONS = env_int("MAX_OPEN_POSITIONS", 2)      # hard ceiling; the 2nd Brain can only go lower
SYMBOL_COOLDOWN_SEC = env_int("SYMBOL_COOLDOWN_SEC", 900)
SIGNAL_MAX_AGE_SEC = env_int("SIGNAL_MAX_AGE_SEC", 30)
SIGNAL_MAX_FUTURE_SEC = env_int("SIGNAL_MAX_FUTURE_SEC", 10)
POSITION_SYNC_DELAY_SEC = env_float("POSITION_SYNC_DELAY_SEC", 0.75)
EXCHANGE_TIMEOUT_MS = env_int("EXCHANGE_TIMEOUT_MS", 10000)
ENFORCE_MARGIN_MODE = env_bool("ENFORCE_MARGIN_MODE", True)
ENFORCE_LEVERAGE = env_bool("ENFORCE_LEVERAGE", True)
FAILSAFE_CLOSE_ON_SL_FAILURE = env_bool("FAILSAFE_CLOSE_ON_SL_FAILURE", True)
ALLOWED_SYMBOLS = {clean_symbol_text(s) for s in env_str("ALLOWED_SYMBOLS").split(",") if s.strip()}

# --- market data ----------------------------------------------------------------
OHLCV_SOURCE = env_str("OHLCV_SOURCE", "auto").lower()     # auto | mexc | gecko
GECKO_BASE_URL = env_str("GECKOTERMINAL_BASE_URL", "https://api.geckoterminal.com/api/v2")
GECKO_MIN_INTERVAL_SEC = env_float("GECKO_MIN_INTERVAL_SEC", 2.2)   # free tier: ~30 calls/min

# --- persistence ----------------------------------------------------------------
def _resolve_db_path() -> str:
    path = env_str("STATE_DB_PATH", "/data/mexc_bridge.db")
    try:
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        probe = path + ".probe"
        with open(probe, "a"):
            pass
        os.remove(probe)
        return path
    except OSError:
        fallback = os.path.join(tempfile.gettempdir(), "mexc_bridge.db")
        logging.getLogger("jiru-mexc-v34").warning(
            "STATE_DB_PATH %s is not writable; falling back to %s (state will NOT survive redeploys; "
            "mount a volume at /data)", path, fallback)
        return fallback


STATE_DB_PATH = _resolve_db_path()
BIAS_FILE_PATH = env_str("BIAS_FILE_PATH") or os.path.join(os.path.dirname(STATE_DB_PATH) or ".", "dynamic_bias.json")

# --- 2nd Brain ------------------------------------------------------------------
BRAIN_ENABLED = env_bool("BRAIN_ENABLED", True)
BRAIN_INTERVAL_MIN = min(30.0, max(15.0, env_float("BRAIN_INTERVAL_MIN", 20.0)))
BRAIN_MODELS = [m.strip() for m in env_str(
    "BRAIN_MODELS", "meta-llama/llama-3.3-70b-instruct:free,google/gemma-2-27b-it:free").split(",") if m.strip()]
OPENROUTER_URL = env_str("OPENROUTER_URL", "https://openrouter.ai/api/v1/chat/completions")
BRAIN_TIMEOUT_SEC = env_float("BRAIN_TIMEOUT_SEC", 45.0)
BRAIN_MIN_TRADES = env_int("BRAIN_MIN_TRADES", 3)
BRAIN_LOOKBACK_TRADES = env_int("BRAIN_LOOKBACK_TRADES", 30)
BREAKER_LOSSES = env_int("BREAKER_LOSSES", 4)
BREAKER_SUSPEND_HOURS = env_float("BREAKER_SUSPEND_HOURS", 6.0)
BREAKER_HALTS_ENTRIES = env_bool("BREAKER_HALTS_ENTRIES", False)  # also block new entries while suspended

# --- autonomous scanner (no TradingView needed) ------------------------------------
SCANNER_ENABLED = env_bool("SCANNER_ENABLED", True)        # self-generated LONG setups; still obeys paper/live gates
SCANNER_SYMBOLS = [s.strip().upper() for s in env_str(
    "SCANNER_SYMBOLS", "BTC,ETH,SOL,XRP,DOGE,BNB,ADA,AVAX,LINK,SUI,LTC,DOT,NEAR,APT,ARB").split(",") if s.strip()]
SCANNER_INTERVAL_SEC = env_float("SCANNER_INTERVAL_SEC", 60.0)
SCANNER_MAX_PER_CYCLE = env_int("SCANNER_MAX_PER_CYCLE", 1)
SCANNER_MIN_SCORE = env_float("SCANNER_MIN_SCORE", 65.0)
SCANNER_SWING_BARS = env_int("SCANNER_SWING_BARS", 8)       # stop sits below the lowest low of N 5m bars
SCANNER_MAX_STOP_ATR = env_float("SCANNER_MAX_STOP_ATR", 3.0)
SCANNER_PULLBACK_ATR = env_float("SCANNER_PULLBACK_ATR", 0.35)
SCANNER_MAX_EXTENSION_ATR = env_float("SCANNER_MAX_EXTENSION_ATR", 1.5)  # do not chase: close vs EMA20

# Hard bounds the 2nd Brain can never leave (max_open_positions can only go DOWN from the env ceiling).
BIAS_BOUNDS = {
    "stop_distance_mult": (0.5, 2.0),
    "max_open_positions": (1, max(1, MAX_OPEN_POSITIONS)),
    "min_volume_accel": (0.3, 3.0),
}
BIAS_MAX_STEP = {"stop_distance_mult": 0.25, "max_open_positions": 1, "min_volume_accel": 0.30}


def bias_defaults() -> dict[str, float]:
    return {
        "stop_distance_mult": BASE_TRAIL_MULT,
        "max_open_positions": MAX_OPEN_POSITIONS,
        "min_volume_accel": MIN_VOLUME_ACCEL,
    }


# ==============================================================================
# Shared runtime state
# ==============================================================================
TRADE_LOCK = threading.RLock()       # guards all trade-state mutation (entries, exits, monitor ticks)
ENTRY_LOCK = threading.Lock()        # serialises the whole signal pipeline (one signal at a time)
STATS_LOCK = threading.Lock()
STATS = {"received": 0, "accepted": 0, "duplicate": 0, "rejected": 0, "executed": 0, "failed": 0,
         "last_signal_at": None, "last_error": None}


def bump(key: str, n: int = 1) -> None:
    with STATS_LOCK:
        STATS[key] = STATS.get(key, 0) + n


# ==============================================================================
# Small utilities
# ==============================================================================
def utc_iso(ts: Optional[float] = None) -> str:
    return datetime.fromtimestamp(ts or time.time(), tz=timezone.utc).isoformat()


def safe_float(value: Any, default: Optional[float] = None) -> Optional[float]:
    try:
        if value is None:
            return default
        f = float(value)
        return f if math.isfinite(f) else default
    except (TypeError, ValueError):
        return default


def clamp(v: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, v))


def pct_distance(a: float, b: float) -> float:
    return float("inf") if a == 0 else abs(a - b) / abs(a) * 100.0


def parse_timestamp(value: Any) -> Optional[float]:
    if value is None or value == "":
        return None
    if isinstance(value, (int, float)):
        v = float(value)
        return v / 1000.0 if v > 10_000_000_000 else v
    if isinstance(value, str):
        s = value.strip()
        try:
            v = float(s)
            return v / 1000.0 if v > 10_000_000_000 else v
        except ValueError:
            pass
        try:
            return datetime.fromisoformat(s.replace("Z", "+00:00")).timestamp()
        except ValueError:
            return None
    return None


def normalize_action(action: str) -> str:
    return str(action).strip().upper().replace("-", "_").replace(" ", "_")


def atomic_write_json(path: str, data: Any) -> None:
    directory = os.path.dirname(path) or "."
    os.makedirs(directory, exist_ok=True)
    tmp = f"{path}.tmp.{os.getpid()}.{threading.get_ident()}"
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(data, fh, indent=2, sort_keys=True)
    os.replace(tmp, path)


class SignalRejected(Exception):
    """A signal failed a gate. `stage` names the agent, `reason` is stored in the signals table."""

    def __init__(self, stage: str, reason: str):
        super().__init__(f"{stage}: {reason}")
        self.stage = stage
        self.reason = reason


# ==============================================================================
# CCXT call wrapper: rate limits, timeouts, partial failures
# ==============================================================================
def ccxt_call(fn, *args, retries: int = 3, retry_network: bool = True, **kwargs):
    """Call a CCXT method with back-off.

    * RateLimitExceeded / DDoSProtection: the request was refused before processing, so it is
      always safe to retry (exponential back-off with jitter).
    * Other network errors / timeouts: retried only when `retry_network` is True. Order-creating
      calls pass False because a timed-out order may still have been accepted; the caller then
      reconciles against the live position instead of blindly re-sending.
    """
    last: Optional[Exception] = None
    for attempt in range(retries + 1):
        try:
            return fn(*args, **kwargs)
        except (ccxt.RateLimitExceeded, ccxt.DDoSProtection) as exc:
            last = exc
        except (ccxt.RequestTimeout, ccxt.NetworkError) as exc:
            if not retry_network:
                raise
            last = exc
        if attempt < retries:
            time.sleep(min(8.0, 0.6 * (2 ** attempt)) + random.random() * 0.3)
    assert last is not None
    raise last


# ==============================================================================
# Dynamic bias (dynamic_bias.json) + circuit breaker
# ==============================================================================
class DynamicBias:
    """Holds the 2nd Brain's calibration overrides.

    File layout (dynamic_bias.json) - the three tunables are top-level so the file is easy to
    read or hand-edit; bookkeeping lives under `_meta`:
        {"stop_distance_mult": 1.0, "max_open_positions": 2, "min_volume_accel": 0.8,
         "_meta": {"updated_at": "...", "source": "...", "note": "...", "suspended_until": 0,
                   "breaker_last_trade_id": 0}}
    While the circuit breaker is active the overrides are ignored and static defaults apply.
    """

    def __init__(self, path: str):
        self.path = path
        self._lock = threading.RLock()
        self._values: dict[str, float] = bias_defaults()
        self._meta: dict[str, Any] = {"updated_at": None, "source": "default", "note": "",
                                      "suspended_until": 0.0, "breaker_last_trade_id": 0}
        self._mtime = 0.0
        self._load()
        if not os.path.exists(self.path):
            self._save()

    # -- persistence --------------------------------------------------------------
    def _load(self) -> None:
        try:
            mtime = os.path.getmtime(self.path)
        except OSError:
            return
        if mtime == self._mtime:
            return
        try:
            with open(self.path, "r", encoding="utf-8") as fh:
                raw = json.load(fh)
            values = bias_defaults()
            for key in BIAS_BOUNDS:
                v = safe_float(raw.get(key))
                if v is not None:
                    values[key] = self._bounded(key, v)
            meta = dict(self._meta)
            meta.update({k: v for k, v in (raw.get("_meta") or {}).items() if k in meta})
            self._values, self._meta, self._mtime = values, meta, mtime
        except Exception as exc:
            logger.warning("Could not read %s (%s); keeping in-memory bias", self.path, redact(exc))

    def _save(self) -> None:
        try:
            data = dict(self._values)
            data["_meta"] = dict(self._meta, updated_at=utc_iso())
            atomic_write_json(self.path, data)
            self._mtime = os.path.getmtime(self.path)
        except Exception as exc:
            logger.warning("Could not write %s: %s", self.path, redact(exc))

    @staticmethod
    def _bounded(key: str, value: float) -> float:
        lo, hi = BIAS_BOUNDS[key]
        value = clamp(float(value), lo, hi)
        return float(int(round(value))) if key == "max_open_positions" else round(value, 4)

    # -- reads --------------------------------------------------------------------
    def suspended_until(self) -> float:
        with self._lock:
            self._load()
            return float(self._meta.get("suspended_until") or 0.0)

    def is_suspended(self) -> bool:
        return self.suspended_until() > time.time()

    def effective(self) -> dict[str, float]:
        """Values the pipeline must use right now (static defaults while suspended)."""
        with self._lock:
            self._load()
            if float(self._meta.get("suspended_until") or 0.0) > time.time():
                return bias_defaults()
            return dict(self._values)

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            self._load()
            return {"effective": self.effective(), "stored": dict(self._values), "meta": dict(self._meta),
                    "suspended": self.is_suspended(), "bounds": {k: list(v) for k, v in BIAS_BOUNDS.items()}}

    # -- writes -------------------------------------------------------------------
    def apply(self, proposal: dict[str, Any], source: str, note: str = "") -> dict[str, float]:
        """Validate, clamp to bounds, limit the per-cycle step, persist. Unknown keys are ignored."""
        with self._lock:
            self._load()
            changed: dict[str, float] = {}
            for key in BIAS_BOUNDS:
                target = safe_float(proposal.get(key))
                if target is None:
                    continue
                current = self._values.get(key, bias_defaults()[key])
                step = BIAS_MAX_STEP[key]
                moved = current + clamp(target - current, -step, step)
                new = self._bounded(key, moved)
                if new != current:
                    self._values[key] = new
                    changed[key] = new
            self._meta.update({"source": source, "note": redact(note)[:300]})
            self._save()
            return changed

    def suspend(self, hours: float, reason: str, trade_id: int = 0) -> None:
        with self._lock:
            self._meta["suspended_until"] = time.time() + hours * 3600.0
            self._meta["breaker_last_trade_id"] = int(trade_id)
            self._meta.update({"source": "circuit_breaker", "note": redact(reason)[:300]})
            self._save()
        logger.warning("CIRCUIT BREAKER: overrides suspended for %.1f h (%s)", hours, reason)

    def on_trade_closed(self, store: "StateStore") -> None:
        """Trip the breaker after BREAKER_LOSSES consecutive losing trades (once per streak)."""
        streak, newest_id = store.consecutive_losses()
        with self._lock:
            self._load()
            already = int(self._meta.get("breaker_last_trade_id") or 0)
            if streak >= BREAKER_LOSSES and newest_id > already:
                self.suspend(BREAKER_SUSPEND_HOURS, f"{streak} consecutive losing trades", newest_id)


# ==============================================================================
# Persistent state (SQLite): signals, trades, analytics view
# ==============================================================================
SIGNAL_EXTRA_COLUMNS = {
    "signal_ts": "REAL",          # timestamp carried by the TradingView alert
    "signal_age_sec": "REAL",     # now - signal_ts at receipt
    "outcome": "TEXT",            # ACCEPTED | REJECTED | ERROR | DRY | EXIT
    "stage": "TEXT",              # agent that decided the outcome
    "reject_reason": "TEXT",
    "trade_id": "INTEGER",
    "price": "REAL",
}

TRADE_EXTRA_COLUMNS = {
    "mode": "TEXT",               # LIVE | PAPER
    "leverage": "INTEGER",
    "remaining": "REAL",          # contracts still open
    "signal_price": "REAL",
    "ref_price": "REAL",          # exchange price just before entry
    "current_sl": "REAL",         # stop currently in force
    "stop_distance": "REAL",      # |entry - initial stop| (price units)
    "risk_usd": "REAL",           # contracts x contractSize x stop distance
    "atr": "REAL", "atr_pct": "REAL", "trend_bias": "INTEGER", "volume_accel": "REAL",
    "cost_pct": "REAL", "rr": "REAL",
    "tp1_done": "INTEGER DEFAULT 0", "tp2_done": "INTEGER DEFAULT 0",
    "peak_price": "REAL", "trough_price": "REAL",
    "mfe_pct": "REAL DEFAULT 0", "mae_pct": "REAL DEFAULT 0",
    "entry_slippage_pct": "REAL", "exit_slippage_pct": "REAL", "exec_drag_pct": "REAL",
    "exit_price": "REAL",         # size-weighted average over all exit legs
    "realized_pnl_usd": "REAL DEFAULT 0",   # net of estimated fees
    "realized_pnl_pct": "REAL",   # realized_pnl_usd / entry notional x 100
    "fees_usd": "REAL DEFAULT 0",
    "r_multiple": "REAL",
    "exit_reason": "TEXT",        # TP1 | TP2 | RUNNER_TRAIL | TIME_STOP | SL | MANUAL_EXIT | FAILSAFE | EXTERNAL_CLOSE
    "trade_path": "TEXT",         # JSON [[seconds_since_entry, price], ...]
    "partials": "TEXT",           # JSON list of exit legs
    "closed_at": "REAL",
    "last_stop_replace_at": "REAL",
    "protect_failures": "INTEGER DEFAULT 0",
    "bias_snapshot": "TEXT",
}


class StateStore:
    def __init__(self, path: str):
        self.path = path
        self._lock = threading.Lock()
        directory = os.path.dirname(path)
        if directory:
            os.makedirs(directory, exist_ok=True)
        self._init_db()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.path, timeout=15)
        conn.row_factory = sqlite3.Row
        return conn

    def _ensure_columns(self, conn: sqlite3.Connection, table: str, columns: dict[str, str]) -> None:
        existing = {r["name"] for r in conn.execute(f"PRAGMA table_info({table})").fetchall()}
        for name, decl in columns.items():
            if name not in existing:
                conn.execute(f"ALTER TABLE {table} ADD COLUMN {name} {decl}")

    def _init_db(self) -> None:
        with self._lock:
            conn = self._connect()
            try:
                try:
                    conn.execute("PRAGMA journal_mode=WAL")
                except sqlite3.DatabaseError:
                    pass
                conn.execute("""CREATE TABLE IF NOT EXISTS signals (
                    signal_id TEXT PRIMARY KEY, received_at REAL NOT NULL, action TEXT NOT NULL,
                    symbol TEXT NOT NULL, status TEXT NOT NULL, details TEXT)""")
                conn.execute("""CREATE TABLE IF NOT EXISTS trades (
                    id INTEGER PRIMARY KEY AUTOINCREMENT, signal_id TEXT NOT NULL, symbol TEXT NOT NULL,
                    side TEXT NOT NULL, amount REAL NOT NULL, contract_size REAL NOT NULL,
                    entry_price REAL NOT NULL, stop_price REAL NOT NULL, tp1_price REAL NOT NULL,
                    tp2_price REAL, entry_order_id TEXT, sl_order_id TEXT, tp1_order_id TEXT, tp2_order_id TEXT,
                    status TEXT NOT NULL, created_at REAL NOT NULL, updated_at REAL NOT NULL)""")
                # Forward migration: databases written by bridge v2.x gain the new columns in place.
                self._ensure_columns(conn, "signals", SIGNAL_EXTRA_COLUMNS)
                self._ensure_columns(conn, "trades", TRADE_EXTRA_COLUMNS)
                conn.execute("CREATE INDEX IF NOT EXISTS idx_trades_status ON trades(status)")
                conn.execute("CREATE INDEX IF NOT EXISTS idx_trades_symbol ON trades(symbol)")
                conn.execute("DROP VIEW IF EXISTS analytics")
                conn.execute("""
                CREATE VIEW analytics AS
                WITH c AS (
                    SELECT COALESCE(mode, 'LEGACY') AS m, * FROM trades WHERE status = 'CLOSED'
                ), d AS (
                    SELECT m, exec_drag_pct,
                           ROW_NUMBER() OVER (PARTITION BY m ORDER BY exec_drag_pct) AS rn,
                           COUNT(*) OVER (PARTITION BY m) AS cnt
                    FROM c WHERE exec_drag_pct IS NOT NULL
                ), med AS (
                    SELECT m, AVG(exec_drag_pct) AS median_exec_drag_pct
                    FROM d WHERE rn IN ((cnt + 1) / 2, (cnt + 2) / 2) GROUP BY m
                )
                SELECT
                    c.m AS mode,
                    COUNT(*) AS trades,
                    SUM(CASE WHEN c.realized_pnl_usd > 0 THEN 1 ELSE 0 END) AS wins,
                    ROUND(100.0 * SUM(CASE WHEN c.realized_pnl_usd > 0 THEN 1 ELSE 0 END) / COUNT(*), 2) AS win_rate_pct,
                    ROUND(SUM(CASE WHEN c.realized_pnl_usd > 0 THEN c.realized_pnl_usd ELSE 0 END) /
                          NULLIF(ABS(SUM(CASE WHEN c.realized_pnl_usd < 0 THEN c.realized_pnl_usd ELSE 0 END)), 0), 3)
                          AS profit_factor,
                    ROUND(SUM(c.realized_pnl_usd), 4) AS total_pnl_usd,
                    ROUND(AVG(c.r_multiple), 3) AS avg_r,
                    ROUND(AVG(c.mfe_pct / NULLIF(c.mae_pct, 0)), 3) AS avg_mfe_mae_ratio,
                    ROUND(AVG(c.mfe_pct), 4) AS avg_mfe_pct,
                    ROUND(AVG(c.mae_pct), 4) AS avg_mae_pct,
                    ROUND(MAX(med.median_exec_drag_pct), 5) AS median_exec_drag_pct
                FROM c LEFT JOIN med ON med.m = c.m
                GROUP BY c.m""")
                conn.commit()
            finally:
                conn.close()

    # -- generic helpers ----------------------------------------------------------
    def _run(self, sql: str, params: tuple = (), fetch: str = "none"):
        with self._lock:
            conn = self._connect()
            try:
                cur = conn.execute(sql, params)
                result = None
                if fetch == "all":
                    result = [dict(r) for r in cur.fetchall()]
                elif fetch == "one":
                    row = cur.fetchone()
                    result = dict(row) if row else None
                elif fetch == "rowcount":
                    result = cur.rowcount
                elif fetch == "lastrowid":
                    result = cur.lastrowid
                conn.commit()
                return result
            finally:
                conn.close()

    # -- signals ------------------------------------------------------------------
    def record_signal(self, signal_id: str, action: str, symbol: str, signal_ts: Optional[float],
                      price: Optional[float]) -> bool:
        now = time.time()
        age = (now - signal_ts) if signal_ts else None
        n = self._run(
            "INSERT OR IGNORE INTO signals (signal_id, received_at, action, symbol, status, signal_ts, "
            "signal_age_sec, price) VALUES (?,?,?,?,?,?,?,?)",
            (signal_id, now, action, symbol, "RECEIVED", signal_ts, age, price), "rowcount")
        return n == 1

    def update_signal(self, signal_id: str, status: str, outcome: Optional[str] = None,
                      stage: Optional[str] = None, reason: Optional[str] = None,
                      details: Any = None, trade_id: Optional[int] = None) -> None:
        self._run(
            "UPDATE signals SET status=?, outcome=?, stage=?, reject_reason=?, details=?, "
            "trade_id=COALESCE(?, trade_id) WHERE signal_id=?",
            (status, outcome, stage, redact(reason) if reason else None,
             redact(json.dumps(details, default=str)) if details is not None else None, trade_id, signal_id))

    def reject_summary(self, since_ts: float) -> dict[str, Any]:
        rows = self._run("SELECT stage, reject_reason FROM signals WHERE outcome='REJECTED' AND received_at>?",
                         (since_ts,), "all") or []
        by_stage: dict[str, int] = {}
        by_reason: dict[str, int] = {}
        for r in rows:
            by_stage[r["stage"] or "?"] = by_stage.get(r["stage"] or "?", 0) + 1
            key = re.sub(r"[\d.]+", "#", (r["reject_reason"] or "?"))[:70]
            by_reason[key] = by_reason.get(key, 0) + 1
        top = dict(sorted(by_reason.items(), key=lambda kv: -kv[1])[:5])
        return {"by_stage": by_stage, "top_reasons": top, "total": len(rows)}

    # -- trades -------------------------------------------------------------------
    def create_trade(self, data: dict[str, Any]) -> int:
        now = time.time()
        known = {"signal_id", "symbol", "side", "amount", "contract_size", "entry_price", "stop_price",
                 "tp1_price", "tp2_price", "entry_order_id", "sl_order_id", "tp1_order_id", "tp2_order_id",
                 "status"} | set(TRADE_EXTRA_COLUMNS)
        row = {k: v for k, v in data.items() if k in known}
        row.setdefault("status", "OPEN")
        row["created_at"] = now
        row["updated_at"] = now
        cols = ", ".join(row)
        marks = ", ".join("?" for _ in row)
        return int(self._run(f"INSERT INTO trades ({cols}) VALUES ({marks})", tuple(row.values()), "lastrowid"))

    def update_trade(self, trade_id: int, **fields: Any) -> None:
        if not fields:
            return
        fields["updated_at"] = time.time()
        assignments = ", ".join(f"{k}=?" for k in fields)
        self._run(f"UPDATE trades SET {assignments} WHERE id=?", tuple(fields.values()) + (trade_id,))

    def get_trade(self, trade_id: int) -> Optional[dict]:
        return self._run("SELECT * FROM trades WHERE id=?", (trade_id,), "one")

    def active_trades(self) -> list[dict]:
        return self._run("SELECT * FROM trades WHERE status IN ('OPEN','PROTECTED') ORDER BY id", (), "all") or []

    def closed_trades(self, limit: int = 30) -> list[dict]:
        return self._run("SELECT * FROM trades WHERE status='CLOSED' ORDER BY COALESCE(closed_at, updated_at) DESC, "
                         "id DESC LIMIT ?", (limit,), "all") or []

    def recent_trades(self, limit: int = 20) -> list[dict]:
        rows = self._run("SELECT * FROM trades ORDER BY id DESC LIMIT ?", (limit,), "all") or []
        for r in rows:
            r.pop("trade_path", None)
        return rows

    def recent_signals(self, limit: int = 40) -> list[dict]:
        rows = self._run("SELECT signal_id, received_at, action, symbol, status, outcome, stage, reject_reason, "
                         "trade_id, price FROM signals ORDER BY received_at DESC LIMIT ?", (limit,), "all") or []
        return rows

    def last_trade_ts(self, symbol: str) -> Optional[float]:
        row = self._run("SELECT MAX(COALESCE(closed_at, created_at)) AS ts FROM trades WHERE symbol=?",
                        (symbol,), "one")
        return row["ts"] if row and row["ts"] else None

    def consecutive_losses(self) -> tuple[int, int]:
        """(length of the current losing streak, id of the newest closed trade)."""
        rows = self.closed_trades(max(BREAKER_LOSSES * 3, 20))
        if not rows:
            return 0, 0
        streak = 0
        for r in rows:
            if (safe_float(r.get("realized_pnl_usd"), 0.0) or 0.0) < 0:
                streak += 1
            else:
                break
        return streak, int(rows[0]["id"])

    def stop_loss_drag_ratios(self, n: int = 20) -> list[float]:
        """loss / planned risk for recent stop-outs; > 1 means stops filled worse than planned."""
        rows = self._run("SELECT realized_pnl_usd, risk_usd FROM trades WHERE status='CLOSED' AND exit_reason='SL' "
                         "AND risk_usd > 0 AND realized_pnl_usd < 0 ORDER BY id DESC LIMIT ?", (n,), "all") or []
        return [(-r["realized_pnl_usd"]) / r["risk_usd"] for r in rows]

    def median_exec_drag_pct(self, n: int = 50) -> Optional[float]:
        rows = self._run("SELECT exec_drag_pct FROM trades WHERE status='CLOSED' AND exec_drag_pct IS NOT NULL "
                         "ORDER BY id DESC LIMIT ?", (n,), "all") or []
        vals = [r["exec_drag_pct"] for r in rows]
        return statistics.median(vals) if vals else None

    def analytics(self) -> list[dict]:
        return self._run("SELECT * FROM analytics", (), "all") or []


# ==============================================================================
# Pydantic payload (TradingView / SOP Sentinel)
# ==============================================================================
class TradingViewPayload(BaseModel):
    model_config = ConfigDict(extra="ignore", populate_by_name=True)

    secret: Optional[str] = None
    action: str
    symbol: str
    price: float = Field(..., gt=0, validation_alias=AliasChoices("price", "entry"))
    sl: Optional[float] = Field(default=None, validation_alias=AliasChoices("sl", "invalidation", "stop"))
    tp1: Optional[float] = Field(default=None, validation_alias=AliasChoices("tp1", "target1"))
    tp2: Optional[float] = Field(default=None, validation_alias=AliasChoices("tp2", "target2"))
    leverage: Optional[int] = Field(default=None, ge=1, le=200)
    risk_pct: Optional[float] = Field(default=None, ge=0.01, le=100)
    risk_usd: Optional[float] = Field(default=None, ge=0.01,
                                      validation_alias=AliasChoices("risk_usd", "risk", "max_risk"))
    suggested_qty: Optional[float] = Field(default=None, gt=0,
                                           validation_alias=AliasChoices("suggested_qty", "qty", "quantity"))
    signal_id: Optional[str] = None
    timestamp: Optional[Any] = None
    timeframe: Optional[str] = None
    system: Optional[str] = None
    version: Optional[str] = None
    sop_score: Optional[float] = None
    rr_tp1: Optional[float] = None
    rr_tp2: Optional[float] = None


def canonical_signal_id(payload: TradingViewPayload) -> str:
    if payload.signal_id:
        return payload.signal_id[:120]
    data = payload.model_dump(exclude={"secret"}, mode="json")
    raw = json.dumps(data, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:32]


store = StateStore(STATE_DB_PATH)
bias = DynamicBias(BIAS_FILE_PATH)


# ==============================================================================
# Exchange gateway (CCXT / MEXC USDT-M swap)
# ==============================================================================
@dataclass
class PositionInfo:
    symbol: str
    side: str
    contracts: float
    entry_price: float
    position_id: Optional[str] = None


def position_side(position: dict[str, Any]) -> Optional[str]:
    side = position.get("side")
    if side in {"long", "short"}:
        return side
    ptype = str((position.get("info") or {}).get("positionType"))
    if ptype == "1":
        return "long"
    if ptype == "2":
        return "short"
    if (safe_float(position.get("contracts"), 0.0) or 0.0) < 0:
        return "short"
    return None


def position_contracts(position: dict[str, Any]) -> float:
    contracts = safe_float(position.get("contracts"), None)
    if contracts is not None:
        return abs(contracts)
    return abs(safe_float((position.get("info") or {}).get("holdVol"), 0.0) or 0.0)


def position_entry_price(position: dict[str, Any]) -> float:
    for key in ("entryPrice", "markPrice", "average"):
        v = safe_float(position.get(key), None)
        if v is not None and v > 0:
            return v
    info = position.get("info") or {}
    for key in ("holdAvgPrice", "openAvgPrice"):
        v = safe_float(info.get(key), None)
        if v is not None and v > 0:
            return v
    return 0.0


class ExchangeGateway:
    """Thin, retrying wrapper over ccxt.mexc. All order parameters match the v2.x bridge
    (ONE-WAY positionMode=2, reduceOnly closes, independent trigger-market stops)."""

    def __init__(self) -> None:
        self.ex: Optional[ccxt.mexc] = None
        self.ready = False
        self.error: Optional[str] = None
        self.has_keys = bool(EXCHANGE_API_KEY and EXCHANGE_API_SECRET)

    # -- lifecycle ----------------------------------------------------------------
    def init(self) -> None:
        ex = ccxt.mexc({
            "apiKey": EXCHANGE_API_KEY, "secret": EXCHANGE_API_SECRET,
            "timeout": EXCHANGE_TIMEOUT_MS, "enableRateLimit": True,
            "options": {"defaultType": "swap", "adjustForTimeDifference": True,
                        "warnOnFetchOpenOrdersWithoutSymbol": False},
        })
        ccxt_call(ex.load_markets)
        self.ex, self.ready, self.error = ex, True, None

    def require(self) -> "ccxt.mexc":
        if self.ex is None or not self.ready:
            raise RuntimeError(f"Exchange not ready: {self.error or 'unknown startup state'}")
        return self.ex

    # -- symbols / precision --------------------------------------------------------
    def resolve_symbol(self, raw_symbol: str) -> str:
        ex = self.require()
        cleaned = clean_symbol_text(raw_symbol)

        def is_usdt_linear(m: dict) -> bool:
            return bool(m.get("swap") and m.get("linear") and m.get("settle") == "USDT")

        if cleaned in ex.markets and is_usdt_linear(ex.markets[cleaned]):
            return cleaned
        normalized = "".join(ch for ch in cleaned if ch.isalnum())
        for sym, m in ex.markets.items():
            if not is_usdt_linear(m):
                continue
            candidates = {str(sym).upper(), str(m.get("id", "")).upper()}
            if any("".join(ch for ch in c if ch.isalnum()) == normalized for c in candidates):
                return sym
        for sym, m in ex.markets.items():
            if not is_usdt_linear(m):
                continue
            base, quote, settle = (str(m.get(k, "")).upper() for k in ("base", "quote", "settle"))
            if normalized in (f"{base}{quote}", f"{base}{settle}"):
                return sym
        raise ValueError(f"MEXC USDT-M Futures symbol not found: {raw_symbol}")

    def market(self, symbol: str) -> dict:
        return self.require().market(symbol)

    def contract_size(self, symbol: str) -> float:
        cs = safe_float(self.market(symbol).get("contractSize"), 1.0) or 1.0
        return cs if cs > 0 else 1.0

    def check_allowlist(self, symbol: str) -> None:
        if not ALLOWED_SYMBOLS:
            return
        m = self.market(symbol)
        aliases = {clean_symbol_text(symbol), clean_symbol_text(str(m.get("id", ""))),
                   clean_symbol_text(f"{m.get('base', '')}USDT")}
        if not aliases & ALLOWED_SYMBOLS:
            raise ValueError(f"Symbol {symbol} is not in ALLOWED_SYMBOLS")

    def p_price(self, symbol: str, price: float) -> float:
        return safe_float(self.require().price_to_precision(symbol, price), 0.0) or 0.0

    def p_amount(self, symbol: str, amount: float) -> float:
        try:
            return safe_float(self.require().amount_to_precision(symbol, amount), 0.0) or 0.0
        except Exception:
            return 0.0

    def min_amount(self, symbol: str) -> float:
        limits = self.market(symbol).get("limits") or {}
        return safe_float((limits.get("amount") or {}).get("min"), 0.0) or 0.0

    # -- market data ----------------------------------------------------------------
    def ticker(self, symbol: str) -> dict[str, float]:
        t = ccxt_call(self.require().fetch_ticker, symbol, {"type": "swap"})
        last = None
        for key in ("last", "mark", "close"):
            v = safe_float(t.get(key), None)
            if v and v > 0:
                last = v
                break
        bid, ask = safe_float(t.get("bid"), None), safe_float(t.get("ask"), None)
        if last is None and bid and ask:
            last = (bid + ask) / 2.0
        if last is None:
            raise RuntimeError(f"No usable MEXC price for {symbol}")
        return {"last": last, "bid": bid if bid and bid > 0 else last, "ask": ask if ask and ask > 0 else last}

    def ohlcv(self, symbol: str, timeframe: str, limit: int) -> list[list[float]]:
        rows = ccxt_call(self.require().fetch_ohlcv, symbol, timeframe, None, limit)
        return [[float(x) for x in r[:6]] for r in rows or [] if r and None not in r[:6]]

    # -- account --------------------------------------------------------------------
    def positions(self, symbol: Optional[str] = None) -> list[PositionInfo]:
        ex = self.require()
        raw = ccxt_call(ex.fetch_positions, [symbol] if symbol else None, {"type": "swap"})
        out: list[PositionInfo] = []
        for r in raw or []:
            contracts, side = position_contracts(r), position_side(r)
            if contracts <= 0 or side not in {"long", "short"}:
                continue
            pid = (r.get("info") or {}).get("positionId") or r.get("id")
            out.append(PositionInfo(r.get("symbol") or symbol or "", side, contracts,
                                    position_entry_price(r), str(pid) if pid is not None else None))
        return out

    def free_usdt(self) -> float:
        bal = ccxt_call(self.require().fetch_balance, {"type": "swap"})
        direct = safe_float((bal.get("USDT") or {}).get("free"), None)
        if direct is not None:
            return max(0.0, direct)
        return max(0.0, safe_float((bal.get("free") or {}).get("USDT"), 0.0) or 0.0)

    def prepare_account(self, symbol: str, leverage: int, side: str) -> None:
        """Verify ONE-WAY mode (never switches it) and apply margin mode / leverage."""
        ex = self.require()
        if not hasattr(ex, "fetch_position_mode"):
            raise RuntimeError("Installed CCXT version does not expose fetch_position_mode()")
        try:
            if bool(ex.fetch_position_mode(symbol).get("hedged")):
                raise RuntimeError("MEXC account is in HEDGE mode; switch it to ONE-WAY before live trading")
        except RuntimeError:
            raise
        except Exception as exc:
            raise RuntimeError(f"Unable to verify MEXC position mode: {redact(exc)}")
        params = {"positionType": 1 if side == "long" else 2, "openType": 1 if MARGIN_MODE == "isolated" else 2}
        if ENFORCE_MARGIN_MODE and hasattr(ex, "set_margin_mode"):
            try:
                ex.set_margin_mode(MARGIN_MODE, symbol, params={"leverage": leverage})
            except Exception as exc:
                if not any(tok in str(exc).lower() for tok in ("same", "already", "unchanged")):
                    raise RuntimeError(f"Unable to set {MARGIN_MODE} margin mode: {redact(exc)}")
        if ENFORCE_LEVERAGE:
            try:
                ex.set_leverage(leverage, symbol, params)
            except Exception as exc:
                raise RuntimeError(f"Unable to set {leverage}x leverage on {symbol}: {redact(exc)}")

    # -- orders ---------------------------------------------------------------------
    @staticmethod
    def _oid(signal_id: str, role: str) -> str:
        digest = hashlib.sha256(f"{signal_id}:{role}:{time.time_ns()}".encode()).hexdigest()[:20]
        return f"jiru34-{role}-{digest}"

    def market_order(self, symbol: str, side: str, amount: float, reduce_only: bool, role: str,
                     signal_id: str) -> dict:
        """side: 'buy' | 'sell'. Never retried on timeouts (ambiguous); caller reconciles."""
        amount = self.p_amount(symbol, amount)
        if amount <= 0:
            raise ValueError("Order amount rounded to zero")
        params = {"reduceOnly": reduce_only, "positionMode": 2,
                  "openType": 1 if MARGIN_MODE == "isolated" else 2,
                  "externalOid": self._oid(signal_id, role)}
        return ccxt_call(self.require().create_order, symbol, "market", side, amount, None, params,
                         retry_network=False)

    def stop_order(self, symbol: str, close_side: str, amount: float, trigger_price: float, role: str,
                   signal_id: str) -> dict:
        amount, trigger = self.p_amount(symbol, amount), self.p_price(symbol, trigger_price)
        if amount <= 0 or trigger <= 0:
            raise ValueError(f"Invalid protective order parameters for {role}")
        params = {"reduceOnly": True, "positionMode": 2,
                  "openType": 1 if MARGIN_MODE == "isolated" else 2,
                  "triggerPriceType": TRIGGER_PRICE_TYPE, "externalOid": self._oid(signal_id, role)}
        return ccxt_call(self.require().create_stop_market_order, symbol, close_side, amount, trigger, params,
                         retry_network=False)

    def cancel(self, order_id: Optional[str], symbol: str) -> bool:
        if not order_id:
            return True
        try:
            ccxt_call(self.require().cancel_order, order_id, symbol)
            return True
        except Exception as exc:
            logger.warning("Could not cancel order %s on %s: %s", order_id, symbol, redact(exc))
            return False

    def fill_price(self, order: Optional[dict], symbol: str) -> Optional[float]:
        """Average fill of a market order; looks the order up once if the create response lacks it."""
        if not order:
            return None
        for key in ("average", "price"):
            v = safe_float(order.get(key), None)
            if v and v > 0:
                return v
        oid = order.get("id")
        if oid is None:
            return None
        try:
            time.sleep(0.4)
            o = ccxt_call(self.require().fetch_order, str(oid), symbol, retries=1)
            for key in ("average", "price"):
                v = safe_float(o.get(key), None)
                if v and v > 0:
                    return v
        except Exception:
            pass
        return None

    def closing_fills_avg(self, symbol: str, close_side: str, since_ts: float) -> Optional[float]:
        """Size-weighted average price of `close_side` fills since `since_ts` (for exchange-side stop-outs)."""
        try:
            trades = ccxt_call(self.require().fetch_my_trades, symbol, int(since_ts * 1000), 50, retries=1)
        except Exception:
            return None
        num = den = 0.0
        for t in trades or []:
            if t.get("side") != close_side:
                continue
            p, a = safe_float(t.get("price")), safe_float(t.get("amount"))
            if p and a:
                num += p * a
                den += a
        return num / den if den > 0 else None


gateway = ExchangeGateway()


# ==============================================================================
# Market data: MEXC OHLCV via CCXT, GeckoTerminal as keyless fallback
# ==============================================================================
class GeckoClient:
    """GeckoTerminal public API (no key, ~30 calls/min). Futures symbols have no native pool, so the
    base asset is looked up by search and the deepest matching pool is used as a proxy."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._last_call = 0.0
        self._pool_cache: dict[str, tuple[float, Optional[tuple[str, str]]]] = {}
        self._candle_cache: dict[tuple, tuple[float, list]] = {}

    def _get(self, path: str, params: Optional[dict] = None) -> dict:
        with self._lock:
            wait = GECKO_MIN_INTERVAL_SEC - (time.time() - self._last_call)
            if wait > 0:
                time.sleep(wait)
            self._last_call = time.time()
        resp = requests.get(GECKO_BASE_URL + path, params=params, timeout=12,
                            headers={"Accept": "application/json;version=20230302"})
        if resp.status_code == 429:
            raise RuntimeError("GeckoTerminal rate limited (429)")
        resp.raise_for_status()
        return resp.json()

    def find_pool(self, base: str) -> Optional[tuple[str, str]]:
        cached = self._pool_cache.get(base)
        if cached and time.time() - cached[0] < 6 * 3600:
            return cached[1]
        best, best_liq = None, 0.0
        data = self._get("/search/pools", {"query": base})
        for pool in data.get("data", []):
            attrs = pool.get("attributes") or {}
            name = str(attrs.get("name", "")).upper()
            if name.split("/")[0].strip() != base.upper():
                continue
            liq = safe_float(attrs.get("reserve_in_usd"), 0.0) or 0.0
            network = (((pool.get("relationships") or {}).get("network") or {}).get("data") or {}).get("id")
            address = attrs.get("address")
            if network and address and liq > best_liq:
                best, best_liq = (network, address), liq
        self._pool_cache[base] = (time.time(), best)
        return best

    def ohlcv(self, base: str, timeframe: str, limit: int) -> list[list[float]]:
        unit, agg = {"1m": ("minute", 1), "5m": ("minute", 5), "1h": ("hour", 1)}[timeframe]
        key = (base, timeframe, limit)
        cached = self._candle_cache.get(key)
        if cached and time.time() - cached[0] < 45:
            return cached[1]
        pool = self.find_pool(base)
        if not pool:
            raise RuntimeError(f"No GeckoTerminal pool found for {base}")
        net, addr = pool
        data = self._get(f"/networks/{net}/pools/{addr}/ohlcv/{unit}",
                         {"aggregate": agg, "limit": limit, "currency": "usd"})
        rows = (((data.get("data") or {}).get("attributes") or {}).get("ohlcv_list")) or []
        candles = sorted(([float(r[0]) * 1000.0] + [float(x) for x in r[1:6]] for r in rows), key=lambda r: r[0])
        self._candle_cache[key] = (time.time(), candles)
        return candles


class MarketData:
    TF_SECONDS = {"1m": 60, "5m": 300, "1h": 3600}

    def __init__(self, gw: ExchangeGateway) -> None:
        self.gw = gw
        self.gecko = GeckoClient()

    def candles(self, symbol: str, timeframe: str, limit: int) -> list[list[float]]:
        """Closed candles, oldest first. Order of sources follows OHLCV_SOURCE."""
        base = str(self.gw.market(symbol).get("base", "")).upper()
        sources = {"mexc": ["mexc"], "gecko": ["gecko"]}.get(OHLCV_SOURCE, ["mexc", "gecko"])
        errors = []
        for src in sources:
            try:
                rows = self.gw.ohlcv(symbol, timeframe, limit) if src == "mexc" else \
                    self.gecko.ohlcv(base, timeframe, limit)
                rows = self._closed(rows, self.TF_SECONDS[timeframe])
                if len(rows) >= 5:
                    return rows
                errors.append(f"{src}: only {len(rows)} candles")
            except Exception as exc:
                errors.append(f"{src}: {redact(exc)}")
        raise RuntimeError("No candle data (" + "; ".join(errors) + ")")

    @staticmethod
    def _closed(rows: list[list[float]], tf_sec: int) -> list[list[float]]:
        if rows and rows[-1][0] / 1000.0 + tf_sec > time.time():     # drop the still-forming candle
            rows = rows[:-1]
        return rows


market_data = MarketData(gateway)


# ==============================================================================
# Indicators
# ==============================================================================
def true_ranges(candles: list[list[float]]) -> list[float]:
    trs = []
    for i in range(1, len(candles)):
        _, _, h, l, _, _ = candles[i]
        pc = candles[i - 1][4]
        trs.append(max(h - l, abs(h - pc), abs(l - pc)))
    return trs


def atr(candles: list[list[float]], period: int = 14) -> Optional[float]:
    """Wilder ATR over closed candles."""
    trs = true_ranges(candles)
    if len(trs) < period:
        return None
    value = sum(trs[:period]) / period
    for tr in trs[period:]:
        value = (value * (period - 1) + tr) / period
    return value


def ema(values: list[float], period: int) -> Optional[float]:
    if len(values) < period:
        return None
    k = 2.0 / (period + 1)
    e = sum(values[:period]) / period
    for v in values[period:]:
        e = v * k + e * (1 - k)
    return e


def trend_bias_1h(candles_1h: list[list[float]]) -> int:
    """+1 bullish, -1 bearish, 0 neutral/unknown: close vs EMA21 and EMA8 vs EMA21 on 1h closes."""
    closes = [c[4] for c in candles_1h]
    fast, slow = ema(closes, 8), ema(closes, 21)
    if fast is None or slow is None:
        return 0
    if closes[-1] > slow and fast > slow:
        return 1
    if closes[-1] < slow and fast < slow:
        return -1
    return 0


def volume_acceleration(candles_5m: list[list[float]], recent: int = 3, baseline: int = 12) -> Optional[float]:
    vols = [c[5] for c in candles_5m]
    if len(vols) < recent + baseline:
        return None
    base = sum(vols[-(recent + baseline):-recent]) / baseline
    if base <= 0:
        return None
    return (sum(vols[-recent:]) / recent) / base


# ==============================================================================
# Plan object passed Research -> Analysis -> Trader
# ==============================================================================
@dataclass
class SignalContext:
    signal_id: str
    action: str
    side: str                      # long | short
    symbol: str                    # unified CCXT symbol
    payload: TradingViewPayload
    leverage: int


@dataclass
class SignalPlan:
    ctx: SignalContext
    ref_price: float
    bid: float
    ask: float
    sl: float
    tp1: float
    tp2: float
    stop_distance: float
    atr: float
    atr_pct: float
    trend_bias: int
    volume_accel: float
    rr: float
    cost_pct: float
    expected_move_pct: float
    risk_scalar: float             # volatility scalar x gap haircut
    vol_scalar: float
    gap_haircut: float
    bias_snapshot: dict = field(default_factory=dict)


# ==============================================================================
# Research & Filtering Agent
# ==============================================================================
class ResearchAgent:
    stage = "research"

    def __init__(self, gw: ExchangeGateway, st: StateStore, dyn: DynamicBias) -> None:
        self.gw, self.store, self.bias = gw, st, dyn

    def evaluate(self, payload: TradingViewPayload, action: str, signal_id: str) -> SignalContext:
        if action not in {"BUY_LONG", "SELL_SHORT"}:
            raise SignalRejected(self.stage, f"Unsupported entry action {action}")
        self.check_freshness(payload)

        if MONDAY_STANDDOWN and datetime.now(timezone.utc).weekday() == 0:
            raise SignalRejected(self.stage, "Monday stand-down (UTC) is active")
        if BREAKER_HALTS_ENTRIES and self.bias.is_suspended():
            raise SignalRejected(self.stage, "circuit breaker active: entries halted")

        try:
            symbol = self.gw.resolve_symbol(payload.symbol)
            self.gw.check_allowlist(symbol)
        except ValueError as exc:
            raise SignalRejected(self.stage, str(exc))
        m = self.gw.market(symbol)
        if not (m.get("swap") and m.get("linear") and m.get("settle") == "USDT"):
            raise SignalRejected(self.stage, f"{symbol} is not a USDT-M linear swap")

        leverage = payload.leverage or DEFAULT_LEVERAGE
        if leverage > MAX_LEVERAGE:
            raise SignalRejected(self.stage, f"leverage {leverage}x exceeds MAX_LEVERAGE={MAX_LEVERAGE}x")

        side = "long" if action == "BUY_LONG" else "short"
        self.check_capacity(symbol)
        last = self.store.last_trade_ts(symbol)
        if SYMBOL_COOLDOWN_SEC > 0 and last and 0 <= time.time() - last < SYMBOL_COOLDOWN_SEC:
            raise SignalRejected(self.stage, f"{symbol} cooldown: {int(SYMBOL_COOLDOWN_SEC - (time.time() - last))}s left")
        return SignalContext(signal_id, action, side, symbol, payload, leverage)

    @staticmethod
    def check_freshness(payload: TradingViewPayload) -> None:
        ts = parse_timestamp(payload.timestamp)
        if ts is None:
            return
        age = time.time() - ts
        if age > SIGNAL_MAX_AGE_SEC:
            raise SignalRejected("research", f"signal is stale by {age:.1f}s")
        if age < -SIGNAL_MAX_FUTURE_SEC:
            raise SignalRejected("research", f"signal timestamp is {abs(age):.1f}s in the future")

    def check_capacity(self, symbol: str) -> None:
        """Position-slot and one-position-per-symbol limits (re-run by the Trader under the trade lock)."""
        limit = int(min(MAX_OPEN_POSITIONS, self.bias.effective()["max_open_positions"]))
        active = self.store.active_trades()
        if any(t["symbol"] == symbol for t in active):
            raise SignalRejected(self.stage, f"position already open on {symbol}; no pyramiding or flipping")
        count = len(active)
        if is_live():
            try:
                live = self.gw.positions()
                if any(p.symbol == symbol for p in live):
                    raise SignalRejected(self.stage, f"exchange position already exists on {symbol}")
                count = max(count, len(live))
            except SignalRejected:
                raise
            except Exception as exc:
                raise SignalRejected(self.stage, f"cannot verify open positions: {redact(exc)}")
        if limit > 0 and count >= limit:
            raise SignalRejected(self.stage, f"open-position limit reached ({count}/{limit})")


# ==============================================================================
# Analysis Agent
# ==============================================================================
class AnalysisAgent:
    stage = "analysis"

    def __init__(self, gw: ExchangeGateway, md: MarketData, st: StateStore, dyn: DynamicBias) -> None:
        self.gw, self.md, self.store, self.bias = gw, md, st, dyn

    def analyze(self, ctx: SignalContext) -> SignalPlan:
        p, side, symbol = ctx.payload, ctx.side, ctx.symbol
        eff = self.bias.effective()

        tk = self.gw.ticker(symbol)
        ref = tk["last"]
        dev = pct_distance(p.price, ref)
        if dev > MAX_PRICE_DEVIATION_PCT:
            raise SignalRejected(self.stage, f"TradingView price {p.price} differs from MEXC {ref} by {dev:.3f}%")

        try:
            c5 = self.md.candles(symbol, "5m", 120)
            c1h = self.md.candles(symbol, "1h", 60)
        except Exception as exc:
            raise SignalRejected(self.stage, f"market data unavailable: {redact(exc)}")
        a = atr(c5, ATR_PERIOD)
        if not a or a <= 0:
            raise SignalRejected(self.stage, "not enough 5m candles for ATR")
        atr_pct = a / ref * 100.0

        # Extreme volatility spike: last closed 5m true range far above the average.
        last_tr = true_ranges(c5[-2:])[-1]
        if VOL_SPIKE_MULT > 0 and last_tr > VOL_SPIKE_MULT * a:
            raise SignalRejected(self.stage, f"volatility spike: last 5m range {last_tr / a:.1f}x ATR")

        trend = trend_bias_1h(c1h)
        want = 1 if side == "long" else -1
        if REQUIRE_TREND_ALIGNMENT and trend == -want:
            raise SignalRejected(self.stage, f"{side} against 1h trend bias ({'bullish' if trend > 0 else 'bearish'})")

        vacc = volume_acceleration(c5)
        if vacc is None:
            raise SignalRejected(self.stage, "volume acceleration unavailable")
        if vacc < eff["min_volume_accel"]:
            raise SignalRejected(self.stage, f"volume acceleration {vacc:.2f} < {eff['min_volume_accel']:.2f}")

        # ---- levels (payload first, ATR-derived when missing) ------------------------------------
        dirn = 1 if side == "long" else -1
        sl = p.sl if p.sl is not None else ref - dirn * SL_ATR_MULT * a
        stop_dist = abs(ref - sl)
        if stop_dist <= 0 or (sl - ref) * dirn >= 0:
            raise SignalRejected(self.stage, f"{side} requires SL on the losing side of the current price")
        tp1 = p.tp1 if p.tp1 is not None else ref + dirn * TP1_R_MULT * stop_dist
        tp2 = p.tp2 if p.tp2 is not None else ref + dirn * TP2_R_MULT * stop_dist
        if (tp1 - ref) * dirn <= 0:
            raise SignalRejected(self.stage, f"{side} requires TP1 on the winning side of the current price")
        if (tp2 - tp1) * dirn <= 0:
            raise SignalRejected(self.stage, f"{side} requires TP2 beyond TP1")

        stop_pct = stop_dist / ref * 100.0
        if stop_pct < MIN_STOP_DISTANCE_PCT:
            raise SignalRejected(self.stage, f"stop distance {stop_pct:.3f}% < MIN_STOP_DISTANCE_PCT")
        if stop_dist < MIN_STOP_ATR_MULT * a:
            raise SignalRejected(self.stage, f"stop {stop_dist / a:.2f} ATR is inside the noise (min {MIN_STOP_ATR_MULT})")

        # ---- SOP Sentinel gates --------------------------------------------------------------------
        rr = abs(tp1 - ref) / stop_dist
        if rr + 1e-9 < MIN_RR:
            raise SignalRejected(self.stage, f"R:R to TP1 is {rr:.2f} < {MIN_RR}")

        spread_pct = max(0.0, (tk["ask"] - tk["bid"]) / ref * 100.0)
        observed = self.store.median_exec_drag_pct()
        slip_cost = max(observed or 0.0, 2.0 * EST_SLIPPAGE_PCT)          # entry + exit
        cost_pct = 2.0 * TAKER_FEE_PCT + spread_pct + slip_cost
        expected_pct = abs(tp1 - ref) / ref * 100.0
        if cost_pct > MAX_COST_TO_TARGET * expected_pct:
            raise SignalRejected(self.stage, f"round-trip cost {cost_pct:.3f}% > {MAX_COST_TO_TARGET:.0%} of "
                                             f"TP1 move {expected_pct:.3f}%")

        # ---- volatility scalar and observed stop-out gap haircut -------------------------------------
        vol_scalar = clamp(VOL_REF_ATR_PCT / atr_pct, MIN_RISK_SCALAR, 1.0)
        ratios = self.store.stop_loss_drag_ratios()
        haircut = 1.0
        if len(ratios) >= 3:
            med = statistics.median(ratios)
            if med > 1.0:
                haircut = clamp(1.0 / med, 0.5, 1.0)

        return SignalPlan(ctx, ref, tk["bid"], tk["ask"], sl, tp1, tp2, stop_dist, a, atr_pct, trend, vacc, rr,
                          cost_pct, expected_pct, vol_scalar * haircut, vol_scalar, haircut,
                          {"bias": eff, "suspended": self.bias.is_suspended()})


# ==============================================================================
# Trader Agent
# ==============================================================================
class TraderAgent:
    stage = "trader"

    def __init__(self, gw: ExchangeGateway, md: MarketData, st: StateStore, dyn: DynamicBias,
                 research: ResearchAgent, core: "ExecutionCore") -> None:
        self.gw, self.md, self.store, self.bias, self.research, self.core = gw, md, st, dyn, research, core

    # -- lower-timeframe validation ------------------------------------------------------------------
    def ltf_check(self, plan: SignalPlan) -> None:
        """Do not buy into an actively falling 1m tape (or sell into an actively rising one)."""
        if not LTF_CHECK_ENABLED:
            return
        symbol, side = plan.ctx.symbol, plan.ctx.side
        try:
            c1 = self.md.candles(symbol, "1m", 10)
            live = self.gw.ticker(symbol)["last"]
        except Exception as exc:
            logger.warning("LTF check skipped for %s (no 1m data): %s", symbol, redact(exc))
            return
        closes = [c[4] for c in c1]
        if len(closes) < 4:
            return
        drift = (live - closes[-4]) / closes[-4] * 100.0
        if side == "long":
            falling = drift < -LTF_MAX_DROP_PCT or (closes[-1] < closes[-2] < closes[-3] and live < closes[-1])
        else:
            falling = drift > LTF_MAX_DROP_PCT or (closes[-1] > closes[-2] > closes[-3] and live > closes[-1])
        if falling:
            raise SignalRejected(self.stage, f"LTF price is moving against the {side} (1m drift {drift:+.3f}%)")

    # -- sizing ----------------------------------------------------------------------------------------
    def requested_risk(self, payload: TradingViewPayload, free_usdt: float) -> float:
        if payload.risk_usd is not None:
            risk = payload.risk_usd
        elif payload.risk_pct is not None:
            risk = free_usdt * payload.risk_pct / 100.0
        else:
            risk = DEFAULT_RISK_USD
        risk = max(0.01, float(risk))
        return min(risk, MAX_RISK_USD) if MAX_RISK_USD > 0 else risk

    def size(self, plan: SignalPlan, free_usdt: float) -> tuple[float, dict[str, float]]:
        """Contracts = risk budget / (stop distance x contractSize), capped by margin; all rounded with
        exchange.amount_to_precision. Risk budget never exceeds MAX_RISK_USD and is scaled by volatility
        and the observed stop-out gap."""
        symbol = plan.ctx.symbol
        cs = self.gw.contract_size(symbol)
        requested = self.requested_risk(plan.ctx.payload, free_usdt)
        budget = min(requested, MAX_RISK_USD) * plan.risk_scalar
        after_buffer = budget * max(0.0, 1.0 - RISK_BUFFER_PCT / 100.0)
        by_risk = after_buffer / (plan.stop_distance * cs)
        by_margin = free_usdt * MARGIN_UTILIZATION * plan.ctx.leverage / (plan.ref_price * cs)
        raw = min(by_risk, by_margin)
        if raw <= 0:
            raise SignalRejected(self.stage, "calculated contract quantity is zero")
        contracts = self.gw.p_amount(symbol, raw)
        if contracts <= 0:
            raise SignalRejected(self.stage, "quantity rounds to zero at MEXC contract precision")
        m = self.gw.market(symbol)
        lim = m.get("limits") or {}
        min_amt = safe_float((lim.get("amount") or {}).get("min"))
        min_cost = safe_float((lim.get("cost") or {}).get("min"))
        notional = contracts * cs * plan.ref_price
        if min_amt is not None and contracts < min_amt:
            raise SignalRejected(self.stage, f"quantity {contracts} below MEXC minimum {min_amt}")
        if min_cost is not None and notional < min_cost:
            raise SignalRejected(self.stage, f"notional {notional:.4f} below MEXC minimum {min_cost}")
        return contracts, {"contract_size": cs, "requested_risk": requested, "risk_budget": budget,
                           "by_risk": by_risk, "by_margin": by_margin, "notional": notional,
                           "planned_risk_usd": contracts * cs * plan.stop_distance}

    def paper_free_usdt(self) -> float:
        """Paper wallet: real balance when keys exist and it is non-zero, else PAPER_BALANCE_USDT;
        minus margin tied up in open paper trades."""
        free = 0.0
        if self.gw.has_keys:
            try:
                free = self.gw.free_usdt()
            except Exception:
                free = 0.0
        if free <= 0:
            free = PAPER_BALANCE_USDT
            logger.info("[PAPER] zero/unavailable exchange balance; using mock balance %.2f USDT", free)
        used = sum((t["entry_price"] * (t.get("remaining") or t["amount"]) * t["contract_size"]) /
                   max(1, t.get("leverage") or 1) for t in self.store.active_trades() if t.get("mode") == "PAPER")
        return max(0.0, free - used)

    # -- execution -------------------------------------------------------------------------------------
    def execute(self, plan: SignalPlan) -> dict[str, Any]:
        ctx = plan.ctx
        self.ltf_check(plan)                                   # network I/O outside the trade lock
        with TRADE_LOCK:
            self.research.check_capacity(ctx.symbol)           # slots may have changed during analysis
            return self._open_live(plan) if is_live() else self._open_paper(plan)

    def _trade_row(self, plan: SignalPlan, mode: str, amount: float, entry: float, sizing: dict,
                   entry_order_id: Optional[str]) -> dict[str, Any]:
        ctx = plan.ctx
        cs = sizing["contract_size"]
        stop_dist = abs(entry - plan.sl)
        dirn = 1 if ctx.side == "long" else -1
        slip = dirn * (entry - plan.ref_price) / plan.ref_price * 100.0       # positive = adverse
        return {
            "signal_id": ctx.signal_id, "symbol": ctx.symbol, "side": ctx.side, "mode": mode,
            "amount": amount, "remaining": amount, "contract_size": cs, "leverage": ctx.leverage,
            "entry_price": entry, "signal_price": ctx.payload.price, "ref_price": plan.ref_price,
            "stop_price": plan.sl, "current_sl": plan.sl, "stop_distance": stop_dist,
            "risk_usd": amount * cs * stop_dist, "tp1_price": plan.tp1, "tp2_price": plan.tp2,
            "atr": plan.atr, "atr_pct": plan.atr_pct, "trend_bias": plan.trend_bias,
            "volume_accel": plan.volume_accel, "cost_pct": plan.cost_pct, "rr": plan.rr,
            "entry_order_id": entry_order_id, "status": "OPEN", "peak_price": entry, "trough_price": entry,
            "mfe_pct": 0.0, "mae_pct": 0.0, "entry_slippage_pct": slip,
            "fees_usd": amount * cs * entry * TAKER_FEE_PCT / 100.0,
            "realized_pnl_usd": -(amount * cs * entry * TAKER_FEE_PCT / 100.0),   # entry fee (estimate)
            "tp1_done": 0, "tp2_done": 0,
            "trade_path": "[]", "partials": "[]",
            "bias_snapshot": json.dumps({"bias": plan.bias_snapshot, "risk_scalar": plan.risk_scalar,
                                         "vol_scalar": plan.vol_scalar, "gap_haircut": plan.gap_haircut}),
        }

    def _summary(self, status: str, plan: SignalPlan, trade_id: int, row: dict, sizing: dict) -> dict[str, Any]:
        return {"status": status, "trade_id": trade_id, "symbol": plan.ctx.symbol, "side": plan.ctx.side,
                "contracts": row["amount"], "entry_price": row["entry_price"], "sl": plan.sl, "tp1": plan.tp1,
                "tp2": plan.tp2, "rr": round(plan.rr, 3), "risk_usd": round(row["risk_usd"], 4),
                "risk_scalar": round(plan.risk_scalar, 3), "atr_pct": round(plan.atr_pct, 4),
                "trend_bias": plan.trend_bias, "volume_accel": round(plan.volume_accel, 3),
                "cost_pct": round(plan.cost_pct, 4), "notional": round(sizing["notional"], 4),
                "leverage": plan.ctx.leverage}

    def _open_paper(self, plan: SignalPlan) -> dict[str, Any]:
        free = self.paper_free_usdt()
        contracts, sizing = self.size(plan, free)
        fill = plan.ask if plan.ctx.side == "long" else plan.bid       # pay the spread like a market order
        row = self._trade_row(plan, "PAPER", contracts, fill, sizing, None)
        trade_id = self.store.create_trade(row)
        logger.info("[PAPER] OPEN #%s %s %s %s @ %s | SL %s TP1 %s TP2 %s | risk $%.2f", trade_id, plan.ctx.side,
                    plan.ctx.symbol, contracts, fill, plan.sl, plan.tp1, plan.tp2, row["risk_usd"])
        return self._summary("PAPER_OPEN", plan, trade_id, row, sizing)

    def _open_live(self, plan: SignalPlan) -> dict[str, Any]:
        ctx, gw = plan.ctx, self.gw
        symbol, side = ctx.symbol, ctx.side
        gw.prepare_account(symbol, ctx.leverage, side)
        free = gw.free_usdt()
        if free <= 0:
            raise SignalRejected(self.stage, "no free USDT margin available")
        contracts, sizing = self.size(plan, free)
        order_side = "buy" if side == "long" else "sell"
        logger.info("ENTRY %s %s | ref=%s qty=%s cs=%s budget=$%.2f scalar=%.2f notional=$%.2f", side, symbol,
                    plan.ref_price, contracts, sizing["contract_size"], sizing["risk_budget"], plan.risk_scalar,
                    sizing["notional"])

        order: Optional[dict] = None
        try:
            order = gw.market_order(symbol, order_side, contracts, False, "entry", ctx.signal_id)
        except ccxt.NetworkError as exc:     # ambiguous: the order may have been accepted
            logger.warning("Entry order outcome unknown (%s); reconciling against the live position", redact(exc))

        pos: Optional[PositionInfo] = None
        for _ in range(4):
            time.sleep(POSITION_SYNC_DELAY_SEC)
            try:
                pos = next((p for p in gw.positions(symbol) if p.side == side and p.contracts > 0), None)
            except Exception as exc:
                logger.warning("Position fetch failed during entry reconciliation: %s", redact(exc))
            if pos:
                break
        if pos is None:
            raise RuntimeError("Entry was submitted but the live MEXC position could not be confirmed")

        live_amount = gw.p_amount(symbol, pos.contracts) or pos.contracts       # partial fills: trust the exchange
        entry = pos.entry_price or gw.fill_price(order, symbol) or plan.ref_price
        if live_amount + 1e-12 < contracts:
            logger.warning("Partial fill: wanted %s, filled %s", contracts, live_amount)

        row = self._trade_row(plan, "LIVE", live_amount, entry, sizing,
                              str(order.get("id")) if order and order.get("id") is not None else None)
        trade_id = self.store.create_trade(row)

        dirn = 1 if side == "long" else -1
        if (entry - plan.sl) * dirn <= 0:      # filled through the stop already
            self.core.emergency_close(self.store.get_trade(trade_id), "FAILSAFE", "entry filled beyond stop")
            raise RuntimeError("Entry filled beyond the stop price; position closed")

        close_side = "sell" if side == "long" else "buy"
        sl_id: Optional[str] = None
        last_exc: Optional[Exception] = None
        for attempt in range(3):
            try:
                o = gw.stop_order(symbol, close_side, live_amount, plan.sl, "sl", ctx.signal_id)
                sl_id = str(o.get("id")) if o.get("id") is not None else None
                break
            except Exception as exc:
                last_exc = exc
                logger.error("Stop-loss placement attempt %d failed: %s", attempt + 1, redact(exc))
                time.sleep(0.7 * (attempt + 1))
        if sl_id is None and last_exc is not None:
            # The trade stays OPEN (monitored) even if the emergency close fails: the monitor's watchdog keeps
            # retrying the stop and the close, so a position is never orphaned.
            if FAILSAFE_CLOSE_ON_SL_FAILURE:
                logger.critical("PROTECTION FAILURE on %s: emergency market close", symbol)
                self.core.emergency_close(self.store.get_trade(trade_id), "FAILSAFE",
                                          "stop-loss could not be placed")
            raise RuntimeError(f"Stop-loss could not be placed: {redact(last_exc)}")

        self.store.update_trade(trade_id, sl_order_id=sl_id, status="OPEN")
        return self._summary("EXECUTED", plan, trade_id, row, sizing)


# ==============================================================================
# Exit ladder (pure functions - no I/O, unit-testable)
# ==============================================================================
@dataclass
class Decision:
    action: Optional[str]          # STOP | SCALE | TIME_STOP | None
    reason: str = ""               # SL | RUNNER_TRAIL | TP1 | TP2 | TIME_STOP
    level: float = 0.0             # stop level in force after this tick


def direction(side: str) -> int:
    return 1 if side == "long" else -1


def breakeven_price(side: str, entry: float) -> float:
    """Entry plus round-trip taker fees and a small buffer (below entry for shorts)."""
    return entry * (1.0 + direction(side) * (2.0 * TAKER_FEE_PCT + BE_BUFFER_PCT) / 100.0)


def compute_stop_level(t: dict, trail_mult: float) -> float:
    """Before TP1: the initial stop. After TP1: breakeven, ratcheted by a trail of
    trail_mult x stop distance behind the best price (high for longs, low for shorts).
    The trail can never be worse than breakeven."""
    side, entry = t["side"], t["entry_price"]
    initial = t["stop_price"]
    if not t.get("tp1_done"):
        return initial
    be = breakeven_price(side, entry)
    dist = t["stop_distance"]
    if side == "long":
        trail = (t.get("peak_price") or entry) - trail_mult * dist
        return max(be, trail, initial)
    trail = (t.get("trough_price") or entry) + trail_mult * dist
    return min(be, trail, initial)


def exit_decision(t: dict, price: float, now: float, trail_mult: float, time_stop_s: float,
                  grace: float = 0.0) -> Decision:
    """One decision of the exit ladder for an open trade. Priority: stop -> TP1 -> TP2 -> time stop."""
    side = t["side"]
    d = direction(side)
    tp1_done, tp2_done = bool(t.get("tp1_done")), bool(t.get("tp2_done"))
    level = compute_stop_level(t, trail_mult)
    cur = t.get("current_sl")
    if cur is not None:                                   # the stop only ever ratchets in the trade's favour
        level = level if (level - cur) * d >= 0 else cur
    if (price - level) * d <= -grace:                     # price at/through the stop
        return Decision("STOP", "RUNNER_TRAIL" if tp1_done else "SL", level)
    if not tp1_done and (price - t["tp1_price"]) * d >= 0:
        return Decision("SCALE", "TP1", level)
    if tp1_done and not tp2_done and t.get("tp2_price") and (price - t["tp2_price"]) * d >= 0:
        return Decision("SCALE", "TP2", level)
    if not tp1_done and time_stop_s > 0 and (now - t["created_at"]) >= time_stop_s:
        return Decision("TIME_STOP", "TIME_STOP", level)
    return Decision(None, "", level)


# ==============================================================================
# Diagnostic Agent: excursions, trade path, close-out analytics
# ==============================================================================
class DiagnosticAgent:
    PATH_MAX_POINTS = 720
    PATH_PERSIST_SEC = 30.0

    def __init__(self, st: StateStore, dyn: DynamicBias) -> None:
        self.store, self.bias = st, dyn
        self._paths: dict[int, list] = {}
        self._last_snap: dict[int, float] = {}
        self._last_persist: dict[int, float] = {}

    def track(self, t: dict, price: float, now: float) -> None:
        """Update peak/trough, MFE/MAE (% of entry, positive numbers) and the time:price path."""
        tid, entry, d = t["id"], t["entry_price"], direction(t["side"])
        peak = max(t.get("peak_price") or entry, price)
        trough = min(t.get("trough_price") or entry, price)
        fav = ((peak - entry) if d == 1 else (entry - trough)) / entry * 100.0
        adv = ((entry - trough) if d == 1 else (peak - entry)) / entry * 100.0
        mfe, mae = max(t.get("mfe_pct") or 0.0, fav), max(t.get("mae_pct") or 0.0, adv)
        changed = (peak != t.get("peak_price") or trough != t.get("trough_price")
                   or mfe != t.get("mfe_pct") or mae != t.get("mae_pct"))
        t.update(peak_price=peak, trough_price=trough, mfe_pct=mfe, mae_pct=mae)

        path = self._paths.get(tid)
        if path is None:
            try:
                path = json.loads(t.get("trade_path") or "[]")
            except Exception:
                path = []
            self._paths[tid] = path
        path_dirty = False
        if now - self._last_snap.get(tid, 0.0) >= PATH_SNAPSHOT_SEC:
            path.append([round(now - t["created_at"], 1), price])
            self._last_snap[tid] = now
            path_dirty = True
            if len(path) > self.PATH_MAX_POINTS:
                del path[1:-1:2]                           # thin by half, keep first and last
        fields: dict[str, Any] = {}
        if changed:
            fields.update(peak_price=peak, trough_price=trough, mfe_pct=mfe, mae_pct=mae)
        if path_dirty and now - self._last_persist.get(tid, 0.0) >= self.PATH_PERSIST_SEC:
            fields["trade_path"] = json.dumps(path)
            self._last_persist[tid] = now
        if fields:
            self.store.update_trade(tid, **fields)

    def path_json(self, t: dict) -> str:
        path = self._paths.get(t["id"])
        return json.dumps(path) if path is not None else (t.get("trade_path") or "[]")

    def on_close(self, t: dict) -> None:
        tid = t["id"]
        for d in (self._paths, self._last_snap, self._last_persist):
            d.pop(tid, None)
        logger.info("[DIAG] CLOSED #%s %s %s %s | exit=%s pnl=$%.4f (%.3f%%, %.2fR) mfe=%.3f%% mae=%.3f%% drag=%.4f%%",
                    tid, t["mode"], t["side"], t["symbol"], t.get("exit_reason"), t.get("realized_pnl_usd") or 0.0,
                    t.get("realized_pnl_pct") or 0.0, t.get("r_multiple") or 0.0, t.get("mfe_pct") or 0.0,
                    t.get("mae_pct") or 0.0, t.get("exec_drag_pct") or 0.0)
        try:
            self.bias.on_trade_closed(self.store)           # circuit breaker
        except Exception as exc:
            logger.warning("Circuit-breaker check failed: %s", redact(exc))


# ==============================================================================
# Execution core: closes, scale-outs, stop management (LIVE and PAPER)
# ==============================================================================
class ExecutionCore:
    def __init__(self, gw: ExchangeGateway, st: StateStore, diag: DiagnosticAgent) -> None:
        self.gw, self.store, self.diag = gw, st, diag

    def persist(self, t: dict, *keys: str) -> None:
        self.store.update_trade(t["id"], **{k: t[k] for k in keys})

    # -- exit legs ------------------------------------------------------------------------------
    def _market_close_leg(self, t: dict, qty: float, decision_price: float, role: str) -> float:
        """Reduce-only market close of `qty` contracts. PAPER fills at the live bid/ask. LIVE retries with
        back-off; after an ambiguous timeout it checks the position instead of re-sending blindly."""
        symbol, side = t["symbol"], t["side"]
        if t["mode"] == "PAPER":
            tk = self.gw.ticker(symbol)
            return tk["bid"] if side == "long" else tk["ask"]
        close_side = "sell" if side == "long" else "buy"
        before = t["remaining"]
        last: Optional[Exception] = None
        for attempt in range(3):
            try:
                order = self.gw.market_order(symbol, close_side, qty, True, role.lower(), t["signal_id"])
                return self.gw.fill_price(order, symbol) or decision_price
            except ccxt.NetworkError as exc:
                last = exc
                try:
                    pos = next((p for p in self.gw.positions(symbol) if p.side == side), None)
                    if pos is None or pos.contracts <= before - qty * 0.5:
                        logger.warning("Close order timed out but the position already shrank; treating as filled")
                        return decision_price
                except Exception:
                    pass
            except Exception as exc:
                last = exc
            time.sleep(0.7 * (attempt + 1))
        assert last is not None
        raise last

    def _record_leg(self, t: dict, qty: float, fill: float, decision_price: float, reason: str) -> None:
        d, cs, entry = direction(t["side"]), t["contract_size"], t["entry_price"]
        gross = d * (fill - entry) * qty * cs
        fee = fill * qty * cs * TAKER_FEE_PCT / 100.0
        slip = d * (decision_price - fill) / decision_price * 100.0          # positive = adverse
        partials = json.loads(t.get("partials") or "[]")
        partials.append({"t": round(time.time(), 1), "reason": reason, "qty": qty, "price": fill,
                         "decision_price": decision_price, "slip_pct": round(slip, 5),
                         "pnl_usd": round(gross - fee, 6)})
        t["partials"] = json.dumps(partials)
        t["realized_pnl_usd"] = (t.get("realized_pnl_usd") or 0.0) + gross - fee
        t["fees_usd"] = (t.get("fees_usd") or 0.0) + fee
        t["remaining"] = max(0.0, t["remaining"] - qty)
        self.persist(t, "partials", "realized_pnl_usd", "fees_usd", "remaining")
        logger.info("[%s] #%s %s leg %s: %s contracts @ %s (decision %s, slip %.4f%%) pnl $%.4f", t["mode"], t["id"],
                    t["symbol"], reason, qty, fill, decision_price, slip, gross - fee)

    def finalize(self, t: dict, reason: str) -> None:
        partials = json.loads(t.get("partials") or "[]")
        qty_sum = sum(p["qty"] for p in partials) or 0.0
        exit_price = (sum(p["price"] * p["qty"] for p in partials) / qty_sum) if qty_sum > 0 else None
        exit_slip = (sum(p["slip_pct"] * p["qty"] for p in partials) / qty_sum) if qty_sum > 0 else 0.0
        entry_slip = t.get("entry_slippage_pct") or 0.0
        notional = t["entry_price"] * t["amount"] * t["contract_size"]
        pnl = t.get("realized_pnl_usd") or 0.0
        t.update(status="CLOSED", exit_reason=reason, exit_price=exit_price, closed_at=time.time(),
                 exit_slippage_pct=exit_slip, exec_drag_pct=entry_slip + exit_slip,
                 realized_pnl_pct=(pnl / notional * 100.0) if notional > 0 else 0.0,
                 r_multiple=(pnl / t["risk_usd"]) if (t.get("risk_usd") or 0) > 0 else None,
                 trade_path=self.diag.path_json(t), remaining=0.0)
        self.persist(t, "status", "exit_reason", "exit_price", "closed_at", "exit_slippage_pct", "exec_drag_pct",
                     "realized_pnl_pct", "r_multiple", "trade_path", "remaining", "mfe_pct", "mae_pct",
                     "peak_price", "trough_price", "tp1_done", "tp2_done")
        self.diag.on_close(t)

    def close_all(self, t: dict, price: float, reason: str, decision_price: Optional[float] = None) -> bool:
        """Close whatever is still open on this trade and finalize it. Returns True when closed."""
        symbol, side = t["symbol"], t["side"]
        remaining = t["remaining"]
        if t["mode"] == "LIVE":
            pos = next((p for p in self.gw.positions(symbol) if p.side == side), None)
            if pos is None:
                self.finalize_external(t, price)
                return True
            remaining = pos.contracts
        decision = decision_price if decision_price else price
        fill = self._market_close_leg(t, remaining, decision, reason)
        self._record_leg(t, remaining, fill, decision, reason)
        if t["mode"] == "LIVE":
            self.gw.cancel(t.get("sl_order_id"), symbol)
            t["sl_order_id"] = None
            self.persist(t, "sl_order_id")
        self.finalize(t, reason)
        return True

    def emergency_close(self, t: Optional[dict], reason: str, detail: str = "") -> bool:
        """Fail-safe: try hard to flatten a position. Never raises; the trade stays OPEN (and therefore
        monitored) if every attempt fails."""
        if t is None:
            return False
        for attempt in range(3):
            try:
                tk = self.gw.ticker(t["symbol"])
                if self.close_all(t, tk["last"], reason):
                    logger.critical("EMERGENCY CLOSE done on %s (%s)", t["symbol"], detail)
                    return True
            except Exception as exc:
                logger.critical("EMERGENCY CLOSE attempt %d failed on %s: %s", attempt + 1, t["symbol"], redact(exc))
                time.sleep(0.8 * (attempt + 1))
        return False

    def finalize_external(self, t: dict, price: float) -> None:
        """The exchange position is gone (our stop trigger fired, or someone closed it by hand)."""
        symbol, side, d = t["symbol"], t["side"], direction(t["side"])
        close_side = "sell" if side == "long" else "buy"
        partials = json.loads(t.get("partials") or "[]")
        since = max([t["created_at"]] + [p["t"] for p in partials]) - 2.0
        fill = self.gw.closing_fills_avg(symbol, close_side, since)
        stop = t.get("current_sl") or t["stop_price"]
        if fill is None:
            fill = stop if (price - stop) * d <= 0 else price      # best estimate when fills are unavailable
        near_stop = (fill - stop) * d <= 0.25 * t["stop_distance"]
        reason = ("RUNNER_TRAIL" if t.get("tp1_done") else "SL") if near_stop else "EXTERNAL_CLOSE"
        if t["remaining"] > 0:
            self._record_leg(t, t["remaining"], fill, stop if near_stop else fill, reason)
        logger.warning("[LIVE] #%s %s: position no longer open on the exchange; recorded as %s", t["id"], symbol, reason)
        self.finalize(t, reason)

    def scale_out(self, t: dict, tag: str, fraction: float, price: float) -> None:
        """Sell `fraction` of the ORIGINAL position at TP1/TP2. Falls back to a full close when the rest
        would be below the exchange minimum."""
        gw, symbol = self.gw, t["symbol"]
        remaining = t["remaining"]
        qty = min(gw.p_amount(symbol, t["amount"] * fraction), remaining)
        min_amt = gw.min_amount(symbol)
        flag = "tp1_done" if tag == "TP1" else "tp2_done"
        if qty <= 0 or qty < min_amt:
            logger.warning("#%s %s scale-out of %.6f contracts is below the minimum %s; skipping the sale",
                           t["id"], tag, qty, min_amt)
            t[flag] = 1
            self.persist(t, flag)
            return
        if remaining - qty < max(min_amt, 1e-12):
            logger.info("#%s %s: remainder would be dust; closing the whole position", t["id"], tag)
            self.close_all(t, price, tag)
            return
        fill = self._market_close_leg(t, qty, price, tag)
        self._record_leg(t, qty, fill, price, tag)
        t[flag] = 1
        self.persist(t, flag)

    # -- stop management ------------------------------------------------------------------------
    def move_stop(self, t: dict, new_level: float, force: bool = False) -> bool:
        """Move the stop in force. PAPER: bookkeeping only. LIVE: place the new trigger FIRST, then cancel
        the old one, so the position is never unprotected. If the new trigger is refused the old one is
        kept (the software trail in the monitor still exits at the computed level)."""
        d, symbol = direction(t["side"]), t["symbol"]
        new_level = self.gw.p_price(symbol, new_level)
        cur = t.get("current_sl")
        if new_level <= 0 or (cur is not None and (new_level - cur) * d <= 0 and not force):
            return False
        now = time.time()
        if t["mode"] == "PAPER":
            t["current_sl"] = new_level
            self.persist(t, "current_sl")
            return True
        if not force and now - (t.get("last_stop_replace_at") or 0.0) < STOP_REPLACE_MIN_INTERVAL_SEC:
            return False
        close_side = "sell" if t["side"] == "long" else "buy"
        try:
            o = self.gw.stop_order(symbol, close_side, t["remaining"], new_level, "sl", t["signal_id"])
        except Exception as exc:
            logger.warning("#%s could not move stop to %s (keeping the existing trigger): %s", t["id"], new_level,
                           redact(exc))
            t["last_stop_replace_at"] = now
            self.persist(t, "last_stop_replace_at")
            return False
        old = t.get("sl_order_id")
        t["sl_order_id"] = str(o.get("id")) if o.get("id") is not None else None
        t["current_sl"] = new_level
        t["last_stop_replace_at"] = now
        self.persist(t, "sl_order_id", "current_sl", "last_stop_replace_at")
        if old and old != t["sl_order_id"]:
            self.gw.cancel(old, symbol)
        logger.info("[LIVE] #%s stop moved to %s", t["id"], new_level)
        return True

    def ensure_stop(self, t: dict) -> bool:
        """Watchdog for a LIVE trade whose exchange stop is missing. After 3 failures: emergency close."""
        close_side = "sell" if t["side"] == "long" else "buy"
        try:
            o = self.gw.stop_order(t["symbol"], close_side, t["remaining"], t["current_sl"] or t["stop_price"], "sl",
                                   t["signal_id"])
            t["sl_order_id"] = str(o.get("id")) if o.get("id") is not None else None
            t["protect_failures"] = 0
            self.persist(t, "sl_order_id", "protect_failures")
            logger.warning("[LIVE] #%s protective stop re-established", t["id"])
            return True
        except Exception as exc:
            t["protect_failures"] = (t.get("protect_failures") or 0) + 1
            self.persist(t, "protect_failures")
            logger.error("[LIVE] #%s stop re-placement failed (%d): %s", t["id"], t["protect_failures"], redact(exc))
            if t["protect_failures"] >= 3 and FAILSAFE_CLOSE_ON_SL_FAILURE:
                self.emergency_close(t, "FAILSAFE", "protective stop missing")
            return False


# ==============================================================================
# Active Position Monitor
# ==============================================================================
class PositionMonitor:
    def __init__(self, gw: ExchangeGateway, st: StateStore, dyn: DynamicBias, core: ExecutionCore,
                 diag: DiagnosticAgent) -> None:
        self.gw, self.store, self.bias, self.core, self.diag = gw, st, dyn, core, diag
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self.ticks = 0
        self.last_tick_at: Optional[float] = None

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._loop, name="position-monitor", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()

    def _loop(self) -> None:
        logger.info("Position monitor started (every %.1fs, time stop %.0f min)", MONITOR_INTERVAL_SEC,
                    TIME_STOP_MINUTES)
        while not self._stop.wait(MONITOR_INTERVAL_SEC):
            try:
                self.tick()
            except Exception as exc:
                logger.error("Monitor tick failed: %s", redact(exc))

    def tick(self) -> None:
        if not self.gw.ready:
            return
        with TRADE_LOCK:
            for t in self.store.active_trades():
                try:
                    self.process(t)
                except Exception as exc:
                    logger.error("Monitor error on trade #%s %s: %s", t.get("id"), t.get("symbol"), redact(exc))
            self.ticks += 1
            self.last_tick_at = time.time()

    def process(self, t: dict) -> None:
        symbol, side, live = t["symbol"], t["side"], t["mode"] == "LIVE"
        d = direction(side)
        price = self.gw.ticker(symbol)["last"]
        now = time.time()

        if live:
            pos = next((p for p in self.gw.positions(symbol) if p.side == side), None)
            if pos is None:
                self.core.finalize_external(t, price)
                return
            if abs(pos.contracts - t["remaining"]) > 1e-9:
                t["remaining"] = pos.contracts
                self.core.persist(t, "remaining")
            if not t.get("sl_order_id"):
                self.core.ensure_stop(t)
                if t.get("status") == "CLOSED":
                    return

        self.diag.track(t, price, now)
        eff = self.bias.effective()
        trail_mult = eff["stop_distance_mult"]
        grace = SW_STOP_GRACE_FRAC * t["stop_distance"] if (live and not t.get("tp1_done")) else 0.0
        dec = exit_decision(t, price, now, trail_mult, TIME_STOP_MINUTES * 60.0, grace)

        if dec.action == "STOP":
            logger.info("#%s %s STOP (%s) price=%s level=%s", t["id"], symbol, dec.reason, price, dec.level)
            self.core.close_all(t, price, dec.reason, decision_price=dec.level)
        elif dec.action == "TIME_STOP":
            logger.info("#%s %s TIME STOP after %.0f min without TP1", t["id"], symbol, (now - t["created_at"]) / 60)
            self.core.close_all(t, price, "TIME_STOP")
        elif dec.action == "SCALE":
            fraction = TP1_FRACTION if dec.reason == "TP1" else TP2_FRACTION
            logger.info("#%s %s %s reached at %s: scaling out %.0f%%", t["id"], symbol, dec.reason, price, fraction * 100)
            self.core.scale_out(t, dec.reason, fraction, price)
            if t.get("status") != "CLOSED":
                # TP1: stop to breakeven + trail. TP2: re-size the stop to the remaining runner.
                level = compute_stop_level(t, trail_mult)
                cur = t.get("current_sl")
                level = level if cur is None or (level - cur) * d >= 0 else cur
                self.core.move_stop(t, level, force=True)
        elif t.get("tp1_done"):
            step = STOP_REPLACE_MIN_STEP_FRAC * t["stop_distance"]
            cur = t.get("current_sl") or t["stop_price"]
            gain = (dec.level - cur) * d                       # how far the trail has improved the stop
            if gain >= step or (not live and gain > 0):
                self.core.move_stop(t, dec.level)


# ==============================================================================
# Maintainer / 2nd Brain Agent
# ==============================================================================
class MaintainerAgent:
    """Out-of-band calibration loop (every 15-30 min). Reads recent SQLite performance, builds a compact
    telemetry summary, asks a free OpenRouter model for bounded adjustments and writes them to
    dynamic_bias.json. Falls back to a deterministic rule set when no key / no valid LLM answer."""

    SYSTEM_PROMPT = (
        "You calibrate entry filters and trailing-stop distance for an automated crypto futures bot. "
        "Reply with ONE JSON object and nothing else. Optional keys: stop_distance_mult (number, trailing-stop "
        "distance multiplier), max_open_positions (integer), min_volume_accel (number, entry volume-acceleration "
        "filter), reason (string, max 200 chars). Every value must stay inside limits.bounds. Be conservative: "
        "change a value only when the data supports it. After losing streaks tighten (higher min_volume_accel, fewer "
        "open positions). When performance is healthy relax gradually toward limits.defaults. Widen "
        "stop_distance_mult only if runners were stopped out with large MFE; tighten it if gains were given back "
        "(MFE much larger than realized PnL). Omit keys you do not want to change.")

    def __init__(self, st: StateStore, dyn: DynamicBias) -> None:
        self.store, self.bias = st, dyn
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self.last_run: Optional[dict[str, Any]] = None

    def start(self) -> None:
        if not BRAIN_ENABLED:
            logger.info("2nd Brain disabled (BRAIN_ENABLED=false)")
            return
        if self._thread and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._loop, name="second-brain", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()

    def _loop(self) -> None:
        logger.info("2nd Brain started (every %.0f min, models=%s, llm=%s)", BRAIN_INTERVAL_MIN, BRAIN_MODELS,
                    "on" if OPENROUTER_API_KEY else "off -> heuristic fallback")
        if self._stop.wait(60.0):
            return
        while True:
            try:
                self.cycle()
            except Exception as exc:
                logger.error("2nd Brain cycle failed: %s", redact(exc))
            if self._stop.wait(BRAIN_INTERVAL_MIN * 60.0):
                return

    # -- telemetry ------------------------------------------------------------------------------
    def telemetry(self) -> dict[str, Any]:
        trades = self.store.closed_trades(BRAIN_LOOKBACK_TRADES)
        streak, _ = self.store.consecutive_losses()
        recent = []
        for t in trades[:15]:
            recent.append({
                "sym": t["symbol"].split("/")[0], "side": t["side"], "exit": t.get("exit_reason"),
                "r": round(t.get("r_multiple") or 0.0, 2), "pnl_pct": round(t.get("realized_pnl_pct") or 0.0, 3),
                "mfe": round(t.get("mfe_pct") or 0.0, 3), "mae": round(t.get("mae_pct") or 0.0, 3),
                "drag": round(t.get("exec_drag_pct") or 0.0, 4), "tp1": bool(t.get("tp1_done")),
                "min": round(((t.get("closed_at") or 0) - t["created_at"]) / 60.0, 1)})
        analytics = [{k: v for k, v in row.items() if v is not None} for row in self.store.analytics()]
        return {"limits": {"bounds": {k: list(v) for k, v in BIAS_BOUNDS.items()}, "defaults": bias_defaults()},
                "current": self.bias.effective(), "losing_streak": streak, "analytics": analytics,
                "recent_trades": recent, "signal_rejections_24h": self.store.reject_summary(time.time() - 86400)}

    # -- LLM ------------------------------------------------------------------------------------
    def ask_llm(self, telemetry: dict[str, Any]) -> Optional[dict[str, Any]]:
        if not OPENROUTER_API_KEY:
            return None
        body_text = json.dumps(telemetry, separators=(",", ":"), default=str)
        headers = {"Authorization": f"Bearer {OPENROUTER_API_KEY}", "Content-Type": "application/json",
                   "X-Title": "JIRU MEXC Bridge 2nd Brain"}
        for model in BRAIN_MODELS:
            try:
                resp = requests.post(OPENROUTER_URL, headers=headers, timeout=BRAIN_TIMEOUT_SEC, json={
                    "model": model, "temperature": 0.2, "max_tokens": 400,
                    "messages": [{"role": "system", "content": self.SYSTEM_PROMPT},
                                 {"role": "user", "content": body_text}]})
                if resp.status_code != 200:
                    logger.warning("2nd Brain: %s returned HTTP %s; trying next model", model, resp.status_code)
                    continue
                content = resp.json()["choices"][0]["message"]["content"]
                parsed = self.parse_json(content)
                if parsed is not None:
                    parsed["_model"] = model
                    return parsed
                logger.warning("2nd Brain: %s returned no parseable JSON", model)
            except Exception as exc:
                logger.warning("2nd Brain: %s failed: %s", model, redact(exc))
        return None

    @staticmethod
    def parse_json(text: Any) -> Optional[dict[str, Any]]:
        if not isinstance(text, str):
            return None
        text = re.sub(r"```(?:json)?", "", text)
        start, end = text.find("{"), text.rfind("}")
        if start < 0 or end <= start:
            return None
        try:
            obj = json.loads(text[start:end + 1])
        except Exception:
            return None
        return obj if isinstance(obj, dict) else None

    def heuristic(self, telemetry: dict[str, Any]) -> dict[str, Any]:
        streak = telemetry["losing_streak"]
        cur, dflt = telemetry["current"], bias_defaults()
        recent = telemetry["recent_trades"][:10]
        wins = sum(1 for r in recent if r["r"] > 0)
        prop: dict[str, Any] = {}
        if streak >= 2:
            prop["min_volume_accel"] = cur["min_volume_accel"] + 0.3
            prop["max_open_positions"] = 1 if streak >= 3 else cur["max_open_positions"]
            prop["reason"] = f"heuristic: {streak} losses in a row -> tighten"
        elif recent and wins / len(recent) >= 0.6:
            prop.update({k: dflt[k] for k in BIAS_BOUNDS})
            prop["reason"] = "heuristic: healthy win rate -> relax toward defaults"
        return prop

    # -- one cycle ------------------------------------------------------------------------------
    def cycle(self) -> dict[str, Any]:
        self.bias.on_trade_closed(self.store)                      # breaker check (idempotent)
        if self.bias.is_suspended():
            until = utc_iso(self.bias.suspended_until())
            logger.info("2nd Brain: circuit breaker active until %s; overrides suspended", until)
            self.last_run = {"at": utc_iso(), "skipped": f"circuit breaker until {until}"}
            return self.last_run
        closed = self.store.closed_trades(BRAIN_LOOKBACK_TRADES)
        if len(closed) < BRAIN_MIN_TRADES:
            self.last_run = {"at": utc_iso(), "skipped": f"only {len(closed)} closed trades (need {BRAIN_MIN_TRADES})"}
            logger.info("2nd Brain: %s", self.last_run["skipped"])
            return self.last_run
        telemetry = self.telemetry()
        proposal = self.ask_llm(telemetry)
        source = "llm"
        if proposal is None:
            proposal, source = self.heuristic(telemetry), "heuristic"
        note = f"{source}{':' + proposal['_model'] if proposal.get('_model') else ''} {proposal.get('reason', '')}"
        changed = self.bias.apply(proposal, source, note) if proposal else {}
        self.last_run = {"at": utc_iso(), "source": source, "proposal": {k: v for k, v in proposal.items()
                                                                          if k in BIAS_BOUNDS},
                         "applied": changed}
        logger.info("2nd Brain (%s): proposal=%s applied=%s", source, self.last_run["proposal"], changed)
        return self.last_run


# ==============================================================================
# Wiring
# ==============================================================================
diagnostic = DiagnosticAgent(store, bias)
core = ExecutionCore(gateway, store, diagnostic)
research_agent = ResearchAgent(gateway, store, bias)
analysis_agent = AnalysisAgent(gateway, market_data, store, bias)
trader_agent = TraderAgent(gateway, market_data, store, bias, research_agent, core)
monitor = PositionMonitor(gateway, store, bias, core, diagnostic)
maintainer = MaintainerAgent(store, bias)


# ==============================================================================
# Signal pipeline
# ==============================================================================
ENTRY_ACTIONS = {"BUY_LONG", "SELL_SHORT"}
EXIT_ACTIONS = {"EXIT_LONG", "EXIT_SHORT"}


def handle_exit(payload: TradingViewPayload, action: str, signal_id: str) -> dict[str, Any]:
    symbol = gateway.resolve_symbol(payload.symbol)
    gateway.check_allowlist(symbol)
    side = "long" if action == "EXIT_LONG" else "short"
    with TRADE_LOCK:
        tracked = [t for t in store.active_trades() if t["symbol"] == symbol and t["side"] == side]
        closed_ids: list[int] = []
        for t in tracked:
            price = gateway.ticker(symbol)["last"]
            core.close_all(t, price, "MANUAL_EXIT")
            closed_ids.append(t["id"])
        if closed_ids:
            return {"status": "CLOSED", "symbol": symbol, "trade_ids": closed_ids}
        if is_live():                                    # untracked exchange position: close it, like v2.x did
            pos = next((p for p in gateway.positions(symbol) if p.side == side), None)
            if pos:
                gateway.market_order(symbol, "sell" if side == "long" else "buy", pos.contracts, True, "exit",
                                     signal_id)
                return {"status": "CLOSED_UNTRACKED", "symbol": symbol, "contracts": pos.contracts}
    return {"status": "NO_POSITION", "symbol": symbol}


def process_signal(payload: TradingViewPayload) -> None:
    """Runs in a worker thread after the webhook has been acknowledged."""
    action = normalize_action(payload.action)
    signal_id = canonical_signal_id(payload)
    symbol_text = clean_symbol_text(payload.symbol)
    with ENTRY_LOCK:
        try:
            if action not in ENTRY_ACTIONS | EXIT_ACTIONS:
                raise SignalRejected("research", f"Unsupported action: {action}")
            if not store.record_signal(signal_id, action, symbol_text, parse_timestamp(payload.timestamp),
                                       payload.price):
                bump("duplicate")
                logger.warning("Duplicate signal ignored: %s", signal_id)
                return
            gateway.require()
            if not is_live():
                logger.info("PAPER mode (trading_enabled=%s dry_run=%s): signal %s %s", TRADING_ENABLED, DRY_RUN,
                            action, symbol_text)
            if action in EXIT_ACTIONS:
                result = handle_exit(payload, action, signal_id)
                outcome = "EXIT"
            else:
                ctx = research_agent.evaluate(payload, action, signal_id)
                plan = analysis_agent.analyze(ctx)
                result = trader_agent.execute(plan)
                outcome = "ACCEPTED"
            store.update_signal(signal_id, result.get("status", "DONE"), outcome, "trader", None, result,
                                result.get("trade_id"))
            bump("executed")
            logger.info("Signal complete %s | %s", signal_id, json.dumps(result, default=str))
        except SignalRejected as rej:
            bump("rejected")
            store.update_signal(signal_id, "REJECTED", "REJECTED", rej.stage, rej.reason)
            logger.info("Signal rejected %s | %s: %s", signal_id, rej.stage, rej.reason)
        except Exception as exc:
            bump("failed")
            with STATS_LOCK:
                STATS["last_error"] = redact(exc)
            store.update_signal(signal_id, "ERROR", "ERROR", "execution", redact(exc))
            logger.error("Signal execution failed %s: %s", signal_id, redact(exc), exc_info=True)


# ==============================================================================
# Scanner (autonomous LONG setups: 1h uptrend + 5m EMA20/50 pullback reclaim)
# ==============================================================================
class ScannerAgent:
    """Finds setups on its own and feeds them through the same Research -> Analysis -> Trader pipeline
    as a webhook signal, so every SOP gate, sizing rule and paper/live switch still applies."""

    def __init__(self, gw: ExchangeGateway, md: MarketData, st: StateStore, dyn: DynamicBias,
                 research: ResearchAgent) -> None:
        self.gw, self.md, self.store, self.bias, self.research = gw, md, st, dyn, research
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._seen: dict[str, float] = {}          # symbol -> last evaluated 5m candle open time
        self._resolved: dict[str, str] = {}
        self.cycles = 0
        self.submitted = 0
        self.last_run: Optional[str] = None
        self.last_error: Optional[str] = None
        self.last_candidates: list[dict[str, Any]] = []

    def start(self) -> None:
        if not SCANNER_ENABLED or (self._thread and self._thread.is_alive()):
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._loop, name="scanner", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()

    def _loop(self) -> None:
        logger.info("Scanner started (%d symbols, every %.0fs, min score %.0f)", len(SCANNER_SYMBOLS),
                    SCANNER_INTERVAL_SEC, SCANNER_MIN_SCORE)
        if self._stop.wait(45.0):
            return
        while True:
            try:
                if self.gw.ready:
                    self.cycle()
            except Exception as exc:
                self.last_error = redact(exc)
                logger.error("Scanner cycle failed: %s", redact(exc))
            if self._stop.wait(SCANNER_INTERVAL_SEC):
                return

    def _symbol(self, base: str) -> Optional[str]:
        if base not in self._resolved:
            try:
                sym = self.gw.resolve_symbol(base + "USDT")
                self.gw.check_allowlist(sym)
                self._resolved[base] = sym
            except Exception:
                self._resolved[base] = ""
        return self._resolved[base] or None

    def evaluate(self, symbol: str) -> Optional[dict[str, Any]]:
        """Return a candidate dict for a fresh long setup on the last closed 5m candle, else None."""
        c5 = self.md.candles(symbol, "5m", 120)
        if len(c5) < 60:
            return None
        last_ts = c5[-1][0]
        if self._seen.get(symbol) == last_ts:
            return None
        self._seen[symbol] = last_ts
        closes = [c[4] for c in c5]
        a, e20, e50 = atr(c5, ATR_PERIOD), ema(closes, 20), ema(closes, 50)
        if not a or a <= 0 or e20 is None or e50 is None:
            return None
        _, o, h, l, c, _v = c5[-1]
        prev_high = c5[-2][2]
        if not (e20 > e50 and c > e20 and c > o and c > prev_high):
            return None                                         # need uptrend + bullish reclaim of EMA20
        if c - e20 > SCANNER_MAX_EXTENSION_ATR * a:
            return None                                         # extended: do not chase
        touched = min(x[3] for x in c5[-7:-1]) <= e20 + SCANNER_PULLBACK_ATR * a
        if not touched:
            return None                                         # no recent pullback to the EMA
        if trend_bias_1h(self.md.candles(symbol, "1h", 60)) != 1:
            return None
        vacc = volume_acceleration(c5)
        if vacc is None or vacc < self.bias.effective()["min_volume_accel"]:
            return None
        swing_low = min(x[3] for x in c5[-SCANNER_SWING_BARS:])
        sl = min(swing_low - 0.1 * a, c - 0.8 * a)
        stop_dist = c - sl
        if stop_dist > SCANNER_MAX_STOP_ATR * a:
            return None
        expected_pct = TP1_R_MULT * stop_dist / c * 100.0       # cheap pre-check of the Analysis cost gate
        if 2.0 * TAKER_FEE_PCT + 2.0 * EST_SLIPPAGE_PCT > MAX_COST_TO_TARGET * expected_pct:
            return None
        score = 50.0 + 10.0                                      # base + 1h uptrend
        score += 15.0 if vacc >= 1.2 else 5.0
        score += 10.0 if min(x[3] for x in c5[-4:-1]) <= e20 else 0.0
        score += 5.0 if (c - l) / max(h - l, 1e-12) >= 0.6 else 0.0
        score += 10.0 if (e20 - e50) / a >= 0.5 else 0.0
        return {"symbol": symbol, "score": min(score, 100.0), "sl": sl, "candle_ts": last_ts, "atr": a,
                "vol_accel": round(vacc, 2)}

    def cycle(self) -> dict[str, Any]:
        self.cycles += 1
        self.last_run = utc_iso()
        out: dict[str, Any] = {"scanned": 0, "candidates": 0, "submitted": 0}
        if BREAKER_HALTS_ENTRIES and self.bias.is_suspended():
            return {**out, "skipped": "circuit breaker active"}
        limit = int(min(MAX_OPEN_POSITIONS, self.bias.effective()["max_open_positions"]))
        active = self.store.active_trades()
        if limit <= 0 or len(active) >= limit:
            return {**out, "skipped": f"position slots full ({len(active)}/{limit})"}
        busy = {t["symbol"] for t in active}
        cands: list[dict[str, Any]] = []
        for base in SCANNER_SYMBOLS:
            sym = self._symbol(base)
            if not sym or sym in busy:
                continue
            last = self.store.last_trade_ts(sym)
            if SYMBOL_COOLDOWN_SEC > 0 and last and 0 <= time.time() - last < SYMBOL_COOLDOWN_SEC:
                continue
            try:
                cand = self.evaluate(sym)
            except Exception as exc:
                logger.debug("Scanner %s skipped: %s", sym, redact(exc))
                continue
            out["scanned"] += 1
            if cand and cand["score"] >= SCANNER_MIN_SCORE:
                cands.append(cand)
        cands.sort(key=lambda x: x["score"], reverse=True)
        self.last_candidates = cands[:5]
        out["candidates"] = len(cands)
        for cand in cands[:max(1, SCANNER_MAX_PER_CYCLE)]:
            try:
                price = self.gw.ticker(cand["symbol"])["last"]
                payload = TradingViewPayload(
                    action="BUY_LONG", symbol=cand["symbol"], price=price, sl=cand["sl"],
                    signal_id=f"SCAN-{clean_symbol_text(cand['symbol'])}-{int(cand['candle_ts'])}",
                    timeframe="5m", system="SCANNER", version=APP_VERSION, sop_score=cand["score"])
            except Exception as exc:
                logger.warning("Scanner could not build signal for %s: %s", cand["symbol"], redact(exc))
                continue
            logger.info("Scanner setup %s score=%.0f sl=%.6g vol_accel=%s", cand["symbol"], cand["score"],
                        cand["sl"], cand["vol_accel"])
            process_signal(payload)
            self.submitted += 1
            out["submitted"] += 1
        return out

    def snapshot(self) -> dict[str, Any]:
        return {"enabled": SCANNER_ENABLED, "symbols": len(SCANNER_SYMBOLS), "cycles": self.cycles,
                "submitted": self.submitted, "last_run": self.last_run, "last_error": self.last_error,
                "last_candidates": self.last_candidates}


scanner = ScannerAgent(gateway, market_data, store, bias, research_agent)


# ==============================================================================
# Dashboard (static page; all data comes from the admin endpoints with the secret header)
# ==============================================================================
DASHBOARD_HTML = r"""<!doctype html>
<html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>JIRU MEXC Bridge</title>
<style>
:root{--base:#1e1e2e;--mantle:#181825;--crust:#11111b;--s0:#313244;--s1:#45475a;--o1:#7f849c;--sub:#a6adc8;--text:#cdd6f4;
--blue:#89b4fa;--green:#a6e3a1;--red:#f38ba8;--peach:#fab387;--yellow:#f9e2af;--teal:#94e2d5;--mauve:#cba6f7}
*{box-sizing:border-box}body{margin:0;background:var(--crust);color:var(--text);font:14px/1.4 system-ui,Segoe UI,Roboto,sans-serif}
header{display:flex;flex-wrap:wrap;gap:10px;align-items:center;justify-content:space-between;padding:14px 16px;background:var(--mantle);border-bottom:1px solid var(--s0)}
h1{font-size:16px;margin:0;font-weight:700;letter-spacing:.3px}h1 small{color:var(--o1);font-weight:500;margin-left:6px}
.pill{display:inline-block;padding:2px 9px;border-radius:99px;font-size:11px;font-weight:700;letter-spacing:.4px}
.p-green{background:#a6e3a122;color:var(--green)}.p-red{background:#f38ba822;color:var(--red)}.p-yellow{background:#f9e2af22;color:var(--yellow)}
.p-gray{background:#7f849c22;color:var(--sub)}
main{padding:14px 16px;max-width:1300px;margin:0 auto}
.grid{display:grid;gap:10px;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));margin-bottom:14px}
.card{background:var(--base);border:1px solid var(--s0);border-left:3px solid var(--blue);border-radius:8px;padding:9px 12px}
.card .t{font-size:10px;color:var(--o1);font-weight:700;letter-spacing:.6px;text-transform:uppercase}
.card .v{font-size:22px;font-weight:700;margin-top:2px}.card .s{font-size:11px;color:var(--sub);min-height:15px}
.panel{background:var(--mantle);border:1px solid var(--s0);border-radius:8px;margin-bottom:14px;overflow:hidden}
.panel h2{margin:0;padding:9px 12px;font-size:11px;letter-spacing:.7px;text-transform:uppercase;color:var(--o1);border-bottom:1px solid var(--s0);display:flex;justify-content:space-between}
.scroll{overflow-x:auto}table{width:100%;border-collapse:collapse;font-size:12.5px;white-space:nowrap}
th{text-align:left;color:var(--o1);font-weight:600;padding:7px 10px;background:var(--base);position:sticky;top:0}
td{padding:6px 10px;border-top:1px solid #31324455}tr:nth-child(even) td{background:#1e1e2e66}
td.n,th.n{text-align:right;font-variant-numeric:tabular-nums}.pos{color:var(--green)}.neg{color:var(--red)}.mut{color:var(--o1)}
.two{display:grid;gap:14px;grid-template-columns:1fr 1fr}@media(max-width:800px){.two{grid-template-columns:1fr}}
.empty{padding:16px;color:var(--o1);text-align:center}.kv{padding:10px 12px;display:grid;grid-template-columns:auto 1fr;gap:4px 14px;font-size:12.5px}.kv b{color:var(--sub);font-weight:600}
#login{max-width:380px;margin:12vh auto;background:var(--base);border:1px solid var(--s0);border-radius:10px;padding:20px}
#login input{width:100%;padding:9px;border-radius:6px;border:1px solid var(--s1);background:var(--crust);color:var(--text);margin:10px 0}
button{background:var(--blue);color:var(--crust);border:0;border-radius:6px;padding:8px 14px;font-weight:700;cursor:pointer}
button.g{background:var(--s0);color:var(--sub)}svg{display:block;width:100%;height:120px}
</style></head><body>
<div id="login" style="display:none"><b>JIRU MEXC Bridge</b><div class="mut" style="font-size:12px;margin-top:4px">Enter your webhook secret. It is kept only in this browser.</div>
<input id="sec" type="password" placeholder="TRADINGVIEW_WEBHOOK_SECRET" autocomplete="off"><button id="go">Open dashboard</button><div id="err" class="neg" style="margin-top:8px;font-size:12px"></div></div>
<div id="app" style="display:none">
<header><h1>JIRU MEXC Bridge<small id="ver"></small></h1>
<div><span id="mode" class="pill p-gray">-</span> <span id="ex" class="pill p-gray">-</span> <span id="brk" class="pill p-green" style="display:none">BREAKER</span>
<span class="mut" style="font-size:12px;margin:0 8px" id="upd"></span><button class="g" id="out">Sign out</button></div></header>
<main>
<div class="grid" id="kpis"></div>
<div class="panel"><h2><span>Open positions</span><span id="npos" class="mut"></span></h2><div class="scroll" id="pos"></div></div>
<div class="two">
<div class="panel"><h2>Equity curve (closed trades, PnL $)</h2><div id="eq" style="padding:8px 10px"></div></div>
<div class="panel"><h2>Performance by mode</h2><div class="scroll" id="perf"></div></div></div>
<div class="panel"><h2><span>Recent trades</span></h2><div class="scroll" id="trades"></div></div>
<div class="two">
<div class="panel"><h2>Scanner</h2><div id="scan"></div></div>
<div class="panel"><h2>2nd Brain / risk bias</h2><div id="brain"></div></div></div>
<div class="panel"><h2><span>Signal feed</span><span id="rej" class="mut"></span></h2><div class="scroll" id="sigs"></div></div>
</main></div>
<script>
const $=id=>document.getElementById(id);let SEC="";try{SEC=localStorage.getItem("bridge_secret")||""}catch(e){}
const esc=s=>String(s==null?"":s).replace(/[&<>"']/g,c=>({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#39;"}[c]));
const num=(v,d=2)=>v==null||isNaN(v)?"-":Number(v).toFixed(d);
const px=v=>v==null||isNaN(v)?"-":(Math.abs(v)>=100?Number(v).toFixed(2):Number(v).toPrecision(6).replace(/\.?0+$/,""));
const cls=v=>v>0?"pos":v<0?"neg":"";const usd=v=>v==null?"-":(v>0?"+":"")+num(v,2);
const tm=t=>t?new Date(t*1000).toLocaleString([], {month:"short",day:"numeric",hour:"2-digit",minute:"2-digit"}):"-";
function age(t){if(!t)return"-";const s=Math.max(0,Date.now()/1000-t);return s<3600?Math.round(s/60)+"m":(s/3600).toFixed(1)+"h"}
function pill(t,c){return `<span class="pill p-${c}">${esc(t)}</span>`}
function card(t,v,s,c){return `<div class="card" style="border-left-color:var(--${c||"blue"})"><div class="t">${esc(t)}</div><div class="v" style="color:var(--${c||"text"})">${v}</div><div class="s">${s||""}</div></div>`}
function table(cols,rows,empty){if(!rows.length)return `<div class="empty">${esc(empty)}</div>`;
 return "<table><thead><tr>"+cols.map(c=>`<th class="${c[2]||""}">${esc(c[0])}</th>`).join("")+"</tr></thead><tbody>"+rows.map(r=>"<tr>"+cols.map(c=>`<td class="${c[2]||""}">${c[1](r)}</td>`).join("")+"</tr>").join("")+"</tbody></table>"}
async function api(p){const r=await fetch(p,{headers:{"X-Webhook-Secret":SEC},cache:"no-store"});if(r.status===401)throw new Error("401");if(!r.ok)throw new Error(r.status);return r.json()}
function showLogin(m){$("app").style.display="none";$("login").style.display="block";$("err").textContent=m||""}
function curve(closed){const pts=[0];let c=0;closed.forEach(t=>{c+=t.realized_pnl_usd||0;pts.push(c)});
 if(pts.length<2)return `<div class="empty">No closed trades yet</div>`;
 const mn=Math.min(...pts,0),mx=Math.max(...pts,0),rg=(mx-mn)||1,W=600,H=120,st=W/(pts.length-1);
 const y=v=>H-6-((v-mn)/rg)*(H-12);const d=pts.map((v,i)=>(i?"L":"M")+(i*st).toFixed(1)+" "+y(v).toFixed(1)).join(" ");
 const col=c>=0?"#a6e3a1":"#f38ba8";
 return `<svg viewBox="0 0 ${W} ${H}" preserveAspectRatio="none"><line x1="0" x2="${W}" y1="${y(0)}" y2="${y(0)}" stroke="#45475a" stroke-dasharray="4 4"/><path d="${d}" fill="none" stroke="${col}" stroke-width="2" vector-effect="non-scaling-stroke"/></svg><div class="mut" style="font-size:11px">Cumulative: <b class="${cls(c)}">${usd(c)} USD</b> over ${pts.length-1} trades</div>`}
async function refresh(){
 try{
  const [st,an,tr,sg]=await Promise.all([api("/status"),api("/analytics"),api("/trades?limit=100"),api("/signals?limit=40")]);
  $("login").style.display="none";$("app").style.display="block";
  $("ver").textContent="v"+st.version;
  const live=st.mode==="LIVE";$("mode").textContent=st.mode;$("mode").className="pill p-"+(live?"red":"yellow");
  $("ex").textContent=st.exchange_ready?"EXCHANGE OK":"EXCHANGE DOWN";$("ex").className="pill p-"+(st.exchange_ready?"green":"red");
  const susp=st.bias&&st.bias.suspended;$("brk").style.display=susp?"inline-block":"none";if(susp){$("brk").className="pill p-red";$("brk").textContent="BREAKER ACTIVE"}
  $("upd").textContent="updated "+new Date().toLocaleTimeString();
  const all=tr.trades||[],closed=all.filter(t=>t.status==="CLOSED").sort((a,b)=>(a.closed_at||a.updated_at)-(b.closed_at||b.updated_at));
  const tot=an.analytics||[];const a=tot.find(x=>x.mode===st.mode)||tot[0]||{};
  const pos=st.positions||[];const upnl=pos.reduce((s,p)=>s+(p.unrealized_usd||0),0);
  const sc=st.scanner||{},S=st.stats||{};
  $("kpis").innerHTML=card("Open positions",pos.length,`unrealized <b class="${cls(upnl)}">${usd(upnl)}</b>`,"blue")+
   card("Closed trades",a.trades||0,`${a.wins||0} wins`,"mauve")+
   card("Win rate",a.win_rate_pct!=null?num(a.win_rate_pct,1)+"%":"-","",a.win_rate_pct>=50?"green":"peach")+
   card("Total PnL (net)",usd(a.total_pnl_usd||0)+" $","",(a.total_pnl_usd||0)>=0?"green":"red")+
   card("Profit factor",a.profit_factor!=null?num(a.profit_factor,2):"-","","teal")+
   card("Avg R",a.avg_r!=null?num(a.avg_r,2):"-","",(a.avg_r||0)>=0?"green":"red")+
   card("Scanner",sc.enabled?(sc.cycles+" cycles"):"off",`${sc.submitted||0} setups submitted`,"blue")+
   card("Signals",S.executed||0,`${S.rejected||0} rejected, ${S.failed||0} errors`,"yellow");
  $("npos").textContent=pos.length?pos.length+" open":"";
  $("pos").innerHTML=table([["#",r=>r.id],["Symbol",r=>esc(r.symbol)],["Side",r=>pill(r.side.toUpperCase(),r.side==="long"?"green":"red")],
   ["Entry",r=>px(r.entry),"n"],["Price",r=>px(r.price),"n"],["Stop",r=>px(r.stop),"n"],["TP1",r=>px(r.tp1)+(r.tp1_done?" ✓":""),"n"],["TP2",r=>px(r.tp2)+(r.tp2_done?" ✓":""),"n"],
   ["uPnL $",r=>`<span class="${cls(r.unrealized_usd)}">${usd(r.unrealized_usd)}</span>`,"n"],["R",r=>`<span class="${cls(r.r)}">${num(r.r,2)}</span>`,"n"],
   ["Remaining",r=>num(r.remaining_pct,0)+"%","n"],["Mode",r=>pill(r.mode||"-",r.mode==="LIVE"?"red":"yellow")],["Age",r=>age(r.opened)]],pos,"No open positions");
  $("eq").innerHTML=curve(closed);
  $("perf").innerHTML=table([["Mode",r=>pill(r.mode,r.mode==="LIVE"?"red":"yellow")],["Trades",r=>r.trades,"n"],["Win %",r=>num(r.win_rate_pct,1),"n"],["PF",r=>num(r.profit_factor,2),"n"],
   ["PnL $",r=>`<span class="${cls(r.total_pnl_usd)}">${usd(r.total_pnl_usd)}</span>`,"n"],["Avg R",r=>num(r.avg_r,2),"n"],["MFE %",r=>num(r.avg_mfe_pct,2),"n"],["MAE %",r=>num(r.avg_mae_pct,2),"n"],["Drag %",r=>num(r.median_exec_drag_pct,3),"n"]],tot,"No closed trades yet");
  const recent=all.slice(0,25);
  $("trades").innerHTML=table([["#",r=>r.id],["Symbol",r=>esc(r.symbol)],["Side",r=>esc(r.side)],["Mode",r=>pill(r.mode||"-",r.mode==="LIVE"?"red":"yellow")],
   ["Status",r=>pill(r.status,r.status==="CLOSED"?"gray":"blue")],["Entry",r=>px(r.entry_price),"n"],["Exit",r=>px(r.exit_price),"n"],
   ["PnL $",r=>`<span class="${cls(r.realized_pnl_usd)}">${r.status==="CLOSED"?usd(r.realized_pnl_usd):"-"}</span>`,"n"],["R",r=>`<span class="${cls(r.r_multiple)}">${r.status==="CLOSED"?num(r.r_multiple,2):"-"}</span>`,"n"],
   ["Reason",r=>esc(r.exit_reason||"")],["Opened",r=>tm(r.created_at)],["Closed",r=>tm(r.closed_at)]],recent,"No trades yet");
  const cands=(sc.last_candidates||[]);
  $("scan").innerHTML=`<div class="kv"><b>Status</b><span>${sc.enabled?pill("RUNNING","green"):pill("OFF","gray")} ${sc.symbols||0} symbols</span><b>Last scan</b><span>${sc.last_run?new Date(sc.last_run).toLocaleTimeString():"-"}</span><b>Cycles / setups</b><span>${sc.cycles||0} / ${sc.submitted||0}</span><b>Last error</b><span class="${sc.last_error?"neg":"mut"}">${esc(sc.last_error||"none")}</span><b>Last candidates</b><span>${cands.length?cands.map(c=>esc(c.symbol.split("/")[0])+" ("+num(c.score,0)+")").join(", "):"<span class='mut'>none yet - waiting for a setup</span>"}</span></div>`;
  const b=st.bias||{},e=b.effective||{},bl=b.bounds||{},m=b.meta||{},br=st.brain_last_run||{};
  $("brain").innerHTML=`<div class="kv"><b>Stop distance x</b><span>${num(e.stop_distance_mult,2)} <span class="mut">(${(bl.stop_distance_mult||[]).join(" - ")})</span></span><b>Max open positions</b><span>${num(e.max_open_positions,0)} <span class="mut">(${(bl.max_open_positions||[]).join(" - ")})</span></span><b>Min volume accel</b><span>${num(e.min_volume_accel,2)} <span class="mut">(${(bl.min_volume_accel||[]).join(" - ")})</span></span><b>Source</b><span>${esc(m.source||"default")}</span><b>Last brain run</b><span>${br.at?new Date(br.at).toLocaleTimeString():"-"} <span class="mut">${esc(br.skipped||br.note||"")}</span></span><b>Breaker</b><span>${susp?pill("SUSPENDED until "+new Date(m.suspended_until*1000).toLocaleTimeString(),"red"):pill("OK","green")}</span></div>`;
  const rj=an.rejections_24h||{};$("rej").textContent=rj.total!=null?rj.total+" rejected in 24h":"";
  $("sigs").innerHTML=table([["Time",r=>tm(r.received_at)],["Symbol",r=>esc(r.symbol)],["Action",r=>esc(r.action)],["Outcome",r=>pill(r.outcome||r.status,r.outcome==="ACCEPTED"?"green":r.outcome==="REJECTED"?"yellow":r.outcome==="ERROR"?"red":"gray")],
   ["Stage",r=>esc(r.stage||"")],["Reason",r=>`<span class="mut">${esc(r.reject_reason||"")}</span>`],["Trade",r=>r.trade_id||""]],sg.signals||[],"No signals yet");
 }catch(e){if(String(e.message)==="401"){try{localStorage.removeItem("bridge_secret")}catch(x){}SEC="";showLogin("Wrong secret, try again")}else{$("upd").textContent="error: "+e.message}}
}
$("go").onclick=()=>{SEC=$("sec").value.trim();if(!SEC)return;try{localStorage.setItem("bridge_secret",SEC)}catch(e){}refresh()};
$("sec").addEventListener("keydown",e=>{if(e.key==="Enter")$("go").click()});
$("out").onclick=()=>{try{localStorage.removeItem("bridge_secret")}catch(e){}SEC="";showLogin("")};
if(SEC){refresh()}else{showLogin("")}
setInterval(()=>{if(SEC)refresh()},10000);
</script></body></html>
"""


def live_positions() -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for t in store.active_trades():
        try:
            price = gateway.ticker(t["symbol"])["last"] if gateway.ready else None
        except Exception:
            price = None
        dirn = 1 if t["side"] == "long" else -1
        entry = safe_float(t.get("entry_price"), 0.0) or 0.0
        qty = (safe_float(t.get("remaining"), None) if t.get("remaining") is not None else safe_float(t.get("amount"), 0.0)) or 0.0
        csize = safe_float(t.get("contract_size"), 1.0) or 1.0
        total = safe_float(t.get("amount"), 0.0) or 0.0
        stop_dist = safe_float(t.get("stop_distance"), 0.0) or 0.0
        unreal = (price - entry) * dirn * qty * csize if price else None
        out.append({"id": t["id"], "symbol": t["symbol"], "side": t["side"], "entry": entry, "price": price,
                    "stop": t.get("current_sl") or t.get("stop_price"), "tp1": t.get("tp1_price"),
                    "tp2": t.get("tp2_price"), "tp1_done": t.get("tp1_done"), "tp2_done": t.get("tp2_done"),
                    "unrealized_usd": unreal,
                    "r": ((price - entry) * dirn / stop_dist) if (price and stop_dist > 0) else None,
                    "remaining_pct": (qty / total * 100.0) if total > 0 else None,
                    "mode": t.get("mode"), "opened": t.get("created_at")})
    return out


# ==============================================================================
# FastAPI
# ==============================================================================
def validate_config() -> None:
    if POSITION_MODE not in {"oneway", "one-way"}:
        raise RuntimeError("This bridge supports MEXC ONE-WAY position mode only (MEXC_POSITION_MODE=oneway)")
    if MARGIN_MODE not in {"isolated", "cross"}:
        raise RuntimeError("MARGIN_MODE must be isolated or cross")
    if not (0.0 < MARGIN_UTILIZATION <= 1.0):
        raise RuntimeError("MAX_MARGIN_UTILIZATION must be > 0 and <= 1")
    if not (0.0 < TP1_FRACTION < 1.0 and 0.0 < TP2_FRACTION < 1.0 and TP1_FRACTION + TP2_FRACTION < 1.0):
        raise RuntimeError("TP1_FRACTION and TP2_FRACTION must be in (0,1) and sum to less than 1 (runner remainder)")
    if TRIGGER_PRICE_TYPE not in {"last", "mark", "index"}:
        raise RuntimeError("TRIGGER_PRICE_TYPE must be last, mark, or index")
    if OHLCV_SOURCE not in {"auto", "mexc", "gecko"}:
        raise RuntimeError("OHLCV_SOURCE must be auto, mexc or gecko")
    if is_live():
        missing = [n for n, v in (("TRADINGVIEW_WEBHOOK_SECRET", WEBHOOK_SECRET), ("EXCHANGE_API_KEY", EXCHANGE_API_KEY),
                                  ("EXCHANGE_API_SECRET", EXCHANGE_API_SECRET)) if not v]
        if missing:
            raise RuntimeError("Missing live-trading configuration: " + ", ".join(missing))
    if TP1_R_MULT < MIN_RR:
        logger.warning("TP1_R_MULT=%.2f is below MIN_RR=%.2f: signals without an explicit TP1 will always be "
                       "rejected by the R:R gate", TP1_R_MULT, MIN_RR)


def secret_ok(provided: Optional[str]) -> bool:
    expected = (WEBHOOK_SECRET or "").strip()
    if not expected or expected == "YOUR_WEBHOOK_SECRET":
        return False
    return hmac.compare_digest((provided or "").strip().encode("utf-8"), expected.encode("utf-8"))


@asynccontextmanager
async def lifespan(app: FastAPI):
    try:
        validate_config()
        await asyncio.to_thread(gateway.init)
        logger.info("%s v%s started | trading_enabled=%s dry_run=%s => %s | db=%s | bias=%s", APP_NAME, APP_VERSION,
                    TRADING_ENABLED, DRY_RUN, "LIVE" if is_live() else "PAPER", STATE_DB_PATH, BIAS_FILE_PATH)
    except Exception as exc:
        gateway.ready, gateway.error = False, redact(exc)
        logger.error("Startup configuration/exchange initialization failed: %s", redact(exc), exc_info=True)
    monitor.start()
    maintainer.start()
    scanner.start()
    yield
    monitor.stop()
    maintainer.stop()
    scanner.stop()
    gateway.ready = False
    logger.info("Bridge shutdown")


app = FastAPI(title=APP_NAME, version=APP_VERSION, lifespan=lifespan)


def require_admin(secret: Optional[str]) -> None:
    if not secret_ok(secret):
        raise HTTPException(status_code=401, detail="Unauthorized")


@app.get("/")
async def root() -> dict[str, Any]:
    return {"service": APP_NAME, "version": APP_VERSION, "status": "ok", "mode": "LIVE" if is_live() else "PAPER",
            "trading_enabled": TRADING_ENABLED, "dry_run": DRY_RUN}


@app.get("/health")
async def health() -> dict[str, Any]:
    return {"status": "ok", "exchange_ready": gateway.ready, "mode": "LIVE" if is_live() else "PAPER",
            "trading_enabled": TRADING_ENABLED, "dry_run": DRY_RUN, "position_mode": POSITION_MODE,
            "margin_mode": MARGIN_MODE, "startup_error": gateway.error, "monitor_ticks": monitor.ticks,
            "timestamp": utc_iso()}


@app.get("/ready")
async def ready() -> dict[str, Any]:
    if not gateway.ready:
        raise HTTPException(status_code=503, detail="Exchange is not ready")
    return {"ready": True, "exchange_ready": True}


@app.get("/dashboard", response_class=HTMLResponse)
async def dashboard() -> str:
    """Static page; it asks for the secret in the browser and calls the admin endpoints itself."""
    return DASHBOARD_HTML


@app.get("/signals")
async def signals_endpoint(limit: int = 40, x_webhook_secret: Optional[str] = Header(default=None)) -> dict[str, Any]:
    require_admin(x_webhook_secret)
    return {"signals": store.recent_signals(max(1, min(limit, 200)))}


@app.get("/status")
async def status(x_webhook_secret: Optional[str] = Header(default=None)) -> dict[str, Any]:
    require_admin(x_webhook_secret)
    with STATS_LOCK:
        snap = dict(STATS)
    positions = await asyncio.to_thread(live_positions)
    return {"service": APP_NAME, "version": APP_VERSION, "mode": "LIVE" if is_live() else "PAPER",
            "exchange_ready": gateway.ready, "stats": snap, "active_trades": len(store.active_trades()),
            "bias": bias.snapshot(), "brain_last_run": maintainer.last_run, "scanner": scanner.snapshot(), "positions": positions,
            "timestamp": utc_iso()}


@app.get("/analytics")
async def analytics_endpoint(x_webhook_secret: Optional[str] = Header(default=None)) -> dict[str, Any]:
    require_admin(x_webhook_secret)
    return {"analytics": store.analytics(), "rejections_24h": store.reject_summary(time.time() - 86400)}


@app.get("/trades")
async def trades_endpoint(limit: int = 20, x_webhook_secret: Optional[str] = Header(default=None)) -> dict[str, Any]:
    require_admin(x_webhook_secret)
    return {"trades": store.recent_trades(max(1, min(limit, 200)))}


@app.get("/bias")
async def bias_endpoint(x_webhook_secret: Optional[str] = Header(default=None)) -> dict[str, Any]:
    require_admin(x_webhook_secret)
    return bias.snapshot()


@app.post("/brain/run")
async def brain_run(x_webhook_secret: Optional[str] = Header(default=None)) -> dict[str, Any]:
    """Run one 2nd Brain cycle immediately (admin)."""
    require_admin(x_webhook_secret)
    return await asyncio.to_thread(maintainer.cycle)


@app.post("/webhook")
async def tradingview_webhook(request: Request, background_tasks: BackgroundTasks,
                              x_webhook_secret: Optional[str] = Header(default=None)) -> dict[str, Any]:
    bump("received")
    with STATS_LOCK:
        STATS["last_signal_at"] = utc_iso()
    try:
        body = await request.json()
        payload = TradingViewPayload(**body)
    except Exception:
        bump("rejected")
        raise HTTPException(status_code=400, detail="Invalid JSON/payload")

    if not secret_ok(payload.secret or x_webhook_secret):        # the secret is never echoed or logged
        bump("rejected")
        raise HTTPException(status_code=401, detail="Unauthorized")

    action = normalize_action(payload.action)
    if action not in ENTRY_ACTIONS | EXIT_ACTIONS:
        bump("rejected")
        raise HTTPException(status_code=400, detail=f"Unsupported action: {action}")
    if action in ENTRY_ACTIONS:
        try:
            ResearchAgent.check_freshness(payload)
        except SignalRejected as rej:
            bump("rejected")
            raise HTTPException(status_code=400, detail=rej.reason)

    bump("accepted")
    background_tasks.add_task(process_signal, payload)     # blocking CCXT work happens after the HTTP response
    return {"status": "ACKNOWLEDGED", "service": APP_NAME, "signal_id": canonical_signal_id(payload),
            "action": action, "symbol": clean_symbol_text(payload.symbol), "timestamp": utc_iso()}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=env_int("PORT", 8000), reload=False)
