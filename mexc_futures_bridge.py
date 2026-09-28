import os
import json
import time
import math
import hmac
import hashlib
import logging
import sqlite3
import threading
import asyncio
from contextlib import asynccontextmanager
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from typing import Any, Optional, Literal

import ccxt
from fastapi import FastAPI, HTTPException, Request, BackgroundTasks
from pydantic import BaseModel, Field, AliasChoices, ConfigDict

# ==============================================================================
# JIRU MEXC FUTURES WEBHOOK BRIDGE
# ==============================================================================
# Architecture:
# TradingView/Pine -> authenticated webhook -> validation/risk gate -> CCXT/MEXC
# -> market entry -> independent market TP/SL trigger orders -> reconciliation.
#
# IMPORTANT:
# - This bot defaults to SAFE MODE: TRADING_ENABLED=false and DRY_RUN=true.
# - Set both explicitly for live trading.
# - Use a dedicated MEXC API key restricted to trading only. Do not enable
#   withdrawals for the bot key.
# - This bridge assumes MEXC Futures is configured for ONE-WAY position mode.
# - Quantity is calculated from actual MEXC contractSize, not blindly from a
#   TradingView base-asset quantity.
# ==============================================================================

APP_NAME = "JIRU MEXC Futures Webhook Execution Bridge"
APP_VERSION = "2.0.0"

# ------------------------------------------------------------------------------
# Logging
# ------------------------------------------------------------------------------
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)s %(name)s | %(message)s",
)
logger = logging.getLogger("jiru-mexc-bridge")

# ------------------------------------------------------------------------------
# Environment helpers
# ------------------------------------------------------------------------------
def env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "y", "on"}


def env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or raw == "":
        return default
    try:
        return int(raw)
    except ValueError:
        raise RuntimeError(f"Environment variable {name} must be an integer")


def env_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None or raw == "":
        return default
    try:
        return float(raw)
    except ValueError:
        raise RuntimeError(f"Environment variable {name} must be a number")


def clean_symbol_text(value: str) -> str:
    s = value.strip().upper()
    # TradingView may send e.g. MEXC:BTCUSDT.P or BTCUSDT.P
    if s.startswith("MEXC:"):
        s = s[5:]
    s = s.replace(".P", "").replace("PERP", "")
    s = s.replace("-SWAP", "").replace("_SWAP", "")
    return s.strip()


# ------------------------------------------------------------------------------
# Configuration
# ------------------------------------------------------------------------------
WEBHOOK_SECRET = os.getenv("TRADINGVIEW_WEBHOOK_SECRET") or os.getenv("WEBHOOK_SECRET", "")
EXCHANGE_API_KEY = os.getenv("EXCHANGE_API_KEY") or os.getenv("MEXC_API_KEY", "")
EXCHANGE_API_SECRET = os.getenv("EXCHANGE_API_SECRET") or os.getenv("MEXC_API_SECRET", "")

TRADING_ENABLED = env_bool("TRADING_ENABLED", False)
DRY_RUN = env_bool("DRY_RUN", True)

DEFAULT_RISK_USD = env_float("DEFAULT_RISK_USD", 10.0)
MAX_RISK_USD = env_float("MAX_RISK_USD", 10.0)
DEFAULT_RISK_PCT = env_float("DEFAULT_RISK_PCT", 2.0)
DEFAULT_LEVERAGE = env_int("DEFAULT_LEVERAGE", 10)
MAX_LEVERAGE = env_int("MAX_LEVERAGE", 50)
MARGIN_MODE = os.getenv("MARGIN_MODE", "isolated").strip().lower()
POSITION_MODE = os.getenv("MEXC_POSITION_MODE", "oneway").strip().lower()
MARGIN_UTILIZATION = env_float("MAX_MARGIN_UTILIZATION", 0.90)

MIN_STOP_DISTANCE_PCT = env_float("MIN_STOP_DISTANCE_PCT", 0.10)
MAX_PRICE_DEVIATION_PCT = env_float("MAX_PRICE_DEVIATION_PCT", 0.30)
RISK_BUFFER_PCT = env_float("RISK_BUFFER_PCT", 5.0)
TP1_FRACTION = env_float("TP1_FRACTION", 0.50)
TRIGGER_PRICE_TYPE = os.getenv("TRIGGER_PRICE_TYPE", "mark").strip().lower()

MAX_OPEN_POSITIONS = env_int("MAX_OPEN_POSITIONS", 1)
SIGNAL_MAX_AGE_SEC = env_int("SIGNAL_MAX_AGE_SEC", 30)
SIGNAL_MAX_FUTURE_SEC = env_int("SIGNAL_MAX_FUTURE_SEC", 10)
IDEMPOTENCY_TTL_SEC = env_int("IDEMPOTENCY_TTL_SEC", 300)
POSITION_SYNC_DELAY_SEC = env_float("POSITION_SYNC_DELAY_SEC", 0.75)
EXCHANGE_TIMEOUT_MS = env_int("EXCHANGE_TIMEOUT_MS", 10000)

FAILSAFE_CLOSE_ON_SL_FAILURE = env_bool("FAILSAFE_CLOSE_ON_SL_FAILURE", True)
FAILSAFE_CLOSE_ON_TP1_FAILURE = env_bool("FAILSAFE_CLOSE_ON_TP1_FAILURE", False)
FAILSAFE_CLOSE_ON_TP2_FAILURE = env_bool("FAILSAFE_CLOSE_ON_TP2_FAILURE", False)
ENFORCE_MARGIN_MODE = env_bool("ENFORCE_MARGIN_MODE", True)
ENFORCE_LEVERAGE = env_bool("ENFORCE_LEVERAGE", True)
ALLOW_SYMBOLS_RAW = os.getenv("ALLOWED_SYMBOLS", "").strip()
ALLOWED_SYMBOLS = {
    clean_symbol_text(s) for s in ALLOW_SYMBOLS_RAW.split(",") if s.strip()
}

# Railway/container state. Set STATE_DB_PATH=/data/mexc_bridge.db when a Railway
# Volume is mounted so signal/trade state survives container recreation.
STATE_DB_PATH = os.getenv("STATE_DB_PATH", "/tmp/mexc_bridge.db")

# ------------------------------------------------------------------------------
# Runtime state
# ------------------------------------------------------------------------------
exchange: Optional[ccxt.mexc] = None
exchange_ready = False
startup_error: Optional[str] = None
execution_lock = threading.RLock()

stats_lock = threading.Lock()
stats = {
    "received": 0,
    "accepted": 0,
    "duplicate": 0,
    "rejected": 0,
    "executed": 0,
    "failed": 0,
    "last_signal_at": None,
    "last_error": None,
}

DB_LOCK = threading.Lock()
IDEMPOTENCY_CACHE: dict[str, float] = {}


# ------------------------------------------------------------------------------
# Persistent SQLite state
# ------------------------------------------------------------------------------
class StateStore:
    def __init__(self, path: str):
        self.path = path
        directory = os.path.dirname(path)
        if directory:
            os.makedirs(directory, exist_ok=True)
        self._init_db()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.path, timeout=10)
        conn.row_factory = sqlite3.Row
        return conn

    def _init_db(self) -> None:
        with DB_LOCK:
            conn = self._connect()
            try:
                conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS signals (
                        signal_id TEXT PRIMARY KEY,
                        received_at REAL NOT NULL,
                        action TEXT NOT NULL,
                        symbol TEXT NOT NULL,
                        status TEXT NOT NULL,
                        details TEXT
                    )
                    """
                )
                conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS trades (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        signal_id TEXT NOT NULL,
                        symbol TEXT NOT NULL,
                        side TEXT NOT NULL,
                        amount REAL NOT NULL,
                        contract_size REAL NOT NULL,
                        entry_price REAL NOT NULL,
                        stop_price REAL NOT NULL,
                        tp1_price REAL NOT NULL,
                        tp2_price REAL,
                        entry_order_id TEXT,
                        sl_order_id TEXT,
                        tp1_order_id TEXT,
                        tp2_order_id TEXT,
                        status TEXT NOT NULL,
                        created_at REAL NOT NULL,
                        updated_at REAL NOT NULL
                    )
                    """
                )
                conn.commit()
            finally:
                conn.close()

    def record_signal(self, signal_id: str, action: str, symbol: str) -> bool:
        with DB_LOCK:
            conn = self._connect()
            try:
                cur = conn.execute(
                    """
                    INSERT OR IGNORE INTO signals
                    (signal_id, received_at, action, symbol, status)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (signal_id, time.time(), action, symbol, "RECEIVED"),
                )
                conn.commit()
                return cur.rowcount == 1
            finally:
                conn.close()

    def update_signal(self, signal_id: str, status: str, details: Any = None) -> None:
        with DB_LOCK:
            conn = self._connect()
            try:
                conn.execute(
                    "UPDATE signals SET status=?, details=? WHERE signal_id=?",
                    (
                        status,
                        json.dumps(details, default=str) if details is not None else None,
                        signal_id,
                    ),
                )
                conn.commit()
            finally:
                conn.close()

    def create_trade(self, data: dict[str, Any]) -> int:
        now = time.time()
        with DB_LOCK:
            conn = self._connect()
            try:
                cur = conn.execute(
                    """
                    INSERT INTO trades (
                        signal_id, symbol, side, amount, contract_size,
                        entry_price, stop_price, tp1_price, tp2_price,
                        entry_order_id, sl_order_id, tp1_order_id, tp2_order_id,
                        status, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        data["signal_id"],
                        data["symbol"],
                        data["side"],
                        data["amount"],
                        data["contract_size"],
                        data["entry_price"],
                        data["stop_price"],
                        data["tp1_price"],
                        data.get("tp2_price"),
                        data.get("entry_order_id"),
                        data.get("sl_order_id"),
                        data.get("tp1_order_id"),
                        data.get("tp2_order_id"),
                        data.get("status", "OPEN"),
                        now,
                        now,
                    ),
                )
                conn.commit()
                return int(cur.lastrowid)
            finally:
                conn.close()

    def update_trade(self, trade_id: int, **fields: Any) -> None:
        if not fields:
            return
        fields["updated_at"] = time.time()
        keys = list(fields.keys())
        assignments = ", ".join(f"{k}=?" for k in keys)
        values = [fields[k] for k in keys] + [trade_id]
        with DB_LOCK:
            conn = self._connect()
            try:
                conn.execute(
                    f"UPDATE trades SET {assignments} WHERE id=?", values
                )
                conn.commit()
            finally:
                conn.close()

    def active_trades(self) -> list[sqlite3.Row]:
        with DB_LOCK:
            conn = self._connect()
            try:
                rows = conn.execute(
                    "SELECT * FROM trades WHERE status IN ('OPEN','PROTECTED') ORDER BY id DESC"
                ).fetchall()
                return rows
            finally:
                conn.close()


state_store = StateStore(STATE_DB_PATH)


# ------------------------------------------------------------------------------
# Pydantic payload
# ------------------------------------------------------------------------------
class TradingViewPayload(BaseModel):
    model_config = ConfigDict(extra="ignore", populate_by_name=True)

    secret: str
    action: str
    symbol: str
    price: float = Field(..., gt=0, validation_alias=AliasChoices("price", "entry"))
    sl: Optional[float] = Field(
        default=None, validation_alias=AliasChoices("sl", "invalidation", "stop")
    )
    tp1: Optional[float] = Field(default=None, validation_alias=AliasChoices("tp1", "target1"))
    tp2: Optional[float] = Field(default=None, validation_alias=AliasChoices("tp2", "target2"))
    leverage: Optional[int] = Field(default=None, ge=1, le=200)
    risk_pct: Optional[float] = Field(default=None, ge=0.01, le=100)
    risk_usd: Optional[float] = Field(
        default=None, ge=0.01, validation_alias=AliasChoices("risk_usd", "risk", "max_risk")
    )
    suggested_qty: Optional[float] = Field(
        default=None, gt=0, validation_alias=AliasChoices("suggested_qty", "qty", "quantity")
    )
    signal_id: Optional[str] = None
    timestamp: Optional[Any] = None
    timeframe: Optional[str] = None
    system: Optional[str] = None
    version: Optional[str] = None
    sop_score: Optional[float] = None
    rr_tp1: Optional[float] = None
    rr_tp2: Optional[float] = None


# ------------------------------------------------------------------------------
# Data structures
# ------------------------------------------------------------------------------
@dataclass
class PositionInfo:
    symbol: str
    side: str
    contracts: float
    entry_price: float
    position_id: Optional[str] = None


# ------------------------------------------------------------------------------
# Exchange initialization
# ------------------------------------------------------------------------------
def build_exchange() -> ccxt.mexc:
    return ccxt.mexc(
        {
            "apiKey": EXCHANGE_API_KEY,
            "secret": EXCHANGE_API_SECRET,
            "timeout": EXCHANGE_TIMEOUT_MS,
            "enableRateLimit": True,
            "options": {
                "defaultType": "swap",
                "adjustForTimeDifference": True,
                "warnOnFetchOpenOrdersWithoutSymbol": False,
            },
        }
    )


def validate_live_config() -> None:
    missing = []
    if not WEBHOOK_SECRET or WEBHOOK_SECRET == "YOUR_WEBHOOK_SECRET":
        missing.append("TRADINGVIEW_WEBHOOK_SECRET")
    if not EXCHANGE_API_KEY:
        missing.append("EXCHANGE_API_KEY")
    if not EXCHANGE_API_SECRET:
        missing.append("EXCHANGE_API_SECRET")
    if POSITION_MODE not in {"oneway", "one-way", "hedge", "hedged"}:
        raise RuntimeError("MEXC_POSITION_MODE must be oneway or hedge")
    if POSITION_MODE in {"hedge", "hedged"}:
        raise RuntimeError(
            "This bridge is intentionally restricted to MEXC ONE-WAY position mode for safe reduceOnly execution."
        )
    if MARGIN_MODE not in {"isolated", "cross"}:
        raise RuntimeError("MARGIN_MODE must be isolated or cross")
    if not (0.0 < MARGIN_UTILIZATION <= 1.0):
        raise RuntimeError("MAX_MARGIN_UTILIZATION must be > 0 and <= 1")
    if not (0.0 < TP1_FRACTION < 1.0):
        raise RuntimeError("TP1_FRACTION must be between 0 and 1")
    if TRIGGER_PRICE_TYPE not in {"last", "mark", "index"}:
        raise RuntimeError("TRIGGER_PRICE_TYPE must be last, mark, or index")
    if missing and (TRADING_ENABLED and not DRY_RUN):
        raise RuntimeError("Missing live-trading configuration: " + ", ".join(missing))


# ------------------------------------------------------------------------------
# Utility functions
# ------------------------------------------------------------------------------
def utc_iso(ts: Optional[float] = None) -> str:
    dt = datetime.fromtimestamp(ts or time.time(), tz=timezone.utc)
    return dt.isoformat()


def safe_float(value: Any, default: Optional[float] = None) -> Optional[float]:
    try:
        if value is None:
            return default
        f = float(value)
        if not math.isfinite(f):
            return default
        return f
    except (TypeError, ValueError):
        return default


def pct_distance(a: float, b: float) -> float:
    if a == 0:
        return float("inf")
    return abs(a - b) / abs(a) * 100.0


def canonical_signal_id(payload: TradingViewPayload) -> str:
    if payload.signal_id:
        return payload.signal_id[:120]
    data = payload.model_dump(exclude={"secret"}, mode="json")
    raw = json.dumps(data, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:32]


def parse_timestamp(value: Any) -> Optional[float]:
    if value is None or value == "":
        return None
    if isinstance(value, (int, float)):
        v = float(value)
        # Heuristic: milliseconds if very large.
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
    return action.strip().upper().replace("-", "_").replace(" ", "_")


def extract_usdt_free(balance: dict[str, Any]) -> float:
    direct = safe_float(balance.get("USDT", {}).get("free"), None)
    if direct is not None:
        return max(0.0, direct)
    free_map = balance.get("free", {}) or {}
    return max(0.0, safe_float(free_map.get("USDT"), 0.0) or 0.0)


def position_side(position: dict[str, Any]) -> Optional[str]:
    side = position.get("side")
    if side in {"long", "short"}:
        return side
    info = position.get("info") or {}
    ptype = info.get("positionType")
    if str(ptype) == "1":
        return "long"
    if str(ptype) == "2":
        return "short"
    contracts = safe_float(position.get("contracts"), 0.0) or 0.0
    if contracts < 0:
        return "short"
    return None


def position_contracts(position: dict[str, Any]) -> float:
    contracts = safe_float(position.get("contracts"), None)
    if contracts is not None:
        return abs(contracts)
    info = position.get("info") or {}
    return abs(safe_float(info.get("holdVol"), 0.0) or 0.0)


def position_entry_price(position: dict[str, Any]) -> float:
    for key in ("entryPrice", "markPrice", "average"):
        value = safe_float(position.get(key), None)
        if value is not None and value > 0:
            return value
    info = position.get("info") or {}
    for key in ("holdAvgPrice", "openAvgPrice"):
        value = safe_float(info.get(key), None)
        if value is not None and value > 0:
            return value
    return 0.0


# ------------------------------------------------------------------------------
# Exchange helpers
# ------------------------------------------------------------------------------
def require_exchange() -> ccxt.mexc:
    if exchange is None or not exchange_ready:
        raise RuntimeError(f"Exchange not ready: {startup_error or 'unknown startup state'}")
    return exchange


def resolve_symbol(raw_symbol: str) -> str:
    ex = require_exchange()
    cleaned = clean_symbol_text(raw_symbol)

    # Exact unified form.
    if cleaned in ex.markets:
        market = ex.markets[cleaned]
        if market.get("swap") and market.get("linear") and market.get("settle") == "USDT":
            return cleaned

    # Normalize candidate ids such as BTC_USDT / BTCUSDT.
    normalized = "".join(ch for ch in cleaned if ch.isalnum())

    # Handle strings like BTC/USDT:USDT.
    for market_symbol, market in ex.markets.items():
        if not (market.get("swap") and market.get("linear")):
            continue
        if market.get("settle") != "USDT":
            continue
        candidates = {
            str(market_symbol).upper(),
            str(market.get("id", "")).upper(),
        }
        if any("".join(ch for ch in c if ch.isalnum()) == normalized for c in candidates):
            return market_symbol

    # If user sends BTCUSDT explicitly, match base+USDT without relying on slicing.
    for market_symbol, market in ex.markets.items():
        if not (market.get("swap") and market.get("linear")):
            continue
        if market.get("settle") != "USDT":
            continue
        base = str(market.get("base", "")).upper()
        quote = str(market.get("quote", "")).upper()
        settle = str(market.get("settle", "")).upper()
        if f"{base}{quote}" == normalized or f"{base}{settle}" == normalized:
            return market_symbol

    raise ValueError(f"MEXC USDT-M Futures symbol not found: {raw_symbol}")


def check_allowlist(symbol: str) -> None:
    if not ALLOWED_SYMBOLS:
        return
    market = require_exchange().market(symbol)
    aliases = {
        clean_symbol_text(symbol),
        clean_symbol_text(str(market.get("id", ""))),
        clean_symbol_text(f"{market.get('base', '')}USDT"),
    }
    if not aliases.intersection(ALLOWED_SYMBOLS):
        raise ValueError(f"Symbol {symbol} is not in ALLOWED_SYMBOLS")


def fetch_ticker_price(symbol: str) -> float:
    ticker = require_exchange().fetch_ticker(symbol, {"type": "swap"})
    for key in ("last", "mark", "close"):
        value = safe_float(ticker.get(key), None)
        if value is not None and value > 0:
            return value
    bid = safe_float(ticker.get("bid"), None)
    ask = safe_float(ticker.get("ask"), None)
    if bid and ask:
        return (bid + ask) / 2.0
    raise RuntimeError(f"No usable MEXC price for {symbol}")


def fetch_positions_for_symbol(symbol: str) -> list[PositionInfo]:
    ex = require_exchange()
    raw_positions = ex.fetch_positions([symbol], {"type": "swap"})
    result: list[PositionInfo] = []
    for raw in raw_positions or []:
        contracts = position_contracts(raw)
        side = position_side(raw)
        if contracts <= 0 or side not in {"long", "short"}:
            continue
        info = raw.get("info") or {}
        pid = info.get("positionId") or raw.get("id")
        result.append(
            PositionInfo(
                symbol=raw.get("symbol") or symbol,
                side=side,
                contracts=contracts,
                entry_price=position_entry_price(raw),
                position_id=str(pid) if pid is not None else None,
            )
        )
    return result


def fetch_all_open_positions() -> list[PositionInfo]:
    ex = require_exchange()
    raw_positions = ex.fetch_positions(None, {"type": "swap"})
    result: list[PositionInfo] = []
    for raw in raw_positions or []:
        contracts = position_contracts(raw)
        side = position_side(raw)
        if contracts <= 0 or side not in {"long", "short"}:
            continue
        info = raw.get("info") or {}
        pid = info.get("positionId") or raw.get("id")
        result.append(
            PositionInfo(
                symbol=raw.get("symbol") or "",
                side=side,
                contracts=contracts,
                entry_price=position_entry_price(raw),
                position_id=str(pid) if pid is not None else None,
            )
        )
    return result


def prepare_account_settings(symbol: str, leverage: int, side: str) -> None:
    ex = require_exchange()
    position_type = 1 if side == "long" else 2
    params = {
        "positionType": position_type,
        "openType": 1 if MARGIN_MODE == "isolated" else 2,
    }

    # Never silently switch the user's account from hedge to one-way. MEXC
    # documents that changing position mode requires no active positions/orders
    # and can reset the risk level, so the bridge only verifies the mode.
    if hasattr(ex, "fetch_position_mode"):
        try:
            mode = ex.fetch_position_mode(symbol)
            if bool(mode.get("hedged")):
                raise RuntimeError(
                    "MEXC account is in HEDGE mode; switch it to ONE-WAY before live trading"
                )
        except RuntimeError:
            raise
        except Exception as exc:
            raise RuntimeError(f"Unable to verify MEXC position mode: {exc}")
    else:
        raise RuntimeError("Installed CCXT version does not expose fetch_position_mode()")

    if ENFORCE_MARGIN_MODE and hasattr(ex, "set_margin_mode"):
        try:
            ex.set_margin_mode(MARGIN_MODE, symbol, params={'leverage': leverage})
        except Exception as exc:
            message = str(exc).lower()
            # MEXC may report that the account is already in the requested mode.
            if not any(token in message for token in ("same", "already", "unchanged")):
                raise RuntimeError(f"Unable to set {MARGIN_MODE} margin mode: {exc}")

    if ENFORCE_LEVERAGE:
        try:
            ex.set_leverage(leverage, symbol, params)
        except Exception as exc:
            raise RuntimeError(f"Unable to set {leverage}x leverage on {symbol}: {exc}")


def calculate_contracts(
    symbol: str,
    reference_price: float,
    stop_price: float,
    requested_risk_usd: float,
    leverage: int,
    market: dict[str, Any],
    free_usdt: float,
) -> tuple[float, dict[str, float]]:
    contract_size = safe_float(market.get("contractSize"), 1.0) or 1.0
    if contract_size <= 0:
        contract_size = 1.0

    stop_distance = abs(reference_price - stop_price)
    if stop_distance <= 0:
        raise ValueError("Stop distance is zero")

    stop_distance_pct = stop_distance / reference_price * 100.0
    if stop_distance_pct < MIN_STOP_DISTANCE_PCT:
        raise ValueError(
            f"Stop distance {stop_distance_pct:.4f}% is below MIN_STOP_DISTANCE_PCT={MIN_STOP_DISTANCE_PCT:.4f}%"
        )

    risk_after_buffer = requested_risk_usd * max(0.0, 1.0 - RISK_BUFFER_PCT / 100.0)
    risk_per_contract = stop_distance * contract_size
    raw_by_risk = risk_after_buffer / risk_per_contract

    max_margin = free_usdt * MARGIN_UTILIZATION
    max_by_margin = max_margin * leverage / (reference_price * contract_size)
    raw_contracts = min(raw_by_risk, max_by_margin)

    if raw_contracts <= 0:
        raise ValueError("Calculated contract quantity is zero or negative")

    amount_str = require_exchange().amount_to_precision(symbol, raw_contracts)
    contracts = safe_float(amount_str, 0.0) or 0.0
    if contracts <= 0:
        raise ValueError("Quantity rounded to zero at MEXC contract precision")

    limits = market.get("limits") or {}
    min_amount = safe_float((limits.get("amount") or {}).get("min"), None)
    min_cost = safe_float((limits.get("cost") or {}).get("min"), None)
    notional = contracts * contract_size * reference_price

    if min_amount is not None and contracts < min_amount:
        raise ValueError(
            f"Calculated quantity {contracts} is below MEXC minimum amount {min_amount}"
        )
    if min_cost is not None and notional < min_cost:
        raise ValueError(
            f"Calculated notional {notional:.8f} is below MEXC minimum cost {min_cost}"
        )

    return contracts, {
        "contract_size": contract_size,
        "stop_distance": stop_distance,
        "stop_distance_pct": stop_distance_pct,
        "risk_per_contract": risk_per_contract,
        "risk_after_buffer": risk_after_buffer,
        "raw_by_risk": raw_by_risk,
        "max_by_margin": max_by_margin,
        "notional": notional,
    }


def create_external_oid(signal_id: str, role: str) -> str:
    digest = hashlib.sha256(f"{signal_id}:{role}:{time.time_ns()}".encode()).hexdigest()[:20]
    return f"jirubridge-{role}-{digest}"


def market_order_params(side: str, reduce_only: bool, external_oid: str) -> dict[str, Any]:
    # MEXC's Futures API documents positionMode=2 as one-way mode and supports
    # reduceOnly for one-way positions.
    return {
        "reduceOnly": reduce_only,
        "positionMode": 2,
        "openType": 1 if MARGIN_MODE == "isolated" else 2,
        "externalOid": external_oid,
    }


def trigger_order_params(external_oid: str) -> dict[str, Any]:
    return {
        "reduceOnly": True,
        "positionMode": 2,
        "openType": 1 if MARGIN_MODE == "isolated" else 2,
        "triggerPriceType": TRIGGER_PRICE_TYPE,
        "externalOid": external_oid,
    }


def formatted_price(symbol: str, price: float) -> float:
    value = require_exchange().price_to_precision(symbol, price)
    return safe_float(value, None) or 0.0


def formatted_amount(symbol: str, amount: float) -> float:
    value = require_exchange().amount_to_precision(symbol, amount)
    return safe_float(value, None) or 0.0


def create_market_entry(symbol: str, action: str, amount: float, signal_id: str) -> dict[str, Any]:
    ex = require_exchange()
    side = "buy" if action == "BUY_LONG" else "sell"
    return ex.create_order(
        symbol,
        "market",
        side,
        amount,
        None,
        market_order_params("long" if action == "BUY_LONG" else "short", False, create_external_oid(signal_id, "entry")),
    )


def create_market_exit(symbol: str, position: PositionInfo, signal_id: str) -> dict[str, Any]:
    ex = require_exchange()
    amount = formatted_amount(symbol, position.contracts)
    if amount <= 0:
        raise ValueError("Position amount rounded to zero during close")
    side = "sell" if position.side == "long" else "buy"
    return ex.create_order(
        symbol,
        "market",
        side,
        amount,
        None,
        market_order_params(position.side, True, create_external_oid(signal_id, "exit")),
    )


def create_trigger_market_close(
    symbol: str,
    side: str,
    amount: float,
    trigger_price: float,
    role: str,
    signal_id: str,
) -> dict[str, Any]:
    ex = require_exchange()
    amount = formatted_amount(symbol, amount)
    trigger_price = formatted_price(symbol, trigger_price)
    if amount <= 0 or trigger_price <= 0:
        raise ValueError(f"Invalid protective order parameters for {role}")

    close_side = "sell" if side == "long" else "buy"
    params = trigger_order_params(create_external_oid(signal_id, role))
    # Current CCXT exposes MEXC swap createStopMarketOrder. Its unified
    # stopLossPrice/takeProfitPrice fields are not implemented for MEXC swap,
    # so protective orders are sent as independent trigger market orders.
    return ex.create_stop_market_order(
        symbol,
        close_side,
        amount,
        trigger_price,
        params,
    )


def cancel_order_safely(order_id: Optional[str], symbol: str) -> None:
    if not order_id:
        return
    try:
        require_exchange().cancel_order(order_id, symbol)
    except Exception as exc:
        logger.warning("Could not cancel order %s on %s: %s", order_id, symbol, exc)


# ------------------------------------------------------------------------------
# Signal validation and execution
# ------------------------------------------------------------------------------
def validate_secret(payload: TradingViewPayload) -> bool:
    expected = (WEBHOOK_SECRET or "").strip()
    received = (payload.secret or "").strip()
    print(f"[AUTH DEBUG] Received: {repr(received)} | Expected: {repr(expected)}", flush=True)
    if not expected or expected == "YOUR_WEBHOOK_SECRET":
        return False
    return hmac.compare_digest(received, expected)


def validate_signal_freshness(payload: TradingViewPayload) -> None:
    timestamp = parse_timestamp(payload.timestamp)
    if timestamp is None:
        return
    now = time.time()
    age = now - timestamp
    if age > SIGNAL_MAX_AGE_SEC:
        raise ValueError(f"Signal is stale by {age:.1f}s")
    if age < -SIGNAL_MAX_FUTURE_SEC:
        raise ValueError(f"Signal timestamp is {abs(age):.1f}s in the future")


def validate_trade_levels(payload: TradingViewPayload, action: str, reference_price: float) -> None:
    if action not in {"BUY_LONG", "SELL_SHORT"}:
        return
    if payload.sl is None:
        raise ValueError("SL/invalidation price is required for entry")
    if payload.tp1 is None:
        raise ValueError("TP1 price is required for entry")

    if action == "BUY_LONG":
        if not payload.sl < reference_price:
            raise ValueError("BUY_LONG requires SL below current reference price")
        if not payload.tp1 > reference_price:
            raise ValueError("BUY_LONG requires TP1 above current reference price")
        if payload.tp2 is not None and payload.tp2 <= payload.tp1:
            raise ValueError("BUY_LONG requires TP2 above TP1")
    else:
        if not payload.sl > reference_price:
            raise ValueError("SELL_SHORT requires SL above current reference price")
        if not payload.tp1 < reference_price:
            raise ValueError("SELL_SHORT requires TP1 below current reference price")
        if payload.tp2 is not None and payload.tp2 >= payload.tp1:
            raise ValueError("SELL_SHORT requires TP2 below TP1")


def requested_risk(payload: TradingViewPayload, free_usdt: float) -> float:
    if payload.risk_usd is not None:
        risk = payload.risk_usd
    elif payload.risk_pct is not None:
        risk = free_usdt * (payload.risk_pct / 100.0)
    elif DEFAULT_RISK_USD > 0:
        risk = DEFAULT_RISK_USD
    else:
        risk = free_usdt * (DEFAULT_RISK_PCT / 100.0)

    risk = max(0.01, float(risk))
    if MAX_RISK_USD > 0:
        risk = min(risk, MAX_RISK_USD)
    return risk


def execute_entry(payload: TradingViewPayload, action: str, signal_id: str) -> dict[str, Any]:
    ex = require_exchange()
    symbol = resolve_symbol(payload.symbol)
    check_allowlist(symbol)
    market = ex.market(symbol)

    if not (market.get("swap") and market.get("linear") and market.get("settle") == "USDT"):
        raise ValueError(f"Symbol {symbol} is not a USDT-M linear swap")

    reference = fetch_ticker_price(symbol)
    deviation = pct_distance(payload.price, reference)
    if deviation > MAX_PRICE_DEVIATION_PCT:
        raise ValueError(
            f"TradingView price {payload.price} differs from MEXC {reference} by {deviation:.4f}%"
        )

    validate_trade_levels(payload, action, reference)

    existing_positions = fetch_positions_for_symbol(symbol)
    if existing_positions:
        raise ValueError(
            f"Position already exists on {symbol}; bridge will not pyramid or flip automatically"
        )

    if MAX_OPEN_POSITIONS > 0:
        all_positions = fetch_all_open_positions()
        if len(all_positions) >= MAX_OPEN_POSITIONS:
            raise ValueError(
                f"MAX_OPEN_POSITIONS={MAX_OPEN_POSITIONS} reached ({len(all_positions)} open)"
            )

    side = "long" if action == "BUY_LONG" else "short"
    leverage = payload.leverage or DEFAULT_LEVERAGE
    if leverage > MAX_LEVERAGE:
        raise ValueError(f"Requested leverage {leverage}x exceeds MAX_LEVERAGE={MAX_LEVERAGE}x")

    prepare_account_settings(symbol, leverage, side)

    balance = ex.fetch_balance({"type": "swap"})
    free_usdt = extract_usdt_free(balance)
    if free_usdt <= 0:
            if DRY_RUN:
                print("[DRY RUN] Zero USDT balance detected on exchange. Using mock balance 1000.0 USDT.", flush=True)
                free_usdt = 1000.0
            else:
                raise ValueError("No free USDT margin available")

    risk_usd = requested_risk(payload, free_usdt)
    contracts, sizing = calculate_contracts(
        symbol=symbol,
        reference_price=reference,
        stop_price=float(payload.sl),
        requested_risk_usd=risk_usd,
        leverage=leverage,
        market=market,
        free_usdt=free_usdt,
    )

    logger.info(
        "ENTRY %s %s | ref=%s qty=%s contractSize=%s risk=$%.4f stop=%.4f%% notional=$%.4f",
        action,
        symbol,
        reference,
        contracts,
        sizing["contract_size"],
        risk_usd,
        sizing["stop_distance_pct"],
        sizing["notional"],
    )

    if DRY_RUN or not TRADING_ENABLED:
        return {
            "status": "DRY_RUN",
            "symbol": symbol,
            "action": action,
            "reference_price": reference,
            "contracts": contracts,
            "risk_usd": risk_usd,
            "leverage": leverage,
            "contract_size": sizing["contract_size"],
            "notional": sizing["notional"],
            "stop_distance_pct": sizing["stop_distance_pct"],
            "suggested_qty_from_tv": payload.suggested_qty,
        }

    entry_order_id: Optional[str] = None
    sl_order_id: Optional[str] = None
    tp1_order_id: Optional[str] = None
    tp2_order_id: Optional[str] = None
    trade_db_id: Optional[int] = None

    try:
        entry = create_market_entry(symbol, action, contracts, signal_id)
        entry_order_id = str(entry.get("id")) if entry.get("id") is not None else None

        # Give MEXC a moment to publish the position, then reconcile using the
        # actual filled position size and entry price.
        time.sleep(POSITION_SYNC_DELAY_SEC)
        positions = fetch_positions_for_symbol(symbol)
        target = next((p for p in positions if p.side == side), None)
        if target is None or target.contracts <= 0:
            raise RuntimeError(
                "Entry order was submitted but the live MEXC position could not be confirmed"
            )

        actual_entry = target.entry_price or reference
        live_amount = formatted_amount(symbol, target.contracts)
        if live_amount <= 0:
            raise RuntimeError("Live position amount rounded to zero")

        sl_price = formatted_price(symbol, float(payload.sl))
        tp1_price = formatted_price(symbol, float(payload.tp1))
        tp2_price = formatted_price(symbol, float(payload.tp2)) if payload.tp2 else None

        trade_db_id = state_store.create_trade(
            {
                "signal_id": signal_id,
                "symbol": symbol,
                "side": side,
                "amount": live_amount,
                "contract_size": sizing["contract_size"],
                "entry_price": actual_entry,
                "stop_price": sl_price,
                "tp1_price": tp1_price,
                "tp2_price": tp2_price,
                "entry_order_id": entry_order_id,
                "status": "OPEN",
            }
        )

        # SL first. If we cannot establish the protective stop, the bot should
        # not keep the position open.
        sl = create_trigger_market_close(
            symbol, side, live_amount, sl_price, "sl", signal_id
        )
        sl_order_id = str(sl.get("id")) if sl.get("id") is not None else None
        state_store.update_trade(trade_db_id, sl_order_id=sl_order_id, status="PROTECTED")

        # Split TP1 / TP2 using the actual live position amount. TP2 becomes the
        # remainder. If rounding makes TP1 zero, TP1 is rejected rather than
        # silently changing the intended sizing.
        tp1_amount = formatted_amount(symbol, live_amount * TP1_FRACTION)
        tp2_amount = formatted_amount(symbol, max(0.0, live_amount - tp1_amount))
        if tp1_amount <= 0:
            raise RuntimeError("TP1 amount rounded to zero; refusing unbalanced protection")
        if tp2_amount <= 0:
            raise RuntimeError("TP2 amount rounded to zero; refusing unbalanced protection")

        try:
            tp1 = create_trigger_market_close(
                symbol, side, tp1_amount, tp1_price, "tp1", signal_id
            )
            tp1_order_id = str(tp1.get("id")) if tp1.get("id") is not None else None
            state_store.update_trade(trade_db_id, tp1_order_id=tp1_order_id)
        except Exception:
            if FAILSAFE_CLOSE_ON_TP1_FAILURE:
                raise
            logger.exception("TP1 placement failed; retaining SL protection on %s", symbol)

        if tp2_price is not None:
            try:
                tp2 = create_trigger_market_close(
                    symbol, side, tp2_amount, tp2_price, "tp2", signal_id
                )
                tp2_order_id = str(tp2.get("id")) if tp2.get("id") is not None else None
                state_store.update_trade(trade_db_id, tp2_order_id=tp2_order_id)
            except Exception:
                if FAILSAFE_CLOSE_ON_TP2_FAILURE:
                    raise
                logger.exception("TP2 placement failed; retaining SL/TP1 protection on %s", symbol)

        state_store.update_trade(trade_db_id, status="PROTECTED")
        return {
            "status": "EXECUTED",
            "symbol": symbol,
            "action": action,
            "entry_order_id": entry_order_id,
            "sl_order_id": sl_order_id,
            "tp1_order_id": tp1_order_id,
            "tp2_order_id": tp2_order_id,
            "contracts": live_amount,
            "entry_price": actual_entry,
            "sl": sl_price,
            "tp1": tp1_price,
            "tp2": tp2_price,
            "risk_usd": risk_usd,
            "leverage": leverage,
        }

    except Exception:
        # SL failure is the hard safety boundary. Cancel anything we may have
        # already created and attempt an immediate market close.
        cancel_order_safely(tp2_order_id, symbol)
        cancel_order_safely(tp1_order_id, symbol)
        cancel_order_safely(sl_order_id, symbol)

        if FAILSAFE_CLOSE_ON_SL_FAILURE:
            try:
                positions = fetch_positions_for_symbol(symbol)
                target = next((p for p in positions if p.side == side), None)
                if target and target.contracts > 0 and not DRY_RUN and TRADING_ENABLED:
                    logger.critical(
                        "PROTECTION FAILURE on %s: attempting emergency market close", symbol
                    )
                    create_market_exit(symbol, target, signal_id)
            except Exception as close_exc:
                logger.critical(
                    "EMERGENCY CLOSE FAILED on %s: %s", symbol, close_exc, exc_info=True
                )

        if trade_db_id is not None:
            state_store.update_trade(trade_db_id, status="PROTECTION_FAILURE")
        raise


def execute_exit(payload: TradingViewPayload, action: str, signal_id: str) -> dict[str, Any]:
    symbol = resolve_symbol(payload.symbol)
    check_allowlist(symbol)
    positions = fetch_positions_for_symbol(symbol)
    desired_side = "long" if action == "EXIT_LONG" else "short"
    target = next((p for p in positions if p.side == desired_side), None)

    if target is None:
        return {
            "status": "NO_POSITION",
            "symbol": symbol,
            "action": action,
        }

    if DRY_RUN or not TRADING_ENABLED:
        return {
            "status": "DRY_RUN",
            "symbol": symbol,
            "action": action,
            "contracts": target.contracts,
            "entry_price": target.entry_price,
        }

    order = create_market_exit(symbol, target, signal_id)
    order_id = str(order.get("id")) if order.get("id") is not None else None

    # Best-effort cancel of protective orders recorded for this symbol.
    for row in state_store.active_trades():
        if row["symbol"] == symbol and row["status"] in {"OPEN", "PROTECTED"}:
            cancel_order_safely(row["tp2_order_id"], symbol)
            cancel_order_safely(row["tp1_order_id"], symbol)
            cancel_order_safely(row["sl_order_id"], symbol)
            state_store.update_trade(row["id"], status="CLOSED")

    return {
        "status": "CLOSED",
        "symbol": symbol,
        "action": action,
        "close_order_id": order_id,
        "contracts": target.contracts,
    }


def execute_signal(payload: TradingViewPayload) -> None:
    action = normalize_action(payload.action)
    signal_id = canonical_signal_id(payload)
    symbol_text = clean_symbol_text(payload.symbol)

    with execution_lock:
        try:
            if action not in {"BUY_LONG", "SELL_SHORT", "EXIT_LONG", "EXIT_SHORT"}:
                raise ValueError(f"Unsupported action: {action}")

            # Prevent exact duplicates both in memory and across process restarts.
            now = time.time()
            cached_at = IDEMPOTENCY_CACHE.get(signal_id)
            if cached_at is not None and now - cached_at < IDEMPOTENCY_TTL_SEC:
                with stats_lock:
                    stats["duplicate"] += 1
                state_store.update_signal(signal_id, "DUPLICATE_MEMORY")
                logger.warning("Duplicate signal ignored: %s", signal_id)
                return

            if not state_store.record_signal(signal_id, action, symbol_text):
                with stats_lock:
                    stats["duplicate"] += 1
                logger.warning("Duplicate persistent signal ignored: %s", signal_id)
                return

            IDEMPOTENCY_CACHE[signal_id] = now
            # Opportunistic cache cleanup.
            for key, timestamp in list(IDEMPOTENCY_CACHE.items()):
                if now - timestamp > IDEMPOTENCY_TTL_SEC:
                    IDEMPOTENCY_CACHE.pop(key, None)

            validate_signal_freshness(payload)

            if not TRADING_ENABLED or DRY_RUN:
                logger.warning(
                    "SAFE MODE signal received: trading_enabled=%s dry_run=%s",
                    TRADING_ENABLED,
                    DRY_RUN,
                )

            if action in {"BUY_LONG", "SELL_SHORT"}:
                result = execute_entry(payload, action, signal_id)
            else:
                result = execute_exit(payload, action, signal_id)

            state_store.update_signal(signal_id, result.get("status", "DONE"), result)
            with stats_lock:
                stats["executed"] += 1
            logger.info("Signal complete %s | %s", signal_id, json.dumps(result, default=str))

        except Exception as exc:
            state_store.update_signal(signal_id, "ERROR", {"error": str(exc)})
            with stats_lock:
                stats["failed"] += 1
                stats["last_error"] = str(exc)
            logger.exception("Signal execution failed: %s", signal_id)


# ------------------------------------------------------------------------------
# FastAPI lifecycle and endpoints
# ------------------------------------------------------------------------------
@asynccontextmanager
async def lifespan(app: FastAPI):
    global exchange, exchange_ready, startup_error
    try:
        validate_live_config()
        exchange = build_exchange()
        await asyncio.to_thread(exchange.load_markets)
        exchange_ready = True
        startup_error = None
        logger.info(
            "%s v%s started | trading_enabled=%s dry_run=%s state_db=%s",
            APP_NAME,
            APP_VERSION,
            TRADING_ENABLED,
            DRY_RUN,
            STATE_DB_PATH,
        )
    except Exception as exc:
        exchange_ready = False
        startup_error = str(exc)
        logger.exception("Startup configuration/exchange initialization failed")
    yield
    exchange_ready = False
    logger.info("Bridge shutdown")


app = FastAPI(title=APP_NAME, version=APP_VERSION, lifespan=lifespan)


@app.get("/")
async def root() -> dict[str, Any]:
    return {
        "service": APP_NAME,
        "version": APP_VERSION,
        "status": "ok",
        "trading_enabled": TRADING_ENABLED,
        "dry_run": DRY_RUN,
    }


@app.get("/health")
async def health() -> dict[str, Any]:
    return {
        "status": "ok",
        "exchange_ready": exchange_ready,
        "trading_enabled": TRADING_ENABLED,
        "dry_run": DRY_RUN,
        "position_mode": POSITION_MODE,
        "margin_mode": MARGIN_MODE,
        "startup_error": startup_error,
        "timestamp": utc_iso(),
    }


@app.get("/ready")
async def ready() -> dict[str, Any]:
    if not exchange_ready and (TRADING_ENABLED and not DRY_RUN):
        raise HTTPException(status_code=503, detail="Exchange is not ready")
    return {"ready": True, "exchange_ready": exchange_ready}


@app.get("/status")
async def status() -> dict[str, Any]:
    with stats_lock:
        snapshot = dict(stats)
    return {
        "service": APP_NAME,
        "version": APP_VERSION,
        "trading_enabled": TRADING_ENABLED,
        "dry_run": DRY_RUN,
        "exchange_ready": exchange_ready,
        "stats": snapshot,
        "active_trades": len(state_store.active_trades()),
        "timestamp": utc_iso(),
    }


@app.post("/webhook")
async def tradingview_webhook(
    request: Request,
    background_tasks: BackgroundTasks,
) -> dict[str, Any]:
    with stats_lock:
        stats["received"] += 1
        stats["last_signal_at"] = utc_iso()

    try:
        body = await request.json()
        payload = TradingViewPayload(**body)
    except Exception as exc:
        with stats_lock:
            stats["rejected"] += 1
        raise HTTPException(status_code=400, detail=f"Invalid JSON/payload: {exc}")

    if not validate_secret(payload):
        with stats_lock:
            stats["rejected"] += 1
        raise HTTPException(status_code=401, detail="Unauthorized")

    try:
        validate_signal_freshness(payload)
    except Exception as exc:
        with stats_lock:
            stats["rejected"] += 1
        raise HTTPException(status_code=400, detail=str(exc))

    signal_id = canonical_signal_id(payload)
    action = normalize_action(payload.action)

    if action not in {"BUY_LONG", "SELL_SHORT", "EXIT_LONG", "EXIT_SHORT"}:
        with stats_lock:
            stats["rejected"] += 1
        raise HTTPException(status_code=400, detail=f"Unsupported action: {action}")

    with stats_lock:
        stats["accepted"] += 1

    # Return to TradingView quickly; the blocking CCXT calls execute after the
    # HTTP response. Avoid logging the secret or the full payload.
    background_tasks.add_task(execute_signal, payload)

    return {
        "status": "ACKNOWLEDGED",
        "service": APP_NAME,
        "signal_id": signal_id,
        "action": action,
        "symbol": clean_symbol_text(payload.symbol),
        "timestamp": utc_iso(),
    }


if __name__ == "__main__":
    import uvicorn

    port = env_int("PORT", 8000)
    uvicorn.run("mexc_futures_bridge:app", host="0.0.0.0", port=port, reload=False)
