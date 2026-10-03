# JIRU MEXC Bridge v34 - environment guide

Set these as environment variables (Railway: *Variables* tab; Render: *Environment*). Never commit them.
Local runs may use a `.env` loaded by your shell; the app itself does not read `.env` files.

## Secrets (required for live; the first one is also required for paper)
| Variable | Purpose |
|---|---|
| `TRADINGVIEW_WEBHOOK_SECRET` | Must equal the `"secret"` field in the TradingView alert JSON. Also protects the admin endpoints (send it as header `X-Webhook-Secret`). |
| `EXCHANGE_API_KEY` / `EXCHANGE_API_SECRET` | MEXC API key, **trade permission only, no withdrawals**. Optional in paper mode (public prices are used; if keys exist the real balance is read, and a zero balance falls back to the mock balance). |
| `OPENROUTER_API_KEY` | Free OpenRouter key for the 2nd Brain. Optional: without it the Brain uses a deterministic rule set instead of an LLM. |

`MEXC_API_KEY`, `MEXC_API_SECRET` and `WEBHOOK_SECRET` are accepted as aliases. Secrets are never logged or written to disk (log output is scrubbed).

## Mode switches
| Variable | Default | Meaning |
|---|---|---|
| `TRADING_ENABLED` | `false` | Master switch. |
| `DRY_RUN` | `true` | Real orders are sent only when `TRADING_ENABLED=true` **and** `DRY_RUN=false`. Anything else = PAPER: simulated fills from live prices, the full exit ladder, ledger and 2nd Brain all run. |
| `PAPER_BALANCE_USDT` | `1000` | Mock wallet when the exchange balance is zero / unavailable in paper mode. |

## Persistence
| Variable | Default | Meaning |
|---|---|---|
| `STATE_DB_PATH` | `/data/mexc_bridge.db` | SQLite file. **Mount a volume at `/data`** (Railway Volume / Render Disk) or history and the circuit-breaker state are lost on redeploy. Falls back to the temp dir with a warning if not writable. |
| `BIAS_FILE_PATH` | next to the DB (`/data/dynamic_bias.json`) | 2nd Brain output file. |

## Risk and sizing (SOP Sentinel)
| Variable | Default | Meaning |
|---|---|---|
| `MAX_RISK_USD` | `10` | Hard cap on risk per trade. |
| `DEFAULT_RISK_USD` | `10` | Used when the alert has no `risk_usd`/`risk_pct`. |
| `MIN_RR` | `1.5` | Reject if (TP1 - entry) / (entry - SL) is lower. |
| `VOL_REF_ATR_PCT` | `0.40` | 5m ATR% considered normal. Risk is scaled by `ref / atr_pct` (never up). |
| `MIN_RISK_SCALAR` | `0.40` | Floor of that volatility scalar. |
| `RISK_BUFFER_PCT` | `5` | Sizing haircut for fees/rounding. |
| `DEFAULT_LEVERAGE` / `MAX_LEVERAGE` | `10` / `50` | Alert `leverage` above the max is rejected. |
| `MARGIN_MODE` | `isolated` | `isolated` or `cross`. |
| `MAX_MARGIN_UTILIZATION` | `0.90` | Share of free USDT usable as margin. |
| `MIN_STOP_DISTANCE_PCT` | `0.10` | Minimum stop distance (% of price). |
| `MIN_STOP_ATR_MULT` | `0.5` | Stop must be at least this many 5m ATRs away. |
| `MAX_PRICE_DEVIATION_PCT` | `0.30` | Max gap between alert price and MEXC price. |
| `SL_ATR_MULT` / `TP1_R_MULT` / `TP2_R_MULT` | `1.5` / `1.5` / `3.0` | Levels used only when the alert omits `sl` / `tp1` / `tp2`. |

## Filters
| Variable | Default | Meaning |
|---|---|---|
| `MAX_COST_TO_TARGET` | `0.25` | Reject if round-trip cost (2x taker fee + spread + slippage) exceeds this share of the TP1 move. |
| `TAKER_FEE_PCT` | `0.02` | Per-side taker fee (%). Check your MEXC fee tier. |
| `EST_SLIPPAGE_PCT` | `0.02` | Per-side slippage assumed until observed drag exists. |
| `MIN_VOLUME_ACCEL` | `0.8` | Last 3x5m volume / prior 12x5m volume. The 2nd Brain can raise it. |
| `REQUIRE_TREND_ALIGNMENT` | `true` | Reject trades against the 1h EMA8/EMA21 bias (neutral passes). |
| `VOL_SPIKE_MULT` | `3.0` | Reject when the last 5m true range exceeds this many ATRs. |
| `MONDAY_STANDDOWN` | `false` | Block entries on UTC Mondays. |
| `LTF_CHECK_ENABLED` / `LTF_MAX_DROP_PCT` | `true` / `0.15` | Don't buy into a falling 1m tape (or sell into a rising one). |
| `ATR_PERIOD` | `14` | |
| `ALLOWED_SYMBOLS` | empty (all) | Comma list, e.g. `BTCUSDT,ETHUSDT,SOLUSDT`. |
| `SYMBOL_COOLDOWN_SEC` | `900` | Per-symbol pause after a trade opens/closes. |
| `MAX_OPEN_POSITIONS` | `2` | Hard ceiling; the 2nd Brain can only lower it. Set `1` for the strictest behaviour. |
| `SIGNAL_MAX_AGE_SEC` / `SIGNAL_MAX_FUTURE_SEC` | `30` / `10` | Alert timestamp tolerance. |

## Exits
| Variable | Default | Meaning |
|---|---|---|
| `TP1_FRACTION` | `0.33` | Share of the **original** position sold at TP1 (use 0.33-0.50). |
| `TP2_FRACTION` | `0.33` | Share of the original position sold at TP2. The rest is the runner. Sum must be < 1. |
| `BASE_TRAIL_MULT` | `1.0` | Runner trails this many stop-distances behind the best price; breakeven (entry + 2x fee + buffer) is the floor. |
| `BE_BUFFER_PCT` | `0.02` | Extra margin above fees for breakeven. |
| `TIME_STOP_MINUTES` | `60` | Market exit if TP1 is not reached in time (`0` disables). |
| `MONITOR_INTERVAL_SEC` | `5` | Position monitor cadence. |
| `PATH_SNAPSHOT_SEC` | `15` | Spacing of `trade_path` snapshots. |
| `STOP_REPLACE_MIN_INTERVAL_SEC` / `STOP_REPLACE_MIN_STEP_FRAC` | `10` / `0.10` | Throttle for moving the exchange stop while trailing. |
| `SW_STOP_GRACE_FRAC` | `0.10` | Live, pre-TP1: the exchange trigger gets this head start (in stop distances) before the software backup fires. |
| `TRIGGER_PRICE_TYPE` | `mark` | `last`, `mark` or `index`. |
| `FAILSAFE_CLOSE_ON_SL_FAILURE` | `true` | Flatten the position if the stop cannot be placed/re-placed. |

## 2nd Brain
| Variable | Default | Meaning |
|---|---|---|
| `BRAIN_ENABLED` | `true` | |
| `BRAIN_INTERVAL_MIN` | `20` | Clamped to 15-30. |
| `BRAIN_MODELS` | `meta-llama/llama-3.3-70b-instruct:free,google/gemma-2-27b-it:free` | Tried in order. OpenRouter rotates its free models; if both disappear, set current `:free` ids here. |
| `BRAIN_MIN_TRADES` | `3` | Closed trades needed before it acts. |
| `BRAIN_LOOKBACK_TRADES` | `30` | |
| `BRAIN_TIMEOUT_SEC` | `45` | |
| `BREAKER_LOSSES` / `BREAKER_SUSPEND_HOURS` | `4` / `6` | N consecutive losing trades suspend all overrides (static defaults apply) for the period. |
| `BREAKER_HALTS_ENTRIES` | `false` | Also block new entries while the breaker is active. |

## Market data and exchange
| Variable | Default | Meaning |
|---|---|---|
| `OHLCV_SOURCE` | `auto` | `auto` = MEXC candles via CCXT, GeckoTerminal as fallback; `mexc`; `gecko`. |
| `GECKOTERMINAL_BASE_URL` / `GECKO_MIN_INTERVAL_SEC` | `https://api.geckoterminal.com/api/v2` / `2.2` | |
| `MEXC_POSITION_MODE` | `oneway` | Only `oneway` is supported. The bridge verifies the mode; it never switches it. |
| `ENFORCE_MARGIN_MODE` / `ENFORCE_LEVERAGE` | `true` / `true` | Apply margin mode / leverage before each entry. |
| `EXCHANGE_TIMEOUT_MS` / `POSITION_SYNC_DELAY_SEC` | `10000` / `0.75` | |
| `LOG_LEVEL` / `PORT` | `INFO` / `8000` | Railway/Render set `PORT` themselves. |

## Presets

**Paper trading (safe default)**
```
TRADINGVIEW_WEBHOOK_SECRET=<long random string>
TRADING_ENABLED=false
DRY_RUN=true
STATE_DB_PATH=/data/mexc_bridge.db
OPENROUTER_API_KEY=<optional>
```

**Live** (only after the paper ledger looks right)
```
TRADINGVIEW_WEBHOOK_SECRET=<same as in TradingView>
EXCHANGE_API_KEY=<trade-only key>
EXCHANGE_API_SECRET=<secret>
TRADING_ENABLED=true
DRY_RUN=false
MAX_RISK_USD=10
MAX_OPEN_POSITIONS=1
ALLOWED_SYMBOLS=BTCUSDT,ETHUSDT
STATE_DB_PATH=/data/mexc_bridge.db
OPENROUTER_API_KEY=<optional>
```
MEXC must be in **one-way** position mode before the first live signal.

## Autonomous scanner (v34.1 - no TradingView needed)
The scanner looks for **long-only** setups by itself every minute on MEXC data and sends them through the same Research -> Analysis -> Trader gates as a webhook signal, so paper/live mode, the $10 risk cap, R:R >= 1.5, cost gate, position limits, cooldown and the circuit breaker all still apply. Setup: 1h uptrend (EMA8 > EMA21, close above EMA21), 5m EMA20 > EMA50, a recent pullback to the 5m EMA20, then a bullish candle that closes above EMA20 and the previous high; volume acceleration must pass; stop = below the last 8 bars' low (max 3 ATR). It trades at most one setup per cycle.

| Variable | Default | Meaning |
|---|---|---|
| `SCANNER_ENABLED` | `true` | Turn the scanner on/off. |
| `SCANNER_SYMBOLS` | BTC,ETH,SOL,XRP,DOGE,BNB,ADA,AVAX,LINK,SUI,LTC,DOT,NEAR,APT,ARB | Base coins to watch (USDT-M). |
| `SCANNER_INTERVAL_SEC` | `60` | Scan frequency. |
| `SCANNER_MAX_PER_CYCLE` | `1` | Max new entries per scan. |
| `SCANNER_MIN_SCORE` | `65` | Minimum setup score (0-100). |
| `SCANNER_SWING_BARS` | `8` | Stop goes below the lowest low of N 5m bars. |
| `SCANNER_MAX_STOP_ATR` | `3.0` | Skip setups whose stop is wider than N ATR. |
| `SCANNER_PULLBACK_ATR` | `0.35` | How close to EMA20 the pullback must come. |
| `SCANNER_MAX_EXTENSION_ATR` | `1.5` | Do not chase when close is more than N ATR above EMA20. |

Check it at `GET /status` (header `X-Webhook-Secret`) under `"scanner"`. Because costs are checked up front, tight-stop setups on very low-volatility majors are skipped; that is expected. Keep `TRADING_ENABLED=false` / `DRY_RUN=true` until the paper ledger (`/analytics`) looks good.

## Deploy
* **Railway**: `railway.json` already holds the start command (`uvicorn mexc_futures_bridge_v34:app ...`) and `/health` check. Add a Volume mounted at `/data`.
* **Render**: Start command `uvicorn mexc_futures_bridge_v34:app --host 0.0.0.0 --port $PORT`, health check path `/health`, attach a Disk at `/data` (disks need a paid instance; on the free tier the ledger resets on every redeploy and free instances sleep when idle, which stops the position monitor - for live trading use an always-on instance).

## Endpoints
**`GET /dashboard`** (visual dashboard: open it in any browser, enter the secret once; auto-refreshes every 10 s), `POST /webhook` (TradingView), `GET /health`, `GET /ready`. With header `X-Webhook-Secret`: `GET /status`, `/analytics`, `/trades?limit=20`, `/signals?limit=40`, `/bias`, and `POST /brain/run` (run one 2nd Brain cycle now).

## TradingView alert body
Same as before (see `clean_test.json`): `secret, signal_id, action (BUY_LONG|SELL_SHORT|EXIT_LONG|EXIT_SHORT), symbol, price, sl, tp1, tp2, leverage, risk_usd, timestamp`. `sl/tp1/tp2` are optional; when omitted they are derived from ATR.
