import os
import time
import json
import hmac
import hashlib
import logging
import threading
import random
from decimal import Decimal, ROUND_DOWN, InvalidOperation
from collections import deque
from concurrent.futures import ThreadPoolExecutor

import requests
import websocket

from flask import Flask, jsonify


# ============================================================
# CONFIG
# ============================================================

BINANCE_API_KEY = os.getenv("BINANCE_API_KEY")
BINANCE_API_SECRET = os.getenv("BINANCE_API_SECRET")

BASE_URL = "https://api.binance.com"
WS_BASE_URL = "wss://stream.binance.com:9443/stream?streams="

TIMEFRAME = "5m"

TOP_SYMBOLS = 150
GROUP_SIZE = 50

BUY_USDT = Decimal("15")

RSI_FAST_PERIOD = 3
RSI_SLOW_PERIOD = 50

BUY_RSI50_MIN = Decimal("52")
BUY_RSI3_MAX = Decimal("2")

SELL_RSI3_LEVEL = Decimal("80")

# Exchange-side 1% protective stop loss
STOP_LOSS_PERCENT = Decimal("1")

# Live RSI3 cross detection: sell as soon as the live 5m candle RSI3
# crosses above 80, instead of waiting for candle close.
LIVE_RSI_SELL = True

# RSI50-এর জন্য পর্যাপ্ত history
HISTORY_CANDLES = 200

WS_PING_INTERVAL = 20
WS_PING_TIMEOUT = 10

REQUEST_TIMEOUT = 15
MAX_RETRIES = 3

# ------------------------------------------------------------
# Render Free / Binance rate-limit protection
# ------------------------------------------------------------
# Keep REST startup data on disk so a normal process restart does
# not immediately repeat the heaviest public requests.
CACHE_DIR = os.getenv("BOT_CACHE_DIR", ".bot_cache")
EXCHANGE_CACHE_FILE = os.path.join(CACHE_DIR, "exchange_info.json")
SYMBOL_CACHE_FILE = os.path.join(CACHE_DIR, "top_symbols.json")
HISTORY_CACHE_FILE = os.path.join(CACHE_DIR, "history.json")

# Exchange metadata / TOP-150 do not need to be refreshed on every restart.
EXCHANGE_CACHE_TTL = 6 * 60 * 60
SYMBOL_CACHE_TTL = 30 * 60
HISTORY_CACHE_TTL = 30 * 60

# Minimum cooldowns after Binance tells us to slow down.
RATE_LIMIT_MIN_COOLDOWN_418 = 300
RATE_LIMIT_MIN_COOLDOWN_429 = 30

# A little slower than the previous 0.15s startup loop.
STARTUP_KLINE_DELAY = 0.50

WORKER_THREADS = 4

DRY_RUN = os.getenv("DRY_RUN", "false").lower() == "true"

# Debug mode:
# true = signal/near-signal এবং candle summary বেশি বিস্তারিত দেখাবে
DEBUG_MODE = os.getenv("DEBUG_MODE", "true").lower() == "true"

# RSI3 কত হলে near-signal হিসেবে log করবে
NEAR_RSI3_LEVEL = Decimal("10")

# RSI50 কত হলে near-signal হিসেবে log করবে
NEAR_RSI50_LEVEL = Decimal("48")


# ============================================================
# LOGGING
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

log = logging.getLogger("BINANCE_RSI_BOT")


# ============================================================
# FLASK
# ============================================================

app = Flask(__name__)


# ============================================================
# GLOBAL STATE
# ============================================================

state_lock = threading.RLock()

candle_history = {}
positions = {}

selling_symbols = set()
buying_symbols = set()

symbol_info = {}
symbols = []

server_time_offset_ms = 0

http = requests.Session()

order_executor = ThreadPoolExecutor(
    max_workers=WORKER_THREADS,
    thread_name_prefix="ORDER",
)

_bot_thread_started = False
_bot_thread_lock = threading.Lock()

websocket_status = {}
last_ws_message_time = {}
last_closed_candle_time = {}

# Prevent duplicate live signal submissions on the same candle.
live_sell_triggered_candle = {}
live_buy_triggered_candle = {}

last_signal_time = None
last_buy_signal = None
last_sell_signal = None

# প্রতি candle timestamp-এ summary রাখার জন্য
candle_summary = {}

summary_lock = threading.Lock()


# ============================================================
# EXCLUDED ASSETS
# ============================================================

EXCLUDED_ASSETS = {
    "BTC",
    "ETH",

    # Stablecoins
    "USDT",
    "USDC",
    "FDUSD",
    "BUSD",
    "TUSD",
    "DAI",
    "USDP",
    "USD1",
    "UST",
    "USTC",
    "FRAX",
    "LUSD",
    "PYUSD",
    "USDD",

    # Fiat
    "EUR",
    "EURI",
    "AEUR",
    "TRY",
    "BRL",
    "GBP",
    "AUD",
    "BIDR",
    "IDRT",
    "UAH",
    "PLN",
    "RON",
    "ARS",
    "NGN",
    "ZAR",
    "RUB",
    "MXN",
    "COP",
    "JPY",

    # Other
    "PAX",
}


# ============================================================
# DECIMAL HELPERS
# ============================================================

def d(value):
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return Decimal("0")


def decimal_places(step):
    step = d(step)

    if step <= 0:
        return 0

    text = format(step, "f")

    if "." not in text:
        return 0

    return len(text.rstrip("0").split(".")[1])


def floor_to_step(value, step):
    value = d(value)
    step = d(step)

    if step <= 0:
        return value

    return (value / step).to_integral_value(
        rounding=ROUND_DOWN
    ) * step


def decimal_to_string(value):
    value = d(value)

    text = format(value, "f")

    if "." in text:
        text = text.rstrip("0").rstrip(".")

    return text if text else "0"


# ============================================================
# REST REQUEST
# ============================================================

class BinanceRateLimitError(RuntimeError):
    def __init__(self, status_code, wait_seconds, path):
        self.status_code = int(status_code)
        self.wait_seconds = max(1, int(wait_seconds))
        self.path = path
        super().__init__(
            f"Binance HTTP {self.status_code} rate limit on {path}; "
            f"cooldown={self.wait_seconds}s"
        )


def ensure_cache_dir():
    try:
        os.makedirs(CACHE_DIR, exist_ok=True)
    except Exception as exc:
        log.warning("Cache directory unavailable: %s", exc)


def cache_age(path):
    try:
        return max(0, time.time() - os.path.getmtime(path))
    except OSError:
        return float("inf")


def load_json_cache(path, max_age):
    if cache_age(path) > max_age:
        return None

    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as exc:
        log.warning("Cache read failed | %s | %s", path, exc)
        return None


def save_json_cache(path, data):
    try:
        ensure_cache_dir()
        temp = path + ".tmp"
        with open(temp, "w", encoding="utf-8") as f:
            json.dump(data, f, separators=(",", ":"))
        os.replace(temp, path)
        return True
    except Exception as exc:
        log.warning("Cache write failed | %s | %s", path, exc)
        return False


def public_get(path, params=None):
    url = BASE_URL + path
    last_error = None

    for attempt in range(MAX_RETRIES):
        try:
            response = http.get(
                url,
                params=params,
                timeout=REQUEST_TIMEOUT,
            )

            if response.status_code == 200:
                return response.json()

            if response.status_code in (418, 429):
                retry_after = response.headers.get("Retry-After")

                try:
                    wait = float(retry_after) if retry_after else 0
                except (TypeError, ValueError):
                    wait = 0

                if response.status_code == 418:
                    wait = max(wait, RATE_LIMIT_MIN_COOLDOWN_418)
                else:
                    wait = max(wait, RATE_LIMIT_MIN_COOLDOWN_429)

                wait += random.uniform(0.5, 1.5)

                log.error(
                    "Binance HTTP %s | %s | cooldown %.0fs | NO rapid retry",
                    response.status_code,
                    path,
                    wait,
                )

                # Do not keep hammering the same endpoint inside this call.
                raise BinanceRateLimitError(
                    response.status_code,
                    wait,
                    path,
                )

            if response.status_code >= 500:
                wait = min(10, 1.5 ** attempt) + random.uniform(0.2, 0.8)
                log.warning(
                    "Binance server error %s | %s | retry %.2fs",
                    response.status_code,
                    path,
                    wait,
                )
                time.sleep(wait)
                continue

            response.raise_for_status()

        except BinanceRateLimitError:
            raise

        except Exception as exc:
            last_error = exc
            wait = min(8, 1.5 ** attempt) + random.uniform(0.2, 0.7)
            log.warning(
                "REST error | %s | retry %.2fs",
                exc,
                wait,
            )
            time.sleep(wait)

    raise RuntimeError(
        f"Public REST failed: {path} | {last_error}"
    )


# ============================================================
# SIGNED REQUEST
# ============================================================

def signed_request(method, path, params=None):
    if params is None:
        params = {}

    params = dict(params)

    params["timestamp"] = int(
        time.time() * 1000
    ) + server_time_offset_ms

    params["recvWindow"] = 5000

    query_string = "&".join(
        f"{key}={value}"
        for key, value in params.items()
    )

    signature = hmac.new(
        BINANCE_API_SECRET.encode(),
        query_string.encode(),
        hashlib.sha256,
    ).hexdigest()

    params["signature"] = signature

    headers = {
        "X-MBX-APIKEY": BINANCE_API_KEY
    }

    response = http.request(
        method,
        BASE_URL + path,
        params=params,
        headers=headers,
        timeout=REQUEST_TIMEOUT,
    )

    if response.status_code == 200:
        return response.json()

    if response.status_code in (418, 429):
        retry_after = response.headers.get("Retry-After")
        try:
            wait = float(retry_after) if retry_after else 0
        except (TypeError, ValueError):
            wait = 0

        if response.status_code == 418:
            wait = max(wait, RATE_LIMIT_MIN_COOLDOWN_418)
        else:
            wait = max(wait, RATE_LIMIT_MIN_COOLDOWN_429)

        wait += random.uniform(0.5, 1.5)
        log.error(
            "Binance signed HTTP %s | %s | cooldown %.0fs",
            response.status_code,
            path,
            wait,
        )
        raise BinanceRateLimitError(
            response.status_code,
            wait,
            path,
        )

    raise RuntimeError(
        f"Signed request failed: "
        f"{response.status_code} | {response.text}"
    )


# ============================================================
# SERVER TIME
# ============================================================

def sync_server_time():
    global server_time_offset_ms

    data = public_get("/api/v3/time")

    server_time = int(data["serverTime"])
    local_time = int(time.time() * 1000)

    server_time_offset_ms = server_time - local_time

    log.info(
        "Binance server time offset: %d ms",
        server_time_offset_ms,
    )


# ============================================================
# EXCHANGE INFO
# ============================================================

def load_exchange_info():
    global symbol_info

    cached = load_json_cache(EXCHANGE_CACHE_FILE, EXCHANGE_CACHE_TTL)
    if cached and isinstance(cached, dict):
        symbol_info = cached
        log.info(
            "Loaded exchange information from cache | eligible=%d | age=%.0fs",
            len(symbol_info),
            cache_age(EXCHANGE_CACHE_FILE),
        )
        return

    log.info("Loading Binance exchange information (REST)...")

    data = public_get("/api/v3/exchangeInfo")
    result = {}

    for item in data.get("symbols", []):
        symbol = item.get("symbol")
        status = item.get("status")
        quote_asset = item.get("quoteAsset")

        if status != "TRADING" or quote_asset != "USDT":
            continue
        if item.get("isSpotTradingAllowed") is False:
            continue

        base_asset = item.get("baseAsset")
        if base_asset in EXCLUDED_ASSETS:
            continue

        filters = {}
        for f in item.get("filters", []):
            filters[f.get("filterType")] = f

        result[symbol] = {
            "symbol": symbol,
            "baseAsset": base_asset,
            "quoteAsset": quote_asset,
            "filters": filters,
        }

    symbol_info = result
    save_json_cache(EXCHANGE_CACHE_FILE, result)

    log.info("Eligible USDT spot symbols: %d", len(symbol_info))


# ============================================================
# TOP 150
# ============================================================

def select_top_symbols():
    global symbols

    cached = load_json_cache(SYMBOL_CACHE_FILE, SYMBOL_CACHE_TTL)
    if cached and isinstance(cached, list):
        valid = [x for x in cached if x in symbol_info]
        if len(valid) >= TOP_SYMBOLS:
            symbols = valid[:TOP_SYMBOLS]
            log.info(
                "Loaded TOP-%d symbols from cache | age=%.0fs",
                len(symbols),
                cache_age(SYMBOL_CACHE_FILE),
            )
            if DEBUG_MODE:
                log.info("TOP SYMBOLS: %s", ", ".join(symbols))
            return

    log.info("Loading 24h ticker data (REST)...")
    tickers = public_get("/api/v3/ticker/24hr")
    eligible = []

    for ticker in tickers:
        symbol = ticker.get("symbol")
        if symbol not in symbol_info:
            continue
        try:
            quote_volume = d(ticker.get("quoteVolume", "0"))
        except Exception:
            continue
        eligible.append((symbol, quote_volume))

    eligible.sort(key=lambda x: x[1], reverse=True)
    symbols = [item[0] for item in eligible[:TOP_SYMBOLS]]
    save_json_cache(SYMBOL_CACHE_FILE, symbols)

    log.info("Selected TOP %d symbols", len(symbols))
    if DEBUG_MODE:
        log.info("TOP SYMBOLS: %s", ", ".join(symbols))


# ============================================================
# RSI WILDER
# ============================================================

def calculate_rsi_wilder(closes, period):
    """
    Wilder RSI / RMA style calculation.
    Returns RSI values aligned to closes.
    """

    if len(closes) < period + 1:
        return []

    closes = [d(x) for x in closes]

    gains = []
    losses = []

    for i in range(1, len(closes)):
        change = closes[i] - closes[i - 1]

        if change > 0:
            gains.append(change)
            losses.append(Decimal("0"))

        else:
            gains.append(Decimal("0"))
            losses.append(-change)

    avg_gain = sum(
        gains[:period],
        Decimal("0"),
    ) / Decimal(period)

    avg_loss = sum(
        losses[:period],
        Decimal("0"),
    ) / Decimal(period)

    result = [None] * (period)

    if avg_loss == 0:

        if avg_gain == 0:
            first_rsi = Decimal("50")
        else:
            first_rsi = Decimal("100")

    else:

        rs = avg_gain / avg_loss

        first_rsi = Decimal("100") - (
            Decimal("100")
            / (Decimal("1") + rs)
        )

    result.append(first_rsi)

    for i in range(period, len(gains)):

        avg_gain = (
            (avg_gain * Decimal(period - 1))
            + gains[i]
        ) / Decimal(period)

        avg_loss = (
            (avg_loss * Decimal(period - 1))
            + losses[i]
        ) / Decimal(period)

        if avg_loss == 0:

            if avg_gain == 0:
                rsi = Decimal("50")
            else:
                rsi = Decimal("100")

        else:

            rs = avg_gain / avg_loss

            rsi = Decimal("100") - (
                Decimal("100")
                / (Decimal("1") + rs)
            )

        result.append(rsi)

    return result


# ============================================================
# CANDLE STORAGE
# ============================================================

def candle_to_dict(k):
    return {
        "open_time": int(k[0]),
        "open": d(k[1]),
        "high": d(k[2]),
        "low": d(k[3]),
        "close": d(k[4]),
        "volume": d(k[5]),
        "close_time": int(k[6]),
    }


def add_closed_candle(symbol, candle):
    with state_lock:

        history = candle_history.setdefault(
            symbol,
            deque(maxlen=HISTORY_CANDLES),
        )

        if history:

            if history[-1]["open_time"] == candle["open_time"]:
                history[-1] = candle
                return

            if candle["open_time"] < history[-1]["open_time"]:
                return

        history.append(candle)


# ============================================================
# HISTORICAL DATA
# ============================================================

def _serialize_history():
    with state_lock:
        return {
            symbol: [
                {
                    "open_time": int(c["open_time"]),
                    "open": decimal_to_string(c["open"]),
                    "high": decimal_to_string(c["high"]),
                    "low": decimal_to_string(c["low"]),
                    "close": decimal_to_string(c["close"]),
                    "volume": decimal_to_string(c["volume"]),
                    "close_time": int(c["close_time"]),
                }
                for c in history
            ]
            for symbol, history in candle_history.items()
        }


def _load_history_cache():
    cached = load_json_cache(HISTORY_CACHE_FILE, HISTORY_CACHE_TTL)
    if not isinstance(cached, dict):
        return 0

    loaded = 0
    with state_lock:
        for symbol in symbols:
            rows = cached.get(symbol)
            if not isinstance(rows, list) or len(rows) < RSI_SLOW_PERIOD + 2:
                continue
            try:
                history = deque(maxlen=HISTORY_CANDLES)
                for row in rows:
                    history.append({
                        "open_time": int(row["open_time"]),
                        "open": d(row["open"]),
                        "high": d(row["high"]),
                        "low": d(row["low"]),
                        "close": d(row["close"]),
                        "volume": d(row["volume"]),
                        "close_time": int(row["close_time"]),
                    })
                candle_history[symbol] = history
                loaded += 1
            except Exception:
                continue

    if loaded:
        log.info(
            "Loaded historical candles from cache | %d/%d symbols | age=%.0fs",
            loaded,
            len(symbols),
            cache_age(HISTORY_CACHE_FILE),
        )
    return loaded


def load_historical_data():
    loaded_from_cache = _load_history_cache()

    # A fresh cache is enough for a normal Render restart. This avoids
    # another 150 REST klines burst immediately after a restart.
    if loaded_from_cache >= len(symbols):
        log.info("Historical REST loading skipped: cache is fresh and complete.")
        return

    log.info(
        "Loading missing initial history | %d candles | %d symbols...",
        HISTORY_CANDLES,
        len(symbols) - loaded_from_cache,
    )

    success = loaded_from_cache

    for index, symbol in enumerate(symbols, start=1):
        with state_lock:
            already_loaded = len(candle_history.get(symbol, [])) >= RSI_SLOW_PERIOD + 2
        if already_loaded:
            continue

        try:
            data = public_get(
                "/api/v3/klines",
                {
                    "symbol": symbol,
                    "interval": TIMEFRAME,
                    "limit": HISTORY_CANDLES,
                },
            )

            history = deque(maxlen=HISTORY_CANDLES)
            now_ms = int(time.time() * 1000)

            for item in data:
                candle = candle_to_dict(item)
                if candle["close_time"] < now_ms:
                    history.append(candle)

            with state_lock:
                candle_history[symbol] = history

            if len(history) >= RSI_SLOW_PERIOD + 1:
                success += 1

        except BinanceRateLimitError:
            raise
        except Exception as exc:
            log.error("%s | Historical data error: %s", symbol, exc)

        if index % 25 == 0 or index == len(symbols):
            log.info(
                "Historical loading progress: %d/%d",
                index,
                len(symbols),
            )

        time.sleep(STARTUP_KLINE_DELAY)

    save_json_cache(HISTORY_CACHE_FILE, _serialize_history())

    log.info(
        "Historical initialization complete: %d/%d",
        success,
        len(symbols),
    )


# ============================================================
# INDICATORS
# ============================================================

def get_current_indicators(symbol):

    with state_lock:
        history = list(
            candle_history.get(symbol, [])
        )

    if len(history) < RSI_SLOW_PERIOD + 2:
        return None

    closes = [
        candle["close"]
        for candle in history
    ]

    rsi3_values = calculate_rsi_wilder(
        closes,
        RSI_FAST_PERIOD,
    )

    rsi50_values = calculate_rsi_wilder(
        closes,
        RSI_SLOW_PERIOD,
    )

    if not rsi3_values or not rsi50_values:
        return None

    current_rsi3 = rsi3_values[-1]
    previous_rsi3 = rsi3_values[-2]

    current_rsi50 = rsi50_values[-1]

    if (
        current_rsi3 is None
        or previous_rsi3 is None
        or current_rsi50 is None
    ):
        return None

    return {
        "rsi3": current_rsi3,
        "previous_rsi3": previous_rsi3,
        "rsi50": current_rsi50,
    }


# ============================================================
# ACCOUNT BALANCE
# ============================================================

def get_asset_balance(asset):

    data = signed_request(
        "GET",
        "/api/v3/account",
    )

    for item in data.get("balances", []):

        if item.get("asset") == asset:
            return d(item.get("free", "0"))

    return Decimal("0")


# ============================================================
# ACCOUNT VERIFY
# ============================================================

def verify_account():

    try:

        data = signed_request(
            "GET",
            "/api/v3/account",
        )

        account_type = data.get(
            "accountType",
            "UNKNOWN",
        )

        log.info(
            "Binance account connection OK | accountType=%s",
            account_type,
        )

        return True

    except Exception as exc:

        log.error(
            "Binance account connection FAILED: %s",
            exc,
        )

        return False


# ============================================================
# ORDER CLIENT ID
# ============================================================

def create_client_order_id(prefix):

    return (
        prefix
        + str(int(time.time() * 1000))
        + "_"
        + str(random.randint(100, 999))
    )[:36]


# ============================================================
# FIND ORDER
# ============================================================

def get_order_by_client_id(symbol, client_order_id):

    try:

        return signed_request(
            "GET",
            "/api/v3/order",
            {
                "symbol": symbol,
                "origClientOrderId": client_order_id,
            },
        )

    except Exception:

        return None


# ============================================================
# ORDER / STOP-LOSS HELPERS
# ============================================================

def get_step_size(symbol, market=True):

    filters = symbol_info[symbol]["filters"]

    if market:
        f = filters.get("MARKET_LOT_SIZE")
        if f:
            step = d(f.get("stepSize", "0"))
            if step > 0:
                return step

    f = filters.get("LOT_SIZE")
    if f:
        step = d(f.get("stepSize", "0"))
        if step > 0:
            return step

    return Decimal("0")


def get_price_tick_size(symbol):

    filters = symbol_info[symbol]["filters"]
    f = filters.get("PRICE_FILTER")

    if f:
        tick = d(f.get("tickSize", "0"))
        if tick > 0:
            return tick

    return Decimal("0")


def cancel_order(symbol, order_id=None, client_order_id=None):

    params = {"symbol": symbol}

    if order_id is not None:
        params["orderId"] = order_id
    elif client_order_id:
        params["origClientOrderId"] = client_order_id
    else:
        return None

    try:
        response = signed_request(
            "DELETE",
            "/api/v3/order",
            params,
        )
        log.info(
            "Order canceled | %s | orderId=%s | clientId=%s",
            symbol,
            response.get("orderId"),
            response.get("clientOrderId"),
        )
        return response
    except Exception as exc:
        log.warning(
            "Cancel order failed | %s | orderId=%s | clientId=%s | %s",
            symbol,
            order_id,
            client_order_id,
            exc,
        )
        return None


def place_stop_loss(symbol, quantity, entry_price):
    """Place a Binance Spot STOP_LOSS MARKET sell 1% below entry."""

    if DRY_RUN:
        return None, None

    if quantity <= 0 or entry_price <= 0:
        return None, None

    stop_price = entry_price * (
        Decimal("1") - STOP_LOSS_PERCENT / Decimal("100")
    )

    tick = get_price_tick_size(symbol)
    if tick > 0:
        stop_price = floor_to_step(stop_price, tick)

    quantity = floor_to_step(
        quantity,
        get_step_size(symbol),
    )

    if stop_price <= 0 or quantity <= 0:
        return None, None

    client_order_id = create_client_order_id("RSISL_")

    params = {
        "symbol": symbol,
        "side": "SELL",
        "type": "STOP_LOSS",
        "quantity": decimal_to_string(quantity),
        "stopPrice": decimal_to_string(stop_price),
        "newOrderRespType": "FULL",
        "newClientOrderId": client_order_id,
    }

    try:
        response = signed_request(
            "POST",
            "/api/v3/order",
            params,
        )

        log.warning(
            "🛡️ 1%% STOP LOSS PLACED | %s | entry=%s | stop=%s | qty=%s | orderId=%s",
            symbol,
            decimal_to_string(entry_price),
            decimal_to_string(stop_price),
            decimal_to_string(quantity),
            response.get("orderId"),
        )

        return response.get("orderId"), client_order_id

    except Exception as exc:
        log.error(
            "STOP LOSS ORDER FAILED | %s | entry=%s | stop=%s | qty=%s | %s",
            symbol,
            decimal_to_string(entry_price),
            decimal_to_string(stop_price),
            decimal_to_string(quantity),
            exc,
        )
        return None, None


def get_live_indicators(symbol, live_close):
    """
    Calculate live RSI3 and live RSI50 by adding the current
    5m candle close to the closed-candle history.

    RSI3 is used for the deep-oversold live BUY trigger and
    live SELL cross. RSI50 is also calculated from the live
    candle so BOTH BUY conditions are checked at the moment
    the live signal occurs.
    """

    with state_lock:
        history = list(candle_history.get(symbol, []))

    if len(history) < RSI_SLOW_PERIOD + 2:
        return None

    closes = [c["close"] for c in history]
    live_closes = closes + [d(live_close)]

    previous_rsi3_values = calculate_rsi_wilder(
        closes,
        RSI_FAST_PERIOD,
    )

    live_rsi3_values = calculate_rsi_wilder(
        live_closes,
        RSI_FAST_PERIOD,
    )

    live_rsi50_values = calculate_rsi_wilder(
        live_closes,
        RSI_SLOW_PERIOD,
    )

    if (
        not previous_rsi3_values
        or not live_rsi3_values
        or not live_rsi50_values
    ):
        return None

    previous_rsi3 = previous_rsi3_values[-1]
    live_rsi3 = live_rsi3_values[-1]
    live_rsi50 = live_rsi50_values[-1]

    if (
        previous_rsi3 is None
        or live_rsi3 is None
        or live_rsi50 is None
    ):
        return None

    return {
        "previous_rsi3": previous_rsi3,
        "rsi3": live_rsi3,
        "rsi50": live_rsi50,
    }


def get_live_rsi3(symbol, live_close):
    """Backward-compatible helper returning previous RSI3 and live RSI3."""

    values = get_live_indicators(symbol, live_close)

    if values is None:
        return None

    return values["previous_rsi3"], values["rsi3"]


# ============================================================
# BUY
# ============================================================

def execute_buy(symbol, indicators):

    with state_lock:

        if symbol in positions:
            return

        if symbol in buying_symbols:
            return

        buying_symbols.add(symbol)

    try:

        log.warning(
            "=================================================="
        )

        log.warning(
            "BUY SIGNAL | %s | RSI50=%.4f | RSI3=%.4f",
            symbol,
            indicators["rsi50"],
            indicators["rsi3"],
        )

        log.warning(
            "BUY ORDER | %s | Amount=%s USDT",
            symbol,
            BUY_USDT,
        )

        if DRY_RUN:

            log.warning(
                "DRY_RUN=True | BUY NOT SENT | %s",
                symbol,
            )

            with state_lock:
                positions[symbol] = {
                    "quantity": Decimal("0"),
                    "dry_run": True,
                }

            return

        client_order_id = create_client_order_id(
            "RSIBUY_"
        )

        params = {
            "symbol": symbol,
            "side": "BUY",
            "type": "MARKET",
            "quoteOrderQty": decimal_to_string(
                BUY_USDT
            ),
            "newOrderRespType": "FULL",
            "newClientOrderId": client_order_id,
        }

        log.info(
            "Sending MARKET BUY | %s | quoteOrderQty=%s",
            symbol,
            params["quoteOrderQty"],
        )

        try:

            response = signed_request(
                "POST",
                "/api/v3/order",
                params,
            )

        except Exception as exc:

            log.error(
                "BUY request error | %s | %s",
                symbol,
                exc,
            )

            # 5xx বা network ambiguity হলে order খুঁজে দেখার চেষ্টা
            found = get_order_by_client_id(
                symbol,
                client_order_id,
            )

            if found:

                response = found

                log.warning(
                    "BUY order found after request error | %s | orderId=%s",
                    symbol,
                    found.get("orderId"),
                )

            else:

                log.error(
                    "BUY NOT CONFIRMED | %s",
                    symbol,
                )

                return

        status = response.get(
            "status",
            "UNKNOWN",
        )

        order_id = response.get(
            "orderId"
        )

        executed_qty = d(
            response.get(
                "executedQty",
                "0",
            )
        )

        log.warning(
            "BUY RESPONSE | %s | status=%s | orderId=%s | executedQty=%s",
            symbol,
            status,
            order_id,
            executed_qty,
        )

        # Commission বাদ
        base_asset = symbol_info[symbol]["baseAsset"]

        total_commission = Decimal("0")

        for fill in response.get("fills", []):

            commission_asset = fill.get(
                "commissionAsset"
            )

            if commission_asset == base_asset:

                total_commission += d(
                    fill.get(
                        "commission",
                        "0",
                    )
                )

        net_quantity = (
            executed_qty
            - total_commission
        )

        filters = symbol_info[symbol]["filters"]

        market_lot = filters.get(
            "MARKET_LOT_SIZE"
        )

        lot = filters.get(
            "LOT_SIZE"
        )

        step = None

        if market_lot:
            step = d(
                market_lot.get(
                    "stepSize",
                    "0",
                )
            )

        if not step and lot:
            step = d(
                lot.get(
                    "stepSize",
                    "0",
                )
            )

        if step and step > 0:

            net_quantity = floor_to_step(
                net_quantity,
                step,
            )

        if status in (
            "FILLED",
            "PARTIALLY_FILLED",
        ) and net_quantity > 0:

            cumulative_quote = d(
                response.get("cummulativeQuoteQty", "0")
            )

            if cumulative_quote > 0 and executed_qty > 0:
                entry_price = cumulative_quote / executed_qty
            else:
                total_quote = Decimal("0")
                total_qty = Decimal("0")
                for fill in response.get("fills", []):
                    price = d(fill.get("price", "0"))
                    qty = d(fill.get("qty", "0"))
                    total_quote += price * qty
                    total_qty += qty
                entry_price = (
                    total_quote / total_qty
                    if total_qty > 0
                    else Decimal("0")
                )

            with state_lock:
                positions[symbol] = {
                    "quantity": net_quantity,
                    "entry_time": time.time(),
                    "order_id": order_id,
                    "entry_price": entry_price,
                    "stop_order_id": None,
                    "stop_client_order_id": None,
                }

            log.warning(
                "BUY CONFIRMED | %s | quantity=%s | entry=%s | orderId=%s",
                symbol,
                net_quantity,
                decimal_to_string(entry_price),
                order_id,
            )

            # Exchange-side 1% stop loss.
            stop_order_id, stop_client_order_id = place_stop_loss(
                symbol,
                net_quantity,
                entry_price,
            )

            with state_lock:
                if symbol in positions:
                    positions[symbol]["stop_order_id"] = stop_order_id
                    positions[symbol]["stop_client_order_id"] = stop_client_order_id

        else:

            log.error(
                "BUY NOT FILLED | %s | status=%s",
                symbol,
                status,
            )

    except Exception as exc:

        log.exception(
            "BUY execution exception | %s | %s",
            symbol,
            exc,
        )

    finally:

        with state_lock:
            buying_symbols.discard(symbol)


# ============================================================
# SELL
# ============================================================

def execute_sell(symbol, indicators):

    with state_lock:

        position = positions.get(symbol)

        if position is None:
            return

        if symbol in selling_symbols:
            return

        selling_symbols.add(symbol)

    try:

        log.warning("==================================================")

        log.warning(
            "SELL SIGNAL | %s | RSI3 %.4f -> %.4f",
            symbol,
            indicators.get("previous_rsi3", Decimal("0")),
            indicators.get("rsi3", Decimal("0")),
        )

        if DRY_RUN:
            log.warning("DRY_RUN=True | SELL NOT SENT | %s", symbol)
            with state_lock:
                positions.pop(symbol, None)
            return

        # Cancel the protective stop before manual RSI sell.
        stop_order_id = position.get("stop_order_id")
        stop_client_order_id = position.get("stop_client_order_id")

        if stop_order_id or stop_client_order_id:
            canceled = cancel_order(
                symbol,
                order_id=stop_order_id,
                client_order_id=stop_client_order_id,
            )

            # If the stop already triggered/filled, do not send a second sell.
            if canceled is None:
                stop_status = get_order_by_client_id(
                    symbol,
                    stop_client_order_id,
                ) if stop_client_order_id else None

                if stop_status and stop_status.get("status") in (
                    "FILLED",
                    "PARTIALLY_FILLED",
                ):
                    log.warning(
                        "STOP LOSS already executed | %s | orderId=%s",
                        symbol,
                        stop_status.get("orderId"),
                    )
                    with state_lock:
                        positions.pop(symbol, None)
                    return

        base_asset = symbol_info[symbol]["baseAsset"]
        balance = get_asset_balance(base_asset)

        if balance <= 0:
            log.warning(
                "SELL skipped | %s | free balance=%s",
                symbol,
                balance,
            )
            with state_lock:
                positions.pop(symbol, None)
            return

        step = get_step_size(symbol)
        quantity = floor_to_step(balance, step) if step > 0 else balance

        if quantity <= 0:
            log.warning("SELL quantity <= 0 | %s", symbol)
            return

        client_order_id = create_client_order_id("RSISELL_")

        params = {
            "symbol": symbol,
            "side": "SELL",
            "type": "MARKET",
            "quantity": decimal_to_string(quantity),
            "newOrderRespType": "FULL",
            "newClientOrderId": client_order_id,
        }

        log.warning(
            "Sending MARKET SELL | %s | quantity=%s",
            symbol,
            params["quantity"],
        )

        try:
            response = signed_request(
                "POST",
                "/api/v3/order",
                params,
            )
        except Exception as exc:
            log.error("SELL request error | %s | %s", symbol, exc)
            found = get_order_by_client_id(symbol, client_order_id)
            if found:
                response = found
                log.warning(
                    "SELL order found after request error | %s | orderId=%s",
                    symbol,
                    found.get("orderId"),
                )
            else:
                log.error("SELL NOT CONFIRMED | %s", symbol)
                return

        status = response.get("status", "UNKNOWN")
        order_id = response.get("orderId")
        executed_qty = d(response.get("executedQty", "0"))

        log.warning(
            "SELL RESPONSE | %s | status=%s | orderId=%s | executedQty=%s",
            symbol,
            status,
            order_id,
            executed_qty,
        )

        if status in ("FILLED", "PARTIALLY_FILLED"):
            with state_lock:
                positions.pop(symbol, None)

            log.warning(
                "SELL CONFIRMED | %s | orderId=%s",
                symbol,
                order_id,
            )

    except Exception as exc:
        log.exception(
            "SELL execution exception | %s | %s",
            symbol,
            exc,
        )

    finally:
        with state_lock:
            selling_symbols.discard(symbol)


# ============================================================
# CANDLE DEBUG SUMMARY
# ============================================================

def update_candle_summary(
    symbol,
    candle_time,
    indicators,
    buy_signal,
    sell_signal,
):

    key = candle_time

    with summary_lock:

        if key not in candle_summary:

            candle_summary[key] = {
                "processed": 0,
                "rsi3_below_2": 0,
                "rsi50_above_52": 0,
                "buy_signals": 0,
                "sell_signals": 0,
            }

        item = candle_summary[key]

        item["processed"] += 1

        if indicators["rsi3"] < BUY_RSI3_MAX:
            item["rsi3_below_2"] += 1

        if indicators["rsi50"] > BUY_RSI50_MIN:
            item["rsi50_above_52"] += 1

        if buy_signal:
            item["buy_signals"] += 1

        if sell_signal:
            item["sell_signals"] += 1

        # 150 symbols processed হলে final summary
        if item["processed"] >= len(symbols):

            log.warning(
                "CANDLE SUMMARY | candle=%s | processed=%d/%d | "
                "RSI3<2=%d | RSI50>52=%d | BUY=%d | SELL=%d",
                candle_time,
                item["processed"],
                len(symbols),
                item["rsi3_below_2"],
                item["rsi50_above_52"],
                item["buy_signals"],
                item["sell_signals"],
            )

            # পুরনো summary পরিষ্কার
            old_keys = [
                x
                for x in candle_summary
                if x < key
            ]

            for old_key in old_keys:
                candle_summary.pop(
                    old_key,
                    None,
                )


# ============================================================
# PROCESS CLOSED CANDLE
# ============================================================

def process_closed_candle(
    symbol,
    candle,
):

    add_closed_candle(
        symbol,
        candle,
    )

    candle_time = candle["open_time"]

    last_closed_candle_time[symbol] = candle_time

    # Keep only the current candle's live BUY/SELL locks for this symbol.
    # The next candle will naturally use a new open_time.
    if live_buy_triggered_candle.get(symbol) != candle_time:
        live_buy_triggered_candle.pop(symbol, None)

    if live_sell_triggered_candle.get(symbol) != candle_time:
        live_sell_triggered_candle.pop(symbol, None)

    indicators = get_current_indicators(
        symbol
    )

    if indicators is None:

        return

    rsi3 = indicators["rsi3"]
    previous_rsi3 = indicators["previous_rsi3"]
    rsi50 = indicators["rsi50"]

    with state_lock:
        has_position = (
            symbol in positions
        )

    # --------------------------------------------------------
    # SELL
    # --------------------------------------------------------

    sell_signal = (
        has_position
        and previous_rsi3 <= SELL_RSI3_LEVEL
        and rsi3 > SELL_RSI3_LEVEL
    )

    # --------------------------------------------------------
    # BUY
    # --------------------------------------------------------

    buy_signal = (
        (not has_position)
        and live_buy_triggered_candle.get(symbol) != candle_time
        and rsi50 > BUY_RSI50_MIN
        and rsi3 < BUY_RSI3_MAX
    )

    # --------------------------------------------------------
    # DEBUG
    # --------------------------------------------------------

    if DEBUG_MODE:

        # Near signal হলে log
        if (
            rsi3 < NEAR_RSI3_LEVEL
            or rsi50 > NEAR_RSI50_LEVEL
            or buy_signal
            or sell_signal
        ):

            log.info(
                "%s | CLOSED %s | RSI50=%.4f | RSI3 %.4f -> %.4f | "
                "BUY=%s | SELL=%s | POSITION=%s",
                symbol,
                TIMEFRAME,
                rsi50,
                previous_rsi3,
                rsi3,
                buy_signal,
                sell_signal,
                has_position,
            )

    # --------------------------------------------------------
    # SUMMARY
    # --------------------------------------------------------

    update_candle_summary(
        symbol,
        candle_time,
        indicators,
        buy_signal,
        sell_signal,
    )

    # --------------------------------------------------------
    # SIGNAL TIME
    # --------------------------------------------------------

    global last_signal_time
    global last_buy_signal
    global last_sell_signal

    if buy_signal:

        last_signal_time = time.time()

        last_buy_signal = {
            "symbol": symbol,
            "time": time.time(),
            "candle_time": candle_time,
            "rsi3": decimal_to_string(rsi3),
            "rsi50": decimal_to_string(rsi50),
        }

        log.warning(
            "🔥🔥 BUY CONDITIONS TRUE 🔥🔥 | "
            "%s | RSI50=%.4f > 52 | RSI3=%.4f < 2",
            symbol,
            rsi50,
            rsi3,
        )

        order_executor.submit(
            execute_buy,
            symbol,
            indicators,
        )

    # --------------------------------------------------------
    # SELL SIGNAL
    # --------------------------------------------------------

    if sell_signal:

        last_signal_time = time.time()

        last_sell_signal = {
            "symbol": symbol,
            "time": time.time(),
            "candle_time": candle_time,
            "previous_rsi3": decimal_to_string(
                previous_rsi3
            ),
            "rsi3": decimal_to_string(
                rsi3
            ),
        }

        log.warning(
            "🔥 SELL CONDITIONS TRUE 🔥 | "
            "%s | RSI3 %.4f -> %.4f",
            symbol,
            previous_rsi3,
            rsi3,
        )

        order_executor.submit(
            execute_sell,
            symbol,
            indicators,
        )


# ============================================================
# SOFTWARE STOP-LOSS FALLBACK
# ============================================================

def process_live_stop_loss(symbol, kline):
    """Fallback 1% stop if the exchange-side stop could not be placed."""

    with state_lock:
        position = positions.get(symbol)

    if not position or position.get("stop_order_id"):
        return

    entry_price = d(position.get("entry_price", "0"))
    if entry_price <= 0:
        return

    stop_price = entry_price * (
        Decimal("1") - STOP_LOSS_PERCENT / Decimal("100")
    )

    live_low = d(kline.get("l", "0"))

    if live_low > 0 and live_low <= stop_price:
        log.warning(
            "🛡️ SOFTWARE 1%% STOP LOSS TRIGGERED | %s | entry=%s | stop=%s | low=%s",
            symbol,
            decimal_to_string(entry_price),
            decimal_to_string(stop_price),
            decimal_to_string(live_low),
        )

        indicators = {
            "previous_rsi3": Decimal("0"),
            "rsi3": Decimal("0"),
            "rsi50": Decimal("0"),
        }

        order_executor.submit(
            execute_sell,
            symbol,
            indicators,
        )


# ============================================================
# LIVE RSI3 + RSI50 BUY
# ============================================================

def process_live_buy(symbol, kline):
    """
    BUY immediately during the live 5m candle when BOTH conditions
    become true:

        RSI50 > 52
        RSI3  < 2

    The values are calculated with the current live candle close.
    One BUY trigger is allowed per candle to prevent duplicate orders.
    """

    with state_lock:
        has_position = symbol in positions
        is_buying = symbol in buying_symbols

    if has_position or is_buying:
        return

    open_time = int(kline["t"])
    live_close = d(kline["c"])

    # Do not submit more than one live BUY for the same 5m candle.
    if live_buy_triggered_candle.get(symbol) == open_time:
        return

    values = get_live_indicators(symbol, live_close)

    if values is None:
        return

    previous_rsi3 = values["previous_rsi3"]
    live_rsi3 = values["rsi3"]
    live_rsi50 = values["rsi50"]

    buy_signal = (
        live_rsi50 > BUY_RSI50_MIN
        and live_rsi3 < BUY_RSI3_MAX
    )

    if not buy_signal:
        return

    # Lock the candle BEFORE submitting the order. This prevents
    # duplicate BUY submissions from rapid WebSocket updates.
    live_buy_triggered_candle[symbol] = open_time

    indicators = {
        "previous_rsi3": previous_rsi3,
        "rsi3": live_rsi3,
        "rsi50": live_rsi50,
        "live": True,
        "candle_time": open_time,
    }

    global last_signal_time, last_buy_signal

    last_signal_time = time.time()

    last_buy_signal = {
        "symbol": symbol,
        "time": time.time(),
        "candle_time": open_time,
        "rsi3": decimal_to_string(live_rsi3),
        "rsi50": decimal_to_string(live_rsi50),
        "live": True,
    }

    log.warning(
        "🔥🔥 LIVE BUY CONDITIONS TRUE 🔥🔥 | %s | "
        "RSI50=%.4f > 52 | RSI3=%.4f < 2 | BUY NOW",
        symbol,
        live_rsi50,
        live_rsi3,
    )

    order_executor.submit(
        execute_buy,
        symbol,
        indicators,
    )


# ============================================================
# LIVE RSI3 SELL CROSS
# ============================================================

def process_live_kline(symbol, kline):
    """
    Process every live 5m candle update.

    Order of checks:
      1) Software stop-loss fallback
      2) Live BUY: RSI50 > 52 AND RSI3 < 2
      3) Live SELL: RSI3 crosses above 80

    BUY and SELL remain independently protected against duplicate
    submissions on the same candle.
    """

    # If exchange-side protection is unavailable, use a software fallback.
    process_live_stop_loss(symbol, kline)

    # --------------------------------------------------------
    # LIVE BUY
    # --------------------------------------------------------
    # This runs on every live kline update, so a temporary RSI3
    # dip below 2 cannot be missed just because the candle later
    # closes above 2.
    process_live_buy(symbol, kline)

    # --------------------------------------------------------
    # LIVE SELL
    # --------------------------------------------------------
    if not LIVE_RSI_SELL:
        return

    with state_lock:
        has_position = symbol in positions

    if not has_position or symbol in selling_symbols:
        return

    open_time = int(kline["t"])
    live_close = d(kline["c"])

    # One trigger per candle is enough. If the order fails,
    # selling_symbols is cleared, but this candle remains locked
    # to avoid duplicate SELL submissions.
    if live_sell_triggered_candle.get(symbol) == open_time:
        return

    values = get_live_indicators(symbol, live_close)

    if values is None:
        return

    previous_rsi3 = values["previous_rsi3"]
    live_rsi3 = values["rsi3"]
    live_rsi50 = values["rsi50"]

    if previous_rsi3 <= SELL_RSI3_LEVEL and live_rsi3 > SELL_RSI3_LEVEL:

        live_sell_triggered_candle[symbol] = open_time

        indicators = {
            "previous_rsi3": previous_rsi3,
            "rsi3": live_rsi3,
            "rsi50": live_rsi50,
            "live": True,
            "candle_time": open_time,
        }

        global last_signal_time, last_sell_signal
        last_signal_time = time.time()
        last_sell_signal = {
            "symbol": symbol,
            "time": time.time(),
            "candle_time": open_time,
            "previous_rsi3": decimal_to_string(previous_rsi3),
            "rsi3": decimal_to_string(live_rsi3),
            "rsi50": decimal_to_string(live_rsi50),
            "live": True,
        }

        log.warning(
            "🔥🔥 LIVE RSI3 CROSS ABOVE 80 🔥🔥 | %s | "
            "RSI3 %.4f -> %.4f | RSI50=%.4f | SELL NOW",
            symbol,
            previous_rsi3,
            live_rsi3,
            live_rsi50,
        )

        order_executor.submit(
            execute_sell,
            symbol,
            indicators,
        )


# ============================================================
# WEBSOCKET MESSAGE
# ============================================================

def on_ws_message(group_id, message):

    try:

        payload = json.loads(message)

        stream_data = payload.get(
            "data",
            {}
        )

        event_type = stream_data.get(
            "e"
        )

        if event_type != "kline":
            return

        kline = stream_data.get(
            "k",
            {}
        )

        symbol = kline.get(
            "s"
        )

        if not symbol:
            return

        last_ws_message_time[symbol] = time.time()

        # Live candle: detect RSI3 crossing above 80 immediately.
        if not kline.get("x", False):
            process_live_kline(symbol, kline)
            return

        candle = {
            "open_time": int(
                kline["t"]
            ),
            "open": d(
                kline["o"]
            ),
            "high": d(
                kline["h"]
            ),
            "low": d(
                kline["l"]
            ),
            "close": d(
                kline["c"]
            ),
            "volume": d(
                kline["v"]
            ),
            "close_time": int(
                kline["T"]
            ),
        }

        log.debug(
            "GROUP %s | CLOSED CANDLE | %s",
            group_id,
            symbol,
        )

        process_closed_candle(
            symbol,
            candle,
        )

    except Exception as exc:

        log.exception(
            "WebSocket message processing error | group=%s | %s",
            group_id,
            exc,
        )


# ============================================================
# WEBSOCKET CALLBACKS
# ============================================================

def make_on_message(group_id):

    def callback(ws, message):
        on_ws_message(
            group_id,
            message,
        )

    return callback


def make_on_open(group_id):

    def callback(ws):
        websocket_status[group_id] = "CONNECTED"

        log.info(
            "WebSocket connected | group=%d",
            group_id,
        )

    return callback


def make_on_error(group_id):

    def callback(ws, error):

        websocket_status[group_id] = (
            "ERROR"
        )

        log.error(
            "WebSocket error | group=%d | %s",
            group_id,
            error,
        )

    return callback


def make_on_close(group_id):

    def callback(
        ws,
        close_status_code,
        close_msg,
    ):

        websocket_status[group_id] = (
            "DISCONNECTED"
        )

        log.warning(
            "WebSocket closed | group=%d | code=%s | msg=%s",
            group_id,
            close_status_code,
            close_msg,
        )

    return callback


# ============================================================
# RUN WEBSOCKET GROUP
# ============================================================

def run_websocket_group(
    group_id,
    group_symbols,
):

    streams = "/".join(
        f"{symbol.lower()}@kline_{TIMEFRAME}"
        for symbol in group_symbols
    )

    url = (
        WS_BASE_URL
        + streams
    )

    reconnect_delay = 2

    while True:

        try:

            websocket_status[group_id] = (
                "CONNECTING"
            )

            log.info(
                "WebSocket group %d connecting...",
                group_id,
            )

            ws = websocket.WebSocketApp(
                url,
                on_open=make_on_open(
                    group_id
                ),
                on_message=make_on_message(
                    group_id
                ),
                on_error=make_on_error(
                    group_id
                ),
                on_close=make_on_close(
                    group_id
                ),
            )

            ws.run_forever(
                ping_interval=WS_PING_INTERVAL,
                ping_timeout=WS_PING_TIMEOUT,
                ping_payload="ping",
                skip_utf8_validation=True,
            )

        except Exception as exc:

            log.exception(
                "WebSocket group %d exception: %s",
                group_id,
                exc,
            )

        websocket_status[group_id] = (
            "RECONNECTING"
        )

        wait = min(
            reconnect_delay,
            60,
        ) + random.uniform(
            0.5,
            1.5,
        )

        log.warning(
            "WebSocket group %d reconnecting in %.1fs",
            group_id,
            wait,
        )

        time.sleep(wait)

        reconnect_delay = min(
            reconnect_delay * 2,
            60,
        )


# ============================================================
# START WEBSOCKETS
# ============================================================

def start_websockets():

    groups = [
        symbols[i:i + GROUP_SIZE]
        for i in range(
            0,
            len(symbols),
            GROUP_SIZE,
        )
    ]

    log.info(
        "Creating %d WebSocket groups...",
        len(groups),
    )

    for index, group in enumerate(
        groups,
        start=1,
    ):

        log.info(
            "Starting WebSocket group %d | %d symbols",
            index,
            len(group),
        )

        thread = threading.Thread(
            target=run_websocket_group,
            args=(
                index,
                group,
            ),
            daemon=True,
            name=f"WS-GROUP-{index}",
        )

        thread.start()

        time.sleep(1)

    log.info(
        "All WebSocket groups started."
    )


# ============================================================
# BOT MAIN
# ============================================================

def run_bot():
    log.info("============================================================")
    log.info("STARTING BINANCE SPOT RSI3 + RSI50 BOT")
    log.info("Timeframe: %s", TIMEFRAME)
    log.info("Top symbols: %d", TOP_SYMBOLS)
    log.info("BUY: LIVE RSI50 > 52 AND LIVE RSI3 < 2")
    log.info("SELL: LIVE RSI3 crossing above 80")
    log.info("BUY amount: %s USDT", BUY_USDT)
    log.info("History candles: %d", HISTORY_CANDLES)
    log.info("REST cache: %s", CACHE_DIR)
    log.info("DEBUG_MODE: %s", DEBUG_MODE)
    log.info("DRY_RUN: %s", DRY_RUN)
    log.info("============================================================")

    # One signed-time sync per real startup. Periodic sync remains below.
    sync_server_time()

    load_exchange_info()
    select_top_symbols()

    if not symbols:
        raise RuntimeError("No eligible symbols found.")

    if not verify_account():
        raise RuntimeError("Binance account verification failed.")

    load_historical_data()

    start_websockets()

    log.info("BOT IS RUNNING")
    log.info("LIVE BUY: RSI50 > 52 AND RSI3 < 2 during live 5m candle")
    log.info("LIVE SELL: RSI3 crossing above 80")
    log.info("1%% exchange-side stop loss enabled")

    last_sync = time.time()
    last_cache_save = time.time()

    while True:
        time.sleep(10)

        # Keep the candle cache reasonably fresh so a normal Render restart
        # does not require another 150-klines startup burst.
        if time.time() - last_cache_save > 900:
            try:
                save_json_cache(HISTORY_CACHE_FILE, _serialize_history())
                last_cache_save = time.time()
                log.info("Historical cache refreshed.")
            except Exception as exc:
                log.warning("Historical cache refresh failed: %s", exc)

        if time.time() - last_sync > 1800:
            try:
                sync_server_time()
            except BinanceRateLimitError as exc:
                # Do not turn a periodic 418 into a restart loop.
                log.error(
                    "Periodic time-sync rate limited | cooldown=%ss",
                    exc.wait_seconds,
                )
                time.sleep(exc.wait_seconds)
            except Exception as exc:
                log.warning("Periodic server time sync failed: %s", exc)
            last_sync = time.time()


# ============================================================
# SUPERVISOR
# ============================================================

def start_background_bot():
    global _bot_thread_started

    with _bot_thread_lock:
        if _bot_thread_started:
            return
        _bot_thread_started = True

    log.info("STARTING BINANCE BOT BACKGROUND THREAD")

    def supervisor():
        restart_delay = 30

        while True:
            try:
                run_bot()
                log.warning("Bot main loop returned unexpectedly.")
                restart_delay = 30

            except BinanceRateLimitError as exc:
                # Critical difference from the old version: 418/429 does not
                # cause a 5s -> 10s -> 20s request storm. We honor the server
                # cooldown once, then make one controlled restart attempt.
                cooldown = max(exc.wait_seconds, 30)
                log.error(
                    "🚫 BINANCE RATE LIMIT | HTTP %s | endpoint=%s | sleeping %ss before next startup",
                    exc.status_code,
                    exc.path,
                    cooldown,
                )
                time.sleep(cooldown)
                restart_delay = 30
                continue

            except Exception as exc:
                log.exception("BOT CRASHED: %s", exc)

            log.warning(
                "Bot restarting in %d seconds...",
                restart_delay,
            )
            time.sleep(restart_delay)
            restart_delay = min(restart_delay * 2, 300)

    thread = threading.Thread(
        target=supervisor,
        daemon=True,
        name="BINANCE-BOT",
    )
    thread.start()


# ============================================================
# HEALTH
# ============================================================

@app.route("/")
def home():

    return jsonify({
        "status": "online",
        "bot": "BINANCE RSI3 + RSI50",
        "strategy": "LIVE RSI50 > 52 AND LIVE RSI3 < 2",
        "sell": "LIVE RSI3 crosses above 80",
        "timeframe": TIMEFRAME,
        "symbols": len(symbols),
        "dry_run": DRY_RUN,
    })


@app.route("/health")
def health():

    now = time.time()

    ws_info = {}

    for group_id, status in websocket_status.items():

        ws_info[str(group_id)] = {
            "status": status,
        }

    with state_lock:

        position_list = list(positions.keys())
        position_details = {}
        for sym, pos in positions.items():
            position_details[sym] = {
                "quantity": decimal_to_string(pos.get("quantity", Decimal("0"))),
                "entry_price": decimal_to_string(pos.get("entry_price", Decimal("0"))),
                "stop_order_id": pos.get("stop_order_id"),
            }

    latest_closed = {}

    for symbol in symbols:

        timestamp = last_closed_candle_time.get(
            symbol
        )

        if timestamp:

            latest_closed[symbol] = timestamp

    return jsonify({
        "status": "ok",

        "bot_started": _bot_thread_started,

        "timeframe": TIMEFRAME,

        "top_symbols": len(symbols),

        "history_loaded": len(
            candle_history
        ),

        "open_positions": position_list,

        "position_count": len(position_list),

        "position_details": position_details,

        "buying": list(
            buying_symbols
        ),

        "selling": list(
            selling_symbols
        ),

        "websocket": ws_info,

        "last_signal_time": last_signal_time,

        "last_buy_signal": last_buy_signal,

        "last_sell_signal": last_sell_signal,

        "server_time_offset_ms":
            server_time_offset_ms,

        "dry_run": DRY_RUN,

        "buy_rsi3_max": decimal_to_string(BUY_RSI3_MAX),
        "sell_rsi3_level": decimal_to_string(SELL_RSI3_LEVEL),
        "stop_loss_percent": decimal_to_string(STOP_LOSS_PERCENT),
        "live_rsi_sell": LIVE_RSI_SELL,

        "live_rsi_buy": True,
        "live_buy_rsi50_min": decimal_to_string(BUY_RSI50_MIN),

        "debug_mode": DEBUG_MODE,
        "cache_dir": CACHE_DIR,
        "exchange_cache_age_sec": None if cache_age(EXCHANGE_CACHE_FILE) == float("inf") else round(cache_age(EXCHANGE_CACHE_FILE), 1),
        "symbol_cache_age_sec": None if cache_age(SYMBOL_CACHE_FILE) == float("inf") else round(cache_age(SYMBOL_CACHE_FILE), 1),
        "history_cache_age_sec": None if cache_age(HISTORY_CACHE_FILE) == float("inf") else round(cache_age(HISTORY_CACHE_FILE), 1),

        "time": time.time(),
    })


# ============================================================
# IMPORTANT FOR GUNICORN
# ============================================================

log.info(
    "GUNICORN IMPORT DETECTED"
)

ensure_bot_started = start_background_bot

ensure_bot_started()


# ============================================================
# DIRECT PYTHON START
# ============================================================

if __name__ == "__main__":

    port = int(
        os.getenv(
            "PORT",
            "10000",
        )
    )

    app.run(
        host="0.0.0.0",
        port=port,
    )
