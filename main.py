import os
import time
import json
import hmac
import hashlib
import logging
import threading
import signal
import atexit
from decimal import Decimal, ROUND_DOWN, InvalidOperation
from collections import deque
from concurrent.futures import ThreadPoolExecutor

import requests
import websocket

from flask import Flask, jsonify


# ============================================================
# CONFIG
# ============================================================

BINANCE_API_KEY = os.getenv("BINANCE_API_KEY", "")
BINANCE_API_SECRET = os.getenv("BINANCE_API_SECRET", "")

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

STOP_LOSS_PERCENT = Decimal("1")

# Live BUY / SELL
LIVE_RSI_BUY = True
LIVE_RSI_SELL = True

HISTORY_CANDLES = 200

# WebSocket
WS_PING_INTERVAL = 20
WS_PING_TIMEOUT = 10

# REST
REQUEST_TIMEOUT = 15
MAX_RETRIES = 3

# Startup load
STARTUP_KLINE_DELAY = Decimal("0.50")

# Thread pool
WORKER_THREADS = 4

# Debug
DRY_RUN = os.getenv("DRY_RUN", "false").lower() == "true"
DEBUG_MODE = os.getenv("DEBUG_MODE", "true").lower() == "true"

# Near signal logging
NEAR_RSI3_LEVEL = Decimal("10")
NEAR_RSI50_LEVEL = Decimal("48")

# Cache
CACHE_DIR = ".bot_cache"

EXCHANGE_INFO_CACHE = os.path.join(
    CACHE_DIR,
    "exchange_info.json"
)

TOP_SYMBOLS_CACHE = os.path.join(
    CACHE_DIR,
    "top_symbols.json"
)

HISTORY_CACHE = os.path.join(
    CACHE_DIR,
    "history.json"
)

POSITIONS_FILE = os.path.join(
    CACHE_DIR,
    "positions.json"
)

EXCHANGE_INFO_CACHE_TTL = 6 * 60 * 60
TOP_SYMBOLS_CACHE_TTL = 30 * 60
HISTORY_CACHE_TTL = 30 * 60

# Binance rate limit cooldown
RATE_LIMIT_418_COOLDOWN = 300
RATE_LIMIT_429_COOLDOWN = 30

# Server time synchronization
SERVER_TIME_SYNC_INTERVAL = 1800

# History refresh
HISTORY_REFRESH_INTERVAL = 900

# WebSocket reconnect
WS_INITIAL_RECONNECT_DELAY = 2
WS_MAX_RECONNECT_DELAY = 60

# Exclude
EXCLUDED_BASE_ASSETS = {
    "BTC",
    "ETH",
}

STABLE_BASE_ASSETS = {
    "USDT",
    "USDC",
    "FDUSD",
    "BUSD",
    "TUSD",
    "USDP",
    "DAI",
    "EUR",
    "GBP",
    "AUD",
    "TRY",
    "BRL",
    "RUB",
    "UAH",
    "BIDR",
    "IDRT",
    "NGN",
    "ZAR",
    "PLN",
    "RON",
    "ARS",
    "MXN",
    "COP",
    "JPY",
    "CAD",
    "CHF",
    "AED",
    "HKD",
    "DKK",
    "NOK",
    "SEK",
}

# ============================================================
# LOGGING
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

logger = logging.getLogger("RSI_BOT")


# ============================================================
# FLASK
# ============================================================

app = Flask(__name__)


# ============================================================
# GLOBAL STATE
# ============================================================

stop_event = threading.Event()

state_lock = threading.RLock()
history_lock = threading.RLock()
position_lock = threading.RLock()

positions = {}

buying_symbols = set()
selling_symbols = set()

# Prevent multiple live entries/exits on same candle
live_buy_triggered_candle = {}
live_sell_triggered_candle = {}

# Current live candle RSI state
live_rsi_state = {}

# WebSocket references
websocket_apps = []

# Bot thread
_bot_thread = None
_bot_thread_started = False
_bot_start_lock = threading.Lock()

# Runtime
symbols = []
symbol_info = {}

server_time_offset_ms = 0

last_history_refresh = 0
last_server_time_sync = 0

last_ws_message_time = 0

websocket_status = {
    "group_1": "starting",
    "group_2": "starting",
    "group_3": "starting",
}

rate_limit_until = 0


# ============================================================
# EXCEPTIONS
# ============================================================

class BinanceRateLimitError(Exception):

    def __init__(self, status_code, message="Rate limited"):
        self.status_code = status_code
        super().__init__(message)


# ============================================================
# DIRECTORY
# ============================================================

def ensure_cache_dir():
    os.makedirs(CACHE_DIR, exist_ok=True)


ensure_cache_dir()


# ============================================================
# DECIMAL HELPERS
# ============================================================

def D(value, default=Decimal("0")):
    try:
        if value is None:
            return default

        if isinstance(value, Decimal):
            return value

        return Decimal(str(value))

    except (InvalidOperation, ValueError, TypeError):
        return default


def decimal_to_string(value):
    value = D(value)

    text = format(value, "f")

    if "." in text:
        text = text.rstrip("0").rstrip(".")

    return text or "0"


# ============================================================
# FILE HELPERS
# ============================================================

def load_json_file(path):

    try:

        if not os.path.exists(path):
            return None

        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)

    except Exception as e:

        logger.warning(
            "Could not load cache %s: %s",
            path,
            e
        )

        return None


def save_json_file(path, data):

    try:

        ensure_cache_dir()

        temp_path = path + ".tmp"

        with open(temp_path, "w", encoding="utf-8") as f:
            json.dump(
                data,
                f,
                ensure_ascii=False,
                separators=(",", ":"),
            )

        os.replace(temp_path, path)

        return True

    except Exception as e:

        logger.error(
            "Could not save %s: %s",
            path,
            e
        )

        return False


def cache_is_fresh(path, ttl):

    try:

        if not os.path.exists(path):
            return False

        age = time.time() - os.path.getmtime(path)

        return age < ttl

    except Exception:
        return False


# ============================================================
# RATE LIMIT
# ============================================================

def activate_rate_limit(status_code):

    global rate_limit_until

    if status_code == 418:
        cooldown = RATE_LIMIT_418_COOLDOWN

    elif status_code == 429:
        cooldown = RATE_LIMIT_429_COOLDOWN

    else:
        cooldown = 10

    rate_limit_until = max(
        rate_limit_until,
        time.time() + cooldown
    )

    logger.warning(
        "Binance rate limit %s. Cooling down for %ss.",
        status_code,
        cooldown
    )


def rate_limit_wait():

    remaining = rate_limit_until - time.time()

    if remaining > 0:

        logger.warning(
            "REST cooldown active: %.1fs remaining",
            remaining
        )

        stop_event.wait(remaining)


# ============================================================
# HTTP SESSION
# ============================================================

session = requests.Session()

session.headers.update({
    "X-MBX-APIKEY": BINANCE_API_KEY,
    "User-Agent": "RSI3-RSI50-Spot-Bot/1.0",
})


# ============================================================
# PUBLIC REQUEST
# ============================================================

def public_get(endpoint, params=None):

    rate_limit_wait()

    url = BASE_URL + endpoint

    last_error = None

    for attempt in range(MAX_RETRIES):

        try:

            response = session.get(
                url,
                params=params,
                timeout=REQUEST_TIMEOUT,
            )

            if response.status_code in (418, 429):

                activate_rate_limit(
                    response.status_code
                )

                raise BinanceRateLimitError(
                    response.status_code,
                    response.text[:300],
                )

            if response.status_code >= 500:

                last_error = Exception(
                    f"HTTP {response.status_code}"
                )

                time.sleep(
                    min(2 ** attempt, 8)
                )

                continue

            response.raise_for_status()

            return response.json()

        except BinanceRateLimitError:
            raise

        except Exception as e:

            last_error = e

            if attempt < MAX_RETRIES - 1:

                time.sleep(
                    min(1.5 ** attempt, 5)
                )

    raise last_error


# ============================================================
# SIGNED REQUEST
# ============================================================

def signed_request(
    method,
    endpoint,
    params=None,
):

    if not BINANCE_API_KEY or not BINANCE_API_SECRET:

        raise RuntimeError(
            "BINANCE_API_KEY / BINANCE_API_SECRET missing"
        )

    rate_limit_wait()

    if params is None:
        params = {}

    params = dict(params)

    params["timestamp"] = int(
        time.time() * 1000
    ) + server_time_offset_ms

    params["recvWindow"] = 10000

    query_string = "&".join(
        f"{key}={value}"
        for key, value in params.items()
    )

    signature = hmac.new(
        BINANCE_API_SECRET.encode("utf-8"),
        query_string.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()

    params["signature"] = signature

    url = BASE_URL + endpoint

    last_error = None

    for attempt in range(MAX_RETRIES):

        try:

            if method.upper() == "GET":

                response = session.get(
                    url,
                    params=params,
                    timeout=REQUEST_TIMEOUT,
                )

            elif method.upper() == "POST":

                response = session.post(
                    url,
                    params=params,
                    timeout=REQUEST_TIMEOUT,
                )

            elif method.upper() == "DELETE":

                response = session.delete(
                    url,
                    params=params,
                    timeout=REQUEST_TIMEOUT,
                )

            else:

                raise ValueError(
                    f"Unsupported method: {method}"
                )

            if response.status_code in (418, 429):

                activate_rate_limit(
                    response.status_code
                )

                raise BinanceRateLimitError(
                    response.status_code,
                    response.text[:500],
                )

            if response.status_code >= 500:

                last_error = Exception(
                    f"HTTP {response.status_code}: "
                    f"{response.text[:300]}"
                )

                time.sleep(
                    min(2 ** attempt, 8)
                )

                continue

            if not response.ok:

                raise Exception(
                    f"HTTP {response.status_code}: "
                    f"{response.text[:1000]}"
                )

            return response.json()

        except BinanceRateLimitError:
            raise

        except Exception as e:

            last_error = e

            if attempt < MAX_RETRIES - 1:

                time.sleep(
                    min(1.5 ** attempt, 5)
                )

    raise last_error


# ============================================================
# SERVER TIME
# ============================================================

def sync_server_time():

    global server_time_offset_ms
    global last_server_time_sync

    try:

        local_before = int(
            time.time() * 1000
        )

        data = public_get(
            "/api/v3/time"
        )

        local_after = int(
            time.time() * 1000
        )

        server_time = int(
            data["serverTime"]
        )

        midpoint = (
            local_before +
            local_after
        ) // 2

        server_time_offset_ms = (
            server_time - midpoint
        )

        last_server_time_sync = time.time()

        logger.info(
            "Binance server time offset: %sms",
            server_time_offset_ms
        )

        return True

    except Exception as e:

        logger.error(
            "Server time sync failed: %s",
            e
        )

        return False


# ============================================================
# RSI - WILDER / RMA
# ============================================================

def calculate_rsi_wilder(
    closes,
    period,
):

    values = [
        D(x)
        for x in closes
    ]

    if len(values) < period + 1:
        return None

    gains = []
    losses = []

    for i in range(1, len(values)):

        change = (
            values[i] -
            values[i - 1]
        )

        if change > 0:

            gains.append(change)
            losses.append(Decimal("0"))

        else:

            gains.append(Decimal("0"))
            losses.append(-change)

    avg_gain = (
        sum(gains[:period]) /
        Decimal(period)
    )

    avg_loss = (
        sum(losses[:period]) /
        Decimal(period)
    )

    def rsi_from_avg():

        if avg_loss == 0:

            if avg_gain == 0:
                return Decimal("50")

            return Decimal("100")

        rs = avg_gain / avg_loss

        return Decimal("100") - (
            Decimal("100") /
            (Decimal("1") + rs)
        )

    rsi = rsi_from_avg()

    for i in range(period, len(gains)):

        avg_gain = (
            (
                avg_gain *
                Decimal(period - 1)
            )
            + gains[i]
        ) / Decimal(period)

        avg_loss = (
            (
                avg_loss *
                Decimal(period - 1)
            )
            + losses[i]
        ) / Decimal(period)

        rsi = rsi_from_avg()

    return rsi


# ============================================================
# SYMBOL FILTER
# ============================================================

def symbol_allowed(item):

    try:

        if item.get("status") != "TRADING":
            return False

        if item.get("quoteAsset") != "USDT":
            return False

        base = item.get(
            "baseAsset",
            ""
        ).upper()

        if base in EXCLUDED_BASE_ASSETS:
            return False

        if base in STABLE_BASE_ASSETS:
            return False

        return True

    except Exception:
        return False


# ============================================================
# EXCHANGE INFO
# ============================================================

def load_exchange_info(force=False):

    global symbol_info

    if (
        not force and
        cache_is_fresh(
            EXCHANGE_INFO_CACHE,
            EXCHANGE_INFO_CACHE_TTL
        )
    ):

        data = load_json_file(
            EXCHANGE_INFO_CACHE
        )

        if data:

            symbol_info = data

            logger.info(
                "Loaded exchange info from cache: %s symbols",
                len(symbol_info)
            )

            return symbol_info

    logger.info(
        "Loading Binance exchange info..."
    )

    data = public_get(
        "/api/v3/exchangeInfo"
    )

    info = {}

    for item in data.get(
        "symbols",
        []
    ):

        if not symbol_allowed(item):
            continue

        symbol = item["symbol"]

        filters = {}

        for f in item.get(
            "filters",
            []
        ):

            filters[
                f["filterType"]
            ] = f

        info[symbol] = {
            "symbol": symbol,
            "baseAsset": item.get(
                "baseAsset"
            ),
            "quoteAsset": item.get(
                "quoteAsset"
            ),
            "status": item.get(
                "status"
            ),
            "filters": filters,
        }

    symbol_info = info

    save_json_file(
        EXCHANGE_INFO_CACHE,
        symbol_info
    )

    logger.info(
        "Exchange info loaded: %s eligible symbols",
        len(symbol_info)
    )

    return symbol_info


# ============================================================
# TOP 150 SYMBOLS
# ============================================================

def load_top_symbols(force=False):

    global symbols

    if (
        not force and
        cache_is_fresh(
            TOP_SYMBOLS_CACHE,
            TOP_SYMBOLS_CACHE_TTL
        )
    ):

        data = load_json_file(
            TOP_SYMBOLS_CACHE
        )

        if data:

            symbols = data[:TOP_SYMBOLS]

            logger.info(
                "Loaded top symbols from cache: %s",
                len(symbols)
            )

            return symbols

    logger.info(
        "Loading 24h ticker data..."
    )

    ticker_data = public_get(
        "/api/v3/ticker/24hr"
    )

    candidates = []

    for item in ticker_data:

        symbol = item.get(
            "symbol",
            ""
        )

        if symbol not in symbol_info:
            continue

        try:

            quote_volume = D(
                item.get(
                    "quoteVolume",
                    "0"
                )
            )

        except Exception:
            continue

        if quote_volume <= 0:
            continue

        candidates.append(
            (
                symbol,
                quote_volume
            )
        )

    candidates.sort(
        key=lambda x: x[1],
        reverse=True
    )

    symbols = [
        symbol
        for symbol, volume
        in candidates[:TOP_SYMBOLS]
    ]

    save_json_file(
        TOP_SYMBOLS_CACHE,
        symbols
    )

    logger.info(
        "Selected %s top USDT symbols",
        len(symbols)
    )

    if symbols:

        logger.info(
            "First symbols: %s",
            ", ".join(symbols[:15])
        )

    return symbols


# ============================================================
# KLINE HISTORY
# ============================================================

def parse_klines(data):

    result = []

    for k in data:

        try:

            result.append({
                "open_time": int(k[0]),
                "open": D(k[1]),
                "high": D(k[2]),
                "low": D(k[3]),
                "close": D(k[4]),
                "volume": D(k[5]),
                "close_time": int(k[6]),
            })

        except Exception:
            continue

    return result


def load_history_cache():

    data = load_json_file(
        HISTORY_CACHE
    )

    if not isinstance(data, dict):
        return {}

    history = {}

    for symbol, candles in data.items():

        parsed = []

        for c in candles:

            try:

                parsed.append({
                    "open_time": int(
                        c["open_time"]
                    ),
                    "open": D(c["open"]),
                    "high": D(c["high"]),
                    "low": D(c["low"]),
                    "close": D(c["close"]),
                    "volume": D(c["volume"]),
                    "close_time": int(
                        c["close_time"]
                    ),
                })

            except Exception:
                continue

        history[symbol] = deque(
            parsed[-HISTORY_CANDLES:],
            maxlen=HISTORY_CANDLES
        )

    return history


def save_history_cache(history):

    output = {}

    for symbol, candles in history.items():

        output[symbol] = []

        for c in list(candles)[-HISTORY_CANDLES:]:

            output[symbol].append({
                "open_time": c["open_time"],
                "open": decimal_to_string(
                    c["open"]
                ),
                "high": decimal_to_string(
                    c["high"]
                ),
                "low": decimal_to_string(
                    c["low"]
                ),
                "close": decimal_to_string(
                    c["close"]
                ),
                "volume": decimal_to_string(
                    c["volume"]
                ),
                "close_time": c["close_time"],
            })

    save_json_file(
        HISTORY_CACHE,
        output
    )


def fetch_symbol_history(symbol):

    data = public_get(
        "/api/v3/klines",
        {
            "symbol": symbol,
            "interval": TIMEFRAME,
            "limit": HISTORY_CANDLES,
        }
    )

    return parse_klines(data)


def load_historical_data():

    history = load_history_cache()

    logger.info(
        "Loaded history cache: %s symbols",
        len(history)
    )

    missing = []

    now_ms = int(
        time.time() * 1000
    )

    for symbol in symbols:

        candles = history.get(symbol)

        valid = (
            candles is not None
            and len(candles) >= RSI_SLOW_PERIOD + 5
        )

        if not valid:
            missing.append(symbol)
            continue

        # Remove any candle that is still open
        while candles and (
            candles[-1]["close_time"]
            >= now_ms
        ):
            candles.pop()

    if missing:

        logger.info(
            "Historical REST load required: %s symbols",
            len(missing)
        )

        for index, symbol in enumerate(
            missing,
            start=1
        ):

            if stop_event.is_set():
                break

            try:

                candles = fetch_symbol_history(
                    symbol
                )

                closed = []

                current_ms = int(
                    time.time() * 1000
                )

                for candle in candles:

                    if candle["close_time"] < current_ms:

                        closed.append(candle)

                history[symbol] = deque(
                    closed[-HISTORY_CANDLES:],
                    maxlen=HISTORY_CANDLES
                )

                if (
                    index == 1
                    or index % 25 == 0
                    or index == len(missing)
                ):

                    logger.info(
                        "Historical init: %s/%s",
                        index,
                        len(missing)
                    )

                stop_event.wait(
                    float(STARTUP_KLINE_DELAY)
                )

            except BinanceRateLimitError:
                raise

            except Exception as e:

                logger.warning(
                    "%s history failed: %s",
                    symbol,
                    e
                )

    # Ensure all selected symbols have deque
    for symbol in symbols:

        if symbol not in history:

            history[symbol] = deque(
                maxlen=HISTORY_CANDLES
            )

    save_history_cache(
        history
    )

    logger.info(
        "Historical data ready: %s/%s",
        len(history),
        len(symbols)
    )

    return history


# ============================================================
# POSITION PERSISTENCE
# ============================================================

def serialize_positions():

    result = {}

    with position_lock:

        for symbol, p in positions.items():

            result[symbol] = {
                "symbol": symbol,
                "quantity": decimal_to_string(
                    p.get("quantity", 0)
                ),
                "entry_price": decimal_to_string(
                    p.get("entry_price", 0)
                ),
                "entry_time": p.get(
                    "entry_time",
                    time.time()
                ),
                "buy_order_id": p.get(
                    "buy_order_id"
                ),
                "stop_order_id": p.get(
                    "stop_order_id"
                ),
                "stop_price": decimal_to_string(
                    p.get("stop_price", 0)
                ),
                "base_asset": p.get(
                    "base_asset"
                ),
            }

    return result


def save_positions():

    try:

        data = serialize_positions()

        save_json_file(
            POSITIONS_FILE,
            data
        )

        logger.info(
            "Positions saved: %s",
            len(data)
        )

    except Exception as e:

        logger.error(
            "Position save failed: %s",
            e
        )


def load_positions():

    global positions

    data = load_json_file(
        POSITIONS_FILE
    )

    if not isinstance(data, dict):

        logger.info(
            "No saved positions found."
        )

        return

    loaded = {}

    for symbol, p in data.items():

        try:

            loaded[symbol] = {
                "symbol": symbol,
                "quantity": D(
                    p.get("quantity")
                ),
                "entry_price": D(
                    p.get("entry_price")
                ),
                "entry_time": p.get(
                    "entry_time",
                    time.time()
                ),
                "buy_order_id": p.get(
                    "buy_order_id"
                ),
                "stop_order_id": p.get(
                    "stop_order_id"
                ),
                "stop_price": D(
                    p.get("stop_price")
                ),
                "base_asset": p.get(
                    "base_asset"
                ),
            }

        except Exception as e:

            logger.warning(
                "Could not recover %s: %s",
                symbol,
                e
            )

    with position_lock:

        positions.update(
            loaded
        )

    logger.info(
        "Recovered saved positions: %s",
        len(loaded)
    )


# ============================================================
# ACCOUNT
# ============================================================

def get_account():

    return signed_request(
        "GET",
        "/api/v3/account"
    )


def get_free_balances():

    account = get_account()

    balances = {}

    for b in account.get(
        "balances",
        []
    ):

        balances[
            b["asset"]
        ] = D(
            b["free"]
        )

    return balances


def verify_account():

    account = get_account()

    logger.info(
        "Binance account connection OK"
    )

    return account


# ============================================================
# SYMBOL FILTER HELPERS
# ============================================================

def get_filter(symbol, filter_type):

    info = symbol_info.get(
        symbol
    )

    if not info:
        return None

    return info.get(
        "filters",
        {}
    ).get(
        filter_type
    )


def get_step_size(symbol):

    f = get_filter(
        symbol,
        "LOT_SIZE"
    )

    if not f:
        return Decimal("0.000001")

    return D(
        f.get("stepSize"),
        Decimal("0.000001")
    )


def get_min_qty(symbol):

    f = get_filter(
        symbol,
        "LOT_SIZE"
    )

    if not f:
        return Decimal("0")

    return D(
        f.get("minQty")
    )


def get_tick_size(symbol):

    f = get_filter(
        symbol,
        "PRICE_FILTER"
    )

    if not f:
        return Decimal("0.00000001")

    return D(
        f.get("tickSize"),
        Decimal("0.00000001")
    )


def floor_to_step(value, step):

    value = D(value)
    step = D(step)

    if step <= 0:
        return value

    return (
        value / step
    ).to_integral_value(
        rounding=ROUND_DOWN
    ) * step


def floor_quantity(symbol, quantity):

    step = get_step_size(symbol)

    qty = floor_to_step(
        quantity,
        step
    )

    min_qty = get_min_qty(symbol)

    if qty < min_qty:
        return Decimal("0")

    return qty


def floor_price(symbol, price):

    tick = get_tick_size(symbol)

    return floor_to_step(
        price,
        tick
    )


# ============================================================
# ORDER HELPERS
# ============================================================

def get_order(
    symbol,
    order_id=None,
    orig_client_order_id=None
):

    params = {
        "symbol": symbol,
    }

    if order_id is not None:
        params["orderId"] = int(
            order_id
        )

    if orig_client_order_id:
        params[
            "origClientOrderId"
        ] = orig_client_order_id

    return signed_request(
        "GET",
        "/api/v3/order",
        params
    )


def cancel_order(
    symbol,
    order_id=None,
    orig_client_order_id=None
):

    params = {
        "symbol": symbol,
    }

    if order_id is not None:
        params["orderId"] = int(
            order_id
        )

    if orig_client_order_id:
        params[
            "origClientOrderId"
        ] = orig_client_order_id

    return signed_request(
        "DELETE",
        "/api/v3/order",
        params
    )


def market_buy(symbol):

    client_id = (
        "RSIBUY_" +
        str(int(time.time() * 1000))
    )

    params = {
        "symbol": symbol,
        "side": "BUY",
        "type": "MARKET",
        "quoteOrderQty": decimal_to_string(
            BUY_USDT
        ),
        "newOrderRespType": "FULL",
        "newClientOrderId": client_id,
    }

    return signed_request(
        "POST",
        "/api/v3/order",
        params
    )


def market_sell(
    symbol,
    quantity
):

    qty = floor_quantity(
        symbol,
        quantity
    )

    if qty <= 0:
        raise Exception(
            f"{symbol}: sell quantity below minimum"
        )

    client_id = (
        "RSISELL_" +
        str(int(time.time() * 1000))
    )

    params = {
        "symbol": symbol,
        "side": "SELL",
        "type": "MARKET",
        "quantity": decimal_to_string(
            qty
        ),
        "newOrderRespType": "FULL",
        "newClientOrderId": client_id,
    }

    return signed_request(
        "POST",
        "/api/v3/order",
        params
    )


def place_stop_loss(
    symbol,
    quantity,
    stop_price
):

    qty = floor_quantity(
        symbol,
        quantity
    )

    if qty <= 0:
        raise Exception(
            f"{symbol}: stop quantity below minimum"
        )

    stop_price = floor_price(
        symbol,
        stop_price
    )

    client_id = (
        "RSISL_" +
        str(int(time.time() * 1000))
    )

    params = {
        "symbol": symbol,
        "side": "SELL",
        "type": "STOP_LOSS",
        "quantity": decimal_to_string(
            qty
        ),
        "stopPrice": decimal_to_string(
            stop_price
        ),
        "newOrderRespType": "RESULT",
        "newClientOrderId": client_id,
    }

    return signed_request(
        "POST",
        "/api/v3/order",
        params
    )


# ============================================================
# ORDER PRICE / QUANTITY
# ============================================================

def extract_executed_qty(order):

    qty = D(
        order.get(
            "executedQty"
        )
    )

    return qty


def extract_avg_price(order):

    executed_qty = D(
        order.get(
            "executedQty"
        )
    )

    cumulative_quote = D(
        order.get(
            "cummulativeQuoteQty"
        )
    )

    if (
        executed_qty > 0
        and cumulative_quote > 0
    ):

        return (
            cumulative_quote /
            executed_qty
        )

    fills = order.get(
        "fills",
        []
    )

    total_qty = Decimal("0")
    total_quote = Decimal("0")

    for fill in fills:

        q = D(
            fill.get("qty")
        )

        p = D(
            fill.get("price")
        )

        total_qty += q
        total_quote += (
            q * p
        )

    if total_qty > 0:
        return (
            total_quote /
            total_qty
        )

    return Decimal("0")


def extract_net_base_quantity(
    order,
    symbol
):

    qty = extract_executed_qty(
        order
    )

    base_asset = symbol_info.get(
        symbol,
        {}
    ).get(
        "baseAsset"
    )

    commission_total = Decimal("0")

    for fill in order.get(
        "fills",
        []
    ):

        if (
            fill.get(
                "commissionAsset"
            ) == base_asset
        ):

            commission_total += D(
                fill.get(
                    "commission"
                )
            )

    net_qty = qty - commission_total

    if net_qty < 0:
        net_qty = Decimal("0")

    return floor_quantity(
        symbol,
        net_qty
    )


# ============================================================
# BUY
# ============================================================

def execute_buy(
    symbol,
    reason="signal"
):

    if stop_event.is_set():
        return False

    with position_lock:

        if symbol in positions:
            return False

        if symbol in buying_symbols:
            return False

        buying_symbols.add(symbol)

    try:

        logger.warning(
            "🔥 BUY SIGNAL | %s | reason=%s",
            symbol,
            reason
        )

        if DRY_RUN:

            with position_lock:

                positions[symbol] = {
                    "symbol": symbol,
                    "quantity": BUY_USDT,
                    "entry_price": Decimal("0"),
                    "entry_time": time.time(),
                    "buy_order_id": "DRY_RUN",
                    "stop_order_id": None,
                    "stop_price": Decimal("0"),
                    "base_asset": symbol_info.get(
                        symbol,
                        {}
                    ).get(
                        "baseAsset"
                    ),
                }

            save_positions()

            logger.warning(
                "DRY RUN BUY | %s",
                symbol
            )

            return True

        order = market_buy(
            symbol
        )

        executed_qty = extract_net_base_quantity(
            order,
            symbol
        )

        entry_price = extract_avg_price(
            order
        )

        if executed_qty <= 0:

            logger.error(
                "%s BUY returned zero quantity: %s",
                symbol,
                order
            )

            return False

        if entry_price <= 0:

            logger.error(
                "%s BUY has invalid entry price",
                symbol
            )

            return False

        stop_price = (
            entry_price *
            (
                Decimal("1") -
                STOP_LOSS_PERCENT /
                Decimal("100")
            )
        )

        stop_price = floor_price(
            symbol,
            stop_price
        )

        position = {
            "symbol": symbol,
            "quantity": executed_qty,
            "entry_price": entry_price,
            "entry_time": time.time(),
            "buy_order_id": order.get(
                "orderId"
            ),
            "stop_order_id": None,
            "stop_price": stop_price,
            "base_asset": symbol_info.get(
                symbol,
                {}
            ).get(
                "baseAsset"
            ),
        }

        # Save position immediately after BUY
        with position_lock:

            positions[symbol] = position

        save_positions()

        logger.warning(
            "✅ BUY EXECUTED | %s | qty=%s | entry=%s | SL=%s",
            symbol,
            decimal_to_string(
                executed_qty
            ),
            decimal_to_string(
                entry_price
            ),
            decimal_to_string(
                stop_price
            )
        )

        # Place exchange-side stop
        try:

            stop_order = place_stop_loss(
                symbol,
                executed_qty,
                stop_price
            )

            stop_order_id = stop_order.get(
                "orderId"
            )

            with position_lock:

                if symbol in positions:

                    positions[symbol][
                        "stop_order_id"
                    ] = stop_order_id

            save_positions()

            logger.info(
                "🛡 STOP LOSS PLACED | %s | stop=%s | order=%s",
                symbol,
                decimal_to_string(
                    stop_price
                ),
                stop_order_id
            )

        except Exception as e:

            logger.error(
                "⚠️ Exchange stop failed | %s | %s",
                symbol,
                e
            )

            # Software fallback remains active

        return True

    except BinanceRateLimitError:
        raise

    except Exception as e:

        logger.error(
            "BUY FAILED | %s | %s",
            symbol,
            e
        )

        return False

    finally:

        with position_lock:
            buying_symbols.discard(
                symbol
            )


# ============================================================
# SELL
# ============================================================

def execute_sell(
    symbol,
    reason="signal"
):

    with position_lock:

        if symbol not in positions:
            return False

        if symbol in selling_symbols:
            return False

        selling_symbols.add(symbol)

        position = dict(
            positions[symbol]
        )

    try:

        logger.warning(
            "🔥 SELL SIGNAL | %s | reason=%s",
            symbol,
            reason
        )

        stop_order_id = position.get(
            "stop_order_id"
        )

        # Cancel exchange stop first
        if (
            not DRY_RUN
            and stop_order_id
        ):

            try:

                cancel_order(
                    symbol,
                    order_id=stop_order_id
                )

                logger.info(
                    "Protective stop cancelled | %s | order=%s",
                    symbol,
                    stop_order_id
                )

            except Exception as e:

                logger.warning(
                    "Could not cancel stop | %s | %s",
                    symbol,
                    e
                )

                # Check whether stop already filled
                try:

                    status = get_order(
                        symbol,
                        order_id=stop_order_id
                    )

                    if status.get(
                        "status"
                    ) == "FILLED":

                        logger.warning(
                            "Stop already FILLED | %s",
                            symbol
                        )

                        with position_lock:
                            positions.pop(
                                symbol,
                                None
                            )

                        save_positions()

                        return True

                except Exception as check_error:

                    logger.warning(
                        "Stop status check failed | %s | %s",
                        symbol,
                        check_error
                    )

        if DRY_RUN:

            with position_lock:

                positions.pop(
                    symbol,
                    None
                )

            save_positions()

            logger.warning(
                "DRY RUN SELL | %s",
                symbol
            )

            return True

        # Get current free balance
        balances = get_free_balances()

        base_asset = position.get(
            "base_asset"
        )

        if not base_asset:

            base_asset = symbol_info.get(
                symbol,
                {}
            ).get(
                "baseAsset"
            )

        free_qty = D(
            balances.get(
                base_asset,
                Decimal("0")
            )
        )

        # Small safety buffer
        sell_qty = floor_quantity(
            symbol,
            free_qty
        )

        if sell_qty <= 0:

            logger.warning(
                "No sellable balance | %s | asset=%s",
                symbol,
                base_asset
            )

            with position_lock:

                positions.pop(
                    symbol,
                    None
                )

            save_positions()

            return False

        order = market_sell(
            symbol,
            sell_qty
        )

        status = order.get(
            "status"
        )

        logger.warning(
            "✅ SELL EXECUTED | %s | qty=%s | status=%s | reason=%s",
            symbol,
            decimal_to_string(
                sell_qty
            ),
            status,
            reason
        )

        with position_lock:

            positions.pop(
                symbol,
                None
            )

        save_positions()

        return True

    except BinanceRateLimitError:
        raise

    except Exception as e:

        logger.error(
            "SELL FAILED | %s | %s",
            symbol,
            e
        )

        return False

    finally:

        with position_lock:
            selling_symbols.discard(
                symbol
            )


# ============================================================
# SOFTWARE STOP LOSS
# ============================================================

def process_live_stop_loss(
    symbol,
    candle
):

    with position_lock:

        position = positions.get(
            symbol
        )

        if not position:
            return False

        stop_order_id = position.get(
            "stop_order_id"
        )

        stop_price = D(
            position.get(
                "stop_price"
            )
        )

    # If exchange stop exists, Binance should handle it.
    # Software fallback is only used if no exchange stop exists.
    if stop_order_id:
        return False

    if stop_price <= 0:
        return False

    low = D(
        candle.get(
            "low"
        )
    )

    if low <= stop_price:

        logger.warning(
            "🛑 SOFTWARE STOP LOSS | %s | low=%s <= stop=%s",
            symbol,
            decimal_to_string(low),
            decimal_to_string(stop_price)
        )

        return execute_sell(
            symbol,
            reason="software_stop_loss"
        )

    return False


# ============================================================
# INDICATOR CALCULATION
# ============================================================

def get_closed_indicators(
    history,
    symbol
):

    candles = history.get(
        symbol
    )

    if not candles:
        return None

    closes = [
        c["close"]
        for c in candles
    ]

    rsi3 = calculate_rsi_wilder(
        closes,
        RSI_FAST_PERIOD
    )

    rsi50 = calculate_rsi_wilder(
        closes,
        RSI_SLOW_PERIOD
    )

    if (
        rsi3 is None
        or rsi50 is None
    ):
        return None

    return {
        "rsi3": rsi3,
        "rsi50": rsi50,
    }


def get_live_indicators(
    history,
    symbol,
    live_close
):

    candles = history.get(
        symbol
    )

    if not candles:
        return None

    closes = [
        c["close"]
        for c in candles
    ]

    closes.append(
        D(live_close)
    )

    rsi3 = calculate_rsi_wilder(
        closes,
        RSI_FAST_PERIOD
    )

    rsi50 = calculate_rsi_wilder(
        closes,
        RSI_SLOW_PERIOD
    )

    if (
        rsi3 is None
        or rsi50 is None
    ):
        return None

    return {
        "rsi3": rsi3,
        "rsi50": rsi50,
    }


# ============================================================
# CLOSED CANDLE PROCESSING
# ============================================================

def process_closed_candle(
    symbol,
    candle,
    history
):

    indicators = get_closed_indicators(
        history,
        symbol
    )

    if not indicators:
        return

    rsi3 = indicators["rsi3"]
    rsi50 = indicators["rsi50"]

    previous_rsi3 = None

    candles = history.get(
        symbol
    )

    if candles and len(candles) >= 2:

        previous_closes = [
            c["close"]
            for c in list(candles)[:-1]
        ]

        previous_rsi3 = calculate_rsi_wilder(
            previous_closes,
            RSI_FAST_PERIOD
        )

    buy_signal = (
        rsi50 > BUY_RSI50_MIN
        and
        rsi3 < BUY_RSI3_MAX
    )

    sell_signal = (
        previous_rsi3 is not None
        and
        previous_rsi3 <= SELL_RSI3_LEVEL
        and
        rsi3 > SELL_RSI3_LEVEL
    )

    has_position = False

    with position_lock:

        has_position = (
            symbol in positions
        )

    logger.info(
        "%s | CLOSED 5m | RSI50=%s | RSI3 %s -> %s | BUY=%s | SELL=%s | POSITION=%s",
        symbol,
        decimal_to_string(
            rsi50
        ),
        decimal_to_string(
            previous_rsi3
        ) if previous_rsi3 is not None else "N/A",
        decimal_to_string(
            rsi3
        ),
        buy_signal,
        sell_signal,
        has_position
    )

    # Closed candle BUY
    if buy_signal and not has_position:

        execute_buy(
            symbol,
            reason="closed_candle_rsi"
        )

    # Closed candle SELL
    elif sell_signal and has_position:

        execute_sell(
            symbol,
            reason="closed_candle_rsi3_cross"
        )


# ============================================================
# LIVE BUY
# ============================================================

def process_live_buy(
    symbol,
    candle,
    history
):

    if not LIVE_RSI_BUY:
        return False

    if candle.get("closed"):
        return False

    candle_open_time = candle[
        "open_time"
    ]

    # Only one BUY trigger per candle
    with state_lock:

        if (
            live_buy_triggered_candle.get(
                symbol
            )
            == candle_open_time
        ):
            return False

    indicators = get_live_indicators(
        history,
        symbol,
        candle["close"]
    )

    if not indicators:
        return False

    rsi3 = indicators["rsi3"]
    rsi50 = indicators["rsi50"]

    buy_signal = (
        rsi50 > BUY_RSI50_MIN
        and
        rsi3 < BUY_RSI3_MAX
    )

    if not buy_signal:
        return False

    with position_lock:

        if symbol in positions:
            return False

        if symbol in buying_symbols:
            return False

    with state_lock:

        live_buy_triggered_candle[
            symbol
        ] = candle_open_time

    logger.warning(
        "🔥🔥 LIVE BUY CONDITIONS TRUE 🔥🔥 | %s | RSI50=%s > %s | RSI3=%s < %s",
        symbol,
        decimal_to_string(rsi50),
        decimal_to_string(BUY_RSI50_MIN),
        decimal_to_string(rsi3),
        decimal_to_string(BUY_RSI3_MAX)
    )

    return execute_buy(
        symbol,
        reason="live_rsi"
    )


# ============================================================
# LIVE SELL CROSS
# ============================================================

def process_live_sell(
    symbol,
    candle,
    history
):

    if not LIVE_RSI_SELL:
        return False

    if candle.get("closed"):
        return False

    with position_lock:

        if symbol not in positions:
            return False

    candle_open_time = candle[
        "open_time"
    ]

    with state_lock:

        if (
            live_sell_triggered_candle.get(
                symbol
            )
            == candle_open_time
        ):
            return False

    indicators = get_live_indicators(
        history,
        symbol,
        candle["close"]
    )

    if not indicators:
        return False

    current_rsi3 = indicators[
        "rsi3"
    ]

    # Current live RSI state
    with state_lock:

        state = live_rsi_state.get(
            symbol
        )

        if (
            state is None
            or state.get(
                "candle_open_time"
            ) != candle_open_time
        ):

            # First tick of this candle:
            # use previous closed RSI as baseline
            candles = history.get(
                symbol
            )

            previous_rsi3 = None

            if candles:

                closes = [
                    c["close"]
                    for c in candles
                ]

                previous_rsi3 = calculate_rsi_wilder(
                    closes,
                    RSI_FAST_PERIOD
                )

            live_rsi_state[
                symbol
            ] = {
                "candle_open_time":
                    candle_open_time,
                "last_rsi3":
                    previous_rsi3,
            }

            previous_live_rsi3 = (
                previous_rsi3
            )

        else:

            previous_live_rsi3 = state.get(
                "last_rsi3"
            )

    # Detect actual live cross
    cross_up = (
        previous_live_rsi3 is not None
        and
        previous_live_rsi3 <= SELL_RSI3_LEVEL
        and
        current_rsi3 > SELL_RSI3_LEVEL
    )

    # Update current live RSI
    with state_lock:

        live_rsi_state[
            symbol
        ] = {
            "candle_open_time":
                candle_open_time,
            "last_rsi3":
                current_rsi3,
        }

    if not cross_up:
        return False

    with state_lock:

        live_sell_triggered_candle[
            symbol
        ] = candle_open_time

    logger.warning(
        "🔥🔥 LIVE SELL CROSS 🔥🔥 | %s | RSI3 %s -> %s | CROSS ABOVE %s",
        symbol,
        decimal_to_string(
            previous_live_rsi3
        ),
        decimal_to_string(
            current_rsi3
        ),
        decimal_to_string(
            SELL_RSI3_LEVEL
        )
    )

    return execute_sell(
        symbol,
        reason="live_rsi3_cross_above_80"
    )


# ============================================================
# LIVE KLINE PROCESS
# ============================================================

def process_live_kline(
    symbol,
    candle,
    history
):

    if stop_event.is_set():
        return

    # Software stop first
    if not candle.get("closed"):

        if process_live_stop_loss(
            symbol,
            candle
        ):
            return

    # Live BUY
    if not candle.get("closed"):

        if process_live_buy(
            symbol,
            candle,
            history
        ):

            return

    # Live SELL
    if not candle.get("closed"):

        process_live_sell(
            symbol,
            candle,
            history
        )


# ============================================================
# WEBSOCKET MESSAGE
# ============================================================

def parse_ws_kline(message):

    try:

        payload = json.loads(
            message
        )

        data = payload.get(
            "data",
            payload
        )

        if data.get(
            "e"
        ) != "kline":
            return None

        k = data.get(
            "k"
        )

        if not k:
            return None

        symbol = k[
            "s"
        ].upper()

        candle = {
            "open_time": int(
                k["t"]
            ),
            "close_time": int(
                k["T"]
            ),
            "open": D(
                k["o"]
            ),
            "high": D(
                k["h"]
            ),
            "low": D(
                k["l"]
            ),
            "close": D(
                k["c"]
            ),
            "volume": D(
                k["v"]
            ),
            "closed": bool(
                k["x"]
            ),
        }

        return symbol, candle

    except Exception as e:

        logger.debug(
            "WS parse error: %s",
            e
        )

        return None


# ============================================================
# CLOSED CANDLE HISTORY UPDATE
# ============================================================

def append_closed_candle(
    symbol,
    candle,
    history
):

    with history_lock:

        if symbol not in history:

            history[symbol] = deque(
                maxlen=HISTORY_CANDLES
            )

        candles = history[
            symbol
        ]

        # Avoid duplicate candle
        if candles:

            last_open = candles[-1][
                "open_time"
            ]

            if (
                candle["open_time"]
                < last_open
            ):
                return False

            if (
                candle["open_time"]
                == last_open
            ):

                candles[-1] = {
                    **candle
                }

                return False

        candles.append({
            **candle
        })

    return True


# ============================================================
# WEBSOCKET CALLBACKS
# ============================================================

def ws_on_message(
    ws,
    message,
    history,
    group_name
):

    global last_ws_message_time

    last_ws_message_time = time.time()

    parsed = parse_ws_kline(
        message
    )

    if not parsed:
        return

    symbol, candle = parsed

    try:

        if candle["closed"]:

            changed = append_closed_candle(
                symbol,
                candle,
                history
            )

            if changed:

                process_closed_candle(
                    symbol,
                    candle,
                    history
                )

        else:

            process_live_kline(
                symbol,
                candle,
                history
            )

    except BinanceRateLimitError:

        raise

    except Exception as e:

        logger.exception(
            "Kline processing error | %s | %s",
            symbol,
            e
        )


def ws_on_error(
    ws,
    error,
    group_name
):

    logger.warning(
        "WebSocket error | %s | %s",
        group_name,
        error
    )


def ws_on_close(
    ws,
    close_status_code,
    close_msg,
    group_name
):

    websocket_status[
        group_name
    ] = "closed"

    logger.warning(
        "WebSocket closed | %s | code=%s | msg=%s",
        group_name,
        close_status_code,
        close_msg
    )


def ws_on_open(
    ws,
    group_name
):

    websocket_status[
        group_name
    ] = "connected"

    logger.info(
        "WebSocket connected | %s",
        group_name
    )


# ============================================================
# WEBSOCKET GROUP
# ============================================================

def websocket_group_worker(
    group_symbols,
    group_number,
    history
):

    group_name = (
        f"group_{group_number}"
    )

    streams = "/".join(
        f"{symbol.lower()}@kline_{TIMEFRAME}"
        for symbol in group_symbols
    )

    url = (
        WS_BASE_URL +
        streams
    )

    reconnect_delay = (
        WS_INITIAL_RECONNECT_DELAY
    )

    while not stop_event.is_set():

        ws_app = None

        try:

            websocket_status[
                group_name
            ] = "connecting"

            ws_app = websocket.WebSocketApp(
                url,
                on_open=lambda ws:
                    ws_on_open(
                        ws,
                        group_name
                    ),
                on_message=lambda ws, msg:
                    ws_on_message(
                        ws,
                        msg,
                        history,
                        group_name
                    ),
                on_error=lambda ws, error:
                    ws_on_error(
                        ws,
                        error,
                        group_name
                    ),
                on_close=lambda ws, code, msg:
                    ws_on_close(
                        ws,
                        code,
                        msg,
                        group_name
                    ),
            )

            with state_lock:

                websocket_apps.append(
                    ws_app
                )

            logger.info(
                "Starting WebSocket %s | symbols=%s",
                group_name,
                len(group_symbols)
            )

            ws_app.run_forever(
                ping_interval=WS_PING_INTERVAL,
                ping_timeout=WS_PING_TIMEOUT,
                skip_utf8_validation=True,
            )

            # If connection was successful and closed normally,
            # reset reconnect delay.
            if not stop_event.is_set():

                reconnect_delay = (
                    WS_INITIAL_RECONNECT_DELAY
                )

        except Exception as e:

            logger.exception(
                "WebSocket worker crashed | %s | %s",
                group_name,
                e
            )

        finally:

            with state_lock:

                try:
                    websocket_apps.remove(
                        ws_app
                    )
                except ValueError:
                    pass

            websocket_status[
                group_name
            ] = "reconnecting"

        if stop_event.is_set():
            break

        logger.warning(
            "WebSocket %s reconnecting in %ss",
            group_name,
            reconnect_delay
        )

        stop_event.wait(
            reconnect_delay
        )

        reconnect_delay = min(
            reconnect_delay * 2,
            WS_MAX_RECONNECT_DELAY
        )


# ============================================================
# CLOSE ALL WEBSOCKETS
# ============================================================

def close_all_websockets():

    with state_lock:

        apps = list(
            websocket_apps
        )

    for ws in apps:

        try:

            ws.close()

        except Exception:
            pass


# ============================================================
# CANDLE SUMMARY
# ============================================================

def candle_summary(
    history
):

    if not DEBUG_MODE:
        return

    try:

        processed = 0
        rsi3_low = 0
        rsi50_high = 0
        buy_count = 0
        sell_count = 0

        for symbol in symbols:

            indicators = get_closed_indicators(
                history,
                symbol
            )

            if not indicators:
                continue

            processed += 1

            rsi3 = indicators[
                "rsi3"
            ]

            rsi50 = indicators[
                "rsi50"
            ]

            if rsi3 < BUY_RSI3_MAX:
                rsi3_low += 1

            if rsi50 > BUY_RSI50_MIN:
                rsi50_high += 1

            if (
                rsi50 > BUY_RSI50_MIN
                and
                rsi3 < BUY_RSI3_MAX
            ):
                buy_count += 1

            with position_lock:

                has_position = (
                    symbol in positions
                )

            if has_position:

                # Summary sell count is only an informational
                # closed-candle cross count.
                candles = history.get(
                    symbol
                )

                if candles and len(candles) >= 2:

                    closes = [
                        c["close"]
                        for c in candles
                    ]

                    previous_rsi3 = calculate_rsi_wilder(
                        closes[:-1],
                        RSI_FAST_PERIOD
                    )

                    if (
                        previous_rsi3 is not None
                        and
                        previous_rsi3 <= SELL_RSI3_LEVEL
                        and
                        rsi3 > SELL_RSI3_LEVEL
                    ):
                        sell_count += 1

        logger.warning(
            "CANDLE SUMMARY | processed=%s/%s | RSI3<%s=%s | RSI50>%s=%s | BUY=%s | SELL=%s",
            processed,
            len(symbols),
            decimal_to_string(
                BUY_RSI3_MAX
            ),
            rsi3_low,
            decimal_to_string(
                BUY_RSI50_MIN
            ),
            rsi50_high,
            buy_count,
            sell_count
        )

    except Exception as e:

        logger.warning(
            "Candle summary failed: %s",
            e
        )


# ============================================================
# RECOVER POSITIONS
# ============================================================

def reconcile_recovered_positions():

    if DRY_RUN:

        logger.info(
            "DRY_RUN=True: skipping balance reconciliation."
        )

        return

    with position_lock:

        recovered_symbols = list(
            positions.keys()
        )

    if not recovered_symbols:

        return

    try:

        balances = get_free_balances()

    except Exception as e:

        logger.warning(
            "Position reconciliation failed: %s",
            e
        )

        return

    removed = 0

    for symbol in recovered_symbols:

        with position_lock:

            position = positions.get(
                symbol
            )

        if not position:
            continue

        base_asset = position.get(
            "base_asset"
        )

        if not base_asset:

            base_asset = symbol_info.get(
                symbol,
                {}
            ).get(
                "baseAsset"
            )

        free_qty = D(
            balances.get(
                base_asset,
                Decimal("0")
            )
        )

        min_qty = get_min_qty(
            symbol
        )

        if free_qty < min_qty:

            logger.warning(
                "Recovered position removed because no free balance | %s",
                symbol
            )

            with position_lock:

                positions.pop(
                    symbol,
                    None
                )

            removed += 1

    if removed:

        save_positions()

    logger.info(
        "Position reconciliation complete | active=%s | removed=%s",
        len(positions),
        removed
    )


# ============================================================
# BOT RUN
# ============================================================

def run_bot():

    global last_history_refresh
    global last_server_time_sync

    logger.info(
        "===================================================="
    )

    logger.info(
        "RSI3 + RSI50 BINANCE SPOT BOT STARTING"
    )

    logger.info(
        "Timeframe: %s",
        TIMEFRAME
    )

    logger.info(
        "Top symbols: %s",
        TOP_SYMBOLS
    )

    logger.info(
        "BUY: RSI50 > %s AND RSI3 < %s",
        BUY_RSI50_MIN,
        BUY_RSI3_MAX
    )

    logger.info(
        "SELL: RSI3 crosses ABOVE %s",
        SELL_RSI3_LEVEL
    )

    logger.info(
        "STOP LOSS: %s%%",
        STOP_LOSS_PERCENT
    )

    logger.info(
        "BUY amount: %s USDT",
        BUY_USDT
    )

    logger.info(
        "LIVE BUY: %s",
        LIVE_RSI_BUY
    )

    logger.info(
        "LIVE SELL: %s",
        LIVE_RSI_SELL
    )

    logger.info(
        "DRY_RUN: %s",
        DRY_RUN
    )

    logger.info(
        "===================================================="
    )

    if not BINANCE_API_KEY or not BINANCE_API_SECRET:

        raise RuntimeError(
            "BINANCE_API_KEY / BINANCE_API_SECRET not configured"
        )

    sync_server_time()

    load_exchange_info()

    load_top_symbols()

    if len(symbols) == 0:

        raise RuntimeError(
            "No eligible symbols found"
        )

    verify_account()

    history = load_historical_data()

    # Recover positions saved before restart
    load_positions()

    reconcile_recovered_positions()

    last_history_refresh = time.time()
    last_server_time_sync = time.time()

    # Divide 150 into 3 groups of 50
    groups = []

    for i in range(
        0,
        len(symbols),
        GROUP_SIZE
    ):

        groups.append(
            symbols[
                i:i + GROUP_SIZE
            ]
        )

    logger.info(
        "Starting %s WebSocket groups",
        len(groups)
    )

    websocket_threads = []

    for index, group in enumerate(
        groups,
        start=1
    ):

        thread = threading.Thread(
            target=websocket_group_worker,
            args=(
                group,
                index,
                history,
            ),
            name=f"WS-Group-{index}",
            daemon=True,
        )

        thread.start()

        websocket_threads.append(
            thread
        )

        logger.info(
            "WebSocket group %s started | %s symbols",
            index,
            len(group)
        )

        time.sleep(1)

    # Main bot loop
    last_summary_minute = None

    while not stop_event.is_set():

        now = time.time()

        # Server time sync
        if (
            now -
            last_server_time_sync
            >= SERVER_TIME_SYNC_INTERVAL
        ):

            sync_server_time()

        # Refresh history cache periodically
        if (
            now -
            last_history_refresh
            >= HISTORY_REFRESH_INTERVAL
        ):

            try:

                save_history_cache(
                    history
                )

                last_history_refresh = now

                logger.info(
                    "History cache refreshed."
                )

            except Exception as e:

                logger.warning(
                    "History cache refresh failed: %s",
                    e
                )

        # Candle summary once per minute
        current_minute = int(
            time.time() // 60
        )

        if (
            DEBUG_MODE
            and
            current_minute != last_summary_minute
        ):

            last_summary_minute = (
                current_minute
            )

            candle_summary(
                history
            )

        stop_event.wait(10)

    # Shutdown
    logger.warning(
        "Bot run loop stopping..."
    )

    close_all_websockets()

    save_history_cache(
        history
    )

    save_positions()

    logger.warning(
        "Bot run loop stopped."
    )


# ============================================================
# SUPERVISOR
# ============================================================

def supervisor():

    logger.info(
        "BOT SUPERVISOR STARTED"
    )

    while not stop_event.is_set():

        try:

            run_bot()

        except BinanceRateLimitError as e:

            if stop_event.is_set():
                break

            if e.status_code == 418:

                cooldown = RATE_LIMIT_418_COOLDOWN

            else:

                cooldown = RATE_LIMIT_429_COOLDOWN

            logger.error(
                "BOT RATE LIMITED | HTTP %s | waiting %ss",
                e.status_code,
                cooldown
            )

            stop_event.wait(
                cooldown
            )

        except Exception as e:

            if stop_event.is_set():
                break

            logger.exception(
                "🔥 BOT CRASHED | %s",
                e
            )

            logger.warning(
                "Bot will restart after 10 seconds..."
            )

            stop_event.wait(10)

    logger.warning(
        "BOT SUPERVISOR STOPPED"
    )


# ============================================================
# START BOT
# ============================================================

def start_background_bot():

    global _bot_thread
    global _bot_thread_started

    with _bot_start_lock:

        if _bot_thread_started:

            return

        if stop_event.is_set():

            logger.warning(
                "Bot start skipped because stop_event is set."
            )

            return

        _bot_thread = threading.Thread(
            target=supervisor,
            name="BOT-SUPERVISOR",
            daemon=True,
        )

        _bot_thread.start()

        _bot_thread_started = True

        logger.info(
            "Background bot thread started."
        )


# ============================================================
# SIGNAL HANDLING
# ============================================================

def handle_shutdown_signal(
    signum,
    frame
):

    logger.warning(
        "Received shutdown signal: %s",
        signum
    )

    # Do not immediately kill internal state.
    # Save what we can and close websocket connections.
    stop_event.set()

    try:

        save_positions()

    except Exception:
        pass

    try:

        close_all_websockets()

    except Exception:
        pass


signal.signal(
    signal.SIGTERM,
    handle_shutdown_signal
)

signal.signal(
    signal.SIGINT,
    handle_shutdown_signal
)


@atexit.register
def shutdown_cleanup():

    try:

        stop_event.set()

        close_all_websockets()

        save_positions()

    except Exception:
        pass


# ============================================================
# FLASK ROUTES
# ============================================================

@app.before_request
def ensure_bot_started():

    start_background_bot()


@app.route("/")
def home():

    with position_lock:

        active_positions = len(
            positions
        )

    return jsonify({
        "status": "online",
        "bot": "RSI3-RSI50-BINANCE-SPOT",
        "timeframe": TIMEFRAME,
        "symbols": len(symbols),
        "positions": active_positions,
        "buy_rule": (
            f"RSI50 > {BUY_RSI50_MIN} "
            f"AND RSI3 < {BUY_RSI3_MAX}"
        ),
        "sell_rule": (
            f"RSI3 crosses above "
            f"{SELL_RSI3_LEVEL}"
        ),
        "stop_loss": (
            f"{STOP_LOSS_PERCENT}%"
        ),
        "live_buy": LIVE_RSI_BUY,
        "live_sell": LIVE_RSI_SELL,
        "dry_run": DRY_RUN,
    })


@app.route("/health")
def health():

    with position_lock:

        active_positions = list(
            positions.keys()
        )

    return jsonify({
        "status": "ok",
        "bot_thread_started":
            _bot_thread_started,
        "stop_requested":
            stop_event.is_set(),
        "symbols":
            len(symbols),
        "positions":
            len(active_positions),
        "position_symbols":
            active_positions,
        "websocket_status":
            websocket_status,
        "last_ws_message_time":
            last_ws_message_time,
        "server_time_offset_ms":
            server_time_offset_ms,
        "rate_limit_active":
            time.time() < rate_limit_until,
    })


@app.route("/positions")
def positions_route():

    with position_lock:

        output = {}

        for symbol, p in positions.items():

            output[symbol] = {
                "quantity":
                    decimal_to_string(
                        p.get("quantity")
                    ),
                "entry_price":
                    decimal_to_string(
                        p.get("entry_price")
                    ),
                "stop_price":
                    decimal_to_string(
                        p.get("stop_price")
                    ),
                "stop_order_id":
                    p.get("stop_order_id"),
                "entry_time":
                    p.get("entry_time"),
            }

    return jsonify(output)


# ============================================================
# MAIN
# ============================================================

if __name__ == "__main__":

    start_background_bot()

    app.run(
        host="0.0.0.0",
        port=int(
            os.getenv(
                "PORT",
                "10000"
            )
        ),
        threaded=True,
    )
