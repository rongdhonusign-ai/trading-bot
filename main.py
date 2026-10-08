import os
import time
import json
import hmac
import hashlib
import logging
import threading
import random
import signal
from decimal import Decimal, ROUND_DOWN
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
WS_BASE = "wss://stream.binance.com:9443/stream"

TIMEFRAME = "5m"

TOP_SYMBOLS = 150
GROUP_SIZE = 50

BUY_USDT = Decimal("15")

RSI_FAST_PERIOD = 3
RSI_SLOW_PERIOD = 50


# ============================================================
# BUY SETTINGS
# ============================================================

BUY_RSI50_MIN = Decimal("50")
BUY_RSI3_MAX = Decimal("10")


# ============================================================
# SELL SETTINGS
# ============================================================

SELL_RSI3_LEVEL = Decimal("80")


# ============================================================
# STOP LOSS
# ============================================================

STOP_LOSS_PERCENT = Decimal("1.00")


# ============================================================
# HISTORY
# ============================================================

HISTORY_CANDLES = 200

STARTUP_KLINE_DELAY = 0.20


# ============================================================
# THREADS
# ============================================================

WORKER_THREADS = 4


# ============================================================
# WEBSOCKET
# ============================================================

WS_PING_INTERVAL = 20
WS_PING_TIMEOUT = 10

WS_RECONNECT_MIN = 3
WS_RECONNECT_MAX = 30


# ============================================================
# WATCHDOG
# ============================================================

WATCHDOG_INTERVAL = 15

STARTUP_RETRY_MIN = 10
STARTUP_RETRY_MAX = 60

TIME_SYNC_INTERVAL = 30 * 60


# ============================================================
# DEBUG
# ============================================================

DEBUG_MODE = (
    os.getenv("DEBUG_MODE", "true").lower() == "true"
)

DRY_RUN = (
    os.getenv("DRY_RUN", "false").lower() == "true"
)


# ============================================================
# LOGGING
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

logger = logging.getLogger("RSI_BOT")


# ============================================================
# FLASK
# ============================================================

app = Flask(__name__)


# ============================================================
# GLOBAL STATE
# ============================================================

session = requests.Session()

server_time_offset = 0

exchange_info = {}
symbol_filters = {}

symbols = []

candles = {}

positions = {}

buying_symbols = set()
selling_symbols = set()

# USDT that has already been reserved for pending BUY orders
reserved_usdt = Decimal("0")

state_lock = threading.RLock()

executor = ThreadPoolExecutor(
    max_workers=WORKER_THREADS
)

ws_threads = []

# group_number -> thread
ws_thread_map = {}

# group_number -> last successful message time
ws_last_message = {}

# group_number -> websocket object
ws_objects = {}

bot_started = False
bot_live = False

shutdown_event = threading.Event()

last_processed_candle = {}

last_time_sync = 0

startup_lock = threading.Lock()

watchdog_thread = None


# ============================================================
# HTTP SESSION HEADERS
# ============================================================

if BINANCE_API_KEY:

    session.headers.update({
        "X-MBX-APIKEY": BINANCE_API_KEY
    })


# ============================================================
# BASIC HELPERS
# ============================================================

def d(value):

    try:
        return Decimal(str(value))

    except Exception:

        return Decimal("0")


def now_ms():

    return int(time.time() * 1000)


def binance_time_ms():

    return int(time.time() * 1000) + server_time_offset


def fmt_decimal(value):

    if not isinstance(value, Decimal):

        value = d(value)

    text = format(value, "f")

    if "." in text:

        text = text.rstrip("0").rstrip(".")

    return text or "0"


# ============================================================
# SAFE SLEEP
# ============================================================

def safe_sleep(seconds):

    end_time = time.time() + seconds

    while (
        time.time() < end_time
        and not shutdown_event.is_set()
    ):

        time.sleep(
            min(1, end_time - time.time())
        )


# ============================================================
# SERVER TIME
# ============================================================

def sync_server_time():

    global server_time_offset
    global last_time_sync

    try:

        local_before = now_ms()

        response = session.get(
            BASE_URL + "/api/v3/time",
            timeout=10
        )

        response.raise_for_status()

        local_after = now_ms()

        data = response.json()

        server_time = int(
            data["serverTime"]
        )

        local_mid = (
            local_before
            + local_after
        ) // 2

        server_time_offset = (
            server_time
            - local_mid
        )

        last_time_sync = time.time()

        logger.info(
            "Server time offset: %s ms",
            server_time_offset
        )

        return True

    except Exception as e:

        logger.warning(
            "Server time sync failed: %s",
            e
        )

        return False


# ============================================================
# SIGNATURE
# ============================================================

def make_signature(params):

    query_string = "&".join(
        f"{key}={params[key]}"
        for key in params
    )

    return hmac.new(
        BINANCE_API_SECRET.encode(),
        query_string.encode(),
        hashlib.sha256
    ).hexdigest()


# ============================================================
# SIGNED REST REQUEST
# ============================================================

def signed_request(
    method,
    path,
    params=None,
    retries=3
):

    if params is None:

        params = {}

    if not BINANCE_API_KEY:

        logger.error(
            "BINANCE_API_KEY missing"
        )

        return None

    if not BINANCE_API_SECRET:

        logger.error(
            "BINANCE_API_SECRET missing"
        )

        return None

    url = BASE_URL + path

    for attempt in range(retries):

        if shutdown_event.is_set():

            return None

        request_params = dict(params)

        # IMPORTANT:
        # Generate fresh timestamp on EVERY retry.
        request_params["timestamp"] = (
            binance_time_ms()
        )

        request_params["recvWindow"] = 10000

        request_params["signature"] = (
            make_signature(
                request_params
            )
        )

        try:

            method_upper = method.upper()

            if method_upper == "GET":

                response = session.get(
                    url,
                    params=request_params,
                    timeout=15
                )

            elif method_upper == "POST":

                response = session.post(
                    url,
                    params=request_params,
                    timeout=15
                )

            elif method_upper == "DELETE":

                response = session.delete(
                    url,
                    params=request_params,
                    timeout=15
                )

            else:

                raise ValueError(
                    f"Unsupported method: {method}"
                )

            if response.status_code == 200:

                try:

                    return response.json()

                except Exception:

                    logger.error(
                        "Invalid JSON response: %s",
                        response.text[:500]
                    )

                    return None

            # ------------------------------------------------
            # Timestamp error
            # ------------------------------------------------

            if response.status_code == 400:

                try:

                    data = response.json()

                    code = data.get(
                        "code"
                    )

                    # Timestamp problem
                    if code == -1021:

                        logger.warning(
                            "Timestamp error. "
                            "Resyncing server time..."
                        )

                        sync_server_time()

                        safe_sleep(1)

                        continue

                    # ------------------------------------------------
                    # Insufficient balance
                    #
                    # DO NOT repeatedly retry this.
                    # ------------------------------------------------

                    if code == -2010:

                        logger.warning(
                            "Binance order rejected -2010: %s",
                            data.get(
                                "msg",
                                "unknown"
                            )
                        )

                        return data

                except Exception:

                    pass

            # ------------------------------------------------
            # Rate limit
            # ------------------------------------------------

            if response.status_code in (
                418,
                429
            ):

                retry_after = (
                    response.headers.get(
                        "Retry-After"
                    )
                )

                wait_time = 10

                if retry_after:

                    try:

                        wait_time = max(
                            5,
                            int(
                                float(
                                    retry_after
                                )
                            )
                        )

                    except Exception:

                        pass

                wait_time += random.uniform(
                    1,
                    3
                )

                logger.warning(
                    "Binance rate limit HTTP %s. "
                    "Waiting %.1fs",
                    response.status_code,
                    wait_time
                )

                safe_sleep(
                    wait_time
                )

                continue

            logger.error(
                "Binance API error HTTP %s: %s",
                response.status_code,
                response.text[:500]
            )

            safe_sleep(
                2 + attempt * 2
            )

        except requests.RequestException as e:

            logger.warning(
                "REST request error: %s",
                e
            )

            safe_sleep(
                2 + attempt * 2
            )

        except Exception as e:

            logger.exception(
                "Signed request exception: %s",
                e
            )

            safe_sleep(
                2 + attempt * 2
            )

    return None


# ============================================================
# PUBLIC REST REQUEST
# ============================================================

def public_get(
    path,
    params=None,
    retries=3
):

    url = BASE_URL + path

    for attempt in range(retries):

        if shutdown_event.is_set():

            return None

        try:

            response = session.get(
                url,
                params=params or {},
                timeout=15
            )

            if response.status_code == 200:

                return response.json()

            # ------------------------------------------------
            # Rate limit
            # ------------------------------------------------

            if response.status_code in (
                418,
                429
            ):

                retry_after = (
                    response.headers.get(
                        "Retry-After"
                    )
                )

                wait_time = 10

                if retry_after:

                    try:

                        wait_time = max(
                            5,
                            int(
                                float(
                                    retry_after
                                )
                            )
                        )

                    except Exception:

                        pass

                wait_time += random.uniform(
                    1,
                    3
                )

                logger.warning(
                    "Public API rate limit HTTP %s. "
                    "Waiting %.1fs",
                    response.status_code,
                    wait_time
                )

                safe_sleep(
                    wait_time
                )

                continue

            logger.error(
                "Public API error HTTP %s: %s",
                response.status_code,
                response.text[:300]
            )

            safe_sleep(
                2 + attempt * 2
            )

        except Exception as e:

            logger.warning(
                "Public REST error: %s",
                e
            )

            safe_sleep(
                2 + attempt * 2
            )

    return None


# ============================================================
# EXCHANGE INFO
# ============================================================

def load_exchange_info():

    global exchange_info
    global symbol_filters

    logger.info(
        "Loading exchange information..."
    )

    data = public_get(
        "/api/v3/exchangeInfo"
    )

    if not data:

        logger.error(
            "Could not load exchangeInfo"
        )

        return False

    exchange_info = data

    symbol_filters.clear()

    for item in data.get(
        "symbols",
        []
    ):

        symbol = item.get(
            "symbol"
        )

        if not symbol:

            continue

        filters = {}

        for f in item.get(
            "filters",
            []
        ):

            filter_type = f.get(
                "filterType"
            )

            if filter_type:

                filters[
                    filter_type
                ] = f

        symbol_filters[
            symbol
        ] = {

            "status": item.get(
                "status"
            ),

            "baseAsset": item.get(
                "baseAsset"
            ),

            "quoteAsset": item.get(
                "quoteAsset"
            ),

            "filters": filters
        }

    logger.info(
        "Exchange info loaded: %d symbols",
        len(symbol_filters)
    )

    return True


# ============================================================
# STABLE ASSETS
# ============================================================

STABLE_ASSETS = {

    "USDT",
    "USDC",
    "BUSD",
    "FDUSD",
    "TUSD",
    "USDP",
    "DAI",

    "EUR",
    "TRY",
    "BRL",
    "GBP",
    "AUD",
    "JPY",
    "RUB",
    "UAH",
    "PLN",
    "ARS",
    "ZAR",
    "NGN",
    "BIDR",
    "IDRT"
}


# ============================================================
# TOP SYMBOLS
# ============================================================

def load_top_symbols():

    global symbols

    logger.info(
        "Loading 24h ticker data..."
    )

    ticker_data = public_get(
        "/api/v3/ticker/24hr"
    )

    if not ticker_data:

        logger.error(
            "Could not load 24h ticker"
        )

        return False

    candidates = []

    for ticker in ticker_data:

        symbol = ticker.get(
            "symbol"
        )

        if not symbol:

            continue

        info = symbol_filters.get(
            symbol
        )

        if not info:

            continue

        if info["status"] != "TRADING":

            continue

        if info["quoteAsset"] != "USDT":

            continue

        base = info["baseAsset"]

        if base in STABLE_ASSETS:

            continue

        if base in {
            "BTC",
            "ETH"
        }:

            continue

        quote_volume = d(
            ticker.get(
                "quoteVolume",
                "0"
            )
        )

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

    selected = [
        x[0]
        for x in candidates[
            :TOP_SYMBOLS
        ]
    ]

    with state_lock:

        symbols = selected

    logger.info(
        "Eligible USDT pairs: %d",
        len(candidates)
    )

    logger.info(
        "Selected top %d symbols",
        len(selected)
    )

    if DEBUG_MODE:

        logger.info(
            "First symbols: %s",
            ", ".join(
                selected[:20]
            )
        )

    return len(selected) > 0


# ============================================================
# KLINE FETCH
# ============================================================

def fetch_klines(symbol):

    data = public_get(
        "/api/v3/klines",
        {
            "symbol": symbol,
            "interval": TIMEFRAME,
            "limit": HISTORY_CANDLES
        }
    )

    if not data:

        return False

    result = deque(
        maxlen=HISTORY_CANDLES
    )

    current_time = binance_time_ms()

    for k in data:

        try:

            open_time = int(k[0])
            close_time = int(k[6])

            if close_time >= current_time:

                continue

            candle = {

                "open_time": open_time,

                "open": d(k[1]),

                "high": d(k[2]),

                "low": d(k[3]),

                "close": d(k[4]),

                "volume": d(k[5]),

                "close_time": close_time
            }

            result.append(
                candle
            )

        except Exception:

            continue

    if len(result) < (
        RSI_SLOW_PERIOD + 5
    ):

        logger.warning(
            "%s insufficient candle history: %d",
            symbol,
            len(result)
        )

        return False

    with state_lock:

        candles[symbol] = result

    return True


# ============================================================
# INITIALIZE CANDLES
# ============================================================

def initialize_candles():

    logger.info(
        "Initializing historical candles for %d symbols...",
        len(symbols)
    )

    success = 0

    for index, symbol in enumerate(
        symbols,
        1
    ):

        if shutdown_event.is_set():

            break

        try:

            if fetch_klines(symbol):

                success += 1

        except Exception as e:

            logger.warning(
                "Historical fetch failed %s: %s",
                symbol,
                e
            )

        if (
            index % 10 == 0
            or index == len(symbols)
        ):

            logger.info(
                "Historical initialization: %d/%d",
                index,
                len(symbols)
            )

        safe_sleep(
            STARTUP_KLINE_DELAY
        )

    logger.info(
        "Historical initialization complete: %d/%d",
        success,
        len(symbols)
    )

    return success > 0


# ============================================================
# WILDER RSI
# ============================================================

def calculate_rsi_wilder(
    values,
    period
):

    if len(values) < period + 1:

        return []

    gains = []
    losses = []

    for i in range(
        1,
        len(values)
    ):

        change = (
            values[i]
            - values[i - 1]
        )

        if change > 0:

            gains.append(
                change
            )

            losses.append(
                Decimal("0")
            )

        else:

            gains.append(
                Decimal("0")
            )

            losses.append(
                abs(change)
            )

    if len(gains) < period:

        return []

    avg_gain = (
        sum(gains[:period])
        / Decimal(period)
    )

    avg_loss = (
        sum(losses[:period])
        / Decimal(period)
    )

    result = []

    def make_rsi(
        gain,
        loss
    ):

        if loss == 0:

            if gain == 0:

                return Decimal("50")

            return Decimal("100")

        rs = gain / loss

        return (
            Decimal("100")
            - (
                Decimal("100")
                / (
                    Decimal("1")
                    + rs
                )
            )
        )

    result.append(
        make_rsi(
            avg_gain,
            avg_loss
        )
    )

    for i in range(
        period,
        len(gains)
    ):

        avg_gain = (
            (
                avg_gain
                * Decimal(
                    period - 1
                )
            )
            + gains[i]
        ) / Decimal(period)

        avg_loss = (
            (
                avg_loss
                * Decimal(
                    period - 1
                )
            )
            + losses[i]
        ) / Decimal(period)

        result.append(
            make_rsi(
                avg_gain,
                avg_loss
            )
        )

    return result


# ============================================================
# INDICATORS
# ============================================================

def get_rsi_values(symbol):

    with state_lock:

        data = list(
            candles.get(
                symbol,
                []
            )
        )

    if len(data) < (
        RSI_SLOW_PERIOD + 5
    ):

        return None

    closes = [
        x["close"]
        for x in data
    ]

    rsi3 = calculate_rsi_wilder(
        closes,
        RSI_FAST_PERIOD
    )

    rsi50 = calculate_rsi_wilder(
        closes,
        RSI_SLOW_PERIOD
    )

    if len(rsi3) < 2:

        return None

    if len(rsi50) < 1:

        return None

    return {

        "rsi3_previous": rsi3[-2],

        "rsi3_current": rsi3[-1],

        "rsi50": rsi50[-1],

        "close": closes[-1],

        "last_candle_time": data[-1][
            "close_time"
        ]
    }


# ============================================================
# FILTER HELPERS
# ============================================================

def get_symbol_filter(
    symbol,
    filter_type
):

    info = symbol_filters.get(
        symbol
    )

    if not info:

        return None

    return info["filters"].get(
        filter_type
    )


def round_step(
    quantity,
    step
):

    quantity = d(quantity)
    step = d(step)

    if step <= 0:

        return quantity

    return (
        quantity / step
    ).to_integral_value(
        rounding=ROUND_DOWN
    ) * step


# ============================================================
# BUY QUANTITY
# ============================================================

def calculate_buy_quantity(
    symbol,
    price
):

    price = d(price)

    if price <= 0:

        return Decimal("0")

    raw_qty = (
        BUY_USDT
        / price
    )

    lot_filter = get_symbol_filter(
        symbol,
        "LOT_SIZE"
    )

    if not lot_filter:

        return raw_qty

    step_size = d(
        lot_filter.get(
            "stepSize",
            "0"
        )
    )

    min_qty = d(
        lot_filter.get(
            "minQty",
            "0"
        )
    )

    qty = round_step(
        raw_qty,
        step_size
    )

    if qty < min_qty:

        return Decimal("0")

    # --------------------------------------------------------
    # MIN NOTIONAL
    # --------------------------------------------------------

    min_notional_filter = (
        get_symbol_filter(
            symbol,
            "MIN_NOTIONAL"
        )
    )

    if min_notional_filter:

        min_notional = d(
            min_notional_filter.get(
                "minNotional",
                "0"
            )
        )

        if (
            qty * price
            < min_notional
        ):

            logger.warning(
                "%s BUY skipped: "
                "notional %s < minimum %s",
                symbol,
                fmt_decimal(
                    qty * price
                ),
                fmt_decimal(
                    min_notional
                )
            )

            return Decimal("0")

    # --------------------------------------------------------
    # NOTIONAL
    # --------------------------------------------------------

    notional_filter = (
        get_symbol_filter(
            symbol,
            "NOTIONAL"
        )
    )

    if notional_filter:

        min_notional = d(
            notional_filter.get(
                "minNotional",
                "0"
            )
        )

        if (
            qty * price
            < min_notional
        ):

            logger.warning(
                "%s BUY skipped: "
                "notional %s < minimum %s",
                symbol,
                fmt_decimal(
                    qty * price
                ),
                fmt_decimal(
                    min_notional
                )
            )

            return Decimal("0")

    return qty


# ============================================================
# SELL QUANTITY
# ============================================================

def calculate_sell_quantity(
    symbol,
    available_qty
):

    available_qty = d(
        available_qty
    )

    if available_qty <= 0:

        return Decimal("0")

    lot_filter = get_symbol_filter(
        symbol,
        "LOT_SIZE"
    )

    if not lot_filter:

        return available_qty

    step_size = d(
        lot_filter.get(
            "stepSize",
            "0"
        )
    )

    min_qty = d(
        lot_filter.get(
            "minQty",
            "0"
        )
    )

    # Keep a small buffer for commission/dust.
    qty = (
        available_qty
        * Decimal("0.999")
    )

    qty = round_step(
        qty,
        step_size
    )

    if qty < min_qty:

        return Decimal("0")

    return qty


# ============================================================
# ACCOUNT
# ============================================================

def get_account():

    if (
        not BINANCE_API_KEY
        or not BINANCE_API_SECRET
    ):

        return None

    return signed_request(
        "GET",
        "/api/v3/account"
    )


# ============================================================
# ASSET BALANCE
# ============================================================

def get_asset_balance(
    asset,
    account=None
):

    if account is None:

        account = get_account()

    if not account:

        return Decimal("0")

    for balance in account.get(
        "balances",
        []
    ):

        if balance.get(
            "asset"
        ) == asset:

            return d(
                balance.get(
                    "free",
                    "0"
                )
            )

    return Decimal("0")


# ============================================================
# AVAILABLE USDT
# ============================================================

def get_available_usdt():

    account = get_account()

    if not account:

        return Decimal("0")

    return get_asset_balance(
        "USDT",
        account
    )


# ============================================================
# MARKET PRICE
# ============================================================

def get_market_price(
    symbol
):

    data = public_get(
        "/api/v3/ticker/price",
        {
            "symbol": symbol
        }
    )

    if not data:

        return Decimal("0")

    return d(
        data.get(
            "price",
            "0"
        )
    )


# ============================================================
# RESERVE BUY BALANCE
# ============================================================

def reserve_buy_balance():

    global reserved_usdt

    # --------------------------------------------------------
    # Get fresh Binance balance
    # --------------------------------------------------------

    available = (
        get_available_usdt()
    )

    if available <= 0:

        return False

    with state_lock:

        remaining = (
            available
            - reserved_usdt
        )

        # Keep a small safety buffer.
        required = (
            BUY_USDT
            + Decimal("0.10")
        )

        if remaining < required:

            logger.warning(
                "BUY skipped: insufficient "
                "available USDT | "
                "free=%s | reserved=%s | "
                "required≈%s",
                fmt_decimal(available),
                fmt_decimal(
                    reserved_usdt
                ),
                fmt_decimal(required)
            )

            return False

        reserved_usdt += BUY_USDT

        logger.info(
            "USDT reserved: %s | total reserved=%s",
            fmt_decimal(BUY_USDT),
            fmt_decimal(reserved_usdt)
        )

        return True


# ============================================================
# RELEASE BUY RESERVATION
# ============================================================

def release_buy_balance():

    global reserved_usdt

    with state_lock:

        reserved_usdt -= BUY_USDT

        if reserved_usdt < 0:

            reserved_usdt = Decimal("0")


# ============================================================
# PLACE BUY
# ============================================================

def place_buy(
    symbol,
    signal_data
):

    reserved = False

    with state_lock:

        if symbol in positions:

            return False

        if symbol in buying_symbols:

            return False

        buying_symbols.add(
            symbol
        )

    try:

        # ----------------------------------------------------
        # Reserve USDT BEFORE order
        # ----------------------------------------------------

        if not reserve_buy_balance():

            return False

        reserved = True

        # ----------------------------------------------------
        # Fresh market price
        # ----------------------------------------------------

        price = get_market_price(
            symbol
        )

        if price <= 0:

            logger.warning(
                "%s BUY skipped: "
                "could not get market price",
                symbol
            )

            return False

        quantity = calculate_buy_quantity(
            symbol,
            price
        )

        if quantity <= 0:

            logger.warning(
                "%s BUY skipped: invalid quantity",
                symbol
            )

            return False

        logger.info(
            "🔥 BUY SIGNAL | %s | "
            "RSI50=%.4f | "
            "RSI3 prev=%.4f | "
            "RSI3 current=%.4f | "
            "price=%s | qty=%s",
            symbol,
            signal_data["rsi50"],
            signal_data["rsi3_previous"],
            signal_data["rsi3_current"],
            fmt_decimal(price),
            fmt_decimal(quantity)
        )

        # ----------------------------------------------------
        # DRY RUN
        # ----------------------------------------------------

        if DRY_RUN:

            entry_price = price

            stop_price = (
                entry_price
                * (
                    Decimal("1")
                    - (
                        STOP_LOSS_PERCENT
                        / Decimal("100")
                    )
                )
            )

            with state_lock:

                positions[symbol] = {

                    "symbol": symbol,

                    "quantity": quantity,

                    "entry_price": entry_price,

                    "stop_price": stop_price,

                    "buy_time": time.time(),

                    "dry_run": True
                }

            logger.info(
                "DRY RUN BUY | %s | qty=%s",
                symbol,
                fmt_decimal(quantity)
            )

            return True

        # ----------------------------------------------------
        # REAL BUY
        # ----------------------------------------------------

        order = signed_request(
            "POST",
            "/api/v3/order",
            {
                "symbol": symbol,

                "side": "BUY",

                "type": "MARKET",

                "quantity": fmt_decimal(
                    quantity
                ),

                "newOrderRespType": "FULL"
            }
        )

        if not order:

            return False

        # ----------------------------------------------------
        # Binance insufficient balance
        # ----------------------------------------------------

        if order.get(
            "code"
        ) == -2010:

            logger.warning(
                "❌ BUY rejected due to "
                "insufficient balance | %s",
                symbol
            )

            return False

        if "orderId" not in order:

            logger.error(
                "BUY failed %s: %s",
                symbol,
                order
            )

            return False

        executed_qty = d(
            order.get(
                "executedQty",
                quantity
            )
        )

        quote_qty = d(
            order.get(
                "cummulativeQuoteQty",
                "0"
            )
        )

        if (
            executed_qty > 0
            and quote_qty > 0
        ):

            entry_price = (
                quote_qty
                / executed_qty
            )

        else:

            entry_price = price

        stop_price = (
            entry_price
            * (
                Decimal("1")
                - (
                    STOP_LOSS_PERCENT
                    / Decimal("100")
                )
            )
        )

        with state_lock:

            positions[symbol] = {

                "symbol": symbol,

                "quantity": executed_qty,

                "entry_price": entry_price,

                "stop_price": stop_price,

                "buy_time": time.time(),

                "order_id": order.get(
                    "orderId"
                )
            }

        logger.info(
            "✅ BUY FILLED | %s | "
            "qty=%s | entry=%s | SL=%s",
            symbol,
            fmt_decimal(executed_qty),
            fmt_decimal(entry_price),
            fmt_decimal(stop_price)
        )

        return True

    except Exception as e:

        logger.exception(
            "BUY exception %s: %s",
            symbol,
            e
        )

        return False

    finally:

        if reserved:

            release_buy_balance()

        with state_lock:

            buying_symbols.discard(
                symbol
            )


# ============================================================
# PLACE SELL
# ============================================================

def place_sell(
    symbol,
    reason
):

    with state_lock:

        if symbol in selling_symbols:

            return False

        position = positions.get(
            symbol
        )

        if not position:

            return False

        selling_symbols.add(
            symbol
        )

    try:

        quantity = d(
            position.get(
                "quantity",
                "0"
            )
        )

        if quantity <= 0:

            return False

        # ----------------------------------------------------
        # DRY RUN
        # ----------------------------------------------------

        if DRY_RUN:

            logger.info(
                "DRY RUN SELL | %s | reason=%s",
                symbol,
                reason
            )

            with state_lock:

                positions.pop(
                    symbol,
                    None
                )

            return True

        # ----------------------------------------------------
        # REAL SELL
        # ----------------------------------------------------

        info = symbol_filters.get(
            symbol
        )

        if not info:

            return False

        base_asset = info.get(
            "baseAsset"
        )

        account = get_account()

        if not account:

            logger.warning(
                "SELL skipped %s: "
                "could not read account",
                symbol
            )

            return False

        actual_balance = (
            get_asset_balance(
                base_asset,
                account
            )
        )

        sell_qty = calculate_sell_quantity(
            symbol,
            actual_balance
        )

        if sell_qty <= 0:

            logger.warning(
                "SELL skipped %s: "
                "no valid balance",
                symbol
            )

            return False

        order = signed_request(
            "POST",
            "/api/v3/order",
            {
                "symbol": symbol,

                "side": "SELL",

                "type": "MARKET",

                "quantity": fmt_decimal(
                    sell_qty
                ),

                "newOrderRespType": "FULL"
            }
        )

        if not order:

            return False

        if order.get(
            "code"
        ):

            logger.error(
                "SELL failed %s: %s",
                symbol,
                order
            )

            return False

        if "orderId" not in order:

            logger.error(
                "SELL failed %s: %s",
                symbol,
                order
            )

            return False

        logger.info(
            "✅ SELL FILLED | %s | "
            "reason=%s | qty=%s",
            symbol,
            reason,
            fmt_decimal(sell_qty)
        )

        with state_lock:

            positions.pop(
                symbol,
                None
            )

        return True

    except Exception as e:

        logger.exception(
            "SELL exception %s: %s",
            symbol,
            e
        )

        return False

    finally:

        with state_lock:

            selling_symbols.discard(
                symbol
            )


# ============================================================
# STOP LOSS
# ============================================================

def check_stop_loss(
    symbol,
    current_price
):

    with state_lock:

        position = positions.get(
            symbol
        )

    if not position:

        return

    stop_price = d(
        position.get(
            "stop_price",
            "0"
        )
    )

    if stop_price <= 0:

        return

    if current_price <= stop_price:

        logger.warning(
            "🛑 STOP LOSS | %s | "
            "price=%s <= SL=%s",
            symbol,
            fmt_decimal(current_price),
            fmt_decimal(stop_price)
        )

        with state_lock:

            if symbol in selling_symbols:

                return

        executor.submit(
            place_sell,
            symbol,
            "1% STOP LOSS"
        )


# ============================================================
# RSI SIGNAL PROCESSING
# ============================================================

def process_symbol(symbol):

    try:

        indicator = get_rsi_values(
            symbol
        )

        if not indicator:

            return

        rsi3_previous = indicator[
            "rsi3_previous"
        ]

        rsi3_current = indicator[
            "rsi3_current"
        ]

        rsi50 = indicator[
            "rsi50"
        ]

        candle_time = indicator[
            "last_candle_time"
        ]

        # ----------------------------------------------------
        # One processing per closed candle
        # ----------------------------------------------------

        with state_lock:

            previous_processed = (
                last_processed_candle.get(
                    symbol
                )
            )

            if previous_processed == candle_time:

                return

            last_processed_candle[
                symbol
            ] = candle_time

        # ====================================================
        # SELL
        # ====================================================

        with state_lock:

            has_position = (
                symbol in positions
            )

        if has_position:

            sell_cross = (

                rsi3_previous
                <= SELL_RSI3_LEVEL

                and

                rsi3_current
                > SELL_RSI3_LEVEL
            )

            if sell_cross:

                logger.info(
                    "🔥 SELL CROSS | %s | "
                    "RSI3 %.4f -> %.4f | "
                    "crossed above 80",
                    symbol,
                    rsi3_previous,
                    rsi3_current
                )

                executor.submit(
                    place_sell,
                    symbol,
                    "RSI3 CROSS ABOVE 80"
                )

            return

        # ====================================================
        # BUY
        # ====================================================

        was_oversold = (
            rsi3_previous
            < BUY_RSI3_MAX
        )

        rsi_turning_up = (
            rsi3_current
            > rsi3_previous
        )

        trend_ok = (
            rsi50
            > BUY_RSI50_MIN
        )

        buy_signal = (

            trend_ok

            and

            was_oversold

            and

            rsi_turning_up
        )

        if buy_signal:

            logger.info(
                "🔥🔥 BUY CONDITIONS TRUE 🔥🔥 | %s | "
                "RSI50=%.4f > 50 | "
                "RSI3 %.4f -> %.4f | "
                "oversold recovery",
                symbol,
                rsi50,
                rsi3_previous,
                rsi3_current
            )

            with state_lock:

                if (
                    symbol in positions
                    or symbol in buying_symbols
                ):

                    return

            executor.submit(
                place_buy,
                symbol,
                indicator
            )

        elif DEBUG_MODE:

            near_signal = (

                rsi50
                > Decimal("45")

                and

                rsi3_current
                < Decimal("20")
            )

            if near_signal:

                logger.info(
                    "NEAR BUY | %s | "
                    "RSI50=%.4f | "
                    "RSI3 prev=%.4f | "
                    "RSI3 current=%.4f | "
                    "turn=%s",
                    symbol,
                    rsi50,
                    rsi3_previous,
                    rsi3_current,
                    rsi_turning_up
                )

    except Exception as e:

        logger.exception(
            "Signal processing error %s: %s",
            symbol,
            e
        )


# ============================================================
# UPDATE CANDLE
# ============================================================

def update_candle(
    symbol,
    kline
):

    try:

        open_time = int(
            kline["t"]
        )

        close_time = int(
            kline["T"]
        )

        is_closed = bool(
            kline["x"]
        )

        open_price = d(
            kline["o"]
        )

        high_price = d(
            kline["h"]
        )

        low_price = d(
            kline["l"]
        )

        close_price = d(
            kline["c"]
        )

        volume = d(
            kline["v"]
        )

        # ====================================================
        # LIVE STOP LOSS
        #
        # IMPORTANT:
        # Use candle LOW as well as CLOSE.
        # This makes SL react even when price briefly
        # goes below the stop during a live 5m candle.
        # ====================================================

        with state_lock:

            position = positions.get(
                symbol
            )

        if position:

            stop_price = d(
                position.get(
                    "stop_price",
                    "0"
                )
            )

            if (
                stop_price > 0
                and low_price <= stop_price
            ):

                check_stop_loss(
                    symbol,
                    stop_price
                )

        # ====================================================
        # ONLY CLOSED CANDLE FOR RSI
        # ====================================================

        if not is_closed:

            return

        candle = {

            "open_time": open_time,

            "open": open_price,

            "high": high_price,

            "low": low_price,

            "close": close_price,

            "volume": volume,

            "close_time": close_time
        }

        with state_lock:

            history = candles.get(
                symbol
            )

            if history is None:

                history = deque(
                    maxlen=HISTORY_CANDLES
                )

                candles[
                    symbol
                ] = history

            if history:

                if (
                    history[-1][
                        "open_time"
                    ]
                    == open_time
                ):

                    history[-1] = candle

                elif (
                    open_time
                    > history[-1][
                        "open_time"
                    ]
                ):

                    history.append(
                        candle
                    )

            else:

                history.append(
                    candle
                )

        # Process closed candle
        process_symbol(
            symbol
        )

    except Exception as e:

        logger.exception(
            "Candle processing error %s: %s",
            symbol,
            e
        )


# ============================================================
# WEBSOCKET CALLBACKS
# ============================================================

def make_ws_callbacks(
    group_symbols,
    group_number
):

    def on_open(ws):

        logger.info(
            "WebSocket group %d CONNECTED: %d symbols",
            group_number,
            len(group_symbols)
        )

        with state_lock:

            ws_objects[
                group_number
            ] = ws

            ws_last_message[
                group_number
            ] = time.time()

    def on_message(
        ws,
        message
    ):

        try:

            with state_lock:

                ws_last_message[
                    group_number
                ] = time.time()

            data = json.loads(
                message
            )

            payload = data.get(
                "data",
                data
            )

            if payload.get(
                "e"
            ) != "kline":

                return

            symbol = payload.get(
                "s"
            )

            kline = payload.get(
                "k"
            )

            if not symbol or not kline:

                return

            update_candle(
                symbol,
                kline
            )

        except Exception as e:

            logger.warning(
                "WebSocket message error "
                "group %d: %s",
                group_number,
                e
            )

    def on_error(
        ws,
        error
    ):

        logger.warning(
            "WebSocket group %d ERROR: %s",
            group_number,
            error
        )

    def on_close(
        ws,
        close_status_code,
        close_msg
    ):

        logger.warning(
            "WebSocket group %d CLOSED | "
            "code=%s | msg=%s",
            group_number,
            close_status_code,
            close_msg
        )

        with state_lock:

            if (
                ws_objects.get(
                    group_number
                )
                is ws
            ):

                ws_objects.pop(
                    group_number,
                    None
                )

    return (
        on_open,
        on_message,
        on_error,
        on_close
    )


# ============================================================
# WEBSOCKET GROUP LOOP
# ============================================================

def websocket_group_loop(
    group_symbols,
    group_number
):

    streams = "/".join(

        f"{symbol.lower()}@kline_{TIMEFRAME}"

        for symbol in group_symbols
    )

    url = (
        WS_BASE
        + "?streams="
        + streams
    )

    reconnect_delay = (
        WS_RECONNECT_MIN
    )

    logger.info(
        "WebSocket group %d loop started",
        group_number
    )

    while not shutdown_event.is_set():

        ws = None

        callbacks = make_ws_callbacks(
            group_symbols,
            group_number
        )

        try:

            ws = websocket.WebSocketApp(

                url,

                on_open=callbacks[0],

                on_message=callbacks[1],

                on_error=callbacks[2],

                on_close=callbacks[3]
            )

            with state_lock:

                ws_objects[
                    group_number
                ] = ws

                ws_last_message[
                    group_number
                ] = time.time()

            ws.run_forever(

                ping_interval=WS_PING_INTERVAL,

                ping_timeout=WS_PING_TIMEOUT,

                ping_payload="ping"
            )

            # If connection exits normally,
            # reconnect.

            reconnect_delay = (
                WS_RECONNECT_MIN
            )

        except Exception as e:

            logger.warning(
                "WebSocket group %d exception: %s",
                group_number,
                e
            )

        finally:

            with state_lock:

                if (
                    ws_objects.get(
                        group_number
                    )
                    is ws
                ):

                    ws_objects.pop(
                        group_number,
                        None
                    )

        if shutdown_event.is_set():

            break

        wait_time = min(
            reconnect_delay,
            WS_RECONNECT_MAX
        )

        wait_time += random.uniform(
            0,
            2
        )

        logger.warning(
            "WebSocket group %d disconnected. "
            "Reconnecting in %.1fs",
            group_number,
            wait_time
        )

        safe_sleep(
            wait_time
        )

        reconnect_delay = min(

            reconnect_delay * 2,

            WS_RECONNECT_MAX
        )

    logger.info(
        "WebSocket group %d loop stopped",
        group_number
    )


# ============================================================
# START ONE WEBSOCKET GROUP
# ============================================================

def start_one_websocket_group(
    group_symbols,
    group_number
):

    if not group_symbols:

        return

    with state_lock:

        existing = ws_thread_map.get(
            group_number
        )

        if (
            existing
            and existing.is_alive()
        ):

            return

    thread = threading.Thread(

        target=websocket_group_loop,

        args=(
            group_symbols,
            group_number
        ),

        daemon=True,

        name=f"WS-{group_number}"
    )

    thread.start()

    with state_lock:

        ws_thread_map[
            group_number
        ] = thread

        ws_threads.append(
            thread
        )

    logger.info(
        "WebSocket group %d STARTED: %d symbols",
        group_number,
        len(group_symbols)
    )


# ============================================================
# START WEBSOCKETS
# ============================================================

def start_websockets():

    groups = [

        symbols[
            i:i + GROUP_SIZE
        ]

        for i in range(
            0,
            len(symbols),
            GROUP_SIZE
        )
    ]

    logger.info(
        "Starting %d WebSocket groups...",
        len(groups)
    )

    for index, group in enumerate(
        groups,
        1
    ):

        if shutdown_event.is_set():

            break

        if not group:

            continue

        start_one_websocket_group(
            group,
            index
        )

        safe_sleep(1)


# ============================================================
# WEBSOCKET WATCHDOG
# ============================================================

def websocket_watchdog():

    while not shutdown_event.is_set():

        try:

            groups = [

                symbols[
                    i:i + GROUP_SIZE
                ]

                for i in range(
                    0,
                    len(symbols),
                    GROUP_SIZE
                )
            ]

            for index, group in enumerate(
                groups,
                1
            ):

                if not group:

                    continue

                with state_lock:

                    thread = (
                        ws_thread_map.get(
                            index
                        )
                    )

                    last_message = (
                        ws_last_message.get(
                            index,
                            0
                        )
                    )

                # ------------------------------------------------
                # Thread died
                # ------------------------------------------------

                if (
                    thread is None
                    or not thread.is_alive()
                ):

                    logger.warning(
                        "WATCHDOG: WebSocket group %d "
                        "thread is not alive. Restarting...",
                        index
                    )

                    start_one_websocket_group(
                        group,
                        index
                    )

                    continue

                # ------------------------------------------------
                # No message for too long
                #
                # 5m stream should normally produce updates
                # frequently. If completely silent for 2 min,
                # force reconnect.
                # ------------------------------------------------

                if last_message > 0:

                    silence = (
                        time.time()
                        - last_message
                    )

                    if silence > 120:

                        logger.warning(
                            "WATCHDOG: WebSocket group %d "
                            "silent for %.0fs. "
                            "Forcing reconnect...",
                            index,
                            silence
                        )

                        with state_lock:

                            ws = ws_objects.get(
                                index
                            )

                        try:

                            if ws:

                                ws.close()

                        except Exception:

                            pass

            # ------------------------------------------------
            # Periodic server time sync
            # ------------------------------------------------

            if (
                time.time()
                - last_time_sync
                > TIME_SYNC_INTERVAL
            ):

                sync_server_time()

        except Exception as e:

            logger.exception(
                "WATCHDOG error: %s",
                e
            )

        safe_sleep(
            WATCHDOG_INTERVAL
        )


# ============================================================
# ACCOUNT CONNECTION
# ============================================================

def check_account_connection():

    if (
        not BINANCE_API_KEY
        or not BINANCE_API_SECRET
    ):

        logger.error(
            "BINANCE_API_KEY / "
            "BINANCE_API_SECRET missing"
        )

        return False

    account = get_account()

    if not account:

        logger.error(
            "Binance account connection failed"
        )

        return False

    logger.info(
        "Binance account connection OK"
    )

    usdt = get_asset_balance(
        "USDT",
        account
    )

    logger.info(
        "Available USDT: %s",
        fmt_decimal(usdt)
    )

    return True


# ============================================================
# POSITION RECOVERY
# ============================================================

def recover_positions():

    if DRY_RUN:

        return

    account = get_account()

    if not account:

        logger.warning(
            "Position recovery skipped: "
            "account unavailable"
        )

        return

    recovered = 0

    balances = account.get(
        "balances",
        []
    )

    selected_set = set(
        symbols
    )

    for balance in balances:

        asset = balance.get(
            "asset"
        )

        free = d(
            balance.get(
                "free",
                "0"
            )
        )

        locked = d(
            balance.get(
                "locked",
                "0"
            )
        )

        total = free + locked

        if total <= 0:

            continue

        symbol = (
            asset
            + "USDT"
        )

        if symbol not in selected_set:

            continue

        price = get_market_price(
            symbol
        )

        if price <= 0:

            continue

        # ----------------------------------------------------
        # IMPORTANT:
        # Recovery does not know original entry price.
        #
        # Use current market as reference.
        # ----------------------------------------------------

        stop_price = (

            price

            * (

                Decimal("1")

                - (

                    STOP_LOSS_PERCENT
                    / Decimal("100")
                )
            )
        )

        with state_lock:

            positions[
                symbol
            ] = {

                "symbol": symbol,

                "quantity": total,

                "entry_price": price,

                "stop_price": stop_price,

                "buy_time": time.time(),

                "recovered": True
            }

        recovered += 1

        logger.warning(
            "RECOVERED POSITION | %s | "
            "qty=%s | reference price=%s | SL=%s",
            symbol,
            fmt_decimal(total),
            fmt_decimal(price),
            fmt_decimal(stop_price)
        )

    logger.info(
        "Position recovery complete: %d",
        recovered
    )


# ============================================================
# BOT INITIALIZATION
# ============================================================

def initialize_bot_once():

    global bot_live
    global last_time_sync

    logger.info(
        "=================================================="
    )

    logger.info(
        "RSI(3) + RSI(50) BINANCE SPOT BOT STARTING"
    )

    logger.info(
        "Timeframe: %s",
        TIMEFRAME
    )

    logger.info(
        "Top symbols: %d",
        TOP_SYMBOLS
    )

    logger.info(
        "Group size: %d",
        GROUP_SIZE
    )

    logger.info(
        "BUY USDT: %s",
        BUY_USDT
    )

    logger.info(
        "BUY RULE: RSI50 > %s AND "
        "previous RSI3 < %s AND "
        "current RSI3 > previous RSI3",
        BUY_RSI50_MIN,
        BUY_RSI3_MAX
    )

    logger.info(
        "SELL RULE: RSI3 crosses above %s",
        SELL_RSI3_LEVEL
    )

    logger.info(
        "STOP LOSS: %s%%",
        STOP_LOSS_PERCENT
    )

    logger.info(
        "DRY_RUN: %s",
        DRY_RUN
    )

    logger.info(
        "=================================================="
    )

    # --------------------------------------------------------
    # Time
    # --------------------------------------------------------

    sync_server_time()

    safe_sleep(1)

    # --------------------------------------------------------
    # Exchange info
    # --------------------------------------------------------

    if not load_exchange_info():

        raise RuntimeError(
            "exchangeInfo failed"
        )

    safe_sleep(1)

    # --------------------------------------------------------
    # Top symbols
    # --------------------------------------------------------

    if not load_top_symbols():

        raise RuntimeError(
            "symbol selection failed"
        )

    # --------------------------------------------------------
    # Account
    # --------------------------------------------------------

    if not check_account_connection():

        raise RuntimeError(
            "account connection failed"
        )

    safe_sleep(1)

    # --------------------------------------------------------
    # Position recovery
    # --------------------------------------------------------

    recover_positions()

    safe_sleep(1)

    # --------------------------------------------------------
    # Historical candles
    # --------------------------------------------------------

    if not initialize_candles():

        raise RuntimeError(
            "historical candle initialization failed"
        )

    # --------------------------------------------------------
    # WebSockets
    # --------------------------------------------------------

    start_websockets()

    # --------------------------------------------------------
    # Watchdog
    # --------------------------------------------------------

    start_watchdog()

    bot_live = True

    last_time_sync = time.time()

    logger.info(
        "=================================================="
    )

    logger.info(
        "🚀 BOT IS LIVE"
    )

    logger.info(
        "Scanning %d symbols in %d WebSocket groups",
        len(symbols),
        (
            len(symbols)
            + GROUP_SIZE
            - 1
        ) // GROUP_SIZE
    )

    logger.info(
        "=================================================="
    )


# ============================================================
# BOT SUPERVISOR
# ============================================================

def bot_supervisor():

    global bot_live

    logger.info(
        "BOT SUPERVISOR STARTED"
    )

    retry_delay = STARTUP_RETRY_MIN

    while not shutdown_event.is_set():

        try:

            with state_lock:

                currently_live = bot_live

            # ------------------------------------------------
            # First startup
            # ------------------------------------------------

            if not currently_live:

                logger.info(
                    "Bot is not live. "
                    "Attempting startup..."
                )

                try:

                    initialize_bot_once()

                    retry_delay = (
                        STARTUP_RETRY_MIN
                    )

                except Exception as e:

                    bot_live = False

                    logger.exception(
                        "Bot startup failed: %s",
                        e
                    )

                    logger.warning(
                        "Bot will retry startup "
                        "in %ss",
                        retry_delay
                    )

                    safe_sleep(
                        retry_delay
                    )

                    retry_delay = min(

                        retry_delay * 2,

                        STARTUP_RETRY_MAX
                    )

                    continue

            # ------------------------------------------------
            # Bot is live
            # ------------------------------------------------

            safe_sleep(10)

        except Exception as e:

            logger.exception(
                "SUPERVISOR ERROR: %s",
                e
            )

            bot_live = False

            safe_sleep(
                STARTUP_RETRY_MIN
            )

    logger.info(
        "BOT SUPERVISOR STOPPED"
    )


# ============================================================
# START WATCHDOG
# ============================================================

def start_watchdog():

    global watchdog_thread

    with state_lock:

        if (
            watchdog_thread
            and watchdog_thread.is_alive()
        ):

            return

        watchdog_thread = threading.Thread(

            target=websocket_watchdog,

            daemon=True,

            name="WS-WATCHDOG"
        )

        watchdog_thread.start()

    logger.info(
        "WebSocket watchdog started"
    )


# ============================================================
# BOT START
# ============================================================

def start_bot_background():

    global bot_started

    with startup_lock:

        if bot_started:

            return

        bot_started = True

    thread = threading.Thread(

        target=bot_supervisor,

        daemon=True,

        name="BOT-SUPERVISOR"
    )

    thread.start()

    logger.info(
        "Bot supervisor background thread started"
    )


# ============================================================
# SHUTDOWN HANDLER
# ============================================================

def handle_shutdown(
    signum,
    frame
):

    # --------------------------------------------------------
    # IMPORTANT:
    #
    # We DO NOT ignore SIGTERM.
    #
    # Render/Gunicorn may intentionally send SIGTERM during
    # deployment/restart.
    #
    # Python cannot safely keep the process alive after Render
    # decides to terminate the container.
    #
    # We only perform graceful cleanup here.
    # --------------------------------------------------------

    logger.warning(
        "Received shutdown signal: %s",
        signum
    )

    shutdown_event.set()

    with state_lock:

        bot_live = False

        websocket_objects = list(
            ws_objects.values()
        )

    # Close WebSockets
    for ws in websocket_objects:

        try:

            ws.close()

        except Exception:

            pass

    logger.warning(
        "Graceful shutdown initiated"
    )


# ============================================================
# SIGNAL REGISTRATION
# ============================================================

try:

    signal.signal(
        signal.SIGTERM,
        handle_shutdown
    )

    signal.signal(
        signal.SIGINT,
        handle_shutdown
    )

except Exception as e:

    logger.warning(
        "Signal handler registration failed: %s",
        e
    )


# ============================================================
# FLASK ROUTES
# ============================================================

@app.route("/")
def home():

    with state_lock:

        return jsonify({

            "status": "running",

            "bot_live": bot_live,

            "symbols": len(symbols),

            "positions": len(positions),

            "timeframe": TIMEFRAME,

            "buy_usdt": fmt_decimal(
                BUY_USDT
            ),

            "buy_rule": (
                f"RSI50 > {BUY_RSI50_MIN} "
                f"+ previous RSI3 < "
                f"{BUY_RSI3_MAX} "
                f"+ RSI3 turning upward"
            ),

            "sell_rule": (
                f"RSI3 cross above "
                f"{SELL_RSI3_LEVEL}"
            ),

            "stop_loss": (
                f"{STOP_LOSS_PERCENT}%"
            ),

            "dry_run": DRY_RUN,

            "websocket_groups": len(
                ws_thread_map
            ),

            "reserved_usdt": fmt_decimal(
                reserved_usdt
            )
        })


# ============================================================
# HEALTH
# ============================================================

@app.route("/health")
def health():

    with state_lock:

        alive_groups = 0

        for thread in ws_thread_map.values():

            if thread.is_alive():

                alive_groups += 1

        return jsonify({

            "ok": True,

            "bot_live": bot_live,

            "symbols": len(symbols),

            "positions": len(positions),

            "websocket_groups": len(
                ws_thread_map
            ),

            "websocket_alive": alive_groups,

            "reserved_usdt": fmt_decimal(
                reserved_usdt
            ),

            "timestamp": int(
                time.time()
            )
        })


# ============================================================
# POSITIONS
# ============================================================

@app.route("/positions")
def get_positions():

    with state_lock:

        data = {}

        for symbol, position in positions.items():

            data[symbol] = {

                "quantity": fmt_decimal(
                    position.get(
                        "quantity",
                        "0"
                    )
                ),

                "entry_price": fmt_decimal(
                    position.get(
                        "entry_price",
                        "0"
                    )
                ),

                "stop_price": fmt_decimal(
                    position.get(
                        "stop_price",
                        "0"
                    )
                ),

                "recovered": bool(
                    position.get(
                        "recovered",
                        False
                    )
                )
            }

        return jsonify(data)


# ============================================================
# MAIN
# ============================================================

if __name__ == "__main__":

    start_bot_background()

    app.run(

        host="0.0.0.0",

        port=int(
            os.getenv(
                "PORT",
                "10000"
            )
        ),

        threaded=True
    )

else:

    # Gunicorn import হলে background bot start
    start_bot_background()
