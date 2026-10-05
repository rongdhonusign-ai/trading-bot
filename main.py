import os
import time
import json
import hmac
import hashlib
import signal
import logging
import threading
import traceback
from decimal import Decimal, ROUND_DOWN
from urllib.parse import urlencode
from collections import deque
from concurrent.futures import ThreadPoolExecutor

import requests
import pandas as pd
import websocket

from flask import Flask, jsonify


# ============================================================
# LOGGING
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

logger = logging.getLogger("RSI_BOT")


# ============================================================
# BINANCE CONFIG
# ============================================================

API_KEY = os.environ.get("BINANCE_API_KEY")
API_SECRET = os.environ.get("BINANCE_API_SECRET")

BASE_URL = "https://api.binance.com"
WS_BASE = "wss://stream.binance.com:9443/stream?streams="

if not API_KEY or not API_SECRET:
    logger.warning("BINANCE_API_KEY / BINANCE_API_SECRET not found.")


# ============================================================
# STRATEGY SETTINGS
# ============================================================

TIMEFRAME = "5m"

TOP_SYMBOLS = 150

GROUPS = 3
SYMBOLS_PER_GROUP = 50

BUY_USDT = Decimal("15")

RSI_FAST_PERIOD = 3
RSI_SLOW_PERIOD = 50

BUY_RSI_SLOW_MIN = 50
BUY_RSI_FAST_MAX = 10

SELL_RSI_LEVEL = 80

STOP_LOSS_PERCENT = Decimal("0.01")


# ============================================================
# API / PERFORMANCE SETTINGS
# ============================================================

# More history makes RSI50 much closer to Binance/TradingView.
HISTORY_LIMIT = 200

REQUEST_TIMEOUT = 15

RECV_WINDOW = 5000

BUY_COOLDOWN_SECONDS = 60

STARTUP_REST_DELAY = 0.25

SELL_BALANCE_BUFFER = Decimal("0.999")

RECOVERY_LOOKBACK_DAYS = 7

RECOVERY_ORDER_LIMIT = 50

ORDER_WORKERS = 4


# ============================================================
# WEBSOCKET SETTINGS
# ============================================================

WS_RECONNECT_MIN = 5
WS_RECONNECT_MAX = 60

WS_MAX_LIFETIME = 23 * 60 * 60


# ============================================================
# EXCLUDED ASSETS
# ============================================================

EXCLUDED_ASSETS = {
    "USDT",
    "USDC",
    "FDUSD",
    "BUSD",
    "TUSD",
    "DAI",
    "USDP",
    "USD",
    "EUR",
    "GBP",
    "AUD",
    "TRY",
    "BRL",
    "RUB",
    "UAH",
    "PLN",
    "RON",
    "JPY",
    "ARS",
    "ZAR",
    "NGN",
    "MXN",
    "COP",
    "CLP",
    "PEN",

    "BTC",
    "ETH",
}


# ============================================================
# FLASK
# ============================================================

app = Flask(__name__)


# ============================================================
# HTTP SESSION
# ============================================================

session = requests.Session()

if API_KEY:
    session.headers.update({
        "X-MBX-APIKEY": API_KEY
    })


# ============================================================
# GLOBAL STATE
# ============================================================

running = True

state_lock = threading.RLock()

positions = {}

orders_in_flight = set()

last_buy_time = {}

symbol_state = {}

exchange_filters = {}

top_symbols = []

ws_connected_groups = set()

bot_started = False
bot_ready = False
bot_error = None

bot_start_time = None

bot_start_lock = threading.Lock()

executor = ThreadPoolExecutor(
    max_workers=ORDER_WORKERS
)

server_time_offset_ms = 0

binance_cooldown_until = 0

binance_cooldown_lock = threading.Lock()


# ============================================================
# CUSTOM EXCEPTION
# ============================================================

class BinanceTemporaryBlocked(Exception):
    pass


# ============================================================
# BASIC HELPERS
# ============================================================

def now_ms():
    return int(time.time() * 1000) + server_time_offset_ms


def decimal_from(value, default=Decimal("0")):
    try:
        return Decimal(str(value))
    except Exception:
        return default


def floor_decimal(value, step):
    value = Decimal(str(value))
    step = Decimal(str(step))

    if step <= 0:
        return value

    return (
        value / step
    ).to_integral_value(
        rounding=ROUND_DOWN
    ) * step


def decimal_to_string(value):
    value = Decimal(str(value))

    s = format(value, "f")

    if "." in s:
        s = s.rstrip("0").rstrip(".")

    return s if s else "0"


def get_retry_after(response):
    try:
        value = response.headers.get("Retry-After")

        if value:
            return max(1, int(float(value)))

    except Exception:
        pass

    return 300


# ============================================================
# BINANCE COOLDOWN
# ============================================================

def set_binance_cooldown(seconds):
    global binance_cooldown_until

    seconds = max(60, int(seconds))

    with binance_cooldown_lock:

        new_until = time.time() + seconds

        if new_until > binance_cooldown_until:
            binance_cooldown_until = new_until

    logger.error(
        "Binance cooldown activated for %s seconds.",
        seconds
    )


def is_binance_cooldown():

    with binance_cooldown_lock:

        return time.time() < binance_cooldown_until


def cooldown_remaining():

    with binance_cooldown_lock:

        return max(
            0,
            int(
                binance_cooldown_until
                - time.time()
            )
        )


# ============================================================
# SERVER TIME SYNC
# ============================================================

def sync_server_time():

    global server_time_offset_ms

    try:

        response = session.get(
            BASE_URL + "/api/v3/time",
            timeout=REQUEST_TIMEOUT
        )

        response.raise_for_status()

        data = response.json()

        server_time = int(
            data["serverTime"]
        )

        local_time = int(
            time.time() * 1000
        )

        server_time_offset_ms = (
            server_time - local_time
        )

        logger.info(
            "Binance server time offset: %sms",
            server_time_offset_ms
        )

        return True

    except Exception as e:

        logger.error(
            "Server time sync error: %s",
            e
        )

        return False


# ============================================================
# BINANCE REST REQUEST
# ============================================================

def binance_request(
    method,
    path,
    params=None,
    signed=False,
    retry_get=True
):

    if params is None:
        params = {}

    method = method.upper()

    if is_binance_cooldown():

        remaining = cooldown_remaining()

        raise BinanceTemporaryBlocked(
            f"Binance cooldown active: "
            f"{remaining}s remaining"
        )

    params = dict(params)

    # --------------------------------------------------------
    # SIGNED REQUEST
    # --------------------------------------------------------

    if signed:

        params.pop("signature", None)

        params["timestamp"] = now_ms()

        params["recvWindow"] = RECV_WINDOW

        query = urlencode(
            params,
            doseq=True
        )

        signature = hmac.new(
            API_SECRET.encode(),
            query.encode(),
            hashlib.sha256
        ).hexdigest()

        params["signature"] = signature

    url = BASE_URL + path

    max_attempts = (
        4
        if (
            method == "GET"
            and retry_get
        )
        else 1
    )

    for attempt in range(
        1,
        max_attempts + 1
    ):

        try:

            response = session.request(
                method,
                url,
                params=(
                    params
                    if method == "GET"
                    else None
                ),
                data=(
                    params
                    if method != "GET"
                    else None
                ),
                timeout=REQUEST_TIMEOUT
            )

            # ------------------------------------------------
            # SUCCESS
            # ------------------------------------------------

            if response.status_code in (
                200,
                201
            ):

                if not response.text:
                    return {}

                return response.json()

            # ------------------------------------------------
            # 418
            # ------------------------------------------------

            if response.status_code == 418:

                retry_after = get_retry_after(
                    response
                )

                cooldown = max(
                    retry_after,
                    300
                )

                set_binance_cooldown(
                    cooldown
                )

                logger.error(
                    "BINANCE HTTP 418 - temporary "
                    "IP restriction. Cooldown=%ss",
                    cooldown
                )

                raise BinanceTemporaryBlocked(
                    "Binance HTTP 418 "
                    "temporary IP restriction"
                )

            # ------------------------------------------------
            # 429
            # ------------------------------------------------

            if response.status_code == 429:

                retry_after = get_retry_after(
                    response
                )

                logger.warning(
                    "Binance HTTP 429 | "
                    "waiting %ss | attempt %s/%s",
                    retry_after,
                    attempt,
                    max_attempts
                )

                if attempt >= max_attempts:
                    return None

                time.sleep(
                    retry_after
                )

                continue

            # ------------------------------------------------
            # ERROR JSON
            # ------------------------------------------------

            try:
                error_json = response.json()
            except Exception:
                error_json = {}

            error_code = error_json.get(
                "code"
            )

            # ------------------------------------------------
            # TIMESTAMP ERROR
            # ------------------------------------------------

            if error_code == -1021:

                logger.warning(
                    "Timestamp error. "
                    "Synchronizing Binance time..."
                )

                if sync_server_time():

                    if signed:

                        # IMPORTANT:
                        # Remove old signature before
                        # generating a new one.
                        params.pop(
                            "signature",
                            None
                        )

                        params["timestamp"] = now_ms()

                        query = urlencode(
                            params,
                            doseq=True
                        )

                        signature = hmac.new(
                            API_SECRET.encode(),
                            query.encode(),
                            hashlib.sha256
                        ).hexdigest()

                        params["signature"] = signature

                        if method == "GET":
                            continue

                return None

            # ------------------------------------------------
            # OTHER ERROR
            # ------------------------------------------------

            logger.error(
                "Binance API error | %s %s | "
                "status=%s | response=%s",
                method,
                path,
                response.status_code,
                response.text[:500]
            )

            return None

        except BinanceTemporaryBlocked:
            raise

        except requests.exceptions.Timeout as e:

            logger.error(
                "Binance request timeout | "
                "%s %s | %s",
                method,
                path,
                e
            )

            # Never automatically retry order POST.
            if method != "GET":
                return None

            if attempt >= max_attempts:
                return None

            time.sleep(
                min(2 * attempt, 5)
            )

        except requests.exceptions.RequestException as e:

            logger.error(
                "Binance request error | "
                "%s %s | %s",
                method,
                path,
                e
            )

            if method != "GET":
                return None

            if attempt >= max_attempts:
                return None

            time.sleep(
                min(2 * attempt, 5)
            )

        except Exception as e:

            logger.error(
                "Unexpected REST error | "
                "%s %s | %s",
                method,
                path,
                e
            )

            return None

    return None


# ============================================================
# RSI - WILDER / RMA
# ============================================================
#
# This implementation is designed to match the standard
# Wilder RSI used by charting platforms much more closely
# than pandas EWM starting from zero.
#
# RSI:
#   1. First average gain/loss = SMA of first period
#   2. Next values use Wilder RMA:
#
#      avg = ((previous_avg * (period - 1)) + current) / period
#
# ============================================================

def calculate_rsi_series(closes, period):

    if closes is None:
        return pd.Series(
            dtype="float64"
        )

    values = [
        float(x)
        for x in closes
    ]

    n = len(values)

    if n <= period:
        return pd.Series(
            [float("nan")] * n,
            dtype="float64"
        )

    result = [
        float("nan")
    ] * n

    gains = [0.0] * n
    losses = [0.0] * n

    for i in range(1, n):

        delta = (
            values[i]
            - values[i - 1]
        )

        if delta > 0:

            gains[i] = delta
            losses[i] = 0.0

        elif delta < 0:

            gains[i] = 0.0
            losses[i] = -delta

        else:

            gains[i] = 0.0
            losses[i] = 0.0

    # --------------------------------------------------------
    # First Wilder average
    # --------------------------------------------------------

    avg_gain = (
        sum(gains[1:period + 1])
        / period
    )

    avg_loss = (
        sum(losses[1:period + 1])
        / period
    )

    # --------------------------------------------------------
    # First RSI
    # --------------------------------------------------------

    if avg_loss == 0:

        if avg_gain == 0:
            result[period] = 50.0
        else:
            result[period] = 100.0

    else:

        rs = avg_gain / avg_loss

        result[period] = (
            100.0
            - (
                100.0
                / (1.0 + rs)
            )
        )

    # --------------------------------------------------------
    # Wilder recursive calculation
    # --------------------------------------------------------

    for i in range(
        period + 1,
        n
    ):

        avg_gain = (
            (
                avg_gain
                * (period - 1)
            )
            + gains[i]
        ) / period

        avg_loss = (
            (
                avg_loss
                * (period - 1)
            )
            + losses[i]
        ) / period

        if avg_loss == 0:

            if avg_gain == 0:
                result[i] = 50.0
            else:
                result[i] = 100.0

        else:

            rs = (
                avg_gain
                / avg_loss
            )

            result[i] = (
                100.0
                - (
                    100.0
                    / (1.0 + rs)
                )
            )

    return pd.Series(
        result,
        dtype="float64"
    )


def calculate_current_rsi(
    closes,
    period
):

    if len(closes) <= period:
        return None

    rsi = calculate_rsi_series(
        closes,
        period
    )

    if rsi.empty:
        return None

    value = rsi.iloc[-1]

    if pd.isna(value):
        return None

    return float(value)


# ============================================================
# EXCHANGE INFO
# ============================================================

def load_exchange_info():

    logger.info(
        "Loading Binance exchange information..."
    )

    data = binance_request(
        "GET",
        "/api/v3/exchangeInfo"
    )

    if data is None:
        raise RuntimeError(
            "Could not load exchangeInfo"
        )

    symbols = data.get(
        "symbols",
        []
    )

    loaded = 0

    with state_lock:

        exchange_filters.clear()

        for item in symbols:

            symbol = item.get(
                "symbol"
            )

            if not symbol:
                continue

            if item.get(
                "status"
            ) != "TRADING":
                continue

            if (
                item.get(
                    "isSpotTradingAllowed"
                )
                is False
            ):
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

            exchange_filters[
                symbol
            ] = filters

            loaded += 1

    logger.info(
        "Exchange info loaded: %s "
        "trading symbols",
        loaded
    )


# ============================================================
# TOP SYMBOLS
# ============================================================

def get_top_symbols():

    logger.info(
        "Getting top %s USDT symbols "
        "by 24h quote volume...",
        TOP_SYMBOLS
    )

    data = binance_request(
        "GET",
        "/api/v3/ticker/24hr"
    )

    if not data:
        raise RuntimeError(
            "Could not load 24hr ticker data"
        )

    candidates = []

    for item in data:

        symbol = item.get(
            "symbol",
            ""
        )

        if not symbol.endswith(
            "USDT"
        ):
            continue

        base_asset = symbol[:-4]

        if base_asset in EXCLUDED_ASSETS:
            continue

        with state_lock:
            filters = exchange_filters.get(
                symbol
            )

        if not filters:
            continue

        min_notional = Decimal("0")

        notional_filter = filters.get(
            "NOTIONAL"
        )

        if notional_filter:

            min_notional = decimal_from(
                notional_filter.get(
                    "minNotional",
                    "0"
                )
            )

        else:

            notional_filter = filters.get(
                "MIN_NOTIONAL"
            )

            if notional_filter:

                min_notional = decimal_from(
                    notional_filter.get(
                        "minNotional",
                        "0"
                    )
                )

        if min_notional > BUY_USDT:
            continue

        try:

            quote_volume = Decimal(
                str(
                    item.get(
                        "quoteVolume",
                        "0"
                    )
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

    selected = [
        symbol
        for symbol, _ in
        candidates[:TOP_SYMBOLS]
    ]

    if not selected:
        raise RuntimeError(
            "No eligible USDT symbols found"
        )

    with state_lock:

        top_symbols.clear()

        top_symbols.extend(
            selected
        )

    logger.info(
        "Selected %s symbols",
        len(selected)
    )

    logger.info(
        "First symbols: %s",
        ", ".join(
            selected[:20]
        )
    )

    return selected


# ============================================================
# GET INITIAL KLINES
# ============================================================

def get_initial_closes(symbol):

    data = binance_request(
        "GET",
        "/api/v3/klines",
        params={
            "symbol": symbol,
            "interval": TIMEFRAME,
            "limit": HISTORY_LIMIT
        }
    )

    if not data:
        return None

    closes = []

    for candle in data:

        try:

            close = float(
                candle[4]
            )

            closes.append(
                close
            )

        except Exception:
            continue

    if len(closes) <= RSI_SLOW_PERIOD:
        return None

    # --------------------------------------------------------
    # Remove currently open candle.
    # --------------------------------------------------------

    try:

        last_candle = data[-1]

        close_time = int(
            last_candle[6]
        )

        if (
            close_time
            >
            int(time.time() * 1000)
        ):

            if closes:
                closes.pop()

    except Exception:
        pass

    return closes[-HISTORY_LIMIT:]


# ============================================================
# SEED SYMBOL
# ============================================================

def seed_symbol(symbol):

    try:

        closes = get_initial_closes(
            symbol
        )

        if not closes:

            logger.warning(
                "%s | Could not seed RSI data",
                symbol
            )

            return False

        rsi3 = calculate_current_rsi(
            closes,
            RSI_FAST_PERIOD
        )

        rsi50 = calculate_current_rsi(
            closes,
            RSI_SLOW_PERIOD
        )

        with state_lock:

            symbol_state[
                symbol
            ] = {

                "closes": deque(
                    closes,
                    maxlen=HISTORY_LIMIT
                ),

                "rsi3": rsi3,

                "rsi50": rsi50,

                "last_closed_time": None
            }

        return True

    except BinanceTemporaryBlocked:
        raise

    except Exception as e:

        logger.error(
            "%s | seed error: %s",
            symbol,
            e
        )

        return False


# ============================================================
# SEED ALL
# ============================================================

def seed_all_symbols():

    logger.info(
        "Seeding RSI history for %s symbols...",
        len(top_symbols)
    )

    success = 0

    for index, symbol in enumerate(
        top_symbols,
        start=1
    ):

        if is_binance_cooldown():

            raise BinanceTemporaryBlocked(
                "Binance cooldown during "
                "symbol seeding"
            )

        ok = seed_symbol(
            symbol
        )

        if ok:
            success += 1

        logger.info(
            "Seed %s/%s | %s | success=%s",
            index,
            len(top_symbols),
            symbol,
            ok
        )

        time.sleep(
            STARTUP_REST_DELAY
        )

    logger.info(
        "RSI seeding completed: %s/%s",
        success,
        len(top_symbols)
    )

    if success < 20:

        raise RuntimeError(
            "Too few symbols seeded successfully"
        )


# ============================================================
# UPDATE CLOSED CANDLE
# ============================================================

def update_symbol_candle(
    symbol,
    candle_close_time,
    close_price
):

    with state_lock:

        state = symbol_state.get(
            symbol
        )

        if state is None:

            state = {

                "closes": deque(
                    maxlen=HISTORY_LIMIT
                ),

                "rsi3": None,

                "rsi50": None,

                "last_closed_time": None
            }

            symbol_state[
                symbol
            ] = state

        last_time = state.get(
            "last_closed_time"
        )

        if (
            last_time is not None
            and
            candle_close_time <= last_time
        ):
            return None

        closes = state[
            "closes"
        ]

        closes.append(
            float(close_price)
        )

        state[
            "last_closed_time"
        ] = candle_close_time

        # IMPORTANT:
        # RSI is calculated using ONLY the
        # newly closed candle.

        rsi3 = calculate_current_rsi(
            closes,
            RSI_FAST_PERIOD
        )

        rsi50 = calculate_current_rsi(
            closes,
            RSI_SLOW_PERIOD
        )

        previous_rsi3 = state.get(
            "rsi3"
        )

        state["rsi3"] = rsi3
        state["rsi50"] = rsi50

    return {
        "rsi3": rsi3,
        "rsi50": rsi50,
        "previous_rsi3": previous_rsi3
    }


# ============================================================
# ACCOUNT
# ============================================================

def get_account():

    return binance_request(
        "GET",
        "/api/v3/account",
        signed=True
    )


def get_free_balance(asset):

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

            return decimal_from(
                balance.get(
                    "free",
                    "0"
                )
            )

    return Decimal("0")


# ============================================================
# SYMBOL FILTER
# ============================================================

def get_symbol_filter(
    symbol,
    filter_type
):

    with state_lock:

        filters = exchange_filters.get(
            symbol,
            {}
        )

        return filters.get(
            filter_type
        )


# ============================================================
# MARKET QUANTITY
# ============================================================

def calculate_market_quantity(
    symbol,
    usdt_amount,
    price
):

    with state_lock:

        filters = exchange_filters.get(
            symbol,
            {}
        )

    lot_filter = filters.get(
        "LOT_SIZE"
    )

    market_lot_filter = filters.get(
        "MARKET_LOT_SIZE"
    )

    if not lot_filter:
        return None

    lot_step = decimal_from(
        lot_filter.get(
            "stepSize",
            "0"
        )
    )

    lot_min = decimal_from(
        lot_filter.get(
            "minQty",
            "0"
        )
    )

    if lot_step <= 0:
        return None

    # Use LOT_SIZE as the primary filter
    # because the reported Binance error
    # was specifically LOT_SIZE.

    qty = (
        Decimal(str(usdt_amount))
        /
        Decimal(str(price))
    )

    qty = floor_decimal(
        qty,
        lot_step
    )

    if qty < lot_min:
        return None

    # --------------------------------------------------------
    # Validate MARKET_LOT_SIZE too.
    # --------------------------------------------------------

    if market_lot_filter:

        market_step = decimal_from(
            market_lot_filter.get(
                "stepSize",
                "0"
            )
        )

        market_min = decimal_from(
            market_lot_filter.get(
                "minQty",
                "0"
            )
        )

        if market_step > 0:

            qty = floor_decimal(
                qty,
                market_step
            )

        if qty < market_min:
            return None

        # Re-check LOT_SIZE after MARKET_LOT_SIZE
        # adjustment.

        qty = floor_decimal(
            qty,
            lot_step
        )

        if qty < lot_min:
            return None

    # --------------------------------------------------------
    # NOTIONAL CHECK
    # --------------------------------------------------------

    estimated_notional = (
        qty
        *
        Decimal(str(price))
    )

    notional_filter = (
        filters.get("NOTIONAL")
        or
        filters.get("MIN_NOTIONAL")
    )

    if notional_filter:

        min_notional = decimal_from(
            notional_filter.get(
                "minNotional",
                "0"
            )
        )

        if (
            min_notional > 0
            and
            estimated_notional < min_notional
        ):

            logger.warning(
                "%s | Quantity rejected by "
                "minNotional | qty=%s | "
                "price=%s | notional=%s | "
                "minNotional=%s",
                symbol,
                qty,
                price,
                estimated_notional,
                min_notional
            )

            return None

    return qty


# ============================================================
# BALANCE QUANTITY
# ============================================================

def calculate_sell_quantity(
    symbol,
    balance
):

    with state_lock:

        filters = exchange_filters.get(
            symbol,
            {}
        )

    lot_filter = filters.get(
        "LOT_SIZE"
    )

    market_lot_filter = filters.get(
        "MARKET_LOT_SIZE"
    )

    if not lot_filter:
        return None

    lot_step = decimal_from(
        lot_filter.get(
            "stepSize",
            "0"
        )
    )

    lot_min = decimal_from(
        lot_filter.get(
            "minQty",
            "0"
        )
    )

    if lot_step <= 0:
        return None

    qty = (
        Decimal(str(balance))
        *
        SELL_BALANCE_BUFFER
    )

    qty = floor_decimal(
        qty,
        lot_step
    )

    if (
        market_lot_filter
        and
        decimal_from(
            market_lot_filter.get(
                "stepSize",
                "0"
            )
        ) > 0
    ):

        market_step = decimal_from(
            market_lot_filter.get(
                "stepSize",
                "0"
            )
        )

        market_min = decimal_from(
            market_lot_filter.get(
                "minQty",
                "0"
            )
        )

        qty = floor_decimal(
            qty,
            market_step
        )

        # Re-align with LOT_SIZE.
        qty = floor_decimal(
            qty,
            lot_step
        )

        if qty < market_min:
            return None

    if qty < lot_min:
        return None

    return qty


# ============================================================
# PRICE ROUNDING
# ============================================================

def round_price_to_tick(
    symbol,
    price
):

    price_filter = get_symbol_filter(
        symbol,
        "PRICE_FILTER"
    )

    if not price_filter:
        return Decimal(str(price))

    tick_size = decimal_from(
        price_filter.get(
            "tickSize",
            "0"
        )
    )

    if tick_size <= 0:
        return Decimal(str(price))

    return floor_decimal(
        Decimal(str(price)),
        tick_size
    )


# ============================================================
# CURRENT PRICE
# ============================================================

def get_current_price(symbol):

    data = binance_request(
        "GET",
        "/api/v3/ticker/price",
        params={
            "symbol": symbol
        }
    )

    if not data:
        return None

    try:

        return Decimal(
            str(
                data["price"]
            )
        )

    except Exception:
        return None


# ============================================================
# POSITION
# ============================================================

def save_position(
    symbol,
    qty,
    entry_price,
    stop_order_id=None
):

    with state_lock:

        positions[
            symbol
        ] = {

            "qty": str(qty),

            "entry_price": str(
                entry_price
            ),

            "stop_order_id":
                stop_order_id,

            "buy_time":
                time.time()
        }


def remove_position(symbol):

    with state_lock:

        positions.pop(
            symbol,
            None
        )


# ============================================================
# STOP LOSS
# ============================================================

def place_stop_loss(
    symbol,
    entry_price
):

    try:

        base_asset = symbol[:-4]

        free_balance = get_free_balance(
            base_asset
        )

        if free_balance <= 0:

            logger.error(
                "%s | No balance for SL",
                symbol
            )

            return None

        qty = calculate_sell_quantity(
            symbol,
            free_balance
        )

        if qty is None:

            logger.error(
                "%s | Invalid SL quantity",
                symbol
            )

            return None

        stop_price = (
            Decimal(str(entry_price))
            *
            (
                Decimal("1")
                -
                STOP_LOSS_PERCENT
            )
        )

        stop_price = round_price_to_tick(
            symbol,
            stop_price
        )

        current_price = get_current_price(
            symbol
        )

        if current_price is not None:

            if stop_price >= current_price:

                stop_price = (
                    current_price
                    *
                    Decimal("0.995")
                )

                stop_price = (
                    round_price_to_tick(
                        symbol,
                        stop_price
                    )
                )

        client_order_id = (
            "RSISL_"
            + symbol
            + "_"
            + str(
                int(
                    time.time() * 1000
                )
            )
        )

        order_params = {

            "symbol": symbol,

            "side": "SELL",

            "type": "STOP_LOSS",

            "quantity":
                decimal_to_string(
                    qty
                ),

            "stopPrice":
                decimal_to_string(
                    stop_price
                ),

            "newClientOrderId":
                client_order_id
        }

        result = binance_request(
            "POST",
            "/api/v3/order",
            params=order_params,
            signed=True,
            retry_get=False
        )

        if not result:

            logger.error(
                "%s | Failed to place "
                "server SL",
                symbol
            )

            return None

        order_id = result.get(
            "orderId"
        )

        logger.info(
            "%s | SERVER SL placed | "
            "entry=%s | stop=%s | "
            "qty=%s | orderId=%s",
            symbol,
            entry_price,
            stop_price,
            qty,
            order_id
        )

        return order_id

    except BinanceTemporaryBlocked:
        raise

    except Exception as e:

        logger.error(
            "%s | SL error: %s",
            symbol,
            e
        )

        return None


# ============================================================
# CANCEL SL
# ============================================================

def cancel_stop_loss(
    symbol,
    order_id
):

    if not order_id:
        return True

    try:

        result = binance_request(
            "DELETE",
            "/api/v3/order",
            params={
                "symbol": symbol,
                "orderId": order_id
            },
            signed=True,
            retry_get=False
        )

        if result is not None:

            logger.info(
                "%s | SL cancelled | "
                "orderId=%s",
                symbol,
                order_id
            )

            return True

        return False

    except BinanceTemporaryBlocked:
        raise

    except Exception as e:

        logger.error(
            "%s | cancel SL error: %s",
            symbol,
            e
        )

        return False


# ============================================================
# BUY ORDER
# ============================================================

def place_buy(symbol):

    with state_lock:

        if symbol in positions:
            return

        if symbol in orders_in_flight:
            return

        last_buy = last_buy_time.get(
            symbol,
            0
        )

        if (
            time.time()
            -
            last_buy
            <
            BUY_COOLDOWN_SECONDS
        ):
            return

        orders_in_flight.add(
            symbol
        )

        last_buy_time[
            symbol
        ] = time.time()

    try:

        # ----------------------------------------------------
        # Get current price
        # ----------------------------------------------------

        price = get_current_price(
            symbol
        )

        if price is None or price <= 0:

            logger.error(
                "%s | Cannot get current price "
                "for BUY",
                symbol
            )

            return

        # ----------------------------------------------------
        # Calculate valid LOT_SIZE quantity
        # ----------------------------------------------------

        qty = calculate_market_quantity(
            symbol,
            BUY_USDT,
            price
        )

        if qty is None:

            logger.error(
                "%s | BUY skipped | "
                "Cannot calculate valid "
                "LOT_SIZE quantity | price=%s",
                symbol,
                price
            )

            return

        estimated_notional = (
            qty * price
        )

        logger.info(
            "%s | BUY ORDER PRECHECK | "
            "price=%s | qty=%s | "
            "estimated USDT=%s",
            symbol,
            price,
            qty,
            estimated_notional
        )

        logger.info(
            "%s | BUY signal | "
            "RSI50 > %s | RSI3 < %s",
            symbol,
            BUY_RSI_SLOW_MIN,
            BUY_RSI_FAST_MAX
        )

        client_order_id = (
            "RSIBUY_"
            + symbol
            + "_"
            + str(
                int(
                    time.time() * 1000
                )
            )
        )

        # ----------------------------------------------------
        # IMPORTANT:
        # Use explicit quantity instead of quoteOrderQty.
        #
        # This prevents Binance from calculating a quantity
        # that violates LOT_SIZE.
        # ----------------------------------------------------

        result = binance_request(
            "POST",
            "/api/v3/order",
            params={

                "symbol": symbol,

                "side": "BUY",

                "type": "MARKET",

                "quantity":
                    decimal_to_string(
                        qty
                    ),

                "newClientOrderId":
                    client_order_id
            },
            signed=True,
            retry_get=False
        )

        if not result:

            logger.error(
                "%s | BUY failed",
                symbol
            )

            return

        status = result.get(
            "status"
        )

        if status not in (
            "FILLED",
            "PARTIALLY_FILLED"
        ):

            logger.error(
                "%s | BUY status=%s",
                symbol,
                status
            )

            return

        executed_qty = decimal_from(
            result.get(
                "executedQty",
                "0"
            )
        )

        quote_qty = decimal_from(
            result.get(
                "cummulativeQuoteQty",
                "0"
            )
        )

        if executed_qty <= 0:

            logger.error(
                "%s | BUY executed "
                "quantity=0",
                symbol
            )

            return

        if quote_qty > 0:

            avg_entry = (
                quote_qty
                /
                executed_qty
            )

        else:

            avg_entry = get_current_price(
                symbol
            )

            if avg_entry is None:

                logger.error(
                    "%s | Cannot determine "
                    "entry price",
                    symbol
                )

                return

        logger.info(
            "%s | BUY FILLED | "
            "qty=%s | entry=%s | "
            "USDT spent=%s",
            symbol,
            executed_qty,
            avg_entry,
            quote_qty
        )

        # ----------------------------------------------------
        # Save before SL
        # ----------------------------------------------------

        save_position(
            symbol,
            executed_qty,
            avg_entry,
            None
        )

        stop_order_id = place_stop_loss(
            symbol,
            avg_entry
        )

        if stop_order_id:

            with state_lock:

                if symbol in positions:

                    positions[
                        symbol
                    ][
                        "stop_order_id"
                    ] = stop_order_id

        else:

            logger.error(
                "%s | SERVER SL failed. "
                "Attempting emergency MARKET SELL.",
                symbol
            )

            emergency_market_sell(
                symbol
            )

    except BinanceTemporaryBlocked:

        logger.error(
            "%s | Binance temporarily "
            "blocked. BUY stopped.",
            symbol
        )

    except Exception as e:

        logger.error(
            "%s | BUY exception: %s",
            symbol,
            e
        )

        logger.error(
            traceback.format_exc()
        )

    finally:

        with state_lock:

            orders_in_flight.discard(
                symbol
            )


# ============================================================
# EMERGENCY SELL
# ============================================================

def emergency_market_sell(symbol):

    try:

        base_asset = symbol[:-4]

        free_balance = get_free_balance(
            base_asset
        )

        if free_balance <= 0:

            remove_position(
                symbol
            )

            return False

        qty = calculate_sell_quantity(
            symbol,
            free_balance
        )

        if qty is None:

            logger.error(
                "%s | Emergency SELL "
                "quantity invalid",
                symbol
            )

            return False

        result = binance_request(
            "POST",
            "/api/v3/order",
            params={

                "symbol": symbol,

                "side": "SELL",

                "type": "MARKET",

                "quantity":
                    decimal_to_string(
                        qty
                    )
            },
            signed=True,
            retry_get=False
        )

        if result:

            logger.warning(
                "%s | EMERGENCY SELL "
                "completed | qty=%s",
                symbol,
                qty
            )

            remove_position(
                symbol
            )

            return True

        return False

    except BinanceTemporaryBlocked:

        logger.error(
            "%s | Emergency SELL blocked "
            "by Binance cooldown",
            symbol
        )

        return False

    except Exception as e:

        logger.error(
            "%s | Emergency SELL error: %s",
            symbol,
            e
        )

        return False


# ============================================================
# NORMAL SELL
# ============================================================

def place_sell(
    symbol,
    reason="RSI"
):

    with state_lock:

        if symbol not in positions:
            return

        if symbol in orders_in_flight:
            return

        orders_in_flight.add(
            symbol
        )

        position = dict(
            positions[
                symbol
            ]
        )

    try:

        logger.info(
            "%s | SELL signal | reason=%s",
            symbol,
            reason
        )

        stop_order_id = position.get(
            "stop_order_id"
        )

        if stop_order_id:

            cancel_stop_loss(
                symbol,
                stop_order_id
            )

        base_asset = symbol[:-4]

        free_balance = get_free_balance(
            base_asset
        )

        if free_balance <= 0:

            logger.warning(
                "%s | No balance found "
                "during SELL",
                symbol
            )

            remove_position(
                symbol
            )

            return

        qty = calculate_sell_quantity(
            symbol,
            free_balance
        )

        if qty is None:

            logger.error(
                "%s | SELL quantity invalid",
                symbol
            )

            return

        result = binance_request(
            "POST",
            "/api/v3/order",
            params={

                "symbol": symbol,

                "side": "SELL",

                "type": "MARKET",

                "quantity":
                    decimal_to_string(
                        qty
                    )
            },
            signed=True,
            retry_get=False
        )

        if not result:

            logger.error(
                "%s | SELL order failed",
                symbol
            )

            new_sl = place_stop_loss(
                symbol,
                decimal_from(
                    position[
                        "entry_price"
                    ]
                )
            )

            if new_sl:

                with state_lock:

                    if symbol in positions:

                        positions[
                            symbol
                        ][
                            "stop_order_id"
                        ] = new_sl

            return

        status = result.get(
            "status"
        )

        if status in (
            "FILLED",
            "PARTIALLY_FILLED"
        ):

            logger.info(
                "%s | SELL completed | "
                "reason=%s",
                symbol,
                reason
            )

            remove_position(
                symbol
            )

        else:

            logger.warning(
                "%s | SELL status=%s",
                symbol,
                status
            )

            new_sl = place_stop_loss(
                symbol,
                decimal_from(
                    position[
                        "entry_price"
                    ]
                )
            )

            if new_sl:

                with state_lock:

                    if symbol in positions:

                        positions[
                            symbol
                        ][
                            "stop_order_id"
                        ] = new_sl

    except BinanceTemporaryBlocked:

        logger.error(
            "%s | Binance cooldown "
            "during SELL",
            symbol
        )

    except Exception as e:

        logger.error(
            "%s | SELL exception: %s",
            symbol,
            e
        )

    finally:

        with state_lock:

            orders_in_flight.discard(
                symbol
            )


# ============================================================
# SIGNAL PROCESSING
# ============================================================

def process_signal(
    symbol,
    rsi3,
    rsi50,
    previous_rsi3
):

    if (
        rsi3 is None
        or
        rsi50 is None
    ):
        return

    # ========================================================
    # SELL
    # ========================================================

    crossed_above_80 = (

        previous_rsi3 is not None

        and

        previous_rsi3 <= SELL_RSI_LEVEL

        and

        rsi3 > SELL_RSI_LEVEL
    )

    if crossed_above_80:

        with state_lock:

            has_position = (
                symbol in positions
            )

        if has_position:

            logger.info(
                "%s | RSI SELL CROSS | "
                "previous RSI3=%.2f | "
                "current RSI3=%.2f",
                symbol,
                previous_rsi3,
                rsi3
            )

            executor.submit(
                place_sell,
                symbol,
                "RSI3_CROSS_ABOVE_80"
            )

            return

    # ========================================================
    # BUY
    #
    # EXACT CONDITION:
    #
    # RSI50 > 50
    # AND
    # RSI3 < 10
    # ========================================================

    buy_signal = (

        rsi50 > BUY_RSI_SLOW_MIN

        and

        rsi3 < BUY_RSI_FAST_MAX
    )

    if buy_signal:

        with state_lock:

            has_position = (
                symbol in positions
            )

        if not has_position:

            logger.info(
                "%s | BUY SIGNAL CONFIRMED | "
                "RSI50=%.4f (>50) | "
                "RSI3=%.4f (<10)",
                symbol,
                rsi50,
                rsi3
            )

            executor.submit(
                place_buy,
                symbol
            )

    else:

        # Debug information.
        # This makes it easier to compare the
        # bot's RSI with Binance chart.

        logger.debug(
            "%s | NO BUY | "
            "RSI50=%.4f | RSI3=%.4f",
            symbol,
            rsi50,
            rsi3
        )


# ============================================================
# WEBSOCKET MESSAGE
# ============================================================

def handle_ws_message(
    message,
    group_id
):

    try:

        data = json.loads(
            message
        )

        payload = data.get(
            "data"
        )

        if not payload:
            return

        if payload.get(
            "e"
        ) != "kline":
            return

        kline = payload.get(
            "k",
            {}
        )

        if not kline:
            return

        symbol = kline.get(
            "s"
        )

        is_closed = kline.get(
            "x"
        )

        # ONLY CLOSED CANDLE.
        if not symbol or not is_closed:
            return

        close_price = kline.get(
            "c"
        )

        close_time = kline.get(
            "T"
        )

        if close_price is None:
            return

        result = update_symbol_candle(
            symbol,
            int(close_time),
            float(close_price)
        )

        if not result:
            return

        rsi3 = result[
            "rsi3"
        ]

        rsi50 = result[
            "rsi50"
        ]

        previous_rsi3 = result[
            "previous_rsi3"
        ]

        if (
            rsi3 is None
            or
            rsi50 is None
        ):
            return

        logger.info(
            "%s | CLOSED 5m CANDLE | "
            "RSI3=%.4f | RSI50=%.4f",
            symbol,
            rsi3,
            rsi50
        )

        process_signal(
            symbol,
            rsi3,
            rsi50,
            previous_rsi3
        )

    except Exception as e:

        logger.error(
            "WebSocket message error: %s",
            e
        )


# ============================================================
# WEBSOCKET GROUP
# ============================================================

def websocket_group(
    group_id,
    symbols
):

    streams = "/".join(
        f"{symbol.lower()}@kline_{TIMEFRAME}"
        for symbol in symbols
    )

    url = (
        WS_BASE
        +
        streams
    )

    reconnect_delay = WS_RECONNECT_MIN

    while running:

        if is_binance_cooldown():

            remaining = cooldown_remaining()

            logger.warning(
                "WS group %s waiting for "
                "Binance cooldown: %ss",
                group_id,
                remaining
            )

            time.sleep(
                min(
                    remaining,
                    60
                )
            )

            continue

        start_time = time.time()

        def on_open(ws):

            nonlocal reconnect_delay

            reconnect_delay = (
                WS_RECONNECT_MIN
            )

            with state_lock:

                ws_connected_groups.add(
                    group_id
                )

            logger.info(
                "WebSocket group %s CONNECTED | "
                "%s symbols",
                group_id,
                len(symbols)
            )

        def on_message(
            ws,
            message
        ):

            handle_ws_message(
                message,
                group_id
            )

        def on_error(
            ws,
            error
        ):

            logger.error(
                "WebSocket group %s error: %s",
                group_id,
                error
            )

        def on_close(
            ws,
            close_status_code,
            close_msg
        ):

            with state_lock:

                ws_connected_groups.discard(
                    group_id
                )

            logger.warning(
                "WebSocket group %s closed | "
                "code=%s | msg=%s",
                group_id,
                close_status_code,
                close_msg
            )

        try:

            ws = websocket.WebSocketApp(
                url,
                on_open=on_open,
                on_message=on_message,
                on_error=on_error,
                on_close=on_close
            )

            ws.run_forever(
                ping_interval=None,
                ping_timeout=None,
                skip_utf8_validation=True
            )

        except Exception as e:

            logger.error(
                "WebSocket group %s "
                "exception: %s",
                group_id,
                e
            )

        finally:

            with state_lock:

                ws_connected_groups.discard(
                    group_id
                )

        if not running:
            break

        lifetime = (
            time.time()
            -
            start_time
        )

        logger.info(
            "WebSocket group %s reconnecting "
            "in %ss | lifetime=%.0fs",
            group_id,
            reconnect_delay,
            lifetime
        )

        time.sleep(
            reconnect_delay
        )

        reconnect_delay = min(
            reconnect_delay * 2,
            WS_RECONNECT_MAX
        )


# ============================================================
# START WEBSOCKETS
# ============================================================

def start_websockets():

    logger.info(
        "Starting %s WebSocket groups...",
        GROUPS
    )

    groups = []

    for i in range(
        GROUPS
    ):

        start = (
            i
            *
            SYMBOLS_PER_GROUP
        )

        end = (
            start
            +
            SYMBOLS_PER_GROUP
        )

        group_symbols = top_symbols[
            start:end
        ]

        if not group_symbols:
            continue

        groups.append(
            (
                i + 1,
                group_symbols
            )
        )

    for group_id, symbols in groups:

        thread = threading.Thread(
            target=websocket_group,
            args=(
                group_id,
                symbols
            ),
            daemon=True,
            name=f"ws-group-{group_id}"
        )

        thread.start()

        logger.info(
            "WebSocket group %s "
            "thread started",
            group_id
        )

        time.sleep(2)


# ============================================================
# RECOVERY
# ============================================================

def recover_positions():

    logger.info(
        "Checking existing account positions..."
    )

    account = get_account()

    if not account:

        logger.warning(
            "Could not load account "
            "for recovery."
        )

        return

    candidate_assets = []

    for balance in account.get(
        "balances",
        []
    ):

        asset = balance.get(
            "asset"
        )

        free = decimal_from(
            balance.get(
                "free",
                "0"
            )
        )

        locked = decimal_from(
            balance.get(
                "locked",
                "0"
            )
        )

        total = (
            free
            +
            locked
        )

        if (
            total > 0
            and
            asset not in EXCLUDED_ASSETS
        ):

            symbol = (
                asset
                +
                "USDT"
            )

            if symbol in top_symbols:

                candidate_assets.append(
                    (
                        asset,
                        symbol
                    )
                )

    logger.info(
        "Recovery candidates: %s",
        len(candidate_assets)
    )

    for asset, symbol in candidate_assets:

        try:

            start_time = (
                int(
                    time.time()
                    *
                    1000
                )
                -
                RECOVERY_LOOKBACK_DAYS
                *
                24
                *
                60
                *
                60
                *
                1000
            )

            orders = binance_request(
                "GET",
                "/api/v3/allOrders",
                params={
                    "symbol": symbol,
                    "startTime": start_time,
                    "limit":
                        RECOVERY_ORDER_LIMIT
                },
                signed=True
            )

            if not orders:
                continue

            bot_buys = []

            for order in orders:

                if order.get(
                    "side"
                ) != "BUY":
                    continue

                client_id = order.get(
                    "clientOrderId",
                    ""
                )

                if not client_id.startswith(
                    "RSIBUY_"
                ):
                    continue

                if order.get(
                    "status"
                ) not in (
                    "FILLED",
                    "PARTIALLY_FILLED"
                ):
                    continue

                bot_buys.append(
                    order
                )

            if not bot_buys:
                continue

            latest_buy = max(
                bot_buys,
                key=lambda x: int(
                    x.get(
                        "time",
                        0
                    )
                )
            )

            executed_qty = decimal_from(
                latest_buy.get(
                    "executedQty",
                    "0"
                )
            )

            quote_qty = decimal_from(
                latest_buy.get(
                    "cummulativeQuoteQty",
                    "0"
                )
            )

            if executed_qty <= 0:
                continue

            if quote_qty > 0:

                entry_price = (
                    quote_qty
                    /
                    executed_qty
                )

            else:

                entry_price = (
                    get_current_price(
                        symbol
                    )
                )

            if entry_price is None:
                continue

            current_balance = (
                get_free_balance(
                    asset
                )
            )

            if current_balance <= 0:
                continue

            open_orders = binance_request(
                "GET",
                "/api/v3/openOrders",
                params={
                    "symbol": symbol
                },
                signed=True
            )

            existing_sl_id = None

            if open_orders:

                for order in open_orders:

                    if order.get(
                        "side"
                    ) != "SELL":
                        continue

                    client_id = order.get(
                        "clientOrderId",
                        ""
                    )

                    if client_id.startswith(
                        "RSISL_"
                    ):

                        existing_sl_id = (
                            order.get(
                                "orderId"
                            )
                        )

                        break

            save_position(
                symbol,
                current_balance,
                entry_price,
                existing_sl_id
            )

            if existing_sl_id:

                logger.info(
                    "%s | RECOVERED position "
                    "+ existing SL | entry=%s",
                    symbol,
                    entry_price
                )

            else:

                logger.warning(
                    "%s | RECOVERED position "
                    "but NO SL. Creating new SL...",
                    symbol
                )

                new_sl = place_stop_loss(
                    symbol,
                    entry_price
                )

                if new_sl:

                    with state_lock:

                        if symbol in positions:

                            positions[
                                symbol
                            ][
                                "stop_order_id"
                            ] = new_sl

            time.sleep(
                STARTUP_REST_DELAY
            )

        except BinanceTemporaryBlocked:
            raise

        except Exception as e:

            logger.error(
                "%s | recovery error: %s",
                symbol,
                e
            )


# ============================================================
# MAIN INITIALIZATION
# ============================================================

def initialize_bot():

    global bot_ready
    global bot_error
    global bot_start_time

    bot_start_time = time.time()

    logger.info(
        "=" * 70
    )

    logger.info(
        "STARTING BINANCE RSI50 + RSI3 BOT"
    )

    logger.info(
        "=" * 70
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
        BUY_RSI_SLOW_MIN,
        BUY_RSI_FAST_MAX
    )

    logger.info(
        "BUY amount: %s USDT",
        BUY_USDT
    )

    logger.info(
        "SELL: RSI3 crossing above %s",
        SELL_RSI_LEVEL
    )

    logger.info(
        "Initial SL: %.2f%%",
        float(
            STOP_LOSS_PERCENT
            *
            100
        )
    )

    logger.info(
        "RSI method: WILDER / RMA"
    )

    logger.info(
        "RSI history: %s candles",
        HISTORY_LIMIT
    )

    logger.info(
        "WebSocket groups: %s",
        GROUPS
    )

    logger.info(
        "REST klines: startup only"
    )

    logger.info(
        "Local RSI calculation: ENABLED"
    )

    try:

        if not API_KEY or not API_SECRET:

            raise RuntimeError(
                "BINANCE_API_KEY or "
                "BINANCE_API_SECRET missing"
            )

        # ----------------------------------------------------
        # Server time
        # ----------------------------------------------------

        if not sync_server_time():

            logger.warning(
                "Initial server time sync failed. "
                "Continuing..."
            )

        # ----------------------------------------------------
        # Exchange information
        # ----------------------------------------------------

        load_exchange_info()

        # ----------------------------------------------------
        # Top symbols
        # ----------------------------------------------------

        get_top_symbols()

        # ----------------------------------------------------
        # Seed RSI
        # ----------------------------------------------------

        seed_all_symbols()

        # ----------------------------------------------------
        # Recover
        # ----------------------------------------------------

        recover_positions()

        # ----------------------------------------------------
        # WebSockets
        # ----------------------------------------------------

        start_websockets()

        bot_ready = True
        bot_error = None

        logger.info(
            "=" * 70
        )

        logger.info(
            "BOT IS READY"
        )

        logger.info(
            "=" * 70
        )

    except BinanceTemporaryBlocked as e:

        bot_ready = False

        bot_error = str(e)

        logger.error(
            "BOT INITIALIZATION STOPPED: %s",
            e
        )

        logger.error(
            "DO NOT repeatedly redeploy. "
            "Wait for Binance cooldown."
        )

    except Exception as e:

        bot_ready = False

        bot_error = str(e)

        logger.error(
            "BOT INITIALIZATION ERROR: %s",
            e
        )

        logger.error(
            traceback.format_exc()
        )


# ============================================================
# BACKGROUND START
# ============================================================

def start_bot_background():

    global bot_started

    with bot_start_lock:

        if bot_started:
            return

        bot_started = True

    logger.info(
        "BOT BACKGROUND THREAD STARTED"
    )

    thread = threading.Thread(
        target=initialize_bot,
        daemon=True,
        name="binance-bot"
    )

    thread.start()


# ============================================================
# GUNICORN FIX
# ============================================================

@app.before_request
def ensure_bot_started():

    start_bot_background()


# ============================================================
# HOME
# ============================================================

@app.route("/")
def home():

    with state_lock:

        return jsonify({

            "status": "running",

            "bot_started":
                bot_started,

            "bot_ready":
                bot_ready,

            "bot_error":
                bot_error,

            "timeframe":
                TIMEFRAME,

            "top_symbols":
                len(top_symbols),

            "positions":
                len(positions),

            "orders_in_flight":
                len(
                    orders_in_flight
                ),

            "websocket_groups_connected":
                len(
                    ws_connected_groups
                ),

            "binance_cooldown":
                is_binance_cooldown(),

            "binance_cooldown_remaining":
                cooldown_remaining(),

            "strategy": {

                "buy":
                    "RSI50 > 50 AND RSI3 < 10",

                "buy_usdt":
                    str(BUY_USDT),

                "sell":
                    "RSI3 crossing above 80",

                "stop_loss":
                    "1%",

                "rsi_method":
                    "Wilder RMA",

                "rsi_source":
                    "Closed candle close"
            }
        })


# ============================================================
# HEALTH
# ============================================================

@app.route("/health")
def health():

    with state_lock:

        healthy = (

            bot_started

            and

            bot_ready

            and

            bot_error is None

            and

            len(
                ws_connected_groups
            ) > 0
        )

        return jsonify({

            "healthy":
                healthy,

            "bot_started":
                bot_started,

            "bot_ready":
                bot_ready,

            "bot_error":
                bot_error,

            "running":
                running,

            "symbols":
                len(top_symbols),

            "positions":
                len(positions),

            "orders_in_flight":
                len(
                    orders_in_flight
                ),

            "websocket_groups":
                len(
                    ws_connected_groups
                ),

            "binance_cooldown":
                is_binance_cooldown(),

            "cooldown_remaining":
                cooldown_remaining()
        })


# ============================================================
# SIGNAL HANDLING
# ============================================================

def shutdown_handler(
    signum,
    frame
):

    global running

    logger.warning(
        "Shutdown signal received: %s",
        signum
    )

    running = False

    try:

        executor.shutdown(
            wait=False,
            cancel_futures=False
        )

    except Exception:
        pass


signal.signal(
    signal.SIGTERM,
    shutdown_handler
)

signal.signal(
    signal.SIGINT,
    shutdown_handler
)


# ============================================================
# LOCAL RUN
# ============================================================

if __name__ == "__main__":

    logger.info(
        "Starting Flask development server..."
    )

    app.run(
        host="0.0.0.0",
        port=int(
            os.environ.get(
                "PORT",
                "10000"
            )
        )
    )
