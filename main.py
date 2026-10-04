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

HISTORY_LIMIT = 120

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

# Binance websocket connection will be periodically recreated.
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

    # Major coins excluded
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

executor = ThreadPoolExecutor(max_workers=ORDER_WORKERS)

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

    return (value / step).to_integral_value(
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
            int(binance_cooldown_until - time.time())
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

        server_time = int(data["serverTime"])

        local_time = int(time.time() * 1000)

        server_time_offset_ms = server_time - local_time

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

    # --------------------------------------------------------
    # Respect global Binance cooldown
    # --------------------------------------------------------

    if is_binance_cooldown():
        remaining = cooldown_remaining()

        raise BinanceTemporaryBlocked(
            f"Binance cooldown active: {remaining}s remaining"
        )

    params = dict(params)

    # --------------------------------------------------------
    # SIGNED REQUEST
    # --------------------------------------------------------

    if signed:

        params["timestamp"] = now_ms()
        params["recvWindow"] = RECV_WINDOW

        query = urlencode(params, doseq=True)

        signature = hmac.new(
            API_SECRET.encode(),
            query.encode(),
            hashlib.sha256
        ).hexdigest()

        params["signature"] = signature

    url = BASE_URL + path

    # --------------------------------------------------------
    # GET retries
    # --------------------------------------------------------

    max_attempts = 4 if (method == "GET" and retry_get) else 1

    for attempt in range(1, max_attempts + 1):

        try:

            response = session.request(
                method,
                url,
                params=params if method == "GET" else None,
                data=params if method != "GET" else None,
                timeout=REQUEST_TIMEOUT
            )

            # =================================================
            # SUCCESS
            # =================================================

            if response.status_code in (200, 201):

                if not response.text:
                    return {}

                return response.json()

            # =================================================
            # 418 - TEMPORARY IP BAN
            # =================================================

            if response.status_code == 418:

                retry_after = get_retry_after(response)

                # Do not hammer Binance.
                cooldown = max(
                    retry_after,
                    300
                )

                set_binance_cooldown(cooldown)

                logger.error(
                    "BINANCE HTTP 418 - temporary IP restriction. "
                    "Cooldown=%ss",
                    cooldown
                )

                raise BinanceTemporaryBlocked(
                    "Binance HTTP 418 temporary IP restriction"
                )

            # =================================================
            # 429 - RATE LIMIT
            # =================================================

            if response.status_code == 429:

                retry_after = get_retry_after(response)

                logger.warning(
                    "Binance HTTP 429 | waiting %ss | attempt %s/%s",
                    retry_after,
                    attempt,
                    max_attempts
                )

                if attempt >= max_attempts:
                    return None

                time.sleep(retry_after)

                continue

            # =================================================
            # TIMESTAMP ERROR
            # =================================================

            try:
                error_json = response.json()
            except Exception:
                error_json = {}

            error_code = error_json.get("code")

            if error_code == -1021:

                logger.warning(
                    "Timestamp error. Synchronizing Binance time..."
                )

                if sync_server_time():

                    if method == "GET" and signed:
                        params["timestamp"] = now_ms()

                        query = urlencode(params, doseq=True)

                        signature = hmac.new(
                            API_SECRET.encode(),
                            query.encode(),
                            hashlib.sha256
                        ).hexdigest()

                        params["signature"] = signature

                        continue

                return None

            # =================================================
            # OTHER ERROR
            # =================================================

            logger.error(
                "Binance API error | %s %s | status=%s | response=%s",
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
                "Binance request timeout | %s %s | %s",
                method,
                path,
                e
            )

            # Never automatically retry order POST/DELETE.
            if method != "GET":
                return None

            if attempt >= max_attempts:
                return None

            time.sleep(min(2 * attempt, 5))

        except requests.exceptions.RequestException as e:

            logger.error(
                "Binance request error | %s %s | %s",
                method,
                path,
                e
            )

            if method != "GET":
                return None

            if attempt >= max_attempts:
                return None

            time.sleep(min(2 * attempt, 5))

        except Exception as e:

            logger.error(
                "Unexpected REST error | %s %s | %s",
                method,
                path,
                e
            )

            return None

    return None


# ============================================================
# RSI CALCULATION
# ============================================================

def calculate_rsi_series(closes, period):

    if closes is None:
        return pd.Series(dtype="float64")

    series = pd.Series(
        closes,
        dtype="float64"
    )

    if len(series) < period + 1:
        return pd.Series(
            [float("nan")] * len(series)
        )

    delta = series.diff()

    gain = delta.clip(lower=0)

    loss = -delta.clip(upper=0)

    avg_gain = gain.ewm(
        alpha=1 / period,
        adjust=False,
        min_periods=period
    ).mean()

    avg_loss = loss.ewm(
        alpha=1 / period,
        adjust=False,
        min_periods=period
    ).mean()

    rs = avg_gain / avg_loss

    rsi = 100 - (
        100 / (1 + rs)
    )

    rsi = rsi.where(
        avg_loss != 0,
        100
    )

    return rsi


def calculate_current_rsi(closes, period):

    if len(closes) < period + 1:
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

    symbols = data.get("symbols", [])

    loaded = 0

    with state_lock:

        exchange_filters.clear()

        for item in symbols:

            symbol = item.get("symbol")

            if not symbol:
                continue

            if item.get("status") != "TRADING":
                continue

            if item.get("isSpotTradingAllowed") is False:
                continue

            filters = {}

            for f in item.get("filters", []):

                filter_type = f.get("filterType")

                if filter_type:
                    filters[filter_type] = f

            exchange_filters[symbol] = filters

            loaded += 1

    logger.info(
        "Exchange info loaded: %s trading symbols",
        loaded
    )


# ============================================================
# TOP 150 SYMBOLS
# ============================================================

def get_top_symbols():

    logger.info(
        "Getting top %s USDT symbols by 24h quote volume...",
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

        symbol = item.get("symbol", "")

        if not symbol.endswith("USDT"):
            continue

        base_asset = symbol[:-4]

        if base_asset in EXCLUDED_ASSETS:
            continue

        with state_lock:
            filters = exchange_filters.get(symbol)

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
        for symbol, _ in candidates[:TOP_SYMBOLS]
    ]

    if not selected:
        raise RuntimeError(
            "No eligible USDT symbols found"
        )

    with state_lock:
        top_symbols.clear()
        top_symbols.extend(selected)

    logger.info(
        "Selected %s symbols",
        len(selected)
    )

    logger.info(
        "First symbols: %s",
        ", ".join(selected[:20])
    )

    return selected


# ============================================================
# GET KLINES - STARTUP ONLY
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
            close = float(candle[4])
            closes.append(close)
        except Exception:
            continue

    if len(closes) < RSI_SLOW_PERIOD + 1:
        return None

    # Last candle may still be open.
    # Remove it if its close time is in the future.
    try:

        last_candle = data[-1]

        close_time = int(
            last_candle[6]
        )

        if close_time > int(time.time() * 1000):

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

        closes = get_initial_closes(symbol)

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

            symbol_state[symbol] = {
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
# SEED ALL SYMBOLS
# ============================================================

def seed_all_symbols():

    logger.info(
        "Seeding RSI history for %s symbols...",
        len(top_symbols)
    )

    success = 0

    for index, symbol in enumerate(top_symbols, start=1):

        if is_binance_cooldown():
            raise BinanceTemporaryBlocked(
                "Binance cooldown during symbol seeding"
            )

        ok = seed_symbol(symbol)

        if ok:
            success += 1

        logger.info(
            "Seed %s/%s | %s | success=%s",
            index,
            len(top_symbols),
            symbol,
            ok
        )

        # Deliberately slow startup REST calls.
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
# UPDATE LOCAL CANDLE / RSI
# ============================================================

def update_symbol_candle(
    symbol,
    candle_close_time,
    close_price
):

    with state_lock:

        state = symbol_state.get(symbol)

        if state is None:

            state = {
                "closes": deque(
                    maxlen=HISTORY_LIMIT
                ),
                "rsi3": None,
                "rsi50": None,
                "last_closed_time": None
            }

            symbol_state[symbol] = state

        last_time = state.get(
            "last_closed_time"
        )

        # Ignore duplicate candle.
        if (
            last_time is not None
            and candle_close_time <= last_time
        ):
            return None

        closes = state["closes"]

        closes.append(
            float(close_price)
        )

        state["last_closed_time"] = (
            candle_close_time
        )

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
# ACCOUNT BALANCE
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

        if balance.get("asset") == asset:

            return decimal_from(
                balance.get(
                    "free",
                    "0"
                )
            )

    return Decimal("0")


# ============================================================
# SYMBOL FILTER HELPERS
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


def calculate_order_quantity(
    symbol,
    usdt_amount,
    price
):

    filters = exchange_filters.get(
        symbol,
        {}
    )

    lot_filter = (
        filters.get("MARKET_LOT_SIZE")
        or
        filters.get("LOT_SIZE")
    )

    if not lot_filter:
        return None

    step_size = decimal_from(
        lot_filter.get(
            "stepSize",
            "0"
        )
    )

    min_qty = decimal_from(
        lot_filter.get(
            "minQty",
            "0"
        )
    )

    if step_size <= 0:
        return None

    qty = (
        Decimal(str(usdt_amount))
        /
        Decimal(str(price))
    )

    qty = floor_decimal(
        qty,
        step_size
    )

    if qty < min_qty:
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
            str(data["price"])
        )
    except Exception:
        return None


# ============================================================
# SAVE POSITION
# ============================================================

def save_position(
    symbol,
    qty,
    entry_price,
    stop_order_id=None
):

    with state_lock:

        positions[symbol] = {
            "qty": str(qty),
            "entry_price": str(entry_price),
            "stop_order_id": stop_order_id,
            "buy_time": time.time()
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

        qty = (
            free_balance
            *
            SELL_BALANCE_BUFFER
        )

        filters = exchange_filters.get(
            symbol,
            {}
        )

        lot_filter = (
            filters.get("MARKET_LOT_SIZE")
            or
            filters.get("LOT_SIZE")
        )

        if not lot_filter:
            return None

        step_size = decimal_from(
            lot_filter.get(
                "stepSize",
                "0"
            )
        )

        min_qty = decimal_from(
            lot_filter.get(
                "minQty",
                "0"
            )
        )

        qty = floor_decimal(
            qty,
            step_size
        )

        if qty < min_qty:

            logger.error(
                "%s | SL quantity below minimum",
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

                stop_price = round_price_to_tick(
                    symbol,
                    stop_price
                )

        client_order_id = (
            "RSISL_"
            + symbol
            + "_"
            + str(int(time.time() * 1000))
        )

        order_params = {
            "symbol": symbol,
            "side": "SELL",
            "type": "STOP_LOSS",
            "quantity": decimal_to_string(qty),
            "stopPrice": decimal_to_string(
                stop_price
            ),
            "newClientOrderId": client_order_id
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
                "%s | Failed to place server SL",
                symbol
            )

            return None

        order_id = result.get(
            "orderId"
        )

        logger.info(
            "%s | SERVER SL placed | "
            "entry=%s | stop=%s | qty=%s | orderId=%s",
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
# CANCEL STOP LOSS
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
                "%s | SL cancelled | orderId=%s",
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
            time.time() - last_buy
            <
            BUY_COOLDOWN_SECONDS
        ):
            return

        orders_in_flight.add(symbol)

        last_buy_time[symbol] = time.time()

    try:

        logger.info(
            "%s | BUY signal | RSI50 > %s | RSI3 < %s",
            symbol,
            BUY_RSI_SLOW_MIN,
            BUY_RSI_FAST_MAX
        )

        client_order_id = (
            "RSIBUY_"
            + symbol
            + "_"
            + str(int(time.time() * 1000))
        )

        result = binance_request(
            "POST",
            "/api/v3/order",
            params={
                "symbol": symbol,
                "side": "BUY",
                "type": "MARKET",
                "quoteOrderQty": decimal_to_string(
                    BUY_USDT
                ),
                "newClientOrderId": client_order_id
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
                "%s | BUY executed quantity=0",
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
                    "%s | Cannot determine entry price",
                    symbol
                )
                return

        logger.info(
            "%s | BUY FILLED | qty=%s | entry=%s",
            symbol,
            executed_qty,
            avg_entry
        )

        # ----------------------------------------------------
        # IMPORTANT:
        # Save position BEFORE placing SL.
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
                    positions[symbol][
                        "stop_order_id"
                    ] = stop_order_id

        else:

            # ------------------------------------------------
            # If SL cannot be created, immediately sell.
            # ------------------------------------------------

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
            "%s | Binance temporarily blocked. "
            "BUY stopped.",
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
# EMERGENCY MARKET SELL
# ============================================================

def emergency_market_sell(symbol):

    try:

        base_asset = symbol[:-4]

        free_balance = get_free_balance(
            base_asset
        )

        if free_balance <= 0:
            remove_position(symbol)
            return False

        filters = exchange_filters.get(
            symbol,
            {}
        )

        lot_filter = (
            filters.get("MARKET_LOT_SIZE")
            or
            filters.get("LOT_SIZE")
        )

        if not lot_filter:
            return False

        step_size = decimal_from(
            lot_filter.get(
                "stepSize",
                "0"
            )
        )

        min_qty = decimal_from(
            lot_filter.get(
                "minQty",
                "0"
            )
        )

        qty = floor_decimal(
            free_balance * SELL_BALANCE_BUFFER,
            step_size
        )

        if qty < min_qty:

            logger.error(
                "%s | Emergency SELL quantity too small",
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
                "quantity": decimal_to_string(qty)
            },
            signed=True,
            retry_get=False
        )

        if result:

            logger.warning(
                "%s | EMERGENCY SELL completed | qty=%s",
                symbol,
                qty
            )

            remove_position(symbol)

            return True

        return False

    except BinanceTemporaryBlocked:

        logger.error(
            "%s | Emergency SELL blocked by Binance cooldown",
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

        orders_in_flight.add(symbol)

        position = dict(
            positions[symbol]
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
                "%s | No balance found during SELL",
                symbol
            )

            remove_position(symbol)

            return

        filters = exchange_filters.get(
            symbol,
            {}
        )

        lot_filter = (
            filters.get("MARKET_LOT_SIZE")
            or
            filters.get("LOT_SIZE")
        )

        if not lot_filter:

            logger.error(
                "%s | No lot-size filter",
                symbol
            )

            return

        step_size = decimal_from(
            lot_filter.get(
                "stepSize",
                "0"
            )
        )

        min_qty = decimal_from(
            lot_filter.get(
                "minQty",
                "0"
            )
        )

        qty = floor_decimal(
            free_balance * SELL_BALANCE_BUFFER,
            step_size
        )

        if qty < min_qty:

            logger.error(
                "%s | SELL quantity below minimum",
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
                "quantity": decimal_to_string(qty)
            },
            signed=True,
            retry_get=False
        )

        if not result:

            logger.error(
                "%s | SELL order failed",
                symbol
            )

            # Recreate SL if normal SELL failed.
            new_sl = place_stop_loss(
                symbol,
                decimal_from(
                    position["entry_price"]
                )
            )

            if new_sl:

                with state_lock:

                    if symbol in positions:

                        positions[symbol][
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
                "%s | SELL completed | reason=%s",
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
                    position["entry_price"]
                )
            )

            if new_sl:

                with state_lock:

                    if symbol in positions:

                        positions[symbol][
                            "stop_order_id"
                        ] = new_sl

    except BinanceTemporaryBlocked:

        logger.error(
            "%s | Binance cooldown during SELL",
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

    if rsi3 is None or rsi50 is None:
        return

    # ========================================================
    # SELL
    # RSI3 crosses ABOVE 80
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
                "previous RSI3=%.2f | current RSI3=%.2f",
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
    # RSI50 > 50 AND RSI3 < 10
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
                "%s | BUY SIGNAL | RSI50=%.2f | RSI3=%.2f",
                symbol,
                rsi50,
                rsi3
            )

            executor.submit(
                place_buy,
                symbol
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

        rsi3 = result["rsi3"]
        rsi50 = result["rsi50"]
        previous_rsi3 = result[
            "previous_rsi3"
        ]

        logger.info(
            "%s | Candle closed | "
            "RSI3=%.2f | RSI50=%.2f",
            symbol,
            rsi3 if rsi3 is not None else -1,
            rsi50 if rsi50 is not None else -1
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

    url = WS_BASE + streams

    reconnect_delay = WS_RECONNECT_MIN

    while running:

        if is_binance_cooldown():

            remaining = cooldown_remaining()

            logger.warning(
                "WS group %s waiting for Binance cooldown: %ss",
                group_id,
                remaining
            )

            time.sleep(
                min(remaining, 60)
            )

            continue

        start_time = time.time()

        def on_open(ws):

            nonlocal reconnect_delay

            reconnect_delay = WS_RECONNECT_MIN

            with state_lock:
                ws_connected_groups.add(
                    group_id
                )

            logger.info(
                "WebSocket group %s CONNECTED | %s symbols",
                group_id,
                len(symbols)
            )

        def on_message(ws, message):

            handle_ws_message(
                message,
                group_id
            )

        def on_error(ws, error):

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
                "WebSocket group %s closed | code=%s | msg=%s",
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

            # ------------------------------------------------
            # IMPORTANT:
            # No automatic ping/pong timeout.
            # ------------------------------------------------

            ws.run_forever(
                ping_interval=None,
                ping_timeout=None,
                skip_utf8_validation=True
            )

        except Exception as e:

            logger.error(
                "WebSocket group %s exception: %s",
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

        # ----------------------------------------------------
        # Reconnect
        # ----------------------------------------------------

        lifetime = time.time() - start_time

        logger.info(
            "WebSocket group %s reconnecting in %ss",
            group_id,
            reconnect_delay
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

    for i in range(GROUPS):

        start = (
            i * SYMBOLS_PER_GROUP
        )

        end = start + SYMBOLS_PER_GROUP

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
            "WebSocket group %s thread started",
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
            "Could not load account for recovery."
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

        total = free + locked

        if (
            total > 0
            and
            asset not in EXCLUDED_ASSETS
        ):

            symbol = asset + "USDT"

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

            # ------------------------------------------------
            # Get recent bot BUY orders only.
            # ------------------------------------------------

            start_time = (
                int(time.time() * 1000)
                -
                RECOVERY_LOOKBACK_DAYS
                * 24
                * 60
                * 60
                * 1000
            )

            orders = binance_request(
                "GET",
                "/api/v3/allOrders",
                params={
                    "symbol": symbol,
                    "startTime": start_time,
                    "limit": RECOVERY_ORDER_LIMIT
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

                entry_price = get_current_price(
                    symbol
                )

            if entry_price is None:
                continue

            # ------------------------------------------------
            # Current balance
            # ------------------------------------------------

            current_balance = get_free_balance(
                asset
            )

            if current_balance <= 0:
                continue

            # ------------------------------------------------
            # Check existing open orders
            # ------------------------------------------------

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

                        existing_sl_id = order.get(
                            "orderId"
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
                    "%s | RECOVERED position + existing SL | entry=%s",
                    symbol,
                    entry_price
                )

            else:

                logger.warning(
                    "%s | RECOVERED position but NO SL. "
                    "Creating new SL...",
                    symbol
                )

                new_sl = place_stop_loss(
                    symbol,
                    entry_price
                )

                if new_sl:

                    with state_lock:

                        if symbol in positions:

                            positions[symbol][
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
# MAIN BOT INITIALIZATION
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
        float(STOP_LOSS_PERCENT * 100)
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

    logger.info(
        "Binance 418 retry hammering: DISABLED"
    )

    try:

        if not API_KEY or not API_SECRET:
            raise RuntimeError(
                "BINANCE_API_KEY or BINANCE_API_SECRET missing"
            )

        # ----------------------------------------------------
        # Sync Binance server time
        # ----------------------------------------------------

        sync_server_time()

        # ----------------------------------------------------
        # Exchange info
        # ----------------------------------------------------

        load_exchange_info()

        # ----------------------------------------------------
        # Top symbols
        # ----------------------------------------------------

        get_top_symbols()

        # ----------------------------------------------------
        # Seed RSI history
        # ----------------------------------------------------

        seed_all_symbols()

        # ----------------------------------------------------
        # Recover existing positions
        # ----------------------------------------------------

        recover_positions()

        # ----------------------------------------------------
        # Start WebSockets
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
# IMPORTANT GUNICORN FIX
# ============================================================
#
# DO NOT call start_bot_background() at module level.
#
# Gunicorn imports main:app in the master process.
# Starting the bot there causes the background thread to live
# in the wrong process.
#
# We start it from Flask worker request instead.
# ============================================================

@app.before_request
def ensure_bot_started():

    start_bot_background()


# ============================================================
# FLASK ROUTES
# ============================================================

@app.route("/")
def home():

    with state_lock:

        return jsonify({
            "status": "running",
            "bot_started": bot_started,
            "bot_ready": bot_ready,
            "bot_error": bot_error,
            "timeframe": TIMEFRAME,
            "top_symbols": len(top_symbols),
            "positions": len(positions),
            "orders_in_flight": len(
                orders_in_flight
            ),
            "websocket_groups_connected": len(
                ws_connected_groups
            ),
            "binance_cooldown": is_binance_cooldown(),
            "binance_cooldown_remaining": cooldown_remaining(),
            "strategy": {
                "buy": "RSI50 > 50 AND RSI3 < 10",
                "buy_usdt": str(BUY_USDT),
                "sell": "RSI3 crossing above 80",
                "stop_loss": "1%"
            }
        })


@app.route("/health")
def health():

    with state_lock:

        healthy = (
            bot_started
            and
            bot_error is None
        )

        return jsonify({
            "healthy": healthy,
            "bot_started": bot_started,
            "bot_ready": bot_ready,
            "bot_error": bot_error,
            "running": running,
            "symbols": len(top_symbols),
            "positions": len(positions),
            "orders_in_flight": len(
                orders_in_flight
            ),
            "websocket_groups": len(
                ws_connected_groups
            ),
            "binance_cooldown": is_binance_cooldown(),
            "cooldown_remaining": cooldown_remaining()
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
