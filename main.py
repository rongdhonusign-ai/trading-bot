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
from datetime import datetime, timezone
from collections import deque

import requests
import pandas as pd
import websocket

from flask import Flask, jsonify


# ============================================================
# CONFIG
# ============================================================

API_KEY = os.environ.get("BINANCE_API_KEY")
API_SECRET = os.environ.get("BINANCE_API_SECRET")

BASE_URL = "https://api.binance.com"
WS_BASE = "wss://stream.binance.com:9443/stream?streams="

TIMEFRAME = "5m"

TOP_SYMBOLS = 150
GROUPS = 3
SYMBOLS_PER_GROUP = 50

BUY_USDT = Decimal("15")

RSI_FAST_PERIOD = 3
RSI_SLOW_PERIOD = 50

BUY_RSI_SLOW_MIN = Decimal("50")
BUY_RSI_FAST_MAX = Decimal("10")

SELL_RSI_LEVEL = Decimal("80")

STOP_LOSS_PERCENT = Decimal("0.01")

# More history = much better RSI initialization
HISTORY_LIMIT = 500

REQUEST_TIMEOUT = 15
RECV_WINDOW = 5000

BUY_COOLDOWN_SECONDS = 60

# Startup REST spacing.
# This intentionally slows startup to reduce API pressure.
STARTUP_REST_DELAY = 0.50

# Sell slightly less than free balance
SELL_BALANCE_BUFFER = Decimal("0.999")

ORDER_WORKERS = 1

# WebSocket reconnect
WS_RECONNECT_MIN = 5
WS_RECONNECT_MAX = 60

# Binance temporary restriction protection
BINANCE_COOLDOWN_418 = 3600
BINANCE_COOLDOWN_429 = 120

# API request retry
MAX_GET_RETRIES = 3

# Recovery
RECOVERY_LOOKBACK_DAYS = 7
RECOVERY_ORDER_LIMIT = 50

# Binance client order prefixes
BUY_CLIENT_PREFIX = "RSIBUY_"
SL_CLIENT_PREFIX = "RSISL_"


# ============================================================
# LOGGING
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

logger = logging.getLogger("RSI_BOT")


# ============================================================
# VALIDATION
# ============================================================

if not API_KEY or not API_SECRET:
    logger.error("BINANCE_API_KEY / BINANCE_API_SECRET missing.")


# ============================================================
# FLASK
# ============================================================

app = Flask(__name__)


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

server_time_offset_ms = 0

binance_cooldown_until = 0

binance_cooldown_lock = threading.Lock()

# Only ONE order request at a time.
order_lock = threading.Lock()

# Session
session = requests.Session()

session.headers.update({
    "X-MBX-APIKEY": API_KEY or ""
})


# ============================================================
# EXCLUDED ASSETS
# ============================================================

EXCLUDED_ASSETS = {
    # Stablecoins
    "USDT",
    "USDC",
    "FDUSD",
    "BUSD",
    "TUSD",
    "DAI",
    "USDP",
    "EUR",
    "GBP",
    "TRY",
    "BRL",
    "AUD",
    "JPY",
    "RUB",
    "UAH",
    "PLN",
    "RON",
    "ARS",
    "ZAR",
    "NGN",
    "BIDR",
    "IDRT",
    "UAH",

    # Major coins excluded by user strategy
    "BTC",
    "ETH",
}


# ============================================================
# HELPERS
# ============================================================

def now_ms():
    return int(time.time() * 1000) + server_time_offset_ms


def utc_time_string(ms=None):
    if ms is None:
        ms = int(time.time() * 1000)

    return datetime.fromtimestamp(
        ms / 1000,
        tz=timezone.utc
    ).strftime("%Y-%m-%d %H:%M:%S UTC")


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

    text = format(value, "f")

    if "." in text:
        text = text.rstrip("0").rstrip(".")

    return text if text else "0"


# ============================================================
# BINANCE COOLDOWN
# ============================================================

def is_binance_on_cooldown():
    with binance_cooldown_lock:
        return time.time() < binance_cooldown_until


def get_binance_cooldown_remaining():
    with binance_cooldown_lock:
        remaining = binance_cooldown_until - time.time()

    return max(0, int(remaining))


def activate_binance_cooldown(seconds, reason):
    global binance_cooldown_until

    with binance_cooldown_lock:
        new_until = time.time() + seconds

        if new_until > binance_cooldown_until:
            binance_cooldown_until = new_until

    remaining = get_binance_cooldown_remaining()

    logger.error(
        "Binance cooldown activated for %s seconds. Reason=%s",
        remaining,
        reason
    )


# ============================================================
# SERVER TIME
# ============================================================

def sync_server_time():
    global server_time_offset_ms

    try:
        local_before = int(time.time() * 1000)

        response = session.get(
            f"{BASE_URL}/api/v3/time",
            timeout=REQUEST_TIMEOUT
        )

        response.raise_for_status()

        local_after = int(time.time() * 1000)

        data = response.json()

        server_time = int(data["serverTime"])

        midpoint = (local_before + local_after) // 2

        server_time_offset_ms = server_time - midpoint

        logger.info(
            "Binance server time synced. Offset=%sms",
            server_time_offset_ms
        )

        return True

    except Exception as exc:
        logger.error(
            "Server time sync failed: %s",
            exc
        )

        return False


# ============================================================
# BINANCE HTTP REQUEST
# ============================================================

def binance_request(
    method,
    endpoint,
    params=None,
    signed=False,
    allow_during_cooldown=False
):
    """
    Centralized Binance REST request.

    IMPORTANT:
    - No candle/RSI REST calls during normal operation.
    - 418/429 activates cooldown.
    - Signed timestamp is recreated correctly.
    """

    if params is None:
        params = {}

    params = dict(params)

    method = method.upper()

    if is_binance_on_cooldown() and not allow_during_cooldown:
        raise RuntimeError(
            "BINANCE_COOLDOWN_ACTIVE"
        )

    for attempt in range(MAX_GET_RETRIES):

        request_params = dict(params)

        if signed:
            request_params["timestamp"] = now_ms()
            request_params["recvWindow"] = RECV_WINDOW

            query = urlencode(
                request_params,
                doseq=True
            )

            signature = hmac.new(
                API_SECRET.encode(),
                query.encode(),
                hashlib.sha256
            ).hexdigest()

            request_params["signature"] = signature

        try:

            response = session.request(
                method,
                BASE_URL + endpoint,
                params=request_params,
                timeout=REQUEST_TIMEOUT
            )

            status = response.status_code

            # ------------------------------------------------
            # 418
            # ------------------------------------------------

            if status == 418:

                activate_binance_cooldown(
                    BINANCE_COOLDOWN_418,
                    "HTTP 418 temporary IP restriction"
                )

                logger.error(
                    "BINANCE HTTP 418 - temporary IP restriction. "
                    "Cooldown=%ss",
                    get_binance_cooldown_remaining()
                )

                raise RuntimeError(
                    "BINANCE_418"
                )

            # ------------------------------------------------
            # 429
            # ------------------------------------------------

            if status == 429:

                retry_after = response.headers.get(
                    "Retry-After"
                )

                try:
                    retry_seconds = int(retry_after)
                except Exception:
                    retry_seconds = BINANCE_COOLDOWN_429

                retry_seconds = max(
                    30,
                    min(retry_seconds, 3600)
                )

                activate_binance_cooldown(
                    retry_seconds,
                    "HTTP 429 rate limit"
                )

                logger.error(
                    "BINANCE HTTP 429 - rate limit. "
                    "Cooldown=%ss",
                    retry_seconds
                )

                raise RuntimeError(
                    "BINANCE_429"
                )

            # ------------------------------------------------
            # Timestamp error
            # ------------------------------------------------

            if status >= 400:

                try:
                    error_data = response.json()
                except Exception:
                    error_data = {
                        "msg": response.text
                    }

                code = error_data.get("code")

                if code == -1021 and signed:
                    logger.warning(
                        "Binance timestamp error. Resyncing server time..."
                    )

                    sync_server_time()

                    time.sleep(1)

                    continue

                logger.error(
                    "Binance HTTP %s | %s",
                    status,
                    error_data
                )

                response.raise_for_status()

            return response.json()

        except RuntimeError:
            raise

        except requests.RequestException as exc:

            if attempt >= MAX_GET_RETRIES - 1:
                raise

            wait_time = 2 ** attempt

            logger.warning(
                "Binance request failed: %s. Retry in %ss",
                exc,
                wait_time
            )

            time.sleep(wait_time)

        except Exception:

            if attempt >= MAX_GET_RETRIES - 1:
                raise

            time.sleep(1)

    raise RuntimeError("BINANCE_REQUEST_FAILED")


# ============================================================
# EXACT WILDER RSI
# ============================================================

def rsi_from_averages(avg_gain, avg_loss):
    avg_gain = float(avg_gain)
    avg_loss = float(avg_loss)

    if avg_loss == 0:

        if avg_gain == 0:
            return 50.0

        return 100.0

    if avg_gain == 0:
        return 0.0

    rs = avg_gain / avg_loss

    return 100.0 - (
        100.0 / (1.0 + rs)
    )


def calculate_wilder_state(closes, period):
    """
    Standard Wilder RSI.

    Returns:
        rsi,
        avg_gain,
        avg_loss

    Initial average is SMA of first `period` changes.
    Then Wilder recursive smoothing is used.
    """

    values = [
        float(x)
        for x in closes
    ]

    if len(values) < period + 1:
        return None, None, None

    gains = []
    losses = []

    for i in range(1, len(values)):

        change = values[i] - values[i - 1]

        if change > 0:
            gains.append(change)
            losses.append(0.0)

        else:
            gains.append(0.0)
            losses.append(-change)

    avg_gain = sum(
        gains[:period]
    ) / period

    avg_loss = sum(
        losses[:period]
    ) / period

    # Process remaining changes
    for i in range(period, len(gains)):

        avg_gain = (
            (avg_gain * (period - 1))
            + gains[i]
        ) / period

        avg_loss = (
            (avg_loss * (period - 1))
            + losses[i]
        ) / period

    rsi = rsi_from_averages(
        avg_gain,
        avg_loss
    )

    return rsi, avg_gain, avg_loss


def update_wilder_state(
    previous_close,
    new_close,
    avg_gain,
    avg_loss,
    period
):
    """
    Incremental Wilder RSI update.
    """

    change = float(new_close) - float(previous_close)

    gain = max(change, 0.0)
    loss = max(-change, 0.0)

    avg_gain = (
        (float(avg_gain) * (period - 1))
        + gain
    ) / period

    avg_loss = (
        (float(avg_loss) * (period - 1))
        + loss
    ) / period

    rsi = rsi_from_averages(
        avg_gain,
        avg_loss
    )

    return rsi, avg_gain, avg_loss


# ============================================================
# EXCHANGE INFO
# ============================================================

def load_exchange_info():

    logger.info("Loading Binance exchange information...")

    data = binance_request(
        "GET",
        "/api/v3/exchangeInfo"
    )

    filters_map = {}

    count = 0

    for symbol_data in data.get("symbols", []):

        symbol = symbol_data.get("symbol")

        if not symbol:
            continue

        if symbol_data.get("status") != "TRADING":
            continue

        if not symbol_data.get(
            "isSpotTradingAllowed",
            True
        ):
            continue

        if symbol_data.get(
            "quoteAsset"
        ) != "USDT":
            continue

        base_asset = symbol_data.get(
            "baseAsset"
        )

        if base_asset in EXCLUDED_ASSETS:
            continue

        symbol_filters = {}

        for f in symbol_data.get(
            "filters",
            []
        ):

            filter_type = f.get(
                "filterType"
            )

            symbol_filters[
                filter_type
            ] = f

        filters_map[symbol] = symbol_filters

        count += 1

    with state_lock:
        exchange_filters.clear()
        exchange_filters.update(
            filters_map
        )

    logger.info(
        "Loaded %s eligible spot symbols.",
        count
    )

    return True


# ============================================================
# TOP SYMBOLS
# ============================================================

def get_top_symbols():

    logger.info(
        "Loading 24h ticker data for top symbols..."
    )

    data = binance_request(
        "GET",
        "/api/v3/ticker/24hr"
    )

    candidates = []

    for item in data:

        symbol = item.get("symbol")

        if symbol not in exchange_filters:
            continue

        if not symbol.endswith("USDT"):
            continue

        try:
            quote_volume = Decimal(
                str(item.get("quoteVolume", "0"))
            )

            last_price = Decimal(
                str(item.get("lastPrice", "0"))
            )

        except Exception:
            continue

        if quote_volume <= 0:
            continue

        if last_price <= 0:
            continue

        filters = exchange_filters.get(
            symbol,
            {}
        )

        # Check min notional
        min_notional = Decimal("0")

        for filter_name in (
            "NOTIONAL",
            "MIN_NOTIONAL"
        ):

            f = filters.get(
                filter_name
            )

            if f:

                min_notional = max(
                    min_notional,
                    decimal_from(
                        f.get(
                            "minNotional",
                            "0"
                        )
                    )
                )

        if min_notional > BUY_USDT:
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
        for x in candidates[:TOP_SYMBOLS]
    ]

    with state_lock:
        top_symbols.clear()
        top_symbols.extend(selected)

    logger.info(
        "Selected %s symbols.",
        len(selected)
    )

    logger.info(
        "Top symbols: %s",
        ", ".join(selected)
    )

    return selected


# ============================================================
# INITIAL KLINES
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

    now = int(time.time() * 1000)

    closed = []

    for k in data:

        close_time = int(k[6])

        # Ignore current still-open candle
        if close_time > now:
            continue

        closed.append(
            float(k[4])
        )

    if len(closed) < RSI_SLOW_PERIOD + 1:
        raise ValueError(
            f"{symbol}: insufficient closed candles"
        )

    return closed


# ============================================================
# SEED SYMBOL
# ============================================================

def seed_symbol(symbol):

    closes = get_initial_closes(
        symbol
    )

    rsi3, avg_gain3, avg_loss3 = (
        calculate_wilder_state(
            closes,
            RSI_FAST_PERIOD
        )
    )

    rsi50, avg_gain50, avg_loss50 = (
        calculate_wilder_state(
            closes,
            RSI_SLOW_PERIOD
        )
    )

    if (
        rsi3 is None
        or rsi50 is None
    ):
        raise ValueError(
            f"{symbol}: RSI initialization failed"
        )

    state = {
        "closes": deque(
            closes,
            maxlen=HISTORY_LIMIT
        ),

        "rsi3": Decimal(
            str(rsi3)
        ),

        "rsi50": Decimal(
            str(rsi50)
        ),

        "previous_rsi3": None,
        "previous_rsi50": None,

        "avg_gain3": avg_gain3,
        "avg_loss3": avg_loss3,

        "avg_gain50": avg_gain50,
        "avg_loss50": avg_loss50,

        "last_candle_close_time": None,
    }

    with state_lock:
        symbol_state[symbol] = state

    return True


# ============================================================
# SEED ALL SYMBOLS
# ============================================================

def seed_all_symbols():

    logger.info(
        "Starting initial RSI history load..."
    )

    success = 0
    failed = 0

    for index, symbol in enumerate(
        list(top_symbols),
        start=1
    ):

        if not running:
            break

        try:

            seed_symbol(
                symbol
            )

            success += 1

            if index % 10 == 0:
                logger.info(
                    "Initial history: %s/%s",
                    index,
                    len(top_symbols)
                )

        except Exception as exc:

            failed += 1

            logger.error(
                "%s | Initial history failed: %s",
                symbol,
                exc
            )

        # IMPORTANT:
        # Do not hammer Binance during startup.
        time.sleep(
            STARTUP_REST_DELAY
        )

    logger.info(
        "Initial history finished | success=%s | failed=%s",
        success,
        failed
    )

    return success > 0


# ============================================================
# UPDATE RSI FROM CLOSED CANDLE
# ============================================================

def update_symbol_candle(
    symbol,
    close_price,
    candle_close_time
):

    with state_lock:

        state = symbol_state.get(
            symbol
        )

        if not state:
            return None

        previous_close = (
            state["closes"][-1]
            if state["closes"]
            else None
        )

        if previous_close is None:
            return None

        # Prevent duplicate candle processing
        if (
            state["last_candle_close_time"]
            == candle_close_time
        ):
            return None

        previous_rsi3 = state[
            "rsi3"
        ]

        previous_rsi50 = state[
            "rsi50"
        ]

        rsi3, avg_gain3, avg_loss3 = (
            update_wilder_state(
                previous_close,
                close_price,
                state["avg_gain3"],
                state["avg_loss3"],
                RSI_FAST_PERIOD
            )
        )

        rsi50, avg_gain50, avg_loss50 = (
            update_wilder_state(
                previous_close,
                close_price,
                state["avg_gain50"],
                state["avg_loss50"],
                RSI_SLOW_PERIOD
            )
        )

        state["previous_rsi3"] = (
            previous_rsi3
        )

        state["previous_rsi50"] = (
            previous_rsi50
        )

        state["rsi3"] = Decimal(
            str(rsi3)
        )

        state["rsi50"] = Decimal(
            str(rsi50)
        )

        state["avg_gain3"] = avg_gain3
        state["avg_loss3"] = avg_loss3

        state["avg_gain50"] = avg_gain50
        state["avg_loss50"] = avg_loss50

        state["closes"].append(
            float(close_price)
        )

        state["last_candle_close_time"] = (
            candle_close_time
        )

        return {
            "rsi3": Decimal(
                str(rsi3)
            ),
            "rsi50": Decimal(
                str(rsi50)
            ),
            "previous_rsi3": previous_rsi3,
            "previous_rsi50": previous_rsi50,
        }


# ============================================================
# ACCOUNT BALANCE
# ============================================================

def get_account():

    if is_binance_on_cooldown():
        raise RuntimeError(
            "BINANCE_COOLDOWN_ACTIVE"
        )

    return binance_request(
        "GET",
        "/api/v3/account",
        signed=True
    )


def get_free_balance(asset):

    data = get_account()

    for balance in data.get(
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
# ORDER FILTER HELPERS
# ============================================================

def get_min_notional(symbol):

    filters = exchange_filters.get(
        symbol,
        {}
    )

    result = Decimal("0")

    for name in (
        "NOTIONAL",
        "MIN_NOTIONAL"
    ):

        f = filters.get(name)

        if f:

            result = max(
                result,
                decimal_from(
                    f.get(
                        "minNotional",
                        "0"
                    )
                )
            )

    return result


def get_quantity_rules(symbol):

    filters = exchange_filters.get(
        symbol,
        {}
    )

    rules = []

    for name in (
        "LOT_SIZE",
        "MARKET_LOT_SIZE"
    ):

        f = filters.get(name)

        if not f:
            continue

        step = decimal_from(
            f.get(
                "stepSize",
                "0"
            )
        )

        min_qty = decimal_from(
            f.get(
                "minQty",
                "0"
            )
        )

        max_qty = decimal_from(
            f.get(
                "maxQty",
                "0"
            )
        )

        if step > 0:
            rules.append({
                "name": name,
                "step": step,
                "min_qty": min_qty,
                "max_qty": max_qty
            })

    return rules


def normalize_quantity(
    symbol,
    raw_quantity
):

    qty = decimal_from(
        raw_quantity
    )

    if qty <= 0:
        return None

    rules = get_quantity_rules(
        symbol
    )

    if not rules:
        return qty

    # First floor according to every applicable step.
    for rule in rules:

        qty = floor_decimal(
            qty,
            rule["step"]
        )

    min_qty = max(
        (
            r["min_qty"]
            for r in rules
        ),
        default=Decimal("0")
    )

    if qty < min_qty:
        return None

    # Verify all filters
    for rule in rules:

        step = rule["step"]

        if step > 0:

            multiple = (
                qty / step
            )

            if multiple != multiple.to_integral_value():
                qty = floor_decimal(
                    qty,
                    step
                )

    if qty < min_qty:
        return None

    return qty


def calculate_buy_quantity(
    symbol,
    price
):

    price = decimal_from(
        price
    )

    if price <= 0:
        return None

    raw_qty = BUY_USDT / price

    qty = normalize_quantity(
        symbol,
        raw_qty
    )

    if qty is None:
        return None

    notional = qty * price

    min_notional = get_min_notional(
        symbol
    )

    if (
        min_notional > 0
        and notional < min_notional
    ):
        return None

    return qty


# ============================================================
# PRICE / TICK SIZE
# ============================================================

def round_price_to_tick(
    symbol,
    price
):

    filters = exchange_filters.get(
        symbol,
        {}
    )

    price_filter = filters.get(
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

    return decimal_from(
        data.get("price")
    )


# ============================================================
# SAVE POSITION
# ============================================================

def save_position(
    symbol,
    entry_price,
    quantity,
    buy_order_id=None
):

    with state_lock:

        positions[symbol] = {
            "entry_price": Decimal(
                str(entry_price)
            ),
            "quantity": Decimal(
                str(quantity)
            ),
            "buy_order_id": buy_order_id,
            "stop_order_id": None,
            "created_at": time.time(),
        }


def remove_position(symbol):

    with state_lock:
        positions.pop(
            symbol,
            None
        )


# ============================================================
# PLACE STOP LOSS
# ============================================================

def place_stop_loss(
    symbol,
    entry_price,
    quantity
):

    if is_binance_on_cooldown():
        logger.warning(
            "%s | Binance cooldown active. "
            "Stop-loss placement skipped temporarily.",
            symbol
        )
        return None

    try:

        entry_price = Decimal(
            str(entry_price)
        )

        quantity = Decimal(
            str(quantity)
        )

        # Get current balance only when actually placing SL.
        base_asset = symbol[:-4]

        free_balance = get_free_balance(
            base_asset
        )

        if free_balance <= 0:
            logger.error(
                "%s | No free balance for SL.",
                symbol
            )
            return None

        quantity = min(
            quantity,
            free_balance
        )

        quantity = (
            quantity
            * SELL_BALANCE_BUFFER
        )

        quantity = normalize_quantity(
            symbol,
            quantity
        )

        if quantity is None:
            logger.error(
                "%s | SL quantity failed LOT_SIZE validation.",
                symbol
            )
            return None

        stop_price = (
            entry_price
            * (
                Decimal("1")
                - STOP_LOSS_PERCENT
            )
        )

        stop_price = round_price_to_tick(
            symbol,
            stop_price
        )

        if stop_price <= 0:
            return None

        client_id = (
            SL_CLIENT_PREFIX
            + str(int(time.time() * 1000))
        )

        response = binance_request(
            "POST",
            "/api/v3/order",
            params={
                "symbol": symbol,
                "side": "SELL",
                "type": "STOP_LOSS",
                "quantity": decimal_to_string(
                    quantity
                ),
                "stopPrice": decimal_to_string(
                    stop_price
                ),
                "newClientOrderId": client_id,
                "newOrderRespType": "RESULT",
            },
            signed=True
        )

        order_id = response.get(
            "orderId"
        )

        with state_lock:

            if symbol in positions:
                positions[symbol][
                    "stop_order_id"
                ] = order_id

        logger.info(
            "%s | STOP LOSS placed | "
            "Entry=%s | Stop=%s | Qty=%s | OrderID=%s",
            symbol,
            decimal_to_string(entry_price),
            decimal_to_string(stop_price),
            decimal_to_string(quantity),
            order_id
        )

        return order_id

    except RuntimeError as exc:

        if str(exc) in (
            "BINANCE_418",
            "BINANCE_429",
            "BINANCE_COOLDOWN_ACTIVE"
        ):

            logger.error(
                "%s | Stop-loss REST request blocked: %s",
                symbol,
                exc
            )

            return None

        logger.error(
            "%s | Stop-loss error: %s",
            symbol,
            exc
        )

        return None

    except Exception as exc:

        logger.error(
            "%s | Stop-loss error: %s",
            symbol,
            exc
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

    if is_binance_on_cooldown():
        return False

    try:

        binance_request(
            "DELETE",
            "/api/v3/order",
            params={
                "symbol": symbol,
                "orderId": order_id
            },
            signed=True
        )

        logger.info(
            "%s | Stop-loss cancelled | OrderID=%s",
            symbol,
            order_id
        )

        return True

    except Exception as exc:

        logger.error(
            "%s | Stop-loss cancel failed: %s",
            symbol,
            exc
        )

        return False


# ============================================================
# EMERGENCY MARKET SELL
# ============================================================

def emergency_market_sell(symbol):

    if is_binance_on_cooldown():
        logger.error(
            "%s | Emergency SELL blocked by Binance cooldown.",
            symbol
        )
        return False

    try:

        base_asset = symbol[:-4]

        free_balance = get_free_balance(
            base_asset
        )

        if free_balance <= 0:
            return False

        qty = (
            free_balance
            * SELL_BALANCE_BUFFER
        )

        qty = normalize_quantity(
            symbol,
            qty
        )

        if qty is None:
            logger.error(
                "%s | Emergency SELL quantity invalid.",
                symbol
            )
            return False

        response = binance_request(
            "POST",
            "/api/v3/order",
            params={
                "symbol": symbol,
                "side": "SELL",
                "type": "MARKET",
                "quantity": decimal_to_string(
                    qty
                ),
                "newOrderRespType": "FULL"
            },
            signed=True
        )

        logger.warning(
            "%s | EMERGENCY MARKET SELL executed | Qty=%s | Order=%s",
            symbol,
            decimal_to_string(qty),
            response.get("orderId")
        )

        return True

    except Exception as exc:

        logger.error(
            "%s | Emergency SELL failed: %s",
            symbol,
            exc
        )

        return False


# ============================================================
# PLACE BUY
# ============================================================

def place_buy(symbol):

    if is_binance_on_cooldown():

        logger.warning(
            "%s | Binance cooldown active. BUY skipped.",
            symbol
        )

        return False

    # Prevent simultaneous orders.
    if not order_lock.acquire(
        blocking=False
    ):

        logger.info(
            "%s | Another order is in progress. BUY skipped.",
            symbol
        )

        return False

    try:

        with state_lock:

            if symbol in positions:
                return False

            if symbol in orders_in_flight:
                return False

            last_time = last_buy_time.get(
                symbol,
                0
            )

            if (
                time.time()
                - last_time
                < BUY_COOLDOWN_SECONDS
            ):
                return False

            orders_in_flight.add(
                symbol
            )

        # ----------------------------------------------------
        # Get current price
        # ----------------------------------------------------

        current_price = get_current_price(
            symbol
        )

        if current_price <= 0:
            return False

        # ----------------------------------------------------
        # Calculate valid LOT_SIZE quantity
        # ----------------------------------------------------

        quantity = calculate_buy_quantity(
            symbol,
            current_price
        )

        if quantity is None:

            logger.warning(
                "%s | BUY skipped. "
                "Could not create valid LOT_SIZE quantity for %s USDT.",
                symbol,
                BUY_USDT
            )

            return False

        logger.info(
            "%s | BUY attempt | Price=%s | Qty=%s | Notional≈%s",
            symbol,
            decimal_to_string(current_price),
            decimal_to_string(quantity),
            decimal_to_string(
                quantity * current_price
            )
        )

        # ----------------------------------------------------
        # MARKET BUY
        # ----------------------------------------------------

        client_id = (
            BUY_CLIENT_PREFIX
            + str(int(time.time() * 1000))
        )

        response = binance_request(
            "POST",
            "/api/v3/order",
            params={
                "symbol": symbol,
                "side": "BUY",
                "type": "MARKET",
                "quantity": decimal_to_string(
                    quantity
                ),
                "newClientOrderId": client_id,
                "newOrderRespType": "FULL"
            },
            signed=True
        )

        order_id = response.get(
            "orderId"
        )

        executed_qty = decimal_from(
            response.get(
                "executedQty",
                quantity
            )
        )

        # ----------------------------------------------------
        # Average fill price
        # ----------------------------------------------------

        total_quote = Decimal("0")
        total_qty = Decimal("0")

        for fill in response.get(
            "fills",
            []
        ):

            fill_qty = decimal_from(
                fill.get("qty")
            )

            fill_price = decimal_from(
                fill.get("price")
            )

            total_qty += fill_qty

            total_quote += (
                fill_qty
                * fill_price
            )

        if total_qty > 0:
            avg_price = (
                total_quote
                / total_qty
            )
        else:
            avg_price = current_price

        # ----------------------------------------------------
        # Save position
        # ----------------------------------------------------

        save_position(
            symbol,
            avg_price,
            executed_qty,
            order_id
        )

        with state_lock:
            last_buy_time[symbol] = (
                time.time()
            )

        logger.info(
            "%s | BUY SUCCESS | "
            "Entry=%s | Qty=%s | OrderID=%s",
            symbol,
            decimal_to_string(avg_price),
            decimal_to_string(executed_qty),
            order_id
        )

        # ----------------------------------------------------
        # Place SL
        # ----------------------------------------------------

        sl_id = place_stop_loss(
            symbol,
            avg_price,
            executed_qty
        )

        if sl_id is None:

            logger.error(
                "%s | STOP LOSS could not be placed. "
                "Attempting emergency SELL.",
                symbol
            )

            emergency_market_sell(
                symbol
            )

            remove_position(
                symbol
            )

            return False

        return True

    except RuntimeError as exc:

        if str(exc) in (
            "BINANCE_418",
            "BINANCE_429",
            "BINANCE_COOLDOWN_ACTIVE"
        ):

            logger.error(
                "%s | Binance temporarily blocked. BUY stopped.",
                symbol
            )

            return False

        logger.error(
            "%s | BUY runtime error: %s",
            symbol,
            exc
        )

        return False

    except Exception as exc:

        logger.error(
            "%s | BUY failed: %s",
            symbol,
            exc
        )

        logger.debug(
            traceback.format_exc()
        )

        return False

    finally:

        with state_lock:
            orders_in_flight.discard(
                symbol
            )

        try:
            order_lock.release()
        except RuntimeError:
            pass


# ============================================================
# PLACE SELL
# ============================================================

def place_sell(
    symbol,
    reason
):

    if is_binance_on_cooldown():

        logger.warning(
            "%s | Binance cooldown active. SELL skipped.",
            symbol
        )

        return False

    if not order_lock.acquire(
        blocking=False
    ):
        logger.info(
            "%s | Another order is in progress. SELL skipped.",
            symbol
        )
        return False

    try:

        with state_lock:

            position = positions.get(
                symbol
            )

        if not position:
            return False

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
            remove_position(symbol)
            return False

        quantity = (
            free_balance
            * SELL_BALANCE_BUFFER
        )

        quantity = normalize_quantity(
            symbol,
            quantity
        )

        if quantity is None:

            logger.error(
                "%s | SELL quantity invalid.",
                symbol
            )

            return False

        response = binance_request(
            "POST",
            "/api/v3/order",
            params={
                "symbol": symbol,
                "side": "SELL",
                "type": "MARKET",
                "quantity": decimal_to_string(
                    quantity
                ),
                "newOrderRespType": "FULL"
            },
            signed=True
        )

        logger.info(
            "%s | SELL SUCCESS | "
            "Reason=%s | Qty=%s | OrderID=%s",
            symbol,
            reason,
            decimal_to_string(quantity),
            response.get("orderId")
        )

        remove_position(
            symbol
        )

        return True

    except Exception as exc:

        logger.error(
            "%s | SELL failed: %s",
            symbol,
            exc
        )

        return False

    finally:

        try:
            order_lock.release()
        except RuntimeError:
            pass


# ============================================================
# PROCESS SIGNAL
# ============================================================

def process_signal(
    symbol,
    rsi_data
):

    if not rsi_data:
        return

    rsi3 = rsi_data["rsi3"]
    rsi50 = rsi_data["rsi50"]

    previous_rsi3 = (
        rsi_data["previous_rsi3"]
    )

    # ========================================================
    # SELL
    # ========================================================

    with state_lock:
        has_position = (
            symbol in positions
        )

    if has_position:

        if (
            previous_rsi3 is not None
            and previous_rsi3 <= SELL_RSI_LEVEL
            and rsi3 > SELL_RSI_LEVEL
        ):

            logger.info(
                "%s | SELL SIGNAL | "
                "RSI3 crossed %.2f upward | RSI3=%.4f | RSI50=%.4f",
                symbol,
                SELL_RSI_LEVEL,
                rsi3,
                rsi50
            )

            place_sell(
                symbol,
                "RSI3_CROSS_ABOVE_80"
            )

        return

    # ========================================================
    # BUY
    # ========================================================

    if (
        rsi50 > BUY_RSI_SLOW_MIN
        and rsi3 < BUY_RSI_FAST_MAX
    ):

        logger.info(
            "%s | BUY SIGNAL | "
            "RSI50=%.4f > %.2f | "
            "RSI3=%.4f < %.2f",
            symbol,
            rsi50,
            BUY_RSI_SLOW_MIN,
            rsi3,
            BUY_RSI_FAST_MAX
        )

        place_buy(
            symbol
        )


# ============================================================
# WEBSOCKET MESSAGE
# ============================================================

def handle_ws_message(
    message
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
            "k"
        )

        if not kline:
            return

        # ONLY CLOSED CANDLE
        if not kline.get(
            "x",
            False
        ):
            return

        symbol = kline.get(
            "s"
        )

        if not symbol:
            return

        close_price = float(
            kline.get("c")
        )

        candle_close_time = int(
            kline.get("T")
        )

        rsi_data = update_symbol_candle(
            symbol,
            close_price,
            candle_close_time
        )

        if not rsi_data:
            return

        logger.info(
            "%s | CLOSED 5m CANDLE | "
            "RSI3=%.4f | RSI50=%.4f | Candle=%s",
            symbol,
            rsi_data["rsi3"],
            rsi_data["rsi50"],
            utc_time_string(
                candle_close_time
            )
        )

        # IMPORTANT:
        # Signal processing is local.
        # No Binance REST request unless an actual order
        # is needed.
        process_signal(
            symbol,
            rsi_data
        )

    except Exception as exc:

        logger.error(
            "WebSocket message error: %s",
            exc
        )


# ============================================================
# WEBSOCKET GROUP
# ============================================================

def websocket_group_worker(
    group_id,
    symbols
):

    streams = "/".join(
        f"{symbol.lower()}@kline_{TIMEFRAME}"
        for symbol in symbols
    )

    url = (
        WS_BASE
        + streams
    )

    reconnect_delay = (
        WS_RECONNECT_MIN
    )

    while running:

        ws = None

        try:

            logger.info(
                "WebSocket group %s connecting | symbols=%s",
                group_id,
                len(symbols)
            )

            def on_open(ws_app):
                with state_lock:
                    ws_connected_groups.add(
                        group_id
                    )

                logger.info(
                    "WebSocket group %s CONNECTED",
                    group_id
                )

            def on_message(
                ws_app,
                message
            ):
                handle_ws_message(
                    message
                )

            def on_error(
                ws_app,
                error
            ):
                logger.error(
                    "WebSocket group %s error: %s",
                    group_id,
                    error
                )

            def on_close(
                ws_app,
                close_status_code,
                close_msg
            ):

                with state_lock:
                    ws_connected_groups.discard(
                        group_id
                    )

                logger.warning(
                    "WebSocket group %s CLOSED | code=%s | msg=%s",
                    group_id,
                    close_status_code,
                    close_msg
                )

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

            reconnect_delay = (
                WS_RECONNECT_MIN
            )

        except Exception as exc:

            logger.error(
                "WebSocket group %s crashed: %s",
                group_id,
                exc
            )

        finally:

            with state_lock:
                ws_connected_groups.discard(
                    group_id
                )

            try:
                if ws:
                    ws.close()
            except Exception:
                pass

        if not running:
            break

        logger.info(
            "WebSocket group %s reconnecting in %ss...",
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

    groups = []

    for i in range(GROUPS):

        start = (
            i
            * SYMBOLS_PER_GROUP
        )

        end = start + SYMBOLS_PER_GROUP

        group_symbols = top_symbols[
            start:end
        ]

        if group_symbols:
            groups.append(
                (
                    i + 1,
                    group_symbols
                )
            )

    for group_id, symbols in groups:

        thread = threading.Thread(
            target=websocket_group_worker,
            args=(
                group_id,
                symbols
            ),
            daemon=True,
            name=f"WS-{group_id}"
        )

        thread.start()

        # Stagger WebSocket connections
        time.sleep(2)

    logger.info(
        "Started %s WebSocket groups.",
        len(groups)
    )


# ============================================================
# RECOVERY
# ============================================================

def recover_positions():

    """
    Lightweight recovery.

    IMPORTANT:
    Do NOT call allOrders for all 150 symbols.
    That was one of the major unnecessary REST loads.

    We first check account balances.
    Only assets with a meaningful balance are candidates.
    """

    logger.info(
        "Starting lightweight position recovery..."
    )

    try:

        account = get_account()

    except Exception as exc:

        logger.error(
            "Position recovery account request failed: %s",
            exc
        )

        return

    balances = account.get(
        "balances",
        []
    )

    candidates = []

    top_set = set(
        top_symbols
    )

    for balance in balances:

        asset = balance.get(
            "asset"
        )

        if not asset:
            continue

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

        if total <= 0:
            continue

        symbol = asset + "USDT"

        if symbol in top_set:
            candidates.append(
                (
                    symbol,
                    total
                )
            )

    if not candidates:

        logger.info(
            "No existing top-symbol balances found for recovery."
        )

        return

    logger.info(
        "Recovery candidates: %s",
        ", ".join(
            x[0]
            for x in candidates
        )
    )

    for symbol, total_balance in candidates:

        if not running:
            break

        try:

            # Only ONE order history request for actual
            # balance-holding symbols.
            data = binance_request(
                "GET",
                "/api/v3/allOrders",
                params={
                    "symbol": symbol,
                    "limit": RECOVERY_ORDER_LIMIT
                },
                signed=True
            )

            buy_orders = []

            for order in data:

                if order.get("side") != "BUY":
                    continue

                client_id = order.get(
                    "clientOrderId",
                    ""
                )

                if not client_id.startswith(
                    BUY_CLIENT_PREFIX
                ):
                    continue

                if order.get(
                    "status"
                ) != "FILLED":
                    continue

                order_time = int(
                    order.get(
                        "time",
                        0
                    )
                )

                age_ms = (
                    int(time.time() * 1000)
                    - order_time
                )

                if age_ms > (
                    RECOVERY_LOOKBACK_DAYS
                    * 24
                    * 60
                    * 60
                    * 1000
                ):
                    continue

                buy_orders.append(
                    order
                )

            if not buy_orders:
                continue

            latest = max(
                buy_orders,
                key=lambda x: int(
                    x.get("time", 0)
                )
            )

            executed_qty = decimal_from(
                latest.get(
                    "executedQty",
                    "0"
                )
            )

            quote_qty = decimal_from(
                latest.get(
                    "cummulativeQuoteQty",
                    "0"
                )
            )

            if executed_qty <= 0:
                continue

            if quote_qty > 0:
                avg_price = (
                    quote_qty
                    / executed_qty
                )
            else:
                avg_price = (
                    await_fill_price_from_order(
                        latest
                    )
                )

            if avg_price <= 0:
                continue

            # Actual available balance
            available_qty = min(
                executed_qty,
                total_balance
            )

            available_qty = normalize_quantity(
                symbol,
                available_qty
            )

            if available_qty is None:
                continue

            save_position(
                symbol,
                avg_price,
                available_qty,
                latest.get("orderId")
            )

            logger.info(
                "%s | RECOVERED POSITION | "
                "Entry=%s | Qty=%s",
                symbol,
                decimal_to_string(
                    avg_price
                ),
                decimal_to_string(
                    available_qty
                )
            )

            # Check existing open orders.
            open_orders = binance_request(
                "GET",
                "/api/v3/openOrders",
                params={
                    "symbol": symbol
                },
                signed=True
            )

            has_sl = False

            for order in open_orders:

                client_id = order.get(
                    "clientOrderId",
                    ""
                )

                if client_id.startswith(
                    SL_CLIENT_PREFIX
                ):
                    has_sl = True

                    with state_lock:
                        if symbol in positions:
                            positions[symbol][
                                "stop_order_id"
                            ] = order.get(
                                "orderId"
                            )

                    break

            if not has_sl:

                place_stop_loss(
                    symbol,
                    avg_price,
                    available_qty
                )

            time.sleep(1)

        except Exception as exc:

            logger.error(
                "%s | Recovery failed: %s",
                symbol,
                exc
            )

            time.sleep(2)


def await_fill_price_from_order(order):
    """
    Fallback average price.
    Normally cummulativeQuoteQty is available.
    """

    executed_qty = decimal_from(
        order.get(
            "executedQty",
            "0"
        )
    )

    quote_qty = decimal_from(
        order.get(
            "cummulativeQuoteQty",
            "0"
        )
    )

    if executed_qty > 0 and quote_qty > 0:
        return (
            quote_qty
            / executed_qty
        )

    return Decimal("0")


# ============================================================
# BOT INITIALIZATION
# ============================================================

def initialize_bot():

    global bot_started
    global bot_ready
    global bot_error
    global bot_start_time

    with bot_start_lock:

        if bot_started:
            return

        bot_started = True
        bot_start_time = time.time()

    try:

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
            "Groups: %s",
            GROUPS
        )

        logger.info(
            "Symbols/group: %s",
            SYMBOLS_PER_GROUP
        )

        logger.info(
            "BUY amount: %s USDT",
            BUY_USDT
        )

        logger.info(
            "BUY: RSI50 > %s AND RSI3 < %s",
            BUY_RSI_SLOW_MIN,
            BUY_RSI_FAST_MAX
        )

        logger.info(
            "SELL: RSI3 cross above %s",
            SELL_RSI_LEVEL
        )

        logger.info(
            "Initial SL: %.2f%%",
            STOP_LOSS_PERCENT * 100
        )

        logger.info(
            "RSI history: %s candles",
            HISTORY_LIMIT
        )

        # ----------------------------------------------------
        # Server time
        # ----------------------------------------------------

        if not sync_server_time():
            raise RuntimeError(
                "Binance server time sync failed"
            )

        time.sleep(1)

        # ----------------------------------------------------
        # Exchange info
        # ----------------------------------------------------

        load_exchange_info()

        time.sleep(1)

        # ----------------------------------------------------
        # Top symbols
        # ----------------------------------------------------

        get_top_symbols()

        if not top_symbols:
            raise RuntimeError(
                "No top symbols found"
            )

        time.sleep(1)

        # ----------------------------------------------------
        # Historical candles
        # ----------------------------------------------------

        seed_all_symbols()

        # ----------------------------------------------------
        # Recovery
        # ----------------------------------------------------

        recover_positions()

        # ----------------------------------------------------
        # WebSocket
        # ----------------------------------------------------

        start_websockets()

        bot_ready = True
        bot_error = None

        logger.info(
            "=" * 70
        )

        logger.info(
            "BOT READY"
        )

        logger.info(
            "REST API is NOT used for candle/RSI monitoring."
        )

        logger.info(
            "=" * 70
        )

    except Exception as exc:

        bot_error = str(exc)

        logger.error(
            "BOT INITIALIZATION FAILED: %s",
            exc
        )

        logger.debug(
            traceback.format_exc()
        )


# ============================================================
# FLASK STARTUP
# ============================================================

@app.before_request
def start_bot_background():

    if not bot_started:

        thread = threading.Thread(
            target=initialize_bot,
            daemon=True,
            name="BotInitializer"
        )

        thread.start()


# ============================================================
# HOME
# ============================================================

@app.route("/")
def home():

    with state_lock:

        return jsonify({
            "status": "running"
            if bot_ready
            else "starting",

            "bot_ready": bot_ready,

            "bot_error": bot_error,

            "timeframe": TIMEFRAME,

            "top_symbols": len(
                top_symbols
            ),

            "websocket_groups": len(
                ws_connected_groups
            ),

            "positions": len(
                positions
            ),

            "binance_cooldown_seconds": (
                get_binance_cooldown_remaining()
            ),

            "buy_condition": (
                "RSI50 > 50 AND RSI3 < 10"
            ),

            "sell_condition": (
                "RSI3 crossing above 80"
            ),

            "stop_loss_percent": (
                float(
                    STOP_LOSS_PERCENT * 100
                )
            ),

            "server_time_offset_ms": (
                server_time_offset_ms
            )
        })


# ============================================================
# HEALTH
# ============================================================

@app.route("/health")
def health():

    with state_lock:

        websocket_count = len(
            ws_connected_groups
        )

        healthy = (
            bot_started
            and bot_ready
            and bot_error is None
            and websocket_count > 0
        )

        status_code = 200 if healthy else 503

        return jsonify({
            "healthy": healthy,

            "bot_started": bot_started,

            "bot_ready": bot_ready,

            "bot_error": bot_error,

            "websocket_groups": websocket_count,

            "positions": len(
                positions
            ),

            "binance_cooldown_seconds": (
                get_binance_cooldown_remaining()
            ),

            "uptime_seconds": (
                int(
                    time.time()
                    - bot_start_time
                )
                if bot_start_time
                else 0
            )
        }), status_code


# ============================================================
# SIGNAL HANDLERS
# ============================================================

def shutdown_handler(
    signum,
    frame
):

    global running

    logger.info(
        "Shutdown signal received: %s",
        signum
    )

    running = False


signal.signal(
    signal.SIGTERM,
    shutdown_handler
)

signal.signal(
    signal.SIGINT,
    shutdown_handler
)


# ============================================================
# MAIN
# ============================================================

if __name__ == "__main__":

    logger.info(
        "Starting Flask directly..."
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
