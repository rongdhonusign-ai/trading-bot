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
# CONFIG
# ============================================================

API_KEY = os.getenv("BINANCE_API_KEY")
API_SECRET = os.getenv("BINANCE_API_SECRET")

if not API_KEY or not API_SECRET:
    raise RuntimeError(
        "BINANCE_API_KEY / BINANCE_API_SECRET environment variables are missing."
    )

BASE_URL = "https://api.binance.com"
WS_BASE = "wss://stream.binance.com:9443/stream?streams="


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
# BOT SETTINGS
# ============================================================

HISTORY_LIMIT = 120

STARTUP_REST_DELAY = 0.15

BUY_COOLDOWN_SECONDS = 60

ORDER_WORKERS = 4

REQUEST_TIMEOUT = 15

RECV_WINDOW = 5000

MAX_REQUEST_RETRIES = 6

SELL_BALANCE_BUFFER = Decimal("0.999")

RECOVERY_LOOKBACK_DAYS = 7

RECOVERY_ORDER_LIMIT = 50

RECONNECT_MIN_SECONDS = 3

RECONNECT_MAX_SECONDS = 60

WS_MAX_LIFETIME = 23 * 60 * 60


# ============================================================
# CLIENT ORDER ID PREFIX
# ============================================================

BOT_BUY_PREFIX = "RSIBUY_"
BOT_SELL_PREFIX = "RSISELL_"
BOT_SL_PREFIX = "RSISL_"


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
# GLOBAL STATE
# ============================================================

app = Flask(__name__)

session = requests.Session()

session.headers.update({
    "X-MBX-APIKEY": API_KEY,
    "User-Agent": "RSI50-RSI3-BOT/1.0"
})


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


# ============================================================
# EXECUTOR
# ============================================================

executor = ThreadPoolExecutor(
    max_workers=ORDER_WORKERS,
    thread_name_prefix="ORDER"
)


# ============================================================
# HELPERS
# ============================================================

def now_ms():
    return int(time.time() * 1000)


def decimal(value):
    try:
        return Decimal(str(value))
    except Exception:
        return Decimal("0")


def decimal_to_string(value):
    value = Decimal(value)

    text = format(value, "f")

    if "." in text:
        text = text.rstrip("0").rstrip(".")

    return text


def floor_decimal(value, step):
    value = Decimal(value)
    step = Decimal(step)

    if step <= 0:
        return value

    return (value / step).to_integral_value(
        rounding=ROUND_DOWN
    ) * step


def safe_json(response):
    try:
        return response.json()
    except Exception:
        return None


# ============================================================
# BINANCE REQUEST
# ============================================================

def sign_params(params):
    query = urlencode(params, doseq=True)

    signature = hmac.new(
        API_SECRET.encode("utf-8"),
        query.encode("utf-8"),
        hashlib.sha256
    ).hexdigest()

    params["signature"] = signature

    return params


def binance_request(
    method,
    path,
    params=None,
    signed=False,
    retry=True
):

    if params is None:
        params = {}

    params = dict(params)

    if signed:

        params["timestamp"] = now_ms()
        params["recvWindow"] = RECV_WINDOW

        params = sign_params(params)

    retries = MAX_REQUEST_RETRIES if retry else 1

    for attempt in range(retries):

        try:

            if method == "GET":

                response = session.get(
                    BASE_URL + path,
                    params=params,
                    timeout=REQUEST_TIMEOUT
                )

            elif method == "POST":

                response = session.post(
                    BASE_URL + path,
                    params=params,
                    timeout=REQUEST_TIMEOUT
                )

            elif method == "DELETE":

                response = session.delete(
                    BASE_URL + path,
                    params=params,
                    timeout=REQUEST_TIMEOUT
                )

            else:
                raise ValueError(f"Unsupported HTTP method: {method}")

            data = safe_json(response)

            # ------------------------------------------------
            # SUCCESS
            # ------------------------------------------------

            if response.status_code in (200, 201):

                return data

            # ------------------------------------------------
            # RATE LIMIT
            # ------------------------------------------------

            if response.status_code in (418, 429):

                retry_after = response.headers.get("Retry-After")

                if retry_after:

                    try:
                        wait = float(retry_after)
                    except Exception:
                        wait = 2 ** attempt

                else:
                    wait = min(60, 2 ** attempt)

                wait += 0.5

                logger.warning(
                    "Binance rate limit %s | waiting %.2fs | attempt %d/%d",
                    response.status_code,
                    wait,
                    attempt + 1,
                    retries
                )

                time.sleep(wait)

                continue

            # ------------------------------------------------
            # TIMESTAMP ERROR
            # ------------------------------------------------

            if isinstance(data, dict):

                code = data.get("code")

                if code == -1021:

                    logger.warning(
                        "Binance timestamp error. Retrying..."
                    )

                    time.sleep(1)

                    continue

            # ------------------------------------------------
            # OTHER API ERROR
            # ------------------------------------------------

            logger.error(
                "Binance API error | HTTP=%s | path=%s | response=%s",
                response.status_code,
                path,
                data
            )

            if attempt < retries - 1:

                time.sleep(
                    min(10, 1.5 * (attempt + 1))
                )

                continue

            return None

        except requests.exceptions.RequestException as e:

            logger.warning(
                "Network error | %s | attempt %d/%d",
                e,
                attempt + 1,
                retries
            )

            if attempt < retries - 1:

                time.sleep(
                    min(10, 1.5 * (attempt + 1))
                )

                continue

            return None

        except Exception as e:

            logger.exception(
                "Unexpected Binance request error: %s",
                e
            )

            return None

    return None


# ============================================================
# RSI
# ============================================================

def calculate_rsi(series, period):

    numeric = pd.to_numeric(
        series,
        errors="coerce"
    )

    numeric = numeric.astype(float)

    delta = numeric.diff()

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

    rs = avg_gain / avg_loss.replace(0, float("nan"))

    rsi = 100 - (
        100 / (1 + rs)
    )

    rsi = rsi.fillna(100)

    return rsi


# ============================================================
# EXCHANGE INFO
# ============================================================

def load_exchange_info():

    global exchange_filters

    logger.info("Loading Binance exchange info...")

    data = binance_request(
        "GET",
        "/api/v3/exchangeInfo"
    )

    if not data:

        raise RuntimeError(
            "Could not load Binance exchange info."
        )

    count = 0

    for symbol_info in data.get("symbols", []):

        symbol = symbol_info.get("symbol")

        status = symbol_info.get("status")

        quote = symbol_info.get("quoteAsset")

        if status != "TRADING":
            continue

        if quote != "USDT":
            continue

        if symbol.endswith("USDT") is False:
            continue

        if symbol_info.get("isSpotTradingAllowed") is False:
            continue

        filters = {}

        for f in symbol_info.get("filters", []):

            filters[f.get("filterType")] = f

        exchange_filters[symbol] = filters

        count += 1

    logger.info(
        "Loaded %d USDT spot trading symbols.",
        count
    )


# ============================================================
# TOP SYMBOLS
# ============================================================

def get_top_symbols():

    global top_symbols

    logger.info(
        "Loading 24h ticker data..."
    )

    tickers = binance_request(
        "GET",
        "/api/v3/ticker/24hr"
    )

    if not tickers:

        raise RuntimeError(
            "Could not load 24h ticker data."
        )

    candidates = []

    for item in tickers:

        symbol = item.get("symbol", "")

        if not symbol.endswith("USDT"):
            continue

        base_asset = symbol[:-4]

        if base_asset in EXCLUDED_ASSETS:
            continue

        if symbol not in exchange_filters:
            continue

        try:
            quote_volume = float(
                item.get("quoteVolume", 0)
            )
        except Exception:
            quote_volume = 0

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

    selected = []

    for symbol, volume in candidates:

        filters = exchange_filters.get(
            symbol,
            {}
        )

        notional_filter = (
            filters.get("NOTIONAL")
            or filters.get("MIN_NOTIONAL")
        )

        if notional_filter:

            try:
                min_notional = Decimal(
                    str(
                        notional_filter.get(
                            "minNotional",
                            "0"
                        )
                    )
                )

                if min_notional > BUY_USDT:
                    continue

            except Exception:
                pass

        selected.append(symbol)

        if len(selected) >= TOP_SYMBOLS:
            break

    top_symbols = selected

    logger.info(
        "Selected %d symbols.",
        len(top_symbols)
    )

    logger.info(
        "Top symbols: %s",
        ", ".join(top_symbols[:20])
    )

    if not top_symbols:

        raise RuntimeError(
            "No eligible symbols found."
        )


# ============================================================
# KLINES
# ============================================================

def get_klines(symbol):

    data = binance_request(
        "GET",
        "/api/v3/klines",
        {
            "symbol": symbol,
            "interval": TIMEFRAME,
            "limit": HISTORY_LIMIT
        }
    )

    if not data:
        return None

    return data


# ============================================================
# SEED SYMBOL
# ============================================================

def seed_symbol(symbol):

    try:

        data = get_klines(symbol)

        if not data or len(data) < RSI_SLOW_PERIOD + 5:

            logger.warning(
                "%s | insufficient candle data",
                symbol
            )

            return False

        rows = []

        for k in data:

            try:

                close = float(k[4])

                close_time = int(k[6])

                rows.append(
                    {
                        "close": close,
                        "close_time": close_time
                    }
                )

            except Exception:
                continue

        if len(rows) < RSI_SLOW_PERIOD + 5:

            return False

        df = pd.DataFrame(rows)

        df["close"] = pd.to_numeric(
            df["close"],
            errors="coerce"
        )

        df = df.dropna(
            subset=["close"]
        )

        if len(df) < RSI_SLOW_PERIOD + 5:

            return False

        # Last candle can still be open.
        # We only use CLOSED candles.
        current_time = now_ms()

        closed = df[
            df["close_time"] <= current_time
        ].copy()

        if len(closed) < RSI_SLOW_PERIOD + 5:

            return False

        rsi3 = calculate_rsi(
            closed["close"],
            RSI_FAST_PERIOD
        )

        rsi50 = calculate_rsi(
            closed["close"],
            RSI_SLOW_PERIOD
        )

        state = {
            "last_closed_time": int(
                closed.iloc[-1]["close_time"]
            ),
            "rsi3": float(rsi3.iloc[-1]),
            "rsi50": float(rsi50.iloc[-1]),
            "prev_rsi3": (
                float(rsi3.iloc[-2])
                if len(rsi3) >= 2
                else None
            ),
            "close": float(
                closed.iloc[-1]["close"]
            )
        }

        with state_lock:

            symbol_state[symbol] = state

        return True

    except Exception:

        logger.exception(
            "%s | seed error",
            symbol
        )

        return False


# ============================================================
# SEED ALL SYMBOLS
# ============================================================

def seed_all_symbols():

    logger.info(
        "Seeding %d symbols...",
        len(top_symbols)
    )

    success = 0

    for index, symbol in enumerate(top_symbols, 1):

        if not running:
            break

        if seed_symbol(symbol):
            success += 1

        if index % 25 == 0:

            logger.info(
                "Seed progress: %d/%d",
                index,
                len(top_symbols)
            )

        time.sleep(
            STARTUP_REST_DELAY
        )

    logger.info(
        "Seed complete: %d/%d symbols.",
        success,
        len(top_symbols)
    )


# ============================================================
# FILTER HELPERS
# ============================================================

def get_quantity_filter(symbol):

    filters = exchange_filters.get(
        symbol,
        {}
    )

    return (
        filters.get("MARKET_LOT_SIZE")
        or filters.get("LOT_SIZE")
    )


def get_sell_quantity(symbol, balance):

    f = get_quantity_filter(symbol)

    if not f:
        return Decimal("0")

    step_size = decimal(
        f.get("stepSize", "0")
    )

    min_qty = decimal(
        f.get("minQty", "0")
    )

    qty = Decimal(balance) * SELL_BALANCE_BUFFER

    qty = floor_decimal(
        qty,
        step_size
    )

    if qty < min_qty:
        return Decimal("0")

    return qty


def get_price_tick(symbol):

    filters = exchange_filters.get(
        symbol,
        {}
    )

    f = filters.get(
        "PRICE_FILTER"
    )

    if not f:
        return Decimal("0")

    return decimal(
        f.get("tickSize", "0")
    )


# ============================================================
# STOP PRICE
# ============================================================

def calculate_stop_price(
    symbol,
    entry_price
):

    tick_size = get_price_tick(symbol)

    if tick_size <= 0:
        return None

    stop_price = (
        Decimal(entry_price)
        * (Decimal("1") - STOP_LOSS_PERCENT)
    )

    stop_price = floor_decimal(
        stop_price,
        tick_size
    )

    if stop_price <= 0:
        return None

    return stop_price


# ============================================================
# ACCOUNT
# ============================================================

def get_account():

    return binance_request(
        "GET",
        "/api/v3/account",
        signed=True
    )


def get_balance(asset):

    account = get_account()

    if not account:
        return Decimal("0")

    for item in account.get(
        "balances",
        []
    ):

        if item.get("asset") == asset:

            return decimal(
                item.get("free", "0")
            )

    return Decimal("0")


# ============================================================
# CURRENT PRICE
# ============================================================

def get_current_price(symbol):

    data = binance_request(
        "GET",
        "/api/v3/ticker/price",
        {
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
# OPEN ORDERS
# ============================================================

def get_open_orders(symbol):

    return binance_request(
        "GET",
        "/api/v3/openOrders",
        {
            "symbol": symbol
        },
        signed=True
    )


def find_existing_stop_loss(symbol):

    orders = get_open_orders(symbol)

    if orders is None:
        return None

    for order in orders:

        if order.get("side") != "SELL":
            continue

        if order.get("type") not in (
            "STOP_LOSS",
            "STOP_LOSS_LIMIT"
        ):
            continue

        client_id = order.get(
            "clientOrderId",
            ""
        )

        if client_id.startswith(
            BOT_SL_PREFIX
        ):

            return order

    return None


# ============================================================
# ORDER STATUS
# ============================================================

def get_order(symbol, order_id):

    return binance_request(
        "GET",
        "/api/v3/order",
        {
            "symbol": symbol,
            "orderId": order_id
        },
        signed=True
    )


# ============================================================
# PLACE SERVER-SIDE STOP LOSS
# ============================================================

def place_stop_loss(
    symbol,
    entry_price
):

    try:

        with state_lock:

            position = positions.get(
                symbol
            )

        if not position:
            return None

        asset = symbol[:-4]

        balance = get_balance(asset)

        quantity = get_sell_quantity(
            symbol,
            balance
        )

        if quantity <= 0:

            logger.error(
                "%s | cannot create SL: quantity too small.",
                symbol
            )

            return None

        stop_price = calculate_stop_price(
            symbol,
            entry_price
        )

        if stop_price is None:

            logger.error(
                "%s | cannot calculate stop price.",
                symbol
            )

            return None

        current_price = get_current_price(
            symbol
        )

        if (
            current_price is not None
            and current_price <= stop_price
        ):

            logger.warning(
                "%s | current price %.8f <= stop %.8f. Emergency SELL required.",
                symbol,
                current_price,
                stop_price
            )

            return "MARKET_SELL_REQUIRED"

        client_id = (
            BOT_SL_PREFIX
            + str(int(time.time() * 1000))[-20:]
        )

        params = {
            "symbol": symbol,
            "side": "SELL",
            "type": "STOP_LOSS",
            "quantity": decimal_to_string(quantity),
            "stopPrice": decimal_to_string(stop_price),
            "newClientOrderId": client_id
        }

        order = binance_request(
            "POST",
            "/api/v3/order",
            params,
            signed=True
        )

        if not order:

            logger.error(
                "%s | server-side SL placement FAILED.",
                symbol
            )

            return None

        with state_lock:

            if symbol in positions:

                positions[symbol]["stop_order_id"] = (
                    order.get("orderId")
                )

                positions[symbol]["stop_price"] = (
                    stop_price
                )

        logger.info(
            "%s | SERVER SL placed | stop=%.8f | qty=%s | orderId=%s",
            symbol,
            stop_price,
            decimal_to_string(quantity),
            order.get("orderId")
        )

        return order

    except Exception:

        logger.exception(
            "%s | stop loss error",
            symbol
        )

        return None


# ============================================================
# CANCEL STOP LOSS
# ============================================================

def cancel_stop_loss(symbol):

    with state_lock:

        position = positions.get(
            symbol
        )

    if not position:
        return True

    order_id = position.get(
        "stop_order_id"
    )

    if not order_id:
        return True

    result = binance_request(
        "DELETE",
        "/api/v3/order",
        {
            "symbol": symbol,
            "orderId": order_id
        },
        signed=True
    )

    if result:

        logger.info(
            "%s | existing SL cancelled | orderId=%s",
            symbol,
            order_id
        )

        with state_lock:

            if symbol in positions:

                positions[symbol]["stop_order_id"] = None

        return True

    # --------------------------------------------------------
    # If DELETE failed, check whether SL was already filled.
    # --------------------------------------------------------

    status = get_order(
        symbol,
        order_id
    )

    if status:

        order_status = status.get(
            "status"
        )

        if order_status == "FILLED":

            logger.warning(
                "%s | SL already FILLED before RSI SELL.",
                symbol
            )

            with state_lock:
                positions.pop(
                    symbol,
                    None
                )

            return False

        if order_status in (
            "CANCELED",
            "EXPIRED",
            "REJECTED"
        ):

            with state_lock:

                if symbol in positions:
                    positions[symbol][
                        "stop_order_id"
                    ] = None

            return True

    logger.error(
        "%s | could not safely cancel SL.",
        symbol
    )

    return False


# ============================================================
# EMERGENCY MARKET SELL
# ============================================================

def emergency_market_sell(
    symbol,
    reason
):

    logger.warning(
        "%s | EMERGENCY MARKET SELL | %s",
        symbol,
        reason
    )

    # Remove the BUY task's in-flight lock first.
    # Otherwise place_sell() would immediately return.
    with state_lock:

        orders_in_flight.discard(
            symbol
        )

    try:

        place_sell(
            symbol,
            reason=reason
        )

    except Exception:

        logger.exception(
            "%s | emergency SELL failed",
            symbol
        )


# ============================================================
# PLACE BUY
# ============================================================

def place_buy(symbol):

    with state_lock:

        if symbol in positions:
            return

        if symbol in orders_in_flight:
            return

        now = time.time()

        previous_buy = last_buy_time.get(
            symbol,
            0
        )

        if now - previous_buy < BUY_COOLDOWN_SECONDS:

            return

        orders_in_flight.add(
            symbol
        )

        last_buy_time[symbol] = now

    try:

        logger.info(
            "%s | BUY signal | RSI50>50 and RSI3<10",
            symbol
        )

        params = {
            "symbol": symbol,
            "side": "BUY",
            "type": "MARKET",
            "quoteOrderQty": decimal_to_string(
                BUY_USDT
            ),
            "newClientOrderId": (
                BOT_BUY_PREFIX
                + str(int(time.time() * 1000))[-20:]
            )
        }

        order = binance_request(
            "POST",
            "/api/v3/order",
            params,
            signed=True
        )

        if not order:

            logger.error(
                "%s | BUY failed.",
                symbol
            )

            return

        status = order.get(
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

        executed_qty = decimal(
            order.get(
                "executedQty",
                "0"
            )
        )

        quote_qty = decimal(
            order.get(
                "cummulativeQuoteQty",
                "0"
            )
        )

        if executed_qty <= 0:

            logger.error(
                "%s | BUY executedQty is zero.",
                symbol
            )

            return

        if quote_qty <= 0:

            logger.error(
                "%s | BUY quote quantity is zero.",
                symbol
            )

            return

        avg_entry = (
            quote_qty
            / executed_qty
        )

        asset = symbol[:-4]

        # ----------------------------------------------------
        # Save position BEFORE placing SL.
        # ----------------------------------------------------

        with state_lock:

            positions[symbol] = {
                "symbol": symbol,
                "qty": executed_qty,
                "entry_price": avg_entry,
                "stop_price": None,
                "stop_order_id": None,
                "buy_order_id": order.get(
                    "orderId"
                ),
                "buy_time": time.time()
            }

        logger.info(
            "%s | BUY FILLED | qty=%s | avg_entry=%.8f",
            symbol,
            decimal_to_string(executed_qty),
            avg_entry
        )

        # ----------------------------------------------------
        # Place server-side 1% SL.
        # ----------------------------------------------------

        sl_result = place_stop_loss(
            symbol,
            avg_entry
        )

        if sl_result == "MARKET_SELL_REQUIRED":

            # Important:
            # remove in-flight lock BEFORE emergency SELL.
            emergency_market_sell(
                symbol,
                "Price already at/below SL after BUY"
            )

            return

        if sl_result is None:

            emergency_market_sell(
                symbol,
                "Server-side SL placement failed"
            )

            return

        logger.info(
            "%s | BUY protection active.",
            symbol
        )

    except Exception:

        logger.exception(
            "%s | BUY processing error",
            symbol
        )

    finally:

        with state_lock:

            orders_in_flight.discard(
                symbol
            )


# ============================================================
# PLACE SELL
# ============================================================

def place_sell(
    symbol,
    reason="RSI SELL"
):

    with state_lock:

        if symbol not in positions:
            return

        if symbol in orders_in_flight:
            return

        orders_in_flight.add(
            symbol
        )

    try:

        logger.info(
            "%s | SELL started | reason=%s",
            symbol,
            reason
        )

        # ----------------------------------------------------
        # Cancel server-side SL first.
        # ----------------------------------------------------

        sl_cancelled = cancel_stop_loss(
            symbol
        )

        if not sl_cancelled:

            logger.warning(
                "%s | SELL stopped because SL may already be filled.",
                symbol
            )

            return

        asset = symbol[:-4]

        balance = get_balance(
            asset
        )

        quantity = get_sell_quantity(
            symbol,
            balance
        )

        if quantity <= 0:

            logger.warning(
                "%s | no sellable quantity.",
                symbol
            )

            with state_lock:
                positions.pop(
                    symbol,
                    None
                )

            return

        params = {
            "symbol": symbol,
            "side": "SELL",
            "type": "MARKET",
            "quantity": decimal_to_string(
                quantity
            ),
            "newClientOrderId": (
                BOT_SELL_PREFIX
                + str(int(time.time() * 1000))[-20:]
            )
        }

        order = binance_request(
            "POST",
            "/api/v3/order",
            params,
            signed=True
        )

        if not order:

            logger.error(
                "%s | SELL failed. Recreating SL.",
                symbol
            )

            with state_lock:

                position = positions.get(
                    symbol
                )

            if position:

                place_stop_loss(
                    symbol,
                    position["entry_price"]
                )

            return

        status = order.get(
            "status"
        )

        executed_qty = decimal(
            order.get(
                "executedQty",
                "0"
            )
        )

        logger.info(
            "%s | SELL result | status=%s | qty=%s",
            symbol,
            status,
            decimal_to_string(executed_qty)
        )

        if status == "FILLED":

            with state_lock:

                positions.pop(
                    symbol,
                    None
                )

            logger.info(
                "%s | POSITION CLOSED.",
                symbol
            )

            return

        if status == "PARTIALLY_FILLED":

            remaining_balance = get_balance(
                asset
            )

            remaining_qty = get_sell_quantity(
                symbol,
                remaining_balance
            )

            if remaining_qty <= 0:

                with state_lock:

                    positions.pop(
                        symbol,
                        None
                    )

                return

            with state_lock:

                if symbol in positions:

                    positions[symbol]["qty"] = (
                        remaining_qty
                    )

            # Re-protect remaining position.
            with state_lock:

                position = positions.get(
                    symbol
                )

            if position:

                place_stop_loss(
                    symbol,
                    position["entry_price"]
                )

            return

        # ----------------------------------------------------
        # If order was rejected/cancelled.
        # ----------------------------------------------------

        logger.error(
            "%s | SELL status=%s. Recreating SL.",
            symbol,
            status
        )

        with state_lock:

            position = positions.get(
                symbol
            )

        if position:

            place_stop_loss(
                symbol,
                position["entry_price"]
            )

    except Exception:

        logger.exception(
            "%s | SELL processing error",
            symbol
        )

        # Try to restore protection.
        try:

            with state_lock:

                position = positions.get(
                    symbol
                )

            if position:

                place_stop_loss(
                    symbol,
                    position["entry_price"]
                )

        except Exception:

            logger.exception(
                "%s | failed to restore SL after SELL error",
                symbol
            )

    finally:

        with state_lock:

            orders_in_flight.discard(
                symbol
            )


# ============================================================
# SUBMIT BUY
# ============================================================

def submit_buy(symbol):

    try:

        executor.submit(
            place_buy,
            symbol
        )

    except Exception:

        logger.exception(
            "%s | submit BUY failed",
            symbol
        )


# ============================================================
# SUBMIT SELL
# ============================================================

def submit_sell(
    symbol,
    reason="RSI SELL"
):

    try:

        executor.submit(
            place_sell,
            symbol,
            reason
        )

    except Exception:

        logger.exception(
            "%s | submit SELL failed",
            symbol
        )


# ============================================================
# RECOVERY
# ============================================================

def find_recent_bot_buy(symbol):

    start_time = (
        now_ms()
        - RECOVERY_LOOKBACK_DAYS
        * 24
        * 60
        * 60
        * 1000
    )

    orders = binance_request(
        "GET",
        "/api/v3/allOrders",
        {
            "symbol": symbol,
            "startTime": start_time,
            "limit": RECOVERY_ORDER_LIMIT
        },
        signed=True
    )

    if not orders:
        return None

    recent = []

    for order in orders:

        if order.get("side") != "BUY":
            continue

        if order.get("status") != "FILLED":
            continue

        client_id = order.get(
            "clientOrderId",
            ""
        )

        if not client_id.startswith(
            BOT_BUY_PREFIX
        ):
            continue

        recent.append(
            order
        )

    if not recent:
        return None

    recent.sort(
        key=lambda x: int(
            x.get(
                "time",
                0
            )
        ),
        reverse=True
    )

    return recent[0]


# ============================================================
# RECOVER POSITIONS
# ============================================================

def recover_positions():

    logger.info(
        "Checking existing Binance positions..."
    )

    account = get_account()

    if not account:

        logger.warning(
            "Could not load account for recovery."
        )

        return

    balances = account.get(
        "balances",
        []
    )

    recovered = 0

    for balance in balances:

        if not running:
            break

        asset = balance.get(
            "asset"
        )

        free = decimal(
            balance.get(
                "free",
                "0"
            )
        )

        locked = decimal(
            balance.get(
                "locked",
                "0"
            )
        )

        total = free + locked

        if total <= 0:
            continue

        if asset in EXCLUDED_ASSETS:
            continue

        symbol = asset + "USDT"

        if symbol not in top_symbols:
            continue

        logger.info(
            "%s | checking recovery position | balance=%s",
            symbol,
            total
        )

        buy_order = find_recent_bot_buy(
            symbol
        )

        if not buy_order:

            continue

        executed_qty = decimal(
            buy_order.get(
                "executedQty",
                "0"
            )
        )

        quote_qty = decimal(
            buy_order.get(
                "cummulativeQuoteQty",
                "0"
            )
        )

        if executed_qty <= 0:
            continue

        if quote_qty <= 0:
            continue

        avg_entry = (
            quote_qty
            / executed_qty
        )

        actual_qty = get_sell_quantity(
            symbol,
            total
        )

        if actual_qty <= 0:

            logger.warning(
                "%s | recovered balance too small.",
                symbol
            )

            continue

        with state_lock:

            positions[symbol] = {
                "symbol": symbol,
                "qty": actual_qty,
                "entry_price": avg_entry,
                "stop_price": None,
                "stop_order_id": None,
                "buy_order_id": buy_order.get(
                    "orderId"
                ),
                "buy_time": (
                    int(
                        buy_order.get(
                            "time",
                            now_ms()
                        )
                    ) / 1000
                )
            }

        recovered += 1

        logger.info(
            "%s | POSITION RECOVERED | qty=%s | entry=%.8f",
            symbol,
            decimal_to_string(actual_qty),
            avg_entry
        )

        # ----------------------------------------------------
        # Check existing server-side SL.
        # ----------------------------------------------------

        existing_sl = find_existing_stop_loss(
            symbol
        )

        if existing_sl:

            stop_price = decimal(
                existing_sl.get(
                    "stopPrice",
                    "0"
                )
            )

            with state_lock:

                if symbol in positions:

                    positions[symbol][
                        "stop_order_id"
                    ] = existing_sl.get(
                        "orderId"
                    )

                    positions[symbol][
                        "stop_price"
                    ] = stop_price

            logger.info(
                "%s | existing SL found | stop=%.8f",
                symbol,
                stop_price
            )

        else:

            logger.warning(
                "%s | no existing SL. Creating one...",
                symbol
            )

            sl = place_stop_loss(
                symbol,
                avg_entry
            )

            if sl == "MARKET_SELL_REQUIRED":

                emergency_market_sell(
                    symbol,
                    "Recovered position already below SL"
                )

        time.sleep(
            STARTUP_REST_DELAY
        )

    logger.info(
        "Recovery complete | recovered=%d",
        recovered
    )


# ============================================================
# PROCESS CLOSED CANDLE
# ============================================================

def process_closed_candle(
    symbol,
    candle
):

    try:

        close_time = int(
            candle["T"]
        )

        close_price = float(
            candle["c"]
        )

        # ----------------------------------------------------
        # Calculate RSI using recent state + current candle.
        # We use REST seed data plus incoming closed candle.
        # ----------------------------------------------------

        with state_lock:

            old_state = symbol_state.get(
                symbol
            )

        if old_state is None:
            return

        previous_rsi3 = old_state.get(
            "rsi3"
        )

        # ----------------------------------------------------
        # Fetch a small fresh candle history only when needed
        # to ensure accurate RSI calculation.
        #
        # This is only once per 5m candle per symbol.
        # ----------------------------------------------------

        data = get_klines(
            symbol
        )

        if not data:
            return

        rows = []

        current_ms = now_ms()

        for k in data:

            try:

                k_close = float(k[4])
                k_close_time = int(k[6])

                if k_close_time <= current_ms:

                    rows.append(
                        {
                            "close": k_close,
                            "close_time": k_close_time
                        }
                    )

            except Exception:
                continue

        if len(rows) < RSI_SLOW_PERIOD + 5:
            return

        df = pd.DataFrame(rows)

        df["close"] = pd.to_numeric(
            df["close"],
            errors="coerce"
        )

        df = df.dropna(
            subset=["close"]
        )

        if len(df) < RSI_SLOW_PERIOD + 5:
            return

        rsi3_series = calculate_rsi(
            df["close"],
            RSI_FAST_PERIOD
        )

        rsi50_series = calculate_rsi(
            df["close"],
            RSI_SLOW_PERIOD
        )

        current_rsi3 = float(
            rsi3_series.iloc[-1]
        )

        current_rsi50 = float(
            rsi50_series.iloc[-1]
        )

        # ----------------------------------------------------
        # Prevent duplicate candle processing.
        # ----------------------------------------------------

        with state_lock:

            current_state = symbol_state.get(
                symbol
            )

            if current_state:

                if close_time <= int(
                    current_state.get(
                        "last_closed_time",
                        0
                    )
                ):

                    return

            symbol_state[symbol] = {
                "last_closed_time": close_time,
                "rsi3": current_rsi3,
                "rsi50": current_rsi50,
                "prev_rsi3": previous_rsi3,
                "close": close_price
            }

        logger.info(
            "%s | CLOSED 5m | close=%.8f | RSI50=%.2f | RSI3=%.2f",
            symbol,
            close_price,
            current_rsi50,
            current_rsi3
        )

        # ====================================================
        # SELL
        # ====================================================

        with state_lock:

            has_position = (
                symbol in positions
            )

        if has_position:

            crossed_above_80 = (
                previous_rsi3 is not None
                and previous_rsi3 <= SELL_RSI_LEVEL
                and current_rsi3 > SELL_RSI_LEVEL
            )

            if crossed_above_80:

                logger.info(
                    "%s | SELL SIGNAL | RSI3 crossed above %d",
                    symbol,
                    SELL_RSI_LEVEL
                )

                submit_sell(
                    symbol,
                    "RSI3 crossed above 80"
                )

                return

        # ====================================================
        # BUY
        # ====================================================

        with state_lock:

            has_position = (
                symbol in positions
            )

        if has_position:
            return

        buy_signal = (
            current_rsi50 > BUY_RSI_SLOW_MIN
            and current_rsi3 < BUY_RSI_FAST_MAX
        )

        if buy_signal:

            logger.info(
                "%s | BUY SIGNAL | RSI50=%.2f > %d | RSI3=%.2f < %d",
                symbol,
                current_rsi50,
                BUY_RSI_SLOW_MIN,
                current_rsi3,
                BUY_RSI_FAST_MAX
            )

            submit_buy(
                symbol
            )

    except Exception:

        logger.exception(
            "%s | candle processing error",
            symbol
        )


# ============================================================
# WEBSOCKET CALLBACKS
# ============================================================

def make_ws_callbacks(
    group_id,
    symbols
):

    def on_open(ws):

        logger.info(
            "WebSocket group %d CONNECTED | symbols=%d",
            group_id,
            len(symbols)
        )

        with state_lock:

            ws_connected_groups.add(
                group_id
            )

    def on_message(ws, message):

        try:

            data = json.loads(
                message
            )

            payload = data.get(
                "data",
                {}
            )

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

            # Only CLOSED candle.
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

            process_closed_candle(
                symbol,
                kline
            )

        except Exception:

            logger.exception(
                "WebSocket group %d message error",
                group_id
            )

    def on_error(ws, error):

        logger.warning(
            "WebSocket group %d error: %s",
            group_id,
            error
        )

    def on_close(
        ws,
        close_status_code,
        close_msg
    ):

        logger.warning(
            "WebSocket group %d CLOSED | code=%s | msg=%s",
            group_id,
            close_status_code,
            close_msg
        )

        with state_lock:

            ws_connected_groups.discard(
                group_id
            )

    return (
        on_open,
        on_message,
        on_error,
        on_close
    )


# ============================================================
# WEBSOCKET WORKER
# ============================================================

def websocket_worker(
    group_id,
    symbols
):

    streams = "/".join(
        symbol.lower()
        + "@kline_"
        + TIMEFRAME
        for symbol in symbols
    )

    url = (
        WS_BASE
        + streams
    )

    reconnect_delay = (
        RECONNECT_MIN_SECONDS
    )

    while running:

        ws = None

        started = time.time()

        try:

            logger.info(
                "Starting WebSocket group %d | %d symbols",
                group_id,
                len(symbols)
            )

            (
                on_open,
                on_message,
                on_error,
                on_close
            ) = make_ws_callbacks(
                group_id,
                symbols
            )

            ws = websocket.WebSocketApp(
                url,
                on_open=on_open,
                on_message=on_message,
                on_error=on_error,
                on_close=on_close
            )

            # ------------------------------------------------
            # IMPORTANT:
            #
            # ping_interval=None / ping_timeout=None
            # avoids the previous:
            #
            # "ping/pong timed out"
            #
            # The Binance websocket connection itself remains
            # active and the worker reconnects if it closes.
            # ------------------------------------------------

            ws.run_forever(
                ping_interval=None,
                ping_timeout=None,
                skip_utf8_validation=True
            )

            elapsed = time.time() - started

            if elapsed >= WS_MAX_LIFETIME:

                logger.info(
                    "WebSocket group %d reached max lifetime. Reconnecting.",
                    group_id
                )

            else:

                logger.warning(
                    "WebSocket group %d disconnected after %.1fs.",
                    group_id,
                    elapsed
                )

        except Exception:

            logger.exception(
                "WebSocket group %d crashed",
                group_id
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
            "WebSocket group %d reconnecting in %d seconds...",
            group_id,
            reconnect_delay
        )

        time.sleep(
            reconnect_delay
        )

        reconnect_delay = min(
            reconnect_delay * 2,
            RECONNECT_MAX_SECONDS
        )


# ============================================================
# START WEBSOCKETS
# ============================================================

def start_websockets():

    if not top_symbols:
        return

    groups = []

    for i in range(
        GROUPS
    ):

        start = (
            i
            * SYMBOLS_PER_GROUP
        )

        end = start + SYMBOLS_PER_GROUP

        group = top_symbols[
            start:end
        ]

        if group:

            groups.append(
                group
            )

    logger.info(
        "Starting %d WebSocket groups...",
        len(groups)
    )

    for group_id, symbols in enumerate(
        groups,
        1
    ):

        thread = threading.Thread(
            target=websocket_worker,
            args=(
                group_id,
                symbols
            ),
            daemon=True,
            name=f"WS-GROUP-{group_id}"
        )

        thread.start()

        time.sleep(
            1
        )


# ============================================================
# BOT STARTUP
# ============================================================

def start_bot():

    global bot_ready
    global bot_error
    global bot_start_time

    bot_start_time = time.time()

    logger.info("=" * 70)

    logger.info(
        "STARTING BINANCE RSI50 + RSI3 BOT"
    )

    logger.info("=" * 70)

    logger.info(
        "Timeframe: %s",
        TIMEFRAME
    )

    logger.info(
        "Top symbols: %d",
        TOP_SYMBOLS
    )

    logger.info(
        "BUY: RSI50 > %d AND RSI3 < %d",
        BUY_RSI_SLOW_MIN,
        BUY_RSI_FAST_MAX
    )

    logger.info(
        "BUY amount: %s USDT",
        BUY_USDT
    )

    logger.info(
        "SELL: RSI3 crossing above %d",
        SELL_RSI_LEVEL
    )

    logger.info(
        "Initial SL: %.2f%%",
        STOP_LOSS_PERCENT * 100
    )

    logger.info(
        "WebSocket groups: %d",
        GROUPS
    )

    logger.info("=" * 70)

    # --------------------------------------------------------
    # Binance connectivity test
    # --------------------------------------------------------

    ping = binance_request(
        "GET",
        "/api/v3/ping"
    )

    if ping is None:

        raise RuntimeError(
            "Binance REST connection failed."
        )

    logger.info(
        "Binance REST connection: OK"
    )

    # --------------------------------------------------------
    # Load exchange data
    # --------------------------------------------------------

    load_exchange_info()

    get_top_symbols()

    # --------------------------------------------------------
    # Recover positions BEFORE new signals.
    # --------------------------------------------------------

    recover_positions()

    # --------------------------------------------------------
    # Seed RSI data.
    # --------------------------------------------------------

    seed_all_symbols()

    # --------------------------------------------------------
    # Start WebSockets.
    # --------------------------------------------------------

    start_websockets()

    time.sleep(3)

    with state_lock:

        bot_ready = True

    logger.info("=" * 70)

    logger.info(
        "BOT IS LIVE"
    )

    logger.info(
        "Symbols: %d",
        len(top_symbols)
    )

    logger.info(
        "Positions: %d",
        len(positions)
    )

    logger.info("=" * 70)


# ============================================================
# SAFE BOT THREAD
# ============================================================

def bot_thread_runner():

    global bot_ready
    global bot_error

    logger.info(
        "BOT BACKGROUND THREAD STARTED"
    )

    try:

        start_bot()

    except Exception as e:

        bot_error = traceback.format_exc()

        logger.exception(
            "FATAL BOT STARTUP ERROR: %s",
            e
        )

        bot_ready = False


def start_bot_background():

    global bot_started

    with bot_start_lock:

        if bot_started:

            return

        bot_started = True

    thread = threading.Thread(
        target=bot_thread_runner,
        daemon=True,
        name="BOT"
    )

    thread.start()

    logger.info(
        "Bot startup thread launched."
    )


# ============================================================
# HEALTH / STATUS
# ============================================================

@app.route("/")
def home():

    with state_lock:

        position_copy = dict(
            positions
        )

        ws_groups = list(
            ws_connected_groups
        )

        state_count = len(
            symbol_state
        )

    return jsonify({

        "status": "running",

        "bot_started": bot_started,

        "bot_ready": bot_ready,

        "bot_error": (
            bot_error
            if bot_error
            else None
        ),

        "strategy": (
            "RSI50 > 50 AND RSI3 < 10 BUY"
        ),

        "sell_strategy": (
            "RSI3 crossing above 80"
        ),

        "timeframe": TIMEFRAME,

        "buy_usdt": float(
            BUY_USDT
        ),

        "stop_loss_percent": float(
            STOP_LOSS_PERCENT * 100
        ),

        "top_symbols": len(
            top_symbols
        ),

        "seeded_symbols": state_count,

        "positions": len(
            position_copy
        ),

        "websocket_groups_connected": ws_groups,

        "position_data": position_copy
    })


@app.route("/health")
def health():

    with state_lock:

        return jsonify({

            "status": "healthy",

            "bot_started": bot_started,

            "bot_ready": bot_ready,

            "bot_error": (
                bot_error
                if bot_error
                else None
            ),

            "running": running,

            "symbols": len(
                top_symbols
            ),

            "positions": len(
                positions
            ),

            "orders_in_flight": len(
                orders_in_flight
            ),

            "ws_connected_groups": len(
                ws_connected_groups
            ),

            "ws_groups": sorted(
                list(
                    ws_connected_groups
                )
            )
        })


# ============================================================
# SHUTDOWN
# ============================================================

def shutdown_handler(
    signum=None,
    frame=None
):

    global running

    logger.warning(
        "Shutdown signal received."
    )

    running = False

    try:
        executor.shutdown(
            wait=False,
            cancel_futures=True
        )
    except Exception:
        pass


# ============================================================
# SIGNAL HANDLERS
# ============================================================

try:

    signal.signal(
        signal.SIGTERM,
        shutdown_handler
    )

    signal.signal(
        signal.SIGINT,
        shutdown_handler
    )

except Exception:

    logger.warning(
        "Could not register signal handlers."
    )


# ============================================================
# START BOT BACKGROUND THREAD
#
# IMPORTANT:
#
# This is intentionally OUTSIDE:
#
# if __name__ == "__main__":
#
# because Render uses Gunicorn:
#
# gunicorn ... main:app
#
# In that case __name__ is NOT "__main__".
# ============================================================

start_bot_background()


# ============================================================
# LOCAL RUN
# ============================================================

if __name__ == "__main__":

    port = int(
        os.getenv(
            "PORT",
            "10000"
        )
    )

    logger.info(
        "Starting Flask development server on port %d",
        port
    )

    app.run(
        host="0.0.0.0",
        port=port,
        threaded=True
    )
