import os
import time
import json
import hmac
import hashlib
import threading
import logging
from decimal import Decimal, ROUND_DOWN
from urllib.parse import urlencode

import requests
import websocket

from flask import Flask, jsonify


# ============================================================
# CONFIG
# ============================================================

API_KEY = os.environ.get("BINANCE_API_KEY")
API_SECRET = os.environ.get("BINANCE_API_SECRET")

if not API_KEY or not API_SECRET:
    raise RuntimeError("BINANCE_API_KEY / BINANCE_API_SECRET missing")

BASE_URL = "https://api.binance.com"
WS_BASE_URL = "wss://stream.binance.com:9443/stream"

TIMEFRAME = "5m"

TOP_SYMBOLS = 150
GROUPS = 3
SYMBOLS_PER_GROUP = 50

BUY_USDT = 15.0

RSI_FAST_PERIOD = 3
RSI_SLOW_PERIOD = 50

BUY_RSI_SLOW_MIN = 50.0
BUY_RSI_FAST_MAX = 10.0

SELL_RSI_LEVEL = 80.0

STOP_LOSS_PERCENT = 0.01
SELL_BALANCE_BUFFER = 0.999

HISTORY_LIMIT = 500

BUY_COOLDOWN_SECONDS = 60

KLINE_DELAY_SECONDS = 0.50

WS_PING_INTERVAL = 30
WS_PING_TIMEOUT = 20

RECONNECT_DELAY = 10

# Binance temporary block protection
COOLDOWN_418_SECONDS = 3600
COOLDOWN_429_SECONDS = 300

REQUEST_TIMEOUT = 20

BUY_CLIENT_PREFIX = "RSIBUY_"
SL_CLIENT_PREFIX = "RSISL_"

BOT_NAME = "BINANCE RSI50 + RSI3 BOT"


# ============================================================
# LOGGING
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

logger = logging.getLogger(BOT_NAME)


# ============================================================
# FLASK
# ============================================================

app = Flask(__name__)


# ============================================================
# GLOBAL STATE
# ============================================================

session = requests.Session()
session.headers.update({
    "X-MBX-APIKEY": API_KEY
})

server_time_offset_ms = 0

exchange_info = None
symbol_info = {}

symbols = []

rsi_states = {}

positions = {}

last_buy_time = {}

websocket_threads = []

order_lock = threading.Lock()

state_lock = threading.Lock()

initialization_lock = threading.Lock()

bot_ready = False
bot_initializing = False

bot_status = "starting"

binance_cooldown_until = 0

initialization_thread_started = False


# ============================================================
# HELPERS
# ============================================================

def now_ms():
    return int(time.time() * 1000)


def cooldown_active():
    return time.time() < binance_cooldown_until


def cooldown_remaining():
    return max(0, int(binance_cooldown_until - time.time()))


def activate_cooldown(seconds, reason):
    global binance_cooldown_until

    new_until = time.time() + seconds

    if new_until > binance_cooldown_until:
        binance_cooldown_until = new_until

    logger.warning(
        "Binance cooldown activated for %s seconds. Reason=%s",
        cooldown_remaining(),
        reason
    )


def clear_cooldown():
    global binance_cooldown_until

    if binance_cooldown_until:
        logger.info("Binance cooldown cleared.")

    binance_cooldown_until = 0


def safe_float(value, default=0.0):
    try:
        return float(value)
    except Exception:
        return default


def decimal_floor(value, step):
    value = Decimal(str(value))
    step = Decimal(str(step))

    if step <= 0:
        return value

    return (value / step).to_integral_value(
        rounding=ROUND_DOWN
    ) * step


def decimal_to_str(value):
    d = Decimal(str(value))

    text = format(d, "f")

    if "." in text:
        text = text.rstrip("0").rstrip(".")

    return text if text else "0"


# ============================================================
# BINANCE REQUEST
# ============================================================

def binance_request(
    method,
    path,
    params=None,
    signed=False,
    retry=True
):
    global server_time_offset_ms

    params = dict(params or {})

    if cooldown_active():
        raise RuntimeError(
            f"Binance cooldown active: {cooldown_remaining()} seconds"
        )

    if signed:
        params["timestamp"] = now_ms() + server_time_offset_ms
        params["recvWindow"] = 10000

        query = urlencode(params)

        signature = hmac.new(
            API_SECRET.encode(),
            query.encode(),
            hashlib.sha256
        ).hexdigest()

        params["signature"] = signature

    url = BASE_URL + path

    try:
        response = session.request(
            method=method,
            url=url,
            params=params,
            timeout=REQUEST_TIMEOUT
        )

    except requests.RequestException as e:
        logger.warning(
            "Binance network error %s: %s",
            path,
            e
        )

        raise

    # --------------------------------------------------------
    # 418 TEMPORARY IP BAN
    # --------------------------------------------------------

    if response.status_code == 418:

        activate_cooldown(
            COOLDOWN_418_SECONDS,
            "HTTP 418 temporary IP restriction"
        )

        logger.error(
            "BINANCE HTTP 418 - temporary IP restriction."
        )

        raise RuntimeError("BINANCE_418")

    # --------------------------------------------------------
    # 429 RATE LIMIT
    # --------------------------------------------------------

    if response.status_code == 429:

        retry_after = response.headers.get("Retry-After")

        try:
            retry_seconds = int(float(retry_after))
        except Exception:
            retry_seconds = COOLDOWN_429_SECONDS

        retry_seconds = max(
            retry_seconds,
            COOLDOWN_429_SECONDS
        )

        activate_cooldown(
            retry_seconds,
            "HTTP 429 rate limit"
        )

        logger.error(
            "BINANCE HTTP 429 - rate limit."
        )

        raise RuntimeError("BINANCE_429")

    # --------------------------------------------------------
    # OTHER ERRORS
    # --------------------------------------------------------

    if response.status_code >= 400:

        try:
            error_data = response.json()
        except Exception:
            error_data = response.text

        logger.error(
            "Binance HTTP %s %s: %s",
            response.status_code,
            path,
            error_data
        )

        response.raise_for_status()

    try:
        data = response.json()
    except Exception:
        data = response.text

    return data


# ============================================================
# SERVER TIME
# ============================================================

def sync_server_time():
    global server_time_offset_ms

    if cooldown_active():
        return False

    try:
        local_before = now_ms()

        data = binance_request(
            "GET",
            "/api/v3/time",
            signed=False
        )

        local_after = now_ms()

        server_time = int(data["serverTime"])

        local_mid = (
            local_before + local_after
        ) // 2

        server_time_offset_ms = (
            server_time - local_mid
        )

        logger.info(
            "Server time synced. Offset=%s ms",
            server_time_offset_ms
        )

        return True

    except Exception as e:

        logger.warning(
            "Server time sync failed: %s",
            e
        )

        logger.warning(
            "Using local clock temporarily."
        )

        return False


# ============================================================
# EXCHANGE INFO
# ============================================================

def load_exchange_info():

    if cooldown_active():
        return False

    try:

        data = binance_request(
            "GET",
            "/api/v3/exchangeInfo"
        )

        symbol_info.clear()

        for s in data.get("symbols", []):

            symbol = s.get("symbol")

            if not symbol:
                continue

            if s.get("status") != "TRADING":
                continue

            if s.get("quoteAsset") != "USDT":
                continue

            if not s.get("isSpotTradingAllowed", True):
                continue

            filters = {}

            for f in s.get("filters", []):
                filters[f["filterType"]] = f

            symbol_info[symbol] = {
                "baseAsset": s.get("baseAsset"),
                "quoteAsset": s.get("quoteAsset"),
                "filters": filters
            }

        logger.info(
            "Exchange info loaded. Spot USDT symbols=%s",
            len(symbol_info)
        )

        return True

    except Exception as e:

        logger.error(
            "Exchange info error: %s",
            e
        )

        return False


# ============================================================
# TOP SYMBOLS
# ============================================================

def get_top_symbols():

    if cooldown_active():
        return []

    try:

        data = binance_request(
            "GET",
            "/api/v3/ticker/24hr"
        )

        excluded = {
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
            "RUB",
            "UAH",
            "BIDR",
            "NGN",
            "ZAR",
            "PLN",
            "RON",
            "ARS",
            "COP",
            "MXN",
            "JPY",
            "BTC",
            "ETH",
        }

        candidates = []

        for item in data:

            symbol = item.get("symbol")

            if not symbol:
                continue

            if symbol not in symbol_info:
                continue

            if not symbol.endswith("USDT"):
                continue

            base = symbol[:-4]

            if base in excluded:
                continue

            quote_volume = safe_float(
                item.get("quoteVolume")
            )

            if quote_volume <= 0:
                continue

            candidates.append(
                (symbol, quote_volume)
            )

        candidates.sort(
            key=lambda x: x[1],
            reverse=True
        )

        result = [
            x[0]
            for x in candidates[:TOP_SYMBOLS]
        ]

        logger.info(
            "Selected %s symbols.",
            len(result)
        )

        logger.info(
            "First symbols: %s",
            result[:20]
        )

        return result

    except Exception as e:

        logger.error(
            "Top symbols error: %s",
            e
        )

        return []


# ============================================================
# KLINES
# ============================================================

def get_klines(symbol, limit=500):

    if cooldown_active():
        raise RuntimeError(
            "BINANCE_COOLDOWN"
        )

    return binance_request(
        "GET",
        "/api/v3/klines",
        params={
            "symbol": symbol,
            "interval": TIMEFRAME,
            "limit": limit
        }
    )


# ============================================================
# WILDER RSI
# ============================================================

def calculate_wilder_state(closes, period):

    if len(closes) < period + 1:
        return None

    gains = []
    losses = []

    for i in range(1, len(closes)):

        change = closes[i] - closes[i - 1]

        if change > 0:
            gains.append(change)
            losses.append(0.0)

        else:
            gains.append(0.0)
            losses.append(abs(change))

    if len(gains) < period:
        return None

    avg_gain = sum(
        gains[:period]
    ) / period

    avg_loss = sum(
        losses[:period]
    ) / period

    for i in range(period, len(gains)):

        avg_gain = (
            (avg_gain * (period - 1))
            + gains[i]
        ) / period

        avg_loss = (
            (avg_loss * (period - 1))
            + losses[i]
        ) / period

    return {
        "avg_gain": avg_gain,
        "avg_loss": avg_loss
    }


def rsi_from_state(state):

    if state is None:
        return None

    avg_gain = state["avg_gain"]
    avg_loss = state["avg_loss"]

    if avg_loss == 0:

        if avg_gain == 0:
            return 50.0

        return 100.0

    rs = avg_gain / avg_loss

    return 100.0 - (
        100.0 / (1.0 + rs)
    )


def update_wilder_state(
    state,
    previous_close,
    current_close,
    period
):

    if state is None:
        return None

    change = current_close - previous_close

    gain = max(change, 0.0)
    loss = max(-change, 0.0)

    avg_gain = (
        (state["avg_gain"] * (period - 1))
        + gain
    ) / period

    avg_loss = (
        (state["avg_loss"] * (period - 1))
        + loss
    ) / period

    return {
        "avg_gain": avg_gain,
        "avg_loss": avg_loss
    }


# ============================================================
# INITIAL RSI STATE
# ============================================================

def initialize_rsi_for_symbol(symbol):

    try:

        klines = get_klines(
            symbol,
            HISTORY_LIMIT
        )

        if not klines:
            return False

        closes = [
            float(k[4])
            for k in klines
        ]

        if len(closes) < RSI_SLOW_PERIOD + 2:
            logger.warning(
                "%s | Not enough candles",
                symbol
            )
            return False

        # All candles returned by Binance include the current
        # still-open candle at the end.
        # Remove it.
        closes = closes[:-1]

        slow_state = calculate_wilder_state(
            closes,
            RSI_SLOW_PERIOD
        )

        fast_state = calculate_wilder_state(
            closes,
            RSI_FAST_PERIOD
        )

        if slow_state is None or fast_state is None:
            return False

        rsi_states[symbol] = {
            "last_close": closes[-1],
            "rsi50_state": slow_state,
            "rsi3_state": fast_state,
            "rsi50": rsi_from_state(slow_state),
            "rsi3": rsi_from_state(fast_state),
            "previous_rsi3": None,
        }

        logger.info(
            "%s | RSI50=%.2f RSI3=%.2f",
            symbol,
            rsi_states[symbol]["rsi50"],
            rsi_states[symbol]["rsi3"]
        )

        return True

    except Exception as e:

        logger.error(
            "%s | RSI initialization error: %s",
            symbol,
            e
        )

        return False


# ============================================================
# UPDATE RSI FROM CLOSED CANDLE
# ============================================================

def update_symbol_rsi(symbol, close_price):

    state = rsi_states.get(symbol)

    if state is None:
        return None, None, None

    previous_close = state["last_close"]

    if close_price == previous_close:
        return (
            state["rsi50"],
            state["rsi3"],
            state["previous_rsi3"]
        )

    previous_rsi3 = state["rsi3"]

    state["rsi50_state"] = update_wilder_state(
        state["rsi50_state"],
        previous_close,
        close_price,
        RSI_SLOW_PERIOD
    )

    state["rsi3_state"] = update_wilder_state(
        state["rsi3_state"],
        previous_close,
        close_price,
        RSI_FAST_PERIOD
    )

    state["last_close"] = close_price

    state["previous_rsi3"] = previous_rsi3

    state["rsi50"] = rsi_from_state(
        state["rsi50_state"]
    )

    state["rsi3"] = rsi_from_state(
        state["rsi3_state"]
    )

    return (
        state["rsi50"],
        state["rsi3"],
        previous_rsi3
    )


# ============================================================
# QUANTITY
# ============================================================

def normalize_quantity(symbol, quantity):

    info = symbol_info.get(symbol)

    if not info:
        return 0.0

    filters = info["filters"]

    lot_filter = (
        filters.get("MARKET_LOT_SIZE")
        or filters.get("LOT_SIZE")
    )

    if not lot_filter:
        return 0.0

    step_size = safe_float(
        lot_filter.get("stepSize"),
        0
    )

    min_qty = safe_float(
        lot_filter.get("minQty"),
        0
    )

    if step_size <= 0:
        return 0.0

    qty = decimal_floor(
        quantity,
        step_size
    )

    qty_float = float(qty)

    if qty_float < min_qty:
        return 0.0

    return qty_float


def get_buy_quantity(symbol, usdt_amount, price):

    if price <= 0:
        return 0.0

    raw_qty = usdt_amount / price

    return normalize_quantity(
        symbol,
        raw_qty
    )


# ============================================================
# MIN NOTIONAL CHECK
# ============================================================

def check_notional(symbol, quantity, price):

    info = symbol_info.get(symbol)

    if not info:
        return False

    filters = info["filters"]

    notional_filter = (
        filters.get("NOTIONAL")
        or filters.get("MIN_NOTIONAL")
    )

    if notional_filter is None:
        return True

    min_notional = safe_float(
        notional_filter.get("minNotional"),
        0
    )

    return (
        quantity * price
    ) >= min_notional


# ============================================================
# ORDER ID
# ============================================================

def make_client_order_id(prefix, symbol):

    timestamp = int(time.time() * 1000)

    symbol_short = symbol[:10]

    return (
        f"{prefix}{symbol_short}_{timestamp}"
    )[:36]


# ============================================================
# BUY ORDER
# ============================================================

def place_buy(symbol, price):

    if cooldown_active():
        logger.warning(
            "%s | BUY skipped - Binance cooldown",
            symbol
        )
        return None

    current_time = time.time()

    last_time = last_buy_time.get(
        symbol,
        0
    )

    if (
        current_time - last_time
        < BUY_COOLDOWN_SECONDS
    ):
        return None

    quantity = get_buy_quantity(
        symbol,
        BUY_USDT,
        price
    )

    if quantity <= 0:
        logger.warning(
            "%s | BUY quantity invalid",
            symbol
        )
        return None

    if not check_notional(
        symbol,
        quantity,
        price
    ):
        logger.warning(
            "%s | BUY notional below minimum",
            symbol
        )
        return None

    client_id = make_client_order_id(
        BUY_CLIENT_PREFIX,
        symbol
    )

    params = {
        "symbol": symbol,
        "side": "BUY",
        "type": "MARKET",
        "quantity": decimal_to_str(quantity),
        "newClientOrderId": client_id,
    }

    with order_lock:

        if cooldown_active():
            return None

        try:

            logger.info(
                "%s | BUY attempt | qty=%s",
                symbol,
                decimal_to_str(quantity)
            )

            data = binance_request(
                "POST",
                "/api/v3/order",
                params=params,
                signed=True
            )

            executed_qty = safe_float(
                data.get("executedQty")
            )

            fills = data.get("fills", [])

            fill_price = price

            if fills:

                total_qty = 0.0
                total_value = 0.0

                for fill in fills:

                    fq = safe_float(
                        fill.get("qty")
                    )

                    fp = safe_float(
                        fill.get("price")
                    )

                    total_qty += fq
                    total_value += (
                        fq * fp
                    )

                if total_qty > 0:
                    fill_price = (
                        total_value
                        / total_qty
                    )

            if executed_qty <= 0:
                executed_qty = quantity

            last_buy_time[symbol] = (
                time.time()
            )

            positions[symbol] = {
                "qty": executed_qty,
                "entry_price": fill_price,
                "buy_order_id": data.get("orderId"),
                "buy_client_id": client_id,
                "time": time.time()
            }

            logger.info(
                "%s | BUY SUCCESS | qty=%.12f | price=%.12f",
                symbol,
                executed_qty,
                fill_price
            )

            # Place SL using executedQty.
            # This avoids an immediate /account request.
            place_stop_loss(
                symbol,
                executed_qty,
                fill_price
            )

            return data

        except Exception as e:

            logger.error(
                "%s | BUY ERROR: %s",
                symbol,
                e
            )

            return None


# ============================================================
# STOP LOSS
# ============================================================

def place_stop_loss(
    symbol,
    executed_qty,
    entry_price
):

    if executed_qty <= 0:
        return False

    if cooldown_active():
        logger.warning(
            "%s | SL skipped because Binance cooldown active",
            symbol
        )
        return False

    sl_price = (
        entry_price
        * (1.0 - STOP_LOSS_PERCENT)
    )

    sl_qty = (
        executed_qty
        * SELL_BALANCE_BUFFER
    )

    sl_qty = normalize_quantity(
        symbol,
        sl_qty
    )

    if sl_qty <= 0:
        logger.warning(
            "%s | SL quantity invalid",
            symbol
        )
        return False

    client_id = make_client_order_id(
        SL_CLIENT_PREFIX,
        symbol
    )

    params = {
        "symbol": symbol,
        "side": "SELL",
        "type": "STOP_LOSS_LIMIT",
        "timeInForce": "GTC",
        "quantity": decimal_to_str(sl_qty),
        "price": decimal_to_str(sl_price),
        "stopPrice": decimal_to_str(sl_price),
        "newClientOrderId": client_id
    }

    with order_lock:

        try:

            data = binance_request(
                "POST",
                "/api/v3/order",
                params=params,
                signed=True
            )

            logger.info(
                "%s | STOP LOSS placed | price=%.12f | qty=%.12f",
                symbol,
                sl_price,
                sl_qty
            )

            if symbol in positions:
                positions[symbol]["sl_order_id"] = (
                    data.get("orderId")
                )

            return True

        except Exception as e:

            logger.error(
                "%s | STOP LOSS ERROR: %s",
                symbol,
                e
            )

            return False


# ============================================================
# ACCOUNT
# ============================================================

def get_account():

    if cooldown_active():
        return None

    try:

        return binance_request(
            "GET",
            "/api/v3/account",
            signed=True
        )

    except Exception as e:

        logger.error(
            "Account request error: %s",
            e
        )

        return None


# ============================================================
# RECOVERY
# ============================================================

def recover_positions():

    logger.info(
        "Checking existing bot positions..."
    )

    account = get_account()

    if not account:
        logger.warning(
            "Position recovery skipped."
        )
        return

    balances = account.get(
        "balances",
        []
    )

    candidates = []

    for balance in balances:

        free = safe_float(
            balance.get("free")
        )

        if free <= 0:
            continue

        asset = balance.get("asset")

        if not asset:
            continue

        symbol = asset + "USDT"

        if symbol in symbols:
            candidates.append(
                (symbol, free)
            )

    logger.info(
        "Recovery candidates: %s",
        len(candidates)
    )

    for symbol, balance_qty in candidates:

        if cooldown_active():
            break

        try:

            orders = binance_request(
                "GET",
                "/api/v3/allOrders",
                params={
                    "symbol": symbol,
                    "limit": 100
                },
                signed=True
            )

            bot_buys = []

            for order in orders:

                if order.get("side") != "BUY":
                    continue

                if order.get("status") != "FILLED":
                    continue

                client_id = (
                    order.get("clientOrderId")
                    or ""
                )

                if not client_id.startswith(
                    BUY_CLIENT_PREFIX
                ):
                    continue

                bot_buys.append(order)

            if not bot_buys:
                continue

            bot_buys.sort(
                key=lambda x: x.get(
                    "time",
                    0
                ),
                reverse=True
            )

            order = bot_buys[0]

            executed_qty = safe_float(
                order.get("executedQty")
            )

            quote_qty = safe_float(
                order.get("cummulativeQuoteQty")
            )

            if executed_qty <= 0:
                continue

            entry_price = (
                quote_qty / executed_qty
                if quote_qty > 0
                else 0
            )

            if entry_price <= 0:
                continue

            recovered_qty = min(
                executed_qty,
                balance_qty
            )

            positions[symbol] = {
                "qty": recovered_qty,
                "entry_price": entry_price,
                "buy_order_id": order.get(
                    "orderId"
                ),
                "buy_client_id": order.get(
                    "clientOrderId"
                ),
                "recovered": True,
                "time": time.time()
            }

            logger.info(
                "%s | POSITION RECOVERED | qty=%.12f | entry=%.12f",
                symbol,
                recovered_qty,
                entry_price
            )

            # Do not create duplicate SL if an existing
            # open bot SL is already there.

            open_orders = binance_request(
                "GET",
                "/api/v3/openOrders",
                params={
                    "symbol": symbol
                },
                signed=True
            )

            has_bot_sl = False

            for oo in open_orders:

                cid = (
                    oo.get("clientOrderId")
                    or ""
                )

                if cid.startswith(
                    SL_CLIENT_PREFIX
                ):
                    has_bot_sl = True
                    break

            if not has_bot_sl:

                place_stop_loss(
                    symbol,
                    recovered_qty,
                    entry_price
                )

        except Exception as e:

            logger.error(
                "%s | Recovery error: %s",
                symbol,
                e
            )

        time.sleep(0.5)


# ============================================================
# SELL
# ============================================================

def place_sell(symbol, reason="SIGNAL"):

    if cooldown_active():
        logger.warning(
            "%s | SELL skipped - Binance cooldown",
            symbol
        )
        return None

    position = positions.get(symbol)

    if not position:
        return None

    qty = safe_float(
        position.get("qty")
    )

    if qty <= 0:
        return None

    qty = qty * SELL_BALANCE_BUFFER

    qty = normalize_quantity(
        symbol,
        qty
    )

    if qty <= 0:
        logger.warning(
            "%s | SELL quantity invalid",
            symbol
        )
        return None

    params = {
        "symbol": symbol,
        "side": "SELL",
        "type": "MARKET",
        "quantity": decimal_to_str(qty)
    }

    with order_lock:

        try:

            logger.info(
                "%s | SELL attempt | reason=%s | qty=%s",
                symbol,
                reason,
                decimal_to_str(qty)
            )

            data = binance_request(
                "POST",
                "/api/v3/order",
                params=params,
                signed=True
            )

            logger.info(
                "%s | SELL SUCCESS | reason=%s",
                symbol,
                reason
            )

            positions.pop(
                symbol,
                None
            )

            return data

        except Exception as e:

            logger.error(
                "%s | SELL ERROR: %s",
                symbol,
                e
            )

            return None


# ============================================================
# CANDLE PROCESSING
# ============================================================

def process_closed_candle(
    symbol,
    close_price
):

    if symbol not in rsi_states:
        return

    result = update_symbol_rsi(
        symbol,
        close_price
    )

    rsi50, rsi3, previous_rsi3 = result

    if rsi50 is None or rsi3 is None:
        return

    logger.info(
        "%s | Close=%.8f | RSI50=%.2f | RSI3=%.2f",
        symbol,
        close_price,
        rsi50,
        rsi3
    )

    # ========================================================
    # SELL
    # RSI3 crosses from below/equal 80 to above 80
    # ========================================================

    position = positions.get(symbol)

    if position:

        if (
            previous_rsi3 is not None
            and previous_rsi3 <= SELL_RSI_LEVEL
            and rsi3 > SELL_RSI_LEVEL
        ):

            place_sell(
                symbol,
                reason="RSI3_CROSS_ABOVE_80"
            )

            return

    # ========================================================
    # BUY
    #
    # RSI50 > 50
    # RSI3 < 10
    # ========================================================

    if symbol in positions:
        return

    if rsi50 > BUY_RSI_SLOW_MIN:

        if rsi3 < BUY_RSI_FAST_MAX:

            place_buy(
                symbol,
                close_price
            )


# ============================================================
# WEBSOCKET
# ============================================================

def websocket_group_worker(
    group_symbols,
    group_number
):

    streams = []

    for symbol in group_symbols:

        streams.append(
            f"{symbol.lower()}@kline_{TIMEFRAME}"
        )

    if not streams:
        return

    stream_url = (
        WS_BASE_URL
        + "?streams="
        + "/".join(streams)
    )

    while True:

        try:

            logger.info(
                "WebSocket group %s connecting. Symbols=%s",
                group_number,
                len(group_symbols)
            )

            def on_message(ws, message):

                try:

                    payload = json.loads(
                        message
                    )

                    data = payload.get(
                        "data",
                        {}
                    )

                    kline = data.get(
                        "k",
                        {}
                    )

                    if not kline:
                        return

                    # Only closed candles
                    if not kline.get("x"):
                        return

                    symbol = kline.get(
                        "s"
                    )

                    close_price = safe_float(
                        kline.get("c")
                    )

                    if not symbol:
                        return

                    if close_price <= 0:
                        return

                    process_closed_candle(
                        symbol,
                        close_price
                    )

                except Exception as e:

                    logger.error(
                        "WS message processing error: %s",
                        e
                    )

            def on_error(ws, error):

                logger.warning(
                    "WS group %s error: %s",
                    group_number,
                    error
                )

            def on_close(
                ws,
                close_status_code,
                close_msg
            ):

                logger.warning(
                    "WS group %s closed. code=%s msg=%s",
                    group_number,
                    close_status_code,
                    close_msg
                )

            def on_open(ws):

                logger.info(
                    "WS group %s connected.",
                    group_number
                )

            ws = websocket.WebSocketApp(
                stream_url,
                on_open=on_open,
                on_message=on_message,
                on_error=on_error,
                on_close=on_close
            )

            ws.run_forever(
                ping_interval=WS_PING_INTERVAL,
                ping_timeout=WS_PING_TIMEOUT
            )

        except Exception as e:

            logger.error(
                "WS group %s exception: %s",
                group_number,
                e
            )

        logger.info(
            "WS group %s reconnecting after %s sec...",
            group_number,
            RECONNECT_DELAY
        )

        time.sleep(
            RECONNECT_DELAY
        )


# ============================================================
# START WEBSOCKETS
# ============================================================

def start_websockets():

    global websocket_threads

    if websocket_threads:
        return

    groups = []

    for i in range(
        0,
        len(symbols),
        SYMBOLS_PER_GROUP
    ):

        groups.append(
            symbols[
                i:i + SYMBOLS_PER_GROUP
            ]
        )

    logger.info(
        "Starting %s WebSocket groups.",
        len(groups)
    )

    for index, group in enumerate(
        groups,
        start=1
    ):

        thread = threading.Thread(
            target=websocket_group_worker,
            args=(group, index),
            daemon=True
        )

        thread.start()

        websocket_threads.append(
            thread
        )

        time.sleep(1)


# ============================================================
# INITIALIZATION
# ============================================================

def initialize_bot():

    global bot_ready
    global bot_initializing
    global bot_status
    global symbols

    if not initialization_lock.acquire(
        blocking=False
    ):
        return

    try:

        bot_initializing = True
        bot_status = "starting"

        logger.info("=" * 70)
        logger.info(
            "STARTING %s",
            BOT_NAME
        )
        logger.info("=" * 70)

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
            "BUY amount: %.2f USDT",
            BUY_USDT
        )

        logger.info(
            "BUY: RSI50 > %.0f AND RSI3 < %.0f",
            BUY_RSI_SLOW_MIN,
            BUY_RSI_FAST_MAX
        )

        logger.info(
            "SELL: RSI3 cross above %.0f",
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
        # Initialization retry loop
        # ----------------------------------------------------

        while not bot_ready:

            try:

                if cooldown_active():

                    remaining = cooldown_remaining()

                    bot_status = (
                        f"binance_cooldown_{remaining}s"
                    )

                    logger.warning(
                        "Binance cooldown active. "
                        "Initialization waiting: %s seconds",
                        remaining
                    )

                    time.sleep(
                        min(
                            max(remaining, 1),
                            60
                        )
                    )

                    continue

                # ------------------------------------------------
                # Server time
                # ------------------------------------------------

                sync_server_time()

                # ------------------------------------------------
                # Exchange info
                # ------------------------------------------------

                if not load_exchange_info():

                    logger.warning(
                        "Exchange info unavailable. "
                        "Retrying in 60 seconds."
                    )

                    time.sleep(60)
                    continue

                # ------------------------------------------------
                # Top symbols
                # ------------------------------------------------

                selected = get_top_symbols()

                if not selected:

                    logger.warning(
                        "No symbols available. "
                        "Retrying in 60 seconds."
                    )

                    time.sleep(60)
                    continue

                symbols = selected

                # ------------------------------------------------
                # Historical RSI initialization
                # ------------------------------------------------

                success_count = 0

                logger.info(
                    "Loading RSI history for %s symbols...",
                    len(symbols)
                )

                for index, symbol in enumerate(
                    symbols,
                    start=1
                ):

                    if cooldown_active():

                        logger.warning(
                            "Binance cooldown occurred "
                            "during RSI initialization."
                        )

                        break

                    ok = initialize_rsi_for_symbol(
                        symbol
                    )

                    if ok:
                        success_count += 1

                    if index < len(symbols):

                        time.sleep(
                            KLINE_DELAY_SECONDS
                        )

                # If cooldown occurred, start whole
                # initialization again later.

                if cooldown_active():

                    logger.warning(
                        "RSI initialization interrupted "
                        "by Binance cooldown."
                    )

                    time.sleep(
                        min(
                            cooldown_remaining(),
                            60
                        )
                    )

                    continue

                if success_count == 0:

                    logger.warning(
                        "No RSI states initialized."
                    )

                    time.sleep(60)
                    continue

                logger.info(
                    "RSI initialization complete: "
                    "%s/%s symbols",
                    success_count,
                    len(symbols)
                )

                # ------------------------------------------------
                # Recover positions
                # ------------------------------------------------

                recover_positions()

                # ------------------------------------------------
                # Start WebSockets
                # ------------------------------------------------

                start_websockets()

                bot_ready = True
                bot_initializing = False
                bot_status = "ready"

                logger.info("=" * 70)
                logger.info(
                    "BOT READY"
                )
                logger.info("=" * 70)

            except Exception as e:

                logger.error(
                    "BOT INITIALIZATION ERROR: %s",
                    e
                )

                bot_status = "retrying"

                # Never crash Render worker.
                # Wait before trying again.

                if cooldown_active():

                    wait_time = min(
                        cooldown_remaining(),
                        60
                    )

                else:

                    wait_time = 60

                logger.info(
                    "Initialization retry in %s seconds.",
                    wait_time
                )

                time.sleep(
                    wait_time
                )

    finally:

        bot_initializing = False

        try:
            initialization_lock.release()
        except Exception:
            pass


# ============================================================
# START INITIALIZATION ON FIRST REQUEST
# ============================================================

def ensure_bot_thread():

    global initialization_thread_started

    if initialization_thread_started:
        return

    with state_lock:

        if initialization_thread_started:
            return

        initialization_thread_started = True

        thread = threading.Thread(
            target=initialize_bot,
            daemon=True
        )

        thread.start()


# ============================================================
# FLASK ROUTES
# ============================================================

@app.route("/", methods=["GET", "HEAD"])
def home():

    ensure_bot_thread()

    return jsonify({
        "bot": BOT_NAME,
        "status": bot_status,
        "ready": bot_ready,
        "symbols": len(symbols),
        "positions": len(positions),
        "cooldown_seconds": cooldown_remaining()
    })


@app.route("/health", methods=["GET", "HEAD"])
def health():

    ensure_bot_thread()

    # IMPORTANT:
    # Always return HTTP 200 during Binance 418.
    # This prevents Render health checks from restarting
    # the service while Binance IP cooldown is active.

    return jsonify({
        "status": bot_status,
        "ready": bot_ready,
        "binance_cooldown": cooldown_active(),
        "cooldown_seconds": cooldown_remaining(),
        "symbols": len(symbols),
        "rsi_states": len(rsi_states),
        "positions": len(positions),
        "websocket_groups": len(websocket_threads)
    }), 200


@app.route("/status", methods=["GET"])
def status():

    ensure_bot_thread()

    return jsonify({
        "bot": BOT_NAME,
        "status": bot_status,
        "ready": bot_ready,
        "timeframe": TIMEFRAME,
        "top_symbols": TOP_SYMBOLS,
        "loaded_symbols": len(symbols),
        "rsi_states": len(rsi_states),
        "positions": positions,
        "cooldown_active": cooldown_active(),
        "cooldown_seconds": cooldown_remaining(),
        "websocket_groups": len(websocket_threads)
    })


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
                10000
            )
        )
    )
