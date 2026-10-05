```python
import os
import time
import json
import hmac
import hashlib
import threading
import logging
from decimal import Decimal, ROUND_DOWN, InvalidOperation
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
    return max(
        0,
        int(binance_cooldown_until - time.time())
    )


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


def to_decimal(value, default=Decimal("0")):

    try:
        return Decimal(str(value))

    except (InvalidOperation, ValueError, TypeError):
        return default


def decimal_floor(value, step):

    value = to_decimal(value)
    step = to_decimal(step)

    if step <= 0:
        return Decimal("0")

    return (
        value / step
    ).to_integral_value(
        rounding=ROUND_DOWN
    ) * step


def decimal_to_str(value):

    d = to_decimal(value)

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
            f"Binance cooldown active: "
            f"{cooldown_remaining()} seconds"
        )

    if signed:

        params["timestamp"] = (
            now_ms()
            + server_time_offset_ms
        )

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

    # ========================================================
    # HTTP 418
    # ========================================================

    if response.status_code == 418:

        activate_cooldown(
            COOLDOWN_418_SECONDS,
            "HTTP 418 temporary IP restriction"
        )

        logger.error(
            "BINANCE HTTP 418 - temporary IP restriction."
        )

        raise RuntimeError("BINANCE_418")

    # ========================================================
    # HTTP 429
    # ========================================================

    if response.status_code == 429:

        retry_after = response.headers.get(
            "Retry-After"
        )

        try:
            retry_seconds = int(
                float(retry_after)
            )

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

    # ========================================================
    # OTHER HTTP ERRORS
    # ========================================================

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

        return response.json()

    except Exception:

        return response.text


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

        server_time = int(
            data["serverTime"]
        )

        local_mid = (
            local_before
            + local_after
        ) // 2

        server_time_offset_ms = (
            server_time
            - local_mid
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

        for s in data.get(
            "symbols",
            []
        ):

            symbol = s.get("symbol")

            if not symbol:
                continue

            if s.get("status") != "TRADING":
                continue

            if s.get("quoteAsset") != "USDT":
                continue

            if not s.get(
                "isSpotTradingAllowed",
                True
            ):
                continue

            filters = {}

            for f in s.get(
                "filters",
                []
            ):

                filters[
                    f["filterType"]
                ] = f

            symbol_info[symbol] = {
                "baseAsset": s.get(
                    "baseAsset"
                ),
                "quoteAsset": s.get(
                    "quoteAsset"
                ),
                "filters": filters
            }

        logger.info(
            "Exchange info loaded. "
            "Spot USDT symbols=%s",
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
                (
                    symbol,
                    quote_volume
                )
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

def get_klines(
    symbol,
    limit=500
):

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

def calculate_wilder_state(
    closes,
    period
):

    if len(closes) < period + 1:
        return None

    gains = []
    losses = []

    for i in range(
        1,
        len(closes)
    ):

        change = (
            closes[i]
            - closes[i - 1]
        )

        if change > 0:

            gains.append(change)
            losses.append(0.0)

        else:

            gains.append(0.0)
            losses.append(abs(change))

    if len(gains) < period:
        return None

    avg_gain = (
        sum(gains[:period])
        / period
    )

    avg_loss = (
        sum(losses[:period])
        / period
    )

    for i in range(
        period,
        len(gains)
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

    rs = (
        avg_gain
        / avg_loss
    )

    return 100.0 - (
        100.0
        / (1.0 + rs)
    )


def update_wilder_state(
    state,
    previous_close,
    current_close,
    period
):

    if state is None:
        return None

    change = (
        current_close
        - previous_close
    )

    gain = max(
        change,
        0.0
    )

    loss = max(
        -change,
        0.0
    )

    avg_gain = (
        (
            state["avg_gain"]
            * (period - 1)
        )
        + gain
    ) / period

    avg_loss = (
        (
            state["avg_loss"]
            * (period - 1)
        )
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

        if len(closes) < (
            RSI_SLOW_PERIOD + 2
        ):

            logger.warning(
                "%s | Not enough candles",
                symbol
            )

            return False

        # Remove currently open candle.
        closes = closes[:-1]

        slow_state = (
            calculate_wilder_state(
                closes,
                RSI_SLOW_PERIOD
            )
        )

        fast_state = (
            calculate_wilder_state(
                closes,
                RSI_FAST_PERIOD
            )
        )

        if (
            slow_state is None
            or fast_state is None
        ):
            return False

        rsi_states[symbol] = {
            "last_close": closes[-1],

            "rsi50_state": slow_state,
            "rsi3_state": fast_state,

            "rsi50": rsi_from_state(
                slow_state
            ),

            "rsi3": rsi_from_state(
                fast_state
            ),

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

def update_symbol_rsi(
    symbol,
    close_price
):

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

    state["rsi50_state"] = (
        update_wilder_state(
            state["rsi50_state"],
            previous_close,
            close_price,
            RSI_SLOW_PERIOD
        )
    )

    state["rsi3_state"] = (
        update_wilder_state(
            state["rsi3_state"],
            previous_close,
            close_price,
            RSI_FAST_PERIOD
        )
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
# QUANTITY FILTER SELECTION
# ============================================================

def get_quantity_filter(symbol):

    info = symbol_info.get(symbol)

    if not info:
        return None

    filters = info.get(
        "filters",
        {}
    )

    market_filter = filters.get(
        "MARKET_LOT_SIZE"
    )

    lot_filter = filters.get(
        "LOT_SIZE"
    )

    # --------------------------------------------------------
    # Prefer MARKET_LOT_SIZE only when it has a valid stepSize.
    # Some symbols may expose a MARKET_LOT_SIZE with an unusable
    # stepSize. In that case fall back to LOT_SIZE.
    # --------------------------------------------------------

    if market_filter:

        market_step = to_decimal(
            market_filter.get(
                "stepSize"
            )
        )

        if market_step > 0:

            return market_filter

    if lot_filter:

        lot_step = to_decimal(
            lot_filter.get(
                "stepSize"
            )
        )

        if lot_step > 0:

            return lot_filter

    return None


# ============================================================
# QUANTITY NORMALIZATION
# ============================================================

def normalize_quantity_decimal(
    symbol,
    quantity,
    log_reason=False
):

    info = symbol_info.get(symbol)

    if not info:

        if log_reason:
            logger.warning(
                "%s | Quantity failed: "
                "symbol info unavailable",
                symbol
            )

        return Decimal("0")

    quantity = to_decimal(
        quantity
    )

    if quantity <= 0:

        if log_reason:
            logger.warning(
                "%s | Quantity failed: "
                "raw quantity <= 0",
                symbol
            )

        return Decimal("0")

    quantity_filter = (
        get_quantity_filter(symbol)
    )

    if not quantity_filter:

        if log_reason:
            logger.warning(
                "%s | Quantity failed: "
                "no valid LOT_SIZE/MARKET_LOT_SIZE",
                symbol
            )

        return Decimal("0")

    step_size = to_decimal(
        quantity_filter.get(
            "stepSize"
        )
    )

    min_qty = to_decimal(
        quantity_filter.get(
            "minQty"
        )
    )

    max_qty = to_decimal(
        quantity_filter.get(
            "maxQty"
        )
    )

    if step_size <= 0:

        if log_reason:
            logger.warning(
                "%s | Quantity failed: "
                "invalid stepSize=%s",
                symbol,
                quantity_filter.get(
                    "stepSize"
                )
            )

        return Decimal("0")

    # --------------------------------------------------------
    # Floor to Binance step size
    # --------------------------------------------------------

    qty = decimal_floor(
        quantity,
        step_size
    )

    if qty <= 0:

        if log_reason:
            logger.warning(
                "%s | Quantity failed after "
                "step rounding | raw=%s step=%s",
                symbol,
                decimal_to_str(quantity),
                decimal_to_str(step_size)
            )

        return Decimal("0")

    # --------------------------------------------------------
    # Minimum quantity
    # --------------------------------------------------------

    if min_qty > 0 and qty < min_qty:

        if log_reason:
            logger.warning(
                "%s | Quantity below minQty | "
                "qty=%s minQty=%s step=%s",
                symbol,
                decimal_to_str(qty),
                decimal_to_str(min_qty),
                decimal_to_str(step_size)
            )

        return Decimal("0")

    # --------------------------------------------------------
    # Maximum quantity
    # --------------------------------------------------------

    if max_qty > 0 and qty > max_qty:

        qty = decimal_floor(
            max_qty,
            step_size
        )

        if qty < min_qty:

            if log_reason:
                logger.warning(
                    "%s | maxQty produces quantity "
                    "below minQty",
                    symbol
                )

            return Decimal("0")

    return qty


def normalize_quantity(
    symbol,
    quantity,
    log_reason=False
):

    qty = normalize_quantity_decimal(
        symbol,
        quantity,
        log_reason=log_reason
    )

    if qty <= 0:
        return 0.0

    return float(qty)


# ============================================================
# MIN NOTIONAL
# ============================================================

def get_min_notional(
    symbol,
    for_market_order=True
):

    info = symbol_info.get(symbol)

    if not info:
        return Decimal("0")

    filters = info.get(
        "filters",
        {}
    )

    # Prefer NOTIONAL if available.
    notional_filter = filters.get(
        "NOTIONAL"
    )

    if notional_filter:

        if for_market_order:

            apply_min = notional_filter.get(
                "applyMinToMarket"
            )

            if apply_min is False:
                return Decimal("0")

        return to_decimal(
            notional_filter.get(
                "minNotional"
            )
        )

    # Fallback to MIN_NOTIONAL.
    min_notional_filter = filters.get(
        "MIN_NOTIONAL"
    )

    if min_notional_filter:

        if for_market_order:

            apply_min = min_notional_filter.get(
                "applyToMarket"
            )

            if apply_min is False:
                return Decimal("0")

        return to_decimal(
            min_notional_filter.get(
                "minNotional"
            )
        )

    return Decimal("0")


def check_notional_decimal(
    symbol,
    quantity,
    price,
    for_market_order=True
):

    quantity = to_decimal(quantity)
    price = to_decimal(price)

    if quantity <= 0 or price <= 0:
        return False

    min_notional = get_min_notional(
        symbol,
        for_market_order=for_market_order
    )

    if min_notional <= 0:
        return True

    actual_notional = (
        quantity * price
    )

    return actual_notional >= min_notional


def check_notional(
    symbol,
    quantity,
    price
):

    return check_notional_decimal(
        symbol,
        quantity,
        price,
        for_market_order=True
    )


# ============================================================
# BUY QUANTITY
# ============================================================

def get_buy_quantity(
    symbol,
    usdt_amount,
    price
):

    price_d = to_decimal(price)
    usdt_d = to_decimal(usdt_amount)

    if price_d <= 0:
        return Decimal("0")

    if usdt_d <= 0:
        return Decimal("0")

    # --------------------------------------------------------
    # Calculate raw quantity.
    # --------------------------------------------------------

    raw_qty = (
        usdt_d
        / price_d
    )

    # --------------------------------------------------------
    # Normalize using valid market/lot filter.
    # --------------------------------------------------------

    qty = normalize_quantity_decimal(
        symbol,
        raw_qty,
        log_reason=True
    )

    if qty <= 0:
        return Decimal("0")

    # --------------------------------------------------------
    # Check market minimum notional.
    # --------------------------------------------------------

    min_notional = get_min_notional(
        symbol,
        for_market_order=True
    )

    actual_notional = (
        qty
        * price_d
    )

    if (
        min_notional > 0
        and actual_notional < min_notional
    ):

        logger.warning(
            "%s | BUY notional too low | "
            "qty=%s price=%s notional=%s minNotional=%s",
            symbol,
            decimal_to_str(qty),
            decimal_to_str(price_d),
            decimal_to_str(actual_notional),
            decimal_to_str(min_notional)
        )

        return Decimal("0")

    return qty


# ============================================================
# ORDER ID
# ============================================================

def make_client_order_id(
    prefix,
    symbol
):

    timestamp = int(
        time.time() * 1000
    )

    symbol_short = symbol[:10]

    return (
        f"{prefix}"
        f"{symbol_short}_"
        f"{timestamp}"
    )[:36]


# ============================================================
# BUY ORDER
# ============================================================

def place_buy(
    symbol,
    price
):

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
        current_time
        - last_time
        < BUY_COOLDOWN_SECONDS
    ):
        return None

    # --------------------------------------------------------
    # Calculate robust quantity
    # --------------------------------------------------------

    quantity = get_buy_quantity(
        symbol,
        BUY_USDT,
        price
    )

    if quantity <= 0:

        logger.warning(
            "%s | BUY quantity invalid | "
            "BUY_USDT=%s price=%s",
            symbol,
            BUY_USDT,
            price
        )

        return None

    # --------------------------------------------------------
    # Final notional verification
    # --------------------------------------------------------

    if not check_notional_decimal(
        symbol,
        quantity,
        price,
        for_market_order=True
    ):

        logger.warning(
            "%s | BUY notional below minimum | "
            "qty=%s price=%s",
            symbol,
            decimal_to_str(quantity),
            decimal_to_str(price)
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
        "quantity": decimal_to_str(
            quantity
        ),
        "newClientOrderId": client_id,
    }

    with order_lock:

        if cooldown_active():
            return None

        try:

            logger.info(
                "%s | BUY attempt | "
                "USDT=%.2f | price=%s | qty=%s",
                symbol,
                BUY_USDT,
                decimal_to_str(
                    to_decimal(price)
                ),
                decimal_to_str(
                    quantity
                )
            )

            data = binance_request(
                "POST",
                "/api/v3/order",
                params=params,
                signed=True
            )

            executed_qty = to_decimal(
                data.get(
                    "executedQty"
                )
            )

            fills = data.get(
                "fills",
                []
            )

            fill_price = to_decimal(
                price
            )

            if fills:

                total_qty = Decimal("0")
                total_value = Decimal("0")

                for fill in fills:

                    fq = to_decimal(
                        fill.get("qty")
                    )

                    fp = to_decimal(
                        fill.get("price")
                    )

                    if fq <= 0 or fp <= 0:
                        continue

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
                "qty": float(
                    executed_qty
                ),

                "entry_price": float(
                    fill_price
                ),

                "buy_order_id": data.get(
                    "orderId"
                ),

                "buy_client_id": client_id,

                "time": time.time()
            }

            logger.info(
                "%s | BUY SUCCESS | "
                "qty=%.12f | price=%.12f",
                symbol,
                float(executed_qty),
                float(fill_price)
            )

            # ------------------------------------------------
            # Place server-side SL
            # ------------------------------------------------

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

    executed_qty = to_decimal(
        executed_qty
    )

    entry_price = to_decimal(
        entry_price
    )

    if executed_qty <= 0:
        return False

    if entry_price <= 0:
        return False

    if cooldown_active():

        logger.warning(
            "%s | SL skipped because "
            "Binance cooldown active",
            symbol
        )

        return False

    sl_price = (
        entry_price
        * (
            Decimal("1")
            - to_decimal(
                STOP_LOSS_PERCENT
            )
        )
    )

    sl_qty = (
        executed_qty
        * to_decimal(
            SELL_BALANCE_BUFFER
        )
    )

    sl_qty = normalize_quantity_decimal(
        symbol,
        sl_qty,
        log_reason=True
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
        "quantity": decimal_to_str(
            sl_qty
        ),
        "price": decimal_to_str(
            sl_price
        ),
        "stopPrice": decimal_to_str(
            sl_price
        ),
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
                "%s | STOP LOSS placed | "
                "price=%s | qty=%s",
                symbol,
                decimal_to_str(sl_price),
                decimal_to_str(sl_qty)
            )

            if symbol in positions:

                positions[symbol][
                    "sl_order_id"
                ] = data.get(
                    "orderId"
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
            con
```
