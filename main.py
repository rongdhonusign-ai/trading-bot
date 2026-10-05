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
BUY_RSI3_MAX = Decimal("10")

SELL_RSI3_LEVEL = Decimal("80")

HISTORY_CANDLES = 120

# REST request spacing during startup
STARTUP_KLINE_DELAY = 0.15

# WebSocket settings
WS_PING_INTERVAL = 20
WS_PING_TIMEOUT = 10

# REST settings
REQUEST_TIMEOUT = 15
MAX_RETRIES = 5

# Do not use more than one Gunicorn worker.
# Multiple workers would create multiple trading bots.
WORKER_THREADS = 4

# If true, orders are NOT actually sent.
DRY_RUN = os.getenv("DRY_RUN", "false").lower() == "true"


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

# symbol -> deque of closed candle dictionaries
candle_history = {}

# symbol -> current bot position information
positions = {}

# symbols currently being sold
selling_symbols = set()

# symbols currently having BUY order in progress
buying_symbols = set()

# Binance exchange info
symbol_info = {}

# Current selected Top 150
symbols = []

# Binance server time offset
server_time_offset_ms = 0

# HTTP session
http = requests.Session()

# Order executor
order_executor = ThreadPoolExecutor(
    max_workers=WORKER_THREADS,
    thread_name_prefix="ORDER"
)

# Startup protection
_bot_thread_started = False
_bot_thread_lock = threading.Lock()


# ============================================================
# EXCLUDED ASSETS
# ============================================================

EXCLUDED_ASSETS = {
    # Major coins
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

    # Other common stable/value tokens
    "PAX",
    "UST",
    "USTC",
    "FRAX",
    "LUSD",
    "PYUSD",
    "USDD",
}


# ============================================================
# HELPERS
# ============================================================

def d(value):
    """
    Safely convert value to Decimal.
    """
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        return Decimal("0")


def decimal_places(step):
    """
    Number of decimal places needed for a step size.
    """
    step = d(step)

    if step <= 0:
        return 8

    text = format(step, "f")

    if "." not in text:
        return 0

    return len(text.rstrip("0").split(".")[1])


def floor_to_step(value, step):
    """
    Round DOWN according to Binance LOT_SIZE step.
    """
    value = d(value)
    step = d(step)

    if step <= 0:
        return value

    return (value / step).to_integral_value(
        rounding=ROUND_DOWN
    ) * step


def decimal_to_string(value):
    """
    Decimal -> Binance compatible string.
    """
    value = d(value)

    text = format(value, "f")

    if "." in text:
        text = text.rstrip("0").rstrip(".")

    return text if text else "0"


def now_ms():
    return int(time.time() * 1000)


# ============================================================
# BINANCE REST
# ============================================================

def public_get(path, params=None):
    """
    Public Binance REST request with conservative retry.
    """

    if params is None:
        params = {}

    last_error = None

    for attempt in range(1, MAX_RETRIES + 1):

        try:

            response = http.get(
                BASE_URL + path,
                params=params,
                timeout=REQUEST_TIMEOUT,
            )

            if response.status_code == 200:
                return response.json()

            # Rate limit
            if response.status_code == 429:
                retry_after = response.headers.get(
                    "Retry-After",
                    "2"
                )

                try:
                    wait = max(float(retry_after), 2.0)
                except Exception:
                    wait = 2.0

                wait = min(wait * attempt, 30)

                log.warning(
                    "Binance 429 rate limit. Waiting %.1fs",
                    wait
                )

                time.sleep(wait)
                continue

            # IP ban
            if response.status_code == 418:
                log.error(
                    "Binance returned HTTP 418 IP BAN response."
                )

                time.sleep(min(10 * attempt, 60))
                continue

            # Server errors
            if response.status_code in (500, 502, 503, 504):

                wait = min(2 ** attempt, 30)

                log.warning(
                    "Binance HTTP %s. Retry in %ss",
                    response.status_code,
                    wait,
                )

                time.sleep(wait)
                continue

            response.raise_for_status()

        except Exception as exc:

            last_error = exc

            wait = min(2 ** attempt, 30)

            log.warning(
                "Public REST error attempt %s/%s: %s",
                attempt,
                MAX_RETRIES,
                exc,
            )

            time.sleep(wait)

    raise RuntimeError(
        f"Public Binance request failed: {last_error}"
    )


def signed_request(method, path, params=None):
    """
    Signed Binance REST request.

    Important:
    We do NOT blindly repeat an order request after a timeout,
    because the order might actually have been accepted.
    """

    if params is None:
        params = {}

    if not BINANCE_API_KEY or not BINANCE_API_SECRET:
        raise RuntimeError(
            "BINANCE_API_KEY / BINANCE_API_SECRET not configured."
        )

    params = dict(params)

    params["timestamp"] = now_ms() + server_time_offset_ms
    params["recvWindow"] = 5000

    query = "&".join(
        f"{key}={value}"
        for key, value in params.items()
    )

    signature = hmac.new(
        BINANCE_API_SECRET.encode("utf-8"),
        query.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()

    url = (
        BASE_URL
        + path
        + "?"
        + query
        + "&signature="
        + signature
    )

    headers = {
        "X-MBX-APIKEY": BINANCE_API_KEY
    }

    if method.upper() == "GET":

        return http.get(
            url,
            headers=headers,
            timeout=REQUEST_TIMEOUT,
        )

    if method.upper() == "POST":

        return http.post(
            url,
            headers=headers,
            timeout=REQUEST_TIMEOUT,
        )

    if method.upper() == "DELETE":

        return http.delete(
            url,
            headers=headers,
            timeout=REQUEST_TIMEOUT,
        )

    raise ValueError("Unsupported HTTP method")


# ============================================================
# SERVER TIME
# ============================================================

def sync_server_time():
    global server_time_offset_ms

    data = public_get("/api/v3/time")

    server_time = int(data["serverTime"])

    local_time = now_ms()

    server_time_offset_ms = server_time - local_time

    log.info(
        "Binance server time offset: %d ms",
        server_time_offset_ms,
    )


def server_time_loop():

    while True:

        try:
            sync_server_time()

        except Exception as exc:
            log.warning(
                "Server time sync error: %s",
                exc,
            )

        # 30 minutes
        time.sleep(1800)


# ============================================================
# EXCHANGE INFO
# ============================================================

def load_exchange_info():

    global symbol_info

    log.info("Loading Binance exchange information...")

    data = public_get("/api/v3/exchangeInfo")

    new_info = {}

    for item in data.get("symbols", []):

        symbol = item.get("symbol")

        if not symbol:
            continue

        if item.get("status") != "TRADING":
            continue

        if item.get("quoteAsset") != "USDT":
            continue

        if item.get("isSpotTradingAllowed") is False:
            continue

        filters = {
            f["filterType"]: f
            for f in item.get("filters", [])
        }

        lot_filter = filters.get("LOT_SIZE", {})
        market_lot_filter = filters.get(
            "MARKET_LOT_SIZE",
            lot_filter
        )

        min_notional_filter = filters.get(
            "NOTIONAL"
        )

        if not min_notional_filter:
            min_notional_filter = filters.get(
                "MIN_NOTIONAL",
                {}
            )

        new_info[symbol] = {
            "base_asset": item.get("baseAsset"),
            "quote_asset": item.get("quoteAsset"),

            "step_size": d(
                market_lot_filter.get(
                    "stepSize",
                    lot_filter.get("stepSize", "0.00000001")
                )
            ),

            "min_qty": d(
                market_lot_filter.get(
                    "minQty",
                    lot_filter.get("minQty", "0")
                )
            ),

            "lot_step_size": d(
                lot_filter.get(
                    "stepSize",
                    "0.00000001"
                )
            ),

            "min_notional": d(
                min_notional_filter.get(
                    "minNotional",
                    "0"
                )
            ),
        }

    symbol_info = new_info

    log.info(
        "Eligible USDT spot symbols: %d",
        len(symbol_info),
    )


# ============================================================
# TOP 150 SYMBOLS
# ============================================================

def select_top_symbols():

    global symbols

    log.info("Loading 24h ticker data...")

    tickers = public_get("/api/v3/ticker/24hr")

    candidates = []

    for ticker in tickers:

        symbol = ticker.get("symbol", "")

        if symbol not in symbol_info:
            continue

        info = symbol_info[symbol]

        base_asset = info["base_asset"]

        if base_asset in EXCLUDED_ASSETS:
            continue

        try:
            quote_volume = d(
                ticker.get("quoteVolume", "0")
            )
        except Exception:
            quote_volume = Decimal("0")

        if quote_volume <= 0:
            continue

        candidates.append(
            (
                quote_volume,
                symbol,
            )
        )

    candidates.sort(
        key=lambda x: x[0],
        reverse=True,
    )

    selected = [
        symbol
        for _, symbol in candidates[:TOP_SYMBOLS]
    ]

    symbols = selected

    log.info(
        "Selected TOP %d symbols",
        len(symbols),
    )

    log.info(
        "Top symbols: %s",
        ", ".join(symbols),
    )

    return symbols


# ============================================================
# RSI - WILDER
# ============================================================

def calculate_rsi_wilder(closes, period):
    """
    Binance-style RSI using Wilder/RMA smoothing.

    Returns RSI values aligned to closes.

    None is used until enough data exists.
    """

    values = [d(x) for x in closes]

    result = [None] * len(values)

    if len(values) <= period:
        return result

    gains = []
    losses = []

    for i in range(1, len(values)):

        change = values[i] - values[i - 1]

        if change > 0:
            gains.append(change)
            losses.append(Decimal("0"))

        else:
            gains.append(Decimal("0"))
            losses.append(-change)

    # Initial Wilder average
    avg_gain = (
        sum(gains[:period], Decimal("0"))
        / Decimal(period)
    )

    avg_loss = (
        sum(losses[:period], Decimal("0"))
        / Decimal(period)
    )

    def make_rsi(gain, loss):

        if loss == 0:

            if gain == 0:
                return Decimal("50")

            return Decimal("100")

        rs = gain / loss

        return Decimal("100") - (
            Decimal("100")
            / (Decimal("1") + rs)
        )

    result[period] = make_rsi(
        avg_gain,
        avg_loss,
    )

    # Wilder smoothing
    for i in range(period + 1, len(values)):

        gain = gains[i - 1]
        loss = losses[i - 1]

        avg_gain = (
            (
                avg_gain * Decimal(period - 1)
            )
            + gain
        ) / Decimal(period)

        avg_loss = (
            (
                avg_loss * Decimal(period - 1)
            )
            + loss
        ) / Decimal(period)

        result[i] = make_rsi(
            avg_gain,
            avg_loss,
        )

    return result


# ============================================================
# CANDLE PROCESSING
# ============================================================

def candle_to_dict(kline):

    return {
        "open_time": int(kline[0]),
        "open": d(kline[1]),
        "high": d(kline[2]),
        "low": d(kline[3]),
        "close": d(kline[4]),
        "volume": d(kline[5]),
        "close_time": int(kline[6]),
    }


def add_closed_candle(symbol, candle):

    with state_lock:

        if symbol not in candle_history:
            candle_history[symbol] = deque(
                maxlen=HISTORY_CANDLES
            )

        history = candle_history[symbol]

        # Avoid duplicate candle
        if history:

            if (
                history[-1]["open_time"]
                == candle["open_time"]
            ):
                history[-1] = candle
                return

            if (
                history[-1]["open_time"]
                > candle["open_time"]
            ):
                return

        history.append(candle)


# ============================================================
# INITIAL HISTORICAL DATA
# ============================================================

def load_historical_data():

    log.info(
        "Loading initial %d-candle history for %d symbols...",
        HISTORY_CANDLES,
        len(symbols),
    )

    successful = 0

    current_time = now_ms()

    for index, symbol in enumerate(symbols, start=1):

        try:

            data = public_get(
                "/api/v3/klines",
                params={
                    "symbol": symbol,
                    "interval": TIMEFRAME,
                    "limit": HISTORY_CANDLES,
                },
            )

            history = deque(
                maxlen=HISTORY_CANDLES
            )

            for row in data:

                candle = candle_to_dict(row)

                # Only CLOSED candles.
                if candle["close_time"] >= current_time:
                    continue

                history.append(candle)

            if len(history) >= RSI_SLOW_PERIOD + 2:

                with state_lock:
                    candle_history[symbol] = history

                successful += 1

            else:

                log.warning(
                    "%s | insufficient history: %d candles",
                    symbol,
                    len(history),
                )

        except Exception as exc:

            log.error(
                "%s | historical data error: %s",
                symbol,
                exc,
            )

        # Conservative REST spacing.
        time.sleep(STARTUP_KLINE_DELAY)

        if index % 25 == 0:

            log.info(
                "Historical loading progress: %d/%d",
                index,
                len(symbols),
            )

    log.info(
        "Historical initialization complete: %d/%d",
        successful,
        len(symbols),
    )


# ============================================================
# INDICATOR VALUES
# ============================================================

def get_current_indicators(symbol):

    with state_lock:

        history = candle_history.get(symbol)

        if not history:
            return None

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

    if len(rsi3_values) < 2:
        return None

    if len(rsi50_values) < 2:
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
        "prev_rsi3": previous_rsi3,
        "rsi50": current_rsi50,
    }


# ============================================================
# ACCOUNT BALANCE
# ============================================================

def get_asset_balance(asset):

    response = signed_request(
        "GET",
        "/api/v3/account",
    )

    if response.status_code != 200:

        raise RuntimeError(
            f"Account request failed "
            f"{response.status_code}: "
            f"{response.text[:300]}"
        )

    data = response.json()

    for balance in data.get("balances", []):

        if balance.get("asset") == asset:

            return {
                "free": d(balance.get("free", "0")),
                "locked": d(balance.get("locked", "0")),
            }

    return {
        "free": Decimal("0"),
        "locked": Decimal("0"),
    }


# ============================================================
# TEST ACCOUNT CONNECTION
# ============================================================

def verify_account():

    response = signed_request(
        "GET",
        "/api/v3/account",
    )

    if response.status_code != 200:

        raise RuntimeError(
            "Binance account connection failed: "
            + response.text[:500]
        )

    log.info("Binance account connection OK")


# ============================================================
# ORDER HELPERS
# ============================================================

def create_client_order_id(prefix):

    # Binance clientOrderId max length is limited.
    return (
        prefix
        + str(int(time.time() * 1000))[-12:]
        + str(random.randint(1000, 9999))
    )


def get_order_by_client_id(symbol, client_order_id):

    response = signed_request(
        "GET",
        "/api/v3/order",
        params={
            "symbol": symbol,
            "origClientOrderId": client_order_id,
        },
    )

    if response.status_code == 200:
        return response.json()

    return None


# ============================================================
# MARKET BUY
# ============================================================

def execute_buy(symbol):

    with state_lock:

        if symbol in positions:
            log.info(
                "%s | BUY skipped - position already exists",
                symbol,
            )
            return

        if symbol in buying_symbols:
            log.info(
                "%s | BUY skipped - order already in progress",
                symbol,
            )
            return

        buying_symbols.add(symbol)

    try:

        log.info(
            "%s | BUY SIGNAL | RSI50 > %s | RSI3 < %s | amount=%s USDT",
            symbol,
            BUY_RSI50_MIN,
            BUY_RSI3_MAX,
            BUY_USDT,
        )

        if DRY_RUN:

            log.warning(
                "%s | DRY_RUN BUY - no real order sent",
                symbol,
            )

            fake_qty = Decimal("0")

            with state_lock:
                positions[symbol] = {
                    "quantity": fake_qty,
                    "entry_time": time.time(),
                    "dry_run": True,
                }

            return

        client_order_id = create_client_order_id(
            "RSIBUY"
        )

        response = signed_request(
            "POST",
            "/api/v3/order",
            params={
                "symbol": symbol,
                "side": "BUY",
                "type": "MARKET",
                "quoteOrderQty": decimal_to_string(
                    BUY_USDT
                ),
                "newClientOrderId": client_order_id,
                "newOrderRespType": "FULL",
            },
        )

        if response.status_code != 200:

            log.error(
                "%s | BUY failed HTTP %s: %s",
                symbol,
                response.status_code,
                response.text[:500],
            )

            # If server returned error after processing,
            # check whether order actually exists.
            if response.status_code >= 500:

                time.sleep(2)

                existing = get_order_by_client_id(
                    symbol,
                    client_order_id,
                )

                if existing:
                    response_data = existing
                else:
                    return

            else:
                return

        else:

            response_data = response.json()

        status = response_data.get("status")

        if status not in (
            "FILLED",
            "PARTIALLY_FILLED",
        ):

            log.warning(
                "%s | BUY status=%s",
                symbol,
                status,
            )

            return

        executed_qty = d(
            response_data.get(
                "executedQty",
                "0"
            )
        )

        if executed_qty <= 0:

            log.error(
                "%s | BUY executed quantity is zero",
                symbol,
            )

            return

        # Commission may be charged in base asset.
        base_asset = symbol_info[symbol]["base_asset"]

        base_commission = Decimal("0")

        for fill in response_data.get("fills", []):

            if fill.get("commissionAsset") == base_asset:

                base_commission += d(
                    fill.get("commission", "0")
                )

        actual_qty = executed_qty - base_commission

        if actual_qty <= 0:
            actual_qty = executed_qty

        step_size = symbol_info[symbol]["step_size"]

        actual_qty = floor_to_step(
            actual_qty,
            step_size,
        )

        if actual_qty <= 0:

            log.error(
                "%s | quantity became zero after rounding",
                symbol,
            )

            return

        with state_lock:

            positions[symbol] = {
                "quantity": actual_qty,
                "entry_time": time.time(),
                "order_id": response_data.get("orderId"),
                "client_order_id": client_order_id,
                "dry_run": False,
            }

        log.info(
            "%s | BUY FILLED | qty=%s | orderId=%s",
            symbol,
            actual_qty,
            response_data.get("orderId"),
        )

    except Exception as exc:

        log.exception(
            "%s | BUY exception: %s",
            symbol,
            exc,
        )

    finally:

        with state_lock:
            buying_symbols.discard(symbol)


# ============================================================
# MARKET SELL
# ============================================================

def execute_sell(symbol):

    with state_lock:

        if symbol not in positions:

            log.info(
                "%s | SELL skipped - no bot position",
                symbol,
            )

            return

        if symbol in selling_symbols:

            log.info(
                "%s | SELL skipped - already selling",
                symbol,
            )

            return

        selling_symbols.add(symbol)

        position = dict(
            positions[symbol]
        )

    try:

        log.info(
            "%s | SELL SIGNAL | RSI3 crossed above %s",
            symbol,
            SELL_RSI3_LEVEL,
        )

        if DRY_RUN:

            log.warning(
                "%s | DRY_RUN SELL - no real order sent",
                symbol,
            )

            with state_lock:
                positions.pop(symbol, None)

            return

        # ----------------------------------------------------
        # Get actual FREE balance before SELL.
        # This helps avoid "insufficient balance" caused by
        # commission / rounding.
        # ----------------------------------------------------

        base_asset = symbol_info[symbol]["base_asset"]

        balance = get_asset_balance(
            base_asset
        )

        free_qty = balance["free"]

        step_size = symbol_info[symbol]["step_size"]

        sell_qty = floor_to_step(
            free_qty,
            step_size,
        )

        min_qty = symbol_info[symbol]["min_qty"]

        if sell_qty <= 0:

            log.error(
                "%s | SELL quantity is zero | free=%s",
                symbol,
                free_qty,
            )

            return

        if min_qty > 0 and sell_qty < min_qty:

            log.error(
                "%s | SELL quantity %s below minQty %s",
                symbol,
                sell_qty,
                min_qty,
            )

            return

        client_order_id = create_client_order_id(
            "RSISELL"
        )

        log.info(
            "%s | SELLING 100%% available balance | qty=%s",
            symbol,
            sell_qty,
        )

        response = signed_request(
            "POST",
            "/api/v3/order",
            params={
                "symbol": symbol,
                "side": "SELL",
                "type": "MARKET",
                "quantity": decimal_to_string(
                    sell_qty
                ),
                "newClientOrderId": client_order_id,
                "newOrderRespType": "FULL",
            },
        )

        if response.status_code != 200:

            log.error(
                "%s | SELL failed HTTP %s: %s",
                symbol,
                response.status_code,
                response.text[:500],
            )

            # Never blindly repeat a possible order submission.
            if response.status_code >= 500:

                time.sleep(2)

                existing = get_order_by_client_id(
                    symbol,
                    client_order_id,
                )

                if existing:
                    response_data = existing
                else:
                    return

            else:

                # Insufficient balance can happen if another
                # process/order changed the balance.
                return

        else:

            response_data = response.json()

        status = response_data.get("status")

        if status not in (
            "FILLED",
            "PARTIALLY_FILLED",
        ):

            log.warning(
                "%s | SELL status=%s",
                symbol,
                status,
            )

            return

        sold_qty = d(
            response_data.get(
                "executedQty",
                "0"
            )
        )

        log.info(
            "%s | SELL FILLED | sold=%s | orderId=%s",
            symbol,
            sold_qty,
            response_data.get("orderId"),
        )

        # Remove position only after confirmed order response.
        with state_lock:
            positions.pop(symbol, None)

    except Exception as exc:

        log.exception(
            "%s | SELL exception: %s",
            symbol,
            exc,
        )

    finally:

        with state_lock:
            selling_symbols.discard(symbol)


# ============================================================
# SIGNAL PROCESSOR
# ============================================================

def process_closed_candle(symbol):

    indicators = get_current_indicators(
        symbol
    )

    if not indicators:
        return

    rsi3 = indicators["rsi3"]
    prev_rsi3 = indicators["prev_rsi3"]
    rsi50 = indicators["rsi50"]

    # --------------------------------------------------------
    # SELL FIRST
    # --------------------------------------------------------
    #
    # Previous closed RSI3 <= 80
    # Current closed RSI3 > 80
    #
    # This is the exact upward cross.
    # --------------------------------------------------------

    sell_cross = (
        prev_rsi3 <= SELL_RSI3_LEVEL
        and
        rsi3 > SELL_RSI3_LEVEL
    )

    with state_lock:
        has_position = symbol in positions

    if has_position and sell_cross:

        log.info(
            "%s | SELL CROSS | RSI3 %.4f -> %.4f",
            symbol,
            prev_rsi3,
            rsi3,
        )

        order_executor.submit(
            execute_sell,
            symbol,
        )

        return

    # --------------------------------------------------------
    # BUY
    # --------------------------------------------------------

    buy_signal = (
        rsi50 > BUY_RSI50_MIN
        and
        rsi3 < BUY_RSI3_MAX
    )

    if buy_signal:

        with state_lock:
            has_position = symbol in positions

        if not has_position:

            log.info(
                "%s | BUY CONDITIONS TRUE | "
                "RSI50=%.4f | RSI3=%.4f",
                symbol,
                rsi50,
                rsi3,
            )

            order_executor.submit(
                execute_buy,
                symbol,
            )


# ============================================================
# WEBSOCKET MESSAGE
# ============================================================

def websocket_on_message(ws, message):

    try:

        data = json.loads(message)

        # Combined stream format:
        #
        # {
        #   "stream": "...",
        #   "data": {...}
        # }

        payload = data.get("data", data)

        if payload.get("e") != "kline":
            return

        kline = payload.get("k", {})

        symbol = kline.get("s")

        if not symbol:
            return

        # Only CLOSED candle.
        if not kline.get("x", False):
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

        add_closed_candle(
            symbol,
            candle,
        )

        # IMPORTANT:
        # Process only after the candle is closed.
        process_closed_candle(
            symbol
        )

    except Exception as exc:

        log.exception(
            "WebSocket message processing error: %s",
            exc,
        )


# ============================================================
# WEBSOCKET CALLBACKS
# ============================================================

def websocket_on_error(ws, error):

    log.warning(
        "WebSocket error: %s",
        error,
    )


def websocket_on_close(
    ws,
    close_status_code,
    close_msg,
):

    log.warning(
        "WebSocket closed | code=%s | msg=%s",
        close_status_code,
        close_msg,
    )


def websocket_on_open(ws):

    log.info(
        "WebSocket connected."
    )


# ============================================================
# WEBSOCKET GROUP
# ============================================================

def run_websocket_group(
    group_number,
    group_symbols,
):

    streams = "/".join(
        f"{symbol.lower()}@kline_{TIMEFRAME}"
        for symbol in group_symbols
    )

    ws_url = (
        WS_BASE_URL
        + streams
    )

    log.info(
        "Starting WebSocket group %d | %d symbols",
        group_number,
        len(group_symbols),
    )

    reconnect_delay = 3

    while True:

        ws = None

        try:

            ws = websocket.WebSocketApp(
                ws_url,

                on_open=websocket_on_open,

                on_message=websocket_on_message,

                on_error=websocket_on_error,

                on_close=websocket_on_close,
            )

            log.info(
                "WebSocket group %d connecting...",
                group_number,
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
                group_number,
                exc,
            )

        finally:

            try:

                if ws:
                    ws.close()

            except Exception:
                pass

        log.warning(
            "WebSocket group %d reconnecting in %ss...",
            group_number,
            reconnect_delay,
        )

        time.sleep(
            reconnect_delay
        )

        reconnect_delay = min(
            reconnect_delay * 2,
            60,
        )


# ============================================================
# START WEBSOCKETS
# ============================================================

def start_websockets():

    if not symbols:

        raise RuntimeError(
            "No symbols available for WebSocket."
        )

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

        thread = threading.Thread(
            target=run_websocket_group,
            args=(index, group),
            daemon=True,
            name=f"WS-GROUP-{index}",
        )

        thread.start()

        # Small stagger to avoid simultaneous connection burst.
        time.sleep(1)

    log.info(
        "All WebSocket groups started."
    )


# ============================================================
# MAIN BOT
# ============================================================

def run_bot():

    log.info("=" * 70)
    log.info(
        "STARTING BINANCE SPOT RSI3 + RSI50 BOT"
    )
    log.info("=" * 70)

    log.info(
        "Timeframe: %s",
        TIMEFRAME,
    )

    log.info(
        "Top symbols: %d",
        TOP_SYMBOLS,
    )

    log.info(
        "BUY: RSI50 > %s AND RSI3 < %s",
        BUY_RSI50_MIN,
        BUY_RSI3_MAX,
    )

    log.info(
        "SELL: RSI3 crosses above %s",
        SELL_RSI3_LEVEL,
    )

    log.info(
        "BUY amount: %s USDT",
        BUY_USDT,
    )

    log.info(
        "DRY_RUN: %s",
        DRY_RUN,
    )

    if not BINANCE_API_KEY or not BINANCE_API_SECRET:

        raise RuntimeError(
            "BINANCE_API_KEY or BINANCE_API_SECRET is missing."
        )

    # --------------------------------------------------------
    # Binance server time
    # --------------------------------------------------------

    sync_server_time()

    # --------------------------------------------------------
    # Exchange information
    # --------------------------------------------------------

    load_exchange_info()

    # --------------------------------------------------------
    # Top 150
    # --------------------------------------------------------

    select_top_symbols()

    if not symbols:

        raise RuntimeError(
            "Top symbol selection returned zero symbols."
        )

    # --------------------------------------------------------
    # Account connection
    # --------------------------------------------------------

    verify_account()

    # --------------------------------------------------------
    # Historical data
    # --------------------------------------------------------

    load_historical_data()

    # --------------------------------------------------------
    # Start background server-time sync
    # --------------------------------------------------------

    threading.Thread(
        target=server_time_loop,
        daemon=True,
        name="SERVER-TIME",
    ).start()

    # --------------------------------------------------------
    # Start WebSockets
    # --------------------------------------------------------

    start_websockets()

    log.info("=" * 70)
    log.info(
        "BOT IS RUNNING"
    )
    log.info("=" * 70)

    # Keep main bot thread alive.
    while True:

        time.sleep(60)


# ============================================================
# BOT SUPERVISOR
# ============================================================

def start_background_bot():

    """
    Keeps the bot alive.

    If an unexpected startup/runtime exception happens,
    restart after a delay.
    """

    restart_delay = 15

    while True:

        try:

            run_bot()

        except Exception as exc:

            log.exception(
                "BOT CRASHED / STARTUP ERROR: %s",
                exc,
            )

            log.error(
                "Bot will restart in %s seconds...",
                restart_delay,
            )

            time.sleep(
                restart_delay
            )

            restart_delay = min(
                restart_delay * 2,
                120,
            )

        else:

            restart_delay = 15

            log.warning(
                "Bot stopped unexpectedly. Restarting..."
            )

            time.sleep(
                restart_delay
            )


# ============================================================
# HEALTH ENDPOINTS
# ============================================================

@app.route("/")
def home():

    with state_lock:

        return jsonify({
            "status": "online",
            "service": "Binance Spot RSI3 + RSI50 Bot",
            "bot_thread_started": _bot_thread_started,
            "symbols": len(symbols),
            "positions": len(positions),
            "buying": len(buying_symbols),
            "selling": len(selling_symbols),
            "timeframe": TIMEFRAME,
            "buy_usdt": decimal_to_string(
                BUY_USDT
            ),
            "dry_run": DRY_RUN,
        })


@app.route("/health")
def health():

    with state_lock:

        return jsonify({
            "status": "healthy",
            "bot_started": _bot_thread_started,
            "top_symbols": len(symbols),
            "history_symbols": len(candle_history),
            "open_positions": len(positions),
            "buying_symbols": len(buying_symbols),
            "selling_symbols": len(selling_symbols),
        })


# ============================================================
# IMPORTANT:
# START BOT WHEN GUNICORN IMPORTS main:app
# ============================================================

def ensure_bot_started():

    global _bot_thread_started

    with _bot_thread_lock:

        if _bot_thread_started:
            return

        _bot_thread_started = True

        log.info("=" * 70)
        log.info(
            "GUNICORN IMPORT DETECTED"
        )
        log.info(
            "STARTING BINANCE BOT BACKGROUND THREAD"
        )
        log.info("=" * 70)

        thread = threading.Thread(
            target=start_background_bot,
            daemon=True,
            name="BOT-MAIN",
        )

        thread.start()


# ------------------------------------------------------------
# THIS IS THE IMPORTANT FIX.
#
# Gunicorn imports:
#
#     main:app
#
# Therefore:
#
#     if __name__ == "__main__":
#
# does NOT run.
#
# We explicitly start the bot during module import.
# ------------------------------------------------------------

ensure_bot_started()


# ============================================================
# DIRECT PYTHON START
# ============================================================

if __name__ == "__main__":

    port = int(
        os.getenv(
            "PORT",
            "10000"
        )
    )

    log.info(
        "Flask server starting on port %d",
        port,
    )

    app.run(
        host="0.0.0.0",
        port=port,
        threaded=True,
        use_reloader=False,
    )
