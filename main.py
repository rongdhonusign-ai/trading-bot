import os
import time
import json
import hmac
import hashlib
import logging
import threading
import random
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

# RSI50 এখন 50-এর উপরে হলেই trend filter pass
BUY_RSI50_MIN = Decimal("50")

# RSI3 আগে 10-এর নিচে যেতে হবে
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

# RSI50 calculation-এর জন্য বেশি history
HISTORY_CANDLES = 200

# Startup rate control
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

state_lock = threading.RLock()

executor = ThreadPoolExecutor(
    max_workers=WORKER_THREADS
)

ws_threads = []

bot_started = False
bot_live = False

shutdown_event = threading.Event()

last_processed_candle = {}


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
# SERVER TIME
# ============================================================

def sync_server_time():

    global server_time_offset

    try:

        local_before = now_ms()

        response = session.get(
            BASE_URL + "/api/v3/time",
            timeout=10
        )

        response.raise_for_status()

        local_after = now_ms()

        data = response.json()

        server_time = int(data["serverTime"])

        local_mid = (
            local_before + local_after
        ) // 2

        server_time_offset = (
            server_time - local_mid
        )

        logger.info(
            "Server time offset: %s ms",
            server_time_offset
        )

        return True

    except Exception as e:

        logger.error(
            "Server time sync failed: %s",
            e
        )

        return False


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

    if not BINANCE_API_SECRET:
        logger.error(
            "BINANCE_API_SECRET missing"
        )
        return None

    params = dict(params)

    params["timestamp"] = binance_time_ms()
    params["recvWindow"] = 10000

    query_string = "&".join(
        f"{key}={params[key]}"
        for key in params
    )

    signature = hmac.new(
        BINANCE_API_SECRET.encode(),
        query_string.encode(),
        hashlib.sha256
    ).hexdigest()

    params["signature"] = signature

    url = BASE_URL + path

    for attempt in range(retries):

        try:

            if method.upper() == "GET":

                response = session.get(
                    url,
                    params=params,
                    timeout=15
                )

            elif method.upper() == "POST":

                response = session.post(
                    url,
                    params=params,
                    timeout=15
                )

            elif method.upper() == "DELETE":

                response = session.delete(
                    url,
                    params=params,
                    timeout=15
                )

            else:

                raise ValueError(
                    f"Unsupported method: {method}"
                )

            if response.status_code == 200:
                return response.json()

            # Timestamp error
            if response.status_code == 400:

                try:

                    data = response.json()

                    if data.get("code") == -1021:

                        logger.warning(
                            "Timestamp error. Resyncing time..."
                        )

                        sync_server_time()

                        time.sleep(1)

                        continue

                except Exception:
                    pass

            # Rate limit
            if response.status_code in (418, 429):

                retry_after = response.headers.get(
                    "Retry-After"
                )

                wait_time = 10

                if retry_after:

                    try:

                        wait_time = max(
                            5,
                            int(float(retry_after))
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

                time.sleep(wait_time)

                continue

            logger.error(
                "Binance API error HTTP %s: %s",
                response.status_code,
                response.text[:500]
            )

            time.sleep(
                2 + attempt * 2
            )

        except requests.RequestException as e:

            logger.warning(
                "REST request error: %s",
                e
            )

            time.sleep(
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

        try:

            response = session.get(
                url,
                params=params or {},
                timeout=15
            )

            if response.status_code == 200:
                return response.json()

            if response.status_code in (418, 429):

                retry_after = response.headers.get(
                    "Retry-After"
                )

                wait_time = 10

                if retry_after:

                    try:

                        wait_time = max(
                            5,
                            int(float(retry_after))
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

                time.sleep(wait_time)

                continue

            logger.error(
                "Public API error HTTP %s: %s",
                response.status_code,
                response.text[:300]
            )

            time.sleep(
                2 + attempt * 2
            )

        except Exception as e:

            logger.warning(
                "Public REST error: %s",
                e
            )

            time.sleep(
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

    for item in data.get("symbols", []):

        symbol = item.get("symbol")

        if not symbol:
            continue

        filters = {}

        for f in item.get("filters", []):

            filter_type = f.get(
                "filterType"
            )

            if filter_type:
                filters[filter_type] = f

        symbol_filters[symbol] = {
            "status": item.get("status"),
            "baseAsset": item.get("baseAsset"),
            "quoteAsset": item.get("quoteAsset"),
            "filters": filters
        }

    logger.info(
        "Exchange info loaded: %d symbols",
        len(symbol_filters)
    )

    return True


# ============================================================
# SYMBOL SELECTION
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

        symbol = ticker.get("symbol")

        if not symbol:
            continue

        info = symbol_filters.get(symbol)

        if not info:
            continue

        if info["status"] != "TRADING":
            continue

        if info["quoteAsset"] != "USDT":
            continue

        base = info["baseAsset"]

        if base in STABLE_ASSETS:
            continue

        if base in {"BTC", "ETH"}:
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
        for x in candidates[:TOP_SYMBOLS]
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
            ", ".join(selected[:20])
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

            result.append(candle)

        except Exception:
            continue

    if len(result) < RSI_SLOW_PERIOD + 5:

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

        if fetch_klines(symbol):
            success += 1

        if (
            index % 10 == 0
            or index == len(symbols)
        ):

            logger.info(
                "Historical initialization: %d/%d",
                index,
                len(symbols)
            )

        time.sleep(
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

            gains.append(change)
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
                * Decimal(period - 1)
            )
            + gains[i]
        ) / Decimal(period)

        avg_loss = (
            (
                avg_loss
                * Decimal(period - 1)
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
# INDICATOR
# ============================================================

def get_rsi_values(symbol):

    with state_lock:

        data = list(
            candles.get(
                symbol,
                []
            )
        )

    if len(data) < RSI_SLOW_PERIOD + 5:
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

    info = symbol_filters.get(symbol)

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


def calculate_buy_quantity(
    symbol,
    price
):

    price = d(price)

    if price <= 0:
        return Decimal("0")

    raw_qty = BUY_USDT / price

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

    # Check minimum notional
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

    # Newer Binance symbols may use NOTIONAL
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


def get_asset_balance(asset):

    account = get_account()

    if not account:
        return Decimal("0")

    for balance in account.get(
        "balances",
        []
    ):

        if balance.get("asset") == asset:

            return d(
                balance.get(
                    "free",
                    "0"
                )
            )

    return Decimal("0")


# ============================================================
# MARKET PRICE
# ============================================================

def get_market_price(symbol):

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
# PLACE BUY
# ============================================================

def place_buy(
    symbol,
    signal_data
):

    with state_lock:

        if symbol in positions:
            return False

        if symbol in buying_symbols:
            return False

        buying_symbols.add(symbol)

    try:

        # Market order-এর আগে fresh price
        price = get_market_price(symbol)

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

            logger.info(
                "DRY RUN BUY | %s",
                symbol
            )

            with state_lock:

                positions[symbol] = {
                    "symbol": symbol,
                    "quantity": quantity,
                    "entry_price": price,
                    "stop_price": (
                        price
                        * (
                            Decimal("1")
                            - STOP_LOSS_PERCENT
                            / Decimal("100")
                        )
                    ),
                    "buy_time": time.time(),
                    "dry_run": True
                }

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
                - STOP_LOSS_PERCENT
                / Decimal("100")
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

        selling_symbols.add(symbol)

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

        actual_balance = get_asset_balance(
            base_asset
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

        executor.submit(
            place_sell,
            symbol,
            "1% STOP LOSS"
        )


# ============================================================
# RSI SIGNAL PROCESSING
# ============================================================

def process_symbol(symbol):

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

    # --------------------------------------------------------
    # One processing per closed candle
    # --------------------------------------------------------

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

    # ========================================================
    # SELL
    # ========================================================

    with state_lock:

        has_position = (
            symbol in positions
        )

    if has_position:

        # RSI3 <= 80 থেকে >80 cross
        sell_cross = (
            rsi3_previous <= SELL_RSI3_LEVEL
            and
            rsi3_current > SELL_RSI3_LEVEL
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

    # ========================================================
    # BUY
    # ========================================================

    # RSI3 আগের candle-এ 10-এর নিচে ছিল
    was_oversold = (
        rsi3_previous < BUY_RSI3_MAX
    )

    # RSI3 এখন আগের চেয়ে উপরে উঠছে
    rsi_turning_up = (
        rsi3_current > rsi3_previous
    )

    # RSI50 trend filter
    trend_ok = (
        rsi50 > BUY_RSI50_MIN
    )

    # --------------------------------------------------------
    # NEW BUY RULE
    #
    # RSI50 > 50
    # AND previous RSI3 < 10
    # AND current RSI3 > previous RSI3
    #
    # Current RSI3 10-এর নিচে থাকা বাধ্যতামূলক নয়।
    # --------------------------------------------------------

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

        executor.submit(
            place_buy,
            symbol,
            indicator
        )

    elif DEBUG_MODE:

        # Near signal
        near_signal = (
            rsi50 > Decimal("45")
            and
            rsi3_current < Decimal("20")
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
        # ====================================================

        check_stop_loss(
            symbol,
            close_price
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

                candles[symbol] = history

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
            "WebSocket group %d connected: %d symbols",
            group_number,
            len(group_symbols)
        )

    def on_message(
        ws,
        message
    ):

        try:

            data = json.loads(
                message
            )

            payload = data.get(
                "data",
                data
            )

            if payload.get("e") != "kline":
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
            "WebSocket group %d error: %s",
            group_number,
            error
        )

    def on_close(
        ws,
        close_status_code,
        close_msg
    ):

        logger.warning(
            "WebSocket group %d closed | "
            "code=%s | msg=%s",
            group_number,
            close_status_code,
            close_msg
        )

    return (
        on_open,
        on_message,
        on_error,
        on_close
    )


# ============================================================
# WEBSOCKET GROUP RUNNER
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

    reconnect_delay = WS_RECONNECT_MIN

    while not shutdown_event.is_set():

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

            ws.run_forever(
                ping_interval=WS_PING_INTERVAL,
                ping_timeout=WS_PING_TIMEOUT
            )

            reconnect_delay = WS_RECONNECT_MIN

        except Exception as e:

            logger.warning(
                "WebSocket group %d exception: %s",
                group_number,
                e
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

        logger.info(
            "WebSocket group %d reconnecting "
            "in %.1fs",
            group_number,
            wait_time
        )

        time.sleep(
            wait_time
        )

        reconnect_delay = min(
            reconnect_delay * 2,
            WS_RECONNECT_MAX
        )


# ============================================================
# START WEBSOCKETS
# ============================================================

def start_websockets():

    global ws_threads

    groups = [
        symbols[i:i + GROUP_SIZE]
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

        if not group:
            continue

        thread = threading.Thread(
            target=websocket_group_loop,
            args=(
                group,
                index
            ),
            daemon=True,
            name=f"WS-{index}"
        )

        thread.start()

        ws_threads.append(
            thread
        )

        logger.info(
            "WebSocket group %d started: %d symbols",
            index,
            len(group)
        )

        time.sleep(1)


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

    return True


# ============================================================
# POSITION RECOVERY
# ============================================================

def recover_positions():

    if DRY_RUN:
        return

    account = get_account()

    if not account:
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

        symbol = asset + "USDT"

        if symbol not in selected_set:
            continue

        price = get_market_price(
            symbol
        )

        if price <= 0:
            continue

        stop_price = (
            price
            * (
                Decimal("1")
                - STOP_LOSS_PERCENT
                / Decimal("100")
            )
        )

        with state_lock:

            positions[symbol] = {
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
# BOT STARTUP
# ============================================================

def start_bot():

    global bot_started
    global bot_live

    with state_lock:

        if bot_started:
            return

        bot_started = True

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

    try:

        # ----------------------------------------------------
        # TIME
        # ----------------------------------------------------

        sync_server_time()

        time.sleep(1)

        # ----------------------------------------------------
        # EXCHANGE INFO
        # ----------------------------------------------------

        if not load_exchange_info():

            logger.error(
                "BOT STOPPED: exchangeInfo failed"
            )

            return

        time.sleep(1)

        # ----------------------------------------------------
        # TOP SYMBOLS
        # ----------------------------------------------------

        if not load_top_symbols():

            logger.error(
                "BOT STOPPED: symbol selection failed"
            )

            return

        # ----------------------------------------------------
        # ACCOUNT
        # ----------------------------------------------------

        if not check_account_connection():

            logger.error(
                "BOT STOPPED: account connection failed"
            )

            return

        time.sleep(1)

        # ----------------------------------------------------
        # RECOVERY
        # ----------------------------------------------------

        recover_positions()

        time.sleep(1)

        # ----------------------------------------------------
        # HISTORICAL CANDLES
        # ----------------------------------------------------

        if not initialize_candles():

            logger.error(
                "BOT STOPPED: historical candle "
                "initialization failed"
            )

            return

        # ----------------------------------------------------
        # WEBSOCKETS
        # ----------------------------------------------------

        start_websockets()

        bot_live = True

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

    except Exception as e:

        logger.exception(
            "BOT STARTUP ERROR: %s",
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

            "dry_run": DRY_RUN
        })


@app.route("/health")
def health():

    with state_lock:

        return jsonify({

            "ok": True,

            "bot_live": bot_live,

            "symbols": len(symbols),

            "positions": len(positions),

            "websocket_groups": len(
                ws_threads
            ),

            "timestamp": int(
                time.time()
            )
        })


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
                )
            }

        return jsonify(data)


# ============================================================
# GUNICORN / RENDER START
# ============================================================

def start_bot_background():

    thread = threading.Thread(
        target=start_bot,
        daemon=True,
        name="BOT-MAIN"
    )

    thread.start()


if __name__ == "__main__":

    start_bot_background()

    app.run(
        host="0.0.0.0",
        port=int(
            os.getenv(
                "PORT",
                "10000"
            )
        )
    )

else:

    # Gunicorn import হলে background bot start
    start_bot_background()
