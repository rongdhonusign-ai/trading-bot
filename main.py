import os
import time
import json
import hmac
import hashlib
import signal
import random
import logging
import threading

from decimal import Decimal, ROUND_DOWN
from urllib.parse import urlencode
from concurrent.futures import ThreadPoolExecutor

import requests
import pandas as pd
import websocket

from flask import Flask, jsonify


# ============================================================
# CONFIG
# ============================================================

API_KEY = os.getenv("BINANCE_API_KEY")
API_SECRET = os.getenv("BINANCE_API_SECRET")

if not API_KEY or not API_SECRET:
    raise RuntimeError(
        "BINANCE_API_KEY and BINANCE_API_SECRET are required."
    )


BASE_URL = "https://api.binance.com"

WS_BASE = (
    "wss://stream.binance.com:9443/stream?streams="
)


# ============================================================
# STRATEGY
# ============================================================

TIMEFRAME = "5m"

TOP_SYMBOLS = 150

GROUPS = 3

SYMBOLS_PER_GROUP = 50


# BUY
BUY_USDT = Decimal("15")

RSI_FAST_PERIOD = 3

RSI_SLOW_PERIOD = 50

BUY_RSI_SLOW_MIN = 50

BUY_RSI_FAST_MAX = 10


# SELL
SELL_RSI_LEVEL = 80


# STOP LOSS
STOP_LOSS_PERCENT = Decimal("0.01")


# ============================================================
# MARKET DATA
# ============================================================

HISTORY_LIMIT = 120

STARTUP_REST_DELAY = 0.15

RECONNECT_MIN = 3

RECONNECT_MAX = 60

WS_MAX_LIFETIME = 23 * 60 * 60


# ============================================================
# POSITION RECOVERY
# ============================================================

BOT_BUY_PREFIX = "RSIBUY_"

BOT_SELL_PREFIX = "RSISELL_"

BOT_SL_PREFIX = "RSISL_"

RECOVERY_LOOKBACK_HOURS = 7 * 24

RECOVERY_ORDER_LIMIT = 50


# ============================================================
# ORDER SETTINGS
# ============================================================

ORDER_WORKERS = 4

RECV_WINDOW = 5000

MAX_RETRIES = 6

HTTP_TIMEOUT = 15

SELL_BALANCE_BUFFER = Decimal("0.999")


# ============================================================
# LOGGING
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

logger = logging.getLogger(
    "TOP150_RSI_BOT"
)


# ============================================================
# FLASK
# ============================================================

app = Flask(__name__)


# ============================================================
# GLOBAL STATE
# ============================================================

running = True

state_lock = threading.RLock()

symbols = []

symbol_filters = {}

indicator_state = {}

positions = {}

orders_in_flight = set()

ws_threads = []

order_executor = ThreadPoolExecutor(
    max_workers=ORDER_WORKERS,
    thread_name_prefix="ORDER"
)


# ============================================================
# HTTP SESSION
# ============================================================

session = requests.Session()

session.headers.update({
    "X-MBX-APIKEY": API_KEY,
    "User-Agent": "Top150-RSI-Bot/3.0"
})


# ============================================================
# FLASK
# ============================================================

@app.route("/")
def home():

    with state_lock:
        position_count = len(
            positions
        )

    return jsonify({
        "status": "running",
        "strategy":
            "TOP150 RSI50 > 50 + RSI3 < 10",
        "timeframe":
            TIMEFRAME,
        "buy_usdt":
            str(BUY_USDT),
        "stop_loss":
            "1%",
        "sell":
            "RSI3 crossing above 80",
        "position_recovery":
            True,
        "server_side_stop_loss":
            True,
        "positions":
            position_count
    })


@app.route("/health")
def health():

    with state_lock:

        return jsonify({
            "status":
                "healthy",

            "running":
                running,

            "symbols":
                len(symbols),

            "positions":
                len(positions),

            "orders_in_flight":
                len(orders_in_flight)
        })


# ============================================================
# SIGNATURE
# ============================================================

def sign_params(params):

    query_string = urlencode(
        params
    )

    signature = hmac.new(
        API_SECRET.encode("utf-8"),
        query_string.encode("utf-8"),
        hashlib.sha256
    ).hexdigest()

    return (
        query_string
        +
        "&signature="
        +
        signature
    )


# ============================================================
# BINANCE REST
# ============================================================

def binance_request(
    method,
    path,
    params=None,
    signed=False
):

    if params is None:
        params = {}

    delay = 1.0


    for attempt in range(
        MAX_RETRIES
    ):

        try:

            request_params = dict(
                params
            )


            if signed:

                request_params[
                    "timestamp"
                ] = int(
                    time.time() * 1000
                )

                request_params[
                    "recvWindow"
                ] = RECV_WINDOW


                query = sign_params(
                    request_params
                )

            else:

                query = urlencode(
                    request_params
                )


            url = BASE_URL + path


            if query:

                url += "?" + query


            if method == "GET":

                response = session.get(
                    url,
                    timeout=HTTP_TIMEOUT
                )

            elif method == "POST":

                response = session.post(
                    url,
                    timeout=HTTP_TIMEOUT
                )

            elif method == "DELETE":

                response = session.delete(
                    url,
                    timeout=HTTP_TIMEOUT
                )

            else:

                raise ValueError(
                    "Unsupported HTTP method"
                )


            # ==================================================
            # SUCCESS
            # ==================================================

            if response.status_code == 200:

                try:

                    return response.json()

                except Exception:

                    return None


            # ==================================================
            # 429
            # ==================================================

            if response.status_code == 429:

                retry_after = (
                    response.headers.get(
                        "Retry-After"
                    )
                )


                if retry_after:

                    try:

                        wait = float(
                            retry_after
                        )

                    except Exception:

                        wait = delay

                else:

                    wait = delay


                wait = max(
                    wait,
                    delay
                )


                logger.warning(
                    "HTTP 429 | waiting %.2fs",
                    wait
                )


                time.sleep(
                    wait
                    +
                    random.uniform(
                        0.2,
                        0.8
                    )
                )


                delay = min(
                    delay * 2,
                    60
                )


                continue


            # ==================================================
            # 418
            # ==================================================

            if response.status_code == 418:

                retry_after = (
                    response.headers.get(
                        "Retry-After",
                        "60"
                    )
                )


                try:

                    wait = float(
                        retry_after
                    )

                except Exception:

                    wait = 60


                logger.error(
                    "HTTP 418 | waiting %.2fs",
                    wait
                )


                time.sleep(
                    wait
                    +
                    random.uniform(
                        1,
                        5
                    )
                )


                continue


            # ==================================================
            # TIMESTAMP ERROR
            # ==================================================

            if response.status_code == 400:

                text = response.text[:1000]

                logger.error(
                    "HTTP 400 | %s",
                    text
                )


                if "-1021" in text:

                    time.sleep(
                        1
                        +
                        random.uniform(
                            0,
                            1
                        )
                    )

                    continue


            # ==================================================
            # OTHER ERROR
            # ==================================================

            logger.error(
                "HTTP %s | %s",
                response.status_code,
                response.text[:1000]
            )


            if attempt == (
                MAX_RETRIES - 1
            ):

                return None


            time.sleep(
                delay
                +
                random.uniform(
                    0.2,
                    0.8
                )
            )


            delay = min(
                delay * 2,
                30
            )


        except requests.RequestException as e:

            logger.warning(
                "REST network error: %s",
                e
            )


            if attempt == (
                MAX_RETRIES - 1
            ):

                return None


            time.sleep(
                delay
                +
                random.uniform(
                    0.2,
                    0.8
                )
            )


            delay = min(
                delay * 2,
                30
            )


        except Exception as e:

            logger.exception(
                "REST exception: %s",
                e
            )

            return None


    return None


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


    if not data:

        raise RuntimeError(
            "exchangeInfo unavailable"
        )


    result = {}


    for item in data.get(
        "symbols",
        []
    ):

        symbol = item.get(
            "symbol"
        )


        if item.get(
            "status"
        ) != "TRADING":

            continue


        if item.get(
            "quoteAsset"
        ) != "USDT":

            continue


        if not item.get(
            "isSpotTradingAllowed",
            True
        ):

            continue


        filters = {}


        for f in item.get(
            "filters",
            []
        ):

            filters[
                f["filterType"]
            ] = f


        result[symbol] = {

            "baseAsset":
                item[
                    "baseAsset"
                ],

            "quoteAsset":
                item[
                    "quoteAsset"
                ],

            "filters":
                filters
        }


    logger.info(
        "Eligible USDT symbols: %d",
        len(result)
    )


    return result


# ============================================================
# TOP 150
# ============================================================

def get_top_150_symbols(
    exchange_info
):

    logger.info(
        "Loading 24h ticker data..."
    )


    data = binance_request(
        "GET",
        "/api/v3/ticker/24hr"
    )


    if not data:

        raise RuntimeError(
            "ticker data unavailable"
        )


    excluded_assets = {

        "USDT",
        "USDC",
        "FDUSD",
        "BUSD",
        "TUSD",
        "DAI",
        "USDP",

        "BTC",
        "ETH",

        "EUR",
        "GBP",
        "AUD",
        "TRY",
        "BRL",
        "RUB",
        "UAH",
        "PLN",
        "RON",
        "ZAR"
    }


    candidates = []


    for item in data:

        symbol = item.get(
            "symbol"
        )


        if symbol not in exchange_info:

            continue


        base_asset = (
            exchange_info[
                symbol
            ]["baseAsset"]
        )


        if base_asset in (
            excluded_assets
        ):

            continue


        try:

            volume = Decimal(
                str(
                    item.get(
                        "quoteVolume",
                        "0"
                    )
                )
            )

        except Exception:

            continue


        if volume <= 0:

            continue


        candidates.append(
            (
                symbol,
                volume
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


    logger.info(
        "Selected %d Top symbols.",
        len(selected)
    )


    return selected


# ============================================================
# RSI
# ============================================================

def calculate_rsi(
    series,
    period
):

    series = pd.to_numeric(
        series,
        errors="coerce"
    ).dropna()


    if len(series) < (
        period + 1
    ):

        return None


    delta = series.diff()


    gains = delta.clip(
        lower=0
    )


    losses = -delta.clip(
        upper=0
    )


    avg_gain = gains.ewm(
        alpha=1 / period,
        adjust=False,
        min_periods=period
    ).mean()


    avg_loss = losses.ewm(
        alpha=1 / period,
        adjust=False,
        min_periods=period
    ).mean()


    gain = avg_gain.iloc[-1]

    loss = avg_loss.iloc[-1]


    if (
        pd.isna(gain)
        or
        pd.isna(loss)
    ):

        return None


    if loss == 0:

        return 100.0


    rs = gain / loss


    return float(
        100
        -
        (
            100
            /
            (
                1 + rs
            )
        )
    )


# ============================================================
# SEED SYMBOL
# ============================================================

def seed_symbol(symbol):

    data = binance_request(
        "GET",
        "/api/v3/klines",
        params={
            "symbol":
                symbol,

            "interval":
                TIMEFRAME,

            "limit":
                HISTORY_LIMIT
        }
    )


    if not data:

        logger.warning(
            "%s | history unavailable",
            symbol
        )

        return


    try:

        if len(data) < 3:

            return


        # Only CLOSED candles.
        closed_data = data[:-1]


        closes = [
            float(
                row[4]
            )
            for row in closed_data
        ]


        series = pd.Series(
            closes,
            dtype="float64"
        )


        rsi3 = calculate_rsi(
            series,
            RSI_FAST_PERIOD
        )


        rsi50 = calculate_rsi(
            series,
            RSI_SLOW_PERIOD
        )


        if (
            rsi3 is None
            or
            rsi50 is None
        ):

            return


        closed_time = int(
            closed_data[-1][6]
        )


        with state_lock:

            indicator_state[
                symbol
            ] = {

                "closes":
                    closes[
                        -HISTORY_LIMIT:
                    ],

                "rsi3":
                    rsi3,

                "rsi50":
                    rsi50,

                "previous_rsi3":
                    None,

                "last_closed_time":
                    closed_time
            }


    except Exception as e:

        logger.error(
            "%s | seed error: %s",
            symbol,
            e
        )


# ============================================================
# SEED ALL
# ============================================================

def seed_all_symbols():

    logger.info(
        "Seeding RSI history for %d symbols...",
        len(symbols)
    )


    for i, symbol in enumerate(
        symbols,
        start=1
    ):

        if not running:

            return


        seed_symbol(
            symbol
        )


        if i % 10 == 0:

            logger.info(
                "Seed progress: %d/%d",
                i,
                len(symbols)
            )


        time.sleep(
            STARTUP_REST_DELAY
        )


    logger.info(
        "RSI history completed."
    )


# ============================================================
# DECIMAL HELPERS
# ============================================================

def decimal_floor(
    value,
    step
):

    value = Decimal(
        str(value)
    )


    step = Decimal(
        str(step)
    )


    if step <= 0:

        return value


    return (
        value / step
    ).to_integral_value(
        rounding=ROUND_DOWN
    ) * step


def decimal_string(value):

    return format(
        Decimal(value),
        "f"
    )


# ============================================================
# QUANTITY
# ============================================================

def get_sell_quantity(
    symbol,
    quantity
):

    info = symbol_filters.get(
        symbol
    )


    if not info:

        return Decimal("0")


    filters = info[
        "filters"
    ]


    market_filter = filters.get(
        "MARKET_LOT_SIZE"
    )


    lot_filter = filters.get(
        "LOT_SIZE"
    )


    selected = (
        market_filter
        or
        lot_filter
    )


    if not selected:

        return Decimal("0")


    step_size = Decimal(
        selected.get(
            "stepSize",
            "0"
        )
    )


    min_qty = Decimal(
        selected.get(
            "minQty",
            "0"
        )
    )


    qty = Decimal(
        str(quantity)
    )


    qty *= SELL_BALANCE_BUFFER


    qty = decimal_floor(
        qty,
        step_size
    )


    if qty < min_qty:

        return Decimal("0")


    return qty


# ============================================================
# STOP PRICE
# ============================================================

def get_stop_price(
    symbol,
    entry_price
):

    info = symbol_filters.get(
        symbol
    )


    if not info:

        return None


    filters = info[
        "filters"
    ]


    price_filter = filters.get(
        "PRICE_FILTER"
    )


    if not price_filter:

        return None


    tick_size = Decimal(
        price_filter.get(
            "tickSize",
            "0"
        )
    )


    if tick_size <= 0:

        return None


    entry = Decimal(
        str(entry_price)
    )


    raw_stop = (
        entry
        *
        (
            Decimal("1")
            -
            STOP_LOSS_PERCENT
        )
    )


    stop_price = decimal_floor(
        raw_stop,
        tick_size
    )


    return stop_price


# ============================================================
# ACCOUNT BALANCE
# ============================================================

def get_account_balances():

    data = binance_request(
        "GET",
        "/api/v3/account",
        signed=True
    )


    if not data:

        return {}


    balances = {}


    for item in data.get(
        "balances",
        []
    ):

        asset = item.get(
            "asset"
        )


        try:

            free = Decimal(
                str(
                    item.get(
                        "free",
                        "0"
                    )
                )
            )


            locked = Decimal(
                str(
                    item.get(
                        "locked",
                        "0"
                    )
                )
            )

        except Exception:

            continue


        total = (
            free
            +
            locked
        )


        if total > 0:

            balances[
                asset
            ] = {

                "free":
                    free,

                "locked":
                    locked,

                "total":
                    total
            }


    return balances


# ============================================================
# CURRENT PRICE
# ============================================================

def get_current_price(
    symbol
):

    data = binance_request(
        "GET",
        "/api/v3/ticker/price",
        params={
            "symbol":
                symbol
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
# FIND RECENT BOT BUY
# ============================================================

def find_recent_bot_buy(
    symbol
):

    start_time = int(
        (
            time.time()
            -
            RECOVERY_LOOKBACK_HOURS
            * 3600
        )
        * 1000
    )


    orders = binance_request(
        "GET",
        "/api/v3/allOrders",
        params={

            "symbol":
                symbol,

            "startTime":
                start_time,

            "limit":
                RECOVERY_ORDER_LIMIT

        },
        signed=True
    )


    if not orders:

        return None


    orders = sorted(
        orders,
        key=lambda x: int(
            x.get(
                "time",
                0
            )
        ),
        reverse=True
    )


    for order in orders:

        client_id = str(
            order.get(
                "clientOrderId",
                ""
            )
        )


        if not client_id.startswith(
            BOT_BUY_PREFIX
        ):

            continue


        if order.get(
            "side"
        ) != "BUY":

            continue


        if order.get(
            "status"
        ) != "FILLED":

            continue


        executed_qty = Decimal(
            str(
                order.get(
                    "executedQty",
                    "0"
                )
            )
        )


        quote_qty = Decimal(
            str(
                order.get(
                    "cummulativeQuoteQty",
                    "0"
                )
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

            entry_price = Decimal(
                str(
                    order.get(
                        "price",
                        "0"
                    )
                )
            )


        return {

            "orderId":
                order.get(
                    "orderId"
                ),

            "clientOrderId":
                client_id,

            "qty":
                executed_qty,

            "entry_price":
                entry_price,

            "time":
                int(
                    order.get(
                        "time",
                        0
                    )
                )
        }


    return None


# ============================================================
# FIND OPEN STOP LOSS
# ============================================================

def find_open_stop_loss(
    symbol
):

    orders = binance_request(
        "GET",
        "/api/v3/openOrders",
        params={
            "symbol":
                symbol
        },
        signed=True
    )


    if not orders:

        return None


    for order in orders:

        client_id = str(
            order.get(
                "clientOrderId",
                ""
            )
        )


        if not client_id.startswith(
            BOT_SL_PREFIX
        ):

            continue


        if order.get(
            "side"
        ) != "SELL":

            continue


        if order.get(
            "type"
        ) != "STOP_LOSS":

            continue


        return order


    return None


# ============================================================
# PLACE SERVER-SIDE STOP LOSS
# ============================================================

def place_stop_loss(
    symbol,
    quantity,
    entry_price
):

    try:

        qty = get_sell_quantity(
            symbol,
            quantity
        )


        if qty <= 0:

            logger.error(
                "%s | SL quantity invalid",
                symbol
            )

            return None


        stop_price = get_stop_price(
            symbol,
            entry_price
        )


        if stop_price is None:

            logger.error(
                "%s | SL price calculation failed",
                symbol
            )

            return None


        current_price = (
            get_current_price(
                symbol
            )
        )


        if current_price is not None:

            # If price already moved below/equal to SL,
            # do not attempt an invalid STOP_LOSS order.
            if current_price <= stop_price:

                logger.warning(
                    "%s | price already at/below SL | "
                    "current=%s | stop=%s | MARKET SELL",
                    symbol,
                    current_price,
                    stop_price
                )

                return "MARKET_SELL_REQUIRED"


        client_id = (
            BOT_SL_PREFIX
            +
            symbol[:8]
            +
            "_"
            +
            str(
                int(
                    time.time() * 1000
                )
            )[-10:]
        )


        logger.warning(
            "%s | placing SERVER-SIDE SL | "
            "entry=%s | SL=%s | qty=%s",
            symbol,
            entry_price,
            stop_price,
            qty
        )


        result = binance_request(
            "POST",
            "/api/v3/order",
            params={

                "symbol":
                    symbol,

                "side":
                    "SELL",

                "type":
                    "STOP_LOSS",

                "quantity":
                    decimal_string(
                        qty
                    ),

                "stopPrice":
                    decimal_string(
                        stop_price
                    ),

                "newClientOrderId":
                    client_id,

                "newOrderRespType":
                    "RESULT"

            },
            signed=True
        )


        if not result:

            logger.error(
                "%s | SERVER SL placement failed",
                symbol
            )

            return None


        logger.warning(
            "%s | SERVER SL ACTIVE | "
            "stop=%s | orderId=%s",
            symbol,
            stop_price,
            result.get(
                "orderId"
            )
        )


        return result


    except Exception as e:

        logger.exception(
            "%s | SL placement exception: %s",
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

        logger.info(
            "%s | cancelling SL order %s",
            symbol,
            order_id
        )


        result = binance_request(
            "DELETE",
            "/api/v3/order",
            params={

                "symbol":
                    symbol,

                "orderId":
                    order_id

            },
            signed=True
        )


        if result:

            logger.info(
                "%s | SL cancelled",
                symbol
            )

            return True


        # It may already have been filled.
        # Check order status.
        status = get_order(
            symbol,
            order_id
        )


        if status:

            if status.get(
                "status"
            ) in (
                "FILLED",
                "CANCELED",
                "EXPIRED"
            ):

                return (
                    status.get(
                        "status"
                    ) != "FILLED"
                )


        return False


    except Exception as e:

        logger.exception(
            "%s | cancel SL error: %s",
            symbol,
            e
        )

        return False


# ============================================================
# GET ORDER
# ============================================================

def get_order(
    symbol,
    order_id
):

    return binance_request(
        "GET",
        "/api/v3/order",
        params={

            "symbol":
                symbol,

            "orderId":
                order_id

        },
        signed=True
    )


# ============================================================
# POSITION RECOVERY
# ============================================================

def recover_positions():

    logger.info(
        "=" * 70
    )

    logger.info(
        "STARTING POSITION RECOVERY..."
    )

    logger.info(
        "=" * 70
    )


    balances = get_account_balances()


    if not balances:

        logger.warning(
            "Could not read account balance."
        )

        return


    candidates = []


    for symbol in symbols:

        info = symbol_filters.get(
            symbol
        )


        if not info:

            continue


        base_asset = info[
            "baseAsset"
        ]


        balance = balances.get(
            base_asset
        )


        if not balance:

            continue


        quantity = balance[
            "total"
        ]


        if quantity <= 0:

            continue


        candidates.append(
            (
                symbol,
                base_asset,
                quantity
            )
        )


    logger.info(
        "Recovery candidates: %d",
        len(candidates)
    )


    recovered = 0


    for (
        symbol,
        base_asset,
        quantity
    ) in candidates:

        if not running:

            return


        try:

            bot_buy = (
                find_recent_bot_buy(
                    symbol
                )
            )


            if not bot_buy:

                logger.info(
                    "%s | balance exists but "
                    "no RSIBUY order | NOT recovered",
                    symbol
                )

                continue


            actual_quantity = quantity


            entry_price = (
                bot_buy[
                    "entry_price"
                ]
            )


            stop_price = get_stop_price(
                symbol,
                entry_price
            )


            with state_lock:

                positions[
                    symbol
                ] = {

                    "qty":
                        actual_quantity,

                    "entry_price":
                        entry_price,

                    "stop_price":
                        stop_price,

                    "buy_order_id":
                        bot_buy[
                            "orderId"
                        ],

                    "stop_order_id":
                        None,

                    "buy_time":
                        bot_buy[
                            "time"
                        ],

                    "recovered":
                        True
                }


            recovered += 1


            logger.warning(
                "%s | POSITION RECOVERED | "
                "qty=%s | entry=%s | SL=%s",
                symbol,
                actual_quantity,
                entry_price,
                stop_price
            )


            # ------------------------------------------------
            # Check existing server-side SL
            # ------------------------------------------------

            existing_sl = (
                find_open_stop_loss(
                    symbol
                )
            )


            if existing_sl:

                with state_lock:

                    positions[
                        symbol
                    ][
                        "stop_order_id"
                    ] = existing_sl.get(
                        "orderId"
                    )


                logger.info(
                    "%s | existing server SL recovered | "
                    "orderId=%s",
                    symbol,
                    existing_sl.get(
                        "orderId"
                    )
                )


            else:

                # Recreate missing SL.
                sl_result = place_stop_loss(
                    symbol,
                    actual_quantity,
                    entry_price
                )


                if sl_result == (
                    "MARKET_SELL_REQUIRED"
                ):

                    logger.warning(
                        "%s | recovered position already "
                        "below SL | MARKET SELL",
                        symbol
                    )

                    submit_sell(
                        symbol
                    )


                elif sl_result:

                    with state_lock:

                        positions[
                            symbol
                        ][
                            "stop_order_id"
                        ] = sl_result.get(
                            "orderId"
                        )


        except Exception as e:

            logger.error(
                "%s | recovery error: %s",
                symbol,
                e
            )


        time.sleep(
            STARTUP_REST_DELAY
        )


    logger.info(
        "POSITION RECOVERY COMPLETE | "
        "recovered=%d",
        recovered
    )


# ============================================================
# BUY
# ============================================================

def place_buy(symbol):

    with state_lock:

        if symbol in positions:

            logger.info(
                "%s | BUY ignored | "
                "position already exists",
                symbol
            )

            return


        if symbol in orders_in_flight:

            return


        orders_in_flight.add(
            symbol
        )


    try:

        client_id = (
            BOT_BUY_PREFIX
            +
            symbol[:8]
            +
            "_"
            +
            str(
                int(
                    time.time() * 1000
                )
            )[-10:]
        )


        logger.warning(
            "%s | MARKET BUY | %s USDT",
            symbol,
            BUY_USDT
        )


        result = binance_request(
            "POST",
            "/api/v3/order",
            params={

                "symbol":
                    symbol,

                "side":
                    "BUY",

                "type":
                    "MARKET",

                "quoteOrderQty":
                    str(
                        BUY_USDT
                    ),

                "newClientOrderId":
                    client_id,

                "newOrderRespType":
                    "FULL"

            },
            signed=True
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


        executed_qty = Decimal(
            str(
                result.get(
                    "executedQty",
                    "0"
                )
            )
        )


        quote_qty = Decimal(
            str(
                result.get(
                    "cummulativeQuoteQty",
                    "0"
                )
            )
        )


        if executed_qty <= 0:

            logger.error(
                "%s | BUY zero quantity",
                symbol
            )

            return


        # Average actual entry price.
        if quote_qty > 0:

            entry_price = (
                quote_qty
                /
                executed_qty
            )

        else:

            entry_price = Decimal(
                str(
                    result.get(
                        "price",
                        "0"
                    )
                )
            )


        stop_price = get_stop_price(
            symbol,
            entry_price
        )


        if stop_price is None:

            logger.error(
                "%s | could not calculate SL",
                symbol
            )

            return


        # ----------------------------------------------------
        # Save position BEFORE SL request.
        # ----------------------------------------------------

        with state_lock:

            positions[
                symbol
            ] = {

                "qty":
                    executed_qty,

                "entry_price":
                    entry_price,

                "stop_price":
                    stop_price,

                "buy_order_id":
                    result.get(
                        "orderId"
                    ),

                "stop_order_id":
                    None,

                "buy_time":
                    int(
                        time.time() * 1000
                    ),

                "recovered":
                    False
            }


        logger.warning(
            "%s | BUY FILLED | "
            "qty=%s | entry=%s | SL=%s",
            symbol,
            executed_qty,
            entry_price,
            stop_price
        )


        # ----------------------------------------------------
        # Immediately place server-side SL.
        # ----------------------------------------------------

        sl_result = place_stop_loss(
            symbol,
            executed_qty,
            entry_price
        )


        if sl_result == (
            "MARKET_SELL_REQUIRED"
        ):

            logger.warning(
                "%s | price already reached SL | "
                "MARKET SELL",
                symbol
            )

            submit_sell(
                symbol
            )

            return


        if sl_result:

            with state_lock:

                if symbol in positions:

                    positions[
                        symbol
                    ][
                        "stop_order_id"
                    ] = sl_result.get(
                        "orderId"
                    )


        else:

            # If server-side SL could not be placed,
            # do not leave the position unprotected.
            logger.error(
                "%s | SERVER SL FAILED | "
                "attempting emergency MARKET SELL",
                symbol
            )

            submit_sell(
                symbol
            )


    except Exception as e:

        logger.exception(
            "%s | BUY exception: %s",
            symbol,
            e
        )


    finally:

        with state_lock:

            orders_in_flight.discard(
                symbol
            )


# ============================================================
# SELL
# ============================================================

def place_sell(symbol):

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

        # ----------------------------------------------------
        # FIRST CANCEL SERVER STOP LOSS
        # ----------------------------------------------------

        stop_order_id = (
            position.get(
                "stop_order_id"
            )
        )


        if stop_order_id:

            cancel_ok = cancel_stop_loss(
                symbol,
                stop_order_id
            )


            if not cancel_ok:

                # Check whether SL has already filled.
                sl_status = get_order(
                    symbol,
                    stop_order_id
                )


                if sl_status:

                    if sl_status.get(
                        "status"
                    ) == "FILLED":

                        logger.warning(
                            "%s | SL already FILLED. "
                            "No RSI SELL required.",
                            symbol
                        )


                        with state_lock:

                            positions.pop(
                                symbol,
                                None
                            )


                        return


                logger.error(
                    "%s | could not safely cancel SL. "
                    "Aborting RSI market sell.",
                    symbol
                )

                return


        # ----------------------------------------------------
        # GET CURRENT ACCOUNT BALANCE
        # ----------------------------------------------------

        balances = (
            get_account_balances()
        )


        info = symbol_filters.get(
            symbol
        )


        if not info:

            logger.error(
                "%s | symbol info unavailable",
                symbol
            )

            return


        base_asset = info[
            "baseAsset"
        ]


        account_balance = balances.get(
            base_asset
        )


        if not account_balance:

            logger.warning(
                "%s | no account balance. "
                "Position already gone.",
                symbol
            )

            with state_lock:

                positions.pop(
                    symbol,
                    None
                )

            return


        actual_qty = (
            account_balance[
                "total"
            ]
        )


        if actual_qty <= 0:

            with state_lock:

                positions.pop(
                    symbol,
                    None
                )

            return


        requested_qty = min(
            Decimal(
                str(
                    position[
                        "qty"
                    ]
                )
            ),
            actual_qty
        )


        sell_qty = get_sell_quantity(
            symbol,
            requested_qty
        )


        if sell_qty <= 0:

            logger.error(
                "%s | invalid SELL quantity",
                symbol
            )

            return


        client_id = (
            BOT_SELL_PREFIX
            +
            symbol[:8]
            +
            "_"
            +
            str(
                int(
                    time.time() * 1000
                )
            )[-10:]
        )


        logger.warning(
            "%s | RSI MARKET SELL | qty=%s",
            symbol,
            sell_qty
        )


        result = binance_request(
            "POST",
            "/api/v3/order",
            params={

                "symbol":
                    symbol,

                "side":
                    "SELL",

                "type":
                    "MARKET",

                "quantity":
                    decimal_string(
                        sell_qty
                    ),

                "newClientOrderId":
                    client_id,

                "newOrderRespType":
                    "FULL"

            },
            signed=True
        )


        if not result:

            logger.error(
                "%s | RSI SELL failed",
                symbol
            )

            # Important:
            # If market sell failed, recreate SL.
            recreate_sl(
                symbol
            )

            return


        status = result.get(
            "status"
        )


        sold_qty = Decimal(
            str(
                result.get(
                    "executedQty",
                    "0"
                )
            )
        )


        logger.warning(
            "%s | SELL result | "
            "status=%s | sold=%s",
            symbol,
            status,
            sold_qty
        )


        if status == "FILLED":

            with state_lock:

                positions.pop(
                    symbol,
                    None
                )


            logger.warning(
                "%s | POSITION CLOSED 100%%",
                symbol
            )


        elif status == (
            "PARTIALLY_FILLED"
        ):

            remaining = (
                requested_qty
                -
                sold_qty
            )


            if remaining > 0:

                with state_lock:

                    positions[
                        symbol
                    ] = {

                        **position,

                        "qty":
                            remaining,

                        "stop_order_id":
                            None
                    }


                # Recreate SL for remaining quantity.
                recreate_sl(
                    symbol
                )

            else:

                with state_lock:

                    positions.pop(
                        symbol,
                        None
                    )


        else:

            # Order failed/rejected.
            # Recreate SL.
            recreate_sl(
                symbol
            )


    except Exception as e:

        logger.exception(
            "%s | SELL exception: %s",
            symbol,
            e
        )

        recreate_sl(
            symbol
        )


    finally:

        with state_lock:

            orders_in_flight.discard(
                symbol
            )


# ============================================================
# RECREATE SL
# ============================================================

def recreate_sl(symbol):

    try:

        with state_lock:

            position = positions.get(
                symbol
            )


        if not position:

            return


        result = place_stop_loss(
            symbol,
            position[
                "qty"
            ],
            position[
                "entry_price"
            ]
        )


        if result and result != (
            "MARKET_SELL_REQUIRED"
        ):

            with state_lock:

                if symbol in positions:

                    positions[
                        symbol
                    ][
                        "stop_order_id"
                    ] = result.get(
                        "orderId"
                    )


        elif result == (
            "MARKET_SELL_REQUIRED"
        ):

            logger.warning(
                "%s | recreated SL already hit | "
                "emergency MARKET SELL",
                symbol
            )

            submit_sell(
                symbol
            )


    except Exception as e:

        logger.exception(
            "%s | recreate SL error: %s",
            symbol,
            e
        )


# ============================================================
# ORDER SUBMISSION
# ============================================================

def submit_buy(symbol):

    order_executor.submit(
        place_buy,
        symbol
    )


def submit_sell(symbol):

    order_executor.submit(
        place_sell,
        symbol
    )


# ============================================================
# CLOSED CANDLE PROCESSING
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


        with state_lock:

            state = indicator_state.get(
                symbol
            )


            if not state:

                return


            if state.get(
                "last_closed_time"
            ) == close_time:

                return


            previous_rsi3 = state.get(
                "rsi3"
            )


            closes = list(
                state.get(
                    "closes",
                    []
                )
            )


        closes.append(
            close_price
        )


        closes = closes[
            -HISTORY_LIMIT:
        ]


        series = pd.Series(
            closes,
            dtype="float64"
        )


        current_rsi3 = calculate_rsi(
            series,
            RSI_FAST_PERIOD
        )


        current_rsi50 = calculate_rsi(
            series,
            RSI_SLOW_PERIOD
        )


        if (
            current_rsi3 is None
            or
            current_rsi50 is None
        ):

            return


        with state_lock:

            indicator_state[
                symbol
            ] = {

                "closes":
                    closes,

                "previous_rsi3":
                    previous_rsi3,

                "rsi3":
                    current_rsi3,

                "rsi50":
                    current_rsi50,

                "last_closed_time":
                    close_time
            }


            has_position = (
                symbol in positions
            )


        logger.info(
            "%s | 5m CLOSED | "
            "RSI50=%.2f | RSI3=%.2f",
            symbol,
            current_rsi50,
            current_rsi3
        )


        # ====================================================
        # SELL
        # ====================================================

        if has_position:

            crossed_above_80 = (

                previous_rsi3
                is not None

                and

                previous_rsi3
                <=
                SELL_RSI_LEVEL

                and

                current_rsi3
                >
                SELL_RSI_LEVEL
            )


            if crossed_above_80:

                logger.warning(
                    "%s | RSI SELL SIGNAL | "
                    "RSI3 %.2f -> %.2f",
                    symbol,
                    previous_rsi3,
                    current_rsi3
                )


                submit_sell(
                    symbol
                )


                return


        # ====================================================
        # BUY
        # ====================================================

        if not has_position:

            buy_condition = (

                current_rsi50
                >
                BUY_RSI_SLOW_MIN

                and

                current_rsi3
                <
                BUY_RSI_FAST_MAX
            )


            if buy_condition:

                logger.warning(
                    "%s | BUY SIGNAL | "
                    "RSI50=%.2f | RSI3=%.2f",
                    symbol,
                    current_rsi50,
                    current_rsi3
                )


                submit_buy(
                    symbol
                )


    except Exception as e:

        logger.exception(
            "%s | candle processing error: %s",
            symbol,
            e
        )


# ============================================================
# WEBSOCKET
# ============================================================

def websocket_worker(
    group_id,
    group_symbols
):

    if not group_symbols:

        return


    streams = "/".join(

        f"{symbol.lower()}@kline_{TIMEFRAME}"

        for symbol in group_symbols
    )


    ws_url = (
        WS_BASE
        +
        streams
    )


    reconnect_delay = (
        RECONNECT_MIN
    )


    while running:

        connection_start = (
            time.time()
        )


        logger.info(
            "WS GROUP %d | connecting | "
            "%d symbols",
            group_id,
            len(group_symbols)
        )


        def on_open(ws):

            nonlocal reconnect_delay

            reconnect_delay = (
                RECONNECT_MIN
            )


            logger.info(
                "WS GROUP %d | CONNECTED",
                group_id
            )


        def on_message(
            ws,
            message
        ):

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

                    return


                kline = data.get(
                    "k",
                    {}
                )


                symbol = kline.get(
                    "s"
                )


                if not symbol:

                    return


                # Only CLOSED candle.
                if kline.get(
                    "x"
                ) is True:

                    process_closed_candle(
                        symbol,
                        kline
                    )


            except Exception as e:

                logger.error(
                    "WS GROUP %d | message error: %s",
                    group_id,
                    e
                )


        def on_error(
            ws,
            error
        ):

            logger.warning(
                "WS GROUP %d | error: %s",
                group_id,
                error
            )


        def on_close(
            ws,
            close_status_code,
            close_msg
        ):

            logger.warning(
                "WS GROUP %d | closed | "
                "code=%s | msg=%s",
                group_id,
                close_status_code,
                close_msg
            )


        try:

            ws = websocket.WebSocketApp(
                ws_url,
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

            logger.exception(
                "WS GROUP %d | exception: %s",
                group_id,
                e
            )


        if not running:

            break


        age = (
            time.time()
            -
            connection_start
        )


        if age >= (
            WS_MAX_LIFETIME
        ):

            logger.info(
                "WS GROUP %d | planned reconnect",
                group_id
            )


        wait = (
            reconnect_delay
            +
            random.uniform(
                0.5,
                2.0
            )
        )


        time.sleep(
            wait
        )


        reconnect_delay = min(
            reconnect_delay * 2,
            RECONNECT_MAX
        )


# ============================================================
# START WEBSOCKETS
# ============================================================

def start_websockets():

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


        groups.append(
            symbols[
                start:end
            ]
        )


    for group_id, group_symbols in enumerate(
        groups,
        start=1
    ):

        logger.info(
            "GROUP %d | %d symbols",
            group_id,
            len(group_symbols)
        )


        thread = threading.Thread(
            target=websocket_worker,
            args=(
                group_id,
                group_symbols
            ),
            daemon=True,
            name=f"WS-GROUP-{group_id}"
        )


        thread.start()


        ws_threads.append(
            thread
        )


        time.sleep(2)


# ============================================================
# SHUTDOWN
# ============================================================

def shutdown_handler(
    signum,
    frame
):

    global running

    logger.warning(
        "Shutdown signal received."
    )


    running = False


    try:

        order_executor.shutdown(
            wait=False,
            cancel_futures=False
        )

    except Exception:

        pass


signal.signal(
    signal.SIGINT,
    shutdown_handler
)


signal.signal(
    signal.SIGTERM,
    shutdown_handler
)


# ============================================================
# BOT START
# ============================================================

def start_bot():

    global symbols

    global symbol_filters


    logger.info(
        "=" * 70
    )

    logger.info(
        "STARTING TOP 150 RSI BOT"
    )

    logger.info(
        "=" * 70
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
        "Groups: %d x %d",
        GROUPS,
        SYMBOLS_PER_GROUP
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
        "STOP LOSS: %.2f%%",
        float(
            STOP_LOSS_PERCENT * 100
        )
    )


    logger.info(
        "SERVER-SIDE STOP LOSS: ENABLED"
    )


    # ========================================================
    # EXCHANGE INFO
    # ========================================================

    exchange_info = (
        load_exchange_info()
    )


    symbol_filters = (
        exchange_info
    )


    # ========================================================
    # TOP 150
    # ========================================================

    symbols = (
        get_top_150_symbols(
            exchange_info
        )
    )


    logger.info(
        "TOP 150 loaded."
    )


    logger.info(
        "First 20: %s",
        ", ".join(
            symbols[:20]
        )
    )


    # ========================================================
    # POSITION RECOVERY
    # ========================================================

    recover_positions()


    # ========================================================
    # RSI HISTORY
    # ========================================================

    seed_all_symbols()


    # ========================================================
    # WEBSOCKETS
    # ========================================================

    start_websockets()


    logger.info(
        "=" * 70
    )


    logger.info(
        "BOT IS LIVE"
    )


    logger.info(
        "SERVER-SIDE 1%% STOP LOSS IS ACTIVE"
    )


    logger.info(
        "=" * 70
    )


# ============================================================
# MAIN
# ============================================================

if __name__ == "__main__":

    bot_thread = threading.Thread(
        target=start_bot,
        daemon=True,
        name="BOT"
    )


    bot_thread.start()


    port = int(
        os.getenv(
            "PORT",
            "10000"
        )
    )


    logger.info(
        "Starting Flask on port %d",
        port
    )


    app.run(
        host="0.0.0.0",
        port=port,
        threaded=True
    )
