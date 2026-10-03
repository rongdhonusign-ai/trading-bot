import os
import time
import json
import threading
import logging
import random
from decimal import Decimal, ROUND_DOWN

import pandas as pd
import websocket

from flask import Flask, jsonify
from binance.client import Client
from binance.exceptions import BinanceAPIException


# ============================================================
# CONFIG
# ============================================================

API_KEY = os.environ.get("BINANCE_API_KEY")
API_SECRET = os.environ.get("BINANCE_API_SECRET")

if not API_KEY or not API_SECRET:
    raise RuntimeError(
        "BINANCE_API_KEY / BINANCE_API_SECRET missing"
    )


# ============================================================
# TRADING SETTINGS
# ============================================================

TRADE_AMOUNT_USDT = 35.0

TIMEFRAME = "5m"

# Bollinger Band
BB_PERIOD = 20
BB_STD = 2.0

# RSI
RSI_PERIOD = 3
RSI_LIMIT = 10.0

# Volume
VOLUME_SMA_PERIOD = 20
VOLUME_MULTIPLIER = 1.20

# Stop Loss = 1%
STOP_LOSS_PCT = 0.010

# Top ALT/USDT pairs
TOP_SYMBOLS = 150

# Sell balance safety buffer
SELL_BALANCE_BUFFER = 0.999


# ============================================================
# REST SAFETY SETTINGS
# ============================================================

# IMPORTANT:
# Market monitoring DOES NOT use REST.
#
# REST is used only for:
#   1. exchange info
#   2. top-symbol selection
#   3. initial historical candles
#   4. BUY orders
#   5. SELL orders
#   6. one-time balance recovery
#
# All repeated price monitoring is WebSocket based.

REST_MIN_INTERVAL = 0.30

INITIAL_KLINE_DELAY = 0.30

TOP_SYMBOL_REFRESH_SECONDS = 1800

# Do not aggressively retry Binance API errors.
REST_MAX_RETRIES = 2

# WebSocket reconnect
WS_MIN_RECONNECT_DELAY = 10
WS_MAX_RECONNECT_DELAY = 300

# Force reconnect before Binance's 24h connection lifetime.
WS_MAX_CONNECTION_SECONDS = 21 * 60 * 60


# ============================================================
# STABLECOINS
# ============================================================

STABLECOINS = {
    "USDT",
    "USDC",
    "FDUSD",
    "BUSD",
    "TUSD",
    "DAI",
    "USDP",
    "PYUSD",
    "USDE",
    "USDS",

    "EUR",
    "GBP",
    "TRY",
    "BRL",
    "ARS",
    "UAH",
    "RUB",
    "BIDR",
    "IDRT",
    "NGN",
    "PLN",
    "RON",
    "ZAR",
    "AED",
    "AUD",
    "JPY",
    "SEK",
    "DKK",
    "NOK",
    "CHF",
}


# ============================================================
# EXCLUDED BASE COINS
# ============================================================

EXCLUDED_BASES = {
    "BTC",
    "ETH",
}


# ============================================================
# LOGGING
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

log = logging.getLogger(
    "BB_RSI3_VOLUME_BOT"
)


# ============================================================
# BINANCE CLIENT
# ============================================================

# ping=False is IMPORTANT.
#
# python-binance normally performs ping() during Client()
# initialization. Your previous Render error happened exactly
# there because Binance had already banned the Render IP.
#
# We therefore disable the automatic startup ping.
#
# This does NOT bypass Binance limits. It only prevents one
# unnecessary startup REST request.
try:

    client = Client(
        API_KEY,
        API_SECRET,
        requests_params={
            "timeout": 20
        },
        ping=False,
    )

except TypeError:

    # Compatibility fallback for older python-binance versions
    client = Client(
        API_KEY,
        API_SECRET,
        requests_params={
            "timeout": 20
        },
    )


# ============================================================
# FLASK
# ============================================================

app = Flask(__name__)


@app.route("/")
def home():

    with state_lock:

        position_count = len(
            positions
        )

        symbol_count = len(
            top_symbols
        )

    return jsonify({

        "status": "running",

        "bot":
            "BB20 + RSI3 + Volume Spot Bot",

        "timeframe":
            TIMEFRAME,

        "buy_rule":
            "Close < BB20 Lower AND "
            "RSI3 < 10 AND "
            "Volume > SMA20 Volume x 1.20",

        "sell_rule":
            "Upper Bollinger Band Touch",

        "stop_loss":
            f"{STOP_LOSS_PCT * 100:.2f}%",

        "trade_amount":
            TRADE_AMOUNT_USDT,

        "top_symbols":
            symbol_count,

        "open_positions":
            position_count,

        "market_data":
            "WebSocket",

        "rest_price_polling":
            False,
    })


@app.route("/health")
def health():

    with state_lock:

        ws_status = (
            websocket_connected
        )

        symbol_count = len(
            top_symbols
        )

        position_count = len(
            positions
        )

    return jsonify({

        "status": "healthy",

        "timestamp":
            int(time.time()),

        "websocket":
            ws_status,

        "symbols":
            symbol_count,

        "positions":
            position_count,
    })


def run_flask():

    port = int(
        os.environ.get(
            "PORT",
            10000
        )
    )

    app.run(
        host="0.0.0.0",
        port=port,
        threaded=True,
        use_reloader=False,
    )


# ============================================================
# GLOBAL STATE
# ============================================================

symbol_info = {}

top_symbols = []

candles = {}

positions = {}

# Current price from WebSocket
live_prices = {}

# Prevent duplicate SELL
selling_symbols = set()

# Prevent duplicate BUY
buying_symbols = set()

state_lock = threading.RLock()

last_top_symbol_update = 0

# WebSocket state
websocket_connected = False

websocket_last_message_time = 0

websocket_started_at = 0

websocket_stop_event = threading.Event()

# Current WebSocket object
active_ws = None


# ============================================================
# REST RATE LIMITER
# ============================================================

rest_lock = threading.Lock()

last_rest_call_time = 0.0


def rest_wait():

    global last_rest_call_time

    with rest_lock:

        now = time.time()

        elapsed = (
            now -
            last_rest_call_time
        )

        if elapsed < REST_MIN_INTERVAL:

            time.sleep(
                REST_MIN_INTERVAL -
                elapsed
            )

        last_rest_call_time = time.time()


def get_retry_after(error):

    try:

        response = getattr(
            error,
            "response",
            None
        )

        if response is not None:

            value = response.headers.get(
                "Retry-After"
            )

            if value:

                return float(value)

    except Exception:

        pass

    return None


def safe_rest_call(
    function,
    *args,
    context="REST",
    **kwargs
):

    """
    Central REST gateway.

    Every REST request passes through here.

    This prevents many different threads from hammering
    Binance simultaneously.
    """

    for attempt in range(
        REST_MAX_RETRIES + 1
    ):

        rest_wait()

        try:

            result = function(
                *args,
                **kwargs
            )

            # python-binance stores latest response here.
            try:

                response = getattr(
                    client,
                    "response",
                    None
                )

                if response is not None:

                    used_weight = (
                        response.headers.get(
                            "X-MBX-USED-WEIGHT-1M"
                        )
                        or
                        response.headers.get(
                            "x-mbx-used-weight-1m"
                        )
                    )

                    if used_weight:

                        log.debug(
                            "REST weight → %s | %s",
                            context,
                            used_weight
                        )

            except Exception:

                pass

            return result

        except BinanceAPIException as e:

            code = getattr(
                e,
                "code",
                None
            )

            status = getattr(
                e,
                "status_code",
                None
            )

            message = str(e)

            # ------------------------------------------------
            # IP BAN / 418
            # ------------------------------------------------

            if (
                code == -1003
                or status == 418
                or "IP banned" in message
                or "Way too much request weight" in message
            ):

                retry_after = (
                    get_retry_after(e)
                )

                if retry_after is None:

                    retry_after = 300

                # Do NOT repeatedly hammer a banned IP.
                wait_time = max(
                    float(retry_after) + 5,
                    60
                )

                log.critical(
                    "BINANCE RATE/IP BAN → %s",
                    message
                )

                log.critical(
                    "REST PAUSED FOR %.1f SECONDS",
                    wait_time
                )

                time.sleep(
                    wait_time
                )

                if attempt >= REST_MAX_RETRIES:

                    raise

                continue

            # ------------------------------------------------
            # 429
            # ------------------------------------------------

            if status == 429:

                retry_after = (
                    get_retry_after(e)
                )

                if retry_after is None:

                    retry_after = (
                        30 *
                        (attempt + 1)
                    )

                wait_time = max(
                    float(retry_after),
                    5
                )

                log.warning(
                    "Binance 429 → %s | wait %.1fs",
                    context,
                    wait_time
                )

                time.sleep(
                    wait_time
                )

                continue

            log.error(
                "Binance API error → %s | %s",
                context,
                message
            )

            raise

        except Exception as e:

            if attempt >= REST_MAX_RETRIES:

                raise

            wait_time = (
                2 ** attempt
            )

            log.warning(
                "REST error → %s | retry in %ss | %s",
                context,
                wait_time,
                e
            )

            time.sleep(
                wait_time
            )

    return None


# ============================================================
# LOAD EXCHANGE INFORMATION
# ============================================================

def load_exchange_info():

    global symbol_info

    log.info(
        "Loading Binance exchange information..."
    )

    info = safe_rest_call(
        client.get_exchange_info,
        context="exchange_info"
    )

    temp = {}

    for s in info["symbols"]:

        symbol = s["symbol"]

        if s["status"] != "TRADING":
            continue

        if s["quoteAsset"] != "USDT":
            continue

        base = s["baseAsset"]

        if base in STABLECOINS:
            continue

        if base in EXCLUDED_BASES:
            continue

        if s.get(
            "isSpotTradingAllowed"
        ) is not True:
            continue

        filters = {
            f["filterType"]: f
            for f in s["filters"]
        }

        lot_filter = filters.get(
            "LOT_SIZE"
        )

        market_lot_filter = filters.get(
            "MARKET_LOT_SIZE"
        )

        price_filter = filters.get(
            "PRICE_FILTER"
        )

        min_notional_filter = filters.get(
            "MIN_NOTIONAL"
        )

        notional_filter = filters.get(
            "NOTIONAL"
        )

        step_size = 0.000001
        min_qty = 0.0
        max_qty = 0.0

        if lot_filter:

            step_size = float(
                lot_filter.get(
                    "stepSize",
                    0
                )
            )

            min_qty = float(
                lot_filter.get(
                    "minQty",
                    0
                )
            )

            max_qty = float(
                lot_filter.get(
                    "maxQty",
                    0
                )
            )

        market_step_size = 0.0
        market_min_qty = 0.0
        market_max_qty = 0.0

        if market_lot_filter:

            market_step_size = float(
                market_lot_filter.get(
                    "stepSize",
                    0
                )
            )

            market_min_qty = float(
                market_lot_filter.get(
                    "minQty",
                    0
                )
            )

            market_max_qty = float(
                market_lot_filter.get(
                    "maxQty",
                    0
                )
            )

        min_notional = 0.0

        if min_notional_filter:

            min_notional = float(
                min_notional_filter.get(
                    "minNotional",
                    0
                )
            )

        if notional_filter:

            min_notional = max(
                min_notional,
                float(
                    notional_filter.get(
                        "minNotional",
                        0
                    )
                )
            )

        temp[symbol] = {

            "base":
                base,

            "quote":
                "USDT",

            "step_size":
                step_size,

            "min_qty":
                min_qty,

            "max_qty":
                max_qty,

            "market_step_size":
                market_step_size,

            "market_min_qty":
                market_min_qty,

            "market_max_qty":
                market_max_qty,

            "tick_size":
                float(
                    price_filter.get(
                        "tickSize",
                        0
                    )
                )
                if price_filter
                else 0.000001,

            "min_notional":
                min_notional,
        }

    symbol_info = temp

    log.info(
        "Loaded %s eligible USDT ALT symbols",
        len(symbol_info)
    )


# ============================================================
# TOP SYMBOLS
# ============================================================

def update_top_symbols():

    global top_symbols
    global last_top_symbol_update

    log.info(
        "Updating top ALT symbols..."
    )

    try:

        tickers = safe_rest_call(
            client.get_ticker,
            context="all_24h_ticker"
        )

        candidates = []

        for t in tickers:

            symbol = t["symbol"]

            if symbol not in symbol_info:
                continue

            try:

                quote_volume = float(
                    t["quoteVolume"]
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
            for symbol, volume
            in candidates[:TOP_SYMBOLS]
        ]

        with state_lock:

            top_symbols = selected

        last_top_symbol_update = (
            time.time()
        )

        log.info(
            "Selected %s ALT/USDT symbols",
            len(selected)
        )

        log.info(
            "First symbols → %s",
            selected[:15]
        )

    except Exception as e:

        log.exception(
            "Top symbol update failed: %s",
            e
        )


# ============================================================
# DECIMAL QUANTITY ROUNDING
# ============================================================

def round_step_quantity(
    quantity,
    step_size
):

    if quantity <= 0:
        return 0.0

    if step_size <= 0:
        return quantity

    qty = Decimal(
        str(quantity)
    )

    step = Decimal(
        str(step_size)
    )

    rounded = (
        qty / step
    ).to_integral_value(
        rounding=ROUND_DOWN
    ) * step

    return float(
        rounded
    )


# ============================================================
# VALID MARKET SELL QUANTITY
# ============================================================

def get_valid_sell_quantity(
    symbol,
    free_balance
):

    info = symbol_info.get(
        symbol
    )

    if not info:
        return 0.0

    quantity = (
        free_balance *
        SELL_BALANCE_BUFFER
    )

    market_step = info.get(
        "market_step_size",
        0
    )

    market_min = info.get(
        "market_min_qty",
        0
    )

    market_max = info.get(
        "market_max_qty",
        0
    )

    lot_step = info.get(
        "step_size",
        0
    )

    lot_min = info.get(
        "min_qty",
        0
    )

    lot_max = info.get(
        "max_qty",
        0
    )

    if market_step > 0:

        quantity = round_step_quantity(
            quantity,
            market_step
        )

        if (
            market_min > 0
            and quantity < market_min
        ):
            return 0.0

        if market_max > 0:

            quantity = min(
                quantity,
                market_max
            )

    if lot_step > 0:

        quantity = round_step_quantity(
            quantity,
            lot_step
        )

        if (
            lot_min > 0
            and quantity < lot_min
        ):
            return 0.0

        if lot_max > 0:

            quantity = min(
                quantity,
                lot_max
            )

    return quantity


# ============================================================
# RSI 3
# ============================================================

def calculate_rsi(
    series,
    period=3
):

    delta = series.diff()

    gain = delta.clip(
        lower=0
    )

    loss = -delta.clip(
        upper=0
    )

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

    rs = (
        avg_gain /
        avg_loss.replace(
            0,
            float("nan")
        )
    )

    rsi = 100 - (
        100 / (1 + rs)
    )

    rsi = rsi.where(
        avg_loss != 0,
        100
    )

    return rsi


# ============================================================
# CALCULATE INDICATORS
# ============================================================

def calculate_indicators(df):

    minimum = max(
        BB_PERIOD,
        RSI_PERIOD,
        VOLUME_SMA_PERIOD
    ) + 5

    if len(df) < minimum:
        return None

    df = df.copy()

    numeric_columns = [
        "open",
        "high",
        "low",
        "close",
        "volume",
    ]

    for column in numeric_columns:

        df[column] = pd.to_numeric(
            df[column],
            errors="coerce"
        )

    df = df.dropna(
        subset=numeric_columns
    )

    if len(df) < minimum:
        return None

    close = df["close"]

    middle = close.rolling(
        BB_PERIOD,
        min_periods=BB_PERIOD
    ).mean()

    std = close.rolling(
        BB_PERIOD,
        min_periods=BB_PERIOD
    ).std(
        ddof=0
    )

    upper = (
        middle +
        BB_STD * std
    )

    lower = (
        middle -
        BB_STD * std
    )

    rsi3 = calculate_rsi(
        close,
        RSI_PERIOD
    )

    volume_sma = (
        df["volume"]
        .rolling(
            VOLUME_SMA_PERIOD,
            min_periods=VOLUME_SMA_PERIOD
        )
        .mean()
    )

    df["bb_middle"] = middle
    df["bb_upper"] = upper
    df["bb_lower"] = lower
    df["rsi3"] = rsi3
    df["volume_sma20"] = volume_sma

    return df


# ============================================================
# ENTRY SIGNAL
# ============================================================

def entry_signal(df):

    if df is None:
        return False

    if len(df) < 30:
        return False

    candle = df.iloc[-1]

    required = [
        "close",
        "bb_lower",
        "rsi3",
        "volume",
        "volume_sma20",
    ]

    for column in required:

        if pd.isna(
            candle[column]
        ):
            return False

    close_price = float(
        candle["close"]
    )

    lower_bb = float(
        candle["bb_lower"]
    )

    rsi3 = float(
        candle["rsi3"]
    )

    volume = float(
        candle["volume"]
    )

    volume_sma20 = float(
        candle["volume_sma20"]
    )

    if volume_sma20 <= 0:
        return False

    condition_1 = (
        close_price < lower_bb
    )

    condition_2 = (
        rsi3 < RSI_LIMIT
    )

    condition_3 = (
        volume >
        volume_sma20 *
        VOLUME_MULTIPLIER
    )

    return (
        condition_1
        and condition_2
        and condition_3
    )


# ============================================================
# INITIAL CANDLES
# ============================================================

def load_initial_candles():

    global candles

    with state_lock:

        symbols = list(
            top_symbols
        )

    log.info(
        "Loading initial 5m candles for %s symbols...",
        len(symbols)
    )

    loaded = 0

    for index, symbol in enumerate(
        symbols
    ):

        try:

            klines = safe_rest_call(

                client.get_klines,

                symbol=symbol,

                interval=(
                    Client.KLINE_INTERVAL_5MINUTE
                ),

                limit=100,

                context=f"initial_klines_{symbol}"
            )

            rows = []

            for k in klines:

                rows.append({

                    "open_time":
                        int(k[0]),

                    "open":
                        float(k[1]),

                    "high":
                        float(k[2]),

                    "low":
                        float(k[3]),

                    "close":
                        float(k[4]),

                    "volume":
                        float(k[5]),

                    "close_time":
                        int(k[6]),
                })

            df = pd.DataFrame(
                rows
            )

            if len(df) > 0:

                current_ms = int(
                    time.time() * 1000
                )

                df = df[
                    df["close_time"]
                    <= current_ms
                ]

            df = calculate_indicators(
                df
            )

            if df is not None:

                with state_lock:

                    candles[symbol] = df

                loaded += 1

            time.sleep(
                INITIAL_KLINE_DELAY
            )

        except Exception as e:

            log.warning(
                "Initial kline error %s → %s",
                symbol,
                e
            )

            time.sleep(
                1
            )

        if (
            index + 1
        ) % 25 == 0:

            log.info(
                "Initial candle progress → %s/%s",
                index + 1,
                len(symbols)
            )

    log.info(
        "Initial candle loading complete → %s/%s",
        loaded,
        len(symbols)
    )


# ============================================================
# BUY
# ============================================================

def buy_symbol(symbol):

    with state_lock:

        if symbol in positions:
            return

        if symbol in selling_symbols:
            return

        if symbol in buying_symbols:
            return

        buying_symbols.add(
            symbol
        )

    try:

        log.info(
            "BUY SIGNAL → %s | $%.2f",
            symbol,
            TRADE_AMOUNT_USDT
        )

        order = safe_rest_call(

            client.create_order,

            symbol=symbol,

            side=Client.SIDE_BUY,

            type=Client.ORDER_TYPE_MARKET,

            quoteOrderQty=TRADE_AMOUNT_USDT,

            context=f"BUY_{symbol}"
        )

        executed_qty = float(
            order.get(
                "executedQty",
                0
            )
        )

        if executed_qty <= 0:

            log.error(
                "BUY returned zero quantity → %s",
                symbol
            )

            return

        fills = order.get(
            "fills",
            []
        )

        total_cost = 0.0

        if fills:

            for fill in fills:

                total_cost += (
                    float(fill["price"])
                    *
                    float(fill["qty"])
                )

        if total_cost > 0:

            entry_price = (
                total_cost /
                executed_qty
            )

        else:

            # Do NOT make another REST ticker request.
            #
            # We already receive live price from WebSocket.
            with state_lock:

                entry_price = float(
                    live_prices.get(
                        symbol,
                        0
                    )
                )

            if entry_price <= 0:

                log.error(
                    "Cannot determine entry price → %s",
                    symbol
                )

                return

        # ----------------------------------------------------
        # IMPORTANT:
        # Do NOT call get_asset_balance() after every BUY.
        #
        # That was an unnecessary REST request.
        #
        # The executed quantity is used.
        # ----------------------------------------------------

        actual_balance = (
            executed_qty
        )

        with state_lock:

            positions[symbol] = {

                "symbol":
                    symbol,

                "quantity":
                    actual_balance,

                "entry_price":
                    entry_price,

                "buy_order_id":
                    order.get(
                        "orderId"
                    ),

                "buy_time":
                    time.time(),

                "recovered":
                    False,
            }

        stop_price = (
            entry_price *
            (1.0 - STOP_LOSS_PCT)
        )

        log.info(
            "BUY FILLED → %s | qty=%.12f | entry=%.12f | SL=%.12f",
            symbol,
            actual_balance,
            entry_price,
            stop_price
        )

    except Exception as e:

        log.exception(
            "BUY error %s → %s",
            symbol,
            e
        )

    finally:

        with state_lock:

            buying_symbols.discard(
                symbol
            )


# ============================================================
# MARKET SELL
# ============================================================

def sell_symbol(
    symbol,
    reason
):

    with state_lock:

        position = positions.get(
            symbol
        )

        if not position:
            return

        if symbol in selling_symbols:

            return

        selling_symbols.add(
            symbol
        )

    try:

        info = symbol_info.get(
            symbol
        )

        if not info:

            log.error(
                "Symbol info missing → %s",
                symbol
            )

            return

        # ----------------------------------------------------
        # IMPORTANT:
        #
        # We do NOT call get_asset_balance() here every time.
        #
        # The position quantity comes from the actual BUY
        # execution quantity.
        # ----------------------------------------------------

        stored_quantity = float(
            position["quantity"]
        )

        if stored_quantity <= 0:

            log.error(
                "Stored quantity invalid → %s",
                symbol
            )

            return

        quantity = get_valid_sell_quantity(
            symbol,
            stored_quantity
        )

        if quantity <= 0:

            log.error(
                "Valid SELL quantity is zero → %s",
                symbol
            )

            return

        # ----------------------------------------------------
        # MARKET SELL
        # ----------------------------------------------------

        log.warning(
            "SELL SIGNAL → %s | reason=%s | qty=%.12f",
            symbol,
            reason,
            quantity
        )

        order = safe_rest_call(

            client.create_order,

            symbol=symbol,

            side=Client.SIDE_SELL,

            type=Client.ORDER_TYPE_MARKET,

            quantity=quantity,

            context=f"SELL_{symbol}"
        )

        status = order.get(
            "status"
        )

        executed_qty = float(
            order.get(
                "executedQty",
                0
            )
        )

        order_id = order.get(
            "orderId"
        )

        log.warning(
            "SELL RESULT → %s | status=%s | executed=%.12f | order=%s",
            symbol,
            status,
            executed_qty,
            order_id
        )

        if status == "FILLED":

            with state_lock:

                positions.pop(
                    symbol,
                    None
                )

            log.warning(
                "POSITION CLOSED → %s | %s",
                symbol,
                reason
            )

        elif status == "PARTIALLY_FILLED":

            remaining = max(
                0.0,
                stored_quantity -
                executed_qty
            )

            with state_lock:

                if remaining > 0:

                    positions[symbol][
                        "quantity"
                    ] = remaining

                else:

                    positions.pop(
                        symbol,
                        None
                    )

            log.warning(
                "PARTIAL SELL → %s | remaining=%.12f",
                symbol,
                remaining
            )

        else:

            log.warning(
                "SELL not filled → %s | status=%s",
                symbol,
                status
            )

    except Exception as e:

        log.exception(
            "SELL error %s → %s",
            symbol,
            e
        )

    finally:

        with state_lock:

            selling_symbols.discard(
                symbol
            )


# ============================================================
# LIVE POSITION CHECK
# ============================================================

def check_position_live(
    symbol,
    current_price,
    candle_high,
    candle_low
):

    with state_lock:

        position = positions.get(
            symbol
        )

        if not position:
            return

        if symbol in selling_symbols:
            return

        df = candles.get(
            symbol
        )

    entry_price = float(
        position["entry_price"]
    )

    stop_price = (
        entry_price *
        (1.0 - STOP_LOSS_PCT)
    )

    # --------------------------------------------------------
    # STOP LOSS
    #
    # Use candle low as an additional trigger.
    # --------------------------------------------------------

    if (
        current_price <= stop_price
        or candle_low <= stop_price
    ):

        sell_symbol(
            symbol,
            f"STOP LOSS {STOP_LOSS_PCT * 100:.2f}%"
        )

        return

    # --------------------------------------------------------
    # UPPER BB
    #
    # Use the latest CLOSED candle's BB upper.
    #
    # If current candle price OR current candle high touches
    # the upper BB, sell.
    # --------------------------------------------------------

    upper_band = None

    if df is not None and len(df) > 0:

        try:

            last = df.iloc[-1]

            if not pd.isna(
                last["bb_upper"]
            ):

                upper_band = float(
                    last["bb_upper"]
                )

        except Exception:

            upper_band = None

    if upper_band is not None:

        if (
            current_price >= upper_band
            or candle_high >= upper_band
        ):

            sell_symbol(
                symbol,
                "UPPER BB TOUCH"
            )


# ============================================================
# WEBSOCKET URL
# ============================================================

def make_stream_url(
    symbols
):

    streams = []

    for symbol in symbols:

        streams.append(
            f"{symbol.lower()}@kline_5m"
        )

    stream_string = "/".join(
        streams
    )

    return (
        "wss://stream.binance.com:9443/"
        f"stream?streams={stream_string}"
    )


# ============================================================
# WEBSOCKET MESSAGE
# ============================================================

def process_ws_message(
    message
):

    global websocket_last_message_time

    try:

        msg = json.loads(
            message
        )

        data = msg.get(
            "data",
            {}
        )

        stream = msg.get(
            "stream",
            ""
        )

        if "@kline_5m" in stream:

            websocket_last_message_time = (
                time.time()
            )

            process_kline(
                data
            )

    except Exception as e:

        log.warning(
            "WebSocket message error → %s",
            e
        )


# ============================================================
# KLINE PROCESSING
# ============================================================

def process_kline(
    data
):

    k = data.get(
        "k"
    )

    if not k:
        return

    symbol = k["s"]

    try:

        current_price = float(
            k["c"]
        )

        candle_high = float(
            k["h"]
        )

        candle_low = float(
            k["l"]
        )

        live_prices[symbol] = (
            current_price
        )

        # ----------------------------------------------------
        # LIVE EXIT CHECK
        #
        # This is done BEFORE the closed-candle BUY check.
        #
        # Therefore open positions are monitored from the
        # WebSocket without REST ticker polling.
        # ----------------------------------------------------

        with state_lock:

            has_position = (
                symbol in positions
            )

        if has_position:

            check_position_live(
                symbol,
                current_price,
                candle_high,
                candle_low
            )

        # ----------------------------------------------------
        # BUY only on CLOSED candle
        # ----------------------------------------------------

        candle_closed = k["x"]

        if not candle_closed:
            return

        row = {

            "open_time":
                int(k["t"]),

            "open":
                float(k["o"]),

            "high":
                float(k["h"]),

            "low":
                float(k["l"]),

            "close":
                float(k["c"]),

            "volume":
                float(k["v"]),

            "close_time":
                int(k["T"]),
        }

        with state_lock:

            old_df = candles.get(
                symbol
            )

            if old_df is None:

                old_df = pd.DataFrame()

            new_row = pd.DataFrame(
                [row]
            )

            df = pd.concat(
                [
                    old_df,
                    new_row
                ],
                ignore_index=True
            )

            df = df.drop_duplicates(
                subset=[
                    "open_time"
                ],
                keep="last"
            )

            df = df.tail(
                100
            )

        df = calculate_indicators(
            df
        )

        if df is None:
            return

        with state_lock:

            candles[symbol] = df

            already_in_position = (
                symbol in positions
            )

        # ----------------------------------------------------
        # BUY
        # ----------------------------------------------------

        if not already_in_position:

            if entry_signal(df):

                last = df.iloc[-1]

                log.info(
                    "ENTRY CONFIRMED → %s | "
                    "close=%.8f | "
                    "BBLOWER=%.8f | "
                    "RSI3=%.2f | "
                    "VOL=%.2f | "
                    "VOL_SMA20=%.2f",

                    symbol,

                    float(
                        last["close"]
                    ),

                    float(
                        last["bb_lower"]
                    ),

                    float(
                        last["rsi3"]
                    ),

                    float(
                        last["volume"]
                    ),

                    float(
                        last["volume_sma20"]
                    )
                )

                # BUY runs in separate thread so one order
                # cannot block WebSocket processing.
                threading.Thread(
                    target=buy_symbol,
                    args=(symbol,),
                    daemon=True
                ).start()

    except Exception as e:

        log.exception(
            "Kline processing error %s → %s",
            symbol,
            e
        )


# ============================================================
# WEBSOCKET CALLBACKS
# ============================================================

def ws_on_open(ws):

    global websocket_connected
    global websocket_started_at

    websocket_connected = True

    websocket_started_at = (
        time.time()
    )

    log.info(
        "WebSocket connected → 150-symbol market stream"
    )


def ws_on_error(
    ws,
    error
):

    log.error(
        "WebSocket error → %s",
        error
    )


def ws_on_close(
    ws,
    close_status_code,
    close_msg
):

    global websocket_connected

    websocket_connected = False

    log.warning(
        "WebSocket closed → code=%s msg=%s",
        close_status_code,
        close_msg
    )


def ws_on_ping(
    ws,
    message
):

    # websocket-client automatically handles standard
    # WebSocket ping/pong frames.
    #
    # Do NOT send extra application-level ping messages.
    log.debug(
        "WebSocket ping received"
    )


# ============================================================
# WEBSOCKET LOOP
# ============================================================

def websocket_loop():

    global active_ws
    global websocket_connected

    reconnect_delay = (
        WS_MIN_RECONNECT_DELAY
    )

    while not websocket_stop_event.is_set():

        try:

            with state_lock:

                symbols = list(
                    top_symbols
                )

            if not symbols:

                log.warning(
                    "No symbols available for WebSocket"
                )

                time.sleep(
                    10
                )

                continue

            url = make_stream_url(
                symbols
            )

            log.info(
                "Opening WebSocket → %s symbols",
                len(symbols)
            )

            started = time.time()

            ws = websocket.WebSocketApp(

                url,

                on_open=ws_on_open,

                on_message=lambda ws, msg:
                    process_ws_message(msg),

                on_error=ws_on_error,

                on_close=ws_on_close,

                on_ping=ws_on_ping,
            )

            with state_lock:

                active_ws = ws

            # ------------------------------------------------
            # IMPORTANT
            #
            # We do NOT send application-level ping messages.
            #
            # Binance sends WebSocket ping frames and the
            # websocket-client library handles pong response.
            # ------------------------------------------------

            ws.run_forever(
                ping_interval=None,
                ping_timeout=None,
                skip_utf8_validation=True,
            )

            websocket_connected = False

            with state_lock:

                active_ws = None

            connection_lifetime = (
                time.time() -
                started
            )

            # If connection lasted reasonably long,
            # reconnect delay can be reset.
            if connection_lifetime > 300:

                reconnect_delay = (
                    WS_MIN_RECONNECT_DELAY
                )

        except Exception as e:

            websocket_connected = False

            log.exception(
                "WebSocket loop error → %s",
                e
            )

        finally:

            with state_lock:

                active_ws = None

                websocket_connected = False

        # ----------------------------------------------------
        # Exponential reconnect
        # ----------------------------------------------------

        jitter = random.uniform(
            0,
            5
        )

        wait_time = min(
            reconnect_delay + jitter,
            WS_MAX_RECONNECT_DELAY
        )

        log.warning(
            "WebSocket reconnect in %.1f seconds",
            wait_time
        )

        time.sleep(
            wait_time
        )

        reconnect_delay = min(
            reconnect_delay * 2,
            WS_MAX_RECONNECT_DELAY
        )


# ============================================================
# POSITION RECOVERY
# ============================================================

def recover_positions():

    """
    One-time recovery after Render restart.

    IMPORTANT:
    This performs REST account/balance calls only once at
    startup, not continuously.
    """

    log.info(
        "Checking existing Binance balances..."
    )

    try:

        account = safe_rest_call(
            client.get_account,
            context="startup_account_recovery"
        )

        balances = account.get(
            "balances",
            []
        )

        recovered = 0

        for b in balances:

            asset = b["asset"]

            if asset in STABLECOINS:
                continue

            free = float(
                b["free"]
            )

            locked = float(
                b["locked"]
            )

            total = (
                free +
                locked
            )

            if total <= 0:
                continue

            symbol = (
                asset +
                "USDT"
            )

            if symbol not in symbol_info:
                continue

            with state_lock:

                is_monitored = (
                    symbol in top_symbols
                )

            if not is_monitored:
                continue

            if free <= 0:
                continue

            # ------------------------------------------------
            # NO ticker REST request.
            #
            # Use latest WebSocket price if available.
            # During startup this may not exist yet.
            # ------------------------------------------------

            with state_lock:

                current_price = float(
                    live_prices.get(
                        symbol,
                        0
                    )
                )

            # If WebSocket hasn't provided a price yet,
            # skip recovery rather than making another REST
            # request for every asset.
            if current_price <= 0:

                log.warning(
                    "Recovery skipped until WebSocket price available → %s",
                    symbol
                )

                continue

            value = (
                free *
                current_price
            )

            if value < 5.0:
                continue

            # ------------------------------------------------
            # IMPORTANT LIMITATION:
            #
            # Exact historical entry price is not fetched here.
            #
            # Therefore current price is used as recovery
            # reference price.
            #
            # This is intentionally done to avoid many REST
            # requests after every Render restart.
            # ------------------------------------------------

            with state_lock:

                if symbol not in positions:

                    positions[symbol] = {

                        "symbol":
                            symbol,

                        "quantity":
                            free,

                        "entry_price":
                            current_price,

                        "buy_order_id":
                            None,

                        "buy_time":
                            time.time(),

                        "recovered":
                            True,
                    }

                    recovered += 1

            log.warning(
                "RECOVERED BALANCE → %s | qty=%.12f | "
                "reference_price=%.12f | value=%.2f",
                symbol,
                free,
                current_price,
                value
            )

        log.info(
            "Position recovery complete → %s positions",
            recovered
        )

    except Exception as e:

        log.exception(
            "Position recovery error → %s",
            e
        )


# ============================================================
# SYMBOL REFRESH LOOP
# ============================================================

def symbol_refresh_loop():

    while True:

        try:

            time.sleep(
                TOP_SYMBOL_REFRESH_SECONDS
            )

            old_symbols = set(
                top_symbols
            )

            update_top_symbols()

            with state_lock:

                new_symbols = set(
                    top_symbols
                )

            if old_symbols != new_symbols:

                log.info(
                    "Top symbols changed → WebSocket will refresh"
                )

                # Force WebSocket to reconnect with new list.
                with state_lock:

                    ws = active_ws

                if ws is not None:

                    try:

                        ws.close()

                    except Exception:

                        pass

        except Exception as e:

            log.exception(
                "Symbol refresh error → %s",
                e
            )


# ============================================================
# WEBSOCKET WATCHDOG
# ============================================================

def websocket_watchdog():

    """
    No REST requests.

    Only watches whether WebSocket is still receiving data.
    """

    while True:

        try:

            now = time.time()

            with state_lock:

                connected = (
                    websocket_connected
                )

                last_message = (
                    websocket_last_message_time
                )

                ws = active_ws

            # If connected but no data for 5 minutes,
            # restart the connection.
            if (
                connected
                and
                last_message > 0
                and
                now - last_message > 300
            ):

                log.warning(
                    "WebSocket appears stale → reconnecting"
                )

                if ws is not None:

                    try:

                        ws.close()

                    except Exception:

                        pass

            time.sleep(
                30
            )

        except Exception as e:

            log.warning(
                "WebSocket watchdog error → %s",
                e
            )

            time.sleep(
                30
            )


# ============================================================
# BOT INITIALIZATION
# ============================================================

def bot_worker():

    log.info(
        "=" * 75
    )

    log.info(
        "BB20 + RSI3 + VOLUME BINANCE SPOT BOT"
    )

    log.info(
        "=" * 75
    )

    log.info(
        "TIMEFRAME → 5m"
    )

    log.info(
        "BUY → CLOSE < BB20 LOWER + RSI3 < 10 + "
        "VOLUME > SMA20 x 1.20"
    )

    log.info(
        "SELL → UPPER BOLLINGER BAND TOUCH"
    )

    log.info(
        "STOP LOSS → %.2f%%",
        STOP_LOSS_PCT * 100
    )

    log.info(
        "TRADE AMOUNT → $%.2f",
        TRADE_AMOUNT_USDT
    )

    log.info(
        "TOP SYMBOLS → %s",
        TOP_SYMBOLS
    )

    log.info(
        "MARKET DATA → WEBSOCKET ONLY"
    )

    log.info(
        "REST PRICE POLLING → DISABLED"
    )

    log.info(
        "=" * 75
    )

    # --------------------------------------------------------
    # EXCHANGE INFO
    # --------------------------------------------------------

    while True:

        try:

            load_exchange_info()

            break

        except Exception as e:

            log.error(
                "Exchange info unavailable → %s",
                e
            )

            log.warning(
                "Initialization paused. Retrying later."
            )

            time.sleep(
                60
            )

    # --------------------------------------------------------
    # TOP SYMBOLS
    # --------------------------------------------------------

    while True:

        try:

            update_top_symbols()

            if top_symbols:

                break

        except Exception as e:

            log.error(
                "Top symbol initialization failed → %s",
                e
            )

        time.sleep(
            60
        )

    # --------------------------------------------------------
    # INITIAL CANDLES
    # --------------------------------------------------------

    load_initial_candles()

    # --------------------------------------------------------
    # START MARKET WEBSOCKET
    # --------------------------------------------------------

    threading.Thread(
        target=websocket_loop,
        daemon=True,
        name="BinanceMarketWebSocket"
    ).start()

    # --------------------------------------------------------
    # Wait for WebSocket prices before recovery
    # --------------------------------------------------------

    log.info(
        "Waiting for WebSocket market data..."
    )

    deadline = (
        time.time() +
        60
    )

    while (
        time.time() < deadline
    ):

        with state_lock:

            has_prices = (
                len(live_prices) > 0
            )

        if has_prices:

            break

        time.sleep(
            1
        )

    # --------------------------------------------------------
    # POSITION RECOVERY
    # --------------------------------------------------------

    recover_positions()

    # --------------------------------------------------------
    # SYMBOL REFRESH
    # --------------------------------------------------------

    threading.Thread(
        target=symbol_refresh_loop,
        daemon=True,
        name="SymbolRefresh"
    ).start()

    # --------------------------------------------------------
    # WEBSOCKET WATCHDOG
    # --------------------------------------------------------

    threading.Thread(
        target=websocket_watchdog,
        daemon=True,
        name="WebSocketWatchdog"
    ).start()

    log.info(
        "=" * 75
    )

    log.info(
        "BOT STARTED SUCCESSFULLY"
    )

    log.info(
        "=" * 75
    )

    while True:

        time.sleep(
            60
        )


# ============================================================
# START
# ============================================================

def start_application():

    # Flask starts immediately.
    # This means Render health checks can succeed even while
    # Binance initialization is waiting for a temporary
    # rate-limit/IP-ban period to expire.

    threading.Thread(
        target=run_flask,
        daemon=True,
        name="FlaskServer"
    ).start()

    threading.Thread(
        target=bot_worker,
        daemon=True,
        name="TradingBot"
    ).start()

    while True:

        time.sleep(
            60
        )


# ============================================================
# MAIN
# ============================================================

if __name__ == "__main__":

    start_application()
