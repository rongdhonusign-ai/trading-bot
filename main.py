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
# API / RATE LIMIT SAFETY
# ============================================================

# Initial REST requests are intentionally slowed down.
INITIAL_KLINE_DELAY = 0.12

# Minimum delay between repeated REST safety checks.
SAFETY_CHECK_INTERVAL = 5.0

# WebSocket reconnect backoff.
WS_MIN_RECONNECT_DELAY = 5
WS_MAX_RECONNECT_DELAY = 120

# Top symbols refresh
TOP_SYMBOL_REFRESH_SECONDS = 1800


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
    "CHF"
}


# ============================================================
# EXCLUDED COINS
# ============================================================

EXCLUDED_BASES = {
    "BTC",
    "ETH"
}


# ============================================================
# LOGGING
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

log = logging.getLogger("BB_RSI3_VOLUME_BOT")


# ============================================================
# BINANCE CLIENT
# ============================================================

client = Client(
    API_KEY,
    API_SECRET
)


# ============================================================
# FLASK
# ============================================================

app = Flask(__name__)


@app.route("/")
def home():

    return jsonify({
        "status": "running",
        "bot": "BB20 + RSI3 + Volume Spot Bot",
        "timeframe": TIMEFRAME,

        "buy_rule": (
            "Close < BB20 Lower AND "
            "RSI3 < 10 AND "
            "Volume > SMA20 Volume x 1.20"
        ),

        "sell_rule": "Upper Bollinger Band Touch",

        "stop_loss": (
            f"{STOP_LOSS_PCT * 100:.2f}%"
        ),

        "trade_amount": TRADE_AMOUNT_USDT
    })


@app.route("/health")
def health():

    return jsonify({
        "status": "healthy",
        "timestamp": int(time.time())
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
        threaded=True
    )


# ============================================================
# GLOBAL STATE
# ============================================================

symbol_info = {}

top_symbols = []

candles = {}

positions = {}

# Current live prices from WebSocket
live_prices = {}

# Prevent duplicate SELL
selling_symbols = set()

# Prevent duplicate BUY
buying_symbols = set()

state_lock = threading.RLock()

last_top_symbol_update = 0

# Last REST safety check time
last_safety_check = {}


# ============================================================
# BINANCE API ERROR HANDLER
# ============================================================

def handle_api_error(
    error,
    context=""
):

    code = getattr(
        error,
        "code",
        None
    )

    message = str(error)

    # --------------------------------------------------------
    # RATE LIMIT
    # --------------------------------------------------------

    if code == -1003 or "Too many requests" in message:

        log.error(
            "BINANCE RATE LIMIT → %s | %s",
            context,
            message
        )

        # Binance may provide Retry-After.
        retry_after = getattr(
            error,
            "retry_after",
            None
        )

        if retry_after:
            try:
                wait_time = float(
                    retry_after
                )
            except Exception:
                wait_time = 60
        else:
            wait_time = 60

        wait_time = min(
            max(wait_time, 30),
            300
        )

        log.warning(
            "RATE LIMIT SAFETY WAIT → %.1f seconds",
            wait_time
        )

        time.sleep(
            wait_time
        )

        return True

    # --------------------------------------------------------
    # IP BAN
    # --------------------------------------------------------

    if code == -1003 and (
        "IP banned" in message
        or "banned" in message.lower()
    ):

        log.critical(
            "BINANCE IP BAN DETECTED → %s",
            message
        )

        time.sleep(
            300
        )

        return True

    return False


# ============================================================
# LOAD EXCHANGE INFORMATION
# ============================================================

def load_exchange_info():

    global symbol_info

    log.info(
        "Loading Binance exchange information..."
    )

    try:

        info = client.get_exchange_info()

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

                "base": base,

                "quote": "USDT",

                "step_size": step_size,

                "min_qty": min_qty,

                "max_qty": max_qty,

                "market_step_size":
                    market_step_size,

                "market_min_qty":
                    market_min_qty,

                "market_max_qty":
                    market_max_qty,

                "tick_size": float(
                    price_filter.get(
                        "tickSize",
                        0
                    )
                )
                if price_filter
                else 0.000001,

                "min_notional":
                    min_notional
            }

        symbol_info = temp

        log.info(
            "Loaded %s eligible USDT ALT symbols",
            len(symbol_info)
        )

    except BinanceAPIException as e:

        handle_api_error(
            e,
            "load_exchange_info"
        )

        raise

    except Exception as e:

        log.exception(
            "Exchange information error: %s",
            e
        )

        raise


# ============================================================
# TOP SYMBOLS
# ============================================================

def update_top_symbols():

    global top_symbols
    global last_top_symbol_update

    try:

        log.info(
            "Updating top ALT symbols..."
        )

        tickers = client.get_ticker()

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

        last_top_symbol_update = time.time()

        log.info(
            "Selected %s ALT/USDT symbols",
            len(selected)
        )

        log.info(
            "First symbols: %s",
            selected[:15]
        )

    except BinanceAPIException as e:

        handle_api_error(
            e,
            "update_top_symbols"
        )

        log.error(
            "Top symbol update failed: %s",
            e
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

    # MARKET LOT SIZE
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

    # LOT SIZE
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

    # When loss is zero RSI should be 100.
    rsi = rsi.where(
        avg_loss != 0,
        100
    )

    return rsi


# ============================================================
# CALCULATE INDICATORS
# ============================================================

def calculate_indicators(df):

    if len(df) < max(
        BB_PERIOD,
        RSI_PERIOD,
        VOLUME_SMA_PERIOD
    ) + 5:

        return None

    df = df.copy()

    # --------------------------------------------------------
    # Make sure numeric columns are actually numeric.
    # Prevents:
    # "No numeric types to aggregate"
    # --------------------------------------------------------

    numeric_columns = [
        "open",
        "high",
        "low",
        "close",
        "volume"
    ]

    for column in numeric_columns:

        df[column] = pd.to_numeric(
            df[column],
            errors="coerce"
        )

    df = df.dropna(
        subset=numeric_columns
    )

    if len(df) < max(
        BB_PERIOD,
        RSI_PERIOD,
        VOLUME_SMA_PERIOD
    ) + 2:

        return None

    close = df["close"]

    # --------------------------------------------------------
    # BOLLINGER BAND 20,2
    # --------------------------------------------------------

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

    # --------------------------------------------------------
    # RSI 3
    # --------------------------------------------------------

    rsi3 = calculate_rsi(
        close,
        RSI_PERIOD
    )

    # --------------------------------------------------------
    # VOLUME SMA 20
    # --------------------------------------------------------

    volume_sma = df["volume"].rolling(
        VOLUME_SMA_PERIOD,
        min_periods=VOLUME_SMA_PERIOD
    ).mean()

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
        "volume_sma20"
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

    # ========================================================
    # USER'S NEW STRATEGY
    # ========================================================

    # 1. Close below BB20 Lower
    condition_1 = (
        close_price < lower_bb
    )

    # 2. RSI3 < 10
    condition_2 = (
        rsi3 < RSI_LIMIT
    )

    # 3. Volume > SMA20 Volume x 1.20
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
        "Loading initial 5m candle data for %s symbols...",
        len(symbols)
    )

    for index, symbol in enumerate(
        symbols
    ):

        try:

            klines = client.get_klines(

                symbol=symbol,

                interval=Client.KLINE_INTERVAL_5MINUTE,

                limit=100
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
                        int(k[6])
                })

            df = pd.DataFrame(
                rows
            )

            # Remove currently open candle.
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

            # ------------------------------------------------
            # RATE LIMIT SAFETY
            # ------------------------------------------------

            time.sleep(
                INITIAL_KLINE_DELAY
            )

        except BinanceAPIException as e:

            if handle_api_error(
                e,
                f"initial kline {symbol}"
            ):

                time.sleep(
                    5
                )

            log.warning(
                "Kline error %s: %s",
                symbol,
                e
            )

        except Exception as e:

            log.warning(
                "Initial data error %s: %s",
                symbol,
                e
            )

            time.sleep(
                0.1
            )

        if (
            index + 1
        ) % 25 == 0:

            log.info(
                "Initial candles progress → %s/%s",
                index + 1,
                len(symbols)
            )

    log.info(
        "Initial candle loading complete: %s symbols",
        len(candles)
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

        order = client.create_order(

            symbol=symbol,

            side=Client.SIDE_BUY,

            type=Client.ORDER_TYPE_MARKET,

            quoteOrderQty=TRADE_AMOUNT_USDT
        )

        executed_qty = float(
            order.get(
                "executedQty",
                0
            )
        )

        if executed_qty <= 0:

            log.error(
                "BUY returned zero quantity: %s",
                order
            )

            return

        fills = order.get(
            "fills",
            []
        )

        total_cost = 0.0

        if fills:

            for f in fills:

                total_cost += (
                    float(f["price"])
                    *
                    float(f["qty"])
                )

        if total_cost > 0:

            entry_price = (
                total_cost /
                executed_qty
            )

        else:

            entry_price = float(
                client.get_symbol_ticker(
                    symbol=symbol
                )["price"]
            )

        # ----------------------------------------------------
        # Verify actual balance
        # ----------------------------------------------------

        info = symbol_info.get(
            symbol
        )

        actual_balance = executed_qty

        if info:

            try:

                balance = client.get_asset_balance(
                    asset=info["base"]
                )

                if balance:

                    free_balance = float(
                        balance["free"]
                    )

                    actual_balance = min(
                        executed_qty,
                        free_balance
                    )

            except Exception as e:

                log.warning(
                    "Could not verify post-buy balance %s: %s",
                    symbol,
                    e
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
                    order["orderId"],

                "buy_time":
                    time.time()
            }

        log.info(
            "BUY FILLED → %s | qty=%.12f | entry=%.12f | RSI3/BB/VOL signal",
            symbol,
            actual_balance,
            entry_price
        )

    except BinanceAPIException as e:

        handle_api_error(
            e,
            f"BUY {symbol}"
        )

        log.error(
            "BUY Binance error %s: %s",
            symbol,
            e
        )

    except Exception as e:

        log.exception(
            "BUY error %s: %s",
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

            log.info(
                "SELL already in progress → %s",
                symbol
            )

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

        asset = info["base"]

        # ----------------------------------------------------
        # REAL BALANCE
        # ----------------------------------------------------

        balance = client.get_asset_balance(
            asset=asset
        )

        if not balance:

            log.error(
                "Balance not found → %s",
                asset
            )

            return

        free_balance = float(
            balance["free"]
        )

        stored_quantity = float(
            position["quantity"]
        )

        if free_balance <= 0:

            log.error(
                "No free balance → %s",
                symbol
            )

            return

        quantity_source = min(
            stored_quantity,
            free_balance
        )

        quantity = (
            quantity_source *
            SELL_BALANCE_BUFFER
        )

        quantity = get_valid_sell_quantity(
            symbol,
            quantity
        )

        if quantity <= 0:

            log.error(
                "Valid SELL quantity is zero → %s",
                symbol
            )

            return

        # ----------------------------------------------------
        # NOTIONAL CHECK
        # ----------------------------------------------------

        try:

            ticker = client.get_symbol_ticker(
                symbol=symbol
            )

            current_price = float(
                ticker["price"]
            )

            notional = (
                current_price *
                quantity
            )

            min_notional = float(
                info.get(
                    "min_notional",
                    0
                )
            )

            if (
                min_notional > 0
                and
                notional < min_notional
            ):

                log.error(
                    "SELL notional too small → %s | %.8f < %.8f",
                    symbol,
                    notional,
                    min_notional
                )

                return

        except BinanceAPIException as e:

            handle_api_error(
                e,
                f"SELL ticker {symbol}"
            )

        except Exception as e:

            log.warning(
                "Notional check failed %s: %s",
                symbol,
                e
            )

        log.warning(
            "SELL SIGNAL → %s | reason=%s | qty=%.12f",
            symbol,
            reason,
            quantity
        )

        # ----------------------------------------------------
        # MARKET SELL
        # ----------------------------------------------------

        order = client.create_order(

            symbol=symbol,

            side=Client.SIDE_SELL,

            type=Client.ORDER_TYPE_MARKET,

            quantity=quantity
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

    except BinanceAPIException as e:

        handle_api_error(
            e,
            f"SELL {symbol}"
        )

        log.error(
            "SELL Binance error %s: %s",
            symbol,
            e
        )

    except Exception as e:

        log.exception(
            "SELL error %s: %s",
            symbol,
            e
        )

    finally:

        with state_lock:

            selling_symbols.discard(
                symbol
            )


# ============================================================
# CHECK POSITION
# ============================================================

def check_position(
    symbol,
    current_price,
    upper_band
):

    with state_lock:

        position = positions.get(
            symbol
        )

        if not position:
            return

        if symbol in selling_symbols:
            return

    entry_price = float(
        position["entry_price"]
    )

    stop_price = (
        entry_price *
        (1.0 - STOP_LOSS_PCT)
    )

    # ========================================================
    # STOP LOSS
    # ========================================================

    if current_price <= stop_price:

        sell_symbol(
            symbol,
            f"STOP LOSS {STOP_LOSS_PCT * 100:.2f}%"
        )

        return

    # ========================================================
    # UPPER BB SELL
    # ========================================================

    if upper_band is not None:

        if current_price >= upper_band:

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

        # Only KLINE stream is necessary
        # for strategy calculation.
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

            process_kline(
                data
            )

    except Exception as e:

        log.warning(
            "WebSocket message error: %s",
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

    candle_closed = k["x"]

    # Only CLOSED candle is used for BUY.
    if not candle_closed:
        return

    try:

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
                int(k["T"])
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

        # ----------------------------------------------------
        # Calculate indicators
        # ----------------------------------------------------

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

        # ====================================================
        # BUY
        # ====================================================

        if not already_in_position:

            if entry_signal(df):

                last = df.iloc[-1]

                log.info(
                    "ENTRY CONFIRMED → %s | close=%.8f | BBLOWER=%.8f | RSI3=%.2f | VOL=%.2f | VOL_SMA20=%.2f",
                    symbol,
                    float(last["close"]),
                    float(last["bb_lower"]),
                    float(last["rsi3"]),
                    float(last["volume"]),
                    float(last["volume_sma20"])
                )

                buy_symbol(
                    symbol
                )

    except Exception as e:

        log.exception(
            "Kline processing error %s: %s",
            symbol,
            e
        )


# ============================================================
# WEBSOCKET LOOP
# ============================================================

def websocket_loop():

    reconnect_delay = (
        WS_MIN_RECONNECT_DELAY
    )

    while True:

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

            ws = websocket.WebSocketApp(

                url,

                on_message=lambda ws, msg:
                    process_ws_message(msg),

                on_error=lambda ws, error:
                    log.error(
                        "WebSocket error: %s",
                        error
                    ),

                on_close=lambda ws, code, msg:
                    log.warning(
                        "WebSocket closed → code=%s msg=%s",
                        code,
                        msg
                    ),

                on_open=lambda ws:
                    log.info(
                        "WebSocket connected successfully"
                    )
            )

            ws.run_forever(

                ping_interval=30,

                ping_timeout=20,

                ping_payload="ping"
            )

            # If connection survives,
            # reset reconnect delay.
            reconnect_delay = (
                WS_MIN_RECONNECT_DELAY
            )

        except Exception as e:

            log.exception(
                "WebSocket loop error: %s",
                e
            )

        # ----------------------------------------------------
        # Exponential reconnect backoff
        # Prevents connection storm / IP pressure.
        # ----------------------------------------------------

        jitter = random.uniform(
            0,
            3
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

    log.info(
        "Checking existing Binance balances..."
    )

    try:

        account = client.get_account()

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

            try:

                ticker = client.get_symbol_ticker(
                    symbol=symbol
                )

                current_price = float(
                    ticker["price"]
                )

            except BinanceAPIException as e:

                handle_api_error(
                    e,
                    f"recovery ticker {symbol}"
                )

                continue

            except Exception:

                continue

            value = (
                free *
                current_price
            )

            # Ignore dust
            if value < 5.0:
                continue

            # ------------------------------------------------
            # IMPORTANT
            # After Render restart, exact entry price is
            # unknown.
            #
            # Therefore current price is used.
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
                            True
                    }

                    recovered += 1

            log.warning(
                "RECOVERED BALANCE → %s | qty=%.12f | price=%.12f | value=%.2f",
                symbol,
                free,
                current_price,
                value
            )

            # Small delay between recovery requests
            time.sleep(
                0.1
            )

        log.info(
            "Position recovery complete → %s positions recovered",
            recovered
        )

    except BinanceAPIException as e:

        handle_api_error(
            e,
            "recover_positions"
        )

        log.exception(
            "Position recovery API error: %s",
            e
        )

    except Exception as e:

        log.exception(
            "Position recovery error: %s",
            e
        )


# ============================================================
# POSITION SAFETY MONITOR
# ============================================================

def position_safety_loop():

    """
    Backup protection.

    WebSocket is the primary price/exit mechanism.

    REST is used only as a backup at a controlled interval.
    """

    while True:

        try:

            with state_lock:

                current_positions = list(
                    positions.items()
                )

            for symbol, position in current_positions:

                with state_lock:

                    if symbol in selling_symbols:
                        continue

                # ------------------------------------------------
                # REST safety request throttling
                # ------------------------------------------------

                now = time.time()

                last_check = last_safety_check.get(
                    symbol,
                    0
                )

                if (
                    now -
                    last_check
                    <
                    SAFETY_CHECK_INTERVAL
                ):

                    continue

                last_safety_check[
                    symbol
                ] = now

                try:

                    ticker = client.get_symbol_ticker(
                        symbol=symbol
                    )

                    price = float(
                        ticker["price"]
                    )

                    with state_lock:

                        df = candles.get(
                            symbol
                        )

                    upper = None

                    if (
                        df is not None
                        and
                        len(df) > 0
                    ):

                        try:

                            last = df.iloc[-1]

                            if not pd.isna(
                                last["bb_upper"]
                            ):

                                upper = float(
                                    last["bb_upper"]
                                )

                        except Exception:

                            pass

                    check_position(
                        symbol,
                        price,
                        upper
                    )

                except BinanceAPIException as e:

                    handle_api_error(
                        e,
                        f"safety {symbol}"
                    )

                    log.warning(
                        "Safety API error %s: %s",
                        symbol,
                        e
                    )

                except Exception as e:

                    log.warning(
                        "Safety check failed %s: %s",
                        symbol,
                        e
                    )

            # ----------------------------------------------------
            # Safety loop intentionally slow.
            # ----------------------------------------------------

            time.sleep(
                2
            )

        except Exception as e:

            log.exception(
                "Safety monitor error: %s",
                e
            )

            time.sleep(
                5
            )


# ============================================================
# SYMBOL REFRESH LOOP
# ============================================================

def symbol_refresh_loop():

    while True:

        try:

            update_top_symbols()

        except Exception as e:

            log.exception(
                "Symbol refresh error: %s",
                e
            )

        time.sleep(
            TOP_SYMBOL_REFRESH_SECONDS
        )


# ============================================================
# START BOT
# ============================================================

def start_bot():

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
        "BUY → CLOSE < BB20 LOWER + RSI3 < 10 + VOLUME > SMA20 x 1.20"
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
        "=" * 75
    )

    # --------------------------------------------------------
    # EXCHANGE INFO
    # --------------------------------------------------------

    load_exchange_info()

    # --------------------------------------------------------
    # TOP SYMBOLS
    # --------------------------------------------------------

    update_top_symbols()

    # --------------------------------------------------------
    # INITIAL CANDLES
    # --------------------------------------------------------

    load_initial_candles()

    # --------------------------------------------------------
    # POSITION RECOVERY
    # --------------------------------------------------------

    recover_positions()

    # --------------------------------------------------------
    # FLASK
    # --------------------------------------------------------

    threading.Thread(
        target=run_flask,
        daemon=True
    ).start()

    # --------------------------------------------------------
    # WEBSOCKET
    # --------------------------------------------------------

    threading.Thread(
        target=websocket_loop,
        daemon=True
    ).start()

    # --------------------------------------------------------
    # SYMBOL REFRESH
    # --------------------------------------------------------

    threading.Thread(
        target=symbol_refresh_loop,
        daemon=True
    ).start()

    # --------------------------------------------------------
    # SAFETY MONITOR
    # --------------------------------------------------------

    threading.Thread(
        target=position_safety_loop,
        daemon=True
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
# MAIN
# ============================================================

if __name__ == "__main__":

    start_bot()
