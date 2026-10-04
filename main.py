import os
import time
import json
import math
import logging
import threading
from decimal import Decimal, ROUND_DOWN

import pandas as pd
import websocket

from flask import Flask, jsonify
from binance.client import Client
from binance.exceptions import BinanceAPIException


# ============================================================
# LOGGING
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

logger = logging.getLogger(__name__)


# ============================================================
# CONFIG
# ============================================================

API_KEY = os.environ.get("BINANCE_API_KEY")
API_SECRET = os.environ.get("BINANCE_API_SECRET")

if not API_KEY or not API_SECRET:
    logger.error("BINANCE_API_KEY / BINANCE_API_SECRET not found.")

TRADE_AMOUNT_USDT = float(os.environ.get("TRADE_AMOUNT_USDT", "35.0"))

TIMEFRAME = Client.KLINE_INTERVAL_5MINUTE

TOP_SYMBOLS = int(os.environ.get("TOP_SYMBOLS", "150"))

INITIAL_STOP_PERCENT = 1.0
SMA20_DROP_PERCENT = 1.0

BB_PERIOD = 20
BB_STD = 2.0

EMA_PERIOD = 5
SMA_PERIOD = 20


# ============================================================
# RATE LIMIT / RETRY SETTINGS
# ============================================================

# IMPORTANT:
# Do NOT repeatedly hammer Binance when IP is banned.

BAN_COOLDOWN_SECONDS = 900          # 15 minutes
REST_RETRY_SECONDS = 300            # 5 minutes
NORMAL_RETRY_SECONDS = 60

REST_REQUEST_DELAY = 0.35

KLINE_HISTORY = 50

REQUEST_TIMEOUT = 15


# ============================================================
# WEBSOCKET SETTINGS
# ============================================================

WS_BASE = "wss://stream.binance.com:9443"

WS_RECONNECT_SECONDS = 15

WS_PING_INTERVAL = 30
WS_PING_TIMEOUT = 20

SYMBOL_BATCH_SIZE = 50


# ============================================================
# STABLECOINS / FIAT
# ============================================================

EXCLUDED_BASE_ASSETS = {
    "BTC",
    "ETH",

    "USDT",
    "USDC",
    "FDUSD",
    "BUSD",
    "TUSD",
    "DAI",
    "USDP",
    "USDD",
    "PYUSD",
    "EUR",
    "GBP",
    "AUD",
    "BRL",
    "TRY",
    "RUB",
    "UAH",
    "NGN",
    "ARS",
    "PLN",
    "RON",
    "ZAR",
    "MXN",
    "JPY",
    "CAD",
    "CHF",
    "AED",
    "SAR"
}


# ============================================================
# GLOBAL STATE
# ============================================================

app = Flask(__name__)

client = None

exchange_info = None
symbol_info = {}

selected_symbols = []

# candle data
candle_data = {}

# websocket prices
live_prices = {}

# positions
positions = {}

selling_symbols = set()

# websocket states
ws_connections = {}

# locks
state_lock = threading.RLock()
positions_lock = threading.RLock()

# startup state
bot_initialized = False
initialization_running = False

last_binance_error = None
last_binance_success = None
last_rest_attempt = None

last_symbol_refresh = 0

# stop / shutdown
shutdown_event = threading.Event()


# ============================================================
# HELPER
# ============================================================

def now_ts():
    return time.time()


def utc_time_string(ts=None):
    if ts is None:
        ts = time.time()

    return time.strftime(
        "%Y-%m-%d %H:%M:%S UTC",
        time.gmtime(ts)
    )


# ============================================================
# BINANCE CLIENT
# ============================================================

def get_client():
    global client

    if client is not None:
        return client

    if not API_KEY or not API_SECRET:
        raise RuntimeError(
            "BINANCE_API_KEY / BINANCE_API_SECRET missing."
        )

    logger.info("Creating Binance client...")

    # IMPORTANT:
    # ping=False prevents python-binance from making
    # an automatic REST ping during import/startup.
    client = Client(
        API_KEY,
        API_SECRET,
        ping=False,
        requests_params={
            "timeout": REQUEST_TIMEOUT
        }
    )

    logger.info("Binance client created successfully.")

    return client


# ============================================================
# BINANCE REST CALL WRAPPER
# ============================================================

def is_rate_limit_error(exc):
    text = str(exc)

    if "-1003" in text:
        return True

    if "Way too much request weight" in text:
        return True

    if "IP banned" in text:
        return True

    if "Too many requests" in text:
        return True

    return False


def binance_rest_call(function, *args, **kwargs):
    """
    Controlled REST wrapper.

    IMPORTANT:
    If Binance returns -1003, do NOT repeatedly retry.
    """

    global last_binance_error
    global last_binance_success
    global last_rest_attempt

    last_rest_attempt = time.time()

    try:

        result = function(*args, **kwargs)

        last_binance_success = time.time()
        last_binance_error = None

        return result

    except BinanceAPIException as exc:

        last_binance_error = str(exc)

        if is_rate_limit_error(exc):

            logger.error(
                "BINANCE RATE LIMIT / IP BAN (-1003). "
                "REST request stopped."
            )

            logger.error(
                "Binance returned -1003. "
                "Long cooldown enabled."
            )

            return None

        logger.error(
            "Binance API error: %s",
            exc
        )

        return None

    except Exception as exc:

        last_binance_error = str(exc)

        logger.error(
            "Binance REST error: %s",
            exc
        )

        return None


# ============================================================
# EXCHANGE INFORMATION
# ============================================================

def load_exchange_info():

    global exchange_info
    global symbol_info

    logger.info(
        "Loading Binance exchange information..."
    )

    c = get_client()

    data = binance_rest_call(
        c.get_exchange_info
    )

    if data is None:
        logger.error(
            "Exchange information unavailable."
        )
        return False

    exchange_info = data

    temp = {}

    for s in data.get("symbols", []):

        symbol = s.get("symbol")

        if not symbol:
            continue

        if s.get("status") != "TRADING":
            continue

        if s.get("isSpotTradingAllowed") is False:
            continue

        temp[symbol] = s

    symbol_info = temp

    logger.info(
        "Exchange information loaded. "
        "Tradable symbols: %d",
        len(symbol_info)
    )

    return True


# ============================================================
# SYMBOL FILTER
# ============================================================

def is_valid_symbol(symbol):

    info = symbol_info.get(symbol)

    if not info:
        return False

    if info.get("status") != "TRADING":
        return False

    if info.get("quoteAsset") != "USDT":
        return False

    if info.get("isSpotTradingAllowed") is False:
        return False

    base_asset = info.get("baseAsset", "")

    if base_asset in EXCLUDED_BASE_ASSETS:
        return False

    return True


# ============================================================
# FILTER TOP SYMBOLS FROM WEBSOCKET TICKER
# ============================================================

def select_top_symbols_from_ticker(ticker_array):

    global selected_symbols

    candidates = []

    for item in ticker_array:

        try:

            symbol = item.get("s")

            if not symbol:
                continue

            if not is_valid_symbol(symbol):
                continue

            quote_volume = float(
                item.get("q", 0)
            )

            if not math.isfinite(quote_volume):
                continue

            if quote_volume <= 0:
                continue

            candidates.append(
                (
                    symbol,
                    quote_volume
                )
            )

        except Exception:
            continue

    candidates.sort(
        key=lambda x: x[1],
        reverse=True
    )

    new_symbols = [
        x[0]
        for x in candidates[:TOP_SYMBOLS]
    ]

    with state_lock:

        selected_symbols = new_symbols

    logger.info(
        "Top %d symbols selected from WebSocket ticker.",
        len(selected_symbols)
    )

    logger.info(
        "First symbols: %s",
        ", ".join(selected_symbols[:20])
    )

    return selected_symbols


# ============================================================
# 24H ALL MARKET TICKER WEBSOCKET
# ============================================================

def ticker_stream_worker():

    url = WS_BASE + "/ws/!ticker@arr"

    while not shutdown_event.is_set():

        try:

            logger.info(
                "Connecting Binance all-market ticker WebSocket..."
            )

            ws = websocket.WebSocketApp(
                url,
                on_open=lambda ws:
                    logger.info(
                        "All-market ticker WebSocket connected."
                    ),

                on_message=
                lambda ws, message:
                    handle_ticker_message(message),

                on_error=
                lambda ws, error:
                    logger.error(
                        "Ticker WebSocket error: %s",
                        error
                    ),

                on_close=
                lambda ws, code, msg:
                    logger.warning(
                        "Ticker WebSocket closed: %s %s",
                        code,
                        msg
                    )
            )

            ws.run_forever(
                ping_interval=WS_PING_INTERVAL,
                ping_timeout=WS_PING_TIMEOUT
            )

        except Exception as exc:

            logger.error(
                "Ticker WebSocket exception: %s",
                exc
            )

        if not shutdown_event.is_set():

            logger.info(
                "Ticker WebSocket reconnecting in %s seconds...",
                WS_RECONNECT_SECONDS
            )

            shutdown_event.wait(
                WS_RECONNECT_SECONDS
            )


def handle_ticker_message(message):

    global last_symbol_refresh

    try:

        data = json.loads(message)

        if not isinstance(data, list):
            return

        # Refresh symbol selection approximately once per hour.
        current = time.time()

        if (
            not selected_symbols
            or current - last_symbol_refresh >= 3600
        ):

            symbols = select_top_symbols_from_ticker(
                data
            )

            if symbols:

                last_symbol_refresh = current

                logger.info(
                    "WebSocket ticker selected %d symbols.",
                    len(symbols)
                )

    except Exception as exc:

        logger.error(
            "Ticker message processing error: %s",
            exc
        )


# ============================================================
# KLINE DATA
# ============================================================

def normalize_kline_dataframe(raw):

    if not raw:
        return None

    columns = [
        "open_time",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "close_time",
        "quote_volume",
        "trades",
        "taker_base",
        "taker_quote",
        "ignore"
    ]

    df = pd.DataFrame(
        raw,
        columns=columns
    )

    numeric_columns = [
        "open",
        "high",
        "low",
        "close",
        "volume",
        "quote_volume"
    ]

    for col in numeric_columns:

        df[col] = pd.to_numeric(
            df[col],
            errors="coerce"
        )

    df = df.dropna(
        subset=numeric_columns
    ).copy()

    return df


def calculate_indicators(df):

    if df is None or len(df) < 25:
        return df

    df = df.copy()

    df["BB_MIDDLE"] = (
        df["close"]
        .rolling(BB_PERIOD)
        .mean()
    )

    rolling_std = (
        df["close"]
        .rolling(BB_PERIOD)
        .std(ddof=0)
    )

    df["BB_UPPER"] = (
        df["BB_MIDDLE"]
        + BB_STD * rolling_std
    )

    df["BB_LOWER"] = (
        df["BB_MIDDLE"]
        - BB_STD * rolling_std
    )

    df["EMA5"] = (
        df["close"]
        .ewm(
            span=EMA_PERIOD,
            adjust=False
        )
        .mean()
    )

    df["SMA20"] = (
        df["close"]
        .rolling(SMA_PERIOD)
        .mean()
    )

    return df


# ============================================================
# INITIAL CANDLES
# ============================================================

def load_initial_candles():

    with state_lock:
        symbols = list(selected_symbols)

    if not symbols:

        logger.warning(
            "No selected symbols available for candles."
        )

        return False

    logger.info(
        "Loading initial candles for %d symbols...",
        len(symbols)
    )

    c = get_client()

    success = 0

    for index, symbol in enumerate(symbols, start=1):

        if shutdown_event.is_set():
            break

        try:

            raw = binance_rest_call(
                c.get_klines,
                symbol=symbol,
                interval=TIMEFRAME,
                limit=KLINE_HISTORY
            )

            if raw is None:

                logger.warning(
                    "%s | Initial kline request failed.",
                    symbol
                )

                # If rate limited, stop immediately.
                if last_binance_error and (
                    "-1003" in last_binance_error
                    or "IP banned" in last_binance_error
                ):
                    logger.error(
                        "Stopping candle loading because "
                        "Binance REST is rate limited."
                    )

                    return False

                continue

            df = normalize_kline_dataframe(
                raw
            )

            if df is None or len(df) < 25:

                logger.warning(
                    "%s | Not enough candle data.",
                    symbol
                )

                continue

            df = calculate_indicators(df)

            with state_lock:

                candle_data[symbol] = df

            success += 1

            if index % 10 == 0:

                logger.info(
                    "Initial candles: %d/%d loaded.",
                    index,
                    len(symbols)
                )

            time.sleep(
                REST_REQUEST_DELAY
            )

        except Exception as exc:

            logger.error(
                "%s | Initial candle error: %s",
                symbol,
                exc
            )

    logger.info(
        "Initial candle loading complete. "
        "Success: %d/%d",
        success,
        len(symbols)
    )

    return success > 0


# ============================================================
# LOT SIZE / QUANTITY
# ============================================================

def get_step_size(symbol):

    info = symbol_info.get(symbol)

    if not info:
        return None

    for f in info.get("filters", []):

        if f.get("filterType") in (
            "LOT_SIZE",
            "MARKET_LOT_SIZE"
        ):

            step = f.get("stepSize")

            if step:
                return Decimal(step)

    return None


def get_min_qty(symbol):

    info = symbol_info.get(symbol)

    if not info:
        return Decimal("0")

    min_qty = Decimal("0")

    for f in info.get("filters", []):

        if f.get("filterType") in (
            "LOT_SIZE",
            "MARKET_LOT_SIZE"
        ):

            value = f.get("minQty")

            if value:
                min_qty = max(
                    min_qty,
                    Decimal(value)
                )

    return min_qty


def get_min_notional(symbol):

    info = symbol_info.get(symbol)

    if not info:
        return Decimal("0")

    for f in info.get("filters", []):

        if f.get("filterType") in (
            "NOTIONAL",
            "MIN_NOTIONAL"
        ):

            value = (
                f.get("minNotional")
                or f.get("notional")
            )

            if value:
                return Decimal(value)

    return Decimal("0")


def round_quantity(symbol, quantity):

    step = get_step_size(symbol)

    if step is None:
        return quantity

    quantity = Decimal(
        str(quantity)
    )

    rounded = (
        quantity // step
    ) * step

    return rounded.quantize(
        step,
        rounding=ROUND_DOWN
    )


# ============================================================
# ACCOUNT BALANCE
# ============================================================

def get_asset_balance(asset):

    c = get_client()

    data = binance_rest_call(
        c.get_asset_balance,
        asset=asset
    )

    if data is None:
        return None

    try:

        return float(
            data.get("free", 0)
        )

    except Exception:
        return None


# ============================================================
# BUY
# ============================================================

def execute_buy(symbol, price):

    if symbol in selling_symbols:
        return False

    with positions_lock:

        if symbol in positions:
            return False

    c = get_client()

    logger.info(
        "%s | BUY attempt | Price %.8f",
        symbol,
        price
    )

    try:

        order = binance_rest_call(
            c.order_market_buy,
            symbol=symbol,
            quoteOrderQty=f"{TRADE_AMOUNT_USDT:.2f}"
        )

        if order is None:

            logger.error(
                "%s | BUY failed.",
                symbol
            )

            return False

        executed_qty = 0.0
        executed_quote = 0.0

        try:

            executed_qty = float(
                order.get(
                    "executedQty",
                    0
                )
            )

            executed_quote = float(
                order.get(
                    "cummulativeQuoteQty",
                    0
                )
            )

        except Exception:
            pass

        if executed_qty <= 0:

            logger.error(
                "%s | BUY returned zero quantity.",
                symbol
            )

            return False

        if executed_quote > 0:

            entry_price = (
                executed_quote /
                executed_qty
            )

        else:

            entry_price = price

        stop_price = (
            entry_price
            * (1 - INITIAL_STOP_PERCENT / 100)
        )

        with positions_lock:

            positions[symbol] = {
                "symbol": symbol,
                "quantity": executed_qty,
                "entry_price": entry_price,
                "initial_stop": stop_price,
                "sma20_touched": False,
                "sma20_touch_price": None,
                "buy_time": time.time()
            }

        logger.info(
            "%s | BUY SUCCESS | Qty %.12f | Entry %.8f | SL %.8f",
            symbol,
            executed_qty,
            entry_price,
            stop_price
        )

        return True

    except Exception as exc:

        logger.error(
            "%s | BUY exception: %s",
            symbol,
            exc
        )

        return False


# ============================================================
# SELL
# ============================================================

def execute_sell(
    symbol,
    reason,
    current_price
):

    with positions_lock:

        position = positions.get(symbol)

        if not position:
            return False

        if symbol in selling_symbols:
            return False

        selling_symbols.add(symbol)

    try:

        quantity = float(
            position["quantity"]
        )

        quantity = round_quantity(
            symbol,
            quantity
        )

        if quantity <= 0:

            logger.error(
                "%s | Invalid sell quantity.",
                symbol
            )

            return False

        c = get_client()

        logger.info(
            "%s | SELL attempt | Reason=%s | Price=%.8f",
            symbol,
            reason,
            current_price
        )

        order = binance_rest_call(
            c.order_market_sell,
            symbol=symbol,
            quantity=str(quantity)
        )

        if order is None:

            logger.error(
                "%s | SELL failed.",
                symbol
            )

            return False

        logger.info(
            "%s | SELL SUCCESS | Reason=%s",
            symbol,
            reason
        )

        with positions_lock:

            positions.pop(
                symbol,
                None
            )

        return True

    except Exception as exc:

        logger.error(
            "%s | SELL exception: %s",
            symbol,
            exc
        )

        return False

    finally:

        with positions_lock:

            selling_symbols.discard(
                symbol
            )


# ============================================================
# POSITION RECOVERY
# ============================================================

def recover_positions():

    logger.info(
        "Recovering existing Binance positions..."
    )

    c = get_client()

    account = binance_rest_call(
        c.get_account
    )

    if account is None:

        logger.warning(
            "Could not recover positions because "
            "account information is unavailable."
        )

        return False

    balances = account.get(
        "balances",
        []
    )

    recovered = 0

    for balance in balances:

        try:

            asset = balance.get(
                "asset"
            )

            free = float(
                balance.get(
                    "free",
                    0
                )
            )

            locked = float(
                balance.get(
                    "locked",
                    0
                )
            )

            total = free + locked

            if total <= 0:
                continue

            symbol = asset + "USDT"

            if symbol not in symbol_info:
                continue

            if not is_valid_symbol(symbol):
                continue

            # Avoid treating tiny dust as a position.
            if total < get_min_qty(symbol):
                continue

            price = live_prices.get(
                symbol
            )

            if not price:
                continue

            entry_price = float(price)

            stop_price = (
                entry_price
                * (1 - INITIAL_STOP_PERCENT / 100)
            )

            with positions_lock:

                if symbol not in positions:

                    positions[symbol] = {
                        "symbol": symbol,
                        "quantity": total,
                        "entry_price": entry_price,
                        "initial_stop": stop_price,
                        "sma20_touched": False,
                        "sma20_touch_price": None,
                        "buy_time": time.time()
                    }

                    recovered += 1

        except Exception:
            continue

    logger.info(
        "Position recovery complete. "
        "Recovered: %d",
        recovered
    )

    return True


# ============================================================
# CANDLE SIGNAL
# ============================================================

def check_buy_signal(symbol):

    with state_lock:

        df = candle_data.get(
            symbol
        )

    if df is None:
        return False

    if len(df) < 25:
        return False

    df = calculate_indicators(
        df.copy()
    )

    # IMPORTANT:
    # Use the last CLOSED candle.
    candle = df.iloc[-2]

    required = [
        "open",
        "high",
        "close",
        "BB_LOWER",
        "EMA5"
    ]

    for col in required:

        value = candle.get(col)

        if pd.isna(value):
            return False

        if not isinstance(
            value,
            (int, float)
        ):
            try:
                float(value)
            except Exception:
                return False

    open_price = float(
        candle["open"]
    )

    high_price = float(
        candle["high"]
    )

    close_price = float(
        candle["close"]
    )

    bb_lower = float(
        candle["BB_LOWER"]
    )

    ema5 = float(
        candle["EMA5"]
    )

    condition = (
        open_price < bb_lower
        and
        close_price > bb_lower
        and
        close_price < ema5
        and
        high_price < ema5
    )

    return condition


# ============================================================
# KLINE WEBSOCKET
# ============================================================

def build_kline_stream_url(symbols):

    streams = []

    for symbol in symbols:

        streams.append(
            symbol.lower()
            + "@kline_5m"
        )

    stream_string = "/".join(
        streams
    )

    return (
        WS_BASE
        + "/stream?streams="
        + stream_string
    )


def kline_worker(batch_id, symbols):

    url = build_kline_stream_url(
        symbols
    )

    while not shutdown_event.is_set():

        try:

            logger.info(
                "Kline WebSocket %d connecting "
                "(%d symbols)...",
                batch_id,
                len(symbols)
            )

            def on_open(ws):

                ws_connections[
                    batch_id
                ] = True

                logger.info(
                    "Kline WebSocket %d connected.",
                    batch_id
                )

            def on_message(ws, message):

                handle_kline_message(
                    message
                )

            def on_error(ws, error):

                logger.error(
                    "Kline WebSocket %d error: %s",
                    batch_id,
                    error
                )

            def on_close(ws, code, msg):

                ws_connections[
                    batch_id
                ] = False

                logger.warning(
                    "Kline WebSocket %d closed: %s %s",
                    batch_id,
                    code,
                    msg
                )

            ws = websocket.WebSocketApp(
                url,
                on_open=on_open,
                on_message=on_message,
                on_error=on_error,
                on_close=on_close
            )

            ws.run_forever(
                ping_interval=WS_PING_INTERVAL,
                ping_timeout=WS_PING_TIMEOUT
            )

        except Exception as exc:

            ws_connections[
                batch_id
            ] = False

            logger.error(
                "Kline WebSocket %d exception: %s",
                batch_id,
                exc
            )

        if not shutdown_event.is_set():

            shutdown_event.wait(
                WS_RECONNECT_SECONDS
            )


def handle_kline_message(message):

    try:

        root = json.loads(
            message
        )

        data = root.get(
            "data",
            root
        )

        if data.get("e") != "kline":
            return

        symbol = data.get("s")

        kline = data.get("k")

        if not symbol or not kline:
            return

        open_price = float(
            kline["o"]
        )

        high_price = float(
            kline["h"]
        )

        low_price = float(
            kline["l"]
        )

        close_price = float(
            kline["c"]
        )

        volume = float(
            kline["v"]
        )

        is_closed = bool(
            kline["x"]
        )

        # Update live price on every kline update.
        with state_lock:

            live_prices[
                symbol
            ] = close_price

        # Only process strategy on CLOSED candle.
        if not is_closed:
            return

        with state_lock:

            old_df = candle_data.get(
                symbol
            )

        if old_df is None:

            return

        new_row = pd.DataFrame(
            [{
                "open_time":
                    int(kline["t"]),

                "open":
                    open_price,

                "high":
                    high_price,

                "low":
                    low_price,

                "close":
                    close_price,

                "volume":
                    volume,

                "close_time":
                    int(kline["T"]),

                "quote_volume":
                    float(kline["q"]),

                "trades":
                    int(kline["n"]),

                "taker_base":
                    float(kline["V"]),

                "taker_quote":
                    float(kline["Q"]),

                "ignore":
                    0
            }]
        )

        df = pd.concat(
            [
                old_df,
                new_row
            ],
            ignore_index=True
        )

        # Remove duplicate candle.
        df = (
            df.drop_duplicates(
                subset=["open_time"],
                keep="last"
            )
            .tail(KLINE_HISTORY)
            .reset_index(drop=True)
        )

        df = normalize_kline_dataframe(
            df.values.tolist()
        )

        df = calculate_indicators(
            df
        )

        with state_lock:

            candle_data[
                symbol
            ] = df

        process_closed_candle(
            symbol
        )

    except Exception as exc:

        logger.error(
            "%s | candle processing error: %s",
            symbol if "symbol" in locals()
            else "UNKNOWN",
            exc
        )


# ============================================================
# PROCESS CLOSED CANDLE
# ============================================================

def process_closed_candle(symbol):

    try:

        if not check_buy_signal(
            symbol
        ):
            return

        with positions_lock:

            if symbol in positions:
                return

        with state_lock:

            price = live_prices.get(
                symbol
            )

        if not price:
            return

        logger.info(
            "%s | BUY SIGNAL | Price %.8f",
            symbol,
            price
        )

        execute_buy(
            symbol,
            price
        )

    except Exception as exc:

        logger.error(
            "%s | Signal processing error: %s",
            symbol,
            exc
        )


# ============================================================
# POSITION SAFETY LOOP
# ============================================================

def position_safety_loop():

    logger.info(
        "Position safety loop started."
    )

    while not shutdown_event.is_set():

        try:

            with positions_lock:

                current_positions = dict(
                    positions
                )

            for symbol, position in current_positions.items():

                try:

                    with state_lock:

                        price = live_prices.get(
                            symbol
                        )

                        df = candle_data.get(
                            symbol
                        )

                    if price is None:
                        continue

                    current_price = float(
                        price
                    )

                    entry_price = float(
                        position["entry_price"]
                    )

                    initial_stop = float(
                        position["initial_stop"]
                    )

                    # ========================================
                    # INITIAL 1% STOP
                    # ========================================

                    if current_price <= initial_stop:

                        logger.warning(
                            "%s | INITIAL SL HIT | "
                            "Entry %.8f | Current %.8f",
                            symbol,
                            entry_price,
                            current_price
                        )

                        execute_sell(
                            symbol,
                            "INITIAL_SL_1%",
                            current_price
                        )

                        continue

                    # ========================================
                    # INDICATORS
                    # ========================================

                    if df is None or len(df) < 20:
                        continue

                    df = calculate_indicators(
                        df.copy()
                    )

                    latest = df.iloc[-1]

                    upper_bb = latest.get(
                        "BB_UPPER"
                    )

                    sma20 = latest.get(
                        "SMA20"
                    )

                    if pd.isna(upper_bb):
                        continue

                    if pd.isna(sma20):
                        continue

                    upper_bb = float(
                        upper_bb
                    )

                    sma20 = float(
                        sma20
                    )

                    # ========================================
                    # UPPER BB SELL
                    # ========================================

                    if current_price >= upper_bb:

                        logger.info(
                            "%s | UPPER BB TOUCH | "
                            "Price %.8f | BB %.8f",
                            symbol,
                            current_price,
                            upper_bb
                        )

                        execute_sell(
                            symbol,
                            "UPPER_BB",
                            current_price
                        )

                        continue

                    # ========================================
                    # SMA20 FIRST TOUCH
                    # ========================================

                    if not position[
                        "sma20_touched"
                    ]:

                        if current_price >= sma20:

                            position[
                                "sma20_touched"
                            ] = True

                            position[
                                "sma20_touch_price"
                            ] = current_price

                            logger.info(
                                "%s | SMA20 FIRST TOUCH | "
                                "Touch price %.8f",
                                symbol,
                                current_price
                            )

                            continue

                    # ========================================
                    # AFTER SMA20 TOUCH:
                    # DROP 1% FROM TOUCH PRICE
                    # ========================================

                    if position[
                        "sma20_touched"
                    ]:

                        touch_price = float(
                            position[
                                "sma20_touch_price"
                            ]
                        )

                        drop_price = (
                            touch_price
                            * (
                                1
                                - SMA20_DROP_PERCENT / 100
                            )
                        )

                        if current_price <= drop_price:

                            logger.info(
                                "%s | SMA20 TOUCH DROP SELL | "
                                "Touch %.8f | Current %.8f | "
                                "Drop trigger %.8f",
                                symbol,
                                touch_price,
                                current_price,
                                drop_price
                            )

                            execute_sell(
                                symbol,
                                "SMA20_TOUCH_DROP_1%",
                                current_price
                            )

                except Exception as exc:

                    logger.error(
                        "%s | Position safety error: %s",
                        symbol,
                        exc
                    )

            shutdown_event.wait(2)

        except Exception as exc:

            logger.error(
                "Position safety loop error: %s",
                exc
            )

            shutdown_event.wait(5)


# ============================================================
# KLINE WEBSOCKET START
# ============================================================

def start_kline_websockets():

    with state_lock:

        symbols = list(
            selected_symbols
        )

    if not symbols:

        logger.error(
            "Cannot start kline WebSockets: "
            "no symbols selected."
        )

        return False

    # Close/replace connection state.
    with state_lock:

        ws_connections.clear()

    batches = []

    for i in range(
        0,
        len(symbols),
        SYMBOL_BATCH_SIZE
    ):

        batches.append(
            symbols[
                i:i + SYMBOL_BATCH_SIZE
            ]
        )

    logger.info(
        "Starting %d kline WebSocket connections...",
        len(batches)
    )

    for index, batch in enumerate(
        batches,
        start=1
    ):

        ws_connections[
            index
        ] = False

        thread = threading.Thread(
            target=kline_worker,
            args=(index, batch),
            daemon=True
        )

        thread.start()

    return True


# ============================================================
# INITIALIZATION
# ============================================================

def initialize_bot():

    global bot_initialized
    global initialization_running

    if initialization_running:
        return

    initialization_running = True

    try:

        logger.info(
            "=" * 70
        )

        logger.info(
            "BINANCE BB20 + EMA5 + SMA20 TOUCH BOT"
        )

        logger.info(
            "Trade amount: %.2f USDT",
            TRADE_AMOUNT_USDT
        )

        logger.info(
            "Timeframe: 5m"
        )

        logger.info(
            "Top symbols: %d",
            TOP_SYMBOLS
        )

        logger.info(
            "Initial SL: %.2f%%",
            INITIAL_STOP_PERCENT
        )

        logger.info(
            "SMA20 touch drop: %.2f%%",
            SMA20_DROP_PERCENT
        )

        logger.info(
            "=" * 70
        )

        # ================================================
        # STEP 1:
        # CREATE CLIENT
        # ================================================

        get_client()

        # ================================================
        # STEP 2:
        # EXCHANGE INFO
        # ================================================

        if not load_exchange_info():

            logger.error(
                "Binance REST unavailable. "
                "Initialization stopped."
            )

            bot_initialized = False
            return

        # ================================================
        # STEP 3:
        # WAIT FOR WEBSOCKET TICKER TO SELECT TOP 150
        # ================================================

        logger.info(
            "Waiting for all-market WebSocket ticker "
            "to select top %d symbols...",
            TOP_SYMBOLS
        )

        wait_start = time.time()

        while (
            not selected_symbols
            and
            time.time() - wait_start < 60
        ):

            shutdown_event.wait(1)

        if not selected_symbols:

            logger.error(
                "Could not select top symbols "
                "from WebSocket ticker."
            )

            bot_initialized = False
            return

        # ================================================
        # STEP 4:
        # INITIAL CANDLES
        # ================================================

        if not load_initial_candles():

            logger.error(
                "Initial candle loading failed."
            )

            bot_initialized = False
            return

        # ================================================
        # STEP 5:
        # POSITION RECOVERY
        # ================================================

        recover_positions()

        # ================================================
        # STEP 6:
        # KLINE WEBSOCKETS
        # ================================================

        start_kline_websockets()

        # ================================================
        # SUCCESS
        # ================================================

        bot_initialized = True

        logger.info(
            "=" * 70
        )

        logger.info(
            "BOT INITIALIZATION COMPLETE"
        )

        logger.info(
            "Symbols: %d",
            len(selected_symbols)
        )

        logger.info(
            "WebSocket connections: %d",
            len(ws_connections)
        )

        logger.info(
            "=" * 70
        )

    except Exception as exc:

        logger.exception(
            "Bot initialization exception: %s",
            exc
        )

        bot_initialized = False

    finally:

        initialization_running = False


# ============================================================
# BACKGROUND INITIALIZATION MANAGER
# ============================================================

def initialization_manager():

    logger.info(
        "Starting bot background initialization..."
    )

    while not shutdown_event.is_set():

        if bot_initialized:

            shutdown_event.wait(
                60
            )

            continue

        logger.info(
            "Bot initialization attempt..."
        )

        initialize_bot()

        if bot_initialized:

            logger.info(
                "Initialization successful."
            )

            continue

        # ================================================
        # IMPORTANT:
        # Do NOT hammer Binance after -1003.
        # ================================================

        if (
            last_binance_error
            and
            (
                "-1003" in last_binance_error
                or
                "IP banned" in last_binance_error
            )
        ):

            logger.warning(
                "Binance REST appears rate limited/banned. "
                "Waiting %d seconds before next "
                "initialization attempt.",
                BAN_COOLDOWN_SECONDS
            )

            shutdown_event.wait(
                BAN_COOLDOWN_SECONDS
            )

        else:

            logger.info(
                "Waiting %d seconds before next "
                "initialization attempt.",
                REST_RETRY_SECONDS
            )

            shutdown_event.wait(
                REST_RETRY_SECONDS
            )


# ============================================================
# SYMBOL REFRESH
# ============================================================

def symbol_refresh_monitor():

    global last_symbol_refresh

    while not shutdown_event.is_set():

        try:

            # Symbol selection is now handled by
            # all-market WebSocket ticker.

            # Just log status periodically.
            if selected_symbols:

                logger.info(
                    "Symbol monitor: %d symbols active.",
                    len(selected_symbols)
                )

        except Exception as exc:

            logger.error(
                "Symbol monitor error: %s",
                exc
            )

        shutdown_event.wait(
            1800
        )


# ============================================================
# FLASK
# ============================================================

@app.route("/")
def home():

    return jsonify({
        "status": "online",
        "bot_initialized": bot_initialized,
        "strategy":
            "BB20 + EMA5 + SMA20 TOUCH",
        "trade_amount_usdt":
            TRADE_AMOUNT_USDT,
        "timeframe":
            "5m"
    })


@app.route("/health")
def health():

    with positions_lock:

        position_count = len(
            positions
        )

    with state_lock:

        symbol_count = len(
            selected_symbols
        )

        websocket_count = sum(
            1
            for value in ws_connections.values()
            if value
        )

    return jsonify({

        "status":
            "healthy",

        "bot_initialized":
            bot_initialized,

        "positions":
            position_count,

        "top_symbols":
            symbol_count,

        "websocket_connections":
            websocket_count,

        "last_binance_error":
            last_binance_error,

        "last_binance_success":
            (
                utc_time_string(
                    last_binance_success
                )
                if last_binance_success
                else None
            ),

        "last_rest_attempt":
            (
                utc_time_string(
                    last_rest_attempt
                )
                if last_rest_attempt
                else None
            ),

        "strategy":
            "BB20 + EMA5 + SMA20 TOUCH",

        "trade_amount_usdt":
            TRADE_AMOUNT_USDT,

        "timeframe":
            "5m"
    })


# ============================================================
# START BACKGROUND THREADS
# ============================================================

def start_background_threads():

    logger.info(
        "Starting background threads..."
    )

    # ================================================
    # ALL MARKET TICKER WEBSOCKET
    # ================================================

    ticker_thread = threading.Thread(
        target=ticker_stream_worker,
        daemon=True
    )

    ticker_thread.start()

    # ================================================
    # INITIALIZATION MANAGER
    # ================================================

    init_thread = threading.Thread(
        target=initialization_manager,
        daemon=True
    )

    init_thread.start()

    # ================================================
    # POSITION SAFETY
    # ================================================

    safety_thread = threading.Thread(
        target=position_safety_loop,
        daemon=True
    )

    safety_thread.start()

    # ================================================
    # SYMBOL MONITOR
    # ================================================

    monitor_thread = threading.Thread(
        target=symbol_refresh_monitor,
        daemon=True
    )

    monitor_thread.start()

    logger.info(
        "Background threads started."
    )


# ============================================================
# START ON IMPORT
# ============================================================

start_background_threads()


# ============================================================
# LOCAL RUN
# ============================================================

if __name__ == "__main__":

    port = int(
        os.environ.get(
            "PORT",
            "10000"
        )
    )

    app.run(
        host="0.0.0.0",
        port=port
    )
