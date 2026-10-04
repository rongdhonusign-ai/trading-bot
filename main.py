import os
import time
import json
import math
import threading
import logging
from decimal import Decimal, ROUND_DOWN

import pandas as pd
import websocket

from flask import Flask, jsonify
from binance.client import Client
from binance.exceptions import BinanceAPIException, BinanceRequestException


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


TRADE_AMOUNT_USDT = 35.0

TIMEFRAME = Client.KLINE_INTERVAL_5MINUTE

TOP_SYMBOLS_COUNT = 150

TOP_SYMBOL_REFRESH_SECONDS = 3600

INITIAL_CANDLE_LIMIT = 50

# REST request spacing
REST_REQUEST_DELAY = 0.30

# Buy protection
BUY_COOLDOWN_SECONDS = 60

# Initial stop loss
INITIAL_STOP_LOSS_PERCENT = 0.01

# SMA20 touch -> drop from touch
SMA_TOUCH_DROP_PERCENT = 0.01

# WebSocket
WS_BATCH_SIZE = 50
WS_PING_INTERVAL = 30
WS_PING_TIMEOUT = 10

# Reconnect
WS_RECONNECT_MIN = 5
WS_RECONNECT_MAX = 60

# REST fallback safety price check
REST_SAFETY_INTERVAL = 30


# ============================================================
# FLASK
# ============================================================

app = Flask(__name__)


@app.route("/")
def home():
    return jsonify({
        "status": "online",
        "bot": "Binance BB20 + EMA5 + SMA20 Touch Bot"
    })


@app.route("/health")
def health():
    with positions_lock:
        position_count = len(positions)

    return jsonify({
        "status": "healthy",
        "positions": position_count,
        "top_symbols": len(top_symbols),
        "websocket_connections": len(ws_threads),
        "bot_initialized": bot_initialized
    })


# ============================================================
# GLOBAL STATE
# ============================================================

client = None
client_lock = threading.Lock()

exchange_info = {}
symbol_filters = {}

top_symbols = []

candles = {}
latest_prices = {}

positions = {}

selling_symbols = set()

last_buy_time = {}

last_rest_safety_check = {}

positions_lock = threading.RLock()
data_lock = threading.RLock()

ws_threads = []
ws_objects = []

ws_stop_event = threading.Event()

bot_initialized = False

bot_starting = False


# ============================================================
# STABLECOINS / FIAT
# ============================================================

EXCLUDED_BASES = {
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
    "USD1",
    "PYUSD",
    "EUR",
    "GBP",
    "AUD",
    "BRL",
    "TRY",
    "RUB",
    "UAH",
    "ARS",
    "MXN",
    "PLN",
    "RON",
    "ZAR",
    "NGN",
    "JPY",
    "CAD",
    "CHF",
    "AED",
    "DKK",
    "NOK",
    "SEK",
    "HKD",
    "SGD"
}


# ============================================================
# BINANCE CLIENT
# ============================================================

def get_client():
    """
    Lazy Binance client.

    IMPORTANT:
    Client() is NOT created at module import.
    ping=False prevents python-binance from doing
    an immediate ping during Gunicorn import.
    """

    global client

    if client is not None:
        return client

    with client_lock:

        if client is not None:
            return client

        if not API_KEY or not API_SECRET:
            raise RuntimeError(
                "BINANCE_API_KEY / BINANCE_API_SECRET missing."
            )

        logger.info("Creating Binance client...")

        client = Client(
            API_KEY,
            API_SECRET,
            ping=False,
            requests_params={"timeout": 15}
        )

        logger.info("Binance client created successfully.")

        return client


# ============================================================
# BINANCE API SAFE CALL
# ============================================================

def binance_call(function, *args, **kwargs):
    """
    Safe REST API wrapper.

    - Does not hammer Binance.
    - Handles -1003 separately.
    - Uses exponential backoff.
    """

    max_attempts = 3

    for attempt in range(1, max_attempts + 1):

        try:

            time.sleep(REST_REQUEST_DELAY)

            return function(*args, **kwargs)

        except BinanceAPIException as e:

            code = getattr(e, "code", None)

            if code == -1003:

                logger.error(
                    "BINANCE RATE LIMIT / IP BAN (-1003). "
                    "REST request stopped. Waiting before retry."
                )

                # Do NOT repeatedly hit Binance while banned.
                if attempt < max_attempts:
                    wait_time = 60 * attempt

                    logger.warning(
                        "Waiting %s seconds before REST retry...",
                        wait_time
                    )

                    time.sleep(wait_time)
                    continue

                raise

            logger.error(
                "Binance API error: code=%s message=%s",
                code,
                str(e)
            )

            if attempt < max_attempts:
                time.sleep(2 * attempt)
                continue

            raise

        except BinanceRequestException as e:

            logger.error(
                "Binance request error: %s",
                str(e)
            )

            if attempt < max_attempts:
                time.sleep(2 * attempt)
                continue

            raise

        except Exception as e:

            logger.error(
                "Unexpected Binance REST error: %s",
                str(e)
            )

            if attempt < max_attempts:
                time.sleep(2 * attempt)
                continue

            raise

    return None


# ============================================================
# EXCHANGE INFO
# ============================================================

def load_exchange_info():

    global exchange_info
    global symbol_filters

    logger.info("Loading Binance exchange information...")

    data = binance_call(
        get_client().get_exchange_info
    )

    if not data:
        raise RuntimeError("Exchange info unavailable.")

    exchange_info = data

    symbol_filters = {}

    for item in data.get("symbols", []):

        symbol = item.get("symbol")

        if not symbol:
            continue

        if item.get("status") != "TRADING":
            continue

        filters = {}

        for f in item.get("filters", []):

            filter_type = f.get("filterType")

            if filter_type:
                filters[filter_type] = f

        symbol_filters[symbol] = filters

    logger.info(
        "Exchange info loaded. Symbols: %s",
        len(symbol_filters)
    )


# ============================================================
# SYMBOL FILTER HELPERS
# ============================================================

def get_symbol_filter(symbol, filter_type):

    return symbol_filters.get(symbol, {}).get(filter_type)


def round_quantity(symbol, quantity):

    try:

        filters = symbol_filters.get(symbol, {})

        lot_filter = (
            filters.get("MARKET_LOT_SIZE")
            or filters.get("LOT_SIZE")
        )

        if not lot_filter:
            return quantity

        step_size = Decimal(
            str(lot_filter.get("stepSize", "0"))
        )

        min_qty = Decimal(
            str(lot_filter.get("minQty", "0"))
        )

        if step_size <= 0:
            return quantity

        qty = Decimal(str(quantity))

        qty = (
            qty / step_size
        ).to_integral_value(
            rounding=ROUND_DOWN
        ) * step_size

        if qty < min_qty:
            return 0.0

        return float(qty)

    except Exception as e:

        logger.error(
            "%s quantity rounding error: %s",
            symbol,
            e
        )

        return 0.0


def check_notional(symbol, quantity, price):

    try:

        filters = symbol_filters.get(symbol, {})

        notional_filter = (
            filters.get("NOTIONAL")
            or filters.get("MIN_NOTIONAL")
        )

        if not notional_filter:
            return True

        min_notional = float(
            notional_filter.get("minNotional", 0)
        )

        return quantity * price >= min_notional

    except Exception:

        return True


# ============================================================
# TOP SYMBOLS
# ============================================================

def update_top_symbols():

    global top_symbols

    logger.info("Updating top %s USDT symbols...", TOP_SYMBOLS_COUNT)

    data = binance_call(
        get_client().get_ticker
    )

    if not data:
        return False

    candidates = []

    for item in data:

        symbol = item.get("symbol", "")

        if not symbol.endswith("USDT"):
            continue

        if symbol not in symbol_filters:
            continue

        base_asset = symbol[:-4]

        if base_asset in EXCLUDED_BASES:
            continue

        filters = symbol_filters.get(symbol, {})

        if not filters:
            continue

        try:

            volume = float(
                item.get("quoteVolume", 0)
            )

        except Exception:

            continue

        if not math.isfinite(volume):
            continue

        candidates.append(
            (symbol, volume)
        )

    candidates.sort(
        key=lambda x: x[1],
        reverse=True
    )

    new_symbols = [
        symbol
        for symbol, volume in candidates[
            :TOP_SYMBOLS_COUNT
        ]
    ]

    if not new_symbols:
        logger.warning(
            "No top symbols found."
        )
        return False

    with data_lock:
        top_symbols = new_symbols

    logger.info(
        "Top symbols selected: %s",
        len(new_symbols)
    )

    logger.info(
        "First symbols: %s",
        ", ".join(new_symbols[:20])
    )

    return True


# ============================================================
# KLINE CLEANING
# ============================================================

def clean_dataframe(raw_klines):

    if not raw_klines:
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
        "taker_buy_base",
        "taker_buy_quote",
        "ignore"
    ]

    try:

        df = pd.DataFrame(
            raw_klines,
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
            subset=[
                "open",
                "high",
                "low",
                "close"
            ]
        ).copy()

        if df.empty:
            return None

        df = df[
            [
                "open_time",
                "open",
                "high",
                "low",
                "close",
                "volume",
                "close_time"
            ]
        ].copy()

        df.reset_index(
            drop=True,
            inplace=True
        )

        return df

    except Exception as e:

        logger.error(
            "DataFrame cleaning error: %s",
            e
        )

        return None


# ============================================================
# INDICATORS
# ============================================================

def calculate_indicators(df):

    if df is None or len(df) < 25:
        return None

    df = df.copy()

    df["sma20"] = (
        df["close"]
        .rolling(
            window=20,
            min_periods=20
        )
        .mean()
    )

    df["std20"] = (
        df["close"]
        .rolling(
            window=20,
            min_periods=20
        )
        .std(
            ddof=0
        )
    )

    df["bb_upper"] = (
        df["sma20"]
        + (
            2.0 * df["std20"]
        )
    )

    df["bb_lower"] = (
        df["sma20"]
        - (
            2.0 * df["std20"]
        )
    )

    df["ema5"] = (
        df["close"]
        .ewm(
            span=5,
            adjust=False
        )
        .mean()
    )

    return df


# ============================================================
# LOAD INITIAL CANDLES
# ============================================================

def load_initial_candles():

    logger.info(
        "Loading initial candles for %s symbols...",
        len(top_symbols)
    )

    loaded = 0

    for index, symbol in enumerate(top_symbols):

        try:

            raw = binance_call(
                get_client().get_klines,
                symbol=symbol,
                interval=TIMEFRAME,
                limit=INITIAL_CANDLE_LIMIT
            )

            df = clean_dataframe(raw)

            if df is None:
                continue

            df = calculate_indicators(df)

            if df is None:
                continue

            with data_lock:
                candles[symbol] = df

            loaded += 1

            if (
                index == 0
                or
                (index + 1) % 25 == 0
            ):
                logger.info(
                    "Initial candles: %s/%s loaded",
                    index + 1,
                    len(top_symbols)
                )

        except BinanceAPIException as e:

            if getattr(e, "code", None) == -1003:

                logger.error(
                    "Rate limit encountered while loading candles. "
                    "Stopping historical loading."
                )

                return False

            logger.error(
                "%s initial candle error: %s",
                symbol,
                e
            )

        except Exception as e:

            logger.error(
                "%s initial candle error: %s",
                symbol,
                e
            )

    logger.info(
        "Initial candles loaded: %s/%s",
        loaded,
        len(top_symbols)
    )

    return loaded > 0


# ============================================================
# POSITION RECOVERY
# ============================================================

def recover_positions():

    logger.info(
        "Checking Binance account for existing positions..."
    )

    try:

        account = binance_call(
            get_client().get_account
        )

        if not account:
            return

        balances = account.get(
            "balances",
            []
        )

        recovered = 0

        for balance in balances:

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

            if asset in EXCLUDED_BASES:
                continue

            symbol = asset + "USDT"

            if symbol not in symbol_filters:
                continue

            try:

                ticker = binance_call(
                    get_client().get_symbol_ticker,
                    symbol=symbol
                )

                if not ticker:
                    continue

                current_price = float(
                    ticker["price"]
                )

                if current_price <= 0:
                    continue

                with positions_lock:

                    positions[symbol] = {
                        "quantity": total,
                        "entry_price": current_price,
                        "stop_price": (
                            current_price
                            * (
                                1
                                - INITIAL_STOP_LOSS_PERCENT
                            )
                        ),
                        "sma_touched": False,
                        "sma_touch_price": None,
                        "recovered": True,
                        "buy_time": time.time()
                    }

                latest_prices[symbol] = current_price

                recovered += 1

                logger.warning(
                    "RECOVERED POSITION | %s | qty=%.8f | "
                    "recovery_price=%.8f",
                    symbol,
                    total,
                    current_price
                )

            except Exception as e:

                logger.error(
                    "%s recovery error: %s",
                    symbol,
                    e
                )

        logger.info(
            "Recovered positions: %s",
            recovered
        )

    except Exception as e:

        logger.error(
            "Position recovery failed: %s",
            e
        )


# ============================================================
# BUY
# ============================================================

def execute_buy(symbol, candle):

    now = time.time()

    previous_buy = last_buy_time.get(
        symbol,
        0
    )

    if now - previous_buy < BUY_COOLDOWN_SECONDS:

        return False

    with positions_lock:

        if symbol in positions:
            return False

        if symbol in selling_symbols:
            return False

    try:

        current_price = latest_prices.get(
            symbol
        )

        if not current_price or current_price <= 0:

            ticker = binance_call(
                get_client().get_symbol_ticker,
                symbol=symbol
            )

            if not ticker:
                return False

            current_price = float(
                ticker["price"]
            )

        if current_price <= 0:
            return False

        quantity = (
            TRADE_AMOUNT_USDT
            / current_price
        )

        quantity = round_quantity(
            symbol,
            quantity
        )

        if quantity <= 0:

            logger.warning(
                "%s BUY skipped: quantity too small.",
                symbol
            )

            return False

        if not check_notional(
            symbol,
            quantity,
            current_price
        ):

            logger.warning(
                "%s BUY skipped: minimum notional.",
                symbol
            )

            return False

        logger.info(
            "BUY SIGNAL | %s | price=%.8f | "
            "BB_lower=%.8f | EMA5=%.8f",
            symbol,
            current_price,
            candle["bb_lower"],
            candle["ema5"]
        )

        order = binance_call(
            get_client().order_market_buy,
            symbol=symbol,
            quantity=quantity
        )

        if not order:
            return False

        executed_qty = float(
            order.get(
                "executedQty",
                quantity
            )
        )

        fills = order.get(
            "fills",
            []
        )

        actual_price = current_price

        if fills:

            total_qty = 0.0
            total_quote = 0.0

            for fill in fills:

                try:

                    fill_qty = float(
                        fill["qty"]
                    )

                    fill_price = float(
                        fill["price"]
                    )

                    total_qty += fill_qty
                    total_quote += (
                        fill_qty
                        * fill_price
                    )

                except Exception:
                    continue

            if total_qty > 0:

                actual_price = (
                    total_quote
                    / total_qty
                )

        stop_price = (
            actual_price
            * (
                1
                - INITIAL_STOP_LOSS_PERCENT
            )
        )

        with positions_lock:

            positions[symbol] = {

                "quantity": executed_qty,

                "entry_price": actual_price,

                "stop_price": stop_price,

                "sma_touched": False,

                "sma_touch_price": None,

                "recovered": False,

                "buy_time": time.time()
            }

        last_buy_time[symbol] = now

        logger.info(
            "BUY EXECUTED | %s | qty=%.8f | "
            "entry=%.8f | SL=%.8f",
            symbol,
            executed_qty,
            actual_price,
            stop_price
        )

        return True

    except BinanceAPIException as e:

        logger.error(
            "%s BUY Binance error: %s",
            symbol,
            e
        )

    except Exception as e:

        logger.error(
            "%s BUY error: %s",
            symbol,
            e
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

        if symbol not in positions:
            return False

        if symbol in selling_symbols:
            return False

        position = positions[symbol]

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
                "%s SELL quantity invalid.",
                symbol
            )

            return False

        logger.info(
            "SELL SIGNAL | %s | reason=%s | "
            "price=%.8f | qty=%.8f",
            symbol,
            reason,
            current_price,
            quantity
        )

        order = binance_call(
            get_client().order_market_sell,
            symbol=symbol,
            quantity=quantity
        )

        if not order:

            return False

        fills = order.get(
            "fills",
            []
        )

        actual_price = current_price

        if fills:

            total_qty = 0.0
            total_quote = 0.0

            for fill in fills:

                try:

                    fill_qty = float(
                        fill["qty"]
                    )

                    fill_price = float(
                        fill["price"]
                    )

                    total_qty += fill_qty
                    total_quote += (
                        fill_qty
                        * fill_price
                    )

                except Exception:
                    continue

            if total_qty > 0:

                actual_price = (
                    total_quote
                    / total_qty
                )

        entry_price = float(
            position["entry_price"]
        )

        pnl_percent = (
            (
                actual_price
                - entry_price
            )
            / entry_price
        ) * 100

        logger.info(
            "SELL EXECUTED | %s | "
            "entry=%.8f | exit=%.8f | "
            "PnL=%.3f%% | reason=%s",
            symbol,
            entry_price,
            actual_price,
            pnl_percent,
            reason
        )

        with positions_lock:

            positions.pop(
                symbol,
                None
            )

        return True

    except BinanceAPIException as e:

        logger.error(
            "%s SELL Binance error: %s",
            symbol,
            e
        )

        return False

    except Exception as e:

        logger.error(
            "%s SELL error: %s",
            symbol,
            e
        )

        return False

    finally:

        with positions_lock:

            selling_symbols.discard(
                symbol
            )


# ============================================================
# CHECK POSITION
# ============================================================

def check_position(
    symbol,
    current_price
):

    with positions_lock:

        position = positions.get(
            symbol
        )

    if not position:
        return

    if current_price <= 0:
        return

    entry_price = float(
        position["entry_price"]
    )

    stop_price = float(
        position["stop_price"]
    )

    # --------------------------------------------------------
    # 1. INITIAL STOP LOSS
    # --------------------------------------------------------

    if current_price <= stop_price:

        execute_sell(
            symbol,
            "INITIAL_STOP_LOSS_1%",
            current_price
        )

        return

    # --------------------------------------------------------
    # Get latest indicators
    # --------------------------------------------------------

    with data_lock:

        df = candles.get(
            symbol
        )

    if df is None:
        return

    if len(df) < 20:
        return

    try:

        last_row = df.iloc[-1]

        sma20 = float(
            last_row["sma20"]
        )

        bb_upper = float(
            last_row["bb_upper"]
        )

        if not math.isfinite(sma20):
            return

        if not math.isfinite(bb_upper):
            return

    except Exception:

        return

    # --------------------------------------------------------
    # 2. SMA20 FIRST TOUCH
    # --------------------------------------------------------

    with positions_lock:

        position = positions.get(
            symbol
        )

        if not position:
            return

        sma_touched = position.get(
            "sma_touched",
            False
        )

        if (
            not sma_touched
            and current_price >= sma20
        ):

            position["sma_touched"] = True

            position["sma_touch_price"] = (
                current_price
            )

            logger.info(
                "SMA20 FIRST TOUCH | %s | "
                "SMA20=%.8f | touch=%.8f",
                symbol,
                sma20,
                current_price
            )

            return

        touch_price = position.get(
            "sma_touch_price"
        )

    # --------------------------------------------------------
    # 3. AFTER SMA20 TOUCH:
    #    1% DROP FROM TOUCH PRICE
    # --------------------------------------------------------

    if touch_price:

        sma_drop_price = (
            float(touch_price)
            * (
                1
                - SMA_TOUCH_DROP_PERCENT
            )
        )

        if current_price <= sma_drop_price:

            execute_sell(
                symbol,
                "SMA20_TOUCH_THEN_1%_DROP",
                current_price
            )

            return

    # --------------------------------------------------------
    # 4. UPPER BB TOUCH
    # --------------------------------------------------------

    if current_price >= bb_upper:

        execute_sell(
            symbol,
            "UPPER_BB_TOUCH",
            current_price
        )

        return


# ============================================================
# BUY SIGNAL
# ============================================================

def process_closed_candle(
    symbol,
    df
):

    if df is None:
        return

    if len(df) < 25:
        return

    try:

        # Last row = current/latest
        # Previous row = CLOSED candle
        candle = df.iloc[-2]

        open_price = float(
            candle["open"]
        )

        high_price = float(
            candle["high"]
        )

        close_price = float(
            candle["close"]
        )

        ema5 = float(
            candle["ema5"]
        )

        bb_lower = float(
            candle["bb_lower"]
        )

        if any(
            not math.isfinite(x)
            for x in [
                open_price,
                high_price,
                close_price,
                ema5,
                bb_lower
            ]
        ):
            return

        # ----------------------------------------------------
        # BUY CONDITIONS
        #
        # 1. Open < lower BB
        # 2. Close > lower BB
        # 3. Close < EMA5
        # 4. High < EMA5
        # ----------------------------------------------------

        condition_1 = (
            open_price < bb_lower
        )

        condition_2 = (
            close_price > bb_lower
        )

        condition_3 = (
            close_price < ema5
        )

        condition_4 = (
            high_price < ema5
        )

        if (
            condition_1
            and condition_2
            and condition_3
            and condition_4
        ):

            execute_buy(
                symbol,
                candle
            )

    except Exception as e:

        logger.error(
            "%s candle processing error: %s",
            symbol,
            e
        )


# ============================================================
# WEBSOCKET KLINE PROCESSING
# ============================================================

def process_kline_message(
    symbol,
    kline
):

    try:

        is_closed = kline.get(
            "x",
            False
        )

        open_time = int(
            kline["t"]
        )

        close_time = int(
            kline["T"]
        )

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

        if not all(
            math.isfinite(x)
            for x in [
                open_price,
                high_price,
                low_price,
                close_price,
                volume
            ]
        ):
            return

        with data_lock:

            df = candles.get(
                symbol
            )

            if df is None:

                return

            new_row = pd.DataFrame(
                [{
                    "open_time": open_time,
                    "open": open_price,
                    "high": high_price,
                    "low": low_price,
                    "close": close_price,
                    "volume": volume,
                    "close_time": close_time
                }]
            )

            if not df.empty:

                existing = df[
                    df["open_time"]
                    == open_time
                ]

                if not existing.empty:

                    df.loc[
                        df["open_time"]
                        == open_time,
                        [
                            "open",
                            "high",
                            "low",
                            "close",
                            "volume",
                            "close_time"
                        ]
                    ] = [
                        open_price,
                        high_price,
                        low_price,
                        close_price,
                        volume,
                        close_time
                    ]

                else:

                    df = pd.concat(
                        [
                            df,
                            new_row
                        ],
                        ignore_index=True
                    )

            else:

                df = new_row

            df = df.tail(
                INITIAL_CANDLE_LIMIT
            ).copy()

            df.reset_index(
                drop=True,
                inplace=True
            )

            df = calculate_indicators(
                df
            )

            if df is None:
                return

            candles[symbol] = df

        if is_closed:

            process_closed_candle(
                symbol,
                df
            )

    except Exception as e:

        logger.error(
            "%s kline processing error: %s",
            symbol,
            e
        )


# ============================================================
# WEBSOCKET TICKER PROCESSING
# ============================================================

def process_ticker_message(
    symbol,
    price
):

    try:

        price = float(price)

        if price <= 0:
            return

        if not math.isfinite(price):
            return

        latest_prices[symbol] = price

        check_position(
            symbol,
            price
        )

    except Exception as e:

        logger.error(
            "%s ticker processing error: %s",
            symbol,
            e
        )


# ============================================================
# WEBSOCKET MESSAGE
# ============================================================

def websocket_on_message(
    ws,
    message
):

    try:

        data = json.loads(
            message
        )

        # Combined stream:
        # {"stream":"...", "data":{...}}

        payload = data.get(
            "data",
            data
        )

        event_type = payload.get(
            "e"
        )

        if event_type == "24hrMiniTicker":

            symbol = payload.get(
                "s"
            )

            price = payload.get(
                "c"
            )

            if symbol and price:

                process_ticker_message(
                    symbol,
                    price
                )

            return

        if event_type == "kline":

            kline = payload.get(
                "k"
            )

            if not kline:
                return

            symbol = kline.get(
                "s"
            )

            if symbol:

                process_kline_message(
                    symbol,
                    kline
                )

    except Exception as e:

        logger.error(
            "WebSocket message error: %s",
            e
        )


# ============================================================
# WEBSOCKET CALLBACKS
# ============================================================

def websocket_on_open(ws):

    logger.info(
        "WebSocket connection opened."
    )


def websocket_on_error(
    ws,
    error
):

    logger.error(
        "WebSocket error: %s",
        error
    )


def websocket_on_close(
    ws,
    close_status_code,
    close_msg
):

    logger.warning(
        "WebSocket closed | code=%s | msg=%s",
        close_status_code,
        close_msg
    )


# ============================================================
# WEBSOCKET WORKER
# ============================================================

def websocket_worker(
    symbols,
    worker_id
):

    streams = []

    for symbol in symbols:

        lower = symbol.lower()

        streams.append(
            f"{lower}@miniTicker"
        )

        streams.append(
            f"{lower}@kline_5m"
        )

    if not streams:

        logger.warning(
            "WS worker %s has no streams.",
            worker_id
        )

        return

    stream_path = "/".join(
        streams
    )

    url = (
        "wss://stream.binance.com:9443"
        "/stream?streams="
        + stream_path
    )

    reconnect_delay = WS_RECONNECT_MIN

    while not ws_stop_event.is_set():

        ws = None

        try:

            logger.info(
                "Starting WebSocket worker %s | "
                "symbols=%s | streams=%s",
                worker_id,
                len(symbols),
                len(streams)
            )

            ws = websocket.WebSocketApp(
                url,
                on_open=websocket_on_open,
                on_message=websocket_on_message,
                on_error=websocket_on_error,
                on_close=websocket_on_close
            )

            with data_lock:

                ws_objects.append(
                    ws
                )

            ws.run_forever(
                ping_interval=WS_PING_INTERVAL,
                ping_timeout=WS_PING_TIMEOUT
            )

            reconnect_delay = WS_RECONNECT_MIN

        except Exception as e:

            logger.error(
                "WebSocket worker %s exception: %s",
                worker_id,
                e
            )

        finally:

            with data_lock:

                if ws in ws_objects:

                    try:
                        ws_objects.remove(
                            ws
                        )
                    except ValueError:
                        pass

        if ws_stop_event.is_set():
            break

        logger.warning(
            "WS worker %s reconnecting in %s seconds...",
            worker_id,
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
# STOP WEBSOCKETS
# ============================================================

def stop_websockets():

    logger.info(
        "Stopping WebSocket connections..."
    )

    ws_stop_event.set()

    with data_lock:

        current_ws = list(
            ws_objects
        )

    for ws in current_ws:

        try:
            ws.close()
        except Exception:
            pass

    time.sleep(2)

    with data_lock:

        ws_objects.clear()

        ws_threads.clear()


# ============================================================
# START WEBSOCKETS
# ============================================================

def start_websockets():

    global ws_threads

    stop_websockets()

    ws_stop_event.clear()

    with data_lock:

        symbols = list(
            top_symbols
        )

    if not symbols:

        logger.warning(
            "No symbols available for WebSocket."
        )

        return

    batches = [
        symbols[i:i + WS_BATCH_SIZE]
        for i in range(
            0,
            len(symbols),
            WS_BATCH_SIZE
        )
    ]

    logger.info(
        "Starting %s WebSocket connections...",
        len(batches)
    )

    for index, batch in enumerate(
        batches,
        start=1
    ):

        thread = threading.Thread(
            target=websocket_worker,
            args=(batch, index),
            daemon=True,
            name=f"WS-{index}"
        )

        thread.start()

        ws_threads.append(
            thread
        )

        # Avoid opening many WS connections simultaneously
        time.sleep(2)

    logger.info(
        "WebSocket connections started."
    )


# ============================================================
# REST FALLBACK SAFETY
# ============================================================

def position_safety_loop():

    while True:

        try:

            with positions_lock:

                active_symbols = list(
                    positions.keys()
                )

            now = time.time()

            for symbol in active_symbols:

                last_check = (
                    last_rest_safety_check.get(
                        symbol,
                        0
                    )
                )

                if (
                    now - last_check
                    < REST_SAFETY_INTERVAL
                ):
                    continue

                last_rest_safety_check[
                    symbol
                ] = now

                # Prefer WebSocket price.
                current_price = (
                    latest_prices.get(
                        symbol
                    )
                )

                if current_price:

                    check_position(
                        symbol,
                        current_price
                    )

                    continue

                # REST fallback only when
                # WebSocket price unavailable.

                try:

                    ticker = binance_call(
                        get_client().get_symbol_ticker,
                        symbol=symbol
                    )

                    if ticker:

                        price = float(
                            ticker["price"]
                        )

                        latest_prices[
                            symbol
                        ] = price

                        check_position(
                            symbol,
                            price
                        )

                except Exception as e:

                    logger.error(
                        "%s REST safety check error: %s",
                        symbol,
                        e
                    )

        except Exception as e:

            logger.error(
                "Safety loop error: %s",
                e
            )

        time.sleep(5)


# ============================================================
# TOP SYMBOL REFRESH LOOP
# ============================================================

def symbol_refresh_loop():

    while True:

        time.sleep(
            TOP_SYMBOL_REFRESH_SECONDS
        )

        try:

            old_symbols = set(
                top_symbols
            )

            if not update_top_symbols():

                continue

            new_symbols = set(
                top_symbols
            )

            if old_symbols != new_symbols:

                logger.info(
                    "Top symbol list changed. "
                    "Reloading candles and WebSockets..."
                )

                # Load candles only for newly added symbols
                # to reduce REST requests.

                new_only = [
                    s
                    for s in top_symbols
                    if s not in old_symbols
                ]

                for symbol in new_only:

                    try:

                        raw = binance_call(
                            get_client().get_klines,
                            symbol=symbol,
                            interval=TIMEFRAME,
                            limit=INITIAL_CANDLE_LIMIT
                        )

                        df = clean_dataframe(
                            raw
                        )

                        if df is not None:

                            df = calculate_indicators(
                                df
                            )

                            if df is not None:

                                with data_lock:

                                    candles[
                                        symbol
                                    ] = df

                    except Exception as e:

                        logger.error(
                            "%s refresh candle error: %s",
                            symbol,
                            e
                        )

                start_websockets()

        except Exception as e:

            logger.error(
                "Symbol refresh loop error: %s",
                e
            )


# ============================================================
# BOT INITIALIZATION
# ============================================================

def initialize_bot():

    global bot_initialized

    logger.info("=" * 70)
    logger.info(
        "BINANCE BB20 + EMA5 + SMA20 TOUCH BOT"
    )
    logger.info("=" * 70)

    logger.info(
        "Trade amount: %.2f USDT",
        TRADE_AMOUNT_USDT
    )

    logger.info(
        "Timeframe: 5m"
    )

    logger.info(
        "Top symbols: %s",
        TOP_SYMBOLS_COUNT
    )

    logger.info(
        "Initial SL: %.2f%%",
        INITIAL_STOP_LOSS_PERCENT * 100
    )

    logger.info(
        "SMA20 touch drop: %.2f%%",
        SMA_TOUCH_DROP_PERCENT * 100
    )

    # --------------------------------------------------------
    # Retry forever.
    #
    # This is important because an IP rate-limit ban should
    # NOT kill Gunicorn/Flask.
    # --------------------------------------------------------

    retry_delay = 60

    while not bot_initialized:

        try:

            logger.info(
                "Bot initialization attempt..."
            )

            # Client creation does NOT ping Binance.
            get_client()

            load_exchange_info()

            if not update_top_symbols():

                raise RuntimeError(
                    "Could not update top symbols."
                )

            load_initial_candles()

            recover_positions()

            start_websockets()

            bot_initialized = True

            logger.info("=" * 70)
            logger.info(
                "BOT INITIALIZATION COMPLETE"
            )
            logger.info("=" * 70)

            break

        except BinanceAPIException as e:

            code = getattr(
                e,
                "code",
                None
            )

            if code == -1003:

                logger.error(
                    "Binance returned -1003 "
                    "(rate limit/IP ban)."
                )

                logger.error(
                    "Bot will keep Flask alive and "
                    "retry after %s seconds.",
                    retry_delay
                )

                time.sleep(
                    retry_delay
                )

                retry_delay = min(
                    retry_delay * 2,
                    600
                )

            else:

                logger.error(
                    "Binance initialization error: %s",
                    e
                )

                time.sleep(
                    retry_delay
                )

        except Exception as e:

            logger.error(
                "Bot initialization error: %s",
                e
            )

            logger.info(
                "Retrying in %s seconds...",
                retry_delay
            )

            time.sleep(
                retry_delay
            )

            retry_delay = min(
                retry_delay * 2,
                600
            )


# ============================================================
# START BACKGROUND THREADS
# ============================================================

def start_background_threads():

    global bot_starting

    with data_lock:

        if bot_starting:
            return

        bot_starting = True

    logger.info(
        "Starting bot background initialization..."
    )

    init_thread = threading.Thread(
        target=initialize_bot,
        daemon=True,
        name="BotInit"
    )

    init_thread.start()

    safety_thread = threading.Thread(
        target=position_safety_loop,
        daemon=True,
        name="SafetyLoop"
    )

    safety_thread.start()

    refresh_thread = threading.Thread(
        target=symbol_refresh_loop,
        daemon=True,
        name="SymbolRefresh"
    )

    refresh_thread.start()


# ============================================================
# GUNICORN STARTUP
# ============================================================

# IMPORTANT:
#
# Gunicorn imports:
#
#     main:app
#
# Therefore we start background threads when this module
# is imported.
#
# But Binance Client is NOT created here.
#
# This prevents:
#
#     Client()
#     -> ping()
#     -> -1003
#     -> Gunicorn worker crash
#
# ============================================================

start_background_threads()


# ============================================================
# LOCAL RUN ONLY
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
        port=port,
        threaded=True
    )
