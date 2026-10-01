import os
import time
import json
import queue
import threading
import logging
from decimal import Decimal, ROUND_DOWN

import numpy as np
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
    raise RuntimeError("BINANCE_API_KEY / BINANCE_API_SECRET missing")

client = Client(API_KEY, API_SECRET)


# ============================================================
# TRADING SETTINGS
# ============================================================

TRADE_AMOUNT_USDT = 35.0

TIMEFRAME = Client.KLINE_INTERVAL_5MINUTE

TOP_SYMBOLS = 150


# ============================================================
# BUY INDICATOR SETTINGS
# ============================================================

BB_PERIOD = 20

ADX_PERIOD = 14
ADX_MIN = 20.0


# ============================================================
# RISK SETTINGS
# ============================================================

# Server-side stop loss = 1%
STOP_LOSS_PCT = 0.0100

# Trailing starts after price goes +1%
TRAILING_ACTIVATION_PCT = 0.0100

# After activation, trail by 0.5%
TRAILING_STOP_PCT = 0.0050


# ============================================================
# SYSTEM SETTINGS
# ============================================================

KLINE_REQUEST_COOLDOWN = 2.0
KLINE_REQUEST_DELAY = 0.05

BUY_COOLDOWN_SECONDS = 60

RECONNECT_DELAY = 10

WS_PING_INTERVAL = 30
WS_PING_TIMEOUT = 20

WS_SYMBOLS_PER_CONNECTION = 40


# ============================================================
# SYMBOL FILTERS
# ============================================================

STABLECOINS = {
    "USDT",
    "USDC",
    "BUSD",
    "TUSD",
    "FDUSD",
    "DAI",
    "EUR",
    "GBP",
    "PAX",
    "WBTC",
    "WETH",
}

EXCLUDED_SYMBOLS = {
    "BTCUSDT",
    "ETHUSDT",
}

STOP_CLIENT_PREFIX = "BBADXSL_"


# ============================================================
# GLOBAL STATE
# ============================================================

positions = {}

positions_lock = threading.Lock()

buying_symbols = set()
buying_lock = threading.Lock()

selling_symbols = set()
selling_lock = threading.Lock()

last_kline_request = {}
last_kline_lock = threading.Lock()

last_buy_time = {}
last_buy_lock = threading.Lock()

symbols = []

symbol_info = {}

symbol_info_lock = threading.Lock()


# ============================================================
# WORK QUEUES
# ============================================================

# WebSocket thread কখনো heavy REST কাজ করবে না।
# Closed candle এখানে ঢুকবে।
candle_queue = queue.Queue(maxsize=5000)

queued_candles = set()
queued_candles_lock = threading.Lock()


# ============================================================
# LOGGING
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

logger = logging.getLogger("BBADX_BOT")


# ============================================================
# FLASK
# ============================================================

app = Flask(__name__)


@app.route("/", methods=["GET", "HEAD"])
def home():
    return "Trading Bot is Active & Running!", 200


@app.route("/health", methods=["GET"])
def health():
    with positions_lock:
        position_count = len(positions)

    return jsonify({
        "status": "running",
        "positions": position_count,
        "symbols": len(symbols),
        "timestamp": int(time.time()),
    })


# ============================================================
# DECIMAL HELPERS
# ============================================================

def floor_to_step(value, step):
    """
    Binance LOT_SIZE / PRICE_FILTER step অনুযায়ী
    নিচের দিকে round করে।
    """

    try:
        value = Decimal(str(value))
        step = Decimal(str(step))

        if step <= 0:
            return value

        return (value / step).to_integral_value(
            rounding=ROUND_DOWN
        ) * step

    except Exception:
        return Decimal("0")


def decimal_to_string(value):
    """
    Decimal কে Binance-compatible string এ convert করে।
    """

    try:
        value = Decimal(str(value))
        return format(value, "f")

    except Exception:
        return str(value)


# ============================================================
# EXCHANGE INFO
# ============================================================

def load_exchange_info():

    global symbol_info

    logger.info("Loading Binance exchange information...")

    info = client.get_exchange_info()

    new_symbol_info = {}

    for item in info.get("symbols", []):

        try:
            symbol = item["symbol"]

            if item.get("status") != "TRADING":
                continue

            if item.get("quoteAsset") != "USDT":
                continue

            filters = {
                f["filterType"]: f
                for f in item.get("filters", [])
            }

            lot_filter = filters.get("LOT_SIZE", {})
            price_filter = filters.get("PRICE_FILTER", {})

            min_notional = 0.0

            if "MIN_NOTIONAL" in filters:
                min_notional = float(
                    filters["MIN_NOTIONAL"].get(
                        "minNotional", 0
                    )
                )

            elif "NOTIONAL" in filters:
                min_notional = float(
                    filters["NOTIONAL"].get(
                        "minNotional", 0
                    )
                )

            new_symbol_info[symbol] = {

                "base_asset": item.get("baseAsset"),

                "quote_asset": item.get("quoteAsset"),

                "step_size": float(
                    lot_filter.get("stepSize", 0)
                ),

                "min_qty": float(
                    lot_filter.get("minQty", 0)
                ),

                "min_notional": min_notional,

                "tick_size": float(
                    price_filter.get("tickSize", 0)
                ),
            }

        except Exception as e:

            logger.warning(
                "Exchange info parse error: %s",
                e
            )

    with symbol_info_lock:
        symbol_info = new_symbol_info

    logger.info(
        "Exchange info loaded: %s USDT symbols",
        len(symbol_info)
    )


# ============================================================
# LOAD TOP SYMBOLS
# ============================================================

def load_top_symbols():

    global symbols

    logger.info(
        "Loading top %s USDT symbols...",
        TOP_SYMBOLS
    )

    tickers = client.get_ticker()

    candidates = []

    with symbol_info_lock:
        info_copy = dict(symbol_info)

    for ticker in tickers:

        try:

            symbol = ticker.get("symbol")

            if symbol not in info_copy:
                continue

            if symbol in EXCLUDED_SYMBOLS:
                continue

            base_asset = info_copy[symbol]["base_asset"]

            if base_asset in STABLECOINS:
                continue

            quote_volume = float(
                ticker.get("quoteVolume", 0)
            )

            candidates.append(
                (symbol, quote_volume)
            )

        except Exception:
            continue

    candidates.sort(
        key=lambda x: x[1],
        reverse=True
    )

    symbols = [
        item[0]
        for item in candidates[:TOP_SYMBOLS]
    ]

    logger.info(
        "Selected %s symbols",
        len(symbols)
    )

    logger.info(
        "Symbols: %s",
        ", ".join(symbols)
    )


# ============================================================
# CURRENT PRICE
# ============================================================

def get_current_price(symbol):

    try:

        ticker = client.get_symbol_ticker(
            symbol=symbol
        )

        return float(ticker["price"])

    except Exception as e:

        logger.error(
            "%s | Current price error: %s",
            symbol,
            e
        )

        return None


# ============================================================
# GET CLOSED KLINES
# ============================================================

def get_closed_klines(symbol):

    now = time.time()

    with last_kline_lock:

        previous = last_kline_request.get(
            symbol,
            0
        )

        if now - previous < KLINE_REQUEST_COOLDOWN:
            return None

        last_kline_request[symbol] = now

    time.sleep(KLINE_REQUEST_DELAY)

    try:

        raw = client.get_klines(
            symbol=symbol,
            interval=TIMEFRAME,
            limit=100,
        )

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
            "taker_buy_base",
            "taker_buy_quote",
            "ignore",
        ]

        # Explicit deep copy
        df = pd.DataFrame(
            raw,
            columns=columns
        ).copy(deep=True)

        numeric_columns = [
            "open",
            "high",
            "low",
            "close",
            "volume",
            "quote_volume",
            "taker_buy_base",
            "taker_buy_quote",
        ]

        for col in numeric_columns:

            converted = pd.to_numeric(
                df[col],
                errors="coerce"
            ).astype("float64")

            df.loc[:, col] = converted

        df = df.dropna(
            subset=[
                "open",
                "high",
                "low",
                "close",
                "volume",
            ]
        ).copy(deep=True)

        if len(df) < 50:
            return None

        return df

    except BinanceAPIException as e:

        logger.error(
            "%s | Kline Binance API error: %s",
            symbol,
            e
        )

        return None

    except Exception as e:

        logger.error(
            "%s | Kline request error: %s",
            symbol,
            e
        )

        return None


# ============================================================
# CALCULATE INDICATORS
# ============================================================

def calculate_indicators(df):

    try:

        if df is None:
            return None

        if len(df) < 50:
            return None

        # Important:
        # Always make an independent DataFrame.
        df = df.copy(deep=True)

        required_columns = [
            "open",
            "high",
            "low",
            "close",
            "volume",
        ]

        for col in required_columns:

            df.loc[:, col] = (
                pd.to_numeric(
                    df[col],
                    errors="coerce"
                )
                .astype("float64")
            )

        df = df.dropna(
            subset=required_columns
        ).copy(deep=True)

        if len(df) < 50:
            return None

        # ====================================================
        # EMA 5
        # ====================================================

        df.loc[:, "ema5"] = (
            df["close"]
            .ewm(
                span=5,
                adjust=False
            )
            .mean()
            .astype("float64")
        )

        # ====================================================
        # BOLLINGER BAND 20
        # ====================================================

        bb_middle = (
            df["close"]
            .rolling(
                window=BB_PERIOD,
                min_periods=BB_PERIOD
            )
            .mean()
        )

        bb_std = (
            df["close"]
            .rolling(
                window=BB_PERIOD,
                min_periods=BB_PERIOD
            )
            .std(ddof=0)
        )

        df.loc[:, "bb_middle"] = (
            bb_middle.astype("float64")
        )

        df.loc[:, "bb_upper"] = (
            (bb_middle + 2.0 * bb_std)
            .astype("float64")
        )

        df.loc[:, "bb_lower"] = (
            (bb_middle - 2.0 * bb_std)
            .astype("float64")
        )

        # ====================================================
        # ADX 14
        # ====================================================

        high = df["high"].astype("float64")
        low = df["low"].astype("float64")
        close = df["close"].astype("float64")

        previous_close = (
            close.shift(1)
            .astype("float64")
        )

        # ----------------------------------------------------
        # True Range
        # ----------------------------------------------------

        tr1 = (
            high - low
        ).abs().astype("float64")

        tr2 = (
            high - previous_close
        ).abs().astype("float64")

        tr3 = (
            low - previous_close
        ).abs().astype("float64")

        tr_df = pd.concat(
            [
                tr1.rename("tr1"),
                tr2.rename("tr2"),
                tr3.rename("tr3"),
            ],
            axis=1
        ).astype("float64")

        # All columns are guaranteed numeric float64.
        # This avoids:
        # "No numeric types to aggregate"
        tr = (
            tr_df
            .max(axis=1, skipna=True)
            .astype("float64")
        )

        # ----------------------------------------------------
        # Directional Movement
        # ----------------------------------------------------

        up_move = (
            high.diff()
            .astype("float64")
        )

        down_move = (
            -low.diff()
            .astype("float64")
        )

        plus_condition = (
            (up_move > down_move)
            & (up_move > 0)
        )

        minus_condition = (
            (down_move > up_move)
            & (down_move > 0)
        )

        # No chained assignment.
        plus_dm = (
            up_move
            .where(
                plus_condition,
                0.0
            )
            .fillna(0.0)
            .astype("float64")
        )

        minus_dm = (
            down_move
            .where(
                minus_condition,
                0.0
            )
            .fillna(0.0)
            .astype("float64")
        )

        # ----------------------------------------------------
        # Wilder-style smoothing
        # ----------------------------------------------------

        alpha = 1.0 / ADX_PERIOD

        atr = (
            tr
            .ewm(
                alpha=alpha,
                adjust=False
            )
            .mean()
            .astype("float64")
        )

        plus_dm_smoothed = (
            plus_dm
            .ewm(
                alpha=alpha,
                adjust=False
            )
            .mean()
            .astype("float64")
        )

        minus_dm_smoothed = (
            minus_dm
            .ewm(
                alpha=alpha,
                adjust=False
            )
            .mean()
            .astype("float64")
        )

        # ----------------------------------------------------
        # DI+
        # ----------------------------------------------------

        atr_safe = (
            atr
            .replace(
                [np.inf, -np.inf],
                np.nan
            )
        )

        atr_safe = (
            atr_safe
            .mask(
                atr_safe <= 0,
                np.nan
            )
            .astype("float64")
        )

        plus_di = (
            100.0
            * plus_dm_smoothed
            / atr_safe
        ).astype("float64")

        minus_di = (
            100.0
            * minus_dm_smoothed
            / atr_safe
        ).astype("float64")

        # ----------------------------------------------------
        # DX
        # ----------------------------------------------------

        di_sum = (
            plus_di + minus_di
        ).astype("float64")

        di_sum = (
            di_sum
            .mask(
                di_sum <= 0,
                np.nan
            )
            .astype("float64")
        )

        dx = (
            100.0
            * (
                plus_di - minus_di
            ).abs()
            / di_sum
        ).astype("float64")

        # ----------------------------------------------------
        # ADX
        # ----------------------------------------------------

        adx = (
            dx
            .ewm(
                alpha=alpha,
                adjust=False
            )
            .mean()
            .astype("float64")
        )

        # Explicit .loc assignment
        df.loc[:, "plus_di"] = (
            plus_di
            .reindex(df.index)
            .astype("float64")
        )

        df.loc[:, "minus_di"] = (
            minus_di
            .reindex(df.index)
            .astype("float64")
        )

        df.loc[:, "adx"] = (
            adx
            .reindex(df.index)
            .astype("float64")
        )

        return df

    except Exception as e:

        logger.exception(
            "Indicator calculation error: %s",
            e
        )

        return None


# ============================================================
# BUY CONDITION
# ============================================================

def check_buy_condition(symbol):

    df = get_closed_klines(symbol)

    if df is None:
        return False

    df = calculate_indicators(df)

    if df is None:
        return False

    if len(df) < 3:
        return False

    # --------------------------------------------------------
    # IMPORTANT:
    # -1 = currently forming candle
    # -2 = last fully closed candle
    # --------------------------------------------------------

    candle = df.iloc[-2]

    try:

        open_price = float(candle["open"])
        close_price = float(candle["close"])

        bb_lower = float(candle["bb_lower"])

        adx = float(candle["adx"])

        plus_di = float(candle["plus_di"])
        minus_di = float(candle["minus_di"])

    except Exception as e:

        logger.error(
            "%s | BUY data conversion error: %s",
            symbol,
            e
        )

        return False

    if not all(
        np.isfinite(x)
        for x in [
            open_price,
            close_price,
            bb_lower,
            adx,
            plus_di,
            minus_di,
        ]
    ):
        return False

    # ========================================================
    # YOUR SAME BUY RULE
    # ========================================================

    condition_1 = (
        open_price < bb_lower
    )

    condition_2 = (
        close_price > bb_lower
    )

    condition_3 = (
        adx > ADX_MIN
    )

    condition_4 = (
        plus_di > minus_di
    )

    if (
        condition_1
        and condition_2
        and condition_3
        and condition_4
    ):

        logger.info(
            "%s | BUY SIGNAL | "
            "Open=%.8f | Close=%.8f | "
            "BBLower=%.8f | ADX=%.2f | "
            "DI+=%.2f | DI-=%.2f",
            symbol,
            open_price,
            close_price,
            bb_lower,
            adx,
            plus_di,
            minus_di,
        )

        return True

    return False


# ============================================================
# QUANTITY NORMALIZATION
# ============================================================

def normalize_quantity(symbol, quantity):

    with symbol_info_lock:
        info = symbol_info.get(symbol)

    if not info:
        return 0.0

    step = info["step_size"]
    min_qty = info["min_qty"]

    if step <= 0:
        return 0.0

    qty = floor_to_step(
        quantity,
        step
    )

    if qty < Decimal(str(min_qty)):
        return 0.0

    return float(qty)


# ============================================================
# PRICE NORMALIZATION
# ============================================================

def normalize_price(symbol, price):

    with symbol_info_lock:
        info = symbol_info.get(symbol)

    if not info:
        return float(price)

    tick_size = info["tick_size"]

    if tick_size <= 0:
        return float(price)

    result = floor_to_step(
        price,
        tick_size
    )

    return float(result)


# ============================================================
# SERVER STOP LOSS
# ============================================================

def place_server_stop_loss(
    symbol,
    quantity,
    entry_price
):

    try:

        stop_price = (
            float(entry_price)
            * (1.0 - STOP_LOSS_PCT)
        )

        stop_price = normalize_price(
            symbol,
            stop_price
        )

        quantity = normalize_quantity(
            symbol,
            quantity
        )

        if quantity <= 0:
            logger.error(
                "%s | Invalid stop quantity",
                symbol
            )
            return None

        if stop_price <= 0:
            logger.error(
                "%s | Invalid stop price",
                symbol
            )
            return None

        client_order_id = (
            STOP_CLIENT_PREFIX
            + symbol
            + "_"
            + str(int(time.time() * 1000))
        )

        order = client.create_order(
            symbol=symbol,
            side="SELL",
            type="STOP_LOSS",
            quantity=decimal_to_string(
                quantity
            ),
            stopPrice=decimal_to_string(
                stop_price
            ),
            newClientOrderId=client_order_id,
            newOrderRespType="RESULT",
        )

        order_id = order.get("orderId")

        logger.info(
            "%s | Server SL placed | "
            "Entry=%.8f | SL=%.8f | OrderID=%s",
            symbol,
            entry_price,
            stop_price,
            order_id,
        )

        return order_id

    except BinanceAPIException as e:

        logger.error(
            "%s | Server SL API error: %s",
            symbol,
            e
        )

        return None

    except Exception as e:

        logger.error(
            "%s | Server SL error: %s",
            symbol,
            e
        )

        return None


# ============================================================
# FIND EXISTING SERVER STOP
# ============================================================

def find_existing_server_stop(symbol):

    try:

        open_orders = client.get_open_orders(
            symbol=symbol
        )

        for order in open_orders:

            if order.get("side") != "SELL":
                continue

            client_id = (
                order.get("clientOrderId")
                or ""
            )

            if not client_id.startswith(
                STOP_CLIENT_PREFIX
            ):
                continue

            if order.get("type") != "STOP_LOSS":
                continue

            return order.get("orderId")

        return None

    except Exception as e:

        logger.error(
            "%s | Find server SL error: %s",
            symbol,
            e
        )

        return None


# ============================================================
# GET STOP ORDER STATUS
# ============================================================

def get_stop_order_status(
    symbol,
    order_id
):

    try:

        order = client.get_order(
            symbol=symbol,
            orderId=order_id
        )

        return order.get("status")

    except Exception as e:

        logger.error(
            "%s | Stop order status error: %s",
            symbol,
            e
        )

        return None


# ============================================================
# CANCEL SERVER STOP
# ============================================================

def cancel_server_stop(
    symbol,
    order_id
):

    if not order_id:
        return True

    try:

        client.cancel_order(
            symbol=symbol,
            orderId=order_id
        )

        logger.info(
            "%s | Server SL cancelled | OrderID=%s",
            symbol,
            order_id
        )

        return True

    except BinanceAPIException as e:

        # If already filled/cancelled, don't treat
        # it as a fatal problem.
        logger.warning(
            "%s | Cancel SL API response: %s",
            symbol,
            e
        )

        return False

    except Exception as e:

        logger.error(
            "%s | Cancel SL error: %s",
            symbol,
            e
        )

        return False


# ============================================================
# MARKET SELL
# ============================================================

def market_sell(
    symbol,
    quantity
):

    try:

        quantity = normalize_quantity(
            symbol,
            quantity
        )

        if quantity <= 0:
            logger.error(
                "%s | Invalid market sell quantity",
                symbol
            )
            return None

        order = client.order_market_sell(
            symbol=symbol,
            quantity=decimal_to_string(
                quantity
            )
        )

        logger.info(
            "%s | MARKET SELL executed | Qty=%s",
            symbol,
            quantity
        )

        return order

    except BinanceAPIException as e:

        logger.error(
            "%s | Market sell API error: %s",
            symbol,
            e
        )

        return None

    except Exception as e:

        logger.error(
            "%s | Market sell error: %s",
            symbol,
            e
        )

        return None


# ============================================================
# BUY SYMBOL
# ============================================================

def buy_symbol(symbol):

    with buying_lock:

        if symbol in buying_symbols:
            return

        buying_symbols.add(symbol)

    try:

        with positions_lock:

            if symbol in positions:
                return

        now = time.time()

        with last_buy_lock:

            previous_buy = last_buy_time.get(
                symbol,
                0
            )

            if (
                now - previous_buy
                < BUY_COOLDOWN_SECONDS
            ):
                return

            last_buy_time[symbol] = now

        logger.info(
            "%s | Sending MARKET BUY | Amount=%.2f USDT",
            symbol,
            TRADE_AMOUNT_USDT
        )

        order = client.order_market_buy(
            symbol=symbol,
            quoteOrderQty=decimal_to_string(
                TRADE_AMOUNT_USDT
            )
        )

        executed_qty = float(
            order.get(
                "executedQty",
                0
            )
        )

        fills = order.get(
            "fills",
            []
        )

        total_cost = 0.0
        total_qty = 0.0

        for fill in fills:

            try:

                fill_price = float(
                    fill["price"]
                )

                fill_qty = float(
                    fill["qty"]
                )

                total_cost += (
                    fill_price * fill_qty
                )

                total_qty += fill_qty

            except Exception:
                continue

        if total_qty > 0:

            entry_price = (
                total_cost
                / total_qty
            )

        else:

            entry_price = (
                get_current_price(symbol)
            )

        if entry_price is None:

            logger.error(
                "%s | Cannot determine entry price",
                symbol
            )

            return

        quantity = normalize_quantity(
            symbol,
            executed_qty
        )

        if quantity <= 0:

            logger.error(
                "%s | Invalid executed quantity: %s",
                symbol,
                executed_qty
            )

            return

        logger.info(
            "%s | BUY executed | "
            "Entry=%.8f | Qty=%.8f",
            symbol,
            entry_price,
            quantity
        )

        # ====================================================
        # SERVER STOP LOSS
        # ====================================================

        stop_order_id = (
            place_server_stop_loss(
                symbol,
                quantity,
                entry_price
            )
        )

        # ====================================================
        # SAFETY:
        # If server SL cannot be placed,
        # immediately market sell.
        # ====================================================

        if not stop_order_id:

            logger.error(
                "%s | Server SL FAILED. "
                "Emergency MARKET SELL.",
                symbol
            )

            market_sell(
                symbol,
                quantity
            )

            return

        with positions_lock:

            positions[symbol] = {

                "entry_price": float(
                    entry_price
                ),

                "quantity": float(
                    quantity
                ),

                "highest_price": float(
                    entry_price
                ),

                "trailing_active": False,

                "stop_order_id": stop_order_id,

                "stop_price": normalize_price(
                    symbol,
                    entry_price
                    * (
                        1.0
                        - STOP_LOSS_PCT
                    )
                ),
            }

        logger.info(
            "%s | POSITION OPENED | "
            "Entry=%.8f | SL=%.8f | "
            "Trailing activates=%.8f",
            symbol,
            entry_price,
            entry_price * (
                1.0 - STOP_LOSS_PCT
            ),
            entry_price * (
                1.0 + TRAILING_ACTIVATION_PCT
            ),
        )

    except BinanceAPIException as e:

        logger.error(
            "%s | BUY API error: %s",
            symbol,
            e
        )

    except Exception as e:

        logger.exception(
            "%s | BUY error: %s",
            symbol,
            e
        )

    finally:

        with buying_lock:
            buying_symbols.discard(symbol)


# ============================================================
# CHECK POSITION
# ============================================================

def check_position(
    symbol,
    current_price
):

    try:

        with positions_lock:

            position = positions.get(
                symbol
            )

            if not position:
                return

            # Make copy so calculations don't hold lock
            position = dict(position)

        entry_price = float(
            position["entry_price"]
        )

        quantity = float(
            position["quantity"]
        )

        highest_price = float(
            position["highest_price"]
        )

        trailing_active = bool(
            position["trailing_active"]
        )

        stop_order_id = position.get(
            "stop_order_id"
        )

        # ====================================================
        # UPDATE HIGHEST PRICE
        # ====================================================

        if current_price > highest_price:

            highest_price = current_price

            with positions_lock:

                if symbol in positions:

                    positions[symbol][
                        "highest_price"
                    ] = highest_price

        # ====================================================
        # SERVER SL LEVEL
        # ====================================================

        server_stop_price = (
            entry_price
            * (1.0 - STOP_LOSS_PCT)
        )

        # ====================================================
        # SERVER STOP CHECK
        # ====================================================

        if (
            current_price
            <= server_stop_price
            and stop_order_id
        ):

            status = (
                get_stop_order_status(
                    symbol,
                    stop_order_id
                )
            )

            if status == "FILLED":

                logger.info(
                    "%s | Server SL FILLED",
                    symbol
                )

                with positions_lock:
                    positions.pop(
                        symbol,
                        None
                    )

                return

            elif status in {
                "CANCELED",
                "EXPIRED",
                "REJECTED",
            }:

                logger.warning(
                    "%s | Server SL status=%s. "
                    "Emergency MARKET SELL.",
                    symbol,
                    status
                )

                sell_symbol(
                    symbol,
                    reason="SERVER_SL_FALLBACK"
                )

                return

            elif status == "NEW":

                # Binance server-side SL is active.
                return

        # ====================================================
        # TRAILING ACTIVATION
        # ====================================================

        activation_price = (
            entry_price
            * (
                1.0
                + TRAILING_ACTIVATION_PCT
            )
        )

        if (
            not trailing_active
            and current_price
            >= activation_price
        ):

            trailing_active = True

            with positions_lock:

                if symbol in positions:

                    positions[symbol][
                        "trailing_active"
                    ] = True

            logger.info(
                "%s | TRAILING ACTIVATED | "
                "Price=%.8f | Activation=%.8f",
                symbol,
                current_price,
                activation_price,
            )

        # ====================================================
        # TRAILING STOP
        # ====================================================

        if trailing_active:

            trailing_stop = (
                highest_price
                * (
                    1.0
                    - TRAILING_STOP_PCT
                )
            )

            if current_price <= trailing_stop:

                logger.info(
                    "%s | TRAILING STOP HIT | "
                    "Current=%.8f | Highest=%.8f | "
                    "TrailingStop=%.8f",
                    symbol,
                    current_price,
                    highest_price,
                    trailing_stop,
                )

                sell_symbol(
                    symbol,
                    reason="TRAILING_STOP"
                )

    except Exception as e:

        logger.error(
            "%s | Position check error: %s",
            symbol,
            e
        )


# ============================================================
# SELL SYMBOL
# ============================================================

def sell_symbol(
    symbol,
    reason="MANUAL"
):

    with selling_lock:

        if symbol in selling_symbols:
            return

        selling_symbols.add(symbol)

    try:

        with positions_lock:

            position = positions.get(
                symbol
            )

            if not position:
                return

            quantity = float(
                position["quantity"]
            )

            stop_order_id = position.get(
                "stop_order_id"
            )

        logger.info(
            "%s | SELL START | Reason=%s",
            symbol,
            reason
        )

        # ====================================================
        # CANCEL SERVER STOP FIRST
        # ====================================================

        if stop_order_id:

            cancel_server_stop(
                symbol,
                stop_order_id
            )

            time.sleep(0.1)

        # ====================================================
        # MARKET SELL
        # ====================================================

        order = market_sell(
            symbol,
            quantity
        )

        if order:

            with positions_lock:

                positions.pop(
                    symbol,
                    None
                )

            logger.info(
                "%s | POSITION CLOSED | Reason=%s",
                symbol,
                reason
            )

        else:

            logger.error(
                "%s | MARKET SELL FAILED | "
                "Position kept in memory for retry.",
                symbol
            )

    except Exception as e:

        logger.exception(
            "%s | SELL error: %s",
            symbol,
            e
        )

    finally:

        with selling_lock:
            selling_symbols.discard(symbol)


# ============================================================
# RECOVER ENTRY PRICE FROM TRADES
# ============================================================

def recover_entry_price_from_trades(
    symbol,
    base_asset
):

    try:

        trades = client.get_my_trades(
            symbol=symbol,
            limit=1000
        )

        total_buy_qty = 0.0
        total_buy_cost = 0.0

        for trade in trades:

            try:

                qty = float(
                    trade["qty"]
                )

                price = float(
                    trade["price"]
                )

                is_buyer = bool(
                    trade["isBuyer"]
                )

                if is_buyer:

                    total_buy_qty += qty

                    total_buy_cost += (
                        qty * price
                    )

                else:

                    sell_qty = min(
                        qty,
                        total_buy_qty
                    )

                    if (
                        total_buy_qty
                        > 0
                    ):

                        average_buy_price = (
                            total_buy_cost
                            / total_buy_qty
                        )

                        total_buy_qty -= (
                            sell_qty
                        )

                        total_buy_cost -= (
                            sell_qty
                            * average_buy_price
                        )

            except Exception:
                continue

        if total_buy_qty > 0:

            return (
                total_buy_cost
                / total_buy_qty
            )

    except Exception as e:

        logger.warning(
            "%s | Recover entry error: %s",
            symbol,
            e
        )

    return None


# ============================================================
# RECOVER EXISTING POSITIONS
# ============================================================

def recover_positions():

    logger.info(
        "Checking existing Binance balances..."
    )

    try:

        account = client.get_account()

        balances = account.get(
            "balances",
            []
        )

        recovered = 0

        for balance in balances:

            try:

                asset = balance["asset"]

                free = float(
                    balance["free"]
                )

                locked = float(
                    balance["locked"]
                )

                total_balance = (
                    free + locked
                )

                if total_balance <= 0:
                    continue

                if asset in STABLECOINS:
                    continue

                symbol = (
                    asset
                    + "USDT"
                )

                if symbol in EXCLUDED_SYMBOLS:
                    continue

                with symbol_info_lock:

                    if symbol not in symbol_info:
                        continue

                current_price = (
                    get_current_price(
                        symbol
                    )
                )

                if current_price is None:
                    continue

                # Try to determine entry
                entry_price = (
                    recover_entry_price_from_trades(
                        symbol,
                        asset
                    )
                )

                if entry_price is None:

                    # Fallback
                    entry_price = (
                        current_price
                    )

                quantity = (
                    normalize_quantity(
                        symbol,
                        total_balance
                    )
                )

                if quantity <= 0:
                    continue

                stop_order_id = (
                    find_existing_server_stop(
                        symbol
                    )
                )

                # If no server SL exists,
                # create one immediately.
                if not stop_order_id:

                    logger.warning(
                        "%s | Existing position "
                        "has no server SL. "
                        "Creating one.",
                        symbol
                    )

                    stop_order_id = (
                        place_server_stop_loss(
                            symbol,
                            quantity,
                            entry_price
                        )
                    )

                if not stop_order_id:

                    logger.error(
                        "%s | Could not create "
                        "recovery SL. Skipping.",
                        symbol
                    )

                    continue

                activation_price = (
                    entry_price
                    * (
                        1.0
                        + TRAILING_ACTIVATION_PCT
                    )
                )

                trailing_active = (
                    current_price
                    >= activation_price
                )

                with positions_lock:

                    positions[symbol] = {

                        "entry_price": float(
                            entry_price
                        ),

                        "quantity": float(
                            quantity
                        ),

                        "highest_price": max(
                            float(entry_price),
                            float(current_price)
                        ),

                        "trailing_active":
                            trailing_active,

                        "stop_order_id":
                            stop_order_id,

                        "stop_price":
                            normalize_price(
                                symbol,
                                entry_price
                                * (
                                    1.0
                                    - STOP_LOSS_PCT
                                )
                            ),
                    }

                recovered += 1

                logger.info(
                    "%s | Position recovered | "
                    "Qty=%.8f | Entry=%.8f | "
                    "Current=%.8f",
                    symbol,
                    quantity,
                    entry_price,
                    current_price,
                )

            except Exception as e:

                logger.error(
                    "Position recovery error: %s",
                    e
                )

        logger.info(
            "Position recovery complete | "
            "Recovered=%s",
            recovered
        )

    except Exception as e:

        logger.exception(
            "Account recovery error: %s",
            e
        )


# ============================================================
# PROCESS CLOSED CANDLE
# ============================================================

def process_closed_candle(symbol):

    try:

        with positions_lock:

            if symbol in positions:
                return

        if check_buy_condition(symbol):

            buy_symbol(symbol)

    except Exception as e:

        logger.error(
            "%s | Candle processing error: %s",
            symbol,
            e
        )


# ============================================================
# CANDLE QUEUE
# ============================================================

def enqueue_closed_candle(symbol):

    with queued_candles_lock:

        if symbol in queued_candles:
            return

        queued_candles.add(symbol)

    try:

        candle_queue.put_nowait(
            symbol
        )

    except queue.Full:

        logger.warning(
            "Candle queue full. Dropping %s",
            symbol
        )

        with queued_candles_lock:
            queued_candles.discard(
                symbol
            )


# ============================================================
# CANDLE WORKER
# ============================================================

def candle_worker():

    logger.info(
        "Candle worker started"
    )

    while True:

        symbol = None

        try:

            symbol = candle_queue.get()

            process_closed_candle(
                symbol
            )

        except Exception as e:

            logger.exception(
                "Candle worker error: %s",
                e
            )

        finally:

            if symbol:

                with queued_candles_lock:

                    queued_candles.discard(
                        symbol
                    )

            candle_queue.task_done()


# ============================================================
# WEBSOCKET MESSAGE PROCESSOR
# ============================================================

def process_ws_message(message):

    try:

        data = json.loads(message)

        payload = data.get(
            "data",
            data
        )

        event_type = payload.get(
            "e"
        )

        # ====================================================
        # KLINE
        # ====================================================

        if event_type == "kline":

            kline = payload.get(
                "k",
                {}
            )

            symbol = kline.get(
                "s"
            )

            candle_closed = bool(
                kline.get("x", False)
            )

            if (
                symbol
                and candle_closed
            ):

                # IMPORTANT:
                # Do NOT calculate indicators here.
                # Put symbol into worker queue.
                enqueue_closed_candle(
                    symbol
                )

        # ====================================================
        # MINI TICKER
        # ====================================================

        elif event_type == "24hrMiniTicker":

            symbol = payload.get(
                "s"
            )

            price_text = payload.get(
                "c"
            )

            if not symbol or not price_text:
                return

            try:

                current_price = float(
                    price_text
                )

            except Exception:
                return

            with positions_lock:

                has_position = (
                    symbol in positions
                )

            if has_position:

                # Position check is normally lightweight.
                check_position(
                    symbol,
                    current_price
                )

    except json.JSONDecodeError:

        logger.warning(
            "WebSocket received invalid JSON"
        )

    except Exception as e:

        logger.error(
            "WebSocket message processing error: %s",
            e
        )


# ============================================================
# WEBSOCKET URL
# ============================================================

def make_stream_url(
    symbol_chunk
):

    streams = []

    for symbol in symbol_chunk:

        lower_symbol = symbol.lower()

        streams.append(
            lower_symbol
            + "@kline_5m"
        )

        streams.append(
            lower_symbol
            + "@miniTicker"
        )

    return (
        "wss://stream.binance.com:9443"
        "/stream?streams="
        + "/".join(streams)
    )


# ============================================================
# WEBSOCKET WORKER
# ============================================================

def websocket_worker(
    symbol_chunk,
    worker_id
):

    url = make_stream_url(
        symbol_chunk
    )

    logger.info(
        "WebSocket worker %s starting | "
        "Symbols=%s",
        worker_id,
        len(symbol_chunk)
    )

    while True:

        try:

            def on_open(ws):

                logger.info(
                    "WebSocket worker %s connected",
                    worker_id
                )

            def on_message(
                ws,
                message
            ):

                # Keep this callback very fast.
                process_ws_message(
                    message
                )

            def on_error(
                ws,
                error
            ):

                logger.error(
                    "WebSocket worker %s error: %s",
                    worker_id,
                    error
                )

            def on_close(
                ws,
                close_status_code,
                close_msg
            ):

                logger.warning(
                    "WebSocket worker %s closed | "
                    "Code=%s | Msg=%s",
                    worker_id,
                    close_status_code,
                    close_msg
                )

            ws = websocket.WebSocketApp(
                url,
                on_open=on_open,
                on_message=on_message,
                on_error=on_error,
                on_close=on_close,
            )

            ws.run_forever(
                ping_interval=WS_PING_INTERVAL,
                ping_timeout=WS_PING_TIMEOUT,
                ping_payload="ping",
            )

        except Exception as e:

            logger.exception(
                "WebSocket worker %s exception: %s",
                worker_id,
                e
            )

        logger.info(
            "WebSocket worker %s reconnecting in %s seconds...",
            worker_id,
            RECONNECT_DELAY
        )

        time.sleep(
            RECONNECT_DELAY
        )


# ============================================================
# WEBSOCKET LOOP
# ============================================================

def websocket_loop():

    if not symbols:

        logger.error(
            "No symbols available for WebSocket"
        )

        return

    chunks = [
        symbols[i:i + WS_SYMBOLS_PER_CONNECTION]
        for i in range(
            0,
            len(symbols),
            WS_SYMBOLS_PER_CONNECTION
        )
    ]

    logger.info(
        "Starting %s WebSocket connections...",
        len(chunks)
    )

    for index, chunk in enumerate(
        chunks,
        start=1
    ):

        thread = threading.Thread(
            target=websocket_worker,
            args=(chunk, index),
            daemon=True,
            name=f"WS-{index}",
        )

        thread.start()

        # Avoid opening all connections
        # at exactly the same moment.
        time.sleep(0.5)


# ============================================================
# POSITION MONITOR
# ============================================================

def position_monitor():

    logger.info(
        "Position monitor started"
    )

    while True:

        try:

            with positions_lock:

                active = list(
                    positions.items()
                )

            if active:

                logger.info(
                    "Active positions: %s",
                    len(active)
                )

                for symbol, position in active:

                    try:

                        current_price = (
                            get_current_price(
                                symbol
                            )
                        )

                        if current_price is None:
                            continue

                        check_position(
                            symbol,
                            current_price
                        )

                    except Exception as e:

                        logger.error(
                            "%s | Monitor error: %s",
                            symbol,
                            e
                        )

            time.sleep(60)

        except Exception as e:

            logger.exception(
                "Position monitor error: %s",
                e
            )

            time.sleep(10)


# ============================================================
# STARTUP
# ============================================================

def startup():

    logger.info(
        "=" * 70
    )

    logger.info(
        "BB20 + ADX14 BINANCE SPOT BOT STARTING"
    )

    logger.info(
        "BUY RULE → "
        "OPEN < BB20 LOWER + "
        "CLOSE > BB20 LOWER + "
        "ADX14 > 20 + DI+ > DI-"
    )

    logger.info(
        "TRADE AMOUNT → %.2f USDT",
        TRADE_AMOUNT_USDT
    )

    logger.info(
        "SERVER STOP LOSS → %.2f%%",
        STOP_LOSS_PCT * 100
    )

    logger.info(
        "TRAILING ACTIVATION → +%.2f%%",
        TRAILING_ACTIVATION_PCT * 100
    )

    logger.info(
        "TRAILING DISTANCE → %.2f%%",
        TRAILING_STOP_PCT * 100
    )

    logger.info(
        "TIMEFRAME → 5 MINUTES"
    )

    logger.info(
        "TOP SYMBOLS → %s",
        TOP_SYMBOLS
    )

    logger.info(
        "=" * 70
    )

    # --------------------------------------------------------
    # Exchange information
    # --------------------------------------------------------

    load_exchange_info()

    # --------------------------------------------------------
    # Top symbols
    # --------------------------------------------------------

    load_top_symbols()

    # --------------------------------------------------------
    # Recover existing positions
    # --------------------------------------------------------

    recover_positions()

    # --------------------------------------------------------
    # Candle worker
    # --------------------------------------------------------

    worker = threading.Thread(
        target=candle_worker,
        daemon=True,
        name="CandleWorker",
    )

    worker.start()

    # --------------------------------------------------------
    # Position monitor
    # --------------------------------------------------------

    monitor = threading.Thread(
        target=position_monitor,
        daemon=True,
        name="PositionMonitor",
    )

    monitor.start()

    # --------------------------------------------------------
    # WebSockets
    # --------------------------------------------------------

    ws_thread = threading.Thread(
        target=websocket_loop,
        daemon=True,
        name="WebSocketManager",
    )

    ws_thread.start()

    logger.info(
        "All trading workers started."
    )


# ============================================================
# MAIN
# ============================================================

if __name__ == "__main__":

    startup()

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
    )
