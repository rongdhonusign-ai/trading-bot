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
# LOGGING
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

logger = logging.getLogger("BB_ADX_BOT")


# ============================================================
# CONFIG
# ============================================================

API_KEY = os.environ.get("BINANCE_API_KEY")
API_SECRET = os.environ.get("BINANCE_API_SECRET")

if not API_KEY or not API_SECRET:
    raise RuntimeError(
        "BINANCE_API_KEY / BINANCE_API_SECRET missing"
    )


client = Client(API_KEY, API_SECRET)


# ============================================================
# TRADING SETTINGS
# ============================================================

TRADE_AMOUNT_USDT = 35.0

TIMEFRAME = Client.KLINE_INTERVAL_5MINUTE

TOP_SYMBOLS = 150


# ============================================================
# INDICATORS
# ============================================================

BB_PERIOD = 20

ADX_PERIOD = 14

ADX_MIN = 20.0


# ============================================================
# STOP LOSS / TRAILING
# ============================================================

STOP_LOSS_PCT = 0.0100
# 1%

TRAILING_ACTIVATION_PCT = 0.0100
# +1%

TRAILING_STOP_PCT = 0.0050
# 0.5% below highest price


# ============================================================
# PERFORMANCE SETTINGS
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
# CANDLE QUEUE
# ============================================================

candle_queue = queue.Queue(
    maxsize=5000
)

queued_candles = set()

queued_candles_lock = threading.Lock()


# ============================================================
# LATEST PRICE
# ============================================================

latest_prices = {}

latest_prices_lock = threading.Lock()


# ============================================================
# FLASK
# ============================================================

app = Flask(__name__)


@app.route("/", methods=["GET", "HEAD"])
def home():

    return (
        "Trading Bot is Active & Running!",
        200,
    )


@app.route("/health", methods=["GET"])
def health():

    with positions_lock:
        position_count = len(positions)

    return jsonify(
        {
            "status": "running",
            "positions": position_count,
            "symbols": len(symbols),
            "timestamp": time.time(),
        }
    )


# ============================================================
# DECIMAL HELPERS
# ============================================================

def floor_to_step(value, step):

    try:

        value = Decimal(str(value))

        step = Decimal(str(step))

        if step <= 0:
            return value

        return (
            value / step
        ).to_integral_value(
            rounding=ROUND_DOWN
        ) * step

    except Exception:

        return Decimal("0")


def decimal_to_string(value):

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

    logger.info(
        "Loading Binance exchange information..."
    )

    info = client.get_exchange_info()

    new_info = {}

    for item in info.get("symbols", []):

        try:

            symbol = item["symbol"]

            status = item.get("status")

            quote_asset = item.get(
                "quoteAsset"
            )

            base_asset = item.get(
                "baseAsset"
            )

            if status != "TRADING":
                continue

            if quote_asset != "USDT":
                continue

            filters = {}

            for f in item.get(
                "filters",
                []
            ):

                filters[
                    f.get("filterType")
                ] = f

            lot_filter = filters.get(
                "LOT_SIZE",
                {}
            )

            price_filter = filters.get(
                "PRICE_FILTER",
                {}
            )

            notional_filter = (
                filters.get("NOTIONAL")
                or
                filters.get("MIN_NOTIONAL")
                or
                {}
            )

            step_size = lot_filter.get(
                "stepSize",
                "0.000001"
            )

            min_qty = lot_filter.get(
                "minQty",
                "0"
            )

            tick_size = price_filter.get(
                "tickSize",
                "0.000001"
            )

            min_notional = (
                notional_filter.get(
                    "minNotional",
                    "0"
                )
            )

            new_info[symbol] = {

                "base_asset": base_asset,

                "quote_asset": quote_asset,

                "step_size": step_size,

                "min_qty": min_qty,

                "min_notional": min_notional,

                "tick_size": tick_size,

            }

        except Exception as e:

            logger.warning(
                "Exchange info parse error: %s",
                e
            )

    with symbol_info_lock:

        symbol_info = new_info

    logger.info(
        "Exchange info loaded: %d USDT symbols",
        len(new_info)
    )


# ============================================================
# TOP SYMBOLS
# ============================================================

def load_top_symbols():

    global symbols

    logger.info(
        "Loading top %d USDT symbols...",
        TOP_SYMBOLS
    )

    try:

        tickers = client.get_ticker()

        candidates = []

        with symbol_info_lock:

            available = dict(symbol_info)

        for ticker in tickers:

            try:

                symbol = ticker.get(
                    "symbol"
                )

                if symbol not in available:
                    continue

                if symbol in EXCLUDED_SYMBOLS:
                    continue

                info = available[symbol]

                base_asset = info[
                    "base_asset"
                ]

                if base_asset in STABLECOINS:
                    continue

                quote_volume = float(
                    ticker.get(
                        "quoteVolume",
                        0
                    )
                    or 0
                )

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

        symbols = [
            x[0]
            for x in candidates[
                :TOP_SYMBOLS
            ]
        ]

        logger.info(
            "Loaded %d symbols",
            len(symbols)
        )

        if symbols:

            logger.info(
                "First symbols: %s",
                ", ".join(
                    symbols[:20]
                )
            )

    except Exception as e:

        logger.error(
            "Failed to load top symbols: %s",
            e
        )

        symbols = []


# ============================================================
# CURRENT PRICE
# ============================================================

def get_current_price(symbol):

    try:

        ticker = client.get_symbol_ticker(
            symbol=symbol
        )

        price = float(
            ticker["price"]
        )

        if price <= 0:
            return None

        return price

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

        if (
            now - previous
            < KLINE_REQUEST_COOLDOWN
        ):

            return None

        last_kline_request[symbol] = now

    time.sleep(
        KLINE_REQUEST_DELAY
    )

    try:

        raw = client.get_klines(
            symbol=symbol,
            interval=TIMEFRAME,
            limit=100,
        )

        if not raw:

            return None


        # ----------------------------------------------------
        # IMPORTANT:
        # Convert Binance response to OBJECT array first.
        # Then construct a completely NEW DataFrame.
        #
        # This avoids:
        #
        # Invalid value [...] for dtype 'str'
        #
        # ----------------------------------------------------

        raw_array = np.asarray(
            raw,
            dtype=object
        )

        if raw_array.ndim != 2:

            return None

        if raw_array.shape[0] < 50:

            return None


        # ----------------------------------------------------
        # Convert OHLCV directly to float64
        # ----------------------------------------------------

        open_time = raw_array[:, 0]

        open_price = (
            pd.to_numeric(
                raw_array[:, 1],
                errors="coerce"
            ).astype(
                np.float64
            )
        )

        high_price = (
            pd.to_numeric(
                raw_array[:, 2],
                errors="coerce"
            ).astype(
                np.float64
            )
        )

        low_price = (
            pd.to_numeric(
                raw_array[:, 3],
                errors="coerce"
            ).astype(
                np.float64
            )
        )

        close_price = (
            pd.to_numeric(
                raw_array[:, 4],
                errors="coerce"
            ).astype(
                np.float64
            )
        )

        volume = (
            pd.to_numeric(
                raw_array[:, 5],
                errors="coerce"
            ).astype(
                np.float64
            )
        )

        close_time = raw_array[:, 6]


        # ----------------------------------------------------
        # Build fresh DataFrame
        # NO problematic dtype assignment
        # ----------------------------------------------------

        df = pd.DataFrame(
            {
                "open_time": open_time,

                "open": open_price,

                "high": high_price,

                "low": low_price,

                "close": close_price,

                "volume": volume,

                "close_time": close_time,
            }
        )


        # ----------------------------------------------------
        # Remove invalid rows
        # ----------------------------------------------------

        df = df.dropna(
            subset=[
                "open",
                "high",
                "low",
                "close",
                "volume",
            ]
        ).reset_index(
            drop=True
        )


        # ----------------------------------------------------
        # Safety: make absolutely sure OHLCV are float64
        # ----------------------------------------------------

        clean_df = pd.DataFrame(
            {
                "open_time":
                    df[
                        "open_time"
                    ].to_numpy(),

                "open":
                    np.asarray(
                        df["open"],
                        dtype=np.float64
                    ),

                "high":
                    np.asarray(
                        df["high"],
                        dtype=np.float64
                    ),

                "low":
                    np.asarray(
                        df["low"],
                        dtype=np.float64
                    ),

                "close":
                    np.asarray(
                        df["close"],
                        dtype=np.float64
                    ),

                "volume":
                    np.asarray(
                        df["volume"],
                        dtype=np.float64
                    ),

                "close_time":
                    df[
                        "close_time"
                    ].to_numpy(),
            }
        )


        if len(clean_df) < 50:

            return None


        return clean_df


    except BinanceAPIException as e:

        logger.error(
            "%s | Binance Kline API error: %s",
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


        # ----------------------------------------------------
        # Create clean source DataFrame
        # ----------------------------------------------------

        source = pd.DataFrame(
            {
                "open":
                    np.asarray(
                        df["open"],
                        dtype=np.float64
                    ),

                "high":
                    np.asarray(
                        df["high"],
                        dtype=np.float64
                    ),

                "low":
                    np.asarray(
                        df["low"],
                        dtype=np.float64
                    ),

                "close":
                    np.asarray(
                        df["close"],
                        dtype=np.float64
                    ),

                "volume":
                    np.asarray(
                        df["volume"],
                        dtype=np.float64
                    ),
            }
        )


        # ====================================================
        # BOLLINGER BAND 20
        # ====================================================

        close = source[
            "close"
        ]

        bb_middle = (
            close
            .rolling(
                window=BB_PERIOD,
                min_periods=BB_PERIOD
            )
            .mean()
        )

        bb_std = (
            close
            .rolling(
                window=BB_PERIOD,
                min_periods=BB_PERIOD
            )
            .std(
                ddof=0
            )
        )

        bb_upper = (
            bb_middle
            + (2.0 * bb_std)
        )

        bb_lower = (
            bb_middle
            - (2.0 * bb_std)
        )


        # ====================================================
        # ADX 14
        # ====================================================

        high = source[
            "high"
        ]

        low = source[
            "low"
        ]

        prev_close = (
            close.shift(1)
        )


        # ----------------------------------------------------
        # True Range
        # ----------------------------------------------------

        tr1 = (
            high - low
        ).astype(
            np.float64
        )

        tr2 = (
            high - prev_close
        ).abs().astype(
            np.float64
        )

        tr3 = (
            low - prev_close
        ).abs().astype(
            np.float64
        )


        # ----------------------------------------------------
        # IMPORTANT:
        # Do NOT use DataFrame.max(numeric_only=True)
        #
        # This avoids:
        # No numeric types to aggregate
        #
        # ----------------------------------------------------

        tr12 = np.fmax(
            tr1.to_numpy(
                dtype=np.float64
            ),
            tr2.to_numpy(
                dtype=np.float64
            )
        )

        tr_array = np.fmax(
            tr12,
            tr3.to_numpy(
                dtype=np.float64
            )
        )

        tr = pd.Series(
            tr_array,
            index=source.index,
            dtype="float64"
        )


        # ----------------------------------------------------
        # Directional Movement
        # ----------------------------------------------------

        up_move = (
            high - high.shift(1)
        ).astype(
            np.float64
        )

        down_move = (
            low.shift(1) - low
        ).astype(
            np.float64
        )


        plus_dm = pd.Series(
            np.where(
                (
                    (up_move > down_move)
                    &
                    (up_move > 0)
                ),
                up_move,
                0.0
            ),
            index=source.index,
            dtype="float64"
        )


        minus_dm = pd.Series(
            np.where(
                (
                    (down_move > up_move)
                    &
                    (down_move > 0)
                ),
                down_move,
                0.0
            ),
            index=source.index,
            dtype="float64"
        )


        # ----------------------------------------------------
        # Wilder-style smoothing
        # ----------------------------------------------------

        alpha = 1.0 / ADX_PERIOD


        atr = (
            tr
            .ewm(
                alpha=alpha,
                adjust=False,
                min_periods=ADX_PERIOD
            )
            .mean()
            .astype(
                np.float64
            )
        )


        smoothed_plus_dm = (
            plus_dm
            .ewm(
                alpha=alpha,
                adjust=False,
                min_periods=ADX_PERIOD
            )
            .mean()
            .astype(
                np.float64
            )
        )


        smoothed_minus_dm = (
            minus_dm
            .ewm(
                alpha=alpha,
                adjust=False,
                min_periods=ADX_PERIOD
            )
            .mean()
            .astype(
                np.float64
            )
        )


        # ----------------------------------------------------
        # DI+
        # ----------------------------------------------------

        plus_di = (
            100.0
            * smoothed_plus_dm
            / atr.replace(
                0,
                np.nan
            )
        ).astype(
            np.float64
        )


        # ----------------------------------------------------
        # DI-
        # ----------------------------------------------------

        minus_di = (
            100.0
            * smoothed_minus_dm
            / atr.replace(
                0,
                np.nan
            )
        ).astype(
            np.float64
        )


        # ----------------------------------------------------
        # DX
        # ----------------------------------------------------

        di_sum = (
            plus_di
            + minus_di
        )

        di_diff = (
            plus_di
            - minus_di
        ).abs()


        dx = (
            100.0
            * di_diff
            / di_sum.replace(
                0,
                np.nan
            )
        ).astype(
            np.float64
        )


        # ----------------------------------------------------
        # ADX
        # ----------------------------------------------------

        adx = (
            dx
            .ewm(
                alpha=alpha,
                adjust=False,
                min_periods=ADX_PERIOD
            )
            .mean()
            .astype(
                np.float64
            )
        )


        # ====================================================
        # CREATE FINAL DATAFRAME
        # ====================================================

        result = pd.DataFrame(
            {
                "open":
                    source[
                        "open"
                    ].to_numpy(
                        dtype=np.float64
                    ),

                "high":
                    source[
                        "high"
                    ].to_numpy(
                        dtype=np.float64
                    ),

                "low":
                    source[
                        "low"
                    ].to_numpy(
                        dtype=np.float64
                    ),

                "close":
                    source[
                        "close"
                    ].to_numpy(
                        dtype=np.float64
                    ),

                "volume":
                    source[
                        "volume"
                    ].to_numpy(
                        dtype=np.float64
                    ),

                "bb_middle":
                    bb_middle.to_numpy(
                        dtype=np.float64
                    ),

                "bb_upper":
                    bb_upper.to_numpy(
                        dtype=np.float64
                    ),

                "bb_lower":
                    bb_lower.to_numpy(
                        dtype=np.float64
                    ),

                "plus_di":
                    plus_di.to_numpy(
                        dtype=np.float64
                    ),

                "minus_di":
                    minus_di.to_numpy(
                        dtype=np.float64
                    ),

                "adx":
                    adx.to_numpy(
                        dtype=np.float64
                    ),
            }
        )


        result = result.replace(
            [np.inf, -np.inf],
            np.nan
        )


        return result


    except Exception as e:

        logger.error(
            "Indicator calculation error: %s",
            e
        )

        return None


# ============================================================
# BUY CONDITION
# ============================================================

def check_buy_condition(symbol):

    try:

        df = get_closed_klines(
            symbol
        )

        if df is None:

            return False


        df = calculate_indicators(
            df
        )

        if df is None:

            return False


        if len(df) < 3:

            return False


        # ----------------------------------------------------
        # Use previous FULLY CLOSED candle
        # ----------------------------------------------------

        candle = df.iloc[-2]


        open_price = float(
            candle["open"]
        )

        close_price = float(
            candle["close"]
        )

        bb_lower = float(
            candle["bb_lower"]
        )

        adx = float(
            candle["adx"]
        )

        plus_di = float(
            candle["plus_di"]
        )

        minus_di = float(
            candle["minus_di"]
        )


        values = [
            open_price,
            close_price,
            bb_lower,
            adx,
            plus_di,
            minus_di,
        ]


        if not all(
            np.isfinite(x)
            for x in values
        ):

            return False


        # ====================================================
        # EXACT BUY RULES
        # ====================================================

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


    except Exception as e:

        logger.error(
            "%s | Buy condition error: %s",
            symbol,
            e
        )

        return False


# ============================================================
# STOP LOSS CLIENT ID
# ============================================================

def make_stop_client_id(symbol):

    timestamp = int(
        time.time() * 1000
    )

    return (
        f"{STOP_CLIENT_PREFIX}"
        f"{symbol}_"
        f"{timestamp}"
    )


# ============================================================
# NORMALIZE QUANTITY
# ============================================================

def normalize_quantity(
    symbol,
    quantity
):

    with symbol_info_lock:

        info = symbol_info.get(
            symbol
        )

    if not info:

        return Decimal("0")


    step_size = Decimal(
        str(
            info[
                "step_size"
            ]
        )
    )

    min_qty = Decimal(
        str(
            info[
                "min_qty"
            ]
        )
    )


    qty = floor_to_step(
        quantity,
        step_size
    )


    if qty < min_qty:

        return Decimal("0")


    return qty


# ============================================================
# NORMALIZE PRICE
# ============================================================

def normalize_price(
    symbol,
    price
):

    with symbol_info_lock:

        info = symbol_info.get(
            symbol
        )

    if not info:

        return Decimal("0")


    tick_size = Decimal(
        str(
            info[
                "tick_size"
            ]
        )
    )


    return floor_to_step(
        price,
        tick_size
    )


# ============================================================
# PLACE SERVER STOP LOSS
# ============================================================

def place_server_stop_loss(
    symbol,
    quantity,
    entry_price
):

    try:

        stop_price = (
            Decimal(
                str(entry_price)
            )
            * (
                Decimal("1")
                - Decimal(
                    str(STOP_LOSS_PCT)
                )
            )
        )


        stop_price = normalize_price(
            symbol,
            stop_price
        )


        qty = normalize_quantity(
            symbol,
            quantity
        )


        if stop_price <= 0:

            logger.error(
                "%s | Invalid stop price",
                symbol
            )

            return None


        if qty <= 0:

            logger.error(
                "%s | Invalid stop quantity",
                symbol
            )

            return None


        client_order_id = (
            make_stop_client_id(
                symbol
            )
        )


        order = client.create_order(

            symbol=symbol,

            side="SELL",

            type="STOP_LOSS",

            quantity=decimal_to_string(
                qty
            ),

            stopPrice=decimal_to_string(
                stop_price
            ),

            newClientOrderId=client_order_id,

            newOrderRespType="RESULT",
        )


        logger.info(
            "%s | SERVER STOP LOSS PLACED | "
            "Stop=%.8f | Qty=%s | OrderID=%s",
            symbol,
            float(stop_price),
            decimal_to_string(qty),
            order.get("orderId"),
        )


        return order


    except BinanceAPIException as e:

        logger.error(
            "%s | Stop loss Binance error: %s",
            symbol,
            e
        )

        return None


    except Exception as e:

        logger.error(
            "%s | Stop loss error: %s",
            symbol,
            e
        )

        return None


# ============================================================
# FIND EXISTING SERVER STOP
# ============================================================

def find_existing_server_stop(
    symbol
):

    try:

        orders = client.get_open_orders(
            symbol=symbol
        )


        for order in orders:

            side = order.get(
                "side"
            )

            order_type = order.get(
                "type"
            )

            client_id = order.get(
                "clientOrderId",
                ""
            )


            if (
                side == "SELL"
                and
                order_type == "STOP_LOSS"
                and
                client_id.startswith(
                    STOP_CLIENT_PREFIX
                )
            ):

                return order


        return None


    except Exception as e:

        logger.error(
            "%s | Find stop order error: %s",
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

        return client.get_order(
            symbol=symbol,
            orderId=order_id
        )

    except Exception as e:

        logger.error(
            "%s | Stop status error: %s",
            symbol,
            e
        )

        return None


# ============================================================
# CANCEL STOP LOSS
# ============================================================

def cancel_server_stop(
    symbol,
    order_id
):

    try:

        client.cancel_order(
            symbol=symbol,
            orderId=order_id
        )

        logger.info(
            "%s | Server stop cancelled | OrderID=%s",
            symbol,
            order_id
        )

        return True

    except BinanceAPIException as e:

        logger.error(
            "%s | Cancel stop Binance error: %s",
            symbol,
            e
        )

        return False

    except Exception as e:

        logger.error(
            "%s | Cancel stop error: %s",
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

        qty = normalize_quantity(
            symbol,
            quantity
        )


        if qty <= 0:

            logger.error(
                "%s | Invalid sell quantity",
                symbol
            )

            return None


        order = client.order_market_sell(

            symbol=symbol,

            quantity=decimal_to_string(
                qty
            )
        )


        logger.info(
            "%s | MARKET SELL EXECUTED | Qty=%s",
            symbol,
            decimal_to_string(qty)
        )


        return order


    except BinanceAPIException as e:

        logger.error(
            "%s | Market sell Binance error: %s",
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

            return False

        buying_symbols.add(symbol)


    try:

        # ----------------------------------------------------
        # Duplicate position protection
        # ----------------------------------------------------

        with positions_lock:

            if symbol in positions:

                return False


        # ----------------------------------------------------
        # Buy cooldown
        # ----------------------------------------------------

        now = time.time()

        with last_buy_lock:

            previous_buy = (
                last_buy_time.get(
                    symbol,
                    0
                )
            )

            if (
                now - previous_buy
                < BUY_COOLDOWN_SECONDS
            ):

                return False

            last_buy_time[symbol] = now


        logger.info(
            "%s | Executing MARKET BUY | "
            "Amount=%.2f USDT",
            symbol,
            TRADE_AMOUNT_USDT
        )


        # ----------------------------------------------------
        # MARKET BUY
        # ----------------------------------------------------

        order = client.order_market_buy(

            symbol=symbol,

            quoteOrderQty=str(
                TRADE_AMOUNT_USDT
            )
        )


        # ----------------------------------------------------
        # Calculate actual filled quantity
        # ----------------------------------------------------

        executed_qty = Decimal(
            str(
                order.get(
                    "executedQty",
                    "0"
                )
            )
        )


        if executed_qty <= 0:

            logger.error(
                "%s | BUY executedQty is zero",
                symbol
            )

            return False


        # ----------------------------------------------------
        # Calculate average entry price
        # ----------------------------------------------------

        total_quote = Decimal("0")

        total_qty = Decimal("0")


        for fill in order.get(
            "fills",
            []
        ):

            fill_price = Decimal(
                str(
                    fill.get(
                        "price",
                        "0"
                    )
                )
            )

            fill_qty = Decimal(
                str(
                    fill.get(
                        "qty",
                        "0"
                    )
                )
            )

            total_quote += (
                fill_price
                * fill_qty
            )

            total_qty += fill_qty


        if total_qty > 0:

            entry_price = (
                total_quote
                / total_qty
            )

        else:

            entry_price = Decimal(
                str(
                    get_current_price(
                        symbol
                    )
                    or 0
                )
            )


        if entry_price <= 0:

            logger.error(
                "%s | Invalid entry price",
                symbol
            )

            return False


        # ----------------------------------------------------
        # Normalize quantity
        # ----------------------------------------------------

        quantity = normalize_quantity(
            symbol,
            executed_qty
        )


        if quantity <= 0:

            logger.error(
                "%s | Quantity below minimum",
                symbol
            )

            return False


        # ----------------------------------------------------
        # PLACE SERVER STOP LOSS
        # ----------------------------------------------------

        stop_order = (
            place_server_stop_loss(
                symbol,
                quantity,
                float(entry_price)
            )
        )


        # ----------------------------------------------------
        # IMPORTANT:
        # If server SL cannot be placed,
        # emergency market sell.
        # ----------------------------------------------------

        if stop_order is None:

            logger.error(
                "%s | STOP LOSS FAILED -> "
                "Emergency MARKET SELL",
                symbol
            )

            market_sell(
                symbol,
                quantity
            )

            return False


        stop_order_id = stop_order.get(
            "orderId"
        )


        stop_price = (
            entry_price
            * (
                Decimal("1")
                - Decimal(
                    str(
                        STOP_LOSS_PCT
                    )
                )
            )
        )


        # ----------------------------------------------------
        # Save position
        # ----------------------------------------------------

        position = {

            "symbol": symbol,

            "entry_price":
                float(
                    entry_price
                ),

            "quantity":
                float(
                    quantity
                ),

            "highest_price":
                float(
                    entry_price
                ),

            "trailing_active":
                False,

            "stop_order_id":
                stop_order_id,

            "stop_price":
                float(
                    stop_price
                ),

            "created_at":
                time.time(),
        }


        with positions_lock:

            positions[
                symbol
            ] = position


        logger.info(
            "%s | BUY SUCCESS | "
            "Entry=%.8f | Qty=%s | "
            "SL=%.8f | Trailing activation=+%.2f%%",
            symbol,
            float(entry_price),
            decimal_to_string(
                quantity
            ),
            float(stop_price),
            TRAILING_ACTIVATION_PCT * 100,
        )


        return True


    except BinanceAPIException as e:

        logger.error(
            "%s | BUY Binance error: %s",
            symbol,
            e
        )

        return False


    except Exception as e:

        logger.error(
            "%s | BUY error: %s",
            symbol,
            e
        )

        return False


    finally:

        with buying_lock:

            buying_symbols.discard(
                symbol
            )


# ============================================================
# SELL SYMBOL
# ============================================================

def sell_symbol(
    symbol,
    reason
):

    with selling_lock:

        if symbol in selling_symbols:

            return False

        selling_symbols.add(
            symbol
        )


    try:

        with positions_lock:

            position = positions.get(
                symbol
            )


        if not position:

            return False


        stop_order_id = position.get(
            "stop_order_id"
        )


        # ----------------------------------------------------
        # Cancel server SL first
        # ----------------------------------------------------

        if stop_order_id:

            cancel_server_stop(
                symbol,
                stop_order_id
            )


        # ----------------------------------------------------
        # Market sell
        # ----------------------------------------------------

        order = market_sell(
            symbol,
            position[
                "quantity"
            ]
        )


        if order is not None:

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

            return True


        return False


    finally:

        with selling_lock:

            selling_symbols.discard(
                symbol
            )


# ============================================================
# CHECK POSITION
# ============================================================

def check_position(
    symbol,
    current_price=None
):

    try:

        with positions_lock:

            position = positions.get(
                symbol
            )


        if not position:

            return


        # ----------------------------------------------------
        # Get current price
        # ----------------------------------------------------

        if current_price is None:

            current_price = (
                get_current_price(
                    symbol
                )
            )


        if current_price is None:

            return


        current_price = float(
            current_price
        )


        if current_price <= 0:

            return


        entry_price = float(
            position[
                "entry_price"
            ]
        )


        # ----------------------------------------------------
        # Update highest price
        # ----------------------------------------------------

        if (
            current_price
            > position[
                "highest_price"
            ]
        ):

            position[
                "highest_price"
            ] = current_price


        highest_price = float(
            position[
                "highest_price"
            ]
        )


        # ====================================================
        # SERVER STOP LOSS CHECK
        # ====================================================

        stop_price = float(
            position[
                "stop_price"
            ]
        )


        if current_price <= stop_price:

            stop_order_id = (
                position.get(
                    "stop_order_id"
                )
            )


            if stop_order_id:

                status = (
                    get_stop_order_status(
                        symbol,
                        stop_order_id
                    )
                )


                if status:

                    order_status = status.get(
                        "status"
                    )


                    if order_status == "FILLED":

                        logger.info(
                            "%s | SERVER STOP LOSS FILLED",
                            symbol
                        )

                        with positions_lock:

                            positions.pop(
                                symbol,
                                None
                            )

                        return


                    if order_status in {
                        "CANCELED",
                        "EXPIRED",
                        "REJECTED",
                    }:

                        logger.warning(
                            "%s | Server stop status=%s "
                            "-> Emergency market sell",
                            symbol,
                            order_status
                        )

                        sell_symbol(
                            symbol,
                            "STOP_LOSS_FALLBACK"
                        )

                        return


                    if order_status == "NEW":

                        return


            else:

                sell_symbol(
                    symbol,
                    "STOP_LOSS"
                )

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
            not position[
                "trailing_active"
            ]
            and
            current_price
            >= activation_price
        ):

            position[
                "trailing_active"
            ] = True


            logger.info(
                "%s | TRAILING STOP ACTIVATED | "
                "Entry=%.8f | Current=%.8f",
                symbol,
                entry_price,
                current_price,
            )


        # ====================================================
        # TRAILING STOP
        # ====================================================

        if position[
            "trailing_active"
        ]:

            trailing_stop = (
                highest_price
                * (
                    1.0
                    - TRAILING_STOP_PCT
                )
            )


            if (
                current_price
                <= trailing_stop
            ):

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
                    "TRAILING_STOP"
                )


    except Exception as e:

        logger.error(
            "%s | Position check error: %s",
            symbol,
            e
        )


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


        net_qty = Decimal("0")

        total_cost = Decimal("0")


        for trade in trades:

            qty = Decimal(
                str(
                    trade.get(
                        "qty",
                        "0"
                    )
                )
            )

            price = Decimal(
                str(
                    trade.get(
                        "price",
                        "0"
                    )
                )
            )

            is_buyer = bool(
                trade.get(
                    "isBuyer",
                    False
                )
            )


            if is_buyer:

                net_qty += qty

                total_cost += (
                    qty * price
                )

            else:

                net_qty -= qty


        if net_qty <= 0:

            return None


        average_price = (
            total_cost
            / net_qty
        )


        return float(
            average_price
        )


    except Exception as e:

        logger.error(
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

        account = (
            client.get_account()
        )

        balances = account.get(
            "balances",
            []
        )


        for balance in balances:

            try:

                asset = balance.get(
                    "asset"
                )

                free = Decimal(
                    str(
                        balance.get(
                            "free",
                            "0"
                        )
                    )
                )

                locked = Decimal(
                    str(
                        balance.get(
                            "locked",
                            "0"
                        )
                    )
                )

                total_balance = (
                    free + locked
                )


                if total_balance <= 0:
                    continue


                if asset in STABLECOINS:
                    continue


                symbol = (
                    asset + "USDT"
                )


                with symbol_info_lock:

                    if symbol not in symbol_info:
                        continue


                if symbol in EXCLUDED_SYMBOLS:
                    continue


                quantity = normalize_quantity(
                    symbol,
                    total_balance
                )


                if quantity <= 0:
                    continue


                current_price = (
                    get_current_price(
                        symbol
                    )
                )


                if not current_price:
                    continue


                # ------------------------------------------------
                # Recover average entry
                # ------------------------------------------------

                entry_price = (
                    recover_entry_price_from_trades(
                        symbol,
                        asset
                    )
                )


                if not entry_price:

                    entry_price = current_price


                # ------------------------------------------------
                # Find existing server stop
                # ------------------------------------------------

                existing_stop = (
                    find_existing_server_stop(
                        symbol
                    )
                )


                stop_order_id = None

                stop_price = (
                    entry_price
                    * (
                        1.0
                        - STOP_LOSS_PCT
                    )
                )


                if existing_stop:

                    stop_order_id = (
                        existing_stop.get(
                            "orderId"
                        )
                    )

                    try:

                        stop_price = float(
                            existing_stop.get(
                                "stopPrice",
                                stop_price
                            )
                        )

                    except Exception:
                        pass


                else:

                    stop_order = (
                        place_server_stop_loss(
                            symbol,
                            quantity,
                            entry_price
                        )
                    )


                    if stop_order:

                        stop_order_id = (
                            stop_order.get(
                                "orderId"
                            )
                        )


                if not stop_order_id:

                    logger.warning(
                        "%s | Could not create recovery stop",
                        symbol
                    )

                    continue


                trailing_active = (
                    current_price
                    >= (
                        entry_price
                        * (
                            1.0
                            + TRAILING_ACTIVATION_PCT
                        )
                    )
                )


                with positions_lock:

                    positions[
                        symbol
                    ] = {

                        "symbol":
                            symbol,

                        "entry_price":
                            float(
                                entry_price
                            ),

                        "quantity":
                            float(
                                quantity
                            ),

                        "highest_price":
                            max(
                                float(
                                    current_price
                                ),
                                float(
                                    entry_price
                                )
                            ),

                        "trailing_active":
                            trailing_active,

                        "stop_order_id":
                            stop_order_id,

                        "stop_price":
                            float(
                                stop_price
                            ),

                        "created_at":
                            time.time(),

                    }


                logger.info(
                    "%s | POSITION RECOVERED | "
                    "Entry=%.8f | Qty=%s | "
                    "Current=%.8f | Trailing=%s",
                    symbol,
                    entry_price,
                    decimal_to_string(
                        quantity
                    ),
                    current_price,
                    trailing_active,
                )


            except Exception as e:

                logger.error(
                    "Balance recovery error: %s",
                    e
                )


    except Exception as e:

        logger.error(
            "Position recovery failed: %s",
            e
        )


# ============================================================
# CANDLE QUEUE
# ============================================================

def enqueue_candle(symbol):

    with queued_candles_lock:

        if symbol in queued_candles:

            return

        queued_candles.add(
            symbol
        )


    try:

        candle_queue.put_nowait(
            symbol
        )

    except queue.Full:

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

        symbol = (
            candle_queue.get()
        )


        try:

            with queued_candles_lock:

                queued_candles.discard(
                    symbol
                )


            # ------------------------------------------------
            # If already holding this symbol,
            # don't look for another BUY.
            # ------------------------------------------------

            with positions_lock:

                has_position = (
                    symbol in positions
                )


            if has_position:

                continue


            # ------------------------------------------------
            # Check BUY condition
            # ------------------------------------------------

            if check_buy_condition(
                symbol
            ):

                buy_symbol(
                    symbol
                )


        except Exception as e:

            logger.error(
                "%s | Candle worker error: %s",
                symbol,
                e
            )


        finally:

            candle_queue.task_done()


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

                current_symbols = list(
                    positions.keys()
                )


            for symbol in current_symbols:

                try:

                    # Use latest WebSocket price
                    with latest_prices_lock:

                        price = latest_prices.get(
                            symbol
                        )


                    check_position(
                        symbol,
                        price
                    )


                except Exception as e:

                    logger.error(
                        "%s | Monitor error: %s",
                        symbol,
                        e
                    )


            time.sleep(
                5
            )


        except Exception as e:

            logger.error(
                "Position monitor loop error: %s",
                e
            )

            time.sleep(
                5
            )


# ============================================================
# WEBSOCKET MESSAGE
# ============================================================

def process_ws_message(
    message
):

    try:

        data = json.loads(
            message
        )


        # ----------------------------------------------------
        # Combined stream format
        # ----------------------------------------------------

        if "data" in data:

            data = data[
                "data"
            ]


        event_type = data.get(
            "e"
        )


        # ====================================================
        # KLINE
        # ====================================================

        if event_type == "kline":

            kline = data.get(
                "k",
                {}
            )

            symbol = kline.get(
                "s"
            )


            is_closed = kline.get(
                "x",
                False
            )


            if (
                symbol
                and is_closed
            ):

                enqueue_candle(
                    symbol
                )


        # ====================================================
        # MINI TICKER
        # ====================================================

        elif event_type == "24hrMiniTicker":

            symbol = data.get(
                "s"
            )

            price_text = data.get(
                "c"
            )


            if (
                symbol
                and price_text
            ):

                try:

                    price = float(
                        price_text
                    )

                    if price > 0:

                        with latest_prices_lock:

                            latest_prices[
                                symbol
                            ] = price


                except Exception:
                    pass


    except Exception as e:

        logger.error(
            "WebSocket message processing error: %s",
            e
        )


# ============================================================
# WEBSOCKET CALLBACKS
# ============================================================

def ws_on_message(
    ws,
    message
):

    process_ws_message(
        message
    )


def ws_on_error(
    ws,
    error
):

    logger.error(
        "WebSocket error: %s",
        error
    )


def ws_on_close(
    ws,
    close_status_code,
    close_msg
):

    logger.warning(
        "WebSocket closed | code=%s | msg=%s",
        close_status_code,
        close_msg
    )


def ws_on_open(ws):

    logger.info(
        "WebSocket connected"
    )


# ============================================================
# MAKE STREAM URL
# ============================================================

def make_stream_url(
    symbol_list
):

    streams = []

    for symbol in symbol_list:

        lower_symbol = (
            symbol.lower()
        )

        streams.append(
            f"{lower_symbol}@kline_5m"
        )

        streams.append(
            f"{lower_symbol}@miniTicker"
        )


    return (
        "wss://stream.binance.com:9443"
        "/stream?streams="
        + "/".join(streams)
    )


# ============================================================
# RUN ONE WEBSOCKET
# ============================================================

def run_websocket(
    symbol_list,
    connection_number
):

    if not symbol_list:

        return


    url = make_stream_url(
        symbol_list
    )


    logger.info(
        "Starting WebSocket #%d | Symbols=%d",
        connection_number,
        len(symbol_list)
    )


    while True:

        try:

            ws = websocket.WebSocketApp(

                url,

                on_open=ws_on_open,

                on_message=ws_on_message,

                on_error=ws_on_error,

                on_close=ws_on_close,
            )


            ws.run_forever(

                ping_interval=
                    WS_PING_INTERVAL,

                ping_timeout=
                    WS_PING_TIMEOUT,

                ping_payload="ping",

            )


        except Exception as e:

            logger.error(
                "WebSocket #%d exception: %s",
                connection_number,
                e
            )


        logger.warning(
            "WebSocket #%d reconnecting in %d seconds...",
            connection_number,
            RECONNECT_DELAY
        )


        time.sleep(
            RECONNECT_DELAY
        )


# ============================================================
# WEBSOCKET MANAGER
# ============================================================

def websocket_manager():

    logger.info(
        "Starting WebSocket manager..."
    )


    chunks = []


    for i in range(
        0,
        len(symbols),
        WS_SYMBOLS_PER_CONNECTION
    ):

        chunks.append(
            symbols[
                i:
                i + WS_SYMBOLS_PER_CONNECTION
            ]
        )


    for index, chunk in enumerate(
        chunks,
        start=1
    ):

        thread = threading.Thread(

            target=run_websocket,

            args=(
                chunk,
                index
            ),

            daemon=True,
        )


        thread.start()


        # Small delay between connections
        time.sleep(
            1
        )


    logger.info(
        "Started %d WebSocket connections",
        len(chunks)
    )


# ============================================================
# FLASK SERVER
# ============================================================

def run_flask():

    port = int(
        os.environ.get(
            "PORT",
            "10000"
        )
    )


    app.run(

        host="0.0.0.0",

        port=port,

        debug=False,

        use_reloader=False,
    )


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
        "BUY RULE:"
    )

    logger.info(
        "Open < BB20 Lower"
    )

    logger.info(
        "AND Close > BB20 Lower"
    )

    logger.info(
        "AND ADX14 > 20"
    )

    logger.info(
        "AND DI+ > DI-"
    )

    logger.info(
        "STOP LOSS = %.2f%%",
        STOP_LOSS_PCT * 100
    )

    logger.info(
        "TRAILING ACTIVATION = +%.2f%%",
        TRAILING_ACTIVATION_PCT * 100
    )

    logger.info(
        "TRAILING DISTANCE = %.2f%%",
        TRAILING_STOP_PCT * 100
    )

    logger.info(
        "TRADE AMOUNT = %.2f USDT",
        TRADE_AMOUNT_USDT
    )

    logger.info(
        "TIMEFRAME = 5 MINUTES"
    )

    logger.info(
        "TOP SYMBOLS = %d",
        TOP_SYMBOLS
    )

    logger.info(
        "=" * 70
    )


    # --------------------------------------------------------
    # Binance exchange info
    # --------------------------------------------------------

    load_exchange_info()


    # --------------------------------------------------------
    # Top symbols
    # --------------------------------------------------------

    load_top_symbols()


    if not symbols:

        raise RuntimeError(
            "No symbols loaded"
        )


    # --------------------------------------------------------
    # Recover existing positions
    # --------------------------------------------------------

    recover_positions()


    # --------------------------------------------------------
    # Candle worker
    # --------------------------------------------------------

    candle_thread = threading.Thread(

        target=candle_worker,

        daemon=True
    )

    candle_thread.start()


    # --------------------------------------------------------
    # Position monitor
    # --------------------------------------------------------

    monitor_thread = threading.Thread(

        target=position_monitor,

        daemon=True
    )

    monitor_thread.start()


    # --------------------------------------------------------
    # WebSockets
    # --------------------------------------------------------

    websocket_thread = threading.Thread(

        target=websocket_manager,

        daemon=True
    )

    websocket_thread.start()


    logger.info(
        "All background services started."
    )


# ============================================================
# MAIN
# ============================================================

if __name__ == "__main__":

    startup()


    # Flask runs in main thread
    run_flask()
