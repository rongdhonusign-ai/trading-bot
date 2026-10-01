import os
import time
import json
import threading
import logging
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

client = Client(API_KEY, API_SECRET)


# ============================================================
# TRADING SETTINGS
# ============================================================

TRADE_AMOUNT_USDT = 35.0

TIMEFRAME = Client.KLINE_INTERVAL_5MINUTE

TOP_SYMBOLS = 150


# ============================================================
# BUY INDICATORS
# ============================================================

BB_PERIOD = 20

ADX_PERIOD = 14

ADX_MIN = 20.0


# ============================================================
# RISK MANAGEMENT
# ============================================================

# Binance server-side stop loss
STOP_LOSS_PCT = 0.0100          # 1%

# Trailing activation
TRAILING_ACTIVATION_PCT = 0.0100    # +1%

# Trailing distance
TRAILING_STOP_PCT = 0.0050          # 0.5%


# ============================================================
# REQUEST / CONNECTION SETTINGS
# ============================================================

KLINE_REQUEST_COOLDOWN = 2.0

KLINE_REQUEST_DELAY = 0.05

BUY_COOLDOWN_SECONDS = 60

RECONNECT_DELAY = 10

# WebSocket heartbeat
WS_PING_INTERVAL = 30

WS_PING_TIMEOUT = 20


# ============================================================
# ASSETS
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


# ============================================================
# SERVER STOP ORDER PREFIX
# ============================================================

STOP_CLIENT_PREFIX = "BBADXSL_"


# ============================================================
# GLOBAL VARIABLES
# ============================================================

positions = {}

positions_lock = threading.Lock()

buying_symbols = set()

selling_symbols = set()

last_kline_request = {}

last_buy_time = {}

symbols = []

symbol_info = {}


# ============================================================
# LOGGING
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

logger = logging.getLogger(__name__)


# ============================================================
# FLASK
# ============================================================

app = Flask(__name__)


@app.route("/")
def home():
    return "Trading Bot is Active & Running!"


@app.route("/health")
def health():

    with positions_lock:
        count = len(positions)

    return jsonify({
        "status": "running",
        "positions": count,
        "symbols": len(symbols),
        "timestamp": int(time.time())
    })


# ============================================================
# DECIMAL HELPERS
# ============================================================

def floor_to_step(value, step):

    value = Decimal(str(value))
    step = Decimal(str(step))

    if step <= 0:
        return value

    return (
        value / step
    ).to_integral_value(
        rounding=ROUND_DOWN
    ) * step


def decimal_to_string(value):

    text = format(
        Decimal(str(value)),
        "f"
    )

    if "." in text:
        text = text.rstrip("0").rstrip(".")

    return text


# ============================================================
# EXCHANGE INFO
# ============================================================

def load_exchange_info():

    logger.info(
        "Loading Binance exchange info..."
    )

    info = client.get_exchange_info()

    for item in info.get("symbols", []):

        symbol = item.get("symbol")

        if not symbol:
            continue

        if item.get("status") != "TRADING":
            continue

        base_asset = item.get("baseAsset")
        quote_asset = item.get("quoteAsset")

        if quote_asset != "USDT":
            continue

        lot_step = 0.00000001
        min_qty = 0.0
        min_notional = 0.0
        tick_size = 0.00000001

        for f in item.get("filters", []):

            filter_type = f.get(
                "filterType"
            )

            # ----------------------------
            # LOT SIZE
            # ----------------------------

            if filter_type == "LOT_SIZE":

                lot_step = float(
                    f.get(
                        "stepSize",
                        "0.00000001"
                    )
                )

                min_qty = float(
                    f.get(
                        "minQty",
                        "0"
                    )
                )

            # ----------------------------
            # PRICE FILTER
            # ----------------------------

            elif filter_type == "PRICE_FILTER":

                tick_size = float(
                    f.get(
                        "tickSize",
                        "0.00000001"
                    )
                )

            # ----------------------------
            # MIN NOTIONAL
            # ----------------------------

            elif filter_type == "MIN_NOTIONAL":

                min_notional = max(
                    min_notional,
                    float(
                        f.get(
                            "minNotional",
                            "0"
                        )
                    )
                )

            # ----------------------------
            # NOTIONAL
            # ----------------------------

            elif filter_type == "NOTIONAL":

                min_notional = max(
                    min_notional,
                    float(
                        f.get(
                            "minNotional",
                            "0"
                        )
                    )
                )

        symbol_info[symbol] = {

            "base_asset": base_asset,

            "quote_asset": quote_asset,

            "step_size": lot_step,

            "min_qty": min_qty,

            "min_notional": min_notional,

            "tick_size": tick_size
        }

    logger.info(
        "Exchange info loaded: %d USDT symbols",
        len(symbol_info)
    )


# ============================================================
# TOP 150 SYMBOLS
# ============================================================

def load_top_symbols():

    global symbols

    logger.info(
        "Loading top USDT symbols..."
    )

    try:

        tickers = client.get_ticker()

    except Exception as e:

        logger.error(
            "Ticker loading failed: %s",
            e
        )

        return

    candidates = []

    for ticker in tickers:

        symbol = ticker.get(
            "symbol",
            ""
        )

        if symbol not in symbol_info:
            continue

        if symbol in EXCLUDED_SYMBOLS:
            continue

        info = symbol_info.get(symbol)

        if not info:
            continue

        base_asset = info.get(
            "base_asset"
        )

        if base_asset in STABLECOINS:
            continue

        try:

            quote_volume = float(
                ticker.get(
                    "quoteVolume",
                    0
                )
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

    symbols = [
        x[0]
        for x in candidates[:TOP_SYMBOLS]
    ]

    logger.info(
        "Loaded top %d symbols",
        len(symbols)
    )


# ============================================================
# CURRENT PRICE
# ============================================================

def get_current_price(symbol):

    try:

        data = client.get_symbol_ticker(
            symbol=symbol
        )

        price = float(
            data.get(
                "price",
                0
            )
        )

        if price <= 0:
            return None

        return price

    except Exception as e:

        logger.error(
            "%s | price error: %s",
            symbol,
            e
        )

        return None


# ============================================================
# GET CLOSED KLINES
# ============================================================

def get_closed_klines(symbol):

    now = time.time()

    last_request = last_kline_request.get(
        symbol,
        0
    )

    if (
        now - last_request
        < KLINE_REQUEST_COOLDOWN
    ):
        return None

    last_kline_request[symbol] = now

    try:

        time.sleep(
            KLINE_REQUEST_DELAY
        )

        raw = client.get_klines(
            symbol=symbol,
            interval=TIMEFRAME,
            limit=100
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
            "ignore"
        ]

        df = pd.DataFrame(
            raw,
            columns=columns
        )

        # ====================================================
        # FORCE NUMERIC CONVERSION
        # ====================================================

        numeric_columns = [
            "open",
            "high",
            "low",
            "close",
            "volume",
            "quote_volume",
            "taker_buy_base",
            "taker_buy_quote"
        ]

        for col in numeric_columns:

            if col in df.columns:

                df[col] = pd.to_numeric(
                    df[col],
                    errors="coerce"
                )

        # ====================================================
        # Remove completely invalid rows
        # ====================================================

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

        return df

    except BinanceAPIException as e:

        logger.error(
            "%s | Binance kline error: %s",
            symbol,
            e
        )

        return None

    except Exception as e:

        logger.error(
            "%s | kline error: %s",
            symbol,
            e
        )

        return None


# ============================================================
# INDICATORS
# ============================================================

def calculate_indicators(df):

    try:

        if df is None:
            return None

        if len(df) < 50:
            return None

        df = df.copy()

        # ====================================================
        # Force numeric one more time
        # ====================================================

        required_columns = [
            "open",
            "high",
            "low",
            "close",
            "volume"
        ]

        for col in required_columns:

            if col not in df.columns:
                return None

            df[col] = pd.to_numeric(
                df[col],
                errors="coerce"
            )

        df = df.dropna(
            subset=required_columns
        ).copy()

        if len(df) < 50:
            return None

        # ====================================================
        # EMA5
        # ====================================================

        df["ema5"] = (
            df["close"]
            .ewm(
                span=5,
                adjust=False
            )
            .mean()
        )

        # ====================================================
        # BB20
        # ====================================================

        df["bb_middle"] = (
            df["close"]
            .rolling(
                BB_PERIOD,
                min_periods=BB_PERIOD
            )
            .mean()
        )

        rolling_std = (
            df["close"]
            .rolling(
                BB_PERIOD,
                min_periods=BB_PERIOD
            )
            .std()
        )

        df["bb_upper"] = (
            df["bb_middle"]
            + 2.0 * rolling_std
        )

        df["bb_lower"] = (
            df["bb_middle"]
            - 2.0 * rolling_std
        )

        # ====================================================
        # ADX14
        # ====================================================

        high = pd.to_numeric(
            df["high"],
            errors="coerce"
        )

        low = pd.to_numeric(
            df["low"],
            errors="coerce"
        )

        close = pd.to_numeric(
            df["close"],
            errors="coerce"
        )

        prev_close = close.shift(1)

        tr1 = (
            high - low
        ).abs()

        tr2 = (
            high - prev_close
        ).abs()

        tr3 = (
            low - prev_close
        ).abs()

        # ====================================================
        # IMPORTANT FIX
        #
        # Explicit numeric DataFrame
        # before aggregation.
        # ====================================================

        tr_df = pd.DataFrame(
            {
                "tr1": pd.to_numeric(
                    tr1,
                    errors="coerce"
                ),
                "tr2": pd.to_numeric(
                    tr2,
                    errors="coerce"
                ),
                "tr3": pd.to_numeric(
                    tr3,
                    errors="coerce"
                )
            }
        )

        tr_df = tr_df.astype(
            "float64"
        )

        tr = tr_df.max(
            axis=1,
            skipna=True
        )

        # ====================================================
        # Directional Movement
        # ====================================================

        up_move = (
            high.diff()
        )

        down_move = (
            -low.diff()
        )

        plus_dm = pd.Series(
            0.0,
            index=df.index,
            dtype="float64"
        )

        minus_dm = pd.Series(
            0.0,
            index=df.index,
            dtype="float64"
        )

        plus_condition = (
            (up_move > down_move)
            & (up_move > 0)
        )

        minus_condition = (
            (down_move > up_move)
            & (down_move > 0)
        )

        plus_dm.loc[
            plus_condition
        ] = up_move.loc[
            plus_condition
        ]

        minus_dm.loc[
            minus_condition
        ] = down_move.loc[
            minus_condition
        ]

        alpha = 1.0 / ADX_PERIOD

        atr = tr.ewm(
            alpha=alpha,
            adjust=False
        ).mean()

        plus_dm_smoothed = (
            plus_dm
            .ewm(
                alpha=alpha,
                adjust=False
            )
            .mean()
        )

        minus_dm_smoothed = (
            minus_dm
            .ewm(
                alpha=alpha,
                adjust=False
            )
            .mean()
        )

        atr_safe = atr.replace(
            0,
            pd.NA
        )

        plus_di = (
            100.0
            * plus_dm_smoothed
            / atr_safe
        )

        minus_di = (
            100.0
            * minus_dm_smoothed
            / atr_safe
        )

        di_sum = (
            plus_di
            + minus_di
        )

        di_sum = di_sum.replace(
            0,
            pd.NA
        )

        dx = (
            100.0
            * (plus_di - minus_di).abs()
            / di_sum
        )

        adx = dx.ewm(
            alpha=alpha,
            adjust=False
        ).mean()

        df["plus_di"] = pd.to_numeric(
            plus_di,
            errors="coerce"
        )

        df["minus_di"] = pd.to_numeric(
            minus_di,
            errors="coerce"
        )

        df["adx"] = pd.to_numeric(
            adx,
            errors="coerce"
        )

        return df

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

    # Last row may be current candle.
    # Use previous fully closed candle.

    candle = df.iloc[-2]

    try:

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

    except Exception:

        logger.warning(
            "%s | Invalid indicator values - skipped",
            symbol
        )

        return False

    values = [
        open_price,
        close_price,
        bb_lower,
        adx,
        plus_di,
        minus_di
    ]

    for value in values:

        if not pd.notna(value):

            return False

    # ========================================================
    # BUY RULE
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
            "Open=%.8f Close=%.8f "
            "BBLower=%.8f ADX=%.2f "
            "+DI=%.2f -DI=%.2f",
            symbol,
            open_price,
            close_price,
            bb_lower,
            adx,
            plus_di,
            minus_di
        )

        return True

    return False


# ============================================================
# NORMALIZE QUANTITY
# ============================================================

def normalize_quantity(
    symbol,
    quantity
):

    info = symbol_info.get(
        symbol
    )

    if not info:
        return float(quantity)

    step_size = info.get(
        "step_size",
        0.00000001
    )

    qty = floor_to_step(
        quantity,
        step_size
    )

    return float(qty)


# ============================================================
# NORMALIZE PRICE
# ============================================================

def normalize_price(
    symbol,
    price
):

    info = symbol_info.get(
        symbol
    )

    if not info:
        return float(price)

    tick_size = info.get(
        "tick_size",
        0.00000001
    )

    p = floor_to_step(
        price,
        tick_size
    )

    return float(p)


# ============================================================
# SERVER-SIDE STOP LOSS
# ============================================================

def place_server_stop_loss(
    symbol,
    quantity,
    entry_price
):

    try:

        quantity = normalize_quantity(
            symbol,
            quantity
        )

        if quantity <= 0:

            logger.error(
                "%s | Invalid STOP quantity",
                symbol
            )

            return None

        stop_price = (
            entry_price
            * (1.0 - STOP_LOSS_PCT)
        )

        stop_price = normalize_price(
            symbol,
            stop_price
        )

        if stop_price <= 0:

            logger.error(
                "%s | Invalid STOP price",
                symbol
            )

            return None

        client_order_id = (
            STOP_CLIENT_PREFIX
            + str(
                int(
                    time.time() * 1000
                )
            )[-12:]
        )

        logger.info(
            "%s | SERVER STOP LOSS | "
            "Entry=%.8f Stop=%.8f Qty=%.8f",
            symbol,
            entry_price,
            stop_price,
            quantity
        )

        order = client.create_order(
            symbol=symbol,
            side="SELL",
            type="STOP_LOSS",
            quantity=quantity,
            stopPrice=decimal_to_string(
                stop_price
            ),
            newClientOrderId=client_order_id,
            newOrderRespType="RESULT"
        )

        order_id = order.get(
            "orderId"
        )

        if not order_id:

            logger.error(
                "%s | STOP LOSS order ID missing",
                symbol
            )

            return None

        logger.info(
            "%s | SERVER STOP ACTIVE | "
            "OrderID=%s Stop=%.8f",
            symbol,
            order_id,
            stop_price
        )

        return {
            "order_id": order_id,
            "client_order_id": client_order_id,
            "stop_price": stop_price
        }

    except BinanceAPIException as e:

        logger.error(
            "%s | SERVER STOP FAILED | %s",
            symbol,
            e
        )

        return None

    except Exception as e:

        logger.error(
            "%s | SERVER STOP ERROR | %s",
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

        open_orders = (
            client.get_open_orders(
                symbol=symbol
            )
        )

        for order in open_orders:

            if order.get(
                "side"
            ) != "SELL":

                continue

            client_order_id = order.get(
                "clientOrderId",
                ""
            )

            order_type = order.get(
                "type",
                ""
            )

            if (
                client_order_id.startswith(
                    STOP_CLIENT_PREFIX
                )
                and order_type == "STOP_LOSS"
            ):

                return {
                    "order_id": order.get(
                        "orderId"
                    ),
                    "client_order_id":
                        client_order_id,
                    "stop_price": float(
                        order.get(
                            "stopPrice",
                            0
                        )
                    )
                }

        return None

    except Exception as e:

        logger.error(
            "%s | Find STOP error: %s",
            symbol,
            e
        )

        return None


# ============================================================
# STOP ORDER STATUS
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
            "%s | STOP status error: %s",
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
            "%s | SERVER STOP CANCELLED | "
            "OrderID=%s",
            symbol,
            order_id
        )

        return True

    except BinanceAPIException as e:

        logger.warning(
            "%s | STOP cancel failed | %s",
            symbol,
            e
        )

        return False

    except Exception as e:

        logger.warning(
            "%s | STOP cancel error | %s",
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
            quantity=quantity
        )

        logger.info(
            "%s | MARKET SELL SUCCESS | "
            "OrderID=%s",
            symbol,
            order.get("orderId")
        )

        return order

    except BinanceAPIException as e:

        logger.error(
            "%s | MARKET SELL FAILED | %s",
            symbol,
            e
        )

        return None

    except Exception as e:

        logger.error(
            "%s | Market sell error | %s",
            symbol,
            e
        )

        return None


# ============================================================
# BUY
# ============================================================

def buy_symbol(
    symbol
):

    if symbol in buying_symbols:
        return

    if symbol in positions:
        return

    last_buy = last_buy_time.get(
        symbol,
        0
    )

    if (
        time.time() - last_buy
        < BUY_COOLDOWN_SECONDS
    ):
        return

    buying_symbols.add(
        symbol
    )

    try:

        logger.info(
            "%s | BUYING %.2f USDT",
            symbol,
            TRADE_AMOUNT_USDT
        )

        order = client.order_market_buy(
            symbol=symbol,
            quoteOrderQty=TRADE_AMOUNT_USDT
        )

        executed_qty = float(
            order.get(
                "executedQty",
                0
            )
        )

        if executed_qty <= 0:

            logger.error(
                "%s | BUY executed quantity is zero",
                symbol
            )

            return

        fills = order.get(
            "fills",
            []
        )

        total_qty = 0.0

        total_cost = 0.0

        for fill in fills:

            fill_qty = float(
                fill.get(
                    "qty",
                    0
                )
            )

            fill_price = float(
                fill.get(
                    "price",
                    0
                )
            )

            if (
                fill_qty <= 0
                or fill_price <= 0
            ):
                continue

            total_qty += fill_qty

            total_cost += (
                fill_qty
                * fill_price
            )

        if total_qty > 0:

            entry_price = (
                total_cost
                / total_qty
            )

        else:

            entry_price = (
                get_current_price(
                    symbol
                )
            )

            if entry_price is None:

                logger.error(
                    "%s | Cannot determine entry price",
                    symbol
                )

                return

        executed_qty = (
            normalize_quantity(
                symbol,
                executed_qty
            )
        )

        if executed_qty <= 0:

            logger.error(
                "%s | Quantity became zero",
                symbol
            )

            return

        logger.info(
            "%s | BUY SUCCESS | "
            "Entry=%.8f Qty=%.8f",
            symbol,
            entry_price,
            executed_qty
        )

        # ====================================================
        # SERVER-SIDE STOP
        # ====================================================

        stop_data = (
            place_server_stop_loss(
                symbol,
                executed_qty,
                entry_price
            )
        )

        # ====================================================
        # NO SERVER STOP = NO UNPROTECTED POSITION
        # ====================================================

        if not stop_data:

            logger.critical(
                "%s | SERVER STOP FAILED -> "
                "EMERGENCY MARKET SELL",
                symbol
            )

            emergency = market_sell(
                symbol,
                executed_qty
            )

            if not emergency:

                logger.critical(
                    "%s | EMERGENCY SELL FAILED! "
                    "MANUAL BINANCE CHECK REQUIRED",
                    symbol
                )

            return

        # ====================================================
        # SAVE POSITION
        # ====================================================

        with positions_lock:

            positions[symbol] = {

                "entry_price": float(
                    entry_price
                ),

                "quantity": float(
                    executed_qty
                ),

                "highest_price": float(
                    entry_price
                ),

                "trailing_active": False,

                "buy_time": time.time(),

                "recovered": False,

                "stop_order_id":
                    stop_data[
                        "order_id"
                    ],

                "stop_client_order_id":
                    stop_data[
                        "client_order_id"
                    ],

                "stop_price":
                    stop_data[
                        "stop_price"
                    ]
            }

        last_buy_time[
            symbol
        ] = time.time()

        logger.info(
            "%s | POSITION ACTIVE | "
            "ServerSL=-1%% | "
            "TrailActivation=+1%% | "
            "Trail=0.5%%",
            symbol
        )

    except BinanceAPIException as e:

        logger.error(
            "%s | BUY FAILED | %s",
            symbol,
            e
        )

    except Exception as e:

        logger.error(
            "%s | BUY ERROR | %s",
            symbol,
            e
        )

    finally:

        buying_symbols.discard(
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

        entry_price = float(
            position[
                "entry_price"
            ]
        )

        highest_price = float(
            position[
                "highest_price"
            ]
        )

        trailing_active = bool(
            position[
                "trailing_active"
            ]
        )

        stop_order_id = position.get(
            "stop_order_id"
        )

    # ========================================================
    # UPDATE HIGHEST
    # ========================================================

    if current_price > highest_price:

        highest_price = current_price

        with positions_lock:

            if symbol in positions:

                positions[symbol][
                    "highest_price"
                ] = highest_price

    # ========================================================
    # SERVER STOP THRESHOLD
    # ========================================================

    server_stop_price = (
        entry_price
        * (1.0 - STOP_LOSS_PCT)
    )

    if (
        current_price <= server_stop_price
        and stop_order_id
    ):

        order_status = (
            get_stop_order_status(
                symbol,
                stop_order_id
            )
        )

        if order_status:

            status = order_status.get(
                "status"
            )

            logger.warning(
                "%s | SERVER STOP CHECK | "
                "Current=%.8f Stop=%.8f Status=%s",
                symbol,
                current_price,
                server_stop_price,
                status
            )

            if status == "FILLED":

                logger.warning(
                    "%s | SERVER STOP FILLED",
                    symbol
                )

                with positions_lock:

                    positions.pop(
                        symbol,
                        None
                    )

                return

            if status in {
                "CANCELED",
                "EXPIRED",
                "REJECTED"
            }:

                logger.warning(
                    "%s | Server stop inactive -> fallback sell",
                    symbol
                )

                sell_symbol(
                    symbol,
                    reason="SERVER_STOP_FALLBACK"
                )

                return

            if status == "NEW":

                return

    # ========================================================
    # TRAILING ACTIVATION
    # ========================================================

    activation_price = (
        entry_price
        * (1.0 + TRAILING_ACTIVATION_PCT)
    )

    if (
        not trailing_active
        and current_price >= activation_price
    ):

        logger.info(
            "%s | TRAILING ACTIVATED | "
            "Entry=%.8f Current=%.8f",
            symbol,
            entry_price,
            current_price
        )

        with positions_lock:

            if symbol in positions:

                positions[symbol][
                    "trailing_active"
                ] = True

                positions[symbol][
                    "highest_price"
                ] = max(
                    highest_price,
                    current_price
                )

        trailing_active = True

    # ========================================================
    # TRAILING STOP
    # ========================================================

    if trailing_active:

        trailing_stop_price = (
            highest_price
            * (1.0 - TRAILING_STOP_PCT)
        )

        if current_price <= trailing_stop_price:

            logger.warning(
                "%s | TRAILING STOP HIT | "
                "Current=%.8f Highest=%.8f "
                "Trail=%.8f",
                symbol,
                current_price,
                highest_price,
                trailing_stop_price
            )

            sell_symbol(
                symbol,
                reason="TRAILING_STOP"
            )


# ============================================================
# SELL SYMBOL
# ============================================================

def sell_symbol(
    symbol,
    reason="UNKNOWN"
):

    if symbol in selling_symbols:
        return

    selling_symbols.add(
        symbol
    )

    try:

        with positions_lock:

            position = positions.get(
                symbol
            )

        if not position:
            return

        quantity = float(
            position[
                "quantity"
            ]
        )

        stop_order_id = position.get(
            "stop_order_id"
        )

        logger.warning(
            "%s | SELL START | Reason=%s",
            symbol,
            reason
        )

        # ====================================================
        # CANCEL SERVER STOP FIRST
        # ====================================================

        if stop_order_id:

            cancelled = (
                cancel_server_stop(
                    symbol,
                    stop_order_id
                )
            )

            if not cancelled:

                status_data = (
                    get_stop_order_status(
                        symbol,
                        stop_order_id
                    )
                )

                if status_data:

                    status = status_data.get(
                        "status"
                    )

                    if status == "FILLED":

                        logger.warning(
                            "%s | Server STOP already FILLED",
                            symbol
                        )

                        with positions_lock:

                            positions.pop(
                                symbol,
                                None
                            )

                        return

        # ====================================================
        # MARKET SELL
        # ====================================================

        result = market_sell(
            symbol,
            quantity
        )

        if result:

            logger.info(
                "%s | POSITION CLOSED | "
                "Reason=%s",
                symbol,
                reason
            )

            with positions_lock:

                positions.pop(
                    symbol,
                    None
                )

        else:

            logger.error(
                "%s | SELL FAILED | "
                "Position retained",
                symbol
            )

    except Exception as e:

        logger.error(
            "%s | SELL ERROR | %s",
            symbol,
            e
        )

    finally:

        selling_symbols.discard(
            symbol
        )


# ============================================================
# RECOVER ENTRY FROM BINANCE TRADES
# ============================================================

def recover_entry_price_from_trades(
    symbol,
    base_asset,
    current_balance
):

    try:

        trades = client.get_my_trades(
            symbol=symbol,
            limit=1000
        )

        if not trades:
            return None

        trades = sorted(
            trades,
            key=lambda x: (
                x.get(
                    "time",
                    0
                ),
                x.get(
                    "id",
                    0
                )
            )
        )

        position_qty = 0.0

        total_cost = 0.0

        for trade in trades:

            qty = float(
                trade.get(
                    "qty",
                    0
                )
            )

            price = float(
                trade.get(
                    "price",
                    0
                )
            )

            is_buyer = bool(
                trade.get(
                    "isBuyer",
                    False
                )
            )

            if (
                qty <= 0
                or price <= 0
            ):
                continue

            if is_buyer:

                position_qty += qty

                total_cost += (
                    qty
                    * price
                )

            else:

                if position_qty > 0:

                    avg_entry = (
                        total_cost
                        / position_qty
                    )

                    sold_qty = min(
                        qty,
                        position_qty
                    )

                    total_cost -= (
                        sold_qty
                        * avg_entry
                    )

                    position_qty -= (
                        sold_qty
                    )

                    if position_qty <= 1e-12:

                        position_qty = 0.0

                        total_cost = 0.0

        if current_balance <= 0:
            return None

        if position_qty <= 0:
            return None

        if total_cost <= 0:
            return None

        return float(
            total_cost
            / position_qty
        )

    except Exception as e:

        logger.error(
            "%s | Trade recovery error: %s",
            symbol,
            e
        )

        return None


# ============================================================
# RECOVER POSITIONS
# ============================================================

def recover_positions():

    global symbols

    logger.info(
        "Checking Binance account for existing positions..."
    )

    try:

        account = client.get_account()

        balances = account.get(
            "balances",
            []
        )

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

            total_balance = (
                free + locked
            )

            if total_balance <= 0:
                continue

            if asset in STABLECOINS:
                continue

            if asset in {
                "BTC",
                "ETH"
            }:
                continue

            symbol = (
                asset
                + "USDT"
            )

            if symbol not in symbol_info:
                continue

            entry_price = (
                recover_entry_price_from_trades(
                    symbol,
                    asset,
                    total_balance
                )
            )

            if not entry_price:

                logger.warning(
                    "%s | Entry recovery failed",
                    symbol
                )

                continue

            current_price = (
                get_current_price(
                    symbol
                )
            )

            if current_price is None:
                continue

            # =================================================
            # Existing server stop
            # =================================================

            stop_data = (
                find_existing_server_stop(
                    symbol
                )
            )

            # =================================================
            # Trailing state
            # =================================================

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

            highest_price = max(
                entry_price,
                current_price
            )

            # =================================================
            # If stop missing, recreate it
            # =================================================

            if not stop_data:

                logger.warning(
                    "%s | Recovery: "
                    "server STOP missing -> creating",
                    symbol
                )

                stop_data = (
                    place_server_stop_loss(
                        symbol,
                        total_balance,
                        entry_price
                    )
                )

            if stop_data:

                stop_order_id = (
                    stop_data[
                        "order_id"
                    ]
                )

                stop_client_order_id = (
                    stop_data[
                        "client_order_id"
                    ]
                )

                stop_price = (
                    stop_data[
                        "stop_price"
                    ]
                )

            else:

                stop_order_id = None

                stop_client_order_id = None

                stop_price = (
                    entry_price
                    * (
                        1.0
                        - STOP_LOSS_PCT
                    )
                )

            # =================================================
            # Store
            # =================================================

            with positions_lock:

                positions[symbol] = {

                    "entry_price":
                        float(entry_price),

                    "quantity":
                        float(total_balance),

                    "highest_price":
                        float(highest_price),

                    "trailing_active":
                        bool(trailing_active),

                    "buy_time":
                        time.time(),

                    "recovered":
                        True,

                    "stop_order_id":
                        stop_order_id,

                    "stop_client_order_id":
                        stop_client_order_id,

                    "stop_price":
                        float(stop_price)
                }

            # =================================================
            # Add recovered symbol to monitoring
            # =================================================

            if symbol not in symbols:

                symbols.append(
                    symbol
                )

            logger.info(
                "%s | POSITION RECOVERED | "
                "Entry=%.8f Current=%.8f "
                "Qty=%.8f Trailing=%s "
                "ServerStop=%s",
                symbol,
                entry_price,
                current_price,
                total_balance,
                trailing_active,
                stop_order_id
            )

    except Exception as e:

        logger.error(
            "Position recovery failed: %s",
            e
        )


# ============================================================
# CLOSED CANDLE PROCESSOR
# ============================================================

def process_closed_candle(
    symbol
):

    try:

        with positions_lock:

            if symbol in positions:
                return

        if check_buy_condition(
            symbol
        ):

            buy_symbol(
                symbol
            )

    except Exception as e:

        logger.error(
            "%s | candle processing error: %s",
            symbol,
            e
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

        event = data.get(
            "data",
            data
        )

        event_type = event.get(
            "e"
        )

        # ====================================================
        # KLINE
        # ====================================================

        if event_type == "kline":

            kline = event.get(
                "k",
                {}
            )

            symbol = kline.get(
                "s"
            )

            is_closed = bool(
                kline.get(
                    "x",
                    False
                )
            )

            if (
                symbol
                and is_closed
            ):

                process_closed_candle(
                    symbol
                )

        # ====================================================
        # MINI TICKER
        # ====================================================

        elif (
            event_type
            == "24hrMiniTicker"
        ):

            symbol = event.get(
                "s"
            )

            close_price = event.get(
                "c"
            )

            if (
                not symbol
                or not close_price
            ):
                return

            try:

                current_price = float(
                    close_price
                )

            except Exception:

                return

            if current_price <= 0:
                return

            with positions_lock:

                has_position = (
                    symbol in positions
                )

            if has_position:

                check_position(
                    symbol,
                    current_price
                )

    except Exception as e:

        logger.error(
            "WebSocket message processing error: %s",
            e
        )


# ============================================================
# WEBSOCKET URL
# ============================================================

def make_stream_url():

    streams = []

    for symbol in symbols:

        lower = symbol.lower()

        streams.append(
            f"{lower}@kline_5m"
        )

        streams.append(
            f"{lower}@miniTicker"
        )

    stream_string = "/".join(
        streams
    )

    return (
        "wss://stream.binance.com:9443/"
        "stream?streams="
        + stream_string
    )


# ============================================================
# WEBSOCKET LOOP
# ============================================================

def websocket_loop():

    while True:

        ws = None

        try:

            if not symbols:

                logger.warning(
                    "No symbols available"
                )

                time.sleep(
                    RECONNECT_DELAY
                )

                continue

            url = make_stream_url()

            logger.info(
                "Connecting WebSocket with %d symbols...",
                len(symbols)
            )

            # =================================================
            # CALLBACKS
            # =================================================

            def on_open(
                ws_app
            ):

                logger.info(
                    "WebSocket connected successfully"
                )

            def on_message(
                ws_app,
                message
            ):

                process_ws_message(
                    message
                )

            def on_error(
                ws_app,
                error
            ):

                logger.error(
                    "WebSocket error: %s",
                    error
                )

            def on_close(
                ws_app,
                close_status_code,
                close_msg
            ):

                logger.warning(
                    "WebSocket closed | "
                    "Code=%s Message=%s",
                    close_status_code,
                    close_msg
                )

            ws = websocket.WebSocketApp(
                url,
                on_open=on_open,
                on_message=on_message,
                on_error=on_error,
                on_close=on_close
            )

            # =================================================
            # MORE TOLERANT HEARTBEAT
            # =================================================

            ws.run_forever(
                ping_interval=WS_PING_INTERVAL,
                ping_timeout=WS_PING_TIMEOUT,
                ping_payload="ping"
            )

        except Exception as e:

            logger.error(
                "WebSocket loop exception: %s",
                e
            )

        logger.warning(
            "WebSocket reconnecting in %d seconds...",
            RECONNECT_DELAY
        )

        time.sleep(
            RECONNECT_DELAY
        )


# ============================================================
# POSITION MONITOR
# ============================================================

def position_monitor():

    while True:

        try:

            with positions_lock:

                current_positions = list(
                    positions.keys()
                )

            if current_positions:

                logger.info(
                    "POSITION MONITOR | "
                    "Active positions: %d",
                    len(current_positions)
                )

        except Exception as e:

            logger.error(
                "Position monitor error: %s",
                e
            )

        time.sleep(
            60
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
        "1. OPEN < BB20 LOWER"
    )

    logger.info(
        "2. CLOSE > BB20 LOWER"
    )

    logger.info(
        "3. ADX14 > %.2f",
        ADX_MIN
    )

    logger.info(
        "4. +DI > -DI"
    )

    logger.info(
        "SERVER STOP LOSS = %.2f%%",
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
        "TIMEFRAME = 5 MIN"
    )

    logger.info(
        "TOP SYMBOLS = %d",
        TOP_SYMBOLS
    )

    logger.info(
        "=" * 70
    )

    # ========================================================
    # Exchange info
    # ========================================================

    load_exchange_info()

    # ========================================================
    # Top symbols
    # ========================================================

    load_top_symbols()

    # ========================================================
    # Recover positions
    # ========================================================

    recover_positions()

    # ========================================================
    # Public WebSocket
    # ========================================================

    ws_thread = threading.Thread(
        target=websocket_loop,
        daemon=True
    )

    ws_thread.start()

    # ========================================================
    # Position monitor
    # ========================================================

    monitor_thread = threading.Thread(
        target=position_monitor,
        daemon=True
    )

    monitor_thread.start()

    logger.info(
        "BOT STARTUP COMPLETE"
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
        port=port
    )
