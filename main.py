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
    raise RuntimeError("BINANCE_API_KEY / BINANCE_API_SECRET missing")

client = Client(API_KEY, API_SECRET)


# ============================================================
# TRADING SETTINGS
# ============================================================

TRADE_AMOUNT_USDT = 35.0

TIMEFRAME = Client.KLINE_INTERVAL_5MINUTE

TOP_SYMBOLS = 150

# BUY CONDITIONS
BB_PERIOD = 20
ADX_PERIOD = 14
ADX_MIN = 20.0

# ============================================================
# RISK MANAGEMENT
# ============================================================

# Binance SERVER-SIDE stop
STOP_LOSS_PCT = 0.0100       # 1%

# Python trailing activation
TRAILING_ACTIVATION_PCT = 0.0100   # +1%

# Python trailing distance
TRAILING_STOP_PCT = 0.0050         # 0.5%

# ============================================================
# RATE LIMIT / SAFETY
# ============================================================

KLINE_REQUEST_COOLDOWN = 2.0

BUY_COOLDOWN_SECONDS = 60

KLINE_REQUEST_DELAY = 0.05

RECONNECT_DELAY = 10

# ============================================================
# EXCLUDED ASSETS
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
# SERVER-SIDE STOP ORDER PREFIX
# ============================================================

STOP_CLIENT_PREFIX = "BBADXSL_"


# ============================================================
# GLOBAL DATA
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
    """
    Binance LOT_SIZE / PRICE_FILTER অনুযায়ী নিচের দিকে round করে।
    """

    value = Decimal(str(value))
    step = Decimal(str(step))

    if step <= 0:
        return value

    return (value / step).to_integral_value(
        rounding=ROUND_DOWN
    ) * step


def decimal_to_string(value):
    """
    Decimal কে Binance-compatible string করে।
    """

    text = format(value, "f")

    if "." in text:
        text = text.rstrip("0").rstrip(".")

    return text


# ============================================================
# EXCHANGE INFO
# ============================================================

def load_exchange_info():

    logger.info("Loading Binance exchange info...")

    info = client.get_exchange_info()

    for item in info["symbols"]:

        symbol = item["symbol"]

        if item.get("status") != "TRADING":
            continue

        base_asset = item.get("baseAsset")
        quote_asset = item.get("quoteAsset")

        if quote_asset != "USDT":
            continue

        lot_step = None
        min_qty = None
        min_notional = 0.0
        tick_size = None

        for f in item.get("filters", []):

            filter_type = f.get("filterType")

            # -----------------------------
            # LOT SIZE
            # -----------------------------

            if filter_type == "LOT_SIZE":

                lot_step = float(
                    f.get("stepSize", "0.00000001")
                )

                min_qty = float(
                    f.get("minQty", "0")
                )

            # -----------------------------
            # PRICE FILTER
            # -----------------------------

            elif filter_type == "PRICE_FILTER":

                tick_size = float(
                    f.get("tickSize", "0.00000001")
                )

            # -----------------------------
            # MIN NOTIONAL
            # -----------------------------

            elif filter_type == "MIN_NOTIONAL":

                min_notional = float(
                    f.get("minNotional", "0")
                )

            # -----------------------------
            # NOTIONAL
            # -----------------------------

            elif filter_type == "NOTIONAL":

                min_notional = max(
                    min_notional,
                    float(f.get("minNotional", "0"))
                )

        symbol_info[symbol] = {
            "base_asset": base_asset,
            "quote_asset": quote_asset,
            "step_size": lot_step or 0.00000001,
            "min_qty": min_qty or 0.0,
            "min_notional": min_notional,
            "tick_size": tick_size or 0.00000001,
        }

    logger.info(
        "Exchange info loaded for %d USDT symbols",
        len(symbol_info)
    )


# ============================================================
# TOP SYMBOLS
# ============================================================

def load_top_symbols():

    global symbols

    logger.info("Loading top USDT symbols...")

    tickers = client.get_ticker()

    candidates = []

    for ticker in tickers:

        symbol = ticker.get("symbol", "")

        if symbol not in symbol_info:
            continue

        if symbol in EXCLUDED_SYMBOLS:
            continue

        info = symbol_info.get(symbol)

        if not info:
            continue

        base_asset = info["base_asset"]

        if base_asset in STABLECOINS:
            continue

        try:
            quote_volume = float(
                ticker.get("quoteVolume", 0)
            )
        except Exception:
            continue

        if quote_volume <= 0:
            continue

        candidates.append(
            (symbol, quote_volume)
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
# PRICE
# ============================================================

def get_current_price(symbol):

    try:

        data = client.get_symbol_ticker(
            symbol=symbol
        )

        return float(data["price"])

    except Exception as e:

        logger.error(
            "%s | price error: %s",
            symbol,
            e
        )

        return None


# ============================================================
# KLINES
# ============================================================

def get_closed_klines(symbol):

    now = time.time()

    last_request = last_kline_request.get(
        symbol,
        0
    )

    if now - last_request < KLINE_REQUEST_COOLDOWN:

        return None

    last_kline_request[symbol] = now

    try:

        time.sleep(KLINE_REQUEST_DELAY)

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

        for col in [
            "open",
            "high",
            "low",
            "close",
            "volume"
        ]:

            df[col] = pd.to_numeric(
                df[col],
                errors="coerce"
            )

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

    if df is None or len(df) < 50:
        return None

    df = df.copy()

    # ========================================================
    # EMA5
    # ========================================================

    df["ema5"] = (
        df["close"]
        .ewm(
            span=5,
            adjust=False
        )
        .mean()
    )

    # ========================================================
    # BB20
    # ========================================================

    df["bb_middle"] = (
        df["close"]
        .rolling(BB_PERIOD)
        .mean()
    )

    rolling_std = (
        df["close"]
        .rolling(BB_PERIOD)
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

    # ========================================================
    # ADX14
    # ========================================================

    high = df["high"]
    low = df["low"]
    close = df["close"]

    prev_close = close.shift(1)

    tr1 = high - low
    tr2 = (high - prev_close).abs()
    tr3 = (low - prev_close).abs()

    tr = pd.concat(
        [tr1, tr2, tr3],
        axis=1
    ).max(axis=1)

    up_move = high.diff()

    down_move = -low.diff()

    plus_dm = pd.Series(
        0.0,
        index=df.index
    )

    minus_dm = pd.Series(
        0.0,
        index=df.index
    )

    plus_condition = (
        (up_move > down_move)
        & (up_move > 0)
    )

    minus_condition = (
        (down_move > up_move)
        & (down_move > 0)
    )

    plus_dm.loc[plus_condition] = (
        up_move.loc[plus_condition]
    )

    minus_dm.loc[minus_condition] = (
        down_move.loc[minus_condition]
    )

    alpha = 1.0 / ADX_PERIOD

    atr = tr.ewm(
        alpha=alpha,
        adjust=False
    ).mean()

    plus_dm_smoothed = plus_dm.ewm(
        alpha=alpha,
        adjust=False
    ).mean()

    minus_dm_smoothed = minus_dm.ewm(
        alpha=alpha,
        adjust=False
    ).mean()

    plus_di = (
        100.0
        * plus_dm_smoothed
        / atr.replace(0, pd.NA)
    )

    minus_di = (
        100.0
        * minus_dm_smoothed
        / atr.replace(0, pd.NA)
    )

    dx = (
        100.0
        * (plus_di - minus_di).abs()
        / (plus_di + minus_di).replace(
            0,
            pd.NA
        )
    )

    adx = dx.ewm(
        alpha=alpha,
        adjust=False
    ).mean()

    df["plus_di"] = plus_di
    df["minus_di"] = minus_di
    df["adx"] = adx

    return df


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

    # Last candle may still be open.
    # Therefore use the previous fully closed candle.

    candle = df.iloc[-2]

    open_price = float(candle["open"])
    close_price = float(candle["close"])
    bb_lower = float(candle["bb_lower"])
    adx = float(candle["adx"])
    plus_di = float(candle["plus_di"])
    minus_di = float(candle["minus_di"])

    if any(
        pd.isna(x)
        for x in [
            open_price,
            close_price,
            bb_lower,
            adx,
            plus_di,
            minus_di
        ]
    ):
        return False

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
            "Open=%.8f Close=%.8f BB20Lower=%.8f "
            "ADX=%.2f +DI=%.2f -DI=%.2f",
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
# ROUND QUANTITY
# ============================================================

def normalize_quantity(symbol, quantity):

    info = symbol_info.get(symbol)

    if not info:
        return quantity

    step_size = info["step_size"]

    qty = floor_to_step(
        quantity,
        step_size
    )

    return float(qty)


# ============================================================
# ROUND PRICE
# ============================================================

def normalize_price(symbol, price):

    info = symbol_info.get(symbol)

    if not info:
        return price

    tick_size = info["tick_size"]

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

        info = symbol_info.get(symbol)

        if not info:
            logger.error(
                "%s | No symbol info for STOP LOSS",
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

        client_order_id = (
            STOP_CLIENT_PREFIX
            + str(int(time.time() * 1000))[-12:]
        )

        logger.info(
            "%s | Placing SERVER STOP LOSS | "
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
                Decimal(str(stop_price))
            ),
            newClientOrderId=client_order_id,
            newOrderRespType="RESULT"
        )

        order_id = order.get("orderId")

        if not order_id:

            logger.error(
                "%s | Server STOP LOSS response has no orderId",
                symbol
            )

            return None

        logger.info(
            "%s | SERVER STOP LOSS ACTIVE | "
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
            "%s | SERVER STOP LOSS FAILED | %s",
            symbol,
            e
        )

        return None

    except Exception as e:

        logger.error(
            "%s | Server stop error: %s",
            symbol,
            e
        )

        return None


# ============================================================
# FIND EXISTING BOT STOP ORDER
# ============================================================

def find_existing_server_stop(symbol):

    try:

        open_orders = client.get_open_orders(
            symbol=symbol
        )

        for order in open_orders:

            if order.get("side") != "SELL":
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
                    "order_id": order.get("orderId"),
                    "client_order_id": client_order_id,
                    "stop_price": float(
                        order.get("stopPrice", 0)
                    )
                }

        return None

    except Exception as e:

        logger.error(
            "%s | find stop error: %s",
            symbol,
            e
        )

        return None


# ============================================================
# GET SERVER STOP STATUS
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

        return order

    except Exception as e:

        logger.error(
            "%s | stop order status error: %s",
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

        logger.info(
            "%s | Cancelling server STOP LOSS | OrderID=%s",
            symbol,
            order_id
        )

        client.cancel_order(
            symbol=symbol,
            orderId=order_id
        )

        logger.info(
            "%s | Server STOP LOSS cancelled",
            symbol
        )

        return True

    except BinanceAPIException as e:

        logger.warning(
            "%s | STOP cancel failed: %s",
            symbol,
            e
        )

        return False

    except Exception as e:

        logger.warning(
            "%s | STOP cancel error: %s",
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
                "%s | Invalid SELL quantity",
                symbol
            )

            return None

        logger.info(
            "%s | MARKET SELL | Qty=%.8f",
            symbol,
            quantity
        )

        order = client.order_market_sell(
            symbol=symbol,
            quantity=quantity
        )

        logger.info(
            "%s | MARKET SELL SUCCESS | OrderID=%s",
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
            "%s | Market sell error: %s",
            symbol,
            e
        )

        return None


# ============================================================
# BUY
# ============================================================

def buy_symbol(symbol):

    if symbol in buying_symbols:
        return

    if symbol in positions:
        return

    last_buy = last_buy_time.get(
        symbol,
        0
    )

    if time.time() - last_buy < BUY_COOLDOWN_SECONDS:
        return

    buying_symbols.add(symbol)

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

        fills = order.get(
            "fills",
            []
        )

        if executed_qty <= 0:

            logger.error(
                "%s | BUY returned zero quantity",
                symbol
            )

            return

        # ====================================================
        # Calculate weighted average entry
        # ====================================================

        total_qty = 0.0
        total_cost = 0.0

        for fill in fills:

            fill_qty = float(
                fill.get("qty", 0)
            )

            fill_price = float(
                fill.get("price", 0)
            )

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

            entry_price = get_current_price(
                symbol
            )

            if entry_price is None:
                logger.error(
                    "%s | Cannot determine entry price",
                    symbol
                )
                return

        executed_qty = normalize_quantity(
            symbol,
            executed_qty
        )

        if executed_qty <= 0:
            logger.error(
                "%s | Quantity became zero after normalization",
                symbol
            )
            return

        logger.info(
            "%s | BUY SUCCESS | Entry=%.8f Qty=%.8f",
            symbol,
            entry_price,
            executed_qty
        )

        # ====================================================
        # IMPORTANT:
        # SERVER-SIDE STOP LOSS FIRST
        # ====================================================

        stop_data = place_server_stop_loss(
            symbol=symbol,
            quantity=executed_qty,
            entry_price=entry_price
        )

        # ====================================================
        # If server stop failed:
        # Do NOT leave position unprotected.
        # Immediately market sell.
        # ====================================================

        if not stop_data:

            logger.error(
                "%s | SERVER STOP FAILED -> "
                "EMERGENCY MARKET SELL",
                symbol
            )

            emergency_sell = market_sell(
                symbol,
                executed_qty
            )

            if emergency_sell:

                logger.info(
                    "%s | Emergency sell completed",
                    symbol
                )

            else:

                logger.critical(
                    "%s | EMERGENCY SELL ALSO FAILED! "
                    "MANUAL CHECK REQUIRED",
                    symbol
                )

            return

        # ====================================================
        # Store position
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

                "stop_order_id": stop_data[
                    "order_id"
                ],

                "stop_client_order_id": stop_data[
                    "client_order_id"
                ],

                "stop_price": stop_data[
                    "stop_price"
                ]
            }

        last_buy_time[symbol] = time.time()

        logger.info(
            "%s | POSITION CREATED | "
            "SERVER STOP=-1%% | "
            "TRAIL ACTIVATION=+1%% | "
            "TRAIL DISTANCE=0.5%%",
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
            "%s | Buy error: %s",
            symbol,
            e
        )

    finally:

        buying_symbols.discard(symbol)


# ============================================================
# CHECK POSITION
# ============================================================

def check_position(
    symbol,
    current_price
):

    with positions_lock:

        position = positions.get(symbol)

        if not position:
            return

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

    # ========================================================
    # UPDATE HIGHEST PRICE
    # ========================================================

    if current_price > highest_price:

        highest_price = current_price

        with positions_lock:

            if symbol in positions:

                positions[symbol][
                    "highest_price"
                ] = highest_price

    # ========================================================
    # SERVER-SIDE STOP THRESHOLD
    #
    # Normally Binance itself should trigger the order.
    # Python does NOT immediately market sell here.
    # Instead check Binance order status.
    # ========================================================

    server_stop_price = (
        entry_price
        * (1.0 - STOP_LOSS_PCT)
    )

    if (
        current_price <= server_stop_price
        and stop_order_id
    ):

        logger.warning(
            "%s | PRICE BELOW SERVER STOP | "
            "Current=%.8f Stop=%.8f",
            symbol,
            current_price,
            server_stop_price
        )

        order_status = get_stop_order_status(
            symbol,
            stop_order_id
        )

        if order_status:

            status = order_status.get(
                "status"
            )

            logger.info(
                "%s | SERVER STOP STATUS=%s",
                symbol,
                status
            )

            # ================================================
            # STOP FILLED
            # ================================================

            if status == "FILLED":

                logger.warning(
                    "%s | SERVER-SIDE STOP FILLED",
                    symbol
                )

                with positions_lock:
                    positions.pop(
                        symbol,
                        None
                    )

                return

            # ================================================
            # STOP CANCELLED / EXPIRED / REJECTED
            # ================================================

            if status in {
                "CANCELED",
                "EXPIRED",
                "REJECTED"
            }:

                logger.error(
                    "%s | Server stop no longer active -> "
                    "fallback MARKET SELL",
                    symbol
                )

                sell_symbol(
                    symbol,
                    reason="SERVER_STOP_FALLBACK"
                )

                return

            # ================================================
            # NEW
            #
            # Let Binance handle it.
            # ================================================

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
            "Entry=%.8f Current=%.8f Activation=%.8f",
            symbol,
            entry_price,
            current_price,
            activation_price
        )

        # ====================================================
        # Mark active first
        # ====================================================

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
    # TRAILING SELL
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
                "TrailStop=%.8f",
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

        logger.warning(
            "%s | SELL START | Reason=%s",
            symbol,
            reason
        )

        # ====================================================
        # Cancel server-side stop BEFORE market sell
        # ====================================================

        if stop_order_id:

            cancel_result = (
                cancel_server_stop(
                    symbol,
                    stop_order_id
                )
            )

            # ================================================
            # If cancellation failed,
            # check whether stop was already filled.
            # ================================================

            if not cancel_result:

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

        order = market_sell(
            symbol,
            quantity
        )

        if order:

            logger.info(
                "%s | POSITION CLOSED | Reason=%s",
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
                "%s | SELL FAILED | Position retained",
                symbol
            )

    except Exception as e:

        logger.error(
            "%s | sell_symbol error: %s",
            symbol,
            e
        )

    finally:

        selling_symbols.discard(symbol)


# ============================================================
# RECOVER ENTRY PRICE FROM TRADES
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
                x.get("time", 0),
                x.get("id", 0)
            )
        )

        position_qty = 0.0
        total_cost = 0.0

        for trade in trades:

            qty = float(
                trade.get("qty", 0)
            )

            price = float(
                trade.get("price", 0)
            )

            is_buyer = bool(
                trade.get("isBuyer", False)
            )

            if qty <= 0 or price <= 0:
                continue

            if is_buyer:

                position_qty += qty

                total_cost += (
                    qty * price
                )

            else:

                # Average-cost reduction
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

                    position_qty -= sold_qty

                    if position_qty <= 1e-12:

                        position_qty = 0.0
                        total_cost = 0.0

        # Current Binance balance is the source
        # of truth for remaining quantity.

        if current_balance <= 0:
            return None

        if position_qty <= 0:
            return None

        if total_cost <= 0:
            return None

        entry_price = (
            total_cost
            / position_qty
        )

        return float(entry_price)

    except Exception as e:

        logger.error(
            "%s | trade-history recovery error: %s",
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
        "Checking Binance balances for recovery..."
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

            symbol = asset + "USDT"

            if symbol not in symbol_info:
                continue

            # =================================================
            # Find actual entry from Binance trades
            # =================================================

            entry_price = (
                recover_entry_price_from_trades(
                    symbol,
                    asset,
                    total_balance
                )
            )

            if not entry_price:

                logger.warning(
                    "%s | Could not recover entry price",
                    symbol
                )

                continue

            current_price = get_current_price(
                symbol
            )

            if current_price is None:
                continue

            # =================================================
            # Find existing server stop
            # =================================================

            stop_data = (
                find_existing_server_stop(
                    symbol
                )
            )

            # =================================================
            # Determine trailing state
            # =================================================

            trailing_active = (
                current_price
                >= entry_price
                * (1.0 + TRAILING_ACTIVATION_PCT)
            )

            highest_price = max(
                entry_price,
                current_price
            )

            # =================================================
            # If no server stop and trailing not active,
            # create new server stop.
            # =================================================

            if (
                not stop_data
                and not trailing_active
            ):

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

            # =================================================
            # If price already activated trailing:
            #
            # Existing fixed stop can remain until Python
            # trailing sell happens.
            #
            # We do NOT immediately cancel it because it
            # remains an emergency downside protection.
            # =================================================

            if stop_data:

                stop_order_id = (
                    stop_data["order_id"]
                )

                stop_client_order_id = (
                    stop_data["client_order_id"]
                )

                stop_price = (
                    stop_data["stop_price"]
                )

            else:

                stop_order_id = None
                stop_client_order_id = None

                stop_price = (
                    entry_price
                    * (1.0 - STOP_LOSS_PCT)
                )

            # =================================================
            # Store recovered position
            # =================================================

            with positions_lock:

                positions[symbol] = {

                    "entry_price": float(
                        entry_price
                    ),

                    "quantity": float(
                        total_balance
                    ),

                    "highest_price": float(
                        highest_price
                    ),

                    "trailing_active": bool(
                        trailing_active
                    ),

                    "buy_time": time.time(),

                    "recovered": True,

                    "stop_order_id": (
                        stop_order_id
                    ),

                    "stop_client_order_id": (
                        stop_client_order_id
                    ),

                    "stop_price": float(
                        stop_price
                    )
                }

            # =================================================
            # Make sure recovered symbol is monitored
            # =================================================

            if symbol not in symbols:

                symbols.append(symbol)

                logger.info(
                    "%s | Added recovered symbol "
                    "to WebSocket monitoring",
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

    with positions_lock:

        if symbol in positions:
            return

    # ========================================================
    # Check BUY
    # ========================================================

    try:

        if check_buy_condition(symbol):

            buy_symbol(symbol)

    except Exception as e:

        logger.error(
            "%s | candle processing error: %s",
            symbol,
            e
        )


# ============================================================
# WEBSOCKET MESSAGE
# ============================================================

def process_ws_message(message):

    try:

        data = json.loads(
            message
        )

        # Combined stream format
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

        elif event_type == "24hrMiniTicker":

            symbol = event.get(
                "s"
            )

            close_price = event.get(
                "c"
            )

            if not symbol or not close_price:
                return

            try:

                current_price = float(
                    close_price
                )

            except Exception:
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

        # 5m candles
        streams.append(
            f"{lower}@kline_5m"
        )

        # mini ticker
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

        try:

            if not symbols:

                logger.warning(
                    "No symbols available for WebSocket"
                )

                time.sleep(10)

                continue

            url = make_stream_url()

            logger.info(
                "Connecting WebSocket with %d symbols...",
                len(symbols)
            )

            def on_message(
                ws,
                message
            ):

                process_ws_message(
                    message
                )

            def on_error(
                ws,
                error
            ):

                logger.error(
                    "WebSocket error: %s",
                    error
                )

            def on_close(
                ws,
                close_status_code,
                close_msg
            ):

                logger.warning(
                    "WebSocket closed | "
                    "Code=%s Message=%s",
                    close_status_code,
                    close_msg
                )

            def on_open(ws):

                logger.info(
                    "WebSocket connected successfully"
                )

            ws = websocket.WebSocketApp(
                url,
                on_open=on_open,
                on_message=on_message,
                on_error=on_error,
                on_close=on_close
            )

            ws.run_forever(
                ping_interval=20,
                ping_timeout=10
            )

        except Exception as e:

            logger.error(
                "WebSocket loop error: %s",
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
# POSITION HEALTH MONITOR
# ============================================================

def position_monitor():

    while True:

        try:

            with positions_lock:

                current_positions = list(
                    positions.keys()
                )

            # This loop is intentionally lightweight.
            #
            # Actual real-time monitoring is done by
            # WebSocket miniTicker.
            #
            # This thread is mainly a heartbeat.

            if current_positions:

                logger.info(
                    "POSITION MONITOR | Active positions: %d",
                    len(current_positions)
                )

        except Exception as e:

            logger.error(
                "Position monitor error: %s",
                e
            )

        time.sleep(60)


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
        "OPEN < BB20 LOWER"
    )

    logger.info(
        "CLOSE > BB20 LOWER"
    )

    logger.info(
        "ADX14 > %.2f",
        ADX_MIN
    )

    logger.info(
        "+DI > -DI"
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
        "TRAILING STOP = %.2f%%",
        TRAILING_STOP_PCT * 100
    )

    logger.info(
        "TRADE AMOUNT = %.2f USDT",
        TRADE_AMOUNT_USDT
    )

    logger.info(
        "TIMEFRAME = 5 MINUTE"
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
    # Recover existing positions
    # ========================================================

    recover_positions()

    # ========================================================
    # Start WebSocket
    # ========================================================

    websocket_thread = threading.Thread(
        target=websocket_loop,
        daemon=True
    )

    websocket_thread.start()

    # ========================================================
    # Start position monitor
    # ========================================================

    monitor_thread = threading.Thread(
        target=position_monitor,
        daemon=True
    )

    monitor_thread.start()

    logger.info(
        "Bot startup completed."
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
