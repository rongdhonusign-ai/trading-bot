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
        "BINANCE_API_KEY and BINANCE_API_SECRET environment variables are required."
    )


# ------------------------------------------------------------
# TRADE SETTINGS
# ------------------------------------------------------------

TRADE_AMOUNT_USDT = 35.0

TIMEFRAME = "5m"

TOP_SYMBOLS = 150

# ------------------------------------------------------------
# BOLLINGER BAND
# ------------------------------------------------------------

BB_PERIOD = 20
BB_STD = 2.0

# ------------------------------------------------------------
# EMA
# ------------------------------------------------------------

EMA_PERIOD = 5

# ------------------------------------------------------------
# ADX
# ------------------------------------------------------------

ADX_PERIOD = 14
ADX_MIN = 20.0

# ------------------------------------------------------------
# STOP LOSS
# ------------------------------------------------------------

STOP_LOSS_PCT = 0.010
# 1.00%

# ------------------------------------------------------------
# TRAILING STOP
# ------------------------------------------------------------

TRAILING_ACTIVATION_PCT = 0.010
# +1.00% profit হলে trailing শুরু

TRAILING_STOP_PCT = 0.005
# Highest price থেকে 0.50% নিচে নামলে sell


# ------------------------------------------------------------
# BALANCE BUFFER
# ------------------------------------------------------------

SELL_BALANCE_BUFFER = 0.999


# ============================================================
# LOGGING
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

logger = logging.getLogger(__name__)


# ============================================================
# BINANCE CLIENT
# ============================================================

client = Client(API_KEY, API_SECRET)


# ============================================================
# FLASK
# ============================================================

app = Flask(__name__)


@app.route("/")
def home():
    return jsonify({
        "status": "Trading Bot is Active & Running!",
        "strategy": "BB20 Lower + EMA5 + ADX14 > 20",
        "timeframe": TIMEFRAME
    })


@app.route("/health")
def health():
    return jsonify({
        "status": "ok",
        "positions": len(positions),
        "symbols": len(symbols)
    })


# ============================================================
# GLOBAL DATA
# ============================================================

symbols = []

symbol_info = {}

positions = {}

selling_symbols = set()

positions_lock = threading.Lock()

symbols_lock = threading.Lock()


# ============================================================
# STABLECOINS / NON-ALT COINS
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
    "WBTC",
    "WETH",
    "PAX"
}


# ============================================================
# EXCHANGE INFO
# ============================================================

def load_exchange_info():
    global symbol_info

    logger.info("Loading Binance exchange information...")

    info = client.get_exchange_info()

    temp = {}

    for s in info["symbols"]:

        if s["status"] != "TRADING":
            continue

        if s["quoteAsset"] != "USDT":
            continue

        if not s.get("isSpotTradingAllowed", False):
            continue

        base_asset = s["baseAsset"]

        if base_asset in STABLECOINS:
            continue

        if base_asset in {"BTC", "ETH"}:
            continue

        temp[s["symbol"]] = s

    symbol_info = temp

    logger.info(
        "Eligible USDT Spot symbols loaded: %d",
        len(symbol_info)
    )


# ============================================================
# TOP 150 SYMBOLS BY VOLUME
# ============================================================

def load_top_symbols():

    global symbols

    logger.info("Loading top %d symbols by 24h quote volume...", TOP_SYMBOLS)

    tickers = client.get_ticker()

    volume_data = []

    for ticker in tickers:

        symbol = ticker.get("symbol")

        if symbol not in symbol_info:
            continue

        try:
            quote_volume = float(ticker.get("quoteVolume", 0))
        except Exception:
            quote_volume = 0.0

        volume_data.append(
            (symbol, quote_volume)
        )

    volume_data.sort(
        key=lambda x: x[1],
        reverse=True
    )

    selected = [
        item[0]
        for item in volume_data[:TOP_SYMBOLS]
    ]

    with symbols_lock:
        symbols = selected

    logger.info(
        "Selected %d symbols for trading.",
        len(symbols)
    )

    logger.info(
        "First symbols: %s",
        ", ".join(symbols[:20])
    )


# ============================================================
# DECIMAL HELPERS
# ============================================================

def get_step_size(symbol):

    info = symbol_info.get(symbol)

    if not info:
        return 0.000001

    for f in info["filters"]:

        if f["filterType"] == "LOT_SIZE":
            return float(f["stepSize"])

    return 0.000001


def get_min_qty(symbol):

    info = symbol_info.get(symbol)

    if not info:
        return 0.0

    for f in info["filters"]:

        if f["filterType"] == "LOT_SIZE":
            return float(f["minQty"])

    return 0.0


def get_min_notional(symbol):

    info = symbol_info.get(symbol)

    if not info:
        return 0.0

    for f in info["filters"]:

        if f["filterType"] in {
            "MIN_NOTIONAL",
            "NOTIONAL"
        }:

            value = f.get("minNotional")

            if value is not None:
                return float(value)

    return 0.0


def round_quantity(symbol, quantity):

    step_size = get_step_size(symbol)

    if step_size <= 0:
        return quantity

    step = Decimal(str(step_size))

    qty = Decimal(str(quantity))

    qty = (
        qty / step
    ).to_integral_value(
        rounding=ROUND_DOWN
    ) * step

    return float(qty)


# ============================================================
# GET CURRENT PRICE
# ============================================================

def get_current_price(symbol):

    try:

        ticker = client.get_symbol_ticker(
            symbol=symbol
        )

        return float(ticker["price"])

    except Exception as e:

        logger.error(
            "%s | Failed to get current price: %s",
            symbol,
            e
        )

        return None


# ============================================================
# GET CLOSED KLINES
# ============================================================

def get_closed_klines(symbol, limit=100):

    try:

        klines = client.get_klines(
            symbol=symbol,
            interval=TIMEFRAME,
            limit=limit
        )

        if not klines:
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
            klines,
            columns=columns
        )

        numeric_columns = [
            "open",
            "high",
            "low",
            "close",
            "volume"
        ]

        for col in numeric_columns:
            df[col] = pd.to_numeric(
                df[col],
                errors="coerce"
            )

        # Remove currently forming candle.
        current_time = int(time.time() * 1000)

        df = df[
            df["close_time"] <= current_time
        ].copy()

        return df.reset_index(drop=True)

    except Exception as e:

        logger.error(
            "%s | Kline error: %s",
            symbol,
            e
        )

        return None


# ============================================================
# CALCULATE INDICATORS
# ============================================================

def calculate_indicators(df):

    df = df.copy()

    # --------------------------------------------------------
    # EMA 5
    # --------------------------------------------------------

    df["ema5"] = (
        df["close"]
        .ewm(
            span=EMA_PERIOD,
            adjust=False
        )
        .mean()
    )

    # --------------------------------------------------------
    # BB20
    # --------------------------------------------------------

    df["bb_middle"] = (
        df["close"]
        .rolling(
            BB_PERIOD
        )
        .mean()
    )

    df["bb_std"] = (
        df["close"]
        .rolling(
            BB_PERIOD
        )
        .std(
            ddof=0
        )
    )

    df["bb_upper"] = (
        df["bb_middle"]
        + (
            BB_STD
            * df["bb_std"]
        )
    )

    df["bb_lower"] = (
        df["bb_middle"]
        - (
            BB_STD
            * df["bb_std"]
        )
    )

    # ========================================================
    # ADX 14
    # ========================================================

    high = df["high"]
    low = df["low"]
    close = df["close"]

    previous_close = close.shift(1)

    # --------------------------------------------------------
    # True Range
    # --------------------------------------------------------

    tr1 = high - low

    tr2 = (
        high - previous_close
    ).abs()

    tr3 = (
        low - previous_close
    ).abs()

    df["tr"] = pd.concat(
        [
            tr1,
            tr2,
            tr3
        ],
        axis=1
    ).max(axis=1)

    # --------------------------------------------------------
    # Directional Movement
    # --------------------------------------------------------

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

    # --------------------------------------------------------
    # Wilder-style smoothing
    # --------------------------------------------------------

    alpha = 1.0 / ADX_PERIOD

    atr = (
        df["tr"]
        .ewm(
            alpha=alpha,
            adjust=False
        )
        .mean()
    )

    smooth_plus_dm = (
        plus_dm
        .ewm(
            alpha=alpha,
            adjust=False
        )
        .mean()
    )

    smooth_minus_dm = (
        minus_dm
        .ewm(
            alpha=alpha,
            adjust=False
        )
        .mean()
    )

    # --------------------------------------------------------
    # +DI / -DI
    # --------------------------------------------------------

    df["plus_di"] = (
        100.0
        * smooth_plus_dm
        / atr.replace(0, pd.NA)
    )

    df["minus_di"] = (
        100.0
        * smooth_minus_dm
        / atr.replace(0, pd.NA)
    )

    # --------------------------------------------------------
    # DX
    # --------------------------------------------------------

    di_sum = (
        df["plus_di"]
        + df["minus_di"]
    )

    di_difference = (
        df["plus_di"]
        - df["minus_di"]
    ).abs()

    df["dx"] = (
        100.0
        * di_difference
        / di_sum.replace(0, pd.NA)
    )

    # --------------------------------------------------------
    # ADX
    # --------------------------------------------------------

    df["adx14"] = (
        df["dx"]
        .ewm(
            alpha=alpha,
            adjust=False
        )
        .mean()
    )

    return df


# ============================================================
# BUY SIGNAL
# ============================================================

def entry_signal(df):

    if df is None:
        return False

    if len(df) < 50:
        return False

    df = calculate_indicators(df)

    candle = df.iloc[-1]

    required_values = [
        candle["open"],
        candle["high"],
        candle["close"],
        candle["ema5"],
        candle["bb_lower"],
        candle["adx14"]
    ]

    if any(
        pd.isna(value)
        for value in required_values
    ):
        return False

    candle_open = float(
        candle["open"]
    )

    candle_high = float(
        candle["high"]
    )

    candle_close = float(
        candle["close"]
    )

    ema5 = float(
        candle["ema5"]
    )

    lower_bb = float(
        candle["bb_lower"]
    )

    adx14 = float(
        candle["adx14"]
    )

    # ========================================================
    # BUY CONDITIONS
    # ========================================================

    condition_1 = (
        candle_open < lower_bb
    )

    condition_2 = (
        candle_close > lower_bb
    )

    condition_3 = (
        candle_close < ema5
    )

    condition_4 = (
        candle_high < ema5
    )

    condition_5 = (
        adx14 > ADX_MIN
    )

    return (
        condition_1
        and condition_2
        and condition_3
        and condition_4
        and condition_5
    )


# ============================================================
# GET SIGNAL DATA
# ============================================================

def get_signal_data(df):

    df = calculate_indicators(df)

    candle = df.iloc[-1]

    return {
        "open": float(candle["open"]),
        "high": float(candle["high"]),
        "low": float(candle["low"]),
        "close": float(candle["close"]),
        "ema5": float(candle["ema5"]),
        "bb_lower": float(candle["bb_lower"]),
        "bb_middle": float(candle["bb_middle"]),
        "bb_upper": float(candle["bb_upper"]),
        "adx14": float(candle["adx14"])
    }


# ============================================================
# BUY SYMBOL
# ============================================================

def buy_symbol(symbol):

    try:

        # ----------------------------------------------------
        # Duplicate position check
        # ----------------------------------------------------

        with positions_lock:

            if symbol in positions:
                return False

            if symbol in selling_symbols:
                return False

        # ----------------------------------------------------
        # Get balance
        # ----------------------------------------------------

        balance_data = client.get_asset_balance(
            asset="USDT"
        )

        if not balance_data:
            return False

        available_usdt = float(
            balance_data["free"]
        )

        if available_usdt < TRADE_AMOUNT_USDT:
            logger.warning(
                "%s | Not enough USDT. Available: %.4f",
                symbol,
                available_usdt
            )

            return False

        # ----------------------------------------------------
        # Get market price
        # ----------------------------------------------------

        current_price = get_current_price(
            symbol
        )

        if not current_price:
            return False

        # ----------------------------------------------------
        # Get quantity
        # ----------------------------------------------------

        quantity = (
            TRADE_AMOUNT_USDT
            / current_price
        )

        quantity = round_quantity(
            symbol,
            quantity
        )

        min_qty = get_min_qty(
            symbol
        )

        min_notional = get_min_notional(
            symbol
        )

        if quantity < min_qty:
            logger.warning(
                "%s | Quantity below minimum. Qty=%.12f MinQty=%.12f",
                symbol,
                quantity,
                min_qty
            )

            return False

        if (
            quantity * current_price
            < min_notional
        ):
            logger.warning(
                "%s | Notional below minimum.",
                symbol
            )

            return False

        if quantity <= 0:
            return False

        # ----------------------------------------------------
        # BUY MARKET ORDER
        # ----------------------------------------------------

        order = client.order_market_buy(
            symbol=symbol,
            quantity=quantity
        )

        # ----------------------------------------------------
        # Calculate actual executed quantity
        # ----------------------------------------------------

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

        if fills:

            total_qty = 0.0
            total_cost = 0.0

            for fill in fills:

                fill_qty = float(
                    fill["qty"]
                )

                fill_price = float(
                    fill["price"]
                )

                total_qty += fill_qty

                total_cost += (
                    fill_qty
                    * fill_price
                )

            if total_qty > 0:

                executed_qty = total_qty

                actual_entry_price = (
                    total_cost
                    / total_qty
                )

            else:

                actual_entry_price = current_price

        else:

            actual_entry_price = current_price

        # ----------------------------------------------------
        # SAVE POSITION
        # ----------------------------------------------------

        with positions_lock:

            positions[symbol] = {
                "entry_price": actual_entry_price,
                "quantity": executed_qty,
                "highest_price": actual_entry_price,
                "trailing_active": False,
                "buy_time": time.time()
            }

        logger.info(
            "============================================================"
        )

        logger.info(
            "BUY EXECUTED | %s",
            symbol
        )

        logger.info(
            "BUY PRICE: %.10f",
            actual_entry_price
        )

        logger.info(
            "QUANTITY: %.10f",
            executed_qty
        )

        logger.info(
            "TRADE VALUE: %.4f USDT",
            actual_entry_price * executed_qty
        )

        logger.info(
            "STOP LOSS: %.2f%%",
            STOP_LOSS_PCT * 100
        )

        logger.info(
            "TRAILING ACTIVATION: +%.2f%%",
            TRAILING_ACTIVATION_PCT * 100
        )

        logger.info(
            "TRAILING STOP: %.2f%%",
            TRAILING_STOP_PCT * 100
        )

        logger.info(
            "============================================================"
        )

        return True

    except BinanceAPIException as e:

        logger.error(
            "%s | Binance BUY error: %s",
            symbol,
            e
        )

        return False

    except Exception as e:

        logger.exception(
            "%s | BUY error: %s",
            symbol,
            e
        )

        return False


# ============================================================
# SELL SYMBOL
# ============================================================

def sell_symbol(symbol, reason):

    with positions_lock:

        if symbol in selling_symbols:
            return False

        position = positions.get(
            symbol
        )

        if not position:
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
            return False

        # ----------------------------------------------------
        # Get actual asset balance
        # ----------------------------------------------------

        base_asset = symbol_info[
            symbol
        ]["baseAsset"]

        balance_data = client.get_asset_balance(
            asset=base_asset
        )

        if balance_data:

            available_qty = float(
                balance_data["free"]
            )

            quantity = min(
                quantity,
                available_qty
                * SELL_BALANCE_BUFFER
            )

            quantity = round_quantity(
                symbol,
                quantity
            )

        if quantity <= 0:

            logger.warning(
                "%s | No quantity available for SELL.",
                symbol
            )

            return False

        # ----------------------------------------------------
        # MARKET SELL
        # ----------------------------------------------------

        order = client.order_market_sell(
            symbol=symbol,
            quantity=quantity
        )

        current_price = get_current_price(
            symbol
        )

        entry_price = float(
            position["entry_price"]
        )

        if current_price:

            pnl_pct = (
                (
                    current_price
                    - entry_price
                )
                / entry_price
            ) * 100

        else:

            pnl_pct = 0.0

        logger.info(
            "============================================================"
        )

        logger.info(
            "SELL EXECUTED | %s",
            symbol
        )

        logger.info(
            "REASON: %s",
            reason
        )

        logger.info(
            "ENTRY: %.10f",
            entry_price
        )

        if current_price:

            logger.info(
                "CURRENT: %.10f",
                current_price
            )

            logger.info(
                "PRICE P/L: %.3f%%",
                pnl_pct
            )

        logger.info(
            "SELL QTY: %.10f",
            quantity
        )

        logger.info(
            "============================================================"
        )

        with positions_lock:

            positions.pop(
                symbol,
                None
            )

        return True

    except BinanceAPIException as e:

        logger.error(
            "%s | Binance SELL error: %s",
            symbol,
            e
        )

        return False

    except Exception as e:

        logger.exception(
            "%s | SELL error: %s",
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

        entry_price = float(
            position["entry_price"]
        )

        highest_price = float(
            position["highest_price"]
        )

        trailing_active = bool(
            position["trailing_active"]
        )

    # ========================================================
    # 1. STOP LOSS
    # ========================================================

    stop_price = (
        entry_price
        * (
            1.0
            - STOP_LOSS_PCT
        )
    )

    if current_price <= stop_price:

        logger.info(
            "%s | STOP LOSS triggered | Entry=%.10f Current=%.10f Stop=%.10f",
            symbol,
            entry_price,
            current_price,
            stop_price
        )

        sell_symbol(
            symbol,
            "STOP LOSS -1%"
        )

        return

    # ========================================================
    # 2. UPDATE HIGHEST PRICE
    # ========================================================

    if current_price > highest_price:

        highest_price = current_price

        with positions_lock:

            if symbol in positions:

                positions[
                    symbol
                ][
                    "highest_price"
                ] = highest_price

    # ========================================================
    # 3. TRAILING ACTIVATION
    # ========================================================

    activation_price = (
        entry_price
        * (
            1.0
            + TRAILING_ACTIVATION_PCT
        )
    )

    if (
        not trailing_active
        and current_price >= activation_price
    ):

        trailing_active = True

        with positions_lock:

            if symbol in positions:

                positions[
                    symbol
                ][
                    "trailing_active"
                ] = True

        logger.info(
            "%s | TRAILING ACTIVATED | Entry=%.10f Current=%.10f Highest=%.10f",
            symbol,
            entry_price,
            current_price,
            highest_price
        )

    # ========================================================
    # 4. TRAILING STOP
    # ========================================================

    if trailing_active:

        trailing_price = (
            highest_price
            * (
                1.0
                - TRAILING_STOP_PCT
            )
        )

        if current_price <= trailing_price:

            logger.info(
                "%s | TRAILING STOP triggered | Highest=%.10f Current=%.10f Trail=%.10f",
                symbol,
                highest_price,
                current_price,
                trailing_price
            )

            sell_symbol(
                symbol,
                "TRAILING STOP -0.50% FROM HIGH"
            )


# ============================================================
# PROCESS CLOSED CANDLE
# ============================================================

def process_closed_candle(
    symbol
):

    try:

        with positions_lock:

            if symbol in positions:
                return

        df = get_closed_klines(
            symbol,
            limit=100
        )

        if df is None:
            return

        if len(df) < 50:
            return

        df = calculate_indicators(
            df
        )

        candle = df.iloc[-1]

        required_values = [
            candle["open"],
            candle["high"],
            candle["close"],
            candle["ema5"],
            candle["bb_lower"],
            candle["adx14"]
        ]

        if any(
            pd.isna(value)
            for value in required_values
        ):
            return

        candle_open = float(
            candle["open"]
        )

        candle_high = float(
            candle["high"]
        )

        candle_close = float(
            candle["close"]
        )

        ema5 = float(
            candle["ema5"]
        )

        lower_bb = float(
            candle["bb_lower"]
        )

        adx14 = float(
            candle["adx14"]
        )

        # ====================================================
        # BUY CONDITIONS
        # ====================================================

        condition_1 = (
            candle_open < lower_bb
        )

        condition_2 = (
            candle_close > lower_bb
        )

        condition_3 = (
            candle_close < ema5
        )

        condition_4 = (
            candle_high < ema5
        )

        condition_5 = (
            adx14 > ADX_MIN
        )

        buy_signal = (
            condition_1
            and condition_2
            and condition_3
            and condition_4
            and condition_5
        )

        # ----------------------------------------------------
        # BUY
        # ----------------------------------------------------

        if buy_signal:

            logger.info(
                "BUY SIGNAL | %s | "
                "Open=%.10f | Close=%.10f | "
                "BB_Lower=%.10f | EMA5=%.10f | "
                "ADX14=%.2f",
                symbol,
                candle_open,
                candle_close,
                lower_bb,
                ema5,
                adx14
            )

            buy_symbol(
                symbol
            )

    except Exception as e:

        logger.exception(
            "%s | Candle processing error: %s",
            symbol,
            e
        )


# ============================================================
# WEBSOCKET MESSAGE PROCESSOR
# ============================================================

def process_ws_message(message):

    try:

        data = json.loads(
            message
        )

        # ----------------------------------------------------
        # Combined stream format
        # ----------------------------------------------------

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
                "k"
            )

            if not kline:
                return

            symbol = kline.get(
                "s"
            )

            is_closed = kline.get(
                "x"
            )

            if not symbol:
                return

            # Only CLOSED candle
            if is_closed:

                process_closed_candle(
                    symbol
                )

        # ====================================================
        # MINI TICKER
        # ====================================================

        elif event_type == "24hrMiniTicker":

            symbol = payload.get(
                "s"
            )

            close_price = payload.get(
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
# WEBSOCKET CALLBACKS
# ============================================================

def websocket_on_message(
    ws,
    message
):

    process_ws_message(
        message
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


def websocket_on_open(
    ws
):

    logger.info(
        "WebSocket connection established."
    )


# ============================================================
# MAKE STREAM URL
# ============================================================

def make_stream_url():

    streams = []

    with symbols_lock:

        current_symbols = list(
            symbols
        )

    for symbol in current_symbols:

        lower_symbol = symbol.lower()

        streams.append(
            f"{lower_symbol}@kline_5m"
        )

        streams.append(
            f"{lower_symbol}@miniTicker"
        )

    stream_path = "/".join(
        streams
    )

    return (
        "wss://stream.binance.com:9443/stream?streams="
        + stream_path
    )


# ============================================================
# WEBSOCKET LOOP
# ============================================================

def websocket_loop():

    while True:

        try:

            if not symbols:

                logger.warning(
                    "No symbols available for WebSocket."
                )

                time.sleep(10)

                continue

            url = make_stream_url()

            logger.info(
                "Connecting WebSocket for %d symbols...",
                len(symbols)
            )

            ws = websocket.WebSocketApp(
                url,
                on_open=websocket_on_open,
                on_message=websocket_on_message,
                on_error=websocket_on_error,
                on_close=websocket_on_close
            )

            ws.run_forever(
                ping_interval=20,
                ping_timeout=10
            )

        except Exception as e:

            logger.exception(
                "WebSocket loop error: %s",
                e
            )

        logger.warning(
            "WebSocket reconnecting in 5 seconds..."
        )

        time.sleep(5)


# ============================================================
# POSITION SAFETY MONITOR
# ============================================================

def position_monitor():

    while True:

        try:

            with positions_lock:

                current_positions = list(
                    positions.keys()
                )

            for symbol in current_positions:

                price = get_current_price(
                    symbol
                )

                if price is not None:

                    check_position(
                        symbol,
                        price
                    )

                time.sleep(
                    0.15
                )

        except Exception as e:

            logger.error(
                "Position monitor error: %s",
                e
            )

        time.sleep(2)


# ============================================================
# RECOVER EXISTING POSITIONS
# ============================================================

def recover_positions():

    logger.info(
        "Checking for existing Binance Spot positions..."
    )

    try:

        account = client.get_account()

        balances = account.get(
            "balances",
            []
        )

        recovered = 0

        for balance in balances:

            asset = balance["asset"]

            free_qty = float(
                balance["free"]
            )

            locked_qty = float(
                balance["locked"]
            )

            total_qty = (
                free_qty
                + locked_qty
            )

            if total_qty <= 0:
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

            current_price = get_current_price(
                symbol
            )

            if not current_price:
                continue

            # ------------------------------------------------
            # IMPORTANT:
            # Existing position's actual historical entry
            # price is not available here.
            #
            # We initialize entry at current price so the bot
            # can safely monitor it after restart.
            # ------------------------------------------------

            with positions_lock:

                positions[symbol] = {
                    "entry_price": current_price,
                    "quantity": total_qty,
                    "highest_price": current_price,
                    "trailing_active": False,
                    "buy_time": time.time()
                }

            recovered += 1

            logger.warning(
                "RECOVERED POSITION | %s | Qty=%.10f | "
                "Entry initialized at current price %.10f",
                symbol,
                total_qty,
                current_price
            )

        logger.info(
            "Recovered positions: %d",
            recovered
        )

    except Exception as e:

        logger.exception(
            "Position recovery error: %s",
            e
        )


# ============================================================
# INITIALIZE
# ============================================================

def initialize():

    logger.info(
        "======================================================================"
    )

    logger.info(
        "BB20 + EMA5 + ADX14 BINANCE SPOT BOT STARTING"
    )

    logger.info(
        "======================================================================"
    )

    logger.info(
        "TIMEFRAME: %s",
        TIMEFRAME
    )

    logger.info(
        "TRADE AMOUNT: %.2f USDT",
        TRADE_AMOUNT_USDT
    )

    logger.info(
        "TOP SYMBOLS: %d",
        TOP_SYMBOLS
    )

    logger.info(
        "BUY RULE → "
        "OPEN < BB20 LOWER + "
        "CLOSE > BB20 LOWER + "
        "CLOSE < EMA5 + "
        "HIGH < EMA5 + "
        "ADX14 > %.2f",
        ADX_MIN
    )

    logger.info(
        "ADX PERIOD: %d",
        ADX_PERIOD
    )

    logger.info(
        "STOP LOSS: %.2f%%",
        STOP_LOSS_PCT * 100
    )

    logger.info(
        "TRAILING ACTIVATION: +%.2f%%",
        TRAILING_ACTIVATION_PCT * 100
    )

    logger.info(
        "TRAILING STOP: %.2f%% FROM HIGHEST PRICE",
        TRAILING_STOP_PCT * 100
    )

    logger.info(
        "UPPER BB SELL: DISABLED"
    )

    logger.info(
        "======================================================================"
    )

    load_exchange_info()

    load_top_symbols()

    recover_positions()


# ============================================================
# MAIN
# ============================================================

if __name__ == "__main__":

    initialize()

    # --------------------------------------------------------
    # Position safety monitor
    # --------------------------------------------------------

    monitor_thread = threading.Thread(
        target=position_monitor,
        daemon=True
    )

    monitor_thread.start()

    # --------------------------------------------------------
    # WebSocket
    # --------------------------------------------------------

    websocket_thread = threading.Thread(
        target=websocket_loop,
        daemon=True
    )

    websocket_thread.start()

    # --------------------------------------------------------
    # Flask server
    # --------------------------------------------------------

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
