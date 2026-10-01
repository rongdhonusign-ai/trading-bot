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
        "BINANCE_API_KEY and BINANCE_API_SECRET are required."
    )


# ============================================================
# TRADING SETTINGS
# ============================================================

TRADE_AMOUNT_USDT = 35.0

TIMEFRAME = Client.KLINE_INTERVAL_5MINUTE

TOP_SYMBOLS = 150


# ============================================================
# BUY FILTER
# ============================================================

ADX_MIN = 20.0


# ============================================================
# STOP LOSS
# ============================================================

STOP_LOSS_PCT = 0.0100
# 1.00% below entry


# ============================================================
# TRAILING STOP
# ============================================================

TRAILING_ACTIVATION_PCT = 0.0100
# +1.00% from entry

TRAILING_STOP_PCT = 0.0050
# 0.50% below highest price


# ============================================================
# API / REQUEST SAFETY
# ============================================================

# Minimum time between REST kline requests for the same symbol
KLINE_REQUEST_COOLDOWN = 2.0

# Prevent duplicate BUY attempts on same symbol
BUY_COOLDOWN_SECONDS = 60

# Small delay between REST kline requests
KLINE_REQUEST_DELAY = 0.05


# ============================================================
# BINANCE CLIENT
# ============================================================

client = Client(
    API_KEY,
    API_SECRET
)


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
    return "BB20 + ADX14 + DI BINANCE SPOT BOT RUNNING"


@app.route("/health")
def health():

    with positions_lock:
        position_count = len(positions)

    return jsonify({
        "status": "running",
        "symbols": len(symbols),
        "positions": position_count
    })


# ============================================================
# GLOBAL VARIABLES
# ============================================================

symbols = []

symbol_info = {}

positions = {}

selling_symbols = set()

positions_lock = threading.Lock()

selling_lock = threading.Lock()

buy_lock = threading.Lock()

last_kline_request = {}

last_buy_attempt = {}


# ============================================================
# STABLECOINS
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
# LOAD BINANCE EXCHANGE INFO
# ============================================================

def load_exchange_info():

    global symbol_info

    logger.info(
        "Loading Binance exchange information..."
    )

    info = client.get_exchange_info()

    temp = {}

    for item in info.get("symbols", []):

        try:

            symbol = item["symbol"]

            if item["status"] != "TRADING":
                continue

            if item["quoteAsset"] != "USDT":
                continue

            filters = {}

            for f in item.get("filters", []):

                filter_type = f.get("filterType")

                if filter_type == "LOT_SIZE":

                    filters["stepSize"] = f.get(
                        "stepSize"
                    )

                    filters["minQty"] = f.get(
                        "minQty"
                    )

                elif filter_type == "MIN_NOTIONAL":

                    filters["minNotional"] = f.get(
                        "minNotional"
                    )

                elif filter_type == "NOTIONAL":

                    filters["minNotional"] = f.get(
                        "minNotional"
                    )

            temp[symbol] = {
                "baseAsset": item["baseAsset"],
                "quoteAsset": item["quoteAsset"],
                "filters": filters
            }

        except Exception:

            continue

    symbol_info = temp

    logger.info(
        f"Loaded {len(symbol_info)} USDT trading symbols."
    )


# ============================================================
# LOAD TOP SYMBOLS
# ============================================================

def load_top_symbols():

    global symbols

    logger.info(
        "Loading top symbols..."
    )

    try:

        tickers = client.get_ticker()

        candidates = []

        for ticker in tickers:

            symbol = ticker.get(
                "symbol",
                ""
            )

            if not symbol.endswith("USDT"):
                continue

            if symbol not in symbol_info:
                continue

            base = symbol_info[symbol]["baseAsset"]

            if base in STABLECOINS:
                continue

            if base in {
                "BTC",
                "ETH"
            }:
                continue

            try:

                quote_volume = float(
                    ticker.get(
                        "quoteVolume",
                        0
                    )
                )

                price_change = abs(
                    float(
                        ticker.get(
                            "priceChangePercent",
                            0
                        )
                    )
                )

                candidates.append(
                    (
                        symbol,
                        quote_volume,
                        price_change
                    )
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
            f"Selected {len(symbols)} symbols for trading."
        )

        if symbols:

            logger.info(
                "First symbols: "
                + ", ".join(symbols[:20])
            )

    except Exception as e:

        logger.exception(
            f"Failed to load top symbols: {e}"
        )


# ============================================================
# DECIMAL HELPERS
# ============================================================

def get_step_size(symbol):

    try:

        return Decimal(
            str(
                symbol_info[symbol]["filters"].get(
                    "stepSize",
                    "0.000001"
                )
            )
        )

    except Exception:

        return Decimal("0.000001")


def get_min_qty(symbol):

    try:

        return Decimal(
            str(
                symbol_info[symbol]["filters"].get(
                    "minQty",
                    "0"
                )
            )
        )

    except Exception:

        return Decimal("0")


def get_min_notional(symbol):

    try:

        return Decimal(
            str(
                symbol_info[symbol]["filters"].get(
                    "minNotional",
                    "0"
                )
            )
        )

    except Exception:

        return Decimal("0")


def round_quantity(symbol, quantity):

    try:

        qty = Decimal(str(quantity))

        step = get_step_size(symbol)

        if step <= 0:

            return float(qty)

        rounded = (
            qty / step
        ).to_integral_value(
            rounding=ROUND_DOWN
        ) * step

        return float(rounded)

    except Exception as e:

        logger.error(
            f"{symbol} | Quantity rounding error: {e}"
        )

        return 0.0


# ============================================================
# REST PRICE
# ONLY USED FOR BUY ORDER / RECOVERY
# ============================================================

def get_current_price(symbol):

    try:

        ticker = client.get_symbol_ticker(
            symbol=symbol
        )

        return float(
            ticker["price"]
        )

    except Exception as e:

        logger.error(
            f"{symbol} | Price error: {e}"
        )

        return None


# ============================================================
# GET CLOSED KLINES
# ============================================================

def get_closed_klines(symbol, limit=100):

    # --------------------------------------------------------
    # Request cooldown protection
    # --------------------------------------------------------

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

        klines = client.get_klines(
            symbol=symbol,
            interval=TIMEFRAME,
            limit=limit
        )

        if not klines:

            return None

        rows = []

        for k in klines:

            try:

                rows.append({
                    "open_time": int(k[0]),
                    "open": float(k[1]),
                    "high": float(k[2]),
                    "low": float(k[3]),
                    "close": float(k[4]),
                    "volume": float(k[5]),
                    "close_time": int(k[6])
                })

            except Exception:

                continue

        if len(rows) < 50:

            return None

        df = pd.DataFrame(rows)

        numeric_cols = [
            "open",
            "high",
            "low",
            "close",
            "volume"
        ]

        for col in numeric_cols:

            df[col] = pd.to_numeric(
                df[col],
                errors="coerce"
            ).astype("float64")

        df = df.dropna(
            subset=numeric_cols
        ).reset_index(
            drop=True
        )

        if len(df) < 50:

            return None

        return df

    except BinanceAPIException as e:

        logger.error(
            f"{symbol} | Binance Kline API error: {e}"
        )

        return None

    except Exception as e:

        logger.exception(
            f"{symbol} | Kline error: {e}"
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

        numeric_cols = [
            "open",
            "high",
            "low",
            "close",
            "volume"
        ]

        for col in numeric_cols:

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

        if len(df) < 50:
            return None

        # ====================================================
        # FLOAT64 SERIES
        # ====================================================

        close = pd.Series(
            df["close"].to_numpy(
                dtype="float64"
            ),
            dtype="float64"
        )

        high = pd.Series(
            df["high"].to_numpy(
                dtype="float64"
            ),
            dtype="float64"
        )

        low = pd.Series(
            df["low"].to_numpy(
                dtype="float64"
            ),
            dtype="float64"
        )

        # ====================================================
        # EMA5
        # ====================================================

        ema5 = close.ewm(
            span=5,
            adjust=False,
            min_periods=5
        ).mean()

        # ====================================================
        # BB20
        # ====================================================

        bb_middle = close.rolling(
            window=20,
            min_periods=20
        ).mean()

        bb_std = close.rolling(
            window=20,
            min_periods=20
        ).std(
            ddof=0
        )

        bb_upper = (
            bb_middle +
            (2.0 * bb_std)
        )

        bb_lower = (
            bb_middle -
            (2.0 * bb_std)
        )

        # ====================================================
        # ADX14
        # ====================================================

        prev_close = close.shift(1)

        prev_high = high.shift(1)

        prev_low = low.shift(1)

        # ----------------------------------------------------
        # TRUE RANGE
        # ----------------------------------------------------

        tr1 = high - low

        tr2 = (
            high -
            prev_close
        ).abs()

        tr3 = (
            low -
            prev_close
        ).abs()

        tr = tr1.copy()

        mask2 = tr2 > tr

        tr.loc[
            mask2
        ] = tr2.loc[
            mask2
        ]

        mask3 = tr3 > tr

        tr.loc[
            mask3
        ] = tr3.loc[
            mask3
        ]

        # ----------------------------------------------------
        # DIRECTIONAL MOVEMENT
        # ----------------------------------------------------

        up_move = (
            high -
            prev_high
        )

        down_move = (
            prev_low -
            low
        )

        plus_dm = pd.Series(
            0.0,
            index=close.index,
            dtype="float64"
        )

        minus_dm = pd.Series(
            0.0,
            index=close.index,
            dtype="float64"
        )

        plus_mask = (
            (up_move > down_move) &
            (up_move > 0)
        )

        minus_mask = (
            (down_move > up_move) &
            (down_move > 0)
        )

        plus_dm.loc[
            plus_mask
        ] = up_move.loc[
            plus_mask
        ]

        minus_dm.loc[
            minus_mask
        ] = down_move.loc[
            minus_mask
        ]

        # ----------------------------------------------------
        # WILDER SMOOTHING
        # ----------------------------------------------------

        atr = tr.ewm(
            alpha=(1.0 / 14.0),
            adjust=False,
            min_periods=14
        ).mean()

        plus_dm_smooth = plus_dm.ewm(
            alpha=(1.0 / 14.0),
            adjust=False,
            min_periods=14
        ).mean()

        minus_dm_smooth = minus_dm.ewm(
            alpha=(1.0 / 14.0),
            adjust=False,
            min_periods=14
        ).mean()

        atr_safe = atr.copy()

        atr_safe.loc[
            atr_safe == 0
        ] = float("nan")

        # ----------------------------------------------------
        # +DI / -DI
        # ----------------------------------------------------

        plus_di = (
            100.0 *
            plus_dm_smooth /
            atr_safe
        )

        minus_di = (
            100.0 *
            minus_dm_smooth /
            atr_safe
        )

        di_sum = (
            plus_di +
            minus_di
        )

        di_sum.loc[
            di_sum == 0
        ] = float("nan")

        # ----------------------------------------------------
        # DX
        # ----------------------------------------------------

        dx = (
            100.0 *
            (plus_di - minus_di).abs() /
            di_sum
        )

        # ----------------------------------------------------
        # ADX
        # ----------------------------------------------------

        adx14 = dx.ewm(
            alpha=(1.0 / 14.0),
            adjust=False,
            min_periods=14
        ).mean()

        # ====================================================
        # ADD INDICATORS
        # ====================================================

        df["EMA5"] = pd.Series(
            ema5.to_numpy(
                dtype="float64"
            ),
            index=df.index,
            dtype="float64"
        )

        df["BB_MIDDLE"] = pd.Series(
            bb_middle.to_numpy(
                dtype="float64"
            ),
            index=df.index,
            dtype="float64"
        )

        df["BB_UPPER"] = pd.Series(
            bb_upper.to_numpy(
                dtype="float64"
            ),
            index=df.index,
            dtype="float64"
        )

        df["BB_LOWER"] = pd.Series(
            bb_lower.to_numpy(
                dtype="float64"
            ),
            index=df.index,
            dtype="float64"
        )

        df["PLUS_DI"] = pd.Series(
            plus_di.to_numpy(
                dtype="float64"
            ),
            index=df.index,
            dtype="float64"
        )

        df["MINUS_DI"] = pd.Series(
            minus_di.to_numpy(
                dtype="float64"
            ),
            index=df.index,
            dtype="float64"
        )

        df["ADX14"] = pd.Series(
            adx14.to_numpy(
                dtype="float64"
            ),
            index=df.index,
            dtype="float64"
        )

        # ====================================================
        # CLEANUP
        # ====================================================

        for col in [
            "EMA5",
            "BB_MIDDLE",
            "BB_UPPER",
            "BB_LOWER",
            "PLUS_DI",
            "MINUS_DI",
            "ADX14"
        ]:

            df[col] = pd.to_numeric(
                df[col],
                errors="coerce"
            ).astype(
                "float64"
            )

        return df

    except Exception as e:

        logger.exception(
            f"Indicator calculation error: {e}"
        )

        return None


# ============================================================
# BUY SIGNAL
# ============================================================

def entry_signal(candle):

    try:

        open_price = float(
            candle["open"]
        )

        close_price = float(
            candle["close"]
        )

        bb_lower = float(
            candle["BB_LOWER"]
        )

        adx14 = float(
            candle["ADX14"]
        )

        plus_di = float(
            candle["PLUS_DI"]
        )

        minus_di = float(
            candle["MINUS_DI"]
        )

        if any([
            pd.isna(open_price),
            pd.isna(close_price),
            pd.isna(bb_lower),
            pd.isna(adx14),
            pd.isna(plus_di),
            pd.isna(minus_di)
        ]):

            return False

        # ====================================================
        # BUY CONDITIONS
        # ====================================================

        # 1. Candle opened below BB20 Lower
        condition_1 = (
            open_price < bb_lower
        )

        # 2. Candle closed back above BB20 Lower
        condition_2 = (
            close_price > bb_lower
        )

        # 3. ADX must show sufficient trend strength
        condition_3 = (
            adx14 > ADX_MIN
        )

        # 4. Bullish directional filter
        condition_4 = (
            plus_di > minus_di
        )

        return (
            condition_1 and
            condition_2 and
            condition_3 and
            condition_4
        )

    except Exception as e:

        logger.error(
            f"Entry signal error: {e}"
        )

        return False


# ============================================================
# BUY ORDER
# ============================================================

def buy_symbol(symbol):

    # --------------------------------------------------------
    # Duplicate BUY protection
    # --------------------------------------------------------

    with buy_lock:

        now = time.time()

        last_attempt = last_buy_attempt.get(
            symbol,
            0
        )

        if (
            now - last_attempt
            < BUY_COOLDOWN_SECONDS
        ):

            return False

        last_buy_attempt[symbol] = now

    try:

        with positions_lock:

            if symbol in positions:

                return False

        price = get_current_price(
            symbol
        )

        if (
            price is None or
            price <= 0
        ):

            return False

        quantity = (
            TRADE_AMOUNT_USDT /
            price
        )

        quantity = round_quantity(
            symbol,
            quantity
        )

        if quantity <= 0:

            logger.warning(
                f"{symbol} | Quantity too small."
            )

            return False

        min_qty = float(
            get_min_qty(symbol)
        )

        if quantity < min_qty:

            logger.warning(
                f"{symbol} | Quantity below minQty."
            )

            return False

        min_notional = float(
            get_min_notional(symbol)
        )

        if (
            quantity * price
            < min_notional
        ):

            logger.warning(
                f"{symbol} | Notional below minimum."
            )

            return False

        logger.info(
            f"{symbol} | "
            f"BUY MARKET | "
            f"Qty={quantity}"
        )

        order = client.order_market_buy(
            symbol=symbol,
            quantity=quantity
        )

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
            total_value = 0.0

            for fill in fills:

                fill_qty = float(
                    fill["qty"]
                )

                fill_price = float(
                    fill["price"]
                )

                total_qty += fill_qty

                total_value += (
                    fill_qty *
                    fill_price
                )

            if total_qty > 0:

                entry_price = (
                    total_value /
                    total_qty
                )

            else:

                entry_price = price

        else:

            entry_price = price

        with positions_lock:

            positions[symbol] = {

                "entry_price":
                    float(entry_price),

                "quantity":
                    float(executed_qty),

                "highest_price":
                    float(entry_price),

                "trailing_active":
                    False,

                "buy_time":
                    time.time()
            }

        logger.info(
            f"{symbol} | "
            f"BUY SUCCESS | "
            f"Entry={entry_price:.10f} | "
            f"Qty={executed_qty}"
        )

        return True

    except BinanceAPIException as e:

        logger.error(
            f"{symbol} | Binance BUY error: {e}"
        )

        return False

    except Exception as e:

        logger.exception(
            f"{symbol} | BUY error: {e}"
        )

        return False


# ============================================================
# SELL ORDER
# ============================================================

def sell_symbol(
    symbol,
    reason="SELL"
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

        quantity = float(
            position["quantity"]
        )

        quantity = round_quantity(
            symbol,
            quantity
        )

        if quantity <= 0:

            return False

        logger.info(
            f"{symbol} | "
            f"SELL MARKET | "
            f"Reason={reason} | "
            f"Qty={quantity}"
        )

        order = client.order_market_sell(
            symbol=symbol,
            quantity=quantity
        )

        with positions_lock:

            positions.pop(
                symbol,
                None
            )

        logger.info(
            f"{symbol} | "
            f"SELL SUCCESS | "
            f"Reason={reason}"
        )

        return True

    except BinanceAPIException as e:

        logger.error(
            f"{symbol} | Binance SELL error: {e}"
        )

        return False

    except Exception as e:

        logger.exception(
            f"{symbol} | SELL error: {e}"
        )

        return False

    finally:

        with selling_lock:

            selling_symbols.discard(
                symbol
            )


# ============================================================
# POSITION CHECK
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

            entry_price = float(
                position["entry_price"]
            )

            highest_price = float(
                position["highest_price"]
            )

            trailing_active = bool(
                position["trailing_active"]
            )

        if entry_price <= 0:
            return

        # ----------------------------------------------------
        # UPDATE HIGHEST PRICE
        # ----------------------------------------------------

        if current_price > highest_price:

            highest_price = current_price

            with positions_lock:

                if symbol in positions:

                    positions[symbol][
                        "highest_price"
                    ] = highest_price

        # ----------------------------------------------------
        # STOP LOSS
        # ----------------------------------------------------

        stop_loss_price = (
            entry_price *
            (1.0 - STOP_LOSS_PCT)
        )

        if current_price <= stop_loss_price:

            logger.info(
                f"{symbol} | "
                f"STOP LOSS | "
                f"Entry={entry_price:.10f} | "
                f"Current={current_price:.10f} | "
                f"SL={stop_loss_price:.10f}"
            )

            sell_symbol(
                symbol,
                "STOP_LOSS"
            )

            return

        # ----------------------------------------------------
        # TRAILING ACTIVATION
        # ----------------------------------------------------

        activation_price = (
            entry_price *
            (1.0 + TRAILING_ACTIVATION_PCT)
        )

        if (
            not trailing_active and
            current_price >= activation_price
        ):

            trailing_active = True

            with positions_lock:

                if symbol in positions:

                    positions[symbol][
                        "trailing_active"
                    ] = True

            logger.info(
                f"{symbol} | "
                f"TRAILING ACTIVATED | "
                f"Entry={entry_price:.10f} | "
                f"Current={current_price:.10f}"
            )

        # ----------------------------------------------------
        # TRAILING STOP
        # ----------------------------------------------------

        if trailing_active:

            trailing_price = (
                highest_price *
                (1.0 - TRAILING_STOP_PCT)
            )

            if current_price <= trailing_price:

                logger.info(
                    f"{symbol} | "
                    f"TRAILING STOP | "
                    f"Highest={highest_price:.10f} | "
                    f"Current={current_price:.10f} | "
                    f"Trail={trailing_price:.10f}"
                )

                sell_symbol(
                    symbol,
                    "TRAILING_STOP"
                )

    except Exception as e:

        logger.exception(
            f"{symbol} | Position check error: {e}"
        )


# ============================================================
# PROCESS CLOSED CANDLE
# ============================================================

def process_closed_candle(symbol):

    try:

        with positions_lock:

            if symbol in positions:

                return

        df = get_closed_klines(
            symbol,
            100
        )

        if df is None:
            return

        df = calculate_indicators(
            df
        )

        if df is None:
            return

        if len(df) < 50:
            return

        # ----------------------------------------------------
        # PREVIOUS CANDLE = CLOSED CANDLE
        # ----------------------------------------------------

        candle = df.iloc[-2]

        # ----------------------------------------------------
        # LOG INDICATORS
        # ----------------------------------------------------

        try:

            logger.info(
                f"{symbol} | "
                f"Open={float(candle['open']):.10f} | "
                f"Close={float(candle['close']):.10f} | "
                f"BBL={float(candle['BB_LOWER']):.10f} | "
                f"ADX={float(candle['ADX14']):.2f} | "
                f"+DI={float(candle['PLUS_DI']):.2f} | "
                f"-DI={float(candle['MINUS_DI']):.2f}"
            )

        except Exception:
            pass

        # ----------------------------------------------------
        # BUY
        # ----------------------------------------------------

        if entry_signal(candle):

            logger.info(
                f"{symbol} | "
                f"BUY SIGNAL | "
                f"Open={float(candle['open']):.10f} | "
                f"Close={float(candle['close']):.10f} | "
                f"BBL={float(candle['BB_LOWER']):.10f} | "
                f"ADX={float(candle['ADX14']):.2f} | "
                f"+DI={float(candle['PLUS_DI']):.2f} | "
                f"-DI={float(candle['MINUS_DI']):.2f}"
            )

            buy_symbol(
                symbol
            )

    except Exception as e:

        logger.exception(
            f"{symbol} | Candle processing error: {e}"
        )


# ============================================================
# WEBSOCKET MESSAGE
# ============================================================

def process_ws_message(message):

    try:

        data = json.loads(message)

        # ----------------------------------------------------
        # Combined stream wrapper
        # ----------------------------------------------------

        if "data" in data:

            data = data["data"]

        event_type = data.get("e")

        # ====================================================
        # KLINE
        # ====================================================

        if event_type == "kline":

            kline = data.get(
                "k",
                {}
            )

            symbol = kline.get("s")

            is_closed = kline.get(
                "x",
                False
            )

            if (
                symbol and
                is_closed
            ):

                process_closed_candle(
                    symbol
                )

        # ====================================================
        # MINI TICKER
        # ====================================================

        elif event_type == "24hrMiniTicker":

            symbol = data.get("s")

            close_price = data.get("c")

            if not symbol or not close_price:
                return

            try:

                current_price = float(
                    close_price
                )

            except Exception:

                return

            # Only monitor symbols that actually have positions
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
            f"WebSocket message error: {e}"
        )


# ============================================================
# WEBSOCKET CALLBACKS
# ============================================================

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
        f"WebSocket error: {error}"
    )


def on_close(
    ws,
    close_status_code,
    close_msg
):

    logger.warning(
        "WebSocket connection closed."
    )


def on_open(
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

    # --------------------------------------------------------
    # KLINE + MINI TICKER
    #
    # KLINE:
    # Used for BUY signal.
    #
    # MINI TICKER:
    # Used for real-time position monitoring.
    #
    # 150 + 150 = 300 streams
    #
    # This is far safer for REST API usage than continuously
    # calling get_symbol_ticker() for every active position.
    # --------------------------------------------------------

    for symbol in symbols:

        streams.append(
            f"{symbol.lower()}@kline_5m"
        )

        streams.append(
            f"{symbol.lower()}@miniTicker"
        )

    return (
        "wss://stream.binance.com:9443/"
        "stream?streams="
        + "/".join(streams)
    )


# ============================================================
# WEBSOCKET LOOP
# ============================================================

def websocket_loop():

    while True:

        try:

            if not symbols:

                logger.warning(
                    "No symbols available."
                )

                time.sleep(10)

                continue

            url = make_stream_url()

            logger.info(
                f"Connecting WebSocket for "
                f"{len(symbols)} symbols..."
            )

            ws = websocket.WebSocketApp(
                url,
                on_open=on_open,
                on_message=on_message,
                on_error=on_error,
                on_close=on_close
            )

            ws.run_forever(
                ping_interval=30,
                ping_timeout=20
            )

        except Exception as e:

            logger.exception(
                f"WebSocket loop error: {e}"
            )

        logger.warning(
            "WebSocket reconnecting in 10 seconds..."
        )

        time.sleep(10)


# ============================================================
# POSITION MONITOR
# ============================================================

def position_monitor():

    logger.info(
        "Position monitor started."
    )

    # --------------------------------------------------------
    # IMPORTANT:
    #
    # Position prices are now received from WebSocket
    # miniTicker.
    #
    # Therefore we DO NOT continuously call REST API here.
    #
    # This significantly reduces Binance REST API usage.
    # --------------------------------------------------------

    while True:

        try:

            with positions_lock:

                position_count = len(
                    positions
                )

            if position_count > 0:

                logger.debug(
                    f"Position monitor active: "
                    f"{position_count} positions"
                )

        except Exception as e:

            logger.exception(
                f"Position monitor error: {e}"
            )

        time.sleep(10)


# ============================================================
# RECOVER POSITIONS
# ============================================================

def recover_positions():

    logger.info(
        "Checking existing balances..."
    )

    try:

        account = client.get_account()

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

            if symbol not in symbols:
                continue

            if asset in STABLECOINS:
                continue

            if asset in {
                "BTC",
                "ETH"
            }:
                continue

            current_price = get_current_price(
                symbol
            )

            if (
                current_price is None or
                current_price <= 0
            ):
                continue

            with positions_lock:

                positions[symbol] = {

                    "entry_price":
                        float(current_price),

                    "quantity":
                        float(total),

                    "highest_price":
                        float(current_price),

                    "trailing_active":
                        False,

                    "buy_time":
                        time.time()
                }

            recovered += 1

            logger.warning(
                f"{symbol} | "
                f"POSITION RECOVERED | "
                f"Qty={total} | "
                f"Reference price={current_price}"
            )

            # Small safety delay
            time.sleep(
                KLINE_REQUEST_DELAY
            )

        logger.info(
            f"Recovered positions: {recovered}"
        )

    except Exception as e:

        logger.exception(
            f"Position recovery error: {e}"
        )


# ============================================================
# INITIALIZE
# ============================================================

def initialize():

    logger.info(
        "=" * 70
    )

    logger.info(
        "BB20 + ADX14 + DI "
        "BINANCE SPOT BOT STARTING"
    )

    logger.info(
        "BUY RULE:"
    )

    logger.info(
        "Open < BB20 Lower"
    )

    logger.info(
        "Close > BB20 Lower"
    )

    logger.info(
        f"ADX14 > {ADX_MIN}"
    )

    logger.info(
        "+DI > -DI"
    )

    logger.info(
        "EMA5 FILTERS DISABLED"
    )

    logger.info(
        f"STOP LOSS = "
        f"{STOP_LOSS_PCT * 100:.2f}%"
    )

    logger.info(
        f"TRAILING ACTIVATION = "
        f"{TRAILING_ACTIVATION_PCT * 100:.2f}%"
    )

    logger.info(
        f"TRAILING STOP = "
        f"{TRAILING_STOP_PCT * 100:.2f}%"
    )

    logger.info(
        f"TRADE AMOUNT = "
        f"{TRADE_AMOUNT_USDT} USDT"
    )

    logger.info(
        f"TIMEFRAME = {TIMEFRAME}"
    )

    logger.info(
        f"TOP SYMBOLS = {TOP_SYMBOLS}"
    )

    logger.info(
        "WebSocket = KLINE + MINI TICKER"
    )

    logger.info(
        "REST position polling = DISABLED"
    )

    logger.info(
        "REST KLINE request cooldown enabled"
    )

    logger.info(
        "=" * 70
    )

    # --------------------------------------------------------
    # Exchange information
    # --------------------------------------------------------

    load_exchange_info()

    # --------------------------------------------------------
    # Select symbols
    # --------------------------------------------------------

    load_top_symbols()

    # --------------------------------------------------------
    # Recover existing positions
    # --------------------------------------------------------

    recover_positions()


# ============================================================
# MAIN
# ============================================================

if __name__ == "__main__":

    initialize()

    # --------------------------------------------------------
    # WebSocket thread
    # --------------------------------------------------------

    websocket_thread = threading.Thread(
        target=websocket_loop,
        daemon=True
    )

    websocket_thread.start()

    # --------------------------------------------------------
    # Position monitor
    # --------------------------------------------------------

    monitor_thread = threading.Thread(
        target=position_monitor,
        daemon=True
    )

    monitor_thread.start()

    # --------------------------------------------------------
    # Flask server
    # --------------------------------------------------------

    port = int(
        os.environ.get(
            "PORT",
            10000
        )
    )

    logger.info(
        f"Starting Flask server on port {port}"
    )

    app.run(
        host="0.0.0.0",
        port=port
    )
