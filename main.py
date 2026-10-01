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


# ------------------------------------------------------------
# Trading settings
# ------------------------------------------------------------

TRADE_AMOUNT_USDT = 35.0

TIMEFRAME = Client.KLINE_INTERVAL_5MINUTE

TOP_SYMBOLS = 150

# BUY
ADX_MIN = 20.0

# STOP LOSS
STOP_LOSS_PCT = 0.0100          # 1.00%

# TRAILING
TRAILING_ACTIVATION_PCT = 0.0100  # +1.00%
TRAILING_STOP_PCT = 0.0050        # 0.50%


# ------------------------------------------------------------
# Binance
# ------------------------------------------------------------

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
    return "BB20 + EMA5 + ADX14 BINANCE SPOT BOT STARTING"


@app.route("/health")
def health():
    return jsonify({
        "status": "running",
        "symbols": len(symbols),
        "positions": len(positions)
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


# ============================================================
# CONSTANTS
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
                    filters["stepSize"] = f.get("stepSize")
                    filters["minQty"] = f.get("minQty")

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
# TOP SYMBOLS
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

            symbol = ticker.get("symbol", "")

            if not symbol.endswith("USDT"):
                continue

            if symbol not in symbol_info:
                continue

            base = symbol_info[symbol]["baseAsset"]

            if base in STABLECOINS:
                continue

            if base in {"BTC", "ETH"}:
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

        # Volume অনুযায়ী sort
        candidates.sort(
            key=lambda x: x[1],
            reverse=True
        )

        symbols = [
            x[0]
            for x in candidates[:TOP_SYMBOLS]
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

        qty = Decimal(
            str(quantity)
        )

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
# CURRENT PRICE
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

def get_closed_klines(
    symbol,
    limit=100
):

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

        # Force numeric dtype
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
            ).astype("float64")

        df = df.dropna(
            subset=[
                "open",
                "high",
                "low",
                "close",
                "volume"
            ]
        ).reset_index(
            drop=True
        )

        if len(df) < 50:
            return None

        return df

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

        # ----------------------------------------------------
        # Force numeric
        # ----------------------------------------------------

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

        # ----------------------------------------------------
        # Convert to pure float64 Series
        # ----------------------------------------------------

        close_values = df[
            "close"
        ].to_numpy(
            dtype="float64"
        )

        high_values = df[
            "high"
        ].to_numpy(
            dtype="float64"
        )

        low_values = df[
            "low"
        ].to_numpy(
            dtype="float64"
        )

        close = pd.Series(
            close_values,
            dtype="float64"
        )

        high = pd.Series(
            high_values,
            dtype="float64"
        )

        low = pd.Series(
            low_values,
            dtype="float64"
        )

        # ====================================================
        # EMA 5
        # ====================================================

        ema5 = close.ewm(
            span=5,
            adjust=False,
            min_periods=5
        ).mean()

        # ====================================================
        # BOLLINGER BAND 20
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
        # ADX 14
        # ====================================================

        prev_close = close.shift(1)
        prev_high = high.shift(1)
        prev_low = low.shift(1)

        # ----------------------------------------------------
        # True Range
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

        # IMPORTANT:
        # Do not use pd.concat(...).max(...)
        # This avoids "No numeric types to aggregate"
        # type of aggregation issue.

        tr = tr1.copy()

        mask_tr2 = tr2 > tr
        tr.loc[mask_tr2] = tr2.loc[mask_tr2]

        mask_tr3 = tr3 > tr
        tr.loc[mask_tr3] = tr3.loc[mask_tr3]

        # ----------------------------------------------------
        # Directional Movement
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
        # Wilder smoothing
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

        # ----------------------------------------------------
        # DI
        # ----------------------------------------------------

        atr_safe = atr.copy()

        atr_safe.loc[
            atr_safe == 0
        ] = float("nan")

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

        df["ADX14"] = pd.Series(
            adx14.to_numpy(
                dtype="float64"
            ),
            index=df.index,
            dtype="float64"
        )

        # Final numeric cleanup
        for col in [
            "EMA5",
            "BB_MIDDLE",
            "BB_UPPER",
            "BB_LOWER",
            "ADX14"
        ]:

            df[col] = pd.to_numeric(
                df[col],
                errors="coerce"
            ).astype("float64")

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

        adx14 = float(
            candle["ADX14"]
        )

        # Invalid indicator
        if any([
            pd.isna(open_price),
            pd.isna(high_price),
            pd.isna(close_price),
            pd.isna(bb_lower),
            pd.isna(ema5),
            pd.isna(adx14)
        ]):
            return False

        # ----------------------------------------------------
        # BUY CONDITIONS
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

        condition_5 = (
            adx14 > ADX_MIN
        )

        return (
            condition_1 and
            condition_2 and
            condition_3 and
            condition_4 and
            condition_5
        )

    except Exception as e:

        logger.error(
            f"Entry signal error: {e}"
        )

        return False


# ============================================================
# BUY
# ============================================================

def buy_symbol(symbol):

    try:

        with positions_lock:

            if symbol in positions:
                return False

        price = get_current_price(
            symbol
        )

        if price is None or price <= 0:
            return False

        # ----------------------------------------------------
        # Quantity
        # ----------------------------------------------------

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

        # ----------------------------------------------------
        # MIN NOTIONAL
        # ----------------------------------------------------

        min_notional = float(
            get_min_notional(symbol)
        )

        if (
            quantity * price
            < min_notional
        ):
            logger.warning(
                f"{symbol} | "
                f"Notional below minimum."
            )
            return False

        logger.info(
            f"{symbol} | BUY MARKET | "
            f"Qty={quantity}"
        )

        order = client.order_market_buy(
            symbol=symbol,
            quantity=quantity
        )

        # ----------------------------------------------------
        # Actual fill price
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

        # ----------------------------------------------------
        # Save position
        # ----------------------------------------------------

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
                "buy_time": time.time()
            }

        logger.info(
            f"{symbol} | BUY SUCCESS | "
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
# SELL
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
            f"{symbol} | SELL SUCCESS | "
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

def process_closed_candle(
    symbol
):

    try:

        # Already holding position
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

        # Last candle = possibly still forming
        # So use previous candle
        candle = df.iloc[-2]

        # ----------------------------------------------------
        # Debug information
        # ----------------------------------------------------

        try:

            logger.info(
                f"{symbol} | "
                f"Close={float(candle['close']):.10f} | "
                f"BBL={float(candle['BB_LOWER']):.10f} | "
                f"EMA5={float(candle['EMA5']):.10f} | "
                f"ADX={float(candle['ADX14']):.2f}"
            )

        except Exception:
            pass

        # ----------------------------------------------------
        # BUY SIGNAL
        # ----------------------------------------------------

        if entry_signal(candle):

            logger.info(
                f"{symbol} | "
                f"BUY SIGNAL | "
                f"Open={float(candle['open']):.10f} | "
                f"High={float(candle['high']):.10f} | "
                f"Close={float(candle['close']):.10f} | "
                f"BBL={float(candle['BB_LOWER']):.10f} | "
                f"EMA5={float(candle['EMA5']):.10f} | "
                f"ADX={float(candle['ADX14']):.2f}"
            )

            buy_symbol(
                symbol
            )

    except Exception as e:

        logger.exception(
            f"{symbol} | "
            f"Candle processing error: {e}"
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
        # Combined stream
        # ----------------------------------------------------

        if "data" in data:

            data = data["data"]

        event_type = data.get(
            "e"
        )

        # ----------------------------------------------------
        # KLINE
        # ----------------------------------------------------

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
                symbol and
                is_closed
            ):

                process_closed_candle(
                    symbol
                )

        # ----------------------------------------------------
        # MINI TICKER
        # ----------------------------------------------------

        elif event_type == "24hrMiniTicker":

            symbol = data.get(
                "s"
            )

            price = data.get(
                "c"
            )

            if (
                symbol and
                price
            ):

                try:

                    current_price = float(
                        price
                    )

                    check_position(
                        symbol,
                        current_price
                    )

                except Exception:
                    pass

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

    for symbol in symbols:

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
                ping_interval=20,
                ping_timeout=10
            )

        except Exception as e:

            logger.exception(
                f"WebSocket loop error: {e}"
            )

        logger.warning(
            "WebSocket reconnecting in 5 seconds..."
        )

        time.sleep(5)


# ============================================================
# POSITION MONITOR
# ============================================================

def position_monitor():

    logger.info(
        "Position monitor started."
    )

    while True:

        try:

            with positions_lock:

                active_symbols = list(
                    positions.keys()
                )

            for symbol in active_symbols:

                try:

                    price = get_current_price(
                        symbol
                    )

                    if price is not None:

                        check_position(
                            symbol,
                            price
                        )

                except Exception as e:

                    logger.error(
                        f"{symbol} | "
                        f"Monitor error: {e}"
                    )

                time.sleep(0.2)

        except Exception as e:

            logger.exception(
                f"Position monitor error: {e}"
            )

        time.sleep(2)


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

            # ------------------------------------------------
            # Important:
            # We do not know historical entry price after
            # restart, so current price is used as recovery
            # reference.
            # ------------------------------------------------

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
                    "entry_price": float(
                        current_price
                    ),
                    "quantity": float(
                        total
                    ),
                    "highest_price": float(
                        current_price
                    ),
                    "trailing_active": False,
                    "buy_time": time.time()
                }

            recovered += 1

            logger.warning(
                f"{symbol} | "
                f"POSITION RECOVERED | "
                f"Qty={total} | "
                f"Reference price={current_price}"
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
        "BB20 + EMA5 + ADX14 "
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
        "Close < EMA5"
    )

    logger.info(
        "High < EMA5"
    )

    logger.info(
        f"ADX14 > {ADX_MIN}"
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
        "=" * 70
    )

    # Exchange info
    load_exchange_info()

    # Top symbols
    load_top_symbols()

    # Recover existing balances
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
    # Position monitor thread
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
