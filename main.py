import os
import time
import json
import math
import threading
import logging
from decimal import Decimal, ROUND_DOWN

import numpy as np
import pandas as pd
import websocket

from flask import Flask, jsonify
from binance.client import Client
from binance.exceptions import BinanceAPIException, BinanceOrderException


# ============================================================
# CONFIG
# ============================================================

API_KEY = os.environ.get("BINANCE_API_KEY")
API_SECRET = os.environ.get("BINANCE_API_SECRET")

if not API_KEY or not API_SECRET:
    raise RuntimeError(
        "BINANCE_API_KEY and BINANCE_API_SECRET environment variables are required."
    )


# -----------------------------
# Trading settings
# -----------------------------

TRADE_AMOUNT_USDT = 35.0

TIMEFRAME = "5m"

TOP_SYMBOLS = 150

BB_PERIOD = 20
BB_STD = 2.0

RSI_PERIOD = 3

VOLUME_SMA_PERIOD = 20
VOLUME_MULTIPLIER = 1.20

STOP_LOSS_PCT = 0.01

SELL_AT_UPPER_BB = True

# Sell quantity slightly below free balance
SELL_BALANCE_BUFFER = Decimal("0.999")

# Minimum seconds between BUY attempts on same symbol
BUY_COOLDOWN_SECONDS = 60

# Initial historical candles
HISTORY_LIMIT = 100

# REST delay between historical kline requests
KLINE_REQUEST_DELAY = 0.50

# WebSocket reconnect
WS_RECONNECT_DELAY = 10

# Maximum positions
# None = unlimited
MAX_POSITIONS = None


# ============================================================
# LOGGING
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

logger = logging.getLogger("BB_RSI_VOLUME_BOT")


# ============================================================
# FLASK
# ============================================================

app = Flask(__name__)


@app.route("/")
def home():
    return jsonify({
        "status": "running",
        "bot": "BB20 + RSI3 + Volume",
        "timeframe": TIMEFRAME,
        "trade_amount_usdt": TRADE_AMOUNT_USDT,
        "top_symbols": TOP_SYMBOLS
    })


@app.route("/health")
def health():
    return jsonify({
        "status": "healthy",
        "symbols": len(symbols),
        "positions": len(positions),
        "websocket_connected": ws_connected
    })


# ============================================================
# GLOBAL STATE
# ============================================================

client = None

symbol_info = {}
symbols = []

# symbol -> dataframe
market_data = {}

# symbol -> current live price
live_prices = {}

# symbol -> position information
positions = {}

# symbol -> last BUY timestamp
last_buy_time = {}

# symbol -> last processed candle open time
last_processed_candle = {}

# Locks
positions_lock = threading.Lock()
data_lock = threading.Lock()
price_lock = threading.Lock()

ws_connected = False
ws_last_message_time = 0

bot_started = False


# ============================================================
# STABLECOINS / EXCLUDED ASSETS
# ============================================================

STABLECOINS = {
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
    "TRY",
    "BRL",
    "AUD",
    "JPY",
    "RUB",
    "UAH",
    "PLN",
    "RON",
    "ARS",
    "NGN",
    "ZAR",
    "IDR",
    "BIDR",
    "COP",
    "MXN"
}

EXCLUDED_BASE_ASSETS = {
    "BTC",
    "ETH"
}


# ============================================================
# BINANCE CLIENT
# ============================================================

def create_binance_client():
    """
    Important:
    ping=False prevents python-binance from making an unnecessary
    startup REST ping request.
    """

    logger.info("Creating Binance client...")

    try:
        c = Client(
            API_KEY,
            API_SECRET,
            ping=False
        )

        logger.info("Binance client created without startup ping.")

        return c

    except TypeError:
        # Compatibility fallback for older python-binance versions
        logger.warning(
            "Installed python-binance does not support ping=False. "
            "Using normal Client constructor."
        )

        return Client(
            API_KEY,
            API_SECRET
        )


# ============================================================
# DECIMAL HELPERS
# ============================================================

def floor_to_step(value, step):
    """
    Floor quantity/price to Binance LOT_SIZE/TICK_SIZE.
    """

    try:
        value_d = Decimal(str(value))
        step_d = Decimal(str(step))

        if step_d <= 0:
            return value_d

        return (value_d / step_d).to_integral_value(
            rounding=ROUND_DOWN
        ) * step_d

    except Exception:
        return Decimal("0")


def decimal_to_str(value):
    """
    Binance accepts decimal strings better than floats.
    """

    if isinstance(value, Decimal):
        return format(value, "f")

    return format(Decimal(str(value)), "f")


# ============================================================
# EXCHANGE INFO
# ============================================================

def load_exchange_info():
    global symbol_info

    logger.info("Loading exchange information...")

    try:
        info = client.get_exchange_info()

    except Exception as e:
        logger.error(f"Exchange info error: {e}")
        raise

    count = 0

    for item in info.get("symbols", []):

        try:
            symbol = item["symbol"]

            if item.get("status") != "TRADING":
                continue

            if item.get("quoteAsset") != "USDT":
                continue

            if item.get("isSpotTradingAllowed") is False:
                continue

            base_asset = item.get("baseAsset", "")

            if base_asset in EXCLUDED_BASE_ASSETS:
                continue

            if base_asset in STABLECOINS:
                continue

            filters = {}

            for f in item.get("filters", []):
                filters[f["filterType"]] = f

            symbol_info[symbol] = {
                "base_asset": base_asset,
                "quote_asset": item.get("quoteAsset"),
                "filters": filters
            }

            count += 1

        except Exception:
            continue

    logger.info(
        f"Loaded {count} eligible USDT spot symbols."
    )


# ============================================================
# SYMBOL FILTERS
# ============================================================

def get_symbol_filter(symbol, filter_name):
    return symbol_info.get(symbol, {}).get(
        "filters", {}
    ).get(filter_name)


def get_step_size(symbol):
    f = get_symbol_filter(symbol, "LOT_SIZE")

    if f:
        return Decimal(str(f.get("stepSize", "0.00000001")))

    f = get_symbol_filter(symbol, "MARKET_LOT_SIZE")

    if f:
        return Decimal(str(f.get("stepSize", "0.00000001")))

    return Decimal("0.00000001")


def get_min_qty(symbol):
    f = get_symbol_filter(symbol, "LOT_SIZE")

    if f:
        return Decimal(str(f.get("minQty", "0")))

    f = get_symbol_filter(symbol, "MARKET_LOT_SIZE")

    if f:
        return Decimal(str(f.get("minQty", "0")))

    return Decimal("0")


def get_min_notional(symbol):
    f = get_symbol_filter(symbol, "NOTIONAL")

    if f:
        return Decimal(str(f.get("minNotional", "0")))

    f = get_symbol_filter(symbol, "MIN_NOTIONAL")

    if f:
        return Decimal(str(f.get("minNotional", "0")))

    return Decimal("0")


# ============================================================
# TOP SYMBOL SELECTION
# ============================================================

def select_top_symbols():
    """
    This REST call happens only at startup.

    We do NOT continuously call get_ticker().
    """

    global symbols

    logger.info("Selecting top USDT ALT symbols by 24h quote volume...")

    try:
        tickers = client.get_ticker()

    except Exception as e:
        logger.error(f"Ticker request failed: {e}")
        raise

    candidates = []

    for ticker in tickers:

        try:
            symbol = ticker.get("symbol")

            if symbol not in symbol_info:
                continue

            quote_volume = float(
                ticker.get("quoteVolume", 0)
            )

            if not math.isfinite(quote_volume):
                continue

            if quote_volume <= 0:
                continue

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
        x[0]
        for x in candidates[:TOP_SYMBOLS]
    ]

    logger.info(
        f"Selected {len(symbols)} symbols."
    )

    logger.info(
        "Top symbols: " +
        ", ".join(symbols[:30])
    )


# ============================================================
# HISTORICAL KLINES
# ============================================================

def load_initial_history():

    logger.info(
        f"Loading {HISTORY_LIMIT} historical {TIMEFRAME} candles "
        f"for {len(symbols)} symbols..."
    )

    loaded = 0

    for index, symbol in enumerate(symbols, start=1):

        try:

            klines = client.get_klines(
                symbol=symbol,
                interval=TIMEFRAME,
                limit=HISTORY_LIMIT
            )

            rows = []

            for k in klines:

                rows.append({
                    "open_time": int(k[0]),
                    "open": float(k[1]),
                    "high": float(k[2]),
                    "low": float(k[3]),
                    "close": float(k[4]),
                    "volume": float(k[5]),
                    "close_time": int(k[6])
                })

            if rows:

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
                    )

                df = df.dropna(
                    subset=numeric_cols
                ).reset_index(drop=True)

                with data_lock:
                    market_data[symbol] = df

                loaded += 1

        except BinanceAPIException as e:

            logger.error(
                f"{symbol} | Kline API error: {e}"
            )

            # If Binance starts rate limiting, slow down.
            if getattr(e, "code", None) == -1003:
                logger.error(
                    "Binance rate limit detected during history loading. "
                    "Stopping history loading."
                )
                break

        except Exception as e:

            logger.error(
                f"{symbol} | Kline error: {e}"
            )

        # Important:
        # Spread REST calls instead of firing 150 requests quickly.
        time.sleep(KLINE_REQUEST_DELAY)

        if index % 25 == 0:
            logger.info(
                f"History progress: {index}/{len(symbols)}"
            )

    logger.info(
        f"Historical data loaded for {loaded}/{len(symbols)} symbols."
    )


# ============================================================
# RSI
# ============================================================

def calculate_rsi(series, period=3):

    series = pd.to_numeric(
        series,
        errors="coerce"
    )

    delta = series.diff()

    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)

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

    rs = avg_gain / avg_loss.replace(
        0,
        np.nan
    )

    rsi = 100 - (
        100 / (1 + rs)
    )

    # If average loss is zero, RSI is effectively 100.
    rsi = rsi.where(
        avg_loss != 0,
        100
    )

    # If both gain/loss unavailable, keep NaN.
    rsi = rsi.where(
        avg_gain.notna(),
        np.nan
    )

    return rsi


# ============================================================
# INDICATORS
# ============================================================

def calculate_indicators(df):

    df = df.copy()

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
        subset=numeric_cols
    ).reset_index(drop=True)

    if len(df) < max(
        BB_PERIOD,
        RSI_PERIOD,
        VOLUME_SMA_PERIOD
    ) + 5:
        return df

    # -------------------------
    # Bollinger Band
    # -------------------------

    df["bb_middle"] = (
        df["close"]
        .rolling(BB_PERIOD)
        .mean()
    )

    df["bb_std"] = (
        df["close"]
        .rolling(BB_PERIOD)
        .std(ddof=0)
    )

    df["bb_upper"] = (
        df["bb_middle"]
        + BB_STD * df["bb_std"]
    )

    df["bb_lower"] = (
        df["bb_middle"]
        - BB_STD * df["bb_std"]
    )

    # -------------------------
    # RSI 3
    # -------------------------

    df["rsi3"] = calculate_rsi(
        df["close"],
        RSI_PERIOD
    )

    # -------------------------
    # Volume SMA20
    # -------------------------

    df["volume_sma20"] = (
        df["volume"]
        .rolling(VOLUME_SMA_PERIOD)
        .mean()
    )

    return df


# ============================================================
# BUY CONDITION
# ============================================================

def buy_signal(df):

    if df is None:
        return False, None

    if len(df) < 30:
        return False, None

    df = calculate_indicators(df)

    if len(df) < 30:
        return False, None

    # Last CLOSED candle
    row = df.iloc[-2]

    required = [
        row["close"],
        row["bb_lower"],
        row["rsi3"],
        row["volume"],
        row["volume_sma20"]
    ]

    if any(
        pd.isna(x)
        for x in required
    ):
        return False, row

    close_price = float(row["close"])
    bb_lower = float(row["bb_lower"])
    rsi3 = float(row["rsi3"])
    volume = float(row["volume"])
    volume_sma20 = float(row["volume_sma20"])

    condition_bb = (
        close_price < bb_lower
    )

    condition_rsi = (
        rsi3 < 10
    )

    condition_volume = (
        volume >
        volume_sma20 * VOLUME_MULTIPLIER
    )

    signal = (
        condition_bb
        and condition_rsi
        and condition_volume
    )

    return signal, row


# ============================================================
# LIVE PRICE
# ============================================================

def set_live_price(symbol, price):

    try:

        price = float(price)

        if price <= 0:
            return

        with price_lock:
            live_prices[symbol] = price

    except Exception:
        pass


def get_live_price(symbol):

    with price_lock:
        return live_prices.get(symbol)


# ============================================================
# BUY
# ============================================================

def execute_buy(symbol, signal_row):

    now = time.time()

    # Cooldown
    last_time = last_buy_time.get(
        symbol,
        0
    )

    if now - last_time < BUY_COOLDOWN_SECONDS:
        return

    # Already holding
    with positions_lock:

        if symbol in positions:
            return

        if (
            MAX_POSITIONS is not None
            and len(positions) >= MAX_POSITIONS
        ):
            logger.info(
                f"{symbol} | MAX_POSITIONS reached."
            )
            return

    price = get_live_price(symbol)

    if not price:
        logger.warning(
            f"{symbol} | No live price available for BUY."
        )
        return

    try:

        logger.info(
            f"{symbol} | BUY signal | "
            f"Close={signal_row['close']:.8f} | "
            f"BBLower={signal_row['bb_lower']:.8f} | "
            f"RSI3={signal_row['rsi3']:.2f} | "
            f"Volume={signal_row['volume']:.2f} | "
            f"VolSMA20={signal_row['volume_sma20']:.2f}"
        )

        logger.info(
            f"{symbol} | Sending MARKET BUY | "
            f"USDT={TRADE_AMOUNT_USDT}"
        )

        order = client.create_order(
            symbol=symbol,
            side=Client.SIDE_BUY,
            type=Client.ORDER_TYPE_MARKET,
            quoteOrderQty=decimal_to_str(
                Decimal(str(TRADE_AMOUNT_USDT))
            ),
            newOrderRespType="FULL"
        )

        executed_qty = Decimal("0")
        executed_quote = Decimal("0")

        # -------------------------
        # Extract fills
        # -------------------------

        fills = order.get("fills", [])

        if fills:

            for fill in fills:

                qty = Decimal(
                    str(fill.get("qty", "0"))
                )

                fill_price = Decimal(
                    str(fill.get("price", "0"))
                )

                executed_qty += qty

                executed_quote += (
                    qty * fill_price
                )

        # -------------------------
        # Fallback
        # -------------------------

        if executed_qty <= 0:

            executed_qty = Decimal(
                str(order.get("executedQty", "0"))
            )

        if executed_qty <= 0:

            logger.error(
                f"{symbol} | BUY executed but quantity "
                f"could not be determined."
            )

            return

        # Actual average entry
        if executed_quote > 0:

            entry_price = (
                executed_quote /
                executed_qty
            )

        else:

            entry_price = Decimal(
                str(price)
            )

        entry_price = entry_price.quantize(
            Decimal("0.00000001")
        )

        stop_price = (
            entry_price *
            Decimal("0.99")
        )

        stop_price = stop_price.quantize(
            Decimal("0.00000001")
        )

        last_buy_time[symbol] = now

        position = {
            "symbol": symbol,
            "entry_price": float(entry_price),
            "quantity": float(executed_qty),
            "stop_price": float(stop_price),
            "buy_time": now,

            # Upper BB of the closed candle
            # that created the signal.
            "signal_upper_bb": float(
                signal_row["bb_upper"]
            )
        }

        with positions_lock:
            positions[symbol] = position

        logger.info(
            f"{symbol} | BUY FILLED | "
            f"Entry={entry_price} | "
            f"Qty={executed_qty} | "
            f"SL={stop_price} | "
            f"UpperBB={signal_row['bb_upper']}"
        )

    except BinanceAPIException as e:

        logger.error(
            f"{symbol} | BUY Binance API error: {e}"
        )

    except BinanceOrderException as e:

        logger.error(
            f"{symbol} | BUY order error: {e}"
        )

    except Exception as e:

        logger.exception(
            f"{symbol} | BUY unexpected error: {e}"
        )


# ============================================================
# SELL
# ============================================================

def execute_sell(
    symbol,
    reason,
    live_price=None
):

    with positions_lock:

        position = positions.get(symbol)

        if not position:
            return

        # Prevent two simultaneous sells
        if position.get("selling"):
            return

        position["selling"] = True

    try:

        # ------------------------------------
        # Get actual free balance
        # ------------------------------------

        base_asset = symbol_info[symbol]["base_asset"]

        balance = client.get_asset_balance(
            asset=base_asset
        )

        free_balance = Decimal(
            str(
                balance.get("free", "0")
                if balance
                else "0"
            )
        )

        if free_balance <= 0:

            logger.error(
                f"{symbol} | No free balance available to SELL."
            )

            with positions_lock:
                positions.pop(symbol, None)

            return

        quantity = (
            free_balance *
            SELL_BALANCE_BUFFER
        )

        step_size = get_step_size(symbol)

        quantity = floor_to_step(
            quantity,
            step_size
        )

        min_qty = get_min_qty(symbol)

        if quantity < min_qty:

            logger.error(
                f"{symbol} | Sell quantity below minQty. "
                f"Qty={quantity} MinQty={min_qty}"
            )

            with positions_lock:
                positions.pop(symbol, None)

            return

        # ------------------------------------
        # Market SELL
        # ------------------------------------

        logger.info(
            f"{symbol} | SELL | Reason={reason} | "
            f"Price={live_price}"
        )

        order = client.create_order(
            symbol=symbol,
            side=Client.SIDE_SELL,
            type=Client.ORDER_TYPE_MARKET,
            quantity=decimal_to_str(quantity),
            newOrderRespType="FULL"
        )

        # ------------------------------------
        # Calculate approximate P/L
        # ------------------------------------

        exit_price = Decimal(
            str(live_price)
        ) if live_price else Decimal(
            str(position["entry_price"])
        )

        entry_price = Decimal(
            str(position["entry_price"])
        )

        pnl_pct = (
            (exit_price - entry_price)
            / entry_price
        ) * Decimal("100")

        logger.info(
            f"{symbol} | SELL FILLED | "
            f"Entry={entry_price} | "
            f"Exit≈{exit_price} | "
            f"P/L≈{pnl_pct:.3f}% | "
            f"Reason={reason}"
        )

        with positions_lock:
            positions.pop(symbol, None)

    except BinanceAPIException as e:

        logger.error(
            f"{symbol} | SELL Binance API error: {e}"
        )

        with positions_lock:
            if symbol in positions:
                positions[symbol]["selling"] = False

    except BinanceOrderException as e:

        logger.error(
            f"{symbol} | SELL order error: {e}"
        )

        with positions_lock:
            if symbol in positions:
                positions[symbol]["selling"] = False

    except Exception as e:

        logger.exception(
            f"{symbol} | SELL unexpected error: {e}"
        )

        with positions_lock:
            if symbol in positions:
                positions[symbol]["selling"] = False


# ============================================================
# POSITION CHECK
# ============================================================

def check_position(symbol):

    with positions_lock:

        position = positions.get(symbol)

        if not position:
            return

        if position.get("selling"):
            return

        position_copy = dict(position)

    price = get_live_price(symbol)

    if price is None:
        return

    entry_price = float(
        position_copy["entry_price"]
    )

    stop_price = float(
        position_copy["stop_price"]
    )

    upper_bb = float(
        position_copy["signal_upper_bb"]
    )

    # ----------------------------------------
    # STOP LOSS
    # ----------------------------------------

    if price <= stop_price:

        execute_sell(
            symbol,
            reason=f"STOP LOSS {STOP_LOSS_PCT * 100:.2f}%",
            live_price=price
        )

        return

    # ----------------------------------------
    # UPPER BB EXIT
    # ----------------------------------------

    if SELL_AT_UPPER_BB:

        if price >= upper_bb:

            execute_sell(
                symbol,
                reason="UPPER BB TOUCH",
                live_price=price
            )

            return


# ============================================================
# PROCESS CLOSED CANDLE
# ============================================================

def process_closed_candle(symbol):

    with data_lock:

        df = market_data.get(symbol)

        if df is None:
            return

        if len(df) < 30:
            return

        df_copy = df.copy()

    # Last completed candle
    candle = df_copy.iloc[-2]

    candle_time = int(
        candle["open_time"]
    )

    # Avoid processing same candle twice
    previous = last_processed_candle.get(
        symbol
    )

    if previous == candle_time:
        return

    last_processed_candle[symbol] = candle_time

    try:

        signal, row = buy_signal(
            df_copy
        )

        if row is None:
            return

        # Log only when conditions are close / useful
        if signal:

            execute_buy(
                symbol,
                row
            )

    except Exception as e:

        logger.exception(
            f"{symbol} | Candle processing error: {e}"
        )


# ============================================================
# UPDATE KLINE DATA FROM WEBSOCKET
# ============================================================

def process_kline_message(data):

    try:

        k = data.get("k")

        if not k:
            return

        symbol = k.get("s")

        if not symbol:
            return

        open_time = int(
            k["t"]
        )

        open_price = float(
            k["o"]
        )

        high_price = float(
            k["h"]
        )

        low_price = float(
            k["l"]
        )

        close_price = float(
            k["c"]
        )

        volume = float(
            k["v"]
        )

        close_time = int(
            k["T"]
        )

        candle_closed = bool(
            k["x"]
        )

        with data_lock:

            df = market_data.get(
                symbol
            )

            if df is None:

                df = pd.DataFrame(
                    columns=[
                        "open_time",
                        "open",
                        "high",
                        "low",
                        "close",
                        "volume",
                        "close_time"
                    ]
                )

            # --------------------------------
            # Update existing current candle
            # --------------------------------

            if len(df) > 0 and int(
                df.iloc[-1]["open_time"]
            ) == open_time:

                df.loc[
                    df.index[-1],
                    "open"
                ] = open_price

                df.loc[
                    df.index[-1],
                    "high"
                ] = high_price

                df.loc[
                    df.index[-1],
                    "low"
                ] = low_price

                df.loc[
                    df.index[-1],
                    "close"
                ] = close_price

                df.loc[
                    df.index[-1],
                    "volume"
                ] = volume

                df.loc[
                    df.index[-1],
                    "close_time"
                ] = close_time

            else:

                new_row = pd.DataFrame([{
                    "open_time": open_time,
                    "open": open_price,
                    "high": high_price,
                    "low": low_price,
                    "close": close_price,
                    "volume": volume,
                    "close_time": close_time
                }])

                df = pd.concat(
                    [
                        df,
                        new_row
                    ],
                    ignore_index=True
                )

            # Keep memory small
            if len(df) > 150:
                df = df.iloc[-150:].reset_index(
                    drop=True
                )

            market_data[symbol] = df

        # --------------------------------
        # Only evaluate BUY after candle close
        # --------------------------------

        if candle_closed:

            process_closed_candle(
                symbol
            )

    except Exception as e:

        logger.exception(
            f"Kline processing error: {e}"
        )


# ============================================================
# WEBSOCKET MESSAGE
# ============================================================

def process_ws_message(raw_message):

    global ws_last_message_time

    ws_last_message_time = time.time()

    try:

        message = json.loads(
            raw_message
        )

        # Binance combined stream:
        # {
        #   "stream": "...",
        #   "data": {...}
        # }

        data = message.get(
            "data",
            message
        )

        event_type = data.get("e")

        # -----------------------------
        # KLINE
        # -----------------------------

        if event_type == "kline":

            process_kline_message(
                data
            )

            return

        # -----------------------------
        # 24hr ticker / miniTicker
        # -----------------------------

        if event_type in (
            "24hrTicker",
            "24hrMiniTicker"
        ):

            symbol = data.get("s")

            if not symbol:
                return

            price = (
                data.get("c")
                or data.get("C")
            )

            if price:

                set_live_price(
                    symbol,
                    price
                )

                # If holding this symbol,
                # immediately check SL / BB.
                check_position(
                    symbol
                )

            return

    except json.JSONDecodeError:
        return

    except Exception as e:

        logger.exception(
            f"WebSocket message processing error: {e}"
        )


# ============================================================
# WEBSOCKET URL
# ============================================================

def make_stream_url():

    streams = []

    for symbol in symbols:

        s = symbol.lower()

        # 5m candle
        streams.append(
            f"{s}@kline_5m"
        )

        # Live price
        streams.append(
            f"{s}@miniTicker"
        )

    stream_string = "/".join(
        streams
    )

    return (
        "wss://stream.binance.com:9443"
        f"/stream?streams={stream_string}"
    )


# ============================================================
# WEBSOCKET LOOP
# ============================================================

def websocket_loop():

    global ws_connected

    while True:

        try:

            if not symbols:

                logger.error(
                    "No symbols available for WebSocket."
                )

                time.sleep(10)
                continue

            url = make_stream_url()

            logger.info(
                "Connecting Binance WebSocket..."
            )

            logger.info(
                f"WebSocket streams: {len(symbols) * 2}"
            )

            def on_open(ws):

                global ws_connected

                ws_connected = True

                logger.info(
                    "Binance WebSocket CONNECTED."
                )

            def on_message(ws, message):

                process_ws_message(
                    message
                )

            def on_error(ws, error):

                logger.error(
                    f"WebSocket error: {error}"
                )

            def on_close(
                ws,
                close_status_code,
                close_msg
            ):

                global ws_connected

                ws_connected = False

                logger.warning(
                    f"WebSocket closed | "
                    f"code={close_status_code} | "
                    f"msg={close_msg}"
                )

            def on_ping(ws, message):

                # websocket-client automatically handles
                # normal ping/pong frames.
                logger.debug(
                    "WebSocket PING received."
                )

            ws = websocket.WebSocketApp(
                url,
                on_open=on_open,
                on_message=on_message,
                on_error=on_error,
                on_close=on_close,
                on_ping=on_ping
            )

            # Important:
            # Do not use very aggressive ping settings.
            # Binance itself sends WebSocket ping frames.
            ws.run_forever(
                ping_interval=None,
                ping_timeout=None,
                skip_utf8_validation=True
            )

        except Exception as e:

            ws_connected = False

            logger.exception(
                f"WebSocket loop exception: {e}"
            )

        logger.warning(
            f"WebSocket reconnecting in "
            f"{WS_RECONNECT_DELAY} seconds..."
        )

        time.sleep(
            WS_RECONNECT_DELAY
        )


# ============================================================
# POSITION RECOVERY
# ============================================================

def recover_positions():

    """
    Called only once at startup.

    This is NOT a continuous REST polling loop.
    """

    logger.info(
        "Checking existing Binance balances..."
    )

    try:

        account = client.get_account()

    except BinanceAPIException as e:

        logger.error(
            f"Account recovery API error: {e}"
        )

        return

    except Exception as e:

        logger.error(
            f"Account recovery error: {e}"
        )

        return

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

            total = free + locked

            if total <= 0:
                continue

            if asset in (
                "USDT",
                "USDC",
                "FDUSD",
                "BUSD",
                "TUSD",
                "DAI",
                "BTC",
                "ETH"
            ):
                continue

            symbol = (
                asset +
                "USDT"
            )

            if symbol not in symbols:
                continue

            # Get current price only for actual
            # recovered positions.
            try:

                ticker = client.get_symbol_ticker(
                    symbol=symbol
                )

                current_price = float(
                    ticker["price"]
                )

            except Exception:

                continue

            # Recovery:
            # We do not know the exact original entry
            # from account balance alone.
            #
            # Therefore current price is used as a
            # conservative restart reference.
            #
            # This is logged clearly.

            entry_price = current_price

            stop_price = (
                entry_price *
                (1 - STOP_LOSS_PCT)
            )

            with positions_lock:

                positions[symbol] = {
                    "symbol": symbol,
                    "entry_price": entry_price,
                    "quantity": float(free),
                    "stop_price": stop_price,
                    "buy_time": time.time(),
                    "signal_upper_bb": float("inf"),
                    "recovered": True
                }

            set_live_price(
                symbol,
                current_price
            )

            recovered += 1

            logger.warning(
                f"{symbol} | Existing balance recovered. "
                f"Entry reference reset to current price "
                f"{current_price:.8f}. "
                f"Upper-BB exit disabled until a new BUY."
            )

        except Exception:
            continue

    logger.info(
        f"Recovered {recovered} existing positions."
    )


# ============================================================
# STARTUP
# ============================================================

def initialize_bot():

    global client
    global bot_started

    logger.info("=" * 70)

    logger.info(
        "STARTING BINANCE BB20 + RSI3 + VOLUME BOT"
    )

    logger.info("=" * 70)

    logger.info(
        f"TRADE_AMOUNT_USDT = {TRADE_AMOUNT_USDT}"
    )

    logger.info(
        "BUY = CLOSE < BB20 LOWER "
        "AND RSI3 < 10 "
        "AND VOLUME > SMA20 × 1.20"
    )

    logger.info(
        "SELL = UPPER BB TOUCH"
    )

    logger.info(
        f"STOP LOSS = {STOP_LOSS_PCT * 100:.2f}%"
    )

    logger.info(
        f"TIMEFRAME = {TIMEFRAME}"
    )

    logger.info(
        f"TOP SYMBOLS = {TOP_SYMBOLS}"
    )

    logger.info("=" * 70)

    # --------------------------------------
    # Binance client
    # --------------------------------------

    client = create_binance_client()

    # --------------------------------------
    # Exchange info
    # --------------------------------------

    load_exchange_info()

    # --------------------------------------
    # Top symbols
    # --------------------------------------

    select_top_symbols()

    if not symbols:

        raise RuntimeError(
            "No symbols selected."
        )

    # --------------------------------------
    # Initial candle history
    # --------------------------------------

    load_initial_history()

    # --------------------------------------
    # Recover positions
    # --------------------------------------

    recover_positions()

    bot_started = True

    logger.info(
        "Bot initialization completed."
    )


# ============================================================
# MAIN
# ============================================================

def main():

    initialize_bot()

    # --------------------------------------
    # Start WebSocket
    # --------------------------------------

    ws_thread = threading.Thread(
        target=websocket_loop,
        daemon=True
    )

    ws_thread.start()

    # --------------------------------------
    # Start Flask
    # --------------------------------------

    port = int(
        os.environ.get(
            "PORT",
            "10000"
        )
    )

    logger.info(
        f"Starting Flask on port {port}"
    )

    app.run(
        host="0.0.0.0",
        port=port,
        threaded=True
    )


if __name__ == "__main__":

    try:

        main()

    except KeyboardInterrupt:

        logger.info(
            "Bot stopped by user."
        )

    except Exception as e:

        logger.exception(
            f"Fatal startup error: {e}"
        )

        raise
