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


# ============================================================
# TRADING SETTINGS
# ============================================================

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

BUY_COOLDOWN_SECONDS = 60

HISTORY_LIMIT = 100

# Free Render-এর জন্য REST request একটু spread করা
KLINE_REQUEST_DELAY = 0.30

# WebSocket reconnect
WS_RECONNECT_DELAY = 5

# প্রতি WebSocket connection-এ সর্বোচ্চ symbol
WS_SYMBOLS_PER_CONNECTION = 50

# Position নেই এমন symbol-এর live price-ও miniTicker থেকে আসবে
# Position check শুধুমাত্র holding symbol-এ করা হবে।

# None = unlimited
MAX_POSITIONS = None

SELL_BALANCE_BUFFER = Decimal("0.999")


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


# ============================================================
# GLOBAL STATE
# ============================================================

client = None

symbol_info = {}
symbols = []

market_data = {}

live_prices = {}

positions = {}

last_buy_time = {}

last_processed_candle = {}

positions_lock = threading.Lock()
data_lock = threading.Lock()
price_lock = threading.Lock()

ws_lock = threading.Lock()

ws_connected_count = 0
ws_total_connections = 0

ws_last_message_time = 0

bot_started = False


# ============================================================
# FLASK ROUTES
# ============================================================

@app.route("/")
def home():
    with positions_lock:
        position_count = len(positions)

    return jsonify({
        "status": "running",
        "bot": "BB20 + RSI3 + Volume",
        "timeframe": TIMEFRAME,
        "trade_amount_usdt": TRADE_AMOUNT_USDT,
        "top_symbols": TOP_SYMBOLS,
        "selected_symbols": len(symbols),
        "positions": position_count,
        "websocket_connections": ws_total_connections,
        "websocket_connected": ws_connected_count > 0
    })


@app.route("/health")
def health():
    with positions_lock:
        position_count = len(positions)

    return jsonify({
        "status": "healthy",
        "symbols": len(symbols),
        "positions": position_count,
        "websocket_connections": ws_total_connections,
        "websocket_connected": ws_connected_count > 0,
        "last_ws_message": ws_last_message_time
    })


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

    logger.info("Creating Binance client...")

    try:
        c = Client(
            API_KEY,
            API_SECRET,
            ping=False
        )

        logger.info("Binance client created.")

        return c

    except TypeError:

        logger.warning(
            "Installed python-binance does not support ping=False."
        )

        return Client(
            API_KEY,
            API_SECRET
        )


# ============================================================
# DECIMAL HELPERS
# ============================================================

def floor_to_step(value, step):

    try:
        value_d = Decimal(str(value))
        step_d = Decimal(str(step))

        if step_d <= 0:
            return value_d

        return (
            value_d / step_d
        ).to_integral_value(
            rounding=ROUND_DOWN
        ) * step_d

    except Exception:
        return Decimal("0")


def decimal_to_str(value):

    if isinstance(value, Decimal):
        return format(value, "f")

    return format(
        Decimal(str(value)),
        "f"
    )


# ============================================================
# EXCHANGE INFO
# ============================================================

def load_exchange_info():

    global symbol_info

    logger.info("Loading Binance exchange information...")

    try:
        info = client.get_exchange_info()

    except Exception as e:
        logger.error(
            f"Exchange info error: {e}"
        )
        raise

    symbol_info.clear()

    count = 0

    for item in info.get("symbols", []):

        try:

            symbol = item.get("symbol")

            if not symbol:
                continue

            if item.get("status") != "TRADING":
                continue

            if item.get("quoteAsset") != "USDT":
                continue

            if item.get("isSpotTradingAllowed") is False:
                continue

            base_asset = item.get(
                "baseAsset",
                ""
            )

            if base_asset in EXCLUDED_BASE_ASSETS:
                continue

            if base_asset in STABLECOINS:
                continue

            filters = {}

            for f in item.get("filters", []):

                filter_type = f.get(
                    "filterType"
                )

                if filter_type:
                    filters[filter_type] = f

            symbol_info[symbol] = {
                "base_asset": base_asset,
                "quote_asset": "USDT",
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

def get_symbol_filter(
    symbol,
    filter_name
):

    return symbol_info.get(
        symbol,
        {}
    ).get(
        "filters",
        {}
    ).get(
        filter_name
    )


def get_step_size(symbol):

    f = get_symbol_filter(
        symbol,
        "LOT_SIZE"
    )

    if f:
        return Decimal(
            str(
                f.get(
                    "stepSize",
                    "0.00000001"
                )
            )
        )

    f = get_symbol_filter(
        symbol,
        "MARKET_LOT_SIZE"
    )

    if f:
        return Decimal(
            str(
                f.get(
                    "stepSize",
                    "0.00000001"
                )
            )
        )

    return Decimal("0.00000001")


def get_min_qty(symbol):

    f = get_symbol_filter(
        symbol,
        "LOT_SIZE"
    )

    if f:
        return Decimal(
            str(
                f.get(
                    "minQty",
                    "0"
                )
            )
        )

    f = get_symbol_filter(
        symbol,
        "MARKET_LOT_SIZE"
    )

    if f:
        return Decimal(
            str(
                f.get(
                    "minQty",
                    "0"
                )
            )
        )

    return Decimal("0")


def get_min_notional(symbol):

    f = get_symbol_filter(
        symbol,
        "NOTIONAL"
    )

    if f:
        return Decimal(
            str(
                f.get(
                    "minNotional",
                    "0"
                )
            )
        )

    f = get_symbol_filter(
        symbol,
        "MIN_NOTIONAL"
    )

    if f:
        return Decimal(
            str(
                f.get(
                    "minNotional",
                    "0"
                )
            )
        )

    return Decimal("0")


# ============================================================
# TOP SYMBOL SELECTION
# ============================================================

def select_top_symbols():

    global symbols

    logger.info(
        "Selecting top USDT ALT symbols by 24h volume..."
    )

    try:

        tickers = client.get_ticker()

    except Exception as e:

        logger.error(
            f"Ticker request failed: {e}"
        )

        raise

    candidates = []

    for ticker in tickers:

        try:

            symbol = ticker.get("symbol")

            if symbol not in symbol_info:
                continue

            quote_volume = float(
                ticker.get(
                    "quoteVolume",
                    0
                )
            )

            if not math.isfinite(
                quote_volume
            ):
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

    symbols = [
        item[0]
        for item in candidates[
            :TOP_SYMBOLS
        ]
    ]

    logger.info(
        f"Selected {len(symbols)} symbols."
    )

    logger.info(
        "Top symbols: "
        + ", ".join(
            symbols[:30]
        )
    )


# ============================================================
# SAFE NUMERIC DATAFRAME
# ============================================================

NUMERIC_COLUMNS = [
    "open",
    "high",
    "low",
    "close",
    "volume"
]


def clean_dataframe(df):

    if df is None:
        return None

    try:

        df = df.copy()

        for col in NUMERIC_COLUMNS:

            if col not in df.columns:
                return None

            df[col] = pd.to_numeric(
                df[col],
                errors="coerce"
            )

        if "open_time" in df.columns:
            df["open_time"] = pd.to_numeric(
                df["open_time"],
                errors="coerce"
            )

        if "close_time" in df.columns:
            df["close_time"] = pd.to_numeric(
                df["close_time"],
                errors="coerce"
            )

        df = df.dropna(
            subset=NUMERIC_COLUMNS
        )

        if df.empty:
            return None

        df = df.reset_index(
            drop=True
        )

        return df

    except Exception as e:

        logger.error(
            f"DataFrame cleaning error: {e}"
        )

        return None


# ============================================================
# HISTORICAL KLINES
# ============================================================

def load_initial_history():

    logger.info(
        f"Loading {HISTORY_LIMIT} historical "
        f"{TIMEFRAME} candles for "
        f"{len(symbols)} symbols..."
    )

    loaded = 0

    for index, symbol in enumerate(
        symbols,
        start=1
    ):

        try:

            klines = client.get_klines(
                symbol=symbol,
                interval=TIMEFRAME,
                limit=HISTORY_LIMIT
            )

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

            if rows:

                df = pd.DataFrame(
                    rows
                )

                df = clean_dataframe(
                    df
                )

                if df is not None and len(df) >= 30:

                    with data_lock:
                        market_data[
                            symbol
                        ] = df

                    loaded += 1

        except BinanceAPIException as e:

            logger.error(
                f"{symbol} | Kline API error: {e}"
            )

            if getattr(
                e,
                "code",
                None
            ) == -1003:

                logger.error(
                    "Binance rate limit detected. "
                    "Stopping history loading."
                )

                break

        except Exception as e:

            logger.error(
                f"{symbol} | Kline error: {e}"
            )

        time.sleep(
            KLINE_REQUEST_DELAY
        )

        if index % 25 == 0:

            logger.info(
                f"History progress: "
                f"{index}/{len(symbols)}"
            )

    logger.info(
        f"Historical data loaded for "
        f"{loaded}/{len(symbols)} symbols."
    )


# ============================================================
# RSI
# ============================================================

def calculate_rsi(
    series,
    period=3
):

    series = pd.to_numeric(
        series,
        errors="coerce"
    )

    delta = series.diff()

    gain = delta.clip(
        lower=0
    )

    loss = -delta.clip(
        upper=0
    )

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

    rs = (
        avg_gain /
        avg_loss.replace(
            0,
            np.nan
        )
    )

    rsi = 100 - (
        100 /
        (1 + rs)
    )

    # Loss = 0 হলে RSI = 100
    rsi = rsi.where(
        avg_loss != 0,
        100
    )

    rsi = rsi.where(
        avg_gain.notna(),
        np.nan
    )

    return rsi


# ============================================================
# INDICATORS
# ============================================================

def calculate_indicators(df):

    df = clean_dataframe(
        df
    )

    if df is None:
        return None

    if len(df) < 30:
        return df

    try:

        df = df.copy()

        # -------------------------
        # BB20
        # -------------------------

        df["bb_middle"] = (
            df["close"]
            .rolling(
                BB_PERIOD,
                min_periods=BB_PERIOD
            )
            .mean()
        )

        df["bb_std"] = (
            df["close"]
            .rolling(
                BB_PERIOD,
                min_periods=BB_PERIOD
            )
            .std(
                ddof=0
            )
        )

        df["bb_upper"] = (
            df["bb_middle"]
            +
            BB_STD * df["bb_std"]
        )

        df["bb_lower"] = (
            df["bb_middle"]
            -
            BB_STD * df["bb_std"]
        )

        # -------------------------
        # RSI3
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
            .rolling(
                VOLUME_SMA_PERIOD,
                min_periods=VOLUME_SMA_PERIOD
            )
            .mean()
        )

        return df

    except Exception as e:

        logger.error(
            f"Indicator calculation error: {e}"
        )

        return None


# ============================================================
# BUY SIGNAL
# ============================================================

def buy_signal(df):

    if df is None:
        return False, None

    if len(df) < 30:
        return False, None

    df = calculate_indicators(
        df
    )

    if df is None:
        return False, None

    if len(df) < 30:
        return False, None

    # IMPORTANT:
    # process_closed_candle() only calls this
    # after Binance says the candle is closed.
    #
    # Therefore the LAST row is the latest
    # completed candle.
    row = df.iloc[-1]

    required = [
        row["close"],
        row["bb_lower"],
        row["bb_upper"],
        row["rsi3"],
        row["volume"],
        row["volume_sma20"]
    ]

    if any(
        pd.isna(x)
        for x in required
    ):
        return False, row

    try:

        close_price = float(
            row["close"]
        )

        bb_lower = float(
            row["bb_lower"]
        )

        rsi3 = float(
            row["rsi3"]
        )

        volume = float(
            row["volume"]
        )

        volume_sma20 = float(
            row["volume_sma20"]
        )

        condition_bb = (
            close_price < bb_lower
        )

        condition_rsi = (
            rsi3 < 10
        )

        condition_volume = (
            volume >
            volume_sma20 *
            VOLUME_MULTIPLIER
        )

        signal = (
            condition_bb
            and condition_rsi
            and condition_volume
        )

        return signal, row

    except Exception as e:

        logger.error(
            f"Buy signal calculation error: {e}"
        )

        return False, row


# ============================================================
# LIVE PRICE
# ============================================================

def set_live_price(
    symbol,
    price
):

    try:

        price = float(price)

        if not math.isfinite(
            price
        ):
            return

        if price <= 0:
            return

        with price_lock:

            live_prices[
                symbol
            ] = price

    except Exception:
        pass


def get_live_price(symbol):

    with price_lock:

        return live_prices.get(
            symbol
        )


# ============================================================
# BUY
# ============================================================

def execute_buy(
    symbol,
    signal_row
):

    now = time.time()

    last_time = last_buy_time.get(
        symbol,
        0
    )

    if (
        now - last_time
        <
        BUY_COOLDOWN_SECONDS
    ):
        return

    with positions_lock:

        if symbol in positions:
            return

        if (
            MAX_POSITIONS is not None
            and
            len(positions)
            >= MAX_POSITIONS
        ):

            logger.info(
                f"{symbol} | "
                f"MAX_POSITIONS reached."
            )

            return

    price = get_live_price(
        symbol
    )

    if not price:

        logger.warning(
            f"{symbol} | "
            f"No live price available."
        )

        return

    try:

        logger.info(
            f"{symbol} | BUY SIGNAL | "
            f"Close={float(signal_row['close']):.8f} | "
            f"BBLower={float(signal_row['bb_lower']):.8f} | "
            f"RSI3={float(signal_row['rsi3']):.2f} | "
            f"Volume={float(signal_row['volume']):.2f} | "
            f"VolSMA20={float(signal_row['volume_sma20']):.2f}"
        )

        logger.info(
            f"{symbol} | "
            f"Sending MARKET BUY | "
            f"USDT={TRADE_AMOUNT_USDT}"
        )

        order = client.create_order(
            symbol=symbol,
            side=Client.SIDE_BUY,
            type=Client.ORDER_TYPE_MARKET,
            quoteOrderQty=decimal_to_str(
                Decimal(
                    str(
                        TRADE_AMOUNT_USDT
                    )
                )
            ),
            newOrderRespType="FULL"
        )

        executed_qty = Decimal("0")
        executed_quote = Decimal("0")

        fills = order.get(
            "fills",
            []
        )

        for fill in fills:

            qty = Decimal(
                str(
                    fill.get(
                        "qty",
                        "0"
                    )
                )
            )

            fill_price = Decimal(
                str(
                    fill.get(
                        "price",
                        "0"
                    )
                )
            )

            executed_qty += qty

            executed_quote += (
                qty * fill_price
            )

        if executed_qty <= 0:

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
                f"{symbol} | "
                f"BUY executed but "
                f"quantity unavailable."
            )

            return

        if executed_quote > 0:

            entry_price = (
                executed_quote /
                executed_qty
            )

        else:

            entry_price = Decimal(
                str(price)
            )

        if entry_price <= 0:
            return

        # Entry price precision
        entry_price = entry_price.quantize(
            Decimal("0.00000001")
        )

        # 1% stop
        stop_price = (
            entry_price *
            Decimal("0.99")
        )

        stop_price = stop_price.quantize(
            Decimal("0.00000001")
        )

        last_buy_time[
            symbol
        ] = now

        position = {
            "symbol": symbol,
            "entry_price": float(
                entry_price
            ),
            "quantity": float(
                executed_qty
            ),
            "stop_price": float(
                stop_price
            ),
            "buy_time": now,

            # Upper BB of signal candle
            "signal_upper_bb": float(
                signal_row[
                    "bb_upper"
                ]
            ),

            "selling": False
        }

        with positions_lock:

            positions[
                symbol
            ] = position

        logger.info(
            f"{symbol} | BUY FILLED | "
            f"Entry={entry_price} | "
            f"Qty={executed_qty} | "
            f"SL={stop_price} | "
            f"UpperBB={float(signal_row['bb_upper']):.8f}"
        )

    except BinanceAPIException as e:

        logger.error(
            f"{symbol} | "
            f"BUY Binance API error: {e}"
        )

    except BinanceOrderException as e:

        logger.error(
            f"{symbol} | "
            f"BUY order error: {e}"
        )

    except Exception as e:

        logger.exception(
            f"{symbol} | "
            f"BUY unexpected error: {e}"
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

        position = positions.get(
            symbol
        )

        if not position:
            return

        if position.get(
            "selling"
        ):
            return

        position[
            "selling"
        ] = True

        position_copy = dict(
            position
        )

    try:

        base_asset = symbol_info[
            symbol
        ][
            "base_asset"
        ]

        balance = client.get_asset_balance(
            asset=base_asset
        )

        free_balance = Decimal(
            str(
                balance.get(
                    "free",
                    "0"
                )
                if balance
                else "0"
            )
        )

        if free_balance <= 0:

            logger.error(
                f"{symbol} | "
                f"No free balance available."
            )

            with positions_lock:
                positions.pop(
                    symbol,
                    None
                )

            return

        quantity = (
            free_balance *
            SELL_BALANCE_BUFFER
        )

        step_size = get_step_size(
            symbol
        )

        quantity = floor_to_step(
            quantity,
            step_size
        )

        min_qty = get_min_qty(
            symbol
        )

        if quantity < min_qty:

            logger.error(
                f"{symbol} | "
                f"Sell quantity below "
                f"minimum. "
                f"Qty={quantity} "
                f"MinQty={min_qty}"
            )

            with positions_lock:
                positions.pop(
                    symbol,
                    None
                )

            return

        logger.info(
            f"{symbol} | SELL | "
            f"Reason={reason} | "
            f"Price={live_price}"
        )

        order = client.create_order(
            symbol=symbol,
            side=Client.SIDE_SELL,
            type=Client.ORDER_TYPE_MARKET,
            quantity=decimal_to_str(
                quantity
            ),
            newOrderRespType="FULL"
        )

        # ----------------------------------
        # Calculate actual average exit
        # ----------------------------------

        exit_qty = Decimal("0")
        exit_quote = Decimal("0")

        fills = order.get(
            "fills",
            []
        )

        for fill in fills:

            qty = Decimal(
                str(
                    fill.get(
                        "qty",
                        "0"
                    )
                )
            )

            fill_price = Decimal(
                str(
                    fill.get(
                        "price",
                        "0"
                    )
                )
            )

            exit_qty += qty

            exit_quote += (
                qty * fill_price
            )

        if (
            exit_qty > 0
            and
            exit_quote > 0
        ):

            exit_price = (
                exit_quote /
                exit_qty
            )

        elif live_price:

            exit_price = Decimal(
                str(live_price)
            )

        else:

            exit_price = Decimal(
                str(
                    position_copy[
                        "entry_price"
                    ]
                )
            )

        entry_price = Decimal(
            str(
                position_copy[
                    "entry_price"
                ]
            )
        )

        pnl_pct = (
            (
                exit_price -
                entry_price
            )
            /
            entry_price
        ) * Decimal("100")

        logger.info(
            f"{symbol} | SELL FILLED | "
            f"Entry={entry_price} | "
            f"Exit≈{exit_price:.8f} | "
            f"P/L≈{pnl_pct:.3f}% | "
            f"Reason={reason}"
        )

        with positions_lock:

            positions.pop(
                symbol,
                None
            )

    except BinanceAPIException as e:

        logger.error(
            f"{symbol} | "
            f"SELL Binance API error: {e}"
        )

        with positions_lock:

            if symbol in positions:
                positions[
                    symbol
                ][
                    "selling"
                ] = False

    except BinanceOrderException as e:

        logger.error(
            f"{symbol} | "
            f"SELL order error: {e}"
        )

        with positions_lock:

            if symbol in positions:
                positions[
                    symbol
                ][
                    "selling"
                ] = False

    except Exception as e:

        logger.exception(
            f"{symbol} | "
            f"SELL unexpected error: {e}"
        )

        with positions_lock:

            if symbol in positions:
                positions[
                    symbol
                ][
                    "selling"
                ] = False


# ============================================================
# POSITION CHECK
# ============================================================

def check_position(
    symbol
):

    with positions_lock:

        position = positions.get(
            symbol
        )

        if not position:
            return

        if position.get(
            "selling"
        ):
            return

        position_copy = dict(
            position
        )

    price = get_live_price(
        symbol
    )

    if price is None:
        return

    try:

        entry_price = float(
            position_copy[
                "entry_price"
            ]
        )

        stop_price = float(
            position_copy[
                "stop_price"
            ]
        )

        upper_bb = float(
            position_copy[
                "signal_upper_bb"
            ]
        )

        # --------------------------------
        # STOP LOSS 1%
        # --------------------------------

        if price <= stop_price:

            execute_sell(
                symbol,
                reason=(
                    f"STOP LOSS "
                    f"{STOP_LOSS_PCT * 100:.2f}%"
                ),
                live_price=price
            )

            return

        # --------------------------------
        # UPPER BB EXIT
        # --------------------------------

        if SELL_AT_UPPER_BB:

            if (
                math.isfinite(
                    upper_bb
                )
                and
                price >= upper_bb
            ):

                execute_sell(
                    symbol,
                    reason="UPPER BB TOUCH",
                    live_price=price
                )

                return

    except Exception as e:

        logger.error(
            f"{symbol} | "
            f"Position check error: {e}"
        )


# ============================================================
# PROCESS CLOSED CANDLE
# ============================================================

def process_closed_candle(
    symbol
):

    try:

        with data_lock:

            df = market_data.get(
                symbol
            )

            if df is None:
                return

            if len(df) < 30:
                return

            df_copy = df.copy()

        df_copy = clean_dataframe(
            df_copy
        )

        if df_copy is None:
            return

        if len(df_copy) < 30:
            return

        # Latest row is the candle that Binance
        # just marked as CLOSED.
        candle = df_copy.iloc[-1]

        candle_time = int(
            candle[
                "open_time"
            ]
        )

        previous = last_processed_candle.get(
            symbol
        )

        if previous == candle_time:
            return

        last_processed_candle[
            symbol
        ] = candle_time

        signal, row = buy_signal(
            df_copy
        )

        if not signal:
            return

        execute_buy(
            symbol,
            row
        )

    except Exception as e:

        logger.exception(
            f"{symbol} | "
            f"Candle processing error: {e}"
        )


# ============================================================
# KLINE MESSAGE
# ============================================================

def process_kline_message(
    data
):

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

        # ----------------------------------
        # Validate numeric data
        # ----------------------------------

        values = [
            open_price,
            high_price,
            low_price,
            close_price,
            volume
        ]

        if not all(
            math.isfinite(x)
            for x in values
        ):
            return

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

            # ----------------------------------
            # Update existing candle
            # ----------------------------------

            if (
                len(df) > 0
                and
                int(
                    df.iloc[-1][
                        "open_time"
                    ]
                )
                ==
                open_time
            ):

                idx = df.index[-1]

                df.loc[
                    idx,
                    "open"
                ] = open_price

                df.loc[
                    idx,
                    "high"
                ] = high_price

                df.loc[
                    idx,
                    "low"
                ] = low_price

                df.loc[
                    idx,
                    "close"
                ] = close_price

                df.loc[
                    idx,
                    "volume"
                ] = volume

                df.loc[
                    idx,
                    "close_time"
                ] = close_time

            else:

                new_row = pd.DataFrame([
                    {
                        "open_time": open_time,
                        "open": open_price,
                        "high": high_price,
                        "low": low_price,
                        "close": close_price,
                        "volume": volume,
                        "close_time": close_time
                    }
                ])

                df = pd.concat(
                    [
                        df,
                        new_row
                    ],
                    ignore_index=True
                )

            # ----------------------------------
            # Keep only latest 150 candles
            # ----------------------------------

            if len(df) > 150:

                df = df.iloc[
                    -150:
                ].reset_index(
                    drop=True
                )

            # ----------------------------------
            # IMPORTANT:
            # Keep numeric columns numeric.
            # ----------------------------------

            for col in NUMERIC_COLUMNS:

                df[col] = pd.to_numeric(
                    df[col],
                    errors="coerce"
                )

            df = df.dropna(
                subset=NUMERIC_COLUMNS
            ).reset_index(
                drop=True
            )

            market_data[
                symbol
            ] = df

        # ----------------------------------
        # Only evaluate BUY after candle close
        # ----------------------------------

        if candle_closed:

            process_closed_candle(
                symbol
            )

    except Exception as e:

        logger.exception(
            f"{data.get('s', 'UNKNOWN')} | "
            f"Kline processing error: {e}"
        )


# ============================================================
# WEBSOCKET MESSAGE
# ============================================================

def process_ws_message(
    raw_message
):

    global ws_last_message_time

    ws_last_message_time = time.time()

    try:

        message = json.loads(
            raw_message
        )

        data = message.get(
            "data",
            message
        )

        event_type = data.get(
            "e"
        )

        # ----------------------------------
        # KLINE
        # ----------------------------------

        if event_type == "kline":

            process_kline_message(
                data
            )

            return

        # ----------------------------------
        # MINI TICKER
        # ----------------------------------

        if event_type in (
            "24hrMiniTicker",
            "24hrTicker"
        ):

            symbol = data.get(
                "s"
            )

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

                # Only position symbols
                # are checked.
                with positions_lock:
                    holding = (
                        symbol
                        in positions
                    )

                if holding:

                    check_position(
                        symbol
                    )

            return

    except json.JSONDecodeError:
        return

    except Exception as e:

        logger.exception(
            "WebSocket message processing error: "
            f"{e}"
        )


# ============================================================
# WEBSOCKET CHUNKS
# ============================================================

def chunk_symbols(
    items,
    size
):

    return [
        items[i:i + size]
        for i in range(
            0,
            len(items),
            size
        )
    ]


def make_stream_url(
    symbol_chunk
):

    streams = []

    for symbol in symbol_chunk:

        s = symbol.lower()

        streams.append(
            f"{s}@kline_5m"
        )

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
# SINGLE WEBSOCKET WORKER
# ============================================================

def websocket_worker(
    connection_id,
    symbol_chunk
):

    global ws_connected_count

    while True:

        ws = None

        try:

            if not symbol_chunk:

                time.sleep(
                    WS_RECONNECT_DELAY
                )

                continue

            url = make_stream_url(
                symbol_chunk
            )

            logger.info(
                f"WS-{connection_id} | "
                f"Connecting | "
                f"Symbols={len(symbol_chunk)} | "
                f"Streams={len(symbol_chunk) * 2}"
            )

            def on_open(
                ws_app
            ):

                global ws_connected_count

                with ws_lock:
                    ws_connected_count += 1

                logger.info(
                    f"WS-{connection_id} | "
                    f"CONNECTED"
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
                    f"WS-{connection_id} | "
                    f"ERROR | {error}"
                )

            def on_close(
                ws_app,
                close_status_code,
                close_msg
            ):

                global ws_connected_count

                with ws_lock:

                    if ws_connected_count > 0:
                        ws_connected_count -= 1

                logger.warning(
                    f"WS-{connection_id} | "
                    f"CLOSED | "
                    f"code={close_status_code} | "
                    f"msg={close_msg}"
                )

            def on_ping(
                ws_app,
                message
            ):

                logger.debug(
                    f"WS-{connection_id} | "
                    f"PING received"
                )

            ws = websocket.WebSocketApp(
                url,
                on_open=on_open,
                on_message=on_message,
                on_error=on_error,
                on_close=on_close,
                on_ping=on_ping
            )

            # Controlled heartbeat.
            #
            # websocket-client sends PING frames
            # periodically and waits for PONG.
            ws.run_forever(
                ping_interval=20,
                ping_timeout=10,
                ping_payload="render"
            )

        except Exception as e:

            logger.exception(
                f"WS-{connection_id} | "
                f"Worker exception: {e}"
            )

        finally:

            # Safety:
            # if connection disappeared without
            # on_close updating the counter.
            with ws_lock:

                if ws is not None:
                    pass

        logger.warning(
            f"WS-{connection_id} | "
            f"Reconnecting in "
            f"{WS_RECONNECT_DELAY}s..."
        )

        time.sleep(
            WS_RECONNECT_DELAY
        )


# ============================================================
# START ALL WEBSOCKETS
# ============================================================

def start_websocket_threads():

    global ws_total_connections

    chunks = chunk_symbols(
        symbols,
        WS_SYMBOLS_PER_CONNECTION
    )

    ws_total_connections = len(
        chunks
    )

    logger.info(
        f"Starting {len(chunks)} "
        f"WebSocket connections."
    )

    for index, chunk in enumerate(
        chunks,
        start=1
    ):

        thread = threading.Thread(
            target=websocket_worker,
            args=(
                index,
                chunk
            ),
            daemon=True
        )

        thread.start()

        logger.info(
            f"WS-{index} started | "
            f"{len(chunk)} symbols"
        )

        # Small gap between connections
        time.sleep(1)


# ============================================================
# POSITION RECOVERY
# ============================================================

def recover_positions():

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

            total = (
                free +
                locked
            )

            if total <= 0:
                continue

            if asset in STABLECOINS:
                continue

            if asset in EXCLUDED_BASE_ASSETS:
                continue

            symbol = (
                asset +
                "USDT"
            )

            if symbol not in symbols:
                continue

            # Only recover meaningful balances.
            ticker = client.get_symbol_ticker(
                symbol=symbol
            )

            current_price = float(
                ticker[
                    "price"
                ]
            )

            if current_price <= 0:
                continue

            # Ignore very small dust.
            min_notional = get_min_notional(
                symbol
            )

            if min_notional > 0:

                estimated_value = (
                    total *
                    Decimal(
                        str(
                            current_price
                        )
                    )
                )

                if (
                    estimated_value
                    <
                    min_notional
                ):
                    continue

            # IMPORTANT:
            # Exact original entry price is unavailable
            # from balance alone.
            #
            # So current price is used as restart reference.

            entry_price = current_price

            stop_price = (
                entry_price *
                (
                    1 -
                    STOP_LOSS_PCT
                )
            )

            with positions_lock:

                positions[
                    symbol
                ] = {
                    "symbol": symbol,
                    "entry_price": entry_price,
                    "quantity": float(free),
                    "stop_price": stop_price,
                    "buy_time": time.time(),

                    # No known signal BB after restart.
                    "signal_upper_bb": float(
                        "inf"
                    ),

                    "selling": False,
                    "recovered": True
                }

            set_live_price(
                symbol,
                current_price
            )

            recovered += 1

            logger.warning(
                f"{symbol} | "
                f"Existing balance recovered | "
                f"Reference price="
                f"{current_price:.8f} | "
                f"SL={stop_price:.8f}"
            )

        except Exception:
            continue

    logger.info(
        f"Recovered {recovered} "
        f"existing positions."
    )


# ============================================================
# INITIALIZE BOT
# ============================================================

def initialize_bot():

    global client
    global bot_started

    logger.info("=" * 70)

    logger.info(
        "STARTING BINANCE "
        "BB20 + RSI3 + VOLUME BOT"
    )

    logger.info("=" * 70)

    logger.info(
        f"TRADE_AMOUNT_USDT = "
        f"{TRADE_AMOUNT_USDT}"
    )

    logger.info(
        "BUY = CLOSE < BB20 LOWER "
        "AND RSI3 < 10 "
        "AND VOLUME > SMA20 × 1.20"
    )

    logger.info(
        "SELL = UPPER BB TOUCH "
        "OR STOP LOSS 1%"
    )

    logger.info(
        f"TIMEFRAME = {TIMEFRAME}"
    )

    logger.info(
        f"TOP SYMBOLS = {TOP_SYMBOLS}"
    )

    logger.info(
        f"WS SYMBOLS PER CONNECTION = "
        f"{WS_SYMBOLS_PER_CONNECTION}"
    )

    logger.info("=" * 70)

    # ----------------------------------
    # Binance client
    # ----------------------------------

    client = create_binance_client()

    # ----------------------------------
    # Exchange information
    # ----------------------------------

    load_exchange_info()

    # ----------------------------------
    # Select symbols
    # ----------------------------------

    select_top_symbols()

    if not symbols:

        raise RuntimeError(
            "No symbols selected."
        )

    # ----------------------------------
    # Historical candles
    # ----------------------------------

    load_initial_history()

    # ----------------------------------
    # Recover balances
    # ----------------------------------

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

    # ----------------------------------
    # Start WebSockets
    # ----------------------------------

    start_websocket_threads()

    # ----------------------------------
    # Flask
    # ----------------------------------

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


# ============================================================
# ENTRY POINT
# ============================================================

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
