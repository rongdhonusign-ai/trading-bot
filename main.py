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


TRADE_AMOUNT_USDT = 35.0

TIMEFRAME = "5m"

BB_PERIOD = 20
BB_STD = 2.0

EMA_PERIOD = 5
SMA_PERIOD = 20

# 1% STOP LOSS
STOP_LOSS_PCT = 0.010

# Top ALT/USDT pairs
TOP_SYMBOLS = 150

# Number of symbols in one WebSocket connection.
# 50 symbols = 100 streams because each symbol uses:
# kline + miniTicker
WS_BATCH_SIZE = 50

# Safety margin for SELL quantity
SELL_BALANCE_BUFFER = 0.999

# Refresh top symbols every 30 minutes
SYMBOL_REFRESH_SECONDS = 1800

# Safety monitor interval
SAFETY_CHECK_SECONDS = 3

# BUY cooldown per symbol
BUY_COOLDOWN_SECONDS = 60


# ============================================================
# STABLECOINS / FIAT
# ============================================================

STABLECOINS = {
    "USDT",
    "USDC",
    "FDUSD",
    "BUSD",
    "TUSD",
    "DAI",
    "USDP",
    "PYUSD",
    "USDE",
    "USDS",

    "EUR",
    "GBP",
    "TRY",
    "BRL",
    "ARS",
    "UAH",
    "RUB",
    "BIDR",
    "IDRT",
    "NGN",
    "PLN",
    "RON",
    "ZAR",
    "AED",
    "AUD",
    "JPY",
    "SEK",
    "DKK",
    "NOK",
    "CHF"
}


# ============================================================
# EXCLUDED BASE ASSETS
# ============================================================

EXCLUDED_BASES = {
    "BTC",
    "ETH"
}


# ============================================================
# LOGGING
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

log = logging.getLogger("BB_BOT")


# ============================================================
# BINANCE CLIENT
# ============================================================

client = Client(
    API_KEY,
    API_SECRET
)


# ============================================================
# FLASK APP
# ============================================================

app = Flask(__name__)


@app.route("/")
def home():

    return jsonify({
        "status": "running",
        "bot": "BB20 EMA5 Spot Bot",
        "timeframe": TIMEFRAME,
        "trade_amount": TRADE_AMOUNT_USDT,
        "stop_loss": f"{STOP_LOSS_PCT * 100:.2f}%",
        "top_symbols": TOP_SYMBOLS
    })


@app.route("/health")
def health():

    with state_lock:

        active_positions = len(positions)
        monitored_symbols = len(top_symbols)

    return jsonify({
        "status": "healthy",
        "timestamp": int(time.time()),
        "positions": active_positions,
        "monitored_symbols": monitored_symbols
    })


# ============================================================
# GLOBAL STATE
# ============================================================

symbol_info = {}

top_symbols = []

candles = {}

positions = {}

# Symbols currently being sold
selling_symbols = set()

# Last BUY time per symbol
last_buy_time = {}

# WebSocket stop event
ws_stop_event = threading.Event()

# WebSocket objects
ws_connections = []

# Global lock
state_lock = threading.RLock()

# Bot startup protection
bot_started = False
bot_start_lock = threading.Lock()

last_top_symbol_update = 0


# ============================================================
# LOAD EXCHANGE INFORMATION
# ============================================================

def load_exchange_info():

    global symbol_info

    log.info(
        "Loading Binance exchange information..."
    )

    info = client.get_exchange_info()

    temp = {}

    for s in info.get("symbols", []):

        try:

            symbol = s["symbol"]

            if s.get("status") != "TRADING":
                continue

            if s.get("quoteAsset") != "USDT":
                continue

            base = s.get("baseAsset")

            if not base:
                continue

            if base in STABLECOINS:
                continue

            if base in EXCLUDED_BASES:
                continue

            # Some Binance exchange responses may not contain
            # this field, so only reject when explicitly False.
            if s.get("isSpotTradingAllowed") is False:
                continue

            filters = {
                f["filterType"]: f
                for f in s.get("filters", [])
            }

            lot_filter = filters.get("LOT_SIZE")
            market_lot_filter = filters.get("MARKET_LOT_SIZE")
            price_filter = filters.get("PRICE_FILTER")

            min_notional_filter = filters.get("MIN_NOTIONAL")
            notional_filter = filters.get("NOTIONAL")

            # ------------------------------------------------
            # LOT SIZE
            # ------------------------------------------------

            step_size = 0.000001
            min_qty = 0.0
            max_qty = 0.0

            if lot_filter:

                step_size = float(
                    lot_filter.get(
                        "stepSize",
                        0
                    )
                )

                min_qty = float(
                    lot_filter.get(
                        "minQty",
                        0
                    )
                )

                max_qty = float(
                    lot_filter.get(
                        "maxQty",
                        0
                    )
                )

            # ------------------------------------------------
            # MARKET LOT SIZE
            # ------------------------------------------------

            market_step_size = 0.0
            market_min_qty = 0.0
            market_max_qty = 0.0

            if market_lot_filter:

                market_step_size = float(
                    market_lot_filter.get(
                        "stepSize",
                        0
                    )
                )

                market_min_qty = float(
                    market_lot_filter.get(
                        "minQty",
                        0
                    )
                )

                market_max_qty = float(
                    market_lot_filter.get(
                        "maxQty",
                        0
                    )
                )

            # ------------------------------------------------
            # MIN NOTIONAL
            # ------------------------------------------------

            min_notional = 0.0

            if min_notional_filter:

                min_notional = float(
                    min_notional_filter.get(
                        "minNotional",
                        0
                    )
                )

            if notional_filter:

                min_notional = max(
                    min_notional,
                    float(
                        notional_filter.get(
                            "minNotional",
                            0
                        )
                    )
                )

            # ------------------------------------------------
            # TICK SIZE
            # ------------------------------------------------

            tick_size = 0.000001

            if price_filter:

                tick_size = float(
                    price_filter.get(
                        "tickSize",
                        0
                    )
                )

            temp[symbol] = {

                "base": base,

                "quote": "USDT",

                "step_size": step_size,

                "min_qty": min_qty,

                "max_qty": max_qty,

                "market_step_size": market_step_size,

                "market_min_qty": market_min_qty,

                "market_max_qty": market_max_qty,

                "tick_size": tick_size,

                "min_notional": min_notional
            }

        except Exception as e:

            log.warning(
                "Exchange symbol parse error: %s",
                e
            )

    symbol_info = temp

    log.info(
        "Loaded %s eligible USDT ALT symbols",
        len(symbol_info)
    )


# ============================================================
# TOP SYMBOLS
# ============================================================

def update_top_symbols():

    global top_symbols
    global last_top_symbol_update

    try:

        log.info(
            "Updating top ALT symbols..."
        )

        tickers = client.get_ticker()

        candidates = []

        for t in tickers:

            try:

                symbol = t.get("symbol")

                if symbol not in symbol_info:
                    continue

                quote_volume = float(
                    t.get(
                        "quoteVolume",
                        0
                    )
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

        selected = [
            symbol
            for symbol, volume
            in candidates[:TOP_SYMBOLS]
        ]

        with state_lock:

            top_symbols = selected

        last_top_symbol_update = time.time()

        log.info(
            "Selected %s ALT/USDT symbols",
            len(selected)
        )

        log.info(
            "First symbols: %s",
            selected[:15]
        )

    except Exception as e:

        log.exception(
            "Top symbol update failed: %s",
            e
        )


# ============================================================
# DECIMAL QUANTITY ROUNDING
# ============================================================

def round_step_quantity(
    quantity,
    step_size
):

    try:

        if quantity <= 0:
            return 0.0

        if step_size <= 0:
            return quantity

        qty = Decimal(
            str(quantity)
        )

        step = Decimal(
            str(step_size)
        )

        rounded = (
            qty / step
        ).to_integral_value(
            rounding=ROUND_DOWN
        ) * step

        return float(rounded)

    except Exception:

        return 0.0


# ============================================================
# VALID MARKET SELL QUANTITY
# ============================================================

def get_valid_sell_quantity(
    symbol,
    free_balance
):

    info = symbol_info.get(symbol)

    if not info:
        return 0.0

    quantity = (
        free_balance *
        SELL_BALANCE_BUFFER
    )

    # --------------------------------------------------------
    # MARKET LOT SIZE
    # --------------------------------------------------------

    market_step = info.get(
        "market_step_size",
        0
    )

    market_min = info.get(
        "market_min_qty",
        0
    )

    market_max = info.get(
        "market_max_qty",
        0
    )

    if market_step > 0:

        quantity = round_step_quantity(
            quantity,
            market_step
        )

        if market_min > 0:

            if quantity < market_min:
                return 0.0

        if market_max > 0:

            quantity = min(
                quantity,
                market_max
            )

    # --------------------------------------------------------
    # LOT SIZE
    # --------------------------------------------------------

    lot_step = info.get(
        "step_size",
        0
    )

    lot_min = info.get(
        "min_qty",
        0
    )

    lot_max = info.get(
        "max_qty",
        0
    )

    if lot_step > 0:

        quantity = round_step_quantity(
            quantity,
            lot_step
        )

        if lot_min > 0:

            if quantity < lot_min:
                return 0.0

        if lot_max > 0:

            quantity = min(
                quantity,
                lot_max
            )

    return quantity


# ============================================================
# CLEAN NUMERIC DATAFRAME
# ============================================================

def clean_ohlcv_dataframe(df):

    if df is None or len(df) == 0:
        return None

    df = df.copy()

    numeric_columns = [
        "open",
        "high",
        "low",
        "close",
        "volume"
    ]

    for column in numeric_columns:

        if column in df.columns:

            df[column] = pd.to_numeric(
                df[column],
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
        subset=numeric_columns
    )

    df = df.reset_index(
        drop=True
    )

    return df


# ============================================================
# CALCULATE INDICATORS
# ============================================================

def calculate_indicators(df):

    if df is None:
        return None

    df = clean_ohlcv_dataframe(df)

    if df is None:
        return None

    if len(df) < BB_PERIOD + 5:
        return None

    try:

        close = pd.to_numeric(
            df["close"],
            errors="coerce"
        )

        # ----------------------------------------------------
        # BB20
        # ----------------------------------------------------

        middle = close.rolling(
            window=BB_PERIOD,
            min_periods=BB_PERIOD
        ).mean()

        std = close.rolling(
            window=BB_PERIOD,
            min_periods=BB_PERIOD
        ).std(
            ddof=0
        )

        upper = (
            middle +
            (BB_STD * std)
        )

        lower = (
            middle -
            (BB_STD * std)
        )

        # ----------------------------------------------------
        # EMA5
        # ----------------------------------------------------

        ema5 = close.ewm(
            span=EMA_PERIOD,
            adjust=False,
            min_periods=EMA_PERIOD
        ).mean()

        # ----------------------------------------------------
        # SMA20
        # ----------------------------------------------------

        sma20 = close.rolling(
            window=SMA_PERIOD,
            min_periods=SMA_PERIOD
        ).mean()

        df["bb_middle"] = middle
        df["bb_upper"] = upper
        df["bb_lower"] = lower

        df["ema5"] = ema5

        df["sma20"] = sma20

        return df

    except Exception as e:

        log.warning(
            "Indicator calculation failed: %s",
            e
        )

        return None


# ============================================================
# ENTRY SIGNAL
# ============================================================

def entry_signal(df):

    if df is None:
        return False

    if len(df) < BB_PERIOD + 2:
        return False

    try:

        # Last row is always the latest CLOSED candle
        candle = df.iloc[-1]

        required = [
            "bb_lower",
            "bb_upper",
            "ema5"
        ]

        for column in required:

            if column not in candle.index:
                return False

            if pd.isna(candle[column]):
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

        lower_bb = float(
            candle["bb_lower"]
        )

        ema5 = float(
            candle["ema5"]
        )

        # ----------------------------------------------------
        # STRATEGY
        # ----------------------------------------------------

        # 1. Candle OPEN below Lower BB
        condition_1 = (
            candle_open < lower_bb
        )

        # 2. Candle CLOSE back above Lower BB
        condition_2 = (
            candle_close > lower_bb
        )

        # 3. Candle CLOSE below EMA5
        condition_3 = (
            candle_close < ema5
        )

        # 4. Candle HIGH must remain below EMA5
        condition_4 = (
            candle_high < ema5
        )

        signal = (
            condition_1
            and condition_2
            and condition_3
            and condition_4
        )

        if signal:

            log.info(
                "ENTRY SIGNAL | open=%.12f | lowBB=%.12f | close=%.12f | EMA5=%.12f",
                candle_open,
                lower_bb,
                candle_close,
                ema5
            )

        return signal

    except Exception as e:

        log.warning(
            "Entry signal error: %s",
            e
        )

        return False


# ============================================================
# INITIAL CANDLES
# ============================================================

def load_initial_candles():

    global candles

    with state_lock:

        symbols = list(
            top_symbols
        )

    log.info(
        "Loading initial 5m candles for %s symbols...",
        len(symbols)
    )

    loaded = 0

    for index, symbol in enumerate(symbols):

        try:

            klines = client.get_klines(
                symbol=symbol,
                interval=Client.KLINE_INTERVAL_5MINUTE,
                limit=60
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

            if not rows:
                continue

            df = pd.DataFrame(rows)

            # ------------------------------------------------
            # REMOVE CURRENTLY OPEN CANDLE
            # ------------------------------------------------

            current_ms = int(
                time.time() * 1000
            )

            df = df[
                df["close_time"] <= current_ms
            ].copy()

            df = calculate_indicators(
                df
            )

            if df is not None:

                with state_lock:

                    candles[symbol] = df

                loaded += 1

        except BinanceAPIException as e:

            log.warning(
                "Kline Binance error %s: %s",
                symbol,
                e
            )

            time.sleep(
                0.15
            )

        except Exception as e:

            log.warning(
                "Initial data error %s: %s",
                symbol,
                e
            )

        # Small delay to reduce REST burst
        time.sleep(
            0.05
        )

    log.info(
        "Initial candle loading complete: %s symbols",
        loaded
    )


# ============================================================
# BUY
# ============================================================

def buy_symbol(symbol):

    # --------------------------------------------------------
    # PRE-CHECK
    # --------------------------------------------------------

    with state_lock:

        if symbol in positions:
            return

        if symbol in selling_symbols:
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

        last_buy_time[symbol] = time.time()

    try:

        log.info(
            "BUY SIGNAL -> %s | $%.2f",
            symbol,
            TRADE_AMOUNT_USDT
        )

        order = client.create_order(

            symbol=symbol,

            side=Client.SIDE_BUY,

            type=Client.ORDER_TYPE_MARKET,

            quoteOrderQty=TRADE_AMOUNT_USDT
        )

        executed_qty = float(
            order.get(
                "executedQty",
                0
            )
        )

        if executed_qty <= 0:

            log.error(
                "BUY returned zero quantity: %s",
                order
            )

            return

        # ----------------------------------------------------
        # CALCULATE ACTUAL ENTRY PRICE
        # ----------------------------------------------------

        fills = order.get(
            "fills",
            []
        )

        total_cost = 0.0

        if fills:

            for fill in fills:

                try:

                    price = float(
                        fill["price"]
                    )

                    qty = float(
                        fill["qty"]
                    )

                    total_cost += (
                        price * qty
                    )

                except Exception:

                    continue

        if total_cost > 0:

            entry_price = (
                total_cost /
                executed_qty
            )

        else:

            ticker = client.get_symbol_ticker(
                symbol=symbol
            )

            entry_price = float(
                ticker["price"]
            )

        # ----------------------------------------------------
        # VERIFY ACTUAL FREE BALANCE
        # ----------------------------------------------------

        info = symbol_info.get(
            symbol
        )

        actual_balance = executed_qty

        if info:

            try:

                balance = client.get_asset_balance(
                    asset=info["base"]
                )

                if balance:

                    free_balance = float(
                        balance["free"]
                    )

                    actual_balance = min(
                        executed_qty,
                        free_balance
                    )

            except Exception as e:

                log.warning(
                    "Post-buy balance check failed %s: %s",
                    symbol,
                    e
                )

        if actual_balance <= 0:

            log.error(
                "Actual balance is zero after BUY -> %s",
                symbol
            )

            return

        # ----------------------------------------------------
        # SAVE POSITION
        # ----------------------------------------------------

        with state_lock:

            positions[symbol] = {

                "symbol": symbol,

                "quantity": actual_balance,

                "entry_price": entry_price,

                "buy_order_id": order.get(
                    "orderId"
                ),

                "buy_time": time.time(),

                "sma20_touched": False,

                "sma20_touch_price": None,

                "recovered": False
            }

        log.info(
            "BUY FILLED -> %s | qty=%.12f | entry=%.12f | order=%s",
            symbol,
            actual_balance,
            entry_price,
            order.get("orderId")
        )

    except BinanceAPIException as e:

        log.error(
            "BUY Binance error %s: %s",
            symbol,
            e
        )

    except Exception as e:

        log.exception(
            "BUY error %s: %s",
            symbol,
            e
        )


# ============================================================
# MARKET SELL
# ============================================================

def sell_symbol(
    symbol,
    reason
):

    # --------------------------------------------------------
    # LOCK POSITION
    # --------------------------------------------------------

    with state_lock:

        position = positions.get(
            symbol
        )

        if not position:
            return

        if symbol in selling_symbols:

            log.info(
                "SELL already in progress -> %s",
                symbol
            )

            return

        selling_symbols.add(
            symbol
        )

    try:

        info = symbol_info.get(
            symbol
        )

        if not info:

            log.error(
                "Symbol info missing -> %s",
                symbol
            )

            return

        asset = info["base"]

        # ----------------------------------------------------
        # REAL BINANCE BALANCE
        # ----------------------------------------------------

        balance = client.get_asset_balance(
            asset=asset
        )

        if not balance:

            log.error(
                "Balance not found -> %s",
                asset
            )

            return

        free_balance = float(
            balance["free"]
        )

        stored_quantity = float(
            position["quantity"]
        )

        log.info(
            "SELL BALANCE -> %s | stored=%.12f | free=%.12f",
            symbol,
            stored_quantity,
            free_balance
        )

        if free_balance <= 0:

            log.error(
                "No free balance -> %s",
                symbol
            )

            return

        # ----------------------------------------------------
        # NEVER SELL MORE THAN ACTUAL BALANCE
        # ----------------------------------------------------

        quantity_source = min(
            stored_quantity,
            free_balance
        )

        quantity_source *= (
            SELL_BALANCE_BUFFER
        )

        quantity = get_valid_sell_quantity(
            symbol,
            quantity_source
        )

        if quantity <= 0:

            log.error(
                "Valid SELL quantity is zero -> %s",
                symbol
            )

            return

        # ----------------------------------------------------
        # NOTIONAL CHECK
        # ----------------------------------------------------

        try:

            ticker = client.get_symbol_ticker(
                symbol=symbol
            )

            current_price = float(
                ticker["price"]
            )

            notional = (
                current_price *
                quantity
            )

            min_notional = float(
                info.get(
                    "min_notional",
                    0
                )
            )

            if (
                min_notional > 0
                and
                notional < min_notional
            ):

                log.error(
                    "SELL notional too small -> %s | value=%.8f | minimum=%.8f",
                    symbol,
                    notional,
                    min_notional
                )

                return

        except Exception as e:

            log.warning(
                "Notional check failed %s: %s",
                symbol,
                e
            )

        # ----------------------------------------------------
        # SELL
        # ----------------------------------------------------

        log.warning(
            "SELL SIGNAL -> %s | reason=%s | qty=%.12f",
            symbol,
            reason,
            quantity
        )

        order = client.create_order(

            symbol=symbol,

            side=Client.SIDE_SELL,

            type=Client.ORDER_TYPE_MARKET,

            quantity=quantity
        )

        status = order.get(
            "status"
        )

        executed_qty = float(
            order.get(
                "executedQty",
                0
            )
        )

        order_id = order.get(
            "orderId"
        )

        log.warning(
            "SELL RESULT -> %s | status=%s | executed=%.12f | order=%s",
            symbol,
            status,
            executed_qty,
            order_id
        )

        # ----------------------------------------------------
        # FILLED
        # ----------------------------------------------------

        if status == "FILLED":

            with state_lock:

                positions.pop(
                    symbol,
                    None
                )

            log.warning(
                "POSITION CLOSED -> %s | %s",
                symbol,
                reason
            )

        # ----------------------------------------------------
        # PARTIALLY FILLED
        # ----------------------------------------------------

        elif status == "PARTIALLY_FILLED":

            remaining = max(
                0.0,
                stored_quantity -
                executed_qty
            )

            with state_lock:

                if remaining > 0:

                    if symbol in positions:

                        positions[symbol][
                            "quantity"
                        ] = remaining

                else:

                    positions.pop(
                        symbol,
                        None
                    )

            log.warning(
                "PARTIAL SELL -> %s | sold=%.12f | remaining=%.12f",
                symbol,
                executed_qty,
                remaining
            )

        else:

            log.warning(
                "SELL not filled -> %s | status=%s",
                symbol,
                status
            )

    except BinanceAPIException as e:

        log.error(
            "SELL Binance error %s: %s",
            symbol,
            e
        )

    except Exception as e:

        log.exception(
            "SELL error %s: %s",
            symbol,
            e
        )

    finally:

        with state_lock:

            selling_symbols.discard(
                symbol
            )


# ============================================================
# CHECK POSITION
# ============================================================

def check_position(
    symbol,
    current_price,
    upper_band,
    sma20
):

    with state_lock:

        position = positions.get(
            symbol
        )

        if not position:
            return

        if symbol in selling_symbols:
            return

        # Copy required values so the lock
        # is not held during REST SELL call.
        entry_price = float(
            position["entry_price"]
        )

        sma20_touched = position.get(
            "sma20_touched",
            False
        )

        touch_price = position.get(
            "sma20_touch_price"
        )

    # --------------------------------------------------------
    # STOP LOSS
    # --------------------------------------------------------

    stop_price = (
        entry_price *
        (1.0 - STOP_LOSS_PCT)
    )

    if current_price <= stop_price:

        sell_symbol(
            symbol,
            f"STOP LOSS {STOP_LOSS_PCT * 100:.2f}%"
        )

        return

    # --------------------------------------------------------
    # SMA20 TOUCH
    # --------------------------------------------------------

    if sma20 is not None:

        try:

            sma20 = float(
                sma20
            )

            if not sma20_touched:

                if current_price >= sma20:

                    with state_lock:

                        if symbol in positions:

                            positions[symbol][
                                "sma20_touched"
                            ] = True

                            positions[symbol][
                                "sma20_touch_price"
                            ] = current_price

                    log.warning(
                        "SMA20 TOUCHED -> %s | SMA20=%.12f | touch=%.12f",
                        symbol,
                        sma20,
                        current_price
                    )

                    # Do not immediately sell.
                    return

            else:

                if touch_price is not None:

                    sma20_drop_price = (
                        float(touch_price) *
                        0.99
                    )

                    if current_price <= sma20_drop_price:

                        sell_symbol(
                            symbol,
                            "SMA20 TOUCH THEN 1% DROP"
                        )

                        return

        except Exception as e:

            log.warning(
                "SMA20 check error %s: %s",
                symbol,
                e
            )

    # --------------------------------------------------------
    # UPPER BB EXIT
    # --------------------------------------------------------

    if upper_band is not None:

        try:

            if current_price >= float(
                upper_band
            ):

                sell_symbol(
                    symbol,
                    "UPPER BB TOUCH"
                )

                return

        except Exception:

            pass


# ============================================================
# WEBSOCKET URL
# ============================================================

def make_stream_url(
    symbols
):

    streams = []

    for symbol in symbols:

        streams.append(
            f"{symbol.lower()}@kline_5m"
        )

        streams.append(
            f"{symbol.lower()}@miniTicker"
        )

    stream_string = "/".join(
        streams
    )

    return (
        "wss://stream.binance.com:9443/"
        f"stream?streams={stream_string}"
    )


# ============================================================
# WEBSOCKET MESSAGE
# ============================================================

def process_ws_message(
    message
):

    try:

        msg = json.loads(
            message
        )

        data = msg.get(
            "data",
            {}
        )

        stream = msg.get(
            "stream",
            ""
        )

        if "@kline_5m" in stream:

            process_kline(
                data
            )

        elif "@miniticker" in stream.lower():

            process_ticker(
                data
            )

    except Exception as e:

        log.warning(
            "WebSocket message error: %s",
            e
        )


# ============================================================
# KLINE PROCESSING
# ============================================================

def process_kline(
    data
):

    k = data.get(
        "k"
    )

    if not k:
        return

    symbol = k.get(
        "s"
    )

    if not symbol:
        return

    # Only CLOSED candles
    candle_closed = k.get(
        "x",
        False
    )

    if not candle_closed:
        return

    try:

        row = {

            "open_time": int(k["t"]),

            "open": float(k["o"]),

            "high": float(k["h"]),

            "low": float(k["l"]),

            "close": float(k["c"]),

            "volume": float(k["v"]),

            "close_time": int(k["T"])
        }

        new_row = pd.DataFrame(
            [row]
        )

        # ----------------------------------------------------
        # GET OLD DATA
        # ----------------------------------------------------

        with state_lock:

            old_df = candles.get(
                symbol
            )

            if old_df is None:

                old_df = pd.DataFrame()

            if len(old_df) > 0:

                df = pd.concat(
                    [
                        old_df,
                        new_row
                    ],
                    ignore_index=True
                )

            else:

                df = new_row.copy()

        # ----------------------------------------------------
        # REMOVE DUPLICATE CANDLE
        # ----------------------------------------------------

        df = df.drop_duplicates(
            subset=[
                "open_time"
            ],
            keep="last"
        )

        df = df.tail(
            100
        ).reset_index(
            drop=True
        )

        # ----------------------------------------------------
        # CALCULATE INDICATORS
        # ----------------------------------------------------

        df = calculate_indicators(
            df
        )

        if df is None:
            return

        # ----------------------------------------------------
        # SAVE
        # ----------------------------------------------------

        with state_lock:

            candles[symbol] = df

            already_in_position = (
                symbol in positions
            )

        # ----------------------------------------------------
        # ENTRY
        # ----------------------------------------------------

        if not already_in_position:

            if entry_signal(df):

                buy_symbol(
                    symbol
                )

    except Exception as e:

        log.exception(
            "Kline processing error %s: %s",
            symbol,
            e
        )


# ============================================================
# TICKER PROCESSING
# ============================================================

def process_ticker(
    data
):

    symbol = data.get(
        "s"
    )

    if not symbol:
        return

    try:

        price = float(
            data["c"]
        )

    except Exception:

        return

    # --------------------------------------------------------
    # POSITION CHECK
    # --------------------------------------------------------

    with state_lock:

        if symbol not in positions:
            return

        df = candles.get(
            symbol
        )

    upper_band = None
    sma20 = None

    if df is not None and len(df) > 0:

        try:

            last = df.iloc[-1]

            if (
                "bb_upper" in last.index
                and
                not pd.isna(
                    last["bb_upper"]
                )
            ):

                upper_band = float(
                    last["bb_upper"]
                )

            if (
                "sma20" in last.index
                and
                not pd.isna(
                    last["sma20"]
                )
            ):

                sma20 = float(
                    last["sma20"]
                )

        except Exception:

            pass

    check_position(
        symbol,
        price,
        upper_band,
        sma20
    )


# ============================================================
# SINGLE WEBSOCKET WORKER
# ============================================================

def websocket_worker(
    symbols,
    worker_id
):

    while not ws_stop_event.is_set():

        try:

            if not symbols:

                time.sleep(
                    5
                )

                continue

            url = make_stream_url(
                symbols
            )

            log.info(
                "WebSocket #%s opening -> %s symbols / %s streams",
                worker_id,
                len(symbols),
                len(symbols) * 2
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

                log.error(
                    "WebSocket #%s error: %s",
                    worker_id,
                    error
                )

            def on_close(
                ws,
                code,
                msg
            ):

                log.warning(
                    "WebSocket #%s closed: %s %s",
                    worker_id,
                    code,
                    msg
                )

            def on_open(
                ws
            ):

                log.info(
                    "WebSocket #%s connected",
                    worker_id
                )

            ws = websocket.WebSocketApp(

                url,

                on_open=on_open,

                on_message=on_message,

                on_error=on_error,

                on_close=on_close
            )

            with state_lock:

                ws_connections.append(
                    ws
                )

            ws.run_forever(

                ping_interval=30,

                ping_timeout=20,

                ping_payload="ping",

                skip_utf8_validation=True
            )

            with state_lock:

                if ws in ws_connections:

                    ws_connections.remove(
                        ws
                    )

        except Exception as e:

            log.exception(
                "WebSocket #%s loop error: %s",
                worker_id,
                e
            )

        if not ws_stop_event.is_set():

            log.info(
                "WebSocket #%s reconnecting in 5 seconds...",
                worker_id
            )

            time.sleep(
                5
            )


# ============================================================
# START WEBSOCKET CONNECTIONS
# ============================================================

def start_websocket_connections():

    with state_lock:

        symbols = list(
            top_symbols
        )

    if not symbols:

        log.warning(
            "No symbols available for WebSocket"
        )

        return

    # --------------------------------------------------------
    # Split symbols into small batches
    # --------------------------------------------------------

    batches = [

        symbols[i:i + WS_BATCH_SIZE]

        for i in range(
            0,
            len(symbols),
            WS_BATCH_SIZE
        )
    ]

    log.info(
        "Starting %s WebSocket connections",
        len(batches)
    )

    for index, batch in enumerate(
        batches,
        start=1
    ):

        thread = threading.Thread(

            target=websocket_worker,

            args=(
                batch,
                index
            ),

            daemon=True,

            name=f"BinanceWS-{index}"
        )

        thread.start()


# ============================================================
# POSITION RECOVERY
# ============================================================

def recover_positions():

    log.info(
        "Checking existing Binance balances..."
    )

    try:

        account = client.get_account()

        balances = account.get(
            "balances",
            []
        )

        recovered = 0

        with state_lock:

            monitored_symbols = set(
                top_symbols
            )

        for b in balances:

            try:

                asset = b["asset"]

                if asset in STABLECOINS:
                    continue

                free = float(
                    b["free"]
                )

                locked = float(
                    b["locked"]
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

                if symbol not in monitored_symbols:
                    continue

                # Only FREE balance can be sold
                if free <= 0:
                    continue

                # ------------------------------------------------
                # Current price
                # ------------------------------------------------

                try:

                    ticker = client.get_symbol_ticker(
                        symbol=symbol
                    )

                    current_price = float(
                        ticker["price"]
                    )

                except Exception:

                    continue

                value = (
                    free *
                    current_price
                )

                # Ignore dust
                if value < 5.0:
                    continue

                # ------------------------------------------------
                # Recovery position
                # ------------------------------------------------

                with state_lock:

                    if symbol in positions:
                        continue

                    positions[symbol] = {

                        "symbol": symbol,

                        "quantity": free,

                        # Historical entry price is unknown
                        # after Render restart.
                        #
                        # Current price is used so recovery
                        # does not trigger an artificial
                        # immediate stop loss.

                        "entry_price": current_price,

                        "buy_order_id": None,

                        "buy_time": time.time(),

                        "sma20_touched": False,

                        "sma20_touch_price": None,

                        "recovered": True
                    }

                    recovered += 1

                log.warning(
                    "RECOVERED BALANCE -> %s | free=%.12f | price=%.12f | value=%.2f",
                    symbol,
                    free,
                    current_price,
                    value
                )

            except Exception as e:

                log.warning(
                    "Recovery symbol error: %s",
                    e
                )

        log.info(
            "Position recovery complete -> %s positions",
            recovered
        )

    except Exception as e:

        log.exception(
            "Position recovery error: %s",
            e
        )


# ============================================================
# POSITION SAFETY MONITOR
# ============================================================

def position_safety_loop():

    log.info(
        "Position safety monitor started"
    )

    while True:

        try:

            with state_lock:

                current_positions = list(
                    positions.items()
                )

            for symbol, position in current_positions:

                with state_lock:

                    if symbol in selling_symbols:
                        continue

                try:

                    ticker = client.get_symbol_ticker(
                        symbol=symbol
                    )

                    price = float(
                        ticker["price"]
                    )

                    with state_lock:

                        df = candles.get(
                            symbol
                        )

                    upper = None
                    sma20 = None

                    if (
                        df is not None
                        and
                        len(df) > 0
                    ):

                        try:

                            last = df.iloc[-1]

                            if (
                                "bb_upper" in last.index
                                and
                                not pd.isna(
                                    last["bb_upper"]
                                )
                            ):

                                upper = float(
                                    last["bb_upper"]
                                )

                            if (
                                "sma20" in last.index
                                and
                                not pd.isna(
                                    last["sma20"]
                                )
                            ):

                                sma20 = float(
                                    last["sma20"]
                                )

                        except Exception:

                            pass

                    check_position(
                        symbol,
                        price,
                        upper,
                        sma20
                    )

                except Exception as e:

                    log.warning(
                        "Safety check failed %s: %s",
                        symbol,
                        e
                    )

                # Small delay between symbols
                time.sleep(
                    0.25
                )

        except Exception as e:

            log.exception(
                "Safety monitor error: %s",
                e
            )

        time.sleep(
            SAFETY_CHECK_SECONDS
        )


# ============================================================
# SYMBOL REFRESH LOOP
# ============================================================

def symbol_refresh_loop():

    while True:

        time.sleep(
            SYMBOL_REFRESH_SECONDS
        )

        try:

            update_top_symbols()

            log.info(
                "Top symbols refreshed."
            )

            # ------------------------------------------------
            # Existing WebSocket connections will continue
            # monitoring their current batch.
            #
            # New symbol list is loaded into state and will
            # be used when a WebSocket reconnects.
            # ------------------------------------------------

        except Exception as e:

            log.exception(
                "Symbol refresh error: %s",
                e
            )


# ============================================================
# BOT STARTUP
# ============================================================

def start_bot():

    global bot_started

    # --------------------------------------------------------
    # Prevent duplicate startup
    # --------------------------------------------------------

    with bot_start_lock:

        if bot_started:

            log.warning(
                "Bot already started. Ignoring duplicate start."
            )

            return

        bot_started = True

    log.info(
        "=" * 70
    )

    log.info(
        "BB20 + EMA5 BINANCE SPOT BOT STARTING"
    )

    log.info(
        "=" * 70
    )

    # --------------------------------------------------------
    # EXCHANGE INFO
    # --------------------------------------------------------

    load_exchange_info()

    # --------------------------------------------------------
    # TOP SYMBOLS
    # --------------------------------------------------------

    update_top_symbols()

    if not top_symbols:

        raise RuntimeError(
            "No eligible USDT symbols found."
        )

    # --------------------------------------------------------
    # HISTORICAL CANDLES
    # --------------------------------------------------------

    load_initial_candles()

    # --------------------------------------------------------
    # POSITION RECOVERY
    # --------------------------------------------------------

    recover_positions()

    # --------------------------------------------------------
    # WEBSOCKET
    # --------------------------------------------------------

    start_websocket_connections()

    # --------------------------------------------------------
    # SYMBOL REFRESH
    # --------------------------------------------------------

    threading.Thread(

        target=symbol_refresh_loop,

        daemon=True,

        name="SymbolRefresh"
    ).start()

    # --------------------------------------------------------
    # SAFETY MONITOR
    # --------------------------------------------------------

    threading.Thread(

        target=position_safety_loop,

        daemon=True,

        name="SafetyMonitor"
    ).start()

    log.info(
        "=" * 70
    )

    log.info(
        "BOT STARTED SUCCESSFULLY"
    )

    log.info(
        "Monitored symbols: %s",
        len(top_symbols)
    )

    log.info(
        "Trade amount: $%.2f",
        TRADE_AMOUNT_USDT
    )

    log.info(
        "Timeframe: %s",
        TIMEFRAME
    )

    log.info(
        "Stop loss: %.2f%%",
        STOP_LOSS_PCT * 100
    )

    log.info(
        "=" * 70

    )


# ============================================================
# IMPORTANT: GUNICORN + RENDER
# ============================================================

# Render runs:
#
# gunicorn main:app
#
# In that situation:
#
# __name__ != "__main__"
#
# Therefore start_bot() must be started in a background
# thread when Gunicorn imports this module.


if __name__ == "__main__":

    # This is mainly for local testing.
    # Render normally uses Gunicorn.

    start_bot()

    # Local Flask server
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

else:

    # Gunicorn / Render
    threading.Thread(

        target=start_bot,

        daemon=True,

        name="TradingBot"
    ).start()
