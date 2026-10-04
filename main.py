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

# RSI(3) BUY confirmation
RSI_PERIOD = 3
RSI_OVERSOLD = 10.0

# 1% STOP LOSS
STOP_LOSS_PCT = 0.010

# Top ALT/USDT pairs
TOP_SYMBOLS = 150

# Safety margin for SELL quantity.
# This prevents insufficient balance caused by commission
# or tiny balance differences.
SELL_BALANCE_BUFFER = 0.999


# ============================================================
# STABLECOINS
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
# EXCLUDED COINS
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
# BINANCE
# ============================================================

client = Client(
    API_KEY,
    API_SECRET
)


# ============================================================
# FLASK
# ============================================================

app = Flask(__name__)


@app.route("/")
def home():

    return jsonify({
        "status": "running",
        "bot": "BB20 EMA5 + RSI3 Recovery Spot Bot",
        "timeframe": TIMEFRAME,
        "trade_amount": TRADE_AMOUNT_USDT,
        "stop_loss": f"{STOP_LOSS_PCT * 100:.2f}%"
    })


@app.route("/health")
def health():

    return jsonify({
        "status": "healthy",
        "timestamp": int(time.time())
    })


def run_flask():

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


# ============================================================
# GLOBAL STATE
# ============================================================

symbol_info = {}

top_symbols = []

candles = {}

positions = {}

# Symbols currently being sold.
# Prevents WebSocket and safety monitor from
# selling the same position simultaneously.
selling_symbols = set()

state_lock = threading.RLock()

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

    for s in info["symbols"]:

        symbol = s["symbol"]

        if s["status"] != "TRADING":
            continue

        if s["quoteAsset"] != "USDT":
            continue

        base = s["baseAsset"]

        if base in STABLECOINS:
            continue

        if base in EXCLUDED_BASES:
            continue

        if s.get("isSpotTradingAllowed") is not True:
            continue

        filters = {
            f["filterType"]: f
            for f in s["filters"]
        }

        lot_filter = filters.get(
            "LOT_SIZE"
        )

        market_lot_filter = filters.get(
            "MARKET_LOT_SIZE"
        )

        price_filter = filters.get(
            "PRICE_FILTER"
        )

        min_notional_filter = filters.get(
            "MIN_NOTIONAL"
        )

        notional_filter = filters.get(
            "NOTIONAL"
        )

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

        temp[symbol] = {

            "base": base,

            "quote": "USDT",

            "step_size": step_size,

            "min_qty": min_qty,

            "max_qty": max_qty,

            "market_step_size": market_step_size,

            "market_min_qty": market_min_qty,

            "market_max_qty": market_max_qty,

            "tick_size": float(
                price_filter.get(
                    "tickSize",
                    0
                )
            )
            if price_filter
            else 0.000001,

            "min_notional": min_notional
        }

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

            symbol = t["symbol"]

            if symbol not in symbol_info:
                continue

            try:

                quote_volume = float(
                    t["quoteVolume"]
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

    return float(
        rounded
    )


# ============================================================
# GET VALID MARKET SELL QUANTITY
# ============================================================

def get_valid_sell_quantity(
    symbol,
    free_balance
):

    info = symbol_info.get(
        symbol
    )

    if not info:
        return 0.0

    quantity = (
        free_balance *
        SELL_BALANCE_BUFFER
    )

    # Prefer MARKET_LOT_SIZE when available.
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

    # -----------------------------------------
    # MARKET LOT SIZE
    # -----------------------------------------

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

    # -----------------------------------------
    # LOT SIZE
    # -----------------------------------------

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
# CALCULATE INDICATORS
# ============================================================

def calculate_indicators(df):

    if len(df) < BB_PERIOD + 5:
        return None

    df = df.copy()

    close = df["close"]

    middle = close.rolling(
        BB_PERIOD
    ).mean()

    std = close.rolling(
        BB_PERIOD
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

    ema5 = close.ewm(
        span=EMA_PERIOD,
        adjust=False
    ).mean()

    # --------------------------------------------------------
    # RSI(3)
    # Wilder-style RSI using EWM smoothing.
    # --------------------------------------------------------
    delta = close.diff()

    gain = delta.clip(
        lower=0
    )

    loss = (-delta).clip(
        lower=0
    )

    avg_gain = gain.ewm(
        alpha=1 / RSI_PERIOD,
        adjust=False,
        min_periods=RSI_PERIOD
    ).mean()

    avg_loss = loss.ewm(
        alpha=1 / RSI_PERIOD,
        adjust=False,
        min_periods=RSI_PERIOD
    ).mean()

    rs = avg_gain / avg_loss.replace(
        0,
        float("nan")
    )

    rsi3 = 100 - (100 / (1 + rs))

    # Edge cases: no loss => RSI 100; no gain => RSI 0.
    rsi3 = rsi3.where(
        avg_loss != 0,
        100.0
    )

    rsi3 = rsi3.where(
        avg_gain != 0,
        0.0
    )

    df["rsi3"] = rsi3

    df["bb_middle"] = middle

    df["bb_upper"] = upper

    df["bb_lower"] = lower

    df["ema5"] = ema5

    return df


# ============================================================
# ENTRY SIGNAL
# ============================================================

def entry_signal(df):

    if df is None:
        return False

    if len(df) < BB_PERIOD + 2:
        return False

    candle = df.iloc[-1]
    previous_candle = df.iloc[-2]

    required = [
        "bb_lower",
        "bb_upper",
        "ema5",
        "rsi3"
    ]

    if any(
        pd.isna(candle[x])
        for x in required
    ):
        return False

    if pd.isna(previous_candle["rsi3"]):
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

    rsi3 = float(
        candle["rsi3"]
    )

    previous_rsi3 = float(
        previous_candle["rsi3"]
    )

    # --------------------------------------------------------
    # USER STRATEGY
    # --------------------------------------------------------

    # Candle OPEN below lower BB
    condition_1 = (
        candle_open < lower_bb
    )

    # Candle CLOSE back above lower BB
    condition_2 = (
        candle_close > lower_bb
    )

    # Candle CLOSE below EMA5
    condition_3 = (
        candle_close < ema5
    )

    # Candle HIGH must stay below EMA5
    # Therefore candle does not touch EMA5
    condition_4 = (
        candle_high < ema5
    )

    # RSI(3) must be oversold.
    condition_5 = (
        rsi3 < RSI_OVERSOLD
    )

    # RSI(3) must be recovering: current RSI is higher
    # than the previous CLOSED candle's RSI.
    condition_6 = (
        rsi3 > previous_rsi3
    )

    return (
        condition_1
        and condition_2
        and condition_3
        and condition_4
        and condition_5
        and condition_6
    )


# ============================================================
# INITIAL CANDLES
# ============================================================

def load_initial_candles():

    global candles

    log.info(
        "Loading initial 5m candle data for %s symbols...",
        len(top_symbols)
    )

    for index, symbol in enumerate(
        top_symbols
    ):

        try:

            klines = client.get_klines(
                symbol=symbol,
                interval=Client.KLINE_INTERVAL_5MINUTE,
                limit=60
            )

            rows = []

            for k in klines:

                # IMPORTANT:
                # Ignore currently open candle.
                #
                # Binance kline response's final candle
                # may still be forming.

                rows.append({

                    "open_time": int(k[0]),

                    "open": float(k[1]),

                    "high": float(k[2]),

                    "low": float(k[3]),

                    "close": float(k[4]),

                    "volume": float(k[5]),

                    "close_time": int(k[6])
                })

            df = pd.DataFrame(
                rows
            )

            # Remove currently open candle
            if len(df) > 0:

                current_ms = int(
                    time.time() * 1000
                )

                df = df[
                    df["close_time"] <= current_ms
                ]

            df = calculate_indicators(
                df
            )

            if df is not None:

                with state_lock:

                    candles[symbol] = df

        except BinanceAPIException as e:

            log.warning(
                "Kline error %s: %s",
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

    log.info(
        "Initial candle loading complete: %s symbols",
        len(candles)
    )


# ============================================================
# BUY
# ============================================================

def buy_symbol(symbol):

    with state_lock:

        if symbol in positions:
            return

        if symbol in selling_symbols:
            return

    try:

        log.info(
            "BUY SIGNAL → %s | $%.2f",
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

        fills = order.get(
            "fills",
            []
        )

        total_cost = 0.0

        if fills:

            for f in fills:

                total_cost += (
                    float(f["price"]) *
                    float(f["qty"])
                )

        if total_cost > 0:

            entry_price = (
                total_cost /
                executed_qty
            )

        else:

            entry_price = float(
                client.get_symbol_ticker(
                    symbol=symbol
                )["price"]
            )

        # ----------------------------------------------------
        # Get actual balance after BUY
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

                    # Do not allow actual balance to be
                    # greater than executed quantity.
                    #
                    # This is mainly to keep internal state
                    # conservative.

                    actual_balance = min(
                        executed_qty,
                        free_balance
                    )

            except Exception as e:

                log.warning(
                    "Could not verify post-buy balance %s: %s",
                    symbol,
                    e
                )

        with state_lock:

            positions[symbol] = {

                "symbol": symbol,

                "quantity": actual_balance,

                "entry_price": entry_price,

                "buy_order_id": order["orderId"],

                "buy_time": time.time()
            }

        log.info(
            "BUY FILLED → %s | qty=%.12f | entry=%.12f | order=%s",
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
    # Lock the position BEFORE doing REST requests.
    #
    # This is extremely important.
    #
    # WebSocket and safety monitor can both detect STOP LOSS
    # almost at the same time.
    #
    # Only ONE of them is allowed to execute SELL.
    # --------------------------------------------------------

    with state_lock:

        position = positions.get(
            symbol
        )

        if not position:
            return

        if symbol in selling_symbols:

            log.info(
                "SELL already in progress → %s",
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
                "Symbol info missing → %s",
                symbol
            )

            return

        asset = info["base"]

        # ----------------------------------------------------
        # GET REAL BINANCE FREE BALANCE
        # ----------------------------------------------------

        balance = client.get_asset_balance(
            asset=asset
        )

        if not balance:

            log.error(
                "Balance not found → %s",
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
            "SELL BALANCE → %s | stored=%.12f | free=%.12f",
            symbol,
            stored_quantity,
            free_balance
        )

        if free_balance <= 0:

            log.error(
                "No free balance available → %s",
                symbol
            )

            return

        # ----------------------------------------------------
        # IMPORTANT:
        #
        # Never try to sell more than actual Binance balance.
        # ----------------------------------------------------

        quantity_source = min(
            stored_quantity,
            free_balance
        )

        # Safety buffer for commission / precision
        quantity = (
            quantity_source *
            SELL_BALANCE_BUFFER
        )

        # ----------------------------------------------------
        # Correct Binance quantity
        # ----------------------------------------------------

        quantity = get_valid_sell_quantity(
            symbol,
            quantity
        )

        if quantity <= 0:

            log.error(
                "Valid SELL quantity is zero → %s | free=%s",
                symbol,
                free_balance
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
                    "SELL notional too small → %s | value=%.8f | minimum=%.8f",
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

        log.warning(
            "SELL SIGNAL → %s | reason=%s | qty=%.12f",
            symbol,
            reason,
            quantity
        )

        # ----------------------------------------------------
        # MARKET SELL
        # ----------------------------------------------------

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
            "SELL ORDER RESULT → %s | status=%s | executed=%.12f | order=%s",
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
                "POSITION CLOSED → %s | %s",
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

                    positions[symbol][
                        "quantity"
                    ] = remaining

                else:

                    positions.pop(
                        symbol,
                        None
                    )

            log.warning(
                "PARTIAL SELL → %s | sold=%.12f | remaining=%.12f",
                symbol,
                executed_qty,
                remaining
            )

        else:

            log.warning(
                "SELL not filled → %s | status=%s",
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

        # ----------------------------------------------------
        # Unlock SELL.
        #
        # If it failed, the position remains in positions,
        # so the next safety cycle can try again.
        # ----------------------------------------------------

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
    upper_band
):

    with state_lock:

        position = positions.get(
            symbol
        )

        if not position:
            return

        if symbol in selling_symbols:
            return

    entry_price = float(
        position["entry_price"]
    )

    stop_price = (
        entry_price *
        (1.0 - STOP_LOSS_PCT)
    )

    # --------------------------------------------------------
    # STOP LOSS
    # --------------------------------------------------------

    if current_price <= stop_price:

        sell_symbol(
            symbol,
            f"STOP LOSS {STOP_LOSS_PCT * 100:.2f}%"
        )

        return

    # --------------------------------------------------------
    # UPPER BB EXIT
    # --------------------------------------------------------

    if upper_band is not None:

        if current_price >= upper_band:

            sell_symbol(
                symbol,
                "UPPER BB TOUCH"
            )


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

    symbol = k["s"]

    candle_closed = k["x"]

    # Only CLOSED candles are used for entry.
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

        with state_lock:

            old_df = candles.get(
                symbol
            )

            if old_df is None:

                old_df = pd.DataFrame()

            new_row = pd.DataFrame(
                [row]
            )

            df = pd.concat(
                [
                    old_df,
                    new_row
                ],
                ignore_index=True
            )

            df = df.drop_duplicates(
                subset=[
                    "open_time"
                ],
                keep="last"
            )

            df = df.tail(
                100
            )

        df = calculate_indicators(
            df
        )

        if df is None:
            return

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

                rsi_value = float(
                    df.iloc[-1]["rsi3"]
                )

                previous_rsi_value = float(
                    df.iloc[-2]["rsi3"]
                )

                log.info(
                    "BUY CONDITIONS PASSED → %s | RSI3=%.2f | Previous RSI3=%.2f",
                    symbol,
                    rsi_value,
                    previous_rsi_value
                )

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

    with state_lock:

        position_exists = (
            symbol in positions
        )

        df = candles.get(
            symbol
        )

    if not position_exists:
        return

    upper_band = None

    if (
        df is not None
        and
        len(df) > 0
    ):

        try:

            last = df.iloc[-1]

            if not pd.isna(
                last["bb_upper"]
            ):

                upper_band = float(
                    last["bb_upper"]
                )

        except Exception:

            pass

    check_position(
        symbol,
        price,
        upper_band
    )


# ============================================================
# WEBSOCKET LOOP
# ============================================================

def websocket_loop():

    while True:

        try:

            with state_lock:

                symbols = list(
                    top_symbols
                )

            if not symbols:

                log.warning(
                    "No symbols available for WebSocket"
                )

                time.sleep(
                    10
                )

                continue

            url = make_stream_url(
                symbols
            )

            log.info(
                "Opening combined WebSocket for %s symbols",
                len(symbols)
            )

            ws = websocket.WebSocketApp(

                url,

                on_message=lambda ws, msg:
                    process_ws_message(msg),

                on_error=lambda ws, error:
                    log.error(
                        "WebSocket error: %s",
                        error
                    ),

                on_close=lambda ws, code, msg:
                    log.warning(
                        "WebSocket closed: %s %s",
                        code,
                        msg
                    )
            )

            ws.run_forever(
                ping_interval=60,
                ping_timeout=20
            )

        except Exception as e:

            log.exception(
                "WebSocket loop error: %s",
                e
            )

        time.sleep(
            5
        )


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

        for b in balances:

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

            # Only recover balances that are part of
            # our currently monitored TOP symbols.
            #
            # This avoids automatically treating every
            # random wallet balance as a bot position.

            with state_lock:

                is_monitored = (
                    symbol in top_symbols
                )

            if not is_monitored:
                continue

            # ------------------------------------------------
            # Conservative recovery.
            #
            # We use FREE balance as sellable quantity.
            # Locked balance is not immediately sellable.
            # ------------------------------------------------

            if free <= 0:
                continue

            # Current price
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

            # Ignore tiny dust.
            if value < 5.0:
                continue

            # ------------------------------------------------
            # We do not know the exact original entry price
            # after a Render restart.
            #
            # Use current price temporarily.
            #
            # IMPORTANT:
            # This prevents a recovered position from being
            # immediately sold due to an unknown historical
            # entry price.
            #
            # The next strategy/monitor cycle will continue
            # monitoring it.
            # ------------------------------------------------

            with state_lock:

                if symbol not in positions:

                    positions[symbol] = {

                        "symbol": symbol,

                        "quantity": free,

                        "entry_price": current_price,

                        "buy_order_id": None,

                        "buy_time": time.time(),

                        "recovered": True
                    }

                    recovered += 1

            log.warning(
                "RECOVERED BALANCE → %s | free=%.12f | price=%.12f | value=%.2f",
                symbol,
                free,
                current_price,
                value
            )

        log.info(
            "Position recovery complete → %s positions recovered",
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

    """
    Backup monitor.

    Checks ONLY current open positions.

    This protects against:
    - WebSocket disconnect
    - missed ticker
    - temporary WebSocket failure
    """

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

                    if (
                        df is not None
                        and
                        len(df) > 0
                    ):

                        try:

                            last = df.iloc[-1]

                            if not pd.isna(
                                last["bb_upper"]
                            ):

                                upper = float(
                                    last["bb_upper"]
                                )

                        except Exception:

                            pass

                    check_position(
                        symbol,
                        price,
                        upper
                    )

                except Exception as e:

                    log.warning(
                        "Safety check failed %s: %s",
                        symbol,
                        e
                    )

                time.sleep(
                    0.5
                )

        except Exception as e:

            log.exception(
                "Safety monitor error: %s",
                e
            )

        time.sleep(
            3
        )


# ============================================================
# SYMBOL REFRESH
# ============================================================

def symbol_refresh_loop():

    while True:

        try:

            update_top_symbols()

        except Exception as e:

            log.exception(
                "Symbol refresh error: %s",
                e
            )

        # Refresh every 30 minutes
        time.sleep(
            1800
        )


# ============================================================
# START BOT
# ============================================================

def start_bot():

    log.info(
        "=" * 70
    )

    log.info(
        "BB20 + EMA5 + RSI3 RECOVERY BINANCE SPOT BOT STARTING"
    )

    log.info(
        "=" * 70
    )

    # --------------------------------------------------------
    # EXCHANGE INFORMATION
    # --------------------------------------------------------

    load_exchange_info()

    # --------------------------------------------------------
    # TOP SYMBOLS
    # --------------------------------------------------------

    update_top_symbols()

    # --------------------------------------------------------
    # HISTORICAL CANDLES
    # --------------------------------------------------------

    load_initial_candles()

    # --------------------------------------------------------
    # RECOVER POSITIONS
    # --------------------------------------------------------

    recover_positions()

    # --------------------------------------------------------
    # FLASK
    # --------------------------------------------------------

    threading.Thread(
        target=run_flask,
        daemon=True
    ).start()

    # --------------------------------------------------------
    # WEBSOCKET
    # --------------------------------------------------------

    threading.Thread(
        target=websocket_loop,
        daemon=True
    ).start()

    # --------------------------------------------------------
    # SYMBOL REFRESH
    # --------------------------------------------------------

    threading.Thread(
        target=symbol_refresh_loop,
        daemon=True
    ).start()

    # --------------------------------------------------------
    # SAFETY MONITOR
    # --------------------------------------------------------

    threading.Thread(
        target=position_safety_loop,
        daemon=True
    ).start()

    log.info(
        "BOT STARTED SUCCESSFULLY"
    )

    while True:

        time.sleep(
            60
        )


# ============================================================
# MAIN
# ============================================================

if __name__ == "__main__":

    start_bot()
