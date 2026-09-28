import os
import time
import json
import math
import threading
import logging
from decimal import Decimal, ROUND_DOWN

import pandas as pd
import requests
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

TRADE_AMOUNT_USDT = 35.0

TIMEFRAME = "5m"

BB_PERIOD = 20
BB_STD = 2.0
EMA_PERIOD = 5

# 1% STOP LOSS
STOP_LOSS_PCT = 0.010

# কতগুলো ALT/USDT pair scan করবে
TOP_SYMBOLS = 150

# Stable coins / unwanted symbols
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

# BTC/ETH বাদ দিয়ে ALT coins হিসেবে ধরা হচ্ছে
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

client = Client(API_KEY, API_SECRET)


# ============================================================
# FLASK / RENDER HEALTH SERVER
# ============================================================

app = Flask(__name__)


@app.route("/")
def home():
    return jsonify({
        "status": "running",
        "bot": "BB20 EMA5 Spot Bot",
        "timeframe": TIMEFRAME,
        "trade_amount": TRADE_AMOUNT_USDT
    })


@app.route("/health")
def health():
    return jsonify({
        "status": "healthy",
        "timestamp": int(time.time())
    })


def run_flask():
    port = int(os.environ.get("PORT", 10000))

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

state_lock = threading.Lock()

last_top_symbol_update = 0


# ============================================================
# LOAD EXCHANGE INFORMATION
# ============================================================

def load_exchange_info():

    global symbol_info

    log.info("Loading Binance exchange information...")

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

        if s["isSpotTradingAllowed"] is not True:
            continue

        filters = {
            f["filterType"]: f
            for f in s["filters"]
        }

        lot_filter = filters.get("LOT_SIZE")
        price_filter = filters.get("PRICE_FILTER")
        min_notional_filter = filters.get("MIN_NOTIONAL")

        temp[symbol] = {
            "base": base,
            "quote": "USDT",
            "step_size": float(lot_filter["stepSize"])
            if lot_filter else 0.000001,

            "min_qty": float(lot_filter["minQty"])
            if lot_filter else 0,

            "tick_size": float(price_filter["tickSize"])
            if price_filter else 0.000001,

            "min_notional": float(
                min_notional_filter.get("minNotional", 0)
            )
            if min_notional_filter else 0
        }

    symbol_info = temp

    log.info(
        "Loaded %s eligible USDT ALT symbols",
        len(symbol_info)
    )


# ============================================================
# GET TOP 150 ALTCOINS
# ============================================================

def update_top_symbols():

    global top_symbols
    global last_top_symbol_update

    try:

        log.info("Updating top ALT symbols...")

        # ONE REST request instead of 150 individual requests
        tickers = client.get_ticker()

        candidates = []

        for t in tickers:

            symbol = t["symbol"]

            if symbol not in symbol_info:
                continue

            try:
                quote_volume = float(t["quoteVolume"])
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
            for symbol, volume in candidates[:TOP_SYMBOLS]
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
# ROUND QUANTITY
# ============================================================

def round_step_quantity(quantity, step_size):

    if step_size <= 0:
        return quantity

    step = Decimal(str(step_size))

    qty = Decimal(str(quantity))

    rounded = (
        qty // step
    ) * step

    return float(rounded)


# ============================================================
# CALCULATE INDICATORS
# ============================================================

def calculate_indicators(df):

    if len(df) < BB_PERIOD + 5:
        return None

    close = df["close"]

    middle = close.rolling(
        BB_PERIOD
    ).mean()

    std = close.rolling(
        BB_PERIOD
    ).std(ddof=0)

    upper = middle + (BB_STD * std)

    lower = middle - (BB_STD * std)

    ema5 = close.ewm(
        span=EMA_PERIOD,
        adjust=False
    ).mean()

    df = df.copy()

    df["bb_middle"] = middle
    df["bb_upper"] = upper
    df["bb_lower"] = lower
    df["ema5"] = ema5

    return df


# ============================================================
# ENTRY CONDITION
# ============================================================

def entry_signal(df):

    if df is None or len(df) < BB_PERIOD + 2:
        return False

    candle = df.iloc[-1]

    required = [
        "bb_lower",
        "bb_upper",
        "ema5"
    ]

    if any(
        pd.isna(candle[x])
        for x in required
    ):
        return False

    candle_open = float(candle["open"])
    candle_high = float(candle["high"])
    candle_close = float(candle["close"])

    lower_bb = float(candle["bb_lower"])
    ema5 = float(candle["ema5"])

    # ========================================================
    # USER STRATEGY
    # ========================================================

    condition_1 = candle_open < lower_bb

    condition_2 = candle_close > lower_bb

    condition_3 = candle_close < ema5

    # High must remain below EMA5.
    # Therefore EMA5 is NOT touched by the candle.
    condition_4 = candle_high < ema5

    return (
        condition_1
        and condition_2
        and condition_3
        and condition_4
    )


# ============================================================
# INITIAL HISTORICAL DATA
# ============================================================

def load_initial_candles():

    global candles

    log.info(
        "Loading initial 5m candle data for %s symbols...",
        len(top_symbols)
    )

    for index, symbol in enumerate(top_symbols):

        try:

            klines = client.get_klines(
                symbol=symbol,
                interval=Client.KLINE_INTERVAL_5MINUTE,
                limit=60
            )

            rows = []

            for k in klines:

                # Ignore currently open candle
                rows.append({
                    "open_time": int(k[0]),
                    "open": float(k[1]),
                    "high": float(k[2]),
                    "low": float(k[3]),
                    "close": float(k[4]),
                    "volume": float(k[5]),
                    "close_time": int(k[6])
                })

            df = pd.DataFrame(rows)

            df = calculate_indicators(df)

            if df is not None:

                candles[symbol] = df

        except BinanceAPIException as e:

            log.warning(
                "Kline error %s: %s",
                symbol,
                e
            )

            time.sleep(0.2)

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
# MARKET BUY
# ============================================================

def buy_symbol(symbol):

    with state_lock:

        if symbol in positions:
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
            order.get("executedQty", 0)
        )

        fills = order.get("fills", [])

        total_cost = 0

        if fills:

            total_cost = sum(
                float(f["price"]) *
                float(f["qty"])
                for f in fills
            )

        if executed_qty <= 0:

            log.error(
                "BUY order returned zero quantity: %s",
                order
            )

            return

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

        with state_lock:

            positions[symbol] = {
                "symbol": symbol,
                "quantity": executed_qty,
                "entry_price": entry_price,
                "buy_order_id": order["orderId"],
                "buy_time": time.time()
            }

        log.info(
            "BUY FILLED → %s | qty=%s | entry=%.8f",
            symbol,
            executed_qty,
            entry_price
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

def sell_symbol(symbol, reason):

    with state_lock:

        position = positions.get(symbol)

    if not position:
        return

    try:

        quantity = position["quantity"]

        info = symbol_info.get(symbol)

        if not info:
            return

        quantity = round_step_quantity(
            quantity,
            info["step_size"]
        )

        if quantity <= 0:
            log.error(
                "Invalid sell quantity %s",
                symbol
            )
            return

        log.warning(
            "SELL SIGNAL → %s | reason=%s | qty=%s",
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

        status = order.get("status")

        log.warning(
            "SELL ORDER RESULT → %s | status=%s | order=%s",
            symbol,
            status,
            order.get("orderId")
        )

        # Remove position ONLY after Binance accepted
        # the market sell order.
        if status in ("FILLED", "PARTIALLY_FILLED"):

            with state_lock:
                positions.pop(symbol, None)

            log.warning(
                "POSITION CLOSED → %s | %s",
                symbol,
                reason
            )

    except BinanceAPIException as e:

        log.error(
            "SELL Binance error %s: %s",
            symbol,
            e
        )

        # Do NOT remove position if sell failed.
        # Sell monitor will try again.

    except Exception as e:

        log.exception(
            "SELL error %s: %s",
            symbol,
            e
        )


# ============================================================
# SELL MONITOR
# ============================================================

def check_position(symbol, current_price, upper_band):

    with state_lock:

        position = positions.get(symbol)

    if not position:
        return

    entry_price = position["entry_price"]

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

def make_stream_url(symbols):

    streams = []

    for symbol in symbols:

        streams.append(
            f"{symbol.lower()}@kline_5m"
        )

        # ticker stream provides real-time price
        streams.append(
            f"{symbol.lower()}@miniTicker"
        )

    stream_string = "/".join(streams)

    return (
        "wss://stream.binance.com:9443/"
        f"stream?streams={stream_string}"
    )


# ============================================================
# WEBSOCKET MESSAGE
# ============================================================

def process_ws_message(message):

    try:

        msg = json.loads(message)

        data = msg.get("data", {})

        stream = msg.get("stream", "")

        if "@kline_5m" in stream:

            process_kline(data)

        elif "@miniticker" in stream.lower():

            process_ticker(data)

    except Exception as e:

        log.warning(
            "WebSocket message processing error: %s",
            e
        )


# ============================================================
# KLINE PROCESSING
# ============================================================

def process_kline(data):

    k = data.get("k")

    if not k:
        return

    symbol = k["s"]

    candle_closed = k["x"]

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

            old_df = candles.get(symbol)

            if old_df is None:

                old_df = pd.DataFrame()

            new_row = pd.DataFrame([row])

            df = pd.concat(
                [old_df, new_row],
                ignore_index=True
            )

            # Prevent duplicate candles
            df = df.drop_duplicates(
                subset=["open_time"],
                keep="last"
            )

            df = df.tail(100)

        df = calculate_indicators(df)

        with state_lock:

            candles[symbol] = df

        # ----------------------------------------------------
        # ENTRY
        # ----------------------------------------------------

        if entry_signal(df):

            with state_lock:

                already_in_position = (
                    symbol in positions
                )

            if not already_in_position:

                buy_symbol(symbol)

    except Exception as e:

        log.exception(
            "Kline processing error %s: %s",
            symbol,
            e
        )


# ============================================================
# REAL-TIME TICKER
# ============================================================

def process_ticker(data):

    symbol = data.get("s")

    if not symbol:
        return

    try:

        price = float(data["c"])

    except Exception:
        return

    with state_lock:

        position_exists = symbol in positions

        df = candles.get(symbol)

    if not position_exists:
        return

    upper_band = None

    if df is not None and len(df) > 0:

        try:

            last = df.iloc[-1]

            if not pd.isna(last["bb_upper"]):

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
                symbols = list(top_symbols)

            if not symbols:

                log.warning(
                    "No symbols available for WebSocket"
                )

                time.sleep(10)

                continue

            url = make_stream_url(symbols)

            log.info(
                "Opening ONE combined WebSocket for %s symbols",
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

        # Exponential-ish reconnect delay
        time.sleep(5)


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

        for b in balances:

            asset = b["asset"]

            if asset in STABLECOINS:
                continue

            free = float(b["free"])
            locked = float(b["locked"])

            total = free + locked

            if total <= 0:
                continue

            symbol = asset + "USDT"

            if symbol not in symbol_info:
                continue

            # We do not automatically treat every dust balance
            # as an open position.
            #
            # Actual position recovery is intentionally conservative.

            if total * 1 > 0:

                log.info(
                    "Existing balance detected: %s = %s",
                    asset,
                    total
                )

    except Exception as e:

        log.error(
            "Position recovery error: %s",
            e
        )


# ============================================================
# POSITION SAFETY MONITOR
# ============================================================

def position_safety_loop():

    """
    Backup monitor.

    If WebSocket misses a message or connection temporarily
    fails, this periodically checks open positions.

    It deliberately checks ONLY currently open positions,
    so REST usage remains very low.
    """

    while True:

        try:

            with state_lock:

                current_positions = list(
                    positions.items()
                )

            for symbol, position in current_positions:

                try:

                    ticker = client.get_symbol_ticker(
                        symbol=symbol
                    )

                    price = float(
                        ticker["price"]
                    )

                    df = candles.get(symbol)

                    upper = None

                    if (
                        df is not None
                        and len(df) > 0
                    ):

                        last = df.iloc[-1]

                        if not pd.isna(
                            last["bb_upper"]
                        ):

                            upper = float(
                                last["bb_upper"]
                            )

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

                # Small delay between position checks
                time.sleep(0.5)

        except Exception as e:

            log.exception(
                "Safety monitor error: %s",
                e
            )

        # Only open positions are checked.
        time.sleep(3)


# ============================================================
# SYMBOL REFRESH LOOP
# ============================================================

def symbol_refresh_loop():

    while True:

        try:

            # Refresh top 150 every 30 minutes
            update_top_symbols()

            # We do NOT reload 150 klines every loop.
            # WebSocket continuously maintains the data.

        except Exception as e:

            log.exception(
                "Symbol refresh error: %s",
                e
            )

        time.sleep(1800)


# ============================================================
# STARTUP
# ============================================================

def start_bot():

    log.info("=" * 70)
    log.info("BB20 + EMA5 BINANCE SPOT BOT STARTING")
    log.info("=" * 70)

    load_exchange_info()

    update_top_symbols()

    # Initial history.
    # This is done only once at startup.
    load_initial_candles()

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
    # POSITION SAFETY
    # --------------------------------------------------------

    threading.Thread(
        target=position_safety_loop,
        daemon=True
    ).start()

    log.info(
        "BOT STARTED SUCCESSFULLY"
    )

    while True:

        time.sleep(60)


# ============================================================
# MAIN
# ============================================================

if __name__ == "__main__":
    start_bot()
