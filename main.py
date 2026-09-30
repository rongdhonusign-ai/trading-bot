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
EMA_TREND_PERIOD = 50

RSI_PERIOD = 14
RSI_MIN = 30.0
RSI_MAX = 50.0

VOLUME_SMA_PERIOD = 20
VOLUME_MULTIPLIER = 1.20

# 1% STOP LOSS
STOP_LOSS_PCT = 0.010

# Number of top ALT/USDT symbols
TOP_SYMBOLS = 150

# Safety margin when selling
SELL_BALANCE_BUFFER = 0.999

# Refresh top symbols every 30 minutes
SYMBOL_REFRESH_SECONDS = 1800

# WebSocket reconnect interval
WEBSOCKET_RECONNECT_SECONDS = 5


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
# BINANCE CLIENT
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
        "bot": "BB20 EMA5 EMA50 RSI14 Volume Spot Bot",
        "timeframe": TIMEFRAME,
        "trade_amount": TRADE_AMOUNT_USDT,
        "stop_loss": f"{STOP_LOSS_PCT * 100:.2f}%",
        "symbols": len(top_symbols),
        "positions": len(positions)
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

selling_symbols = set()

state_lock = threading.RLock()

last_top_symbol_update = 0

websocket_restart_event = threading.Event()


# ============================================================
# LOAD BINANCE EXCHANGE INFORMATION
# ============================================================

def load_exchange_info():
    global symbol_info

    log.info(
        "Loading Binance exchange information..."
    )

    info = client.get_exchange_info()

    temp = {}

    for s in info.get("symbols", []):

        symbol = s.get("symbol")

        if s.get("status") != "TRADING":
            continue

        if s.get("quoteAsset") != "USDT":
            continue

        base = s.get("baseAsset")

        if base in STABLECOINS:
            continue

        if base in EXCLUDED_BASES:
            continue

        if s.get("isSpotTradingAllowed") is not True:
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

        # ----------------------------------------------------
        # LOT SIZE
        # ----------------------------------------------------

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

        # ----------------------------------------------------
        # MARKET LOT SIZE
        # ----------------------------------------------------

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

        # ----------------------------------------------------
        # MIN NOTIONAL
        # ----------------------------------------------------

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

        # ----------------------------------------------------
        # PRICE TICK
        # ----------------------------------------------------

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

    symbol_info = temp

    log.info(
        "Loaded %s eligible USDT ALT symbols",
        len(symbol_info)
    )


# ============================================================
# UPDATE TOP SYMBOLS
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

        for ticker in tickers:

            symbol = ticker.get("symbol")

            if symbol not in symbol_info:
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

        selected = [
            symbol
            for symbol, volume in candidates[:TOP_SYMBOLS]
        ]

        with state_lock:
            old_symbols = set(top_symbols)
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

        # ----------------------------------------------------
        # Force WebSocket to refresh if symbols changed
        # ----------------------------------------------------

        if set(selected) != old_symbols:

            websocket_restart_event.set()

            log.info(
                "Top symbols changed -> WebSocket refresh requested"
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

    # --------------------------------------------------------
    # MARKET LOT SIZE
    # --------------------------------------------------------

    if market_step > 0:

        quantity = round_step_quantity(
            quantity,
            market_step
        )

        if market_min > 0 and quantity < market_min:
            return 0.0

        if market_max > 0:
            quantity = min(
                quantity,
                market_max
            )

    # --------------------------------------------------------
    # LOT SIZE
    # --------------------------------------------------------

    if lot_step > 0:

        quantity = round_step_quantity(
            quantity,
            lot_step
        )

        if lot_min > 0 and quantity < lot_min:
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

    minimum_length = max(
        BB_PERIOD + 5,
        EMA_TREND_PERIOD + 5,
        RSI_PERIOD + 5,
        VOLUME_SMA_PERIOD + 5
    )

    if len(df) < minimum_length:
        return None

    df = df.copy()

    close = df["close"]

    # --------------------------------------------------------
    # BOLLINGER BAND 20,2
    # --------------------------------------------------------

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
        BB_STD * std
    )

    lower = (
        middle -
        BB_STD * std
    )

    # --------------------------------------------------------
    # EMA5
    # --------------------------------------------------------

    ema5 = close.ewm(
        span=EMA_PERIOD,
        adjust=False
    ).mean()

    # --------------------------------------------------------
    # EMA50
    # --------------------------------------------------------

    ema50 = close.ewm(
        span=EMA_TREND_PERIOD,
        adjust=False
    ).mean()

    # --------------------------------------------------------
    # RSI14
    # --------------------------------------------------------

    delta = close.diff()

    gain = delta.clip(
        lower=0
    )

    loss = -delta.clip(
        upper=0
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
        pd.NA
    )

    rsi14 = 100 - (
        100 / (1 + rs)
    )

    rsi14 = rsi14.fillna(100)

    # --------------------------------------------------------
    # VOLUME SMA20
    # --------------------------------------------------------

    volume_sma20 = df["volume"].rolling(
        VOLUME_SMA_PERIOD
    ).mean()

    df["bb_middle"] = middle
    df["bb_upper"] = upper
    df["bb_lower"] = lower

    df["ema5"] = ema5
    df["ema50"] = ema50

    df["rsi14"] = rsi14
    df["volume_sma20"] = volume_sma20

    return df


# ============================================================
# ENTRY SIGNAL
# ============================================================

def entry_signal(df):

    if df is None:
        return False

    minimum_length = max(
        BB_PERIOD + 2,
        EMA_TREND_PERIOD + 2,
        RSI_PERIOD + 2,
        VOLUME_SMA_PERIOD + 2
    )

    if len(df) < minimum_length:
        return False

    candle = df.iloc[-1]

    required = [
        "bb_lower",
        "bb_upper",
        "ema5",
        "ema50",
        "rsi14",
        "volume_sma20"
    ]

    if any(
        pd.isna(candle[x])
        for x in required
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

    candle_volume = float(
        candle["volume"]
    )

    lower_bb = float(
        candle["bb_lower"]
    )

    ema5 = float(
        candle["ema5"]
    )

    ema50 = float(
        candle["ema50"]
    )

    rsi14 = float(
        candle["rsi14"]
    )

    volume_sma20 = float(
        candle["volume_sma20"]
    )

    # ========================================================
    # BUY CONDITIONS
    # ========================================================

    # 1. Candle OPEN below Lower BB
    condition_1 = (
        candle_open < lower_bb
    )

    # 2. Candle CLOSE above Lower BB
    condition_2 = (
        candle_close > lower_bb
    )

    # 3. Candle CLOSE below EMA5
    condition_3 = (
        candle_close < ema5
    )

    # 4. Candle HIGH below EMA5
    #    EMA5 must NOT be touched
    condition_4 = (
        candle_high < ema5
    )

    # 5. Candle CLOSE above EMA50
    condition_5 = (
        candle_close > ema50
    )

    # 6. RSI between 30 and 50
    condition_6 = (
        RSI_MIN < rsi14 < RSI_MAX
    )

    # 7. Volume 20% above Volume SMA20
    condition_7 = (
        candle_volume >
        volume_sma20 * VOLUME_MULTIPLIER
    )

    all_conditions = (
        condition_1
        and condition_2
        and condition_3
        and condition_4
        and condition_5
        and condition_6
        and condition_7
    )

    log.info(
        "ENTRY CHECK | "
        "BB=%s | "
        "EMA5=%s | "
        "EMA50=%s | "
        "RSI14=%.2f | "
        "VOL=%.2f/%.2f | "
        "RESULT=%s",

        "OK"
        if (
            condition_1
            and condition_2
        )
        else "NO",

        "OK"
        if (
            condition_3
            and condition_4
        )
        else "NO",

        "OK"
        if condition_5
        else "NO",

        rsi14,

        candle_volume,

        volume_sma20,

        "BUY"
        if all_conditions
        else "SKIP"
    )

    return all_conditions


# ============================================================
# LOAD INITIAL CANDLES
# ============================================================

def load_initial_candles():

    global candles

    with state_lock:
        symbols = list(top_symbols)

    log.info(
        "Loading initial 5m candle data for %s symbols...",
        len(symbols)
    )

    loaded = 0

    for symbol in symbols:

        try:

            klines = client.get_klines(
                symbol=symbol,
                interval=Client.KLINE_INTERVAL_5MINUTE,
                limit=100
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

            df = pd.DataFrame(rows)

            # ------------------------------------------------
            # Remove currently open candle
            # ------------------------------------------------

            if len(df) > 0:

                current_ms = int(
                    time.time() * 1000
                )

                df = df[
                    df["close_time"] <= current_ms
                ]

            df = calculate_indicators(df)

            if df is not None:

                with state_lock:
                    candles[symbol] = df

                loaded += 1

        except BinanceAPIException as e:

            log.warning(
                "Kline error %s: %s",
                symbol,
                e
            )

            time.sleep(0.15)

        except Exception as e:

            log.warning(
                "Initial data error %s: %s",
                symbol,
                e
            )

    log.info(
        "Initial candle loading complete: %s symbols",
        loaded
    )


# ============================================================
# GET ACTUAL BUY FILL PRICE
# ============================================================

def get_buy_fill_price(
    order,
    symbol,
    executed_qty
):

    fills = order.get(
        "fills",
        []
    )

    total_cost = 0.0
    total_qty = 0.0

    if fills:

        for fill in fills:

            fill_price = float(
                fill.get(
                    "price",
                    0
                )
            )

            fill_qty = float(
                fill.get(
                    "qty",
                    0
                )
            )

            total_cost += (
                fill_price *
                fill_qty
            )

            total_qty += fill_qty

    if total_qty > 0:

        return (
            total_cost /
            total_qty
        )

    try:

        trades = client.get_my_trades(
            symbol=symbol,
            limit=20
        )

        buy_trades = [
            t for t in trades
            if (
                t.get("isBuyer") is True
                and
                float(t.get("qty", 0)) > 0
            )
        ]

        if buy_trades:

            total_cost = 0.0
            total_qty = 0.0

            for trade in buy_trades:

                qty = float(
                    trade["qty"]
                )

                price = float(
                    trade["price"]
                )

                total_qty += qty
                total_cost += (
                    qty * price
                )

            if total_qty > 0:

                return (
                    total_cost /
                    total_qty
                )

    except Exception as e:

        log.warning(
            "Could not obtain trade fill price %s: %s",
            symbol,
            e
        )

    try:

        ticker = client.get_symbol_ticker(
            symbol=symbol
        )

        return float(
            ticker["price"]
        )

    except Exception:

        return 0.0


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

        log.warning(
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

        log.info(
            "BUY ORDER RESULT -> %s | status=%s | executed=%.12f | order=%s",
            symbol,
            status,
            executed_qty,
            order_id
        )

        if executed_qty <= 0:

            log.error(
                "BUY returned zero quantity -> %s",
                symbol
            )

            return

        # ----------------------------------------------------
        # Actual weighted average fill price
        # ----------------------------------------------------

        entry_price = get_buy_fill_price(
            order,
            symbol,
            executed_qty
        )

        if entry_price <= 0:

            log.error(
                "Could not determine entry price -> %s",
                symbol
            )

            return

        # ----------------------------------------------------
        # Get actual free balance
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

                    if free_balance > 0:

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

        with state_lock:

            positions[symbol] = {

                "symbol": symbol,

                "quantity": actual_balance,

                "entry_price": entry_price,

                "buy_order_id": order_id,

                "buy_time": time.time()
            }

        stop_price = (
            entry_price *
            (1.0 - STOP_LOSS_PCT)
        )

        log.warning(
            "BUY FILLED -> %s | qty=%.12f | entry=%.12f | stop=%.12f | order=%s",
            symbol,
            actual_balance,
            entry_price,
            stop_price,
            order_id
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
    # Lock position
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

        # Never sell more than actual free balance
        quantity_source = min(
            stored_quantity,
            free_balance
        )

        quantity = (
            quantity_source *
            SELL_BALANCE_BUFFER
        )

        quantity = get_valid_sell_quantity(
            symbol,
            quantity
        )

        if quantity <= 0:

            log.error(
                "Valid SELL quantity is zero -> %s",
                symbol
            )

            return

        # ----------------------------------------------------
        # MIN NOTIONAL CHECK
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

        log.warning(
            "SELL SIGNAL -> %s | reason=%s | qty=%.12f",
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
            "SELL ORDER RESULT -> %s | status=%s | executed=%.12f | order=%s",
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
                "POSITION CLOSED -> %s | reason=%s",
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

                if (
                    symbol in positions
                    and
                    remaining > 0
                ):

                    positions[symbol][
                        "quantity"
                    ] = remaining

                elif symbol in positions:

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
                "SELL NOT FILLED -> %s | status=%s",
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
# CHECK OPEN POSITION
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

    # ========================================================
    # 1% STOP LOSS
    # ========================================================

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

    # ========================================================
    # UPPER BB TOUCH / CROSS
    # ========================================================

    if upper_band is not None:

        if current_price >= upper_band:

            sell_symbol(
                symbol,
                "UPPER BB TOUCH"
            )

            return


# ============================================================
# MAKE WEBSOCKET URL
# ============================================================

def make_stream_url(symbols):

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

def process_ws_message(message):

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

def process_kline(data):

    k = data.get("k")

    if not k:
        return

    symbol = k.get("s")

    if not symbol:
        return

    candle_closed = k.get("x")

    # Only closed candles are used for BUY
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

            df = df.tail(100)

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

        # ====================================================
        # BUY
        # ====================================================

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

def process_ticker(data):

    symbol = data.get("s")

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

            websocket_restart_event.clear()

            url = make_stream_url(
                symbols
            )

            log.info(
                "Opening combined WebSocket for %s symbols",
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
                log.error(
                    "WebSocket error: %s",
                    error
                )

            def on_close(
                ws,
                close_status_code,
                close_msg
            ):
                log.warning(
                    "WebSocket closed: %s %s",
                    close_status_code,
                    close_msg
                )

            ws = websocket.WebSocketApp(

                url,

                on_message=on_message,

                on_error=on_error,

                on_close=on_close
            )

            # ------------------------------------------------
            # Automatic WebSocket refresh
            # ------------------------------------------------

            def force_restart():

                time.sleep(
                    SYMBOL_REFRESH_SECONDS
                )

                if not websocket_restart_event.is_set():

                    websocket_restart_event.set()

                    try:
                        ws.close()
                    except Exception:
                        pass

            threading.Thread(
                target=force_restart,
                daemon=True
            ).start()

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
            WEBSOCKET_RECONNECT_SECONDS
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

        for balance_item in balances:

            asset = balance_item.get(
                "asset"
            )

            if not asset:
                continue

            if asset in STABLECOINS:
                continue

            free = float(
                balance_item.get(
                    "free",
                    0
                )
            )

            locked = float(
                balance_item.get(
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

            with state_lock:

                if symbol not in top_symbols:
                    continue

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
            # Try to find recent BUY price
            # ------------------------------------------------

            entry_price = current_price

            try:

                trades = client.get_my_trades(
                    symbol=symbol,
                    limit=50
                )

                buy_trades = [
                    t for t in trades
                    if (
                        t.get("isBuyer") is True
                        and
                        float(
                            t.get(
                                "qty",
                                0
                            )
                        ) > 0
                    )
                ]

                if buy_trades:

                    # Most recent BUY trade
                    latest_buy = buy_trades[-1]

                    latest_buy_price = float(
                        latest_buy["price"]
                    )

                    if latest_buy_price > 0:

                        entry_price = (
                            latest_buy_price
                        )

            except Exception as e:

                log.warning(
                    "Could not recover historical BUY price %s: %s",
                    symbol,
                    e
                )

            # ------------------------------------------------
            # Recover position
            # ------------------------------------------------

            with state_lock:

                if symbol not in positions:

                    positions[symbol] = {

                        "symbol": symbol,

                        "quantity": free,

                        "entry_price": entry_price,

                        "buy_order_id": None,

                        "buy_time": time.time(),

                        "recovered": True
                    }

                    recovered += 1

            stop_price = (
                entry_price *
                (1.0 - STOP_LOSS_PCT)
            )

            log.warning(
                "RECOVERED BALANCE -> %s | qty=%.12f | entry=%.12f | stop=%.12f | value=%.2f",
                symbol,
                free,
                entry_price,
                stop_price,
                value
            )

        log.info(
            "Position recovery complete -> %s positions recovered",
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
    Backup REST monitor.

    Checks open positions every few seconds.

    Protects against:
    - WebSocket disconnect
    - missed ticker
    - temporary WebSocket failure
    - missed 1% stop loss
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
# SYMBOL REFRESH LOOP
# ============================================================

def symbol_refresh_loop():

    while True:

        try:

            time.sleep(
                SYMBOL_REFRESH_SECONDS
            )

            update_top_symbols()

        except Exception as e:

            log.exception(
                "Symbol refresh error: %s",
                e
            )


# ============================================================
# START BOT
# ============================================================

def start_bot():

    log.info(
        "=" * 70
    )

    log.info(
        "BB20 + EMA5 + EMA50 + RSI14 + VOLUME BINANCE SPOT BOT STARTING"
    )

    log.info(
        "=" * 70
    )

    # --------------------------------------------------------
    # Exchange information
    # --------------------------------------------------------

    load_exchange_info()

    # --------------------------------------------------------
    # Top symbols
    # --------------------------------------------------------

    update_top_symbols()

    # --------------------------------------------------------
    # Initial candle data
    # --------------------------------------------------------

    load_initial_candles()

    # --------------------------------------------------------
    # Recover positions
    # --------------------------------------------------------

    recover_positions()

    # --------------------------------------------------------
    # Flask
    # --------------------------------------------------------

    threading.Thread(
        target=run_flask,
        daemon=True
    ).start()

    # --------------------------------------------------------
    # WebSocket
    # --------------------------------------------------------

    threading.Thread(
        target=websocket_loop,
        daemon=True
    ).start()

    # --------------------------------------------------------
    # Symbol refresh
    # --------------------------------------------------------

    threading.Thread(
        target=symbol_refresh_loop,
        daemon=True
    ).start()

    # --------------------------------------------------------
    # Safety monitor
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
