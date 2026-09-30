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

API_KEY = os.getenv("BINANCE_API_KEY")
API_SECRET = os.getenv("BINANCE_API_SECRET")

if not API_KEY or not API_SECRET:
    raise RuntimeError("BINANCE_API_KEY / BINANCE_API_SECRET not set")

TRADE_AMOUNT_USDT = 35.0

TIMEFRAME = "5m"

BB_PERIOD = 20
BB_STD = 2

EMA_FAST_PERIOD = 5
EMA_SLOW_PERIOD = 20

STOP_LOSS_PCT = 0.01

TOP_SYMBOLS = 150

SELL_BALANCE_BUFFER = 0.999

KLINE_LIMIT = 100

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
}

EXCLUDED_SYMBOLS = {
    "BTCUSDT",
    "ETHUSDT",
}


# ============================================================
# LOGGING
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

logger = logging.getLogger(__name__)


# ============================================================
# BINANCE
# ============================================================

client = Client(API_KEY, API_SECRET)


# ============================================================
# FLASK
# ============================================================

app = Flask(__name__)


@app.route("/")
def home():
    return "Trading Bot is Active & Running!"


@app.route("/health")
def health():
    return jsonify({
        "status": "running",
        "positions": len(open_positions),
        "symbols": len(symbols),
    })


# ============================================================
# GLOBAL DATA
# ============================================================

exchange_info = {}
symbol_filters = {}

symbols = []

dataframes = {}

open_positions = {}

data_lock = threading.Lock()

ws_restart_event = threading.Event()


# ============================================================
# EXCHANGE INFO
# ============================================================

def load_exchange_info():
    global exchange_info
    global symbol_filters

    logger.info("Loading Binance exchange information...")

    exchange_info = client.get_exchange_info()

    symbol_filters = {}

    for item in exchange_info["symbols"]:

        if item["status"] != "TRADING":
            continue

        symbol = item["symbol"]

        filters = {}

        for f in item["filters"]:
            filters[f["filterType"]] = f

        symbol_filters[symbol] = filters

    logger.info(
        "Exchange information loaded: %d symbols",
        len(symbol_filters)
    )


# ============================================================
# SYMBOL FILTER HELPERS
# ============================================================

def get_step_size(symbol):

    filters = symbol_filters.get(symbol, {})

    lot_filter = (
        filters.get("MARKET_LOT_SIZE")
        or filters.get("LOT_SIZE")
    )

    if not lot_filter:
        return None

    return float(lot_filter["stepSize"])


def get_min_qty(symbol):

    filters = symbol_filters.get(symbol, {})

    lot_filter = (
        filters.get("MARKET_LOT_SIZE")
        or filters.get("LOT_SIZE")
    )

    if not lot_filter:
        return 0.0

    return float(lot_filter["minQty"])


def get_min_notional(symbol):

    filters = symbol_filters.get(symbol, {})

    notional_filter = (
        filters.get("NOTIONAL")
        or filters.get("MIN_NOTIONAL")
    )

    if not notional_filter:
        return 0.0

    return float(
        notional_filter.get(
            "minNotional",
            0
        )
    )


def adjust_quantity(symbol, quantity):

    step = get_step_size(symbol)

    if not step or step <= 0:
        return quantity

    quantity_decimal = Decimal(str(quantity))
    step_decimal = Decimal(str(step))

    adjusted = (
        quantity_decimal // step_decimal
    ) * step_decimal

    return float(
        adjusted.quantize(
            step_decimal,
            rounding=ROUND_DOWN
        )
    )


# ============================================================
# TOP SYMBOLS
# ============================================================

def refresh_symbols():

    global symbols

    try:

        tickers = client.get_ticker()

        candidates = []

        for ticker in tickers:

            symbol = ticker["symbol"]

            if not symbol.endswith("USDT"):
                continue

            if symbol in EXCLUDED_SYMBOLS:
                continue

            if symbol in STABLECOINS:
                continue

            try:
                volume = float(ticker["quoteVolume"])
            except Exception:
                continue

            if volume <= 0:
                continue

            candidates.append(
                (symbol, volume)
            )

        candidates.sort(
            key=lambda x: x[1],
            reverse=True
        )

        new_symbols = [
            x[0]
            for x in candidates[:TOP_SYMBOLS]
        ]

        with data_lock:
            symbols = new_symbols

        logger.info(
            "Top %d USDT symbols loaded",
            len(symbols)
        )

    except Exception as e:

        logger.exception(
            "Symbol refresh failed: %s",
            e
        )


# ============================================================
# INDICATORS
# ============================================================

def calculate_indicators(df):

    df = df.copy()

    close = df["close"]

    # --------------------------------------------------------
    # Bollinger Band 20,2
    # --------------------------------------------------------

    df["bb_middle"] = (
        close
        .rolling(BB_PERIOD)
        .mean()
    )

    rolling_std = (
        close
        .rolling(BB_PERIOD)
        .std(ddof=0)
    )

    df["bb_upper"] = (
        df["bb_middle"]
        + BB_STD * rolling_std
    )

    df["bb_lower"] = (
        df["bb_middle"]
        - BB_STD * rolling_std
    )

    # --------------------------------------------------------
    # EMA 5
    # --------------------------------------------------------

    df["ema5"] = (
        close
        .ewm(
            span=EMA_FAST_PERIOD,
            adjust=False
        )
        .mean()
    )

    # --------------------------------------------------------
    # EMA 20
    # --------------------------------------------------------

    df["ema20"] = (
        close
        .ewm(
            span=EMA_SLOW_PERIOD,
            adjust=False
        )
        .mean()
    )

    return df


# ============================================================
# BUY SIGNAL
# ============================================================

def entry_signal(df, symbol):

    if df is None or len(df) < 30:
        return False

    row = df.iloc[-1]

    try:

        candle_open = float(row["open"])
        candle_high = float(row["high"])
        candle_close = float(row["close"])

        lower_bb = float(row["bb_lower"])

        ema5 = float(row["ema5"])
        ema20 = float(row["ema20"])

    except Exception:

        return False

    # --------------------------------------------------------
    # 5 BUY CONDITIONS
    # --------------------------------------------------------

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
        ema5 > ema20
    )

    all_conditions = (
        condition_1
        and condition_2
        and condition_3
        and condition_4
        and condition_5
    )

    # --------------------------------------------------------
    # DETAILED LOG
    # --------------------------------------------------------

    logger.info(
        "BUY CHECK %s | "
        "Open<LowerBB=%s | "
        "Close>LowerBB=%s | "
        "Close<EMA5=%s | "
        "High<EMA5=%s | "
        "EMA5>EMA20=%s | "
        "RESULT=%s",
        symbol,
        condition_1,
        condition_2,
        condition_3,
        condition_4,
        condition_5,
        "BUY" if all_conditions else "SKIP"
    )

    logger.info(
        "VALUES %s | "
        "Open=%.8f | "
        "High=%.8f | "
        "Close=%.8f | "
        "LowerBB=%.8f | "
        "EMA5=%.8f | "
        "EMA20=%.8f",
        symbol,
        candle_open,
        candle_high,
        candle_close,
        lower_bb,
        ema5,
        ema20
    )

    return all_conditions


# ============================================================
# GET KLINES
# ============================================================

def get_initial_klines(symbol):

    try:

        klines = client.get_klines(
            symbol=symbol,
            interval=TIMEFRAME,
            limit=KLINE_LIMIT
        )

        rows = []

        for k in klines:

            rows.append({
                "open_time": k[0],
                "open": float(k[1]),
                "high": float(k[2]),
                "low": float(k[3]),
                "close": float(k[4]),
                "volume": float(k[5]),
                "close_time": k[6],
            })

        df = pd.DataFrame(rows)

        if df.empty:
            return None

        # Remove currently open candle
        now_ms = int(time.time() * 1000)

        df = df[
            df["close_time"] <= now_ms
        ].copy()

        if len(df) < 30:
            return None

        df = calculate_indicators(df)

        return df

    except Exception as e:

        logger.error(
            "Kline load failed %s: %s",
            symbol,
            e
        )

        return None


# ============================================================
# LOAD INITIAL DATA
# ============================================================

def load_initial_data():

    logger.info(
        "Loading initial candle data..."
    )

    with data_lock:
        current_symbols = list(symbols)

    for symbol in current_symbols:

        df = get_initial_klines(symbol)

        if df is not None:

            with data_lock:
                dataframes[symbol] = df

    logger.info(
        "Initial candle data loaded: %d",
        len(dataframes)
    )


# ============================================================
# BUY FILL PRICE
# ============================================================

def get_buy_fill_price(order, symbol):

    try:

        fills = order.get("fills", [])

        if fills:

            total_qty = 0.0
            total_cost = 0.0

            for fill in fills:

                qty = float(
                    fill["qty"]
                )

                price = float(
                    fill["price"]
                )

                total_qty += qty
                total_cost += (
                    qty * price
                )

            if total_qty > 0:

                return (
                    total_cost
                    / total_qty
                )

    except Exception:
        pass

    # --------------------------------------------------------
    # Try recent trades
    # --------------------------------------------------------

    try:

        trades = client.get_my_trades(
            symbol=symbol,
            limit=50
        )

        buy_trades = [
            t for t in trades
            if t.get("isBuyer") is True
        ]

        if buy_trades:

            latest = buy_trades[-1]

            return float(
                latest["price"]
            )

    except Exception:
        pass

    # --------------------------------------------------------
    # Final fallback
    # --------------------------------------------------------

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
# BUY ORDER
# ============================================================

def execute_buy(symbol):

    with data_lock:

        if symbol in open_positions:
            return

    try:

        logger.info(
            "BUY SIGNAL → %s | amount=%.2f USDT",
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
            "status",
            ""
        )

        if status not in (
            "FILLED",
            "PARTIALLY_FILLED"
        ):

            logger.warning(
                "BUY order not filled → %s | status=%s",
                symbol,
                status
            )

            return

        entry_price = get_buy_fill_price(
            order,
            symbol
        )

        if entry_price <= 0:

            logger.error(
                "Could not determine entry price → %s",
                symbol
            )

            return

        base_asset = symbol[:-4]

        balance = client.get_asset_balance(
            asset=base_asset
        )

        if not balance:
            logger.error(
                "No balance found after BUY → %s",
                symbol
            )
            return

        quantity = float(
            balance["free"]
        )

        logger.info(
            "BUY FILLED → %s | entry=%.8f | qty=%.8f",
            symbol,
            entry_price,
            quantity
        )

        with data_lock:

            open_positions[symbol] = {
                "symbol": symbol,
                "entry_price": entry_price,
                "quantity": quantity,
                "buy_time": time.time(),
            }

    except BinanceAPIException as e:

        logger.error(
            "BUY Binance API error %s: %s",
            symbol,
            e
        )

    except Exception as e:

        logger.exception(
            "BUY error %s: %s",
            symbol,
            e
        )


# ============================================================
# SELL ORDER
# ============================================================

def execute_sell(symbol, reason):

    with data_lock:

        position = open_positions.get(
            symbol
        )

    if not position:
        return

    try:

        base_asset = symbol[:-4]

        balance = client.get_asset_balance(
            asset=base_asset
        )

        if not balance:

            logger.warning(
                "SELL balance not found → %s",
                symbol
            )

            return

        free_balance = float(
            balance["free"]
        )

        quantity = (
            free_balance
            * SELL_BALANCE_BUFFER
        )

        quantity = adjust_quantity(
            symbol,
            quantity
        )

        min_qty = get_min_qty(symbol)

        if quantity < min_qty:

            logger.warning(
                "SELL quantity below minimum → %s | qty=%.8f | min=%.8f",
                symbol,
                quantity,
                min_qty
            )

            return

        ticker = client.get_symbol_ticker(
            symbol=symbol
        )

        current_price = float(
            ticker["price"]
        )

        min_notional = get_min_notional(
            symbol
        )

        if (
            min_notional > 0
            and quantity * current_price
            < min_notional
        ):

            logger.warning(
                "SELL notional too small → %s | notional=%.4f | minimum=%.4f",
                symbol,
                quantity * current_price,
                min_notional
            )

            return

        logger.warning(
            "SELL SIGNAL → %s | reason=%s | qty=%.8f",
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
            "status",
            ""
        )

        if status in (
            "FILLED",
            "PARTIALLY_FILLED"
        ):

            logger.info(
                "SELL COMPLETED → %s | reason=%s",
                symbol,
                reason
            )

            with data_lock:

                if symbol in open_positions:
                    del open_positions[symbol]

    except BinanceAPIException as e:

        logger.error(
            "SELL Binance API error %s: %s",
            symbol,
            e
        )

    except Exception as e:

        logger.exception(
            "SELL error %s",
            symbol
        )


# ============================================================
# POSITION RECOVERY
# ============================================================

def recover_positions():

    logger.info(
        "Checking existing balances for position recovery..."
    )

    try:

        account = client.get_account()

        balances = account.get(
            "balances",
            []
        )

        with data_lock:
            monitored_symbols = set(
                symbols
            )

        recovered = 0

        for balance in balances:

            asset = balance["asset"]

            free = float(
                balance["free"]
            )

            locked = float(
                balance["locked"]
            )

            total = free + locked

            if total <= 0:
                continue

            if asset in STABLECOINS:
                continue

            symbol = (
                asset + "USDT"
            )

            if symbol not in monitored_symbols:
                continue

            if symbol in EXCLUDED_SYMBOLS:
                continue

            # ------------------------------------------------
            # Try to recover actual recent BUY price
            # ------------------------------------------------

            entry_price = 0.0

            try:

                trades = client.get_my_trades(
                    symbol=symbol,
                    limit=50
                )

                buy_trades = [
                    t for t in trades
                    if t.get("isBuyer") is True
                ]

                if buy_trades:

                    latest_buy = buy_trades[-1]

                    entry_price = float(
                        latest_buy["price"]
                    )

            except Exception:
                pass

            if entry_price <= 0:

                try:

                    ticker = client.get_symbol_ticker(
                        symbol=symbol
                    )

                    entry_price = float(
                        ticker["price"]
                    )

                except Exception:
                    continue

            with data_lock:

                open_positions[symbol] = {
                    "symbol": symbol,
                    "entry_price": entry_price,
                    "quantity": total,
                    "buy_time": time.time(),
                    "recovered": True,
                }

            recovered += 1

            logger.warning(
                "RECOVERED POSITION → %s | entry=%.8f | qty=%.8f",
                symbol,
                entry_price,
                total
            )

        logger.info(
            "Position recovery completed → %d positions",
            recovered
        )

    except Exception as e:

        logger.exception(
            "Position recovery failed: %s",
            e
        )


# ============================================================
# PROCESS CLOSED CANDLE
# ============================================================

def process_kline(symbol, kline):

    try:

        candle_closed = bool(
            kline["x"]
        )

        if not candle_closed:
            return

        candle = {
            "open_time": kline["t"],
            "open": float(kline["o"]),
            "high": float(kline["h"]),
            "low": float(kline["l"]),
            "close": float(kline["c"]),
            "volume": float(kline["v"]),
            "close_time": kline["T"],
        }

        with data_lock:

            old_df = dataframes.get(
                symbol
            )

        if old_df is None:

            return

        new_row = pd.DataFrame(
            [candle]
        )

        df = pd.concat(
            [
                old_df[
                    [
                        "open_time",
                        "open",
                        "high",
                        "low",
                        "close",
                        "volume",
                        "close_time",
                    ]
                ],
                new_row,
            ],
            ignore_index=True
        )

        df = df.drop_duplicates(
            subset=["open_time"],
            keep="last"
        )

        df = df.tail(
            KLINE_LIMIT
        ).reset_index(
            drop=True
        )

        df = calculate_indicators(
            df
        )

        with data_lock:

            dataframes[symbol] = df

            already_in_position = (
                symbol in open_positions
            )

        if already_in_position:
            return

        # ----------------------------------------------------
        # BUY CHECK
        # ----------------------------------------------------

        if entry_signal(
            df,
            symbol
        ):

            execute_buy(symbol)

    except Exception as e:

        logger.exception(
            "KLINE processing error %s: %s",
            symbol,
            e
        )


# ============================================================
# TICKER / SELL MONITOR
# ============================================================

def process_ticker(symbol, price):

    try:

        current_price = float(price)

        with data_lock:

            position = open_positions.get(
                symbol
            )

            df = dataframes.get(
                symbol
            )

        if not position:
            return

        entry_price = float(
            position["entry_price"]
        )

        stop_price = (
            entry_price
            * (1 - STOP_LOSS_PCT)
        )

        upper_bb = None

        if (
            df is not None
            and not df.empty
        ):

            last_row = df.iloc[-1]

            value = last_row.get(
                "bb_upper"
            )

            if pd.notna(value):

                upper_bb = float(
                    value
                )

        # ----------------------------------------------------
        # STOP LOSS
        # ----------------------------------------------------

        if current_price <= stop_price:

            execute_sell(
                symbol,
                f"STOP LOSS {STOP_LOSS_PCT * 100:.2f}%"
            )

            return

        # ----------------------------------------------------
        # UPPER BB
        # ----------------------------------------------------

        if (
            upper_bb is not None
            and current_price >= upper_bb
        ):

            execute_sell(
                symbol,
                "UPPER BB TOUCH"
            )

    except Exception as e:

        logger.error(
            "Ticker processing error %s: %s",
            symbol,
            e
        )


# ============================================================
# WEBSOCKET MESSAGE
# ============================================================

def on_message(ws, message):

    try:

        data = json.loads(
            message
        )

        stream_data = data.get(
            "data",
            data
        )

        event_type = stream_data.get(
            "e"
        )

        # ----------------------------------------------------
        # KLINE
        # ----------------------------------------------------

        if event_type == "kline":

            kline = stream_data.get(
                "k",
                {}
            )

            symbol = kline.get(
                "s"
            )

            if symbol:

                process_kline(
                    symbol,
                    kline
                )

        # ----------------------------------------------------
        # MINI TICKER
        # ----------------------------------------------------

        elif event_type == "24hrMiniTicker":

            symbol = stream_data.get(
                "s"
            )

            price = stream_data.get(
                "c"
            )

            if symbol and price:

                process_ticker(
                    symbol,
                    price
                )

    except Exception as e:

        logger.error(
            "WebSocket message error: %s",
            e
        )


# ============================================================
# WEBSOCKET ERROR
# ============================================================

def on_error(ws, error):

    logger.error(
        "WebSocket error: %s",
        error
    )


# ============================================================
# WEBSOCKET CLOSE
# ============================================================

def on_close(
    ws,
    close_status_code,
    close_msg
):

    logger.warning(
        "WebSocket closed | code=%s | msg=%s",
        close_status_code,
        close_msg
    )


# ============================================================
# WEBSOCKET OPEN
# ============================================================

def on_open(ws):

    logger.info(
        "WebSocket connected"
    )


# ============================================================
# BUILD STREAM URL
# ============================================================

def build_stream_url():

    with data_lock:
        current_symbols = list(
            symbols
        )

    streams = []

    for symbol in current_symbols:

        s = symbol.lower()

        streams.append(
            f"{s}@kline_5m"
        )

        streams.append(
            f"{s}@miniTicker"
        )

    return (
        "wss://stream.binance.com:9443/stream?streams="
        + "/".join(streams)
    )


# ============================================================
# WEBSOCKET LOOP
# ============================================================

def websocket_loop():

    while True:

        try:

            ws_restart_event.clear()

            url = build_stream_url()

            logger.info(
                "Connecting WebSocket..."
            )

            ws = websocket.WebSocketApp(
                url,
                on_open=on_open,
                on_message=on_message,
                on_error=on_error,
                on_close=on_close,
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
# SYMBOL REFRESH LOOP
# ============================================================

def symbol_refresh_loop():

    while True:

        try:

            time.sleep(1800)

            logger.info(
                "Refreshing symbols..."
            )

            refresh_symbols()

            load_initial_data()

            ws_restart_event.set()

        except Exception as e:

            logger.exception(
                "Symbol refresh error: %s",
                e
            )


# ============================================================
# SAFETY POSITION LOOP
# ============================================================

def safety_loop():

    while True:

        try:

            with data_lock:

                current_positions = list(
                    open_positions.items()
                )

            for symbol, position in current_positions:

                try:

                    ticker = client.get_symbol_ticker(
                        symbol=symbol
                    )

                    price = float(
                        ticker["price"]
                    )

                    process_ticker(
                        symbol,
                        price
                    )

                except Exception as e:

                    logger.error(
                        "Safety check failed %s: %s",
                        symbol,
                        e
                    )

            time.sleep(3)

        except Exception as e:

            logger.exception(
                "Safety loop error: %s",
                e
            )

            time.sleep(5)


# ============================================================
# WEBSOCKET REFRESH WATCHER
# ============================================================

def websocket_restart_watcher():

    while True:

        if ws_restart_event.is_set():

            logger.warning(
                "WebSocket refresh requested."
            )

            # The main websocket reconnect loop
            # will reconnect after the current
            # connection closes.

            ws_restart_event.clear()

        time.sleep(5)


# ============================================================
# BOT START
# ============================================================

def start_bot():

    logger.info(
        "=================================================="
    )

    logger.info(
        "BB20 + EMA5 + EMA20 BINANCE SPOT BOT STARTING"
    )

    logger.info(
        "=================================================="
    )

    load_exchange_info()

    refresh_symbols()

    load_initial_data()

    recover_positions()

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
    # Safety position monitor
    # --------------------------------------------------------

    threading.Thread(
        target=safety_loop,
        daemon=True
    ).start()

    # --------------------------------------------------------
    # WebSocket watcher
    # --------------------------------------------------------

    threading.Thread(
        target=websocket_restart_watcher,
        daemon=True
    ).start()

    logger.info(
        "BOT STARTED SUCCESSFULLY"
    )


# ============================================================
# MAIN
# ============================================================

if __name__ == "__main__":

    start_bot()

    port = int(
        os.getenv(
            "PORT",
            "10000"
        )
    )

    app.run(
        host="0.0.0.0",
        port=port,
        threaded=True
    )
