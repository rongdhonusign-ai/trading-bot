import os
import time
import json
import signal
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
        "BINANCE_API_KEY and BINANCE_API_SECRET "
        "environment variables are required."
    )


TRADE_AMOUNT_USDT = 35.0

TIMEFRAME = "5m"

BB_PERIOD = 49
BB_STD = 2.0

RSI_PERIOD = 3

RSI_BUY_LEVEL = 10.0
RSI_SELL_LEVEL = 80.0

TOP_SYMBOLS = 150

SELL_BALANCE_BUFFER = 0.999

CANDLE_LIMIT = 150

SYMBOL_REFRESH_SECONDS = 1800

SAFETY_CHECK_SECONDS = 30

SHUTDOWN_EVENT = threading.Event()


# ============================================================
# LOGGING
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

log = logging.getLogger(__name__)


# ============================================================
# BINANCE CLIENT
# ============================================================

client = Client(API_KEY, API_SECRET)


# ============================================================
# FLASK
# ============================================================

app = Flask(__name__)


# ============================================================
# GLOBAL STATE
# ============================================================

state_lock = threading.RLock()

exchange_info = {}

symbol_info = {}

top_symbols = []

candle_data = {}

open_positions = {}

last_closed_candle = {}

last_buy_attempt = {}

last_sell_attempt = {}


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
    "CHF",
}


# ============================================================
# EXCLUDED BASE ASSETS
# ============================================================

EXCLUDED_BASES = {
    "BTC",
    "ETH",
}


# ============================================================
# SIGNAL HANDLERS
# ============================================================

def handle_shutdown(signum, frame):

    log.warning(
        "Shutdown signal received: %s",
        signum
    )

    SHUTDOWN_EVENT.set()


try:
    signal.signal(
        signal.SIGTERM,
        handle_shutdown
    )

    signal.signal(
        signal.SIGINT,
        handle_shutdown
    )

except Exception:
    pass


# ============================================================
# INDICATORS
# ============================================================

def calculate_indicators(df):

    df = df.copy()

    if df.empty:
        return df

    # --------------------------------------------------------
    # Bollinger Band 49
    # --------------------------------------------------------

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

    # --------------------------------------------------------
    # RSI 3
    # --------------------------------------------------------

    delta = df["close"].diff()

    gain = delta.clip(lower=0)

    loss = -delta.clip(upper=0)

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

    df["rsi"] = (
        100
        - (
            100
            / (1 + rs)
        )
    )

    # If there is no loss, RSI can mathematically be 100.
    df["rsi"] = df["rsi"].fillna(100.0)

    return df


# ============================================================
# BUY SIGNAL
# ============================================================

def entry_signal(df):

    """
    BUY:

    Latest CLOSED candle:

        Close < BB49 Lower
        AND
        RSI3 < 10
    """

    if df is None:
        return False

    if len(df) < BB_PERIOD:
        return False

    row = df.iloc[-1]

    try:

        close_price = float(
            row["close"]
        )

        bb_lower = float(
            row["bb_lower"]
        )

        rsi = float(
            row["rsi"]
        )

    except Exception:

        return False

    if pd.isna(bb_lower):
        return False

    if pd.isna(rsi):
        return False

    condition_bb = (
        close_price < bb_lower
    )

    condition_rsi = (
        rsi < RSI_BUY_LEVEL
    )

    if condition_bb and condition_rsi:

        log.info(
            "BUY SIGNAL → "
            "Close %.8f < BB49 Lower %.8f | "
            "RSI3 %.2f < %.2f",
            close_price,
            bb_lower,
            rsi,
            RSI_BUY_LEVEL
        )

        return True

    return False


# ============================================================
# SELL SIGNAL
# ============================================================

def sell_signal(df):

    """
    SELL:

    Previous CLOSED candle:
        RSI3 <= 80

    Current CLOSED candle:
        RSI3 > 80

    Therefore:
        RSI3 CROSS ABOVE 80
    """

    if df is None:
        return False

    if len(df) < 2:
        return False

    previous = df.iloc[-2]

    current = df.iloc[-1]

    try:

        previous_rsi = float(
            previous["rsi"]
        )

        current_rsi = float(
            current["rsi"]
        )

    except Exception:

        return False

    if pd.isna(previous_rsi):
        return False

    if pd.isna(current_rsi):
        return False

    crossed_above = (
        previous_rsi <= RSI_SELL_LEVEL
        and
        current_rsi > RSI_SELL_LEVEL
    )

    if crossed_above:

        log.info(
            "SELL SIGNAL → "
            "RSI3 CROSS ABOVE %.2f | "
            "%.2f → %.2f",
            RSI_SELL_LEVEL,
            previous_rsi,
            current_rsi
        )

        return True

    return False


# ============================================================
# LOAD EXCHANGE INFO
# ============================================================

def load_exchange_info():

    global exchange_info
    global symbol_info

    log.info(
        "Loading Binance exchange information..."
    )

    data = client.get_exchange_info()

    exchange_info = data

    new_symbol_info = {}

    eligible_count = 0

    for item in data.get("symbols", []):

        symbol = item.get("symbol")

        if not symbol:
            continue

        if item.get("status") != "TRADING":
            continue

        if item.get("quoteAsset") != "USDT":
            continue

        base_asset = item.get(
            "baseAsset",
            ""
        )

        if base_asset in STABLECOINS:
            continue

        if base_asset in EXCLUDED_BASES:
            continue

        new_symbol_info[symbol] = item

        eligible_count += 1

    with state_lock:

        symbol_info = new_symbol_info

    log.info(
        "Loaded %s eligible USDT ALT symbols",
        eligible_count
    )


# ============================================================
# TOP SYMBOLS
# ============================================================

def update_top_symbols():

    global top_symbols

    try:

        log.info(
            "Updating top ALT symbols..."
        )

        tickers = client.get_ticker()

        candidates = []

        with state_lock:
            available_symbols = set(
                symbol_info.keys()
            )

        for ticker in tickers:

            symbol = ticker.get(
                "symbol"
            )

            if symbol not in available_symbols:
                continue

            try:

                quote_volume = float(
                    ticker.get(
                        "quoteVolume",
                        0
                    )
                )

            except Exception:

                quote_volume = 0.0

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
            item[0]
            for item in candidates[
                :TOP_SYMBOLS
            ]
        ]

        with state_lock:

            top_symbols = selected

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
            "Failed to update top symbols: %s",
            e
        )


# ============================================================
# INITIAL CANDLES
# ============================================================

def load_initial_candles():

    with state_lock:

        symbols = list(
            top_symbols
        )

    log.info(
        "Loading initial 5m candle data for %s symbols...",
        len(symbols)
    )

    success = 0

    for symbol in symbols:

        if SHUTDOWN_EVENT.is_set():
            break

        try:

            klines = client.get_klines(
                symbol=symbol,
                interval=TIMEFRAME,
                limit=CANDLE_LIMIT
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
                    "close_time": int(k[6]),
                })

            if not rows:
                continue

            df = pd.DataFrame(rows)

            # ------------------------------------------------
            # Remove currently OPEN candle
            # ------------------------------------------------

            now_ms = int(
                time.time() * 1000
            )

            df = df[
                df["close_time"] <= now_ms
            ].copy()

            if df.empty:
                continue

            df = df.tail(
                CANDLE_LIMIT
            ).reset_index(
                drop=True
            )

            df = calculate_indicators(
                df
            )

            with state_lock:

                candle_data[symbol] = df

                last_closed_candle[symbol] = int(
                    df.iloc[-1]["open_time"]
                )

            success += 1

        except BinanceAPIException as e:

            log.warning(
                "Initial candle Binance error %s: %s",
                symbol,
                e
            )

        except Exception as e:

            log.warning(
                "Initial candle load failed %s: %s",
                symbol,
                e
            )

        time.sleep(0.05)

    log.info(
        "Initial candle loading complete: %s symbols",
        success
    )


# ============================================================
# SYMBOL FILTERS
# ============================================================

def get_symbol_filters(symbol):

    with state_lock:

        info = symbol_info.get(
            symbol
        )

    if not info:
        return None

    result = {}

    for item in info.get(
        "filters",
        []
    ):

        filter_type = item.get(
            "filterType"
        )

        if filter_type == "LOT_SIZE":

            result["min_qty"] = float(
                item["minQty"]
            )

            result["max_qty"] = float(
                item["maxQty"]
            )

            result["step_size"] = float(
                item["stepSize"]
            )

        elif filter_type == "MIN_NOTIONAL":

            result["min_notional"] = float(
                item.get(
                    "minNotional",
                    0
                )
            )

        elif filter_type == "NOTIONAL":

            result["min_notional"] = float(
                item.get(
                    "minNotional",
                    0
                )
            )

    return result


# ============================================================
# ROUND QUANTITY
# ============================================================

def round_step_size(
    quantity,
    step_size
):

    if step_size <= 0:
        return quantity

    quantity_decimal = Decimal(
        str(quantity)
    )

    step_decimal = Decimal(
        str(step_size)
    )

    rounded = (
        quantity_decimal
        / step_decimal
    ).to_integral_value(
        rounding=ROUND_DOWN
    ) * step_decimal

    return float(
        rounded
    )


# ============================================================
# BUY SYMBOL
# ============================================================

def buy_symbol(symbol):

    try:

        now = time.time()

        # ----------------------------------------------------
        # Prevent repeated BUY attempts
        # ----------------------------------------------------

        with state_lock:

            if symbol in open_positions:
                return False

            previous_attempt = (
                last_buy_attempt.get(
                    symbol,
                    0
                )
            )

            if now - previous_attempt < 30:
                return False

            last_buy_attempt[symbol] = now

        log.info(
            "Executing MARKET BUY → %s | %.2f USDT",
            symbol,
            TRADE_AMOUNT_USDT
        )

        order = client.order_market_buy(
            symbol=symbol,
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
                "BUY returned zero quantity → %s",
                symbol
            )

            return False

        fills = order.get(
            "fills",
            []
        )

        total_quote = 0.0
        total_qty = 0.0

        for fill in fills:

            price = float(
                fill["price"]
            )

            qty = float(
                fill["qty"]
            )

            total_quote += (
                price * qty
            )

            total_qty += qty

        if total_qty > 0:

            entry_price = (
                total_quote
                / total_qty
            )

        else:

            quote_qty = float(
                order.get(
                    "cummulativeQuoteQty",
                    0
                )
            )

            if quote_qty <= 0:

                log.error(
                    "Cannot determine BUY price → %s",
                    symbol
                )

                return False

            entry_price = (
                quote_qty
                / executed_qty
            )

        with state_lock:

            open_positions[symbol] = {
                "entry_price": entry_price,
                "quantity": executed_qty,
                "buy_time": int(
                    time.time()
                ),
            }

        log.info(
            "BUY SUCCESS → %s | "
            "Qty %.8f | Entry %.8f",
            symbol,
            executed_qty,
            entry_price
        )

        return True

    except BinanceAPIException as e:

        log.error(
            "BUY Binance API error %s: %s",
            symbol,
            e
        )

        return False

    except Exception as e:

        log.exception(
            "BUY error %s: %s",
            symbol,
            e
        )

        return False


# ============================================================
# SELL SYMBOL
# ============================================================

def sell_symbol(
    symbol,
    reason
):

    try:

        now = time.time()

        with state_lock:

            previous_attempt = (
                last_sell_attempt.get(
                    symbol,
                    0
                )
            )

            if now - previous_attempt < 30:
                return False

            last_sell_attempt[symbol] = now

        with state_lock:

            info = symbol_info.get(
                symbol
            )

        if not info:
            return False

        asset = info["baseAsset"]

        account = client.get_asset_balance(
            asset=asset
        )

        if not account:

            log.warning(
                "No balance found → %s",
                asset
            )

            return False

        free_balance = float(
            account.get(
                "free",
                0
            )
        )

        if free_balance <= 0:

            log.warning(
                "No free balance → %s",
                symbol
            )

            with state_lock:
                open_positions.pop(
                    symbol,
                    None
                )

            return False

        quantity = (
            free_balance
            * SELL_BALANCE_BUFFER
        )

        filters = get_symbol_filters(
            symbol
        )

        if not filters:

            log.warning(
                "No filters found → %s",
                symbol
            )

            return False

        step_size = filters.get(
            "step_size",
            0
        )

        min_qty = filters.get(
            "min_qty",
            0
        )

        min_notional = filters.get(
            "min_notional",
            0
        )

        quantity = round_step_size(
            quantity,
            step_size
        )

        if quantity <= 0:

            log.warning(
                "Sell quantity is zero → %s",
                symbol
            )

            return False

        if quantity < min_qty:

            log.warning(
                "Quantity below minimum → %s | "
                "%.12f < %.12f",
                symbol,
                quantity,
                min_qty
            )

            return False

        # ----------------------------------------------------
        # Current price
        # ----------------------------------------------------

        ticker = client.get_symbol_ticker(
            symbol=symbol
        )

        current_price = float(
            ticker["price"]
        )

        notional = (
            quantity
            * current_price
        )

        if (
            min_notional > 0
            and
            notional < min_notional
        ):

            log.warning(
                "Sell notional too small → %s | "
                "%.8f < %.8f",
                symbol,
                notional,
                min_notional
            )

            return False

        log.info(
            "SELL MARKET → %s | "
            "Qty %.12f | Reason: %s",
            symbol,
            quantity,
            reason
        )

        order = client.order_market_sell(
            symbol=symbol,
            quantity=quantity
        )

        log.info(
            "SELL SUCCESS → %s",
            symbol
        )

        with state_lock:

            open_positions.pop(
                symbol,
                None
            )

        return True

    except BinanceAPIException as e:

        log.error(
            "SELL Binance API error %s: %s",
            symbol,
            e
        )

        return False

    except Exception as e:

        log.exception(
            "SELL error %s: %s",
            symbol,
            e
        )

        return False


# ============================================================
# PROCESS CLOSED KLINE
# ============================================================

def process_kline(
    symbol,
    kline
):

    try:

        if not kline:
            return

        # ----------------------------------------------------
        # ONLY CLOSED CANDLE
        # ----------------------------------------------------

        candle_closed = bool(
            kline.get(
                "x",
                False
            )
        )

        if not candle_closed:
            return

        open_time = int(
            kline["t"]
        )

        close_time = int(
            kline["T"]
        )

        # ----------------------------------------------------
        # DUPLICATE CANDLE PROTECTION
        # ----------------------------------------------------

        with state_lock:

            previous_candle = (
                last_closed_candle.get(
                    symbol
                )
            )

            if previous_candle == open_time:
                return

        row = {
            "open_time": open_time,
            "open": float(kline["o"]),
            "high": float(kline["h"]),
            "low": float(kline["l"]),
            "close": float(kline["c"]),
            "volume": float(kline["v"]),
            "close_time": close_time,
        }

        with state_lock:

            old_df = candle_data.get(
                symbol,
                pd.DataFrame()
            )

        # ----------------------------------------------------
        # Handle restart / missing history
        # ----------------------------------------------------

        if old_df.empty:

            new_df = pd.DataFrame(
                [row]
            )

        else:

            # If same candle somehow exists, replace it.
            existing = old_df[
                old_df["open_time"]
                == open_time
            ]

            if not existing.empty:

                old_df = old_df[
                    old_df["open_time"]
                    != open_time
                ].copy()

            new_df = pd.concat(
                [
                    old_df,
                    pd.DataFrame([row])
                ],
                ignore_index=True
            )

        new_df = (
            new_df
            .drop_duplicates(
                subset=["open_time"],
                keep="last"
            )
            .sort_values(
                "open_time"
            )
            .tail(CANDLE_LIMIT)
            .reset_index(
                drop=True
            )
        )

        new_df = calculate_indicators(
            new_df
        )

        with state_lock:

            candle_data[symbol] = new_df

            last_closed_candle[symbol] = open_time

            position_exists = (
                symbol in open_positions
            )

        # ====================================================
        # SELL FIRST
        # ====================================================

        if position_exists:

            if sell_signal(
                new_df
            ):

                sell_symbol(
                    symbol,
                    "RSI3 CROSS ABOVE 80"
                )

            return

        # ====================================================
        # BUY
        # ====================================================

        if entry_signal(
            new_df
        ):

            buy_symbol(
                symbol
            )

    except Exception as e:

        log.exception(
            "process_kline error %s: %s",
            symbol,
            e
        )


# ============================================================
# WEBSOCKET MESSAGE
# ============================================================

def process_ws_message(
    message
):

    try:

        if not message:
            return

        data = json.loads(
            message
        )

        # Combined stream:
        #
        # {
        #   "stream": "...",
        #   "data": {...}
        # }

        payload = data.get(
            "data",
            data
        )

        event_type = payload.get(
            "e"
        )

        if event_type != "kline":
            return

        symbol = payload.get(
            "s"
        )

        kline = payload.get(
            "k"
        )

        if not symbol:
            return

        if not kline:
            return

        process_kline(
            symbol,
            kline
        )

    except json.JSONDecodeError:

        log.warning(
            "Invalid WebSocket JSON received."
        )

    except Exception as e:

        log.exception(
            "WebSocket message error: %s",
            e
        )


# ============================================================
# WEBSOCKET URL
# ============================================================

def make_stream_url(
    symbols
):

    streams = "/".join(
        f"{symbol.lower()}@kline_{TIMEFRAME}"
        for symbol in symbols
    )

    return (
        "wss://stream.binance.com:9443/stream?streams="
        + streams
    )


# ============================================================
# WEBSOCKET LOOP
# ============================================================

def websocket_loop():

    while not SHUTDOWN_EVENT.is_set():

        ws = None

        try:

            with state_lock:

                symbols = list(
                    top_symbols
                )

            if not symbols:

                log.warning(
                    "No symbols available "
                    "for WebSocket."
                )

                SHUTDOWN_EVENT.wait(
                    10
                )

                continue

            url = make_stream_url(
                symbols
            )

            log.info(
                "Opening combined WebSocket "
                "for %s symbols",
                len(symbols)
            )

            ws = websocket.WebSocketApp(
                url,

                on_message=lambda ws, message:
                    process_ws_message(
                        message
                    ),

                on_error=lambda ws, error:
                    log.error(
                        "WebSocket error: %s",
                        error
                    ),

                on_close=lambda ws, code, msg:
                    log.warning(
                        "WebSocket closed: "
                        "%s %s",
                        code,
                        msg
                    ),

                on_open=lambda ws:
                    log.info(
                        "WebSocket connected successfully."
                    ),
            )

            ws.run_forever(
                ping_interval=60,
                ping_timeout=20,
                reconnect=5
            )

        except Exception as e:

            log.exception(
                "WebSocket loop error: %s",
                e
            )

        finally:

            try:

                if ws is not None:

                    ws.close()

            except Exception:
                pass

        if not SHUTDOWN_EVENT.is_set():

            log.info(
                "WebSocket reconnecting in 5 seconds..."
            )

            SHUTDOWN_EVENT.wait(
                5
            )

    log.info(
        "WebSocket loop stopped."
    )


# ============================================================
# POSITION RECOVERY
# ============================================================

def recover_positions():

    log.info(
        "Checking existing Binance balances..."
    )

    recovered = 0

    try:

        account = client.get_account()

        balances = account.get(
            "balances",
            []
        )

        with state_lock:

            available_symbols = dict(
                symbol_info
            )

        for balance in balances:

            asset = balance.get(
                "asset"
            )

            if not asset:
                continue

            if asset in STABLECOINS:
                continue

            if asset in EXCLUDED_BASES:
                continue

            try:

                free = float(
                    balance.get(
                        "free",
                        0
                    )
                )

            except Exception:

                continue

            if free <= 0:
                continue

            symbol = (
                asset
                + "USDT"
            )

            if symbol not in available_symbols:
                continue

            try:

                ticker = client.get_symbol_ticker(
                    symbol=symbol
                )

                current_price = float(
                    ticker["price"]
                )

            except Exception:

                continue

            with state_lock:

                # Don't overwrite a position
                # that was already created.
                if symbol in open_positions:
                    continue

                open_positions[symbol] = {
                    "entry_price": current_price,
                    "quantity": free,
                    "buy_time": int(
                        time.time()
                    ),
                    "recovered": True,
                }

            recovered += 1

            log.info(
                "Recovered position → %s | "
                "Qty %.12f",
                symbol,
                free
            )

    except BinanceAPIException as e:

        log.error(
            "Position recovery Binance error: %s",
            e
        )

    except Exception as e:

        log.exception(
            "Position recovery error: %s",
            e
        )

    log.info(
        "Position recovery complete → "
        "%s positions recovered",
        recovered
    )


# ============================================================
# POSITION SAFETY CHECK
# ============================================================

def check_position_once(
    symbol
):

    try:

        klines = client.get_klines(
            symbol=symbol,
            interval=TIMEFRAME,
            limit=CANDLE_LIMIT
        )

        if not klines:
            return

        rows = []

        for k in klines:

            rows.append({
                "open_time": int(k[0]),
                "open": float(k[1]),
                "high": float(k[2]),
                "low": float(k[3]),
                "close": float(k[4]),
                "volume": float(k[5]),
                "close_time": int(k[6]),
            })

        df = pd.DataFrame(
            rows
        )

        now_ms = int(
            time.time() * 1000
        )

        # ----------------------------------------------------
        # ONLY CLOSED CANDLES
        # ----------------------------------------------------

        df = df[
            df["close_time"] <= now_ms
        ].copy()

        if len(df) < 2:
            return

        df = df.tail(
            CANDLE_LIMIT
        ).reset_index(
            drop=True
        )

        df = calculate_indicators(
            df
        )

        latest_open_time = int(
            df.iloc[-1]["open_time"]
        )

        # ----------------------------------------------------
        # Prevent processing same candle twice
        # ----------------------------------------------------

        with state_lock:

            already_processed = (
                last_closed_candle.get(
                    symbol
                )
            )

        if already_processed == latest_open_time:
            return

        with state_lock:

            last_closed_candle[symbol] = (
                latest_open_time
            )

        # ----------------------------------------------------
        # SELL
        # ----------------------------------------------------

        if sell_signal(df):

            sell_symbol(
                symbol,
                "RSI3 CROSS ABOVE 80 - SAFETY CHECK"
            )

    except BinanceAPIException as e:

        log.warning(
            "Safety Binance error %s: %s",
            symbol,
            e
        )

    except Exception as e:

        log.warning(
            "Safety check failed %s: %s",
            symbol,
            e
        )


# ============================================================
# POSITION SAFETY LOOP
# ============================================================

def position_safety_loop():

    while not SHUTDOWN_EVENT.is_set():

        try:

            with state_lock:

                positions = list(
                    open_positions.keys()
                )

            for symbol in positions:

                if SHUTDOWN_EVENT.is_set():
                    break

                check_position_once(
                    symbol
                )

                time.sleep(
                    0.15
                )

        except Exception as e:

            log.exception(
                "Position safety loop error: %s",
                e
            )

        SHUTDOWN_EVENT.wait(
            SAFETY_CHECK_SECONDS
        )

    log.info(
        "Position safety loop stopped."
    )


# ============================================================
# SYMBOL REFRESH LOOP
# ============================================================

def symbol_refresh_loop():

    while not SHUTDOWN_EVENT.is_set():

        try:

            update_top_symbols()

        except Exception as e:

            log.exception(
                "Symbol refresh error: %s",
                e
            )

        SHUTDOWN_EVENT.wait(
            SYMBOL_REFRESH_SECONDS
        )

    log.info(
        "Symbol refresh loop stopped."
    )


# ============================================================
# FLASK ROUTES
# ============================================================

@app.route("/")
def home():

    with state_lock:

        symbols_count = len(
            top_symbols
        )

        positions_count = len(
            open_positions
        )

    return jsonify({

        "status": "running",

        "bot": (
            "BB49(2) + RSI3 "
            "BINANCE SPOT BOT"
        ),

        "timeframe": TIMEFRAME,

        "trade_amount_usdt":
            TRADE_AMOUNT_USDT,

        "buy_rule": (
            "CLOSED 5m candle: "
            "Close < BB49 Lower "
            "AND RSI3 < 10"
        ),

        "sell_rule": (
            "CLOSED 5m candle: "
            "Previous RSI3 <= 80 "
            "AND Current RSI3 > 80"
        ),

        "stop_loss":
            "DISABLED",

        "trailing_stop":
            "DISABLED",

        "top_symbols":
            symbols_count,

        "open_positions":
            positions_count,

    })


@app.route("/health")
def health():

    return jsonify({

        "status": "healthy",

        "timestamp":
            int(time.time()),

        "shutdown":
            SHUTDOWN_EVENT.is_set(),

    })


# ============================================================
# START BOT
# ============================================================

def start_bot():

    log.info(
        "=" * 70
    )

    log.info(
        "BB49(2) + RSI3 BINANCE SPOT BOT STARTING"
    )

    log.info(
        "=" * 70
    )

    log.info(
        "BUY RULE → "
        "CLOSE BELOW BB49 LOWER + RSI3 < 10"
    )

    log.info(
        "SELL RULE → "
        "CLOSED 5m RSI3 CROSS ABOVE 80"
    )

    log.info(
        "TRADE AMOUNT = %.2f USDT",
        TRADE_AMOUNT_USDT
    )

    log.info(
        "STOP LOSS = DISABLED"
    )

    log.info(
        "TRAILING STOP = DISABLED"
    )

    log.info(
        "RSI SELL CROSS LEVEL = %.2f",
        RSI_SELL_LEVEL
    )

    log.info(
        "=" * 70
    )

    # --------------------------------------------------------
    # 1. Exchange information
    # --------------------------------------------------------

    load_exchange_info()

    # --------------------------------------------------------
    # 2. Select top symbols
    # --------------------------------------------------------

    update_top_symbols()

    # --------------------------------------------------------
    # 3. Load initial candles
    # --------------------------------------------------------

    load_initial_candles()

    # --------------------------------------------------------
    # 4. Recover existing positions
    # --------------------------------------------------------

    recover_positions()

    # --------------------------------------------------------
    # 5. Start Flask
    # --------------------------------------------------------

    port = int(
        os.environ.get(
            "PORT",
            "10000"
        )
    )

    flask_thread = threading.Thread(
        target=lambda: app.run(
            host="0.0.0.0",
            port=port,
            debug=False,
            use_reloader=False,
            threaded=True
        ),
        name="FlaskThread",
        daemon=True
    )

    flask_thread.start()

    # --------------------------------------------------------
    # 6. Start WebSocket
    # --------------------------------------------------------

    websocket_thread = threading.Thread(
        target=websocket_loop,
        name="WebSocketThread",
        daemon=True
    )

    websocket_thread.start()

    # --------------------------------------------------------
    # 7. Start symbol refresh
    # --------------------------------------------------------

    refresh_thread = threading.Thread(
        target=symbol_refresh_loop,
        name="SymbolRefreshThread",
        daemon=True
    )

    refresh_thread.start()

    # --------------------------------------------------------
    # 8. Start safety monitor
    # --------------------------------------------------------

    safety_thread = threading.Thread(
        target=position_safety_loop,
        name="SafetyThread",
        daemon=True
    )

    safety_thread.start()

    log.info(
        "All bot threads started successfully."
    )

    log.info(
        "BOT IS RUNNING."
    )

    # --------------------------------------------------------
    # Keep main process alive
    # --------------------------------------------------------

    try:

        while not SHUTDOWN_EVENT.is_set():

            time.sleep(5)

    except KeyboardInterrupt:

        log.info(
            "Keyboard interrupt received."
        )

        SHUTDOWN_EVENT.set()

    finally:

        log.info(
            "Bot shutdown requested."
        )

        # Give daemon threads a little time
        SHUTDOWN_EVENT.wait(2)

        log.info(
            "Bot stopped."
        )


# ============================================================
# MAIN
# ============================================================

if __name__ == "__main__":

    start_bot()
