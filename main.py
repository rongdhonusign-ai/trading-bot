import os
import time
import json
import hmac
import hashlib
import logging
import threading
from decimal import Decimal, ROUND_DOWN
from urllib.parse import urlencode

import requests
import websocket

from flask import Flask, jsonify


# ============================================================
# CONFIG
# ============================================================

API_KEY = os.getenv("BINANCE_API_KEY", "").strip()
API_SECRET = os.getenv("BINANCE_API_SECRET", "").strip()

BASE_URL = "https://api.binance.com"
WS_BASE_URL = "wss://stream.binance.com:9443/stream?streams="

TIMEFRAME = "5m"

TOP_SYMBOLS = 150
GROUP_SIZE = 50

BUY_USDT = Decimal("15")

RSI_FAST_PERIOD = 3
RSI_SLOW_PERIOD = 50

BUY_RSI_SLOW_MIN = Decimal("52")
BUY_RSI_FAST_MAX = Decimal("10")

SELL_RSI_LEVEL = Decimal("80")

HISTORY_LIMIT = 120

# কতবার REST request retry করবে
REST_RETRIES = 3

# WebSocket reconnect delay
WS_RECONNECT_DELAY = 5

# WebSocket ping
WS_PING_INTERVAL = 30
WS_PING_TIMEOUT = 15

# Top 150 refresh interval
# 30 মিনিটে একবার নতুন Top 150 নির্ধারণ হবে।
TOP_SYMBOL_REFRESH_SECONDS = 30 * 60

# TRUE করলে কোনো real order যাবে না
# LIVE trading করতে FALSE রাখতে হবে
DRY_RUN = os.getenv("DRY_RUN", "false").lower() == "true"

# API request-এর মধ্যে অতিরিক্ত নিরাপত্তা delay
REST_DELAY = 0.12

# Binance-এর সাধারণ stablecoins
STABLECOINS = {
    "USDT",
    "USDC",
    "FDUSD",
    "BUSD",
    "TUSD",
    "DAI",
    "USDP",
    "PYUSD",
    "EUR",
    "EURI",
    "AEUR",
    "GBP",
    "TRY",
    "BRL",
    "UAH",
    "RUB",
    "PLN",
    "ARS",
    "ZAR",
    "NGN",
    "RON",
    "MXN",
    "IDRT",
    "BIDR",
    "AUD",
    "CAD",
    "CHF",
    "JPY",
    "CZK",
    "DKK",
    "SEK",
    "NOK",
}

EXCLUDED_BASE_ASSETS = {
    "BTC",
    "ETH",
} | STABLECOINS


# ============================================================
# LOGGING
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

log = logging.getLogger("RSI_BOT")


# ============================================================
# FLASK
# ============================================================

app = Flask(__name__)


@app.route("/")
def home():
    return jsonify(
        {
            "status": "running",
            "bot": "Binance Spot RSI3/RSI50 Bot",
            "timeframe": TIMEFRAME,
            "top_symbols": TOP_SYMBOLS,
            "buy_usdt": str(BUY_USDT),
            "buy_rule": "RSI50 > 52 AND RSI3 < 10",
            "sell_rule": "RSI3 crosses above 80",
            "dry_run": DRY_RUN,
        }
    )


@app.route("/health")
def health():
    return jsonify(
        {
            "status": "healthy",
            "bot_running": bot_started.is_set(),
            "symbols": len(symbols),
            "positions": len(positions),
            "websocket_threads": len(ws_threads),
        }
    )


# ============================================================
# GLOBAL STATE
# ============================================================

session = requests.Session()

session.headers.update(
    {
        "User-Agent": "Mozilla/5.0 Binance-RSI-Bot/1.0",
        "Accept": "application/json",
    }
)

state_lock = threading.RLock()

symbols = []
symbol_info = {}

# symbol -> {
#     "qty": Decimal,
#     "buy_order_id": ...,
#     "buy_time": ...
# }
positions = {}

# symbol -> {
#     "closes": [Decimal, ...],
#     "last_closed_time": int,
#     "rsi3": Decimal,
#     "rsi50": Decimal,
# }
market_data = {}

ws_threads = []

bot_started = threading.Event()

server_time_offset_ms = 0


# ============================================================
# TIME / SIGNATURE
# ============================================================

def local_timestamp_ms():
    return int(time.time() * 1000)


def server_timestamp_ms():
    return local_timestamp_ms() + server_time_offset_ms


def update_server_time():
    global server_time_offset_ms

    for attempt in range(REST_RETRIES):
        try:
            started = local_timestamp_ms()

            r = session.get(
                BASE_URL + "/api/v3/time",
                timeout=10,
            )

            ended = local_timestamp_ms()

            r.raise_for_status()

            data = r.json()
            server_time = int(data["serverTime"])

            midpoint = (started + ended) // 2

            server_time_offset_ms = server_time - midpoint

            log.info(
                "Binance server time offset: %d ms",
                server_time_offset_ms,
            )

            return True

        except Exception as e:
            log.warning(
                "Server time sync failed (%d/%d): %s",
                attempt + 1,
                REST_RETRIES,
                e,
            )

            time.sleep(1 + attempt)

    return False


def sign_params(params):
    query = urlencode(params, doseq=True)

    signature = hmac.new(
        API_SECRET.encode("utf-8"),
        query.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()

    return query + "&signature=" + signature


# ============================================================
# REST HELPERS
# ============================================================

def public_get(path, params=None):
    for attempt in range(REST_RETRIES):
        try:
            time.sleep(REST_DELAY)

            response = session.get(
                BASE_URL + path,
                params=params,
                timeout=15,
            )

            if response.status_code == 429:
                retry_after = response.headers.get("Retry-After")

                if retry_after:
                    try:
                        wait_seconds = float(retry_after)
                    except Exception:
                        wait_seconds = 5
                else:
                    wait_seconds = 5

                log.warning(
                    "Binance 429 rate limit. Sleeping %.2f seconds.",
                    wait_seconds,
                )

                time.sleep(wait_seconds)
                continue

            if response.status_code == 418:
                log.error(
                    "Binance returned HTTP 418 IP BAN. "
                    "Stopping REST requests."
                )

                raise RuntimeError("BINANCE_IP_BANNED")

            response.raise_for_status()

            return response.json()

        except Exception as e:
            log.warning(
                "Public REST error %d/%d: %s",
                attempt + 1,
                REST_RETRIES,
                e,
            )

            if attempt < REST_RETRIES - 1:
                time.sleep(2 + attempt * 2)

    raise RuntimeError(f"REST failed: {path}")


def signed_request(method, path, params=None):
    if not API_KEY or not API_SECRET:
        raise RuntimeError("BINANCE_API_KEY / BINANCE_API_SECRET missing")

    params = dict(params or {})

    params["timestamp"] = server_timestamp_ms()
    params["recvWindow"] = 5000

    query_string = sign_params(params)

    headers = {
        "X-MBX-APIKEY": API_KEY,
        "Content-Type": "application/x-www-form-urlencoded",
    }

    url = BASE_URL + path

    for attempt in range(REST_RETRIES):
        try:
            time.sleep(REST_DELAY)

            if method == "GET":
                response = session.get(
                    url + "?" + query_string,
                    headers=headers,
                    timeout=15,
                )

            elif method == "POST":
                response = session.post(
                    url,
                    data=query_string,
                    headers=headers,
                    timeout=15,
                )

            elif method == "DELETE":
                response = session.delete(
                    url + "?" + query_string,
                    headers=headers,
                    timeout=15,
                )

            else:
                raise ValueError("Unsupported HTTP method")

            if response.status_code == 429:
                log.warning(
                    "Signed request received 429. "
                    "Backing off before retry."
                )

                time.sleep(5 + attempt * 5)
                continue

            if response.status_code == 418:
                log.error(
                    "BINANCE HTTP 418. "
                    "Do NOT continue hammering the API."
                )

                raise RuntimeError("BINANCE_IP_BANNED")

            if response.status_code >= 500:
                # 5XX order response can have unknown execution status.
                # Do not blindly duplicate an order.
                raise RuntimeError(
                    f"Binance server error {response.status_code}"
                )

            if response.status_code >= 400:
                try:
                    error_data = response.json()
                except Exception:
                    error_data = response.text

                raise RuntimeError(
                    f"Binance API error {response.status_code}: "
                    f"{error_data}"
                )

            return response.json()

        except Exception as e:
            log.warning(
                "Signed REST error %d/%d: %s",
                attempt + 1,
                REST_RETRIES,
                e,
            )

            if attempt < REST_RETRIES - 1:
                time.sleep(2 + attempt * 2)

    raise RuntimeError(f"Signed REST failed: {path}")


# ============================================================
# EXCHANGE INFO
# ============================================================

def load_exchange_info():
    global symbol_info

    data = public_get("/api/v3/exchangeInfo")

    result = {}

    for item in data.get("symbols", []):

        symbol = item.get("symbol")

        if not symbol:
            continue

        if item.get("status") != "TRADING":
            continue

        if item.get("quoteAsset") != "USDT":
            continue

        if item.get("isSpotTradingAllowed") is False:
            continue

        base_asset = item.get("baseAsset", "")

        if base_asset in EXCLUDED_BASE_ASSETS:
            continue

        filters = {}

        for f in item.get("filters", []):
            filters[f["filterType"]] = f

        result[symbol] = {
            "symbol": symbol,
            "baseAsset": base_asset,
            "quoteAsset": item.get("quoteAsset"),
            "filters": filters,
        }

    symbol_info = result

    log.info(
        "Eligible USDT spot symbols: %d",
        len(symbol_info),
    )


# ============================================================
# TOP 150
# ============================================================

def select_top_symbols():
    global symbols

    ticker_data = public_get("/api/v3/ticker/24hr")

    eligible = []

    for item in ticker_data:

        symbol = item.get("symbol")

        if symbol not in symbol_info:
            continue

        try:
            quote_volume = Decimal(item.get("quoteVolume", "0"))
        except Exception:
            continue

        if quote_volume <= 0:
            continue

        eligible.append(
            (
                symbol,
                quote_volume,
            )
        )

    eligible.sort(
        key=lambda x: x[1],
        reverse=True,
    )

    selected = [
        x[0]
        for x in eligible[:TOP_SYMBOLS]
    ]

    symbols = selected

    log.info(
        "Selected TOP %d symbols",
        len(symbols),
    )

    log.info(
        "Top symbols: %s",
        ", ".join(symbols),
    )

    return symbols


# ============================================================
# DECIMAL / QUANTITY HELPERS
# ============================================================

def decimal_from_filter(filters, filter_name, key, default="0"):
    f = filters.get(filter_name)

    if not f:
        return Decimal(default)

    value = f.get(key)

    if value is None:
        return Decimal(default)

    return Decimal(str(value))


def floor_to_step(value, step):
    value = Decimal(str(value))
    step = Decimal(str(step))

    if step <= 0:
        return value

    return (
        value / step
    ).to_integral_value(
        rounding=ROUND_DOWN
    ) * step


def format_decimal(value):
    value = Decimal(str(value))

    text = format(
        value,
        "f",
    )

    if "." in text:
        text = text.rstrip("0").rstrip(".")

    return text if text else "0"


def get_sell_step(symbol):
    info = symbol_info[symbol]

    filters = info["filters"]

    market_lot = filters.get("MARKET_LOT_SIZE")

    if market_lot:
        step = Decimal(
            market_lot.get(
                "stepSize",
                "0",
            )
        )

        if step > 0:
            return step

    lot = filters.get("LOT_SIZE")

    if lot:
        return Decimal(
            lot.get(
                "stepSize",
                "0",
            )
        )

    return Decimal("0")


def validate_notional_for_buy(symbol):
    info = symbol_info[symbol]

    filters = info["filters"]

    minimum = Decimal("0")

    if "NOTIONAL" in filters:
        minimum = Decimal(
            filters["NOTIONAL"].get(
                "minNotional",
                "0",
            )
        )

    elif "MIN_NOTIONAL" in filters:
        minimum = Decimal(
            filters["MIN_NOTIONAL"].get(
                "minNotional",
                "0",
            )
        )

    if minimum > 0 and BUY_USDT < minimum:
        log.warning(
            "%s | Buy %.2f USDT is below minimum notional %.2f",
            symbol,
            BUY_USDT,
            minimum,
        )

        return False

    return True


# ============================================================
# RSI - WILDER
# ============================================================

def calculate_wilder_rsi(closes, period):
    """
    Wilder RSI calculation.

    This uses:
        Initial average gain/loss = SMA
        Next values = Wilder RMA:
            avg = (previous_avg * (period - 1) + current) / period

    Returns the RSI value of the latest closed candle.
    """

    if len(closes) < period + 1:
        return None

    values = [
        Decimal(str(x))
        for x in closes
    ]

    gains = []
    losses = []

    for i in range(1, len(values)):
        change = values[i] - values[i - 1]

        if change > 0:
            gains.append(change)
            losses.append(Decimal("0"))

        else:
            gains.append(Decimal("0"))
            losses.append(abs(change))

    if len(gains) < period:
        return None

    avg_gain = (
        sum(gains[:period])
        / Decimal(period)
    )

    avg_loss = (
        sum(losses[:period])
        / Decimal(period)
    )

    for i in range(period, len(gains)):

        avg_gain = (
            (
                avg_gain
                * Decimal(period - 1)
            )
            + gains[i]
        ) / Decimal(period)

        avg_loss = (
            (
                avg_loss
                * Decimal(period - 1)
            )
            + losses[i]
        ) / Decimal(period)

    if avg_loss == 0:
        if avg_gain == 0:
            return Decimal("50")

        return Decimal("100")

    rs = avg_gain / avg_loss

    rsi = Decimal("100") - (
        Decimal("100")
        / (Decimal("1") + rs)
    )

    return rsi


# ============================================================
# INITIAL HISTORICAL DATA
# ============================================================

def load_initial_klines(symbol):
    """
    Load enough 5m closed candles to calculate RSI(50).

    We request 120 candles.
    The last candle may still be open, so it is removed.
    """

    data = public_get(
        "/api/v3/klines",
        {
            "symbol": symbol,
            "interval": TIMEFRAME,
            "limit": HISTORY_LIMIT,
        },
    )

    closes = []

    now_ms = server_timestamp_ms()

    for kline in data:

        open_time = int(kline[0])
        close_time = int(kline[6])

        close_price = Decimal(str(kline[4]))

        # Only closed candles
        if close_time < now_ms:
            closes.append(close_price)

    if len(closes) < RSI_SLOW_PERIOD + 2:
        raise RuntimeError(
            f"{symbol}: insufficient historical candles"
        )

    with state_lock:
        market_data[symbol] = {
            "closes": closes[-HISTORY_LIMIT:],
            "last_closed_time": 0,
            "rsi3": None,
            "rsi50": None,
        }

    return True


def load_all_initial_data():
    log.info(
        "Loading initial %d-candle history for %d symbols...",
        HISTORY_LIMIT,
        len(symbols),
    )

    success = 0

    for index, symbol in enumerate(symbols, start=1):

        try:
            load_initial_klines(symbol)

            success += 1

            if index % 10 == 0:
                log.info(
                    "Historical data loaded: %d/%d",
                    index,
                    len(symbols),
                )

        except Exception as e:
            log.error(
                "%s | Initial history error: %s",
                symbol,
                e,
            )

        # Keep REST request rate conservative
        time.sleep(0.15)

    log.info(
        "Historical initialization complete: %d/%d",
        success,
        len(symbols),
    )

    if success < len(symbols) * 0.90:
        raise RuntimeError(
            "Too many symbols failed initial history loading"
        )


# ============================================================
# BUY / SELL POSITION STATE
# ============================================================

def has_position(symbol):
    with state_lock:
        return symbol in positions


def save_position(symbol, qty, order_id):
    with state_lock:
        positions[symbol] = {
            "qty": str(qty),
            "buy_order_id": order_id,
            "buy_time": int(time.time()),
        }


def remove_position(symbol):
    with state_lock:
        positions.pop(symbol, None)


# ============================================================
# MARKET BUY
# ============================================================

def market_buy(symbol):
    if has_position(symbol):
        log.info(
            "%s | BUY skipped - position already exists",
            symbol,
        )
        return False

    if not validate_notional_for_buy(symbol):
        return False

    client_order_id = (
        "RSI3B" + str(int(time.time() * 1000))[-18:]
    )

    if DRY_RUN:
        log.warning(
            "%s | DRY RUN BUY %.2f USDT",
            symbol,
            BUY_USDT,
        )

        # Fake quantity for state testing is intentionally NOT saved.
        # This prevents accidental fake position from becoming a real sell.
        return True

    params = {
        "symbol": symbol,
        "side": "BUY",
        "type": "MARKET",
        "quoteOrderQty": format_decimal(BUY_USDT),
        "newOrderRespType": "FULL",
        "newClientOrderId": client_order_id,
    }

    log.warning(
        "%s | MARKET BUY %.2f USDT",
        symbol,
        BUY_USDT,
    )

    try:
        result = signed_request(
            "POST",
            "/api/v3/order",
            params,
        )

    except Exception as e:
        log.error(
            "%s | BUY order failed: %s",
            symbol,
            e,
        )
        return False

    executed_qty = Decimal(
        str(
            result.get(
                "executedQty",
                "0",
            )
        )
    )

    if executed_qty <= 0:

        log.error(
            "%s | BUY returned zero executed quantity",
            symbol,
        )

        return False

    # If commission was charged in BASE asset,
    # subtract it from the sellable quantity.
    base_asset = symbol_info[symbol]["baseAsset"]

    base_commission = Decimal("0")

    for fill in result.get("fills", []):

        commission_asset = fill.get(
            "commissionAsset"
        )

        if commission_asset == base_asset:

            base_commission += Decimal(
                str(
                    fill.get(
                        "commission",
                        "0",
                    )
                )
            )

    sellable_qty = (
        executed_qty
        - base_commission
    )

    if sellable_qty <= 0:
        log.error(
            "%s | No sellable quantity after commission",
            symbol,
        )
        return False

    step = get_sell_step(symbol)

    sellable_qty = floor_to_step(
        sellable_qty,
        step,
    )

    if sellable_qty <= 0:
        log.error(
            "%s | Quantity became zero after LOT_SIZE rounding",
            symbol,
        )
        return False

    order_id = result.get("orderId")

    save_position(
        symbol,
        sellable_qty,
        order_id,
    )

    log.warning(
        "%s | BUY FILLED | qty=%s | orderId=%s",
        symbol,
        format_decimal(sellable_qty),
        order_id,
    )

    return True


# ============================================================
# MARKET SELL
# ============================================================

def market_sell(symbol):
    with state_lock:
        position = positions.get(symbol)

    if not position:
        return False

    stored_qty = Decimal(
        str(position["qty"])
    )

    step = get_sell_step(symbol)

    sell_qty = floor_to_step(
        stored_qty,
        step,
    )

    if sell_qty <= 0:
        log.error(
            "%s | SELL quantity is zero",
            symbol,
        )
        return False

    client_order_id = (
        "RSI3S" + str(int(time.time() * 1000))[-18:]
    )

    if DRY_RUN:

        log.warning(
            "%s | DRY RUN SELL qty=%s",
            symbol,
            format_decimal(sell_qty),
        )

        return True

    params = {
        "symbol": symbol,
        "side": "SELL",
        "type": "MARKET",
        "quantity": format_decimal(sell_qty),
        "newOrderRespType": "FULL",
        "newClientOrderId": client_order_id,
    }

    log.warning(
        "%s | MARKET SELL qty=%s",
        symbol,
        format_decimal(sell_qty),
    )

    try:
        result = signed_request(
            "POST",
            "/api/v3/order",
            params,
        )

    except Exception as e:

        log.error(
            "%s | SELL order failed: %s",
            symbol,
            e,
        )

        return False

    executed_qty = Decimal(
        str(
            result.get(
                "executedQty",
                "0",
            )
        )
    )

    if executed_qty <= 0:

        log.error(
            "%s | SELL returned zero executed quantity",
            symbol,
        )

        return False

    log.warning(
        "%s | SELL FILLED | qty=%s | orderId=%s",
        symbol,
        format_decimal(executed_qty),
        result.get("orderId"),
    )

    # Position closed.
    remove_position(symbol)

    return True


# ============================================================
# PROCESS CLOSED CANDLE
# ============================================================

def process_closed_candle(symbol, candle):
    """
    candle:
        [
            open_time,
            open,
            high,
            low,
            close,
            volume,
            close_time,
            ...
        ]
    """

    open_time = int(candle[0])
    close_time = int(candle[6])

    close_price = Decimal(
        str(candle[4])
    )

    with state_lock:

        data = market_data.get(symbol)

        if not data:
            return

        # Duplicate candle protection
        if (
            data["last_closed_time"]
            == close_time
        ):
            return

        data["last_closed_time"] = close_time

        data["closes"].append(
            close_price
        )

        if len(data["closes"]) > HISTORY_LIMIT:
            data["closes"] = data["closes"][
                -HISTORY_LIMIT:
            ]

        closes = list(
            data["closes"]
        )

    # Calculate RSI outside lock
    rsi3 = calculate_wilder_rsi(
        closes,
        RSI_FAST_PERIOD,
    )

    rsi50 = calculate_wilder_rsi(
        closes,
        RSI_SLOW_PERIOD,
    )

    if rsi3 is None or rsi50 is None:
        return

    with state_lock:

        old_rsi3 = data.get(
            "rsi3"
        )

        data["rsi3"] = rsi3
        data["rsi50"] = rsi50

    log.info(
        "%s | CLOSED 5m | Close=%s | RSI3=%.4f | RSI50=%.4f",
        symbol,
        format_decimal(close_price),
        float(rsi3),
        float(rsi50),
    )

    # ========================================================
    # SELL FIRST
    # ========================================================

    if has_position(symbol):

        if (
            old_rsi3 is not None
            and old_rsi3 <= SELL_RSI_LEVEL
            and rsi3 > SELL_RSI_LEVEL
        ):

            log.warning(
                "%s | SELL SIGNAL | RSI3 %.4f -> %.4f",
                symbol,
                float(old_rsi3),
                float(rsi3),
            )

            # Sell immediately in this same candle event.
            market_sell(symbol)

        return

    # ========================================================
    # BUY
    # ========================================================

    buy_signal = (
        rsi50 > BUY_RSI_SLOW_MIN
        and rsi3 < BUY_RSI_FAST_MAX
    )

    if buy_signal:

        log.warning(
            "%s | BUY SIGNAL | RSI50=%.4f > %.2f | RSI3=%.4f < %.2f",
            symbol,
            float(rsi50),
            float(BUY_RSI_SLOW_MIN),
            float(rsi3),
            float(BUY_RSI_FAST_MAX),
        )

        market_buy(symbol)


# ============================================================
# WEBSOCKET
# ============================================================

def build_stream_url(group):
    streams = []

    for symbol in group:

        streams.append(
            f"{symbol.lower()}@kline_{TIMEFRAME}"
        )

    return WS_BASE_URL + "/".join(streams)


def websocket_on_open(ws):
    log.info(
        "WebSocket connected."
    )


def websocket_on_message(ws, message):

    try:

        payload = json.loads(message)

        data = payload.get("data")

        if not data:
            return

        if data.get("e") != "kline":
            return

        kline = data.get("k")

        if not kline:
            return

        # Binance:
        # x = true means candle is closed
        if not kline.get("x", False):
            return

        symbol = kline.get("s")

        if not symbol:
            return

        candle = [
            int(kline["t"]),
            kline["o"],
            kline["h"],
            kline["l"],
            kline["c"],
            kline["v"],
            int(kline["T"]),
        ]

        process_closed_candle(
            symbol,
            candle,
        )

    except Exception as e:

        log.exception(
            "WebSocket message processing error: %s",
            e,
        )


def websocket_on_error(ws, error):

    log.error(
        "WebSocket error: %s",
        error,
    )


def websocket_on_close(
    ws,
    close_status_code,
    close_msg,
):

    log.warning(
        "WebSocket closed | code=%s | message=%s",
        close_status_code,
        close_msg,
    )


def run_websocket_group(group, group_number):

    url = build_stream_url(group)

    log.info(
        "Starting WebSocket group %d | %d symbols",
        group_number,
        len(group),
    )

    while True:

        try:

            ws = websocket.WebSocketApp(
                url,
                on_open=websocket_on_open,
                on_message=websocket_on_message,
                on_error=websocket_on_error,
                on_close=websocket_on_close,
            )

            ws.run_forever(
                ping_interval=WS_PING_INTERVAL,
                ping_timeout=WS_PING_TIMEOUT,
                ping_payload="ping",
                skip_utf8_validation=True,
            )

        except Exception as e:

            log.exception(
                "WebSocket group %d crashed: %s",
                group_number,
                e,
            )

        log.warning(
            "WebSocket group %d reconnecting in %d seconds...",
            group_number,
            WS_RECONNECT_DELAY,
        )

        time.sleep(
            WS_RECONNECT_DELAY
        )


def start_websocket_groups():

    global ws_threads

    ws_threads = []

    groups = [
        symbols[i:i + GROUP_SIZE]
        for i in range(
            0,
            len(symbols),
            GROUP_SIZE,
        )
    ]

    log.info(
        "Creating %d WebSocket groups...",
        len(groups),
    )

    for number, group in enumerate(
        groups,
        start=1,
    ):

        thread = threading.Thread(
            target=run_websocket_group,
            args=(group, number),
            daemon=True,
            name=f"WS-GROUP-{number}",
        )

        thread.start()

        ws_threads.append(
            thread
        )

        # Avoid simultaneous connection burst
        time.sleep(2)

    log.info(
        "All WebSocket groups started."
    )


# ============================================================
# SERVER TIME MAINTENANCE
# ============================================================

def server_time_loop():

    while True:

        try:
            update_server_time()

        except Exception as e:
            log.error(
                "Server time maintenance error: %s",
                e,
            )

        # Very low frequency REST request
        time.sleep(30 * 60)


# ============================================================
# TOP SYMBOL REFRESH
# ============================================================

def symbol_refresh_loop():

    global symbols

    while True:

        time.sleep(
            TOP_SYMBOL_REFRESH_SECONDS
        )

        try:

            log.info(
                "Refreshing Top 150 symbols..."
            )

            load_exchange_info()

            new_symbols = select_top_symbols()

            if new_symbols != symbols:

                log.warning(
                    "Top 150 changed. "
                    "Bot restart/reconnect required for new stream set."
                )

                # Do NOT automatically kill active WS threads.
                #
                # A full process restart is safer than accidentally
                # running old and new streams simultaneously.
                #
                # Therefore we only log the change here.
                #
                # The current 150 continue running safely.

        except Exception as e:

            log.error(
                "Top symbol refresh failed: %s",
                e,
            )


# ============================================================
# POSITION RECOVERY
# ============================================================

def check_account_connection():

    try:

        data = signed_request(
            "GET",
            "/api/v3/account",
            {},
        )

        if data.get("accountType"):

            log.info(
                "Binance account connection OK | accountType=%s",
                data.get("accountType"),
            )

        else:

            log.info(
                "Binance account connection OK."
            )

        return True

    except Exception as e:

        log.error(
            "Binance account connection failed: %s",
            e,
        )

        return False


# ============================================================
# STARTUP
# ============================================================

def initialize_bot():

    log.info("=" * 70)
    log.info(
        "STARTING BINANCE SPOT RSI3 + RSI50 BOT"
    )
    log.info("=" * 70)

    log.info(
        "Timeframe: %s",
        TIMEFRAME,
    )

    log.info(
        "Top symbols: %d",
        TOP_SYMBOLS,
    )

    log.info(
        "BUY: RSI50 > %s AND RSI3 < %s",
        BUY_RSI_SLOW_MIN,
        BUY_RSI_FAST_MAX,
    )

    log.info(
        "SELL: RSI3 crosses above %s",
        SELL_RSI_LEVEL,
    )

    log.info(
        "BUY amount: %s USDT",
        BUY_USDT,
    )

    log.info(
        "DRY_RUN: %s",
        DRY_RUN,
    )

    if not API_KEY or not API_SECRET:

        raise RuntimeError(
            "BINANCE_API_KEY and BINANCE_API_SECRET "
            "must be set in Render Environment Variables."
        )

    # Sync server clock
    if not update_server_time():

        raise RuntimeError(
            "Could not synchronize Binance server time."
        )

    # Exchange information
    load_exchange_info()

    # Select Top 150
    select_top_symbols()

    if len(symbols) == 0:

        raise RuntimeError(
            "No eligible symbols found."
        )

    # Account connection
    if not check_account_connection():

        raise RuntimeError(
            "Binance account/API connection failed."
        )

    # Historical RSI data
    load_all_initial_data()

    # Start background time sync
    threading.Thread(
        target=server_time_loop,
        daemon=True,
        name="SERVER-TIME",
    ).start()

    # Start Top 150 monitor
    threading.Thread(
        target=symbol_refresh_loop,
        daemon=True,
        name="SYMBOL-REFRESH",
    ).start()

    # Start exactly 3 groups for 150 symbols
    start_websocket_groups()

    bot_started.set()

    log.info("=" * 70)
    log.info(
        "BOT IS RUNNING"
    )
    log.info("=" * 70)


# ============================================================
# MAIN
# ============================================================

def start_background_bot():

    try:

        initialize_bot()

    except Exception as e:

        log.exception(
            "FATAL BOT INITIALIZATION ERROR: %s",
            e,
        )


if __name__ == "__main__":

    # Start bot in background
    threading.Thread(
        target=start_background_bot,
        daemon=True,
        name="BOT-MAIN",
    ).start()

    # Render supplies PORT.
    port = int(
        os.getenv(
            "PORT",
            "10000",
        )
    )

    log.info(
        "Flask server starting on port %d",
        port,
    )

    app.run(
        host="0.0.0.0",
        port=port,
        threaded=True,
        use_reloader=False,
    )
