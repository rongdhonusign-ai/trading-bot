import os
import time
import json
import hmac
import hashlib
import threading
import logging
from decimal import Decimal, ROUND_DOWN
from urllib.parse import urlencode

import requests
import websocket

from flask import Flask, jsonify

# ============================================================

# LOGGING

# ============================================================

logging.basicConfig(
level=logging.INFO,
format="%(asctime)s | %(levelname)s | %(message)s"
)

log = logging.getLogger("RSI_BOT")

# ============================================================

# CONFIG

# ============================================================

API_KEY = os.environ.get("BINANCE_API_KEY")
API_SECRET = os.environ.get("BINANCE_API_SECRET")

if not API_KEY or not API_SECRET:
raise RuntimeError("BINANCE_API_KEY / BINANCE_API_SECRET missing")

BASE_URL = "https://api.binance.com"
WS_BASE = "wss://stream.binance.com:9443/stream"

TIMEFRAME = "5m"

TOP_SYMBOLS = 150

BUY_USDT = Decimal("15")

RSI_FAST_PERIOD = 3
RSI_SLOW_PERIOD = 50

BUY_RSI_SLOW_MIN = Decimal("50")
BUY_RSI_FAST_MAX = Decimal("10")

SELL_RSI_LEVEL = Decimal("80")

STOP_LOSS_PERCENT = Decimal("0.01")

HISTORY_LIMIT = 100

REST_DELAY = 0.40

BUY_COOLDOWN_SECONDS = 60

RECONNECT_DELAY = 15

DEFAULT_BAN_SECONDS = 3600

REQUEST_TIMEOUT = 20

RECV_WINDOW = 10000

# ============================================================

# GLOBAL STATE

# ============================================================

app = Flask(**name**)

session = requests.Session()
session.headers.update({
"X-MBX-APIKEY": API_KEY,
"User-Agent": "RSI50-RSI3-BOT/1.0"
})

exchange_info = {}
symbol_filters = {}
symbols = []

rsi_state = {}

positions = {}

last_buy_time = {}

last_candle_time = {}

ws_thread = None

bot_ready = False

bot_starting = False

bot_lock = threading.Lock()

order_lock = threading.Lock()

rest_lock = threading.Lock()

ban_until = 0.0

shutdown_event = threading.Event()

# ============================================================

# BINANCE EXCEPTIONS

# ============================================================

class BinanceBlocked(Exception):
pass

class BinanceRateLimited(Exception):
pass

class BinanceAPIError(Exception):
pass

# ============================================================

# REST BAN / COOLDOWN

# ============================================================

def set_binance_cooldown(seconds, reason):
global ban_until

```
seconds = max(1, int(seconds))

new_until = time.time() + seconds

if new_until > ban_until:
    ban_until = new_until

log.error(
    "BINANCE COOLDOWN | %s | wait=%ss",
    reason,
    seconds
)
```

def cooldown_remaining():
return max(0, int(ban_until - time.time()))

def wait_for_binance():
while True:

```
    remaining = cooldown_remaining()

    if remaining <= 0:
        return

    log.warning(
        "Binance REST disabled. Remaining cooldown=%ss",
        remaining
    )

    time.sleep(min(30, remaining))
```

# ============================================================

# DECIMAL HELPERS

# ============================================================

def D(value):
try:
return Decimal(str(value))
except Exception:
return Decimal("0")

def floor_step(value, step):
value = D(value)
step = D(step)

```
if step <= 0:
    return value

return (value / step).to_integral_value(
    rounding=ROUND_DOWN
) * step
```

def decimal_to_str(value):
value = D(value)

```
text = format(value, "f")

if "." in text:
    text = text.rstrip("0").rstrip(".")

return text or "0"
```

# ============================================================

# SIGNED REST REQUEST

# ============================================================

def signed_request(method, path, params=None):
params = dict(params or {})

```
wait_for_binance()

with rest_lock:

    wait_for_binance()

    params["timestamp"] = int(time.time() * 1000)
    params["recvWindow"] = RECV_WINDOW

    query = urlencode(params, doseq=True)

    signature = hmac.new(
        API_SECRET.encode(),
        query.encode(),
        hashlib.sha256
    ).hexdigest()

    query += "&signature=" + signature

    url = BASE_URL + path + "?" + query

    try:

        response = session.request(
            method=method,
            url=url,
            timeout=REQUEST_TIMEOUT
        )

    except requests.RequestException as e:
        log.error("REST network error: %s", e)
        raise

    if response.status_code == 418:

        retry_after = response.headers.get("Retry-After")

        try:
            seconds = int(retry_after)
        except Exception:
            seconds = DEFAULT_BAN_SECONDS

        set_binance_cooldown(
            seconds,
            "HTTP 418 temporary IP ban"
        )

        raise BinanceBlocked(
            "Binance HTTP 418 temporary IP restriction"
        )

    if response.status_code == 429:

        retry_after = response.headers.get("Retry-After")

        try:
            seconds = int(retry_after)
        except Exception:
            seconds = 60

        set_binance_cooldown(
            seconds,
            "HTTP 429 rate limit"
        )

        raise BinanceRateLimited(
            "Binance HTTP 429 rate limit"
        )

    if response.status_code >= 400:

        try:
            data = response.json()
        except Exception:
            data = response.text

        raise BinanceAPIError(
            f"HTTP {response.status_code}: {data}"
        )

    try:
        data = response.json()
    except Exception as e:
        raise BinanceAPIError(
            f"Invalid JSON response: {e}"
        )

    return data
```

def public_request(path, params=None):
wait_for_binance()

```
with rest_lock:

    wait_for_binance()

    try:

        response = session.get(
            BASE_URL + path,
            params=params or {},
            timeout=REQUEST_TIMEOUT
        )

    except requests.RequestException as e:
        log.error("Public REST network error: %s", e)
        raise

    if response.status_code == 418:

        retry_after = response.headers.get("Retry-After")

        try:
            seconds = int(retry_after)
        except Exception:
            seconds = DEFAULT_BAN_SECONDS

        set_binance_cooldown(
            seconds,
            "HTTP 418 temporary IP ban"
        )

        raise BinanceBlocked(
            "Binance HTTP 418 temporary IP restriction"
        )

    if response.status_code == 429:

        retry_after = response.headers.get("Retry-After")

        try:
            seconds = int(retry_after)
        except Exception:
            seconds = 60

        set_binance_cooldown(
            seconds,
            "HTTP 429 rate limit"
        )

        raise BinanceRateLimited(
            "Binance HTTP 429 rate limit"
        )

    if response.status_code >= 400:

        try:
            data = response.json()
        except Exception:
            data = response.text

        raise BinanceAPIError(
            f"HTTP {response.status_code}: {data}"
        )

    return response.json()
```

# ============================================================

# EXACT WILDER RSI

# ============================================================

def calculate_rsi_series(closes, period):
values = [D(x) for x in closes]

```
if len(values) < period + 1:
    return []

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

avg_gain = sum(
    gains[:period],
    Decimal("0")
) / Decimal(period)

avg_loss = sum(
    losses[:period],
    Decimal("0")
) / Decimal(period)

result = [None] * period

def make_rsi(gain, loss):

    if loss == 0:

        if gain == 0:
            return Decimal("50")

        return Decimal("100")

    rs = gain / loss

    return Decimal("100") - (
        Decimal("100") /
        (Decimal("1") + rs)
    )

result.append(
    make_rsi(avg_gain, avg_loss)
)

for i in range(period, len(gains)):

    avg_gain = (
        avg_gain * Decimal(period - 1)
        + gains[i]
    ) / Decimal(period)

    avg_loss = (
        avg_loss * Decimal(period - 1)
        + losses[i]
    ) / Decimal(period)

    result.append(
        make_rsi(avg_gain, avg_loss)
    )

return result
```

def calculate_latest_rsi(closes, period):
series = calculate_rsi_series(
closes,
period
)

```
if not series:
    return None

return series[-1]
```

# ============================================================

# SYMBOL FILTERS

# ============================================================

def load_symbol_filters(info):

```
symbol_filters.clear()

for item in info.get("symbols", []):

    symbol = item.get("symbol")

    if not symbol:
        continue

    filters = {}

    for f in item.get("filters", []):

        filter_type = f.get("filterType")

        if filter_type:
            filters[filter_type] = f

    symbol_filters[symbol] = filters
```

def get_quantity_filter(symbol):

```
filters = symbol_filters.get(symbol, {})

market = filters.get("MARKET_LOT_SIZE")
lot = filters.get("LOT_SIZE")

candidates = []

if market:
    step = D(market.get("stepSize"))

    if step > 0:
        candidates.append(market)

if lot:
    step = D(lot.get("stepSize"))

    if step > 0:
        candidates.append(lot)

if not candidates:
    return None

# Prefer MARKET_LOT_SIZE
return candidates[0]
```

def normalize_quantity(symbol, quantity):

```
quantity = D(quantity)

if quantity <= 0:
    return Decimal("0")

filt = get_quantity_filter(symbol)

if not filt:
    log.error(
        "%s | No valid quantity filter",
        symbol
    )
    return Decimal("0")

min_qty = D(filt.get("minQty", "0"))
max_qty = D(filt.get("maxQty", "0"))
step = D(filt.get("stepSize", "0"))

if step <= 0:
    log.error(
        "%s | Invalid stepSize=%s",
        symbol,
        step
    )
    return Decimal("0")

qty = floor_step(quantity, step)

if qty < min_qty:

    log.warning(
        "%s | Quantity %s below minQty %s",
        symbol,
        decimal_to_str(qty),
        decimal_to_str(min_qty)
    )

    return Decimal("0")

if max_qty > 0 and qty > max_qty:

    qty = floor_step(max_qty, step)

if qty <= 0:
    return Decimal("0")

return qty
```

def get_min_notional(symbol):

```
filters = symbol_filters.get(symbol, {})

notional_filter = filters.get("NOTIONAL")

if notional_filter:

    apply_market = notional_filter.get(
        "applyMinToMarket",
        True
    )

    if apply_market:

        value = D(
            notional_filter.get(
                "minNotional",
                "0"
            )
        )

        if value > 0:
            return value

min_filter = filters.get("MIN_NOTIONAL")

if min_filter:

    apply_market = min_filter.get(
        "applyToMarket",
        True
    )

    if apply_market:

        value = D(
            min_filter.get(
                "minNotional",
                "0"
            )
        )

        if value > 0:
            return value

return Decimal("0")
```

def get_buy_quantity(symbol, price):

```
price = D(price)

if price <= 0:
    return Decimal("0")

raw_qty = BUY_USDT / price

qty = normalize_quantity(
    symbol,
    raw_qty
)

if qty <= 0:
    return Decimal("0")

notional = qty * price

min_notional = get_min_notional(symbol)

if (
    min_notional > 0
    and notional < min_notional
):

    log.warning(
        "%s | Notional %.8f below minimum %.8f",
        symbol,
        notional,
        min_notional
    )

    return Decimal("0")

log.info(
    "%s | BUY quantity OK | price=%s | raw=%s | qty=%s | notional=%s",
    symbol,
    decimal_to_str(price),
    decimal_to_str(raw_qty),
    decimal_to_str(qty),
    decimal_to_str(notional)
)

return qty
```

# ============================================================

# EXCHANGE INFORMATION

# ============================================================

def load_exchange_info():

```
log.info("Loading Binance exchange information...")

info = public_request(
    "/api/v3/exchangeInfo"
)

load_symbol_filters(info)

return info
```

# ============================================================

# TOP SYMBOLS

# ============================================================

STABLE_BASES = {
"USDT",
"USDC",
"FDUSD",
"BUSD",
"TUSD",
"DAI",
"USDP",
"EUR",
"GBP",
"TRY",
"BRL",
"AUD",
"RUB",
"UAH",
"PLN",
"RON",
"ZAR"
}

def get_top_symbols(info):

```
eligible = []

for item in info.get("symbols", []):

    symbol = item.get("symbol", "")

    if item.get("status") != "TRADING":
        continue

    if item.get("quoteAsset") != "USDT":
        continue

    if item.get("isSpotTradingAllowed") is False:
        continue

    base = item.get("baseAsset", "")

    if base in STABLE_BASES:
        continue

    if base in {"BTC", "ETH"}:
        continue

    eligible.append(symbol)

log.info(
    "Eligible USDT spot symbols=%s",
    len(eligible)
)

if not eligible:
    raise BinanceAPIError(
        "No eligible USDT symbols found"
    )

# --------------------------------------------------------
# ONE 24HR TICKER REQUEST
# --------------------------------------------------------

tickers = public_request(
    "/api/v3/ticker/24hr"
)

volume_map = {}

for item in tickers:

    symbol = item.get("symbol")

    if symbol not in eligible:
        continue

    try:
        volume = D(
            item.get(
                "quoteVolume",
                "0"
            )
        )
    except Exception:
        volume = Decimal("0")

    volume_map[symbol] = volume

ranked = sorted(
    eligible,
    key=lambda s: volume_map.get(
        s,
        Decimal("0")
    ),
    reverse=True
)

selected = ranked[:TOP_SYMBOLS]

log.info(
    "Selected %s symbols.",
    len(selected)
)

return selected
```

# ============================================================

# STARTUP RSI HISTORY

# ============================================================

def load_history_for_symbol(symbol):

```
data = public_request(
    "/api/v3/klines",
    {
        "symbol": symbol,
        "interval": TIMEFRAME,
        "limit": HISTORY_LIMIT
    }
)

closes = []

for candle in data:

    try:
        close = D(candle[4])
        closes.append(close)
    except Exception:
        continue

if len(closes) < RSI_SLOW_PERIOD + 1:

    log.warning(
        "%s | Not enough candles: %s",
        symbol,
        len(closes)
    )

    return False

rsi3 = calculate_latest_rsi(
    closes,
    RSI_FAST_PERIOD
)

rsi50 = calculate_latest_rsi(
    closes,
    RSI_SLOW_PERIOD
)

if rsi3 is None or rsi50 is None:
    return False

rsi_state[symbol] = {
    "closes": closes[-HISTORY_LIMIT:],
    "rsi3": rsi3,
    "rsi50": rsi50,
    "prev_rsi3": rsi3,
    "last_close": closes[-1]
}

return True
```

def initialize_rsi_history():

```
log.info(
    "Loading RSI history for %s symbols...",
    len(symbols)
)

success = 0

for index, symbol in enumerate(symbols, 1):

    if shutdown_event.is_set():
        return False

    while cooldown_remaining() > 0:

        remaining = cooldown_remaining()

        log.warning(
            "Waiting for Binance cooldown before history: %ss",
            remaining
        )

        time.sleep(
            min(30, remaining)
        )

    try:

        ok = load_history_for_symbol(
            symbol
        )

        if ok:
            success += 1

    except BinanceBlocked:

        log.error(
            "%s | Binance blocked during history loading.",
            symbol
        )

        return False

    except BinanceRateLimited:

        log.error(
            "%s | Binance rate limited during history loading.",
            symbol
        )

        return False

    except Exception as e:

        log.error(
            "%s | History error: %s",
            symbol,
            e
        )

    if index % 10 == 0:

        log.info(
            "RSI history progress: %s/%s",
            index,
            len(symbols)
        )

    time.sleep(REST_DELAY)

log.info(
    "RSI initialization complete: %s/%s symbols",
    success,
    len(symbols)
)

return success > 0
```

# ============================================================

# LOCAL RSI UPDATE

# ============================================================

def update_rsi(symbol, close):

```
close = D(close)

state = rsi_state.get(symbol)

if not state:
    return None, None

closes = state["closes"]

if closes and close == closes[-1]:
    return (
        state["rsi3"],
        state["rsi50"]
    )

closes.append(close)

if len(closes) > HISTORY_LIMIT:
    del closes[:-HISTORY_LIMIT]

old_rsi3 = state["rsi3"]

rsi3 = calculate_latest_rsi(
    closes,
    RSI_FAST_PERIOD
)

rsi50 = calculate_latest_rsi(
    closes,
    RSI_SLOW_PERIOD
)

if rsi3 is None or rsi50 is None:
    return None, None

state["prev_rsi3"] = old_rsi3
state["rsi3"] = rsi3
state["rsi50"] = rsi50
state["last_close"] = close

return rsi3, rsi50
```

# ============================================================

# ACCOUNT / ORDER

# ============================================================

def get_account():

```
return signed_request(
    "GET",
    "/api/v3/account"
)
```

def get_asset_balance(asset):

```
data = get_account()

for item in data.get("balances", []):

    if item.get("asset") == asset:

        return D(
            item.get(
                "free",
                "0"
            )
        )

return Decimal("0")
```

def place_buy(symbol, price):

```
if cooldown_remaining() > 0:
    log.warning(
        "%s | BUY blocked by Binance cooldown",
        symbol
    )
    return None

qty = get_buy_quantity(
    symbol,
    price
)

if qty <= 0:

    log.warning(
        "%s | BUY quantity invalid",
        symbol
    )

    return None

client_id = (
    "RSIBUY_"
    + str(int(time.time() * 1000))
)

params = {
    "symbol": symbol,
    "side": "BUY",
    "type": "MARKET",
    "quantity": decimal_to_str(qty),
    "newClientOrderId": client_id
}

log.info(
    "%s | BUY attempt | USDT=%s | price=%s | qty=%s",
    symbol,
    decimal_to_str(BUY_USDT),
    decimal_to_str(price),
    decimal_to_str(qty)
)

try:

    with order_lock:

        result = signed_request(
            "POST",
            "/api/v3/order",
            params
        )

    log.info(
        "%s | BUY SUCCESS | orderId=%s",
        symbol,
        result.get("orderId")
    )

    executed_qty = D(
        result.get(
            "executedQty",
            qty
        )
    )

    quote_qty = D(
        result.get(
            "cummulativeQuoteQty",
            BUY_USDT
        )
    )

    if executed_qty > 0:
        entry_price = (
            quote_qty / executed_qty
        )
    else:
        entry_price = price

    positions[symbol] = {
        "qty": executed_qty,
        "entry_price": entry_price,
        "buy_time": time.time(),
        "sl_order_id": None
    }

    last_buy_time[symbol] = time.time()

    place_stop_loss(
        symbol,
        executed_qty,
        entry_price
    )

    return result

except BinanceBlocked:

    log.error(
        "%s | BUY stopped: Binance 418",
        symbol
    )

    return None

except BinanceRateLimited:

    log.error(
        "%s | BUY stopped: Binance 429",
        symbol
    )

    return None

except Exception as e:

    log.error(
        "%s | BUY error: %s",
        symbol,
        e
    )

    return None
```

def place_stop_loss(symbol, quantity, entry_price):

```
quantity = normalize_quantity(
    symbol,
    quantity
)

if quantity <= 0:
    return None

stop_price = (
    entry_price
    * (Decimal("1") - STOP_LOSS_PERCENT)
)

stop_price = stop_price.quantize(
    Decimal("0.00000001"),
    rounding=ROUND_DOWN
)

client_id = (
    "RSISL_"
    + str(int(time.time() * 1000))
)

params = {
    "symbol": symbol,
    "side": "SELL",
    "type": "STOP_LOSS_LIMIT",
    "timeInForce": "GTC",
    "quantity": decimal_to_str(quantity),
    "price": decimal_to_str(stop_price),
    "stopPrice": decimal_to_str(stop_price),
    "newClientOrderId": client_id
}

try:

    with order_lock:

        result = signed_request(
            "POST",
            "/api/v3/order",
            params
        )

    order_id = result.get("orderId")

    if symbol in positions:
        positions[symbol]["sl_order_id"] = order_id

    log.info(
        "%s | STOP LOSS placed | stop=%s | qty=%s | orderId=%s",
        symbol,
        decimal_to_str(stop_price),
        decimal_to_str(quantity),
        order_id
    )

    return result

except BinanceBlocked:

    log.error(
        "%s | Stop loss blocked by Binance 418",
        symbol
    )

except BinanceRateLimited:

    log.error(
        "%s | Stop loss blocked by Binance 429",
        symbol
    )

except Exception as e:

    log.error(
        "%s | Stop loss error: %s",
        symbol,
        e
    )

return None
```

def place_sell(symbol, quantity, reason):

```
if cooldown_remaining() > 0:
    log.warning(
        "%s | SELL blocked by Binance cooldown",
        symbol
    )
    return None

quantity = normalize_quantity(
    symbol,
    quantity
)

if quantity <= 0:

    log.warning(
        "%s | SELL quantity invalid",
        symbol
    )

    return None

client_id = (
    "RSELL_"
    + str(int(time.time() * 1000))
)

params = {
    "symbol": symbol,
    "side": "SELL",
    "type": "MARKET",
    "quantity": decimal_to_str(quantity),
    "newClientOrderId": client_id
}

try:

    with order_lock:

        result = signed_request(
            "POST",
            "/api/v3/order",
            params
        )

    log.info(
        "%s | SELL SUCCESS | reason=%s | orderId=%s",
        symbol,
        reason,
        result.get("orderId")
    )

    positions.pop(
        symbol,
        None
    )

    return result

except BinanceBlocked:

    log.error(
        "%s | SELL stopped: Binance 418",
        symbol
    )

except BinanceRateLimited:

    log.error(
        "%s | SELL stopped: Binance 429",
        symbol
    )

except Exception as e:

    log.error(
        "%s | SELL error: %s",
        symbol,
        e
    )

return None
```

# ============================================================

# BUY / SELL STRATEGY

# ============================================================

def process_closed_candle(symbol, close):

```
if symbol not in rsi_state:
    return

try:

    rsi3, rsi50 = update_rsi(
        symbol,
        close
    )

    if rsi3 is None or rsi50 is None:
        return

    state = rsi_state[symbol]

    previous_rsi3 = state.get(
        "prev_rsi3",
        rsi3
    )

    log.info(
        "%s | Close=%s | RSI50=%.2f | RSI3=%.2f",
        symbol,
        decimal_to_str(close),
        float(rsi50),
        float(rsi3)
    )

    # ----------------------------------------------------
    # SELL
    # ----------------------------------------------------

    if symbol in positions:

        if (
            previous_rsi3 <= SELL_RSI_LEVEL
            and rsi3 > SELL_RSI_LEVEL
        ):

            qty = positions[symbol]["qty"]

            log.info(
                "%s | SELL SIGNAL | RSI3 crossed above %s",
                symbol,
                decimal_to_str(SELL_RSI_LEVEL)
            )

            place_sell(
                symbol,
                qty,
                "RSI3_CROSS_ABOVE_80"
            )

        return

    # ----------------------------------------------------
    # BUY COOLDOWN
    # ----------------------------------------------------

    last_buy = last_buy_time.get(
        symbol,
        0
    )

    if (
        time.time() - last_buy
        < BUY_COOLDOWN_SECONDS
    ):
        return

    # ----------------------------------------------------
    # BUY
    # ----------------------------------------------------

    if (
        rsi50 > BUY_RSI_SLOW_MIN
        and rsi3 < BUY_RSI_FAST_MAX
    ):

        log.info(
            "%s | BUY SIGNAL | RSI50=%.2f > %.2f | RSI3=%.2f < %.2f",
            symbol,
            float(rsi50),
            float(BUY_RSI_SLOW_MIN),
            float(rsi3),
            float(BUY_RSI_FAST_MAX)
        )

        place_buy(
            symbol,
            close
        )

except Exception as e:

    log.error(
        "%s | Strategy error: %s",
        symbol,
        e
    )
```

# ============================================================

# WEBSOCKET

# ============================================================

def websocket_url():

```
streams = []

for symbol in symbols:

    streams.append(
        symbol.lower()
        + "@kline_5m"
    )

return (
    WS_BASE
    + "?streams="
    + "/".join(streams)
)
```

def ws_on_message(ws, message):

```
try:

    payload = json.loads(message)

    data = payload.get(
        "data",
        payload
    )

    if data.get("e") != "kline":
        return

    kline = data.get("k", {})

    symbol = kline.get("s")

    if not symbol:
        return

    is_closed = kline.get(
        "x",
        False
    )

    if not is_closed:
        return

    close = D(
        kline.get("c")
    )

    candle_time = int(
        kline.get("T", 0)
    )

    if (
        last_candle_time.get(symbol)
        == candle_time
    ):
        return

    last_candle_time[symbol] = candle_time

    process_closed_candle(
        symbol,
        close
    )

except Exception as e:

    log.error(
        "WebSocket message error: %s",
        e
    )
```

def ws_on_error(ws, error):

```
log.error(
    "WebSocket error: %s",
    error
)
```

def ws_on_close(ws, close_status_code, close_msg):

```
log.warning(
    "WebSocket closed | code=%s | msg=%s",
    close_status_code,
    close_msg
)
```

def ws_on_open(ws):

```
log.info(
    "WebSocket connected | %s symbols",
    len(symbols)
)
```

def websocket_worker():

```
global bot_ready

while not shutdown_event.is_set():

    try:

        url = websocket_url()

        log.info(
            "Connecting combined WebSocket..."
        )

        ws = websocket.WebSocketApp(
            url,
            on_open=ws_on_open,
            on_message=ws_on_message,
            on_error=ws_on_error,
            on_close=ws_on_close
        )

        ws.run_forever(
            ping_interval=120,
            ping_timeout=30,
            skip_utf8_validation=True
        )

    except Exception as e:

        log.error(
            "WebSocket worker exception: %s",
            e
        )

    bot_ready = False

    if shutdown_event.is_set():
        break

    log.warning(
        "WebSocket reconnect in %ss",
        RECONNECT_DELAY
    )

    time.sleep(
        RECONNECT_DELAY
    )

    if not bot_ready:
        bot_ready = True
```

# ============================================================

# INITIALIZATION

# ============================================================

def initialize_bot():

```
global exchange_info
global symbols
global bot_ready

log.info("=" * 70)
log.info(
    "STARTING BINANCE RSI50 + RSI3 BOT"
)
log.info("=" * 70)

log.info(
    "Timeframe: %s",
    TIMEFRAME
)

log.info(
    "Top symbols: %s",
    TOP_SYMBOLS
)

log.info(
    "BUY: RSI50 > %s AND RSI3 < %s",
    decimal_to_str(BUY_RSI_SLOW_MIN),
    decimal_to_str(BUY_RSI_FAST_MAX)
)

log.info(
    "BUY amount: %s USDT",
    decimal_to_str(BUY_USDT)
)

log.info(
    "SELL: RSI3 crossing above %s",
    decimal_to_str(SELL_RSI_LEVEL)
)

log.info(
    "Initial SL: %.2f%%",
    float(STOP_LOSS_PERCENT * 100)
)

log.info(
    "WebSocket: 1 combined connection"
)

log.info(
    "REST klines: startup only"
)

log.info(
    "Local RSI calculation: ENABLED"
)

log.info(
    "Server time sync: DISABLED"
)

log.info(
    "Anti-418 REST protection: ENABLED"
)

# --------------------------------------------------------
# IMPORTANT:
# If Binance is already in cooldown, do NOT send request.
# --------------------------------------------------------

wait_for_binance()

# --------------------------------------------------------
# EXCHANGE INFO
# --------------------------------------------------------

try:

    exchange_info = load_exchange_info()

except BinanceBlocked:

    log.error(
        "BOT INITIALIZATION PAUSED: Binance HTTP 418"
    )

    return False

except BinanceRateLimited:

    log.error(
        "BOT INITIALIZATION PAUSED: Binance HTTP 429"
    )

    return False

except Exception as e:

    log.error(
        "Exchange info error: %s",
        e
    )

    return False

time.sleep(REST_DELAY)

# --------------------------------------------------------
# TOP SYMBOLS
# --------------------------------------------------------

try:

    symbols = get_top_symbols(
        exchange_info
    )

except BinanceBlocked:

    log.error(
        "BOT INITIALIZATION PAUSED: Binance HTTP 418"
    )

    return False

except BinanceRateLimited:

    log.error(
        "BOT INITIALIZATION PAUSED: Binance HTTP 429"
    )

    return False

except Exception as e:

    log.error(
        "Top symbols error: %s",
        e
    )

    return False

# --------------------------------------------------------
# RSI HISTORY
# --------------------------------------------------------

if not initialize_rsi_history():

    log.error(
        "BOT INITIALIZATION FAILED: RSI history unavailable"
    )

    return False

# --------------------------------------------------------
# START WEBSOCKET
# --------------------------------------------------------

log.info(
    "Starting single combined WebSocket..."
)

bot_ready = True

thread = threading.Thread(
    target=websocket_worker,
    daemon=True,
    name="BinanceWebSocket"
)

thread.start()

log.info("=" * 70)
log.info("BOT READY")
log.info("=" * 70)

return True
```

def bot_background():

```
global bot_starting

log.info(
    "BOT BACKGROUND THREAD STARTED"
)

while not shutdown_event.is_set():

    if bot_ready:
        time.sleep(60)
        continue

    with bot_lock:

        if bot_starting:
            time.sleep(5)
            continue

        bot_starting = True

    try:

        success = initialize_bot()

        if success:

            while (
                not shutdown_event.is_set()
                and bot_ready
            ):
                time.sleep(30)

        else:

            remaining = cooldown_remaining()

            if remaining > 0:

                log.warning(
                    "Initialization paused because Binance cooldown remains: %ss",
                    remaining
                )

                while (
                    cooldown_remaining() > 0
                    and not shutdown_event.is_set()
                ):
                    time.sleep(
                        min(
                            30,
                            cooldown_remaining()
                        )
                    )

            else:

                log.warning(
                    "Initialization failed. Waiting 60 seconds before retry."
                )

                time.sleep(60)

    except Exception as e:

        log.exception(
            "BOT INITIALIZATION EXCEPTION: %s",
            e
        )

        time.sleep(60)

    finally:

        bot_starting = False
```

# ============================================================

# FLASK ROUTES

# ============================================================

@app.route("/", methods=["GET", "HEAD"])
def home():

```
return jsonify({
    "status": "online",
    "bot": "RSI50 + RSI3",
    "ready": bot_ready,
    "symbols": len(symbols),
    "binance_cooldown_seconds": cooldown_remaining()
})
```

@app.route("/health", methods=["GET", "HEAD"])
def health():

```
return jsonify({
    "status": "healthy",
    "bot_ready": bot_ready
}), 200
```

@app.route("/status", methods=["GET"])
def status():

```
return jsonify({
    "bot_ready": bot_ready,
    "symbols": len(symbols),
    "positions": {
        symbol: {
            "qty": decimal_to_str(
                data.get("qty", 0)
            ),
            "entry_price": decimal_to_str(
                data.get("entry_price", 0)
            )
        }
        for symbol, data in positions.items()
    },
    "binance_cooldown_seconds": cooldown_remaining()
})
```

# ============================================================

# START BACKGROUND THREAD

# ============================================================

_background_started = False

with bot_lock:

```
if not _background_started:

    _background_started = True

    threading.Thread(
        target=bot_background,
        daemon=True,
        name="BotBackground"
    ).start()
```

# ============================================================

# LOCAL RUN

# ============================================================

if **name** == "**main**":

```
port = int(
    os.environ.get(
        "PORT",
        "10000"
    )
)

app.run(
    host="0.0.0.0",
    port=port
)
```
