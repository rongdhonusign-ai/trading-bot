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
raise RuntimeError(
"BINANCE_API_KEY / BINANCE_API_SECRET missing"
)

BASE_URL = "https://api.binance.com"
WS_URL = "wss://stream.binance.com:9443/stream"

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

REST_DELAY = 0.50

BUY_COOLDOWN_SECONDS = 60

RECONNECT_DELAY = 15

REQUEST_TIMEOUT = 20

RECV_WINDOW = 10000

DEFAULT_418_COOLDOWN = 3600

# ============================================================

# FLASK

# ============================================================

app = Flask(**name**)

# ============================================================

# HTTP SESSION

# ============================================================

session = requests.Session()

session.headers.update({
"X-MBX-APIKEY": API_KEY,
"User-Agent": "RSI50-RSI3-BOT"
})

# ============================================================

# GLOBAL STATE

# ============================================================

exchange_info = {}

symbol_filters = {}

symbols = []

rsi_state = {}

positions = {}

last_buy_time = {}

last_candle_time = {}

binance_cooldown_until = 0

bot_ready = False

bot_initializing = False

shutdown_event = threading.Event()

rest_lock = threading.Lock()

order_lock = threading.Lock()

state_lock = threading.Lock()

# ============================================================

# DECIMAL HELPERS

# ============================================================

def D(value):
try:
return Decimal(str(value))
except Exception:
return Decimal("0")

def decimal_string(value):
value = D(value)

```
text = format(value, "f")

if "." in text:
    text = text.rstrip("0").rstrip(".")

return text or "0"
```

def floor_to_step(value, step):

```
value = D(value)
step = D(step)

if value <= 0 or step <= 0:
    return Decimal("0")

return (
    value / step
).to_integral_value(
    rounding=ROUND_DOWN
) * step
```

# ============================================================

# BINANCE COOLDOWN

# ============================================================

def cooldown_remaining():

```
remaining = int(
    binance_cooldown_until - time.time()
)

return max(0, remaining)
```

def activate_cooldown(seconds, reason):

```
global binance_cooldown_until

seconds = max(
    int(seconds),
    1
)

new_until = time.time() + seconds

if new_until > binance_cooldown_until:

    binance_cooldown_until = new_until

log.error(
    "BINANCE COOLDOWN | %s | %s seconds",
    reason,
    seconds
)
```

def wait_for_binance():

```
while not shutdown_event.is_set():

    remaining = cooldown_remaining()

    if remaining <= 0:
        return

    log.warning(
        "Binance REST paused. Remaining=%s seconds",
        remaining
    )

    time.sleep(
        min(30, remaining)
    )
```

# ============================================================

# BINANCE REST

# ============================================================

def handle_binance_error(response):

```
if response.status_code == 418:

    retry_after = response.headers.get(
        "Retry-After"
    )

    try:
        seconds = int(retry_after)
    except Exception:
        seconds = DEFAULT_418_COOLDOWN

    activate_cooldown(
        seconds,
        "HTTP 418 temporary IP restriction"
    )

    raise RuntimeError(
        "BINANCE_418"
    )

if response.status_code == 429:

    retry_after = response.headers.get(
        "Retry-After"
    )

    try:
        seconds = int(retry_after)
    except Exception:
        seconds = 60

    activate_cooldown(
        seconds,
        "HTTP 429 rate limit"
    )

    raise RuntimeError(
        "BINANCE_429"
    )

if response.status_code >= 400:

    try:
        data = response.json()
    except Exception:
        data = response.text

    raise RuntimeError(
        f"BINANCE_HTTP_{response.status_code}: {data}"
    )
```

def public_get(path, params=None):

```
wait_for_binance()

with rest_lock:

    wait_for_binance()

    response = session.get(
        BASE_URL + path,
        params=params or {},
        timeout=REQUEST_TIMEOUT
    )

    handle_binance_error(response)

    return response.json()
```

def signed_request(method, path, params=None):

```
wait_for_binance()

with rest_lock:

    wait_for_binance()

    data = dict(params or {})

    data["timestamp"] = int(
        time.time() * 1000
    )

    data["recvWindow"] = RECV_WINDOW

    query = urlencode(
        data,
        doseq=True
    )

    signature = hmac.new(
        API_SECRET.encode(),
        query.encode(),
        hashlib.sha256
    ).hexdigest()

    query += "&signature=" + signature

    url = (
        BASE_URL
        + path
        + "?"
        + query
    )

    response = session.request(
        method,
        url,
        timeout=REQUEST_TIMEOUT
    )

    handle_binance_error(response)

    return response.json()
```

# ============================================================

# EXACT WILDER RSI

# ============================================================

def calculate_rsi(closes, period):

```
if len(closes) < period + 1:
    return None

values = [
    D(x)
    for x in closes
]

gains = []
losses = []

for i in range(
    1,
    len(values)
):

    change = (
        values[i]
        - values[i - 1]
    )

    if change > 0:

        gains.append(change)
        losses.append(
            Decimal("0")
        )

    else:

        gains.append(
            Decimal("0")
        )

        losses.append(
            abs(change)
        )

avg_gain = (
    sum(
        gains[:period],
        Decimal("0")
    )
    / Decimal(period)
)

avg_loss = (
    sum(
        losses[:period],
        Decimal("0")
    )
    / Decimal(period)
)

for i in range(
    period,
    len(gains)
):

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

return (
    Decimal("100")
    - (
        Decimal("100")
        / (
            Decimal("1")
            + rs
        )
    )
)
```

# ============================================================

# EXCHANGE INFO

# ============================================================

def load_exchange_info():

```
log.info(
    "Loading Binance exchange information..."
)

info = public_get(
    "/api/v3/exchangeInfo"
)

symbol_filters.clear()

for item in info.get(
    "symbols",
    []
):

    symbol = item.get(
        "symbol"
    )

    if not symbol:
        continue

    filters = {}

    for f in item.get(
        "filters",
        []
    ):

        filter_type = f.get(
            "filterType"
        )

        if filter_type:
            filters[filter_type] = f

    symbol_filters[symbol] = filters

return info
```

# ============================================================

# SYMBOL SELECTION

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

def select_symbols(info):

```
eligible = set()

for item in info.get(
    "symbols",
    []
):

    if item.get(
        "status"
    ) != "TRADING":
        continue

    if item.get(
        "quoteAsset"
    ) != "USDT":
        continue

    if item.get(
        "isSpotTradingAllowed"
    ) is False:
        continue

    base = item.get(
        "baseAsset",
        ""
    )

    if base in STABLE_BASES:
        continue

    if base in {
        "BTC",
        "ETH"
    }:
        continue

    symbol = item.get(
        "symbol"
    )

    if symbol:
        eligible.add(symbol)

log.info(
    "Eligible USDT spot symbols=%s",
    len(eligible)
)

if not eligible:
    raise RuntimeError(
        "No eligible USDT symbols"
    )

# One ticker request only.
ticker_data = public_get(
    "/api/v3/ticker/24hr"
)

ranked = []

for item in ticker_data:

    symbol = item.get(
        "symbol"
    )

    if symbol not in eligible:
        continue

    quote_volume = D(
        item.get(
            "quoteVolume",
            "0"
        )
    )

    ranked.append(
        (
            symbol,
            quote_volume
        )
    )

ranked.sort(
    key=lambda x: x[1],
    reverse=True
)

selected = [
    item[0]
    for item in ranked[
        :TOP_SYMBOLS
    ]
]

log.info(
    "Selected %s symbols.",
    len(selected)
)

return selected
```

# ============================================================

# STARTUP HISTORY

# ============================================================

def load_symbol_history(symbol):

```
data = public_get(
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
        closes.append(
            D(candle[4])
        )
    except Exception:
        pass

if len(closes) < (
    RSI_SLOW_PERIOD + 1
):

    log.warning(
        "%s | insufficient history: %s",
        symbol,
        len(closes)
    )

    return False

rsi3 = calculate_rsi(
    closes,
    RSI_FAST_PERIOD
)

rsi50 = calculate_rsi(
    closes,
    RSI_SLOW_PERIOD
)

if rsi3 is None or rsi50 is None:
    return False

rsi_state[symbol] = {
    "closes": closes,
    "rsi3": rsi3,
    "rsi50": rsi50,
    "previous_rsi3": rsi3,
    "last_close": closes[-1]
}

return True
```

def initialize_history():

```
log.info(
    "Loading RSI history for %s symbols...",
    len(symbols)
)

success = 0

for index, symbol in enumerate(
    symbols,
    start=1
):

    if shutdown_event.is_set():
        return False

    try:

        if load_symbol_history(
            symbol
        ):
            success += 1

    except RuntimeError as e:

        if str(e) in {
            "BINANCE_418",
            "BINANCE_429"
        }:

            log.error(
                "History loading stopped because Binance rate limit is active."
            )

            return False

        log.error(
            "%s | history error: %s",
            symbol,
            e
        )

    except Exception as e:

        log.error(
            "%s | history error: %s",
            symbol,
            e
        )

    if index % 10 == 0:

        log.info(
            "History progress: %s/%s",
            index,
            len(symbols)
        )

    time.sleep(
        REST_DELAY
    )

log.info(
    "RSI initialization complete: %s/%s",
    success,
    len(symbols)
)

return success > 0
```

# ============================================================

# LOCAL RSI UPDATE

# ============================================================

def update_local_rsi(
symbol,
close
):

```
state = rsi_state.get(
    symbol
)

if not state:
    return None, None

close = D(close)

if (
    state["last_close"]
    == close
):
    return (
        state["rsi3"],
        state["rsi50"]
    )

previous_rsi3 = state["rsi3"]

closes = state["closes"]

closes.append(close)

if len(closes) > HISTORY_LIMIT:

    del closes[
        :-HISTORY_LIMIT
    ]

rsi3 = calculate_rsi(
    closes,
    RSI_FAST_PERIOD
)

rsi50 = calculate_rsi(
    closes,
    RSI_SLOW_PERIOD
)

if rsi3 is None or rsi50 is None:
    return None, None

state["previous_rsi3"] = (
    previous_rsi3
)

state["rsi3"] = rsi3

state["rsi50"] = rsi50

state["last_close"] = close

return rsi3, rsi50
```

# ============================================================

# QUANTITY FILTERS

# ============================================================

def get_quantity_filter(symbol):

```
filters = symbol_filters.get(
    symbol,
    {}
)

market_filter = filters.get(
    "MARKET_LOT_SIZE"
)

lot_filter = filters.get(
    "LOT_SIZE"
)

if market_filter:

    step = D(
        market_filter.get(
            "stepSize",
            "0"
        )
    )

    if step > 0:
        return market_filter

if lot_filter:

    step = D(
        lot_filter.get(
            "stepSize",
            "0"
        )
    )

    if step > 0:
        return lot_filter

return None
```

def normalize_quantity(
symbol,
quantity
):

```
quantity = D(quantity)

if quantity <= 0:
    return Decimal("0")

filt = get_quantity_filter(
    symbol
)

if not filt:

    log.error(
        "%s | No valid LOT_SIZE filter",
        symbol
    )

    return Decimal("0")

step = D(
    filt.get(
        "stepSize",
        "0"
    )
)

min_qty = D(
    filt.get(
        "minQty",
        "0"
    )
)

max_qty = D(
    filt.get(
        "maxQty",
        "0"
    )
)

if step <= 0:

    log.error(
        "%s | Invalid stepSize=%s",
        symbol,
        step
    )

    return Decimal("0")

qty = floor_to_step(
    quantity,
    step
)

if qty < min_qty:

    log.warning(
        "%s | qty=%s < minQty=%s",
        symbol,
        decimal_string(qty),
        decimal_string(min_qty)
    )

    return Decimal("0")

if max_qty > 0 and qty > max_qty:

    qty = floor_to_step(
        max_qty,
        step
    )

return qty
```

def get_min_notional(symbol):

```
filters = symbol_filters.get(
    symbol,
    {}
)

notional = filters.get(
    "NOTIONAL"
)

if notional:

    if notional.get(
        "applyMinToMarket",
        True
    ):

        value = D(
            notional.get(
                "minNotional",
                "0"
            )
        )

        if value > 0:
            return value

minimum = filters.get(
    "MIN_NOTIONAL"
)

if minimum:

    if minimum.get(
        "applyToMarket",
        True
    ):

        value = D(
            minimum.get(
                "minNotional",
                "0"
            )
        )

        if value > 0:
            return value

return Decimal("0")
```

def calculate_buy_quantity(
symbol,
price
):

```
price = D(price)

if price <= 0:
    return Decimal("0")

raw_quantity = (
    BUY_USDT / price
)

quantity = normalize_quantity(
    symbol,
    raw_quantity
)

if quantity <= 0:
    return Decimal("0")

notional = (
    quantity * price
)

minimum = get_min_notional(
    symbol
)

if (
    minimum > 0
    and notional < minimum
):

    log.warning(
        "%s | notional=%s < minNotional=%s",
        symbol,
        decimal_string(notional),
        decimal_string(minimum)
    )

    return Decimal("0")

log.info(
    "%s | Quantity OK | price=%s | qty=%s | notional=%s",
    symbol,
    decimal_string(price),
    decimal_string(quantity),
    decimal_string(notional)
)

return quantity
```

# ============================================================

# BUY

# ============================================================

def place_buy(
symbol,
price
):

```
if cooldown_remaining() > 0:

    log.warning(
        "%s | BUY blocked by cooldown",
        symbol
    )

    return None

quantity = calculate_buy_quantity(
    symbol,
    price
)

if quantity <= 0:

    log.warning(
        "%s | BUY quantity invalid",
        symbol
    )

    return None

client_id = (
    "RSIBUY_"
    + str(
        int(
            time.time() * 1000
        )
    )
)

params = {
    "symbol": symbol,
    "side": "BUY",
    "type": "MARKET",
    "quantity": decimal_string(
        quantity
    ),
    "newClientOrderId": client_id
}

log.info(
    "%s | BUY attempt | $%s | qty=%s",
    symbol,
    decimal_string(BUY_USDT),
    decimal_string(quantity)
)

try:

    with order_lock:

        result = signed_request(
            "POST",
            "/api/v3/order",
            params
        )

    executed_qty = D(
        result.get(
            "executedQty",
            "0"
        )
    )

    quote_qty = D(
        result.get(
            "cummulativeQuoteQty",
            "0"
        )
    )

    if (
        executed_qty > 0
        and quote_qty > 0
    ):

        entry_price = (
            quote_qty
            / executed_qty
        )

    else:

        entry_price = D(price)

    positions[symbol] = {
        "qty": executed_qty,
        "entry_price": entry_price,
        "buy_time": time.time()
    }

    last_buy_time[symbol] = (
        time.time()
    )

    log.info(
        "%s | BUY SUCCESS | orderId=%s | executed=%s | entry=%s",
        symbol,
        result.get("orderId"),
        decimal_string(executed_qty),
        decimal_string(entry_price)
    )

    # Place server-side stop loss.
    place_stop_loss(
        symbol,
        executed_qty,
        entry_price
    )

    return result

except RuntimeError as e:

    log.error(
        "%s | BUY error: %s",
        symbol,
        e
    )

except Exception as e:

    log.error(
        "%s | BUY exception: %s",
        symbol,
        e
    )

return None
```

# ============================================================

# STOP LOSS

# ============================================================

def place_stop_loss(
symbol,
quantity,
entry_price
):

```
quantity = normalize_quantity(
    symbol,
    quantity
)

if quantity <= 0:
    return None

entry_price = D(
    entry_price
)

stop_price = (
    entry_price
    * (
        Decimal("1")
        - STOP_LOSS_PERCENT
    )
)

stop_price = stop_price.quantize(
    Decimal("0.00000001"),
    rounding=ROUND_DOWN
)

client_id = (
    "RSISL_"
    + str(
        int(
            time.time() * 1000
        )
    )
)

params = {
    "symbol": symbol,
    "side": "SELL",
    "type": "STOP_LOSS_LIMIT",
    "timeInForce": "GTC",
    "quantity": decimal_string(
        quantity
    ),
    "price": decimal_string(
        stop_price
    ),
    "stopPrice": decimal_string(
        stop_price
    ),
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
        "%s | STOP LOSS placed | stop=%s | qty=%s | orderId=%s",
        symbol,
        decimal_string(stop_price),
        decimal_string(quantity),
        result.get("orderId")
    )

    return result

except RuntimeError as e:

    log.error(
        "%s | STOP LOSS error: %s",
        symbol,
        e
    )

except Exception as e:

    log.error(
        "%s | STOP LOSS exception: %s",
        symbol,
        e
    )

return None
```

# ============================================================

# SELL

# ============================================================

def place_sell(
symbol,
quantity,
reason
):

```
if cooldown_remaining() > 0:

    log.warning(
        "%s | SELL blocked by cooldown",
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
    "RSISELL_"
    + str(
        int(
            time.time() * 1000
        )
    )
)

params = {
    "symbol": symbol,
    "side": "SELL",
    "type": "MARKET",
    "quantity": decimal_string(
        quantity
    ),
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

except RuntimeError as e:

    log.error(
        "%s | SELL error: %s",
        symbol,
        e
    )

except Exception as e:

    log.error(
        "%s | SELL exception: %s",
        symbol,
        e
    )

return None
```

# ============================================================

# STRATEGY

# ============================================================

def process_closed_candle(
symbol,
close
):

```
state = rsi_state.get(
    symbol
)

if not state:
    return

rsi3, rsi50 = update_local_rsi(
    symbol,
    close
)

if rsi3 is None or rsi50 is None:
    return

previous_rsi3 = state.get(
    "previous_rsi3",
    rsi3
)

log.info(
    "%s | Close=%s | RSI50=%.2f | RSI3=%.2f",
    symbol,
    decimal_string(close),
    float(rsi50),
    float(rsi3)
)

# ========================================================
# SELL
# ========================================================

if symbol in positions:

    if (
        previous_rsi3
        <= SELL_RSI_LEVEL
        and rsi3
        > SELL_RSI_LEVEL
    ):

        quantity = positions[
            symbol
        ]["qty"]

        log.info(
            "%s | SELL SIGNAL | RSI3 crossed above %s",
            symbol,
            decimal_string(
                SELL_RSI_LEVEL
            )
        )

        place_sell(
            symbol,
            quantity,
            "RSI3_CROSS_ABOVE_80"
        )

    return

# ========================================================
# BUY COOLDOWN
# ========================================================

last_buy = last_buy_time.get(
    symbol,
    0
)

if (
    time.time()
    - last_buy
    < BUY_COOLDOWN_SECONDS
):
    return

# ========================================================
# BUY
# ========================================================

if (
    rsi50 > BUY_RSI_SLOW_MIN
    and rsi3 < BUY_RSI_FAST_MAX
):

    log.info(
        "%s | BUY SIGNAL | RSI50=%.2f RSI3=%.2f",
        symbol,
        float(rsi50),
        float(rsi3)
    )

    place_buy(
        symbol,
        close
    )
```

# ============================================================

# WEBSOCKET

# ============================================================

def build_websocket_url():

```
streams = []

for symbol in symbols:

    streams.append(
        symbol.lower()
        + "@kline_5m"
    )

return (
    WS_URL
    + "?streams="
    + "/".join(streams)
)
```

def ws_message(
ws,
message
):

```
try:

    payload = json.loads(
        message
    )

    data = payload.get(
        "data",
        payload
    )

    if data.get("e") != "kline":
        return

    kline = data.get(
        "k",
        {}
    )

    symbol = kline.get(
        "s"
    )

    if not symbol:
        return

    # Only CLOSED candle.
    if not kline.get(
        "x",
        False
    ):
        return

    close = D(
        kline.get(
            "c",
            "0"
        )
    )

    candle_time = int(
        kline.get(
            "T",
            0
        )
    )

    if (
        last_candle_time.get(
            symbol
        )
        == candle_time
    ):
        return

    last_candle_time[
        symbol
    ] = candle_time

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

def ws_open(ws):

```
log.info(
    "WebSocket connected | symbols=%s",
    len(symbols)
)
```

def ws_error(
ws,
error
):

```
log.error(
    "WebSocket error: %s",
    error
)
```

def ws_close(
ws,
status,
message
):

```
log.warning(
    "WebSocket closed | code=%s | %s",
    status,
    message
)
```

def websocket_worker():

```
global bot_ready

while not shutdown_event.is_set():

    try:

        url = build_websocket_url()

        log.info(
            "Starting combined WebSocket..."
        )

        ws = websocket.WebSocketApp(
            url,
            on_open=ws_open,
            on_message=ws_message,
            on_error=ws_error,
            on_close=ws_close
        )

        ws.run_forever(
            ping_interval=120,
            ping_timeout=30,
            skip_utf8_validation=True
        )

    except Exception as e:

        log.error(
            "WebSocket exception: %s",
            e
        )

    if shutdown_event.is_set():
        break

    bot_ready = False

    log.warning(
        "WebSocket reconnect in %s seconds",
        RECONNECT_DELAY
    )

    time.sleep(
        RECONNECT_DELAY
    )

    bot_ready = True
```

# ============================================================

# BOT INITIALIZATION

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
    "BUY: RSI50 > 50 AND RSI3 < 10"
)

log.info(
    "BUY amount: %s USDT",
    decimal_string(BUY_USDT)
)

log.info(
    "SELL: RSI3 crossing above 80"
)

log.info(
    "Initial SL: %.2f%%",
    float(
        STOP_LOSS_PERCENT * 100
    )
)

log.info(
    "WebSocket: 1 combined connection"
)

log.info(
    "REST klines: startup only"
)

log.info(
    "Local RSI: ENABLED"
)

log.info(
    "Pandas: NOT USED"
)

log.info(
    "Server time sync: DISABLED"
)

log.info(
    "Anti-418 protection: ENABLED"
)

# --------------------------------------------------------
# EXCHANGE INFO
# --------------------------------------------------------

try:

    exchange_info = (
        load_exchange_info()
    )

except Exception as e:

    log.error(
        "Exchange info failed: %s",
        e
    )

    return False

time.sleep(
    REST_DELAY
)

# --------------------------------------------------------
# SYMBOLS
# --------------------------------------------------------

try:

    symbols = select_symbols(
        exchange_info
    )

except Exception as e:

    log.error(
        "Symbol selection failed: %s",
        e
    )

    return False

# --------------------------------------------------------
# HISTORY
# --------------------------------------------------------

if not initialize_history():

    log.error(
        "RSI history initialization failed."
    )

    return False

# --------------------------------------------------------
# WEBSOCKET
# --------------------------------------------------------

bot_ready = True

threading.Thread(
    target=websocket_worker,
    daemon=True,
    name="BinanceWebSocket"
).start()

log.info("=" * 70)

log.info(
    "BOT READY"
)

log.info("=" * 70)

return True
```

# ============================================================

# BACKGROUND

# ============================================================

def background_bot():

```
global bot_initializing

log.info(
    "BOT BACKGROUND THREAD STARTED"
)

while not shutdown_event.is_set():

    if bot_ready:

        time.sleep(60)

        continue

    if bot_initializing:

        time.sleep(5)

        continue

    bot_initializing = True

    try:

        success = (
            initialize_bot()
        )

        if success:

            while (
                bot_ready
                and not shutdown_event.is_set()
            ):

                time.sleep(30)

        else:

            remaining = (
                cooldown_remaining()
            )

            if remaining > 0:

                log.warning(
                    "Waiting for Binance cooldown: %s seconds",
                    remaining
                )

                time.sleep(
                    min(
                        60,
                        remaining
                    )
                )

            else:

                log.warning(
                    "Initialization failed. Retry in 60 seconds."
                )

                time.sleep(60)

    except Exception as e:

        log.exception(
            "Background bot error: %s",
            e
        )

        time.sleep(60)

    finally:

        bot_initializing = False
```

# ============================================================

# FLASK ROUTES

# ============================================================

@app.route(
"/",
methods=["GET", "HEAD"]
)
def home():

```
return jsonify({
    "status": "online",
    "bot": "RSI50 + RSI3",
    "ready": bot_ready,
    "symbols": len(symbols),
    "positions": len(positions),
    "binance_cooldown_seconds":
        cooldown_remaining()
})
```

@app.route(
"/health",
methods=["GET", "HEAD"]
)
def health():

```
return jsonify({
    "status": "healthy",
    "bot_ready": bot_ready
}), 200
```

@app.route(
"/status",
methods=["GET"]
)
def status():

```
output_positions = {}

for symbol, data in positions.items():

    output_positions[
        symbol
    ] = {
        "qty": decimal_string(
            data.get(
                "qty",
                0
            )
        ),
        "entry_price": decimal_string(
            data.get(
                "entry_price",
                0
            )
        )
    }

return jsonify({
    "bot_ready": bot_ready,
    "symbols": len(symbols),
    "positions":
        output_positions,
    "binance_cooldown_seconds":
        cooldown_remaining()
})
```

# ============================================================

# START BACKGROUND THREAD

# ============================================================

threading.Thread(
target=background_bot,
daemon=True,
name="BotBackground"
).start()

# ============================================================

# LOCAL DEVELOPMENT

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
