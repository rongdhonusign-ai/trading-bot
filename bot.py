import os
import time
import json
import threading
import collections
from concurrent.futures import ThreadPoolExecutor
import pandas as pd
import numpy as np
import websocket
from flask import Flask
from binance.client import Client
from binance.exceptions import BinanceAPIException
import logging

# ==========================================
# 0. TRADE CONFIGURATION & STRATEGY SETTINGS
# ==========================================
TRADE_AMOUNT_USDT = 40.0   # প্রতি ট্রেডে ৪০ ডলারের মার্কেট বাই

# RSI Parameters
RSI_LONG_PERIOD = 50       # ট্রেন্ড ফিল্টার (RSI 50)
RSI_SHORT_PERIOD = 3       # ইনস্ট্যান্ট সিগন্যাল (RSI 3)

BUY_RSI_LONG_MIN = 50.0    # RSI(50) > 50 হতে হবে
BUY_RSI_SHORT_MAX = 5.0    # RSI(3) < 5 হতে হবে
SELL_RSI_SHORT_TARGET = 85.0 # RSI(3) > 85 হলে মার্কেট সেল

# ==========================================
# 1. LIVE LOG BUFFER & FLASK WEB SERVER
# ==========================================
log_buffer = collections.deque(maxlen=100)

def log_print(message):
    print(message)
    log_buffer.append(message)

app = Flask(__name__)
log = logging.getLogger('werkzeug')
log.setLevel(logging.ERROR)

@app.route('/')
@app.route('/status')
def status():
    logs_html = "<br>".join(log_buffer)
    return f"""
    <html>
        <head>
            <title>Fast RSI Execution Bot ($40 Spot USDT)</title>
            <meta http-equiv="refresh" content="5">
            <style>
                body {{ background-color: #0d1117; color: #3fb950; font-family: monospace; padding: 20px; }}
                h2 {{ color: #58a6ff; }}
                .log-box {{ background: #161b22; padding: 15px; border-radius: 6px; border: 1px solid #30363d; max-height: 80vh; overflow-y: auto; }}
            </style>
        </head>
        <body>
            <h2>🤖 Fast RSI Execution Trading Bot (100 USDT Altcoins)</h2>
            <p>Strategy: Market Buy ($40 USDT) when RSI(50) > 50 & RSI(3) < 5 | Market Sell when RSI(3) > 85</p>
            <hr>
            <div class="log-box">{logs_html if logs_html else "Initializing scanner and preloading data..."}</div>
        </body>
    </html>
    """, 200

# ==========================================
# 2. BINANCE CLIENT SETUP & TOP 100 USDT ALTCOINS
# ==========================================
API_KEY = os.environ.get("BINANCE_API_KEY", "yRwdwQAR1S9G8DLVeQp39lW99BAGEF4XDG6hoImJkFTol2RFvWmTvksMKy5Bav0M")
API_SECRET = os.environ.get("BINANCE_API_SECRET", "3qsGUF6nPgfluSLPe8VXo0DE2gtR1jQIud9URVC5NHezEFp9YQV1lLqG1WncAltV")
client = Client(API_KEY, API_SECRET)

def get_target_altcoins():
    return [
        # Major & Layer 1 / Layer 2 (35)
        "BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT", 
        "ADAUSDT", "AVAXUSDT", "DOTUSDT", "NEARUSDT", "SUIUSDT", 
        "APTUSDT", "LTCUSDT", "ICPUSDT", "INJUSDT", "TIAUSDT", 
        "SEIUSDT", "ARBUSDT", "OPUSDT", "ATOMUSDT", "TRXUSDT", 
        "FTMUSDT", "ALGOUSDT", "EGLDUSDT", "FLOWUSDT", "STXUSDT", 
        "KAVAUSDT", "IMXUSDT", "BCHUSDT", "ETCUSDT", "FILUSDT",
        "HBARUSDT", "VETUSDT", "POLUSDT", "ROSEUSDT", "MINAUSDT",
        
        # Meme Coins (10)
        "DOGEUSDT", "SHIBUSDT", "PEPEUSDT", "WIFUSDT", "FLOKIUSDT", 
        "BONKUSDT", "MEMEUSDT", "1000SATSUSDT", "BOMEUSDT", "PEOPLEUSDT",

        # AI & Big Data (10)
        "FETUSDT", "RENDERUSDT", "TAOUSDT", "RNDRUSDT", "AGIXUSDT", 
        "OCEANUSDT", "AKTUSDT", "WLDUSDT", "ARKMUSDT", "AIUSDT",

        # DeFi & Infrastructure (25)
        "LINKUSDT", "UNIUSDT", "AAVEUSDT", "MKRUSDT", "CRVUSDT", 
        "SNXUSDT", "COMPUSDT", "LDOUSDT", "QNTUSDT", "DYDXUSDT", 
        "PENDLEUSDT", "JUPUSDT", "RAYUSDT", "ENAUSDT", "RUNEUSDT", 
        "CAKEUSDT", "1INCHUSDT", "SUSHIUSDT", "RDNTUSDT", "JTOUSDT", 
        "ORDIUSDT", "BLURUSDT", "ARUSDT", "ANKRUSDT", "ONDOUSDT",

        # Gaming & Metaverse & Storage (20)
        "SANDUSDT", "MANAUSDT", "GALAUSDT", "AXSUSDT", "CHZUSDT", 
        "BEAMXUSDT", "ILVUSDT", "ENJUSDT", "PIXELUSDT", "GMXUSDT",
        "THETAUSDT", "JASMYUSDT", "CKBUSDT", "XLMUSDT", "STXUSDT",
        "KSMUSDT", "GLMRUSDT", "ZILUSDT", "IOTAUSDT", "GMTUSDT"
    ]

# Global Trackers
candle_data = {}
positions = {}      # {symbol: True/False}
buy_prices = {}     # {symbol: entry_price}

symbols = list(set(get_target_altcoins()))  # Duplicate clean-up
for sym in symbols:
    candle_data[sym] = []
    positions[sym] = False
    buy_prices[sym] = 0.0

# ==========================================
# 3. FAST PARALLEL PRELOADER (IP SAFE)
# ==========================================
def fetch_single_symbol(sym):
    try:
        klines = client.get_klines(symbol=sym, interval=Client.KLINE_INTERVAL_5MINUTE, limit=75)
        closes = [float(k[4]) for k in klines]
        candle_data[sym] = closes
    except Exception as e:
        log_print(f"⚠️ Preload error for {sym}: {e}")

def preload_history():
    log_print("⚡ Preloading Historical 5M Data for 100 USDT Tokens in Parallel...")
    start_time = time.time()
    
    with ThreadPoolExecutor(max_workers=5) as executor:
        executor.map(fetch_single_symbol, symbols)
        
    elapsed = time.time() - start_time
    log_print(f"✅ Preload Complete in {elapsed:.2f} Seconds! Live WebSocket Scanning Active.")

# ==========================================
# 4. RSI INDICATOR CALCULATION
# ==========================================
def calculate_rsi(closes, period):
    if len(closes) < period + 1:
        return 50.0
    
    df = pd.DataFrame({'close': closes})
    delta = df['close'].diff()
    
    gain = (delta.where(delta > 0, 0)).ewm(alpha=1/period, adjust=False).mean()
    loss = (-delta.where(delta < 0, 0)).ewm(alpha=1/period, adjust=False).mean()
    
    rs = gain / loss
    rsi = 100 - (100 / (1 + rs))
    
    val = rsi.iloc[-1]
    return 50.0 if np.isnan(val) else val

# ==========================================
# 5. INSTANT EXECUTION STRATEGY LOGIC
# ==========================================
def process_tick(symbol, current_price):
    if len(candle_data[symbol]) < 55:
        return

    temp_closes = candle_data[symbol] + [current_price]
    rsi_50 = calculate_rsi(temp_closes, RSI_LONG_PERIOD)
    rsi_3 = calculate_rsi(temp_closes, RSI_SHORT_PERIOD)
    
    has_pos = positions[symbol]

    # 🛒 ১. ইনস্ট্যান্ট মার্কেট বাই: RSI(50) > 50 এবং RSI(3) < 5
    if not has_pos and rsi_50 > BUY_RSI_LONG_MIN and rsi_3 < BUY_RSI_SHORT_MAX:
        log_print(f"⚡ [BUY SIGNAL] {symbol} | Price: ${current_price} | RSI(50): {rsi_50:.2f} > 50 | RSI(3): {rsi_3:.2f} < 5")
        try:
            # $40 USDT দিয়ে মার্কেট বাই অর্ডার
            order = client.order_market_buy(symbol=symbol, quoteOrderQty=TRADE_AMOUNT_USDT)
            
            executed_price = current_price
            if 'fills' in order and len(order['fills']) > 0:
                executed_price = float(order['fills'][0]['price'])

            positions[symbol] = True
            buy_prices[symbol] = executed_price
            log_print(f"✅ [MARKET BOUGHT] {symbol} @ ${executed_price:.4f} | Order ID: {order['orderId']}")

        except BinanceAPIException as e:
            log_print(f"❌ [BUY ERROR] {symbol}: {e.message}")
        except Exception as e:
            log_print(f"❌ [BUY EXCEPTION] {symbol}: {e}")

    # 💰 ২. ইনস্ট্যান্ট মার্কেট সেল: RSI(3) > 85
    elif has_pos and rsi_3 > SELL_RSI_SHORT_TARGET:
        entry_price = buy_prices[symbol]
        pnl_pct = ((current_price - entry_price) / entry_price) * 100 if entry_price > 0 else 0.0
        log_print(f"🎯 [SELL SIGNAL] {symbol} | Price: ${current_price} | RSI(3): {rsi_3:.2f} > 85 | Est PnL: {pnl_pct:+.2f}%")
        execute_market_sell(symbol)

def execute_market_sell(symbol):
    try:
        asset = symbol.replace("USDT", "")
        balance_info = client.get_asset_balance(asset=asset)
        free_qty = float(balance_info['free']) if balance_info else 0.0

        if free_qty > 0:
            order = client.order_market_sell(symbol=symbol, quantity=free_qty)
            log_print(f"✅ [MARKET SOLD] {symbol} | Order ID: {order['orderId']}")
        else:
            log_print(f"⚠️ [SELL SKIPPED] {symbol}: No free balance available.")

        positions[symbol] = False
        buy_prices[symbol] = 0.0

    except BinanceAPIException as e:
        log_print(f"❌ [SELL ERROR] {symbol}: {e.message}")
    except Exception as e:
        log_print(f"❌ [SELL EXCEPTION] {symbol}: {e}")

# ==========================================
# 6. WEBSOCKET REAL-TIME TICK LISTENER
# ==========================================
def on_message(ws, message):
    data = json.loads(message)
    if 'k' in data:
        k = data['k']
        symbol = data['s']
        current_price = float(k['c'])
        is_closed = k['x']

        if symbol in candle_data:
            process_tick(symbol, current_price)

        if is_closed and symbol in candle_data:
            candle_data[symbol].append(current_price)
            if len(candle_data[symbol]) > 100:
                candle_data[symbol].pop(0)

def start_websocket():
    streams = [f"{sym.lower()}@kline_5m" for sym in symbols]
    socket_url = f"wss://stream.binance.com:9443/ws/{'/'.join(streams)}"
    
    ws = websocket.WebSocketApp(
        socket_url,
        on_message=on_message,
        on_error=lambda ws, err: log_print(f"❌ WS Error: {err}"),
        on_close=lambda ws, code, msg: log_print("🔌 WS Connection Closed. Reconnecting...")
    )
    ws.run_forever()

def start_bot():
    preload_history()
    log_print("🤖 Real-Time Execution Bot Running! Scanning 100 USDT tokens constantly...")
    while True:
        try:
            start_websocket()
        except Exception as e:
            log_print(f"⚠️ WebSocket connection lost: {e}. Retrying in 5 seconds...")
            time.sleep(5)

# ==========================================
# 7. MAIN EXECUTION
# ==========================================
if __name__ == '__main__':
    bot_thread = threading.Thread(target=start_bot)
    bot_thread.daemon = True
    bot_thread.start()
    
    port = int(os.environ.get("PORT", 5000))
    app.run(host='0.0.0.0', port=port)
