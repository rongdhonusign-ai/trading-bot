import os
import time
import json
import threading
import collections
import pandas as pd
import numpy as np
import websocket
from flask import Flask
from binance.client import Client
from binance.exceptions import BinanceAPIException
import logging

# ==========================================
# 0. TRADE CONFIGURATION
# ==========================================
TRADE_AMOUNT_USDT = 40.0   # প্রতি ট্রেডে ৪০ ডলারের মার্কেট বাই
STOP_LOSS_PERCENT = 0.03    # ৩% স্টপ লস (0.03 = 3%)

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
            <title>Instant Execution RSI Trading Bot ($40 Spot)</title>
            <meta http-equiv="refresh" content="5">
            <style>
                body {{ background-color: #0d1117; color: #3fb950; font-family: monospace; padding: 20px; }}
                h2 {{ color: #58a6ff; }}
                .log-box {{ background: #161b22; padding: 15px; border-radius: 6px; border: 1px solid #30363d; max-height: 80vh; overflow-y: auto; }}
            </style>
        </head>
        <body>
            <h2>🤖 Real-Time RSI Strategy Trading Bot (Active Scanning)</h2>
            <p>Strategy: Market Buy (RSI50 > 50 AND RSI3 < 5) | Market Sell (RSI3 > 85 OR 3% Stop Loss)</p>
            <hr>
            <div class="log-box">{logs_html if logs_html else "Initializing scanner and preloading data..."}</div>
        </body>
    </html>
    """, 200

# ==========================================
# 2. BINANCE CLIENT SETUP & VALID ALTCOINS
# ==========================================
API_KEY = os.environ.get("BINANCE_API_KEY", "yRwdwQAR1S9G8DLVeQp39lW99BAGEF4XDG6hoImJkFTol2RFvWmTvksMKy5Bav0M")
API_SECRET = os.environ.get("BINANCE_API_SECRET", "3qsGUF6nPgfluSLPe8VXo0DE2gtR1jQIud9URVC5NHezEFp9YQV1lLqG1WncAltV")
client = Client(API_KEY, API_SECRET)

def get_target_altcoins():
    return [
        # Major & Layer 1 / Layer 2
        "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT", 
        "ADAUSDT", "AVAXUSDT", "DOTUSDT", "NEARUSDT", "SUIUSDT", 
        "APTUSDT", "LTCUSDT", "ICPUSDT", "INJUSDT", "TIAUSDT", 
        "SEIUSDT", "ARBUSDT", "OPUSDT", "ATOMUSDT", "TRXUSDT", 
        "FTMUSDT", "ALGOUSDT", "EGLDUSDT", "FLOWUSDT", "STXUSDT", 
        "KAVAUSDT", "IMXUSDT", "BCHUSDT", "ETCUSDT", "FILUSDT", 
        "HBARUSDT", "VETUSDT", "ROSEUSDT", "MINAUSDT", 
        
        # Meme Coins
        "DOGEUSDT", "SHIBUSDT", "PEPEUSDT", "WIFUSDT", "FLOKIUSDT", 
        "BONKUSDT", "MEMEUSDT", "1000SATSUSDT", "BOMEUSDT", "PEOPLEUSDT",

        # AI & Big Data
        "FETUSDT", "RENDERUSDT", "TAOUSDT", "WLDUSDT", "ARKMUSDT", "AIUSDT",

        # DeFi & Infrastructure
        "LINKUSDT", "UNIUSDT", "AAVEUSDT", "MKRUSDT", "CRVUSDT", 
        "SNXUSDT", "COMPUSDT", "LDOUSDT", "QNTUSDT", "DYDXUSDT", 
        "PENDLEUSDT", "JUPUSDT", "RAYUSDT", "ENAUSDT", "RUNEUSDT", 
        "CAKEUSDT", "1INCHUSDT", "SUSHIUSDT", "RDNTUSDT", "JTOUSDT", 
        "ORDIUSDT", "BLURUSDT", "ARUSDT", "ANKRUSDT", "ONDOUSDT",

        # Gaming & Metaverse & Storage
        "SANDUSDT", "MANAUSDT", "GALAUSDT", "AXSUSDT", "CHZUSDT", 
        "BEAMXUSDT", "ILVUSDT", "ENJUSDT", "PIXELUSDT", "GMXUSDT", 
        "THETAUSDT", "JASMYUSDT", "CKBUSDT", "XLMUSDT", "KSMUSDT", 
        "GLMRUSDT", "ZILUSDT", "IOTAUSDT", "GMTUSDT"
    ]

# Global Trackers
candle_data = {}
positions = {}      # {symbol: True/False}
buy_prices = {}     # {symbol: entry_price}
last_scan_log = 0   

symbols = list(set(get_target_altcoins()))
for sym in symbols:
    candle_data[sym] = []
    positions[sym] = False
    buy_prices[sym] = 0.0

# ==========================================
# 3. ULTRA-FAST HISTORICAL PRELOADER
# ==========================================
def preload_history():
    log_print("🔄 Fast Preloading Historical Data (IP Safe & Ultra Fast)...")
    for sym in symbols:
        try:
            klines = client.get_klines(symbol=sym, interval=Client.KLINE_INTERVAL_5MINUTE, limit=60)
            closes = [float(k[4]) for k in klines]
            candle_data[sym] = closes
            time.sleep(0.01)
        except Exception as e:
            log_print(f"⚠️ Preload error for {sym}: {e}")
    log_print("✅ Preload Complete! Live WebSocket Scanning Started.")

# ==========================================
# 4. RSI INDICATOR CALCULATION
# ==========================================
def calculate_rsi(closes, period=14):
    if len(closes) < period + 1:
        return 50.0
    
    series = pd.Series(closes)
    delta = series.diff()
    
    gain = (delta.where(delta > 0, 0)).ewm(alpha=1/period, adjust=False).mean()
    loss = (-delta.where(delta < 0, 0)).ewm(alpha=1/period, adjust=False).mean()
    
    rs = gain / loss
    rsi = 100 - (100 / (1 + rs))
    return rsi.iloc[-1]

# ==========================================
# 5. INSTANT EXECUTION STRATEGY LOGIC
# ==========================================
def process_tick(symbol, current_price):
    global last_scan_log
    if len(candle_data[symbol]) < 52:
        return

    temp_closes = candle_data[symbol] + [current_price]
    rsi50 = calculate_rsi(temp_closes, period=50)
    rsi3 = calculate_rsi(temp_closes, period=3)
    
    has_pos = positions[symbol]
    entry_price = buy_prices[symbol]

    # 🛒 ১. শর্ত: RSI(50) > 50 এবং RSI(3) < 5 হলে মার্কেট বাই
    if not has_pos and rsi50 > 50 and rsi3 < 5:
        log_print(f"⚡ [BUY SIGNAL] {symbol} | Price: ${current_price} | RSI(50): {rsi50:.2f} | RSI(3): {rsi3:.2f}")
        try:
            order = client.order_market_buy(symbol=symbol, quoteOrderQty=TRADE_AMOUNT_USDT)
            
            executed_price = current_price
            if 'fills' in order and len(order['fills']) > 0:
                executed_price = float(order['fills'][0]['price'])

            positions[symbol] = True
            buy_prices[symbol] = executed_price
            log_print(f"✅ [BOUGHT] {symbol} @ ${executed_price:.4f} | Order ID: {order['orderId']}")

        except BinanceAPIException as e:
            log_print(f"❌ [BUY ERROR] {symbol}: {e.message}")
        except Exception as e:
            log_print(f"❌ [BUY EXCEPTION] {symbol}: {e}")

    # 💰 ২. শর্ত: RSI(3) > 85 হলে মার্কেট সেল
    elif has_pos and rsi3 > 85:
        log_print(f"🎯 [PROFIT SELL SIGNAL] {symbol} | Price: ${current_price} | RSI(3): {rsi3:.2f} > 85")
        execute_market_sell(symbol)

    # 🚨 ৩. ৩% স্টপ লস সেল
    elif has_pos and entry_price > 0 and current_price <= (entry_price * (1 - STOP_LOSS_PERCENT)):
        loss_pct = ((current_price - entry_price) / entry_price) * 100
        log_print(f"🚨 [STOP LOSS TRIGGERED] {symbol} | Price: ${current_price} ({loss_pct:.2f}% drop)")
        execute_market_sell(symbol)

    # 📉 বাই কন্ডিশনের কাছাকাছি গেলে অ্যালার্ট দেবে
    elif not has_pos and rsi3 < 15:
        log_print(f"📉 [NEAR BUY SIGNAL] {symbol} | Price: ${current_price} | RSI(50): {rsi50:.1f} | RSI(3): {rsi3:.1f}")

    # 🔍 ১ মিনিট পর পর দুটি RSI-এর মানই লগে প্রিন্ট করবে
    if time.time() - last_scan_log > 60:
        last_scan_log = time.time()
        log_print(f"🔍 [ACTIVE SCANNING] {symbol} | Price: ${current_price} | RSI(50): {rsi50:.1f} | RSI(3): {rsi3:.1f}")

def execute_market_sell(symbol):
    try:
        asset = symbol.replace("USDT", "")
        balance_info = client.get_asset_balance(asset=asset)
        free_qty = float(balance_info['free']) if balance_info else 0.0

        if free_qty > 0:
            info = client.get_symbol_info(symbol)
            step_size = None
            for f in info['filters']:
                if f['filterType'] == 'LOT_SIZE':
                    step_size = float(f['stepSize'])
                    break
            
            if step_size:
                precision = int(round(-np.log10(step_size)))
                free_qty = float(np.floor(free_qty * (10 ** precision)) / (10 ** precision))

            order = client.order_market_sell(symbol=symbol, quantity=free_qty)
            log_print(f"✅ [MARKET SOLD] {symbol} | Order ID: {order['orderId']}")
        else:
            log_print(f"⚠️ [SELL SKIPPED] {symbol}: No balance found.")

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
    if 'data' in data:
        k = data['data']['k']
        symbol = data['data']['s']
        current_price = float(k['c'])
        is_closed = k['x']

        if symbol in candle_data:
            process_tick(symbol, current_price)

        if is_closed and symbol in candle_data:
            candle_data[symbol].append(current_price)
            if len(candle_data[symbol]) > 80:
                candle_data[symbol].pop(0)

def start_websocket():
    streams = [f"{sym.lower()}@kline_5m" for sym in symbols]
    stream_url = "/".join(streams)
    socket_url = f"wss://stream.binance.com:9443/stream?streams={stream_url}"
    
    ws = websocket.WebSocketApp(
        socket_url,
        on_message=on_message,
        on_error=lambda ws, err: log_print(f"❌ WS Error: {err}"),
        on_close=lambda ws, code, msg: log_print("🔌 WS Connection Closed. Reconnecting...")
    )
    ws.run_forever()

def start_bot():
    preload_history()
    log_print("🤖 Real-Time RSI Bot Active! Scanning target coins continuously...")
    while True:
        try:
            start_websocket()
        except Exception as e:
            log_print(f"⚠️ WS Reconnecting in 5s due to: {e}")
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
