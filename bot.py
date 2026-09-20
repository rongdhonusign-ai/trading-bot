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

# Bollinger Band & EMA Parameters
BB_PERIOD = 20             # Bollinger Band window
BB_STD_DEV = 2             # Standard Deviation multiplier
EMA_PERIOD = 200           # Trend Filter EMA (200)

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
            <title>Bollinger + EMA200 Strategy Bot ($40 Spot)</title>
            <meta http-equiv="refresh" content="5">
            <style>
                body {{ background-color: #0d1117; color: #3fb950; font-family: monospace; padding: 20px; }}
                h2 {{ color: #58a6ff; }}
                .log-box {{ background: #161b22; padding: 15px; border-radius: 6px; border: 1px solid #30363d; max-height: 80vh; overflow-y: auto; }}
            </style>
        </head>
        <body>
            <h2>🤖 Bollinger Band + EMA200 Trading Bot (40 Altcoins)</h2>
            <p>Strategy: Buy ($40 USDT) when 5m Candle Closes < Lower BB & > EMA200 | Sell when Price > Upper BB</p>
            <hr>
            <div class="log-box">{logs_html if logs_html else "Initializing scanner and preloading data..."}</div>
        </body>
    </html>
    """, 200

# ==========================================
# 2. BINANCE CLIENT SETUP & TOP 47 ALTCOINS
# ==========================================
API_KEY = os.environ.get("BINANCE_API_KEY", "yRwdwQAR1S9G8DLVeQp39lW99BAGEF4XDG6hoImJkFTol2RFvWmTvksMKy5Bav0M")
API_SECRET = os.environ.get("BINANCE_API_SECRET", "3qsGUF6nPgfluSLPe8VXo0DE2gtR1jQIud9URVC5NHezEFp9YQV1lLqG1WncAltV")
client = Client(API_KEY, API_SECRET)

def get_target_altcoins():
    # ৪৭টি সেরা টপ-ভলিউম অল্টকয়েন (কোনো স্ট্যাবলকয়েন পেয়ার নেই)
    return [
        "BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT", 
        "ADAUSDT", "AVAXUSDT", "DOTUSDT", "NEARUSDT", "SUIUSDT", 
        "APTUSDT", "LTCUSDT", "ICPUSDT", "INJUSDT", "TIAUSDT", 
        "SEIUSDT", "ARBUSDT", "OPUSDT", "ATOMUSDT", "TRXUSDT", 
        "FTMUSDT", "LINKUSDT", "UNIUSDT", "AAVEUSDT", "FETUSDT", 
        "RENDERUSDT", "TAOUSDT", "WLDUSDT", "ONDOUSDT", "DOGEUSDT", 
        "SHIBUSDT", "PEPEUSDT", "WIFUSDT", "BONKUSDT", "FLOKIUSDT", 
        "SANDUSDT", "MANAUSDT", "GALAUSDT", "CHZUSDT", "JUPUSDT", 
        "RAYUSDT", "ENAUSDT", "RUNEUSDT", "PENDLEUSDT", "ORDIUSDT", 
        "FILUSDT", "STXUSDT"
    ]

# Global Trackers
candle_data = {}
positions = {}      # {symbol: True/False}
buy_prices = {}     # {symbol: entry_price}

symbols = list(set(get_target_altcoins()))  # Duplicate cleanup
for sym in symbols:
    candle_data[sym] = []
    positions[sym] = False
    buy_prices[sym] = 0.0

# ==========================================
# 3. FAST PARALLEL PRELOADER (IP SAFE)
# ==========================================
def fetch_single_symbol(sym):
    try:
        # EMA(200) সঠিক পেতে অন্তত ২২০টি ঐতিহাসিক ক্যান্ডেল প্রয়োজন
        klines = client.get_klines(symbol=sym, interval=Client.KLINE_INTERVAL_5MINUTE, limit=220)
        closes = [float(k[4]) for k in klines]
        candle_data[sym] = closes
    except Exception as e:
        log_print(f"⚠️ Preload error for {sym}: {e}")

def preload_history():
    log_print("⚡ Preloading 220 Historical 5M Candles for 47 Altcoins in Parallel...")
    start_time = time.time()
    
    # ১০টি সমান্তরাল থ্রেড ব্যবহার করে কয়েক সেকেন্ডে প্রিলোড শেষ করা হবে (IP Safe)
    with ThreadPoolExecutor(max_workers=10) as executor:
        executor.map(fetch_single_symbol, symbols)
        
    elapsed = time.time() - start_time
    log_print(f"✅ Preload Complete in {elapsed:.2f} Seconds! Live WebSocket Scanning Active.")

# ==========================================
# 4. INDICATOR CALCULATIONS (BB & EMA200)
# ==========================================
def calculate_indicators(closes):
    if len(closes) < EMA_PERIOD:
        return None, None, None

    df = pd.DataFrame({'close': closes})
    
    # Bollinger Bands Calculation (20, 2)
    df['SMA20'] = df['close'].rolling(window=BB_PERIOD).mean()
    df['STD20'] = df['close'].rolling(window=BB_PERIOD).std()
    df['Upper_BB'] = df['SMA20'] + (df['STD20'] * BB_STD_DEV)
    df['Lower_BB'] = df['SMA20'] - (df['STD20'] * BB_STD_DEV)
    
    # Exponential Moving Average (EMA 200)
    df['EMA200'] = df['close'].ewm(span=EMA_PERIOD, adjust=False).mean()
    
    latest = df.iloc[-1]
    return latest['Lower_BB'], latest['Upper_BB'], latest['EMA200']

# ==========================================
# 5. STRATEGY EXECUTION LOGIC
# ==========================================
def process_tick(symbol, current_price, is_closed):
    if len(candle_data[symbol]) < EMA_PERIOD:
        return

    has_pos = positions[symbol]

    # 🛒 ১. মার্কেট বাই শর্ত: ক্যান্ডেল ক্লোজ হতে হবে + ক্লোজ প্রাইস < Lower BB + ক্লোজ প্রাইস > EMA200
    if is_closed and not has_pos:
        lower_bb, upper_bb, ema200 = calculate_indicators(candle_data[symbol])
        
        if lower_bb is not None and ema200 is not None:
            last_close = candle_data[symbol][-1]
            
            if last_close < lower_bb and last_close > ema200:
                log_print(f"⚡ [BUY SIGNAL] {symbol} | Closed Price: ${last_close} < Lower BB: ${lower_bb:.4f} | EMA200: ${ema200:.4f}")
                try:
                    order = client.order_market_buy(symbol=symbol, quoteOrderQty=TRADE_AMOUNT_USDT)
                    
                    executed_price = last_close
                    if 'fills' in order and len(order['fills']) > 0:
                        executed_price = float(order['fills'][0]['price'])

                    positions[symbol] = True
                    buy_prices[symbol] = executed_price
                    log_print(f"✅ [MARKET BOUGHT] {symbol} @ ${executed_price:.4f} | Order ID: {order['orderId']}")

                except BinanceAPIException as e:
                    log_print(f"❌ [BUY ERROR] {symbol}: {e.message}")
                except Exception as e:
                    log_print(f"❌ [BUY EXCEPTION] {symbol}: {e}")

    # 💰 ২. মার্কেট সেল শর্ত: ক্যান্ডেল চলাকালীন রিয়েল-টাইম প্রাইস Upper BB স্পর্শ বা পার হলে
    elif has_pos:
        temp_closes = candle_data[symbol] + [current_price]
        _, upper_bb, _ = calculate_indicators(temp_closes)
        
        if upper_bb is not None and current_price >= upper_bb:
            entry_price = buy_prices[symbol]
            pnl_pct = ((current_price - entry_price) / entry_price) * 100 if entry_price > 0 else 0.0
            log_print(f"🎯 [SELL SIGNAL] {symbol} | Live Price: ${current_price} >= Upper BB: ${upper_bb:.4f} | PnL: {pnl_pct:+.2f}%")
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

        # ক্যান্ডেল ক্লোজ হলে মেমোরিতে ডাটা যুক্ত করা
        if is_closed and symbol in candle_data:
            candle_data[symbol].append(current_price)
            if len(candle_data[symbol]) > 250:
                candle_data[symbol].pop(0)

        # ট্রেডিং স্ট্র্যাটেজি চেক
        if symbol in candle_data:
            process_tick(symbol, current_price, is_closed)

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
    log_print("🤖 Real-Time Execution Bot Running! Scanning 47 Altcoins...")
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
