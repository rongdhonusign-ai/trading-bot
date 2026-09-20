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
import logging

# ==========================================
# 1. LIVE LOG BUFFER & FLASK WEB SERVER
# ==========================================
# সাম্প্রতিক ১০০টি লগ মেমরিতে জমা রাখার জন্য তৈরি
log_buffer = collections.deque(maxlen=100)

def log_print(message):
    """টার্মিনালেও প্রিন্ট করবে এবং ব্রাউজারে দেখার জন্য মেমরিতে সেভ রাখবে"""
    print(message)
    log_buffer.append(message)

app = Flask(__name__)

# Flask-এর অতিরিক্ত সার্ভিস লগ (GET /status 200) হাইড রাখা হলো
log = logging.getLogger('werkzeug')
log.setLevel(logging.ERROR)

@app.route('/')
@app.route('/status')
def status():
    # ব্রাউজারে টার্মিনালের মতো লাইভ লগ দেখার এইচটিএমএল ডিজাইন
    logs_html = "<br>".join(log_buffer)
    return f"""
    <html>
        <head>
            <title>Trading Bot Dashboard</title>
            <meta http-equiv="refresh" content="10"> <!-- প্রতি ১০ সেকেন্ডে পেজ অটো রিফ্রেশ হবে -->
            <style>
                body {{ 
                    background-color: #0d1117; 
                    color: #3fb950; 
                    font-family: 'Courier New', Courier, monospace; 
                    padding: 20px; 
                    line-height: 1.5;
                }}
                h2 {{ color: #58a6ff; margin-bottom: 5px; }}
                p {{ color: #8b949e; margin-top: 0; font-size: 14px; }}
                hr {{ border: 0; height: 1px; background: #30363d; margin-bottom: 20px; }}
                .log-box {{ 
                    background: #161b22; 
                    padding: 15px; 
                    border-radius: 6px; 
                    border: 1px solid #30363d; 
                    max-height: 80vh; 
                    overflow-y: auto; 
                }}
            </style>
        </head>
        <body>
            <h2>🤖 Trading Bot Live Dashboard (EMA 100)</h2>
            <p>Status: Active & Scanning 47 Altcoins | Auto-refreshes every 10s</p>
            <hr>
            <div class="log-box">
                {logs_html if logs_html else "Waiting for initial scanning logs..."}
            </div>
        </body>
    </html>
    """, 200

# ==========================================
# 2. BINANCE CLIENT SETUP & TARGET ALTCOINS
# ==========================================
API_KEY = os.environ.get("BINANCE_API_KEY", "yRwdwQAR1S9G8DLVeQp39lW99BAGEF4XDG6hoImJkFTol2RFvWmTvksMKy5Bav0M")
API_SECRET = os.environ.get("BINANCE_API_SECRET", "3qsGUF6nPgfluSLPe8VXo0DE2gtR1jQIud9URVC5NHezEFp9YQV1lLqG1WncAltV")

client = Client(API_KEY, API_SECRET)

def get_target_altcoins():
    """স্ক্যান করার জন্য ৪৭টি টোকেন"""
    return [
        "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT", "ADAUSDT", 
        "DOGEUSDT", "AVAXUSDT", "DOTUSDT", "LINKUSDT", "NEARUSDT", 
        "SUIUSDT", "FETUSDT", "APTUSDT", "LTCUSDT", "UNIUSDT", 
        "ICPUSDT", "INJUSDT", "RENDERUSDT", "TIAUSDT", "SEIUSDT", 
        "ARBUSDT", "OPUSDT", "WIFUSDT", "FLOKIUSDT", "ATOMUSDT", 
        "TRXUSDT", "XLMUSDT", "FTMUSDT", "SANDUSDT", "MANAUSDT", 
        "GALAUSDT", "ALGOUSDT", "LDOUSDT", "QNTUSDT", "AAVEUSDT", 
        "EGLDUSDT", "FLOWUSDT", "CHZUSDT", "AXSUSDT", "CRVUSDT", 
        "GRTUSDT", "SNXUSDT", "STXUSDT", "MKRUSDT", "KAVAUSDT", 
        "COMPUSDT", "IMXUSDT"
    ]

# Global Storage
candle_data = {}
positions = {}

symbols = get_target_altcoins()
for sym in symbols:
    candle_data[sym] = []
    positions[sym] = False

# ==========================================
# 3. HISTORICAL CANDLE PRELOADER (EMA 100)
# ==========================================
def preload_history():
    log_print("🔄 Preloading historical candle data for EMA 100...")
    for sym in symbols:
        try:
            klines = client.get_klines(symbol=sym, interval=Client.KLINE_INTERVAL_5MINUTE, limit=150)
            closes = [float(k[4]) for k in klines]
            candle_data[sym] = closes
            log_print(f"✅ [{sym}] History Loaded: ({len(closes)} candles)")
            time.sleep(0.1)
        except Exception as e:
            log_print(f"⚠️ Error loading history for {sym}: {e}")

# ==========================================
# 4. INDICATOR & STRATEGY LOGIC (EMA 100)
# ==========================================
def calculate_indicators(closes):
    df = pd.DataFrame({'close': closes})
    
    # Bollinger Bands
    df['SMA20'] = df['close'].rolling(window=20).mean()
    df['STD20'] = df['close'].rolling(window=20).std()
    df['Upper_BB'] = df['SMA20'] + (df['STD20'] * 2)
    df['Lower_BB'] = df['SMA20'] - (df['STD20'] * 2)
    
    # EMA 100
    df['EMA100'] = df['close'].ewm(span=100, adjust=False).mean()
    
    latest = df.iloc[-1]
    return latest['close'], latest['Lower_BB'], latest['Upper_BB'], latest['EMA100']

def evaluate_strategy(symbol, close, lower_bb, upper_bb, ema100):
    has_pos = positions[symbol]
    
    # 🛒 BUY CONDITION
    if not has_pos and close <= lower_bb and close > ema100:
        log_print(f"🚀 [BUY SIGNAL TRIGGERED] {symbol} | Price: ${close} | Lower BB: ${lower_bb:.4f} | EMA 100: ${ema100:.4f}")
        positions[symbol] = True

    # 💰 SELL CONDITION
    elif has_pos and close >= upper_bb:
        log_print(f"🎯 [SELL SIGNAL TRIGGERED] {symbol} | Price: ${close} | Upper BB: ${upper_bb:.4f}")
        positions[symbol] = False

# ==========================================
# 5. WEBSOCKET LISTENER FOR 5M CANDLES
# ==========================================
def on_message(ws, message):
    data = json.loads(message)
    
    if 'k' in data:
        k = data['k']
        is_closed = k['x']
        symbol = data['s']
        close_price = float(k['c'])
        
        if is_closed:
            if symbol in candle_data:
                candle_data[symbol].append(close_price)
                
                if len(candle_data[symbol]) > 150:
                    candle_data[symbol].pop(0)
                
                if len(candle_data[symbol]) >= 100:
                    close, lower_bb, upper_bb, ema100 = calculate_indicators(candle_data[symbol])
                    
                    # লাইভ লগ তৈরি
                    log_print(f"📊 [5M SCAN {symbol}] Close: ${close:.4f} | Lower BB: ${lower_bb:.4f} | Upper BB: ${upper_bb:.4f} | EMA 100: ${ema100:.4f}")
                    
                    evaluate_strategy(symbol, close, lower_bb, upper_bb, ema100)
                else:
                    log_print(f"⏳ [{symbol}] Gathering Candle History for EMA 100: ({len(candle_data[symbol])}/100)")

def start_websocket():
    streams = [f"{sym.lower()}@kline_5m" for sym in symbols]
    socket_url = f"wss://stream.binance.com:9443/ws/{'/'.join(streams)}"
    
    ws = websocket.WebSocketApp(
        socket_url,
        on_message=on_message,
        on_error=lambda ws, err: log_print(f"❌ WS Error: {err}"),
        on_close=lambda ws, code, msg: log_print("🔌 WS Connection Closed")
    )
    ws.run_forever()

def start_bot():
    preload_history()
    log_print("🤖 Bot Trading Logic (EMA 100) Started Successfully...")
    start_websocket()

# ==========================================
# 6. MAIN EXECUTION
# ==========================================
if __name__ == '__main__':
    bot_thread = threading.Thread(target=start_bot)
    bot_thread.daemon = True
    bot_thread.start()
    
    port = int(os.environ.get("PORT", 5000))
    app.run(host='0.0.0.0', port=port)
