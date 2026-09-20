import os
import time
import json
import threading
import pandas as pd
import numpy as np
import websocket
from flask import Flask
from binance.client import Client
import logging

# ==========================================
# 1. FLASK WEB SERVER & LOGGING SETUP
# ==========================================
app = Flask(__name__)

# 🛠️ Flask-এর Werkzeug সার্ভার লগ (GET /status) বন্ধ রাখা হলো
log = logging.getLogger('werkzeug')
log.setLevel(logging.ERROR)

@app.route('/')
@app.route('/status')
def status():
    return "Trading Bot is Live and Operational!", 200

# ==========================================
# 2. BINANCE CLIENT SETUP & TARGET ALTCOINS
# ==========================================
API_KEY = os.environ.get("BINANCE_API_KEY", "yRwdwQAR1S9G8DLVeQp39lW99BAGEF4XDG6hoImJkFTol2RFvWmTvksMKy5Bav0M")
API_SECRET = os.environ.get("BINANCE_API_SECRET", "3qsGUF6nPgfluSLPe8VXo0DE2gtR1jQIud9URVC5NHezEFp9YQV1lLqG1WncAltV")

client = Client(API_KEY, API_SECRET)

def get_target_altcoins():
    """বট যে ৪৭টি টোকেন স্ক্যান করবে তার তালিকা"""
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

# Global Data Store & State Tracking
candle_data = {}
positions = {}

symbols = get_target_altcoins()
for sym in symbols:
    candle_data[sym] = []
    positions[sym] = False  # শুরুতে কোনো পজিশন কেনা নেই

# ==========================================
# 3. HISTORICAL CANDLE PRELOADER (EMA 100)
# ==========================================
def preload_history():
    """EMA 100 ক্যালকুলেশনের জন্য ক্যান্ডেল প্রিলোড করা"""
    print("🔄 Preloading historical candle data for EMA 100...")
    for sym in symbols:
        try:
            klines = client.get_klines(symbol=sym, interval=Client.KLINE_INTERVAL_5MINUTE, limit=150)
            closes = [float(k[4]) for k in klines]
            candle_data[sym] = closes
            print(f"✅ [{sym}] History Loaded: ({len(closes)} candles)")
            time.sleep(0.1)  # Binance API Rate Limit এড়াতে
        except Exception as e:
            print(f"⚠️ Error loading history for {sym}: {e}")

# ==========================================
# 4. INDICATOR & STRATEGY LOGIC (EMA 100)
# ==========================================
def calculate_indicators(closes):
    """Bollinger Bands (20, 2) এবং EMA 100 গণনা করা"""
    df = pd.DataFrame({'close': closes})
    
    # Bollinger Bands
    df['SMA20'] = df['close'].rolling(window=20).mean()
    df['STD20'] = df['close'].rolling(window=20).std()
    df['Upper_BB'] = df['SMA20'] + (df['STD20'] * 2)
    df['Lower_BB'] = df['SMA20'] - (df['STD20'] * 2)
    
    # EMA 100 (আগে EMA 200 ছিল)
    df['EMA100'] = df['close'].ewm(span=100, adjust=False).mean()
    
    latest = df.iloc[-1]
    return latest['close'], latest['Lower_BB'], latest['Upper_BB'], latest['EMA100']

def evaluate_strategy(symbol, close, lower_bb, upper_bb, ema100):
    """বাই/সেল শর্ত যাচাই ও এক্সিকিউশন"""
    has_pos = positions[symbol]
    
    # 🛒 BUY CONDITION: (Close <= Lower BB) AND (Close > EMA 100) AND (No Position)
    if not has_pos and close <= lower_bb and close > ema100:
        print(f"🚀 [BUY SIGNAL TRIGGERED] {symbol} | Price: ${close} | Lower BB: ${lower_bb:.4f} | EMA 100: ${ema100:.4f}")
        # বাই অর্ডার ফাংশন
        positions[symbol] = True

    # 💰 SELL CONDITION: (Close >= Upper BB) AND (Has Position)
    elif has_pos and close >= upper_bb:
        print(f"🎯 [SELL SIGNAL TRIGGERED] {symbol} | Price: ${close} | Upper BB: ${upper_bb:.4f}")
        # সেল অর্ডার ফাংশন
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
        
        # ৫-মিনিটের ক্যান্ডেল ক্লোজ হলে প্রসেস করবে
        if is_closed:
            if symbol in candle_data:
                candle_data[symbol].append(close_price)
                
                # হিস্ট্রি ক্যান্ডেল ১৫০টির মধ্যে সীমাবদ্ধ রাখা
                if len(candle_data[symbol]) > 150:
                    candle_data[symbol].pop(0)
                
                # যদি ক্যান্ডেল সংখ্যা ১০০ বা তার বেশি হয়
                if len(candle_data[symbol]) >= 100:
                    close, lower_bb, upper_bb, ema100 = calculate_indicators(candle_data[symbol])
                    
                    # স্ক্যানিং প্রিন্ট
                    print(f"📊 [5M SCAN {symbol}] Close: ${close:.4f} | Lower BB: ${lower_bb:.4f} | Upper BB: ${upper_bb:.4f} | EMA 100: ${ema100:.4f}")
                    
                    # স্ট্রাটেজি ইভালুয়েট
                    evaluate_strategy(symbol, close, lower_bb, upper_bb, ema100)
                else:
                    print(f"⏳ [{symbol}] Gathering Candle History for EMA 100: ({len(candle_data[symbol])}/100)")

def start_websocket():
    streams = [f"{sym.lower()}@kline_5m" for sym in symbols]
    socket_url = f"wss://stream.binance.com:9443/ws/{'/'.join(streams)}"
    
    ws = websocket.WebSocketApp(
        socket_url,
        on_message=on_message,
        on_error=lambda ws, err: print(f"❌ WS Error: {err}"),
        on_close=lambda ws, code, msg: print("🔌 WS Connection Closed")
    )
    ws.run_forever()

def start_bot():
    preload_history()
    print("🤖 Bot Trading Logic (EMA 100) Started Successfully...")
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
