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
# 0. TRADE CONFIGURATION & RULES
# ==========================================
TRADE_AMOUNT_USDT = 40.0   # প্রতি ট্রেডে ৪০ ডলারের মার্কেট বাই
STOP_LOSS_PERCENT = 0.03    # ৩% সেফটি স্টপ লস (০.০৩ = ৩%)

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
            <title>RSI Instant Execution Trading Bot ($40 Spot)</title>
            <meta http-equiv="refresh" content="5">
            <style>
                body {{ background-color: #0d1117; color: #3fb950; font-family: monospace; padding: 20px; }}
                h2 {{ color: #58a6ff; }}
                .log-box {{ background: #161b22; padding: 15px; border-radius: 6px; border: 1px solid #30363d; max-height: 80vh; overflow-y: auto; }}
            </style>
        </head>
        <body>
            <h2>🤖 Real-Time Execution Spot Bot (RSI Strategy)</h2>
            <p>Strategy: Market Buy (RSI50 > 50 AND RSI3 < 5) | Market Sell (RSI3 > 85 OR 3% Stop Loss)</p>
            <hr>
            <div class="log-box">{logs_html if logs_html else "Initializing scanner and preloading data..."}</div>
        </body>
    </html>
    """, 200

# ==========================================
# 2. BINANCE CLIENT SETUP & TARGET 100 ALTCOINS
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
        "THETAUSDT", "JASMYUSDT", "CKBUSDT", "XLMUSDT", "KSMUSDT", 
        "GLMRUSDT", "ZILUSDT", "IOTAUSDT", "GMTUSDT", "LPTUSDT"
    ]

# Global Trackers
candle_data = {}
positions = {}      # {symbol: True/False}
buy_prices = {}     # {symbol: entry_price}

symbols = get_target_altcoins()
for sym in symbols:
    candle_data[sym] = []
    positions[sym] = False
    buy_prices[sym] = 0.0

# ==========================================
# 3. FAST HISTORICAL CANDLE PRELOADER (IP SAFE)
# ==========================================
def preload_history():
    log_print("🔄 Preloading candles for 100 Tokens (IP Safe & Ultra Fast)...")
    # ১০০টি টোকেন এর জন্য কে-লাইন ফেচ করা (৬০টি ৫m ক্যান্ডেল RSI(50) ক্যালকুলেশনের জন্য যথেষ্ট)
    for sym in symbols:
        try:
            klines = client.get_klines(symbol=sym, interval=Client.KLINE_INTERVAL_5MINUTE, limit=75)
            closes = [float(k[4]) for k in klines]
            candle_data[sym] = closes
            time.sleep(0.015)  # Render IP Ban ঠেকানোর নিরাপদ ইন্টারভাল (প্রায় ১.৫ সেকেন্ডেই ১০০ টোকেন লোড শেষ)
        except Exception as e:
            log_print(f"⚠️ Preload error for {sym}: {e}")
    log_print("✅ Preload Complete! Websocket Real-time Trading Active.")

# ==========================================
# 4. RSI INDICATOR CALCULATION
# ==========================================
def calculate_rsi(series, period):
    delta = series.diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
    
    # zero division সমস্যা এড়াতে small epsilon যোগ করা হয়েছে
    rs = gain / (loss + 1e-10)
    rsi = 100 - (100 / (1 + rs))
    return rsi.iloc[-1]

def get_rsi_indicators(closes):
    df = pd.DataFrame({'close': closes})
    rsi_50 = calculate_rsi(df['close'], period=50)
    rsi_3 = calculate_rsi(df['close'], period=3)
    return rsi_50, rsi_3

# ==========================================
# 5. INSTANT EXECUTION STRATEGY LOGIC
# ==========================================
def process_tick(symbol, current_price):
    if len(candle_data[symbol]) < 55:
        return

    # পয়েন্ট-ইন-টাইম কারেন্ট প্রাইস যোগ করে ইন্ডিকেটর নির্ণয়
    temp_closes = candle_data[symbol] + [current_price]
    rsi_50, rsi_3 = get_rsi_indicators(temp_closes)
    
    has_pos = positions[symbol]
    entry_price = buy_prices[symbol]

    # 🛒 ১. ইনস্ট্যান্ট মার্কেট বাই: RSI(50) > 50 এবং RSI(3) < 5 হলে
    if not has_pos and rsi_50 > 50 and rsi_3 < 5:
        log_print(f"⚡ [BUY TRIGGER] {symbol} | Price: ${current_price} | RSI(50): {rsi_50:.2f} (>50) | RSI(3): {rsi_3:.2f} (<5)")
        try:
            order = client.order_market_buy(symbol=symbol, quoteOrderQty=TRADE_AMOUNT_USDT)
            
            executed_price = current_price
            if 'fills' in order and len(order['fills']) > 0:
                executed_price = float(order['fills'][0]['price'])

            positions[symbol] = True
            buy_prices[symbol] = executed_price
            log_print(f"✅ [BOUGHT MARKET] {symbol} @ ${executed_price:.4f} | Order ID: {order['orderId']}")

        except BinanceAPIException as e:
            log_print(f"❌ [BUY ERROR] {symbol}: {e.message}")
        except Exception as e:
            log_print(f"❌ [BUY EXCEPTION] {symbol}: {e}")

    # 🚨 ২. সেফটি ৩% স্টপ লস সেল
    elif has_pos and entry_price > 0 and current_price <= (entry_price * (1 - STOP_LOSS_PERCENT)):
        loss_pct = ((current_price - entry_price) / entry_price) * 100
        log_print(f"🚨 [STOP LOSS] {symbol} | Price: ${current_price} ({loss_pct:.2f}% drop from entry ${entry_price:.4f})")
        execute_market_sell(symbol)

    # 💰 ৩. ইনস্ট্যান্ট মার্কেট সেল: RSI(3) > 85 হলে
    elif has_pos and rsi_3 > 85:
        log_print(f"🎯 [PROFIT SELL TRIGGER] {symbol} | Price: ${current_price} | RSI(3): {rsi_3:.2f} (>85)")
        execute_market_sell(symbol)

def execute_market_sell(symbol):
    try:
        asset = symbol.replace("USDT", "")
        balance_info = client.get_asset_balance(asset=asset)
        free_qty = float(balance_info['free']) if balance_info else 0.0

        if free_qty > 0:
            # এক্সচেঞ্জের Precision বা Step size ফিক্স করতে ফরম্যাটিং
            info = client.get_symbol_info(symbol)
            step_size = None
            for f in info['filters']:
                if f['filterType'] == 'LOT_SIZE':
                    step_size = float(f['stepSize'])
                    break

            if step_size:
                precision = int(round(-np.log10(step_size)))
                free_qty = float(np.floor(free_qty * (10 ** precision)) / (10 ** precision))

            if free_qty > 0:
                order = client.order_market_sell(symbol=symbol, quantity=free_qty)
                log_print(f"✅ [SOLD MARKET] {symbol} | Order ID: {order['orderId']}")
            else:
                log_print(f"⚠️ [SELL SKIPPED] {symbol}: Quantity zero after step size truncation.")
        else:
            log_print(f"⚠️ [SELL SKIPPED] {symbol}: No free balance.")

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

        # লাইভ প্রসেসিং
        if symbol in candle_data:
            process_tick(symbol, current_price)

        # ৫-মিনিটের ক্যান্ডেল ক্লোজ ব্যাকলগ আপডেট
        if is_closed and symbol in candle_data:
            candle_data[symbol].append(current_price)
            if len(candle_data[symbol]) > 80:
                candle_data[symbol].pop(0)

def start_websocket():
    streams = [f"{sym.lower()}@kline_5m" for sym in symbols]
    socket_url = f"wss://stream.binance.com:9443/ws/{'/'.join(streams)}"
    
    ws = websocket.WebSocketApp(
        socket_url,
        on_message=on_message,
        on_error=lambda ws, err: log_print(f"❌ WS Error: {err}"),
        on_close=lambda ws, code, msg: log_print("🔌 Connection Closed. Reconnecting...")
    )
    ws.run_forever()

def start_bot():
    preload_history()
    log_print("🤖 RSI Strategy Active! Live scanning 100 Altcoins...")
    while True:
        try:
            start_websocket()
        except Exception as e:
            log_print(f"⚠️ WebSocket disconnect: {e}. Reconnecting in 5s...")
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
