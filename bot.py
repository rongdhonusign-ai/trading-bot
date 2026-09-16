import time
import os
import json
import pandas as pd
import ccxt
import websocket
from flask import Flask
from threading import Thread

# ==========================================
# 1. FLASK APP (Render Web Service Active রাখার জন্য)
# ==========================================
app = Flask(__name__)

bot_logs = []
last_prices = {}

@app.route('/')
def home():
    return "Binance Bot Active & Running!", 200

@app.route('/status')
def status():
    html = "<h2>🚀 Binance Custom Strategy Bot Status</h2>"
    html += "<p><b>Strategy:</b> BUY when Open < Lower BB & Close > Lower BB | SELL when Close > Upper BB (No Stop Loss)</p><hr>"
    html += "<h3>📜 Recent Activity Logs:</h3><pre style='background:#f4f4f4; padding:10px; border-radius:5px; max-height:400px; overflow-y:auto;'>"
    if bot_logs:
        html += "\n".join(bot_logs[-30:])
    else:
        html += "Waiting for WebSocket data..."
    html += "</pre>"
    return html, 200

def add_log(message):
    timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
    log_entry = f"[{timestamp}] {message}"
    print(log_entry, flush=True)
    bot_logs.append(log_entry)
    if len(bot_logs) > 150:
        bot_logs.pop(0)

# ==========================================
# 2. CONFIGURATION & STATIC TOP ALTCOINS
# ==========================================
trade_amount_usdt = 6.0    

trade_exchange = ccxt.binance({
    'apiKey': os.environ.get('BINANCE_API_KEY', 'yRwdwQAR1S9G8DLVeQp39lW99BAGEF4XDG6hoImJkFTol2RFvWmTvksMKy5Bav0M'),
    'secret': os.environ.get('BINANCE_SECRET_KEY', '3qsGUF6nPgfluSLPe8VXo0DE2gtR1jQIud9URVC5NHezEFp9YQV1lLqG1WncAltV'),
    'enableRateLimit': True,
    'options': {
        'defaultType': 'spot',
        'adjustForTimeDifference': True,
        'recvWindow': 10000
    }
})

def get_target_altcoins():
    add_log("✅ Loading static top Altcoins list (WebSocket Only - Safe Mode)...")
    return [
        'ethusdt', 'solusdt', 'bnbusdt', 'xrpusdt', 'adausdt', 'dogeusdt', 'avaxusdt', 
        'dotusdt', 'linkusdt', 'nearusdt', 'suiusdt', 'fetusdt', 'aptusdt', 'ltcusdt',
        'uniusdt', 'icpusdt', 'injusdt', 'renderusdt', 'tiausdt', 'seiusdt', 'arbusdt',
        'opusdt', 'wifusdt', 'flokiusdt', 'atomusdt', 'trxusdt', 'xlmusdt', 'ftmusdt', 
        'sandusdt', 'manausdt', 'galausdt', 'algousdt', 'ldousdt', 'qntusdt', 'aaveusdt', 
        'egldusdt', 'flowusdt', 'chzusdt', 'axsusdt', 'crvusdt', 'grtusdt', 'snxusdt', 
        'stxusdt', 'mkrusdt', 'kavausdt', 'compusdt', 'imxusdt'
    ]

target_symbols = get_target_altcoins()

prices_history = {sym: [] for sym in target_symbols}
positions = {sym: False for sym in target_symbols}
entry_prices = {sym: 0.0 for sym in target_symbols}
position_amounts = {sym: 0.0 for sym in target_symbols}

# ==========================================
# 3. TECHNICAL INDICATORS (BOLLINGER BANDS 20, 2)
# ==========================================
def calculate_indicators(df):
    # Bollinger Bands (20, std=2)
    df['sma_20'] = df['close'].rolling(window=20).mean()
    df['std_20'] = df['close'].rolling(window=20).std()
    df['bb_upper'] = df['sma_20'] + (df['std_20'] * 2)
    df['bb_lower'] = df['sma_20'] - (df['std_20'] * 2)
    return df

# ==========================================
# 4. WEBSOCKET DATA PROCESSOR
# ==========================================
def process_kline_data(symbol, open_price, close_price, high_price, low_price, is_closed):
    formatted_symbol = symbol.upper().replace('USDT', '/USDT')
    last_prices[symbol] = close_price
    
    if is_closed:
        prices_history[symbol].append({
            'open': open_price,
            'close': close_price,
            'high': high_price,
            'low': low_price
        })
        
        if len(prices_history[symbol]) > 100:
            prices_history[symbol].pop(0)

        df = pd.DataFrame(prices_history[symbol])

        # ⚡ ২০টির বেশি ক্যান্ডেল জমলেই Bollinger Bands হিসাব হবে
        if len(df) >= 20:
            df = calculate_indicators(df)
            last_row = df.iloc[-1]

            c_open = float(last_row['open'])
            c_close = float(last_row['close'])
            bb_upper = float(last_row['bb_upper'])
            bb_lower = float(last_row['bb_lower'])

            # 🎯 কৌশল শর্তাবলী
            buy_condition = (c_open < bb_lower) and (c_close > bb_lower)
            sell_condition = c_close > bb_upper

            # 🔍 লাইভ স্ক্যানিং লগ
            add_log(f"📊 [5M SCAN {formatted_symbol}] Open: ${c_open} | Close: ${c_close} | Lower BB: {bb_lower:.4f} | Upper BB: {bb_upper:.4f}")

            # 🛒 BUY EXECUTION (MARKET ORDER $6 USDT)
            if not positions[formatted_symbol] and buy_condition:
                add_log(f"🔥 BUY SIGNAL MATCHED (Open < Lower BB & Close > Lower BB): {formatted_symbol} at ${c_close}")
                try:
                    raw_qty = trade_amount_usdt / c_close
                    formatted_qty = float(trade_exchange.amount_to_precision(formatted_symbol, raw_qty))
                    
                    order = trade_exchange.create_market_buy_order(formatted_symbol, formatted_qty)
                    add_log(f"✅ EXECUTED MARKET BUY: {formatted_symbol} | Qty: {formatted_qty} | Order ID: {order['id']}")
                    
                    positions[formatted_symbol] = True
                    entry_prices[formatted_symbol] = c_close
                    position_amounts[formatted_symbol] = formatted_qty
                except Exception as e:
                    add_log(f"❌ BUY ERROR for {formatted_symbol}: {e}")

            # 💰 SELL EXECUTION (MARKET ORDER - ONLY AT UPPER BB)
            elif positions[formatted_symbol] and sell_condition:
                add_log(f"🛑 EXIT SIGNAL [UPPER BB CROSS]: {formatted_symbol} at ${c_close}")
                try:
                    qty = position_amounts[formatted_symbol]
                    formatted_qty = float(trade_exchange.amount_to_precision(formatted_symbol, qty))
                    
                    order = trade_exchange.create_market_sell_order(formatted_symbol, formatted_qty)
                    add_log(f"✅ EXECUTED MARKET SELL: {formatted_symbol} | Order ID: {order['id']}")
                    
                    positions[formatted_symbol] = False
                    entry_prices[formatted_symbol] = 0.0
                    position_amounts[formatted_symbol] = 0.0
                except Exception as e:
                    add_log(f"❌ SELL ERROR for {formatted_symbol}: {e}")
        else:
            add_log(f"⏳ [{formatted_symbol}] Gathering Candle History: ({len(df)}/20)")

def on_message(ws, message):
    try:
        data = json.loads(message)
        if 'data' in data:
            kline = data['data']['k']
            sym = kline['s'].lower()
            if sym in target_symbols:
                open_price = float(kline['o'])
                close_price = float(kline['c'])
                high_price = float(kline['h'])
                low_price = float(kline['l'])
                is_closed = kline['x']
                process_kline_data(sym, open_price, close_price, high_price, low_price, is_closed)
    except Exception:
        pass

def on_error(ws, error):
    add_log(f"❌ WS ERROR: {error}")

def on_close(ws, close_status_code, close_msg):
    add_log("⚠️ WS Connection Closed. Reconnecting in 5 seconds...")

def on_open(ws):
    add_log("✅ CONNECTED TO BINANCE 5M KLINE STREAM (Safe Mode)")

def start_websocket():
    streams = "/".join([f"{sym}@kline_5m" for sym in target_symbols])
    ws_url = f"wss://stream.binance.com:9443/stream?streams={streams}"
    while True:
        try:
            ws = websocket.WebSocketApp(
                ws_url,
                on_open=on_open,
                on_message=on_message,
                on_error=on_error,
                on_close=on_close
            )
            ws.run_forever(ping_interval=20, ping_timeout=10)
            time.sleep(5)
        except Exception as e:
            add_log(f"❌ Connection Exception: {e}")
            time.sleep(5)

# ==========================================
# 5. MAIN EXECUTION
# ==========================================
if __name__ == '__main__':
    ws_thread = Thread(target=start_websocket)
    ws_thread.daemon = True
    ws_thread.start()
    
    port = int(os.environ.get("PORT", 10000))
    app.run(host='0.0.0.0', port=port, debug=False, use_reloader=False)
