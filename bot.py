import time
import os
import json
import sys
import pandas as pd
import ccxt
import websocket
from flask import Flask
from threading import Thread

# ==========================================
# 1. FLASK APP FOR HEALTH CHECK & LIVE STATUS
# ==========================================
app = Flask(__name__)

# গ্লোবাল স্ট্যাটাস স্টোর করার জন্য
bot_logs = []
last_prices = {}

@app.route('/')
def home():
    return "Trading Bot Active!", 200

@app.route('/status')
def status():
    html = "<h2>🚀 Binance 24/7 Bot Live Status</h2>"
    html += "<p><b>System:</b> Active & Scanning via Global WebSocket</p><hr>"
    
    html += "<h3>📊 Live Coin Buffer & Prices:</h3><ul>"
    for sym in sorted(target_symbols):
        hist_len = len(prices_history.get(sym, []))
        price = last_prices.get(sym, 'N/A')
        formatted = sym.upper().replace('USDT', '/USDT')
        html += f"<li><b>{formatted}:</b> ${price} | Buffer Data: {hist_len}/14</li>"
    html += "</ul><hr>"

    html += "<h3>📜 Recent Activity Logs:</h3><pre style='background:#f4f4f4; padding:10px; border-radius:5px;'>"
    if bot_logs:
        html += "\n".join(bot_logs[-25:])
    else:
        html += "Waiting for data updates..."
    html += "</pre>"
    
    return html, 200

def run_flask():
    port = int(os.environ.get("PORT", 10000))
    app.run(host='0.0.0.0', port=port, debug=False, use_reloader=False)

def add_log(message):
    timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
    log_entry = f"[{timestamp}] {message}"
    print(log_entry, flush=True)
    bot_logs.append(log_entry)
    if len(bot_logs) > 100:
        bot_logs.pop(0)

# ==========================================
# 2. CONFIGURATION & TARGET SYMBOLS
# ==========================================
target_symbols = [
    'listausdt', 'flokiusdt', 'bmtusdt', 'bnbusdt', 'theusdt', 
    'belusdt', 'cakeusdt', 'ontusdt', 'zamausdt', 'megausdt', 
    'ensusdt', 'bicousdt', 'tusdt', 'ssvusdt', 'glmusdt', 
    'altusdt', 'axlusdt', 'iousdt', 'zrousdt', 'heiusdt', 
    'redusdt', 'zkusdt', 'qntusdt', 'thetausdt', 'trbusdt', 
    'zenusdt', 'iotxusdt', 'berausdt'
]

trade_amount_usdt = 6.0   
stop_loss_pct = 0.02      

trade_exchange = ccxt.binance({
    'apiKey': os.environ.get('BINANCE_API_KEY', 'yRwdwQAR1S9G8DLVeQp39lW99BAGEF4XDG6hoImJkFTol2RFvWmTvksMKy5Bav0M'),
    'secret': os.environ.get('BINANCE_SECRET_KEY', '3qsGUF6nPgfluSLPe8VXo0DE2gtR1jQIud9URVC5NHezEFp9YQV1lLqG1WncAltV'),
    'enableRateLimit': True,
    'options': {
        'defaultType': 'spot',
        'adjustForTimeDifference': False,
        'recvWindow': 10000
    }
})

prices_history = {sym: [] for sym in target_symbols}
positions = {sym: False for sym in target_symbols}
entry_prices = {sym: 0.0 for sym in target_symbols}
last_print_time = {sym: 0 for sym in target_symbols}

# ==========================================
# 3. INDICATOR CALCULATIONS
# ==========================================
def calculate_rsi(series, period):
    delta = series.diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
    rs = gain / loss
    return 100 - (100 / (1 + rs))

def calculate_indicators(df):
    df['rsi'] = calculate_rsi(df['close'], 3)
    updn = [0.0] * len(df)
    close_vals = df['close'].values
    for i in range(1, len(df)):
        if close_vals[i] > close_vals[i-1]:
            updn[i] = updn[i-1] + 1 if updn[i-1] > 0 else 1
        elif close_vals[i] < close_vals[i-1]:
            updn[i] = updn[i-1] - 1 if updn[i-1] < 0 else -1
        else:
            updn[i] = 0
            
    df['updn'] = updn
    df['updn_rsi'] = calculate_rsi(pd.Series(updn), 2)
    roc = df['close'].pct_change(1)
    df['percent_rank'] = roc.rolling(2).apply(
        lambda x: (pd.Series(x).rank(pct=True).iloc[-1]) * 100, raw=False
    )
    df['crsi'] = (df['rsi'] + df['updn_rsi'] + df['percent_rank']) / 3
    low_min = df['low'].rolling(window=14).min()
    high_max = df['high'].rolling(window=14).max()
    stoch_raw = 100 * ((df['close'] - low_min) / (high_max - low_min))
    df['stoch_k'] = stoch_raw.rolling(window=3).mean().rolling(window=3).mean()
    return df

# ==========================================
# 4. WEBSOCKET PROCESSOR
# ==========================================
def process_single_ticker(symbol, current_price, high_price, low_price):
    formatted_symbol = symbol.upper().replace('USDT', '/USDT')
    last_prices[symbol] = current_price
    
    prices_history[symbol].append({
        'close': current_price,
        'high': high_price,
        'low': low_price
    })
    
    if len(prices_history[symbol]) > 30:
        prices_history[symbol].pop(0)

    df = pd.DataFrame(prices_history[symbol])
    current_time = time.time()

    if len(df) >= 14:
        df = calculate_indicators(df)
        last_row = df.iloc[-1]
        prev_row = df.iloc[-2]

        crsi = last_row.get('crsi', 0)
        stoch_k = last_row.get('stoch_k', 0)

        if current_time - last_print_time[symbol] >= 5:
            add_log(f"⚡ [SCAN {formatted_symbol}] Price: {current_price} | CRSI: {crsi:.1f} | Stoch: {stoch_k:.1f}")
            last_print_time[symbol] = current_time

        buy_condition = (crsi < 20) and (stoch_k < 20)
        sell_condition = (prev_row.get('crsi', 0) <= 80 and crsi > 80) and (prev_row.get('stoch_k', 0) <= 80 and stoch_k > 80)

        if not positions[formatted_symbol] and buy_condition:
            crypto_quantity = trade_amount_usdt / current_price
            add_log(f"🔥 BUY SIGNAL: {formatted_symbol} at ${current_price}")
            order = trade_exchange.create_market_buy_order(formatted_symbol, crypto_quantity)
            add_log(f"✅ EXECUTED BUY: {order}")
            positions[formatted_symbol] = True
            entry_prices[formatted_symbol] = current_price

        elif positions[formatted_symbol]:
            stop_price = entry_prices[formatted_symbol] * (1 - stop_loss_pct)
            if current_price <= stop_price or sell_condition:
                crypto_quantity = trade_amount_usdt / entry_prices[formatted_symbol]
                add_log(f"🛑 EXIT/STOP LOSS: {formatted_symbol} at ${current_price}")
                order = trade_exchange.create_market_sell_order(formatted_symbol, crypto_quantity)
                add_log(f"✅ EXECUTED SELL: {order}")
                positions[formatted_symbol] = False
                entry_prices[formatted_symbol] = 0.0
    else:
        if current_time - last_print_time[symbol] >= 5:
            add_log(f"⏳ [SCAN {formatted_symbol}] Data Gathering: Price {current_price} ({len(df)}/14)")
            last_print_time[symbol] = current_time

def on_message(ws, message):
    try:
        data = json.loads(message)
        if isinstance(data, list):
            for item in data:
                sym = item.get('s', '').lower()
                if sym in target_symbols:
                    close_price = float(item['c'])
                    high_price = float(item['h'])
                    low_price = float(item['l'])
                    process_single_ticker(sym, close_price, high_price, low_price)
    except Exception as e:
        pass

def on_error(ws, error):
    add_log(f"❌ WS ERROR: {error}")

def on_close(ws, close_status_code, close_msg):
    add_log(f"⚠️ WS CLOSED: Code={close_status_code}")

def on_open(ws):
    add_log("✅ GLOBAL WEBSOCKET CONNECTED! REAL-TIME SCAN RUNNING...")

# ==========================================
# 5. MAIN EXECUTION
# ==========================================
if __name__ == '__main__':
    flask_thread = Thread(target=run_flask)
    flask_thread.daemon = True
    flask_thread.start()
    
    ws_url = "wss://stream.binance.com:9443/ws/!ticker@arr"
    
    while True:
        try:
            add_log("⏳ Connecting to Binance Stream...")
            ws = websocket.WebSocketApp(
                ws_url,
                on_open=on_open,
                on_message=on_message,
                on_error=on_error,
                on_close=on_close
            )
            ws.run_forever(ping_interval=20, ping_timeout=10)
            time.sleep(3)
        except Exception as e:
            add_log(f"❌ Main Exception: {e}")
            time.sleep(3)
