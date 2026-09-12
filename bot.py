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
# 1. FLASK APP FOR RENDER HEALTH CHECK
# ==========================================
app = Flask(__name__)

@app.route('/')
@app.route('/<path:path>')
def home(path=""):
    return "Trading Bot Active!", 200

def run_flask():
    port = int(os.environ.get("PORT", 10000))
    app.run(host='0.0.0.0', port=port, debug=False, use_reloader=False)

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
# 4. WEBSOCKET CALLBACKS
# ==========================================
def process_single_ticker(symbol, current_price, high_price, low_price):
    formatted_symbol = symbol.upper().replace('USDT', '/USDT')
    
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

        if current_time - last_print_time[symbol] >= 3:
            print(f"⚡ [SCAN {formatted_symbol}] Price: {current_price} | CRSI: {crsi:.1f} | Stoch: {stoch_k:.1f}", flush=True)
            last_print_time[symbol] = current_time

        buy_condition = (crsi < 20) and (stoch_k < 20)
        sell_condition = (prev_row.get('crsi', 0) <= 80 and crsi > 80) and (prev_row.get('stoch_k', 0) <= 80 and stoch_k > 80)

        if not positions[formatted_symbol] and buy_condition:
            crypto_quantity = trade_amount_usdt / current_price
            print(f"🔥 BUY SIGNAL: {formatted_symbol} at ${current_price}", flush=True)
            order = trade_exchange.create_market_buy_order(formatted_symbol, crypto_quantity)
            print(f"✅ EXECUTED BUY: {order}", flush=True)
            positions[formatted_symbol] = True
            entry_prices[formatted_symbol] = current_price

        elif positions[formatted_symbol]:
            stop_price = entry_prices[formatted_symbol] * (1 - stop_loss_pct)
            if current_price <= stop_price or sell_condition:
                crypto_quantity = trade_amount_usdt / entry_prices[formatted_symbol]
                print(f"🛑 EXIT/STOP LOSS: {formatted_symbol} at ${current_price}", flush=True)
                order = trade_exchange.create_market_sell_order(formatted_symbol, crypto_quantity)
                print(f"✅ EXECUTED SELL: {order}", flush=True)
                positions[formatted_symbol] = False
                entry_prices[formatted_symbol] = 0.0
    else:
        if current_time - last_print_time[symbol] >= 3:
            print(f"⏳ [SCAN {formatted_symbol}] Data Gathering: Price {current_price} ({len(df)}/14)", flush=True)
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
        print(f"Parsing error: {e}", flush=True)

def on_error(ws, error):
    print(f"❌ WS ERROR DETECTED: {error}", flush=True)

def on_close(ws, close_status_code, close_msg):
    print(f"⚠️ WS CLOSED: Code={close_status_code}, Msg={close_msg}", flush=True)

def on_open(ws):
    print("✅ GLOBAL WEBSOCKET CONNECTED! STARTING SCANNER...", flush=True)

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
            print("⏳ Connecting to Binance Global Stream...", flush=True)
            ws = websocket.WebSocketApp(
                ws_url,
                on_open=on_open,
                on_message=on_message,
                on_error=on_error,
                on_close=on_close
            )
            # ping_interval দেওয়া হলো যেন কানেকশন ড্রপ না করে
            ws.run_forever(ping_interval=20, ping_timeout=10)
            print("🔄 Loop ended, reconnecting in 3 seconds...", flush=True)
            time.sleep(3)
        except Exception as e:
            print(f"❌ Main Loop Exception: {e}", flush=True)
            time.sleep(3)
