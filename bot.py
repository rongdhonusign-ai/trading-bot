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
# 1. FLASK APP (HEALTH CHECK FOR RENDER)
# ==========================================
app = Flask(__name__)

@app.route('/')
@app.route('/<path:path>')
def home(path=""):
    return "Trading Bot is Live!", 200

def run_flask():
    port = int(os.environ.get("PORT", 10000))
    # Flask-এর লগ যেন কনসোল না ভরিয়ে ফেলে তার জন্য quiet রান করা হচ্ছে
    app.run(host='0.0.0.0', port=port, debug=False, use_reloader=False)

# ==========================================
# 2. CONFIGURATION & SYMBOL LIST
# ==========================================
symbols = [
    'listaUsdt', 'flokiUsdt', 'bmtUsdt', 'bnbUsdt', 'theUsdt', 
    'belUsdt', 'cakeUsdt', 'ontUsdt', 'zamaUsdt', 'megaUsdt', 
    'ensUsdt', 'bicoUsdt', 'tUsdt', 'ssvUsdt', 'glmUsdt', 
    'altUsdt', 'axlUsdt', 'ioUsdt', 'zroUsdt', 'heiUsdt', 
    'redUsdt', 'zkUsdt', 'qntUsdt', 'thetaUsdt', 'trbUsdt', 
    'zenUsdt', 'iotxUsdt', 'beraUsdt'
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

trade_exchange.has['fetchMarkets'] = False
trade_exchange.has['fetchCurrencies'] = False

prices_history = {sym: [] for sym in symbols}
positions = {sym: False for sym in symbols}
entry_prices = {sym: 0.0 for sym in symbols}

# ==========================================
# 3. INDICATORS
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
def process_ticker_data(symbol_key, current_price, high_price, low_price):
    formatted_symbol = symbol_key.upper().replace('USDT', '/USDT')
    
    prices_history[symbol_key].append({
        'close': current_price,
        'high': high_price,
        'low': low_price
    })
    
    if len(prices_history[symbol_key]) > 30:
        prices_history[symbol_key].pop(0)

    df = pd.DataFrame(prices_history[symbol_key])

    if len(df) >= 14:
        df = calculate_indicators(df)
        last_row = df.iloc[-1]
        prev_row = df.iloc[-2]

        crsi = last_row.get('crsi', 0)
        stoch_k = last_row.get('stoch_k', 0)

        print(f"⚡ [SCAN {formatted_symbol}] Price: {current_price} | CRSI: {crsi:.1f} | Stoch: {stoch_k:.1f}", flush=True)

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
        print(f"⏳ [SCAN {formatted_symbol}] Gathering Price Data: {current_price} ({len(df)}/14)", flush=True)

def on_message(ws, message):
    try:
        data = json.loads(message)
        if 'data' in data:
            ticker = data['data']
            symbol = ticker['s'].lower()
            current_price = float(ticker['c'])
            high_price = float(ticker['h'])
            low_price = float(ticker['l'])
            
            process_ticker_data(symbol, current_price, high_price, low_price)
    except Exception as e:
        print(f"WS Parsing Error: {e}", flush=True)

def on_open(ws):
    print("✅ LIVE WEBSOCKET CONNECTED! STARTING REAL-TIME MARKET SCAN...", flush=True)

# ==========================================
# 5. MAIN ENTRY POINT
# ==========================================
if __name__ == '__main__':
    # ১. Flask-কে ব্যাকগ্রাউন্ড থ্রেডে চালনা
    flask_thread = Thread(target=run_flask)
    flask_thread.daemon = True
    flask_thread.start()
    
    # ২. WebSocket-কে মেইন থ্রেডে চালানো
    streams = "/".join([f"{sym}@ticker" for sym in symbols])
    ws_url = f"wss://stream.binance.com:9443/stream?streams={streams}"
    
    while True:
        try:
            print("⏳ Connecting to Binance WebSocket Stream...", flush=True)
            ws = websocket.WebSocketApp(
                ws_url,
                on_open=on_open,
                on_message=on_message,
                on_error=lambda ws, err: print(f"WS Error: {err}", flush=True),
                on_close=lambda ws, c, m: print("WS Closed. Reconnecting...", flush=True)
            )
            ws.run_forever()
            time.sleep(3)
        except Exception as e:
            print(f"WS Main Loop Exception: {e}", flush=True)
            time.sleep(3)
