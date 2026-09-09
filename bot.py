import time
import os
import json
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
    return "Trading Bot is Running via WebSocket!", 200

# ==========================================
# 2. BOT CONFIGURATION & SYMBOL LIST
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

klines_data = {sym: [] for sym in symbols}
positions = {sym: False for sym in symbols}
entry_prices = {sym: 0.0 for sym in symbols}

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
# 4. DATA PROCESSOR & SCAN LOGS
# ==========================================
def process_symbol_data(symbol_key, df):
    formatted_symbol = symbol_key.upper().replace('USDT', '/USDT')
    close_price = df.iloc[-1]['close']

    # যথেষ্ট ক্যান্ডেল ডাটা জমে গেলে ইন্ডিকেটর ক্যালকুলেশন করবে
    if len(df) >= 14:
        df = calculate_indicators(df)
        last_row = df.iloc[-1]
        prev_row = df.iloc[-2]

        crsi = last_row.get('crsi', 0)
        stoch_k = last_row.get('stoch_k', 0)

        print(f"⚡ [WS SCAN {formatted_symbol}] Price: {close_price} | CRSI: {crsi:.1f} | Stoch: {stoch_k:.1f}", flush=True)

        buy_condition = (crsi < 20) and (stoch_k < 20)
        sell_condition = (prev_row.get('crsi', 0) <= 80 and crsi > 80) and (prev_row.get('stoch_k', 0) <= 80 and stoch_k > 80)

        if not positions[formatted_symbol] and buy_condition:
            crypto_quantity = trade_amount_usdt / close_price
            print(f"🔥 BUY SIGNAL: {formatted_symbol} at ${close_price}", flush=True)
            order = trade_exchange.create_market_buy_order(formatted_symbol, crypto_quantity)
            print(f"✅ EXECUTED BUY: {order}", flush=True)
            positions[formatted_symbol] = True
            entry_prices[formatted_symbol] = close_price

        elif positions[formatted_symbol]:
            stop_price = entry_prices[formatted_symbol] * (1 - stop_loss_pct)
            if close_price <= stop_price or sell_condition:
                crypto_quantity = trade_amount_usdt / entry_prices[formatted_symbol]
                print(f"🛑 EXIT/STOP LOSS: {formatted_symbol} at ${close_price}", flush=True)
                order = trade_exchange.create_market_sell_order(formatted_symbol, crypto_quantity)
                print(f"✅ EXECUTED SELL: {order}", flush=True)
                positions[formatted_symbol] = False
                entry_prices[formatted_symbol] = 0.0
    else:
        # ১৪টি ক্যান্ডেল না হওয়া পর্যন্ত ডাটা জমার আপডেট দেখাবে
        print(f"⏳ [WS SCAN {formatted_symbol}] Gathering Klines... Price: {close_price} (Bars: {len(df)}/14)", flush=True)

def on_message(ws, message):
    try:
        data = json.loads(message)
        if 'data' in data:
            kline = data['data']['k']
            symbol = kline['s'].lower()
            
            close_price = float(kline['c'])
            high_price = float(kline['h'])
            low_price = float(kline['l'])
            open_price = float(kline['o'])
            
            if symbol not in klines_data:
                klines_data[symbol] = []
                
            klines_data[symbol].append([0, open_price, high_price, low_price, close_price, 0])
            if len(klines_data[symbol]) > 30:
                klines_data[symbol].pop(0)
                
            # অন্তত ২টি ক্যান্ডেল পেলেই স্ক্যানিং আউটপুট প্রিন্ট করা শুরু করবে
            if len(klines_data[symbol]) >= 2:
                df = pd.DataFrame(klines_data[symbol], columns=['time', 'open', 'high', 'low', 'close', 'volume'])
                process_symbol_data(symbol, df)
    except Exception as e:
        print(f"Error parsing WS data: {e}", flush=True)

def on_open(ws):
    print("✅ Binance WebSocket Connected Successfully!", flush=True)

def start_websocket():
    streams = "/".join([f"{sym}@kline_5m" for sym in symbols])
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
            print(f"WS Loop Exception: {e}", flush=True)
            time.sleep(3)

# ==========================================
# 5. BACKGROUND THREAD LAUNCH
# ==========================================
ws_thread = Thread(target=start_websocket)
ws_thread.daemon = True
ws_thread.start()

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 10000))
    app.run(host='0.0.0.0', port=port)
