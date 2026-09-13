import time
import os
import json
import pandas as pd
import ccxt
import websocket
from flask import Flask
from threading import Thread

# ==========================================
# 1. FLASK APP FOR LIVE STATUS PAGE
# ==========================================
app = Flask(__name__)

bot_logs = []
last_prices = {}

@app.route('/')
def home():
    return "Trading Bot Active!", 200

@app.route('/status')
def status():
    html = "<h2>🚀 Binance 5-Minute Candle Bot Live Status</h2>"
    html += "<p><b>System:</b> Active & Scanning via Binance 5m Kline Stream</p><hr>"
    
    html += "<h3>📊 Live Coin Buffer & Prices:</h3><ul>"
    for sym in sorted(target_symbols):
        hist_len = len(prices_history.get(sym, []))
        price = last_prices.get(sym, 'N/A')
        formatted = sym.upper().replace('USDT', '/USDT')
        html += f"<li><b>{formatted}:</b> ${price} | 5m Candles Buffer: {hist_len}/14</li>"
    html += "</ul><hr>"

    html += "<h3>📜 Recent Activity Logs:</h3><pre style='background:#f4f4f4; padding:10px; border-radius:5px;'>"
    if bot_logs:
        html += "\n".join(bot_logs[-25:])
    else:
        html += "Waiting for data stream..."
    html += "</pre>"
    
    return html, 200

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

# ==========================================
# 3. INDICATOR CALCULATIONS (NAN SAFE)
# ==========================================
def calculate_rsi(series, period):
    delta = series.diff().fillna(0)
    gain = (delta.where(delta > 0, 0)).rolling(window=period).mean().fillna(0)
    loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean().fillna(0)
    rs = gain / loss.replace(0, 1e-9)
    rsi = 100 - (100 / (1 + rs))
    return rsi.fillna(50)

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
    roc = df['close'].pct_change(1).fillna(0)
    df['percent_rank'] = roc.rolling(2).apply(
        lambda x: (pd.Series(x).rank(pct=True).iloc[-1]) * 100 if len(x) > 0 else 50, raw=False
    ).fillna(50)
    
    df['crsi'] = (df['rsi'] + df['updn_rsi'] + df['percent_rank']) / 3
    df['crsi'] = df['crsi'].fillna(50)

    low_min = df['low'].rolling(window=14).min()
    high_max = df['high'].rolling(window=14).max()
    denom = (high_max - low_min).replace(0, 1e-9)
    stoch_raw = 100 * ((df['close'] - low_min) / denom)
    df['stoch_k'] = stoch_raw.rolling(window=3).mean().rolling(window=3).mean().fillna(50)
    return df

# ==========================================
# 4. WEBSOCKET PROCESSOR (5M KLINE)
# ==========================================
def process_kline_data(symbol, close_price, high_price, low_price, is_closed):
    formatted_symbol = symbol.upper().replace('USDT', '/USDT')
    last_prices[symbol] = close_price
    
    # কেবল প্রতি ৫ মিনিটের ক্যান্ডেল ক্লোজ হলে ডাটা বাফারে জমা হবে
    if is_closed:
        prices_history[symbol].append({
            'close': close_price,
            'high': high_price,
            'low': low_price
        })
        
        if len(prices_history[symbol]) > 30:
            prices_history[symbol].pop(0)

        df = pd.DataFrame(prices_history[symbol])

        if len(df) >= 14:
            df = calculate_indicators(df)
            last_row = df.iloc[-1]
            prev_row = df.iloc[-2]

            crsi = float(last_row.get('crsi', 50))
            stoch_k = float(last_row.get('stoch_k', 50))

            add_log(f"📊 [5M CLOSED {formatted_symbol}] Price: {close_price} | CRSI: {crsi:.1f} | Stoch: {stoch_k:.1f}")

            # বাই শর্ত: ২৫/২৫ এবং সেল শর্ত: ৭৫
            buy_condition = (crsi < 25) and (stoch_k < 25)
            sell_condition = (prev_row.get('crsi', 50) <= 75 and crsi > 75) or (stoch_k > 75)

            if not positions[formatted_symbol] and buy_condition:
                crypto_quantity = trade_amount_usdt / close_price
                add_log(f"🔥 BUY SIGNAL (5m): {formatted_symbol} at ${close_price}")
                try:
                    order = trade_exchange.create_market_buy_order(formatted_symbol, crypto_quantity)
                    add_log(f"✅ EXECUTED BUY: {order}")
                    positions[formatted_symbol] = True
                    entry_prices[formatted_symbol] = close_price
                except Exception as e:
                    add_log(f"❌ BUY ERROR: {e}")

            elif positions[formatted_symbol]:
                stop_price = entry_prices[formatted_symbol] * (1 - stop_loss_pct)
                if close_price <= stop_price or sell_condition:
                    crypto_quantity = trade_amount_usdt / entry_prices[formatted_symbol]
                    add_log(f"🛑 EXIT/STOP LOSS: {formatted_symbol} at ${close_price}")
                    try:
                        order = trade_exchange.create_market_sell_order(formatted_symbol, crypto_quantity)
                        add_log(f"✅ EXECUTED SELL: {order}")
                        positions[formatted_symbol] = False
                        entry_prices[formatted_symbol] = 0.0
                    except Exception as e:
                        add_log(f"❌ SELL ERROR: {e}")
        else:
            add_log(f"⏳ [5M KLINE {formatted_symbol}] Gathering 5m Candles: ({len(df)}/14)")

def on_message(ws, message):
    try:
        data = json.loads(message)
        if 'data' in data:
            kline = data['data']['k']
            sym = kline['s'].lower()
            if sym in target_symbols:
                close_price = float(kline['c'])
                high_price = float(kline['h'])
                low_price = float(kline['l'])
                is_closed = kline['x']  # True if 5m candle finished
                process_kline_data(sym, close_price, high_price, low_price, is_closed)
    except Exception:
        pass

def on_error(ws, error):
    add_log(f"❌ WS ERROR: {error}")

def on_close(ws, close_status_code, close_msg):
    add_log(f"⚠️ WS CLOSED. Reconnecting in 5 seconds...")

def on_open(ws):
    add_log("✅ CONNECTED TO BINANCE 5M KLINE STREAM!")

def start_websocket():
    streams = "/".join([f"{sym}@kline_5m" for sym in target_symbols])
    ws_url = f"wss://stream.binance.com:9443/stream?streams={streams}"
    while True:
        try:
            add_log("⏳ Connecting to Binance 5m Stream...")
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
