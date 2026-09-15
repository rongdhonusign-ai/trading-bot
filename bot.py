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
    html += "<p><b>Strategy:</b> BUY when Green Candle Closes < EMA(5) & RSI(14) was < 30 in last 3 candles | SELL when Candle Closes > Upper BB(20,2) OR Stop Loss 2%</p><hr>"
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
# 2. CONFIGURATION & STATIC TOP ALTCOINS (SAFE FROM IP BAN)
# ==========================================
trade_amount_usdt = 6.0   
stop_loss_pct = 0.02  # ২% ইমার্জেন্সি স্টপ লস

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
    add_log("✅ Loading static top Altcoins list (Safe from IP Ban)...")
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
# 3. PRE-LOAD HISTORICAL CANDLES (INSTANT SCAN ENABLE)
# ==========================================
def preload_past_candles():
    add_log("⏳ Pre-loading last 25 candles via CCXT (Safe Mode)...")
    for sym in target_symbols:
        formatted_symbol = sym.upper().replace('USDT', '/USDT')
        try:
            # বিগত ২৫টি 5M ক্যান্ডেল লোড
            ohlcv = trade_exchange.fetch_ohlcv(formatted_symbol, timeframe='5m', limit=25)
            
            prices_history[sym] = []
            for candle in ohlcv:
                prices_history[sym].append({
                    'open': float(candle[1]),
                    'high': float(candle[2]),
                    'low': float(candle[3]),
                    'close': float(candle[4])
                })
            time.sleep(0.05)  # Rate limit safe-এর জন্য ছোট ব্রেক
        except Exception as e:
            add_log(f"⚠️ Preload warning for {formatted_symbol}: {e}")
            
    add_log("✅ Pre-loading complete! Bot is ready to scan immediately on first candle close.")

# ==========================================
# 4. TECHNICAL INDICATORS (EMA, RSI, BB)
# ==========================================
def calculate_rsi(series, period=14):
    delta = series.diff()
    gain = delta.where(delta > 0, 0.0)
    loss = -delta.where(delta < 0, 0.0)
    avg_gain = gain.ewm(alpha=1/period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1/period, adjust=False).mean()
    rs = avg_gain / avg_loss.replace(0, 1e-9)
    rsi = 100.0 - (100.0 / (1.0 + rs))
    return rsi.fillna(50.0)

def calculate_indicators(df):
    # EMA 5
    df['ema_5'] = df['close'].ewm(span=5, adjust=False).mean()
    
    # RSI 14
    df['rsi_14'] = calculate_rsi(df['close'], period=14)
    
    # Bollinger Bands (20, std=2)
    df['sma_20'] = df['close'].rolling(window=20).mean()
    df['std_20'] = df['close'].rolling(window=20).std()
    df['bb_upper'] = df['sma_20'] + (df['std_20'] * 2)
    
    return df

# ==========================================
# 5. WEBSOCKET DATA PROCESSOR
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

        # ২৫টি বা তার বেশি ক্যান্ডেল থাকলে স্ক্যানিং শুরু হবে
        if len(df) >= 25:
            df = calculate_indicators(df)
            last_row = df.iloc[-1]

            c_open = float(last_row['open'])
            c_close = float(last_row['close'])
            ema_5 = float(last_row['ema_5'])
            rsi_14 = float(last_row['rsi_14'])
            bb_upper = float(last_row['bb_upper'])

            # 🟢 RSI লজিক: গত ৩টি ক্যান্ডেলের মধ্যে অন্তত ১টিতে RSI < 30 ছিল কিনা
            recent_rsi = df['rsi_14'].tail(3)
            was_rsi_oversold = (recent_rsi < 30.0).any()

            is_green_candle = c_close > c_open
            is_below_ema5 = c_close < ema_5

            buy_condition = is_green_candle and is_below_ema5 and was_rsi_oversold
            sell_condition = c_close > bb_upper

            # 🔍 লাইভ স্ক্যানিং লগ
            add_log(f"📊 [5M SCAN {formatted_symbol}] Close: ${c_close} | Green: {is_green_candle} | EMA5: {ema_5:.4f} | RSI14: {rsi_14:.1f} | Oversold(3 Candles): {was_rsi_oversold} | Upper BB: {bb_upper:.4f}")

            # 🛒 BUY EXECUTION
            if not positions[formatted_symbol] and buy_condition:
                add_log(f"🔥 BUY SIGNAL MATCHED: {formatted_symbol} at ${c_close} (RSI was < 30 in last 3 candles)")
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

            # 💰 SELL EXECUTION
            elif positions[formatted_symbol]:
                stop_price = entry_prices[formatted_symbol] * (1.0 - stop_loss_pct)
                is_stop_loss = c_close <= stop_price

                if sell_condition or is_stop_loss:
                    exit_reason = "STOP LOSS (2%)" if is_stop_loss else "UPPER BB CROSS"
                    add_log(f"🛑 EXIT SIGNAL [{exit_reason}]: {formatted_symbol} at ${c_close}")
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
            add_log(f"⏳ [{formatted_symbol}] Gathering Candle History: ({len(df)}/25)")

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
    add_log("✅ CONNECTED TO BINANCE 5M KLINE STREAM (Safe from IP Ban)")

def start_websocket():
    # ওয়েব সকেটের আগে হিস্টোরিক্যাল ক্যান্ডেল প্রিলোড হবে
    preload_past_candles()
    
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
# 6. MAIN EXECUTION
# ==========================================
if __name__ == '__main__':
    ws_thread = Thread(target=start_websocket)
    ws_thread.daemon = True
    ws_thread.start()
    
    port = int(os.environ.get("PORT", 10000))
    app.run(host='0.0.0.0', port=port, debug=False, use_reloader=False)
