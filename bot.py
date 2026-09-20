import os
import time
import json
import threading
import pandas as pd
import ccxt
import websocket
from flask import Flask, render_template_string

# ==========================================
# 1. FLASK APP & LOGGING SETUP
# ==========================================
app = Flask(__name__)
bot_logs = []

def add_log(message):
    timestamp = time.strftime("[%Y-%m-%d %H:%M:%S]")
    log_line = f"{timestamp} {message}"
    print(log_line)
    bot_logs.append(log_line)
    if len(bot_logs) > 100:  # লগের সাইজ ১০০ রাখা হলো
        bot_logs.pop(0)

# ==========================================
# 2. CONFIGURATION & TARGET ALTCOINS
# ==========================================
trade_amount_usdt = 40.0  # 🛠️ বাই অ্যামাউন্ট ৪০ ডলার করা হলো    

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

# 🛠️ বড় হাতের এবং স্ল্যাশ যুক্ত সিম্বল ('KAVA/USDT') দিয়ে ডিকশনারি সেটআপ
formatted_symbols = [sym.upper().replace('USDT', '/USDT') for sym in target_symbols]

prices_history = {sym: [] for sym in target_symbols}
positions = {sym: False for sym in formatted_symbols}
entry_prices = {sym: 0.0 for sym in formatted_symbols}
position_amounts = {sym: 0.0 for sym in formatted_symbols}

# ==========================================
# 3. TECHNICAL INDICATORS & PRELOAD HISTORY
# ==========================================
def calculate_indicators(df):
    # Bollinger Bands (20 Period)
    df['sma_20'] = df['close'].rolling(window=20).mean()
    df['std_20'] = df['close'].rolling(window=20).std()
    df['bb_upper'] = df['sma_20'] + (df['std_20'] * 2.0)
    df['bb_lower'] = df['sma_20'] - (df['std_20'] * 2.0)
    
    # 🎯 TREND FILTER: EMA 200
    df['ema_200'] = df['close'].ewm(span=200, adjust=False).mean()
    
    return df

def preload_history():
    add_log("⏳ Preloading 5M Candle History (250 candles) for EMA 200...")
    for sym in target_symbols:
        formatted_symbol = sym.upper().replace('USDT', '/USDT')
        try:
            # EMA 200 সঠিকভাবে হিসাব করতে ২৫০টি ক্যান্ডেল লোড করা হচ্ছে
            ohlcv = trade_exchange.fetch_ohlcv(formatted_symbol, timeframe='5m', limit=250)
            history = []
            for candle in ohlcv[:-1]:  # রানিং ক্যান্ডেল বাদ দিয়ে ক্লোজড ক্যান্ডেল
                history.append({
                    'open': candle[1],
                    'high': candle[2],
                    'low': candle[3],
                    'close': candle[4]
                })
            prices_history[sym] = history  # ২৫০টি ক্যান্ডেল স্টোর
            add_log(f"✅ Loaded history for {formatted_symbol}")
            
            # 🛠️ API রেট লিমিট এড়াতে ০.৩ সেকেন্ডের বিরতি
            time.sleep(0.3)
            
        except Exception as e:
            add_log(f"⚠️ History preload failed for {formatted_symbol}: {e}")

# ==========================================
# 4. WEBSOCKET & TRADE LOGIC (UPDATED WITH EMA 200)
# ==========================================
def process_kline_data(symbol, open_p, close_p, high_p, low_p, is_closed):
    formatted_symbol = symbol.upper().replace('USDT', '/USDT')
    base_currency = formatted_symbol.split('/')[0]  # যেমন: ETH, SOL, KAVA
    
    if is_closed:
        prices_history[symbol].append({
            'open': open_p,
            'high': high_p,
            'low': low_p,
            'close': close_p
        })
        if len(prices_history[symbol]) > 300:
            prices_history[symbol].pop(0)

    # EMA 200 হিসাবের জন্য কমপক্ষে ২০০টি ক্যান্ডেল প্রয়োজন
    if len(prices_history[symbol]) >= 200:
        df = pd.DataFrame(prices_history[symbol])
        
        # রানিং ক্যান্ডেলের লাইভ প্রাইস আপডেট
        if not is_closed:
            running_candle = pd.DataFrame([{
                'open': open_p, 'high': high_p, 'low': low_p, 'close': close_p
            }])
            df = pd.concat([df, running_candle], ignore_index=True)

        df = calculate_indicators(df)
        last_row = df.iloc[-1]
        c_close = float(last_row['close'])
        bb_lower = float(last_row['bb_lower'])
        bb_upper = float(last_row['bb_upper'])
        ema_200 = float(last_row['ema_200'])

        if is_closed:
            add_log(f"📊 [5M SCAN {formatted_symbol}] Close: ${c_close} | Lower BB: ${bb_lower:.4f} | Upper BB: ${bb_upper:.4f} | EMA 200: ${ema_200:.4f}")

            # 🛠️ বাইন্যান্স ওয়ালেট ব্যালেন্স চেক করা
            actual_balance = 0.0
            has_position = False
            try:
                balance = trade_exchange.fetch_balance()
                actual_balance = float(balance['free'].get(base_currency, 0.0))
                
                # ওয়ালেটে যদি এই কয়েন $5.0 USDT-এর বেশি থাকে, ধরে নেওয়া হবে পজিশন কেনা আছে
                if (actual_balance * c_close) >= 5.0:
                    has_position = True
            except Exception as e:
                add_log(f"⚠️ Balance Check Failed for {formatted_symbol}: {e}")
                has_position = positions.get(formatted_symbol, False)

            # 🟢 BUY STRATEGY (WITH TREND FILTER)
            # কন্ডিশন: ১. কোনো খোলা পজিশন থাকা যাবে না
            #          ২. দাম Lower BB এর নিচে বা সমান হতে হবে
            #          ৩. দাম অবশ্যই EMA 200 এর উপরে থাকতে হবে (Uptrend Filter)
            if not has_position and c_close <= bb_lower and c_close > ema_200:
                add_log(f"🎯 BUY SIGNAL MATCHED: {formatted_symbol} | Price: ${c_close} <= Lower BB: ${bb_lower:.4f} AND Price > EMA 200: ${ema_200:.4f}")
                try:
                    raw_amount = trade_amount_usdt / c_close
                    amount_to_buy = float(trade_exchange.amount_to_precision(formatted_symbol, raw_amount))
                    
                    order = trade_exchange.create_market_buy_order(formatted_symbol, amount_to_buy)
                    positions[formatted_symbol] = True
                    add_log(f"✅ BUY EXECUTED for {formatted_symbol} | Order ID: {order['id']}")
                except Exception as e:
                    add_log(f"❌ BUY ERROR for {formatted_symbol}: {e}")

            # 🔴 SELL STRATEGY
            elif has_position and c_close >= bb_upper:
                add_log(f"🎯 SELL SIGNAL MATCHED: {formatted_symbol} | Price: ${c_close} >= Upper BB: ${bb_upper:.4f}")
                try:
                    amount_to_sell = float(trade_exchange.amount_to_precision(formatted_symbol, actual_balance))
                    
                    order = trade_exchange.create_market_sell_order(formatted_symbol, amount_to_sell)
                    positions[formatted_symbol] = False
                    add_log(f"✅ SELL EXECUTED for {formatted_symbol} | Order ID: {order['id']}")
                except Exception as e:
                    add_log(f"❌ SELL ERROR for {formatted_symbol}: {e}")
    else:
        if is_closed:
            add_log(f"⏳ [{formatted_symbol}] Gathering Candle History for EMA 200: ({len(prices_history[symbol])}/200)")

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
    except Exception as e:
        add_log(f"⚠️ Message Processing Error: {e}")

def on_error(ws, error):
    add_log(f"🚨 WebSocket Error: {error}")

def on_close(ws, close_status_code, close_msg):
    add_log("🔌 WebSocket Connection Closed. Reconnecting...")
    time.sleep(5)
    run_websocket()

def run_websocket():
    stream_names = "/".join([f"{sym}@kline_5m" for sym in target_symbols])
    ws_url = f"wss://stream.binance.com:9443/stream?streams={stream_names}"
    ws = websocket.WebSocketApp(
        ws_url,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close
    )
    ws.run_forever()

def start_bot():
    preload_history()
    run_websocket()

# ==========================================
# 5. FLASK ROUTES
# ==========================================
@app.route('/')
def home():
    return "Binance Spot Trading Bot is Live & Running!"

@app.route('/status')
def status():
    logs_display = "<br>".join(bot_logs[-60:]) if bot_logs else "No logs yet."
    html = f"""
    <html>
        <head>
            <title>Bot Status</title>
            <meta http-equiv="refresh" content="10">
            <style>
                body {{ font-family: monospace; background-color: #121212; color: #00ff00; padding: 20px; }}
                h2 {{ color: #ffffff; }}
                .log-box {{ background-color: #000; border: 1px solid #333; padding: 15px; border-radius: 5px; }}
            </style>
        </head>
        <body>
            <h2>🤖 Binance Spot Bot Status Dashboard</h2>
            <div class="log-box">{logs_display}</div>
        </body>
    </html>
    """
    return html, 200

# ==========================================
# 6. MAIN EXECUTION
# ==========================================
if __name__ == '__main__':
    bot_thread = threading.Thread(target=start_bot)
    bot_thread.daemon = True
    bot_thread.start()
    
    port = int(os.environ.get("PORT", 5000))
    app.run(host='0.0.0.0', port=port)
