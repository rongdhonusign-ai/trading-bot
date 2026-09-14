import time
import os
import json
import pandas as pd
import ccxt
import websocket
from flask import Flask
from threading import Thread

# ==========================================
# 1. FLASK APP FOR LIVE STATUS PAGE & HEALTH CHECK
# ==========================================
app = Flask(__name__)

bot_logs = []
last_prices = {}

@app.route('/')
def home():
    return "Binance RSI Strategy Bot Active!", 200

@app.route('/status')
def status():
    html = "<h2>🚀 Binance Altcoins RSI Strategy Bot Status</h2>"
    html += "<p><b>System:</b> Active & Scanning via Binance 5m Kline WebSocket Stream</p><hr>"
    
    html += f"<h3>📊 Live Monitored Symbols ({len(target_symbols)} Active Altcoins):</h3>"
    html += "<p><b>Strategy Rules:</b> BUY when RSI(50) > 50 & RSI(2) < 5 | SELL when RSI(2) > 85 OR RSI(50) < 48 OR SL 2%</p><hr>"
    
    html += "<h3>📜 Recent Activity Logs:</h3><pre style='background:#f4f4f4; padding:10px; border-radius:5px; max-height:400px; overflow-y:auto;'>"
    if bot_logs:
        html += "\n".join(bot_logs[-30:])
    else:
        html += "Waiting for WebSocket data stream..."
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
# 2. CONFIGURATION & ALL ALTCOINS SELECTION
# ==========================================
trade_amount_usdt = 6.0   
stop_loss_pct = 0.02      

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

excluded_coins = {'USDT', 'USDC', 'BUSD', 'TUSD', 'FDUSD', 'DAI', 'USDE', 'BTC', 'BTTC', 'WBTC'}

def get_target_altcoins():
    try:
        add_log("🔍 Fetching active USDT altcoin pairs from Binance via CCXT...")
        markets = trade_exchange.load_markets()
        usdt_pairs = []
        for symbol, market in markets.items():
            if market['spot'] and market['active'] and market['quote'] == 'USDT':
                base = market['base'].upper()
                if base not in excluded_coins and not base.startswith('LD'):
                    ws_sym = symbol.replace('/', '').lower()
                    usdt_pairs.append(ws_sym)
        add_log(f"✅ Found {len(usdt_pairs)} valid Altcoins. Selecting top active pairs...")
        return usdt_pairs[:60]
    except Exception as e:
        add_log(f"⚠️ Error loading markets dynamically: {e}. Falling back to default list.")
        return [
            'ethusdt', 'solusdt', 'bnbusdt', 'xrpusdt', 'adausdt', 'dogeusdt', 'avaxusdt', 
            'shibusdt', 'dotusdt', 'linkusdt', 'nearusdt', 'suiusdt', 'fetusdt', 'icpusdt',
            'aptusdt', 'ltcusdt', 'unicusdt', 'rndrusdt', 'pepeusdt', 'injusdt',
            'renderusdt', 'tiausdt', 'seiusdt', 'arbusdt', 'opusdt', 'wifusdt', 'flokiusdt'
        ]

target_symbols = get_target_altcoins()

prices_history = {sym: [] for sym in target_symbols}
positions = {sym: False for sym in target_symbols}
entry_prices = {sym: 0.0 for sym in target_symbols}
position_amounts = {sym: 0.0 for sym in target_symbols}

# ==========================================
# 3. INDICATOR CALCULATIONS (WILDER'S RSI - TRADINGVIEW MATCHED)
# ==========================================
def calculate_rsi(series, period):
    delta = series.diff()
    gain = delta.where(delta > 0, 0.0)
    loss = -delta.where(delta < 0, 0.0)
    
    avg_gain = gain.ewm(alpha=1/period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1/period, adjust=False).mean()
    
    rs = avg_gain / avg_loss.replace(0, 1e-9)
    rsi = 100.0 - (100.0 / (1.0 + rs))
    return rsi.fillna(50.0)

def calculate_indicators(df):
    df['rsi_50'] = calculate_rsi(df['close'], 50)
    df['rsi_2'] = calculate_rsi(df['close'], 2)
    return df

# ==========================================
# 4. WEBSOCKET PROCESSOR (5M KLINE)
# ==========================================
def process_kline_data(symbol, close_price, high_price, low_price, is_closed):
    formatted_symbol = symbol.upper().replace('USDT', '/USDT')
    last_prices[symbol] = close_price
    
    if is_closed:
        prices_history[symbol].append({
            'close': close_price,
            'high': high_price,
            'low': low_price
        })
        
        if len(prices_history[symbol]) > 60:
            prices_history[symbol].pop(0)

        df = pd.DataFrame(prices_history[symbol])

        if len(df) >= 51:
            df = calculate_indicators(df)
            last_row = df.iloc[-1]
            prev_row = df.iloc[-2]

            rsi_50 = float(last_row.get('rsi_50', 50.0))
            rsi_2 = float(last_row.get('rsi_2', 50.0))
            prev_rsi_2 = float(prev_row.get('rsi_2', 50.0))
            prev_rsi_50 = float(prev_row.get('rsi_50', 50.0))

            add_log(f"📊 [5M CLOSED {formatted_symbol}] Price: ${close_price} | RSI(50): {rsi_50:.1f} | RSI(2): {rsi_2:.1f}")

            # STRATEGY CONDITIONS
            buy_condition = (rsi_50 > 50.0) and (rsi_2 < 5.0)
            
            sell_rsi_2_cross = (prev_rsi_2 <= 85.0 and rsi_2 > 85.0)
            sell_rsi_50_cross = (prev_rsi_50 >= 48.0 and rsi_50 < 48.0)
            sell_condition = sell_rsi_2_cross or sell_rsi_50_cross

            # ENTRY EXECUTION WITH PRECISION HANDLING
            if not positions[formatted_symbol] and buy_condition:
                add_log(f"🔥 BUY SIGNAL MATCHED: {formatted_symbol} at ${close_price} (RSI50: {rsi_50:.1f}, RSI2: {rsi_2:.1f})")
                try:
                    raw_qty = trade_amount_usdt / close_price
                    formatted_qty = float(trade_exchange.amount_to_precision(formatted_symbol, raw_qty))
                    
                    order = trade_exchange.create_market_buy_order(formatted_symbol, formatted_qty)
                    add_log(f"✅ EXECUTED MARKET BUY: {formatted_symbol} | Qty: {formatted_qty} | Order ID: {order['id']}")
                    positions[formatted_symbol] = True
                    entry_prices[formatted_symbol] = close_price
                    position_amounts[formatted_symbol] = formatted_qty
                except Exception as e:
                    add_log(f"❌ BUY ERROR for {formatted_symbol}: {e}")

            # EXIT EXECUTION
            elif positions[formatted_symbol]:
                stop_price = entry_prices[formatted_symbol] * (1.0 - stop_loss_pct)
                is_stop_loss = close_price <= stop_price

                if is_stop_loss or sell_condition:
                    reason = "STOP LOSS 2%" if is_stop_loss else ("RSI(2) > 85 CROSS" if sell_rsi_2_cross else "RSI(50) < 48 CROSS")
                    add_log(f"🛑 EXIT SIGNAL [{reason}]: {formatted_symbol} at ${close_price}")
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
            add_log(f"⏳ [5M KLINE {formatted_symbol}] Gathering Data Buffer for RSI(50): ({len(df)}/51)")

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
                is_closed = kline['x']
                process_kline_data(sym, close_price, high_price, low_price, is_closed)
    except Exception:
        pass

def on_error(ws, error):
    add_log(f"❌ WS ERROR: {error}")

def on_close(ws, close_status_code, close_msg):
    add_log("⚠️ WS CLOSED. Reconnecting in 5 seconds...")

def on_open(ws):
    add_log("✅ CONNECTED TO BINANCE 5M KLINE MULTI-STREAM!")

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
