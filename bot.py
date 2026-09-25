import os
import json
import time
import threading
import pandas as pd
from flask import Flask
from binance.client import Client
from binance.enums import *
import websocket

API_KEY = os.environ.get("yRwdwQAR1S9G8DLVeQp39lW99BAGEF4XDG6hoImJkFTol2RFvWmTvksMKy5Bav0M")
API_SECRET = os.environ.get("3qsGUF6nPgfluSLPe8VXo0DE2gtR1jQIud9URVC5NHezEFp9YQV1lLqG1WncAltV")

client = Client(API_KEY, API_SECRET)

TRADE_AMOUNT_USDT = 30.0
TIMEFRAME = Client.KLINE_INTERVAL_5MINUTE

open_positions = {}
symbol_data = {}  # ক্যান্ডেল ডাটা ফ্রেম জমানোর জন্য

app = Flask(__name__)

@app.route('/')
def home():
    return "Trading Bot via WebSocket is Active!", 200

# ---------------------------------------------------------
# INDICATORS (RSI13 + Bollinger Bands 30,2)
# ---------------------------------------------------------
def calculate_indicators(df):
    df = df.copy()
    delta = df['close'].diff()
    gain = delta.where(delta > 0, 0.0)
    loss = -delta.where(delta < 0, 0.0)

    alpha13 = 1.0 / 13
    avg_gain13 = gain.ewm(alpha=alpha13, adjust=False).mean()
    avg_loss13 = loss.ewm(alpha=alpha13, adjust=False).mean()
    rs13 = avg_gain13 / (avg_loss13 + 1e-10) # Division by zero এড়াতে
    df['rsi13'] = 100.0 - (100.0 / (1.0 + rs13))

    df['bb_middle'] = df['close'].rolling(window=30).mean()
    df['bb_std'] = df['close'].rolling(window=30).std()
    df['bb_upper'] = df['bb_middle'] + (2 * df['bb_std'])
    df['bb_lower'] = df['bb_middle'] - (2 * df['bb_std'])
    return df

# ---------------------------------------------------------
# GET TOP 120 USDT PAIRS & INITIAL HISTORICAL DATA
# ---------------------------------------------------------
def get_top_120_usdt_pairs():
    exchange_info = client.get_exchange_info()
    symbols = exchange_info['symbols']
    usdt_pairs = [s['symbol'] for s in symbols if s['quoteAsset'] == 'USDT' and s['status'] == 'TRADING']

    tickers = client.get_ticker()
    ticker_dict = {t['symbol']: float(t['quoteVolume']) for t in tickers}

    usdt_pairs_sorted = sorted(usdt_pairs, key=lambda x: ticker_dict.get(x, 0), reverse=True)
    return usdt_pairs_sorted[:120]

def preload_historical_candles(symbols):
    """বট চালুর পরপরই ৩০টি অতীত ক্যান্ডেল লোড করে নেবে যাতে BB কাজ করে"""
    print("⏳ Preloading historical candles for indicators...", flush=True)
    for sym in symbols:
        try:
            klines = client.get_klines(symbol=sym, interval=TIMEFRAME, limit=40)
            data = []
            for k in klines:
                data.append({
                    'open': float(k[1]),
                    'high': float(k[2]),
                    'low': float(k[3]),
                    'close': float(k[4]),
                    'volume': float(k[5])
                })
            symbol_data[sym] = pd.DataFrame(data)
        except Exception as e:
            print(f"Error preloading {sym}: {e}", flush=True)
    print("✅ Preload Complete!", flush=True)

# ---------------------------------------------------------
# EXECUTION FUNCTIONS
# ---------------------------------------------------------
def execute_buy(symbol, amount):
    if symbol in open_positions:
        return # আগে থেকেই কেনা থাকলে দ্বিতীয়বার কিনবে না
        
    try:
        order = client.create_order(
            symbol=symbol,
            side=SIDE_BUY,
            type=ORDER_TYPE_MARKET,
            quoteOrderQty=amount
        )
        exec_qty = float(order['executedQty'])
        cum_qty = float(order['cummulativeQuoteQty'])
        avg_price = cum_qty / exec_qty if exec_qty > 0 else 0

        open_positions[symbol] = {
            'buy_price': avg_price,
            'qty': exec_qty
        }
        print(f"✅ SUCCESS: Bought {symbol} at {avg_price} (${amount} USDT)", flush=True)

    except Exception as e:
        print(f"Buy Error {symbol}: {e}", flush=True)

def execute_sell(symbol, reason):
    try:
        asset = symbol.replace("USDT","")
        balance = client.get_asset_balance(asset=asset)
        if not balance:
            return

        free_qty = float(balance['free'])
        if free_qty <= 0:
            if symbol in open_positions:
                del open_positions[symbol]
            return

        client.create_order(
            symbol=symbol,
            side=SIDE_SELL,
            type=ORDER_TYPE_MARKET,
            quantity=free_qty
        )
        print(f"⚡ SUCCESS: Sold {free_qty} {symbol} | Reason: {reason}", flush=True)
        if symbol in open_positions:
            del open_positions[symbol]
    except Exception as e:
        print(f"Sell Error {symbol}: {e}", flush=True)

# ---------------------------------------------------------
# HANDLE MESSAGE & EVALUATE SIGNALS
# ---------------------------------------------------------
def handle_combined_message(ws, msg):
    try:
        data = json.loads(msg)
        if 'data' not in data:
            return
        
        kline = data['data']['k']
        symbol = kline['s']
        
        if kline['x']:  # ক্যান্ডেল ক্লোজ হয়েছে
            close_price = float(kline['c'])
            open_price = float(kline['o'])
            high_price = float(kline['h'])
            low_price = float(kline['l'])
            volume = float(kline['v'])

            new_row = {
                'open': open_price,
                'high': high_price,
                'low': low_price,
                'close': close_price,
                'volume': volume
            }

            if symbol not in symbol_data:
                symbol_data[symbol] = pd.DataFrame(columns=['open','high','low','close','volume'])
            
            # ক্যান্ডেল আপডেট
            df = pd.concat([symbol_data[symbol], pd.DataFrame([new_row])], ignore_index=True).tail(50)
            symbol_data[symbol] = df

            print(f"--> Candle Closed & Scanned: {symbol} | Price: {close_price}", flush=True)

            # ইন্ডিকেটর হিসাব
            df_calc = calculate_indicators(df)
            if len(df_calc) >= 30:
                closed_candle = df_calc.iloc[-1]
                rsi13 = closed_candle['rsi13']
                bb_upper = closed_candle['bb_upper']
                bb_lower = closed_candle['bb_lower']

                # BUY SIGNAL CHECK (যদি পজিশন না থাকে)
                if symbol not in open_positions:
                    if close_price < bb_lower and rsi13 < 30:
                        print(f"🚀 [BUY SIGNAL] {symbol} | Price: {close_price} | RSI13: {rsi13:.2f}", flush=True)
                        execute_buy(symbol, TRADE_AMOUNT_USDT)

                # SELL SIGNAL CHECK (যদি কেনা থাকে)
                else:
                    if close_price > bb_upper and rsi13 > 70:
                        print(f"💰 [SELL SIGNAL] {symbol} | Price: {close_price} | RSI13: {rsi13:.2f}", flush=True)
                        execute_sell(symbol, "RSI13 > 70 & BB Upper Break")

    except Exception as e:
        print(f"Message Handling Error: {e}", flush=True)

# ---------------------------------------------------------
# PERIODIC STOP LOSS CHECKER
# ---------------------------------------------------------
def periodic_stop_loss_checker(interval=30):
    while True:
        try:
            for symbol, pos in list(open_positions.items()):
                buy_price = pos['buy_price']
                ticker = client.get_symbol_ticker(symbol=symbol)
                current_price = float(ticker['price'])

                # Stop Loss (-3%)
                if current_price <= buy_price * 0.97:
                    print(f"🛑 [STOP LOSS] {symbol} | Current: {current_price} | Buy: {buy_price}", flush=True)
                    execute_sell(symbol, "Price dropped 3% below buy price")

        except Exception as e:
            print(f"Stop Loss Checker Error: {e}", flush=True)

        time.sleep(interval)

# ---------------------------------------------------------
# START SYSTEM (Combined Websocket Stream)
# ---------------------------------------------------------
def start_websocket_system():
    pairs = get_top_120_usdt_pairs()
    preload_historical_candles(pairs)
    
    # 120টি কানেকশনের বদলে Binance Combined Stream
    streams = "/".join([f"{s.lower()}@kline_5m" for s in pairs])
    combined_url = f"wss://stream.binance.com:9443/stream?streams={streams}"
    
    ws = websocket.WebSocketApp(
        combined_url,
        on_message=handle_combined_message
    )
    ws.run_forever()

# ---------------------------------------------------------
# MAIN
# ---------------------------------------------------------
if __name__ == '__main__':
    try:
        t_main = threading.Thread(target=start_websocket_system)
        t_main.daemon = True
        t_main.start()

        t_checker = threading.Thread(target=periodic_stop_loss_checker, args=(30,))
        t_checker.daemon = True
        t_checker.start()

        port = int(os.environ.get("PORT", 10000))
        app.run(host='0.0.0.0', port=port)
    except Exception as e:
        print(f"Main Error: {e}", flush=True)
