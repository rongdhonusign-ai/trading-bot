import os
import json
import time
import math
import threading
import pandas as pd
from flask import Flask
from binance.client import Client
from binance.enums import *
import websocket

# ---------------------------------------------------------
# BINANCE API KEYS
# ---------------------------------------------------------
API_KEY = os.environ.get("BINANCE_API_KEY", "yRwdwQAR1S9G8DLVeQp39lW99BAGEF4XDG6hoImJkFTol2RFvWmTvksMKy5Bav0M")
API_SECRET = os.environ.get("BINANCE_API_SECRET", "3qsGUF6nPgfluSLPe8VXo0DE2gtR1jQIud9URVC5NHezEFp9YQV1lLqG1WncAltV")

client = Client(API_KEY, API_SECRET)

TRADE_AMOUNT_USDT = 35.0   # $35 Market Order per Trade
STOP_LOSS_PCT = 0.03       # 3% Stop Loss
TIMEFRAME = Client.KLINE_INTERVAL_5MINUTE

STABLECOINS = ['USDT', 'USDC', 'BUSD', 'TUSD', 'FDUSD', 'DAI', 'EUR', 'GBP', 'WBTC', 'WETH', 'PAX']
open_positions = {}
symbol_data = {}  # ক্যান্ডেল হিস্ট্রি স্টোর করার জন্য

# Thread-Safe Counter Variables
scanned_count = 0
counter_lock = threading.Lock()

# ---------------------------------------------------------
# FLASK SERVER
# ---------------------------------------------------------
app = Flask(__name__)

@app.route('/')
def home():
    return "Trading Bot via Multi-Stream WebSocket is Active!", 200

# ---------------------------------------------------------
# INDICATORS
# ---------------------------------------------------------
def calculate_indicators(df):
    df['ema50'] = df['close'].ewm(span=50, adjust=False).mean()
    df['ema100'] = df['close'].ewm(span=100, adjust=False).mean()
    df['ema200'] = df['close'].ewm(span=200, adjust=False).mean()

    delta = df['close'].diff()
    gain = delta.where(delta > 0, 0.0)
    loss = -delta.where(delta < 0, 0.0)

    alpha3 = 1.0 / 3
    avg_gain3 = gain.ewm(alpha=alpha3, adjust=False).mean()
    avg_loss3 = loss.ewm(alpha=alpha3, adjust=False).mean()
    rs3 = avg_gain3 / avg_loss3
    df['rsi3'] = 100.0 - (100.0 / (1.0 + rs3))

    alpha14 = 1.0 / 14
    avg_gain14 = gain.ewm(alpha=alpha14, adjust=False).mean()
    avg_loss14 = loss.ewm(alpha=alpha14, adjust=False).mean()
    rs14 = avg_gain14 / avg_loss14
    rsi14 = 100.0 - (100.0 / (1.0 + rs14))

    stoch_rsi = (rsi14 - rsi14.rolling(14).min()) / (rsi14.rolling(14).max() - rsi14.rolling(14).min())
    df['stoch_k'] = stoch_rsi.rolling(3).mean() * 100.0

    return df

# ---------------------------------------------------------
# FETCH INITIAL TOP 120 PAIRS
# ---------------------------------------------------------
def get_top_120_usdt_pairs():
    try:
        tickers = client.get_all_tickers()
        usdt_pairs = []
        for t in tickers:
            symbol = t['symbol']
            if symbol.endswith('USDT'):
                base_asset = symbol.replace('USDT', '')
                if base_asset not in STABLECOINS:
                    usdt_pairs.append(symbol)
        return usdt_pairs[:120]
    except Exception as e:
        print(f"Error getting pairs: {e}", flush=True)
        return []

def load_initial_candles(symbol):
    try:
        klines = client.get_klines(symbol=symbol, interval=TIMEFRAME, limit=100)
        df = pd.DataFrame(klines, columns=[
            'time', 'open', 'high', 'low', 'close', 'volume',
            'close_time', 'qav', 'num_trades', 'taker_base_vol', 'taker_quote_vol', 'ignore'
        ])
        df['close'] = df['close'].astype(float)
        return df
    except:
        return None

# ---------------------------------------------------------
# WEBSOCKET STREAM HANDLER
# ---------------------------------------------------------
def on_message(ws, message):
    global scanned_count
    data = json.loads(message)
    if 'data' in data:
        kline = data['data']['k']
        symbol = kline['s']
        is_closed = kline['x']  # ক্যান্ডেল ক্লোজ হয়েছে কিনা
        close_price = float(kline['c'])

        # ১. ওপেন পজিশন ট্র্যাকিং (প্রফিট/স্টপ লস)
        if symbol in open_positions:
            buy_price = open_positions[symbol]['buy_price']
            stop_price = buy_price * (1 - STOP_LOSS_PCT)
            
            if close_price <= stop_price:
                execute_sell(symbol, "Stop-Loss Hit")
            elif is_closed:
                df = symbol_data.get(symbol)
                if df is not None:
                    df = calculate_indicators(df)
                    if df.iloc[-1]['rsi3'] >= 85:
                        execute_sell(symbol, "Take Profit Hit (RSI3 >= 85)")

        # ২. ক্যান্ডেল ক্লোজ হলে বাই সিগন্যাল স্ক্যানিং
        if is_closed:
            df = symbol_data.get(symbol)
            if df is not None:
                new_row = pd.DataFrame([{'close': close_price}])
                df = pd.concat([df, new_row], ignore_index=True).iloc[-100:]
                df = calculate_indicators(df)
                symbol_data[symbol] = df

                prev_closed = df.iloc[-3]
                closed = df.iloc[-2]

                if symbol not in open_positions:
                    # বাই সিগন্যাল টেস্ট
                    if (closed['ema50'] > closed['ema100'] > closed['ema200']) and \
                       (closed['close'] > closed['ema50']) and \
                       (prev_closed['rsi3'] >= 6) and (closed['rsi3'] < 6) and \
                       (closed['stoch_k'] < 20):
                        print(f"--> [SIGNAL MATCHED] Buying {symbol} | RSI(3): {closed['rsi3']:.2f} | Price: {close_price}", flush=True)
                        execute_buy(symbol)

            # কাউন্টার আপডেট (প্রিন্ট ক্লিন রাখার জন্য)
            with counter_lock:
                scanned_count += 1
                if scanned_count % 20 == 0:
                    print(f"--> [Live Scan Active] Processed {scanned_count} Candle Closes...", flush=True)

def execute_buy(symbol):
    try:
        order = client.create_order(symbol=symbol, side=SIDE_BUY, type=ORDER_TYPE_MARKET, quoteOrderQty=TRADE_AMOUNT_USDT)
        exec_qty = float(order['executedQty'])
        cum_qty = float(order['cummulativeQuoteQty'])
        avg_price = cum_qty / exec_qty if exec_qty > 0 else 0
        open_positions[symbol] = {'buy_price': avg_price, 'qty': exec_qty}
        print(f"SUCCESS: Bought {symbol} at {avg_price}", flush=True)
    except Exception as e:
        print(f"Buy Error {symbol}: {e}", flush=True)

def execute_sell(symbol, reason):
    try:
        qty = open_positions[symbol]['qty']
        info = client.get_symbol_info(symbol)
        step_size = next(f['stepSize'] for f in info['filters'] if f['filterType'] == 'LOT_SIZE')
        precision = int(round(-math.log10(float(step_size)))) if float(step_size) < 1 else 0
        qty = round(qty, precision)

        client.create_order(symbol=symbol, side=SIDE_SELL, type=ORDER_TYPE_MARKET, quantity=qty)
        print(f"SUCCESS: Sold {symbol} | Reason: {reason}", flush=True)
        del open_positions[symbol]
    except Exception as e:
        print(f"Sell Error {symbol}: {e}", flush=True)

def start_single_socket(stream_pairs):
    streams = "/".join([f"{p.lower()}@kline_5m" for p in stream_pairs])
    socket_url = f"wss://stream.binance.com:9443/stream?streams={streams}"
    ws = websocket.WebSocketApp(socket_url, on_message=on_message)
    ws.run_forever()

def start_websocket_system():
    pairs = get_top_120_usdt_pairs()
    print(f"Loading initial candles for {len(pairs)} pairs...", flush=True)
    
    for p in pairs:
        df = load_initial_candles(p)
        if df is not None:
            symbol_data[p] = df
        time.sleep(0.05)

    print(f"Initial Candles Loaded for {len(symbol_data)} Pairs!", flush=True)

    # ৪০টি করে ৩টি থ্রেড গ্রুপ
    chunk_size = 40
    chunks = [pairs[i:i + chunk_size] for i in range(0, len(pairs), chunk_size)]

    print(f"Starting {len(chunks)} Multi-Threaded Websockets...", flush=True)
    for idx, chunk in enumerate(chunks):
        t = threading.Thread(target=start_single_socket, args=(chunk,))
        t.daemon = True
        t.start()
        print(f"WebSocket Stream #{idx+1} Started ({len(chunk)} pairs)", flush=True)

# ---------------------------------------------------------
# MAIN
# ---------------------------------------------------------
if __name__ == '__main__':
    t_main = threading.Thread(target=start_websocket_system)
    t_main.daemon = True
    t_main.start()

    port = int(os.environ.get("PORT", 10000))
    app.run(host='0.0.0.0', port=port)
