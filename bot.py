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
symbol_data = {}

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
# INDICATORS CALCULATOR
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
        # EMA200 নিখুঁতভাবে বের করতে অন্তত ৩০০টি ক্যান্ডেল লোড করা হচ্ছে
        klines = client.get_klines(symbol=symbol, interval=TIMEFRAME, limit=300)
        df = pd.DataFrame(klines, columns=[
            'time', 'open', 'high', 'low', 'close', 'volume',
            'close_time', 'qav', 'num_trades', 'taker_base_vol', 'taker_quote_vol', 'ignore'
        ])
        df['close'] = df['close'].astype(float)
        return df
    except Exception as e:
        print(f"Error loading candles for {symbol}: {e}", flush=True)
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
        is_closed = kline['x']
        close_price = float(kline['c'])

        df = symbol_data.get(symbol)

        # -----------------------------------------------------
        # ১. রিয়েল-টাইম সেল ফিল্টার (স্টপ লস ও টেক প্রফিট চেক)
        # -----------------------------------------------------
        if symbol in open_positions:
            buy_price = open_positions[symbol]['buy_price']
            stop_price = buy_price * (1 - STOP_LOSS_PCT)

            if df is not None:
                # টেম্পোরারি প্রাইজ দিয়ে লাইভ RSI3 হিসাব
                df_temp = pd.concat([df, pd.DataFrame([{'close': close_price}], dtype=float)], ignore_index=True)
                df_calc = calculate_indicators(df_temp)
                current_rsi3 = df_calc.iloc[-1]['rsi3']

                if close_price <= stop_price:
                    execute_sell(symbol, f"Stop-Loss Hit (Price: {close_price})")
                elif current_rsi3 >= 85:
                    execute_sell(symbol, f"Take-Profit Hit (RSI3: {current_rsi3:.2f})")

        # ---------------------------------------------------------
        # ২. কেবল ক্যান্ডেল ক্লোজ হলেই বাই ফিল্টার রান করবে
        # ---------------------------------------------------------
        if is_closed:
            if df is not None:
                # ক্যান্ডেল পারমানেন্টলি সেভ করা ও ইন্ডেক্স রিসেট করা
                new_row = pd.DataFrame([{'close': close_price}], dtype=float)
                df = pd.concat([df, new_row], ignore_index=True).iloc[-300:].reset_index(drop=True)
                df = calculate_indicators(df)
                symbol_data[symbol] = df

                # নতুন বন্ধ হওয়া ক্যান্ডেল
                closed_candle = df.iloc[-1]

                if symbol not in open_positions:
                    # ১. প্রাইজ EMA50 এর উপরে থাকতে হবে
                    c1_price_above_ema50 = (closed_candle['close'] > closed_candle['ema50'])
                    
                    # ২. EMA ট্রেন্ড: EMA50 > EMA100 > EMA200 হতে হবে
                    c2_ema_alignment = (closed_candle['ema50'] > closed_candle['ema100']) and (closed_candle['ema100'] > closed_candle['ema200'])
                    
                    # ৩. RSI(3) ১০ এর কম হতে হবে
                    c3_rsi3_low = (closed_candle['rsi3'] < 10.0)
                    
                    # ৪. Stoch K ২০ এর কম হতে হবে
                    c4_stoch_low = (closed_candle['stoch_k'] < 20.0)

                    # সবগুলো শর্ত একসাথে পূরণ হলেই কেবল বাই এক্সিকিউট হবে
                    if c1_price_above_ema50 and c2_ema_alignment and c3_rsi3_low and c4_stoch_low:
                        print(f"\n[BUY SIGNAL MATCHED] {symbol}", flush=True)
                        print(f"--> Price: {closed_candle['close']} | EMA50: {closed_candle['ema50']:.2f} | EMA100: {closed_candle['ema100']:.2f} | EMA200: {closed_candle['ema200']:.2f}", flush=True)
                        print(f"--> RSI3: {closed_candle['rsi3']:.2f} | Stoch K: {closed_candle['stoch_k']:.2f}\n", flush=True)
                        
                        execute_buy(symbol)

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
        if symbol in open_positions:
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
        time.sleep(0.12)

    print(f"Initial Candles Loaded for {len(symbol_data)} Pairs!", flush=True)

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
