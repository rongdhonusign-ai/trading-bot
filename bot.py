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

TRADE_AMOUNT_USDT = 15.0   # $15 Market Order per Trade
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
# INDICATORS CALCULATOR (EMA50, EMA100, Stoch RSI 14,14,3,3)
# ---------------------------------------------------------
def calculate_indicators(df):
    df['ema50'] = df['close'].ewm(span=50, adjust=False).mean()
    df['ema100'] = df['close'].ewm(span=100, adjust=False).mean()

    # RSI 14 Calculation
    delta = df['close'].diff()
    gain = delta.where(delta > 0, 0.0)
    loss = -delta.where(delta < 0, 0.0)

    alpha14 = 1.0 / 14
    avg_gain14 = gain.ewm(alpha=alpha14, adjust=False).mean()
    avg_loss14 = loss.ewm(alpha=alpha14, adjust=False).mean()
    rs14 = avg_gain14 / avg_loss14
    rsi14 = 100.0 - (100.0 / (1.0 + rs14))

    # Stochastic RSI (14, 14, 3, 3)
    rsi_min = rsi14.rolling(14).min()
    rsi_max = rsi14.rolling(14).max()
    
    # Avoid division by zero
    stoch_rsi = (rsi14 - rsi_min) / (rsi_max - rsi_min).replace(0, 0.00001)
    
    # %K line (3-period SMA of Stoch RSI)
    df['stoch_k'] = stoch_rsi.rolling(3).mean() * 100.0
    # %D line (3-period SMA of %K line)
    df['stoch_d'] = df['stoch_k'].rolling(3).mean()

    return df

# ---------------------------------------------------------
# FETCH INITIAL TOP 120 PAIRS & OPEN POSITIONS
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

def sync_existing_binance_positions():
    """বাইন্যান্সে আগে থেকে কোনো টোকেন কেনা থাকলে তা চেক করে বটের ওপেন পজিশনে যুক্ত করে"""
    try:
        print("\n--> Checking Binance Account for existing open positions...", flush=True)
        account = client.get_account()
        balances = account.get('balances', [])
        
        for b in balances:
            asset = b['asset']
            free_qty = float(b['free'])
            locked_qty = float(b['locked'])
            total_qty = free_qty + locked_qty

            if total_qty > 0 and asset not in STABLECOINS:
                symbol = f"{asset}USDT"
                try:
                    ticker = client.get_symbol_ticker(symbol=symbol)
                    price = float(ticker['price'])
                    value_usdt = total_qty * price

                    # ধরে নেওয়া হচ্ছে ন্যূনতম ৫ ডলারের বেশি ব্যালেন্স থাকলে সেটা ট্রেকিংয়ে রাখা হবে
                    if value_usdt >= 5.0:
                        open_positions[symbol] = {
                            'buy_price': price,
                            'qty': total_qty,
                            'prev_k': 0.0,
                            'prev_d': 0.0
                        }
                        print(f"--> [EXISTING POSITION FOUND] {symbol} | Qty: {total_qty} | Approx Value: ${value_usdt:.2f}", flush=True)
                except Exception:
                    pass
    except Exception as e:
        print(f"Error syncing existing positions: {e}", flush=True)

def load_initial_candles(symbol):
    try:
        klines = client.get_klines(symbol=symbol, interval=TIMEFRAME, limit=200)
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
        # ১. রিয়েল-টাইম সেল ফিল্টার (দ্রুততম সেল বাস্তবায়নের জন্য)
        # -----------------------------------------------------
        if symbol in open_positions:
            if df is not None:
                # চলন্ত লাইভ প্রাইসের সাহায্যে ইন্ডিকেটর হিসাব
                df_temp = pd.concat([df, pd.DataFrame([{'close': close_price}], dtype=float)], ignore_index=True)
                df_calc = calculate_indicators(df_temp)
                
                curr_k = df_calc.iloc[-1]['stoch_k']
                curr_d = df_calc.iloc[-1]['stoch_d']
                
                prev_k = open_positions[symbol].get('prev_k', curr_k)
                prev_d = open_positions[symbol].get('prev_d', curr_d)

                # Cross-Up Check: Stoch K & D ৮৫-এর উপরে যাওয়া বা অতিক্রম করা
                is_crossed_above = (prev_k <= 85.0 or prev_d <= 85.0) and (curr_k > 85.0 and curr_d > 85.0)
                is_above_85 = (curr_k > 85.0 and curr_d > 85.0)

                if is_crossed_above or is_above_85:
                    execute_sell(symbol, f"Stoch RSI (14,14,3,3) Cross Above 85! (K: {curr_k:.2f}, D: {curr_d:.2f})")

                # হিস্টোরিকাল ডাটা আপডেট রাখা
                open_positions[symbol]['prev_k'] = curr_k
                open_positions[symbol]['prev_d'] = curr_d

        # ---------------------------------------------------------
        # ২. কেবল ক্যান্ডেল ক্লোজ হলেই বাই ফিল্টার রান করবে
        # ---------------------------------------------------------
        if is_closed:
            if df is not None:
                new_row = pd.DataFrame([{'close': close_price}], dtype=float)
                df = pd.concat([df, new_row], ignore_index=True).iloc[-200:].reset_index(drop=True)
                df = calculate_indicators(df)
                symbol_data[symbol] = df

                closed_candle = df.iloc[-1]

                if symbol not in open_positions:
                    ema50 = closed_candle['ema50']
                    ema100 = closed_candle['ema100']
                    stoch_k = closed_candle['stoch_k']
                    stoch_d = closed_candle['stoch_d']

                    # বাই শর্তাবলি:
                    # ১. EMA50 > EMA100
                    c1_ema_trend = (ema50 > ema100)
                    
                    # ২. ৫ মিনিটের ক্যান্ডেল ক্লোজ প্রাইজ > EMA50
                    c2_price_above_ema50 = (closed_candle['close'] > ema50)
                    
                    # ৩. Stoch RSI 14,14,3,3 এর K-line ও D-line < 10.0
                    c3_stoch_low = (stoch_k < 10.0) and (stoch_d < 10.0)

                    if c1_ema_trend and c2_price_above_ema50 and c3_stoch_low:
                        print(f"\n[BUY SIGNAL MATCHED] {symbol}", flush=True)
                        print(f"--> Price: {closed_candle['close']} | EMA50: {ema50:.2f} | EMA100: {ema100:.2f}", flush=True)
                        print(f"--> Stoch K: {stoch_k:.2f} | Stoch D: {stoch_d:.2f}\n", flush=True)
                        
                        execute_buy(symbol)

            with counter_lock:
                scanned_count += 1
                print(f"--> [{scanned_count}] Candle Closed & Scanned: {symbol} | Price: {close_price}", flush=True)

def execute_buy(symbol):
    try:
        order = client.create_order(
            symbol=symbol, 
            side=SIDE_BUY, 
            type=ORDER_TYPE_MARKET, 
            quoteOrderQty=TRADE_AMOUNT_USDT
        )
        exec_qty = float(order['executedQty'])
        cum_qty = float(order['cummulativeQuoteQty'])
        avg_price = cum_qty / exec_qty if exec_qty > 0 else 0
        
        open_positions[symbol] = {
            'buy_price': avg_price, 
            'qty': exec_qty,
            'prev_k': 0.0,
            'prev_d': 0.0
        }
        print(f"SUCCESS: Bought {symbol} at {avg_price} ($15 USDT)", flush=True)
    except Exception as e:
        print(f"Buy Error {symbol}: {e}", flush=True)

def execute_sell(symbol, reason):
    try:
        # ১. কয়েন নাম আলাদা করা (যেমন CELRUSDT থেকে CELR)
        asset = symbol.replace("USDT", "").replace("BUSD", "").replace("USDC", "").replace("FDUSD", "")
        
        # ২. সরাসরি বাইন্যান্স ওয়ালেট থেকে Free Balance চেক করা
        balance = client.get_asset_balance(asset=asset)
        if not balance:
            print(f"Sell Error {symbol}: Asset balance not found", flush=True)
            return

        free_qty = float(balance['free'])

        # ৩. এক্সচেঞ্জের LOT_SIZE ফিল্টার অনুযায়ী রাউন্ড করা
        info = client.get_symbol_info(symbol)
        step_size = next(f['stepSize'] for f in info['filters'] if f['filterType'] == 'LOT_SIZE')
        precision = int(round(-math.log10(float(step_size)))) if float(step_size) < 1 else 0
        
        # ওয়ালেটের ফ্রি ব্যালেন্সকে প্রিসিশন অনুযায়ী ডাউন-রাউন্ড (Floor) করা
        sell_qty = math.floor(free_qty * (10 ** precision)) / (10 ** precision)

        # ৪. ন্যূনতম লট সাইজের চেয়ে ব্যালেন্স বেশি থাকলে অর্ডার এক্সিকিউট করা
        min_qty = float(next(f['minQty'] for f in info['filters'] if f['filterType'] == 'LOT_SIZE'))
        
        if sell_qty >= min_qty:
            client.create_order(
                symbol=symbol, 
                side=SIDE_SELL, 
                type=ORDER_TYPE_MARKET, 
                quantity=sell_qty
            )
            print(f"\n⚡ SUCCESS: Instant Sold {sell_qty} {symbol} | Reason: {reason}\n", flush=True)
        else:
            print(f"Sell Cancelled {symbol}: Insufficient free quantity ({sell_qty} < {min_qty})", flush=True)

        # সফলভাবে প্রসেস হলে অপেন পজিশন থেকে মুছে ফেলা
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
    # সর্বপ্রথমে বাইন্যান্সে থাকা বিদ্যমান পজিশন চেক
    sync_existing_binance_positions()

    pairs = get_top_120_usdt_pairs()
    
    chunk_size = 40
    chunks = [pairs[i:i + chunk_size] for i in range(0, len(pairs), chunk_size)]

    print(f"Starting Batch Initialization for {len(pairs)} Pairs in {len(chunks)} Chunks...", flush=True)

    for idx, chunk in enumerate(chunks):
        print(f"\n--> [Batch {idx+1}/{len(chunks)}] Loading candles for {len(chunk)} pairs...", flush=True)
        
        for p in chunk:
            df = load_initial_candles(p)
            if df is not None:
                symbol_data[p] = df
            time.sleep(0.4)

        t = threading.Thread(target=start_single_socket, args=(chunk,))
        t.daemon = True
        t.start()
        print(f"--> WebSocket Stream #{idx+1} Active ({len(chunk)} pairs)", flush=True)

        if idx < len(chunks) - 1:
            print("--> Waiting 20 seconds before initializing next batch to keep Binance Rate Limit safe...", flush=True)
            time.sleep(20)

    print("\n[ALL 120 PAIRS FULLY LOADED & LIVE SCANNING ACTIVE]", flush=True)

# ---------------------------------------------------------
# MAIN
# ---------------------------------------------------------
if __name__ == '__main__':
    t_main = threading.Thread(target=start_websocket_system)
    t_main.daemon = True
    t_main.start()

    port = int(os.environ.get("PORT", 10000))
    app.run(host='0.0.0.0', port=port)
