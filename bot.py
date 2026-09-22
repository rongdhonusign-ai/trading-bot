import os
import time
import math
import threading
import pandas as pd
from flask import Flask
from binance.client import Client
from binance.enums import *

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

# ---------------------------------------------------------
# FLASK SERVER (Render Port Binding)
# ---------------------------------------------------------
app = Flask(__name__)

@app.route('/')
def home():
    return "Trading Bot is Active & Running!", 200

# ---------------------------------------------------------
# CUSTOM INDICATOR CALCULATIONS
# ---------------------------------------------------------
def calculate_indicators(df):
    # 1. EMA 200
    df['ema200'] = df['close'].ewm(span=200, adjust=False).mean()

    # 2. RSI (Period = 3)
    delta = df['close'].diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=3).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=3).mean()
    rs = gain / loss
    df['rsi3'] = 100 - (100 / (1 + rs))

    # 3. RSI (Period = 14) for Stoch RSI
    gain14 = (delta.where(delta > 0, 0)).rolling(window=14).mean()
    loss14 = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
    rs14 = gain14 / loss14
    rsi14 = 100 - (100 / (1 + rs14))

    # Stoch RSI (14, 14, 3, 3) -> K line
    stoch_rsi = (rsi14 - rsi14.rolling(14).min()) / (rsi14.rolling(14).max() - rsi14.rolling(14).min())
    df['stoch_k'] = stoch_rsi.rolling(3).mean() * 100

    return df

# ---------------------------------------------------------
# STRATEGY FUNCTIONS
# ---------------------------------------------------------
def get_top_50_usdt_pairs():
    try:
        tickers = client.get_ticker()
        usdt_pairs = []
        for t in tickers:
            symbol = t['symbol']
            if symbol.endswith('USDT'):
                base_asset = symbol.replace('USDT', '')
                if base_asset not in STABLECOINS:
                    usdt_pairs.append({'symbol': symbol, 'quoteVolume': float(t['quoteVolume'])})
        sorted_pairs = sorted(usdt_pairs, key=lambda x: x['quoteVolume'], reverse=True)
        return [p['symbol'] for p in sorted_pairs[:50]]
    except Exception as e:
        print(f"Error fetching pairs: {e}", flush=True)
        return []

def get_klines_data(symbol):
    try:
        klines = client.get_klines(symbol=symbol, interval=TIMEFRAME, limit=250)
        df = pd.DataFrame(klines, columns=[
            'time', 'open', 'high', 'low', 'close', 'volume',
            'close_time', 'qav', 'num_trades', 'taker_base_vol', 'taker_quote_vol', 'ignore'
        ])
        df['close'] = df['close'].astype(float)
        df = calculate_indicators(df)
        return df
    except Exception as e:
        return None

def execute_buy(symbol):
    try:
        print(f"--> [BUY SIGNAL] Buying {symbol} for ${TRADE_AMOUNT_USDT}", flush=True)
        order = client.create_order(
            symbol=symbol, side=SIDE_BUY, type=ORDER_TYPE_MARKET, quoteOrderQty=TRADE_AMOUNT_USDT
        )
        executed_qty = float(order['executedQty'])
        cummulative_quote_qty = float(order['cummulativeQuoteQty'])
        avg_price = cummulative_quote_qty / executed_qty if executed_qty > 0 else 0

        open_positions[symbol] = {'buy_price': avg_price, 'qty': executed_qty}
        print(f"SUCCESS: Bought {symbol} at avg price {avg_price}", flush=True)
    except Exception as e:
        print(f"Error buying {symbol}: {e}", flush=True)

def execute_sell(symbol, reason):
    try:
        qty = open_positions[symbol]['qty']
        print(f"--> [SELL SIGNAL: {reason}] Selling 100% of {symbol}", flush=True)
        
        info = client.get_symbol_info(symbol)
        step_size = None
        for f in info['filters']:
            if f['filterType'] == 'LOT_SIZE':
                step_size = float(f['stepSize'])
                break
        
        if step_size:
            precision = int(round(-math.log10(step_size))) if step_size < 1 else 0
            qty = round(qty, precision)

        client.create_order(symbol=symbol, side=SIDE_SELL, type=ORDER_TYPE_MARKET, quantity=qty)
        print(f"SUCCESS: Sold {symbol}. Reason: {reason}", flush=True)
        del open_positions[symbol]
    except Exception as e:
        print(f"Error selling {symbol}: {e}", flush=True)

def strategy_loop():
    print("Bot Scanner Thread Started...", flush=True)
    time.sleep(5)
    
    while True:
        try:
            symbols = get_top_50_usdt_pairs()
            if not symbols:
                print("Could not fetch pairs or IP Banned. Waiting 2 minutes...", flush=True)
                time.sleep(120)
                continue

            print(f"\n================ Scanning {len(symbols)} Top Pairs (Closed Candles Only) ================", flush=True)
            
            for index, symbol in enumerate(symbols):
                df = get_klines_data(symbol)
                
                # IP Rate Limit ডিলে
                time.sleep(1.5)

                if df is None or len(df) < 200:
                    continue
                
                # -------------------------------------------------------------
                # ক্যান্ডেল ক্লোজড ডাটা ফিল্টারিং:
                # df.iloc[-1] = রানিং/চলতি ক্যান্ডেল
                # df.iloc[-2] = সবেমাত্র ক্লোজ হওয়া শেষ ৫-মিনিটের ক্যান্ডেল
                # -------------------------------------------------------------
                closed_candle = df.iloc[-2]
                live_candle = df.iloc[-1]
                
                closed_price = closed_candle['close']
                closed_ema200 = closed_candle['ema200']
                closed_rsi3 = closed_candle['rsi3']
                closed_stoch_k = closed_candle['stoch_k']
                
                current_live_price = live_candle['close']

                # BUY CONDITION (ক্লোজড ক্যান্ডেলের ইন্ডিকেটর ভ্যালু দিয়ে)
                if symbol not in open_positions:
                    if (closed_rsi3 < 6) and (closed_stoch_k < 20) and (closed_price > closed_ema200):
                        print(f"Signal Confirmed on Closed Candle for {symbol} | RSI(3): {closed_rsi3:.2f} | Stoch_K: {closed_stoch_k:.2f}", flush=True)
                        execute_buy(symbol)

                # SELL CONDITION
                else:
                    buy_price = open_positions[symbol]['buy_price']
                    stop_price = buy_price * (1 - STOP_LOSS_PCT)
                    
                    # ১. ৩% স্টপ লস (লাইভ প্রাইসে চেক হবে)
                    if current_live_price <= stop_price:
                        execute_sell(symbol, reason=f"3% Stop-Loss Hit (Live Price: {current_live_price})")
                    
                    # ২. প্রফিট টেক (RSI-3 ক্লোজড ক্যান্ডেলে ৮৫ বা তার উপরে থাকলেই সেল হবে)
                    elif closed_rsi3 >= 85:
                        execute_sell(symbol, reason=f"RSI(3) Closed at {closed_rsi3:.2f} (>= 85)")

            print("================ Scan Finished. Waiting 30s ================\n", flush=True)
            time.sleep(30)
            
        except Exception as e:
            print(f"Loop error: {e}", flush=True)
            time.sleep(30)

# ---------------------------------------------------------
# MAIN RUNNER
# ---------------------------------------------------------
if __name__ == '__main__':
    t = threading.Thread(target=strategy_loop)
    t.daemon = True
    t.start()

    port = int(os.environ.get("PORT", 10000))
    app.run(host='0.0.0.0', port=port)
