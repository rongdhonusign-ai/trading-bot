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

# ক্যাশিংয়ের জন্য গ্লোবাল ভ্যারিয়েবল
cached_symbols = []
last_symbol_fetch_time = 0

# ---------------------------------------------------------
# FLASK SERVER (Render Port Binding)
# ---------------------------------------------------------
app = Flask(__name__)

@app.route('/')
def home():
    return "Trading Bot is Active & Running!", 200

# ---------------------------------------------------------
# ACCURATE TRADINGVIEW / BINANCE INDICATOR CALCULATIONS
# ---------------------------------------------------------
def calculate_indicators(df):
    df['ema50'] = df['close'].ewm(span=50, adjust=False).mean()
    df['ema100'] = df['close'].ewm(span=100, adjust=False).mean()
    df['ema200'] = df['close'].ewm(span=200, adjust=False).mean()

    # RSI 3
    delta = df['close'].diff()
    gain = delta.where(delta > 0, 0.0)
    loss = -delta.where(delta < 0, 0.0)

    alpha3 = 1.0 / 3
    avg_gain3 = gain.ewm(alpha=alpha3, adjust=False).mean()
    avg_loss3 = loss.ewm(alpha=alpha3, adjust=False).mean()
    rs3 = avg_gain3 / avg_loss3
    df['rsi3'] = 100.0 - (100.0 / (1.0 + rs3))

    # RSI 14 for Stoch RSI
    alpha14 = 1.0 / 14
    avg_gain14 = gain.ewm(alpha=alpha14, adjust=False).mean()
    avg_loss14 = loss.ewm(alpha=alpha14, adjust=False).mean()
    rs14 = avg_gain14 / avg_loss14
    rsi14 = 100.0 - (100.0 / (1.0 + rs14))

    stoch_rsi = (rsi14 - rsi14.rolling(14).min()) / (rsi14.rolling(14).max() - rsi14.rolling(14).min())
    df['stoch_k'] = stoch_rsi.rolling(3).mean() * 100.0

    return df

# ---------------------------------------------------------
# STRATEGY FUNCTIONS (OPTIMIZED FOR API WEIGHT)
# ---------------------------------------------------------
def get_top_120_usdt_pairs():
    """get_all_tickers() দিয়ে মাত্র ১ API Weight খরচে ১২০টি পেয়ার ফিল্টার করা"""
    try:
        tickers = client.get_all_tickers() # এটি অত্যন্ত হালকা এবং সেফ কল
        usdt_pairs = []
        for t in tickers:
            symbol = t['symbol']
            if symbol.endswith('USDT'):
                base_asset = symbol.replace('USDT', '')
                if base_asset not in STABLECOINS:
                    usdt_pairs.append({'symbol': symbol, 'price': float(t['price'])})
        
        return [p['symbol'] for p in usdt_pairs[:120]] # টপ ১২০টি পেয়ার
    except Exception as e:
        print(f"Error fetching pairs: {e}", flush=True)
        return []

def get_klines_data(symbol):
    try:
        # limit=100 দেওয়া হয়েছে যেন API Ban না হয়
        klines = client.get_klines(symbol=symbol, interval=TIMEFRAME, limit=100)
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
        if "-2010" in str(e) or "insufficient balance" in str(e).lower():
            print(f"Manually sold detected! Clearing {symbol} from memory.", flush=True)
            if symbol in open_positions:
                del open_positions[symbol]

# ---------------------------------------------------------
# POSITIONS TRACKER
# ---------------------------------------------------------
def check_and_sell_open_positions():
    """ওপেন পজিশনগুলোর টেক প্রফিট ও স্টপ লস ট্র্যাকিং"""
    if not open_positions:
        return

    for pos_symbol in list(open_positions.keys()):
        df_pos = get_klines_data(pos_symbol)
        if df_pos is not None and len(df_pos) >= 50:
            live_candle = df_pos.iloc[-1]
            current_live_price = live_candle['close']
            live_rsi3 = live_candle['rsi3']
            
            buy_price = open_positions[pos_symbol]['buy_price']
            stop_price = buy_price * (1 - STOP_LOSS_PCT)
            profit_pct = ((current_live_price - buy_price) / buy_price) * 100
            
            if current_live_price <= stop_price:
                execute_sell(pos_symbol, reason=f"3% Stop-Loss Hit (Price: {current_live_price})")
            elif live_rsi3 >= 85:
                execute_sell(pos_symbol, reason=f"Take Profit Hit | Live RSI3: {live_rsi3:.2f} >= 85 | PnL: {profit_pct:.2f}%")
        
        time.sleep(0.3)

# ---------------------------------------------------------
# STRATEGY LOOP (SAFE BATCH SCANNING: 3 x 40 PAIRS)
# ---------------------------------------------------------
def strategy_loop():
    global cached_symbols, last_symbol_fetch_time
    print("Bot Scanner Thread Started...", flush=True)
    time.sleep(5)
    
    while True:
        try:
            current_time = time.time()
            
            # প্রতি ১৫ মিনিটে পেয়ার লিস্ট রিফ্রেশ
            if not cached_symbols or (current_time - last_symbol_fetch_time) > 900:
                print("Fetching Top 120 USDT Pairs from Binance...", flush=True)
                new_pairs = get_top_120_usdt_pairs()
                last_symbol_fetch_time = current_time
                if new_pairs:
                    cached_symbols = new_pairs

            if not cached_symbols:
                print("Could not fetch pairs. Waiting 3 minutes...", flush=True)
                time.sleep(180)
                continue

            # ১২০টি পেয়ারকে ৪০টি করে ৩টি ব্যাচে ভাগ করা (40 x 3 = 120)
            batch_size = 40
            batches = [cached_symbols[i:i + batch_size] for i in range(0, len(cached_symbols), batch_size)]

            for batch_index, current_batch in enumerate(batches):
                
                # ব্যাচ শুরুর আগে ওপেন পজিশন চেক
                if open_positions:
                    check_and_sell_open_positions()

                print(f"\n---> Scanning Batch {batch_index + 1}/3 ({len(current_batch)} Pairs) <---", flush=True)

                for index, symbol in enumerate(current_batch):
                    
                    df = get_klines_data(symbol)
                    time.sleep(0.4) # সেফ ডিলে

                    if df is None or len(df) < 50:
                        continue
                    
                    prev_closed_candle = df.iloc[-3]
                    closed_candle = df.iloc[-2]
                    
                    closed_price = closed_candle['close']
                    closed_ema50 = closed_candle['ema50']
                    closed_ema100 = closed_candle['ema100']
                    closed_ema200 = closed_candle['ema200']
                    closed_rsi3 = closed_candle['rsi3']
                    prev_closed_rsi3 = prev_closed_candle['rsi3']
                    closed_stoch_k = closed_candle['stoch_k']

                    # BUY CONDITION
                    if symbol not in open_positions:
                        if (closed_ema50 > closed_ema100) and \
                           (closed_ema100 > closed_ema200) and \
                           (closed_price > closed_ema50) and \
                           (prev_closed_rsi3 >= 6) and (closed_rsi3 < 6) and \
                           (closed_stoch_k < 20):
                            
                            print(f"Signal Confirmed for {symbol} | RSI(3): {closed_rsi3:.2f} | Stoch_K: {closed_stoch_k:.2f}", flush=True)
                            execute_buy(symbol)

                # ব্যাচ শেষে সেফটি কুল-ডাউন ২০ সেকেন্ড
                if batch_index < len(batches) - 1:
                    print(f"--> Batch {batch_index + 1} done. Cooling down for 20 seconds...", flush=True)
                    time.sleep(20)

            # ব্যাচ শেষে পজিশন চেক
            if open_positions:
                check_and_sell_open_positions()

            print("\n================ All Batches Finished. Waiting 15s ================\n", flush=True)
            time.sleep(15)
            
        except Exception as e:
            print(f"Loop error: {e}", flush=True)
            time.sleep(20)

# ---------------------------------------------------------
# MAIN RUNNER
# ---------------------------------------------------------
if __name__ == '__main__':
    t = threading.Thread(target=strategy_loop)
    t.daemon = True
    t.start()

    port = int(os.environ.get("PORT", 10000))
    app.run(host='0.0.0.0', port=port)
