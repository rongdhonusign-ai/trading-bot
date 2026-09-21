import os
import time
import threading
import pandas as pd
import pandas_ta as ta
from flask import Flask
from binance.client import Client
from binance.enums import *

# ---------------------------------------------------------
# BINANCE API KEYS (Render Environment Variable থেকে নিবে)
# ---------------------------------------------------------
API_KEY = os.environ.get("BINANCE_API_KEY", "yRwdwQAR1S9G8DLVeQp39lW99BAGEF4XDG6hoImJkFTol2RFvWmTvksMKy5Bav0M")
API_SECRET = os.environ.get("BINANCE_API_SECRET", "3qsGUF6nPgfluSLPe8VXo0DE2gtR1jQIud9URVC5NHezEFp9YQV1lLqG1WncAltV")

client = Client(API_KEY, API_SECRET)

# ---------------------------------------------------------
# CONFIGURATION
# ---------------------------------------------------------
TRADE_AMOUNT_USDT = 35.0   # প্রতি অর্ডারে $35 usdt
STOP_LOSS_PCT = 0.03       # 3% Stop Loss
TIMEFRAME = Client.KLINE_INTERVAL_5MINUTE

STABLECOINS = [
    'USDT', 'USDC', 'BUSD', 'TUSD', 'FDUSD', 'DAI', 'EUR', 
    'GBP', 'WBTC', 'WETH', 'AEUR', 'PAX'
]

# ট্রেড ট্র্যাকিং ডিকশনারি
# Structure: { 'BTCUSDT': {'buy_price': 50000, 'qty': 0.001, 'prev_rsi': 82} }
open_positions = {}

# ---------------------------------------------------------
# FLASK APP (Render IP / Health Check Server)
# ---------------------------------------------------------
app = Flask(__name__)

@app.route('/')
def health_check():
    return "Binance Trading Bot is Running!", 200

# ---------------------------------------------------------
# HELPER FUNCTIONS
# ---------------------------------------------------------
def get_top_100_usdt_pairs():
    """সবচেয়ে বেশি ভলিউম থাকা ১০০টি non-stablecoin USDT পেয়ার আনবে"""
    try:
        tickers = client.get_ticker()
        usdt_pairs = []
        for t in tickers:
            symbol = t['symbol']
            if symbol.endswith('USDT'):
                base_asset = symbol.replace('USDT', '')
                if base_asset not in STABLECOINS:
                    usdt_pairs.append({
                        'symbol': symbol,
                        'quoteVolume': float(t['quoteVolume'])
                    })
        # ভলিউম অনুযায়ী সর্ট করে টপ ১০০ বাছাই
        sorted_pairs = sorted(usdt_pairs, key=lambda x: x['quoteVolume'], reverse=True)
        return [p['symbol'] for p in sorted_pairs[:100]]
    except Exception as e:
        print(f"Error fetching top pairs: {e}")
        return []

def get_klines_data(symbol):
    """ক্যান্ডেলস্টিক ডাটা টেনে ইন্ডকেটর ক্যালকুলেট করা"""
    try:
        # ৫০টি ক্যান্ডেল ডাটা (EMA 200 এর জন্য ১০০+ প্রয়োজন হতে পারে, নিরাপত্তার জন্য ১৫০ আনা হচ্ছে)
        klines = client.get_klines(symbol=symbol, interval=TIMEFRAME, limit=250)
        df = pd.DataFrame(klines, columns=[
            'time', 'open', 'high', 'low', 'close', 'volume',
            'close_time', 'qav', 'num_trades', 'taker_base_vol', 'taker_quote_vol', 'ignore'
        ])
        df['close'] = df['close'].astype(float)

        # 1. EMA 200
        df['ema200'] = ta.ema(df['close'], length=200)

        # 2. RSI 3
        df['rsi3'] = ta.rsi(df['close'], length=3)

        # 3. Stochastic RSI (14, 14, 3, 3)
        stoch_rsi = ta.stochrsi(df['close'], length=14, rsi_length=14, k=3, d=3)
        df['stoch_k'] = stoch_rsi['STOCHRSIk_14_14_3_3']
        df['stoch_d'] = stoch_rsi['STOCHRSId_14_14_3_3']

        return df
    except Exception as e:
        # IP Rate limit এর ঝুঁকি এড়াতে সামান্য বিরতি
        time.sleep(0.5)
        return None

def execute_buy(symbol):
    """Market Buy Order Executed"""
    try:
        print(f"--> [BUY SIGNAL DETECTED] Buying {symbol} for ${TRADE_AMOUNT_USDT}")
        order = client.create_order(
            symbol=symbol,
            side=SIDE_BUY,
            type=ORDER_TYPE_MARKET,
            quoteOrderQty=TRADE_AMOUNT_USDT
        )
        
        # কেনা দাম এবং পরিমান বের করা
        executed_qty = float(order['executedQty'])
        cummulative_quote_qty = float(order['cummulativeQuoteQty'])
        avg_price = cummulative_quote_qty / executed_qty if executed_qty > 0 else 0

        open_positions[symbol] = {
            'buy_price': avg_price,
            'qty': executed_qty,
            'prev_rsi': None
        }
        print(f"SUCCESS: Bought {symbol} at avg price {avg_price}")
    except Exception as e:
        print(f"Error buying {symbol}: {e}")

def execute_sell(symbol, reason):
    """Market Sell Order Executed (100% Position)"""
    try:
        qty = open_positions[symbol]['qty']
        print(f"--> [SELL SIGNAL: {reason}] Selling 100% of {symbol} (Qty: {qty})")
        
        # পজিশন সাইজ ও প্রিসিশন সংক্রান্ত সমস্যা এড়াতে কোয়ান্টিটি ফরম্যাট
        info = client.get_symbol_info(symbol)
        step_size = None
        for f in info['filters']:
            if f['filterType'] == 'LOT_SIZE':
                step_size = float(f['stepSize'])
                break
        
        if step_size:
            precision = int(round(-pd.np.log10(step_size))) if step_size < 1 else 0
            qty = round(qty, precision)

        order = client.create_order(
            symbol=symbol,
            side=SIDE_SELL,
            type=ORDER_TYPE_MARKET,
            quantity=qty
        )
        print(f"SUCCESS: Sold {symbol} completely. Reason: {reason}")
        del open_positions[symbol]
    except Exception as e:
        print(f"Error selling {symbol}: {e}")

# ---------------------------------------------------------
# STRATEGY ENGINE LOOP
# ---------------------------------------------------------
def strategy_loop():
    print("Strategy Bot Thread Started...")
    time.sleep(2) # ১-২ সেকেন্ড পরেই স্ক্যানিং শুরু হবে
    
    while True:
        try:
            symbols = get_top_100_usdt_pairs()
            print(f"--- Starting Scan for Top {len(symbols)} Pairs ---")
            
            for index, symbol in enumerate(symbols):
                df = get_klines_data(symbol)
                
                if df is None or len(df) < 200:
                    continue
                
                # সর্বশেষ ক্যান্ডেলের তথ্য
                last_row = df.iloc[-1]
                prev_row = df.iloc[-2]
                
                current_price = last_row['close']
                ema200 = last_row['ema200']
                rsi3 = last_row['rsi3']
                stoch_k = last_row['stoch_k']
                
                # -----------------------------------------------------
                # BUY LOGIC:
                # 1. RSI{3} < 6
                # 2. Stoch RSI K < 20
                # 3. Current Price > EMA200
                # 4. Not currently open position
                # -----------------------------------------------------
                if symbol not in open_positions:
                    if (rsi3 < 6) and (stoch_k < 20) and (current_price > ema200):
                        execute_buy(symbol)

                # -----------------------------------------------------
                # SELL LOGIC:
                # 1. RSI{3} Crossing Above 85 (আগের ক্যান্ডেলে ৮৫ এর নিচে বা সমান ছিল, এখন ৮৫ এর উপরে)
                # 2. Price dropped >= 3% from buy price
                # -----------------------------------------------------
                else:
                    pos_data = open_positions[symbol]
                    buy_price = pos_data['buy_price']
                    
                    # Target 1: Stop Loss (3% Down)
                    stop_price = buy_price * (1 - STOP_LOSS_PCT)
                    
                    # Target 2: RSI Crossing Above 85
                    prev_rsi3 = prev_row['rsi3']
                    rsi_crossed_above_85 = (prev_rsi3 <= 85) and (rsi3 > 85)
                    
                    if current_price <= stop_price:
                        execute_sell(symbol, reason="3% Stop-Loss Hit")
                    elif rsi_crossed_above_85:
                        execute_sell(symbol, reason="RSI(3) Crossed Above 85")

                # IP Ban প্রতিরোধে রেট লিমিট হ্যান্ডলিং
                if index % 10 == 0:
                    time.sleep(1) # প্রতি ১০ টি টোকেন স্ক্যান করার পর ১ সেকেন্ড বিরতি
                else:
                    time.sleep(0.1)

            print("--- Scan Loop Completed. Waiting for 3 seconds ---")
            time.sleep(3)

        except Exception as e:
            print(f"Error in strategy loop: {e}")
            time.sleep(10)

# ---------------------------------------------------------
# MAIN ENTRY POINT
# ---------------------------------------------------------
if __name__ == '__main__':
    # Flask এর ব্যাকগ্রাউন্ডে ট্রেডিং লুপ চালানো
    t = threading.Thread(target=strategy_loop)
    t.daemon = True
    t.start()

    # Render Default Port
    port = int(os.environ.get("PORT", 10000))
    app.run(host='0.0.0.0', port=port)
