import os
import time
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

TRADE_AMOUNT_USDT = 35.0   # $35 Market Order
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
# CUSTOM INDICATOR CALCULATIONS (NO PANDAS_TA NEEDED)
# ---------------------------------------------------------
def calculate_indicators(df):
    """EMA200, RSI(3), এবং Stoch RSI(14,14,3,3) হিসাব করার কাস্টম ফাংশন"""
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
def get_top_100_usdt_pairs():
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
        return [p['symbol'] for p in sorted_pairs[:100]]
    except Exception as e:
        print(f"Error fetching pairs: {e}")
        return []

def get_klines_data(symbol):
    try:
        klines = client.get_klines(symbol=symbol, interval=TIMEFRAME, limit=250)
        df = pd.DataFrame(klines, columns=[
            'time', 'open', 'high', 'low', 'close', 'volume',
            'close_time', 'qav', 'num_trades', 'taker_base_vol', 'taker_quote_vol', 'ignore'
        ])
        df['close'] = df['close'].astype(float)
        
        # ইন্ডিকেটর হিসাব করা
        df = calculate_indicators(df)
        return df
    except Exception as e:
        time.sleep(0.5)
        return None

def execute_buy(symbol):
    try:
        print(f"--> [BUY SIGNAL] Buying {symbol} for ${TRADE_AMOUNT_USDT}")
        order = client.create_order(
            symbol=symbol, side=SIDE_BUY, type=ORDER_TYPE_MARKET, quoteOrderQty=TRADE_AMOUNT_USDT
        )
        executed_qty = float(order['executedQty'])
        cummulative_quote_qty = float(order['cummulativeQuoteQty'])
        avg_price = cummulative_quote_qty / executed_qty if executed_qty > 0 else 0

        open_positions[symbol] = {'buy_price': avg_price, 'qty': executed_qty}
        print(f"SUCCESS: Bought {symbol} at avg price {avg_price}")
    except Exception as e:
        print(f"Error buying {symbol}: {e}")

def execute_sell(symbol, reason):
    try:
        qty = open_positions[symbol]['qty']
        print(f"--> [SELL SIGNAL: {reason}] Selling 100% of {symbol}")
        
        info = client.get_symbol_info(symbol)
        step_size = None
        for f in info['filters']:
            if f['filterType'] == 'LOT_SIZE':
                step_size = float(f['stepSize'])
                break
        
        if step_size:
            precision = int(round(-pd.np.log10(step_size))) if step_size < 1 else 0
            qty = round(qty, precision)

        client.create_order(symbol=symbol, side=SIDE_SELL, type=ORDER_TYPE_MARKET, quantity=qty)
        print(f"SUCCESS: Sold {symbol}. Reason: {reason}")
        del open_positions[symbol]
    except Exception as e:
        print(f"Error selling {symbol}: {e}")

def strategy_loop():
    print("Bot Scanner Thread Started...")
    time.sleep(2)
    while True:
        try:
            symbols = get_top_100_usdt_pairs()
            for index, symbol in enumerate(symbols):
                df = get_klines_data(symbol)
                if df is None or len(df) < 200:
                    continue
                
                last_row = df.iloc[-1]
                prev_row = df.iloc[-2]
                
                current_price = last_row['close']
                ema200 = last_row['ema200']
                rsi3 = last_row['rsi3']
                stoch_k = last_row['stoch_k']
                
                # BUY CONDITION
                if symbol not in open_positions:
                    if (rsi3 < 6) and (stoch_k < 20) and (current_price > ema200):
                        execute_buy(symbol)

                # SELL CONDITION
                else:
                    buy_price = open_positions[symbol]['buy_price']
                    stop_price = buy_price * (1 - STOP_LOSS_PCT)
                    prev_rsi3 = prev_row['rsi3']
                    
                    if current_price <= stop_price:
                        execute_sell(symbol, reason="3% Stop-Loss Hit")
                    elif (prev_rsi3 <= 85) and (rsi3 > 85):
                        execute_sell(symbol, reason="RSI(3) Crossed Above 85")

                # IP Ban প্রতিরোধের জন্য রেট লিমিট হ্যান্ডলিং
                if index % 10 == 0:
                    time.sleep(1)
                else:
                    time.sleep(0.1)

            time.sleep(3)
        except Exception as e:
            print(f"Loop error: {e}")
            time.sleep(10)

# ---------------------------------------------------------
# MAIN RUNNER
# ---------------------------------------------------------
if __name__ == '__main__':
    t = threading.Thread(target=strategy_loop)
    t.daemon = True
    t.start()

    port = int(os.environ.get("PORT", 10000))
    app.run(host='0.0.0.0', port=port)
