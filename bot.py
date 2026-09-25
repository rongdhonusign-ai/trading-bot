import os
import json
import time
import threading
import pandas as pd
from flask import Flask
from binance.client import Client
from binance.enums import *
import websocket

# ---------------------------------------------------------
# BINANCE API KEYS (Environment Variables)
# ---------------------------------------------------------
API_KEY = os.environ.get("yRwdwQAR1S9G8DLVeQp39lW99BAGEF4XDG6hoImJkFTol2RFvWmTvksMKy5Bav0M")
API_SECRET = os.environ.get("3qsGUF6nPgfluSLPe8VXo0DE2gtR1jQIud9URVC5NHezEFp9YQV1lLqG1WncAltV")

client = Client(API_KEY, API_SECRET)

TRADE_AMOUNT_USDT = 30.0   # প্রতি ট্রেডে $30
TIMEFRAME = Client.KLINE_INTERVAL_5MINUTE

STABLECOINS = ['USDT','USDC','BUSD','TUSD','FDUSD','DAI','EUR','GBP','WBTC','WETH','PAX']
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
    rs13 = avg_gain13 / avg_loss13
    df['rsi13'] = 100.0 - (100.0 / (1.0 + rs13))

    df['bb_middle'] = df['close'].rolling(window=30).mean()
    df['bb_std'] = df['close'].rolling(window=30).std()
    df['bb_upper'] = df['bb_middle'] + (2 * df['bb_std'])
    df['bb_lower'] = df['bb_middle'] - (2 * df['bb_std'])
    return df

# ---------------------------------------------------------
# EXECUTION FUNCTIONS
# ---------------------------------------------------------
def execute_buy(symbol, amount):
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
        return

def execute_sell(symbol, reason):
    try:
        asset = symbol.replace("USDT","")
        balance = client.get_asset_balance(asset=asset)
        if not balance:
            print(f"Sell Error {symbol}: Asset balance not found", flush=True)
            return

        free_qty = float(balance['free'])
        if free_qty <= 0:
            print(f"Sell Cancelled {symbol}: Free balance is 0", flush=True)
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
# PERIODIC CHECKER (BUY + SELL)
# ---------------------------------------------------------
def periodic_sell_checker(interval=60):
    while True:
        try:
            for symbol, pos in list(open_positions.items()):
                buy_price = pos['buy_price']
                balance = client.get_asset_balance(asset=symbol.replace("USDT",""))
                if not balance:
                    continue

                free_qty = float(balance['free'])
                if free_qty <= 0:
                    continue

                ticker = client.get_symbol_ticker(symbol=symbol)
                current_price = float(ticker['price'])

                # Stop Loss (-3%)
                if current_price <= buy_price * 0.97:
                    print(f"[STOP LOSS SELL] {symbol} | Current: {current_price} | Buy: {buy_price}")
                    execute_sell(symbol, "Price dropped 3% below buy price")
                    continue

                # RSI + BB Upper SELL
                df = symbol_data.get(symbol)
                if df is not None and not df.empty:
                    df_calc = calculate_indicators(df)
                    closed_candle = df_calc.iloc[-1]   # সর্বশেষ ক্লোজ হওয়া ক্যান্ডেল
                    rsi13 = closed_candle['rsi13']
                    bb_upper = closed_candle['bb_upper']
                    bb_lower = closed_candle['bb_lower']
                    close_price = closed_candle['close']

                    # SELL শর্ত
                    if close_price > bb_upper and rsi13 > 70:
                        print(f"[SELL SIGNAL] {symbol} | Close: {close_price} | RSI13: {rsi13:.2f}")
                        execute_sell(symbol, "RSI13 > 70 & BB Upper Break")

                    # BUY শর্ত (ক্যান্ডেল ক্লোজ হলে)
                    if close_price < bb_lower and rsi13 < 30:
                        print(f"[BUY SIGNAL] {symbol} | Close: {close_price} | RSI13: {rsi13:.2f}")
                        execute_buy(symbol, TRADE_AMOUNT_USDT)

        except Exception as e:
            print(f"Periodic Checker Error: {e}", flush=True)

        time.sleep(interval)

# ---------------------------------------------------------
# START SYSTEM
# ---------------------------------------------------------
def start_websocket_system():
    pairs = get_top_120_usdt_pairs()
    chunk_size = 40
    chunks = [pairs[i:i+chunk_size] for i in range(0, len(pairs), chunk_size)]
    # এখানে WebSocket চালু করার লজিক থাকবে
    # ...

if __name__ == '__main__':
    try:
        t_main = threading.Thread(target=start_websocket_system)
        t_main.daemon = True
        t_main.start()

        t_checker = threading.Thread(target=periodic_sell_checker, args=(60,))
        t_checker.daemon = True
        t_checker.start()

        port = int(os.environ.get("PORT", 10000))
        app.run(host='0.0.0.0', port=port)
    except Exception as e:
        print(f"Main Error: {e}", flush=True)
