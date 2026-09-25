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
# BINANCE API KEYS (Environment Variables)
# ---------------------------------------------------------
API_KEY = os.environ.get("yRwdwQAR1S9G8DLVeQp39lW99BAGEF4XDG6hoImJkFTol2RFvWmTvksMKy5Bav0M")
API_SECRET = os.environ.get("3qsGUF6nPgfluSLPe8VXo0DE2gtR1jQIud9URVC5NHezEFp9YQV1lLqG1WncAltV")

client = Client(API_KEY, API_SECRET)

TRADE_AMOUNT_USDT = 30.0   # $30 Market Order per Trade
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

    # RSI(13)
    delta = df['close'].diff()
    gain = delta.where(delta > 0, 0.0)
    loss = -delta.where(delta < 0, 0.0)

    alpha13 = 1.0 / 13
    avg_gain13 = gain.ewm(alpha=alpha13, adjust=False).mean()
    avg_loss13 = loss.ewm(alpha=alpha13, adjust=False).mean()
    rs13 = avg_gain13 / avg_loss13
    df['rsi13'] = 100.0 - (100.0 / (1.0 + rs13))

    # Bollinger Bands (30,2)
    df['bb_middle'] = df['close'].rolling(window=30).mean()
    df['bb_std'] = df['close'].rolling(window=30).std()
    df['bb_upper'] = df['bb_middle'] + (2 * df['bb_std'])
    df['bb_lower'] = df['bb_middle'] - (2 * df['bb_std'])

    return df

# ---------------------------------------------------------
# FETCH INITIAL PAIRS
# ---------------------------------------------------------
def get_top_120_usdt_pairs():
    try:
        tickers = client.get_all_tickers()
        usdt_pairs = []
        for t in tickers:
            symbol = t['symbol']
            if symbol.endswith('USDT'):
                base_asset = symbol.replace('USDT','')
                if base_asset not in STABLECOINS:
                    usdt_pairs.append(symbol)
        return usdt_pairs[:120]
    except Exception as e:
        print(f"Error getting pairs: {e}", flush=True)
        return []

def load_initial_candles(symbol):
    try:
        klines = client.get_klines(symbol=symbol, interval=TIMEFRAME, limit=200)
        df = pd.DataFrame(klines, columns=[
            'time','open','high','low','close','volume',
            'close_time','qav','num_trades','taker_base_vol','taker_quote_vol','ignore'
        ])
        df['close'] = df['close'].astype(float)
        return df
    except Exception as e:
        print(f"Error loading candles for {symbol}: {e}", flush=True)
        return None

# ---------------------------------------------------------
# WEBSOCKET HANDLER
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
        if df is None:
            df = load_initial_candles(symbol)

        if df is not None:
            new_row = pd.DataFrame([{'close': close_price}], dtype=float)
            df = pd.concat([df, new_row], ignore_index=True).iloc[-200:].reset_index(drop=True)
            df = calculate_indicators(df)
            symbol_data[symbol] = df

            closed_candle = df.iloc[-1]
            rsi13 = closed_candle['rsi13']
            bb_lower = closed_candle['bb_lower']
            bb_upper = closed_candle['bb_upper']

            # BUY RULE
            if is_closed and symbol not in open_positions:
                if rsi13 < 30 and close_price < bb_lower:
                    print(f"[BUY SIGNAL] {symbol} | Price: {close_price} | RSI13: {rsi13:.2f} | BB Lower: {bb_lower:.2f}")
                    execute_buy(symbol, amount=TRADE_AMOUNT_USDT)

            with counter_lock:
                scanned_count += 1
                print(f"--> [{scanned_count}] Candle Closed & Scanned: {symbol} | Price: {close_price}", flush=True)

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
        print(f"SUCCESS: Bought {symbol} at {avg_price} (${amount} USDT)", flush=True)
    except Exception as e:
        print(f"Buy Error {symbol}: {e}", flush=True)

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
# PERIODIC SELL CHECKER
# ---------------------------------------------------------
def periodic_sell_checker(interval=60):
    """প্রতি interval সেকেন্ড পর Binance একাউন্টে ওপেন পজিশন চেক করবে"""
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

                # বর্তমান দাম বের করা
                ticker = client.get_symbol_ticker(symbol=symbol)
                current_price = float(ticker['price'])

                # Stop Loss (-3%)
                if current_price <= buy_price * 0.97:
                    print(f"[STOP LOSS SELL] {symbol} | Current: {current_price} | Buy: {buy_price}")
                    execute_sell(symbol, "Price dropped 3% below buy price")
                    continue

                # RSI + BB Upper শর্ত
                df = symbol_data.get(symbol)
                if df is not None and not df.empty:
                    df_calc = calculate_indicators(df)
                    closed_candle = df_calc.iloc[-1]
                    rsi13 = closed_candle['rsi13']
                    bb_upper = closed_candle['bb_upper']
                    close_price = closed_candle['close']

                    if close_price > bb_upper and rsi13 > 70:
                        print(f"[SELL SIGNAL] {symbol} | Price: {close_price} | RSI13: {rsi13:.2f} | BB Upper: {bb_upper:.2f}")
                        execute_sell(symbol, "RSI13 > 70 & BB Upper Break")
        except Exception as e:
            print(f"Periodic Checker Error: {e}", flush=True)

        time.sleep(interval)

# ---------------------------------------------------------
# START SYSTEM
# ---------------------------------------------------------
def start_single_socket(stream_pairs):
    streams = "/".join([f"{p.lower()}@kline_5m" for p in stream_pairs])
    socket_url = f"wss://stream.binance.com:9443/stream?streams={streams}"
    ws = websocket.WebSocketApp(socket_url, on_message=on_message)
    ws.run_forever()

def start_websocket_system():
    pairs = get_top_120_usdt_pairs()
    chunk_size = 40
    chunks = [pairs[i:i+chunk_size] for i in range(0,BINANCE
