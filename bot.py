import os
import time
import logging
import threading
import json
from flask import Flask
from binance.client import Client
from binance.ws.spot_websocket import SpotWebsocketStreamClient
from binance.exceptions import BinanceAPIException
import pandas as pd

# Logging Setup
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

API_KEY = os.environ.get('BINANCE_API_KEY', 'YOUR_API_KEY')
API_SECRET = os.environ.get('BINANCE_API_SECRET', 'YOUR_API_SECRET')

client = Client(API_KEY, API_SECRET)

TRADE_AMOUNT_USDT = 35.0
active_positions = {}

# ক্যান্ডেল হিস্ট্রি ডাটা মেমোরিতে রাখার জন্য ডিকশনারি
candles_history = {}
top_pairs_list = []

app = Flask(__name__)

@app.route('/')
def home():
    return "Binance WebSocket Trading Bot is Live & Running!"

def run_flask():
    port = int(os.environ.get("PORT", 10000))
    app.run(host='0.0.0.0', port=port)

def is_stablecoin_or_fiat(symbol):
    stable_fiat_keywords = [
        'USD', 'USDC', 'FDUSD', 'TUSD', 'BUSD', 'DAI', 'USDE', 'PYUSD', 'USDD', 'FRAX',
        'EUR', 'GBP', 'BRL', 'TRY', 'RUB', 'AUD', 'CAD', 'CHF', 'JPY', 'AEUR', 'PAX'
    ]
    base_asset = symbol.replace('USDT', '')
    for keyword in stable_fiat_keywords:
        if base_asset == keyword or base_asset.startswith(keyword):
            return True
    return False

def init_top_150_pairs():
    """বট চালুর সময় মাত্র ১ বার টপ ১৫০ পেয়ার ফিল্টার করবে"""
    global top_pairs_list
    try:
        logging.info("Initializing Top 150 USDT pairs list...")
        tickers = client.get_ticker()
        usdt_pairs = []
        for t in tickers:
            symbol = t['symbol']
            if (symbol.endswith('USDT') and 
                not any(x in symbol for x in ['UP', 'DOWN', 'BEAR', 'BULL']) and 
                not is_stablecoin_or_fiat(symbol)):
                
                usdt_pairs.append({
                    'symbol': symbol,
                    'volume': float(t['quoteVolume'])
                })
        
        sorted_pairs = sorted(usdt_pairs, key=lambda x: x['volume'], reverse=True)
        top_pairs_list = [p['symbol'] for p in sorted_pairs[:150]]
        logging.info(f"Loaded {len(top_pairs_list)} top pairs successfully!")
    except Exception as e:
        logging.error(f"Error fetching top pairs: {e}")

def execute_market_sell(symbol, qty, reason="SELL"):
    """স্লিপেজ এড়াতে ১০ বার দ্রুত রিট্রাই সহ মার্কেট সেল"""
    try:
        info = client.get_symbol_info(symbol)
        step_size = float([f['stepSize'] for f in info['filters'] if f['filterType'] == 'LOT_SIZE'][0])
        
        adjusted_qty = float(int(qty / step_size) * step_size)
        precision = len(str(step_size).split('.')[1]) if '.' in str(step_size) else 0
        adjusted_qty = round(adjusted_qty, precision)

        # স্লিপেজে অর্ডার আটকে না থাকার জন্য ১০ বার দ্রুত ০.২ সেকেন্ড পর পর রিট্রাই করবে
        for attempt in range(10):
            try:
                sell_order = client.order_market_sell(
                    symbol=symbol,
                    quantity=adjusted_qty
                )
                logging.info(f"SUCCESSFUL {reason}: Sold {adjusted_qty} of {symbol} (Attempt {attempt+1})")
                if symbol in active_positions:
                    del active_positions[symbol]
                return True
            except BinanceAPIException as binance_err:
                logging.warning(f"Sell Attempt {attempt+1} failed for {symbol}: {binance_err}")
                time.sleep(0.2)

        logging.critical(f"ALERT: Could NOT sell {symbol} after 10 attempts!")
        return False
    except Exception as e:
        logging.error(f"Sell Execution Error for {symbol}: {e}")
        return False

def process_klines_and_signal(symbol, df, current_close):
    """Bollinger Band (20,2) এবং EMA (5) দিয়ে বায় ও সেলের সিগন্যাল প্রসেস"""
    
    # ১. কেনা থাকা পজিশনের স্টপ-লস ও টেক-প্রফিট ট্র্যাকিং
    if symbol in active_positions:
        entry_price = active_positions[symbol]['entry_price']
        qty = active_positions[symbol]['qty']
        
        # ১.০% থেকে ১.১% স্লিপেজ উইন্ডো হিসেব
        stop_loss_trigger = entry_price * (1 - 0.010)  # ১.০% ড্রপ হলে সেল শুরু

        # ১.০% ড্রপ করা মাত্রই মার্কেট সেল হিট করবে
        if current_close <= stop_loss_trigger:
            drop_percent = round(((entry_price - current_close) / entry_price) * 100, 2)
            logging.warning(f"STOP LOSS TRIGGERED ({drop_percent}% drop): {symbol} | Current: {current_close} | Entry: {entry_price}")
            
            execute_market_sell(symbol, qty, reason=f"STOP LOSS SELL ({drop_percent}%)")
            return

        # Bollinger Upper Band হিট করলে (টেক-প্রফিট)
        upper_b = df.iloc[-1]['upper_band']
        if current_close >= upper_b:
            logging.info(f"TAKE PROFIT HIT: {symbol} | Current: {current_close} >= Upper BB: {upper_b}")
            execute_market_sell(symbol, qty, reason="TAKE PROFIT SELL")
            return

    # ২. নতুন বায় সিগন্যাল চেকিং
    if symbol not in active_positions:
        last_closed_candle = df.iloc[-2]  # আগের বন্ধ হওয়া ৫ মিনিটের ক্যান্ডেল
        open_p = last_closed_candle['open']
        close_p = last_closed_candle['close']
        high_p = last_closed_candle['high']
        lower_b = last_closed_candle['lower_band']
        ema5_p = last_closed_candle['ema5']

        # বায় করার ৩টি শর্তাবলি:
        # ১. Open < Lower Band
        # ২. Close > Lower Band
        # ৩. High < EMA5 (EMA5 না স্পর্শ করে সম্পূর্ণ নিচে থাকতে হবে)
        if (open_p < lower_b) and (close_p > lower_b) and (high_p < ema5_p):
            logging.info(f"BUY SIGNAL FOUND: {symbol} | Open: {open_p}, Close: {close_p}, High: {high_p}, Lower BB: {lower_b}, EMA5: {ema5_p}")
            try:
                order = client.order_market_buy(
                    symbol=symbol,
                    quoteOrderQty=TRADE_AMOUNT_USDT
                )
                executed_qty = float(order['executedQty'])
                cummulative_quote_qty = float(order['cummulativeQuoteQty'])
                
                actual_entry_price = cummulative_quote_qty / executed_qty if executed_qty > 0 else current_close

                active_positions[symbol] = {
                    'qty': executed_qty,
                    'entry_price': actual_entry_price
                }
                logging.info(f"SUCCESSFUL BUY: {symbol} | Entry Price: {actual_entry_price}")
            except Exception as e:
                logging.error(f"Buy Order Failed for {symbol}: {e}")

def handle_socket_message(ws_client, message):
    """WebSocket থেকে লাইভ ক্যান্ডেল ডাটা পাওয়ার সাথে সাথে প্রসেসিং"""
    try:
        data = json.loads(message)
        if 'data' in data and 'k' in data['data']:
            kline = data['data']['k']
            symbol = kline['s']
            
            candle = {
                'timestamp': kline['t'],
                'open': float(kline['o']),
                'high': float(kline['h']),
                'low': float(kline['l']),
                'close': float(kline['c']),
                'is_closed': kline['x']
            }

            if symbol not in candles_history:
                candles_history[symbol] = []

            # মেমোরিতে ক্যান্ডেল ডাটা স্টোর রাখা
            if candle['is_closed']:
                candles_history[symbol].append(candle)
                if len(candles_history[symbol]) > 25:
                    candles_history[symbol].pop(0)

            # যথেষ্ট ক্যান্ডেল ডাটা থাকলে ইন্ডিকেটর হিসেব করা
            if len(candles_history[symbol]) >= 20:
                df = pd.DataFrame(candles_history[symbol])
                sma = df['close'].rolling(window=20).mean()
                std = df['close'].rolling(window=20).std()
                df['lower_band'] = sma - (std * 2)
                df['upper_band'] = sma + (std * 2)
                df['ema5'] = df['close'].ewm(span=5, adjust=False).mean()

                process_klines_and_signal(symbol, df, candle['close'])

    except Exception as e:
        logging.error(f"Error handling websocket msg: {e}")

def start_websocket_listener():
    """সবগুলো টোকেনের জন্য WebSocket Stream সাবস্ক্রাইব করা"""
    ws_client = SpotWebsocketStreamClient(on_message=handle_socket_message)
    
    # 5-minute kline stream subscription for top 150 symbols
    streams = [f"{symbol.lower()}@kline_5m" for symbol in top_pairs_list]
    
    # ১৫০টি পেয়ারকে ছোট ব্যাচে ভাগ করে সাবস্ক্রাইব করানো
    batch_size = 50
    for i in range(0, len(streams), batch_size):
        sub_streams = streams[i:i + batch_size]
        ws_client.subscribe(stream=sub_streams)
        time.sleep(1)

    logging.info("WebSocket Stream Successfully Connected and Listening...")

if __name__ == "__main__":
    # ১. পোর্ট বাইন্ডিংয়ের জন্য Flask থ্রেড চালু
    flask_thread = threading.Thread(target=run_flask)
    flask_thread.daemon = True
    flask_thread.start()

    # ২. টপ ১৫০ পেয়ার লিস্ট প্রস্তুত করা
    init_top_150_pairs()

    # ৩. WebSocket দিয়ে রিয়েল-টাইম ডাটা ট্র্যাকিং চালু
    start_websocket_listener()
