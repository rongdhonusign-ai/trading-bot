import os
import time
import logging
import threading
from flask import Flask
from binance.client import Client
from binance.exceptions import BinanceAPIException, BinanceOrderException
import pandas as pd

# Logging Setup
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

API_KEY = os.environ.get('BINANCE_API_KEY', 'YOUR_API_KEY')
API_SECRET = os.environ.get('BINANCE_API_SECRET', 'YOUR_API_SECRET')

client = Client(API_KEY, API_SECRET)

TRADE_AMOUNT_USDT = 35.0
CHECK_INTERVAL_SECONDS = 10
STOP_LOSS_PERCENT = 0.01  # ১% স্টপ লস (0.01)
active_positions = {}

app = Flask(__name__)

@app.route('/')
def home():
    return "Binance Bollinger Band Trading Bot is Running Smoothly!"

def run_flask():
    port = int(os.environ.get("PORT", 5000))
    app.run(host='0.0.0.0', port=port)

def get_top_150_usdt_pairs():
    try:
        tickers = client.get_ticker()
        usdt_pairs = []
        for t in tickers:
            symbol = t['symbol']
            if symbol.endswith('USDT') and not any(x in symbol for x in ['UP', 'DOWN', 'BEAR', 'BULL']):
                usdt_pairs.append({
                    'symbol': symbol,
                    'volume': float(t['quoteVolume'])
                })
        
        sorted_pairs = sorted(usdt_pairs, key=lambda x: x['volume'], reverse=True)
        return [p['symbol'] for p in sorted_pairs[:150]]
    except Exception as e:
        logging.error(f"Error fetching top pairs: {e}")
        return []

def get_klines_and_bb(symbol):
    try:
        # IP Ban রোধ করতে কেবল ২০টি ক্যান্ডেল কল করা হচ্ছে
        klines = client.get_klines(symbol=symbol, interval=Client.KLINE_INTERVAL_5MINUTE, limit=21)
        df = pd.DataFrame(klines, columns=[
            'timestamp', 'open', 'high', 'low', 'close', 'volume',
            'close_time', 'quote_asset_volume', 'number_of_trades',
            'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume', 'ignore'
        ])
        
        df['open'] = df['open'].astype(float)
        df['high'] = df['high'].astype(float)
        df['low'] = df['low'].astype(float)
        df['close'] = df['close'].astype(float)

        sma = df['close'].rolling(window=20).mean()
        std = df['close'].rolling(window=20).std()

        df['lower_band'] = sma - (std * 2)
        df['upper_band'] = sma + (std * 2)

        return df
    except Exception as e:
        logging.error(f"Error calculating BB for {symbol}: {e}")
        return None

def execute_market_sell(symbol, qty, reason="SELL"):
    """যে কোনো মূল্যে সেল সফল করতে অপটিমাইজড রিট্রাই মেকানিজম"""
    try:
        success = False
        info = client.get_symbol_info(symbol)
        step_size = float([f['stepSize'] for f in info['filters'] if f['filterType'] == 'LOT_SIZE'][0])
        
        # একিউরেট কোয়ান্টিটি ফিল্টারিং
        adjusted_qty = float(int(qty / step_size) * step_size)
        # দশমিকের অতিরিক্ত ঘর বাদ দেওয়া
        precision = len(str(step_size).split('.')[1]) if '.' in str(step_size) else 0
        adjusted_qty = round(adjusted_qty, precision)

        for attempt in range(5): # ৩ বারের জায়গায় ৫ বার রিট্রাই
            try:
                sell_order = client.order_market_sell(
                    symbol=symbol,
                    quantity=adjusted_qty
                )
                logging.info(f"SUCCESSFUL {reason}: Sold {adjusted_qty} of {symbol}")
                if symbol in active_positions:
                    del active_positions[symbol]
                success = True
                break
            except BinanceAPIException as binance_err:
                logging.warning(f"Sell Attempt {attempt+1} failed. Retrying... Error: {binance_err}")
                time.sleep(0.5)

        if not success:
            logging.critical(f"ALERT: Could NOT sell {symbol} after 5 attempts!")
    except Exception as e:
        logging.error(f"Sell Execution Failed for {symbol}: {e}")

def check_active_positions():
    """অ্যাক্টিভ পজিশন দ্রুত মনিটর করা যেন কোনো সেল মিস না হয়"""
    if not active_positions:
        return

    for symbol in list(active_positions.keys()):
        try:
            # লাইভ টিকার দিয়ে প্রাইস চ্যাকিং (অত্যন্ত দ্রুত এবং কম ওয়েট খরচ করে)
            ticker = client.get_symbol_ticker(symbol=symbol)
            current_price = float(ticker['price'])
            
            entry_price = active_positions[symbol]['entry_price']
            stop_loss_price = active_positions[symbol]['stop_loss_price']

            # ১. ১% স্টপ-লস ট্রিগার
            if current_price <= stop_loss_price:
                logging.warning(f"STOP LOSS HIT (1%): {symbol} | Current: {current_price} <= Stop: {stop_loss_price} (Entry: {entry_price})")
                execute_market_sell(symbol, active_positions[symbol]['qty'], reason="STOP LOSS SELL")
                continue

            # ২. টেক-প্রফিট (Upper Band টপকানো মাত্রই সেল)
            df = get_klines_and_bb(symbol)
            if df is not None and len(df) > 0:
                upper_b = df.iloc[-1]['upper_band']
                if current_price >= upper_b:
                    logging.info(f"TAKE PROFIT SIGNAL: {symbol} | Current: {current_price} >= Upper BB: {upper_b}")
                    execute_market_sell(symbol, active_positions[symbol]['qty'], reason="TAKE PROFIT SELL")

        except Exception as e:
            logging.error(f"Error checking position for {symbol}: {e}")

def trading_loop():
    logging.info("Trading Loop Started...")
    
    while True:
        try:
            # প্রথমে কেনা থাকা পজিশন চেক (সেল মিস না হওয়ার নিশ্চয়তা)
            check_active_positions()

            # ১৫০টি টোকেন স্ক্যান করা
            top_symbols = get_top_150_usdt_pairs()
            logging.info(f"Scanning {len(top_symbols)} symbols...")

            for symbol in top_symbols:
                # লুপ চলাকালীনও সক্রিয় ট্রেডগুলোর টেক-প্রফিট/স্টপ-লস মনিটর করা
                check_active_positions()

                # IP Ban রোধের জন্য API রিকোয়েস্ট বিরতি (0.12s pause)
                time.sleep(0.12)

                df = get_klines_and_bb(symbol)
                if df is None or len(df) < 20:
                    continue

                last_candle = df.iloc[-2]
                open_p = last_candle['open']
                close_p = last_candle['close']
                lower_b = last_candle['lower_band']

                # বায় করার শর্ত: Open < Lower Band এবং Close > Lower Band
                if symbol not in active_positions:
                    if open_p < lower_b and close_p > lower_b:
                        logging.info(f"BUY SIGNAL FOUND: {symbol} | Open: {open_p}, Close: {close_p}, Lower BB: {lower_b}")
                        
                        try:
                            order = client.order_market_buy(
                                symbol=symbol,
                                quoteOrderQty=TRADE_AMOUNT_USDT
                            )
                            executed_qty = float(order['executedQty'])
                            cummulative_quote_qty = float(order['cummulativeQuoteQty'])
                            
                            # আসল এন্ট্রি প্রাইস হিসেব করা
                            actual_entry_price = cummulative_quote_qty / executed_qty if executed_qty > 0 else float(df.iloc[-1]['close'])
                            
                            # কেনার সাথে সাথেই ১% স্টপ লস প্রাইস ফিক্সড ক্যালকুলেট করে মেমোরিতে রাখা
                            calculated_stop_loss = actual_entry_price * (1 - STOP_LOSS_PERCENT)

                            active_positions[symbol] = {
                                'qty': executed_qty,
                                'entry_price': actual_entry_price,
                                'stop_loss_price': calculated_stop_loss
                            }
                            logging.info(f"SUCCESSFUL BUY: Bought {executed_qty} of {symbol} at avg price: {actual_entry_price} | 1% Stop Loss Set At: {calculated_stop_loss}")
                        except Exception as e:
                            logging.error(f"Buy Failed for {symbol}: {e}")

            time.sleep(CHECK_INTERVAL_SECONDS)

        except Exception as e:
            logging.error(f"Global Loop Error: {e}")
            time.sleep(5)

if __name__ == "__main__":
    flask_thread = threading.Thread(target=run_flask)
    flask_thread.daemon = True
    flask_thread.start()

    trading_loop()
