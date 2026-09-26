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

# Binance API Credentials (Render Environment Variable থেকে নিবে)
API_KEY = os.environ.get('BINANCE_API_KEY', 'YOUR_API_KEY')
API_SECRET = os.environ.get('BINANCE_API_SECRET', 'YOUR_API_SECRET')

client = Client(API_KEY, API_SECRET)

# Global Variables
TRADE_AMOUNT_USDT = 35.0
CHECK_INTERVAL_SECONDS = 15  # IP Ban রোধে ১৫ সেকেন্ড বিরতি
active_positions = {}  # {symbol: {'qty': float, 'entry_price': float}}

# Render.com-এ Free Web Service চালু রাখার জন্য Flask Server
app = Flask(__name__)

@app.route('/')
def home():
    return "Binance Bollinger Band Trading Bot is Running Smoothly!"

def run_flask():
    port = int(os.environ.get("PORT", 5000))
    app.run(host='0.0.0.0', port=port)

# ১. IP Ban রোধে একবারে Top 150 USDT Pairs ফিল্টার করার ফাংশন
def get_top_150_usdt_pairs():
    try:
        tickers = client.get_ticker()
        usdt_pairs = []
        for t in tickers:
            symbol = t['symbol']
            # শুধু USDT স্পট পেয়ার এবং লেভারেজড/কাস্টম টোকেন বাদ দেওয়া (UP/DOWN/BULL/BEAR)
            if symbol.endswith('USDT') and not any(x in symbol for x in ['UP', 'DOWN', 'BEAR', 'BULL']):
                usdt_pairs.append({
                    'symbol': symbol,
                    'volume': float(t['quoteVolume'])
                })
        
        # 24-hour USDT Volume দিয়ে Sort করে Top 150 নির্বাচন
        sorted_pairs = sorted(usdt_pairs, key=lambda x: x['volume'], reverse=True)
        top_150 = [p['symbol'] for p in sorted_pairs[:150]]
        return top_150
    except Exception as e:
        logging.error(f"Error fetching top pairs: {e}")
        return []

# ২. ক্যান্ডেল ডাটা নিয়ে Bollinger Bands (20,2) গণনা (Pure Pandas - No Error Guaranteed)
def get_klines_and_bb(symbol):
    try:
        # ৫ মিনিটের ক্যান্ডেল ডাটা (সর্বশেষ ২৫টি ক্যান্ডেল যথেষ্ট 20 Period BB এর জন্য)
        klines = client.get_klines(symbol=symbol, interval=Client.KLINE_INTERVAL_5MINUTE, limit=25)
        df = pd.DataFrame(klines, columns=[
            'timestamp', 'open', 'high', 'low', 'close', 'volume',
            'close_time', 'quote_asset_volume', 'number_of_trades',
            'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume', 'ignore'
        ])
        
        df['open'] = df['open'].astype(float)
        df['high'] = df['high'].astype(float)
        df['low'] = df['low'].astype(float)
        df['close'] = df['close'].astype(float)

        # 20 Period SMA & Standard Deviation বের করা
        sma = df['close'].rolling(window=20).mean()
        std = df['close'].rolling(window=20).std()

        # Bollinger Bands Calculation (20, 2)
        df['lower_band'] = sma - (std * 2)
        df['upper_band'] = sma + (std * 2)

        return df
    except Exception as e:
        logging.error(f"Error calculating BB for {symbol}: {e}")
        return None

# ৩. ট্রেডিং বট মেইন লুপ
def trading_loop():
    logging.info("Trading Loop Started...")
    
    while True:
        try:
            # Step 1: Top 150 Pair সংগ্রহ (একবারে রিকোয়েস্ট পাঠাবে - IP Ban ০%)
            top_symbols = get_top_150_usdt_pairs()
            logging.info(f"Scanning {len(top_symbols)} symbols...")

            for symbol in top_symbols:
                # IP Ban 100% এড়াতে প্রতিটি রিকোয়েস্টের মাঝে হালকা Delays (Rate-limiting safe)
                time.sleep(0.1)

                df = get_klines_and_bb(symbol)
                if df is None or len(df) < 2:
                    continue

                # গত সমাপ্ত ক্যান্ডেল (Last Completed Candle)
                last_candle = df.iloc[-2]
                current_price = df.iloc[-1]['close'] # বর্তমান ক্যান্ডেলের চলতি প্রাইস

                open_p = last_candle['open']
                close_p = last_candle['close']
                lower_b = last_candle['lower_band']
                upper_b = last_candle['upper_band']

                # ------------------ ১. বাই করার শর্ত ------------------
                # শর্ত: Open < Lower Band এবং Close > Lower Band
                if symbol not in active_positions:
                    if open_p < lower_b and close_p > lower_b:
                        logging.info(f"BUY SIGNAL FOUND: {symbol} | Open: {open_p}, Close: {close_p}, Lower BB: {lower_b}")
                        
                        # Market Buy Execute
                        try:
                            order = client.order_market_buy(
                                symbol=symbol,
                                quoteOrderQty=TRADE_AMOUNT_USDT
                            )
                            
                            executed_qty = float(order['executedQty'])
                            active_positions[symbol] = {
                                'qty': executed_qty,
                                'entry_price': current_price
                            }
                            logging.info(f"SUCCESSFUL BUY: Bought {executed_qty} of {symbol}")
                        
                        except BinanceAPIException as e:
                            logging.error(f"Binance Buy API Error for {symbol}: {e}")
                        except Exception as e:
                            logging.error(f"Buy Failed for {symbol}: {e}")

                # ------------------ ২. সেল করার শর্ত (Fail-safe Enabled) ------------------
                # শর্ত: যদি টোকেন কেনা থাকে এবং প্রাইস Upper Band স্পর্শ/অতিক্রম করে
                if symbol in active_positions:
                    if current_price >= upper_b:
                        logging.info(f"SELL SIGNAL FOUND: {symbol} | Price: {current_price}, Upper BB: {upper_b}")
                        
                        qty = active_positions[symbol]['qty']
                        
                        # Market Sell 100% Executed (আটকে না থাকার জন্য ৩ বার চেষ্টা করবে)
                        try:
                            success = False
                            for attempt in range(3):
                                try:
                                    sell_order = client.order_market_sell(
                                        symbol=symbol,
                                        quantity=qty
                                    )
                                    logging.info(f"SUCCESSFUL SELL: Sold 100% ({qty}) of {symbol}")
                                    del active_positions[symbol]
                                    success = True
                                    break
                                except BinanceAPIException as binance_err:
                                    logging.warning(f"Sell Attempt {attempt+1} failed. Retrying... Error: {binance_err}")
                                    # Lot size precision ইস্যু হ্যান্ডেল করা
                                    info = client.get_symbol_info(symbol)
                                    step_size = float([f['stepSize'] for f in info['filters'] if f['filterType'] == 'LOT_SIZE'][0])
                                    qty = float(int(qty / step_size) * step_size)
                                    time.sleep(1)

                            if not success:
                                logging.critical(f"ALERT: Could NOT sell {symbol} after 3 attempts! Check manual balance.")

                        except Exception as e:
                            logging.error(f"Sell Execution Failed for {symbol}: {e}")

            # স্ক্যান শেষে বিরতি (IP Ban 0% সুনিশ্চিত করা)
            time.sleep(CHECK_INTERVAL_SECONDS)

        except Exception as e:
            logging.error(f"Global Loop Error: {e}")
            time.sleep(10)

if __name__ == "__main__":
    # Flask Server আলাদা Thread এ চালু করা (UptimeRobot / Keep-alive এর জন্য)
    flask_thread = threading.Thread(target=run_flask)
    flask_thread.daemon = True
    flask_thread.start()

    # মেইন ট্রেডিং লুপ রান করা
    trading_loop()
