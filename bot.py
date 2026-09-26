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

        sma = df['close'].rolling(window=20).mean()
        std = df['close'].rolling(window=20).std()

        df['lower_band'] = sma - (std * 2)
        df['upper_band'] = sma + (std * 2)

        return df
    except Exception as e:
        logging.error(f"Error calculating BB for {symbol}: {e}")
        return None

def execute_market_sell(symbol, qty):
    try:
        success = False
        for attempt in range(3):
            try:
                sell_order = client.order_market_sell(
                    symbol=symbol,
                    quantity=qty
                )
                logging.info(f"SUCCESSFUL SELL: Sold 100% ({qty}) of {symbol}")
                if symbol in active_positions:
                    del active_positions[symbol]
                success = True
                break
            except BinanceAPIException as binance_err:
                logging.warning(f"Sell Attempt {attempt+1} failed. Retrying... Error: {binance_err}")
                info = client.get_symbol_info(symbol)
                step_size = float([f['stepSize'] for f in info['filters'] if f['filterType'] == 'LOT_SIZE'][0])
                qty = float(int(qty / step_size) * step_size)
                time.sleep(1)

        if not success:
            logging.critical(f"ALERT: Could NOT sell {symbol} after 3 attempts!")
    except Exception as e:
        logging.error(f"Sell Execution Failed for {symbol}: {e}")

def trading_loop():
    logging.info("Trading Loop Started...")
    
    while True:
        try:
            # অগ্রাধিকার ১: কেনা থাকা টোকেন থাকলে দ্রুত রিয়েল-টাইম টিক চেক করে সেল করা
            if active_positions:
                for symbol in list(active_positions.keys()):
                    df = get_klines_and_bb(symbol)
                    if df is not None and len(df) > 0:
                        current_ticker = client.get_symbol_ticker(symbol=symbol)
                        current_price = float(current_ticker['price'])
                        upper_b = df.iloc[-1]['upper_band']

                        # চলতি দাম বা হাই প্রাইস Upper Band টাচ করলেই সেল
                        if current_price >= upper_b or df.iloc[-1]['high'] >= upper_b:
                            logging.info(f"INSTANT SELL SIGNAL: {symbol} | Price: {current_price} >= Upper BB: {upper_b}")
                            execute_market_sell(symbol, active_positions[symbol]['qty'])

            # অগ্রাধিকার ২: ১৫০টি টোকেন স্ক্যান করা
            top_symbols = get_top_150_usdt_pairs()
            logging.info(f"Scanning {len(top_symbols)} symbols...")

            for symbol in top_symbols:
                time.sleep(0.08) # Fast scanning without rate limit

                df = get_klines_and_bb(symbol)
                if df is None or len(df) < 2:
                    continue

                last_candle = df.iloc[-2]
                current_price = df.iloc[-1]['close']

                open_p = last_candle['open']
                close_p = last_candle['close']
                lower_b = last_candle['lower_band']
                upper_b = last_candle['upper_band']

                # ১. বাই করার শর্ত: Open < Lower Band এবং Close > Lower Band
                if symbol not in active_positions:
                    if open_p < lower_b and close_p > lower_b:
                        logging.info(f"BUY SIGNAL FOUND: {symbol} | Open: {open_p}, Close: {close_p}, Lower BB: {lower_b}")
                        
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
                        except Exception as e:
                            logging.error(f"Buy Failed for {symbol}: {e}")

                # ২. সেল শর্ত (স্ক্যান করার সময়)
                elif symbol in active_positions:
                    if current_price >= upper_b or df.iloc[-1]['high'] >= upper_b:
                        logging.info(f"SELL SIGNAL FOUND: {symbol} | Price: {current_price}, Upper BB: {upper_b}")
                        execute_market_sell(symbol, active_positions[symbol]['qty'])

            time.sleep(CHECK_INTERVAL_SECONDS)

        except Exception as e:
            logging.error(f"Global Loop Error: {e}")
            time.sleep(5)

if __name__ == "__main__":
    flask_thread = threading.Thread(target=run_flask)
    flask_thread.daemon = True
    flask_thread.start()

    trading_loop()
