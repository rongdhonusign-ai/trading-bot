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
STOP_LOSS_PERCENT = 0.01  # ১% ফিক্সড স্টপ লস
active_positions = {}

# IP Ban সম্পূর্ণ বন্ধ করতে মেমোরি ক্যাশিং
top_150_cached_pairs = []
last_top_pairs_fetch_time = 0

app = Flask(__name__)

@app.route('/')
def home():
    return "Binance Bollinger Band + EMA5 Trading Bot is Running Safely!"

def run_flask():
    port = int(os.environ.get("PORT", 5000))
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

def get_top_150_usdt_pairs():
    global top_150_cached_pairs, last_top_pairs_fetch_time
    
    # ১৫ মিনিটে মাত্র ১ বার টপ ১৫০ ফিল্টার ডাটা ফেচ করবে (API Weight সুরক্ষার জন্য)
    if time.time() - last_top_pairs_fetch_time < 900 and top_150_cached_pairs:
        return top_150_cached_pairs

    try:
        logging.info("Updating Top 150 Volume Pairs Cache (Runs once every 15 mins)...")
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
        top_150_cached_pairs = [p['symbol'] for p in sorted_pairs[:150]]
        last_top_pairs_fetch_time = time.time()
        return top_150_cached_pairs
    except Exception as e:
        logging.error(f"Error fetching top pairs: {e}")
        return top_150_cached_pairs

def get_klines_bb_and_ema(symbol):
    try:
        # Bollinger Band (20) এবং EMA (5) হিসেব করতে ২৫টি ক্যান্ডেল ডাটা আনা হচ্ছে
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

        # Bollinger Bands (20, 2)
        sma = df['close'].rolling(window=20).mean()
        std = df['close'].rolling(window=20).std()
        df['lower_band'] = sma - (std * 2)
        df['upper_band'] = sma + (std * 2)

        # EMA 5 Calculation
        df['ema5'] = df['close'].ewm(span=5, adjust=False).mean()

        return df
    except Exception as e:
        logging.error(f"Error calculating Indicators for {symbol}: {e}")
        return None

def execute_market_sell(symbol, qty, reason="SELL"):
    try:
        success = False
        info = client.get_symbol_info(symbol)
        step_size = float([f['stepSize'] for f in info['filters'] if f['filterType'] == 'LOT_SIZE'][0])
        
        adjusted_qty = float(int(qty / step_size) * step_size)
        precision = len(str(step_size).split('.')[1]) if '.' in str(step_size) else 0
        adjusted_qty = round(adjusted_qty, precision)

        for attempt in range(5):
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
                time.sleep(1)

        if not success:
            logging.critical(f"ALERT: Could NOT sell {symbol} after 5 attempts!")
    except Exception as e:
        logging.error(f"Sell Execution Failed for {symbol}: {e}")

def check_active_positions(all_tickers_dict):
    """একত্রে মাত্র ১টি কলেই সমস্ত অ্যাক্টিভ ট্রেডের স্টপ-লস ও টেক-প্রফিট ট্র্যাকিং"""
    if not active_positions:
        return

    for symbol in list(active_positions.keys()):
        try:
            if symbol not in all_tickers_dict:
                continue
                
            current_price = all_tickers_dict[symbol]
            entry_price = active_positions[symbol]['entry_price']
            stop_loss_price = active_positions[symbol]['stop_loss_price']

            # ১. ১% স্টপ-লস ট্রিগার
            if current_price <= stop_loss_price:
                logging.warning(f"STOP LOSS HIT (1%): {symbol} | Current: {current_price} <= Stop: {stop_loss_price}")
                execute_market_sell(symbol, active_positions[symbol]['qty'], reason="STOP LOSS SELL")
                continue

            # ২. টেক-প্রফিট (Bollinger Upper Band পার হলে)
            df = get_klines_bb_and_ema(symbol)
            if df is not None and len(df) > 0:
                upper_b = df.iloc[-1]['upper_band']
                if current_price >= upper_b:
                    logging.info(f"TAKE PROFIT SIGNAL: {symbol} | Current: {current_price} >= Upper BB: {upper_b}")
                    execute_market_sell(symbol, active_positions[symbol]['qty'], reason="TAKE PROFIT SELL")

        except Exception as e:
            logging.error(f"Error checking position for {symbol}: {e}")

def trading_loop():
    logging.info("Safe Trading Loop Started...")
    
    while True:
        try:
            # ১টি কলেই পুরো মার্কেটের লাইভ দাম নিয়ে আসা (API Weight = 2)
            all_tickers = client.get_all_tickers()
            all_tickers_dict = {t['symbol']: float(t['price']) for t in all_tickers}

            # ১. কেনা থাকা টোকেনের ১% স্টপ-লস ও টেক-প্রফিট চেক
            check_active_positions(all_tickers_dict)

            # ২. স্ক্যান করার পেয়ার লিস্ট
            top_symbols = get_top_150_usdt_pairs()

            for symbol in top_symbols:
                # IP Ban সুরক্ষার জন্য প্রতি রিকোয়েস্টের মাঝে ০.৮ সেকেন্ডের বিরতি
                time.sleep(0.8)

                df = get_klines_bb_and_ema(symbol)
                if df is None or len(df) < 20:
                    continue

                last_candle = df.iloc[-2]
                open_p = last_candle['open']
                close_p = last_candle['close']
                high_p = last_candle['high']
                lower_b = last_candle['lower_band']
                ema5_p = last_candle['ema5']

                # বায় করার শর্তাবলি:
                # ১. Open < Lower Band
                # ২. Close > Lower Band
                # ৩. ক্যান্ডেল EMA5 এর নিচে থাকতে হবে এবং High প্রাইস EMA5 টাচ করতে পারবে না (high_p < ema5_p)
                if symbol not in active_positions:
                    if (open_p < lower_b) and (close_p > lower_b) and (high_p < ema5_p):
                        logging.info(f"BUY SIGNAL FOUND: {symbol} | Open: {open_p}, Close: {close_p}, High: {high_p}, Lower BB: {lower_b}, EMA5: {ema5_p}")
                        
                        try:
                            order = client.order_market_buy(
                                symbol=symbol,
                                quoteOrderQty=TRADE_AMOUNT_USDT
                            )
                            executed_qty = float(order['executedQty'])
                            cummulative_quote_qty = float(order['cummulativeQuoteQty'])
                            
                            actual_entry_price = cummulative_quote_qty / executed_qty if executed_qty > 0 else float(df.iloc[-1]['close'])
                            calculated_stop_loss = actual_entry_price * (1 - STOP_LOSS_PERCENT)

                            active_positions[symbol] = {
                                'qty': executed_qty,
                                'entry_price': actual_entry_price,
                                'stop_loss_price': calculated_stop_loss
                            }
                            logging.info(f"SUCCESSFUL BUY: Bought {executed_qty} of {symbol} at avg price: {actual_entry_price} | 1% Stop Loss: {calculated_stop_loss}")
                        except Exception as e:
                            logging.error(f"Buy Failed for {symbol}: {e}")

            # লুপ শেষে ১০ সেকেন্ড বিরতি
            time.sleep(CHECK_INTERVAL_SECONDS)

        except Exception as e:
            logging.error(f"Global Loop Error: {e}")
            time.sleep(15)

if __name__ == "__main__":
    flask_thread = threading.Thread(target=run_flask)
    flask_thread.daemon = True
    flask_thread.start()

    trading_loop()
