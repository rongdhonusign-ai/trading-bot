import os
import time
import logging
import threading
import json
import asyncio
import websockets
from flask import Flask
from binance.client import Client
from binance.exceptions import BinanceAPIException
import pandas as pd

# Logging Setup
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

API_KEY = os.environ.get('BINANCE_API_KEY', 'YOUR_API_KEY')
API_SECRET = os.environ.get('BINANCE_API_SECRET', 'YOUR_API_SECRET')

client = Client(API_KEY, API_SECRET)

TRADE_AMOUNT_USDT = 35.0
POSITIONS_FILE = 'active_positions.json'

# --- JSON Persistent Storage Functions ---
def load_positions():
    """ফাইল থেকে সেভ থাকা সক্রিয় ট্রেড লোড করে"""
    if os.path.exists(POSITIONS_FILE):
        try:
            with open(POSITIONS_FILE, 'r') as f:
                return json.load(f)
        except Exception as e:
            logging.error(f"Positions file read error: {e}")
            return {}
    return {}

def save_positions(positions):
    """সক্রিয় ট্রেড ফাইলেই স্থায়ীভাবে সেভ করে রাখে"""
    try:
        with open(POSITIONS_FILE, 'w') as f:
            json.dump(positions, f, indent=4)
    except Exception as e:
        logging.error(f"Positions file save error: {e}")

# বটের শুরুতেই সেভড পজিশন লোড করা
active_positions = load_positions()
candles_history = {}

# টপ ৫০-৬০টি হাই ভলিউম USDT পেয়ারের ইউনিক লিস্ট
top_pairs_list = list(set([
    "BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT", "DOGEUSDT", "ADAUSDT",
    "AVAXUSDT", "LINKUSDT", "SUIUSDT", "DOTUSDT", "NEARUSDT", "APTUSDT", "LTCUSDT",
    "BCHUSDT", "FETUSDT", "SHIBUSDT", "PEPEUSDT", "WIFUSDT", "INJUSDT",
    "TIAUSDT", "RNDRUSDT", "ATOMUSDT", "STXUSDT", "FILUSDT", "TRXUSDT", "ARBUSDT",
    "OPUSDT", "FTMUSDT", "AAVEUSDT", "GALAUSDT", "THETAUSDT", "ALGOUSDT", "LDOUSDT",
    "FLOKIUSDT", "KASUSDT", "ORDIUSDT", "SEIUSDT", "DYDXUSDT", "SANDUSDT", "MANAUSDT"
]))

app = Flask(__name__)

@app.route('/')
def home():
    return "Binance Zero-Weight WebSocket Bot is Active!"

def run_flask():
    port = int(os.environ.get("PORT", 10000))
    app.run(host='0.0.0.0', port=port)

def execute_market_sell(symbol, qty, reason="SELL"):
    """যে কোনো মূল্যে সেল সফল করার সেফ ফাংশন"""
    try:
        info = client.get_symbol_info(symbol)
        step_size = float([f['stepSize'] for f in info['filters'] if f['filterType'] == 'LOT_SIZE'][0])
        base_asset = info['baseAsset']
        
        # ১. ওয়ালেটের প্রকৃত ফ্রি ব্যালেন্স যাচাই
        balance = float(client.get_asset_balance(asset=base_asset)['free'])
        sell_qty = min(qty, balance)

        # ২. স্টেপ সাইজ অনুযায়ী পরিমাণ অ্যাডজাস্ট করা
        adjusted_qty = float(int(sell_qty / step_size) * step_size)
        precision = len(str(step_size).split('.')[1]) if '.' in str(step_size) else 0
        adjusted_qty = round(adjusted_qty, precision)

        if adjusted_qty <= 0:
            logging.error(f"Cannot sell {symbol}: Adjusted quantity is 0 (Balance: {balance})")
            if symbol in active_positions:
                del active_positions[symbol]
                save_positions(active_positions)
            return False

        # ৩. ১০ বার সেল রিট্রাই লুপ
        for attempt in range(10):
            try:
                sell_order = client.order_market_sell(
                    symbol=symbol,
                    quantity=adjusted_qty
                )
                logging.info(f"SUCCESSFUL {reason}: Sold {adjusted_qty} of {symbol} (Attempt {attempt+1})")
                
                # সেল সফল হলে মেমোরি এবং JSON উভয় স্থান থেকেই ডিলিট
                if symbol in active_positions:
                    del active_positions[symbol]
                    save_positions(active_positions)
                return True

            except BinanceAPIException as binance_err:
                logging.warning(f"Sell Attempt {attempt+1} failed for {symbol}: {binance_err}")
                time.sleep(0.3)
                
                # ব্যালেন্স সংক্রান্ত সমস্যা হলে ওয়ালেট থেকে রিয়েল-টাইম ব্যালেন্স নিয়ে সেল চেষ্টা
                if "insufficient balance" in str(binance_err).lower():
                    balance = float(client.get_asset_balance(asset=base_asset)['free'])
                    adjusted_qty = float(int(balance / step_size) * step_size)
                    adjusted_qty = round(adjusted_qty, precision)

        logging.critical(f"ALERT: Could NOT sell {symbol} after 10 attempts!")
        return False

    except Exception as e:
        logging.error(f"Sell Execution Error for {symbol}: {e}")
        return False

def process_klines_and_signal(symbol, df, current_close):
    # ১. কেনা পজিশনের টেক-প্রফিট ও স্টপ-লস প্রসেসিং
    if symbol in active_positions:
        entry_price = active_positions[symbol]['entry_price']
        qty = active_positions[symbol]['qty']
        
        stop_loss_trigger = entry_price * (1 - 0.010)  # ১.০% স্টপ লস
        upper_b = df.iloc[-1]['upper_band']

        # টেক-প্রফিট হিট করলে
        if current_close >= upper_b:
            logging.info(f"TAKE PROFIT HIT: {symbol} | Current: {current_close} >= Upper BB: {upper_b}")
            execute_market_sell(symbol, qty, reason="TAKE PROFIT SELL")
            return

        # স্টপ-লস হিট করলে
        if current_close <= stop_loss_trigger:
            drop_percent = round(((entry_price - current_close) / entry_price) * 100, 2)
            logging.warning(f"STOP LOSS TRIGGERED ({drop_percent}% drop): {symbol} | Current: {current_close} | Entry: {entry_price}")
            execute_market_sell(symbol, qty, reason=f"STOP LOSS SELL ({drop_percent}%)")
            return

    # ২. নতুন কেনা (BUY) সিগন্যাল চেকিং (শুধু ক্লোজড ক্যান্ডেলে)
    else:
        if len(df) < 20:
            return

        last_closed_candle = df.iloc[-1]
        open_p = last_closed_candle['open']
        close_p = last_closed_candle['close']
        high_p = last_closed_candle['high']
        lower_b = last_closed_candle['lower_band']
        ema5_p = last_closed_candle['ema5']

        # বাই করার শর্তাবলি
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

                # মেমোরি ও JSON ফাইলে ডাটা সেভ
                active_positions[symbol] = {
                    'qty': executed_qty,
                    'entry_price': actual_entry_price
                }
                save_positions(active_positions)
                
                logging.info(f"SUCCESSFUL BUY: {symbol} | Entry Price: {actual_entry_price} | Qty: {executed_qty}")

            except Exception as e:
                logging.error(f"Buy Order Failed for {symbol}: {e}")

async def listen_binance_websocket():
    """বাইনান্সের স্পট ক্যান্ডেল স্ট্রিম গ্রহণ"""
    stream_names = "/".join([f"{symbol.lower()}@kline_5m" for symbol in top_pairs_list])
    url = f"wss://stream.binance.com:9443/ws/{stream_names}"
    
    logging.info("Connecting directly to Binance WebSocket Stream...")
    
    while True:
        try:
            async with websockets.connect(url) as websocket:
                logging.info("WebSocket Connected Successfully!")
                while True:
                    message = await websocket.recv()
                    data = json.loads(message)
                    
                    if 'k' in data:
                        kline = data['k']
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

                        # ক্যান্ডেল চলাকালীন রিয়েল-টাইমে টেক-প্রফিট/স্টপ-লস মনিটর করা
                        if symbol in active_positions and len(candles_history[symbol]) >= 20:
                            temp_list = candles_history[symbol] + [candle]
                            df_temp = pd.DataFrame(temp_list)
                            sma = df_temp['close'].rolling(window=20).mean()
                            std = df_temp['close'].rolling(window=20).std()
                            df_temp['upper_band'] = sma + (std * 2)
                            
                            process_klines_and_signal(symbol, df_temp, candle['close'])

                        # ক্যান্ডেল বন্ধ (Close) হলে হিস্ট্রি আপডেট ও বাই সিগন্যাল ফিল্টারিং
                        if candle['is_closed']:
                            candles_history[symbol].append(candle)
                            if len(candles_history[symbol]) > 30:
                                candles_history[symbol].pop(0)

                            if len(candles_history[symbol]) >= 20:
                                df = pd.DataFrame(candles_history[symbol])
                                sma = df['close'].rolling(window=20).mean()
                                std = df['close'].rolling(window=20).std()
                                df['lower_band'] = sma - (std * 2)
                                df['upper_band'] = sma + (std * 2)
                                df['ema5'] = df['close'].ewm(span=5, adjust=False).mean()

                                process_klines_and_signal(symbol, df, candle['close'])

        except Exception as e:
            logging.error(f"WebSocket Error: {e}. Reconnecting in 5 seconds...")
            await asyncio.sleep(5)

def start_async_loop():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(listen_binance_websocket())

if __name__ == "__main__":
    flask_thread = threading.Thread(target=run_flask)
    flask_thread.daemon = True
    flask_thread.start()

    ws_thread = threading.Thread(target=start_async_loop)
    ws_thread.daemon = True
    ws_thread.start()
    
    ws_thread.join()
