import os
import asyncio
import threading
from flask import Flask
import ccxt.pro as ccxt
import pandas as pd
import pandas_ta as ta

# ----------------------------------------------------
# ১. Flask Server (Render Health Check)
# ----------------------------------------------------
app = Flask(__name__)

@app.route('/')
def home():
    return "Binance WebSocket Bot is Active & Running!"

# ----------------------------------------------------
# ২. কনফিগারেশন
# ----------------------------------------------------
API_KEY = os.environ.get('BINANCE_API_KEY', '')
SECRET_KEY = os.environ.get('BINANCE_SECRET_KEY', '')

TRADE_AMOUNT_USDT = 15.0
TIME_FRAME = '5m'
BOLLINGER_PERIOD = 20
BOLLINGER_STD = 2
STOP_LOSS_PCT = 0.03

positions = {}

# ৫০টি পপুলার USDT ট্রেডিং পেয়ার (REST API ছাড়া সরাসরি ব্যবহৃত)
TOP_50_COINS = [
    'BTC/USDT', 'ETH/USDT', 'SOL/USDT', 'BNB/USDT', 'XRP/USDT', 
    'DOGE/USDT', 'ADA/USDT', 'AVAX/USDT', 'SHIB/USDT', 'DOT/USDT', 
    'LINK/USDT', 'NEAR/USDT', 'SUI/USDT', 'LTC/USDT', 'PEPE/USDT', 
    'FET/USDT', 'APT/USDT', 'ICP/USDT', 'UNI/USDT', 'RENDER/USDT', 
    'BCH/USDT', 'TIA/USDT', 'FIL/USDT', 'STX/USDT', 'INJ/USDT', 
    'WIF/USDT', 'GALA/USDT', 'ETC/USDT', 'SEI/USDT', 'ATOM/USDT', 
    'AR/USDT', 'FLOKI/USDT', 'BONK/USDT', 'FTM/USDT', 'OP/USDT', 
    'ARB/USDT', 'AAVE/USDT', 'GRT/USDT', 'RUNE/USDT', 'THETA/USDT', 
    'ALGO/USDT', 'SAND/USDT', 'MANA/USDT', 'ENA/USDT', 'JUP/USDT', 
    'ORDI/USDT', 'NOT/USDT', 'WLD/USDT', 'MKR/USDT', 'LDO/USDT'
]

# CCXT Pro WebSocket Client Setup
exchange = ccxt.binance({
    'apiKey': API_KEY,
    'secret': SECRET_KEY,
    'enableRateLimit': True,
    'options': {
        'defaultType': 'spot',
        'adjustForTimeDifference': True
    }
})

# ----------------------------------------------------
# ৩. WebSocket পজিশন মনিটর (Fast Sell Check)
# ----------------------------------------------------
async def watch_position_symbol(symbol):
    while symbol in positions:
        try:
            ticker = await exchange.watch_ticker(symbol)
            current_price = ticker['last']
            
            entry_price = positions[symbol]['entry_price']
            upper_band = positions[symbol]['upper_band']
            amount = positions[symbol]['amount']
            stop_loss_price = entry_price * (1 - STOP_LOSS_PCT)

            if current_price >= upper_band or current_price <= stop_loss_price:
                reason = "Upper Band Hit" if current_price >= upper_band else "Stop Loss Hit (3%)"
                print(f"⚡ [FAST SELL TRIGGERED] [{symbol}] Reason: {reason} at Price: {current_price}", flush=True)
                
                order = await exchange.create_market_sell_order(symbol, amount)
                print(f"✅ Executed Market Sell Order ID: {order['id']}", flush=True)
                
                del positions[symbol]
                break

        except Exception as e:
            print(f"WebSocket Ticker Error for {symbol}: {e}", flush=True)
            await asyncio.sleep(2)

# ----------------------------------------------------
# ৪. WebSocket দিয়ে কয়েন অ্যানালাইসিস ও বাই সিগন্যাল
# ----------------------------------------------------
async def watch_and_analyze_symbol(symbol):
    print(f"📡 WebSocket Connected & Watching: {symbol}", flush=True)
    while True:
        try:
            if symbol in positions:
                await asyncio.sleep(5)
                continue

            ohlcv = await exchange.watch_ohlcv(symbol, timeframe=TIME_FRAME, limit=30)
            if len(ohlcv) < 26:
                continue

            df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
            bb = ta.bbands(df['close'], length=BOLLINGER_PERIOD, std=BOLLINGER_STD)
            
            df['lower_band'] = bb.iloc[:, 0]
            df['ma20'] = bb.iloc[:, 1]
            df['upper_band'] = bb.iloc[:, 2]

            last_row = df.iloc[-1]
            five_candles_ago = df.iloc[-6]

            current_close = last_row['close']
            current_lower_band = last_row['lower_band']
            current_upper_band = last_row['upper_band']
            current_ma20 = last_row['ma20']
            prev_ma20 = five_candles_ago['ma20']

            condition_1 = current_close < current_lower_band
            condition_2 = current_ma20 > prev_ma20

            if condition_1 and condition_2:
                print(f"🎯 [BUY SIGNAL DETECTED] [{symbol}] Price: {current_close}", flush=True)
                
                order = await exchange.create_market_buy_order(
                    symbol, 
                    amount=None, 
                    params={'quoteOrderQty': TRADE_AMOUNT_USDT}
                )
                
                filled_amount = order['filled']
                executed_price = order['price'] or current_close
                
                positions[symbol] = {
                    'entry_price': executed_price,
                    'upper_band': current_upper_band,
                    'amount': filled_amount
                }
                print(f"✅ Bought {symbol} at {executed_price} USDT", flush=True)

                asyncio.create_task(watch_position_symbol(symbol))

        except Exception as e:
            print(f"WebSocket Analysis Error for {symbol}: {e}", flush=True)
            await asyncio.sleep(5)

# ----------------------------------------------------
# ৫. প্রধান লুপ (Rate-Limit Safe WebSocket Connection)
# ----------------------------------------------------
async def main_loop():
    print(f"🚀 Starting WebSocket Streams for Top {len(TOP_50_COINS)} Coins...", flush=True)
    
    # IP Ban এড়াতে প্রতিটি কয়েন কানেক্ট করার মাঝে ২.৫ সেকেন্ডের ডিল দেওয়া হয়েছে
    for symbol in TOP_50_COINS:
        asyncio.create_task(watch_and_analyze_symbol(symbol))
        await asyncio.sleep(2.5)

    while True:
        await asyncio.sleep(3600)

# ----------------------------------------------------
# ৬. ব্যাকগ্রাউন্ড থ্রেড ও অ্যাপ স্টার্টআপ
# ----------------------------------------------------
def start_async_loop():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(main_loop())
    finally:
        loop.run_until_complete(exchange.close())

bot_thread = threading.Thread(target=start_async_loop, daemon=True)
bot_thread.start()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    app.run(host='0.0.0.0', port=port)
