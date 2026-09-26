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
    return "Binance Bot is Active & Safe!"

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

# REST exchange setup
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
# ৩. পজিশন মনিটর (Sell Check)
# ----------------------------------------------------
async def watch_positions():
    while True:
        if positions:
            for symbol in list(positions.keys()):
                try:
                    ticker = await exchange.fetch_ticker(symbol)
                    current_price = ticker['last']
                    
                    entry_price = positions[symbol]['entry_price']
                    upper_band = positions[symbol]['upper_band']
                    amount = positions[symbol]['amount']
                    stop_loss_price = entry_price * (1 - STOP_LOSS_PCT)

                    if current_price >= upper_band or current_price <= stop_loss_price:
                        reason = "Upper Band Hit" if current_price >= upper_band else "Stop Loss Hit (3%)"
                        print(f"⚡ [SELL TRIGGERED] [{symbol}] Reason: {reason} | Price: {current_price}", flush=True)
                        
                        order = await exchange.create_market_sell_order(symbol, amount)
                        print(f"✅ Sold {symbol} | Order ID: {order['id']}", flush=True)
                        
                        del positions[symbol]

                    await asyncio.sleep(2)
                except Exception as e:
                    print(f"Position Error for {symbol}: {e}", flush=True)
        await asyncio.sleep(5)

# ----------------------------------------------------
# ৪. সেফ স্ক্যানার ফাংশন
# ----------------------------------------------------
async def analyze_symbol(symbol):
    try:
        ohlcv = await exchange.fetch_ohlcv(symbol, timeframe=TIME_FRAME, limit=30)
        if len(ohlcv) < 26:
            return

        df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
        bb = ta.bbands(df['close'], length=BOLLINGER_PERIOD, std=BOLLINGER_STD)
        
        df['lower_band'] = bb.iloc[:, 0]
        df['ma20'] = bb.iloc[:, 1]
        df['upper_band'] = bb.iloc[:, 2]

        last_row = df.iloc[-1]
        five_candles_ago = df.iloc[-6]

        current_price = last_row['close']
        current_lower_band = last_row['lower_band']
        current_upper_band = last_row['upper_band']
        current_ma20 = last_row['ma20']
        prev_ma20 = five_candles_ago['ma20']

        # লগে MA20 এর বর্তমান ও ৫ ক্যান্ডেল আগের মানসহ বিস্তারিত প্রিন্ট
        print(f"🔍 Scanning {symbol} | Price: {current_price} | Lower: {round(current_lower_band, 4)} | MA20: {round(current_ma20, 4)} (Prev: {round(prev_ma20, 4)})", flush=True)

        condition_1 = current_price < current_lower_band
        condition_2 = current_ma20 > prev_ma20

        if condition_1 and condition_2:
            print(f"🎯 [BUY SIGNAL] [{symbol}] Price: {current_price} < Lower Band: {current_lower_band}", flush=True)
            
            order = await exchange.create_market_buy_order(
                symbol, 
                amount=None, 
                params={'quoteOrderQty': TRADE_AMOUNT_USDT}
            )
            
            filled_amount = order['filled']
            executed_price = order['price'] or current_price
            
            positions[symbol] = {
                'entry_price': executed_price,
                'upper_band': current_upper_band,
                'amount': filled_amount
            }
            print(f"✅ Bought {symbol} at {executed_price} USDT", flush=True)

    except Exception as e:
        if "1003" in str(e) or "418" in str(e):
            print(f"⚠️ IP Temporarily Blocked or Rate Limit! Pausing 30s...", flush=True)
            await asyncio.sleep(30)
        else:
            print(f"Error on {symbol}: {e}", flush=True)

# ----------------------------------------------------
# ৫. প্রধান স্ক্যানিং লুপ (Sequential Execution)
# ----------------------------------------------------
async def main_loop():
    print(f"🚀 Engine Started for Top {len(TOP_50_COINS)} Coins...", flush=True)
    asyncio.create_task(watch_positions())

    while True:
        for symbol in TOP_50_COINS:
            if symbol not in positions:
                await analyze_symbol(symbol)
                # প্রতিটি রিকোয়েস্টের মাঝে ২ সেকেন্ড গ্যাপ রাখা হয়েছে যাতে IP Block না খায়
                await asyncio.sleep(2) 
        
        # ৫০টি কয়েন ১ রাউন্ড স্ক্যান শেষে ১০ সেকেন্ড বিরতি
        await asyncio.sleep(10)

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
