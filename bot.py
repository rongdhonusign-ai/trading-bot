import os
import asyncio
import threading
from flask import Flask
import ccxt.pro as ccxt  # WebSocket-এর জন্য ccxt.pro ব্যবহার করা হয়েছে
import pandas as pd
import pandas_ta as ta

# ----------------------------------------------------
# ১. Flask Server
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
TOP_COINS_REFRESH_INTERVAL = 1200  # ২০ মিনিট পর পর টপ ৫০ রিফ্রেশ

positions = {}

STABLECOINS = {
    'USDT', 'USDC', 'BUSD', 'FDUSD', 'TUSD', 'DAI', 'EUR', 'GBP', 
    'WBTC', 'WEAX', 'AEUR', 'PAX', 'USDP', 'SUSD'
}

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
# ৩. সেফ টপ ৫০ ফেচার (REST API)
# ----------------------------------------------------
async def get_top_50_altcoins_safely():
    try:
        tickers = await exchange.fetch_tickers()
        usdt_pairs = []

        for symbol, item in tickers.items():
            if symbol.endswith('/USDT'):
                base = symbol.split('/')[0]
                if base not in STABLECOINS and not any(x in base for x in ['UP', 'DOWN', 'BULL', 'BEAR']):
                    quote_volume = float(item.get('quoteVolume', 0) or 0)
                    usdt_pairs.append((symbol, quote_volume))

        usdt_pairs.sort(key=lambda x: x[1], reverse=True)
        return [item[0] for item in usdt_pairs[:50]]

    except Exception as e:
        print(f"Error fetching top coins: {e}", flush=True)
        return []

# ----------------------------------------------------
# ৪. WebSocket পজিশন মনিটর (Fast Sell Check)
# ----------------------------------------------------
async def watch_position_symbol(symbol):
    """শুধুমাত্র যেসব কয়েনে পজিশন ওপেন আছে সেগুলোর রিয়েল-টাইম টিক ওয়াচ করবে"""
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
# ৫. WebSocket দিয়ে কয়েন অ্যানালাইসিস ও বাই সিগন্যাল
# ----------------------------------------------------
async def watch_and_analyze_symbol(symbol):
    """WebSocket দিয়ে ক্যাণ্ডেল (OHLCV) স্ট্রিম রিসিভ করে বাই সিগন্যাল অ্যানালাইসিস করবে"""
    while True:
        try:
            if symbol in positions:
                await asyncio.sleep(5)
                continue

            # WebSocket দিয়ে রিয়েল-টাইম ৫ মিনিটের ক্যাণ্ডেল ডেটা স্ট্রিম
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

                # কেনা হয়ে গেলে সাথে সাথে আলাদা WebSocket মনিটর টাস্ক চালু হবে
                asyncio.create_task(watch_position_symbol(symbol))

        except Exception as e:
            print(f"WebSocket Analysis Error for {symbol}: {e}", flush=True)
            await asyncio.sleep(5)

# ----------------------------------------------------
# ৬. প্রধান লুপ (WebSocket Manager Task)
# ----------------------------------------------------
async def main_loop():
    active_tasks = {}

    while True:
        try:
            print("🔄 Fetching Top 50 Altcoins via API...", flush=True)
            top_50_coins = await get_top_50_altcoins_safely()

            if top_50_coins:
                print(f"✅ Active WebSocket Streams Starting for Top {len(top_50_coins)} Coins...", flush=True)
                
                # পুরানো টাস্ক যেগুলো টপ ৫০ তালিকায় নেই সেগুলো বাদ দেওয়া
                for symbol in list(active_tasks.keys()):
                    if symbol not in top_50_coins:
                        active_tasks[symbol].cancel()
                        del active_tasks[symbol]

                # নতুন কয়েনগুলোর জন্য আলাদা কনকারেন্ট ব্যাকগ্রাউন্ড WebSocket স্ট্রিম শুরু
                for symbol in top_50_coins:
                    if symbol not in active_tasks or active_tasks[symbol].done():
                        task = asyncio.create_task(watch_and_analyze_symbol(symbol))
                        active_tasks[symbol] = task

            # ২০ মিনিট পর আবার টপ ৫০ রিফ্রেশ হবে
            await asyncio.sleep(TOP_COINS_REFRESH_INTERVAL)

        except Exception as e:
            print(f"Error in Main Manager Loop: {e}", flush=True)
            await asyncio.sleep(10)

# ----------------------------------------------------
# ৭. ব্যাকগ্রাউন্ড থ্রেড
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
