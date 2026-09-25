import os
import asyncio
import threading
from flask import Flask
import ccxt.async_support as ccxt
import pandas as pd
import pandas_ta as ta

# ----------------------------------------------------
# ১. Flask Web Server
# ----------------------------------------------------
app = Flask(__name__)

@app.route('/')
def home():
    return "Binance Trading Bot with Fast-Sell Engine is Running!"

# ----------------------------------------------------
# ২. প্যারামিটার ও কনফিগারেশন
# ----------------------------------------------------
API_KEY = os.environ.get('BINANCE_API_KEY', '')
SECRET_KEY = os.environ.get('BINANCE_SECRET_KEY', '')

TRADE_AMOUNT_USDT = 15.0
TIME_FRAME = '5m'
BOLLINGER_PERIOD = 20
BOLLINGER_STD = 2
STOP_LOSS_PCT = 0.03 # 3% Stop Loss

# ওপেন থাকা ট্রেড ট্র্যাকিং
positions = {}

STABLECOINS = {
    'USDT', 'USDC', 'BUSD', 'FDUSD', 'TUSD', 'DAI', 'EUR', 'GBP', 
    'WBTC', 'WEAX', 'AEUR', 'PAX', 'USDP', 'SUSD'
}

# ----------------------------------------------------
# ৩. CCXT ইনিশিয়ালাইজেশন
# ----------------------------------------------------
exchange = ccxt.binance({
    'apiKey': API_KEY,
    'secret': SECRET_KEY,
    'enableRateLimit': True,
    'options': {'defaultType': 'spot'}
})

# ----------------------------------------------------
# ৪. টপ অল্টকয়েন ফিল্টারিং
# ----------------------------------------------------
async def get_top_50_altcoins():
    try:
        tickers = await exchange.fetch_tickers()
        usdt_pairs = []

        for symbol, ticker in tickers.items():
            if symbol.endswith('/USDT'):
                base = symbol.split('/')[0]
                if base not in STABLECOINS and not any(x in base for x in ['UP', 'DOWN', 'BULL', 'BEAR']):
                    quote_volume = ticker.get('quoteVolume', 0)
                    if quote_volume:
                        usdt_pairs.append((symbol, quote_volume))

        usdt_pairs.sort(key=lambda x: x[1], reverse=True)
        return [item[0] for item in usdt_pairs[:50]]
    except Exception as e:
        print(f"Error fetching top coins: {e}", flush=True)
        return []

# ----------------------------------------------------
# ৫. ইনস্ট্যান্ট সেল ইনজিন (প্রতি ৫ সেকেণ্ডে চেক হবে)
# ----------------------------------------------------
async def monitor_open_positions():
    """কেনা কয়েনগুলো রিয়েল-টাইমে (Upper Band & 3% SL) মনিটর করবে"""
    if not positions:
        return

    for symbol in list(positions.keys()):
        try:
            ohlcv = await exchange.fetch_ohlcv(symbol, timeframe=TIME_FRAME, limit=25)
            if len(ohlcv) < 20:
                continue

            df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
            bb = ta.bbands(df['close'], length=BOLLINGER_PERIOD, std=BOLLINGER_STD)
            
            current_close = df.iloc[-1]['close']
            current_upper_band = bb.iloc[-1, 2] # ডায়নামিক আপার ব্যান্ড

            entry_price = positions[symbol]['entry_price']
            amount = positions[symbol]['amount']
            stop_loss_price = entry_price * (1 - STOP_LOSS_PCT)

            # টেক প্রফিট (Upper Band Hit) অথবা স্টপ লস (3%) কন্ডিশন
            if current_close > current_upper_band or current_close <= stop_loss_price:
                reason = "Upper Band Touched/Crossed" if current_close > current_upper_band else "Stop Loss Hit (3%)"
                print(f"⚡ [FAST SELL TRIGGERED] [{symbol}] Reason: {reason} at Price: {current_close}", flush=True)
                
                order = await exchange.create_market_sell_order(symbol, amount)
                print(f"✅ Executed Market Sell Order ID: {order['id']}", flush=True)
                
                del positions[symbol] # পজিশন খালি করা

        except Exception as e:
            print(f"Error in fast sell monitor for {symbol}: {e}", flush=True)

# ----------------------------------------------------
# ৬. স্ক্যান ও বাই লজিক
# ----------------------------------------------------
async def analyze_and_buy(symbol):
    try:
        # যদি আগে থেকেই কেনা থাকে তবে নতুন করে বাই সিগন্যাল চেক করবে না
        if symbol in positions:
            return

        ohlcv = await exchange.fetch_ohlcv(symbol, timeframe=TIME_FRAME, limit=30)
        if len(ohlcv) < 26:
            return

        df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
        bb = ta.bbands(df['close'], length=BOLLINGER_PERIOD, std=BOLLINGER_STD)
        
        df['lower_band'] = bb.iloc[:, 0]
        df['ma20'] = bb.iloc[:, 1]

        last_row = df.iloc[-1]
        five_candles_ago = df.iloc[-6]

        current_close = last_row['close']
        current_lower_band = last_row['lower_band']
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
                'amount': filled_amount
            }
            print(f"✅ Bought {symbol} at {executed_price} USDT, Amount: {filled_amount}", flush=True)

    except Exception as e:
        print(f"Error evaluating {symbol}: {e}", flush=True)

# ----------------------------------------------------
# ৭. প্রধান স্ক্যান লুপ
# ----------------------------------------------------
async def main_loop():
    while True:
        try:
            # আগে একবার ফাস্ট সেল লুপ রান করে নিবে
            await monitor_open_positions()

            print("Fetching top 50 altcoins...", flush=True)
            top_50_symbols = await get_top_50_altcoins()
            
            if not top_50_symbols:
                print("IP Banned or Fetch Failed. Waiting 5 minutes...", flush=True)
                await asyncio.sleep(300)
                continue

            print(f"Scanning {len(top_50_symbols)} coins...", flush=True)

            for symbol in top_50_symbols:
                await analyze_and_buy(symbol)
                # প্রতি কয়েন চেক করার পর আবার ওপেন পজিশনে দ্রুত সেল চেক করবে
                await monitor_open_positions()
                await asyncio.sleep(0.8) 

            print("Scan completed. Waiting 2 minutes for next cycle...", flush=True)
            
            # ২ মিনিটের অপেক্ষার সময় প্রতি ৫ সেকেন্ড পর পর সেল চেক চলবে
            for _ in range(24):
                await monitor_open_positions()
                await asyncio.sleep(5)

        except Exception as e:
            print(f"Error in main loop: {e}", flush=True)
            await asyncio.sleep(10)

# ----------------------------------------------------
# ৮. ব্যাকগ্রাউন্ড রানার
# ----------------------------------------------------
def start_async_loop():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(main_loop())

bot_thread = threading.Thread(target=start_async_loop, daemon=True)
bot_thread.start()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    app.run(host='0.0.0.0', port=port)
