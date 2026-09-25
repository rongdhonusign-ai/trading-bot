import os
import asyncio
import threading
from flask import Flask
import ccxt.async_support as ccxt
import pandas as pd
import pandas_ta as ta

# ----------------------------------------------------
# ১. Flask Web Server (Render Free Tier Activity Handler)
# ----------------------------------------------------
app = Flask(__name__)

@app.route('/')
def home():
    return "Binance Trading Bot is Running!"

def run_flask():
    port = int(os.environ.get("PORT", 8080))
    app.run(host='0.0.0.0', port=port)

# ----------------------------------------------------
# ২. ট্রেডিং বটের প্যারামিটার ও কনফিগারেশন
# ----------------------------------------------------
API_KEY = os.environ.get('BINANCE_API_KEY', 'yRwdwQAR1S9G8DLVeQp39lW99BAGEF4XDG6hoImJkFTol2RFvWmTvksMKy5Bav0M')
SECRET_KEY = os.environ.get('BINANCE_SECRET_KEY', '3qsGUF6nPgfluSLPe8VXo0DE2gtR1jQIud9URVC5NHezEFp9YQV1lLqG1WncAltV')

TRADE_AMOUNT_USDT = 15.0
TIME_FRAME = '5m'
BOLLINGER_PERIOD = 20
BOLLINGER_STD = 2
STOP_LOSS_PCT = 0.03 # 3%

# পজিশন ট্র্যাকিং (কয়েন অনুযায়ী ক্রয়মূল্য ও পরিমাণ রাখা)
positions = {}

# স্ট্যাবলকয়েন ও অনাকাঙ্ক্ষিত পেয়ারের ব্লকলিস্ট
STABLECOINS = {
    'USDT', 'USDC', 'BUSD', 'FDUSD', 'TUSD', 'DAI', 'EUR', 'GBP', 
    'WBTC', 'WEAX', 'AEUR', 'PAX', 'USDP', 'SUSD'
}

# ----------------------------------------------------
# ৩. এক্সচেঞ্জ ইনিশিয়ালাইজেশন
# ----------------------------------------------------
exchange = ccxt.binance({
    'apiKey': API_KEY,
    'secret': SECRET_KEY,
    'enableRateLimit': True, # IP Ban প্রতিরোধী
    'options': {
        'defaultType': 'spot'
    }
})

# ----------------------------------------------------
# ৪. টপ ৫০ অল্টকয়েন ফিল্টারিং ফাংশন
# ----------------------------------------------------
async def get_top_50_altcoins():
    try:
        tickers = await exchange.fetch_tickers()
        usdt_pairs = []

        for symbol, ticker in tickers.items():
            if symbol.endswith('/USDT'):
                base = symbol.split('/')[0]
                # স্ট্যাবলকয়েন, ডাউন/আপ লেভারেজড টোকেন বাদ দেওয়া
                if base not in STABLECOINS and not any(x in base for x in ['UP', 'DOWN', 'BULL', 'BEAR']):
                    quote_volume = ticker.get('quoteVolume', 0)
                    if quote_volume:
                        usdt_pairs.append((symbol, quote_volume))

        # ভলিউম অনুযায়ী সাজিয়ে শীর্ষ ৫০ বেছে নেওয়া
        usdt_pairs.sort(key=lambda x: x[1], reverse=True)
        top_50 = [item[0] for item in usdt_pairs[:50]]
        return top_50
    except Exception as e:
        print(f"Error fetching top coins: {e}")
        return []

# ----------------------------------------------------
# ৫. ক্যান্ডেলস্টিক ও ইন্ডিকেটর ডেটা এনালাইসিস
# ----------------------------------------------------
async def analyze_and_trade(symbol):
    try:
        # Bollinger Bands এবং MA20 ক্যালকুলেশনের জন্য ক্যান্ডেল ডেটা
        ohlcv = await exchange.fetch_ohlcv(symbol, timeframe=TIME_FRAME, limit=30)
        if len(ohlcv) < 26:
            return

        df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
        
        # Bollinger Bands নির্ণয়
        bb = ta.bbands(df['close'], length=BOLLINGER_PERIOD, std=BOLLINGER_STD)
        df['lower_band'] = bb[f'BBL_{BOLLINGER_PERIOD}_{BOLLINGER_STD}.0']
        df['upper_band'] = bb[f'BBU_{BOLLINGER_PERIOD}_{BOLLINGER_STD}.0']
        df['ma20'] = bb[f'BBM_{BOLLINGER_PERIOD}_{BOLLINGER_STD}.0'] # MA20 হলো মিডল ব্যান্ড

        last_row = df.iloc[-1]
        five_candles_ago = df.iloc[-6]

        current_close = last_row['close']
        current_lower_band = last_row['lower_band']
        current_upper_band = last_row['upper_band']
        current_ma20 = last_row['ma20']
        prev_ma20 = five_candles_ago['ma20']

        # ------------------------------------------------
        # সেল লজিক (যদি পজিশন খোলা থাকে)
        # ------------------------------------------------
        if symbol in positions:
            entry_price = positions[symbol]['entry_price']
            amount = positions[symbol]['amount']
            
            # ৩% প্রফিট/লস অথবা Upper Band ক্রস
            stop_loss_price = entry_price * (1 - STOP_LOSS_PCT)
            
            if current_close > current_upper_band or current_close <= stop_loss_price:
                reason = "Upper Band Hit" if current_close > current_upper_band else "Stop Loss Hit (3%)"
                print(f"[{symbol}] Selling 100%. Reason: {reason}")
                
                # Market Order এ সম্পূর্ণ বিক্রি
                order = await exchange.create_market_sell_order(symbol, amount)
                print(f"Sell Order Executed: {order['id']}")
                del positions[symbol]

        # ------------------------------------------------
        # বায় লজিক (যদি আগে কেনা না থাকে)
        # ------------------------------------------------
        else:
            condition_1 = current_close < current_lower_band
            condition_2 = current_ma20 > prev_ma20

            if condition_1 and condition_2:
                print(f"[{symbol}] BUY Signal Detected!")
                
                # Market Order এ কেনা
                order = await exchange.create_market_buy_order_requires_price(symbol, TRADE_AMOUNT_USDT)
                # ক্রয়কৃত পরিমাণ বের করা
                filled_amount = order['filled']
                executed_price = order['price'] or current_close
                
                positions[symbol] = {
                    'entry_price': executed_price,
                    'amount': filled_amount
                }
                print(f"Bought {symbol} at {executed_price} USDT, Amount: {filled_amount}")

    except Exception as e:
        # এপিআই লিমিটিং এড়াতে ছোটখাটো এরর প্রিন্ট করে ওভারলুক করবে
        pass

# ----------------------------------------------------
# ৬. প্রধান লুপ (Main Loop)
# ----------------------------------------------------
async def main_loop():
    while True:
        try:
            print("Fetching top 50 altcoins...")
            top_50_symbols = await get_top_50_altcoins()
            print(f"Scanning {len(top_50_symbols)} coins...")

            for symbol in top_50_symbols:
                await analyze_and_trade(symbol)
                # IP Ban এড়াতে প্রতিটি রিকোয়েস্টের মাঝে হালকা বিরতি
                await asyncio.sleep(0.2) 

            # প্রতি ৫ মিনিটের ক্যান্ডেল ক্লোজের কাছাকাছি সময়ে আবার চেক করবে
            print("Scan completed. Waiting for next cycle...")
            await asyncio.sleep(60) 

        except Exception as e:
            print(f"Error in main loop: {e}")
            await asyncio.sleep(10)

if __name__ == "__main__":
    # Flask সার্ভার ব্যাকগ্রাউন্ডে চালু করা
    threading.Thread(target=run_flask, daemon=True).start()
    
    # Asyncio বট চালু করা
    asyncio.run(main_loop())
