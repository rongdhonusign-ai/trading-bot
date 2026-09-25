import os
import asyncio
import threading
import time
from flask import Flask
import ccxt.async_support as ccxt
import pandas as pd
import pandas_ta as ta

app = Flask(__name__)

@app.route('/')
def home():
    return "Binance Bot is Active!"

API_KEY = os.environ.get('BINANCE_API_KEY', '')
SECRET_KEY = os.environ.get('BINANCE_SECRET_KEY', '')

TRADE_AMOUNT_USDT = 15.0
TIME_FRAME = '5m'
BOLLINGER_PERIOD = 20
BOLLINGER_STD = 2
STOP_LOSS_PCT = 0.03
TOP_COINS_REFRESH_INTERVAL = 1200 

positions = {}

STABLECOINS = {
    'USDT', 'USDC', 'BUSD', 'FDUSD', 'TUSD', 'DAI', 'EUR', 'GBP', 
    'WBTC', 'WEAX', 'AEUR', 'PAX', 'USDP', 'SUSD'
}

exchange = ccxt.binance({
    'apiKey': API_KEY,
    'secret': SECRET_KEY,
    'enableRateLimit': True,
    'proxies': {
        'http': 'http://43.134.68.173:3128',
        'https': 'http://43.134.68.173:3128',
    },
    'options': {
        'defaultType': 'spot',
        'adjustForTimeDifference': True
    }
})

async def get_top_50_altcoins_safely():
    try:
        tickers = await exchange.publicGetTicker24hr()
        usdt_pairs = []

        for item in tickers:
            symbol_raw = item['symbol']
            if symbol_raw.endswith('USDT'):
                base = symbol_raw[:-4]
                if base not in STABLECOINS and not any(x in base for x in ['UP', 'DOWN', 'BULL', 'BEAR']):
                    quote_volume = float(item.get('quoteVolume', 0))
                    formatted_symbol = f"{base}/USDT"
                    usdt_pairs.append((formatted_symbol, quote_volume))

        usdt_pairs.sort(key=lambda x: x[1], reverse=True)
        return [item[0] for item in usdt_pairs[:50]]

    except Exception as e:
        print(f"Error fetching top coins: {e}", flush=True)
        return []

# স্বাধীন পজিশন মনিটর লুপ
async def fast_position_monitor_loop():
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
                        print(f"⚡ [FAST SELL TRIGGERED] [{symbol}] Reason: {reason} at Price: {current_price}", flush=True)
                        
                        order = await exchange.create_market_sell_order(symbol, amount)
                        print(f"✅ Executed Market Sell Order ID: {order['id']}", flush=True)
                        
                        del positions[symbol]

                except Exception as e:
                    print(f"Error in monitor for {symbol}: {e}", flush=True)
        
        await asyncio.sleep(5) # প্রতি ৫ সেকেন্ড পর পর ওপেন পজিশন চেক করবে

async def analyze_and_buy(symbol):
    try:
        if symbol in positions:
            return

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

        current_close = last_row['close']
        current_lower_band = last_row['lower_band']
        current_upper_band = last_row['upper_band']
        current_ma20 = last_row['ma20']
        prev_ma20 = five_candles_ago['ma20']

        if current_close < current_lower_band and current_ma20 > prev_ma20:
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

    except Exception as e:
        print(f"Error evaluating {symbol}: {e}", flush=True)

async def main_loop():
    # মনিটর লুপটি ব্যাকগ্রাউন্ডে আলাদাভাবে শুরু করা হলো
    asyncio.create_task(fast_position_monitor_loop())
    
    current_top_50 = []
    last_fetch_time = 0

    while True:
        try:
            current_time = time.time()

            if current_time - last_fetch_time >= TOP_COINS_REFRESH_INTERVAL or not current_top_50:
                print("🔄 Fetching Top 50 Altcoins...", flush=True)
                new_list = await get_top_50_altcoins_safely()
                
                if new_list:
                    current_top_50 = new_list
                    last_fetch_time = current_time
                    print(f"✅ Top 50 list updated! ({len(current_top_50)} coins)", flush=True)
                else:
                    print("⚠️ Proxy / Network Error! Retrying in 2 minutes...", flush=True)
                    await asyncio.sleep(120)
                    continue

            if current_top_50:
                print(f"🔍 Scanning top coins...", flush=True)
                for symbol in current_top_50:
                    await analyze_and_buy(symbol)
                    await asyncio.sleep(1)

                print("Cycle finished. Waiting 60 seconds...", flush=True)
                await asyncio.sleep(60)

        except Exception as e:
            print(f"Error in main loop: {e}", flush=True)
            await asyncio.sleep(30)

def start_async_loop():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(main_loop())

bot_thread = threading.Thread(target=start_async_loop, daemon=True)
bot_thread.start()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    app.run(host='0.0.0.0', port=port)
