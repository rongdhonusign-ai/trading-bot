import os
import asyncio
import threading
from flask import Flask
import ccxt.async_support as ccxt
import pandas as pd
import pandas_ta as ta

app = Flask(__name__)

@app.route('/')
def home():
    return "Binance Trading Bot is Running!"

def run_flask():
    port = int(os.environ.get("PORT", 8080))
    app.run(host='0.0.0.0', port=port)

API_KEY = os.environ.get('BINANCE_API_KEY', '')
SECRET_KEY = os.environ.get('BINANCE_SECRET_KEY', '')

TRADE_AMOUNT_USDT = 15.0
TIME_FRAME = '5m'
BOLLINGER_PERIOD = 20
BOLLINGER_STD = 2
STOP_LOSS_PCT = 0.03

positions = {}

STABLECOINS = {
    'USDT', 'USDC', 'BUSD', 'FDUSD', 'TUSD', 'DAI', 'EUR', 'GBP', 
    'WBTC', 'WEAX', 'AEUR', 'PAX', 'USDP', 'SUSD'
}

exchange = ccxt.binance({
    'apiKey': API_KEY,
    'secret': SECRET_KEY,
    'enableRateLimit': True,
    'options': {'defaultType': 'spot'}
})

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
        print(f"Error fetching top coins: {e}")
        return []

async def analyze_and_trade(symbol):
    try:
        ohlcv = await exchange.fetch_ohlcv(symbol, timeframe=TIME_FRAME, limit=30)
        if len(ohlcv) < 26:
            return

        df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
        
        # Bollinger Bands ক্যালকুলেশন
        bb = ta.bbands(df['close'], length=BOLLINGER_PERIOD, std=BOLLINGER_STD)
        
        # dynamic column name বের করা
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

        if symbol in positions:
            entry_price = positions[symbol]['entry_price']
            amount = positions[symbol]['amount']
            stop_loss_price = entry_price * (1 - STOP_LOSS_PCT)
            
            if current_close > current_upper_band or current_close <= stop_loss_price:
                reason = "Upper Band Hit" if current_close > current_upper_band else "Stop Loss Hit (3%)"
                print(f"[{symbol}] Selling. Reason: {reason}")
                
                order = await exchange.create_market_sell_order(symbol, amount)
                print(f"Sell Order Executed: {order['id']}")
                del positions[symbol]

        else:
            condition_1 = current_close < current_lower_band
            condition_2 = current_ma20 > prev_ma20

            if condition_1 and condition_2:
                print(f"[{symbol}] BUY Signal Detected!")
                
                # Binance Spot Market-এ নির্দিষ্ট পরিমাণ USDT খরচ করে কেনার নিয়ম:
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
                print(f"Bought {symbol} at {executed_price} USDT, Amount: {filled_amount}")

    except Exception as e:
        print(f"Error processing {symbol}: {e}")

async def main_loop():
    while True:
        try:
            print("Fetching top 50 altcoins...")
            top_50_symbols = await get_top_50_altcoins()
            print(f"Scanning {len(top_50_symbols)} coins...")

            for symbol in top_50_symbols:
                await analyze_and_trade(symbol)
                await asyncio.sleep(0.2) 

            print("Scan completed. Waiting for next cycle...")
            await asyncio.sleep(60) 

        except Exception as e:
            print(f"Error in main loop: {e}")
            await asyncio.sleep(10)

if __name__ == "__main__":
    threading.Thread(target=run_flask, daemon=True).start()
    
    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(main_loop())
    finally:
        loop.run_until_complete(exchange.close())
