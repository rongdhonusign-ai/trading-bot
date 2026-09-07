import time
import os
import pandas as pd
import ccxt
from flask import Flask
from threading import Thread

# ==========================================
# 1. FLASK APP FOR RENDER HEALTH CHECK & UPTIMEROBOT
# ==========================================
app = Flask(__name__)

@app.route('/')
@app.route('/<path:path>')
def home(path=""):
    return "Trading Bot is Running Alive!", 200

# ==========================================
# 2. BOT CONFIGURATION & SYMBOL LIST
# ==========================================
symbols = [
    'LISTA/USDT', 'FLOKI/USDT', 'BMT/USDT', 'BNB/USDT', 'THE/USDT', 
    'BEL/USDT', 'CAKE/USDT', 'ONT/USDT', 'ZAMA/USDT', 'MEGA/USDT', 
    'ENS/USDT', 'BICO/USDT', 'T/USDT', 'SSV/USDT', 'GLM/USDT', 
    'ALT/USDT', 'AXL/USDT', 'IO/USDT', 'ZRO/USDT', 'HEI/USDT', 
    'RED/USDT', 'ZK/USDT', 'QNT/USDT', 'THETA/USDT', 'TRB/USDT', 
    'ZEN/USDT', 'IOTX/USDT', 'BERA/USDT'
]

timeframe = '5m'          # ৫ মিনিটের টাইমফ্রেম
trade_amount_usdt = 6.0   # প্রতিটি ট্রেড ৬ ডলার
stop_loss_pct = 0.02      # ২% স্টপ লস

PROXY_URL = os.environ.get('PROXY_URL', '')

# ------------------------------------------
# A. PUBLIC EXCHANGE INSTANCE (ক্যান্ডেলস্টিক/ডেটা নেওয়ার জন্য)
# ------------------------------------------
public_config = {
    'enableRateLimit': True,
}
if PROXY_URL:
    public_config['proxies'] = {'http': PROXY_URL, 'https': PROXY_URL}

public_exchange = ccxt.binance(public_config)

# 🛑 IP Ban বাইপাস: CCXT-এর সব API রিকোয়েস্টকে api3 ডোমেইনে রিডাইরেক্ট করা হচ্ছে
public_exchange.urls['api']['public'] = 'https://api3.binance.com/api/v3'
public_exchange.urls['api']['sapi'] = 'https://api3.binance.com/sapi/v1'

# ------------------------------------------
# B. PRIVATE EXCHANGE INSTANCE (বাই/সেল অর্ডার নেওয়ার জন্য)
# ------------------------------------------
private_config = {
    'apiKey': os.environ.get('BINANCE_API_KEY', 'yRwdwQAR1S9G8DLVeQp39lW99BAGEF4XDG6hoImJkFTol2RFvWmTvksMKy5Bav0M'),
    'secret': os.environ.get('BINANCE_SECRET_KEY', '3qsGUF6nPgfluSLPe8VXo0DE2gtR1jQIud9URVC5NHezEFp9YQV1lLqG1WncAltV'),
    'enableRateLimit': True,
    'options': {'defaultType': 'spot'}
}
if PROXY_URL:
    private_config['proxies'] = {'http': PROXY_URL, 'https': PROXY_URL}

trade_exchange = ccxt.binance(private_config)
trade_exchange.urls['api']['public'] = 'https://api3.binance.com/api/v3'
trade_exchange.urls['api']['private'] = 'https://api3.binance.com/api/v3'

positions = {sym: False for sym in symbols}
entry_prices = {sym: 0.0 for sym in symbols}

# ==========================================
# 3. INDICATOR CALCULATIONS
# ==========================================
def calculate_rsi(series, period):
    delta = series.diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
    rs = gain / loss
    return 100 - (100 / (1 + rs))

def calculate_indicators(df):
    df['rsi'] = calculate_rsi(df['close'], 3)

    updn = [0.0] * len(df)
    close_vals = df['close'].values
    for i in range(1, len(df)):
        if close_vals[i] > close_vals[i-1]:
            updn[i] = updn[i-1] + 1 if updn[i-1] > 0 else 1
        elif close_vals[i] < close_vals[i-1]:
            updn[i] = updn[i-1] - 1 if updn[i-1] < 0 else -1
        else:
            updn[i] = 0
            
    df['updn'] = updn
    df['updn_rsi'] = calculate_rsi(pd.Series(updn), 2)

    roc = df['close'].pct_change(1)
    df['percent_rank'] = roc.rolling(2).apply(
        lambda x: (pd.Series(x).rank(pct=True).iloc[-1]) * 100, raw=False
    )

    df['crsi'] = (df['rsi'] + df['updn_rsi'] + df['percent_rank']) / 3

    low_min = df['low'].rolling(window=14).min()
    high_max = df['high'].rolling(window=14).max()
    stoch_raw = 100 * ((df['close'] - low_min) / (high_max - low_min))
    df['stoch_k'] = stoch_raw.rolling(window=3).mean().rolling(window=3).mean()

    return df

# ==========================================
# 4. MAIN TRADING LOOP
# ==========================================
def run_bot():
    print(f"🚀 Trading Bot started for {len(symbols)} coins", flush=True)

    while True:
        print("\n--- Starting New Market Scan Loop ---", flush=True)
        for symbol in symbols:
            try:
                # রিকোয়েস্টের আগে ৪ সেকেন্ড বিরতি
                time.sleep(4.0) 
                
                # exchangeInfo কল না করার জন্য সরাসরি OHLCV আনা
                bars = public_exchange.fetch_ohlcv(symbol, timeframe=timeframe, limit=100)
                df = pd.DataFrame(bars, columns=['time', 'open', 'high', 'low', 'close', 'volume'])
                
                df = calculate_indicators(df)

                last_row = df.iloc[-1]
                prev_row = df.iloc[-2]

                crsi = last_row['crsi']
                stoch_k = last_row['stoch_k']
                close_price = last_row['close']

                print(f"🔍 [{symbol}] Price: {close_price} | CRSI: {crsi:.1f} | Stoch: {stoch_k:.1f}", flush=True)

                buy_condition = (crsi < 20) and (stoch_k < 20)
                sell_condition = (prev_row['crsi'] <= 80 and crsi > 80) and (prev_row['stoch_k'] <= 80 and stoch_k > 80)

                if not positions[symbol]:
                    if buy_condition:
                        crypto_quantity = trade_amount_usdt / close_price
                        print(f"🔥 BUY SIGNAL: {symbol} at ${close_price}", flush=True)
                        order = trade_exchange.create_market_buy_order(symbol, crypto_quantity)
                        print(f"✅ EXECUTED BUY: {order}", flush=True)
                        positions[symbol] = True
                        entry_prices[symbol] = close_price

                elif positions[symbol]:
                    stop_price = entry_prices[symbol] * (1 - stop_loss_pct)

                    if close_price <= stop_price:
                        crypto_quantity = trade_amount_usdt / entry_prices[symbol]
                        print(f"🛑 STOP LOSS: {symbol} at ${close_price}", flush=True)
                        order = trade_exchange.create_market_sell_order(symbol, crypto_quantity)
                        print(f"✅ EXECUTED STOP LOSS: {order}", flush=True)
                        positions[symbol] = False
                        entry_prices[symbol] = 0.0

                    elif sell_condition:
                        crypto_quantity = trade_amount_usdt / entry_prices[symbol]
                        print(f"🎯 EXIT SIGNAL: {symbol} at ${close_price}", flush=True)
                        order = trade_exchange.create_market_sell_order(symbol, crypto_quantity)
                        print(f"✅ EXECUTED EXIT: {order}", flush=True)
                        positions[symbol] = False
                        entry_prices[symbol] = 0.0

            except Exception as e:
                print(f"⚠️ Error processing {symbol}: {e}", flush=True)

        print("--- Scan Loop Completed. Waiting 60s ---\n", flush=True)
        time.sleep(60)

# ==========================================
# 5. BACKGROUND THREAD LAUNCH
# ==========================================
bot_thread = Thread(target=run_bot)
bot_thread.daemon = True
bot_thread.start()

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 10000))
    app.run(host='0.0.0.0', port=port)
