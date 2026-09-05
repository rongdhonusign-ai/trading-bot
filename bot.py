import time
import os
import pandas as pd
import pandas_ta as ta
import ccxt
from flask import Flask
from threading import Thread

# ==========================================
# 1. DUMMY FLASK SERVER (For Render Web Service)
# ==========================================
app = Flask('')

@app.route('/')
def home():
    return "Trading Bot is Running Alive!"

def run_flask():
    port = int(os.environ.get("PORT", 8080))
    app.run(host='0.0.0.0', port=port)

# Background Thread for Flask
Thread(target=run_flask).start()

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

# Binance Connection Setup
exchange = ccxt.binance({
    'apiKey': 'yRwdwQAR1S9G8DLVeQp39lW99BAGEF4XDG6hoImJkFTol2RFvWmTvksMKy5Bav0M',       # 👈 এখানে আপনার Binance API Key বসান
    'secret': '3qsGUF6nPgfluSLPe8VXo0DE2gtR1jQIud9URVC5NHezEFp9YQV1lLqG1WncAltV',   # 👈 এখানে আপনার Binance Secret Key বসান
    'enableRateLimit': True,
    'options': {'defaultType': 'spot'}
})

positions = {sym: False for sym in symbols}
entry_prices = {sym: 0.0 for sym in symbols}

# ==========================================
# 3. INDICATOR CALCULATIONS (CRSI + STOCH)
# ==========================================
def calculate_indicators(df):
    df['rsi'] = ta.rsi(df['close'], length=3)

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
    df['updn_rsi'] = ta.rsi(df['updn'], length=2)

    df['roc'] = ta.roc(df['close'], length=1)
    df['percent_rank'] = df['roc'].rolling(2).apply(
        lambda x: (pd.Series(x).rank(pct=True).iloc[-1]) * 100, raw=False
    )

    df['crsi'] = (df['rsi'] + df['updn_rsi'] + df['percent_rank']) / 3

    stoch = ta.stoch(df['high'], df['low'], df['close'], k=14, d=3, smooth_k=3)
    df['stoch_k'] = stoch['STOCHk_14_3_3']

    return df

# ==========================================
# 4. MAIN MULTI-SYMBOL TRADING LOOP
# ==========================================
def run_bot():
    print(f"🚀 Real Trading Bot started for {len(symbols)} coins on {timeframe} timeframe (${trade_amount_usdt}/trade)", flush=True)

    while True:
        print("\n--- Starting New Market Scan Loop ---", flush=True)
        for symbol in symbols:
            try:
                bars = exchange.fetch_ohlcv(symbol, timeframe=timeframe, limit=100)
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

                # REAL BUY EXECUTION
                if not positions[symbol]:
                    if buy_condition:
                        crypto_quantity = trade_amount_usdt / close_price
                        print(f"🔥 BUY SIGNAL DETECTED: {symbol} at ${close_price} | Purchasing ${trade_amount_usdt}", flush=True)
                        
                        order = exchange.create_market_buy_order(symbol, crypto_quantity)
                        print(f"✅ REAL BUY ORDER EXECUTED: {order}", flush=True)
                        
                        positions[symbol] = True
                        entry_prices[symbol] = close_price

                # REAL SELL EXECUTION
                elif positions[symbol]:
                    stop_price = entry_prices[symbol] * (1 - stop_loss_pct)

                    # Stop Loss
                    if close_price <= stop_price:
                        crypto_quantity = trade_amount_usdt / entry_prices[symbol]
                        print(f"🛑 STOP LOSS TRIGGERED: {symbol} at ${close_price}", flush=True)
                        
                        order = exchange.create_market_sell_order(symbol, crypto_quantity)
                        print(f"✅ REAL STOP LOSS SELL EXECUTED: {order}", flush=True)
                        
                        positions[symbol] = False
                        entry_prices[symbol] = 0.0

                    # Technical Target Exit
                    elif sell_condition:
                        crypto_quantity = trade_amount_usdt / entry_prices[symbol]
                        print(f"🎯 EXIT SIGNAL TRIGGERED: {symbol} at ${close_price}", flush=True)
                        
                        order = exchange.create_market_sell_order(symbol, crypto_quantity)
                        print(f"✅ REAL EXIT SELL EXECUTED: {order}", flush=True)
                        
                        positions[symbol] = False
                        entry_prices[symbol] = 0.0

            except Exception as e:
                print(f"⚠️ Error processing {symbol}: {e}", flush=True)

        print("--- Scan Loop Completed. Waiting 15s ---\n", flush=True)
        time.sleep(15)

# Run loop
run_bot()
