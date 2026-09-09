import time
import os
import sys
import pandas as pd
import ccxt
from flask import Flask
from threading import Thread

# ==========================================
# 1. FLASK APP (FOR RENDER HEALTH CHECK)
# ==========================================
app = Flask(__name__)

@app.route('/')
@app.route('/<path:path>')
def home(path=""):
    return "Trading Bot is Active and Scanning!", 200

def run_flask():
    port = int(os.environ.get("PORT", 10000))
    app.run(host='0.0.0.0', port=port, debug=False, use_reloader=False)

# ==========================================
# 2. CONFIGURATION & TARGET SYMBOLS
# ==========================================
target_symbols = [
    'LISTA/USDT', 'FLOKI/USDT', 'BMT/USDT', 'BNB/USDT', 'THE/USDT', 
    'BEL/USDT', 'CAKE/USDT', 'ONT/USDT', 'ZAMA/USDT', 'MEGA/USDT', 
    'ENS/USDT', 'BICO/USDT', 'T/USDT', 'SSV/USDT', 'GLM/USDT', 
    'ALT/USDT', 'AXL/USDT', 'IO/USDT', 'ZRO/USDT', 'HEI/USDT', 
    'RED/USDT', 'ZK/USDT', 'QNT/USDT', 'THETA/USDT', 'TRB/USDT', 
    'ZEN/USDT', 'IOTX/USDT', 'BERA/USDT'
]

trade_amount_usdt = 6.0   
stop_loss_pct = 0.02      

trade_exchange = ccxt.binance({
    'apiKey': os.environ.get('BINANCE_API_KEY', 'yRwdwQAR1S9G8DLVeQp39lW99BAGEF4XDG6hoImJkFTol2RFvWmTvksMKy5Bav0M'),
    'secret': os.environ.get('BINANCE_SECRET_KEY', '3qsGUF6nPgfluSLPe8VXo0DE2gtR1jQIud9URVC5NHezEFp9YQV1lLqG1WncAltV'),
    'enableRateLimit': True,
    'options': {
        'defaultType': 'spot',
        'adjustForTimeDifference': False,
        'recvWindow': 10000
    }
})

prices_history = {sym: [] for sym in target_symbols}
positions = {sym: False for sym in target_symbols}
entry_prices = {sym: 0.0 for sym in target_symbols}

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
# 4. BOT CORE LOOP
# ==========================================
def start_bot_loop():
    print("🚀 BOT LOOP STARTED SUCCESSFULLY! FETCHING PRICES...", flush=True)
    
    while True:
        try:
            # একবারে সব টোকেনের রেট ফেস করা
            tickers = trade_exchange.fetch_tickers(target_symbols)
            
            for symbol in target_symbols:
                if symbol in tickers:
                    current_price = tickers[symbol]['close']
                    high_price = tickers[symbol]['high']
                    low_price = tickers[symbol]['low']
                    
                    prices_history[symbol].append({
                        'close': current_price,
                        'high': high_price,
                        'low': low_price
                    })
                    
                    if len(prices_history[symbol]) > 30:
                        prices_history[symbol].pop(0)

                    df = pd.DataFrame(prices_history[symbol])

                    if len(df) >= 14:
                        df = calculate_indicators(df)
                        last_row = df.iloc[-1]
                        prev_row = df.iloc[-2]

                        crsi = last_row.get('crsi', 0)
                        stoch_k = last_row.get('stoch_k', 0)

                        print(f"⚡ [SCAN {symbol}] Price: {current_price} | CRSI: {crsi:.1f} | Stoch: {stoch_k:.1f}", flush=True)

                        buy_condition = (crsi < 20) and (stoch_k < 20)
                        sell_condition = (prev_row.get('crsi', 0) <= 80 and crsi > 80) and (prev_row.get('stoch_k', 0) <= 80 and stoch_k > 80)

                        if not positions[symbol] and buy_condition:
                            crypto_quantity = trade_amount_usdt / current_price
                            print(f"🔥 BUY SIGNAL: {symbol} at ${current_price}", flush=True)
                            order = trade_exchange.create_market_buy_order(symbol, crypto_quantity)
                            print(f"✅ EXECUTED BUY: {order}", flush=True)
                            positions[symbol] = True
                            entry_prices[symbol] = current_price

                        elif positions[symbol]:
                            stop_price = entry_prices[symbol] * (1 - stop_loss_pct)
                            if current_price <= stop_price or sell_condition:
                                crypto_quantity = trade_amount_usdt / entry_prices[symbol]
                                print(f"🛑 EXIT/STOP LOSS: {symbol} at ${current_price}", flush=True)
                                order = trade_exchange.create_market_sell_order(symbol, crypto_quantity)
                                print(f"✅ EXECUTED SELL: {order}", flush=True)
                                positions[symbol] = False
                                entry_prices[symbol] = 0.0
                    else:
                        print(f"⏳ [SCAN {symbol}] Gathering Data: Price {current_price} ({len(df)}/14)", flush=True)

            # ৫ সেকেন্ড পর পর আবার চেক করবে
            time.sleep(5)
            
        except Exception as e:
            print(f"Loop Error: {e}", flush=True)
            time.sleep(5)

# ==========================================
# 5. MAIN EXECUTION
# ==========================================
if __name__ == '__main__':
    # ১. Flask চলবে ব্যাকগ্রাউন্ড থ্রেডে
    flask_thread = Thread(target=run_flask)
    flask_thread.daemon = True
    flask_thread.start()
    
    # ২. ট্রেডিং লুপ চলবে মেইন প্রসেসে
    start_bot_loop()
