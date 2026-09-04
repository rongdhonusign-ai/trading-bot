import time
import pandas as pd
import pandas_ta as ta
import ccxt

# ==========================================
# 1. API KEY & EXCHANGE SETUP (SPOT)
# ==========================================
API_KEY = 'yRwdwQAR1S9G8DLVeQp39lW99BAGEF4XDG6hoImJkFTol2RFvWmTvksMKy5Bav0M'        # আপনার Binance API Key বসান
SECRET_KEY = '3qsGUF6nPgfluSLPe8VXo0DE2gtR1jQIud9URVC5NHezEFp9YQV1lLqG1WncAltV'  # আপনার Binance Secret Key বসান

# Binance Spot Setup
exchange = ccxt.binance({
    'apiKey': API_KEY,
    'secret': SECRET_KEY,
    'enableRateLimit': True,
    'options': {
        'defaultType': 'spot'
    }
})

# ==========================================
# 2. CONFIGURATION & SYMBOLS
# ==========================================
TIMEFRAME = '5m'
STOP_LOSS_PCT = 0.02  # 2% Stop Loss
TRADE_USDT_AMOUNT = 6.0  # Trade amount $6 USDT

SYMBOLS = [
    'LISTA/USDT', 'FLOKI/USDT', 'BMT/USDT', 'BNB/USDT', 'THE/USDT', 
    'BEL/USDT', 'CAKE/USDT', 'ONT/USDT', 'ZAMA/USDT', 'MEGA/USDT', 
    'ENS/USDT', 'BICO/USDT', 'T/USDT', 'SSV/USDT', 'GLM/USDT', 
    'ALT/USDT', 'AXL/USDT', 'IO/USDT', 'ZRO/USDT', 'HEI/USDT', 
    'RED/USDT', 'ZK/USDT', 'QNT/USDT', 'THETA/USDT', 'TRB/USDT', 
    'ZEN/USDT', 'IOTX/USDT', 'BERA/USDT'
]

# ==========================================
# 3. INDICATOR CALCULATIONS
# ==========================================
def fetch_ohlcv(symbol, timeframe=TIMEFRAME, limit=150):
    bars = exchange.fetch_ohlcv(symbol, timeframe=timeframe, limit=limit)
    df = pd.DataFrame(bars, columns=['time', 'open', 'high', 'low', 'close', 'volume'])
    return df

def calculate_crsi(df, rsi_len=3, updn_len=2, roc_len=2):
    rsi_val = ta.rsi(df['close'], length=rsi_len)
    
    streak = [0]
    for i in range(1, len(df)):
        if df['close'].iloc[i] > df['close'].iloc[i-1]:
            streak.append(streak[-1] + 1 if streak[-1] > 0 else 1)
        elif df['close'].iloc[i] < df['close'].iloc[i-1]:
            streak.append(streak[-1] - 1 if streak[-1] < 0 else -1)
        else:
            streak.append(0)
    
    df['streak'] = streak
    updn_rsi = ta.rsi(df['streak'], length=updn_len)
    
    roc = ta.roc(df['close'], length=1)
    roc_rank = roc.rolling(window=roc_len).apply(
        lambda x: (pd.Series(x).rank().iloc[-1] - 1) / (len(x) - 1) * 100 if len(x) > 1 else 0
    )
    
    return (rsi_val + updn_rsi + roc_rank) / 3.0

def calculate_stoch_k(df, k_len=14, k_smooth=3):
    stoch = ta.stoch(high=df['high'], low=df['low'], close=df['close'], k=k_len, d=14, smooth_k=k_smooth)
    k_col = [col for col in stoch.columns if col.startswith('STOCHk')][0]
    return stoch[k_col]

# ==========================================
# 4. BOT CYCLE EXECUTION
# ==========================================
def check_markets():
    print("\n--- [ Scanning Spot Markets... ] ---")
    
    for symbol in SYMBOLS:
        while True:
            try:
                df = fetch_ohlcv(symbol)
                if df.empty or len(df) < 100:
                    break

                df['crsi'] = calculate_crsi(df)
                df['stoch_k'] = calculate_stoch_k(df)

                crsi_curr = df['crsi'].iloc[-1]
                stoch_k_curr = df['stoch_k'].iloc[-1]
                crsi_prev = df['crsi'].iloc[-2]
                stoch_k_prev = df['stoch_k'].iloc[-2]

                buy_condition = (crsi_curr < 20) and (stoch_k_curr < 20)
                sell_condition = (crsi_prev <= 80 and crsi_curr > 80) and (stoch_k_prev <= 80 and stoch_k_curr > 80)

                print(f"[{symbol}] CRSI: {crsi_curr:.2f} | Stoch %K: {stoch_k_curr:.2f}")

                current_price = df['close'].iloc[-1]

                # 1. LIVE MARKET BUY ORDER TRIGGER
                if buy_condition:
                    raw_amount = TRADE_USDT_AMOUNT / current_price
                    amount = float(exchange.amount_to_precision(symbol, raw_amount))
                    stop_price = current_price * (1 - STOP_LOSS_PCT)
                    
                    print(f"\n🚀 [BUY SIGNAL DETECTED] {symbol}")
                    print(f"Size: ${TRADE_USDT_AMOUNT} | Quantity: {amount}")
                    print(f"Entry Price: ${current_price:.4f} | Stop Loss (2%): ${stop_price:.4f}")
                    
                    # 🛒 Executing Market Buy Order
                    try:
                        buy_order = exchange.create_market_buy_order(symbol, amount)
                        print(f"✅ MARKET BUY ORDER SUCCESSFUL! Order ID: {buy_order['id']}")
                    except Exception as buy_error:
                        print(f"❌ BUY FAILED: {buy_error}")

                # 2. LIVE MARKET SELL ORDER TRIGGER
                elif sell_condition:
                    print(f"\n🔻 [EXIT SIGNAL DETECTED] {symbol} - CRSI & Stoch Cross Above 80")
                    
                    # 🏷️ Executing Market Sell Order for Available Balance
                    try:
                        coin_name = symbol.split('/')[0]
                        balance = exchange.fetch_balance()
                        coin_balance = balance['free'].get(coin_name, 0)
                        
                        if coin_balance > 0:
                            sell_amount = float(exchange.amount_to_precision(symbol, coin_balance))
                            sell_order = exchange.create_market_sell_order(symbol, sell_amount)
                            print(f"✅ MARKET SELL ORDER SUCCESSFUL! Order ID: {sell_order['id']}")
                        else:
                            print(f"⚠️ No balance found for {coin_name} to sell.")
                    except Exception as sell_error:
                        print(f"❌ SELL FAILED: {sell_error}")

                break

            except (ccxt.NetworkError, ccxt.ExchangeNotAvailable, ccxt.RequestTimeout) as e:
                print(f"\n⚠️ Network Drop! Reconnecting in 10s...")
                time.sleep(10)
            except Exception as e:
                print(f"Error checking {symbol}: {e}")
                break

# ==========================================
# 5. MAIN LOOP
# ==========================================
if __name__ == "__main__":
    print("Bot Started Successfully! (Spot Mode - $6 Per Trade - Live Market Orders Enabled)")
    try:
        while True:
            try:
                check_markets()
            except Exception as main_e:
                print(f"\n⚠️ Error: {main_e}")
                time.sleep(10)

            # Wait 5 minutes for next candle scan
            for _ in range(300):
                time.sleep(1)
                
    except KeyboardInterrupt:
        print("\nBot Stopped by User.")
