import asyncio
import json
import logging
import os
import time
from binance.client import Client
from binance.exceptions import BinanceAPIException
import numpy as np
import requests
import pandas as pd
import websockets

# Logging setup
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

# Binance API Credentials (Environment Variables)
API_KEY = os.environ.get("BINANCE_API_KEY", "YOUR_API_KEY")
API_SECRET = os.environ.get("BINANCE_API_SECRET", "YOUR_API_SECRET")

client = Client(API_KEY, API_SECRET)

USDT_TRADE_AMOUNT = 35.0  # USDT per trade
MAX_PAIRS = 150
TIMEFRAME = "5m"

# Store OHLC memory for pairs
# Format: { 'BTCUSDT': { 'closes': [], 'lows': [], 'opens': [], 'positions': None } }
market_data = {}


def get_top_150_usdt_pairs():
    """Fetches top 150 USDT spot pairs by 24h volume safely"""
    try:
        tickers = client.get_ticker()
        usdt_pairs = [
            t
            for t in tickers
            if t["symbol"].endswith("USDT")
            and not t["symbol"].endswith("UPUSDT")
            and not t["symbol"].endswith("DOWNUSDT")
        ]
        sorted_pairs = sorted(
            usdt_pairs, key=lambda x: float(x["quoteVolume"]), reverse=True
        )
        top_pairs = [p["symbol"] for p in sorted_pairs[:MAX_PAIRS]]
        logging.info(f"Successfully loaded Top {len(top_pairs)} USDT pairs.")
        return top_pairs
    except Exception as e:
        logging.error(f"Error fetching top pairs: {e}")
        return []


def calculate_indicators(closes):
    """Calculates BB(20,2) and EMA(5) using numpy/pandas"""
    if len(closes) < 20:
        return None, None, None

    df = pd.DataFrame({"close": closes})

    # BB (20, 2)
    sma = df["close"].rolling(window=20).mean()
    std = df["close"].rolling(window=20).std()
    lower_band = sma - (std * 2)
    upper_band = sma + (std * 2)

    # EMA 5
    ema5 = df["close"].ewm(span=5, adjust=False).mean()

    return lower_band.iloc[-1], upper_band.iloc[-1], ema5.iloc[-1]


def load_initial_klines(symbol):
    """Initial kline fetch with delay to prevent IP ban on startup"""
    try:
        klines = client.get_klines(symbol=symbol, interval=TIMEFRAME, limit=30)
        closes = [float(k[4]) for k in klines]
        opens = [float(k[1]) for k in klines]
        return closes, opens
    except Exception as e:
        logging.error(f"Error loading initial klines for {symbol}: {e}")
        return [], []


async def execute_market_buy(symbol):
    """Executes $35 USDT Market Buy Order safely"""
    try:
        # Get minimum precision/quantity filter
        order = client.order_market_buy(
            symbol=symbol, quoteOrderQty=USDT_TRADE_AMOUNT
        )
        logging.info(f"BUY ORDER EXECUTED for {symbol}: {order}")
        return order
    except BinanceAPIException as e:
        logging.error(f"Binance Buy API Error ({symbol}): {e}")
    except Exception as e:
        logging.error(f"Unexpected error on Buy ({symbol}): {e}")
    return None


async def execute_market_sell(symbol):
    """Executes 100% Market Sell Order safely with retries to prevent stuck positions"""
    max_retries = 5
    for attempt in range(max_retries):
        try:
            # Check current balance of the asset
            asset = symbol.replace("USDT", "")
            balance_info = client.get_asset_balance(asset=asset)
            free_qty = float(balance_info["free"])

            if free_qty <= 0:
                logging.warning(
                    f"No free balance found for {asset} to sell."
                )
                return True

            # Get stepSize precision for the symbol
            info = client.get_symbol_info(symbol)
            step_size = None
            for f in info["filters"]:
                if f["filterType"] == "LOT_SIZE":
                    step_size = float(f["stepSize"])
                    break

            if step_size:
                precision = int(-np.log10(step_size))
                free_qty = np.floor(free_qty * (10**precision)) / (
                    10**precision
                )

            order = client.order_market_sell(symbol=symbol, quantity=free_qty)
            logging.info(f"SELL ORDER EXECUTED for {symbol}: {order}")
            return True
        except BinanceAPIException as e:
            logging.error(
                f"Attempt {attempt+1} - Sell API Error ({symbol}): {e}"
            )
            await asyncio.sleep(1)
        except Exception as e:
            logging.error(
                f"Attempt {attempt+1} - Unexpected error on Sell ({symbol}): {e}"
            )
            await asyncio.sleep(1)

    logging.critical(
        f"CRITICAL: Failed to sell {symbol} after {max_retries} attempts!"
    )
    return False


async def process_kline(data):
    """Processes real-time kline WebSocket stream and applies trading rules"""
    kline = data["k"]
    symbol = kline["s"]
    is_closed = kline["x"]  # Candle is closed or not

    open_price = float(kline["o"])
    close_price = float(kline["c"])
    high_price = float(kline["h"])

    if symbol not in market_data:
        return

    # Update current candle live data
    closes = market_data[symbol]["closes"]
    opens = market_data[symbol]["opens"]

    # When a candle CLOSES (5 minute completed)
    if is_closed:
        closes.append(close_price)
        opens.append(open_price)
        if len(closes) > 50:
            closes.pop(0)
            opens.pop(0)

        # Calculate Indicators on Closed Candle
        lower_band, upper_band, ema5 = calculate_indicators(closes)

        if lower_band is None:
            return

        # Buy Condition Check:
        # 1. Candle Open < Lower Band (20,2)
        # 2. Candle Close > Lower Band (20,2)
        # 3. Candle Close & High < EMA5 (EMA Line touch korbe na)
        has_position = market_data[symbol]["has_position"]

        if not has_position:
            if (
                open_price < lower_band
                and close_price > lower_band
                and high_price < ema5
            ):

                logging.info(f"BUY SIGNAL DETECTED FOR {symbol}!")
                buy_res = await execute_market_buy(symbol)
                if buy_res:
                    market_data[symbol]["has_position"] = True

    # Real-time Check for SELL (When position is open)
    # Target: Candle touches or goes above BB Upper Band
    if market_data[symbol]["has_position"]:
        # Recalculate Upper Band on real-time price
        temp_closes = closes + [close_price]
        _, upper_band, _ = calculate_indicators(temp_closes)

        if upper_band and close_price >= upper_band:
            logging.info(
                f"SELL SIGNAL (BB Upper Band Touched) FOR {symbol}!"
            )
            sell_success = await execute_market_sell(symbol)
            if sell_success:
                market_data[symbol]["has_position"] = False


async def start_websocket_streams(symbols):
    """Binance Combined WebSocket Streams (Prevents Rate Limit / IP Ban)"""
    streams = [f"{s.lower()}@kline_{TIMEFRAME}" for s in symbols]
    url = f"wss://stream.binance.com:9443/ws/{'/'.join(streams)}"

    while True:
        try:
            async with websockets.connect(url, ping_interval=20) as ws:
                logging.info("WebSocket Successfully Connected to Binance!")
                while True:
                    msg = await ws.recv()
                    data = json.loads(msg)
                    if "k" in data:
                        await process_kline(data)
        except Exception as e:
            logging.error(f"WebSocket Connection Lost: {e}. Reconnecting in 5s...")
            await asyncio.sleep(5)


async def main():
    pairs = get_top_150_usdt_pairs()

    logging.info("Pre-loading historic OHLC data safely (IP Ban protection)...")
    for symbol in pairs:
        closes, opens = load_initial_klines(symbol)
        market_data[symbol] = {
            "closes": closes,
            "opens": opens,
            "has_position": False,
        }
        # Delay to maintain zero risk of IP ban during initialization
        await asyncio.sleep(0.1)

    logging.info("Initialization completed. Starting WebSocket Engine...")
    await start_websocket_streams(pairs)


if __name__ == "__main__":
    asyncio.run(main())
