# ==========================================
# 2. CONFIGURATION & COIN SELECTION (IP BAN SAFE)
# ==========================================
trade_amount_usdt = 6.0   
stop_loss_pct = 0.02  # ২% ইমার্জেন্সি স্টপ লস

trade_exchange = ccxt.binance({
    'apiKey': os.environ.get('BINANCE_API_KEY', 'yRwdwQAR1S9G8DLVeQp39lW99BAGEF4XDG6hoImJkFTol2RFvWmTvksMKy5Bav0M'),
    'secret': os.environ.get('BINANCE_SECRET_KEY', '3qsGUF6nPgfluSLPe8VXo0DE2gtR1jQIud9URVC5NHezEFp9YQV1lLqG1WncAltV'),
    'enableRateLimit': True,
    'options': {
        'defaultType': 'spot',
        'adjustForTimeDifference': True,
        'recvWindow': 10000
    }
})

# CCXT মার্কেট কল বাদ দিয়ে সরাসরি সেরা ৫০টি অল্টকয়েনের পেয়ার নির্ধারণ (Render-এ IP Ban থামাবে)
def get_target_altcoins():
    add_log("✅ Loading static top 50 Altcoins list (Bypassing Binance IP Rate-limit)...")
    return [
        'ethusdt', 'solusdt', 'bnbusdt', 'xrpusdt', 'adausdt', 'dogeusdt', 'avaxusdt', 
        'dotusdt', 'linkusdt', 'nearusdt', 'suiusdt', 'fetusdt', 'aptusdt', 'ltcusdt',
        'uniusdt', 'icpusdt', 'injusdt', 'renderusdt', 'tiausdt', 'seiusdt', 'arbusdt',
        'opusdt', 'wifusdt', 'flokiusdt', 'atomusdt', 'nearusdt', 'trxusdt', 'xlmusdt',
        'nearusdt', 'ftmusdt', 'sandusdt', 'manausdt', 'galausdt', 'algousdt', 'ldousdt',
        'qntusdt', 'aaveusdt', 'egldusdt', 'flowusdt', 'chzusdt', 'axsusdt', 'crvusdt',
        'grtusdt', 'snxusdt', 'stxusdt', 'mknusdt', 'kavausdt', 'compusdt', 'imxusdt'
    ]

target_symbols = get_target_altcoins()
