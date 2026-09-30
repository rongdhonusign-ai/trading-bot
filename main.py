Deploying...
==> Setting WEB_CONCURRENCY=1 by default, based on available CPUs in the instance
2026-09-30 16:53:55,627 | INFO | TRADE AMOUNT = 35.00 USDT
2026-09-30 16:53:55,628 | INFO | STOP LOSS = DISABLED
2026-09-30 16:53:55,628 | INFO | TRAILING STOP = DISABLED
2026-09-30 16:53:55,628 | INFO | RSI SELL CROSS LEVEL = 80.00
2026-09-30 16:53:55,628 | INFO | Loading Binance exchange information...
2026-09-30 16:53:57,854 | INFO | Loaded 495 eligible USDT ALT symbols
2026-09-30 16:53:58,043 | INFO | Updating top ALT symbols...
2026-09-30 16:53:58,347 | INFO | Selected 150 ALT/USDT symbols
2026-09-30 16:53:58,348 | INFO | First symbols: ['SOLUSDT', 'XRPUSDT', 'ZECUSDT', 'NEARUSDT', 'QNTUSDT', 'SUIUSDT', 'USD1USDT', 'BNBUSDT', 'ENAUSDT', 'DOGEUSDT', 'AVAXUSDT', 'WLDUSDT', 'UNIUSDT', 'PUMPUSDT', 'RLUSDUSDT']
2026-09-30 16:53:58,350 | INFO | Loading initial 5m candle data for 150 symbols...
2026-09-30 16:54:12,611 | INFO | Initial candle loading complete: 150 symbols
2026-09-30 16:54:12,611 | INFO | Checking existing Binance balances...
2026-09-30 16:54:12,688 | INFO | Position recovery complete → 0 positions recovered
 * Serving Flask app 'main'
Traceback (most recent call last):
  File "/opt/render/project/src/main.py", line 2129, in <module>
    start_bot()
    ~~~~~~~~~^^
  File "/opt/render/project/src/main.py", line 2090, in start_bot
    target=websocket_loop,
           ^^^^^^^^^^^^^^
NameError: name 'websocket_loop' is not defined
 * Debug mode: off
==> Exited with status 1
==> Common ways to troubleshoot your deploy: https://render.com/docs/troubleshooting-deploys
==> Running 'python main.py'
==> Running 'python main.py'
==> Deploying...
==> Setting WEB_CONCURRENCY=1 by default, based on available CPUs in the instance
