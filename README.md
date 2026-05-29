# Sports Price Edge Bot

Lightweight Python websocket bot for collecting real-time Polymarket and Kalshi sports prices.

It reuses the same manual match shape as `autotradebot.price-monitor.manual-matches` in the Java Spring config, so you can point it directly at `src/main/resources/application.yml` or use `python_bot/config.example.yml`.

## Install

```powershell
python -m venv .venv-pricebot
.\.venv-pricebot\Scripts\pip install -r python_bot\requirements.txt
```

Kalshi websocket auth uses these env vars by default:

```powershell
$env:KALSHI_ACCESS_KEY="your-key-id"
$env:KALSHI_PRIVATE_KEY_PATH="C:\path\to\kalshi-private-key.pem"
```

You can also set `KALSHI_PRIVATE_KEY` to the PEM contents.

## Run

Use the current Java config:

```powershell
.\.venv-pricebot\Scripts\python.exe python_bot\sports_price_bot.py --config src\main\resources\application.yml
```

Use only one configured match:

```powershell
.\.venv-pricebot\Scripts\python.exe python_bot\sports_price_bot.py --config src\main\resources\application.yml --match-id manual-nba-okc-sas-2026-05-28-moneyline
```

## Output

By default files are written under `data/realtime/`:

- `raw_polymarket_YYYYMMDD.jsonl` and `raw_kalshi_YYYYMMDD.jsonl`: every websocket push with `received_at_ms`, `received_at`, platform, source, and raw payload.
- `ticks_YYYYMMDD.jsonl`: normalized best bid/ask/last trade updates mapped to match/outcome.
- `edges_YYYYMMDD.jsonl`: conservative arbitrage, price-lead, and latency edge signals.

Each line is standalone JSON, suitable for replay and backtesting.
