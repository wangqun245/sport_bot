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
- `raw_polymarket_user_YYYYMMDD.jsonl`: Polymarket private user websocket messages when live trading is enabled.
- `ticks_YYYYMMDD.jsonl`: normalized best bid/ask/last trade updates mapped to match/outcome.
- `edges_YYYYMMDD.jsonl`: conservative arbitrage, price-lead, and latency edge signals.
- `orders_YYYYMMDD.jsonl`: dry-run/live order submissions and websocket confirmations.

Each line is standalone JSON, suitable for replay and backtesting.

## Dry-Run Execution

The bot can place a dry-run Polymarket buy when Polymarket YES ask is at least 5 percentage points below the matching Kalshi YES bid:

```yaml
price-monitor:
  execution:
    enabled: true
    dry-run: true
    polymarket-vs-kalshi-edge-threshold: 0.05
    sell-when-polymarket-over-kalshi: 0.01
    trade-amount-usd: 5.00
    cooldown-ms: 300000
    result-sweep-enabled: true
    result-sweep-kalshi-certainty-price: 0.99
    result-sweep-max-usd: 25.00
```

Set `TELEGRAM_BOT_TOKEN` and `TELEGRAM_CHAT_ID` to receive alerts. Live trading is off unless `dry-run: false`; live mode requires `PRIVATE_KEY` and Polymarket CLOB credentials derivable by `executor.py`. Live fills are confirmed from the Polymarket user websocket before being recorded as confirmed.

After a buy is filled, the bot tracks the Polymarket position and sells when Polymarket YES bid is at least 1 cent above the matching Kalshi YES bid. The result-sweep mode uses Kalshi websocket ticks; if a mapped Kalshi YES price reaches `0.99` while Polymarket still has an ask, it buys that Polymarket YES side up to `result-sweep-max-usd`. Set `result-sweep-max-usd: 0` only if you intentionally want live mode to use the available balance.
