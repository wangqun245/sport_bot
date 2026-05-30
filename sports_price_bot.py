from __future__ import annotations

import argparse
import asyncio
import base64
import inspect
import json
import logging
import os
import signal
import time
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

try:
    import websockets
except ModuleNotFoundError:  # pragma: no cover - depends on local environment
    websockets = None

try:
    import yaml
except ModuleNotFoundError:  # pragma: no cover - depends on local environment
    yaml = None

try:
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import padding
except ModuleNotFoundError:  # pragma: no cover - depends on local environment
    hashes = None
    serialization = None
    padding = None


LOG = logging.getLogger("sports-price-bot")
KALSHI_WS_PATH = "/trade-api/ws/v2"


@dataclass(frozen=True)
class Contract:
    match_id: str
    platform_id: str
    event_id: str
    outcome_key: str
    outcome_name: str
    yes_instrument_id: str
    no_instrument_id: str | None = None


@dataclass
class PriceState:
    match_id: str
    platform_id: str
    outcome_key: str
    outcome_name: str
    instrument_id: str
    yes_bid: float | None = None
    yes_ask: float | None = None
    no_bid: float | None = None
    no_ask: float | None = None
    last_trade_price: float | None = None
    updated_at_ms: int = 0

    def yes_mid(self) -> float | None:
        if self.yes_bid is None or self.yes_ask is None:
            return None
        return (self.yes_bid + self.yes_ask) / 2.0


class JsonlWriter:
    def __init__(self, output_dir: Path):
        self.output_dir = output_dir
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self._locks: dict[str, asyncio.Lock] = {}

    async def append(self, name: str, record: dict[str, Any]) -> None:
        path = self.output_dir / f"{name}_{datetime.now(timezone.utc):%Y%m%d}.jsonl"
        lock = self._locks.setdefault(str(path), asyncio.Lock())
        async with lock:
            with path.open("a", encoding="utf-8") as file:
                file.write(json.dumps(record, ensure_ascii=False, separators=(",", ":")) + "\n")


class PriceBook:
    def __init__(self, contracts: list[Contract]):
        self.contracts_by_instrument: dict[str, Contract] = {}
        self.prices: dict[tuple[str, str, str], PriceState] = {}
        for contract in contracts:
            self.contracts_by_instrument[contract.yes_instrument_id] = contract
            if contract.no_instrument_id:
                self.contracts_by_instrument[contract.no_instrument_id] = contract
            key = (contract.platform_id, contract.match_id, contract.outcome_key)
            self.prices[key] = PriceState(
                match_id=contract.match_id,
                platform_id=contract.platform_id,
                outcome_key=contract.outcome_key,
                outcome_name=contract.outcome_name,
                instrument_id=contract.yes_instrument_id,
            )

    def contract_for(self, instrument_id: str) -> Contract | None:
        return self.contracts_by_instrument.get(instrument_id)

    def update(
        self,
        instrument_id: str,
        yes_bid: float | None = None,
        yes_ask: float | None = None,
        no_bid: float | None = None,
        no_ask: float | None = None,
        last_trade_price: float | None = None,
        updated_at_ms: int | None = None,
    ) -> PriceState | None:
        contract = self.contract_for(instrument_id)
        if contract is None:
            return None
        price = self.prices[(contract.platform_id, contract.match_id, contract.outcome_key)]
        if yes_bid is not None:
            price.yes_bid = yes_bid
        if yes_ask is not None:
            price.yes_ask = yes_ask
        if no_bid is not None:
            price.no_bid = no_bid
        if no_ask is not None:
            price.no_ask = no_ask
        if last_trade_price is not None:
            price.last_trade_price = last_trade_price
        price.updated_at_ms = updated_at_ms or now_ms()
        return price

    def prices_for_match(self, match_id: str) -> list[PriceState]:
        return [price for price in self.prices.values() if price.match_id == match_id]


class EdgeAnalyzer:
    def __init__(self, config: dict[str, Any], writer: JsonlWriter):
        self.writer = writer
        self.min_arbitrage_profit = float(config.get("min-arbitrage-profit", 0.02))
        self.polymarket_fee_rate = float(config.get("polymarket-sports-taker-fee-rate", 0.03))
        self.kalshi_fee_rate = float(config.get("kalshi-taker-fee-rate", 0.07))
        self.price_lead_threshold = float(config.get("price-lead-threshold", 0.03))
        self.latency_move_threshold = float(config.get("latency-move-threshold", 0.05))
        self.latency_stable_threshold = float(config.get("latency-stable-threshold", 0.01))
        self.latency_window_ms = int(config.get("latency-window-ms", 30000))
        self.last_mid: dict[tuple[str, str, str], float] = {}
        self.last_move: dict[tuple[str, str, str], tuple[int, float, float]] = {}
        self.recent_signal_keys: dict[str, int] = {}

    async def on_price(self, book: PriceBook, price: PriceState) -> None:
        await self._evaluate_arbitrage(book, price.match_id)
        await self._evaluate_price_lead(book, price.match_id)
        await self._evaluate_latency(book, price)

    async def _signal(self, signal_type: str, key: str, payload: dict[str, Any]) -> None:
        received = now_ms()
        previous = self.recent_signal_keys.get(key)
        if previous is not None and received - previous < 5_000:
            return
        self.recent_signal_keys[key] = received
        record = {
            "received_at_ms": received,
            "received_at": iso_ms(received),
            "signal_type": signal_type,
            **payload,
        }
        LOG.info("edge signal: %s", json.dumps(record, ensure_ascii=False))
        await self.writer.append("edges", record)

    async def _evaluate_arbitrage(self, book: PriceBook, match_id: str) -> None:
        prices = book.prices_for_match(match_id)
        cheapest_by_outcome: dict[str, PriceState] = {}
        for price in prices:
            if price.yes_ask is None:
                continue
            existing = cheapest_by_outcome.get(price.outcome_key)
            if existing is None or self._adjusted_cost(price.platform_id, price.yes_ask) < self._adjusted_cost(existing.platform_id, existing.yes_ask or 1):
                cheapest_by_outcome[price.outcome_key] = price
        if len(cheapest_by_outcome) >= 2 and len({p.platform_id for p in cheapest_by_outcome.values()}) > 1:
            total_cost = sum(p.yes_ask or 0 for p in cheapest_by_outcome.values())
            total_fees = sum(self._taker_fee(p.platform_id, p.yes_ask or 0) for p in cheapest_by_outcome.values())
            net_profit = 1.0 - total_cost - total_fees
            if net_profit >= self.min_arbitrage_profit:
                await self._signal(
                    "exhaustive_yes_arbitrage",
                    f"exhaustive#{match_id}#{total_cost:.3f}",
                    {
                        "match_id": match_id,
                        "total_cost": total_cost,
                        "total_fees": total_fees,
                        "net_profit": net_profit,
                        "legs": [self._leg(p, "YES", p.yes_ask) for p in cheapest_by_outcome.values()],
                    },
                )
        by_outcome: dict[str, list[PriceState]] = {}
        for price in prices:
            by_outcome.setdefault(price.outcome_key, []).append(price)
        for outcome_key, outcome_prices in by_outcome.items():
            for yes_leg in outcome_prices:
                for no_leg in outcome_prices:
                    if yes_leg.platform_id == no_leg.platform_id or yes_leg.yes_ask is None or no_leg.no_ask is None:
                        continue
                    total_cost = yes_leg.yes_ask + no_leg.no_ask
                    total_fees = self._taker_fee(yes_leg.platform_id, yes_leg.yes_ask) + self._taker_fee(no_leg.platform_id, no_leg.no_ask)
                    net_profit = 1.0 - total_cost - total_fees
                    if net_profit >= self.min_arbitrage_profit:
                        await self._signal(
                            "yes_no_arbitrage",
                            f"yesno#{match_id}#{outcome_key}#{total_cost:.3f}",
                            {
                                "match_id": match_id,
                                "outcome_key": outcome_key,
                                "total_cost": total_cost,
                                "total_fees": total_fees,
                                "net_profit": net_profit,
                                "legs": [self._leg(yes_leg, "YES", yes_leg.yes_ask), self._leg(no_leg, "NO", no_leg.no_ask)],
                            },
                        )

    async def _evaluate_price_lead(self, book: PriceBook, match_id: str) -> None:
        by_outcome: dict[str, list[PriceState]] = {}
        for price in book.prices_for_match(match_id):
            by_outcome.setdefault(price.outcome_key, []).append(price)
        for outcome_key, outcome_prices in by_outcome.items():
            for leading in outcome_prices:
                for lagging in outcome_prices:
                    if leading.platform_id == lagging.platform_id or leading.yes_bid is None or lagging.yes_ask is None:
                        continue
                    gross_edge = leading.yes_bid - lagging.yes_ask
                    total_fees = self._taker_fee(leading.platform_id, leading.yes_bid) + self._taker_fee(lagging.platform_id, lagging.yes_ask)
                    net_edge = gross_edge - total_fees
                    if gross_edge >= self.price_lead_threshold and net_edge >= self.min_arbitrage_profit:
                        await self._signal(
                            "cross_platform_price_lead",
                            f"lead#{match_id}#{outcome_key}#{leading.platform_id}#{gross_edge:.3f}",
                            {
                                "match_id": match_id,
                                "outcome_key": outcome_key,
                                "leading_platform": leading.platform_id,
                                "leading_yes_bid": leading.yes_bid,
                                "lagging_platform": lagging.platform_id,
                                "lagging_yes_ask": lagging.yes_ask,
                                "gross_edge": gross_edge,
                                "total_fees": total_fees,
                                "net_edge": net_edge,
                            },
                        )

    async def _evaluate_latency(self, book: PriceBook, price: PriceState) -> None:
        mid = price.yes_mid()
        if mid is None:
            return
        key = (price.platform_id, price.match_id, price.outcome_key)
        previous = self.last_mid.get(key)
        self.last_mid[key] = mid
        if previous is None:
            return
        move = mid - previous
        if abs(move) < self.latency_move_threshold:
            return
        at_ms = now_ms()
        self.last_move[key] = (at_ms, previous, mid)
        for other in book.prices_for_match(price.match_id):
            if other.platform_id == price.platform_id or other.outcome_key != price.outcome_key:
                continue
            other_key = (other.platform_id, other.match_id, other.outcome_key)
            other_move = self.last_move.get(other_key)
            other_stable = other_move is None or at_ms - other_move[0] > self.latency_window_ms or abs(other_move[2] - other_move[1]) <= self.latency_stable_threshold
            if not other_stable:
                continue
            trade = self._latency_trade(price, other, move)
            if trade and trade["net_edge"] >= self.min_arbitrage_profit:
                await self._signal(
                    "latency_edge",
                    f"latency#{price.match_id}#{price.outcome_key}#{price.platform_id}",
                    {
                        "match_id": price.match_id,
                        "outcome_key": price.outcome_key,
                        "leading_platform": price.platform_id,
                        "leading_mid_from": previous,
                        "leading_mid_to": mid,
                        "move": move,
                        **trade,
                    },
                )

    def _latency_trade(self, leading: PriceState, lagging: PriceState, move: float) -> dict[str, Any] | None:
        if move > 0:
            if lagging.yes_ask is None or leading.yes_bid is None:
                return None
            side = "YES"
            entry = lagging.yes_ask
            reference = leading.yes_bid
        else:
            if lagging.no_ask is None or leading.no_bid is None:
                return None
            side = "NO"
            entry = lagging.no_ask
            reference = leading.no_bid
        gross_edge = reference - entry
        total_fees = self._taker_fee(lagging.platform_id, entry) + self._taker_fee(leading.platform_id, reference)
        return {
            "side": side,
            "entry_platform": lagging.platform_id,
            "entry_price": entry,
            "reference_exit_platform": leading.platform_id,
            "reference_exit_price": reference,
            "gross_edge": gross_edge,
            "total_fees": total_fees,
            "net_edge": gross_edge - total_fees,
        }

    def _adjusted_cost(self, platform_id: str, price: float) -> float:
        return price + self._taker_fee(platform_id, price)

    def _taker_fee(self, platform_id: str, price: float) -> float:
        if platform_id == "polymarket":
            return round(self.polymarket_fee_rate * price * (1.0 - price), 5)
        if platform_id == "kalshi":
            return ceil_cents(self.kalshi_fee_rate * price * (1.0 - price))
        return 0.0

    @staticmethod
    def _leg(price: PriceState, side: str, leg_price: float | None) -> dict[str, Any]:
        return {
            "platform_id": price.platform_id,
            "outcome_key": price.outcome_key,
            "outcome_name": price.outcome_name,
            "side": side,
            "price": leg_price,
            "instrument_id": price.instrument_id,
        }


class SportsPriceBot:
    def __init__(self, config: dict[str, Any], output_dir: Path, match_id: str | None):
        self.config = config
        self.matches = load_matches(config, match_id)
        self.contracts = contracts_from_matches(self.matches)
        if not self.contracts:
            raise ValueError("No contracts found in config. Check price-monitor.manual-matches.")
        self.writer = JsonlWriter(output_dir)
        self.book = PriceBook(self.contracts)
        self.analyzer = EdgeAnalyzer(config, self.writer)
        self.stop_event = asyncio.Event()

    async def run(self) -> None:
        LOG.info("loaded %s matches and %s contracts", len(self.matches), len(self.contracts))
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, self.stop_event.set)
            except NotImplementedError:
                pass
        tasks = [
            asyncio.create_task(self._run_polymarket(), name="polymarket"),
            asyncio.create_task(self._run_kalshi(), name="kalshi"),
        ]
        await self.stop_event.wait()
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

    async def _run_polymarket(self) -> None:
        asset_ids = sorted({
            instrument
            for contract in self.contracts
            if contract.platform_id == "polymarket"
            for instrument in (contract.yes_instrument_id, contract.no_instrument_id)
            if instrument
        })
        if not asset_ids:
            LOG.warning("polymarket skipped: no asset ids")
            return
        subscribe = {"assets_ids": asset_ids, "type": "market", "custom_feature_enabled": True}
        await self._connect_loop("polymarket", self.config.get("polymarket-ws-url"), subscribe, self._handle_polymarket)

    async def _run_kalshi(self) -> None:
        tickers = sorted({contract.yes_instrument_id for contract in self.contracts if contract.platform_id == "kalshi"})
        if not tickers:
            LOG.warning("kalshi skipped: no tickers")
            return
        headers = kalshi_auth_headers(self.config)
        if headers is None:
            LOG.warning("kalshi skipped: missing auth env/private key")
            return
        subscribe = {"id": 1, "cmd": "subscribe", "params": {"channels": ["ticker"], "market_tickers": tickers}}
        await self._connect_loop("kalshi", self.config.get("kalshi-ws-url"), subscribe, self._handle_kalshi, headers)

    async def _connect_loop(self, platform: str, url: str, subscribe: dict[str, Any], handler, headers: dict[str, str] | None = None) -> None:
        if not url:
            LOG.warning("%s skipped: missing websocket url", platform)
            return
        while not self.stop_event.is_set():
            try:
                LOG.info("%s connecting to %s", platform, url)
                async with websocket_connect(url, headers) as websocket:
                    await websocket.send(json.dumps(subscribe, separators=(",", ":")))
                    LOG.info("%s subscribed", platform)
                    async for message in websocket:
                        received_ms = now_ms()
                        await self._record_raw(platform, message, received_ms)
                        await handler(message, received_ms)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                LOG.warning("%s websocket error: %s", platform, exc)
                await asyncio.sleep(5)

    async def _record_raw(self, platform: str, message: str | bytes, received_ms: int) -> None:
        text = message.decode("utf-8", errors="replace") if isinstance(message, bytes) else message
        record: dict[str, Any] = {
            "received_at_ms": received_ms,
            "received_at": iso_ms(received_ms),
            "platform_id": platform,
            "source": "websocket",
            "raw_payload": text,
        }
        try:
            record["payload"] = json.loads(text)
        except json.JSONDecodeError:
            pass
        await self.writer.append(f"raw_{platform}", record)

    async def _handle_polymarket(self, message: str | bytes, received_ms: int) -> None:
        text = message.decode("utf-8", errors="replace") if isinstance(message, bytes) else message
        try:
            payload = json.loads(text)
        except json.JSONDecodeError:
            return
        nodes = payload if isinstance(payload, list) else [payload]
        for node in nodes:
            if not isinstance(node, dict):
                continue
            event_type = node.get("event_type", "")
            if event_type == "book":
                await self._update_polymarket_asset(node.get("asset_id"), "book", best_price(node.get("bids"), True), best_price(node.get("asks"), False), None, received_ms)
            elif event_type == "best_bid_ask":
                await self._update_polymarket_asset(node.get("asset_id"), "best_bid_ask", decimal(node.get("best_bid")), decimal(node.get("best_ask")), None, received_ms)
            elif event_type == "price_change":
                for change in node.get("price_changes") or []:
                    await self._update_polymarket_asset(change.get("asset_id"), "price_change", decimal(change.get("best_bid")), decimal(change.get("best_ask")), None, received_ms)
            elif event_type == "last_trade_price":
                await self._update_polymarket_asset(node.get("asset_id"), "last_trade_price", None, None, decimal(node.get("price")), received_ms)

    async def _update_polymarket_asset(self, asset_id: Any, message_type: str, bid: float | None, ask: float | None, last: float | None, received_ms: int) -> None:
        if not isinstance(asset_id, str):
            return
        contract = self.book.contract_for(asset_id)
        if contract is None:
            return
        yes_token = asset_id == contract.yes_instrument_id
        price = self.book.update(
            asset_id,
            yes_bid=bid if yes_token else None,
            yes_ask=ask if yes_token else None,
            no_bid=None if yes_token else bid,
            no_ask=None if yes_token else ask,
            last_trade_price=last,
            updated_at_ms=received_ms,
        )
        if price:
            await self._record_tick(message_type, price, received_ms)
            await self.analyzer.on_price(self.book, price)

    async def _handle_kalshi(self, message: str | bytes, received_ms: int) -> None:
        text = message.decode("utf-8", errors="replace") if isinstance(message, bytes) else message
        try:
            payload = json.loads(text)
        except json.JSONDecodeError:
            return
        if payload.get("type") != "ticker":
            if payload.get("type") == "error":
                LOG.warning("kalshi server error: %s", text)
            return
        msg = payload.get("msg") or {}
        ticker = msg.get("market_ticker")
        if not isinstance(ticker, str):
            return
        yes_bid = decimal(first_value(msg, "yes_bid_dollars", "yes_bid"))
        yes_ask = decimal(first_value(msg, "yes_ask_dollars", "yes_ask"))
        no_bid = decimal(first_value(msg, "no_bid_dollars", "no_bid"))
        no_ask = decimal(first_value(msg, "no_ask_dollars", "no_ask"))
        if no_ask is None and yes_bid is not None:
            no_ask = 1.0 - yes_bid
        if no_bid is None and yes_ask is not None:
            no_bid = 1.0 - yes_ask
        price = self.book.update(
            ticker,
            yes_bid=yes_bid,
            yes_ask=yes_ask,
            no_bid=no_bid,
            no_ask=no_ask,
            last_trade_price=decimal(first_value(msg, "last_price_dollars", "last_price")),
            updated_at_ms=received_ms,
        )
        if price:
            await self._record_tick("ticker", price, received_ms)
            await self.analyzer.on_price(self.book, price)

    async def _record_tick(self, message_type: str, price: PriceState, received_ms: int) -> None:
        await self.writer.append(
            "ticks",
            {
                "received_at_ms": received_ms,
                "received_at": iso_ms(received_ms),
                "platform_id": price.platform_id,
                "message_type": message_type,
                "match_id": price.match_id,
                "outcome_key": price.outcome_key,
                "outcome_name": price.outcome_name,
                "instrument_id": price.instrument_id,
                "yes_bid": price.yes_bid,
                "yes_ask": price.yes_ask,
                "no_bid": price.no_bid,
                "no_ask": price.no_ask,
                "last_trade_price": price.last_trade_price,
                "updated_at_ms": price.updated_at_ms,
            },
        )


def websocket_connect(url: str, headers: dict[str, str] | None):
    if websockets is None:
        raise RuntimeError("Missing dependency websockets. Install with: pip install -r python_bot/requirements.txt")
    if "additional_headers" in inspect.signature(websockets.connect).parameters:
        return websockets.connect(url, additional_headers=headers)
    return websockets.connect(url, extra_headers=headers)


def load_config(path: Path) -> dict[str, Any]:
    if yaml is None:
        raise RuntimeError("Missing dependency PyYAML. Install with: pip install -r python_bot/requirements.txt")
    if not path.exists():
        for ext in (".yml", ".yaml"):
            alt_path = path.parent / (path.name + ext)
            if alt_path.exists():
                path = alt_path
                break
    with path.open("r", encoding="utf-8") as file:
        loaded = yaml.safe_load(file) or {}
    return loaded.get("autotradebot", {}).get("price-monitor") or loaded.get("price-monitor") or {}


def load_matches(config: dict[str, Any], match_id: str | None) -> list[dict[str, Any]]:
    matches = list(config.get("manual-matches") or [])
    if match_id:
        matches = [match for match in matches if match.get("match-id") == match_id]
    return matches


def contracts_from_matches(matches: list[dict[str, Any]]) -> list[Contract]:
    contracts: list[Contract] = []
    for match in matches:
        match_id = match.get("match-id")
        if not match_id:
            continue
        for platform_id, event_key, contracts_key in (
            ("polymarket", "polymarket-event-id", "polymarket-contracts"),
            ("kalshi", "kalshi-event-id", "kalshi-contracts"),
        ):
            for item in match.get(contracts_key) or []:
                outcome_key = item.get("outcome-key")
                yes_id = item.get("yes-instrument-id")
                if not outcome_key or not yes_id:
                    continue
                contracts.append(
                    Contract(
                        match_id=match_id,
                        platform_id=platform_id,
                        event_id=match.get(event_key) or "",
                        outcome_key=outcome_key,
                        outcome_name=item.get("outcome-name") or outcome_key,
                        yes_instrument_id=str(yes_id),
                        no_instrument_id=str(item["no-instrument-id"]) if item.get("no-instrument-id") else None,
                    )
                )
    return contracts


def kalshi_auth_headers(config: dict[str, Any]) -> dict[str, str] | None:
    key_env = config.get("kalshi-access-key-env", "KALSHI_ACCESS_KEY")
    private_key_env = config.get("kalshi-private-key-env", "KALSHI_PRIVATE_KEY")
    private_key_path_env = config.get("kalshi-private-key-path-env", "KALSHI_PRIVATE_KEY_PATH")
    key_id = os.getenv(key_env)
    private_key_pem = os.getenv(private_key_env)
    if private_key_pem:
        private_key_pem = private_key_pem.replace("\\n", "\n")
    else:
        private_key_path = os.getenv(private_key_path_env)
        if private_key_path:
            private_key_pem = Path(private_key_path).read_text(encoding="utf-8")
    if not key_id or not private_key_pem:
        return None
    if serialization is None or padding is None or hashes is None:
        raise RuntimeError("Missing dependency cryptography. Install with: pip install -r python_bot/requirements.txt")
    timestamp = str(now_ms())
    payload = f"{timestamp}GET{KALSHI_WS_PATH}".encode("utf-8")
    private_key = serialization.load_pem_private_key(private_key_pem.encode("utf-8"), password=None)
    signature = private_key.sign(
        payload,
        padding.PSS(mgf=padding.MGF1(hashes.SHA256()), salt_length=32),
        hashes.SHA256(),
    )
    return {
        "KALSHI-ACCESS-KEY": key_id,
        "KALSHI-ACCESS-TIMESTAMP": timestamp,
        "KALSHI-ACCESS-SIGNATURE": base64.b64encode(signature).decode("ascii"),
    }


def decimal(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed / 100.0 if parsed > 1.0 else parsed


def first_value(values: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        value = values.get(key)
        if value is not None and value != "":
            return value
    return None


def best_price(levels: Any, bid: bool) -> float | None:
    if not isinstance(levels, list):
        return None
    prices = [decimal(level.get("price")) for level in levels if isinstance(level, dict)]
    prices = [price for price in prices if price is not None]
    if not prices:
        return None
    return max(prices) if bid else min(prices)


def ceil_cents(value: float) -> float:
    if value <= 0:
        return 0.0
    return int(value * 100 + 0.999999999) / 100.0


def now_ms() -> int:
    return time.time_ns() // 1_000_000


def find_matching_kalshi_market(pm_outcome: str, kalshi_markets: list[dict]) -> dict | None:
    pm_clean = pm_outcome.lower().replace(".", "").replace(",", "")
    pm_words = set(pm_clean.split())
    
    best_market = None
    best_score = -1
    
    for market in kalshi_markets:
        title = (market.get("title") or "").lower()
        subtitle = (market.get("subtitle") or "").lower()
        ticker = (market.get("ticker") or "").lower()
        
        if pm_clean in title or pm_clean in subtitle or pm_clean in ticker:
            return market
        
        market_text = f"{title} {subtitle} {ticker}"
        market_words = set(market_text.replace(".", "").replace(",", "").split())
        overlap = len(pm_words.intersection(market_words))
        if overlap > best_score:
            best_score = overlap
            best_market = market
            
    return best_market


async def resolve_match_from_urls(pm_url: str, kalshi_url: str) -> dict[str, Any]:
    pm_url_clean = pm_url.split("?")[0].rstrip("/")
    pm_slug = pm_url_clean.split("/")[-1]
    
    pm_api_url = f"https://gamma-api.polymarket.com/markets?slug={pm_slug}"
    loop = asyncio.get_event_loop()
    
    def fetch_pm():
        req = urllib.request.Request(pm_api_url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req) as res:
            return json.loads(res.read().decode("utf-8"))
            
    pm_data = await loop.run_in_executor(None, fetch_pm)
    if not pm_data or not isinstance(pm_data, list):
        raise ValueError(f"Could not find Polymarket market details for slug: {pm_slug}")
        
    market_data = pm_data[0]
    pm_event_id = f"polymarket#{pm_slug}"
    
    pm_outcomes = market_data.get("outcomes")
    if isinstance(pm_outcomes, str):
        pm_outcomes = json.loads(pm_outcomes)
    pm_tokens = market_data.get("clobTokenIds")
    if isinstance(pm_tokens, str):
        pm_tokens = json.loads(pm_tokens)
        
    if not pm_outcomes or not pm_tokens or len(pm_outcomes) != len(pm_tokens):
        raise ValueError("Invalid outcome or token structure in Polymarket response")
        
    kalshi_url_clean = kalshi_url.split("?")[0].rstrip("/")
    kalshi_event_ticker = kalshi_url_clean.split("/")[-1].upper()
    kalshi_event_id = f"kalshi#{kalshi_event_ticker}"
    
    kalshi_api_url = f"https://api.kalshi.com/trade-api/v2/markets?event_ticker={kalshi_event_ticker}"
    
    def fetch_kalshi():
        req = urllib.request.Request(kalshi_api_url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req) as res:
            return json.loads(res.read().decode("utf-8"))
            
    kalshi_data = await loop.run_in_executor(None, fetch_kalshi)
    kalshi_markets = kalshi_data.get("markets") or []
    if not kalshi_markets:
        raise ValueError(f"No markets found for Kalshi event ticker: {kalshi_event_ticker}")
        
    pm_contracts = []
    kalshi_contracts = []
    
    for i, pm_outcome in enumerate(pm_outcomes):
        outcome_key = pm_outcome.lower().replace(" ", "-")
        matching_market = find_matching_kalshi_market(pm_outcome, kalshi_markets)
        if not matching_market:
            if i < len(kalshi_markets):
                matching_market = kalshi_markets[i]
            else:
                continue
                
        pm_contracts.append({
            "outcome-key": outcome_key,
            "outcome-name": pm_outcome,
            "yes-instrument-id": pm_tokens[i]
        })
        
        kalshi_contracts.append({
            "outcome-key": outcome_key,
            "outcome-name": pm_outcome,
            "yes-instrument-id": matching_market["ticker"]
        })
        
    match_id = f"dynamic-{pm_slug}"
    return {
        "match-id": match_id,
        "name": market_data.get("question") or f"{pm_slug} Moneyline",
        "polymarket-event-id": pm_event_id,
        "kalshi-event-id": kalshi_event_id,
        "polymarket-url": pm_url,
        "kalshi-url": kalshi_url,
        "polymarket-contracts": pm_contracts,
        "kalshi-contracts": kalshi_contracts
    }


async def resolve_config_matches(config: dict[str, Any]) -> None:
    matches = config.get("manual-matches") or []
    for match in matches:
        if not match.get("polymarket-contracts") or not match.get("kalshi-contracts"):
            pm_url = match.get("polymarket-url")
            kalshi_url = match.get("kalshi-url")
            if pm_url and kalshi_url:
                LOG.info("Dynamically resolving contracts for match: %s", match.get("name") or match.get("match-id"))
                try:
                    resolved = await resolve_match_from_urls(pm_url, kalshi_url)
                    match.update(resolved)
                except Exception as e:
                    LOG.error("Failed to resolve contracts for %s: %s", pm_url, e)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Collect Polymarket/Kalshi sports websocket prices and detect edge.")
    parser.add_argument("--config", type=Path, default=Path("src/main/resources/application.yml"))
    parser.add_argument("--output-dir", type=Path, default=Path("data/realtime"))
    parser.add_argument("--match-id", help="Only monitor one configured manual match id.")
    parser.add_argument("--polymarket-url", help="Polymarket market URL to monitor dynamically.")
    parser.add_argument("--kalshi-url", help="Kalshi market URL to monitor dynamically.")
    parser.add_argument("--log-level", default="INFO")
    return parser.parse_args()


async def async_main() -> None:
    args = parse_args()
    logging.basicConfig(level=getattr(logging, args.log_level.upper(), logging.INFO), format="%(asctime)s %(levelname)s %(name)s %(message)s")
    
    if args.polymarket_url and args.kalshi_url:
        config = {
            "polymarket-ws-url": "wss://ws-subscriptions-clob.polymarket.com/ws/market",
            "kalshi-ws-url": "wss://api.elections.kalshi.com/trade-api/ws/v2",
        }
        try:
            default_config = load_config(args.config)
            config.update(default_config)
        except Exception:
            pass
        
        LOG.info("Dynamically resolving match from command line URLs...")
        resolved_match = await resolve_match_from_urls(args.polymarket_url, args.kalshi_url)
        config["manual-matches"] = [resolved_match]
        args.match_id = resolved_match["match-id"]
        bot = SportsPriceBot(config, args.output_dir, args.match_id)
    else:
        config = load_config(args.config)
        await resolve_config_matches(config)
        bot = SportsPriceBot(config, args.output_dir, args.match_id)
        
    await bot.run()


def main() -> None:
    asyncio.run(async_main())


if __name__ == "__main__":
    main()
