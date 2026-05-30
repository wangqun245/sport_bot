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
from urllib.parse import parse_qs, quote, urlparse
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
POLYMARKET_USER_WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/user"


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


@dataclass
class PendingBuy:
    side: str
    order_id: str
    match_id: str
    outcome_key: str
    outcome_name: str
    token_id: str
    price: float
    shares: float
    amount_usd: float
    edge: float
    polymarket_ask: float
    kalshi_bid: float
    dry_run: bool
    created_at_ms: int
    status: str = "PENDING"


@dataclass
class Position:
    match_id: str
    outcome_key: str
    outcome_name: str
    token_id: str
    shares: float
    avg_price: float
    amount_usd: float
    entry_edge: float
    dry_run: bool
    opened_at_ms: int


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

    def prices_for_outcome(self, match_id: str, outcome_key: str) -> list[PriceState]:
        return [
            price for price in self.prices.values()
            if price.match_id == match_id and price.outcome_key == outcome_key
        ]


class TradeManager:
    def __init__(self, config: dict[str, Any], writer: JsonlWriter, contracts: list[Contract]):
        execution = config.get("execution") or {}
        self.enabled = bool(execution.get("enabled", False))
        self.dry_run = bool(execution.get("dry-run", True))
        self.edge_threshold = float(execution.get("polymarket-vs-kalshi-edge-threshold", 0.05))
        self.exit_edge_threshold = float(execution.get("sell-when-polymarket-over-kalshi", 0.01))
        self.trade_amount_usd = float(execution.get("trade-amount-usd", 5.0))
        self.cooldown_ms = int(execution.get("cooldown-ms", 300_000))
        self.max_open_orders = int(execution.get("max-open-orders", 1))
        self.result_sweep_enabled = bool(execution.get("result-sweep-enabled", True))
        self.result_sweep_certainty_price = float(execution.get("result-sweep-kalshi-certainty-price", 0.99))
        self.result_sweep_max_usd = float(execution.get("result-sweep-max-usd", 25.0))
        self.user_ws_url = execution.get("polymarket-user-ws-url") or POLYMARKET_USER_WS_URL
        self.private_key_env = execution.get("polymarket-private-key-env", "PRIVATE_KEY")
        self.safe_address_env = execution.get("polymarket-safe-address-env", "SAFE_ADDRESS")
        self.funder_address_env = execution.get("polymarket-funder-address-env", "FUNDER_ADDRESS")
        self.signature_type = int(execution.get("polymarket-signature-type", os.getenv("SIGNATURE_TYPE", "0")))
        self.writer = writer
        self.contracts_by_platform_outcome = {
            (contract.platform_id, contract.match_id, contract.outcome_key): contract
            for contract in contracts
        }
        self.pending_by_order_id: dict[str, PendingBuy] = {}
        self.positions: dict[tuple[str, str], Position] = {}
        self.last_signal_at: dict[tuple[str, str], int] = {}
        self.result_sweep_done: set[tuple[str, str]] = set()
        self.executor = None
        self.telegram = self._load_telegram()
        self._loop: asyncio.AbstractEventLoop | None = None
        self._ws_ready = asyncio.Event()

    def _load_telegram(self):
        try:
            from telegram_notifier import TelegramNotifier
            return TelegramNotifier()
        except Exception as exc:
            LOG.warning("telegram notifier disabled: %s", exc)
            return None

    def initialize(self) -> None:
        if not self.enabled:
            return
        if self.dry_run:
            LOG.info(
                "trade manager enabled in DRY RUN mode threshold=%.3f amount=%.2f",
                self.edge_threshold,
                self.trade_amount_usd,
            )
            self._notify(
                "*Sports price bot started*\n"
                "Mode: *DRY RUN*\n"
                f"Trigger: Polymarket ask at least {self.edge_threshold * 100:.1f}% below Kalshi bid\n"
                f"Exit: Polymarket bid at least {self.exit_edge_threshold * 100:.1f}% above Kalshi bid\n"
                f"Dry-run amount: ${self.trade_amount_usd:.2f}"
            )
            return
        try:
            from executor import Executor
        except Exception as exc:
            raise RuntimeError(
                "Live execution requires executor.py dependencies. Install py-clob-client-v2 and python-dotenv."
            ) from exc
        self.executor = Executor(
            private_key=os.getenv(self.private_key_env, ""),
            safe_address=os.getenv(self.safe_address_env, ""),
            dry_run=False,
            signature_type=self.signature_type,
            funder_address=os.getenv(self.funder_address_env, ""),
        )
        if not self.executor.initialize():
            raise RuntimeError("Polymarket executor initialization failed; live trading disabled.")
        self._notify(
            "*Sports price bot started*\n"
            "Mode: *LIVE*\n"
            f"Trigger: Polymarket ask at least {self.edge_threshold * 100:.1f}% below Kalshi bid\n"
            f"Exit: Polymarket bid at least {self.exit_edge_threshold * 100:.1f}% above Kalshi bid\n"
            f"Order amount: ${self.trade_amount_usd:.2f}"
        )

    async def start_user_ws(self, stop_event: asyncio.Event) -> None:
        if not self.enabled or self.dry_run:
            return
        if self.executor is None:
            LOG.warning("polymarket user ws skipped: executor not initialized")
            return
        creds = self.executor.get_api_creds()
        if creds is None:
            LOG.warning("polymarket user ws skipped: missing CLOB api creds")
            return
        auth = {
            "apiKey": getattr(creds, "api_key", None) or getattr(creds, "key", None),
            "secret": getattr(creds, "api_secret", None) or getattr(creds, "secret", None),
            "passphrase": getattr(creds, "api_passphrase", None) or getattr(creds, "passphrase", None),
        }
        if not all(auth.values()):
            LOG.warning("polymarket user ws skipped: incomplete CLOB api creds")
            return
        while not stop_event.is_set():
            try:
                LOG.info("polymarket user ws connecting")
                async with websocket_connect(self.user_ws_url, None) as websocket:
                    await websocket.send(json.dumps({"auth": auth, "type": "user"}, separators=(",", ":")))
                    self._ws_ready.set()
                    LOG.info("polymarket user ws subscribed")
                    async for message in websocket:
                        received_ms = now_ms()
                        await self._record_user_raw(message, received_ms)
                        await self.handle_user_message(message, received_ms)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self._ws_ready.clear()
                LOG.warning("polymarket user ws error: %s", exc)
                await asyncio.sleep(5)

    async def _record_user_raw(self, message: str | bytes, received_ms: int) -> None:
        text = message.decode("utf-8", errors="replace") if isinstance(message, bytes) else message
        record = {
            "received_at_ms": received_ms,
            "received_at": iso_ms(received_ms),
            "platform_id": "polymarket",
            "source": "user_websocket",
            "raw_payload": text,
        }
        try:
            record["payload"] = json.loads(text)
        except json.JSONDecodeError:
            pass
        await self.writer.append("raw_polymarket_user", record)

    async def evaluate(self, book: PriceBook, updated_price: PriceState) -> None:
        if not self.enabled or updated_price.platform_id not in {"polymarket", "kalshi"}:
            return
        await self._maybe_sweep_on_kalshi_ws_result(book, updated_price)
        await self._maybe_sell_polymarket(book, updated_price.match_id, updated_price.outcome_key)
        if len(self.pending_by_order_id) >= self.max_open_orders:
            return
        await self._maybe_buy_polymarket(book, updated_price.match_id, updated_price.outcome_key)

    async def _maybe_buy_polymarket(self, book: PriceBook, match_id: str, outcome_key: str) -> None:
        if (match_id, outcome_key) in self.positions:
            return
        polymarket = None
        kalshi = None
        for price in book.prices_for_outcome(match_id, outcome_key):
            if price.platform_id == "polymarket":
                polymarket = price
            elif price.platform_id == "kalshi":
                kalshi = price
        if polymarket is None or kalshi is None or polymarket.yes_ask is None or kalshi.yes_bid is None:
            return
        edge = kalshi.yes_bid - polymarket.yes_ask
        if edge < self.edge_threshold:
            return
        signal_key = (match_id, outcome_key)
        at_ms = now_ms()
        if at_ms - self.last_signal_at.get(signal_key, 0) < self.cooldown_ms:
            return
        self.last_signal_at[signal_key] = at_ms
        contract = self.contracts_by_platform_outcome.get(("polymarket", match_id, outcome_key))
        if contract is None:
            LOG.warning("trade skipped: no polymarket contract for %s %s", match_id, outcome_key)
            return
        await self._submit_buy(contract, polymarket, kalshi, edge, at_ms, self.trade_amount_usd, "edge_entry")

    async def _maybe_sell_polymarket(self, book: PriceBook, match_id: str, outcome_key: str) -> None:
        position = self.positions.get((match_id, outcome_key))
        if position is None:
            return
        polymarket = None
        kalshi = None
        for price in book.prices_for_outcome(match_id, outcome_key):
            if price.platform_id == "polymarket":
                polymarket = price
            elif price.platform_id == "kalshi":
                kalshi = price
        if polymarket is None or kalshi is None or polymarket.yes_bid is None or kalshi.yes_bid is None:
            return
        exit_edge = polymarket.yes_bid - kalshi.yes_bid
        if exit_edge < self.exit_edge_threshold:
            return
        if len(self.pending_by_order_id) >= self.max_open_orders:
            return
        await self._submit_sell(position, polymarket.yes_bid, kalshi.yes_bid, exit_edge, "polymarket_over_kalshi")

    async def _submit_buy(self, contract: Contract, polymarket: PriceState, kalshi: PriceState, edge: float, at_ms: int, amount_usd: float, reason: str) -> None:
        order_price = round(float(polymarket.yes_ask), 2)
        if self.dry_run:
            shares = amount_usd / order_price if order_price > 0 else 0.0
            pending = PendingBuy(
                side="BUY",
                order_id=f"DRY-{at_ms}",
                match_id=contract.match_id,
                outcome_key=contract.outcome_key,
                outcome_name=contract.outcome_name,
                token_id=contract.yes_instrument_id,
                price=order_price,
                shares=shares,
                amount_usd=amount_usd,
                edge=edge,
                polymarket_ask=polymarket.yes_ask,
                kalshi_bid=kalshi.yes_bid,
                dry_run=True,
                created_at_ms=at_ms,
                status="FILLED",
            )
            self._add_position_from_buy(pending)
            await self._record_order("dry_run_buy", pending, {"status": "FILLED", "reason": reason})
            self._notify_buy(pending, "DRY RUN BUY")
            return
        if not self._ws_ready.is_set():
            LOG.warning("live buy skipped: polymarket user websocket is not ready for order confirmation")
            self._notify(
                "*LIVE BUY SKIPPED*\n"
                "Reason: Polymarket user websocket is not ready for confirmation\n"
                f"Match: `{contract.match_id}`\n"
                f"Outcome: *{contract.outcome_name}*\n"
                f"Edge: {edge * 100:.2f}%"
            )
            return
        result = await asyncio.to_thread(
            self.executor.place_buy_order,
            contract.yes_instrument_id,
            amount_usd,
            order_price,
        )
        if not result.success:
            await self.writer.append("orders", {
                "received_at_ms": now_ms(),
                "received_at": iso_ms(now_ms()),
                "event": "buy_rejected",
                "match_id": contract.match_id,
                "outcome_key": contract.outcome_key,
                "price": order_price,
                "edge": edge,
                "reason": reason,
                "error": result.error,
            })
            self._notify(
                "*LIVE BUY REJECTED*\n"
                f"Match: `{contract.match_id}`\n"
                f"Outcome: *{contract.outcome_name}*\n"
                f"Price: ${order_price:.2f}\n"
                f"Edge: {edge * 100:.2f}%\n"
                f"Error: `{result.error[:180]}`"
            )
            return
        pending = PendingBuy(
            side="BUY",
            order_id=result.order_id,
            match_id=contract.match_id,
            outcome_key=contract.outcome_key,
            outcome_name=contract.outcome_name,
            token_id=contract.yes_instrument_id,
            price=result.price or order_price,
            shares=result.shares,
            amount_usd=result.amount_usd,
            edge=edge,
            polymarket_ask=polymarket.yes_ask,
            kalshi_bid=kalshi.yes_bid,
            dry_run=False,
            created_at_ms=at_ms,
        )
        self.pending_by_order_id[pending.order_id] = pending
        await self._record_order("live_buy_submitted", pending, {"status": result.status, "reason": reason})
        self._notify_buy(pending, "LIVE BUY SUBMITTED")

    async def _submit_sell(self, position: Position, polymarket_bid: float, kalshi_bid: float, exit_edge: float, reason: str) -> None:
        sell_price = round(float(polymarket_bid), 2)
        at_ms = now_ms()
        pending = PendingBuy(
            side="SELL",
            order_id=f"DRY-SELL-{at_ms}" if self.dry_run else "",
            match_id=position.match_id,
            outcome_key=position.outcome_key,
            outcome_name=position.outcome_name,
            token_id=position.token_id,
            price=sell_price,
            shares=position.shares,
            amount_usd=position.shares * sell_price,
            edge=exit_edge,
            polymarket_ask=sell_price,
            kalshi_bid=kalshi_bid,
            dry_run=self.dry_run,
            created_at_ms=at_ms,
            status="FILLED" if self.dry_run else "PENDING",
        )
        if self.dry_run:
            self.positions.pop((position.match_id, position.outcome_key), None)
            await self._record_order("dry_run_sell", pending, {
                "status": "FILLED",
                "reason": reason,
                "entry_price": position.avg_price,
                "estimated_pnl": (sell_price - position.avg_price) * position.shares,
            })
            self._notify_sell(pending, "DRY RUN SELL", position.avg_price)
            return
        if not self._ws_ready.is_set():
            LOG.warning("live sell skipped: polymarket user websocket is not ready for order confirmation")
            return
        result = await asyncio.to_thread(
            self.executor.place_sell_order,
            position.token_id,
            position.shares,
            sell_price,
        )
        if not result.success:
            await self.writer.append("orders", {
                "received_at_ms": now_ms(),
                "received_at": iso_ms(now_ms()),
                "event": "sell_rejected",
                "match_id": position.match_id,
                "outcome_key": position.outcome_key,
                "price": sell_price,
                "edge": exit_edge,
                "reason": reason,
                "error": result.error,
            })
            self._notify(
                "*LIVE SELL REJECTED*\n"
                f"Match: `{position.match_id}`\n"
                f"Outcome: *{position.outcome_name}*\n"
                f"Price: ${sell_price:.2f}\n"
                f"Exit edge: {exit_edge * 100:.2f}%\n"
                f"Error: `{result.error[:180]}`"
            )
            return
        pending.order_id = result.order_id
        pending.price = result.price or sell_price
        pending.shares = result.shares or position.shares
        pending.amount_usd = result.amount_usd or pending.shares * pending.price
        self.pending_by_order_id[pending.order_id] = pending
        await self._record_order("live_sell_submitted", pending, {
            "status": result.status,
            "reason": reason,
            "entry_price": position.avg_price,
        })
        self._notify_sell(pending, "LIVE SELL SUBMITTED", position.avg_price)

    async def _maybe_sweep_on_kalshi_ws_result(self, book: PriceBook, updated_price: PriceState) -> None:
        if not self.result_sweep_enabled or updated_price.platform_id != "kalshi":
            return
        resolved_outcome_key = self._resolved_outcome_from_kalshi_tick(book, updated_price)
        if resolved_outcome_key is None:
            return
        await self._sweep_polymarket_resolved_outcome(book, updated_price.match_id, resolved_outcome_key)

    def _resolved_outcome_from_kalshi_tick(self, book: PriceBook, updated_price: PriceState) -> str | None:
        yes_prices = [updated_price.yes_bid, updated_price.yes_ask, updated_price.last_trade_price]
        if any(price is not None and price >= self.result_sweep_certainty_price for price in yes_prices):
            return updated_price.outcome_key
        no_prices = [updated_price.no_bid, updated_price.no_ask]
        if not any(price is not None and price >= self.result_sweep_certainty_price for price in no_prices):
            return None
        other_outcomes = sorted({
            price.outcome_key
            for price in book.prices_for_match(updated_price.match_id)
            if price.platform_id == "kalshi" and price.outcome_key != updated_price.outcome_key
        })
        return other_outcomes[0] if len(other_outcomes) == 1 else None

    async def _sweep_polymarket_resolved_outcome(self, book: PriceBook, match_id: str, outcome_key: str) -> None:
        if (match_id, outcome_key) in self.result_sweep_done:
            return
        if len(self.pending_by_order_id) >= self.max_open_orders:
            return
        polymarket_contract = self.contracts_by_platform_outcome.get(("polymarket", match_id, outcome_key))
        kalshi_contract = self.contracts_by_platform_outcome.get(("kalshi", match_id, outcome_key))
        if polymarket_contract is None or kalshi_contract is None:
            return
        polymarket_price = next((p for p in book.prices_for_outcome(match_id, outcome_key) if p.platform_id == "polymarket"), None)
        if polymarket_price is None or polymarket_price.yes_ask is None:
            return
        if polymarket_price.yes_ask >= 0.995:
            return
        amount = await self._result_sweep_amount()
        if amount <= 0:
            return
        synthetic_kalshi = PriceState(
            match_id=match_id,
            platform_id="kalshi",
            outcome_key=outcome_key,
            outcome_name=kalshi_contract.outcome_name,
            instrument_id=kalshi_contract.yes_instrument_id,
            yes_bid=1.0,
            yes_ask=1.0,
            updated_at_ms=now_ms(),
        )
        edge = 1.0 - polymarket_price.yes_ask
        self.result_sweep_done.add((match_id, outcome_key))
        await self._submit_buy(
            polymarket_contract,
            polymarket_price,
            synthetic_kalshi,
            edge,
            now_ms(),
            amount,
            "kalshi_ws_0.99_result_sweep",
        )

    async def _result_sweep_amount(self) -> float:
        if self.dry_run:
            return self.result_sweep_max_usd if self.result_sweep_max_usd > 0 else self.trade_amount_usd
        if self.executor is None:
            return 0.0
        balance = await asyncio.to_thread(self.executor.get_balance, True)
        if self.result_sweep_max_usd > 0:
            return min(balance, self.result_sweep_max_usd)
        return balance

    async def handle_user_message(self, message: str | bytes, received_ms: int) -> None:
        text = message.decode("utf-8", errors="replace") if isinstance(message, bytes) else message
        try:
            payload = json.loads(text)
        except json.JSONDecodeError:
            return
        messages = payload if isinstance(payload, list) else [payload]
        for msg in messages:
            if not isinstance(msg, dict):
                continue
            order_id, fill_price, fill_shares = self._matched_pending_order(msg)
            if not order_id:
                continue
            pending = self.pending_by_order_id.pop(order_id, None)
            if pending is None:
                continue
            if fill_price is not None:
                pending.price = fill_price
            if fill_shares is not None:
                pending.shares = fill_shares
            pending.amount_usd = pending.price * pending.shares
            pending.status = "CONFIRMED"
            if pending.side == "BUY":
                self._add_position_from_buy(pending)
                await self._record_order("live_buy_confirmed", pending, {"user_ws_message": msg})
                self._notify_buy(pending, "LIVE BUY CONFIRMED")
            else:
                position = self.positions.pop((pending.match_id, pending.outcome_key), None)
                await self._record_order("live_sell_confirmed", pending, {
                    "user_ws_message": msg,
                    "entry_price": position.avg_price if position else None,
                    "estimated_pnl": ((pending.price - position.avg_price) * pending.shares) if position else None,
                })
                self._notify_sell(pending, "LIVE SELL CONFIRMED", position.avg_price if position else 0.0)

    def _matched_pending_order(self, msg: dict[str, Any]) -> tuple[str | None, float | None, float | None]:
        event_type = str(msg.get("event_type") or "").lower()
        status = str(msg.get("status") or msg.get("type") or "").upper()
        if event_type == "trade" or status in {"MATCHED", "TRADE"}:
            taker_order_id = str(msg.get("taker_order_id") or "")
            if taker_order_id in self.pending_by_order_id:
                return taker_order_id, decimal(msg.get("price")), number(msg.get("size"))
            for maker in msg.get("maker_orders") or []:
                order_id = str(maker.get("order_id") or "")
                if order_id in self.pending_by_order_id:
                    return order_id, decimal(maker.get("price") or msg.get("price")), number(maker.get("matched_amount") or msg.get("size"))
        if event_type == "order":
            order_id = str(msg.get("id") or "")
            size_matched = number(msg.get("size_matched"))
            if order_id in self.pending_by_order_id and size_matched:
                return order_id, decimal(msg.get("price")), size_matched
        return None, None, None

    def _add_position_from_buy(self, pending: PendingBuy) -> None:
        key = (pending.match_id, pending.outcome_key)
        existing = self.positions.get(key)
        if existing is None:
            self.positions[key] = Position(
                match_id=pending.match_id,
                outcome_key=pending.outcome_key,
                outcome_name=pending.outcome_name,
                token_id=pending.token_id,
                shares=pending.shares,
                avg_price=pending.price,
                amount_usd=pending.amount_usd,
                entry_edge=pending.edge,
                dry_run=pending.dry_run,
                opened_at_ms=now_ms(),
            )
            return
        total_shares = existing.shares + pending.shares
        if total_shares <= 0:
            return
        existing.avg_price = ((existing.avg_price * existing.shares) + (pending.price * pending.shares)) / total_shares
        existing.shares = total_shares
        existing.amount_usd += pending.amount_usd

    async def _record_order(self, event: str, pending: PendingBuy, extra: dict[str, Any]) -> None:
        received_ms = now_ms()
        await self.writer.append("orders", {
            "received_at_ms": received_ms,
            "received_at": iso_ms(received_ms),
            "event": event,
            "order_id": pending.order_id,
            "match_id": pending.match_id,
            "outcome_key": pending.outcome_key,
            "outcome_name": pending.outcome_name,
            "token_id": pending.token_id,
            "price": pending.price,
            "shares": pending.shares,
            "amount_usd": pending.amount_usd,
            "edge": pending.edge,
            "polymarket_ask": pending.polymarket_ask,
            "kalshi_bid": pending.kalshi_bid,
            "dry_run": pending.dry_run,
            "status": pending.status,
            **extra,
        })

    def _notify_buy(self, pending: PendingBuy, title: str) -> None:
        self._notify(
            f"*{title}*\n"
            f"Match: `{pending.match_id}`\n"
            f"Outcome: *{pending.outcome_name}*\n"
            f"Polymarket YES ask: ${pending.polymarket_ask:.3f}\n"
            f"Kalshi YES bid: ${pending.kalshi_bid:.3f}\n"
            f"Edge: *{pending.edge * 100:.2f}%*\n"
            f"Buy: ${pending.amount_usd:.2f} @ ${pending.price:.2f}\n"
            f"Shares: {pending.shares:.2f}\n"
            f"Order: `{pending.order_id}`"
        )

    def _notify_sell(self, pending: PendingBuy, title: str, entry_price: float) -> None:
        estimated_pnl = (pending.price - entry_price) * pending.shares if entry_price else 0.0
        self._notify(
            f"*{title}*\n"
            f"Match: `{pending.match_id}`\n"
            f"Outcome: *{pending.outcome_name}*\n"
            f"Sell price: ${pending.price:.2f}\n"
            f"Entry price: ${entry_price:.2f}\n"
            f"Kalshi YES bid: ${pending.kalshi_bid:.3f}\n"
            f"Exit edge: *{pending.edge * 100:.2f}%*\n"
            f"Shares: {pending.shares:.2f}\n"
            f"Estimated P&L: ${estimated_pnl:+.2f}\n"
            f"Order: `{pending.order_id}`"
        )

    def _notify(self, message: str) -> None:
        if self.telegram is not None:
            self.telegram.send(message)
        else:
            LOG.info("telegram disabled; notification=%s", message)


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
            details = []
            for match in self.matches:
                details.append(
                    "%s pm_url=%s kalshi_url=%s error=%s" % (
                        match.get("match-id") or "<missing match-id>",
                        match.get("polymarket-url") or "",
                        match.get("kalshi-url") or "",
                        match.get("_resolve_error") or "",
                    )
                )
            raise ValueError(
                "No contracts found in config. URL auto-resolution did not produce "
                "polymarket-contracts/kalshi-contracts. Details: " + "; ".join(details)
            )
        self.writer = JsonlWriter(output_dir)
        self.book = PriceBook(self.contracts)
        self.analyzer = EdgeAnalyzer(config, self.writer)
        self.trade_manager = TradeManager(config, self.writer, self.contracts)
        self.stop_event = asyncio.Event()

    async def run(self) -> None:
        LOG.info("loaded %s matches and %s contracts", len(self.matches), len(self.contracts))
        self.trade_manager.initialize()
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, self.stop_event.set)
            except NotImplementedError:
                pass
        tasks = [
            asyncio.create_task(self._run_polymarket(), name="polymarket"),
            asyncio.create_task(self._run_kalshi(), name="kalshi"),
            asyncio.create_task(self.trade_manager.start_user_ws(self.stop_event), name="polymarket-user"),
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
            await self.trade_manager.evaluate(self.book, price)

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
            await self.trade_manager.evaluate(self.book, price)

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


def number(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


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


def iso_ms(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).isoformat(timespec="milliseconds")
    

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


def fetch_json(url: str) -> Any:
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=20) as res:
        return json.loads(res.read().decode("utf-8"))


def first_list_item(value: Any) -> dict[str, Any] | None:
    if isinstance(value, list) and value and isinstance(value[0], dict):
        return value[0]
    if isinstance(value, dict):
        return value
    return None


def first_query_value(query: dict[str, list[str]], key: str) -> str | None:
    values = query.get(key)
    if not values:
        return None
    value = values[0]
    return value if value else None


def choose_polymarket_market(event_data: dict[str, Any] | None) -> dict[str, Any] | None:
    if not event_data:
        return None
    markets = event_data.get("markets") or []
    if not isinstance(markets, list):
        return None
    candidates = [market for market in markets if isinstance(market, dict)]
    if not candidates:
        return None
    moneyline_words = ("moneyline", "winner", "win the match", "win the game")
    for market in candidates:
        text = " ".join(str(market.get(key) or "") for key in ("question", "title", "description", "groupItemTitle")).lower()
        if any(word in text for word in moneyline_words) and market.get("clobTokenIds"):
            return market
    for market in candidates:
        if market.get("clobTokenIds"):
            return market
    return candidates[0]


async def resolve_match_from_urls(pm_url: str, kalshi_url: str, config: dict[str, Any] | None = None) -> dict[str, Any]:
    pm_url_clean = pm_url.split("?")[0].rstrip("/")
    pm_slug = pm_url_clean.split("/")[-1]
    loop = asyncio.get_event_loop()

    pm_data = await loop.run_in_executor(
        None,
        fetch_json,
        f"https://gamma-api.polymarket.com/markets?slug={quote(pm_slug)}",
    )
    market_data = first_list_item(pm_data)
    event_data = None
    if market_data is None:
        event_data = first_list_item(await loop.run_in_executor(
            None,
            fetch_json,
            f"https://gamma-api.polymarket.com/events?slug={quote(pm_slug)}",
        ))
        market_data = choose_polymarket_market(event_data)
    if market_data is None:
        raise ValueError(f"Could not find Polymarket market/event details for slug: {pm_slug}")

    pm_event_id = f"polymarket#{pm_slug}"
    
    pm_outcomes = market_data.get("outcomes")
    if isinstance(pm_outcomes, str):
        pm_outcomes = json.loads(pm_outcomes)
    pm_tokens = market_data.get("clobTokenIds")
    if isinstance(pm_tokens, str):
        pm_tokens = json.loads(pm_tokens)
        
    if not pm_outcomes or not pm_tokens or len(pm_outcomes) != len(pm_tokens):
        raise ValueError("Invalid outcome or token structure in Polymarket response")
        
    parsed_kalshi_url = urlparse(kalshi_url)
    query = parse_qs(parsed_kalshi_url.query)
    op_market_ticker = first_query_value(query, "op_market_ticker")
    kalshi_url_clean = kalshi_url.split("?")[0].rstrip("/")
    kalshi_event_ticker = kalshi_url_clean.split("/")[-1].upper()
    kalshi_event_id = f"kalshi#{kalshi_event_ticker}"

    kalshi_base_url = (config or {}).get("kalshi-rest-base-url") or "https://api.elections.kalshi.com/trade-api/v2"
    kalshi_data = await loop.run_in_executor(
        None,
        fetch_json,
        f"{kalshi_base_url.rstrip('/')}/markets?event_ticker={quote(kalshi_event_ticker)}",
    )
    kalshi_markets = kalshi_data.get("markets") or []
    if not kalshi_markets:
        raise ValueError(f"No markets found for Kalshi event ticker: {kalshi_event_ticker}")
    if op_market_ticker:
        op_market_ticker = op_market_ticker.upper()
        kalshi_markets = sorted(kalshi_markets, key=lambda market: 0 if market.get("ticker") == op_market_ticker else 1)
        
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

    if len(pm_contracts) != len(pm_outcomes) or len(kalshi_contracts) != len(pm_outcomes):
        raise ValueError(
            "Could not map all Polymarket outcomes to Kalshi markets. "
            f"polymarket_outcomes={pm_outcomes} kalshi_market_tickers={[m.get('ticker') for m in kalshi_markets]}"
        )

    match_id = f"dynamic-{pm_slug}"
    return {
        "match-id": match_id,
        "name": market_data.get("question") or (event_data or {}).get("title") or f"{pm_slug} Moneyline",
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
                    resolved = await resolve_match_from_urls(pm_url, kalshi_url, config)
                    match.update(resolved)
                except Exception as e:
                    match["_resolve_error"] = str(e)
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
        resolved_match = await resolve_match_from_urls(args.polymarket_url, args.kalshi_url, config)
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
