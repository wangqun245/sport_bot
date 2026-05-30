"""Lightweight Polymarket fair-value trading bot.

The old strategy stack is intentionally gone from this runner. Runtime logic is:
1. Subscribe to the current BTC 5-minute UP/DOWN market.
2. Record each completed window's max Binance up/down move from its opening price.
3. Estimate the current market fair value from current BTC delta divided by the
   average completed-window move in that direction.
4. Buy UP or DOWN when Polymarket is priced below that fair value by enough
   after fees and the configured profit tier.
5. While a buy is pending, cancel if the fair-value signal turns against the order.
6. While holding, sell when fair value drops enough below current market price.
7. Hold unresolved positions to market resolution if no clean sell happens.
"""

from __future__ import annotations

import builtins
import json
import math
import os
import signal
import threading
import time
import queue
import subprocess
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Optional

from dotenv import load_dotenv

from executor import Executor
from market import MarketWindow, get_current_market, get_market_winner
from polymarket_ws import PolymarketMarketFeed, PolymarketUserFeed, TokenPrice
from price_feed import BinancePriceFeed
from telegram_notifier import TelegramNotifier


_ORIGINAL_PRINT = builtins.print
WINDOW_SECONDS = 300.0


def _normal_cdf(value: float) -> float:
    return 0.5 * (1.0 + math.erf(value / math.sqrt(2.0)))


def _timestamped_print(*args, **kwargs):
    now = datetime.now()
    prefix = now.strftime("[%Y-%m-%d %H:%M:%S.%f")[:-3] + "]"
    if args:
        first = str(args[0])
        if first.startswith("\n"):
            args = ("\n" + prefix + " " + first[1:], *args[1:])
        else:
            args = (prefix, *args)
    else:
        args = (prefix,)
    _ORIGINAL_PRINT(*args, **kwargs)
    try:
        log_dir = Path(os.getenv("LOG_DIR", "logs"))
        log_dir.mkdir(parents=True, exist_ok=True)
        log_path = log_dir / f"bot_{now.strftime('%Y%m%d')}.log"
        sep = kwargs.get("sep", " ")
        end = kwargs.get("end", "\n")
        with log_path.open("a", encoding="utf-8") as handle:
            handle.write(sep.join(str(arg) for arg in args) + end)
    except Exception:
        pass


builtins.print = _timestamped_print


@dataclass
class Position:
    side: str
    token_id: str
    window_ts: int
    market_slug: str
    entry_price: float
    shares: float
    cost: float
    opening_price: float
    entry_btc_price: float
    entry_ts: float
    entry_edge: float = 0.0
    entry_delta_pct: float = 0.0
    entry_elapsed_ms: float = 0.0
    peak_unrealized_profit: float = 0.0
    fair_value_history: deque = field(default_factory=deque)
    fair_profit_history: deque = field(default_factory=deque)
    closed: bool = False


@dataclass
class PendingOrder:
    kind: str
    order_id: str
    side: str
    token_id: str
    window_ts: int
    market_slug: str
    price: float
    shares: float
    amount_usd: float
    balance_before: float = 0.0
    token_balance_before: Optional[float] = None
    next_check_ts: float = 0.0
    created_ts: float = 0.0
    cancel_requested: bool = False
    cancel_reason: str = ""
    strategy_reason: str = ""
    last_cancel_ts: float = 0.0
    cancel_attempts: int = 0
    cancel_attempt_ts: deque = field(default_factory=deque)


@dataclass
class BuyObservation:
    side: str
    started_price: float
    last_distinct_price: float
    base_profit: float
    last_profit: float
    confirm_count: int = 0
    started_ts: float = 0.0


@dataclass
class LagSignal:
    side: str
    current_mid: float
    buy_price: float
    sell_price: float
    predictions: dict[int, float]
    residuals: dict[int, float]
    profit_5: float
    profit_7: float
    profit_9: float
    score: float
    elapsed_ms: float
    binance_delta: float
    reason: str
    avg_up_move: float = 0.0
    avg_down_move: float = 0.0


@dataclass
class BinanceTradeFeaturePoint:
    ts: float
    price: float
    qty: float
    taker_buy_qty: float
    taker_sell_qty: float


@dataclass
class WindowMove:
    window_ts: int
    up_move: float
    down_move: float


class RawWsRecorder:
    def __init__(self) -> None:
        self.enabled = os.getenv("WS_RAW_RECORD_ENABLED", "false").lower() == "true"
        self.out_dir = Path(os.getenv("WS_RAW_RECORD_DIR", "data/ws_raw"))
        self.max_file_bytes = int(float(os.getenv("WS_RAW_RECORD_MAX_FILE_BYTES", "1000000000")))
        self.local_retention_hours = float(os.getenv("WS_RAW_RECORD_LOCAL_RETENTION_HOURS", "3"))
        self.max_closed_files = int(float(os.getenv("WS_RAW_RECORD_MAX_CLOSED_FILES", "3")))
        self.s3_uri = os.getenv("WS_RAW_RECORD_S3_URI", "s3://elasticbeanstalk-eu-west-1-687088702113/log/").strip()
        self.delete_after_upload = os.getenv("WS_RAW_RECORD_DELETE_AFTER_UPLOAD", "false").lower() == "true"
        self.queue = queue.SimpleQueue()
        self.upload_queue = queue.SimpleQueue()
        self.handles: dict[str, object] = {}
        self.paths: dict[str, Path] = {}
        self.bytes_written: dict[str, int] = {}
        self.part: dict[str, int] = {}
        self.thread: Optional[threading.Thread] = None
        self.upload_thread: Optional[threading.Thread] = None
        self.cleanup_thread: Optional[threading.Thread] = None
        self.running = False

    def start(self) -> None:
        if not self.enabled or self.running:
            return
        self.out_dir.mkdir(parents=True, exist_ok=True)
        self.running = True
        self.thread = threading.Thread(target=self._loop, daemon=True)
        self.thread.start()
        if self.s3_uri:
            self.upload_thread = threading.Thread(target=self._upload_loop, daemon=True)
            self.upload_thread.start()
        self.cleanup_thread = threading.Thread(target=self._cleanup_loop, daemon=True)
        self.cleanup_thread.start()
        print(
            f"[raw-ws] Recording existing bot streams to {self.out_dir.resolve()} "
            f"max_file_bytes={self.max_file_bytes} retention={self.local_retention_hours:g}h "
            f"max_closed_files={self.max_closed_files} s3={self.s3_uri or '-'}"
        )

    def stop(self) -> None:
        if not self.running:
            return
        self.running = False
        self.queue.put(None)
        self.upload_queue.put(None)
        if self.thread:
            self.thread.join(timeout=3)
        if self.upload_thread:
            self.upload_thread.join(timeout=3)
        if self.cleanup_thread:
            self.cleanup_thread.join(timeout=3)

    def write(self, source: str, record: dict) -> None:
        if not self.enabled or not self.running:
            return
        self.queue.put((source, record))

    def _loop(self) -> None:
        while self.running:
            item = self.queue.get()
            if item is None:
                break
            source, record = item
            try:
                self._write_now(source, record)
            except Exception as exc:
                print(f"[raw-ws] write failed: {type(exc).__name__}: {exc}")
        self._close_all()

    def _write_now(self, source: str, record: dict) -> None:
        handle = self.handles.get(source)
        if handle is None:
            handle = self._open(source)
        line = json.dumps(record, separators=(",", ":"), ensure_ascii=False) + "\n"
        handle.write(line)
        self.bytes_written[source] = self.bytes_written.get(source, 0) + len(line.encode("utf-8"))
        if self.bytes_written[source] >= self.max_file_bytes:
            self._rotate(source)

    def _open(self, source: str):
        stamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        part = self.part.get(source, 0) + 1
        self.part[source] = part
        path = self.out_dir / f"{source}_{stamp}_part{part:04d}.jsonl"
        handle = path.open("a", encoding="utf-8", newline="\n")
        self.handles[source] = handle
        self.paths[source] = path
        self.bytes_written[source] = path.stat().st_size if path.exists() else 0
        print(f"[raw-ws] opened {path}")
        return handle

    def _rotate(self, source: str) -> None:
        handle = self.handles.pop(source, None)
        if handle:
            handle.flush()
            handle.close()
        old = self.paths.pop(source, None)
        print(f"[raw-ws] rotated {old}")
        if old:
            self.upload_queue.put(old)
            self._cleanup_old_files()
        self._open(source)

    def _close_all(self) -> None:
        for source, handle in list(self.handles.items()):
            try:
                handle.flush()
                handle.close()
                print(f"[raw-ws] closed {self.paths.get(source)}")
                path = self.paths.get(source)
                if path:
                    self.upload_queue.put(path)
            except Exception:
                pass
        self.handles.clear()

    def _upload_loop(self) -> None:
        while self.running:
            path = self.upload_queue.get()
            if path is None:
                break
            try:
                self._upload_path(path)
            except Exception as exc:
                print(f"[raw-ws] upload failed for {path}: {type(exc).__name__}: {exc}")

    def _upload_path(self, path: Path) -> None:
        if not self.s3_uri or not path or not path.exists():
            return
        dest = self.s3_uri.rstrip("/") + "/" + path.name
        print(f"[raw-ws] uploading {path} -> {dest}")
        proc = subprocess.run(
            ["aws", "s3", "cp", str(path), dest],
            capture_output=True,
            text=True,
        )
        if proc.returncode != 0:
            err = (proc.stderr or proc.stdout or "").strip()
            print(f"[raw-ws] upload failed for {path.name}: {err}")
            return
        print(f"[raw-ws] uploaded {path.name}")
        if self.delete_after_upload:
            path.unlink(missing_ok=True)
            print(f"[raw-ws] deleted uploaded local file {path}")

    def _cleanup_loop(self) -> None:
        while self.running:
            self._cleanup_old_files()
            time.sleep(300)

    def _cleanup_old_files(self) -> None:
        if self.local_retention_hours <= 0:
            cutoff = 0.0
        else:
            cutoff = time.time() - self.local_retention_hours * 3600.0
        active = {path.resolve() for path in self.paths.values() if path}
        deleted = 0
        bytes_deleted = 0
        closed_files = []
        for path in self.out_dir.glob("*.jsonl"):
            try:
                if path.resolve() not in active:
                    closed_files.append(path)
            except FileNotFoundError:
                continue
        keep_by_count = set()
        if self.max_closed_files > 0:
            keep_by_count = {
                path.resolve()
                for path in sorted(closed_files, key=lambda item: item.stat().st_mtime, reverse=True)[: self.max_closed_files]
            }
        for path in closed_files:
            try:
                stat = path.stat()
                time_ok = True if self.local_retention_hours <= 0 else (stat.st_mtime >= cutoff)
                count_ok = True if self.max_closed_files <= 0 else (path.resolve() in keep_by_count)
                if time_ok and count_ok:
                    continue
                size = stat.st_size
                path.unlink()
                deleted += 1
                bytes_deleted += size
            except FileNotFoundError:
                continue
            except Exception as exc:
                print(f"[raw-ws] cleanup failed for {path}: {type(exc).__name__}: {exc}")
        if deleted:
            print(f"[raw-ws] cleanup deleted {deleted} files, freed {bytes_deleted / 1024 / 1024:.1f} MB")


class PolyBot:
    def __init__(self) -> None:
        load_dotenv()

        self.dry_run = os.getenv("DRY_RUN", "true").lower() == "true"
        self.period_minutes = int(float(os.getenv("MARKET_PERIOD", "5")))
        self.trade_amount = float(os.getenv("TRADE_AMOUNT", "5.0"))
        self.min_buy_price = float(os.getenv("MIN_BUY_PRICE", "0.10"))
        self.max_buy_price = float(os.getenv("MAX_BUY_PRICE", "0.48"))
        self.buy_price_improve_ticks = max(0, int(float(os.getenv("BUY_PRICE_IMPROVE_TICKS", "1"))))
        self.buy_price_improve_max = max(0.0, float(os.getenv("BUY_PRICE_IMPROVE_MAX", "0.02")))
        self.min_profit_pct = float(os.getenv("LAG_MODEL_MIN_PROFIT_PCT", os.getenv("MIN_PROFIT_PCT", "0.05")))
        self.buy_profit_tiers = self._load_buy_profit_tiers()
        self.sell_min_drop = float(os.getenv("LAG_MODEL_SELL_MIN_DROP", "0.01"))
        self.trade_cooldown_seconds = float(os.getenv("TRADE_COOLDOWN_SECONDS", "2"))
        self.max_buys_per_window = int(float(os.getenv("MAX_BUYS_PER_WINDOW", "1")))
        self.orderbook_competition_mult = float(os.getenv("ORDERBOOK_COMPETITION_MULT", "3"))
        self.buy_observe_confirm_ticks = max(1, int(float(os.getenv("BUY_OBSERVE_CONFIRM_TICKS", "2"))))
        self.buy_observe_min_total_profit_gain = max(
            0.0,
            float(os.getenv("BUY_OBSERVE_MIN_TOTAL_PROFIT_GAIN", "0.20")),
        )
        self.early_entry_block_seconds = max(0.0, float(os.getenv("LAG_MODEL_EARLY_ENTRY_BLOCK_SECONDS", "5")))
        self.polymarket_sell_smooth_points = max(1, int(float(os.getenv("POLYMARKET_SELL_SMOOTH_POINTS", "10"))))
        self.fair_value_sell_smooth_points = max(1, int(float(os.getenv("FAIR_VALUE_SELL_SMOOTH_POINTS", "3"))))
        self.late_entry_seconds = float(os.getenv("LAG_MODEL_LATE_ENTRY_SECONDS", "30"))
        self.late_entry_min_future_price = float(os.getenv("LAG_MODEL_LATE_ENTRY_MIN_FUTURE_PRICE", "0.50"))
        self.fair_value_window_count = max(1, int(float(os.getenv("FAIR_VALUE_WINDOW_COUNT", "4"))))
        self.fair_value_min_windows = max(
            self.fair_value_window_count,
            int(float(os.getenv("FAIR_VALUE_MIN_WINDOWS", str(self.fair_value_window_count)))),
        )
        self.fair_value_time_scaling_enabled = os.getenv("FAIR_VALUE_TIME_SCALING_ENABLED", "true").lower() == "true"
        self.fair_value_time_z_at_avg = max(0.01, float(os.getenv("FAIR_VALUE_TIME_Z_AT_AVG", "1.25")))
        self.fair_value_min_remaining_seconds = max(
            1.0,
            float(os.getenv("FAIR_VALUE_MIN_REMAINING_SECONDS", "10")),
        )
        self.fair_value_state_file = Path(os.getenv("FAIR_VALUE_STATE_FILE", "data/fair_value_window_state.json"))
        self.fair_value_state_max_age_seconds = float(os.getenv("FAIR_VALUE_STATE_MAX_AGE_SECONDS", "600"))
        self.dry_run_min_hold_seconds = float(os.getenv("DRY_RUN_MIN_HOLD_SECONDS", "5"))
        self.max_profit_drawdown_pct = float(os.getenv("MAX_PROFIT_DRAWDOWN_PCT", "0.33"))
        self.max_principal_drawdown_pct = float(os.getenv("MAX_PRINCIPAL_DRAWDOWN_PCT", "0.30"))
        self.take_profit_sell_price = max(0.0, float(os.getenv("TAKE_PROFIT_SELL_PRICE", "0")))
        self.status_interval = float(os.getenv("STATUS_INTERVAL_SECONDS", "10"))
        self.decision_log_interval = float(os.getenv("DECISION_LOG_INTERVAL_SECONDS", "10"))
        self.pending_check_seconds = float(os.getenv("PENDING_ORDER_CHECK_SECONDS", "1"))
        self.pending_buy_max_seconds = float(os.getenv("PENDING_BUY_MAX_SECONDS", "4"))
        self.pending_sell_max_seconds = float(os.getenv("PENDING_SELL_MAX_SECONDS", "4"))
        self.pending_cancel_retry_seconds = max(0.3, float(os.getenv("PENDING_CANCEL_RETRY_SECONDS", "0.3")))
        self.pending_cancel_max_per_second = max(1, int(float(os.getenv("PENDING_CANCEL_MAX_PER_SECOND", "3"))))
        self.polymarket_taker_fee_rate = float(
            os.getenv("POLYMARKET_TAKER_FEE_RATE", os.getenv("POLYMARKET_CRYPTO_TAKER_FEE_RATE", "0.07"))
        )
        self.polymarket_taker_fees_enabled = os.getenv("POLYMARKET_TAKER_FEES_ENABLED", "true").lower() == "true"

        self.executor = Executor(
            private_key=os.getenv("PRIVATE_KEY", ""),
            safe_address=os.getenv("SAFE_ADDRESS", ""),
            dry_run=self.dry_run,
            signature_type=int(os.getenv("SIGNATURE_TYPE", "2")),
            funder_address=os.getenv("FUNDER_ADDRESS", os.getenv("SAFE_ADDRESS", "")),
        )
        self.telegram = TelegramNotifier()
        self.price_feed = BinancePriceFeed("BTCUSDT", "BTC")
        self.raw_recorder = RawWsRecorder()
        self.poly_feed = PolymarketMarketFeed(on_raw_message=self._on_polymarket_raw if self.raw_recorder.enabled else None)
        self.user_feed: Optional[PolymarketUserFeed] = None

        self.market: Optional[MarketWindow] = None
        self.opening_price = 0.0
        self.current_btc_price = 0.0
        self.last_binance_ts = 0.0
        self.last_signal: Optional[LagSignal] = None
        self.buy_observation: Optional[BuyObservation] = None
        self.position: Optional[Position] = None
        self.pending_buy: Optional[PendingOrder] = None
        self.pending_sell: Optional[PendingOrder] = None
        self.realized_pnl = 0.0
        self.session_start_balance = 0.0
        self.running = False
        self.shutdown_requested = False
        self.force_shutdown_requested = False
        self.last_shutdown_wait_log_ts = 0.0
        self.boot_wait_window_ts: Optional[int] = None
        self.boot_wait_complete = False
        self.last_market_probe_ts = 0.0
        self.last_status_ts = 0.0
        self.last_trade_action_ts = 0.0
        self.buy_window_ts = 0
        self.buy_count_in_window = 0
        self.current_window_had_trade = False
        self.last_signal_log_ts = 0.0
        self.last_decision_log_ts = 0.0
        self.hourly_summary_interval = float(os.getenv("HOURLY_SUMMARY_INTERVAL_SECONDS", "3600"))
        self.last_hourly_summary_ts = time.time()
        self.total_trades = 0
        self.total_wins = 0
        self.total_losses = 0
        self.hour_trades = 0
        self.hour_wins = 0
        self.hour_losses = 0
        self.hour_pnl = 0.0
        self.hour_edge_sum = 0.0
        self.hour_delta_sum = 0.0
        self.hour_best_trade: Optional[float] = None
        self.hour_worst_trade: Optional[float] = None
        self.hour_windows_seen = 0
        self.hour_windows_with_trade = 0
        self.binance_trade_history: deque[BinanceTradeFeaturePoint] = deque()
        self.completed_window_moves: deque[WindowMove] = deque(maxlen=self.fair_value_window_count)
        self.current_window_high = 0.0
        self.current_window_low = 0.0
        self.last_finalized_window_ts: Optional[int] = None
        self._order_event_lock = threading.Lock()
        self._load_fair_value_state()

    def _model_elapsed_ms(self, ts: float) -> float:
        if not self.market:
            return 0.0
        raw_ms = (ts - self.market.window_start) * 1000.0
        # Training rows are sampled on a strict 20ms grid. Keep live inference
        # on the same time grid while still using the exact latest Binance price.
        rounded_ms = round(raw_ms / 20.0) * 20.0
        return max(0.0, min(300000.0, rounded_ms))

    def start(self) -> None:
        print("=" * 72)
        print("PolyBot - window-volatility fair value")
        print("=" * 72)
        print(f"Mode: {'DRY RUN' if self.dry_run else 'LIVE'}")
        print(f"Trade amount: ${self.trade_amount:.2f}")
        print(f"Buy price range: ${self.min_buy_price:.2f}-${self.max_buy_price:.2f}")
        if self.buy_price_improve_ticks > 0 and self.buy_price_improve_max > 0:
            print(
                f"Buy price improvement: up to {self.buy_price_improve_ticks} ticks "
                f"or ${self.buy_price_improve_max:.2f} when profit gate still passes"
            )
        print(f"Orderbook competition depth: {self.orderbook_competition_mult:.1f}x")
        print(
            f"Buy rule: fair value profit >= price tier, "
            f"vol windows {self.fair_value_min_windows}/{self.fair_value_window_count}, "
            f"pending buy/sell timeout {self.pending_buy_max_seconds:.1f}s/{self.pending_sell_max_seconds:.1f}s"
        )
        if self.fair_value_time_scaling_enabled:
            print(
                f"Fair value time scaling: normal CDF z_at_avg={self.fair_value_time_z_at_avg:.2f}, "
                f"min remaining {self.fair_value_min_remaining_seconds:.0f}s"
            )
        else:
            print("Fair value time scaling: disabled, using linear window move model")
        if self.dry_run and self.dry_run_min_hold_seconds > 0:
            print(f"Dry-run sell gate: allow sells after {self.dry_run_min_hold_seconds:.1f}s")
        print(f"Fair-value profit drawdown sell: {self.max_profit_drawdown_pct:.0%}")
        if self.max_principal_drawdown_pct > 0:
            print(f"Fair-value principal protection sell: {self.max_principal_drawdown_pct:.0%}")
        if self.take_profit_sell_price > 0:
            early_entry_cutoff = max(0.0, WINDOW_SECONDS - self.late_entry_seconds)
            print(
                f"Early-entry take profit: sell >= ${self.take_profit_sell_price:.2f} "
                f"for entries in first {early_entry_cutoff:.0f}s"
            )
        print(f"Buy profit tiers: {self._format_buy_profit_tiers()}")
        print(
            f"Late-entry rule: last {self.late_entry_seconds:.0f}s requires "
            f"fair value > ${self.late_entry_min_future_price:.2f}"
        )
        print(f"Early-entry rule: first {self.early_entry_block_seconds:.1f}s no buys")
        print("Sell rule: take-profit threshold plus Binance-derived fair-value drawdown/protection")

        ready = self.executor.initialize()
        if not ready and not self.dry_run:
            raise RuntimeError("Executor initialization failed")
        if not ready:
            print("[executor] DRY RUN continuing without CLOB auth initialization")
        self.session_start_balance = float(os.getenv("DRY_RUN_BALANCE", "100")) if self.dry_run else self.executor.get_balance(refresh=True)
        print(f"Starting balance: ${self.session_start_balance:.2f}")
        self._wait_for_external_activity_before_start()

        self.price_feed.start(on_price=self._on_binance_trade)
        self.poly_feed.start()
        if not self.dry_run:
            self.user_feed = PolymarketUserFeed(
                self.executor.get_api_creds(),
                on_message=self._on_user_order_message,
            )
            self.user_feed.start()
        signal.signal(signal.SIGINT, self._handle_shutdown)
        signal.signal(signal.SIGTERM, self._handle_shutdown)
        self.telegram.send(
            "*PolyBot Started*\n"
            "Strategy: window-volatility fair value\n"
            f"Mode: {'DRY RUN' if self.dry_run else 'LIVE'}\n"
            f"Buy cap: ${self.max_buy_price:.2f}\n"
            f"Buy tiers: {self._format_buy_profit_tiers()}\n"
            f"Pending buy timeout: {self.pending_buy_max_seconds:.1f}s"
        )

        self.running = True
        while self.running:
            self._tick()
            self._maybe_complete_shutdown()
            time.sleep(0.02)
        self._save_fair_value_state()
        self._stop_services()

    def _wait_for_external_activity_before_start(self) -> None:
        if self.dry_run:
            return
        self._wait_for_external_open_orders()
        self._wait_for_external_current_market_position()
        self._wait_for_external_open_orders()

    def _wait_for_external_open_orders(self) -> None:
        last_log_ts = 0.0
        while True:
            orders = self.executor.get_open_orders()
            if not orders:
                return
            now = time.time()
            if now - last_log_ts >= 10.0:
                last_log_ts = now
                print(
                    f"[startup-safety] Waiting for {len(orders)} existing open CLOB order(s) "
                    "before bot startup continues"
                )
            time.sleep(1.0)

    def _wait_for_external_current_market_position(self) -> None:
        market = get_current_market(self.period_minutes, asset="btc")
        if not market:
            return
        balances = []
        for side, token_id in (("UP", market.token_id_up), ("DOWN", market.token_id_down)):
            balance = self.executor.get_token_balance(token_id, refresh=True)
            if balance >= 1:
                balances.append((side, balance))
        if not balances:
            return
        balance_text = ", ".join(f"{side} {shares:.2f}" for side, shares in balances)
        print(
            f"[startup-safety] Existing current-window position detected ({balance_text}); "
            "waiting until this 5m market ends before starting new trading"
        )
        while time.time() < market.window_end + 2.0:
            remaining = max(0.0, market.window_end - time.time())
            print(f"[startup-safety] Position window ends in {remaining:.1f}s")
            time.sleep(min(10.0, max(1.0, remaining)))

    def _on_binance_trade(self, price: float, source: str = "ws", event_ts: float = None, received_ts: float = None, raw: dict = None, **_) -> None:
        self.current_btc_price = float(price or 0.0)
        self.last_binance_ts = received_ts or time.time()
        if self.raw_recorder.enabled:
            self._record_raw_binance(source, event_ts, self.last_binance_ts, raw or {})
        self._record_binance_trade(self.last_binance_ts, self.current_btc_price, raw or {})
        self._ensure_market()
        if not self.market:
            return
        self._update_current_window_extremes(self.current_btc_price)
        if self._boot_wait_active():
            self.last_signal = None
            self.buy_observation = None
            self._trade_on_latest_signal()
            self._log_decision_sample()
            return
        signal = self._build_lag_signal(self.current_btc_price, self.last_binance_ts)
        if signal:
            self.last_signal = signal
            if time.time() - self.last_signal_log_ts >= 3:
                self.last_signal_log_ts = time.time()
                required_profit = self._min_profit_for_buy_price(signal.buy_price)
                tier_text = f"{required_profit:.1%}" if required_profit is not None else "n/a"
                print(
                    f"  Fair signal {signal.side}: buy ${signal.buy_price:.3f}, "
                    f"mid ${signal.current_mid:.3f}, fair ${signal.predictions[5]:.3f}, "
                    f"profit {signal.profit_5:.1%} (gate >= {tier_text}), "
                    f"btc {signal.binance_delta:+.2f}, avg max-move ${signal.avg_up_move:.2f}, "
                    f"elapsed {signal.elapsed_ms:.0f}ms"
                )
        else:
            self.last_signal = None
        self._trade_on_latest_signal()
        self._log_decision_sample()

    def _record_raw_binance(self, source: str, event_ts: float, received_ts: float, raw: dict) -> None:
        if source != "ws" or not raw:
            return
        self.raw_recorder.write(
            "binance_trade",
            {
                "source": "binance",
                "symbol": "btcusdt",
                "event_ts": event_ts,
                "received_ts": received_ts,
                "received_ms": int(received_ts * 1000),
                "raw": json.dumps(raw, separators=(",", ":"), ensure_ascii=False),
            },
        )

    def _on_polymarket_raw(self, raw: str, received_ts: float = None) -> None:
        if not self.raw_recorder.enabled:
            return
        received_ts = received_ts or time.time()
        market = self.market
        self.raw_recorder.write(
            "polymarket",
            {
                "source": "polymarket",
                "asset": "btc",
                "period_minutes": self.period_minutes,
                "market_slug": market.slug if market else "",
                "token_ids": [market.token_id_up, market.token_id_down] if market else [],
                "received_ts": received_ts,
                "received_ms": int(received_ts * 1000),
                "raw": raw,
            },
        )

    def _tick(self) -> None:
        if self.current_btc_price <= 0:
            return
        self._ensure_market()
        if not self.market:
            return
        self._process_pending_buy()
        self._process_pending_sell()
        if self.position and not self.position.closed and self.market.window_start != self.position.window_ts:
            self._resolve_position()
        self._print_status()

    def _ensure_market(self) -> None:
        now = time.time()
        if self.market and self.market.window_start <= now < self.market.window_end:
            return
        if now - self.last_market_probe_ts < 1.0:
            return
        self.last_market_probe_ts = now
        previous = self.market
        market = get_current_market(self.period_minutes, asset="btc")
        if not market:
            return
        if previous and previous.window_start != market.window_start:
            if previous.window_start == self.boot_wait_window_ts:
                print(f"[startup-safety] Skipping partial boot window {previous.window_start} in volatility history")
            else:
                self._finalize_window_move(previous.window_start)
        if previous and previous.window_start != market.window_start and self.position and not self.position.closed:
            self._resolve_position()
        if previous and previous.window_start != market.window_start and self.pending_buy:
            self._cancel_pending_buy("window changed")
        if previous and previous.window_start != market.window_start and self.pending_sell:
            self._cancel_pending_sell("window changed")
        self.market = market
        if self.boot_wait_window_ts is None:
            self.boot_wait_window_ts = market.window_start
            self.boot_wait_complete = False
        elif market.window_start != self.boot_wait_window_ts and not self.boot_wait_complete:
            self.boot_wait_complete = True
            print("[startup-safety] Next full 5m window reached; trading enabled")
        self.opening_price = self.current_btc_price
        self.current_window_high = self.current_btc_price
        self.current_window_low = self.current_btc_price
        self.last_signal = None
        self.buy_observation = None
        self.buy_window_ts = market.window_start
        self.buy_count_in_window = 0
        self.current_window_had_trade = False
        self.hour_windows_seen += 1
        self.poly_feed.subscribe(
            [market.token_id_up, market.token_id_down],
            {market.token_id_up: "UP", market.token_id_down: "DOWN"},
        )
        if self.user_feed and market.condition_id:
            self.user_feed.subscribe([market.condition_id])
        avg_up, avg_down = self._average_window_moves()
        history_text = f"{len(self.completed_window_moves)}/{self.fair_value_window_count}"
        print(
            f"\n[new window] {market.slug} | open BTC ${self.opening_price:,.2f} | "
            f"vol history {history_text} avg max-move ${avg_up:.2f}"
        )
        if self._boot_wait_active():
            print(
                "[startup-safety] Bot restarted inside an already-running window; "
                "new buys are blocked until the next 5m window"
            )

    def _boot_wait_active(self) -> bool:
        return (
            not self.boot_wait_complete
            and self.boot_wait_window_ts is not None
            and self.market is not None
            and self.market.window_start == self.boot_wait_window_ts
        )

    def _update_current_window_extremes(self, btc_price: float) -> None:
        if btc_price <= 0:
            return
        if self.current_window_high <= 0 or self.current_window_low <= 0:
            self.current_window_high = btc_price
            self.current_window_low = btc_price
            return
        self.current_window_high = max(self.current_window_high, btc_price)
        self.current_window_low = min(self.current_window_low, btc_price)

    def _finalize_window_move(self, window_ts: int) -> None:
        if not window_ts or self.last_finalized_window_ts == window_ts:
            return
        if self.opening_price <= 0 or self.current_window_high <= 0 or self.current_window_low <= 0:
            return
        up_move = max(0.0, self.current_window_high - self.opening_price)
        down_move = max(0.0, self.opening_price - self.current_window_low)
        if up_move <= 0 and down_move <= 0:
            return
        self.completed_window_moves.append(WindowMove(window_ts=window_ts, up_move=up_move, down_move=down_move))
        self.last_finalized_window_ts = window_ts
        avg_up, avg_down = self._average_window_moves()
        print(
            f"[vol] finalized {window_ts}: up ${up_move:.2f}, down ${down_move:.2f}; "
            f"window max ${max(up_move, down_move):.2f}; "
            f"avg max-move ${avg_up:.2f} over {len(self.completed_window_moves)} windows"
        )
        self._save_fair_value_state()

    def _load_fair_value_state(self) -> None:
        try:
            if self.fair_value_state_max_age_seconds <= 0 or not self.fair_value_state_file.exists():
                return
            payload = json.loads(self.fair_value_state_file.read_text(encoding="utf-8"))
            saved_at = float(payload.get("saved_at") or 0.0)
            age = time.time() - saved_at
            if saved_at <= 0 or age < 0 or age > self.fair_value_state_max_age_seconds:
                print(f"[vol] saved state ignored: age {age:.1f}s > {self.fair_value_state_max_age_seconds:.1f}s")
                return
            rows = payload.get("windows") or []
            moves: list[WindowMove] = []
            for row in rows[-self.fair_value_window_count:]:
                if not isinstance(row, dict):
                    continue
                window_ts = int(float(row.get("window_ts") or 0))
                up_move = float(row.get("up_move") or 0.0)
                down_move = float(row.get("down_move") or 0.0)
                if window_ts > 0 and (up_move >= 0 or down_move >= 0):
                    moves.append(WindowMove(window_ts=window_ts, up_move=max(0.0, up_move), down_move=max(0.0, down_move)))
            if not moves:
                return
            self.completed_window_moves.clear()
            self.completed_window_moves.extend(moves)
            self.last_finalized_window_ts = moves[-1].window_ts
            avg_up, avg_down = self._average_window_moves()
            print(
                f"[vol] loaded saved state: {len(self.completed_window_moves)}/{self.fair_value_window_count} "
                f"windows, avg max-move ${avg_up:.2f}, age {age:.1f}s"
            )
        except Exception as exc:
            print(f"[vol] saved state load failed: {exc}")

    def _save_fair_value_state(self) -> None:
        try:
            avg_up, avg_down = self._average_window_moves()
            payload = {
                "saved_at": time.time(),
                "saved_at_iso": datetime.now().isoformat(timespec="seconds"),
                "window_count": self.fair_value_window_count,
                "min_windows": self.fair_value_min_windows,
                "avg_up_move": avg_up,
                "avg_down_move": avg_down,
                "avg_max_move": avg_up,
                "windows": [
                    {
                        "window_ts": item.window_ts,
                        "up_move": item.up_move,
                        "down_move": item.down_move,
                    }
                    for item in self.completed_window_moves
                ],
            }
            self.fair_value_state_file.parent.mkdir(parents=True, exist_ok=True)
            self.fair_value_state_file.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        except Exception as exc:
            print(f"[vol] saved state write failed: {exc}")

    def _average_window_moves(self) -> tuple[float, float]:
        if not self.completed_window_moves:
            return 0.0, 0.0
        avg_max_move = sum(
            max(item.up_move, item.down_move)
            for item in self.completed_window_moves
        ) / len(self.completed_window_moves)
        return avg_max_move, avg_max_move

    def _fair_prices_from_btc_delta(
        self,
        binance_delta: float,
        elapsed_ms: Optional[float] = None,
    ) -> Optional[tuple[float, float, float, float]]:
        if len(self.completed_window_moves) < self.fair_value_min_windows:
            return None
        avg_up, avg_down = self._average_window_moves()
        if binance_delta >= 0:
            if avg_up <= 0:
                return None
            if self.fair_value_time_scaling_enabled:
                up_fair = self._time_scaled_up_fair(binance_delta, avg_up, elapsed_ms)
            else:
                offset = (binance_delta / avg_up) / 2.0
                up_fair = 0.5 + offset
        else:
            if avg_down <= 0:
                return None
            if self.fair_value_time_scaling_enabled:
                up_fair = 1.0 - self._time_scaled_up_fair(abs(binance_delta), avg_down, elapsed_ms)
            else:
                offset = (abs(binance_delta) / avg_down) / 2.0
                up_fair = 0.5 - offset
        up_fair = min(0.99, max(0.01, up_fair))
        down_fair = min(0.99, max(0.01, 1.0 - up_fair))
        return up_fair, down_fair, avg_up, avg_down

    def _time_scaled_up_fair(self, abs_delta: float, avg_move: float, elapsed_ms: Optional[float]) -> float:
        if avg_move <= 0:
            return 0.5
        elapsed_seconds = max(0.0, float(elapsed_ms or 0.0) / 1000.0)
        remaining_seconds = max(
            self.fair_value_min_remaining_seconds,
            WINDOW_SECONDS - min(WINDOW_SECONDS, elapsed_seconds),
        )
        time_scale = math.sqrt(WINDOW_SECONDS / remaining_seconds)
        z = (abs_delta / avg_move) * self.fair_value_time_z_at_avg * time_scale
        return _normal_cdf(z)

    def _build_lag_signal(self, btc_price: float, ts: float) -> Optional[LagSignal]:
        if not self.market or self.opening_price <= 0:
            return None
        if not (self.market.window_start <= ts < self.market.window_end):
            return None
        up = self.poly_feed.get_price(self.market.token_id_up)
        down = self.poly_feed.get_price(self.market.token_id_down)
        up_mid, up_ask, up_bid = self._mid_ask_bid(up)
        down_mid, down_ask, down_bid = self._mid_ask_bid(down)
        if up_mid <= 0 or down_mid <= 0:
            return None

        elapsed_ms = self._model_elapsed_ms(ts)
        if elapsed_ms < self.early_entry_block_seconds * 1000.0:
            return None
        binance_delta = btc_price - self.opening_price
        fair = self._fair_prices_from_btc_delta(binance_delta, elapsed_ms)
        if fair is None:
            return None
        up_fair, down_fair, avg_up, avg_down = fair
        candidates: list[LagSignal] = []
        for side, token_id, current_mid, fallback_buy_price, sell_price in (
            ("UP", self.market.token_id_up, up_mid, up_ask, up_bid),
            ("DOWN", self.market.token_id_down, down_mid, down_ask, down_bid),
        ):
            buy_price = self._depth_adjusted_buy_price(token_id, fallback_buy_price)
            if buy_price < self.min_buy_price or buy_price > self.max_buy_price:
                continue
            fair_price = up_fair if side == "UP" else down_fair
            side_prices = {5: fair_price, 7: fair_price, 9: fair_price, 11: fair_price}
            side_residuals = {5: 0.0, 7: 0.0, 9: 0.0, 11: 0.0}
            profit_5 = self._expected_profit_pct_after_fees(buy_price, fair_price)
            profit_7 = profit_5
            profit_9 = profit_5
            required_profit = self._min_profit_for_buy_price(buy_price)
            late_entry_ok = self._late_entry_future_price_ok(elapsed_ms, side_prices)
            if (
                required_profit is not None
                and profit_5 >= required_profit
                and late_entry_ok
            ):
                candidates.append(
                    LagSignal(
                        side=side,
                        current_mid=current_mid,
                        buy_price=buy_price,
                        sell_price=sell_price,
                        predictions=side_prices,
                        residuals=side_residuals,
                        profit_5=profit_5,
                        profit_7=profit_7,
                        profit_9=profit_9,
                        score=profit_5,
                        elapsed_ms=elapsed_ms,
                        binance_delta=binance_delta,
                        reason="window_vol_fair_value",
                        avg_up_move=avg_up,
                        avg_down_move=avg_down,
                    )
                )
        if not candidates:
            return None
        return max(candidates, key=lambda item: item.score)

    def _trade_on_latest_signal(self) -> None:
        if not self.market:
            return
        self._process_pending_buy()
        self._process_pending_sell()
        if self.pending_buy or self.pending_sell:
            return
        if self.position and not self.position.closed:
            self.buy_observation = None
            self._maybe_sell_on_lag_drop()
            return
        if self.shutdown_requested or self._boot_wait_active():
            self.buy_observation = None
            return
        if self.buy_count_in_window >= self.max_buys_per_window:
            self.buy_observation = None
            return
        if time.time() - self.last_trade_action_ts < self.trade_cooldown_seconds:
            return
        if not self.last_signal:
            self.buy_observation = None
            return
        self._observe_or_submit_buy(self.last_signal)

    def _observe_or_submit_buy(self, sig: LagSignal) -> None:
        price = self.current_btc_price
        if price <= 0:
            return
        signal_profit = min(sig.profit_5, sig.profit_7)
        if not self.buy_observation or self.buy_observation.side != sig.side:
            self.buy_observation = BuyObservation(
                side=sig.side,
                started_price=price,
                last_distinct_price=price,
                base_profit=signal_profit,
                last_profit=signal_profit,
                started_ts=time.time(),
            )
            print(
                f"  Buy observation started {sig.side}: BTC ${price:,.2f}, "
                f"profit {signal_profit:.1%}; wait for {self.buy_observe_confirm_ticks} ticks "
                f"and total profit gain {self.buy_observe_min_total_profit_gain:.1%}"
            )
            return

        obs = self.buy_observation
        if abs(price - obs.last_distinct_price) <= 1e-9:
            return

        direction = 1 if price > obs.last_distinct_price else -1
        target_direction = 1 if obs.side == "UP" else -1
        obs.last_distinct_price = price

        if direction == target_direction and signal_profit >= obs.last_profit:
            obs.confirm_count += 1
            obs.last_profit = signal_profit
        else:
            obs.confirm_count = 0
            obs.base_profit = signal_profit
            obs.last_profit = signal_profit

        if obs.confirm_count < self.buy_observe_confirm_ticks:
            return
        if signal_profit - obs.base_profit < self.buy_observe_min_total_profit_gain:
            return

        self._submit_buy(sig)
        if self.pending_buy:
            self.buy_observation = None

    def _log_decision_sample(self) -> None:
        now = time.time()
        if self.decision_log_interval <= 0 or now - self.last_decision_log_ts < self.decision_log_interval:
            return
        self.last_decision_log_ts = now

        if not self.market:
            print("[decision] no active Polymarket market yet")
            return
        if self.current_btc_price <= 0:
            print("[decision] waiting for Binance BTC price")
            return
        if self.opening_price <= 0:
            print("[decision] waiting for window opening BTC price")
            return

        trade_blocks = []
        if self.pending_buy:
            trade_blocks.append(f"pending BUY {self.pending_buy.side} @ ${self.pending_buy.price:.2f}")
        if self.pending_sell:
            trade_blocks.append(f"pending SELL {self.pending_sell.side} @ ${self.pending_sell.price:.2f}")
        if self.buy_observation and not self.position:
            trade_blocks.append(
                f"observing {self.buy_observation.side} profit expansion "
                f"{self.buy_observation.confirm_count}/{self.buy_observe_confirm_ticks}"
            )
        if self.buy_count_in_window >= self.max_buys_per_window and not self.position:
            trade_blocks.append(f"window buy limit {self.buy_count_in_window}/{self.max_buys_per_window}")
        if self._boot_wait_active() and not self.position:
            trade_blocks.append("startup wait until next full 5m window")
        if self.shutdown_requested and not self.position:
            trade_blocks.append("shutdown requested; new buys disabled")
        cooldown_left = self.trade_cooldown_seconds - (now - self.last_trade_action_ts)
        if cooldown_left > 0:
            trade_blocks.append(f"cooldown {cooldown_left:.1f}s")

        if self.position and not self.position.closed:
            sell_diag = self._sell_decision_diagnostic()
            block_text = "; ".join(trade_blocks) if trade_blocks else "none"
            print(f"[decision] holding {self.position.side}; trade_blocks={block_text}; {sell_diag}")
            return

        entry_lines = self._entry_decision_diagnostics()
        block_text = "; ".join(trade_blocks) if trade_blocks else "none"
        print(f"[decision] flat; trade_blocks={block_text}")
        for line in entry_lines:
            print(f"[decision]   {line}")

    def _entry_decision_diagnostics(self) -> list[str]:
        if not self.market:
            return ["no market"]
        if self._boot_wait_active():
            remaining = max(0.0, self.market.window_end - time.time())
            return [f"startup safety wait: next full 5m window in {remaining:.1f}s"]
        if self.shutdown_requested:
            return ["shutdown requested: new buys disabled"]
        up = self.poly_feed.get_price(self.market.token_id_up)
        down = self.poly_feed.get_price(self.market.token_id_down)
        up_mid, up_ask, up_bid = self._mid_ask_bid(up)
        down_mid, down_ask, down_bid = self._mid_ask_bid(down)
        if up_mid <= 0 or down_mid <= 0:
            return [
                f"waiting for Polymarket prices: UP mid=${up_mid:.3f} ask=${up_ask:.2f} bid=${up_bid:.2f}; "
                f"DOWN mid=${down_mid:.3f} ask=${down_ask:.2f} bid=${down_bid:.2f}"
            ]

        ts = self.last_binance_ts or time.time()
        if not (self.market.window_start <= ts < self.market.window_end):
            return [f"Binance tick outside current window: tick={ts:.3f}, window={self.market.window_start}-{self.market.window_end}"]

        elapsed_ms = self._model_elapsed_ms(ts)
        binance_delta = self.current_btc_price - self.opening_price
        fair = self._fair_prices_from_btc_delta(binance_delta, elapsed_ms)
        if fair is None:
            return [
                f"waiting for volatility history: {len(self.completed_window_moves)}/{self.fair_value_min_windows} "
                f"completed windows, btc_delta=${binance_delta:+.2f}"
            ]
        up_fair, down_fair, avg_up, avg_down = fair
        lines = []
        for side, token_id, current_mid, fallback_buy_price in (
            ("UP", self.market.token_id_up, up_mid, up_ask),
            ("DOWN", self.market.token_id_down, down_mid, down_ask),
        ):
            buy_price = self._depth_adjusted_buy_price(token_id, fallback_buy_price)
            fair_price = up_fair if side == "UP" else down_fair
            side_prices = {5: fair_price, 7: fair_price, 9: fair_price, 11: fair_price}
            profit_5 = self._expected_profit_pct_after_fees(buy_price, fair_price) if buy_price > 0 else 0.0
            profit_7 = profit_5
            profit_9 = profit_5
            required_profit = self._min_profit_for_buy_price(buy_price) if buy_price > 0 else None
            edge = fair_price - buy_price
            blocks = []
            if buy_price <= 0:
                blocks.append(f"insufficient ask depth {self.orderbook_competition_mult:.1f}x")
            if 0 < buy_price < self.min_buy_price:
                blocks.append(f"ask ${buy_price:.2f}<floor ${self.min_buy_price:.2f}")
            if buy_price > self.max_buy_price:
                blocks.append(f"ask ${buy_price:.2f}>cap ${self.max_buy_price:.2f}")
            if elapsed_ms < self.early_entry_block_seconds * 1000.0:
                blocks.append(f"early window {elapsed_ms / 1000.0:.1f}s<{self.early_entry_block_seconds:.1f}s")
            if required_profit is None:
                blocks.append("no profit tier")
            elif profit_5 < required_profit:
                blocks.append(f"fair profit {profit_5:.1%}<tier {required_profit:.1%}")
            if not self._late_entry_future_price_ok(elapsed_ms, side_prices):
                blocks.append(
                    f"late fair ${fair_price:.3f}"
                    f"<=${self.late_entry_min_future_price:.2f}"
                )
            status = "BUY_OK" if not blocks else "skip " + ", ".join(blocks)
            tier_text = f"{required_profit:.1%}" if required_profit is not None else "n/a"
            lines.append(
                f"{side}: {status}; ask=${buy_price:.2f}, mid=${current_mid:.3f}, "
                f"fair=${fair_price:.3f}, edge=${edge:+.3f}, profit={profit_5:.1%}, "
                f"tier_min={tier_text}, btc_delta=${binance_delta:+.2f}, "
                f"avg max-move=${avg_up:.2f}, "
                f"elapsed={elapsed_ms:.0f}ms"
            )
        return lines

    def _record_binance_trade(self, ts: float, price: float, raw: dict) -> None:
        try:
            qty = float(raw.get("q") or 0.0)
        except (TypeError, ValueError):
            qty = 0.0
        is_buyer_maker = raw.get("m")
        self.binance_trade_history.append(
            BinanceTradeFeaturePoint(
                ts=ts,
                price=price,
                qty=qty,
                taker_buy_qty=qty if is_buyer_maker is False else 0.0,
                taker_sell_qty=qty if is_buyer_maker is True else 0.0,
            )
        )
        keep_after = ts - 5.0
        while self.binance_trade_history and self.binance_trade_history[0].ts < keep_after:
            self.binance_trade_history.popleft()

    def _binance_price_at_or_before(self, target_ts: float, fallback: float) -> float:
        for point in reversed(self.binance_trade_history):
            if point.ts <= target_ts:
                return point.price
        if self.binance_trade_history:
            return self.binance_trade_history[0].price
        return fallback

    def _binance_trailing_avg_delta(self, ts: float, current_price: float, lookback_seconds: float) -> float:
        start_ts = ts - lookback_seconds
        total = 0.0
        count = 0
        for point in reversed(self.binance_trade_history):
            if point.ts <= start_ts:
                break
            if point.ts <= ts:
                total += point.price
                count += 1
        if count <= 0:
            return 0.0
        return current_price - (total / count)

    def _binance_feature_values(self, ts: float, current_price: float) -> dict[str, float]:
        buy_qty = 0.0
        sell_qty = 0.0
        start_ts = ts - 0.5
        for point in reversed(self.binance_trade_history):
            if point.ts < start_ts:
                break
            if point.ts <= ts:
                buy_qty += point.taker_buy_qty
                sell_qty += point.taker_sell_qty

        return {
            "binance_delta_40ms": self._binance_trailing_avg_delta(ts, current_price, 0.040),
            "binance_delta_100ms": self._binance_trailing_avg_delta(ts, current_price, 0.100),
            "binance_delta_250ms": self._binance_trailing_avg_delta(ts, current_price, 0.250),
            "binance_delta_500ms": self._binance_trailing_avg_delta(ts, current_price, 0.500),
            "binance_taker_buy_qty_500ms": buy_qty,
            "binance_taker_sell_qty_500ms": sell_qty,
            "binance_taker_qty_imbalance_500ms": buy_qty - sell_qty,
        }

    def _on_user_order_message(self, raw: str, received_ts: float = None) -> None:
        try:
            payload = json.loads(raw)
        except Exception:
            return
        messages = payload if isinstance(payload, list) else [payload]
        for msg in messages:
            if not isinstance(msg, dict):
                continue
            self._handle_user_order_update(msg, received_ts or time.time())

    def _handle_user_order_update(self, msg: dict, received_ts: float) -> None:
        event_type = str(msg.get("event_type") or msg.get("type") or "").lower()
        status = str(msg.get("status") or msg.get("type") or "").upper()
        if event_type not in {"order", "trade"} and status not in {"MATCHED", "UPDATE"}:
            return

        pending = self.pending_buy or self.pending_sell
        if not pending:
            return

        match = self._matched_order_from_user_msg(msg, pending.order_id)
        if not match:
            return
        matched_shares, matched_price = match
        if matched_shares <= 0 or matched_price <= 0:
            return

        shares_left = max(0.0, pending.shares - matched_shares)
        result = SimpleNamespace(
            success=True,
            order_id=pending.order_id,
            status="FILLED" if shares_left < 1 else "PARTIAL",
            side=pending.kind,
            price=matched_price,
            amount_usd=matched_shares * matched_price,
            shares=matched_shares,
            shares_remaining=shares_left,
            token_id=pending.token_id[:16] + "...",
            dry_run=False,
        )
        print(
            f"  [user-ws] {pending.kind} {pending.side} matched: "
            f"{matched_shares:.2f}/{pending.shares:.2f} @ ${matched_price:.3f} "
            f"status={status or msg.get('type', '')}"
        )
        if pending.kind == "BUY":
            self._activate_buy_result(pending, result, source="user_ws")
        else:
            self._apply_sell_result(pending, result, source="user_ws")

    def _matched_order_from_user_msg(self, msg: dict, order_id: str) -> Optional[tuple[float, float]]:
        event_type = str(msg.get("event_type") or "").lower()
        msg_type = str(msg.get("type") or "").upper()
        if event_type == "order" or msg_type in {"UPDATE", "PLACEMENT", "CANCELLATION"}:
            if str(msg.get("id") or "") != order_id:
                return None
            return (self._as_float(msg.get("size_matched")), self._as_float(msg.get("price")))

        if event_type == "trade" or str(msg.get("status") or "").upper() == "MATCHED":
            if str(msg.get("taker_order_id") or "") == order_id:
                return (self._as_float(msg.get("size")), self._as_float(msg.get("price")))
            for maker in msg.get("maker_orders") or []:
                if not isinstance(maker, dict):
                    continue
                if str(maker.get("order_id") or "") == order_id:
                    return (
                        self._as_float(maker.get("matched_amount")),
                        self._as_float(maker.get("price") or msg.get("price")),
                    )
        return None

    def _as_float(self, value) -> float:
        try:
            if value in ("", None):
                return 0.0
            return float(value)
        except Exception:
            return 0.0

    def _late_entry_future_price_ok(self, elapsed_ms: float, side_prices: dict[int, float]) -> bool:
        if self.late_entry_seconds <= 0:
            return True
        remaining_ms = 300000.0 - elapsed_ms
        if remaining_ms > self.late_entry_seconds * 1000.0:
            return True
        return (
            side_prices.get(5, 0.0) > self.late_entry_min_future_price
            and side_prices.get(7, 0.0) > self.late_entry_min_future_price
        )

    def _sell_decision_diagnostic(self) -> str:
        signal = self._held_side_projection()
        if not signal:
            return "sell skip: missing held-side projection or Polymarket prices"
        current = signal.current_mid
        predicted_min = min(signal.predictions[5], signal.predictions[7])
        fair_profit = self._expected_profit_pct_after_fees(self.position.entry_price, predicted_min) if self.position else 0.0
        fair_principal_profit = 0.0
        principal_drawdown = 0.0
        principal_gate = max(0.0, self.max_principal_drawdown_pct)
        if self.position and self.position.cost > 0:
            fair_revenue = self._net_sell_revenue(predicted_min, self.position.shares)
            fair_principal_profit = fair_revenue / self.position.cost - 1.0
            principal_drawdown = max(0.0, -fair_principal_profit)
        peak = self.position.peak_unrealized_profit if self.position else 0.0
        drawdown = peak - fair_profit
        gate = peak * max(0.0, min(1.0, self.max_profit_drawdown_pct))
        sell_price = self._live_sell_price(self.position.side, self.position.shares) if self.position else 0.0
        if sell_price <= 0:
            status = "sell skip: no bid"
        elif principal_gate > 0 and principal_drawdown >= principal_gate:
            status = f"SELL_OK principal drawdown {principal_drawdown:.1%}>=gate {principal_gate:.1%}"
        elif peak <= 0 or drawdown < gate:
            status = f"sell skip: fair profit drawdown {drawdown:.1%}<gate {gate:.1%}"
        else:
            status = "SELL_OK"
        return (
            f"{status}; bid=${sell_price:.2f}, mid=${current:.3f}, "
            f"fair=${predicted_min:.3f}, fair_profit={fair_profit:.1%}, "
            f"fair_principal_profit={fair_principal_profit:.1%}, peak={peak:.1%}, "
            f"btc_delta=${signal.binance_delta:+.2f}, avg max-move=${signal.avg_up_move:.2f}, "
            f"elapsed={signal.elapsed_ms:.0f}ms"
        )

    def _submit_buy(self, sig: LagSignal) -> None:
        if not self.market:
            return
        token_id = self._token_id(sig.side)
        if not token_id:
            return
        raw_buy_price = self._live_buy_price(sig.side)
        buy_price = self._improved_buy_price(raw_buy_price, sig)
        if buy_price < self.min_buy_price or buy_price > self.max_buy_price:
            return
        required_profit = self._min_profit_for_buy_price(buy_price)
        live_profit_5 = self._expected_profit_pct_after_fees(buy_price, sig.predictions[5])
        live_profit_7 = self._expected_profit_pct_after_fees(buy_price, sig.predictions[7])
        if required_profit is None:
            print(f"  Buy skipped: no profit tier for price ${buy_price:.3f}")
            return
        if live_profit_5 < required_profit or live_profit_7 < 0:
            print(
                f"  Buy skipped: live price ${buy_price:.3f} needs {required_profit:.1%}, "
                f"profit 5/7 {live_profit_5:.1%}/{live_profit_7:.1%}"
            )
            return
        balance = self.executor.get_balance(refresh=False)
        amount = round(self.trade_amount if self.dry_run else min(self.trade_amount, max(0.0, balance)), 2)
        if not self.dry_run and amount < 5:
            print(f"  Buy skipped: balance ${balance:.2f} below $5")
            return
        print(
            f"\n[ENTRY] {sig.side} fair value | buy ${buy_price:.3f}, "
            f"fair ${sig.predictions[5]:.3f}, "
            f"profit {live_profit_5:.1%}, "
            f"tier_min {required_profit:.1%}, "
            f"btc {sig.binance_delta:+.2f}, avg max-move ${sig.avg_up_move:.2f}"
        )
        if buy_price > raw_buy_price:
            print(f"  Buy price improved from ${raw_buy_price:.3f} to ${buy_price:.3f} within profit gate")
        result = self.executor.place_buy_order(token_id, amount, price=buy_price)
        if not result.success:
            print(f"  Buy submit failed: {result.error}")
            return
        now = time.time()
        print(
            f"  Buy order pending: {result.order_id} | target {sig.side} "
            f"{result.shares:.0f} shares @ ${result.price or buy_price:.3f}; "
            f"checking every {self.pending_check_seconds:.1f}s; "
            f"cancel if unmatched after {self.pending_buy_max_seconds:.1f}s"
        )
        self.pending_buy = PendingOrder(
            kind="BUY",
            order_id=result.order_id,
            side=sig.side,
            token_id=token_id,
            window_ts=self.market.window_start,
            market_slug=self.market.slug,
            price=result.price or buy_price,
            shares=result.shares,
            amount_usd=result.amount_usd or amount,
            balance_before=result.balance_before,
            token_balance_before=result.token_balance_before,
            next_check_ts=now + self.pending_check_seconds,
            created_ts=now,
        )
        self.buy_count_in_window += 1
        self.last_trade_action_ts = now

    def _process_pending_buy(self) -> None:
        pending = self.pending_buy
        if not pending:
            return
        if not self.market or pending.window_ts != self.market.window_start:
            self._cancel_pending_buy("window changed")
            return
        now = time.time()
        if self.pending_buy_max_seconds > 0 and now - pending.created_ts >= self.pending_buy_max_seconds:
            self._cancel_pending_buy(f"unmatched after {self.pending_buy_max_seconds:.1f}s")
            return
        if self._pending_buy_should_cancel(pending):
            self._cancel_pending_buy("fair-value signal turned against buy")
            return
        if now < pending.next_check_ts:
            return
        pending.next_check_ts = now + self.pending_check_seconds
        result = self.executor.check_pending_buy(
            pending.order_id,
            pending.price,
            pending.shares,
            pending.token_id,
            pending.balance_before,
            pending.token_balance_before,
        )
        if not result:
            return
        self._activate_buy_result(pending, result, source="poll")

    def _activate_buy_result(self, pending: PendingOrder, result, source: str) -> None:
        with self._order_event_lock:
            if self.pending_buy and self.pending_buy.order_id == pending.order_id:
                self.pending_buy = None
            else:
                return
        confirm_elapsed = time.time() - pending.created_ts
        if result.shares < 1:
            print(
                f"  Buy verification returned {result.shares:.2f} shares after {confirm_elapsed:.2f}s; "
                "not activating a zero-share position"
            )
            self.executor.cancel_order(pending.order_id)
            self.last_trade_action_ts = time.time()
            return
        if str(result.status).upper() == "PARTIAL":
            print(
                f"  Buy partial confirmed after {confirm_elapsed:.2f}s via {source}: "
                f"{result.shares:.2f}/{pending.shares:.2f} shares @ ${result.price:.3f}; "
                "cancelling unfilled remainder and tracking partial position"
            )
            self.executor.cancel_order(pending.order_id)
        else:
            print(
                f"  Buy confirmed after {confirm_elapsed:.2f}s via {source}: "
                f"{result.shares:.2f}/{pending.shares:.2f} shares @ ${result.price:.3f}"
            )
        cost = self._net_buy_cost(result.price, result.shares) if result.dry_run else result.amount_usd
        self.position = Position(
            side=pending.side,
            token_id=pending.token_id,
            window_ts=pending.window_ts,
            market_slug=pending.market_slug,
            entry_price=result.price,
            shares=result.shares,
            cost=cost,
            opening_price=self.opening_price,
            entry_btc_price=self.current_btc_price,
            entry_ts=time.time(),
            entry_edge=max(0.0, self.last_signal.score if self.last_signal and self.last_signal.side == pending.side else 0.0),
            entry_delta_pct=((self.current_btc_price - self.opening_price) / self.opening_price * 100.0) if self.opening_price > 0 else 0.0,
            entry_elapsed_ms=self._model_elapsed_ms(time.time()),
        )
        if not self.current_window_had_trade:
            self.current_window_had_trade = True
            self.hour_windows_with_trade += 1
        print(f"  Buy position active: {pending.side} {result.shares:.2f} @ ${result.price:.3f}, cost ${cost:.2f}")
        self.telegram.strategy_trade_alert(
            "Window fair value",
            pending.side,
            result.price,
            cost,
            pending.market_slug,
            self.dry_run,
            self.realized_pnl,
            self._account_total_pnl(refresh=True),
        )

    def _pending_buy_should_cancel(self, pending: PendingOrder) -> bool:
        sig = self.last_signal
        if not sig:
            return False
        if sig.side != pending.side:
            return True
        required_profit = self._min_profit_for_buy_price(pending.price)
        if required_profit is None:
            return True
        pending_profit_5 = self._expected_profit_pct_after_fees(pending.price, sig.predictions[5])
        pending_profit_7 = self._expected_profit_pct_after_fees(pending.price, sig.predictions[7])
        return pending_profit_5 < required_profit or pending_profit_7 < 0

    def _attempt_pending_cancel(self, pending: PendingOrder, reason: str) -> bool:
        now = time.time()
        while pending.cancel_attempt_ts and now - pending.cancel_attempt_ts[0] >= 1.0:
            pending.cancel_attempt_ts.popleft()
        if pending.last_cancel_ts > 0 and now - pending.last_cancel_ts < self.pending_cancel_retry_seconds:
            return False
        if len(pending.cancel_attempt_ts) >= self.pending_cancel_max_per_second:
            return False

        pending.cancel_requested = True
        pending.cancel_reason = reason
        pending.cancel_attempts += 1
        pending.last_cancel_ts = now
        pending.cancel_attempt_ts.append(now)
        print(
            f"  Cancelling pending {pending.kind.lower()} {pending.order_id}: "
            f"{reason} (attempt {pending.cancel_attempts})"
        )
        ok = self.executor.cancel_order(pending.order_id)
        self.last_trade_action_ts = now
        if not ok:
            print(
                f"  Pending {pending.kind.lower()} cancel not confirmed; "
                f"will retry after {self.pending_cancel_retry_seconds:.3f}s"
            )
        return ok

    def _cancel_pending_buy(self, reason: str) -> None:
        if not self.pending_buy:
            return
        if self._attempt_pending_cancel(self.pending_buy, reason):
            self.pending_buy = None

    def _position_take_profit_enabled(self) -> bool:
        if not self.position or self.take_profit_sell_price <= 0:
            return False
        early_entry_cutoff_ms = max(0.0, WINDOW_SECONDS - self.late_entry_seconds) * 1000.0
        return self.position.entry_elapsed_ms <= early_entry_cutoff_ms

    def _maybe_take_profit_sell(self) -> bool:
        if not self.position or self.position.closed or self.pending_sell:
            return False
        if not self._position_take_profit_enabled():
            return False
        live_sell_price = self._live_sell_price(self.position.side, self.position.shares)
        if live_sell_price < self.take_profit_sell_price:
            return False
        entry_elapsed_seconds = self.position.entry_elapsed_ms / 1000.0
        reason = (
            f"take_profit_sell_price_{live_sell_price:.3f}_"
            f"gate_{self.take_profit_sell_price:.3f}_"
            f"entry_elapsed_{entry_elapsed_seconds:.1f}s"
        )
        print(
            f"\n[EXIT] {reason} {self.position.side}: sell ${live_sell_price:.3f}, "
            f"entry ${self.position.entry_price:.3f}, "
            f"entry_elapsed {entry_elapsed_seconds:.1f}s"
        )
        self._submit_sell(live_sell_price, reason)
        return self.pending_sell is not None

    def _maybe_sell_on_lag_drop(self) -> None:
        if not self.position or self.position.closed or not self.market:
            return
        if self.pending_sell:
            return
        held_seconds = time.time() - self.position.entry_ts
        if self.dry_run and self.dry_run_min_hold_seconds > 0 and held_seconds < self.dry_run_min_hold_seconds:
            return
        if self._maybe_take_profit_sell():
            return
        signal = self._held_side_projection()
        if not signal:
            return
        current = signal.current_mid
        fair_value = min(signal.predictions[5], signal.predictions[7])
        fair_profit = self._expected_profit_pct_after_fees(self.position.entry_price, fair_value)
        smoothed_sell_price = self._smoothed_live_sell_price(self.position.side, self.position.shares)
        live_sell_price = self._live_sell_price(self.position.side, self.position.shares)
        if smoothed_sell_price <= 0 or live_sell_price <= 0:
            return
        order_sell_price = min(smoothed_sell_price, live_sell_price)
        fair_revenue = self._net_sell_revenue(fair_value, self.position.shares)
        fair_principal_profit = (fair_revenue / self.position.cost - 1.0) if self.position.cost > 0 else 0.0
        principal_drawdown = max(0.0, -fair_principal_profit)
        principal_drawdown_limit = max(0.0, self.max_principal_drawdown_pct)
        if principal_drawdown_limit > 0 and principal_drawdown >= principal_drawdown_limit:
            reason = f"fair_principal_drawdown_{principal_drawdown:.1%}_limit_{principal_drawdown_limit:.1%}"
            print(
                f"\n[EXIT] {reason} {self.position.side}: sell ${order_sell_price:.3f}, "
                f"mid ${current:.3f}, fair ${fair_value:.3f}, "
                f"smoothed_sell ${smoothed_sell_price:.3f}, live_sell ${live_sell_price:.3f}, "
                f"fair_principal_profit {fair_principal_profit:.1%}, "
                f"fair_profit {fair_profit:.1%}, "
                f"peak {self.position.peak_unrealized_profit:.1%}"
            )
            self._submit_sell(order_sell_price, reason)
            return
        self.position.peak_unrealized_profit = max(self.position.peak_unrealized_profit, fair_profit)
        drawdown = self.position.peak_unrealized_profit - fair_profit
        drawdown_limit = self.position.peak_unrealized_profit * max(0.0, min(1.0, self.max_profit_drawdown_pct))
        if self.position.peak_unrealized_profit <= 0 or drawdown < drawdown_limit:
            return
        reason = f"fair_profit_drawdown_{drawdown:.1%}_from_peak_{self.position.peak_unrealized_profit:.1%}"
        print(
            f"\n[EXIT] {reason} {self.position.side}: sell ${order_sell_price:.3f}, "
            f"mid ${current:.3f}, fair ${fair_value:.3f}, "
            f"smoothed_sell ${smoothed_sell_price:.3f}, live_sell ${live_sell_price:.3f}, "
            f"fair_profit {fair_profit:.1%}, "
            f"peak {self.position.peak_unrealized_profit:.1%}"
        )
        self._submit_sell(order_sell_price, reason)

    def _submit_sell(self, sell_price: float, reason: str) -> None:
        if not self.position or not self.market:
            return
        result = self.executor.place_sell_order(self.position.token_id, self.position.shares, price=sell_price)
        if not result.success:
            print(f"  Sell submit failed/held: {result.error}")
            return
        now = time.time()
        print(
            f"  Sell order pending: {result.order_id} | {self.position.side} "
            f"{result.shares:.2f} shares @ ${result.price or sell_price:.3f}; "
            f"checking every {self.pending_check_seconds:.1f}s | {reason}"
        )
        self.pending_sell = PendingOrder(
            kind="SELL",
            order_id=result.order_id,
            side=self.position.side,
            token_id=self.position.token_id,
            window_ts=self.position.window_ts,
            market_slug=self.position.market_slug,
            price=result.price or sell_price,
            shares=result.shares,
            amount_usd=result.amount_usd,
            balance_before=result.balance_before,
            token_balance_before=result.token_balance_before,
            strategy_reason=reason,
            next_check_ts=now + self.pending_check_seconds,
            created_ts=now,
        )
        self.last_trade_action_ts = now

    def _process_pending_sell(self) -> None:
        pending = self.pending_sell
        if not pending:
            return
        now = time.time()
        if self.pending_sell_max_seconds > 0 and now - pending.created_ts >= self.pending_sell_max_seconds:
            self._cancel_pending_sell(f"unmatched after {self.pending_sell_max_seconds:.1f}s")
            return
        if self._pending_sell_should_cancel(pending):
            self._cancel_pending_sell("fair value says held side keeps rising")
            return
        if now < pending.next_check_ts:
            return
        pending.next_check_ts = now + self.pending_check_seconds
        result = self.executor.check_pending_sell(
            pending.order_id,
            pending.price,
            pending.shares,
            pending.token_id,
            pending.balance_before,
            pending.token_balance_before,
        )
        if not result:
            return
        self._apply_sell_result(pending, result, source="poll")

    def _apply_sell_result(self, pending: PendingOrder, result, source: str) -> None:
        with self._order_event_lock:
            if self.pending_sell and self.pending_sell.order_id == pending.order_id:
                self.pending_sell = None
            else:
                return
        self.pending_sell = None
        if not self.position:
            return
        confirm_elapsed = time.time() - pending.created_ts
        revenue = self._net_sell_revenue(result.price, result.shares) if result.dry_run else result.amount_usd
        sold_shares = min(result.shares, self.position.shares)
        sold_ratio = sold_shares / self.position.shares if self.position.shares > 0 else 1.0
        cost_basis = self.position.cost * sold_ratio
        profit = revenue - cost_basis
        self.realized_pnl += profit
        if str(result.status).upper() == "PARTIAL" and result.shares_remaining >= 1:
            self.position.shares = max(0.0, self.position.shares - sold_shares)
            self.position.cost = max(0.0, self.position.cost - cost_basis)
            self.executor.cancel_order(pending.order_id)
            print(
                f"  Sell partial confirmed after {confirm_elapsed:.2f}s via {source}: "
                f"sold {sold_shares:.2f}, remaining {self.position.shares:.2f}, "
                f"revenue ${revenue:.2f}, partial profit ${profit:+.2f}"
            )
            self.telegram.strategy_result_alert(
                "Window fair value partial sell",
                profit,
                self.realized_pnl,
                self._account_total_pnl(refresh=True),
            )
            return
        self._record_closed_trade(profit, self.position)
        self.position.closed = True
        print(
            f"  Sell confirmed after {confirm_elapsed:.2f}s via {source}: "
            f"{sold_shares:.2f} @ ${result.price:.3f}, profit ${profit:+.2f}"
        )
        self.telegram.strategy_result_alert(
            "Window fair value",
            profit,
            self.realized_pnl,
            self._account_total_pnl(refresh=True),
        )
        self.position = None

    def _pending_sell_should_cancel(self, pending: PendingOrder) -> bool:
        fixed_exit_prefixes = (
            "take_profit_",
            "fair_principal_drawdown_",
            "fair_profit_drawdown_",
        )
        if pending.strategy_reason.startswith(fixed_exit_prefixes):
            return False
        signal = self._held_side_projection()
        if not signal:
            return False
        current = signal.current_mid
        future = max(signal.predictions[5], signal.predictions[7])
        fair_profit = self._expected_profit_pct_after_fees(self.position.entry_price, future) if self.position else 0.0
        peak = self.position.peak_unrealized_profit if self.position else 0.0
        drawdown_limit = peak * max(0.0, min(1.0, self.max_profit_drawdown_pct))
        return peak > 0 and peak - fair_profit < drawdown_limit

    def _cancel_pending_sell(self, reason: str) -> None:
        if not self.pending_sell:
            return
        if self._attempt_pending_cancel(self.pending_sell, reason):
            self.pending_sell = None

    def _held_side_projection(self) -> Optional[LagSignal]:
        if not self.position or not self.market:
            return None
        up = self.poly_feed.get_price(self.market.token_id_up)
        down = self.poly_feed.get_price(self.market.token_id_down)
        up_mid, up_ask, up_bid = self._sell_smoothed_mid_ask_bid(up)
        down_mid, down_ask, down_bid = self._sell_smoothed_mid_ask_bid(down)
        if up_mid <= 0 or down_mid <= 0 or self.opening_price <= 0:
            return None
        elapsed_ms = self._model_elapsed_ms(time.time())
        binance_delta = self.current_btc_price - self.opening_price
        fair = self._fair_prices_from_btc_delta(binance_delta, elapsed_ms)
        if fair is None:
            return None
        up_fair, down_fair, avg_up, avg_down = fair
        side = self.position.side
        current = up_mid if side == "UP" else down_mid
        buy = up_ask if side == "UP" else down_ask
        sell = up_bid if side == "UP" else down_bid
        fair_price = up_fair if side == "UP" else down_fair
        prices = {5: fair_price, 7: fair_price, 9: fair_price, 11: fair_price}
        residuals = {5: 0.0, 7: 0.0, 9: 0.0, 11: 0.0}
        return LagSignal(
            side=side,
            current_mid=current,
            buy_price=buy,
            sell_price=sell,
            predictions=prices,
            residuals=residuals,
            profit_5=self._expected_profit_pct_after_fees(buy, prices[5]) if buy > 0 else 0.0,
            profit_7=self._expected_profit_pct_after_fees(buy, prices[7]) if buy > 0 else 0.0,
            profit_9=self._expected_profit_pct_after_fees(buy, prices[9]) if buy > 0 and 9 in prices else 0.0,
            score=0.0,
            elapsed_ms=elapsed_ms,
            binance_delta=binance_delta,
            reason="held_projection",
            avg_up_move=avg_up,
            avg_down_move=avg_down,
        )

    def _resolve_position(self) -> None:
        if not self.position or self.position.closed:
            return
        winner = None if self.dry_run else get_market_winner(self.period_minutes, self.position.window_ts, asset="btc")
        if not winner:
            winner = "UP" if self.current_btc_price >= self.position.opening_price else "DOWN"
        won = winner == self.position.side
        revenue = self.position.shares * 1.0 if won else 0.0
        profit = revenue - self.position.cost
        self.realized_pnl += profit
        self._record_closed_trade(profit, self.position)
        print(f"\n[RESOLVE] {self.position.side} winner {winner} | profit ${profit:+.2f}")
        self.telegram.strategy_result_alert(
            "Window fair value",
            profit,
            self.realized_pnl,
            self._account_total_pnl(refresh=True),
        )
        self.position.closed = True
        self.position = None

    def _mid_ask_bid(self, price: Optional[TokenPrice]) -> tuple[float, float, float]:
        if not price:
            return 0.0, 0.0, 0.0
        mid = price.mid if price.mid > 0 else 0.0
        return round(mid, 4), round(price.best_ask, 2) if price.best_ask > 0 else 0.0, round(price.best_bid, 2) if price.best_bid > 0 else 0.0

    def _sell_smoothed_mid_ask_bid(self, price: Optional[TokenPrice]) -> tuple[float, float, float]:
        if not price:
            return 0.0, 0.0, 0.0
        points = self.polymarket_sell_smooth_points
        mid = self._average_recent(price.mid_history, points) or (price.mid if price.mid > 0 else 0.0)
        ask = self._average_recent(price.ask_history, points) or price.best_ask
        bid = self._average_recent(price.bid_history, points) or price.best_bid
        return round(mid, 4), round(ask, 2) if ask > 0 else 0.0, round(bid, 2) if bid > 0 else 0.0

    def _token_id(self, side: str) -> str:
        if not self.market:
            return ""
        return self.market.token_id_up if side.upper() == "UP" else self.market.token_id_down

    def _live_buy_price(self, side: str) -> float:
        token_id = self._token_id(side)
        price = self.poly_feed.get_price(token_id)
        if price and price.best_ask > 0:
            return self._depth_adjusted_buy_price(token_id, price.best_ask)
        return round(self.executor.get_market_price(token_id, "BUY", self.trade_amount), 2)

    def _improved_buy_price(self, base_price: float, sig: LagSignal) -> float:
        if base_price <= 0 or self.buy_price_improve_ticks <= 0 or self.buy_price_improve_max <= 0:
            return base_price
        best_price = round(base_price, 2)
        max_steps = min(self.buy_price_improve_ticks, int(round(self.buy_price_improve_max / 0.01)))
        for step in range(1, max_steps + 1):
            candidate = round(base_price + 0.01 * step, 2)
            if candidate > self.max_buy_price:
                break
            required_profit = self._min_profit_for_buy_price(candidate)
            if required_profit is None:
                break
            profit_5 = self._expected_profit_pct_after_fees(candidate, sig.predictions[5])
            profit_7 = self._expected_profit_pct_after_fees(candidate, sig.predictions[7])
            if profit_5 < required_profit or profit_7 < 0:
                break
            best_price = candidate
        return best_price

    def _live_sell_price(self, side: str, shares: float) -> float:
        token_id = self._token_id(side)
        price = self.poly_feed.get_price(token_id)
        if price and price.best_bid > 0:
            depth_price = self._depth_adjusted_sell_price(token_id, shares, price.best_bid)
            return depth_price if depth_price > 0 else round(price.best_bid, 2)
        notional = max(5.0, shares * 0.5)
        return round(self.executor.get_market_price(token_id, "SELL", notional), 2)

    def _smoothed_live_sell_price(self, side: str, shares: float) -> float:
        token_id = self._token_id(side)
        price = self.poly_feed.get_price(token_id)
        if not price:
            return self._live_sell_price(side, shares)
        smoothed_bid = self._average_recent(price.bid_history, self.polymarket_sell_smooth_points)
        if smoothed_bid <= 0:
            return self._live_sell_price(side, shares)
        if price.bids:
            live_depth = self._depth_adjusted_sell_price(token_id, shares, price.best_bid)
            if live_depth > 0 and price.best_bid > 0:
                depth_gap = max(0.0, price.best_bid - live_depth)
                return round(max(0.01, smoothed_bid - depth_gap), 2)
        return round(smoothed_bid, 2)

    @staticmethod
    def _average_recent(values: list[float], points: int) -> float:
        clean = [float(value) for value in (values or [])[-max(1, points):] if float(value) > 0]
        if not clean:
            return 0.0
        return sum(clean) / len(clean)

    @staticmethod
    def _append_changed(values: deque, value: float) -> None:
        value = float(value)
        if not values or abs(float(values[-1]) - value) > 1e-9:
            values.append(value)

    def _depth_adjusted_buy_price(self, token_id: str, fallback_ask: float) -> float:
        price = self.poly_feed.get_price(token_id)
        if not price:
            return round(fallback_ask, 2) if fallback_ask > 0 else 0.0
        target_notional = max(self.trade_amount, self.trade_amount * max(1.0, self.orderbook_competition_mult))
        depth_price = self._price_for_buy_notional(price.asks, target_notional)
        if depth_price <= 0:
            return 0.0
        return round(depth_price, 2)

    def _depth_adjusted_sell_price(self, token_id: str, shares: float, fallback_bid: float) -> float:
        price = self.poly_feed.get_price(token_id)
        if not price:
            return round(fallback_bid, 2) if fallback_bid > 0 else 0.0
        target_shares = max(float(shares), float(shares) * max(1.0, self.orderbook_competition_mult))
        depth_price = self._price_for_sell_shares(price.bids, target_shares)
        if depth_price <= 0:
            return 0.0
        return round(depth_price, 2)

    @staticmethod
    def _price_for_buy_notional(asks: list[tuple[float, float]], target_notional: float) -> float:
        if target_notional <= 0:
            return 0.0
        cumulative = 0.0
        for price, size in sorted(asks or [], key=lambda item: item[0]):
            if price <= 0 or size <= 0:
                continue
            cumulative += price * size
            if cumulative >= target_notional:
                return price
        return 0.0

    @staticmethod
    def _price_for_sell_shares(bids: list[tuple[float, float]], target_shares: float) -> float:
        if target_shares <= 0:
            return 0.0
        cumulative = 0.0
        for price, size in sorted(bids or [], key=lambda item: item[0], reverse=True):
            if price <= 0 or size <= 0:
                continue
            cumulative += size
            if cumulative >= target_shares:
                return price
        return 0.0

    def _taker_fee_per_share(self, price: float) -> float:
        if not self.polymarket_taker_fees_enabled:
            return 0.0
        price = min(0.99, max(0.01, float(price)))
        return max(0.0, self.polymarket_taker_fee_rate) * price * (1.0 - price)

    def _net_buy_cost(self, price: float, shares: float) -> float:
        return shares * (price + self._taker_fee_per_share(price))

    def _net_sell_revenue(self, price: float, shares: float) -> float:
        return shares * max(0.0, price - self._taker_fee_per_share(price))

    def _expected_profit_pct_after_fees(self, buy_price: float, sell_price: float) -> float:
        buy_cost = buy_price + self._taker_fee_per_share(buy_price)
        if buy_cost <= 0:
            return 0.0
        sell_revenue = max(0.0, sell_price - self._taker_fee_per_share(sell_price))
        return (sell_revenue - buy_cost) / buy_cost

    def _load_buy_profit_tiers(self) -> list[tuple[float, float]]:
        raw = os.getenv(
            "LAG_MODEL_BUY_PROFIT_TIERS",
            "0.10:4.50,0.15:3.50,0.20:2.00,0.25:1.40,0.30:0.95,"
            "0.35:0.60,0.40:0.40,0.45:0.23,0.50:0.13,0.55:0.10,0.75:0.08",
        )
        tiers: list[tuple[float, float]] = []
        for item in raw.split(","):
            item = item.strip()
            if not item:
                continue
            if ":" not in item:
                print(f"[config] Ignoring invalid LAG_MODEL_BUY_PROFIT_TIERS item: {item}")
                continue
            upper_text, profit_text = [part.strip() for part in item.split(":", 1)]
            try:
                upper = float(upper_text)
                profit = self._parse_profit_value(profit_text)
            except ValueError:
                print(f"[config] Ignoring invalid LAG_MODEL_BUY_PROFIT_TIERS item: {item}")
                continue
            if upper <= 0 or profit < 0:
                print(f"[config] Ignoring invalid LAG_MODEL_BUY_PROFIT_TIERS item: {item}")
                continue
            tiers.append((upper, profit))
        if not tiers:
            return [(999.0, self.min_profit_pct)]
        return sorted(tiers, key=lambda pair: pair[0])

    @staticmethod
    def _parse_profit_value(value: str) -> float:
        value = value.strip()
        if value.endswith("%"):
            return float(value[:-1]) / 100.0
        return float(value)

    def _min_profit_for_buy_price(self, buy_price: float) -> Optional[float]:
        if buy_price <= 0:
            return None
        epsilon = 1e-9
        for upper, profit in self.buy_profit_tiers:
            if buy_price <= upper + epsilon:
                return profit
        return None

    def _format_buy_profit_tiers(self) -> str:
        parts = []
        lower = 0.0
        for upper, profit in self.buy_profit_tiers:
            if lower <= 0:
                parts.append(f"<={upper:.2f}:{profit:.0%}")
            else:
                parts.append(f"({lower:.2f},{upper:.2f}]:{profit:.0%}")
            lower = upper
        return ", ".join(parts)

    def _account_balance(self, refresh: bool = False) -> float:
        if self.dry_run:
            return self.session_start_balance + self.realized_pnl
        balance = self.executor.get_balance(refresh=refresh)
        if balance <= 0 and self.session_start_balance > 0:
            return self.session_start_balance + self.realized_pnl
        return balance

    def _account_total_pnl(self, refresh: bool = False) -> float:
        if self.session_start_balance <= 0:
            return self.realized_pnl
        return self._account_balance(refresh=refresh) - self.session_start_balance

    def _print_status(self) -> None:
        now = time.time()
        if now - self.last_status_ts < self.status_interval:
            return
        self.last_status_ts = now
        self._maybe_send_hourly_summary(now)
        pos = "flat"
        if self.pending_buy:
            pos = f"pending buy {self.pending_buy.side} @ ${self.pending_buy.price:.2f}"
        elif self.pending_sell:
            pos = f"pending sell {self.pending_sell.side} @ ${self.pending_sell.price:.2f}"
        elif self.position and not self.position.closed:
            pos = f"{self.position.side} {self.position.shares:.0f} @ ${self.position.entry_price:.2f}"
        seconds = self.market.seconds_remaining if self.market else 0.0
        print(f"[status] BTC ${self.current_btc_price:,.2f} | T-{seconds:.0f}s | {pos} | P&L ${self.realized_pnl:+.2f}")

    def _record_closed_trade(self, profit: float, position: Position) -> None:
        self.total_trades += 1
        self.hour_trades += 1
        self.hour_pnl += profit
        self.hour_edge_sum += position.entry_edge
        self.hour_delta_sum += position.entry_delta_pct
        if profit > 0:
            self.total_wins += 1
            self.hour_wins += 1
        else:
            self.total_losses += 1
            self.hour_losses += 1
        self.hour_best_trade = profit if self.hour_best_trade is None else max(self.hour_best_trade, profit)
        self.hour_worst_trade = profit if self.hour_worst_trade is None else min(self.hour_worst_trade, profit)

    def _maybe_send_hourly_summary(self, now: float) -> None:
        if self.hourly_summary_interval <= 0:
            return
        if now - self.last_hourly_summary_ts < self.hourly_summary_interval:
            return
        self.last_hourly_summary_ts = now
        hourly = self._hourly_summary_payload()
        overall = self._overall_summary_payload()
        self._print_hourly_summary(hourly, overall)
        self.telegram.hourly_summary(hourly, overall)
        self._reset_hourly_stats()

    def _hourly_summary_payload(self) -> dict:
        win_rate = (self.hour_wins / self.hour_trades * 100.0) if self.hour_trades else 0.0
        return {
            "trades": self.hour_trades,
            "wins": self.hour_wins,
            "losses": self.hour_losses,
            "win_rate": win_rate,
            "pnl": self.hour_pnl,
            "avg_edge": (self.hour_edge_sum / self.hour_trades) if self.hour_trades else 0.0,
            "avg_delta": (self.hour_delta_sum / self.hour_trades) if self.hour_trades else 0.0,
            "best_trade": self.hour_best_trade or 0.0,
            "worst_trade": self.hour_worst_trade or 0.0,
            "windows_seen": self.hour_windows_seen,
            "windows_skipped": max(0, self.hour_windows_seen - self.hour_windows_with_trade),
        }

    def _overall_summary_payload(self) -> dict:
        win_rate = (self.total_wins / self.total_trades * 100.0) if self.total_trades else 0.0
        account_balance = self._account_balance(refresh=True)
        account_pnl = account_balance - self.session_start_balance if self.session_start_balance > 0 else self.realized_pnl
        return {
            "total_trades": self.total_trades,
            "wins": self.total_wins,
            "losses": self.total_losses,
            "win_rate": win_rate,
            "pnl": account_pnl,
            "strategy_pnl": self.realized_pnl,
            "bankroll": account_balance,
        }

    def _print_hourly_summary(self, hourly: dict, overall: dict) -> None:
        print("\n" + "=" * 56)
        print("[HOURLY SUMMARY]")
        print(
            f"This hour: trades {hourly['trades']} "
            f"({hourly['wins']}W/{hourly['losses']}L), "
            f"WR {hourly['win_rate']:.1f}%, P&L ${hourly['pnl']:+.2f}"
        )
        if hourly["trades"] > 0:
            print(
                f"Avg edge {hourly['avg_edge']:.1%}, avg BTC delta {hourly['avg_delta']:.3f}%, "
                f"best ${hourly['best_trade']:+.2f}, worst ${hourly['worst_trade']:+.2f}"
            )
        print(f"Windows seen {hourly['windows_seen']}, skipped {hourly['windows_skipped']}")
        print(
            f"Overall: trades {overall['total_trades']} "
            f"({overall['wins']}W/{overall['losses']}L), "
            f"WR {overall['win_rate']:.1f}%, P&L ${overall['pnl']:+.2f}, "
            f"bankroll ${overall['bankroll']:.2f}"
        )
        print("=" * 56)

    def _reset_hourly_stats(self) -> None:
        self.hour_trades = 0
        self.hour_wins = 0
        self.hour_losses = 0
        self.hour_pnl = 0.0
        self.hour_edge_sum = 0.0
        self.hour_delta_sum = 0.0
        self.hour_best_trade = None
        self.hour_worst_trade = None
        self.hour_windows_seen = 0
        self.hour_windows_with_trade = 0

    def _safe_to_stop(self) -> bool:
        return not (
            self.pending_buy
            or self.pending_sell
            or (self.position and not self.position.closed)
        )

    def _shutdown_state_text(self) -> str:
        parts = []
        if self.pending_buy:
            parts.append(f"pending BUY {self.pending_buy.side} @ ${self.pending_buy.price:.2f}")
        if self.pending_sell:
            parts.append(f"pending SELL {self.pending_sell.side} @ ${self.pending_sell.price:.2f}")
        if self.position and not self.position.closed:
            parts.append(f"position {self.position.side} {self.position.shares:.2f} @ ${self.position.entry_price:.2f}")
        return "; ".join(parts) if parts else "flat"

    def _maybe_complete_shutdown(self) -> None:
        if not self.shutdown_requested:
            return
        if self.force_shutdown_requested:
            print("[shutdown] Force shutdown requested; stopping services now")
            self.running = False
            return
        if self._safe_to_stop():
            print("[shutdown] No pending order or open position; stopping bot")
            self.running = False
            return
        now = time.time()
        if now - self.last_shutdown_wait_log_ts >= 10.0:
            self.last_shutdown_wait_log_ts = now
            print(f"[shutdown] Waiting for active trading lifecycle to finish: {self._shutdown_state_text()}")

    def _handle_shutdown(self, *_):
        if self.shutdown_requested:
            print("\n[shutdown] Second stop signal received; force shutdown will stop immediately")
            self.force_shutdown_requested = True
            return
        print("\n[shutdown] Stop requested; disabling new buys and waiting for pending orders/positions to finish")
        self.shutdown_requested = True
        self.buy_observation = None
        self._save_fair_value_state()
        self._maybe_complete_shutdown()

    def _stop_services(self) -> None:
        self.price_feed.stop()
        self.poly_feed.stop()
        if self.user_feed:
            self.user_feed.stop()
        if self.raw_recorder.enabled:
            self.raw_recorder.stop()


def main() -> None:
    PolyBot().start()


if __name__ == "__main__":
    main()
