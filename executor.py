"""Order executor for Polymarket CLOB v2.

This module keeps the bot-facing Executor API stable while using
py-clob-client-v2 underneath. The v2 client signs current Exchange V2 orders
and retries once if the CLOB reports an order-version mismatch.
"""

import os
import re
import time
from dataclasses import dataclass
from typing import Optional

from dotenv import load_dotenv

from py_clob_client_v2 import (
    ApiCreds,
    AssetType,
    BalanceAllowanceParams,
    ClobClient,
    MarketOrderArgs,
    OrderArgs,
    OrderPayload,
    OrderType,
    PartialCreateOrderOptions,
    SignatureTypeV2,
)
from py_clob_client_v2.constants import POLYGON


load_dotenv()

FILLED = "FILLED"
PARTIAL = "PARTIAL"
REJECTED = "REJECTED"
FAILED = "FAILED"

MIN_SHARES = 1.0
MIN_AMOUNT_USD = 1.0
DEFAULT_MAX_BUY_PRICE = 0.90
POLY_MIN_NOTIONAL = 5.0
BUY_VERIFY_ATTEMPTS = 8
BUY_VERIFY_DELAY_SECONDS = 3.0
BUY_RETRY_BUFFER_USD = 0.05
BALANCE_REFRESH_MIN_INTERVAL = float(os.getenv("BALANCE_SYNC_INTERVAL_SECONDS", "300"))
BALANCE_REFRESH_BACKOFF_SECONDS = 90.0

DEFAULT_TICK_SIZE = "0.01"
DEFAULT_NEG_RISK = False


@dataclass
class OrderResult:
    success: bool
    order_id: str = ""
    status: str = FAILED
    side: str = ""
    price: float = 0.0
    amount_usd: float = 0.0
    shares: float = 0.0
    shares_remaining: float = 0.0
    token_id: str = ""
    error: str = ""
    dry_run: bool = True
    balance_before: float = 0.0
    token_balance_before: Optional[float] = None


def calculate_order_size(price: float, max_usd: float) -> tuple[float, float]:
    """Return whole shares and clean USD spend for a limit buy."""
    if price <= 0 or max_usd <= 0:
        return 0.0, 0.0

    price_cents = round(price * 100)
    max_usd_cents = int(max_usd * 100)
    if price_cents <= 0:
        return 0.0, 0.0

    max_shares = max_usd_cents // price_cents
    min_notional_cents = int(POLY_MIN_NOTIONAL * 100)
    min_notional_shares = (min_notional_cents + price_cents - 1) // price_cents
    min_required_shares = max(int(MIN_SHARES), min_notional_shares)

    # Whole-share rounding can turn a $5 Kelly budget into a sub-$5 notional
    # at high prices. Round up to the smallest valid Polymarket notional.
    shares = int(max(max_shares, min_required_shares))
    spend = shares * price_cents / 100.0
    return float(shares), spend


def _env(name: str) -> str:
    return os.getenv(name, "").strip()


def max_buy_price() -> float:
    raw = os.getenv("MAX_BUY_PRICE", str(DEFAULT_MAX_BUY_PRICE))
    raw = str(raw).split("#", 1)[0].strip()
    try:
        cap = float(raw)
        if cap <= 0 or cap >= 1:
            print(f"[executor] Invalid MAX_BUY_PRICE={raw!r}; using ${DEFAULT_MAX_BUY_PRICE:.2f}")
            return DEFAULT_MAX_BUY_PRICE
        return cap
    except ValueError:
        print(f"[executor] Invalid MAX_BUY_PRICE={raw!r}; using ${DEFAULT_MAX_BUY_PRICE:.2f}")
        return DEFAULT_MAX_BUY_PRICE


def _load_manual_api_creds() -> Optional[ApiCreds]:
    mode = _env("CLOB_CREDS_MODE").lower() or "auto"
    key = _env("CLOB_API_KEY")
    secret = _env("CLOB_SECRET")
    passphrase = _env("CLOB_PASS_PHRASE")

    if mode in {"auto", "derive", "wallet"}:
        if key or secret or passphrase:
            print(
                "[executor] Ignoring CLOB_API_KEY/CLOB_SECRET/CLOB_PASS_PHRASE "
                "because CLOB_CREDS_MODE=auto. Wallet-derived CLOB creds will be used."
            )
        return None

    if mode not in {"manual", "env"}:
        raise ValueError(
            "CLOB_CREDS_MODE must be 'auto' or 'manual' "
            f"(got {mode!r})"
        )

    missing = [
        name for name, value in (
            ("CLOB_API_KEY", key),
            ("CLOB_SECRET", secret),
            ("CLOB_PASS_PHRASE", passphrase),
        )
        if not value
    ]
    if missing:
        raise ValueError(
            "CLOB_CREDS_MODE=manual requires all CLOB API credential fields: "
            + ", ".join(missing)
        )

    return ApiCreds(api_key=key, api_secret=secret, api_passphrase=passphrase)


def _redacted(value: str) -> str:
    if not value:
        return "<empty>"
    if len(value) <= 8:
        return "***"
    return f"{value[:4]}...{value[-4:]}"


def _creds_mode() -> str:
    return _env("CLOB_CREDS_MODE").lower() or "auto"


def _uses_possible_relayer_keys() -> bool:
    return bool(_env("RELAYER_API_KEY") or _env("RELAYER_API_KEY_ADDRESS"))


def _manual_creds_are_configured() -> bool:
    return bool(_env("CLOB_API_KEY") or _env("CLOB_SECRET") or _env("CLOB_PASS_PHRASE"))


def _print_auth_hint() -> None:
    if _uses_possible_relayer_keys():
        print(
            "[executor] Note: RELAYER_API_KEY is for gasless relayer operations, "
            "not CLOB order auth. This bot derives CLOB L2 creds from PRIVATE_KEY "
            "unless CLOB_CREDS_MODE=manual."
        )
    if _manual_creds_are_configured() and _creds_mode() == "manual":
        print(
            "[executor] Using manual CLOB API credentials: "
            f"key={_redacted(_env('CLOB_API_KEY'))}, "
            f"secret={_redacted(_env('CLOB_SECRET'))}, "
            f"passphrase={_redacted(_env('CLOB_PASS_PHRASE'))}"
        )
    elif _creds_mode() in {"auto", "derive", "wallet"}:
        print("[executor] CLOB creds mode: auto (derive from PRIVATE_KEY)")


def _load_api_creds() -> Optional[ApiCreds]:
    """Backward-compatible wrapper for callers/tests that still import it."""
    return _load_manual_api_creds()


def _friendly_error(exc: Exception) -> str:
    message = str(exc)
    if "order signer address has to be the address of the API KEY" in message:
        return (
            f"{message} | Fix: set CLOB_CREDS_MODE=auto and clear "
            "CLOB_API_KEY/CLOB_SECRET/CLOB_PASS_PHRASE, then run "
            "`python scripts/check_clob_config.py --build-order`."
        )
    return message


def _balance_from_not_enough_error(message: str) -> float:
    match = re.search(r"balance:\s*(\d+)", message)
    if not match:
        return 0.0
    try:
        return float(match.group(1)) / 1e6
    except Exception:
        return 0.0


def _not_enough_balance_parts(message: str) -> Optional[dict]:
    if "not enough balance" not in message.lower():
        return None
    patterns = {
        "balance": r"balance:\s*(\d+)",
        "active_orders": r"sum of active orders:\s*(\d+)",
        "matched_orders": r"sum of matched orders:\s*(\d+)",
        "order_amount": r"order amount(?: \(inc\. fees\))?:\s*(\d+)",
    }
    parts = {}
    for key, pattern in patterns.items():
        match = re.search(pattern, message, re.IGNORECASE)
        parts[key] = float(match.group(1)) / 1e6 if match else 0.0
    return parts


def _signature_type(value: int) -> SignatureTypeV2:
    try:
        return SignatureTypeV2(int(value))
    except Exception:
        return SignatureTypeV2.EOA


def _order_options() -> PartialCreateOrderOptions:
    return PartialCreateOrderOptions(
        tick_size=DEFAULT_TICK_SIZE,
        neg_risk=DEFAULT_NEG_RISK,
    )


class Executor:
    def __init__(
        self,
        private_key: str,
        safe_address: str = "",
        dry_run: bool = True,
        signature_type: int = 0,
        funder_address: str = "",
    ):
        self.dry_run = dry_run
        self.private_key = private_key
        self.funder_address = funder_address or safe_address
        self.signature_type = _signature_type(signature_type)
        self.client: Optional[ClobClient] = None
        self._initialized = False
        self._balance_refresh_last: dict[str, float] = {}
        self._balance_refresh_blocked_until: float = 0.0
        self._balance_cache_value: float = 0.0
        self._balance_cache_ts: float = 0.0

        if self.signature_type == SignatureTypeV2.POLY_1271:
            print(
                "[executor] SIGNATURE_TYPE=3 (POLY_1271 deposit wallet). "
                "Requires py-clob-client-v2>=1.0.1rc1 for correct wrapped order signatures."
            )

    def initialize(self) -> bool:
        try:
            _print_auth_hint()
            manual_creds = _load_manual_api_creds()
            self.client = ClobClient(
                host="https://clob.polymarket.com",
                key=self.private_key,
                chain_id=POLYGON,
                creds=manual_creds,
                funder=self.funder_address or None,
                signature_type=self.signature_type,
            )
            if self.client.creds is None:
                self.client.set_api_creds(self.client.create_or_derive_api_key())

            self._initialized = True
            print(f"[executor] Initialized ({'DRY RUN' if self.dry_run else 'LIVE'})")
            print(f"[executor] Max buy price: ${max_buy_price():.2f}")
            print(f"[executor] Address: {self.client.get_address()}")
            print(f"[executor] Funder: {self.funder_address or self.client.get_address()}")
            print(f"[executor] Signature type: {int(self.signature_type)} ({self.signature_type.name})")

            try:
                self.client.update_balance_allowance(
                    BalanceAllowanceParams(asset_type=AssetType.COLLATERAL)
                )
            except Exception as e:
                print(f"[executor] Balance cache update warning: {e}")
            return True
        except Exception as e:
            print(f"[executor] Init failed: {e}")
            return False

    def get_api_creds(self) -> Optional[ApiCreds]:
        if not self.client:
            return None
        return self.client.creds

    def _refresh_balance_allowance(self, params: BalanceAllowanceParams, cache_key: str, force: bool = False) -> bool:
        now = time.time()
        if now < self._balance_refresh_blocked_until:
            return False
        if not force and now - self._balance_refresh_last.get(cache_key, 0.0) < BALANCE_REFRESH_MIN_INTERVAL:
            return False
        try:
            self.client.update_balance_allowance(params)
            self._balance_refresh_last[cache_key] = now
            return True
        except Exception as e:
            err = str(e).lower()
            if "429" in err or "1015" in err or "access denied" in err:
                self._balance_refresh_blocked_until = now + BALANCE_REFRESH_BACKOFF_SECONDS
                print(
                    f"[executor] Balance refresh rate-limited; backing off "
                    f"{BALANCE_REFRESH_BACKOFF_SECONDS:.0f}s"
                )
            else:
                print(f"[executor] Balance refresh warning: {_friendly_error(e)}")
            return False

    def get_balance(self, refresh: bool = False) -> float:
        if not self._initialized:
            return 0.0
        now = time.time()
        if not refresh and self._balance_cache_value > 0 and now - self._balance_cache_ts < BALANCE_REFRESH_MIN_INTERVAL:
            return self._balance_cache_value
        try:
            params = BalanceAllowanceParams(asset_type=AssetType.COLLATERAL)
            if refresh:
                self._refresh_balance_allowance(params, "collateral", force=True)
            bal = self.client.get_balance_allowance(params)
            value = float(bal.get("balance", 0)) / 1e6
            self._balance_cache_value = value
            self._balance_cache_ts = now
            return value
        except Exception as e:
            print(f"[executor] Balance check failed: {e}")
            return self._balance_cache_value if self._balance_cache_value > 0 else 0.0

    def _get_token_balance_optional(self, token_id: str, refresh: bool = False) -> Optional[float]:
        """Return the wallet's conditional-token balance, or None if unavailable."""
        if not self._initialized or not token_id:
            return None
        try:
            conditional_type = getattr(AssetType, "CONDITIONAL", None)
            if conditional_type is None:
                return None
            try:
                params = BalanceAllowanceParams(
                    asset_type=conditional_type,
                    token_id=str(token_id),
                )
            except TypeError:
                params = BalanceAllowanceParams(
                    asset_type=conditional_type,
                    asset_id=str(token_id),
                )
            if refresh:
                self._refresh_balance_allowance(params, f"conditional:{token_id}", force=True)
            bal = self.client.get_balance_allowance(params)
            raw = bal.get("balance", 0) if isinstance(bal, dict) else 0
            return float(raw) / 1e6
        except Exception as e:
            print(f"[executor] Token balance check failed: {e}")
            return None

    def get_token_balance(self, token_id: str, refresh: bool = True) -> float:
        """Return the wallet's conditional-token balance for one outcome token."""
        balance = self._get_token_balance_optional(token_id, refresh=refresh)
        if balance is None:
            return 0.0
        return balance

    def get_market_price(self, token_id: str, side: str, amount_usd: float) -> float:
        if not self._initialized:
            return 0.0
        try:
            price = self.client.calculate_market_price(
                token_id=token_id,
                side=side,
                amount=amount_usd,
                order_type=OrderType.GTC,
            )
            return float(price) if price else 0.0
        except Exception as e:
            err = str(e).lower()
            if "no match" not in err and "none" not in err and "no orderbook exists" not in err:
                print(f"[executor] Price check failed: {e}")
            return 0.0

    def _shares_within_budget(self, price: float, budget_usd: float) -> tuple[float, float]:
        if price <= 0 or budget_usd <= 0:
            return 0.0, 0.0
        price_cents = round(price * 100)
        budget_cents = int(max(0.0, budget_usd) * 100)
        if price_cents <= 0:
            return 0.0, 0.0
        shares = budget_cents // price_cents
        if shares < 1:
            return 0.0, 0.0
        spend = shares * price_cents / 100.0
        return float(shares), spend

    def _retry_buy_after_balance_error(
        self,
        error_message: str,
        token_id: str,
        market_price: float,
        original_shares: float,
        original_clean_amount: float,
        balance_before: float,
        token_balance_before: Optional[float],
    ) -> Optional[OrderResult]:
        parts = _not_enough_balance_parts(error_message)
        if not parts:
            return None

        free_balance = max(
            0.0,
            parts["balance"] - parts["active_orders"] - parts["matched_orders"],
        )
        fee_multiplier = 1.0
        if parts["order_amount"] > 0 and original_clean_amount > 0:
            fee_multiplier = max(1.0, parts["order_amount"] / original_clean_amount)

        retry_budget = max(0.0, free_balance / fee_multiplier - BUY_RETRY_BUFFER_USD)
        retry_shares, retry_amount = self._shares_within_budget(market_price, retry_budget)
        retry_shares = min(retry_shares, max(0.0, original_shares - 1.0))
        retry_amount = retry_shares * round(market_price * 100) / 100.0

        if retry_amount < POLY_MIN_NOTIONAL or retry_shares < 1:
            print(
                f"  [order] Balance available after active orders is only "
                f"${free_balance:.2f}; below ${POLY_MIN_NOTIONAL:.0f} buy minimum"
            )
            return OrderResult(
                success=False, status=REJECTED,
                error=(
                    f"Insufficient available balance after active orders: "
                    f"${free_balance:.2f}"
                ),
                side="BUY", price=market_price, token_id=token_id[:16] + "...",
            )

        print(
            f"  [order] Balance reserved in active orders: ${parts['active_orders']:.2f}; "
            f"retrying smaller buy {int(retry_shares)} shares for ${retry_amount:.2f}"
        )
        try:
            result = self.client.create_and_post_order(
                order_args=OrderArgs(
                    token_id=token_id,
                    price=market_price,
                    size=float(int(retry_shares)),
                    side="BUY",
                ),
                options=_order_options(),
                order_type=OrderType.GTC,
            )
            order_id = result.get("orderID", "")
            if not order_id:
                return OrderResult(
                    success=False, status=REJECTED,
                    error=f"No orderID on retry: {result}", side="BUY",
                    price=market_price, token_id=token_id[:16] + "...",
                )
            time.sleep(5)
            return self._verify_buy_via_balance(
                order_id, market_price, float(retry_shares), token_id,
                balance_before, token_balance_before,
            )
        except Exception as retry_error:
            return OrderResult(
                success=False, status=FAILED, error=_friendly_error(retry_error),
                side="BUY", price=market_price, token_id=token_id[:16] + "...",
            )

    def buy(self, token_id: str, amount_usd: float, price: float = 0.0) -> OrderResult:
        """Buy via v2 resting limit order with integer shares."""
        amount_usd = round(float(amount_usd), 2)
        if amount_usd < MIN_AMOUNT_USD:
            return OrderResult(
                success=False, status=REJECTED,
                error=f"Amount ${amount_usd:.2f} below min", side="BUY",
            )

        if self.dry_run:
            sim_price = round(price, 2) if price > 0 else 0.55
            return OrderResult(
                success=True, order_id=f"DRY-{int(time.time())}",
                status=FILLED, side="BUY", price=sim_price,
                amount_usd=amount_usd, shares=amount_usd / sim_price,
                token_id=token_id[:16] + "...", dry_run=True,
            )

        if not self._initialized:
            return OrderResult(success=False, status=FAILED, error="Not initialized")

        if price > 0:
            market_price = round(price, 2)
        else:
            market_price = self.get_market_price(token_id, "BUY", amount_usd)
            if market_price <= 0:
                return OrderResult(
                    success=False, status=FAILED,
                    error="Could not get market price", side="BUY",
                    token_id=token_id[:16] + "...",
                )
            market_price = round(market_price, 2)

        cap_price = max_buy_price()
        if market_price > cap_price:
            return OrderResult(
                success=False, status=REJECTED,
                error=f"Price ${market_price:.3f} > cap ${cap_price:.2f}",
                side="BUY", price=market_price, token_id=token_id[:16] + "...",
            )

        shares, clean_amount = calculate_order_size(market_price, amount_usd)
        if shares < 1 or clean_amount <= 0:
            return OrderResult(
                success=False, status=REJECTED,
                error=f"Can't afford 1 share at ${market_price:.3f} within ${amount_usd:.2f}",
                side="BUY", price=market_price, token_id=token_id[:16] + "...",
            )
        if clean_amount < POLY_MIN_NOTIONAL:
            return OrderResult(
                success=False, status=REJECTED,
                error=f"Amount ${clean_amount:.2f} < ${POLY_MIN_NOTIONAL:.0f} min",
                side="BUY", price=market_price, token_id=token_id[:16] + "...",
            )

        print(f"  [order] Market price: ${market_price:.3f}/share "
              f"-> {int(shares)} shares for ${clean_amount:.2f}")

        balance_before = self.get_balance()
        token_balance_before = self._get_token_balance_optional(token_id)
        try:
            result = self.client.create_and_post_order(
                order_args=OrderArgs(
                    token_id=token_id,
                    price=market_price,
                    size=float(int(shares)),
                    side="BUY",
                ),
                options=_order_options(),
                order_type=OrderType.GTC,
            )

            order_id = result.get("orderID", "")
            if not order_id:
                return OrderResult(
                    success=False, status=REJECTED,
                    error=f"No orderID: {result}", side="BUY", price=market_price,
                    token_id=token_id[:16] + "...",
                )

            time.sleep(5)
            return self._verify_buy_via_balance(
                order_id, market_price, float(shares), token_id,
                balance_before, token_balance_before,
            )
        except Exception as e:
            time.sleep(3)
            balance_after = self.get_balance(refresh=True)
            spent = balance_before - balance_after if balance_before > 0 else 0
            if spent > 1.0:
                token_balance_after = self._get_token_balance_optional(token_id, refresh=True)
                token_delta = (
                    max(0.0, token_balance_after - token_balance_before)
                    if token_balance_before is not None and token_balance_after is not None
                    else None
                )
                actual_shares = token_delta if token_delta is not None and token_delta > 0 else (
                    spent / market_price if token_delta is None and market_price > 0 else 0
                )
                if actual_shares < 1:
                    return OrderResult(
                        success=False, order_id="ghost-buy-unverified",
                        status=FAILED, side="BUY", price=market_price,
                        amount_usd=spent, shares=0.0,
                        error="USDC balance dropped but token balance/order fill not verified",
                        token_id=token_id[:16] + "...", dry_run=False,
                    )
                print(f"  [order] Ghost buy: balance dropped ${spent:.2f} despite error")
                return OrderResult(
                    success=True, order_id="ghost-buy",
                    status=FILLED if actual_shares >= shares - 0.001 else PARTIAL,
                    side="BUY", price=market_price,
                    amount_usd=spent, shares=actual_shares,
                    token_id=token_id[:16] + "...", dry_run=False,
                )
            retry_result = self._retry_buy_after_balance_error(
                str(e), token_id, market_price, shares, clean_amount,
                balance_before, token_balance_before,
            )
            if retry_result is not None:
                return retry_result
            return OrderResult(
                success=False, status=FAILED, error=_friendly_error(e),
                side="BUY", price=market_price, token_id=token_id[:16] + "...",
            )

    def place_buy_order(self, token_id: str, amount_usd: float, price: float = 0.0) -> OrderResult:
        """Post a buy order and return immediately; caller verifies/cancels later."""
        amount_usd = round(float(amount_usd), 2)
        if amount_usd < MIN_AMOUNT_USD:
            return OrderResult(
                success=False, status=REJECTED,
                error=f"Amount ${amount_usd:.2f} below min", side="BUY",
            )

        if price > 0:
            market_price = round(price, 2)
        else:
            market_price = 0.55 if self.dry_run else self.get_market_price(token_id, "BUY", amount_usd)
            if market_price <= 0:
                return OrderResult(
                    success=False, status=FAILED,
                    error="Could not get market price", side="BUY",
                    token_id=token_id[:16] + "...",
                )
            market_price = round(market_price, 2)

        cap_price = max_buy_price()
        if market_price > cap_price:
            return OrderResult(
                success=False, status=REJECTED,
                error=f"Price ${market_price:.3f} > cap ${cap_price:.2f}",
                side="BUY", price=market_price, token_id=token_id[:16] + "...",
            )

        shares, clean_amount = calculate_order_size(market_price, amount_usd)
        if shares < 1 or clean_amount <= 0:
            return OrderResult(
                success=False, status=REJECTED,
                error=f"Can't afford 1 share at ${market_price:.3f} within ${amount_usd:.2f}",
                side="BUY", price=market_price, token_id=token_id[:16] + "...",
            )
        if clean_amount < POLY_MIN_NOTIONAL:
            return OrderResult(
                success=False, status=REJECTED,
                error=f"Amount ${clean_amount:.2f} < ${POLY_MIN_NOTIONAL:.0f} min",
                side="BUY", price=market_price, token_id=token_id[:16] + "...",
            )

        print(f"  [order] Posting buy: ${market_price:.3f}/share "
              f"-> {int(shares)} shares for ${clean_amount:.2f}")

        if self.dry_run:
            return OrderResult(
                success=True, order_id=f"DRY-{int(time.time() * 1000)}",
                status="PENDING", side="BUY", price=market_price,
                amount_usd=clean_amount, shares=shares,
                token_id=token_id[:16] + "...", dry_run=True,
            )

        if not self._initialized:
            return OrderResult(success=False, status=FAILED, error="Not initialized")

        balance_before = self.get_balance()
        token_balance_before = self._get_token_balance_optional(token_id)
        try:
            result = self.client.create_and_post_order(
                order_args=OrderArgs(
                    token_id=token_id,
                    price=market_price,
                    size=float(int(shares)),
                    side="BUY",
                ),
                options=_order_options(),
                order_type=OrderType.GTC,
            )
            order_id = result.get("orderID", "")
            if not order_id:
                return OrderResult(
                    success=False, status=REJECTED,
                    error=f"No orderID: {result}", side="BUY", price=market_price,
                    token_id=token_id[:16] + "...",
                )
            return OrderResult(
                success=True, order_id=order_id, status="PENDING",
                side="BUY", price=market_price, amount_usd=clean_amount,
                shares=float(shares), token_id=token_id[:16] + "...",
                dry_run=False, balance_before=balance_before,
                token_balance_before=token_balance_before,
            )
        except Exception as e:
            return OrderResult(
                success=False, status=FAILED, error=_friendly_error(e),
                side="BUY", price=market_price, token_id=token_id[:16] + "...",
                balance_before=balance_before,
                token_balance_before=token_balance_before,
            )

    def check_pending_buy(
        self,
        order_id: str,
        price: float,
        shares: float,
        token_id: str,
        balance_before: float = 0.0,
        token_balance_before: Optional[float] = None,
    ) -> Optional[OrderResult]:
        """Single non-blocking verification pass for a previously posted buy."""
        if self.dry_run:
            return OrderResult(
                success=True, order_id=order_id, status=FILLED,
                side="BUY", price=price, amount_usd=shares * price,
                shares=shares, token_id=token_id[:16] + "...", dry_run=True,
            )

        if balance_before > 0:
            balance_after = self.get_balance(refresh=True)
            spent = balance_before - balance_after
            token_balance_after = self._get_token_balance_optional(token_id, refresh=True)
            token_delta = (
                max(0.0, token_balance_after - token_balance_before)
                if token_balance_before is not None and token_balance_after is not None
                else None
            )
            if spent > 0.50 or (token_delta is not None and token_delta > 0):
                actual_shares = token_delta if token_delta is not None and token_delta > 0 else (
                    spent / price if spent > 0.50 and price > 0 else shares
                )
                actual_spent = spent if spent > 0.10 else actual_shares * price
                status = FILLED if actual_shares >= shares - 0.001 else PARTIAL
                return OrderResult(
                    success=True, order_id=order_id, status=status,
                    side="BUY", price=price, amount_usd=actual_spent,
                    shares=actual_shares, token_id=token_id[:16] + "...",
                    dry_run=False,
                )

        fill = self._check_order(order_id)
        if not fill:
            return None
        matched = self._extract_fill(fill, price)
        if not matched:
            return None
        status = FILLED if matched[2] >= shares - 0.001 else PARTIAL
        return OrderResult(
            success=True, order_id=order_id, status=status,
            side="BUY", price=matched[0], amount_usd=matched[1],
            shares=matched[2], token_id=token_id[:16] + "...",
            dry_run=False,
        )

    def place_sell_order(self, token_id: str, shares: float, price: float = 0.0) -> OrderResult:
        """Post a sell order and return immediately; caller verifies/cancels later."""
        sell_shares = int(shares)
        if sell_shares < 1:
            return OrderResult(
                success=False, status=REJECTED,
                error="Less than 1 share", side="SELL",
            )

        if price <= 0:
            notional = float(sell_shares) * 0.50
            price = 0.90 if self.dry_run else self.get_market_price(token_id, "SELL", notional)
            if price <= 0:
                return OrderResult(
                    success=False, status=FAILED,
                    error="Could not get sell price", side="SELL",
                    token_id=token_id[:16] + "...",
                )
        market_price = round(price, 2)

        sell_amount = round(sell_shares * market_price, 2)
        if sell_amount < POLY_MIN_NOTIONAL:
            return OrderResult(
                success=False, status=REJECTED,
                error=f"Notional ${sell_amount:.2f} < ${POLY_MIN_NOTIONAL:.0f} min - hold to resolution",
                side="SELL", price=market_price, shares=float(sell_shares),
                shares_remaining=float(sell_shares), token_id=token_id[:16] + "...",
            )

        print(f"  [order] Posting sell: {sell_shares} shares @ ${market_price:.3f} = ${sell_amount:.2f}")

        if self.dry_run:
            return OrderResult(
                success=True, order_id=f"DRY-SELL-{int(time.time() * 1000)}",
                status="PENDING", side="SELL", price=market_price,
                amount_usd=sell_amount, shares=float(sell_shares),
                shares_remaining=0.0, token_id=token_id[:16] + "...", dry_run=True,
            )

        if not self._initialized:
            return OrderResult(success=False, status=FAILED, error="Not initialized")

        token_balance = self._get_token_balance_optional(token_id, refresh=True)
        if token_balance is not None and token_balance > 0:
            available_shares = int(token_balance)
            if available_shares < sell_shares:
                print(
                    f"  [order] Sell size adjusted to actual token balance: "
                    f"{sell_shares} -> {available_shares} shares"
                )
                sell_shares = available_shares
                sell_amount = round(sell_shares * market_price, 2)
        if token_balance is not None and token_balance <= 0:
            print(
                "  [order] Token balance API returned 0; posting sell with "
                "tracked position shares anyway"
            )
        if sell_shares < 1:
            return OrderResult(
                success=False, status=REJECTED,
                error="No sellable token balance", side="SELL",
                price=market_price, shares=0.0, token_id=token_id[:16] + "...",
            )
        if sell_amount < POLY_MIN_NOTIONAL:
            return OrderResult(
                success=False, status=REJECTED,
                error=f"Notional ${sell_amount:.2f} < ${POLY_MIN_NOTIONAL:.0f} min - hold to resolution",
                side="SELL", price=market_price, shares=float(sell_shares),
                shares_remaining=float(sell_shares), token_id=token_id[:16] + "...",
            )

        balance_before = self.get_balance()
        token_balance_before = token_balance if token_balance is not None else self._get_token_balance_optional(token_id)
        try:
            result = self.client.create_and_post_market_order(
                order_args=MarketOrderArgs(
                    token_id=token_id,
                    amount=float(sell_shares),
                    side="SELL",
                    price=market_price,
                    order_type=OrderType.GTC,
                ),
                options=_order_options(),
                order_type=OrderType.GTC,
            )
            order_id = result.get("orderID", "")
            if not order_id:
                return OrderResult(
                    success=False, status=REJECTED,
                    error=f"No orderID: {result}", side="SELL", price=market_price,
                    token_id=token_id[:16] + "...",
                )
            return OrderResult(
                success=True, order_id=order_id, status="PENDING",
                side="SELL", price=market_price, amount_usd=sell_amount,
                shares=float(sell_shares), shares_remaining=0.0,
                token_id=token_id[:16] + "...", dry_run=False,
                balance_before=balance_before, token_balance_before=token_balance_before,
            )
        except Exception as e:
            return OrderResult(
                success=False, status=FAILED, error=_friendly_error(e),
                side="SELL", price=market_price, token_id=token_id[:16] + "...",
                balance_before=balance_before, token_balance_before=token_balance_before,
            )

    def check_pending_sell(
        self,
        order_id: str,
        price: float,
        shares: float,
        token_id: str,
        balance_before: float = 0.0,
        token_balance_before: Optional[float] = None,
    ) -> Optional[OrderResult]:
        """Single non-blocking verification pass for a previously posted sell."""
        if self.dry_run:
            return OrderResult(
                success=True, order_id=order_id, status=FILLED,
                side="SELL", price=price, amount_usd=shares * price,
                shares=shares, shares_remaining=0.0,
                token_id=token_id[:16] + "...", dry_run=True,
            )

        if balance_before > 0:
            balance_after = self.get_balance(refresh=True)
            received = balance_after - balance_before
            token_balance_after = self._get_token_balance_optional(token_id, refresh=True)
            token_delta = (
                max(0.0, token_balance_before - token_balance_after)
                if token_balance_before is not None and token_balance_after is not None
                else None
            )
            if received > 0.10 or (token_delta is not None and token_delta > 0):
                sold_shares = token_delta if token_delta is not None else (received / price if price > 0 else shares)
                revenue = received if received > 0.10 else sold_shares * price
                shares_left = max(0.0, shares - sold_shares)
                status = FILLED if shares_left < 1 else PARTIAL
                return OrderResult(
                    success=True, order_id=order_id, status=status,
                    side="SELL", price=price, amount_usd=revenue,
                    shares=sold_shares, shares_remaining=shares_left,
                    token_id=token_id[:16] + "...", dry_run=False,
                )

        fill = self._check_order(order_id)
        if not fill:
            return None
        matched = self._extract_fill(fill, price)
        if not matched:
            return None
        shares_left = max(0.0, shares - matched[2])
        status = FILLED if shares_left < 1 else PARTIAL
        return OrderResult(
            success=True, order_id=order_id, status=status,
            side="SELL", price=matched[0], amount_usd=matched[1],
            shares=matched[2], shares_remaining=shares_left,
            token_id=token_id[:16] + "...", dry_run=False,
        )

    def _verify_buy_via_balance(
        self, order_id: str, price: float, shares: float,
        token_id: str, balance_before: float, token_balance_before: Optional[float],
    ) -> OrderResult:
        best_result: Optional[OrderResult] = None
        last_seen: Optional[tuple[float, float]] = None
        stable_seen_count = 0

        for attempt in range(BUY_VERIFY_ATTEMPTS):
            balance_after = self.get_balance(refresh=True)
            spent = balance_before - balance_after if balance_before > 0 else 0
            token_balance_after = self._get_token_balance_optional(token_id, refresh=True)
            token_delta = (
                max(0.0, token_balance_after - token_balance_before)
                if token_balance_before is not None and token_balance_after is not None
                else None
            )
            if spent > 0.50 or (token_delta is not None and token_delta > 0):
                actual_shares = token_delta if token_delta is not None and token_delta > 0 else (
                    spent / price if spent > 0.50 and price > 0 else shares
                )
                status = FILLED if actual_shares >= shares - 0.001 else PARTIAL
                suffix = f" (attempt {attempt + 1})" if attempt > 0 else ""
                print(f"  [order] Balance verified{suffix}: spent ${spent:.2f} "
                      f"(~{actual_shares:.0f}/{shares:.0f} shares @ ${price:.3f})")

                best_result = OrderResult(
                    success=True, order_id=order_id, status=status,
                    side="BUY", price=price,
                    amount_usd=spent, shares=actual_shares,
                    token_id=token_id[:16] + "...", dry_run=False,
                )
                if status == FILLED:
                    return best_result

                seen = (round(spent, 6), round(actual_shares, 6))
                if seen == last_seen:
                    stable_seen_count += 1
                else:
                    stable_seen_count = 1
                    last_seen = seen

                if stable_seen_count >= 2 and attempt >= 2:
                    print(
                        f"  [order] Partial buy settled after {attempt + 1} checks: "
                        f"tracking actual {actual_shares:.0f} shares"
                    )
                    return best_result

                print(
                    "  [order] Partial buy still settling - refreshing Polymarket "
                    "token balance again"
                )

            fill = self._check_order(order_id)
            if fill:
                matched = self._extract_fill(fill, price)
                if matched:
                    cap_price = max_buy_price()
                    if matched[0] > cap_price:
                        print(
                            f"  [order] WARNING: verified buy price ${matched[0]:.3f} "
                            f"exceeds MAX_BUY_PRICE ${cap_price:.2f}"
                        )
                    suffix = f" (attempt {attempt + 1})" if attempt > 0 else ""
                    print(f"  [order] Order API verified{suffix}: "
                          f"{matched[2]:.0f} shares @ ${matched[0]:.3f}")
                    if matched[2] >= shares - 0.001:
                        return OrderResult(
                            success=True, order_id=order_id, status=FILLED,
                            side="BUY", price=matched[0],
                            amount_usd=matched[1], shares=matched[2],
                            token_id=token_id[:16] + "...", dry_run=False,
                        )
                    best_result = OrderResult(
                        success=True, order_id=order_id, status=PARTIAL,
                        side="BUY", price=matched[0],
                        amount_usd=matched[1], shares=matched[2],
                        token_id=token_id[:16] + "...", dry_run=False,
                    )

            if attempt < BUY_VERIFY_ATTEMPTS - 1:
                time.sleep(BUY_VERIFY_DELAY_SECONDS)

        if best_result:
            if best_result.status == PARTIAL:
                print(
                    f"  [order] Partial buy detected after final verification: "
                    f"tracking actual {best_result.shares:.0f} shares"
                )
            return best_result

        total_wait = 5 + (BUY_VERIFY_ATTEMPTS - 1) * BUY_VERIFY_DELAY_SECONDS
        print(f"  [order] Buy unverified after {total_wait:.0f}s - NOT cancelling")
        return OrderResult(
            success=False, order_id=order_id, status=FAILED,
            error="UNVERIFIED_BUY",
            side="BUY", price=price, amount_usd=shares * price,
            shares=shares, token_id=token_id[:16] + "...",
        )

    def sell(self, token_id: str, shares: float, price: float = 0.0) -> OrderResult:
        """Sell shares via v2 market order, balance verified."""
        sell_shares = int(shares)
        if sell_shares < 1:
            return OrderResult(
                success=False, status=REJECTED,
                error="Less than 1 share", side="SELL",
            )

        if self.dry_run:
            sim_price = price if price > 0 else 0.90
            revenue = sell_shares * sim_price
            return OrderResult(
                success=True, order_id=f"DRY-SELL-{int(time.time())}",
                status=FILLED, side="SELL", price=sim_price,
                amount_usd=revenue, shares=float(sell_shares),
                shares_remaining=0.0,
                token_id=token_id[:16] + "...", dry_run=True,
            )

        if not self._initialized:
            return OrderResult(success=False, status=FAILED, error="Not initialized")

        token_balance = self._get_token_balance_optional(token_id, refresh=True)
        if token_balance is not None and token_balance > 0:
            available_shares = int(token_balance)
            if available_shares < sell_shares:
                print(
                    f"  [order] Sell size adjusted to actual token balance: "
                    f"{sell_shares} -> {available_shares} shares"
                )
                sell_shares = available_shares
        if token_balance is not None and token_balance <= 0:
            return OrderResult(
                success=False, status=REJECTED,
                error="No sellable token balance", side="SELL",
                price=price, shares=0.0, token_id=token_id[:16] + "...",
            )
        if sell_shares < 1:
            return OrderResult(
                success=False, status=REJECTED,
                error="No sellable token balance", side="SELL",
                price=price, shares=0.0, token_id=token_id[:16] + "...",
            )

        if price <= 0:
            notional = float(sell_shares) * 0.50
            price = self.get_market_price(token_id, "SELL", notional)
            if price <= 0:
                return OrderResult(
                    success=False, status=FAILED,
                    error="Could not get sell price", side="SELL",
                    token_id=token_id[:16] + "...",
                )

        sell_amount = round(sell_shares * price, 2)
        if sell_amount < POLY_MIN_NOTIONAL:
            return OrderResult(
                success=False, status=REJECTED,
                error=f"Notional ${sell_amount:.2f} < ${POLY_MIN_NOTIONAL:.0f} min - hold to resolution",
                side="SELL", price=price, shares=float(sell_shares),
                shares_remaining=float(sell_shares),
                token_id=token_id[:16] + "...",
            )

        print(f"  [order] Sell: {sell_shares} shares @ ${price:.3f} = ${sell_amount:.2f}")
        balance_before = self.get_balance()

        try:
            result = self.client.create_and_post_market_order(
                order_args=MarketOrderArgs(
                    token_id=token_id,
                    amount=float(sell_shares),
                    side="SELL",
                    price=round(price, 2),
                    order_type=OrderType.GTC,
                ),
                options=_order_options(),
                order_type=OrderType.GTC,
            )
            order_id = result.get("orderID", "")
            time.sleep(2)

            balance_after = self.get_balance()
            received = balance_after - balance_before
            if received > 0.10:
                shares_sold = received / price if price > 0 else 0
                shares_left = max(0, sell_shares - shares_sold)
                status = FILLED if shares_left < 1 else PARTIAL
                if status == PARTIAL:
                    print(f"  [order] Partial fill: sold ~{shares_sold:.0f} of {sell_shares}, "
                          f"~{shares_left:.0f} remaining")
                return OrderResult(
                    success=True, order_id=order_id or "balance-verified",
                    status=status, side="SELL", price=price,
                    amount_usd=received, shares=shares_sold,
                    shares_remaining=shares_left,
                    token_id=token_id[:16] + "...", dry_run=False,
                )

            if order_id:
                fill = self._check_order(order_id)
                if fill:
                    matched = self._extract_fill(fill, price)
                    if matched:
                        shares_left = max(0, sell_shares - matched[2])
                        return OrderResult(
                            success=True, order_id=order_id,
                            status=FILLED if shares_left < 1 else PARTIAL,
                            side="SELL", price=matched[0],
                            amount_usd=matched[1], shares=matched[2],
                            shares_remaining=shares_left,
                            token_id=token_id[:16] + "...", dry_run=False,
                        )
                self.cancel_order(order_id)

            return OrderResult(
                success=False, order_id=order_id or "", status=FAILED,
                error="Sell not verified (no balance change)",
                side="SELL", price=price, token_id=token_id[:16] + "...",
            )
        except Exception as e:
            time.sleep(1)
            balance_after = self.get_balance()
            received = balance_after - balance_before
            if received > 0.10:
                shares_sold = received / price if price > 0 else 0
                shares_left = max(0, sell_shares - shares_sold)
                print(f"  [order] Ghost sell: got ${received:.2f} despite error")
                return OrderResult(
                    success=True, order_id="ghost-sell",
                    status=PARTIAL if shares_left >= 1 else FILLED,
                    side="SELL", price=price,
                    amount_usd=received, shares=shares_sold,
                    shares_remaining=shares_left,
                    token_id=token_id[:16] + "...", dry_run=False,
                )

            err = _friendly_error(e)
            actual_balance = _balance_from_not_enough_error(err)
            retry_shares = int(actual_balance)
            if "not enough balance" in err.lower() and retry_shares >= 1 and retry_shares < sell_shares:
                retry_amount = round(retry_shares * price, 2)
                if retry_amount >= POLY_MIN_NOTIONAL:
                    print(
                        f"  [order] Retrying sell with actual token balance: "
                        f"{retry_shares} shares @ ${price:.3f} = ${retry_amount:.2f}"
                    )
                    try:
                        retry_result = self.client.create_and_post_market_order(
                            order_args=MarketOrderArgs(
                                token_id=token_id,
                                amount=float(retry_shares),
                                side="SELL",
                                price=round(price, 2),
                                order_type=OrderType.GTC,
                            ),
                            options=_order_options(),
                            order_type=OrderType.GTC,
                        )
                        retry_order_id = retry_result.get("orderID", "")
                        time.sleep(2)
                        retry_balance = self.get_balance()
                        retry_received = retry_balance - balance_before
                        if retry_received > 0.10:
                            shares_sold = retry_received / price if price > 0 else retry_shares
                            return OrderResult(
                                success=True,
                                order_id=retry_order_id or "balance-retry-verified",
                                status=FILLED,
                                side="SELL", price=price,
                                amount_usd=retry_received, shares=shares_sold,
                                shares_remaining=0.0,
                                token_id=token_id[:16] + "...", dry_run=False,
                            )
                    except Exception as retry_error:
                        err = _friendly_error(retry_error)
                else:
                    return OrderResult(
                        success=False, status=REJECTED,
                        error=(
                            f"Actual token balance {retry_shares} shares has "
                            f"notional ${retry_amount:.2f} < ${POLY_MIN_NOTIONAL:.0f} min"
                        ),
                        side="SELL", price=price,
                        shares=float(retry_shares),
                        shares_remaining=float(retry_shares),
                        token_id=token_id[:16] + "...",
                    )

            return OrderResult(
                success=False, status=FAILED, error=err,
                side="SELL", price=price, token_id=token_id[:16] + "...",
            )

    def _extract_fill(self, fill: dict, fallback_price: float) -> Optional[tuple]:
        size_matched = float(
            fill.get("size_matched", 0) if isinstance(fill, dict)
            else getattr(fill, "size_matched", 0)
        )
        if size_matched <= 0:
            return None
        fill_price = float(
            fill.get("price", fallback_price) if isinstance(fill, dict)
            else getattr(fill, "price", fallback_price)
        )
        return (fill_price, size_matched * fill_price, size_matched)

    def _check_order(self, order_id: str) -> Optional[dict]:
        if not self._initialized:
            return None
        try:
            return self.client.get_order(order_id)
        except Exception as e:
            print(f"[executor] Order check failed: {e}")
            return None

    def check_buy_fill(self, order_id: str, fallback_price: float) -> Optional[tuple]:
        fill = self._check_order(order_id)
        if not fill:
            return None
        return self._extract_fill(fill, fallback_price)

    def order_is_cancelled(self, order_id: str) -> bool:
        if self.dry_run:
            return True
        order = self._check_order(order_id)
        if not order:
            return False
        status = str(
            order.get("status", "")
            or order.get("state", "")
            or order.get("orderStatus", "")
        ).lower()
        return status in {"cancelled", "canceled", "cancelled_order", "canceled_order"}

    def cancel_order(self, order_id: str) -> bool:
        if self.dry_run or not self._initialized:
            return True
        try:
            self.client.cancel_order(OrderPayload(orderID=order_id))
            return True
        except Exception as e:
            print(f"[executor] Cancel failed: {e}")
            return False

    def get_open_orders(self) -> list:
        if self.dry_run or not self._initialized:
            return []
        try:
            orders = self.client.get_open_orders()
            return orders if isinstance(orders, list) else []
        except Exception as e:
            print(f"[executor] Open order check failed: {e}")
            return []

    def cancel_all(self) -> bool:
        if self.dry_run or not self._initialized:
            return True
        try:
            self.client.cancel_all()
            return True
        except Exception as e:
            print(f"[executor] Cancel all failed: {e}")
            return False
