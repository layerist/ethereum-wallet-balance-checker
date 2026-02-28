#!/usr/bin/env python3
"""
Ethereum Wallet Balance Checker (Hardened Production Version)

Improvements:
- Thread-local Web3 instance (connection reuse per thread)
- Deterministic ordering
- Exponential backoff with cap
- Retry only transient errors
- Decimal-safe arithmetic
- Structured JSON output
- RPC connectivity validation
- Graceful shutdown handling
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import time
import threading
from dataclasses import dataclass, asdict
from decimal import Decimal, getcontext
from pathlib import Path
from typing import Callable, Dict, Iterable, List, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed, CancelledError

from web3 import Web3
from web3.exceptions import InvalidAddress

# =========================
# Optional Dependencies
# =========================
try:
    from tqdm import tqdm
    HAS_TQDM = True
except ImportError:
    HAS_TQDM = False

try:
    from colorama import Fore, Style, init as colorama_init
    colorama_init(autoreset=True)
    COLOR_ENABLED = True
except ImportError:
    COLOR_ENABLED = False


# =========================
# Constants
# =========================
ETH_DECIMALS = 4
RETRY_ATTEMPTS = 4
RETRY_DELAY = 0.5
MAX_RETRY_DELAY = 4.0
REQUEST_TIMEOUT = 10
WEI_IN_ETH = Decimal("1000000000000000000")

getcontext().prec = 28


# =========================
# Thread-Local Web3
# =========================
_thread_local = threading.local()


def get_web3(node_url: str) -> Web3:
    if not hasattr(_thread_local, "web3"):
        _thread_local.web3 = Web3(
            Web3.HTTPProvider(
                node_url,
                request_kwargs={"timeout": REQUEST_TIMEOUT},
            )
        )
    return _thread_local.web3


# =========================
# Data Model
# =========================
@dataclass(frozen=True)
class BalanceResult:
    address: str
    balance_wei: Optional[int]
    balance_eth: Optional[Decimal]
    error: Optional[str] = None

    def to_dict(self) -> dict:
        data = asdict(self)
        if self.balance_eth is not None:
            data["balance_eth"] = f"{self.balance_eth:.{ETH_DECIMALS}f}"
        return data


# =========================
# Logging / Color
# =========================
def configure_logging(verbose: bool) -> None:
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s | %(levelname)-8s | %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )


def color(text: str, name: str) -> str:
    if not COLOR_ENABLED:
        return text
    palette = {
        "green": Fore.GREEN,
        "red": Fore.RED,
        "yellow": Fore.YELLOW,
        "blue": Fore.CYAN,
        "bold": Style.BRIGHT,
    }
    return f"{palette.get(name, '')}{text}{Style.RESET_ALL}"


# =========================
# Address Loading
# =========================
def load_wallet_addresses(path: Path) -> List[str]:
    if not path.is_file():
        raise FileNotFoundError(f"Input file not found: {path.resolve()}")

    seen = set()
    addresses = []

    for line in path.read_text(encoding="utf-8").splitlines():
        raw = line.strip()
        if not raw or raw in seen:
            continue

        seen.add(raw)

        if not Web3.is_address(raw):
            logging.warning("Invalid address skipped: %s", raw)
            continue

        addresses.append(Web3.to_checksum_address(raw))

    if not addresses:
        raise ValueError("No valid Ethereum addresses found.")

    logging.info("Loaded %d unique addresses", len(addresses))
    return addresses


# =========================
# Retry Logic
# =========================
def with_retries(
    fn: Callable[[], int],
    *,
    attempts: int,
    base_delay: float,
) -> int:
    delay = base_delay

    for attempt in range(1, attempts + 1):
        try:
            return fn()
        except (TimeoutError, ConnectionError) as exc:
            if attempt >= attempts:
                raise
            logging.debug(
                "Retry %d/%d failed (%s), sleeping %.2fs",
                attempt,
                attempts,
                exc.__class__.__name__,
                delay,
            )
            time.sleep(delay)
            delay = min(delay * 2, MAX_RETRY_DELAY)

    raise RuntimeError("Retry loop exited unexpectedly")


# =========================
# Balance Fetching
# =========================
def fetch_wallet_balance(node_url: str, address: str) -> BalanceResult:
    try:
        web3 = get_web3(node_url)

        wei = with_retries(
            lambda: web3.eth.get_balance(address),
            attempts=RETRY_ATTEMPTS,
            base_delay=RETRY_DELAY,
        )

        eth = Decimal(wei) / WEI_IN_ETH
        return BalanceResult(address, wei, eth)

    except InvalidAddress:
        return BalanceResult(address, None, None, "InvalidAddress")

    except Exception as exc:
        logging.debug("Failed to fetch %s", address, exc_info=True)
        return BalanceResult(address, None, None, exc.__class__.__name__)


# =========================
# Concurrency
# =========================
def fetch_balances_concurrently(
    node_url: str,
    addresses: Iterable[str],
    max_workers: int,
) -> List[BalanceResult]:
    ordered = list(addresses)
    results: Dict[str, BalanceResult] = {}

    if max_workers < 1:
        raise ValueError("Workers must be >= 1")

    logging.info("Fetching balances using %d threads", max_workers)

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(fetch_wallet_balance, node_url, addr): addr
            for addr in ordered
        }

        completed = as_completed(futures)
        if HAS_TQDM:
            completed = tqdm(completed, total=len(futures), desc="Fetching", ncols=80)

        try:
            for future in completed:
                addr = futures[future]
                try:
                    results[addr] = future.result()
                except CancelledError:
                    results[addr] = BalanceResult(addr, None, None, "Cancelled")
                except Exception as exc:
                    results[addr] = BalanceResult(addr, None, None, exc.__class__.__name__)
        except KeyboardInterrupt:
            logging.warning("Cancelling remaining tasks...")
            executor.shutdown(cancel_futures=True)
            raise

    logging.info(color("Balance fetch completed", "green"))
    return [results[a] for a in ordered]


# =========================
# Persistence
# =========================
def save_balances(results: List[BalanceResult], path: Path) -> None:
    payload = {r.address: r.to_dict() for r in results}
    path.write_text(json.dumps(payload, indent=4), encoding="utf-8")
    logging.info(color(f"Saved results to {path.resolve()}", "blue"))


# =========================
# CLI
# =========================
def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Ethereum Wallet Balance Checker (multithreaded)"
    )
    parser.add_argument("-i", "--input", default="wallets.txt")
    parser.add_argument("-o", "--output", default="balances.json")
    parser.add_argument("-n", "--node", required=True)
    parser.add_argument("--workers", type=int, default=10)
    parser.add_argument("--no-save", action="store_true")
    parser.add_argument("-v", "--verbose", action="store_true")
    return parser.parse_args()


# =========================
# RPC Health Check
# =========================
def validate_rpc(node_url: str) -> None:
    logging.info("Validating RPC connectivity...")
    web3 = Web3(Web3.HTTPProvider(node_url))
    if not web3.is_connected():
        raise ConnectionError("Unable to connect to RPC endpoint")
    logging.info(color("RPC connection established", "green"))


# =========================
# Main
# =========================
def main() -> None:
    start_ts = time.time()
    args = parse_args()
    configure_logging(args.verbose)

    try:
        validate_rpc(args.node)

        addresses = load_wallet_addresses(Path(args.input))

        results = fetch_balances_concurrently(
            args.node,
            addresses,
            max_workers=args.workers,
        )

        output = {r.address: r.to_dict() for r in results}
        print(json.dumps(output, indent=4))

        if not args.no_save:
            save_balances(results, Path(args.output))

        elapsed = time.time() - start_ts
        logging.info(color(f"Completed in {elapsed:.2f}s", "green"))

    except KeyboardInterrupt:
        logging.warning("Interrupted by user")
        sys.exit(130)
    except Exception as exc:
        logging.critical("Fatal error: %s", exc, exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
