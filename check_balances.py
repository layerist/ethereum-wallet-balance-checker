#!/usr/bin/env python3
"""
Ethereum Wallet Balance Checker (Improved & Hardened)

Features:
- Thread-safe Web3 usage (per-thread provider)
- Deterministic output ordering
- Exponential backoff with capped delay
- Safer numeric handling via Decimal
- Clear error classification
- Optional progress bar & colored output
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from dataclasses import dataclass
from decimal import Decimal, getcontext
from pathlib import Path
from typing import Callable, Dict, Iterable, List, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed

from web3 import Web3
from web3.exceptions import InvalidAddress

# -------------------------
# Optional dependencies
# -------------------------
try:
    from tqdm import tqdm
    HAS_TQDM = True
except ImportError:
    HAS_TQDM = False

try:
    from colorama import Fore, Style, init as colorama_init
    colorama_init()
    COLOR_ENABLED = True
except ImportError:
    COLOR_ENABLED = False


# =========================
# Constants & Precision
# =========================
ETH_DECIMALS = 4
RETRY_ATTEMPTS = 4
RETRY_DELAY = 0.5
MAX_RETRY_DELAY = 4.0
REQUEST_TIMEOUT = 10

getcontext().prec = 28


# =========================
# Data Model
# =========================
@dataclass(frozen=True)
class BalanceResult:
    address: str
    balance_eth: Optional[Decimal]
    error: Optional[str] = None

    def display(self) -> str:
        if self.balance_eth is not None:
            return f"{self.balance_eth:.{ETH_DECIMALS}f} ETH"
        return f"Error: {self.error or 'Unknown'}"


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
    return {
        "green": Fore.GREEN,
        "red": Fore.RED,
        "yellow": Fore.YELLOW,
        "blue": Fore.CYAN,
        "bold": Style.BRIGHT,
    }.get(name, "") + text + Style.RESET_ALL


# =========================
# Address Loading
# =========================
def load_wallet_addresses(path: Path) -> List[str]:
    if not path.is_file():
        raise FileNotFoundError(f"Input file not found: {path.resolve()}")

    seen = set()
    addresses: List[str] = []

    for line in path.read_text(encoding="utf-8").splitlines():
        addr = line.strip()
        if not addr or addr in seen:
            continue
        seen.add(addr)

        if not Web3.is_address(addr):
            logging.warning(f"Invalid address skipped: {addr}")
            continue

        try:
            addresses.append(Web3.to_checksum_address(addr))
        except ValueError:
            logging.warning(f"Checksum conversion failed: {addr}")

    if not addresses:
        raise ValueError("No valid Ethereum addresses found.")

    logging.info(f"Loaded {len(addresses)} addresses")
    return addresses


# =========================
# Retry Helper
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
        except Exception as e:
            if attempt >= attempts:
                raise
            logging.debug(f"Retry {attempt}/{attempts} after error: {e}")
            time.sleep(delay)
            delay = min(delay * 2, MAX_RETRY_DELAY)
    raise RuntimeError("Unreachable")


# =========================
# Balance Fetching
# =========================
def fetch_wallet_balance(node_url: str, address: str) -> BalanceResult:
    try:
        web3 = Web3(
            Web3.HTTPProvider(node_url, request_kwargs={"timeout": REQUEST_TIMEOUT})
        )

        def call() -> int:
            return web3.eth.get_balance(address)

        wei = with_retries(call, attempts=RETRY_ATTEMPTS, base_delay=RETRY_DELAY)
        eth = Decimal(wei) / Decimal(10**18)

        return BalanceResult(address, eth)

    except InvalidAddress:
        return BalanceResult(address, None, "InvalidAddress")

    except Exception as e:
        logging.debug(f"{address} failed: {e}", exc_info=True)
        return BalanceResult(address, None, e.__class__.__name__)


# =========================
# Concurrency
# =========================
def fetch_balances_concurrently(
    node_url: str,
    addresses: Iterable[str],
    max_workers: int,
) -> List[BalanceResult]:
    addresses = list(addresses)
    results: Dict[str, BalanceResult] = {}

    logging.info(f"Fetching balances with {max_workers} threads")

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(fetch_wallet_balance, node_url, addr): addr
            for addr in addresses
        }

        iterator = (
            tqdm(as_completed(futures), total=len(futures), desc="Fetching", ncols=80)
            if HAS_TQDM
            else as_completed(futures)
        )

        for future in iterator:
            addr = futures[future]
            try:
                results[addr] = future.result()
            except Exception as e:
                results[addr] = BalanceResult(addr, None, e.__class__.__name__)

    ordered = [results[a] for a in addresses]
    logging.info(color("Balance fetch completed", "green"))
    return ordered


# =========================
# Save
# =========================
def save_balances(results: List[BalanceResult], path: Path) -> None:
    data = {r.address: r.display() for r in results}
    path.write_text(json.dumps(data, indent=4), encoding="utf-8")
    logging.info(color(f"Saved to {path.resolve()}", "blue"))


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
# Main
# =========================
def main() -> None:
    start = time.time()
    args = parse_args()
    configure_logging(args.verbose)

    try:
        addresses = load_wallet_addresses(Path(args.input))
        results = fetch_balances_concurrently(
            args.node,
            addresses,
            max_workers=max(1, args.workers),
        )

        output = {r.address: r.display() for r in results}
        print(json.dumps(output, indent=4))

        if not args.no_save:
            save_balances(results, Path(args.output))

        logging.info(
            color(f"Completed in {time.time() - start:.2f}s", "green")
        )

    except KeyboardInterrupt:
        logging.warning("Interrupted by user")
    except Exception as e:
        logging.critical(f"Fatal error: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
