#!/usr/bin/env python3
"""
Improved Ethereum Wallet Balance Checker

Key improvements:
- Clearer separation of concerns and stricter typing
- Centralized, predictable retry/backoff logic
- Safer numeric balance handling (store float + display formatting)
- Better error classification and logging
- More robust concurrency and result ordering
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed

from web3 import Web3
from web3.exceptions import InvalidAddress

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
# Constants
# =========================
ETH_DECIMALS = 4
RETRY_ATTEMPTS = 4
RETRY_DELAY = 0.5
MAX_RETRY_DELAY = 4.0
REQUEST_TIMEOUT = 10


# =========================
# Data Models
# =========================
@dataclass(frozen=True)
class BalanceResult:
    address: str
    balance_eth: Optional[float]  # None if error
    error: Optional[str] = None

    def display(self) -> str:
        if self.balance_eth is not None:
            return f"{self.balance_eth:.{ETH_DECIMALS}f} ETH"
        return f"Error: {self.error or 'Unknown'}"


# =========================
# Logging & Color Helpers
# =========================
def configure_logging(verbose: bool) -> None:
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s | %(levelname)-8s | %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )


def color(text: str, c: str) -> str:
    if not COLOR_ENABLED:
        return text
    return {
        "green": Fore.GREEN,
        "red": Fore.RED,
        "yellow": Fore.YELLOW,
        "blue": Fore.CYAN,
        "bold": Style.BRIGHT,
    }.get(c, "") + text + Style.RESET_ALL


# =========================
# Address Loading
# =========================
def load_wallet_addresses(path: Path) -> List[str]:
    if not path.is_file():
        raise FileNotFoundError(f"Input file not found: {path.resolve()}")

    raw = {line.strip() for line in path.read_text(encoding="utf-8").splitlines() if line.strip()}
    addresses: List[str] = []

    for addr in raw:
        if not Web3.is_address(addr):
            logging.warning(f"Skipped invalid Ethereum address: {addr}")
            continue
        try:
            addresses.append(Web3.to_checksum_address(addr))
        except Exception:
            logging.warning(f"Skipped invalid checksum address: {addr}")

    if not addresses:
        raise ValueError("No valid Ethereum addresses found.")

    logging.info(f"Loaded {len(addresses)} valid addresses from {path.name}")
    return addresses


# =========================
# Node Connection
# =========================
def connect_to_node(node_url: str) -> Web3:
    if not node_url.startswith(("http://", "https://")):
        raise ValueError("Node URL must start with http:// or https://")

    provider = Web3.HTTPProvider(node_url, request_kwargs={"timeout": REQUEST_TIMEOUT})
    web3 = Web3(provider)

    if not web3.is_connected():
        raise ConnectionError(f"Failed to connect to Ethereum node: {node_url}")

    logging.info(f"Connected to Ethereum node: {web3.client_version}")
    return web3


# =========================
# Retry Helper
# =========================
def with_retries(func, *, attempts: int, base_delay: float) -> any:
    delay = base_delay
    for attempt in range(1, attempts + 1):
        try:
            return func()
        except Exception:
            if attempt >= attempts:
                raise
            time.sleep(delay)
            delay = min(delay * 2, MAX_RETRY_DELAY)


# =========================
# Balance Fetching
# =========================
def fetch_wallet_balance(web3: Web3, address: str) -> BalanceResult:
    try:
        def call():
            return web3.eth.get_balance(address)

        wei = with_retries(call, attempts=RETRY_ATTEMPTS, base_delay=RETRY_DELAY)
        eth = float(web3.from_wei(wei, "ether"))
        return BalanceResult(address=address, balance_eth=eth)

    except InvalidAddress:
        return BalanceResult(address=address, balance_eth=None, error="InvalidAddress")

    except Exception as e:
        msg = str(e)
        logging.debug(f"Failed to fetch {address}: {msg}")
        return BalanceResult(address=address, balance_eth=None, error=e.__class__.__name__)


def fetch_balances_concurrently(
    web3: Web3,
    addresses: Iterable[str],
    max_workers: int,
) -> List[BalanceResult]:
    logging.info(f"Fetching balances using {max_workers} workers...")

    results: List[BalanceResult] = []

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(fetch_wallet_balance, web3, a): a for a in addresses}
        iterator = tqdm(as_completed(futures), total=len(futures), desc="Fetching", ncols=80) \
            if HAS_TQDM else as_completed(futures)

        for future in iterator:
            try:
                results.append(future.result())
            except Exception as e:
                addr = futures[future]
                results.append(BalanceResult(addr, None, e.__class__.__name__))

    results.sort(
        key=lambda r: (r.balance_eth is None, -(r.balance_eth or 0.0))
    )
    logging.info(color("All balances fetched successfully.", "green"))
    return results


# =========================
# Save
# =========================
def save_balances(results: List[BalanceResult], path: Path) -> None:
    data: Dict[str, str] = {r.address: r.display() for r in results}
    path.write_text(json.dumps(data, indent=4, ensure_ascii=False), encoding="utf-8")
    logging.info(color(f"Results saved to {path.resolve()}", "blue"))


# =========================
# CLI
# =========================
def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Ethereum Wallet Balance Checker (multithreaded)")
    parser.add_argument("-i", "--input", default="wallets.txt", help="Input file with wallet addresses")
    parser.add_argument("-o", "--output", default="balances.json", help="Output JSON file")
    parser.add_argument("-n", "--node", required=True, help="Ethereum node URL")
    parser.add_argument("--workers", type=int, default=10, help="Number of threads")
    parser.add_argument("--no-save", action="store_true", help="Do not save output file")
    parser.add_argument("-v", "--verbose", action="store_true", help="Verbose logging")
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
        web3 = connect_to_node(args.node)
        results = fetch_balances_concurrently(web3, addresses, args.workers)

        printable = {r.address: r.display() for r in results}
        print(json.dumps(printable, indent=4, ensure_ascii=False))

        if not args.no_save:
            save_balances(results, Path(args.output))

        logging.info(color(f"Completed in {time.time() - start:.2f} seconds", "green"))

    except KeyboardInterrupt:
        logging.warning("Interrupted by user.")
    except Exception as e:
        logging.critical(f"Fatal error: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
