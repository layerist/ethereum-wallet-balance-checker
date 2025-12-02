#!/usr/bin/env python3
"""
Enhanced Ethereum Wallet Balance Checker

Improvements:
- Cleaner structure and stronger type hints
- Deterministic, centralized retry logic
- More robust address validation and error handling
- Safe normalized color/log usage
- Simplified concurrency workflow and safer JSON exporting
"""

import argparse
import json
import logging
import sys
import time
from pathlib import Path
from typing import Dict, List, Union
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
# Logging & Color Helpers
# =========================
def configure_logging(verbose: bool) -> None:
    log_format = "%(asctime)s | %(levelname)-8s | %(message)s"
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format=log_format,
        handlers=[logging.StreamHandler(sys.stdout)],
    )


def color(text: str, color: str) -> str:
    if not COLOR_ENABLED:
        return text

    if color == "green":
        return Fore.GREEN + text + Style.RESET_ALL
    if color == "red":
        return Fore.RED + text + Style.RESET_ALL
    if color == "yellow":
        return Fore.YELLOW + text + Style.RESET_ALL
    if color == "blue":
        return Fore.CYAN + text + Style.RESET_ALL
    if color == "bold":
        return Style.BRIGHT + text + Style.RESET_ALL

    return text


# =========================
# Address Loading
# =========================
def load_wallet_addresses(file_path: Union[str, Path]) -> List[str]:
    path = Path(file_path)
    if not path.is_file():
        raise FileNotFoundError(f"Input file not found: {path.resolve()}")

    with path.open("r", encoding="utf-8") as f:
        lines = {line.strip() for line in f if line.strip()}

    valid: List[str] = []
    for addr in lines:
        if Web3.is_address(addr):
            try:
                valid.append(Web3.to_checksum_address(addr))
            except Exception:
                logging.warning(f"Skipped invalid checksum address: {addr}")
        else:
            logging.warning(f"Skipped invalid Ethereum address: {addr}")

    if not valid:
        raise ValueError("No valid Ethereum addresses found.")

    logging.info(f"Loaded {len(valid)} valid addresses from {path.name}")
    return valid


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

    logging.info(f"Connected to Ethereum node: {getattr(web3, 'client_version', 'Unknown')}")
    return web3


# =========================
# Balance Fetching
# =========================
def fetch_wallet_balance(web3: Web3, address: str) -> str:
    delay = RETRY_DELAY

    for attempt in range(1, RETRY_ATTEMPTS + 1):
        try:
            wei = web3.eth.get_balance(address)
            eth = web3.from_wei(wei, "ether")
            return f"{eth:.{ETH_DECIMALS}f} ETH"

        except InvalidAddress:
            return "Error: InvalidAddress"

        except Exception as e:
            msg = str(e).lower()
            retryable = any(k in msg for k in ["429", "rate", "timeout", "connection"])

            if retryable and attempt < RETRY_ATTEMPTS:
                logging.warning(
                    f"{address[:10]}… | transient error: {msg}. "
                    f"Retrying in {delay:.1f}s (attempt {attempt}/{RETRY_ATTEMPTS})"
                )
                time.sleep(delay)
                delay = min(delay * 2, MAX_RETRY_DELAY)
            else:
                logging.error(f"Failed to fetch {address}: {e}")
                return f"Error: {e.__class__.__name__}"

    return "Error: Unknown"


def fetch_balances_concurrently(web3: Web3, addresses: List[str], max_workers: int) -> Dict[str, str]:
    logging.info(f"Fetching balances using {max_workers} workers...")

    results: Dict[str, str] = {}

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(fetch_wallet_balance, web3, a): a for a in addresses}

        if HAS_TQDM:
            iterator = tqdm(as_completed(futures), total=len(addresses), desc="Fetching", ncols=80)
        else:
            iterator = as_completed(futures)

        for future in iterator:
            addr = futures[future]
            try:
                results[addr] = future.result()
            except Exception as e:
                results[addr] = f"Error: {e.__class__.__name__}"
                logging.debug(f"Unhandled exception for {addr}: {e}")

    # Sort balances first (by numeric value), errors last
    def sort_key(item):
        val = item[1]
        try:
            return (0, float(val.split()[0]))  # valid ETH balance
        except Exception:
            return (1, -1)  # errors

    sorted_results = dict(sorted(results.items(), key=sort_key, reverse=True))
    logging.info(color("All balances fetched successfully.", "green"))
    return sorted_results


# =========================
# Save
# =========================
def save_balances(balances: Dict[str, str], output_path: Union[str, Path]) -> None:
    path = Path(output_path)

    try:
        with path.open("w", encoding="utf-8") as f:
            json.dump(balances, f, indent=4, ensure_ascii=False)
        logging.info(color(f"Results saved to {path.resolve()}", "blue"))
    except Exception as e:
        logging.error(f"Failed to write {path}: {e}")
        raise


# =========================
# CLI
# =========================
def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Ethereum Wallet Balance Checker (multithreaded)")
    parser.add_argument("-i", "--input", default="wallets.txt", help="Input file with wallet addresses")
    parser.add_argument("-o", "--output", default="balances.json", help="Output JSON file")
    parser.add_argument("-n", "--node", required=True, help="Ethereum node URL")
    parser.add_argument("--workers", type=int, default=10, help="Number of threads (default: 10)")
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
        addresses = load_wallet_addresses(args.input)
        web3 = connect_to_node(args.node)
        balances = fetch_balances_concurrently(web3, addresses, args.workers)

        print(json.dumps(balances, indent=4, ensure_ascii=False))

        if not args.no_save:
            save_balances(balances, args.output)

        logging.info(color(f"Completed in {time.time() - start:.2f} seconds", "green"))

    except KeyboardInterrupt:
        logging.warning("Interrupted by user.")
    except Exception as e:
        logging.critical(f"Fatal error: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
