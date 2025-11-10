#!/usr/bin/env python3
"""
Enhanced Ethereum Wallet Balance Checker

Features:
- Concurrent ETH balance fetching with retry logic and exponential backoff
- Automatic progress bar (tqdm)
- Detailed, colorized logging
- Graceful error handling for network and node errors
- JSON export with sorted balances
"""

import json
import logging
import argparse
import time
import sys
from pathlib import Path
from typing import List, Dict, Union
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
# Configuration
# =========================
ETH_DECIMALS = 4
RETRY_ATTEMPTS = 4
RETRY_DELAY = 0.5
MAX_RETRY_DELAY = 4.0


# =========================
# Setup
# =========================
def configure_logging(verbose: bool) -> None:
    """Configure logging output."""
    log_format = "%(asctime)s | %(levelname)-8s | %(message)s"
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format=log_format,
        handlers=[logging.StreamHandler(sys.stdout)],
    )


def c(text: str, color: str) -> str:
    """Return colored text if colorama is available."""
    if not COLOR_ENABLED:
        return text
    colors = {
        "green": Fore.GREEN,
        "red": Fore.RED,
        "yellow": Fore.YELLOW,
        "blue": Fore.CYAN,
        "bold": Style.BRIGHT,
    }
    return colors.get(color, "") + text + Style.RESET_ALL


# =========================
# Wallet Loading
# =========================
def load_wallet_addresses(file_path: Union[str, Path]) -> List[str]:
    """Load Ethereum addresses from a file and normalize them."""
    path = Path(file_path)
    if not path.is_file():
        raise FileNotFoundError(f"Input file not found: {path.resolve()}")

    with path.open("r", encoding="utf-8") as f:
        raw_addresses = {line.strip() for line in f if line.strip()}

    valid_addresses = []
    for addr in raw_addresses:
        if Web3.is_address(addr):
            try:
                valid_addresses.append(Web3.to_checksum_address(addr))
            except Exception:
                logging.warning(f"Skipped invalid checksum address: {addr}")
        else:
            logging.warning(f"Skipped invalid Ethereum address: {addr}")

    if not valid_addresses:
        raise ValueError("No valid Ethereum addresses found in the input file.")

    logging.info(f"Loaded {len(valid_addresses)} valid addresses from {path.name}")
    return valid_addresses


# =========================
# Web3 Connection
# =========================
def connect_to_node(node_url: str) -> Web3:
    """Establish and verify connection to an Ethereum node."""
    if not node_url.startswith("http"):
        raise ValueError(f"Invalid node URL: {node_url}")

    web3 = Web3(Web3.HTTPProvider(node_url, request_kwargs={"timeout": 10}))
    if not web3.is_connected():
        raise ConnectionError(f"Failed to connect to Ethereum node: {node_url}")

    version = getattr(web3, "client_version", "Unknown client")
    logging.info(f"Connected to Ethereum node: {version}")
    return web3


# =========================
# Balance Fetching
# =========================
def fetch_wallet_balance(web3: Web3, address: str) -> str:
    """Fetch ETH balance for a wallet with retry logic and backoff."""
    delay = RETRY_DELAY
    for attempt in range(1, RETRY_ATTEMPTS + 1):
        try:
            balance_wei = web3.eth.get_balance(address)
            balance_eth = web3.from_wei(balance_wei, "ether")
            return f"{balance_eth:.{ETH_DECIMALS}f} ETH"

        except InvalidAddress:
            return "Error: InvalidAddress"

        except Exception as e:
            err_msg = str(e).lower()
            if "429" in err_msg or "rate limit" in err_msg:
                logging.warning(f"Rate limited. Sleeping {delay:.1f}s...")
            elif "connection" in err_msg:
                logging.warning(f"Connection issue for {address}: retrying...")

            if attempt < RETRY_ATTEMPTS:
                time.sleep(delay)
                delay = min(delay * 2, MAX_RETRY_DELAY)
            else:
                logging.error(f"Failed to fetch {address}: {e}")
                return f"Error: {e.__class__.__name__}"

    return "Error: Unknown"


def fetch_balances_concurrently(
    web3: Web3, addresses: List[str], max_workers: int
) -> Dict[str, str]:
    """Fetch balances concurrently using ThreadPoolExecutor."""
    results: Dict[str, str] = {}
    logging.info(f"Fetching balances using {max_workers} workers...")

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(fetch_wallet_balance, web3, addr): addr for addr in addresses}

        iterator = (
            tqdm(as_completed(futures), total=len(addresses), desc="Fetching", ncols=80)
            if HAS_TQDM
            else as_completed(futures)
        )

        for future in iterator:
            address = futures[future]
            try:
                results[address] = future.result()
            except Exception as e:
                results[address] = f"Error: {e.__class__.__name__}"
                logging.debug(f"Unhandled error for {address}: {e}")

    # Sort: balances first (descending), errors last
    def sort_key(item):
        val = item[1]
        try:
            return (0, float(val.split()[0]))
        except Exception:
            return (1, -1)

    sorted_results = dict(sorted(results.items(), key=sort_key, reverse=True))
    logging.info(c("All balances fetched successfully.", "green"))
    return sorted_results


# =========================
# Save Results
# =========================
def save_balances(balances: Dict[str, str], output_path: Union[str, Path]) -> None:
    """Save wallet balances to a JSON file."""
    path = Path(output_path)
    try:
        with path.open("w", encoding="utf-8") as f:
            json.dump(balances, f, indent=4, ensure_ascii=False)
        logging.info(c(f"Balances saved to {path.resolve()}", "blue"))
    except IOError as e:
        logging.error(f"Failed to write file {path}: {e}")
        raise


# =========================
# CLI Parsing
# =========================
def parse_args() -> argparse.Namespace:
    """Parse CLI arguments."""
    parser = argparse.ArgumentParser(
        description="Ethereum Wallet Balance Checker (multithreaded)"
    )
    parser.add_argument("-i", "--input", default="wallets.txt", help="Input file with Ethereum addresses")
    parser.add_argument("-o", "--output", default="balances.json", help="Output JSON file for results")
    parser.add_argument("-n", "--node", required=True, help="Ethereum node URL (e.g., Infura endpoint)")
    parser.add_argument("--workers", type=int, default=10, help="Number of concurrent threads (default: 10)")
    parser.add_argument("--no-save", action="store_true", help="Do not save results to file")
    parser.add_argument("-v", "--verbose", action="store_true", help="Enable verbose logging")
    return parser.parse_args()


# =========================
# Entry Point
# =========================
def main() -> None:
    start_time = time.time()
    args = parse_args()
    configure_logging(args.verbose)

    try:
        addresses = load_wallet_addresses(args.input)
        web3 = connect_to_node(args.node)
        balances = fetch_balances_concurrently(web3, addresses, args.workers)

        print(json.dumps(balances, indent=4, ensure_ascii=False))
        if not args.no_save:
            save_balances(balances, args.output)

        elapsed = time.time() - start_time
        logging.info(c(f"Completed in {elapsed:.2f} seconds", "green"))

    except KeyboardInterrupt:
        logging.warning("Interrupted by user. Exiting...")
    except Exception as e:
        logging.critical(f"Fatal error: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
