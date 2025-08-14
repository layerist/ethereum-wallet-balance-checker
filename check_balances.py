import json
import logging
import argparse
import time
from pathlib import Path
from typing import List, Dict, Union
from concurrent.futures import ThreadPoolExecutor, as_completed
from web3 import Web3
from web3.exceptions import InvalidAddress
from functools import partial

try:
    from tqdm import tqdm
    TQDM_AVAILABLE = True
except ImportError:
    TQDM_AVAILABLE = False

ETH_DECIMALS: int = 4
RETRY_ATTEMPTS: int = 3
RETRY_DELAY: float = 0.5


def configure_logging(verbose: bool) -> None:
    """Set up logging format and verbosity."""
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )


def load_wallet_addresses(file_path: Union[str, Path]) -> List[str]:
    """Load, normalize, and validate Ethereum addresses from a file."""
    path = Path(file_path)
    if not path.is_file():
        logging.error(f"Input file does not exist: {path}")
        raise FileNotFoundError(f"Input file not found: {path}")

    with path.open("r", encoding="utf-8") as f:
        raw_addresses = {line.strip() for line in f if line.strip()}

    valid = []
    for addr in raw_addresses:
        if Web3.is_address(addr):
            try:
                valid.append(Web3.to_checksum_address(addr))
            except Exception:
                continue

    if not valid:
        raise ValueError("No valid Ethereum addresses found in the input file.")

    logging.info(f"Loaded {len(valid)} valid Ethereum addresses from {path}")
    return valid


def connect_to_node(node_url: str) -> Web3:
    """Connect to Ethereum node."""
    web3 = Web3(Web3.HTTPProvider(node_url))
    if not web3.is_connected():
        raise ConnectionError(f"Failed to connect to Ethereum node: {node_url}")

    try:
        node_version = web3.client_version
    except Exception:
        node_version = "Unknown"

    logging.info(f"Connected to Ethereum node ({node_version}): {node_url}")
    return web3


def fetch_wallet_balance(web3: Web3, address: str) -> str:
    """Fetch ETH balance with retry logic."""
    for attempt in range(RETRY_ATTEMPTS):
        try:
            balance_wei = web3.eth.get_balance(address)
            balance_eth = web3.from_wei(balance_wei, "ether")
            return f"{balance_eth:.{ETH_DECIMALS}f} ETH"
        except InvalidAddress:
            return "Error: Invalid address"
        except Exception as e:
            if attempt < RETRY_ATTEMPTS - 1:
                time.sleep(RETRY_DELAY)
            else:
                return f"Error: {e}"


def fetch_balances_concurrently(
    web3: Web3, addresses: List[str], max_workers: int
) -> Dict[str, str]:
    """Fetch balances concurrently with optional progress bar."""
    results: Dict[str, str] = {}
    task_iter = (as_completed(
        {ThreadPoolExecutor(max_workers=max_workers).submit(fetch_wallet_balance, web3, addr): addr for addr in addresses}
    ))

    if TQDM_AVAILABLE:
        task_iter = tqdm(task_iter, total=len(addresses), desc="Fetching balances")

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(fetch_wallet_balance, web3, addr): addr for addr in addresses}
        for future in (tqdm(as_completed(futures), total=len(addresses), desc="Fetching balances") if TQDM_AVAILABLE else as_completed(futures)):
            address = futures[future]
            try:
                results[address] = future.result()
            except Exception as e:
                logging.error(f"Unhandled exception for {address}: {e}")
                results[address] = f"Error: {e}"
    return dict(sorted(results.items(), key=lambda x: (x[1] != "Error" and float(x[1].split()[0]) or -1), reverse=True))


def save_balances(balances: Dict[str, str], output_path: Union[str, Path]) -> None:
    """Save balances to JSON."""
    path = Path(output_path)
    try:
        with path.open("w", encoding="utf-8") as f:
            json.dump(balances, f, indent=4, ensure_ascii=False)
        logging.info(f"Balances saved to: {path}")
    except IOError as e:
        logging.error(f"Failed to write balances to {path}: {e}")
        raise


def parse_args() -> argparse.Namespace:
    """Parse CLI arguments."""
    parser = argparse.ArgumentParser(description="Ethereum Wallet Balance Checker")
    parser.add_argument("-i", "--input", default="wallets.txt", help="Path to input file")
    parser.add_argument("-o", "--output", default="balances.json", help="Path to output JSON file")
    parser.add_argument("-n", "--node", required=True, help="Ethereum node URL (e.g., Infura)")
    parser.add_argument("-v", "--verbose", action="store_true", help="Enable verbose logging")
    parser.add_argument("--no-save", action="store_true", help="Don't save balances to file")
    parser.add_argument("--workers", type=int, default=10, help="Number of concurrent workers")
    return parser.parse_args()


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

        logging.info(f"Execution completed in {time.time() - start_time:.2f} seconds")

    except Exception as e:
        logging.critical(f"Fatal error occurred: {e}")
        exit(1)


if __name__ == "__main__":
    main()
