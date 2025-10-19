import json
import logging
import argparse
import time
from pathlib import Path
from typing import List, Dict, Union
from concurrent.futures import ThreadPoolExecutor, as_completed
from web3 import Web3
from web3.exceptions import InvalidAddress, BlockNotFound, TransactionNotFound

try:
    from tqdm import tqdm
    HAS_TQDM = True
except ImportError:
    HAS_TQDM = False


# =========================
# Configuration
# =========================
ETH_DECIMALS = 4
RETRY_ATTEMPTS = 3
RETRY_DELAY = 0.5


# =========================
# Setup
# =========================
def configure_logging(verbose: bool) -> None:
    """Configure logging format and verbosity level."""
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s | %(levelname)-8s | %(message)s",
    )


# =========================
# Wallet Loading
# =========================
def load_wallet_addresses(file_path: Union[str, Path]) -> List[str]:
    """Load Ethereum addresses from file and normalize them."""
    path = Path(file_path)
    if not path.is_file():
        raise FileNotFoundError(f"Input file not found: {path.resolve()}")

    with path.open("r", encoding="utf-8") as f:
        raw_addresses = {line.strip() for line in f if line.strip()}

    valid_addresses: List[str] = []
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
    web3 = Web3(Web3.HTTPProvider(node_url, request_kwargs={"timeout": 10}))
    if not web3.is_connected():
        raise ConnectionError(f"Failed to connect to Ethereum node: {node_url}")

    try:
        version = web3.client_version
    except Exception:
        version = "Unknown client"

    logging.info(f"Connected to Ethereum node: {version}")
    return web3


# =========================
# Balance Fetching
# =========================
def fetch_wallet_balance(web3: Web3, address: str) -> str:
    """Fetch ETH balance for a wallet with retries and error handling."""
    for attempt in range(1, RETRY_ATTEMPTS + 1):
        try:
            balance_wei = web3.eth.get_balance(address)
            balance_eth = web3.from_wei(balance_wei, "ether")
            return f"{balance_eth:.{ETH_DECIMALS}f} ETH"
        except (InvalidAddress, BlockNotFound, TransactionNotFound) as e:
            return f"Error: {e.__class__.__name__}"
        except Exception as e:
            if attempt < RETRY_ATTEMPTS:
                logging.debug(f"Retry {attempt}/{RETRY_ATTEMPTS} for {address}: {e}")
                time.sleep(RETRY_DELAY)
            else:
                logging.error(f"Failed to fetch balance for {address}: {e}")
                return f"Error: {e.__class__.__name__}"

    return "Error: Unknown"


def fetch_balances_concurrently(
    web3: Web3, addresses: List[str], max_workers: int
) -> Dict[str, str]:
    """Fetch balances for multiple addresses concurrently."""
    results: Dict[str, str] = {}
    logging.info(f"Fetching balances using {max_workers} workers...")

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(fetch_wallet_balance, web3, addr): addr for addr in addresses}
        iterator = tqdm(as_completed(futures), total=len(addresses), desc="Fetching") if HAS_TQDM else as_completed(futures)

        for future in iterator:
            address = futures[future]
            try:
                results[address] = future.result()
            except Exception as e:
                logging.error(f"Unhandled error for {address}: {e}")
                results[address] = f"Error: {e.__class__.__name__}"

    # Sort results: balances first (descending), errors last
    def sort_key(item):
        val = item[1]
        try:
            return (0, float(val.split()[0]))
        except Exception:
            return (1, -1)

    sorted_results = dict(sorted(results.items(), key=sort_key, reverse=True))
    logging.info("All balances fetched successfully.")
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
        logging.info(f"Balances saved to: {path.resolve()}")
    except IOError as e:
        logging.error(f"Failed to write file {path}: {e}")
        raise


# =========================
# CLI Parsing
# =========================
def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(description="Ethereum Wallet Balance Checker")
    parser.add_argument("-i", "--input", default="wallets.txt", help="Input file with Ethereum addresses")
    parser.add_argument("-o", "--output", default="balances.json", help="Output file to save results as JSON")
    parser.add_argument("-n", "--node", required=True, help="Ethereum node URL (e.g., Infura endpoint)")
    parser.add_argument("--workers", type=int, default=10, help="Number of concurrent threads (default: 10)")
    parser.add_argument("--no-save", action="store_true", help="Do not save results to file")
    parser.add_argument("-v", "--verbose", action="store_true", help="Enable verbose logging")
    return parser.parse_args()


# =========================
# Entry Point
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

        logging.info(f"Completed in {time.time() - start:.2f} seconds")

    except KeyboardInterrupt:
        logging.warning("Interrupted by user. Exiting...")
    except Exception as e:
        logging.critical(f"Fatal error: {e}", exc_info=True)
        exit(1)


if __name__ == "__main__":
    main()
