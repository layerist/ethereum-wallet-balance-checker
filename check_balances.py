import json
import logging
import argparse
from pathlib import Path
from typing import List, Dict, Union
from concurrent.futures import ThreadPoolExecutor, as_completed
from web3 import Web3
from web3.exceptions import InvalidAddress

ETH_DECIMALS: int = 4


def configure_logging(verbose: bool) -> None:
    """Set up the logging format and verbosity."""
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )


def load_wallet_addresses(file_path: Union[str, Path]) -> List[str]:
    """Load and validate Ethereum addresses from a text file."""
    path = Path(file_path)
    if not path.is_file():
        logging.error(f"Input file does not exist: {path}")
        raise FileNotFoundError(f"Input file not found: {path}")

    with path.open("r", encoding="utf-8") as f:
        addresses = {line.strip() for line in f if line.strip()}

    valid = [addr for addr in addresses if Web3.is_address(addr)]
    if not valid:
        raise ValueError("No valid Ethereum addresses found in the input file.")

    logging.info(f"Loaded {len(valid)} valid Ethereum addresses from {path}")
    return valid


def connect_to_node(node_url: str) -> Web3:
    """Establish a connection to the Ethereum node."""
    web3 = Web3(Web3.HTTPProvider(node_url))
    if not web3.is_connected():
        raise ConnectionError(f"Failed to connect to Ethereum node: {node_url}")
    logging.info(f"Connected to Ethereum node: {node_url}")
    return web3


def fetch_wallet_balance(web3: Web3, address: str) -> str:
    """Fetch the ETH balance of a given Ethereum address."""
    try:
        balance_wei = web3.eth.get_balance(address)
        balance_eth = web3.from_wei(balance_wei, "ether")
        return f"{balance_eth:.{ETH_DECIMALS}f} ETH"
    except InvalidAddress:
        logging.warning(f"Invalid address: {address}")
        return "Error: Invalid address"
    except Exception as e:
        logging.error(f"Error fetching balance for {address}: {e}")
        return f"Error: {e}"


def fetch_balances_concurrently(
    web3: Web3, addresses: List[str], max_workers: int
) -> Dict[str, str]:
    """Fetch wallet balances concurrently using threads."""
    results: Dict[str, str] = {}
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(fetch_wallet_balance, web3, addr): addr for addr in addresses}
        for future in as_completed(futures):
            address = futures[future]
            try:
                results[address] = future.result()
            except Exception as e:
                logging.error(f"Unhandled exception for {address}: {e}")
                results[address] = f"Error: {e}"
    return results


def save_balances(balances: Dict[str, str], output_path: Union[str, Path]) -> None:
    """Save wallet balances to a JSON file."""
    path = Path(output_path)
    try:
        with path.open("w", encoding="utf-8") as f:
            json.dump(balances, f, indent=4, ensure_ascii=False)
        logging.info(f"Balances saved to: {path}")
    except IOError as e:
        logging.error(f"Failed to write balances to {path}: {e}")
        raise


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(description="Ethereum Wallet Balance Checker")
    parser.add_argument("-i", "--input", default="wallets.txt", help="Path to input file")
    parser.add_argument("-o", "--output", default="balances.json", help="Path to output JSON file")
    parser.add_argument("-n", "--node", required=True, help="Ethereum node URL (e.g., Infura)")
    parser.add_argument("-v", "--verbose", action="store_true", help="Enable verbose logging")
    parser.add_argument("--no-save", action="store_true", help="Don't save balances to file")
    parser.add_argument("--workers", type=int, default=10, help="Number of concurrent workers")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    configure_logging(args.verbose)

    try:
        addresses = load_wallet_addresses(args.input)
        web3 = connect_to_node(args.node)
        balances = fetch_balances_concurrently(web3, addresses, args.workers)

        print(json.dumps(balances, indent=4, ensure_ascii=False))

        if not args.no_save:
            save_balances(balances, args.output)

    except Exception as e:
        logging.critical(f"Fatal error occurred: {e}")
        exit(1)


if __name__ == "__main__":
    main()
