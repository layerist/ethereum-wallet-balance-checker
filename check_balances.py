import json
import logging
import argparse
from pathlib import Path
from typing import List, Dict, Union
from concurrent.futures import ThreadPoolExecutor, as_completed
from web3 import Web3
from web3.exceptions import InvalidAddress

ETH_DECIMALS = 4


def configure_logging(verbose: bool) -> None:
    """Configure the logging system."""
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )


def load_wallet_addresses(file_path: Union[str, Path]) -> List[str]:
    """
    Load unique Ethereum addresses from a file and validate them.
    """
    path = Path(file_path)
    if not path.is_file():
        logging.error("The specified input file does not exist: %s", path)
        raise FileNotFoundError(f"Input file not found: {path}")

    with path.open("r", encoding="utf-8") as f:
        raw_lines = {line.strip() for line in f if line.strip()}

    valid_addresses = [addr for addr in raw_lines if Web3.is_address(addr)]

    if not valid_addresses:
        raise ValueError("No valid Ethereum addresses found in the provided file.")

    logging.info("Successfully loaded %d valid addresses from %s", len(valid_addresses), path)
    return valid_addresses


def connect_to_node(node_url: str) -> Web3:
    """
    Establish a Web3 connection to the specified Ethereum node.
    """
    web3 = Web3(Web3.HTTPProvider(node_url))
    if not web3.is_connected():
        raise ConnectionError(f"Could not connect to Ethereum node at {node_url}")
    logging.info("Successfully connected to Ethereum node: %s", node_url)
    return web3


def fetch_wallet_balance(web3: Web3, address: str) -> str:
    """
    Retrieve the ETH balance for a specific wallet address.
    """
    try:
        balance_wei = web3.eth.get_balance(address)
        balance_eth = web3.from_wei(balance_wei, "ether")
        logging.debug("Balance for %s: %.4f ETH", address, balance_eth)
        return f"{balance_eth:.{ETH_DECIMALS}f} ETH"
    except InvalidAddress:
        logging.error("Invalid Ethereum address encountered: %s", address)
        return "Error: Invalid address"
    except Exception as e:
        logging.exception("Failed to retrieve balance for %s", address)
        return f"Error: {e}"


def fetch_balances_concurrently(web3: Web3, addresses: List[str], max_workers: int) -> Dict[str, str]:
    """
    Fetch balances for multiple addresses using a thread pool.
    """
    balances: Dict[str, str] = {}
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_address = {
            executor.submit(fetch_wallet_balance, web3, address): address for address in addresses
        }
        for future in as_completed(future_to_address):
            address = future_to_address[future]
            try:
                balances[address] = future.result()
            except Exception as e:
                logging.error("Unexpected error while processing %s: %s", address, e)
                balances[address] = f"Error: {e}"
    return balances


def save_balances(balances: Dict[str, str], output_file: Union[str, Path]) -> None:
    """
    Persist the balances dictionary to a JSON file.
    """
    path = Path(output_file)
    try:
        with path.open("w", encoding="utf-8") as f:
            json.dump(balances, f, indent=4, ensure_ascii=False)
        logging.info("Balances have been written to %s", path)
    except IOError as e:
        logging.error("Could not write balances to %s: %s", path, e)
        raise


def parse_args() -> argparse.Namespace:
    """
    Parse command-line arguments.
    """
    parser = argparse.ArgumentParser(description="Ethereum wallet balance checker")
    parser.add_argument(
        "-i", "--input", default="wallets.txt", help="Input file containing wallet addresses"
    )
    parser.add_argument(
        "-o", "--output", default="balances.json", help="Output file to store balances"
    )
    parser.add_argument(
        "-n", "--node", required=True, help="Ethereum node URL (for example Infura endpoint)"
    )
    parser.add_argument(
        "-v", "--verbose", action="store_true", help="Enable verbose debug output"
    )
    parser.add_argument(
        "--no-save", action="store_true", help="Skip saving the balances to file"
    )
    parser.add_argument(
        "--workers", type=int, default=10, help="Number of concurrent worker threads (default: 10)"
    )
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
        logging.critical("Fatal error: %s", e)
        exit(1)


if __name__ == "__main__":
    main()
