import json
import logging
import argparse
from typing import List, Dict
from concurrent.futures import ThreadPoolExecutor, as_completed
from web3 import Web3
from web3.exceptions import InvalidAddress


def configure_logging(verbose: bool) -> None:
    """Configure logging level and format."""
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s"
    )


def load_wallet_addresses(file_path: str) -> List[str]:
    """Load and validate unique Ethereum wallet addresses from a file."""
    try:
        with open(file_path, "r", encoding="utf-8") as file:
            addresses = {line.strip() for line in file if Web3.is_address(line.strip())}

        if not addresses:
            raise ValueError("No valid wallet addresses found in the input file.")

        logging.info("Loaded %d unique wallet addresses from '%s'.", len(addresses), file_path)
        return list(addresses)

    except FileNotFoundError:
        logging.error("File '%s' not found.", file_path)
        raise
    except Exception as e:
        logging.error("Failed to read from '%s': %s", file_path, e)
        raise


def connect_to_ethereum_node(node_url: str) -> Web3:
    """Establish connection to an Ethereum node."""
    web3 = Web3(Web3.HTTPProvider(node_url))
    if not web3.is_connected():
        raise ConnectionError(f"Could not connect to Ethereum node at '{node_url}'.")
    logging.info("Successfully connected to Ethereum node at '%s'.", node_url)
    return web3


def get_wallet_balance(web3: Web3, address: str) -> str:
    """Retrieve ETH balance for a given wallet address."""
    try:
        balance_wei = web3.eth.get_balance(address)
        balance_eth = web3.from_wei(balance_wei, "ether")
        logging.debug("Address %s: %.4f ETH", address, balance_eth)
        return f"{balance_eth:.4f} ETH"
    except InvalidAddress:
        logging.error("Invalid address: %s", address)
        return "Error: Invalid Address"
    except Exception as e:
        logging.error("Error fetching balance for %s: %s", address, e)
        return f"Error: {e}"


def check_balances(web3: Web3, addresses: List[str], max_workers: int) -> Dict[str, str]:
    """Check ETH balances concurrently using a thread pool."""
    balances: Dict[str, str] = {}
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_address = {executor.submit(get_wallet_balance, web3, addr): addr for addr in addresses}

        for future in as_completed(future_to_address):
            address = future_to_address[future]
            try:
                balances[address] = future.result()
            except Exception as e:
                logging.debug("Unexpected error for %s: %s", address, e)
                balances[address] = f"Error: {e}"
    return balances


def save_balances_to_file(balances: Dict[str, str], file_path: str) -> None:
    """Save balances to a JSON file."""
    try:
        with open(file_path, "w", encoding="utf-8") as file:
            json.dump(balances, file, indent=4)
        logging.info("Saved balances to '%s'.", file_path)
    except IOError as e:
        logging.error("Unable to write to '%s': %s", file_path, e)
        raise


def parse_arguments() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(description="Ethereum Wallet Balance Checker")
    parser.add_argument("-i", "--input", default="wallets.txt", help="Input file with wallet addresses")
    parser.add_argument("-o", "--output", default="balances.json", help="Output JSON file for balances")
    parser.add_argument("-n", "--node", required=True, help="Ethereum node URL (Infura or local)")
    parser.add_argument("-v", "--verbose", action="store_true", help="Enable verbose logging")
    parser.add_argument("--no-save", action="store_true", help="Do not save output to file")
    parser.add_argument("--workers", type=int, default=10, help="Number of threads to use (default: 10)")
    return parser.parse_args()


def main() -> None:
    """Main program entry point."""
    args = parse_arguments()
    configure_logging(args.verbose)

    try:
        addresses = load_wallet_addresses(args.input)
        web3 = connect_to_ethereum_node(args.node)
        balances = check_balances(web3, addresses, args.workers)

        print(json.dumps(balances, indent=4, ensure_ascii=False))

        if not args.no_save:
            save_balances_to_file(balances, args.output)

    except Exception as e:
        logging.error("Script execution failed: %s", e)
        exit(1)


if __name__ == "__main__":
    main()
