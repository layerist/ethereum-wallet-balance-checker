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
        format="%(asctime)s - %(levelname)s - %(message)s",
    )

def load_wallet_addresses(filename: str) -> List[str]:
    """Load unique wallet addresses from a file."""
    try:
        with open(filename, "r", encoding="utf-8") as file:
            addresses = {line.strip() for line in file if Web3.is_address(line.strip())}
        
        if not addresses:
            raise ValueError("No valid wallet addresses found in the input file.")
        
        logging.info(f"Loaded {len(addresses)} unique wallet addresses from '{filename}'.")
        return list(addresses)
    except FileNotFoundError:
        logging.error(f"File '{filename}' not found. Provide a valid file.")
        raise
    except Exception as e:
        logging.error(f"Error reading file '{filename}': {e}")
        raise

def connect_to_ethereum_node(node_url: str) -> Web3:
    """Establish connection to an Ethereum node."""
    web3 = Web3(Web3.HTTPProvider(node_url))
    if not web3.is_connected():
        raise ConnectionError(f"Failed to connect to Ethereum node at '{node_url}'.")
    logging.info(f"Connected to Ethereum node at '{node_url}'.")
    return web3

def get_wallet_balance(web3: Web3, address: str) -> str:
    """Retrieve the ETH balance of a wallet."""
    try:
        balance_wei = web3.eth.get_balance(address)
        balance_eth = web3.from_wei(balance_wei, "ether")
        logging.debug(f"Address {address}: {balance_eth:.4f} ETH")
        return f"{balance_eth:.4f} ETH"
    except InvalidAddress:
        logging.error(f"Invalid Ethereum address: {address}")
        return "Error: Invalid Address"
    except Exception as e:
        logging.error(f"Error fetching balance for '{address}': {e}")
        return f"Error: {e}"

def check_balances(web3: Web3, addresses: List[str], max_workers: int = 10) -> Dict[str, str]:
    """Fetch balances for multiple wallet addresses using multithreading."""
    balances = {}
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_address = {executor.submit(get_wallet_balance, web3, addr): addr for addr in addresses}
        for future in as_completed(future_to_address):
            address = future_to_address[future]
            try:
                balances[address] = future.result()
            except Exception as e:
                balances[address] = f"Error: {e}"
                logging.debug(f"Failed to retrieve balance for {address}: {e}")
    return balances

def save_balances_to_file(balances: Dict[str, str], filename: str) -> None:
    """Save wallet balances to a JSON file."""
    try:
        with open(filename, "w", encoding="utf-8") as file:
            json.dump(balances, file, indent=4)
        logging.info(f"Balances saved to '{filename}'.")
    except IOError as e:
        logging.error(f"Error saving balances to file '{filename}': {e}")
        raise

def parse_arguments() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(description="Ethereum Wallet Balance Checker")
    parser.add_argument("-i", "--input", type=str, default="wallets.txt", help="Input file with wallet addresses.")
    parser.add_argument("-o", "--output", type=str, default="balances.json", help="Output file to save wallet balances.")
    parser.add_argument("-n", "--node", type=str, required=True, help="Ethereum node URL (e.g., Infura or local node).")
    parser.add_argument("-v", "--verbose", action="store_true", help="Enable verbose logging output.")
    parser.add_argument("--no-save", action="store_true", help="Skip saving balances to a file.")
    parser.add_argument("--workers", type=int, default=10, help="Number of worker threads for balance checking.")
    return parser.parse_args()

def main() -> None:
    """Main execution function."""
    args = parse_arguments()
    configure_logging(args.verbose)
    try:
        addresses = load_wallet_addresses(args.input)
        web3 = connect_to_ethereum_node(args.node)
        balances = check_balances(web3, addresses, args.workers)
        print(json.dumps(balances, indent=4))
        if not args.no_save:
            save_balances_to_file(balances, args.output)
    except Exception as e:
        logging.error(f"Execution failed: {e}")
        exit(1)

if __name__ == "__main__":
    main()
