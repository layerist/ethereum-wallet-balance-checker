import json
import logging
import argparse
from pathlib import Path
from typing import List, Dict
from concurrent.futures import ThreadPoolExecutor, as_completed
from web3 import Web3
from web3.exceptions import InvalidAddress


ETH_PRECISION = 4


def configure_logging(verbose: bool) -> None:
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s"
    )


def load_wallet_addresses(file_path: str | Path) -> List[str]:
    path = Path(file_path)
    if not path.exists():
        logging.error("Input file '%s' not found.", path)
        raise FileNotFoundError(f"Input file not found: {path}")
    
    with path.open("r", encoding="utf-8") as f:
        addresses = {line.strip() for line in f if Web3.is_address(line.strip())}

    if not addresses:
        raise ValueError("No valid Ethereum addresses found in the input file.")

    logging.info("Loaded %d unique valid wallet addresses from '%s'.", len(addresses), path)
    return list(addresses)


def connect_to_ethereum_node(node_url: str) -> Web3:
    web3 = Web3(Web3.HTTPProvider(node_url))
    if not web3.is_connected():
        raise ConnectionError(f"Unable to connect to Ethereum node at '{node_url}'")
    logging.info("Connected to Ethereum node at '%s'.", node_url)
    return web3


def get_wallet_balance(web3: Web3, address: str) -> str:
    try:
        balance_wei = web3.eth.get_balance(address)
        balance_eth = web3.from_wei(balance_wei, "ether")
        logging.debug("Fetched balance for %s: %.{prec}f ETH", address, balance_eth, extra={"prec": ETH_PRECISION})
        return f"{balance_eth:.{ETH_PRECISION}f} ETH"
    except InvalidAddress:
        logging.error("Invalid Ethereum address: %s", address)
        return "Error: Invalid address"
    except Exception as e:
        logging.exception("Failed to retrieve balance for %s", address)
        return f"Error: {e}"


def check_balances(web3: Web3, addresses: List[str], max_workers: int) -> Dict[str, str]:
    balances: Dict[str, str] = {}
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_address = {
            executor.submit(get_wallet_balance, web3, address): address for address in addresses
        }

        for future in as_completed(future_to_address):
            address = future_to_address[future]
            try:
                balances[address] = future.result()
            except Exception as e:
                logging.error("Unhandled error for %s: %s", address, e)
                balances[address] = f"Error: {e}"
    return balances


def save_balances_to_file(balances: Dict[str, str], file_path: str | Path) -> None:
    try:
        path = Path(file_path)
        with path.open("w", encoding="utf-8") as f:
            json.dump(balances, f, indent=4)
        logging.info("Balances saved to '%s'.", path)
    except IOError as e:
        logging.error("Failed to save file '%s': %s", file_path, e)
        raise


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Ethereum Wallet Balance Checker")
    parser.add_argument("-i", "--input", default="wallets.txt", help="Input file with wallet addresses")
    parser.add_argument("-o", "--output", default="balances.json", help="Output JSON file for balances")
    parser.add_argument("-n", "--node", required=True, help="Ethereum node URL (e.g., Infura)")
    parser.add_argument("-v", "--verbose", action="store_true", help="Enable detailed logging")
    parser.add_argument("--no-save", action="store_true", help="Do not save balances to file")
    parser.add_argument("--workers", type=int, default=10, help="Number of threads (default: 10)")
    return parser.parse_args()


def main() -> None:
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
        logging.critical("Script execution failed: %s", e)
        exit(1)


if __name__ == "__main__":
    main()
