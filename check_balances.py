import json
import logging
import argparse
from web3 import Web3
from web3.exceptions import InvalidAddress

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def load_wallet_addresses(filename: str) -> list[str]:
    """
    Load wallet addresses from a file, remove duplicates, and return the list.
    """
    try:
        with open(filename, 'r') as file:
            addresses = {line.strip() for line in file if line.strip()}
        logging.info(f"Loaded {len(addresses)} unique addresses from '{filename}'.")
        return list(addresses)
    except FileNotFoundError:
        logging.error(f"File '{filename}' not found.")
        raise
    except Exception as e:
        logging.error(f"Error reading '{filename}': {e}")
        raise

def connect_to_ethereum_node(node_url: str) -> Web3:
    """
    Connect to an Ethereum node via the provided node URL.
    """
    try:
        web3 = Web3(Web3.HTTPProvider(node_url))
        if not web3.isConnected():
            raise ConnectionError(f"Failed to connect to Ethereum node at '{node_url}'.")
        logging.info(f"Connected to Ethereum node at '{node_url}'.")
        return web3
    except Exception as e:
        logging.error(f"Error connecting to Ethereum node '{node_url}': {e}")
        raise

def get_wallet_balance(web3: Web3, address: str) -> float:
    """
    Retrieve the Ether balance of a wallet address.
    """
    try:
        if not web3.isAddress(address):
            raise InvalidAddress(f"Invalid Ethereum address: '{address}'")
        balance_wei = web3.eth.get_balance(address)
        balance_eth = web3.fromWei(balance_wei, 'ether')
        logging.info(f"Balance of {address}: {balance_eth:.4f} ETH")
        return balance_eth
    except InvalidAddress as e:
        logging.error(e)
        raise
    except Exception as e:
        logging.error(f"Failed to retrieve balance for address '{address}': {e}")
        raise

def check_balances(addresses: list[str], web3: Web3) -> dict[str, str]:
    """
    Check balances for a list of wallet addresses and return them as a dictionary.
    """
    balances = {}
    for address in addresses:
        try:
            balance = get_wallet_balance(web3, address)
            balances[address] = f"{balance:.4f} ETH"
        except Exception as e:
            balances[address] = f"Error: {e}"
            logging.debug(f"Error retrieving balance for {address}: {e}")
    return balances

def save_balances_to_file(balances: dict[str, str], filename: str) -> None:
    """
    Save wallet balances to a JSON file.
    """
    try:
        with open(filename, 'w') as file:
            json.dump(balances, file, indent=4)
        logging.info(f"Balances saved to '{filename}'.")
    except IOError as e:
        logging.error(f"Error saving balances to '{filename}': {e}")
        raise

def parse_arguments() -> argparse.Namespace:
    """
    Parse and return command-line arguments.
    """
    parser = argparse.ArgumentParser(description="Check Ethereum wallet balances.")
    parser.add_argument('-i', '--input', type=str, default='wallets.txt', help='Input file with wallet addresses.')
    parser.add_argument('-o', '--output', type=str, default='balances.json', help='Output file to save balances.')
    parser.add_argument('-n', '--node', type=str, required=True, help='Ethereum node URL (e.g., Infura or local node).')
    parser.add_argument('-v', '--verbose', action='store_true', help='Enable verbose logging.')
    parser.add_argument('--no-save', action='store_true', help="Do not save balances to file.")
    return parser.parse_args()

def main() -> None:
    """
    Main function to execute the Ethereum wallet balance checker.
    """
    args = parse_arguments()

    # Set logging level based on verbosity
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    try:
        addresses = load_wallet_addresses(args.input)
        web3 = connect_to_ethereum_node(args.node)
        balances = check_balances(addresses, web3)

        print(json.dumps(balances, indent=4))  # Output balances to console

        if not args.no_save:
            save_balances_to_file(balances, args.output)
    except Exception as e:
        logging.error(f"Execution failed: {e}")
        exit(1)

if __name__ == '__main__':
    main()
