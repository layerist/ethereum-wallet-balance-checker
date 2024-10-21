import json
import logging
import argparse
from web3 import Web3
from web3.exceptions import InvalidAddress

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def load_wallet_addresses(filename):
    """
    Load wallet addresses from a file and remove duplicates.

    Args:
        filename (str): Path to the file containing wallet addresses.

    Returns:
        list: List of wallet addresses.
    """
    try:
        with open(filename, 'r') as file:
            addresses = set(line.strip() for line in file if line.strip())
        logging.info(f"Loaded {len(addresses)} unique addresses from {filename}.")
        return list(addresses)
    except FileNotFoundError:
        logging.error(f"File not found: {filename}")
        raise
    except Exception as e:
        logging.error(f"Error while reading {filename}: {e}")
        raise

def connect_to_ethereum_node(node_url):
    """
    Connect to an Ethereum node.

    Args:
        node_url (str): Ethereum node URL.

    Returns:
        Web3: Instance of Web3 connected to the Ethereum node.
    """
    try:
        web3 = Web3(Web3.HTTPProvider(node_url))
        if not web3.isConnected():
            raise ConnectionError(f"Could not connect to the Ethereum node: {node_url}")
        logging.info("Successfully connected to the Ethereum node.")
        return web3
    except Exception as e:
        logging.error(f"Error connecting to Ethereum node: {e}")
        raise

def get_wallet_balance(web3, address):
    """
    Get the balance of a wallet address.

    Args:
        web3 (Web3): Web3 instance.
        address (str): Wallet address.

    Returns:
        float: Wallet balance in Ether.
    """
    try:
        if not web3.isAddress(address):
            raise InvalidAddress(f"Invalid Ethereum address: {address}")

        balance_wei = web3.eth.get_balance(address)
        balance_eth = web3.fromWei(balance_wei, 'ether')
        logging.info(f"Balance for {address}: {balance_eth:.4f} ETH")
        return balance_eth
    except InvalidAddress as e:
        logging.error(f"Invalid address: {e}")
        raise
    except Exception as e:
        logging.error(f"Failed to retrieve balance for {address}: {e}")
        raise

def check_balances(addresses, web3):
    """
    Check balances for all wallet addresses.

    Args:
        addresses (list): List of wallet addresses.
        web3 (Web3): Web3 instance connected to the Ethereum node.

    Returns:
        dict: Dictionary of wallet addresses and their balances or errors.
    """
    balances = {}
    for address in addresses:
        try:
            balances[address] = str(get_wallet_balance(web3, address))
        except Exception as e:
            balances[address] = f"Error: {e}"
            logging.debug(f"Error processing balance for {address}: {e}")
    return balances

def save_balances_to_file(balances, filename):
    """
    Save wallet balances to a JSON file.

    Args:
        balances (dict): Dictionary of wallet balances.
        filename (str): Output file path.
    """
    try:
        with open(filename, 'w') as file:
            json.dump(balances, file, indent=4)
        logging.info(f"Balances successfully saved to {filename}.")
    except IOError as e:
        logging.error(f"Error saving balances to {filename}: {e}")
        raise

def parse_arguments():
    """
    Parse command-line arguments.

    Returns:
        argparse.Namespace: Parsed arguments.
    """
    parser = argparse.ArgumentParser(description="Check Ethereum wallet balances.")
    parser.add_argument('-i', '--input', type=str, default='wallets.txt', help='Input file with wallet addresses')
    parser.add_argument('-o', '--output', type=str, default='balances.json', help='Output file for saving balances')
    parser.add_argument('-n', '--node', type=str, required=True, help='Ethereum node URL (e.g., Infura or local)')
    parser.add_argument('-v', '--verbose', action='store_true', help='Increase output verbosity')
    parser.add_argument('--no-save', action='store_true', help="Do not save balances to file")
    return parser.parse_args()

def main():
    """
    Main function to execute the script.
    """
    args = parse_arguments()

    # Adjust logging level based on verbosity
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    try:
        addresses = load_wallet_addresses(args.input)
        web3 = connect_to_ethereum_node(args.node)
        balances = check_balances(addresses, web3)
        print(json.dumps(balances, indent=4))  # Print balances to console

        if not args.no_save:
            save_balances_to_file(balances, args.output)
    except Exception as e:
        logging.error(f"An error occurred during execution: {e}")
        exit(1)

if __name__ == '__main__':
    main()
