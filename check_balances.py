import json
import logging
import argparse
from web3 import Web3

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def load_wallet_addresses(filename):
    """Load wallet addresses from a file."""
    try:
        with open(filename, 'r') as file:
            addresses = [line.strip() for line in file if line.strip()]
        logging.info(f"Successfully loaded {len(addresses)} addresses from {filename}.")
        return addresses
    except FileNotFoundError:
        logging.error(f"The file {filename} was not found.")
        raise
    except Exception as e:
        logging.error(f"An unexpected error occurred while reading {filename}: {e}")
        raise

def connect_to_ethereum_node(node_url):
    """Connect to an Ethereum node."""
    try:
        web3 = Web3(Web3.HTTPProvider(node_url))
        if web3.isConnected():
            logging.info("Successfully connected to the Ethereum node.")
            return web3
        else:
            logging.error("Failed to connect to the Ethereum node. Please check the URL.")
            raise ConnectionError("Failed to connect to the Ethereum node.")
    except Exception as e:
        logging.error(f"An error occurred while connecting to the Ethereum node: {e}")
        raise

def get_wallet_balance(web3, address):
    """Get the balance of a wallet address."""
    try:
        balance_wei = web3.eth.get_balance(address)
        balance_eth = web3.fromWei(balance_wei, 'ether')
        logging.info(f"Balance for {address}: {balance_eth} ETH")
        return balance_eth
    except ValueError:
        logging.error(f"Invalid address: {address}")
        raise
    except Exception as e:
        logging.error(f"Failed to retrieve balance for {address}: {e}")
        raise

def check_balances(addresses, web3):
    """Check balances for all addresses in the list."""
    balances = {}
    for address in addresses:
        try:
            balances[address] = get_wallet_balance(web3, address)
        except Exception as e:
            balances[address] = f"Error: {e}"
    return balances

def save_balances_to_file(balances, filename):
    """Save balances to a file."""
    try:
        with open(filename, 'w') as file:
            json.dump(balances, file, indent=4)
        logging.info(f"Successfully saved balances to {filename}.")
    except IOError as e:
        logging.error(f"Failed to save balances to {filename}: {e}")
        raise

def parse_arguments():
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(description="Check Ethereum wallet balances.")
    parser.add_argument('-i', '--input', type=str, default='wallets.txt', help='Input file containing wallet addresses')
    parser.add_argument('-o', '--output', type=str, default='balances.json', help='Output file to save balances')
    parser.add_argument('-n', '--node', type=str, required=True, help='Ethereum node URL')
    return parser.parse_args()

def main():
    """Main function."""
    args = parse_arguments()

    try:
        addresses = load_wallet_addresses(args.input)
        web3 = connect_to_ethereum_node(args.node)
        balances = check_balances(addresses, web3)
        print(json.dumps(balances, indent=4))
        save_balances_to_file(balances, args.output)
    except Exception as e:
        logging.error(f"An error occurred: {e}")
        exit(1)

if __name__ == '__main__':
    main()
