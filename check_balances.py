import json
import logging
from web3 import Web3

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def load_wallet_addresses(filename):
    """Load wallet addresses from a file."""
    try:
        with open(filename, 'r') as file:
            addresses = file.read().splitlines()
        logging.info(f"Successfully loaded {len(addresses)} addresses from {filename}.")
        return addresses
    except FileNotFoundError:
        logging.error(f"The file {filename} was not found.")
        raise
    except Exception as e:
        logging.error(f"An error occurred while reading {filename}: {e}")
        raise

def connect_to_ethereum_node(node_url):
    """Connect to an Ethereum node."""
    web3 = Web3(Web3.HTTPProvider(node_url))
    if web3.isConnected():
        logging.info("Successfully connected to the Ethereum node.")
    else:
        logging.error("Failed to connect to the Ethereum node.")
        raise ConnectionError("Failed to connect to the Ethereum node.")
    return web3

def get_wallet_balance(web3, address):
    """Get the balance of a wallet address."""
    try:
        balance_wei = web3.eth.get_balance(address)
        balance_eth = web3.fromWei(balance_wei, 'ether')
        logging.info(f"Balance for {address}: {balance_eth} ETH")
        return balance_eth
    except Exception as e:
        logging.error(f"Failed to get balance for {address}: {e}")
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
        logging.error(f"Error saving to file {filename}: {e}")
        raise

def main():
    """Main function."""
    input_filename = 'wallets.txt'
    output_filename = 'balances.json'
    node_url = 'https://mainnet.infura.io/v3/YOUR_INFURA_PROJECT_ID'  # Replace with your Infura project ID

    try:
        addresses = load_wallet_addresses(input_filename)
        web3 = connect_to_ethereum_node(node_url)
        balances = check_balances(addresses, web3)
        print(json.dumps(balances, indent=4))
        save_balances_to_file(balances, output_filename)
    except Exception as e:
        logging.error(f"An error occurred: {e}")

if __name__ == '__main__':
    main()
