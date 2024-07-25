import json
from web3 import Web3

def load_wallet_addresses(filename):
    """Load wallet addresses from a file."""
    with open(filename, 'r') as file:
        addresses = file.read().splitlines()
    return addresses

def connect_to_ethereum_node(node_url):
    """Connect to an Ethereum node."""
    web3 = Web3(Web3.HTTPProvider(node_url))
    if not web3.isConnected():
        raise Exception("Failed to connect to the Ethereum node.")
    return web3

def get_wallet_balance(web3, address):
    """Get the balance of a wallet address."""
    balance_wei = web3.eth.get_balance(address)
    balance_eth = web3.fromWei(balance_wei, 'ether')
    return balance_eth

def check_balances(addresses, web3):
    """Check balances for all addresses in the list."""
    balances = {}
    for address in addresses:
        try:
            balance = get_wallet_balance(web3, address)
            balances[address] = balance
        except Exception as e:
            balances[address] = f"Error: {e}"
    return balances

def save_balances_to_file(balances, filename):
    """Save balances to a file."""
    with open(filename, 'w') as file:
        json.dump(balances, file, indent=4)

def main():
    """Main function."""
    input_filename = 'wallets.txt'
    output_filename = 'balances.json'
    node_url = 'https://mainnet.infura.io/v3/YOUR_INFURA_PROJECT_ID'  # Replace with your Infura project ID

    try:
        addresses = load_wallet_addresses(input_filename)
    except FileNotFoundError:
        print(f"Error: The file {input_filename} was not found.")
        return

    try:
        web3 = connect_to_ethereum_node(node_url)
    except Exception as e:
        print(f"Error: {e}")
        return

    balances = check_balances(addresses, web3)
    
    print(json.dumps(balances, indent=4))

    try:
        save_balances_to_file(balances, output_filename)
    except IOError as e:
        print(f"Error saving to file: {e}")

if __name__ == '__main__':
    main()
