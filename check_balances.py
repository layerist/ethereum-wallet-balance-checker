import json
from web3 import Web3

# Load wallet addresses from a file
def load_wallet_addresses(filename):
    with open(filename, 'r') as file:
        addresses = file.read().splitlines()
    return addresses

# Connect to an Ethereum node
def connect_to_ethereum_node(node_url):
    web3 = Web3(Web3.HTTPProvider(node_url))
    if not web3.isConnected():
        raise Exception("Failed to connect to the Ethereum node.")
    return web3

# Get the balance of a wallet address
def get_wallet_balance(web3, address):
    balance_wei = web3.eth.get_balance(address)
    balance_eth = web3.fromWei(balance_wei, 'ether')
    return balance_eth

# Check balances for all addresses in the list
def check_balances(addresses, web3):
    balances = {}
    for address in addresses:
        try:
            balance = get_wallet_balance(web3, address)
            balances[address] = balance
        except Exception as e:
            balances[address] = f"Error: {e}"
    return balances

# Save balances to a file
def save_balances_to_file(balances, filename):
    with open(filename, 'w') as file:
        json.dump(balances, file, indent=4)

# Main function
def main():
    input_filename = 'wallets.txt'
    output_filename = 'balances.json'
    node_url = 'https://mainnet.infura.io/v3/YOUR_INFURA_PROJECT_ID'  # Replace with your Infura project ID

    addresses = load_wallet_addresses(input_filename)
    web3 = connect_to_ethereum_node(node_url)
    balances = check_balances(addresses, web3)
    
    print(balances)
    save_balances_to_file(balances, output_filename)

if __name__ == '__main__':
    main()
