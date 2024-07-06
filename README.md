## Ethereum Wallet Balance Checker

This script checks the balance of Ethereum wallets listed in a file using the Web3 library and prints the balances to the console and saves them to a file.

### Prerequisites

- Python 3.6+
- `web3.py` library
- An Ethereum node endpoint (e.g., Infura)

### Installation

1. Clone the repository:
    ```bash
    git clone https://github.com/layerist/ethereum-wallet-balance-checker.git
    cd ethereum-wallet-balance-checker
    ```

2. Install the required Python packages:
    ```bash
    pip install web3
    ```

### Configuration

1. Create a file named `wallets.txt` in the project directory and list the Ethereum wallet addresses you want to check, one per line.

2. Replace `YOUR_INFURA_PROJECT_ID` in the script with your actual Infura project ID or another Ethereum node endpoint.

### Usage

Run the script:
```bash
python check_balances.py
```

### Script Explanation

- **load_wallet_addresses(filename):** Reads wallet addresses from a specified file.
- **connect_to_ethereum_node(node_url):** Connects to an Ethereum node using a provided URL.
- **get_wallet_balance(web3, address):** Retrieves the balance of a given wallet address in Ether.
- **check_balances(addresses, web3):** Checks the balance of all addresses and handles errors.
- **save_balances_to_file(balances, filename):** Saves the balance information to a specified file.
- **main():** The main function that orchestrates the loading of addresses, connecting to the node, checking balances, and saving the results.

### Example Output

Console:
```json
{
    "0xAddress1": "Balance1 ETH",
    "0xAddress2": "Balance2 ETH",
    "0xAddress3": "Error: Some error message"
}
```

`balances.json`:
```json
{
    "0xAddress1": "Balance1 ETH",
    "0xAddress2": "Balance2 ETH",
    "0xAddress3": "Error: Some error message"
}
```

### License

This project is licensed under the MIT License.
