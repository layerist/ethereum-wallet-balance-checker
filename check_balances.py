import json
import logging
import argparse
from web3 import Web3
from web3.exceptions import InvalidAddress


def configure_logging(verbose: bool) -> None:
    """
    Configure logging level and format based on verbosity.
    """
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )


def load_wallet_addresses(filename: str) -> list[str]:
    """
    Load wallet addresses from a file, removing duplicates and empty lines.
    
    Args:
        filename (str): Path to the file containing wallet addresses.
    
    Returns:
        list[str]: List of unique wallet addresses.
    
    Raises:
        ValueError: If no valid addresses are found in the file.
        FileNotFoundError: If the input file doesn't exist.
    """
    try:
        with open(filename, "r") as file:
            addresses = {line.strip() for line in file if line.strip()}
        if not addresses:
            raise ValueError("The input file contains no valid addresses.")
        logging.info(f"Loaded {len(addresses)} unique wallet addresses from '{filename}'.")
        return list(addresses)
    except FileNotFoundError:
        logging.error(f"File '{filename}' not found. Please provide a valid input file.")
        raise
    except Exception as e:
        logging.error(f"Error reading from '{filename}': {e}")
        raise


def connect_to_ethereum_node(node_url: str) -> Web3:
    """
    Connect to an Ethereum node using the provided URL.
    
    Args:
        node_url (str): Ethereum node URL (e.g., Infura or local node).
    
    Returns:
        Web3: An instance of Web3 connected to the node.
    
    Raises:
        ConnectionError: If the connection to the Ethereum node fails.
    """
    try:
        web3 = Web3(Web3.HTTPProvider(node_url))
        if not web3.isConnected():
            raise ConnectionError(f"Unable to connect to Ethereum node at '{node_url}'.")
        logging.info(f"Successfully connected to Ethereum node at '{node_url}'.")
        return web3
    except Exception as e:
        logging.error(f"Error connecting to Ethereum node '{node_url}': {e}")
        raise


def get_wallet_balance(web3: Web3, address: str) -> float:
    """
    Get the Ether balance of a wallet address in ETH.
    
    Args:
        web3 (Web3): Web3 instance to interact with Ethereum.
        address (str): Ethereum wallet address.
    
    Returns:
        float: Balance in Ether.
    
    Raises:
        InvalidAddress: If the address is not valid.
    """
    try:
        if not web3.isAddress(address):
            raise InvalidAddress(f"Invalid Ethereum address: '{address}'.")
        balance_wei = web3.eth.get_balance(address)
        balance_eth = web3.fromWei(balance_wei, "ether")
        logging.debug(f"Address {address}: Balance = {balance_eth:.4f} ETH")
        return balance_eth
    except InvalidAddress as e:
        logging.error(f"Invalid address '{address}': {e}")
        raise
    except Exception as e:
        logging.error(f"Failed to fetch balance for address '{address}': {e}")
        raise


def check_balances(addresses: list[str], web3: Web3) -> dict[str, str]:
    """
    Retrieve balances for a list of wallet addresses.
    
    Args:
        addresses (list[str]): List of wallet addresses to check.
        web3 (Web3): Web3 instance to interact with Ethereum.
    
    Returns:
        dict[str, str]: Dictionary mapping addresses to their balance in ETH or error messages.
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
    
    Args:
        balances (dict[str, str]): Dictionary of wallet addresses and their balances.
        filename (str): Path to the output file.
    
    Raises:
        IOError: If an error occurs while writing to the file.
    """
    try:
        with open(filename, "w") as file:
            json.dump(balances, file, indent=4)
        logging.info(f"Balances successfully saved to '{filename}'.")
    except IOError as e:
        logging.error(f"Error saving balances to file '{filename}': {e}")
        raise


def parse_arguments() -> argparse.Namespace:
    """
    Parse command-line arguments.
    
    Returns:
        argparse.Namespace: Parsed command-line arguments.
    """
    parser = argparse.ArgumentParser(description="Ethereum Wallet Balance Checker")
    parser.add_argument(
        "-i", "--input", type=str, default="wallets.txt", help="Input file with wallet addresses."
    )
    parser.add_argument(
        "-o", "--output", type=str, default="balances.json", help="Output file to save wallet balances."
    )
    parser.add_argument(
        "-n", "--node", type=str, required=True, help="Ethereum node URL (e.g., Infura or local node)."
    )
    parser.add_argument(
        "-v", "--verbose", action="store_true", help="Enable verbose logging output."
    )
    parser.add_argument(
        "--no-save", action="store_true", help="Skip saving balances to a file."
    )
    return parser.parse_args()


def main() -> None:
    """
    Main execution function for checking Ethereum wallet balances.
    """
    args = parse_arguments()

    # Configure logging
    configure_logging(args.verbose)

    try:
        # Load wallet addresses
        addresses = load_wallet_addresses(args.input)

        # Connect to Ethereum node
        web3 = connect_to_ethereum_node(args.node)

        # Check balances
        balances = check_balances(addresses, web3)

        # Display balances
        print(json.dumps(balances, indent=4))

        # Save balances to file (if not skipped)
        if not args.no_save:
            save_balances_to_file(balances, args.output)

    except Exception as e:
        logging.error(f"Program execution failed: {e}")
        exit(1)


if __name__ == "__main__":
    main()
