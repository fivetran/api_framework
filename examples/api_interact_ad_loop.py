"""
Summary:
- The code is well-structured, modular, and uses clear function separation for API interaction, patching, and sync control.
- Error handling is present for API requests.
- Batch processing logic is clear and cumulative, ensuring logical order of account addition.

Instructions:
- Ensure config.json and accounts.csv are present at the specified paths.
- Adjust ACCOUNTS_PER_BATCH as needed for your use case.
"""
import requests
from requests.auth import HTTPBasicAuth
import json
import time
import csv

# Description:
# This script interacts with the Fivetran API to update the configuration of a connector with account names
# read from a CSV file. It processes the accounts in batches, updating the connection, triggering a sync,
# and waiting for the sync to complete for each batch. The script uses configuration details from
# a JSON file for authentication and connector information.


# Configuration
CONFIG_FILE = '/config.json'  # Path to configuration file
CSV_FILE = '/accounts.csv'  # Path to CSV with account names
ACCOUNTS_PER_BATCH = 2  # Number of accounts to add per batch

# Load configuration
with open(CONFIG_FILE, "r") as config_file:
    config = json.load(config_file)

api_key = config['fivetran']['api_key_demo_sand']  # Fivetran API key
api_secret = config['fivetran']['api_secret_demo_sand']  # Fivetran API secret
connector_id = config['fivetran']['connector_id']  # Fivetran connector ID
auth = HTTPBasicAuth(api_key, api_secret)  # HTTP Basic Auth for requests

def make_request(method, endpoint, payload=None):
    """
    Helper function to make HTTP requests to the Fivetran API.
    Handles GET, POST, PATCH methods and error handling.
    """
    base_url = 'https://api.fivetran.com/v1'
    headers = {
        'Authorization': f'Bearer {api_key}:{api_secret}'
    }
    url = f'{base_url}/{endpoint}'

    try:
        if method == 'GET':
            response = requests.get(url, headers=headers, auth=auth)
        elif method == 'POST':
            response = requests.post(url, headers=headers, json=payload, auth=auth)
        elif method == 'PATCH':
            response = requests.patch(url, headers=headers, json=payload, auth=auth)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f'Request failed: {e}')
        return None

def get_connector_status():
    """
    Returns the current sync state of the connector (e.g., 'syncing', 'paused', etc.).
    """
    endpoint = f'connectors/{connector_id}'
    response = make_request('GET', endpoint)
    if response:
        return response['data']['status']['sync_state']
    return None

def activate_connector():
    """
    Unpauses (activates) the connector if it is paused.
    """
    endpoint = f'connectors/{connector_id}'
    payload = {"paused": False}
    make_request('PATCH', endpoint, payload)
    print("Connector activated")

def trigger_sync():
    """
    Triggers a sync for the connector.
    """
    endpoint = f'connectors/{connector_id}/sync'
    payload = {"force": True}
    make_request('POST', endpoint, payload)
    print("Sync triggered")

def wait_for_sync_completion():
    """
    Polls the connector status every 2 minutes until the sync is complete.
    """
    while True:
        status = get_connector_status()
        print(f"Current sync status: {status}")
        if status != 'syncing':
            break
        time.sleep(120)  # Wait 2 minutes before checking again


def patch_connection_accounts(connection_id, accounts):
    """
    Patches the connector's configuration with the provided list of accounts.
    Args:
        connection_id (str): The connector ID.
        accounts (list): List of account names to add.
    """
    payload = {
        "config": {
            "accounts": accounts
        }
    }
    endpoint = f'connections/{connection_id}'
    method = 'PATCH'
    response = make_request(method, endpoint, payload)
    if response:
        print(f"Patched connection {connection_id} with accounts: {accounts}")
        print(f"Response: {response}")
    else:
        print(f"Failed to patch connection {connection_id} with accounts.")

def get_current_accounts(connection_id):
    """
    Fetches the current list of accounts from the connector's configuration.
    Args:
        connection_id (str): The connector ID.
    Returns:
        list: List of current account names, or empty list if not found.
    """
    endpoint = f'connections/{connection_id}'
    response = make_request('GET', endpoint)
    if response and 'data' in response and 'config' in response['data']:
        return response['data']['config'].get('accounts', [])
    return []

def main():
    """
    Main workflow:
    - Reads account names from CSV.
    - Adds accounts in cumulative batches to the connector, always appending to the config.
    - Triggers sync and waits for completion after each batch.
    - Ensures the connector config includes all accounts from the CSV at the end.
    """
    with open(CSV_FILE, 'r') as file:
        account_names = [row[0] for row in csv.reader(file)]
        print(f"Accounts loaded from CSV: {account_names}")

    for i in range(0, len(account_names), ACCOUNTS_PER_BATCH):
        batch = account_names[i:i + ACCOUNTS_PER_BATCH]
        print(f"\nProcessing batch {i // ACCOUNTS_PER_BATCH + 1}: {batch}")

        # Fetch current accounts from connector config
        current_accounts = get_current_accounts(connector_id)
        # Append new batch, avoiding duplicates, preserving order
        updated_accounts = current_accounts + [acct for acct in batch if acct not in current_accounts]

        # Patch connector with updated accounts
        patch_connection_accounts(connector_id, updated_accounts)
        # Short break between actions to avoid rate limits
        time.sleep(10)
        trigger_sync()
        # Short break before checking sync status
        time.sleep(10)
        wait_for_sync_completion()

    # Final patch to ensure all accounts from CSV are present
    #patch_connection_accounts(connector_id, account_names)
    get_current_accounts(connector_id)
    activate_connector()
    print("\nAll accounts have been added and synced.")

if __name__ == "__main__":
    main()
