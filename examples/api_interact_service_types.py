"""
Fivetran Connector Types Fetcher & Dynamic Connector Creation Helper

This script demonstrates how to fetch ALL available Fivetran connector types using the Fivetran Metadata API with proper authentication and pagination.

How to Use for Dynamic Connector Creation:
------------------------------------------------
1. **Run this script** to retrieve a complete list of connector types (source types) available in your Fivetran account. Each connector type includes its name and unique ID.

2. **Review the output**: The script prints all connector types with their names and IDs. Use these IDs as the `connector_type` when creating new connectors via the Fivetran API.

3. **Adapt for automation**: You can import the `fivetran_request` function and the pagination logic into your own scripts to:
    - List all connector types programmatically
    - Select a connector type by name or ID
    - Use the selected type to dynamically create new connectors (see Fivetran API docs for the POST /connectors endpoint)

4. **Example for connector creation**:
    - After identifying the desired connector type ID, use the Fivetran API's `POST /connectors` endpoint with the required parameters (group_id, service, config, etc.).
    - You can extend this script to prompt for user input or automate connector creation based on the fetched types.

This script is a template for building more advanced Fivetran automation workflows.
"""
import requests
import json
import base64
from colorama import Fore, Style
import os

# Set this to True to save connector types to a JSON file
SAVE_TO_JSON = True
JSON_FILENAME = '/connector_types.json'

# Load API credentials from config.json
config_path = '/config.json'
with open(config_path, 'r') as f:
    config = json.load(f)
api_key = config['fivetran']['api_key']
api_secret = config['fivetran']['api_secret']

def fivetran_request(method, endpoint, params=None, payload=None):
    """
    Generic function to interact with Fivetran API using Basic Auth.
    """
    base_url = 'https://api.fivetran.com/v1'
    credentials = f'{api_key}:{api_secret}'.encode('utf-8')
    b64_credentials = base64.b64encode(credentials).decode('utf-8')
    headers = {
        'Authorization': f'Basic {b64_credentials}',
        'Accept': 'application/json'
    }
    url = f'{base_url}/{endpoint}'
    try:
        if method == 'GET':
            response = requests.get(url, headers=headers, params=params, json=payload)
        elif method == 'POST':
            response = requests.post(url, headers=headers, params=params, json=payload)
        elif method == 'PATCH':
            response = requests.patch(url, headers=headers, params=params, json=payload)
        elif method == 'DELETE':
            response = requests.delete(url, headers=headers, params=params, json=payload)
        else:
            raise ValueError('Invalid request method.')
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(Fore.RED + f'Request failed: {e}' + Style.RESET_ALL)
        return None

def main():
    # Example: Get connector types metadata
    method = 'GET'
    endpoint = 'metadata/connector-types'
    limit = 100  # You can increase up to 1000 if needed
    cursor = ''
    all_items = []
    page = 1

    while True:
        params = {"cursor": cursor, "limit": str(limit)}
        print(Fore.CYAN + f'Calling: {method} {endpoint} page {page} with params {params}' + Style.RESET_ALL)
        response = fivetran_request(method, endpoint, params=params, payload=None)

        if response is not None:
            data = response.get('data', {})
            items = data.get('items', [])
            all_items.extend(items)
            next_cursor = data.get('next_cursor')
            print(Fore.YELLOW + f'Fetched {len(items)} connector types on page {page}.' + Style.RESET_ALL)
            if not next_cursor:
                break
            cursor = next_cursor
            page += 1
        else:
            print(Fore.RED + 'No response received from Fivetran API.' + Style.RESET_ALL)
            break

    print(Fore.GREEN + f'\nTotal connector types found: {len(all_items)}' + Style.RESET_ALL)
    for item in all_items:
        print(Fore.CYAN + f"- {item.get('name', 'Unknown')} (ID: {item.get('id', 'N/A')})" + Style.RESET_ALL)

    # Optionally save to JSON file as {id: name}
    if SAVE_TO_JSON:
        id_name_dict = {item.get('id', 'N/A'): item.get('name', 'Unknown') for item in all_items}
        with open(JSON_FILENAME, 'w') as f:
            json.dump(id_name_dict, f, indent=2)
        print(Fore.MAGENTA + f"\nConnector types saved to {os.path.abspath(JSON_FILENAME)}" + Style.RESET_ALL)

if __name__ == '__main__':
    main()
