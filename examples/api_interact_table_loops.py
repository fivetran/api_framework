import requests
from requests.auth import HTTPBasicAuth
import json
import time
import csv

# Configuration
CONFIG_FILE = '/config.json'
CSV_FILE = '/table_names.csv'
TABLES_PER_BATCH = 2

# Load configuration
with open(CONFIG_FILE, "r") as config_file:
    config = json.load(config_file)

api_key = config['api_key']
api_secret = config['api_secret']
connector_id = config['connector_id']
auth = HTTPBasicAuth(api_key, api_secret)

def make_request(method, endpoint, payload=None):
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
    endpoint = f'connectors/{connector_id}'
    response = make_request('GET', endpoint)
    if response:
        return response['data']['status']['sync_state']
    return None

def activate_connector():
    endpoint = f'connectors/{connector_id}'
    payload = {"paused": False}
    make_request('PATCH', endpoint, payload)
    print("Connector activated")

def trigger_sync():
    endpoint = f'connectors/{connector_id}/sync'
    payload = {"force": True}
    make_request('POST', endpoint, payload)
    print("Sync triggered")

def wait_for_sync_completion():
    while True:
        status = get_connector_status()
        print(f"Current sync status: {status}")
        if status != 'syncing':
            break
        time.sleep(120)  # Check every x minute

def update_schema(table_names):
    endpoint = f'connectors/{connector_id}/schemas/{config["fivetran"]["schema"]}'
    payload = {
        "enabled": True,
        "tables": {table: {"enabled": True} for table in table_names}
    }
    response = make_request('PATCH', endpoint, payload)
    if response:
        print(f"Schema updated for tables: {', '.join(table_names)}")
    else:
        print("Failed to update schema")

def main():
    with open(CSV_FILE, 'r') as file:
        table_names = [row[0] for row in csv.reader(file)]
        print(table_names)

    for i in range(0, len(table_names), TABLES_PER_BATCH):
        batch = table_names[i:i + TABLES_PER_BATCH]
        print(f"\nProcessing batch {i // TABLES_PER_BATCH + 1}")

        update_schema(batch)
        #short break between actions
        #time.sleep(10)
        
        #activate_connector()

        #short break between actions
        time.sleep(10)
        trigger_sync()

        #short break between actions
        time.sleep(10)
        wait_for_sync_completion()

    activate_connector()
    print("\nAll tables have been added and synced.")

if __name__ == "__main__":
    main()
