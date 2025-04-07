import requests
from requests.auth import HTTPBasicAuth
import json
import colorama
from colorama import Fore, Back, Style
import time
import csv
import logging
from datetime import datetime
import sys


#configuration file
r = '/Users/elijah.davis/Documents/config.json'
with open(r, "r") as i:
    l = i.read()
    y = json.loads(l)
api_key = y['fivetran']['api_key']
api_secret = y['fivetran']['api_secret']

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'connector_creation_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'),
        logging.StreamHandler(sys.stdout)
    ]
)

def read_schemas_from_csv(filepath):
    """Read schema names from a CSV file."""
    try:
        with open(filepath, 'r') as file:
            reader = csv.reader(file)
            # Assuming schema names are in the first column
            schemas = [row[0] for row in reader if row]
        return schemas
    except Exception as e:
        logging.error(f"Failed to read CSV file: {e}")
        sys.exit(1)

def atlas(method, endpoint, payload):
    """Make API calls to Fivetran with retry logic."""
    base_url = 'https://api.fivetran.com/v1'
    h = {
        'Authorization': f'Bearer {api_key}:{api_secret}'
    }
    url = f'{base_url}/{endpoint}'
    max_retries = 3
    
    for attempt in range(max_retries):
        try:
            if method == 'GET':
                response = requests.get(url, headers=h, auth=a)
            elif method == 'POST':
                response = requests.post(url, headers=h, json=payload, auth=a)
            elif method == 'PATCH':
                response = requests.patch(url, headers=h, json=payload, auth=a)
            elif method == 'DELETE':
                response = requests.delete(url, headers=h, auth=a)
            else:
                raise ValueError('Invalid request method.')
                
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logging.error(f'Request failed (attempt {attempt + 1}/{max_retries}): {e}')
            if attempt == max_retries - 1:
                return None
            time.sleep(5)  # Wait before retry

def create_connectors(schemas, connector_count, destination, p, host):
    """Create multiple connectors with rate limiting."""
    method = 'POST'
    endpoint = 'connectors/'
    successful_connectors = 0
    failed_connectors = 0

    # Ensure we don't try to create more connectors than we have schemas
    if connector_count > len(schemas):
        logging.warning(f"Requested {connector_count} connectors but only {len(schemas)} schemas available. Using available schemas.")
        connector_count = len(schemas)

    for i in range(connector_count):
        schema_name = schemas[i]
        
        payload = {
            "service": "sql_server_rds",
            "group_id": destination,
            "trust_certificates": "true",
            "run_setup_tests": "true",
            "paused": "true",
            "pause_after_trial": "true",
            "config": {
                "schema_prefix": schema_name,
                "host": host,
                "port": 1433,
                "database": "sqlserver",
                "user": "fivetran",
                "password": p
            }
        }

        logging.info(f"Creating connector {i+1}/{connector_count} with schema: {schema_name}")
        print(Fore.CYAN + f"Submitting Connector {i+1}/{connector_count}")
        
        response = atlas(method, endpoint, payload)
        
        if response is not None:
            logging.info(f"Call: {method} {endpoint}")
            logging.info(f"Response: {response['code']} {response['message']}")
            print(Fore.MAGENTA + f"Connector: {response['data']['id']} successfully created in {destination}")
            successful_connectors += 1
        else:
            logging.error(f"Failed to create connector for schema: {schema_name}")
            failed_connectors += 1
        
        # Rate limiting - sleep between API calls
        time.sleep(5)
    
    return successful_connectors, failed_connectors

def main():
    # Configuration
    api_key = y['fivetran']['api_key']  # Add your API key
    api_secret = y['fivetran']['api_secret']  # Add your API secret
    connector_count = 50  # Number of connectors to create
    destination = y['fivetran']['destination'] # destination ID
    p = y['T']['pw']                # source auth
    host = y['fivetran']['h']
    schemas_file = '/schemas.csv'  # Path to your CSV file
    
    # Initialize auth
    global a
    a = HTTPBasicAuth(api_key, api_secret)
    
    # Read schemas
    schemas = read_schemas_from_csv(schemas_file)
    if not schemas:
        logging.error("No schemas found in CSV file")
        return
    
    logging.info(f"Starting connector creation process for {connector_count} connectors")
    start_time = time.time()
    
    # Create connectors
    successful, failed = create_connectors(schemas, connector_count, destination, p,host)
    
    # Log summary
    elapsed_time = time.time() - start_time
    logging.info(f"""
    Connector Creation Summary:
    - Total Attempted: {connector_count}
    - Successful: {successful}
    - Failed: {failed}
    - Total Time: {elapsed_time:.2f} seconds
    """)

if __name__ == "__main__":
    main()
