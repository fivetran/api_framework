import requests
from requests.auth import HTTPBasicAuth
import json
import colorama
from colorama import Fore
import time
import csv
import logging
from datetime import datetime
import sys
import os

# Set up colorama
colorama.init(autoreset=True)

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f's3_connector_creation_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'),
        logging.StreamHandler(sys.stdout)
    ]
)

def load_config(config_path):
    """Load configuration from a JSON file."""
    try:
        with open(config_path, "r") as file:
            config = json.loads(file.read())
        return config
    except Exception as e:
        logging.error(f"Failed to load configuration: {e}")
        sys.exit(1)

def read_csv_data(filepath):
    """Read data from a CSV file."""
    try:
        with open(filepath, 'r') as file:
            reader = csv.reader(file)
            # Skip header row
            next(reader, None)
            # Read all rows
            data = [row for row in reader if row and len(row) >= 3]
        return data
    except Exception as e:
        logging.error(f"Failed to read CSV file: {e}")
        sys.exit(1)

def api_request(method, endpoint, payload=None, auth=None):
    """Make API calls to Fivetran with retry logic."""
    base_url = 'https://api.fivetran.com/v1'
    headers = {
        'Accept': 'application/json;version=2',
        'Content-Type': 'application/json'
    }
    
    url = f'{base_url}/{endpoint}'
    max_retries = 3
    
    for attempt in range(max_retries):
        try:
            if method == 'GET':
                response = requests.get(url, headers=headers, auth=auth)
            elif method == 'POST':
                response = requests.post(url, headers=headers, json=payload, auth=auth)
            elif method == 'PATCH':
                response = requests.patch(url, headers=headers, json=payload, auth=auth)
            elif method == 'DELETE':
                response = requests.delete(url, headers=headers, auth=auth)
            else:
                raise ValueError('Invalid request method.')
                
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logging.error(f'Request failed (attempt {attempt + 1}/{max_retries}): {e}')
            if attempt == max_retries - 1:
                return None
            time.sleep(3)  # Wait before retry

def create_s3_connectors(connector_data, auth, group_id, base_config):
    """Create multiple S3 connectors based on CSV data."""
    method = 'POST'
    endpoint = 'connectors/'
    successful = 0
    failed = 0

    for i, row in enumerate(connector_data):
        if len(row) < 3:
            logging.error(f"Row {i+1} has incomplete data. Skipping.")
            failed += 1
            continue
            
        schema_name = row[0]
        table_name = row[1]
        pattern = row[2]
        
        # Create a copy of the base config and update with specific values
        connector_config = base_config.copy()
        connector_config["pattern"] = pattern
        
        payload = {
            "service": "s3",
            "group_id": group_id,
            "trust_certificates": True,
            "run_setup_tests": True,
            "paused": False,
            "config": connector_config
        }
        
        # Add schema and table if provided
        if schema_name:
            payload["config"]["schema"] = schema_name
        if table_name:
            payload["config"]["table"] = table_name
        
        logging.info(f"Creating connector {i+1}/{len(connector_data)} for schema: {schema_name}, table: {table_name}")
        print(Fore.CYAN + f"Creating S3 connector {i+1}/{len(connector_data)} - {schema_name}.{table_name}")
        
        response = api_request(method, endpoint, payload, auth)
        
        if response and response.get('code') == 'Success':
            connector_id = response['data']['id']
            logging.info(f"Response: {response['code']} {response['message']}")
            print(Fore.GREEN + f"Connector: {connector_id} successfully created in {group_id}")
            successful += 1
        else:
            error_msg = response.get('message', 'Unknown error') if response else 'API request failed'
            logging.error(f"Failed to create connector for {schema_name}.{table_name}: {error_msg}")
            print(Fore.RED + f"Failed to create connector: {error_msg}")
            failed += 1
        
        # Rate limiting
        time.sleep(1)
    
    return successful, failed

def main():
    # Parse command line arguments
    import argparse
    parser = argparse.ArgumentParser(description='Create S3 connectors with configuration from CSV')
    parser.add_argument('--config', default=os.path.expanduser('~/config.json'), help='Path to config JSON file')
    parser.add_argument('--csv', default=os.path.expanduser('~/s3_connectors.csv'), help='Path to connector data CSV file')
    parser.add_argument('--count', type=int, default=5, help='Number of connectors to create (0 for all in CSV)')
    
    args = parser.parse_args()
    
    # Load configuration
    config = load_config(args.config)
    api_key = config['fivetran']['api_key']
    api_secret = config['fivetran']['api_secret']
    destination_group_id = config['fivetran']['og']  # Group ID
    
    # Initialize auth
    auth = HTTPBasicAuth(api_key, api_secret)
    
    # Create base S3 config from the config file
    s3_base_config = {
        "quote_character_enabled": True,
        "prefix": config.get('s3', {}).get('prefix', "folder/"),
        "non_standard_escape_char": False,
        "json_delivery_mode": "Packed",
        "quote_char": "\"",
        "empty_header": False,
        "use_pgp_encryption_options": False,
        "delimiter": ",",
        "file_type": "csv",
        "on_error": "fail",
        "auth_type": "IAM_ROLE",
        "append_file_option": "upsert_file",
        "connection_type": "Directly",
        "escape_char_options": "CUSTOM_ESCAPE_CHAR",
        "list_strategy": "complete_listing",
        "bucket": config.get('s3', {}).get('bucket', "test"),
        "line_separator": "\\n",
        "role_arn": config.get('s3', {}).get('role_arn', ""),
        "is_public": False,
        "compression": "infer",
        "is_private_link_required": False
    }
    
    # Read connector data from CSV
    connector_data = read_csv_data(args.csv)
    if not connector_data:
        logging.error("No connector data found in CSV file")
        return
    
    # Limit to requested count if specified
    if args.count > 0 and args.count < len(connector_data):
        connector_data = connector_data[:args.count]
    
    logging.info(f"Starting S3 connector creation process for {len(connector_data)} connectors")
    start_time = time.time()
    
    # Create connectors
    successful, failed = create_s3_connectors(connector_data, auth, destination_group_id, s3_base_config)
    
    # Log summary
    elapsed_time = time.time() - start_time
    logging.info(f"""
    S3 Connector Creation Summary:
    - Total Attempted: {len(connector_data)}
    - Successful: {successful}
    - Failed: {failed}
    - Total Time: {elapsed_time:.2f} seconds
    """)

if __name__ == "__main__":
    main()
