"""
Fivetran BigQuery Destination Setup Script
Creates a BigQuery destination in Fivetran for the AI/ML demo pipeline

This script automates the creation of a BigQuery destination using the Fivetran API,
following the sample provided in the devpost.md requirements.
"""

import requests
import json
import base64
import os
from typing import Dict, Optional
import colorama
from colorama import Fore, Back, Style

# Initialize colorama for colored output
colorama.init(autoreset=True)

class FivetranBigQueryDestinationSetup:
    """
    Enterprise-grade BigQuery destination setup for Fivetran API.
    
    This class provides comprehensive functionality for creating BigQuery destinations
    through the Fivetran API, including robust error handling, validation,
    and enterprise-level logging.
    """
    
    def __init__(self, config_file_path: str = None):
        """Initialize the destination setup with configuration."""
        self.config = self._load_config(config_file_path)
        self.api_key = self.config['fivetran_api_key']
        self.api_secret = self.config['fivetran_api_secret']
        self.base_url = 'https://api.fivetran.com/v1'
        
        # Create base64 encoded credentials for Basic Auth
        credentials = f"{self.api_key}:{self.api_secret}"
        self.auth_header = base64.b64encode(credentials.encode()).decode()
        
        print(f"[INFO] BigQuery destination setup initialized")
        print(f"[DEBUG] API Key: {self.api_key[:10]}...{self.api_key[-10:] if len(self.api_key) > 20 else '***'}")
        print(f"[DEBUG] Base URL: {self.base_url}")
    
    def _load_config(self, config_file_path: str = None) -> Dict:
        """Load configuration from JSON file."""
        if config_file_path is None:
            config_file_path = 'configuration.json'
        
        try:
            with open(config_file_path, "r") as f:
                config_content = f.read()
                print(f"[INFO] Configuration file loaded successfully from: {config_file_path}")
                config = json.loads(config_content)
                print(f"[DEBUG] Configuration keys found: {list(config.keys())}")
                if 'fivetran' in config:
                    print(f"[DEBUG] Fivetran configuration keys: {list(config['fivetran'].keys())}")
                return config
        except Exception as e:
            raise Exception(f"Failed to load configuration file: {e}")
    
    def _make_api_request(self, method: str, endpoint: str, payload: Dict = None) -> Optional[Dict]:
        """Make API calls to Fivetran with error handling."""
        url = f'{self.base_url}/{endpoint}'
        headers = {
            'Accept': 'application/json;version=2',
            'Authorization': f'Basic {self.auth_header}',
            'Content-Type': 'application/json'
        }
        
        print(f"[DEBUG] Executing {method} request to: {url}")
        if payload:
            print(f"[DEBUG] Request payload: {json.dumps(payload, indent=2)}")
        
        try:
            if method == 'GET':
                response = requests.get(url, headers=headers)
            elif method == 'POST':
                response = requests.post(url, headers=headers, json=payload)
            elif method == 'PATCH':
                response = requests.patch(url, headers=headers, json=payload)
            elif method == 'DELETE':
                response = requests.delete(url, headers=headers, json=payload)
            else:
                raise ValueError(f'Invalid request method: {method}')
            
            print(f"[DEBUG] Response status: {response.status_code}")
            
            if response.status_code >= 400:
                print(f"[ERROR] API Error {response.status_code}: {response.text}")
            
            response.raise_for_status()
            
            response_data = response.json()
            if isinstance(response_data, str):
                print(f"[WARNING] API returned string instead of JSON: {response_data}")
                return None
            
            return response_data
            
        except requests.exceptions.RequestException as e:
            print(f'[ERROR] Request failed: {e}')
            return None
        except json.JSONDecodeError as e:
            print(f'[ERROR] Failed to parse JSON response: {e}')
            return None
    
    def create_bigquery_destination(
        self,
        group_id: str,
        project_id: str,
        dataset_location: str = "US",
        region: str = "GCP_US_WEST1",
        time_zone_offset: str = "0",
        **kwargs
    ) -> Optional[Dict]:
        """
        Create a BigQuery destination in Fivetran.
        
        Args:
            group_id: Fivetran group/destination ID
            project_id: Google Cloud Project ID
            dataset_location: BigQuery dataset location (US, EU, etc.)
            region: Fivetran region for the destination
            time_zone_offset: Time zone offset for the destination
            **kwargs: Additional configuration options
        
        Returns:
            API response dict or None if failed
        """
        
        # Validate required parameters
        if not all([project_id]):
            raise ValueError("group_id and project_id are required")
        
        print(Fore.CYAN + f"[INFO] Creating BigQuery destination for project: {project_id}")
        
        # Build payload following the devpost.md sample
        payload = {
            "group_id": group_id,
            "service": "big_query",
            "region": region,
            "time_zone_offset": time_zone_offset,
            "trust_certificates": True,
            "trust_fingerprints": True,
            "run_setup_tests": True,
            "daylight_saving_time_enabled": True,
            "networking_method": "Directly",
            "config": {
                "project_id": 'internal-sales',
                "support_json_type": True,
                "data_set_location": "US"
            }
        }
        
        # Add optional parameters from kwargs
        optional_params = [
            "hybrid_deployment_agent_id", "private_link_id", 
            "proxy_agent_id", "destination_configuration"
        ]
        
        for param in optional_params:
            if param in kwargs:
                payload[param] = kwargs[param]
        
        print(f"[DEBUG] Final payload: {json.dumps(payload, indent=2)}")
        
        # Make API request
        response = self._make_api_request('POST', 'destinations', payload)
        
        if response:
            destination_id = response.get('data', {}).get('id', 'Unknown')
            print(f"[SUCCESS] BigQuery destination created successfully with ID: {destination_id}")
            print(Fore.GREEN + f"✅ BigQuery destination creation completed successfully!")
            print(Fore.MAGENTA + f"Destination ID: {destination_id}")
            print(Fore.CYAN + f"Project ID: {project_id}")
            print(Fore.CYAN + f"Dataset Location: {dataset_location}")
            return response
        else:
            print("[ERROR] BigQuery destination creation failed")
            print(Fore.RED + "❌ BigQuery destination creation process failed")
            return None
    
    def test_destination_connection(self, destination_id: str) -> Optional[Dict]:
        """Test the connection to a BigQuery destination."""
        print(f"[INFO] Testing connection for destination: {destination_id}")
        
        response = self._make_api_request('GET', f'destinations/{destination_id}/test')
        
        if response:
            print(Fore.GREEN + f"✅ Destination connection test successful!")
            return response
        else:
            print(Fore.RED + f"❌ Destination connection test failed!")
            return None
    
    def get_destination_status(self, destination_id: str) -> Optional[Dict]:
        """Get the status of a BigQuery destination."""
        return self._make_api_request('GET', f'destinations/{destination_id}')
    
    def list_destinations(self, group_id: str = None) -> Optional[Dict]:
        """List all destinations, optionally filtered by group."""
        endpoint = 'destinations'
        if group_id:
            endpoint += f'?group_id={group_id}'
        return self._make_api_request('GET', endpoint)
    
    def create_group(self, group_name: str) -> Optional[Dict]:
        """
        Create a new group in Fivetran.
        
        Args:
            group_name: Name for the new group
        
        Returns:
            API response dict with group details or None if failed
        """
        print(f"[INFO] Creating group: {group_name}")
        
        payload = {
            "name": group_name
        }
        
        print(f"[DEBUG] Group creation payload: {json.dumps(payload, indent=2)}")
        
        response = self._make_api_request('POST', 'groups', payload)
        
        if response:
            group_id = response.get('data', {}).get('id', 'Unknown')
            print(f"[SUCCESS] Group created successfully with ID: {group_id}")
            print(Fore.GREEN + f"✅ Group creation completed successfully!")
            print(Fore.MAGENTA + f"Group ID: {group_id}")
            print(Fore.CYAN + f"Group Name: {group_name}")
            return response
        else:
            print("[ERROR] Group creation failed")
            print(Fore.RED + "❌ Group creation process failed")
            return None
    
    def test_api_connection(self) -> Optional[Dict]:
        """Test the API connection by calling a simple endpoint."""
        return self._make_api_request('GET', 'users/clean_penitence')


def main():
    """
    Main execution function for BigQuery destination setup.
    
    This function demonstrates the creation of a BigQuery destination
    for the Fivetran Challenge demo, following the requirements from devpost.md.
    """
    
    # Configuration - Update these values for your environment
    config_file = 'configuration.json'
    group_name = "AI_Hackathon_Demo"  # Name for the new group
    project_id = ""  # Google Cloud Project ID
    dataset_location = "US"  # BigQuery dataset location
    region = "GCP_US_WEST1"  # Fivetran region
    
    try:
        # Initialize destination setup
        destination_setup = FivetranBigQueryDestinationSetup(config_file)
        
        # Test API connection first
        print(Fore.YELLOW + "=== Testing Fivetran API Connection ===")
        test_response = destination_setup.test_api_connection()
        if test_response:
            print(Fore.GREEN + "✅ Fivetran API connection successful!")
            print(f"User info: {test_response}")
        else:
            print(Fore.RED + "❌ Fivetran API connection failed!")
            return
        
        # Create group first
        print(Fore.YELLOW + "\n=== Creating Fivetran Group ===")
        print(f"[INFO] Group Name: {group_name}")
        
        group_response = destination_setup.create_group(group_name)
        if not group_response:
            print(Fore.RED + "❌ Group creation failed! Cannot proceed with destination creation.")
            return
        
        # Extract group ID from response
        group_id = group_response.get('data', {}).get('id')
        if not group_id:
            print(Fore.RED + "❌ Failed to extract group ID from response!")
            return
        
        print(f"[INFO] Using Group ID: {group_id}")
        
        # Create BigQuery destination
        print(Fore.YELLOW + "\n=== Creating BigQuery Destination ===")
        print(f"[INFO] Group ID: {group_id}")
        print(f"[INFO] Project ID: {project_id}")
        print(f"[INFO] Dataset Location: {dataset_location}")
        print(f"[INFO] Region: {region}")
        
        destination_response = destination_setup.create_bigquery_destination(
            group_id=group_id,
            project_id=project_id,
            dataset_location=dataset_location,
            region=region,
            time_zone_offset="0"
        )
        
        if destination_response:
            destination_id = destination_response.get('data', {}).get('id')
            
            # Test the destination connection
            print(Fore.YELLOW + "\n=== Testing Destination Connection ===")
            if destination_id:
                connection_test = destination_setup.test_destination_connection(destination_id)
                if connection_test:
                    print(Fore.GREEN + f"✅ Destination {destination_id} is ready for use!")
                else:
                    print(Fore.YELLOW + f"⚠️ Destination {destination_id} created but connection test failed")
            
            # Get destination status
            print(Fore.YELLOW + "\n=== Destination Status ===")
            if destination_id:
                status = destination_setup.get_destination_status(destination_id)
                if status:
                    print(f"[INFO] Destination Status: {status.get('data', {}).get('status', 'Unknown')}")
                    print(f"[INFO] Service: {status.get('data', {}).get('service', 'Unknown')}")
                    print(f"[INFO] Region: {status.get('data', {}).get('region', 'Unknown')}")
        
        # List all destinations in the group
        print(Fore.YELLOW + "\n=== All Destinations in Group ===")
        destinations = destination_setup.list_destinations(group_id)
        if destinations:
            dest_list = destinations.get('data', [])
            print(f"[INFO] Total destinations in group: {len(dest_list)}")
            for dest in dest_list:
                print(f"[INFO] - {dest.get('id', 'Unknown')}: {dest.get('service', 'Unknown')} ({dest.get('status', 'Unknown')})")
        else:
            print("[INFO] No destinations found or unable to retrieve destination list")
        
        print(Fore.GREEN + f"\n🎉 BigQuery destination setup completed successfully!")
        print(Fore.CYAN + f"\nNext steps:")
        print(f"1. Configure your Google Sheets connectors to use this destination")
        print(f"2. Set up the Fivetran Connector SDK")
        print(f"3. Deploy the Vertex AI SQL agent")
        print(f"4. Start syncing AI/ML data to BigQuery!")
        
    except Exception as e:
        print(Fore.RED + f"[ERROR] BigQuery destination setup failed: {e}")
        print(f"[ERROR] Main execution error: {e}")


if __name__ == "__main__":
    main()

#Grant the BigQuery User role in your project to the above Fivetran service account
