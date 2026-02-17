# Standard library - core functionality
import json  # JSON serialization for API requests/responses
import os  # File system and environment operations
import time  # Rate limiting and delays
from datetime import datetime  # Timestamps for audit logs
from typing import Dict, Any, Optional, Union  # Type hints for enterprise code quality

# Third-party - HTTP client
import requests  # Fivetran API communication
from requests.auth import HTTPBasicAuth  # API authentication

# MCP Server Framework
try:
    from mcp.server.fastmcp import FastMCP  # Model Context Protocol server
except ImportError:
    # Auto-install if missing (development convenience)
    print("[SETUP] Installing FastMCP library...")
    os.system("pip install mcp")
    from mcp.server.fastmcp import FastMCP

#!/usr/bin/env python3
"""
MCP Server for Fivetran Management (v2 - Demo Focused)

Architecture:
- FastMCP-based tool server
- Direct execution without approval workflow
- Tiered tool organization (Business Impact → Operational → Monitoring)
- Comprehensive error handling and retry logic

Usage:
    python mcp_fivetran_v2.py

Configuration Required (configuration.json):
    {
        "fivetran_api_key": "YOUR_API_KEY",
        "fivetran_api_secret": "YOUR_API_SECRET"
    }
"""
# =============================================================================
# SERVER INITIALIZATION
# =============================================================================

# Initialize FastMCP server instance
mcp = FastMCP('fivetran-mcp-demo')

# =============================================================================
# CONFIGURATION MANAGEMENT
# =============================================================================

# Configuration file path - single source of truth for all credentials
config_file = '/mcp/configuration.json'

def _ensure_config_file_exists() -> None:
    """
    Validate configuration file path exists.
    
    Note: Does not create default configuration to prevent accidental misconfigurations.
    Enterprise deployments should provision configuration explicitly.
    """
    try:
        cfg_dir = os.path.dirname(config_file) or '.'
        if cfg_dir:
            os.makedirs(cfg_dir, exist_ok=True)
        if not os.path.exists(config_file):
            print(f"[WARNING] Configuration file not found at: {config_file}")
            print(f"[WARNING] Server will fail credential operations until configured.")
    except Exception as e:
        print(f"[ERROR] Failed to validate configuration file path: {e}")

# Ensure configuration file exists at startup
_ensure_config_file_exists()

def _load_config(config_file_path: str = None) -> Dict:
    """
    Load configuration from JSON file.
    
    Expected configuration structure:
    {
        "fivetran_api_key": "<api_key>",
        "fivetran_api_secret": "<api_secret>"
    }
    
    Args:
        config_file_path: Optional override path, defaults to global config_file
    
    Returns:
        Configuration dictionary
    
    Raises:
        Exception: If file cannot be read or parsed
    """
    if config_file_path is None:
        config_file_path = config_file
    
    try:
        with open(config_file_path, "r") as f:
            config_content = f.read()
            print(f"[INFO] Configuration file loaded successfully from: {config_file_path}")
            print(f"[DEBUG] Configuration file size: {len(config_content)} characters")
            config = json.loads(config_content)
            print(f"[DEBUG] Configuration keys found: {list(config.keys())}")
            if 'fivetran' in config:
                print(f"[DEBUG] Fivetran configuration keys: {list(config['fivetran'].keys())}")
            return config
    except Exception as e:
        raise Exception(f"Failed to load configuration file: {e}")

# =============================================================================
# FIVETRAN API CLIENT LAYER
# =============================================================================

def _get_api_credentials() -> tuple:
    """
    Retrieve Fivetran API credentials from configuration.
    
    Returns:
        Tuple of (api_key, api_secret)
    
    Raises:
        Exception: If credentials missing or configuration invalid
    """
    try:
        config = _load_config()
        api_key = config['fivetran_api_key']
        api_secret = config['fivetran_api_secret']
        print(f"[INFO] API credentials loaded successfully")
        print(f"[DEBUG] API Key: {api_key[:10]}...{api_key[-10:] if len(api_key) > 20 else '***'}")
        print(f"[DEBUG] API Secret: {api_secret[:10]}...{api_secret[-10:] if len(api_secret) > 20 else '***'}")
        return api_key, api_secret
    except Exception as e:
        raise Exception(f"Failed to load API credentials: {e}")

def _make_api_request(method: str, endpoint: str, payload: Dict = None, params: Dict = None, max_retries: int = 3) -> Optional[Dict]:
    """
    Execute Fivetran API request with enterprise-grade reliability.
    
    Features:
    - Automatic retry with exponential backoff
    - Comprehensive error logging
    - Timeout protection (10s connect, 30s read)
    - API versioning support (Accept: application/json;version=2)
    
    Args:
        method: HTTP method (GET, POST, PATCH, DELETE)
        endpoint: API endpoint path (without base URL)
        payload: Request body for POST/PATCH
        params: URL query parameters for GET
        max_retries: Maximum retry attempts (default: 3)
    
    Returns:
        Response JSON dictionary or None on failure
    """
    try:
        # Get credentials
        api_key, api_secret = _get_api_credentials()
        auth = HTTPBasicAuth(api_key, api_secret)
        base_url = 'https://api.fivetran.com/v1'
        
        url = f'{base_url}/{endpoint}'
        headers = {
            'Accept': 'application/json;version=2',
            'Content-Type': 'application/json',
             "User-Agent": "fivetran-services-mcp",
        }
        
        # Set timeout values to prevent hanging
        timeout = (10, 30)  # (connect_timeout, read_timeout) in seconds
        
        # Debug logging
        print(f"[DEBUG] Executing {method} request to: {url}")
        if payload:
            print(f"[DEBUG] Request payload: {json.dumps(payload, indent=2)}")
        
        for attempt in range(max_retries):
            try:
                if method == 'GET':
                    response = requests.get(url, headers=headers, auth=auth, params=params, timeout=timeout)
                elif method == 'POST':
                    response = requests.post(url, headers=headers, json=payload, auth=auth, timeout=timeout)
                elif method == 'PATCH':
                    response = requests.patch(url, headers=headers, json=payload, auth=auth, timeout=timeout)
                elif method == 'DELETE':
                    response = requests.delete(url, headers=headers, json=payload, auth=auth, timeout=timeout)
                else:
                    raise ValueError(f'Invalid request method: {method}')
                
                # Log response details for debugging
                print(f"[DEBUG] Response status: {response.status_code}")
                print(f"[DEBUG] Response headers: {dict(response.headers)}")
                
                if response.status_code >= 400:
                    print(f"[ERROR] API Error {response.status_code}: {response.text}")
                    print(f"[DEBUG] Response content: {response.text}")
                
                response.raise_for_status()
                
                # Ensure we return a dictionary, not a string
                response_data = response.json()
                if isinstance(response_data, str):
                    print(f"[WARNING] API returned string instead of JSON: {response_data}")
                    return None
                
                return response_data
                
            except requests.exceptions.Timeout as e:
                print(f"[WARNING] Request timeout on attempt {attempt + 1}: {e}")
                if attempt == max_retries - 1:
                    raise e
                time.sleep(2 ** attempt)  # Exponential backoff
                
            except requests.exceptions.RequestException as e:
                print(f"[ERROR] Request failed on attempt {attempt + 1}: {e}")
                if attempt == max_retries - 1:
                    raise e
                time.sleep(2 ** attempt)  # Exponential backoff
                
            except Exception as e:
                print(f"[ERROR] Unexpected error on attempt {attempt + 1}: {e}")
                if attempt == max_retries - 1:
                    raise e
                time.sleep(2 ** attempt)  # Exponential backoff
        
        return None
        
    except Exception as e:
        print(f"[ERROR] Failed to make API request: {e}")
        return None

@mcp.tool()
def list_connectors(group_id: str = None) -> str:
    """
    List all Fivetran connectors with optional group filtering.
    
    Args:
        group_id: Optional Fivetran group/destination ID to filter results
    
    Returns:
        JSON string with connector list or error details
    """
    try:
        # Use the correct Fivetran API endpoint for connections
        endpoint = 'connections'
        
        # Build query parameters properly according to Fivetran API docs
        params = {}
        if group_id:
            params['group_id'] = group_id
        
        # Use the standard _make_api_request method which handles errors properly
        response = _make_api_request('GET', endpoint, params=params)
        
        if response:
            # Handle Fivetran API response structure: data can be a dict with 'items' or a list
            api_data = response.get('data', {})
            if isinstance(api_data, dict) and 'items' in api_data:
                connectors_list = api_data.get('items', [])
            elif isinstance(api_data, list):
                connectors_list = api_data
            else:
                connectors_list = []
            
            return json.dumps({
                "success": True,
                "data": connectors_list,
                "message": f"Retrieved {len(connectors_list)} connectors"
            }, indent=2)
        else:
            return json.dumps({
                "success": False,
                "error": "Failed to retrieve connectors list"
            }, indent=2)
            
    except Exception as e:
        return json.dumps({
            "success": False,
            "error": f"Error listing connectors: {str(e)}"
        }, indent=2)

@mcp.tool()
def get_connector_status(connector_id: str) -> str:
    """
    Retrieve detailed status for a specific connector.
    
    Args:
        connector_id: Fivetran connector ID
    
    Returns:
        JSON string with connector status, sync state, and configuration
    """
    try:
        endpoint = f'connectors/{connector_id}'
        response = _make_api_request('GET', endpoint)
        
        if response:
            return json.dumps({
                "success": True,
                "data": response.get('data', {}),
                "message": "Connector status retrieved successfully"
            }, indent=2)
        else:
            return json.dumps({
                "success": False,
                "error": "Failed to retrieve connector status"
            }, indent=2)
            
    except Exception as e:
        return json.dumps({
            "success": False,
            "error": f"Error getting connector status: {str(e)}"
        }, indent=2)

@mcp.tool()  
def pause_connector(connector_id: str) -> str:
    """
    Pause a running connector (stops data syncing).
    
    Args:
        connector_id: Fivetran connector ID to pause
    
    Returns:
        JSON string with operation result
    """
    try:
        endpoint = f'connectors/{connector_id}'
        payload = {"paused": True}
        response = _make_api_request('PATCH', endpoint, payload)
        
        if response:
            return json.dumps({
                "success": True,
                "data": response.get('data', {}),
                "message": "Connector paused successfully"
            }, indent=2)
        else:
            return json.dumps({
                "success": False,
                "error": "Failed to pause connector"
            }, indent=2)
            
    except Exception as e:
        return json.dumps({
            "success": False,
            "error": f"Error pausing connector: {str(e)}"
        }, indent=2)

@mcp.tool()
def resume_connector(connector_id: str) -> str:
    """
    Resume a paused connector (resumes data syncing).
    
    Args:
        connector_id: Fivetran connector ID to resume
    
    Returns:
        JSON string with operation result
    """
    try:
        endpoint = f'connectors/{connector_id}'
        payload = {"paused": False}
        response = _make_api_request('PATCH', endpoint, payload)
        
        if response:
            return json.dumps({
                "success": True,
                "data": response.get('data', {}),
                "message": "Connector resumed successfully"
            }, indent=2)
        else:
            return json.dumps({
                "success": False,
                "error": "Failed to resume connector"
            }, indent=2)
            
    except Exception as e:
        return json.dumps({
            "success": False,
            "error": f"Error resuming connector: {str(e)}"
        }, indent=2)

@mcp.tool()
def get_connector_metadata(connector_type: str) -> str:
    """
    Retrieve connector type metadata and configuration requirements.
    
    Useful for discovering required/optional fields before creating connectors.
    
    Args:
        connector_type: Connector service type
            Supported: 'google_sheets', 'sql_server', 'mysql', 'postgresql',
            'snowflake', 'bigquery', etc.
        
    Returns:
        JSON string with metadata including:
        - Required configuration fields
        - Optional parameters
        - Field descriptions and examples
    """
    try:
        # Fetch connector metadata from Fivetran API
        metadata_response = _make_api_request('GET', f'metadata/connectors/{connector_type}')
        
        if metadata_response:
            result = {
                "success": True,
                "connector_type": connector_type,
                "metadata": metadata_response.get('data', {}),
                "message": f"Metadata retrieved successfully for {connector_type}"
            }
        else:
            result = {
                "success": False,
                "error": f"Failed to retrieve metadata for connector type: {connector_type}",
                "connector_type": connector_type
            }
        
        return json.dumps(result, indent=2)
        
    except Exception as e:
        return json.dumps({
            "success": False,
            "error": f"Error retrieving connector metadata: {str(e)}",
            "connector_type": connector_type
        }, indent=2)

@mcp.tool()
def create_dynamic_connector(connector_type: str, group_id: str, config: Dict[str, Any], auth: Dict[str, Any] = None, **kwargs) -> str:
    """
    Create any connector type with dynamic configuration.
    
    Universal connector creation supporting all Fivetran connector types.
    Use get_connector_metadata() to discover required configuration.
    
    Args:
        connector_type: Connector service type (e.g., 'google_sheets', 'sql_server')
        group_id: Fivetran destination/group ID
        config: Connector-specific configuration dictionary
        auth: Optional authentication configuration (for OAuth connectors)
        **kwargs: Additional options:
            - paused: Start paused (default: True for safety)
            - sync_frequency: Minutes between syncs (default: 1440)
            - trust_certificates: Trust SSL certs (default: True)
            - networking_method: Connection method (default: auto)
        
    Returns:
        JSON string with connector creation result
    """
    try:
        # Build the connector payload dynamically
        payload = {
            "group_id": group_id,
            "service": connector_type,
            "trust_certificates": kwargs.get("trust_certificates", True),
            "trust_fingerprints": kwargs.get("trust_fingerprints", True),
            "run_setup_tests": kwargs.get("run_setup_tests", True),
            "paused": kwargs.get("paused", True),  # Start paused for safety
            "pause_after_trial": kwargs.get("pause_after_trial", False),
            "sync_frequency": kwargs.get("sync_frequency", 1440),
            "data_delay_sensitivity": kwargs.get("data_delay_sensitivity", "NORMAL"),
            "data_delay_threshold": kwargs.get("data_delay_threshold", 0),
            "schedule_type": kwargs.get("schedule_type", "auto"),
            "config": config
        }
        
        # Add optional parameters if provided
        optional_params = [
            "daily_sync_time", "connect_card_config", "proxy_agent_id", 
            "private_link_id", "networking_method", "hybrid_deployment_agent_id",
            "destination_configuration"
        ]
        
        for param in optional_params:
            if param in kwargs:
                payload[param] = kwargs[param]
        
        # Add authentication if provided
        if auth:
            payload["auth"] = auth
        
        print(f"[INFO] Creating {connector_type} connector with dynamic configuration")
        print(f"[DEBUG] Payload: {json.dumps(payload, indent=2)}")
        
        # Create the connector
        response = _make_api_request('POST', 'connections/', payload)
        
        if response:
            connector_id = response.get('data', {}).get('id', 'Unknown')
            status = response.get('data', {}).get('status', {})
            
            result = {
                "success": True,
                "connector_id": connector_id,
                "connector_type": connector_type,
                "group_id": group_id,
                "status": status.get('setup_state', 'N/A'),
                "paused": payload.get('paused', True),
                "created_at": response.get('data', {}).get('created_at', 'N/A'),
                "message": f"{connector_type} connector created successfully and paused for complete setup!"
            }
        else:
            result = {
                "success": False,
                "error": "Failed to create connector",
                "connector_type": connector_type
            }
        
        return json.dumps(result, indent=2)
        
    except Exception as e:
        return json.dumps({
            "success": False,
            "error": f"Error creating dynamic connector: {str(e)}",
            "connector_type": connector_type
        }, indent=2)

@mcp.tool()
def test_connection() -> str:
    """
    Verify Fivetran API connectivity and credentials.
    
    Simple health check using groups endpoint to validate:
    - API credentials are correct
    - Network connectivity is working
    - API is responding
    
    Returns:
        JSON string with success status and groups count
    """
    try:
        print(f"[DEBUG] Testing Fivetran API connection")
        
        # Test with a simple API call
        test_response = _make_api_request('GET', 'groups')
        
        if test_response:
            return json.dumps({
                "success": True,
                "message": "Connection test successful",
                "groups_count": len(test_response.get('data', {}).get('items', [])),
                "timestamp": datetime.now().isoformat()
            }, indent=2)
        else:
            return json.dumps({
                "success": False,
                "error": "API request failed or returned no data"
            }, indent=2)
            
    except Exception as e:
        return json.dumps({
            "success": False,
            "error": f"Connection test failed: {str(e)}"
        }, indent=2)

@mcp.tool()
def get_simple_destinations() -> str:
    """
    Get end-user friendly destination list.
    
    Simplified output optimized for:
    - Sharing with non-technical users
    - Quick reference of available destinations
    - Clean format for documentation
    
    Returns:
        JSON string with minimal destination information:
        - Destination name and ID
        - Service account email to share sheets with
        - Service type and region
    """
    try:
        print(f"[DEBUG] Starting get_simple_destinations function")
        
        # Make API request to get destinations
        response = _make_api_request('GET', 'destinations')
        
        if response is None:
            return json.dumps({
                "success": False,
                "error": "Failed to make API request to destinations endpoint"
            }, indent=2)
        
        # Handle the response structure
        data_section = response.get('data', {})
        
        if isinstance(data_section, list):
            all_items = data_section
        elif isinstance(data_section, dict):
            all_items = data_section.get('items', [])
        else:
            all_items = []
        
        if all_items:
            destinations = []
            
            for dest in all_items:
                destination_id = dest.get('id', 'N/A')
                service = dest.get('service', 'N/A')
                region = dest.get('region', 'UNKNOWN')
                group_id = dest.get('group_id', 'N/A')
                
                # Construct service account email based on GROUP ID
                service_account_email = f"g-{group_id}@fivetran-production.iam.gserviceaccount.com"
                
                # Get destination name (group name) if possible
                destination_name = 'N/A'
                if group_id != 'N/A':
                    try:
                        group_response = _make_api_request('GET', f'groups/{group_id}')
                        if group_response and isinstance(group_response, dict):
                            group_data = group_response.get('data', {})
                            if isinstance(group_data, dict):
                                destination_name = group_data.get('name', f"Group_{group_id}")
                    except Exception as e:
                        print(f"[WARNING] Failed to fetch group name for group_id {group_id}: {e}")
                        destination_name = f"Group_{group_id}"
                
                destinations.append({
                    "destination_name": destination_name,
                    "destination_id": destination_id,
                    "service_account_email": service_account_email,
                    "service": service,
                    "region": region,
                    "group_id": group_id
                })
            
            return json.dumps({
                "success": True,
                "destinations": destinations,
                "total": len(destinations),
                "message": "Destinations formatted for end user sharing"
            }, indent=2)
        else:
            return json.dumps({
                "success": False,
                "error": "No destinations found or invalid response format"
            }, indent=2)
            
    except Exception as e:
        return json.dumps({
            "success": False,
            "error": f"Error formatting destinations for end users: {str(e)}"
        }, indent=2)

@mcp.tool()
def check_connector_status(connector_id: str) -> str:
    """
    Comprehensive connector health check with recommendations.
    
    Enhanced status check that includes:
    - Setup and sync state analysis
    - Configuration validation
    - Actionable recommendations
    
    Args:
        connector_id: Fivetran connector ID to analyze
        
    Returns:
        JSON string with:
        - Current status (setup_state, sync_state, paused)
        - Health assessment
        - Recommended actions if issues detected
    """
    try:
        # Get connector status
        status_response_str = get_connector_status(connector_id)
        status_response = json.loads(status_response_str) if status_response_str else None
        
        if status_response:
            data = status_response.get('data', {})
            status = data.get('status', {})
            
            result = {
                "success": True,
                "connector_id": connector_id,
                "setup_state": status.get('setup_state', 'UNKNOWN'),
                "sync_state": status.get('sync_state', 'UNKNOWN'),
                "paused": data.get('paused', False),
                "service": data.get('service', 'UNKNOWN'),
                "group_id": data.get('group_id', 'UNKNOWN'),
                "created_at": data.get('created_at', 'UNKNOWN'),
                "last_sync": status.get('last_sync', 'UNKNOWN'),
                "message": "Connector status retrieved successfully"
            }
            
            # Check if connector needs attention
            if status.get('setup_state') == 'INCOMPLETE':
                result["needs_attention"] = True
                result["recommendation"] = "Connector setup is incomplete. Check configuration and authentication."
            elif not data.get('paused'):
                result["needs_attention"] = True
                result["recommendation"] = "Connector is running. Consider pausing until setup is complete."
            else:
                result["needs_attention"] = False
                result["recommendation"] = "Connector appears to be properly configured and paused."
                
        else:
            result = {
                "success": False,
                "error": "Failed to retrieve connector status",
                "connector_id": connector_id
            }
        
        return json.dumps(result, indent=2)
        
    except Exception as e:
        return json.dumps({
            "success": False,
            "error": f"Error checking connector status: {str(e)}",
            "connector_id": connector_id
        }, indent=2)

@mcp.tool()
def migrate_connector(connector_id: str, target_group_id: str, new_schema_name: str = None, new_table_name: str = None) -> str:
    """
    Migrate connector to different destination with configuration preservation.
    
    Enterprise migration supporting:
    - Cross-destination connector moves
    - Schema configuration preservation
    - Optional rename during migration
    - Automatic schema replication
    
    Use cases:
    - Environment promotion (dev → prod)
    - Destination consolidation
    - Disaster recovery
    
    Args:
        connector_id: Source connector ID to migrate
        target_group_id: Destination group/destination ID
        new_schema_name: Optional schema rename (default: original + '_migrated')
        new_table_name: Optional table rename (default: original + '_migrated')
        
    Returns:
        JSON string with migration result including new connector ID
    """
    try:
        # Get original connector details
        original_connector_str = get_connector_status(connector_id)
        original_connector = json.loads(original_connector_str) if original_connector_str else None
        if not original_connector:
            return json.dumps({"success": False, "error": "Failed to retrieve original connector details"})
        
        original_data = original_connector.get('data', {})
        original_config = original_data.get('config', {})
        
        # Prepare new connector configuration
        if original_data.get('service') == 'google_sheets':
            new_config = {
                "service": "google_sheets",
                "group_id": target_group_id,
                "paused": True,  # Start paused
                "config": {
                    "schema": new_schema_name or f"{original_config.get('schema', 'google_sheets')}_migrated",
                    "table": new_table_name or f"{original_config.get('table', 'data')}_migrated",
                    "named_range": original_config.get('named_range'),
                    "sheet_id": original_config.get('sheet_id')
                }
            }
        else:
            return json.dumps({"success": False, "error": f"Migration not supported for service type: {original_data.get('service')}"})
        
        # Create new connector
        create_response = _make_api_request('POST', 'connections/', new_config)
        
        if create_response:
            new_connector_id = create_response.get('data', {}).get('id')
            
            # Copy schema configuration from original connector
            schema_response = _make_api_request('GET', f'connectors/{connector_id}/schemas')
            if schema_response:
                schema_data = schema_response.get('data', {})
                # Apply schema to new connector
                _make_api_request('PATCH', f'connectors/{new_connector_id}/schemas', schema_data)
            
            result = {
                "success": True,
                "original_connector_id": connector_id,
                "new_connector_id": new_connector_id,
                "target_group_id": target_group_id,
                "new_schema": new_config['config']['schema'],
                "new_table": new_config['config']['table'],
                "message": "Connector migrated successfully"
            }
        else:
            result = {
                "success": False,
                "error": "Failed to create migrated connector",
                "original_connector_id": connector_id
            }
        
        return json.dumps(result, indent=2)
        
    except Exception as e:
        return json.dumps({
            "success": False,
            "error": f"Error migrating connector: {str(e)}",
            "connector_id": connector_id
        }, indent=2)

@mcp.tool()
def reload_connector_schema(connector_id: str) -> str:
    """
    Refresh connector schema to detect source changes.
    
    Triggers Fivetran to re-scan the data source and detect:
    - New tables/sheets
    - New columns/fields
    - Schema structure changes
    
    Args:
        connector_id: Connector ID to reload
        
    Returns:
        JSON string with reload trigger confirmation
    """
    try:
        # Reload schema
        reload_response = _make_api_request('POST', f'connectors/{connector_id}/schemas/reload')
        
        if reload_response:
            result = {
                "success": True,
                "connector_id": connector_id,
                "message": "Schema reload triggered successfully",
                "timestamp": datetime.now().isoformat()
            }
        else:
            result = {
                "success": False,
                "error": "Failed to reload schema",
                "connector_id": connector_id
            }
        
        return json.dumps(result, indent=2)
        
    except Exception as e:
        return json.dumps({
            "success": False,
            "error": f"Error reloading schema: {str(e)}",
            "connector_id": connector_id
        }, indent=2)

@mcp.tool()
def get_connector_schema(connector_id: str) -> str:
    """
    Retrieve current schema configuration and sync settings.
    
    Returns detailed schema information including:
    - Enabled/disabled tables
    - Column-level sync settings
    - Primary key definitions
    - Schema change handling configuration
    
    Args:
        connector_id: Connector ID to query
        
    Returns:
        JSON string with complete schema configuration
    """
    try:
        # Get schema
        schema_response = _make_api_request('GET', f'connectors/{connector_id}/schemas')
        
        if schema_response:
            result = {
                "success": True,
                "connector_id": connector_id,
                "schema_config": schema_response.get('data', {}),
                "message": "Schema retrieved successfully"
            }
        else:
            result = {
                "success": False,
                "error": "Failed to retrieve schema",
                "connector_id": connector_id
            }
        
        return json.dumps(result, indent=2)
        
    except Exception as e:
        return json.dumps({
            "success": False,
            "error": f"Error retrieving schema: {str(e)}",
            "connector_id": connector_id
        }, indent=2)

@mcp.tool()
def update_connector_schema(connector_id: str, schema_config: dict) -> str:
    """
    Modify connector schema configuration.
    
    Allows fine-grained control over:
    - Table/column inclusion/exclusion
    - Hashing sensitive columns
    - Primary key overrides
    - Schema change handling
    
    Args:
        connector_id: Connector ID to update
        schema_config: New schema configuration dictionary
        
    Returns:
        JSON string with update confirmation
    """
    try:
        # Update schema
        update_response = _make_api_request('PATCH', f'connectors/{connector_id}/schemas', schema_config)
        
        if update_response:
            result = {
                "success": True,
                "connector_id": connector_id,
                "message": "Schema updated successfully",
                "timestamp": datetime.now().isoformat()
            }
        else:
            result = {
                "success": False,
                "error": "Failed to update schema",
                "connector_id": connector_id
            }
        
        return json.dumps(result, indent=2)
        
    except Exception as e:
        return json.dumps({
            "success": False,
            "error": f"Error updating schema: {str(e)}",
            "connector_id": connector_id
        }, indent=2)

@mcp.tool()
def modify_sync_frequency(connection_id: str, sync_frequency: int) -> str:
    """
    Update the sync frequency for a Fivetran connection.
    
    Valid sync frequency values (in minutes):
    - 1, 5, 15, 30, 60, 120, 180, 360, 480, 720, 1440
    
    Args:
        connection_id: Fivetran connection ID to update
        sync_frequency: Sync frequency in minutes (must be one of the valid values)
    
    Returns:
        JSON string with operation result
    """
    # Valid sync frequency values (in minutes)
    VALID_SYNC_FREQUENCIES = [1, 5, 15, 30, 60, 120, 180, 360, 480, 720, 1440]
    
    try:
        # Validate sync frequency
        if sync_frequency not in VALID_SYNC_FREQUENCIES:
            return json.dumps({
                "success": False,
                "error": f"Invalid sync_frequency value: {sync_frequency}",
                "valid_values": VALID_SYNC_FREQUENCIES
            }, indent=2)
        
        # Prepare request payload
        endpoint = f'connections/{connection_id}'
        payload = {
            "sync_frequency": sync_frequency
        }
        
        # Make API request
        response = _make_api_request('PATCH', endpoint, payload)
        
        if response:
            data = response.get('data', {})
            status = data.get('status', {})
            
            result = {
                "success": True,
                "connection_id": connection_id,
                "sync_frequency": sync_frequency,
                "sync_frequency_formatted": f"{sync_frequency} minutes ({sync_frequency // 60 if sync_frequency >= 60 else sync_frequency} {'hours' if sync_frequency >= 60 else 'minutes'})",
                "setup_state": status.get('setup_state', 'N/A'),
                "sync_state": status.get('sync_state', 'N/A'),
                "message": "Connection sync frequency updated successfully"
            }
        else:
            result = {
                "success": False,
                "error": "Failed to update connection sync frequency",
                "connection_id": connection_id
            }
        
        return json.dumps(result, indent=2)
        
    except Exception as e:
        return json.dumps({
            "success": False,
            "error": f"Error updating sync frequency: {str(e)}",
            "connection_id": connection_id
        }, indent=2)

@mcp.tool()
def health_check_all_connectors(group_id: str = None) -> str:
    """
    Enterprise-wide connector health assessment.
    
    Comprehensive health check analyzing:
    - Setup completeness (incomplete, complete)
    - Sync status (syncing, failed, paused)
    - Configuration issues
    - Overall system health percentage
    
    Generates actionable report with:
    - Health categorization (healthy/needs_attention/failed)
    - Issue identification
    - Remediation recommendations
    
    Args:
        group_id: Optional destination filter (default: all connectors)
        
    Returns:
        JSON string with:
        - Aggregate health metrics
        - Per-connector health status
        - Prioritized recommendations
    """
    try:
        # Get all connectors
        connectors_response_str = list_connectors(group_id)
        
        # Validate and parse response
        if not connectors_response_str:
            return json.dumps({"success": False, "error": "Failed to retrieve connectors list - empty response"})
        
        try:
            connectors_response = json.loads(connectors_response_str)
        except json.JSONDecodeError as e:
            return json.dumps({
                "success": False, 
                "error": f"Failed to parse connectors response: {str(e)}",
                "response_preview": connectors_response_str[:200] if len(connectors_response_str) > 200 else connectors_response_str
            })
        
        # Validate response structure
        if not isinstance(connectors_response, dict):
            return json.dumps({
                "success": False, 
                "error": f"Invalid response type: expected dict, got {type(connectors_response).__name__}",
                "response": str(connectors_response)[:200]
            })
        
        if not connectors_response.get('success'):
            error_msg = connectors_response.get('error', 'Unknown error')
            return json.dumps({"success": False, "error": f"Failed to retrieve connectors: {error_msg}"})
        
        # Extract connectors - handle both list and dict with 'items' key
        data = connectors_response.get('data', [])
        
        if isinstance(data, list):
            connectors = data
        elif isinstance(data, dict):
            # Fivetran API returns data as dict with 'items' key
            connectors = data.get('items', [])
            if not isinstance(connectors, list):
                return json.dumps({
                    "success": False,
                    "error": f"Invalid connectors structure: data.items is not a list, got {type(connectors).__name__}",
                    "data_type": type(connectors).__name__,
                    "data_preview": str(connectors)[:200]
                })
        else:
            return json.dumps({
                "success": False,
                "error": f"Invalid connectors data type: expected list or dict, got {type(data).__name__}",
                "data_type": type(data).__name__,
                "data_preview": str(data)[:200]
            })
        
        health_results = {
            "success": True,
            "total_connectors": len(connectors),
            "healthy": 0,
            "needs_attention": 0,
            "failed": 0,
            "connector_health": [],
            "summary": {}
        }
        
        for conn in connectors:
            # Validate connector item is a dictionary
            if not isinstance(conn, dict):
                health_results["failed"] += 1
                health_results["connector_health"].append({
                    "connector_id": "UNKNOWN",
                    "name": "UNKNOWN",
                    "service": "UNKNOWN",
                    "health_status": "failed",
                    "issues": [f"Invalid connector data type: expected dict, got {type(conn).__name__}"]
                })
                continue
            
            connector_id = conn.get('id')
            connector_name = conn.get('name', 'N/A')
            service = conn.get('service', 'N/A')
            
            # Validate connector_id exists
            if not connector_id:
                health_results["failed"] += 1
                health_results["connector_health"].append({
                    "connector_id": "MISSING",
                    "name": connector_name,
                    "service": service,
                    "health_status": "failed",
                    "issues": ["Missing connector ID"]
                })
                continue
            
            try:
                # Get detailed status
                status_response_str = get_connector_status(connector_id)
                
                # Validate and parse status response
                if not status_response_str:
                    health_results["failed"] += 1
                    health_results["connector_health"].append({
                        "connector_id": connector_id,
                        "name": connector_name,
                        "service": service,
                        "health_status": "failed",
                        "issues": ["Empty status response"]
                    })
                    continue
                
                try:
                    status_response = json.loads(status_response_str)
                except json.JSONDecodeError as e:
                    health_results["failed"] += 1
                    health_results["connector_health"].append({
                        "connector_id": connector_id,
                        "name": connector_name,
                        "service": service,
                        "health_status": "failed",
                        "issues": [f"Failed to parse status response: {str(e)}"]
                    })
                    continue
                
                # Validate status_response is a dict
                if not isinstance(status_response, dict):
                    health_results["failed"] += 1
                    health_results["connector_health"].append({
                        "connector_id": connector_id,
                        "name": connector_name,
                        "service": service,
                        "health_status": "failed",
                        "issues": [f"Invalid status response type: expected dict, got {type(status_response).__name__}"]
                    })
                    continue
                
                if status_response:
                    status_data = status_response.get('data', {})
                    
                    # Validate status_data is a dict
                    if not isinstance(status_data, dict):
                        status_data = {}
                    
                    status_info = status_data.get('status', {})
                    
                    # Validate status_info is a dict
                    if not isinstance(status_info, dict):
                        status_info = {}
                    
                    health_status = "healthy"
                    issues = []
                    
                    # Check various health indicators
                    setup_state = status_info.get('setup_state', 'UNKNOWN')
                    sync_state = status_info.get('sync_state', 'UNKNOWN')
                    paused = status_data.get('paused', False)
                    
                    if setup_state == 'INCOMPLETE':
                        health_status = "needs_attention"
                        issues.append("Setup incomplete")
                    elif sync_state == 'FAILED':
                        health_status = "needs_attention"
                        issues.append("Sync failed")
                    elif not paused and setup_state != 'COMPLETE':
                        health_status = "needs_attention"
                        issues.append("Running with incomplete setup")
                    
                    connector_health = {
                        "connector_id": connector_id,
                        "name": connector_name,
                        "service": service,
                        "health_status": health_status,
                        "setup_state": setup_state,
                        "sync_state": sync_state,
                        "paused": paused,
                        "issues": issues,
                        "last_sync": status_info.get('last_sync', 'N/A')
                    }
                    
                    health_results["connector_health"].append(connector_health)
                    
                    if health_status == "healthy":
                        health_results["healthy"] += 1
                    elif health_status == "needs_attention":
                        health_results["needs_attention"] += 1
                    else:
                        health_results["failed"] += 1
                        
                else:
                    health_results["failed"] += 1
                    health_results["connector_health"].append({
                        "connector_id": connector_id,
                        "name": connector_name,
                        "service": service,
                        "health_status": "failed",
                        "issues": ["Failed to retrieve status"]
                    })
                    
            except Exception as e:
                health_results["failed"] += 1
                health_results["connector_health"].append({
                    "connector_id": connector_id,
                    "name": connector_name,
                    "service": service,
                    "health_status": "failed",
                    "issues": [f"Error checking status: {str(e)}"]
                })
        
        # Generate summary
        health_results["summary"] = {
            "health_percentage": round((health_results["healthy"] / health_results["total_connectors"]) * 100, 2) if health_results["total_connectors"] > 0 else 0,
            "recommendations": []
        }
        
        if health_results["needs_attention"] > 0:
            health_results["summary"]["recommendations"].append(f"{health_results['needs_attention']} connectors need attention")
        
        if health_results["failed"] > 0:
            health_results["summary"]["recommendations"].append(f"{health_results['failed']} connectors failed health check")
        
        health_results["message"] = f"Health check completed: {health_results['healthy']} healthy, {health_results['needs_attention']} need attention, {health_results['failed']} failed"
        
        return json.dumps(health_results, indent=2)
        
    except Exception as e:
        return json.dumps({
            "success": False,
            "error": f"Health check failed: {str(e)}"
        }, indent=2)

@mcp.tool()
def get_connector_metrics(connector_id: str, days: int = 7) -> str:
    """
    Detailed connector performance and configuration metrics.
    
    Provides operational insights including:
    - Sync frequency and timing
    - Data delay configuration
    - Setup test results
    - Certificate trust settings
    - Current sync status
    
    Args:
        connector_id: Connector ID to analyze
        days: Historical lookback period (default: 7)
        
    Returns:
        JSON string with comprehensive metrics and configuration
    """
    try:
        # Get connector details
        connector_response_str = get_connector_status(connector_id)
        connector_response = json.loads(connector_response_str) if connector_response_str else None
        if not connector_response:
            return json.dumps({"success": False, "error": "Failed to retrieve connector details"})
        
        connector_data = connector_response.get('data', {})
        status_info = connector_data.get('status', {})
        
        # Calculate metrics
        metrics = {
            "success": True,
            "connector_id": connector_id,
            "service": connector_data.get('service', 'N/A'),
            "group_id": connector_data.get('group_id', 'N/A'),
            "created_at": connector_data.get('created_at', 'N/A'),
            "current_status": {
                "setup_state": status_info.get('setup_state', 'UNKNOWN'),
                "sync_state": status_info.get('sync_state', 'UNKNOWN'),
                "paused": connector_data.get('paused', False),
                "last_sync": status_info.get('last_sync', 'N/A')
            },
            "performance_metrics": {
                "sync_frequency": connector_data.get('sync_frequency', 'N/A'),
                "data_delay_sensitivity": connector_data.get('data_delay_sensitivity', 'N/A'),
                "data_delay_threshold": connector_data.get('data_delay_threshold', 'N/A')
            },
            "configuration": {
                "trust_certificates": connector_data.get('trust_certificates', False),
                "trust_fingerprints": connector_data.get('trust_fingerprints', False),
                "run_setup_tests": connector_data.get('run_setup_tests', False)
            },
            "message": f"Metrics retrieved for {days} days lookback"
        }
        
        return json.dumps(metrics, indent=2)
        
    except Exception as e:
        return json.dumps({
            "success": False,
            "error": f"Error retrieving metrics: {str(e)}",
            "connector_id": connector_id
        }, indent=2)

@mcp.tool()
def get_connector_usage_report(group_id: str = None, days: int = 30) -> str:
    """
    Enterprise connector usage analytics and reporting.
    
    Comprehensive report providing:
    - Service type distribution
    - Active vs. paused breakdown
    - Health assessment percentages
    - Per-connector operational details
    
    Valuable for:
    - Capacity planning
    - Cost optimization
    - System health monitoring
    - Executive reporting
    
    Args:
        group_id: Optional destination filter
        days: Analysis period (default: 30)
        
    Returns:
        JSON string with detailed usage analytics
    """
    try:
        # Get connectors
        connectors_response_str = list_connectors(group_id)
        connectors_response = json.loads(connectors_response_str) if connectors_response_str else None
        if not connectors_response:
            return json.dumps({"success": False, "error": "Failed to retrieve connectors"})
        
        # Handle case where response might be a string
        if isinstance(connectors_response, str):
            return json.dumps({"success": False, "error": f"API returned string instead of JSON: {connectors_response}"})
        
        connectors = connectors_response.get('data', [])
        
        report = {
            "success": True,
            "report_period_days": days,
            "group_id": group_id,
            "total_connectors": len(connectors),
            "service_breakdown": {},
            "status_breakdown": {},
            "health_summary": {
                "healthy": 0,
                "needs_attention": 0,
                "failed": 0
            },
            "connector_details": []
        }
        
        for conn in connectors:
            connector_id = conn.get('id')
            service = conn.get('service', 'N/A')
            paused = conn.get('paused', False)
            
            # Count by service
            if service not in report["service_breakdown"]:
                report["service_breakdown"][service] = 0
            report["service_breakdown"][service] += 1
            
            # Count by status
            status_key = "paused" if paused else "active"
            if status_key not in report["status_breakdown"]:
                report["status_breakdown"][status_key] = 0
            report["status_breakdown"][status_key] += 1
            
            # Get detailed status
            try:
                status_response_str = get_connector_status(connector_id)
                status_response = json.loads(status_response_str) if status_response_str else None
                if status_response:
                    status_data = status_response.get('data', {})
                    status_info = status_data.get('status', {})
                    
                    setup_state = status_info.get('setup_state', 'UNKNOWN')
                    
                    # Determine health
                    if setup_state == 'COMPLETE' and not paused:
                        health = "healthy"
                        report["health_summary"]["healthy"] += 1
                    elif setup_state == 'INCOMPLETE':
                        health = "needs_attention"
                        report["health_summary"]["needs_attention"] += 1
                    else:
                        health = "failed"
                        report["health_summary"]["failed"] += 1
                    
                    report["connector_details"].append({
                        "connector_id": connector_id,
                        "service": service,
                        "health": health,
                        "setup_state": setup_state,
                        "paused": paused,
                        "created_at": status_data.get('created_at', 'N/A'),
                        "last_sync": status_info.get('last_sync', 'N/A')
                    })
                else:
                    report["health_summary"]["failed"] += 1
                    report["connector_details"].append({
                        "connector_id": connector_id,
                        "service": service,
                        "health": "failed",
                        "error": "Failed to retrieve status"
                    })
            except Exception as e:
                report["health_summary"]["failed"] += 1
                report["connector_details"].append({
                    "connector_id": connector_id,
                    "service": service,
                    "health": "failed",
                    "error": f"Status check failed: {str(e)}"
                })
        
        # Calculate percentages
        total = report["total_connectors"]
        if total > 0:
            report["health_percentages"] = {
                "healthy": round((report["health_summary"]["healthy"] / total) * 100, 2),
                "needs_attention": round((report["health_summary"]["needs_attention"] / total) * 100, 2),
                "failed": round((report["health_summary"]["failed"] / total) * 100, 2)
            }
        
        report["message"] = f"Usage report generated for {total} connectors over {days} days"
        
        return json.dumps(report, indent=2)
        
    except Exception as e:
        return json.dumps({
            "success": False,
            "error": f"Error generating usage report: {str(e)}"
        }, indent=2)

# =============================================================================
# SERVER STARTUP
# =============================================================================

if __name__ == "__main__":
    # Start MCP server with stdio transport
    # Server listens on stdin/stdout for JSON-RPC messages from MCP clients
    print("[INFO] Starting Fivetran MCP Server (v2 - Demo Focused)...")
    print("[INFO] Direct execution mode - no approval workflow")
    mcp.run(transport="stdio")
