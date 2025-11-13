#!/usr/bin/env python3
"""
Simple MCP Server for Fivetran Operations - Universal Demo

A clean, minimal MCP server demonstrating the power of Model Context Protocol
for integrating Fivetran API operations into AI workflows.

Features:
- Destination listing and details
- Connector listing, details, and sync operations
- Connector metadata retrieval
- Object review and health analysis

Architecture:
- FastMCP-based tool server
- Simple configuration management
- Universal transport support (stdio/http)
- Core Fivetran API operations only
- Clean, minimal codebase for quick enablement

Configuration:
    Option 1: Environment variables (recommended)
        export FIVETRAN_API_KEY="your_key"
        export FIVETRAN_API_SECRET="your_secret"
    
    Option 2: Configuration file (configuration.json)
        {
            "fivetran_api_key": "YOUR_API_KEY",
            "fivetran_api_secret": "YOUR_API_SECRET"
        }
"""

# =============================================================================
# CORE IMPORTS
# =============================================================================

# Standard library - core functionality
import json  # JSON serialization for API requests/responses
import logging  # Structured logging for enterprise deployments
import os  # File system and environment operations
import time  # Rate limiting and delays
from datetime import datetime  # Timestamps for audit logs
from typing import Dict, Any, Optional  # Type hints for enterprise code quality

# Third-party - HTTP client
import requests  # Fivetran API communication
from requests.auth import HTTPBasicAuth  # API authentication

# MCP Server Framework
try:
    from mcp.server.fastmcp import FastMCP
except ImportError:
    raise ImportError(
        "FastMCP is required. Install with: pip install mcp"
    )

# =============================================================================
# SERVER INITIALIZATION
# =============================================================================

# Initialize FastMCP server instance
mcp = FastMCP('mcp-example')

# Configure logging
logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format='[%(levelname)s] %(message)s'
)

# =============================================================================
# CONFIGURATION MANAGEMENT
# =============================================================================

# Configuration file path - single source of truth for all credentials
# Supports multiple deployment environments via environment variable or default location
default_config_path = os.path.join(os.path.dirname(__file__), 'configuration.json')
config_file = os.environ.get('MCP_CONFIG_FILE', default_config_path)

def _ensure_config_file_exists() -> None:
    """
    Validate configuration file path exists.
    
    Note: Configuration file is optional if environment variables are provided.
    Enterprise deployments should use environment variables for security.
    """
    try:
        # Check if environment variables are available
        if os.environ.get('FIVETRAN_API_KEY') and os.environ.get('FIVETRAN_API_SECRET'):
            logger.info("Environment variables found - configuration file is optional")
            return
            
        # If no environment variables, check for configuration file
        cfg_dir = os.path.dirname(config_file) or '.'
        if cfg_dir:
            os.makedirs(cfg_dir, exist_ok=True)
        if not os.path.exists(config_file):
            logger.warning(f"Configuration file not found at: {config_file}")
            logger.warning("Set FIVETRAN_API_KEY and FIVETRAN_API_SECRET environment variables or create configuration file")
        else:
            logger.info(f"Configuration file found at: {config_file}")
    except Exception as e:
        logger.error(f"Failed to validate configuration file path: {e}")

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
            logger.info(f"Configuration file loaded successfully from: {config_file_path}")
            logger.debug(f"Configuration file size: {len(config_content)} characters")
            config = json.loads(config_content)
            logger.debug(f"Configuration keys found: {list(config.keys())}")
            return config
    except Exception as e:
        raise Exception(f"Failed to load configuration file: {e}")

def _get_api_credentials() -> tuple:
    """
    Retrieve Fivetran API credentials from environment variables or configuration file.
    
    Priority order:
    1. Environment variables (FIVETRAN_API_KEY, FIVETRAN_API_SECRET)
    2. Configuration file (fallback)
    
    Returns:
        Tuple of (api_key, api_secret)
    
    Raises:
        Exception: If credentials missing or configuration invalid
    """
    try:
        # First, try to get credentials from environment variables
        api_key = os.environ.get('FIVETRAN_API_KEY')
        api_secret = os.environ.get('FIVETRAN_API_SECRET')
        
        if api_key and api_secret:
            logger.info("Using credentials from environment variables")
            logger.debug(f"API Key: {api_key[:10]}...{api_key[-10:] if len(api_key) > 20 else '***'}")
            logger.debug(f"API Secret: {api_secret[:10]}...{api_secret[-10:] if len(api_secret) > 20 else '***'}")
            return api_key, api_secret
        
        # Fallback to configuration file
        logger.info("Environment variables not found, falling back to configuration file")
        config = _load_config()
        
        # Handle nested fivetran config structure
        if 'fivetran' in config and isinstance(config['fivetran'], dict):
            nested_config = config['fivetran']
            api_key = nested_config.get('fivetran_api_key') or nested_config.get('api_key')
            api_secret = nested_config.get('fivetran_api_secret') or nested_config.get('api_secret')
        else:
            api_key = config.get('fivetran_api_key') or config.get('api_key')
            api_secret = config.get('fivetran_api_secret') or config.get('api_secret')
        
        logger.info("Using credentials from configuration file")
        logger.debug(f"API Key: {api_key[:10]}...{api_key[-10:] if len(api_key) > 20 else '***'}")
        logger.debug(f"API Secret: {api_secret[:10]}...{api_secret[-10:] if len(api_secret) > 20 else '***'}")
        return api_key, api_secret
        
    except Exception as e:
        raise Exception(f"Failed to load API credentials: {e}")

# =============================================================================
# FIVETRAN API CLIENT LAYER
# =============================================================================

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
            'Content-Type': 'application/json'
        }
        
        # Set timeout values to prevent hanging
        timeout = (10, 30)  # (connect_timeout, read_timeout) in seconds
        
        # Debug logging
        logger.debug(f"Executing {method} request to: {url}")
        if payload:
            logger.debug(f"Request payload: {json.dumps(payload, indent=2)}")
        
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
                logger.debug(f"Response status: {response.status_code}")
                logger.debug(f"Response headers: {dict(response.headers)}")
                
                if response.status_code >= 400:
                    logger.error(f"API Error {response.status_code}: {response.text}")
                    logger.debug(f"Response content: {response.text}")
                
                response.raise_for_status()
                
                # Ensure we return a dictionary, not a string
                try:
                    response_data = response.json()
                    if isinstance(response_data, str):
                        logger.warning(f"API returned string instead of JSON: {response_data}")
                        return None
                    return response_data
                except Exception as json_error:
                    logger.error(f"Failed to parse JSON response: {json_error}")
                    logger.debug(f"Response text: {response.text}")
                    return None
                
            except requests.exceptions.Timeout as e:
                logger.warning(f"Request timeout on attempt {attempt + 1}: {e}")
                if attempt == max_retries - 1:
                    raise e
                time.sleep(2 ** attempt)  # Exponential backoff
                
            except requests.exceptions.RequestException as e:
                logger.error(f"Request failed on attempt {attempt + 1}: {e}")
                if attempt == max_retries - 1:
                    raise e
                time.sleep(2 ** attempt)  # Exponential backoff
                
            except Exception as e:
                logger.error(f"Unexpected error on attempt {attempt + 1}: {e}")
                if attempt == max_retries - 1:
                    raise e
                time.sleep(2 ** attempt)  # Exponential backoff
        
        return None
        
    except Exception as e:
        logger.error(f"Failed to make API request: {e}")
        return None

# =============================================================================
# DESTINATION OPERATIONS
# =============================================================================

@mcp.tool()
def list_destinations() -> str:
    """
    List all Fivetran destinations.
    
    Returns:
        JSON string with destination list including IDs, names, and service accounts
    """
    try:
        result = _make_api_request('GET', 'groups')
        
        if result:
            return json.dumps({
                'success': True,
                'data': result.get('data', {})
            }, indent=2)
        else:
            return json.dumps({
                'success': False,
                'error': 'Failed to retrieve destinations'
            }, indent=2)
        
    except Exception as e:
        return json.dumps({
            'success': False,
            'error': str(e)
        }, indent=2)

@mcp.tool()
def get_destination_details(group_id: str) -> str:
    """
    Get detailed information about a specific destination.
    
    Args:
        group_id: Fivetran destination/group ID
        
    Returns:
        JSON string with detailed destination information
    """
    try:
        group_id = group_id.strip() if group_id else group_id
        result = _make_api_request('GET', f'groups/{group_id}')
        
        if result:
            return json.dumps({
                'success': True,
                'data': result.get('data', {})
            }, indent=2)
        else:
            return json.dumps({
                'success': False,
                'error': 'Failed to retrieve destination details'
            }, indent=2)
        
    except Exception as e:
        return json.dumps({
            'success': False,
            'error': str(e)
        }, indent=2)

# =============================================================================
# CONNECTOR OPERATIONS
# =============================================================================

@mcp.tool()
def list_connectors(group_id: Optional[str] = None) -> str:
    """
    List all connectors with optional group filtering.
    
    Args:
        group_id: Optional Fivetran group/destination ID to filter results
        
    Returns:
        JSON string with connector list
    """
    try:
        # Use the v1 connections endpoint, with optional group_id filter
        params = {}
        normalized_group_id = group_id.strip() if group_id else None
        if normalized_group_id:
            params['group_id'] = normalized_group_id
        endpoint = 'connections'
        result = _make_api_request('GET', endpoint, params=params)
        
        if result:
            return json.dumps({
                'success': True,
                'data': result.get('data', {})
            }, indent=2)
        else:
            return json.dumps({
                'success': False,
                'error': 'Failed to retrieve connectors'
            }, indent=2)
        
    except Exception as e:
        return json.dumps({
            'success': False,
            'error': str(e)
        }, indent=2)

@mcp.tool()
def get_connector_details(connector_id: str) -> str:
    """
    Get detailed information about a specific connector.
    
    Args:
        connector_id: Fivetran connector ID
        
    Returns:
        JSON string with detailed connector information
    """
    try:
        connector_id = connector_id.strip() if connector_id else connector_id
        result = _make_api_request('GET', f'connections/{connector_id}')
        
        if result:
            return json.dumps({
                'success': True,
                'data': result.get('data', {})
            }, indent=2)
        else:
            return json.dumps({
                'success': False,
                'error': 'Failed to retrieve connector details'
            }, indent=2)
        
    except Exception as e:
        return json.dumps({
            'success': False,
            'error': str(e)
        }, indent=2)

@mcp.tool()
def sync_connector(connector_id: str) -> str:
    """
    Trigger a manual sync for a connector.
    
    Args:
        connector_id: Fivetran connector ID to sync
        
    Returns:
        JSON string with sync trigger result
    """
    try:
        connector_id = connector_id.strip() if connector_id else connector_id
        result = _make_api_request('POST', f'connections/{connector_id}/sync')
        
        if result:
            return json.dumps({
                'success': True,
                'message': f'Sync triggered for connector {connector_id}',
                'data': result.get('data', {}),
                'timestamp': datetime.now().isoformat()
            }, indent=2)
        else:
            return json.dumps({
                'success': False,
                'error': 'Failed to trigger sync'
            }, indent=2)
        
    except Exception as e:
        return json.dumps({
            'success': False,
            'error': str(e)
        }, indent=2)

# =============================================================================
# METADATA OPERATIONS
# =============================================================================

@mcp.tool()
def get_connector_metadata(connector_type: str) -> str:
    """
    Get metadata and configuration requirements for a connector type.
    
    Args:
        connector_type: Connector service type (e.g., 'google_sheets', 'sql_server', 'mysql', 'postgresql')
        
    Returns:
        JSON string with connector metadata and configuration requirements
    """
    try:
        result = _make_api_request('GET', f'metadata/connectors/{connector_type}')
        
        if result:
            metadata = result.get('data', {})
            # Return the full data section from the API so callers see all fields
            return json.dumps({
                'success': True,
                'connector_type': connector_type,
                'metadata': metadata
            }, indent=2)
        else:
            return json.dumps({
                'success': False,
                'error': 'Failed to retrieve connector metadata'
            }, indent=2)
        
    except Exception as e:
        return json.dumps({
            'success': False,
            'error': str(e)
        }, indent=2)

# =============================================================================
# OBJECT REVIEW OPERATIONS
# =============================================================================

@mcp.tool()
def review_connector_health(connector_id: str) -> str:
    """
    Review connector health and provide recommendations.
    
    Args:
        connector_id: Fivetran connector ID to review
        
    Returns:
        JSON string with health assessment and recommendations
    """
    try:
        connector_id = connector_id.strip() if connector_id else connector_id
        # Get connector details
        connector_result = _make_api_request('GET', f'connections/{connector_id}')
        
        if not connector_result:
            return json.dumps({
                'success': False,
                'error': 'Failed to retrieve connector details'
            }, indent=2)
        
        connector_data = connector_result.get('data', {})
        
        # Analyze health
        status = connector_data.get('status', {})
        setup_state = status.get('setup_state')
        sync_state = status.get('sync_state')
        paused = connector_data.get('paused', False)
        
        health_score = 100
        issues = []
        recommendations = []
        
        # Check setup state
        if setup_state != 'connected':
            health_score -= 30
            issues.append(f"Setup state: {setup_state}")
            recommendations.append("Complete connector setup")
        
        # Check sync state
        if sync_state == 'failed':
            health_score -= 40
            issues.append("Sync state: failed")
            recommendations.append("Check connector configuration and retry sync")
        elif sync_state == 'paused':
            health_score -= 20
            issues.append("Sync state: paused")
            recommendations.append("Resume connector if needed")
        
        # Check if paused
        if paused:
            health_score -= 10
            issues.append("Connector is paused")
            recommendations.append("Resume connector to enable syncing")
        
        # Determine health level
        if health_score >= 90:
            health_level = "excellent"
        elif health_score >= 70:
            health_level = "good"
        elif health_score >= 50:
            health_level = "needs_attention"
        else:
            health_level = "critical"
        
        return json.dumps({
            'success': True,
            'connector_id': connector_id,
            'health_assessment': {
                'health_score': health_score,
                'health_level': health_level,
                'issues': issues,
                'recommendations': recommendations,
                'setup_state': setup_state,
                'sync_state': sync_state,
                'paused': paused,
                'succeeded_at': connector_data.get('succeeded_at')
            },
            'raw': {
                'connector': connector_data
            }
        }, indent=2)
        
    except Exception as e:
        return json.dumps({
            'success': False,
            'error': str(e)
        }, indent=2)

@mcp.tool()
def get_object_summary() -> str:
    """
    Get a summary of all objects (destinations and connectors) in the account.
    
    Returns:
        JSON string with comprehensive object summary
    """
    try:
        # Get destinations
        destinations_result = _make_api_request('GET', 'groups')
        if destinations_result:
            data_section = destinations_result.get('data', {})
            if isinstance(data_section, list):
                destinations = data_section
            elif isinstance(data_section, dict):
                destinations = data_section.get('items', [])
            else:
                destinations = []
        else:
            destinations = []
        
        # Get connectors
        connectors_result = _make_api_request('GET', 'connections')
        if connectors_result:
            data_section = connectors_result.get('data', {})
            if isinstance(data_section, list):
                connectors = data_section
            elif isinstance(data_section, dict):
                connectors = data_section.get('items', [])
            else:
                connectors = []
        else:
            connectors = []
        
        # Analyze connector states
        connector_stats = {
            'total': len(connectors),
            'connected': 0,
            'incomplete': 0,
            'paused': 0,
            'failed': 0,
            'by_service': {}
        }
        
        for connector in connectors:
            service = connector.get('service', 'unknown')
            if service not in connector_stats['by_service']:
                connector_stats['by_service'][service] = 0
            connector_stats['by_service'][service] += 1
            
            if connector.get('paused', False):
                connector_stats['paused'] += 1
            
            setup_state = connector.get('status', {}).get('setup_state')
            if setup_state == 'connected':
                connector_stats['connected'] += 1
            elif setup_state == 'incomplete':
                connector_stats['incomplete'] += 1
            
            sync_state = connector.get('status', {}).get('sync_state')
            if sync_state == 'failed':
                connector_stats['failed'] += 1
        
        return json.dumps({
            'success': True,
            'summary': {
                'destinations': {
                    'total': len(destinations),
                    'services': list(set(d.get('service', 'unknown') for d in destinations))
                },
                'connectors': connector_stats,
                'health_percentage': round((connector_stats['connected'] / max(connector_stats['total'], 1)) * 100, 1),
                'generated_at': datetime.now().isoformat()
            },
            'raw': {
                'destinations': destinations,
                'connectors': connectors
            }
        }, indent=2)
        
    except Exception as e:
        return json.dumps({
            'success': False,
            'error': str(e)
        }, indent=2)



# =============================================================================
# SERVER STARTUP
# =============================================================================

def main():
    """Main entry point for the MCP server."""
    mcp.run(transport="stdio")
if __name__ == "__main__":
    main()
