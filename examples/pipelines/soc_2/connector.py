# This is a SOC2 compliance connector for Fivetran API endpoints.
""" This connector demonstrates how to fetch SOC2 compliance data from Fivetran API endpoints 
and create audit trails for access control monitoring and compliance reporting."""
# See the Technical Reference documentation (https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update)
# and the Best Practices documentation (https://fivetran.com/docs/connectors/connector-sdk/best-practices) for details

# Import required classes from fivetran_connector_sdk
from fivetran_connector_sdk import Connector # For supporting Connector operations like Update() and Schema()
from fivetran_connector_sdk import Logging as log # For enabling Logs in your connector code
from fivetran_connector_sdk import Operations as op # For supporting Data operations like Upsert(), Update(), Delete() and checkpoint()

# Source-specific imports
import json
import requests
import base64
import time
import random
from datetime import datetime
from typing import Dict, List, Any, Optional, Tuple
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from concurrent.futures import ThreadPoolExecutor, as_completed

"""
SOC2 COMPLIANCE CONNECTOR GUIDELINES:
- Fetches data from Fivetran API endpoints for SOC2 compliance monitoring
- Creates audit trails for access control and user permissions
- Implements proper error handling and logging for compliance requirements
- Uses pagination to handle large datasets efficiently
- Implements checkpointing for incremental syncs
- Transforms raw API data into SOC2-compliant format
- Provides clear audit trail of all API requests and responses
"""

# Constants for API configuration
BASE_URL = 'https://api.fivetran.com/v1'
CHECKPOINT_INTERVAL = 50  # Checkpoint every 50 records (more frequent for better recovery)
MAX_RETRIES = 3
REQUEST_TIMEOUT = 30
MAX_WORKERS = 5  # For parallel API requests
BATCH_SIZE = 10  # Process users in batches
RATE_LIMIT_BASE_DELAY = 60  # Base delay for rate limiting (seconds)

def validate_configuration(configuration: dict):
    """
    Validate the configuration dictionary to ensure it contains all required parameters.
    This function is called at the start of the update method to ensure that the connector has all necessary configuration values.
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
    Raises:
        ValueError: if any required configuration parameter is missing or invalid.
    """
    required_configs = ["api_key", "api_secret"]
    for key in required_configs:
        if key not in configuration:
            raise ValueError(f"Missing required configuration value: {key}")
        if not isinstance(configuration[key], str) or len(configuration[key].strip()) == 0:
            raise ValueError(f"Invalid configuration value: {key} must be a non-empty string")
    
    # Validate API key format (basic check)
    if len(configuration.get("api_key", "")) < 10:
        raise ValueError("API key appears to be invalid (too short)")
    
    # Validate optional parameters
    if "checkpoint_interval" in configuration:
        try:
            interval = int(configuration["checkpoint_interval"])
            if interval < 1:
                raise ValueError("checkpoint_interval must be a positive integer")
        except (ValueError, TypeError):
            raise ValueError("checkpoint_interval must be a valid integer")
    
    # Validate debug mode parameters
    if "debug_mode" in configuration:
        debug_mode_str = str(configuration["debug_mode"]).lower().strip()
        valid_debug_values = ["true", "false", "1", "0", "yes", "no", "on", "off"]
        if debug_mode_str not in valid_debug_values:
            raise ValueError(f"debug_mode must be one of: {', '.join(valid_debug_values)}")
    
    if "debug_user_limit" in configuration:
        try:
            limit = int(configuration["debug_user_limit"])
            if limit < 1:
                raise ValueError("debug_user_limit must be a positive integer")
        except (ValueError, TypeError):
            raise ValueError("debug_user_limit must be a valid integer")
    
    if "debug_team_limit" in configuration:
        try:
            limit = int(configuration["debug_team_limit"])
            if limit < 1:
                raise ValueError("debug_team_limit must be a positive integer")
        except (ValueError, TypeError):
            raise ValueError("debug_team_limit must be a valid integer")

def log_structured(level: str, message: str, **kwargs):
    """
    Create structured log entries for better observability.
    Args:
        level: Log level (info, warning, severe)
        message: Log message
        **kwargs: Additional structured fields
    """
    log_entry = {
        "message": message,
        "timestamp": datetime.now().isoformat(),
        **kwargs
    }
    # Use appropriate log level
    if level.lower() == "info":
        log.info(json.dumps(log_entry))
    elif level.lower() == "warning":
        log.warning(json.dumps(log_entry))
    elif level.lower() == "severe":
        log.severe(json.dumps(log_entry))

def create_auth_headers(api_key: str, api_secret: str) -> Dict[str, str]:
    """
    Create authentication headers for Fivetran API requests.
    Args:
        api_key: Fivetran API key
        api_secret: Fivetran API secret
    Returns:
        Dictionary containing authorization headers
    """
    credentials = f'{api_key}:{api_secret}'.encode('utf-8')
    b64_credentials = base64.b64encode(credentials).decode('utf-8')
    return {
        'Authorization': f'Basic {b64_credentials}',
        'Accept': 'application/json;version=2',
        'Content-Type': 'application/json'
    }

def create_session() -> requests.Session:
    """
    Create a requests session with connection pooling and retry strategy.
    Returns:
        Configured requests.Session object
    """
    session = requests.Session()
    
    # Configure retry strategy
    retry_strategy = Retry(
        total=MAX_RETRIES,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "POST", "PATCH", "DELETE"],
        raise_on_status=False
    )
    
    adapter = HTTPAdapter(
        max_retries=retry_strategy,
        pool_connections=10,
        pool_maxsize=20
    )
    
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    
    return session

def make_api_request(endpoint: str, headers: Dict[str, str], session: requests.Session, 
                     method: str = 'GET', payload: Optional[Dict] = None) -> Optional[Dict]:
    """
    Make API request to Fivetran endpoints with retry logic, rate limiting, and proper error handling.
    Args:
        endpoint: API endpoint path
        headers: Authentication headers
        session: Requests session with connection pooling
        method: HTTP method (GET, POST, PATCH, DELETE)
        payload: Request payload for POST/PATCH requests
    Returns:
        API response data or None if request failed after all retries
    """
    url = f'{BASE_URL}/{endpoint}'
    
    for attempt in range(MAX_RETRIES):
        try:
            if method == 'GET':
                response = session.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
            elif method == 'POST':
                response = session.post(url, headers=headers, json=payload, timeout=REQUEST_TIMEOUT)
            elif method == 'PATCH':
                response = session.patch(url, headers=headers, json=payload, timeout=REQUEST_TIMEOUT)
            elif method == 'DELETE':
                response = session.delete(url, headers=headers, timeout=REQUEST_TIMEOUT)
            else:
                raise ValueError(f'Invalid request method: {method}')

            # Handle rate limiting (429)
            if response.status_code == 429:
                # Safely parse Retry-After header
                retry_after_header = response.headers.get('Retry-After', str(RATE_LIMIT_BASE_DELAY))
                try:
                    retry_after = int(retry_after_header)
                except (ValueError, TypeError):
                    retry_after = RATE_LIMIT_BASE_DELAY
                
                log_structured("warning", f"Rate limited on {endpoint}", 
                              endpoint=endpoint, retry_after=retry_after, attempt=attempt+1)
                if attempt < MAX_RETRIES - 1:
                    time.sleep(retry_after)
                    continue
                else:
                    log_structured("severe", f"Rate limit exceeded after {MAX_RETRIES} attempts", 
                                  endpoint=endpoint)
                    return None

            # Handle other HTTP errors (400+)
            if response.status_code >= 400:
                # Safely get response text for logging
                response_text = ""
                try:
                    response_text = response.text[:500] if hasattr(response, 'text') else ""
                except Exception:
                    pass
                
                if attempt < MAX_RETRIES - 1:
                    wait_time = (2 ** attempt) + (random.random() * 0.1)
                    log_structured("warning", f"API request failed, retrying", 
                                  endpoint=endpoint, status_code=response.status_code, 
                                  attempt=attempt+1, wait_time=wait_time)
                    time.sleep(wait_time)
                    continue
                else:
                    log_structured("severe", f"API request failed after {MAX_RETRIES} attempts", 
                                  endpoint=endpoint, status_code=response.status_code,
                                  response_text=response_text)
                    return None

            # Success - parse and return JSON
            try:
                return response.json()
            except (ValueError, json.JSONDecodeError) as e:
                # If response is not valid JSON, log and return None
                log_structured("severe", f"Invalid JSON response from {endpoint}", 
                              endpoint=endpoint, error=str(e), status_code=response.status_code)
                return None
        
        except requests.exceptions.Timeout as e:
            if attempt < MAX_RETRIES - 1:
                wait_time = (2 ** attempt) + (random.random() * 0.1)
                log_structured("warning", f"Request timeout, retrying", 
                              endpoint=endpoint, attempt=attempt+1, wait_time=wait_time)
                time.sleep(wait_time)
                continue
            else:
                log_structured("severe", f"Request timeout after {MAX_RETRIES} attempts", 
                              endpoint=endpoint, error=str(e))
                return None
        
        except requests.exceptions.RequestException as e:
            if attempt < MAX_RETRIES - 1:
                wait_time = (2 ** attempt) + (random.random() * 0.1)
                log_structured("warning", f"Request failed, retrying", 
                              endpoint=endpoint, attempt=attempt+1, wait_time=wait_time, error=str(e))
                time.sleep(wait_time)
                continue
            else:
                log_structured("severe", f"API request failed after {MAX_RETRIES} attempts", 
                              endpoint=endpoint, error=str(e))
                return None
    
    # This should never be reached, but included for safety
    return None

def get_teams_data(headers: Dict[str, str], session: requests.Session, limit: Optional[int] = None, cursor: Optional[str] = None) -> Tuple[List[Dict], Optional[str]]:
    """
    Fetch all teams data from Fivetran API with pagination support.
    Args:
        headers: Authentication headers
        session: Requests session
        limit: Maximum number of records to fetch per page
        cursor: Pagination cursor for next page
    Returns:
        Tuple of (list of team data dictionaries, next cursor or None)
    """
    log_structured("info", "Fetching teams data", limit=limit, cursor=cursor)
    
    endpoint = 'teams'
    if limit:
        endpoint += f'?limit={limit}'
    if cursor:
        endpoint += f'&cursor={cursor}' if limit else f'?cursor={cursor}'
    
    teams_data = make_api_request(endpoint, headers, session)
    
    # Handle both possible response structures: data.items or just data
    items = []
    next_cursor = None
    
    if teams_data:
        if 'data' in teams_data:
            if 'items' in teams_data['data']:
                items = teams_data['data']['items']
            else:
                items = teams_data['data'] if isinstance(teams_data['data'], list) else [teams_data['data']]
            
            # Check for pagination cursor
            if 'next_cursor' in teams_data.get('data', {}):
                next_cursor = teams_data['data']['next_cursor']
    
    return items, next_cursor

def get_team_details(headers: Dict[str, str], session: requests.Session, team_id: str) -> Optional[Dict]:
    """
    Fetch detailed information for a specific team.
    Args:
        headers: Authentication headers
        session: Requests session
        team_id: Team identifier
    Returns:
        Team details dictionary or None
    """
    log_structured("info", f"Fetching details for team", team_id=team_id)
    return make_api_request(f'teams/{team_id}', headers, session)

def get_team_groups(headers: Dict[str, str], session: requests.Session, team_id: str) -> List[Dict]:
    """
    Fetch groups associated with a team.
    Args:
        headers: Authentication headers
        session: Requests session
        team_id: Team identifier
    Returns:
        List of group data dictionaries
    """
    log_structured("info", f"Fetching groups for team", team_id=team_id)
    groups_data = make_api_request(f'teams/{team_id}/groups', headers, session)
    
    # Handle both possible response structures: data.items or just data
    if groups_data and 'data' in groups_data:
        if 'items' in groups_data['data']:
            return groups_data['data']['items']
        else:
            return groups_data['data'] if isinstance(groups_data['data'], list) else [groups_data['data']]
    return []

def get_team_users(headers: Dict[str, str], session: requests.Session, team_id: str) -> List[Dict]:
    """
    Fetch users associated with a team.
    Args:
        headers: Authentication headers
        session: Requests session
        team_id: Team identifier
    Returns:
        List of user data dictionaries
    """
    log_structured("info", f"Fetching users for team", team_id=team_id)
    users_data = make_api_request(f'teams/{team_id}/users', headers, session)
    
    # Handle both possible response structures: data.items or just data
    if users_data and 'data' in users_data:
        if 'items' in users_data['data']:
            return users_data['data']['items']
        else:
            return users_data['data'] if isinstance(users_data['data'], list) else [users_data['data']]
    return []

def get_user_details(headers: Dict[str, str], session: requests.Session, user_id: str) -> Optional[Dict]:
    """
    Fetch detailed information for a specific user.
    Args:
        headers: Authentication headers
        session: Requests session
        user_id: User identifier
    Returns:
        User details dictionary or None
    """
    log_structured("info", f"Fetching details for user", user_id=user_id)
    return make_api_request(f'users/{user_id}', headers, session)

def get_user_connections(headers: Dict[str, str], session: requests.Session, user_id: str) -> List[Dict]:
    """
    Fetch connections associated with a user.
    Args:
        headers: Authentication headers
        session: Requests session
        user_id: User identifier
    Returns:
        List of connection data dictionaries
    """
    log_structured("info", f"Fetching connections for user", user_id=user_id)
    connections_data = make_api_request(f'users/{user_id}/connections', headers, session)
    
    # Handle both possible response structures: data.items or just data
    if connections_data and 'data' in connections_data:
        if 'items' in connections_data['data']:
            return connections_data['data']['items']
        else:
            return connections_data['data'] if isinstance(connections_data['data'], list) else [connections_data['data']]
    return []

def get_user_groups(headers: Dict[str, str], session: requests.Session, user_id: str) -> List[Dict]:
    """
    Fetch groups associated with a user.
    Args:
        headers: Authentication headers
        session: Requests session
        user_id: User identifier
    Returns:
        List of group data dictionaries
    """
    log_structured("info", f"Fetching groups for user", user_id=user_id)
    groups_data = make_api_request(f'users/{user_id}/groups', headers, session)
    
    # Handle both possible response structures: data.items or just data
    if groups_data and 'data' in groups_data:
        if 'items' in groups_data['data']:
            return groups_data['data']['items']
        else:
            return groups_data['data'] if isinstance(groups_data['data'], list) else [groups_data['data']]
    return []

def get_roles_data(headers: Dict[str, str], session: requests.Session) -> List[Dict]:
    """
    Fetch all roles data from Fivetran API.
    Args:
        headers: Authentication headers
        session: Requests session
    Returns:
        List of role data dictionaries
    """
    log_structured("info", "Fetching roles data")
    roles_data = make_api_request('roles', headers, session)
    
    if roles_data and 'data' in roles_data and 'items' in roles_data['data']:
        return roles_data['data']['items']
    return []

def get_all_users(headers: Dict[str, str], session: requests.Session, limit: Optional[int] = None, cursor: Optional[str] = None) -> Tuple[List[Dict], Optional[str]]:
    """
    Fetch all users data from Fivetran API with pagination support.
    Args:
        headers: Authentication headers
        session: Requests session
        limit: Maximum number of records to fetch per page
        cursor: Pagination cursor for next page
    Returns:
        Tuple of (list of user data dictionaries, next cursor or None)
    """
    log_structured("info", "Fetching all users data", limit=limit, cursor=cursor)
    
    endpoint = 'users'
    if limit:
        endpoint += f'?limit={limit}'
    if cursor:
        endpoint += f'&cursor={cursor}' if limit else f'?cursor={cursor}'
    
    users_data = make_api_request(endpoint, headers, session)
    
    # Handle both possible response structures: data.items or just data
    items = []
    next_cursor = None
    
    if users_data:
        if 'data' in users_data:
            if 'items' in users_data['data']:
                items = users_data['data']['items']
            else:
                items = users_data['data'] if isinstance(users_data['data'], list) else [users_data['data']]
            
            # Check for pagination cursor
            if 'next_cursor' in users_data.get('data', {}):
                next_cursor = users_data['data']['next_cursor']
    
    return items, next_cursor

def create_api_log_record(endpoint: str, method: str, request_data: Optional[Dict], response_data: Optional[Dict], status: str) -> Dict:
    """
    Create a standardized API log record for audit trail.
    Args:
        endpoint: API endpoint
        method: HTTP method
        request_data: Request payload
        response_data: Response data
        status: Request status (SUCCESS, FAILED)
    Returns:
        Dictionary containing API log record
    """
    return {
        "log_id": f"{endpoint}_{method}_{datetime.now().isoformat()}",
        "timestamp": datetime.now().isoformat(),
        "endpoint": endpoint,
        "method": method,
        "request_data": json.dumps(request_data) if request_data else None,
        "response_data": json.dumps(response_data) if response_data else None,
        "status": status,
        "record_count": len(response_data.get('data', [])) if response_data and 'data' in response_data else 0
    }

def get_user_data_batch(headers: Dict[str, str], session: requests.Session, user_ids: List[str]) -> Dict[str, Dict]:
    """
    Fetch multiple users in parallel for improved efficiency.
    Args:
        headers: Authentication headers
        session: Requests session
        user_ids: List of user IDs to fetch
    Returns:
        Dictionary mapping user_id to user details
    """
    results = {}
    
    def fetch_user(user_id: str) -> Tuple[str, Optional[Dict]]:
        return user_id, get_user_details(headers, session, user_id)
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_user = {executor.submit(fetch_user, user_id): user_id for user_id in user_ids}
        for future in as_completed(future_to_user):
            try:
                user_id, user_data = future.result()
                if user_data:
                    results[user_id] = user_data
            except Exception as e:
                user_id = future_to_user[future]
                log_structured("warning", f"Failed to fetch user in batch", user_id=user_id, error=str(e))
    
    return results

def create_soc2_access_record(user_data: Dict, team_data: Dict, group_data: Dict, role_data: Dict, connection_data: Dict) -> Dict:
    """
    Create a SOC2 compliant access record showing user permissions and access levels.
    Args:
        user_data: User information
        team_data: Team information
        group_data: Group information
        role_data: Role information
        connection_data: Connection information
    Returns:
        Dictionary containing SOC2 access record
    """
    # Extract user information with fallbacks
    user_id = user_data.get('id') or user_data.get('user_id') or 'UNKNOWN_USER'
    user_email = user_data.get('email') or user_data.get('email_address') or 'unknown@example.com'
    
    # Try different name field combinations
    user_name = None
    if user_data.get('given_name') and user_data.get('family_name'):
        user_name = f"{user_data.get('given_name')} {user_data.get('family_name')}".strip()
    elif user_data.get('first_name') and user_data.get('last_name'):
        user_name = f"{user_data.get('first_name')} {user_data.get('last_name')}".strip()
    elif user_data.get('name'):
        user_name = user_data.get('name')
    elif user_data.get('display_name'):
        user_name = user_data.get('display_name')
    else:
        user_name = f"User {user_id}"
    
    # Determine access level based on what data is provided
    if connection_data and group_data and role_data:
        access_level = f"GRANULAR: {role_data.get('name', 'Unknown')} on {connection_data.get('name', 'Unknown Connection')}"
    elif group_data and role_data:
        access_level = f"TEAM_LEVEL: {role_data.get('name', 'Unknown')} in {group_data.get('name', 'Unknown Group')}"
    elif connection_data:
        access_level = f"CONNECTION_LEVEL: Access to {connection_data.get('name', 'Unknown Connection')}"
    else:
        access_level = "BASE_LEVEL: System User"
    
    # Create unique record ID with access level indicator
    access_type = "GRANULAR" if (connection_data and group_data) else "TEAM" if group_data else "CONNECTION" if connection_data else "BASE"
    record_id = f"{user_id}_{access_type}_{datetime.now().strftime('%Y%m%d_%H%M%S_%f')}"
    
    return {
        "access_record_id": record_id,
        "user_id": user_id,
        "user_email": user_email,
        "user_name": user_name,
        "team_id": team_data.get('id') if team_data else None,
        "team_name": team_data.get('name') if team_data else None,
        "group_id": group_data.get('id') if group_data else None,
        "group_name": group_data.get('name') if group_data else None,
        "role_id": role_data.get('name') if role_data else None,  # Roles use name as ID
        "role_name": role_data.get('name') if role_data else None,
        "connection_id": connection_data.get('id') if connection_data else None,
        "connection_name": connection_data.get('name') if connection_data else None,
        "access_level": access_level,
        "access_type": access_type,  # GRANULAR, TEAM, CONNECTION, BASE
        "permissions": json.dumps(role_data.get('permissions', [])) if role_data else json.dumps([]),
        "last_accessed": datetime.now().isoformat(),
        "compliance_status": "ACTIVE",
        "audit_timestamp": datetime.now().isoformat()
    }

def schema(configuration: dict):
    """
    Define the schema function which lets you configure the schema your connector delivers.
    See the technical reference documentation for more details on the schema function:
    https://fivetran.com/docs/connectors/connector-sdk/technical-reference#schema
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
    """
    return [
        {
            "table": "api_logs",
            "primary_key": ["log_id"]
        },
        {
            "table": "soc2_access_control",
            "primary_key": ["access_record_id"]
        }
    ]

def update(configuration: dict, state: dict):
    """
    Define the update function, which is a required function, and is called by Fivetran during each sync.
    See the technical reference documentation for more details on the update function
    https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update
    Args:
        configuration: A dictionary containing connection details
        state: A dictionary containing state information from previous runs
        The state dictionary is empty for the first sync or for any full re-sync
    """
    log_structured("info", "SOC2 Compliance Connector: Starting data sync")

    # Validate the configuration to ensure it contains all required values
    validate_configuration(configuration=configuration)

    # Extract configuration parameters
    api_key = configuration.get("api_key")
    api_secret = configuration.get("api_secret")
    
    # Parse and validate debug mode
    debug_mode_str = configuration.get("debug_mode", "false").lower().strip()
    debug_mode = debug_mode_str in ("true", "1", "yes", "on")
    debug_user_limit = 5  # Default limit
    debug_team_limit = 3  # Default limit for teams
    
    # Parse debug limits if provided
    if "debug_user_limit" in configuration:
        try:
            debug_user_limit = max(1, int(configuration["debug_user_limit"]))
        except (ValueError, TypeError):
            log_structured("warning", "Invalid debug_user_limit, using default", default=5)
    
    if "debug_team_limit" in configuration:
        try:
            debug_team_limit = max(1, int(configuration["debug_team_limit"]))
        except (ValueError, TypeError):
            log_structured("warning", "Invalid debug_team_limit, using default", default=3)
    
    # Log debug mode status
    if debug_mode:
        log_structured("info", "DEBUG MODE ENABLED", 
                      user_limit=debug_user_limit, team_limit=debug_team_limit,
                      note="Processing will be limited for testing purposes")

    # Parse checkpoint interval
    checkpoint_interval = int(configuration.get("checkpoint_interval", CHECKPOINT_INTERVAL))

    # Get the state variable for the sync
    last_sync_time = state.get("last_sync_time")
    # Ensure processed_users is always a set, even when loaded from checkpoint (which saves it as a list)
    processed_users_list = state.get("processed_users", [])
    processed_users = set(processed_users_list) if isinstance(processed_users_list, list) else processed_users_list

    # Create authentication headers and session
    headers = create_auth_headers(api_key, api_secret)
    session = create_session()

    try:
        record_count = 0
        
        # Fetch all users first to get the complete user list (with pagination)
        log_structured("info", "Fetching all users for SOC2 compliance audit", debug_mode=debug_mode)
        all_users = []
        cursor = None
        users_fetched = 0
        
        while True:
            # In debug mode, limit the initial fetch to reduce API calls
            fetch_limit = debug_user_limit if debug_mode else 100
            users_batch, next_cursor = get_all_users(headers, session, limit=fetch_limit, cursor=cursor)
            all_users.extend(users_batch)
            users_fetched += len(users_batch)
            
            # In debug mode, stop after first page or when limit reached
            if debug_mode:
                if len(all_users) >= debug_user_limit or not next_cursor:
                    all_users = all_users[:debug_user_limit]
                    log_structured("info", "DEBUG MODE: Limited user fetch", 
                                  users_fetched=len(all_users), limit=debug_user_limit)
                    break
            
            if not next_cursor:
                break
            cursor = next_cursor
            log_structured("info", f"Fetched {len(users_batch)} users, total: {len(all_users)}")
        
        # Create API log record for users endpoint
        api_log = create_api_log_record('users', 'GET', None, {'data': all_users, 'count': len(all_users)}, 'SUCCESS')
        op.upsert(table="api_logs", data=api_log)
        record_count += 1

        # Fetch roles data
        log_structured("info", "Fetching roles data for access control mapping")
        roles_data = get_roles_data(headers, session)
        
        # Create API log record for roles endpoint
        api_log = create_api_log_record('roles', 'GET', None, {'data': {'items': roles_data}, 'count': len(roles_data)}, 'SUCCESS')
        op.upsert(table="api_logs", data=api_log)
        record_count += 1

        # Create roles lookup dictionary (using name as key since roles don't have id)
        log_structured("info", f"Found {len(roles_data)} roles", role_count=len(roles_data))
        roles_lookup = {role.get('name'): role for role in roles_data}

        # Fetch teams data (with pagination)
        log_structured("info", "Fetching teams data for organizational structure", debug_mode=debug_mode)
        teams_data = []
        cursor = None
        
        while True:
            # In debug mode, limit the initial fetch
            fetch_limit = debug_team_limit if debug_mode else 100
            teams_batch, next_cursor = get_teams_data(headers, session, limit=fetch_limit, cursor=cursor)
            teams_data.extend(teams_batch)
            
            # In debug mode, stop after first page or when limit reached
            if debug_mode:
                if len(teams_data) >= debug_team_limit or not next_cursor:
                    teams_data = teams_data[:debug_team_limit]
                    log_structured("info", "DEBUG MODE: Limited team fetch", 
                                  teams_fetched=len(teams_data), limit=debug_team_limit)
                    break
            
            if not next_cursor:
                break
            cursor = next_cursor
            log_structured("info", f"Fetched {len(teams_batch)} teams, total: {len(teams_data)}")
        
        # Create API log record for teams endpoint
        api_log = create_api_log_record('teams', 'GET', None, {'data': teams_data, 'count': len(teams_data)}, 'SUCCESS')
        op.upsert(table="api_logs", data=api_log)
        record_count += 1

        # Create teams lookup dictionary
        log_structured("info", f"Found {len(teams_data)} teams", team_count=len(teams_data))
        teams_lookup = {team.get('id'): team for team in teams_data}

        # Process users in batches for better efficiency
        users_to_process = [user for user in all_users if user.get('id') and user.get('id') not in processed_users]
        log_structured("info", f"Processing {len(users_to_process)} users", total_users=len(all_users), 
                      processed_count=len(processed_users), remaining=len(users_to_process))
        
        # Batch fetch user details
        user_ids_to_fetch = [user.get('id') for user in users_to_process]
        user_details_map = get_user_data_batch(headers, session, user_ids_to_fetch)

        # Process each user for SOC2 compliance
        for user in users_to_process:
            user_id = user.get('id')
            if not user_id:
                continue

            log_structured("info", f"Processing user", user_id=user_id)

            # Get user details from batch fetch
            user_details = user_details_map.get(user_id)
            if user_details:
                # Create API log record for user details endpoint
                api_log = create_api_log_record(f'users/{user_id}', 'GET', None, user_details, 'SUCCESS')
                op.upsert(table="api_logs", data=api_log)
                record_count += 1

            # Fetch user connections
            user_connections = get_user_connections(headers, session, user_id)
            log_structured("info", f"User has connections", user_id=user_id, connection_count=len(user_connections))
            if user_connections:
                # Create API log record for user connections endpoint
                api_log = create_api_log_record(f'users/{user_id}/connections', 'GET', None, {'data': user_connections, 'count': len(user_connections)}, 'SUCCESS')
                op.upsert(table="api_logs", data=api_log)
                record_count += 1

            # Fetch user groups
            user_groups = get_user_groups(headers, session, user_id)
            log_structured("info", f"User has groups", user_id=user_id, group_count=len(user_groups))
            if user_groups:
                # Create API log record for user groups endpoint
                api_log = create_api_log_record(f'users/{user_id}/groups', 'GET', None, {'data': user_groups, 'count': len(user_groups)}, 'SUCCESS')
                op.upsert(table="api_logs", data=api_log)
                record_count += 1

            # Create comprehensive SOC2 access control records showing complete access hierarchy
            # Optimize: Create only unique access combinations to avoid exponential record growth
            log_structured("info", f"Creating SOC2 records for user", user_id=user_id, 
                          connection_count=len(user_connections), group_count=len(user_groups))
            soc2_records_created = 0
            
            user_data = user_details.get('data') if user_details and 'data' in user_details else user
            
            # Track unique access combinations to avoid duplicates
            access_combinations = set()
            
            # 1. Create records for each group (team-level access) - only if not already covered by granular
            for group in user_groups:
                team_id = group.get('team_id')
                team_data = teams_lookup.get(team_id) if team_id else None
                role_name = group.get('role_name') or group.get('role')
                role_data = roles_lookup.get(role_name) if role_name else None
                
                # Only create team-level record if user has no connections (to avoid redundancy)
                if not user_connections:
                    access_key = f"TEAM_{team_id}_{group.get('id')}"
                    if access_key not in access_combinations:
                        soc2_record = create_soc2_access_record(
                            user_data,
                            team_data,
                            group,
                            role_data,
                            None  # No specific connection - team-level access
                        )
                        op.upsert(table="soc2_access_control", data=soc2_record)
                        soc2_records_created += 1
                        record_count += 1
                        access_combinations.add(access_key)
            
            # 2. Create granular records for each connection-group combination (most specific access)
            # This covers both connection-level and team-level access in one record
            for connection in user_connections:
                connection_id = connection.get('id')
                
                # If user has groups, create granular records
                if user_groups:
                    for group in user_groups:
                        team_id = group.get('team_id')
                        team_data = teams_lookup.get(team_id) if team_id else None
                        role_name = group.get('role_name') or group.get('role')
                        role_data = roles_lookup.get(role_name) if role_name else None
                        
                        access_key = f"GRANULAR_{connection_id}_{group.get('id')}"
                        if access_key not in access_combinations:
                            soc2_record = create_soc2_access_record(
                                user_data,
                                team_data,
                                group,
                                role_data,
                                connection
                            )
                            op.upsert(table="soc2_access_control", data=soc2_record)
                            soc2_records_created += 1
                            record_count += 1
                            access_combinations.add(access_key)
                else:
                    # No groups - create connection-level record
                    access_key = f"CONNECTION_{connection_id}"
                    if access_key not in access_combinations:
                        soc2_record = create_soc2_access_record(
                            user_data,
                            None,  # No team data
                            None,  # No group data
                            None,  # No role data
                            connection
                        )
                        op.upsert(table="soc2_access_control", data=soc2_record)
                        soc2_records_created += 1
                        record_count += 1
                        access_combinations.add(access_key)
            
            # 3. Create a base-level record for all users (shows user exists in system)
            # Only if user has no connections or groups
            if not user_connections and not user_groups:
                soc2_record = create_soc2_access_record(
                    user_data,
                    None,  # No team data
                    None,  # No group data
                    None,  # No role data
                    None   # No connection data
                )
                op.upsert(table="soc2_access_control", data=soc2_record)
                soc2_records_created += 1
                record_count += 1
            
            log_structured("info", f"Created SOC2 records for user", user_id=user_id, 
                          records_created=soc2_records_created)

            # Mark user as processed
            processed_users.add(user_id)

            # Checkpoint every checkpoint_interval records (more frequent for better recovery)
            if record_count % checkpoint_interval == 0:
                new_state = {
                    "last_sync_time": datetime.now().isoformat(),
                    "processed_users": list(processed_users),
                    "records_processed": record_count
                }
                op.checkpoint(state=new_state)
                log_structured("info", f"Checkpointed progress", records_processed=record_count, 
                              users_processed=len(processed_users))

        # Process teams and their associated data (for audit trail completeness)
        log_structured("info", f"Processing {len(teams_data)} teams for audit trail")
        for team in teams_data:
            team_id = team.get('id')
            if not team_id:
                continue

            log_structured("info", f"Processing team", team_id=team_id)

            # Fetch team details
            team_details = get_team_details(headers, session, team_id)
            if team_details:
                # Create API log record for team details endpoint
                api_log = create_api_log_record(f'teams/{team_id}', 'GET', None, team_details, 'SUCCESS')
                op.upsert(table="api_logs", data=api_log)
                record_count += 1

            # Fetch team groups
            team_groups = get_team_groups(headers, session, team_id)
            if team_groups:
                # Create API log record for team groups endpoint
                api_log = create_api_log_record(f'teams/{team_id}/groups', 'GET', None, {'data': team_groups, 'count': len(team_groups)}, 'SUCCESS')
                op.upsert(table="api_logs", data=api_log)
                record_count += 1

            # Fetch team users
            team_users = get_team_users(headers, session, team_id)
            if team_users:
                # Create API log record for team users endpoint
                api_log = create_api_log_record(f'teams/{team_id}/users', 'GET', None, {'data': team_users, 'count': len(team_users)}, 'SUCCESS')
                op.upsert(table="api_logs", data=api_log)
                record_count += 1

        # Final checkpoint with updated state
        new_state = {
            "last_sync_time": datetime.now().isoformat(),
            "processed_users": list(processed_users),
            "records_processed": record_count
        }
        op.checkpoint(state=new_state)
        
        log_structured("info", "SOC2 Compliance Connector: Completed sync", 
                      total_records=record_count, users_processed=len(processed_users),
                      debug_mode=debug_mode, teams_processed=len(teams_data))

    except Exception as e:
        log_structured("severe", "SOC2 Compliance Connector failed", error=str(e), 
                      error_type=type(e).__name__)
        raise RuntimeError(f"Failed to sync SOC2 compliance data: {str(e)}")
    finally:
        # Clean up session
        if 'session' in locals():
            session.close()

# Create the connector object using the schema and update functions
connector = Connector(update=update, schema=schema)

# Check if the script is being run as the main module.
# This is Python's standard entry method allowing your script to be run directly from the command line or IDE 'run' button.
# This is useful for debugging while you write your code. Note this method is not called by Fivetran when executing your connector in production.
# Please test using the Fivetran debug command prior to finalizing and deploying your connector.
if __name__ == "__main__":
    # Open the configuration.json file and load its contents
    with open("configuration.json", 'r') as f:
        configuration = json.load(f)

    # Test the connector locally
    connector.debug(configuration=configuration)
