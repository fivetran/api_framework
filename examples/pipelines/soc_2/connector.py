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
from datetime import datetime
from typing import Dict, List, Any, Optional, Tuple
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

"""
SOC2 COMPLIANCE CONNECTOR GUIDELINES:
- Fetches data from Fivetran API endpoints for SOC2 compliance monitoring
- Uses multiple API endpoints:
  * Teams API: Fetch teams, team details, and team user memberships
  * Users API: Fetch users, user details, group memberships, and connection memberships
  * Groups API: Fetch full group details including group names
  * Roles API: Fetch all roles with full role details (name, description, scope, etc.)
- Creates audit trails for access control and user permissions
- Implements proper error handling and logging for compliance requirements
- Uses pagination to handle large datasets efficiently
- Implements checkpointing for incremental syncs
- Creates audit records for:
  * Group-level access (user-group relationships with team context and role details)
  * Connection-level access (user-connection relationships with team context and role details)
  * Granular access (user-group-connection combinations when detected with role details)
  * Base-level access (users with no groups or connections, but may have team membership and account-level role)
"""

# Constants for API configuration
BASE_URL = 'https://api.fivetran.com/v1'
CHECKPOINT_INTERVAL = 100  # Checkpoint every 100 records
MAX_RETRIES = 3
REQUEST_TIMEOUT = 30
RATE_LIMIT_DELAY = 60  # Base delay for rate limiting (seconds)

def validate_configuration(configuration: dict):
    """
    Validate the configuration dictionary to ensure it contains all required parameters.
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
        allowed_methods=["GET"],
        raise_on_status=False
    )
    
    adapter = HTTPAdapter(
        max_retries=retry_strategy,
        pool_connections=10,
        pool_maxsize=20
    )
    
    session.mount("https://", adapter)
    return session

def make_api_request(endpoint: str, headers: Dict[str, str], session: requests.Session, 
                     params: Optional[Dict] = None) -> Optional[Dict]:
    """
    Make API request to Fivetran endpoints with retry logic and proper error handling.
    Args:
        endpoint: API endpoint path
        headers: Authentication headers
        session: Requests session with connection pooling
        params: Query parameters
    Returns:
        API response data or None if request failed after all retries
    """
    url = f'{BASE_URL}/{endpoint}'
    
    for attempt in range(MAX_RETRIES):
        try:
            response = session.get(url, headers=headers, params=params, timeout=REQUEST_TIMEOUT)

            # Handle rate limiting (429)
            if response.status_code == 429:
                retry_after = int(response.headers.get('Retry-After', RATE_LIMIT_DELAY))
                log.warning(f"Rate limited on {endpoint}, waiting {retry_after} seconds")
                if attempt < MAX_RETRIES - 1:
                    time.sleep(retry_after)
                    continue
                else:
                    log.severe(f"Rate limit exceeded after {MAX_RETRIES} attempts on {endpoint}")
                    return None

            # Handle other HTTP errors
            if response.status_code >= 400:
                log.warning(f"API request failed: {endpoint}, status: {response.status_code}, attempt: {attempt+1}")
                if attempt < MAX_RETRIES - 1:
                    time.sleep(2 ** attempt)
                    continue
                else:
                    log.severe(f"API request failed after {MAX_RETRIES} attempts: {endpoint}, status: {response.status_code}")
                    return None

            # Success - parse and return JSON
            try:
                return response.json()
            except (ValueError, json.JSONDecodeError) as e:
                log.severe(f"Invalid JSON response from {endpoint}: {str(e)}")
                return None
        
        except requests.exceptions.Timeout as e:
            log.warning(f"Request timeout on {endpoint}, attempt: {attempt+1}")
            if attempt < MAX_RETRIES - 1:
                time.sleep(2 ** attempt)
                continue
            else:
                log.severe(f"Request timeout after {MAX_RETRIES} attempts: {endpoint}")
                return None
        
        except requests.exceptions.RequestException as e:
            log.warning(f"Request failed on {endpoint}, attempt: {attempt+1}: {str(e)}")
            if attempt < MAX_RETRIES - 1:
                time.sleep(2 ** attempt)
                continue
            else:
                log.severe(f"API request failed after {MAX_RETRIES} attempts: {endpoint}")
                return None
    
    return None

def extract_items(data: Optional[Dict]) -> Tuple[List[Dict], Optional[str]]:
    """
    Extract items and cursor from API response.
    Handles standard Fivetran API response structure: {"code": "Success", "data": {"items": [...], "next_cursor": "..."}}
    Args:
        data: API response dictionary
    Returns:
        Tuple of (list of items, next cursor or None)
    """
    if not data or 'data' not in data:
        return [], None
    
    data_obj = data['data']
    
    # Extract items
    if isinstance(data_obj, list):
        items = data_obj
        next_cursor = None
    elif isinstance(data_obj, dict):
        items = data_obj.get('items', [])
        next_cursor = data_obj.get('next_cursor')
    else:
        items = [data_obj] if data_obj else []
        next_cursor = None
    
    return items, next_cursor

def fetch_paginated(endpoint: str, headers: Dict[str, str], session: requests.Session, 
                   limit: int = 100) -> List[Dict]:
    """
    Fetch all items from a paginated endpoint.
    Args:
        endpoint: API endpoint path
        headers: Authentication headers
        session: Requests session
        limit: Maximum number of records per page
    Returns:
        List of all items
    """
    all_items = []
    cursor = None
    
    while True:
        params = {'limit': limit}
        if cursor:
            params['cursor'] = cursor
        
        response_data = make_api_request(endpoint, headers, session, params)
        if not response_data:
            break
        
        items, next_cursor = extract_items(response_data)
        all_items.extend(items)
        
        if not next_cursor:
            break
        
        cursor = next_cursor
        log.info(f"Fetched {len(items)} items from {endpoint}, total: {len(all_items)}")
    
    return all_items

def extract_nested_data(data: Optional[Dict]) -> Optional[Dict]:
    """
    Extract data from nested API response structures.
    Handles cases where API returns {'data': {'data': {...}}} or {'data': {...}}.
    Args:
        data: Dictionary that might contain nested data
    Returns:
        Extracted data dictionary or original data if no nesting found
    """
    if not data:
        return None
    
    # If data has a 'data' field, extract it
    if 'data' in data and isinstance(data['data'], dict):
        nested = data['data']
        # Check if nested data also has a 'data' field (double nesting)
        if 'data' in nested and isinstance(nested['data'], dict):
            return nested['data']
        return nested
    
    return data

def get_safe_value(data: Optional[Dict], *keys, default: str = 'N/A') -> str:
    """
    Safely extract a value from nested dictionary using multiple possible keys.
    Args:
        data: Dictionary to search
        *keys: Possible keys to try
        default: Default value if not found
    Returns:
        Extracted value or default
    """
    if not data:
        return default
    
    for key in keys:
        if key in data and data[key] is not None:
            value = data[key]
            # Convert boolean to string
            if isinstance(value, bool):
                return str(value)
            # Handle lists/arrays - convert to readable string format
            if isinstance(value, list):
                if len(value) == 0:
                    return default
                # Convert list to string representation
                return str(value)
            # Handle None
            if value is None:
                return default
            # Handle empty string
            if value == '':
                return default
            return str(value)
    
    return default

def create_audit_record(user: Dict, team: Optional[Dict], role: Optional[Dict], group_membership: Optional[Dict], group_details: Optional[Dict], 
                       connection_membership: Optional[Dict], connection_details: Optional[Dict]) -> Dict:
    """
    Create a comprehensive SOC2 compliant audit record showing all user access relationships.
    All fields will be populated with actual values or 'N/A' to ensure every row is actionable for auditors.
    Args:
        user: User information dictionary
        team: Team information dictionary from teams API (optional)
        role: Full role details from roles API (optional)
        group_membership: Group membership information from users/{userId}/groups (contains id, role, created_at)
        group_details: Full group details from groups/{groupId} (optional)
        connection_membership: Connection membership information from users/{userId}/connections (contains id, role, created_at)
        connection_details: Full connection details from users/{userId}/connections/{connectionId} (optional)
    Returns:
        Dictionary containing comprehensive SOC2 audit record with all fields populated
    """
    # Extract nested data structures if present
    user = extract_nested_data(user) if user else {}
    team = extract_nested_data(team) if team else None
    role = extract_nested_data(role) if role else None
    group_membership = extract_nested_data(group_membership) if group_membership else None
    group_details = extract_nested_data(group_details) if group_details else None
    connection_membership = extract_nested_data(connection_membership) if connection_membership else None
    connection_details = extract_nested_data(connection_details) if connection_details else None
    
    # Extract user information with fallbacks
    user_id = get_safe_value(user, 'id', 'user_id', default='UNKNOWN_USER')
    user_email = get_safe_value(user, 'email', 'email_address', default='N/A')
    user_role = get_safe_value(user, 'role', default='N/A')  # Account-level role
    
    # Extract user name with multiple fallback strategies
    user_name = 'N/A'
    if user.get('given_name') and user.get('family_name'):
        user_name = f"{user['given_name']} {user['family_name']}".strip()
    elif user.get('first_name') and user.get('last_name'):
        user_name = f"{user['first_name']} {user['last_name']}".strip()
    elif user.get('name'):
        user_name = user['name']
    elif user.get('display_name'):
        user_name = user['display_name']
    
    if user_name == 'N/A' and user_id != 'UNKNOWN_USER':
        user_name = f"User {user_id}"
    
    # Extract team information from teams API
    team_id = get_safe_value(team, 'id', 'team_id') if team else 'N/A'
    team_name = get_safe_value(team, 'name', 'team_name', 'display_name') if team else 'N/A'
    team_description = get_safe_value(team, 'description') if team else 'N/A'
    team_role = get_safe_value(team, 'role') if team else 'N/A'
    
    # Extract group information from membership and details
    group_id = 'N/A'
    group_role = 'N/A'
    group_created_at = 'N/A'
    
    if group_membership:
        group_id = get_safe_value(group_membership, 'id', 'group_id', default='N/A')
        group_role = get_safe_value(group_membership, 'role', 'role_name', default='N/A')
        group_created_at = get_safe_value(group_membership, 'created_at', default='N/A')
    
    # Override with group details if available
    if group_details:
        group_id = get_safe_value(group_details, 'id', 'group_id') if group_id == 'N/A' else group_id
        group_name = get_safe_value(group_details, 'name', 'group_name', 'display_name', default='N/A')
        group_created_at = get_safe_value(group_details, 'created_at') if group_created_at == 'N/A' else group_created_at
    else:
        group_name = 'N/A'
    
    # Extract connection information from membership and details
    connection_id = 'N/A'
    connection_role = 'N/A'
    connection_created_at = 'N/A'
    
    if connection_membership:
        connection_id = get_safe_value(connection_membership, 'id', 'connection_id', default='N/A')
        connection_role = get_safe_value(connection_membership, 'role', 'role_name', default='N/A')
        connection_created_at = get_safe_value(connection_membership, 'created_at', default='N/A')
    
    # Override with connection details if available
    if connection_details:
        connection_id = get_safe_value(connection_details, 'id', 'connection_id') if connection_id == 'N/A' else connection_id
        connection_schema = get_safe_value(connection_details, 'schema', default='N/A')
        # Connection name: prefer schema (most descriptive), fall back to service or connection_id
        connection_name = get_safe_value(connection_details, 'schema', default='N/A')
        if connection_name == 'N/A':
            connection_name = get_safe_value(connection_details, 'name', 'connection_name', 'service', default='N/A')
        connection_service = get_safe_value(connection_details, 'service', 'connector_type', default='N/A')
        connection_group_id = get_safe_value(connection_details, 'group_id', default='N/A')
        connection_created_at = get_safe_value(connection_details, 'created_at') if connection_created_at == 'N/A' else connection_created_at
        # Destination ID: connections belong to groups, and groups map 1:1 to destinations
        # So group_id IS the destination_id
        connection_destination_id = connection_group_id if connection_group_id != 'N/A' else get_safe_value(connection_details, 'destination_id', default='N/A')
        connection_connector_type = get_safe_value(connection_details, 'connector_type', 'service', default='N/A')
        connection_connected_by = get_safe_value(connection_details, 'connected_by', default='N/A')
    else:
        connection_schema = 'N/A'
        connection_name = 'N/A'
        connection_service = 'N/A'
        connection_group_id = 'N/A'
        connection_destination_id = 'N/A'
        connection_connector_type = 'N/A'
        connection_connected_by = 'N/A'
    
    # Extract role information from roles API
    # First, determine the effective role name from available sources
    effective_role_name = None
    if role and role.get('name'):
        effective_role_name = role.get('name')
    elif group_membership and group_membership.get('role'):
        effective_role_name = group_membership.get('role')
    elif connection_membership and connection_membership.get('role'):
        effective_role_name = connection_membership.get('role')
    elif user and user.get('role'):
        effective_role_name = user.get('role')
    
    role_name = effective_role_name if effective_role_name else 'N/A'
    role_description = 'N/A'
    role_scope = 'N/A'
    role_is_deprecated = 'N/A'
    role_replacement_role_name = 'N/A'
    role_is_custom = 'N/A'
    
    # If we have role details from roles API, use them; otherwise use the effective role name
    if role:
        role_name_from_api = get_safe_value(role, 'name', default='')
        if role_name_from_api and role_name_from_api != 'N/A':
            role_name = role_name_from_api
        role_description = get_safe_value(role, 'description', default='N/A')
        # Handle role_scope - it might be a list, so extract it properly
        if 'scope' in role and role['scope'] is not None:
            scope_value = role['scope']
            if isinstance(scope_value, list):
                role_scope = str(scope_value)  # Convert list to string representation
            else:
                role_scope = str(scope_value)
        else:
            role_scope = 'N/A'
        role_is_deprecated = get_safe_value(role, 'is_deprecated', default='N/A')
        role_replacement_role_name = get_safe_value(role, 'replacement_role_name', default='N/A')
        role_is_custom = get_safe_value(role, 'is_custom', default='N/A')
    
    # Ensure role_name always has a value - fall back to group/connection role or user role
    if role_name == 'N/A':
        if group_role != 'N/A':
            role_name = group_role
        elif connection_role != 'N/A':
            role_name = connection_role
        elif user_role != 'N/A':
            role_name = user_role
    
    # Use role name from roles API, or fall back to group/connection role name, or user role
    effective_role = role_name if role_name != 'N/A' else (group_role if group_role != 'N/A' else (connection_role if connection_role != 'N/A' else user_role))
    
    # Determine access level and type based on what data is provided
    if connection_id != 'N/A' and group_id != 'N/A':
        role_display = effective_role if effective_role != 'N/A' else 'Unknown'
        conn_display = connection_name if connection_name != 'N/A' else 'Unknown Connection'
        access_level = f"GRANULAR: {role_display} on {conn_display}"
        access_type = "GRANULAR"
    elif group_id != 'N/A':
        role_display = effective_role if effective_role != 'N/A' else 'Unknown'
        group_display = group_name if group_name != 'N/A' else 'Unknown Group'
        access_level = f"GROUP_LEVEL: {role_display} in {group_display}"
        access_type = "GROUP"
    elif connection_id != 'N/A':
        role_display = effective_role if effective_role != 'N/A' else 'Unknown'
        conn_display = connection_name if connection_name != 'N/A' else 'Unknown Connection'
        access_level = f"CONNECTION_LEVEL: {role_display} on {conn_display}"
        access_type = "CONNECTION"
    else:
        access_level = f"BASE_LEVEL: {user_role}" if user_role != 'N/A' else "BASE_LEVEL: System User"
        access_type = "BASE"
    
    # Create unique record ID with access level indicator
    record_id = f"{user_id}_{access_type}_{datetime.now().strftime('%Y%m%d_%H%M%S_%f')}"
    
    # Build comprehensive audit record with ALL fields required for auditors
    audit_record = {
        "access_record_id": record_id,
        "user_id": user_id,
        "user_email": user_email,
        "user_name": user_name,
        "user_role": user_role,  # Account-level role
        # Team fields - populated from teams API
        "team_id": team_id,
        "team_name": team_name,
        "team_description": team_description,
        "team_role": team_role,
        # Group fields - populated from user API
        "group_id": group_id,
        "group_name": group_name,
        "group_created_at": group_created_at,
        "group_role": group_role,  # Role in the group
        # Role fields - populated from roles API
        "role_id": role_name if role_name != 'N/A' else 'N/A',  # Role ID is the role name
        "role_name": role_name,
        "role_description": role_description,
        "role_scope": role_scope,
        "role_is_custom": role_is_custom,
        # Connection fields - populated from user API
        "connection_id": connection_id,
        "connection_name": connection_name,
        "connection_service": connection_service,
        "connection_schema": connection_schema,
        "connection_group_id": connection_group_id,
        "connection_created_at": connection_created_at,
        "connection_destination_id": connection_destination_id,
        "connection_connector_type": connection_connector_type,
        "connection_connected_by": connection_connected_by,
        "connection_role": connection_role,  # Role on the connection
        # Access control fields
        "access_level": access_level,
        "access_type": access_type,  # GRANULAR, GROUP, CONNECTION, BASE
        "audit_timestamp": datetime.now().isoformat()
    }
    
    return audit_record

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
            "table": "soc2_access_control",
            "primary_key": ["access_record_id"]
        }
    ]

def update(configuration: dict, state: dict):
    """
    Define the update function, which is a required function, and is called by Fivetran during each sync.
    Uses Teams API, Users API, and Groups API to fetch all access control data.
    See the technical reference documentation for more details on the update function
    https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update
    Args:
        configuration: A dictionary containing connection details
        state: A dictionary containing state information from previous runs
        The state dictionary is empty for the first sync or for any full re-sync
    """
    log.info("SOC2 Compliance Connector: Starting data sync")

    # Validate the configuration
    validate_configuration(configuration=configuration)

    # Extract configuration parameters
    api_key = configuration.get("api_key")
    api_secret = configuration.get("api_secret")

    # Get state for incremental sync
    processed_users = set(state.get("processed_users", []))
    last_sync_time = state.get("last_sync_time")

    # Create authentication headers and session
    headers = create_auth_headers(api_key, api_secret)
    session = create_session()

    try:
        record_count = 0
        
        # Step 1: Fetch all teams and build team lookup
        log.info("Step 1: Fetching all teams")
        all_teams = fetch_paginated('teams', headers, session)
        log.info(f"Found {len(all_teams)} teams")
        
        teams_lookup = {}
        user_teams_mapping = {}  # Maps user_id -> list of (team_id, team_role) tuples
        
        # Fetch full team details and build user-team mappings
        for team in all_teams:
            team_id = team.get('id')
            if not team_id:
                continue
            
            # Fetch full team details
            team_details_response = make_api_request(f'teams/{team_id}', headers, session)
            if team_details_response:
                team_data = extract_nested_data(team_details_response)
                if team_data:
                    teams_lookup[team_id] = team_data
                else:
                    teams_lookup[team_id] = team
            else:
                teams_lookup[team_id] = team
            
            # Fetch team user memberships to build user-team-role relationships
            log.info(f"  Fetching user memberships for team {team_id}")
            team_user_memberships = fetch_paginated(f'teams/{team_id}/users', headers, session)
            for team_user_membership in team_user_memberships:
                user_id = team_user_membership.get('user_id') or team_user_membership.get('id')
                team_role = team_user_membership.get('role')
                if user_id:
                    if user_id not in user_teams_mapping:
                        user_teams_mapping[user_id] = []
                    user_teams_mapping[user_id].append((team_id, team_role))
        
        log.info(f"Built user-team mappings: {len(user_teams_mapping)} users with team memberships")
        
        # Step 2: Fetch all roles and build roles lookup (case-insensitive for flexible matching)
        log.info("Step 2: Fetching all roles")
        all_roles = fetch_paginated('roles', headers, session)
        # Build lookup with both exact and case-insensitive keys
        roles_lookup = {}
        roles_lookup_case_insensitive = {}
        for role in all_roles:
            role_name = role.get('name')
            if role_name:
                roles_lookup[role_name] = role
                roles_lookup_case_insensitive[role_name.lower()] = role
        log.info(f"Found {len(roles_lookup)} roles")
        
        def lookup_role(role_name: Optional[str]) -> Optional[Dict]:
            """Look up role by name with case-insensitive fallback and partial matching."""
            if not role_name:
                return None
            # Try exact match first
            if role_name in roles_lookup:
                return roles_lookup[role_name]
            # Try case-insensitive match
            role_name_lower = role_name.lower()
            if role_name_lower in roles_lookup_case_insensitive:
                return roles_lookup_case_insensitive[role_name_lower]
            # Try partial matching (e.g., "Destination Administrator" might match "Destination Administrator" or similar)
            # Check if any role name contains the search term or vice versa
            for stored_role_name, role_data in roles_lookup.items():
                if stored_role_name.lower() == role_name_lower:
                    return role_data
                # Check if one contains the other (for variations like "Destination Admin" vs "Destination Administrator")
                if role_name_lower in stored_role_name.lower() or stored_role_name.lower() in role_name_lower:
                    return role_data
            return None
        
        # Step 3: Fetch all users
        log.info("Step 3: Fetching all users")
        all_users = fetch_paginated('users', headers, session)
        log.info(f"Found {len(all_users)} users")

        # Process each user
        users_to_process = [u for u in all_users if u.get('id') and u.get('id') not in processed_users]
        log.info(f"Processing {len(users_to_process)} users (skipping {len(processed_users)} already processed)")

        # Cache for group and connection details to avoid redundant API calls
        group_details_cache = {}
        connection_details_cache = {}
        
        # Step 0: Fetch all connections once and cache by group_id for efficient lookup
        log.info("Step 0: Fetching all connections for group-based access matching")
        all_connections = fetch_paginated('connections', headers, session)
        log.info(f"Found {len(all_connections)} total connections in system")
        
        # Build connections lookup by group_id
        connections_by_group = {}
        for conn in all_connections:
            conn_group_id = conn.get('group_id')
            if conn_group_id:
                if conn_group_id not in connections_by_group:
                    connections_by_group[conn_group_id] = []
                connections_by_group[conn_group_id].append(conn)
        
        log.info(f"Connections distributed across {len(connections_by_group)} groups")

        for user in users_to_process:
            user_id = user.get('id')
            if not user_id:
                log.warning(f"Skipping user with no ID: {user.get('email', 'unknown')}")
                continue

            log.info(f"Processing user: {user_id}")

            # Step 3: Fetch user details
            user_details_response = make_api_request(f'users/{user_id}', headers, session)
            user_details = extract_nested_data(user_details_response) if user_details_response else user

            # Get all team memberships for this user (CRITICAL: process ALL teams, not just first)
            user_teams_list = user_teams_mapping.get(user_id, [])

            # Step 4: Fetch user group memberships
            log.info(f"  Fetching group memberships for user {user_id}")
            user_group_memberships = fetch_paginated(f'users/{user_id}/groups', headers, session)
            log.info(f"  Found {len(user_group_memberships)} group memberships")

            # Step 5: Build list of user's group IDs for connection matching
            user_group_ids = [gm.get('id') for gm in user_group_memberships if gm.get('id')]
            log.info(f"  User belongs to {len(user_group_ids)} groups: {user_group_ids}")

            # Step 6: For each group membership, create audit records for EACH team (if user has teams)
            # If no teams, create one record per group
            for group_membership in user_group_memberships:
                group_id = group_membership.get('id')
                if not group_id:
                    continue
                
                # Fetch full group details using groups API endpoint
                if group_id not in group_details_cache:
                    group_details_response = make_api_request(f'groups/{group_id}', headers, session)
                    group_details_cache[group_id] = extract_nested_data(group_details_response) if group_details_response else None
                
                group_details = group_details_cache.get(group_id)
                
                # Look up role details from roles API - try multiple sources
                role_name_from_membership = group_membership.get('role') or group_membership.get('role_name')
                role_data = lookup_role(role_name_from_membership)
                if not role_data and role_name_from_membership:
                    log.warning(f"Role lookup failed for '{role_name_from_membership}' - role details will be limited")
                
                # Create a record for EACH team membership (or one record if no teams)
                if user_teams_list:
                    for team_id, team_role in user_teams_list:
                        user_team_data = teams_lookup.get(team_id)
                        # Ensure team_role from membership is included in team data
                        if user_team_data and team_role:
                            user_team_data = user_team_data.copy()
                            user_team_data['role'] = team_role
                        # Create group-level audit record with team context
                        audit_record = create_audit_record(
                            user=user_details,
                            team=user_team_data,
                            role=role_data,
                            group_membership=group_membership,
                            group_details=group_details,
                            connection_membership=None,
                            connection_details=None
                        )
                        op.upsert(table="soc2_access_control", data=audit_record)
                        record_count += 1
                else:
                    # No teams - create one record for this group
                    audit_record = create_audit_record(
                        user=user_details,
                        team=None,
                        role=role_data,
                        group_membership=group_membership,
                        group_details=group_details,
                        connection_membership=None,
                        connection_details=None
                    )
                    op.upsert(table="soc2_access_control", data=audit_record)
                    record_count += 1

            # Step 7: Find all connections accessible to this user through their group memberships
            # For each group the user belongs to, get all connections in that group
            user_accessible_connections = []
            for group_membership in user_group_memberships:
                group_id = group_membership.get('id')
                if group_id and group_id in connections_by_group:
                    group_connections = connections_by_group[group_id]
                    for conn in group_connections:
                        # Add group membership context to connection for audit record
                        conn_with_group = conn.copy()
                        conn_with_group['_group_membership'] = group_membership
                        user_accessible_connections.append(conn_with_group)
            
            log.info(f"  Found {len(user_accessible_connections)} connections accessible to user through group memberships")
            
            # Step 8: For each accessible connection, create audit records for EACH team (if user has teams)
            # If no teams, create one record per connection
            for connection_data in user_accessible_connections:
                connection_id = connection_data.get('id')
                if not connection_id:
                    log.warning(f"  Skipping connection with no ID: {connection_data}")
                    continue
                
                # Get the associated group membership from the connection data
                associated_group_membership = connection_data.get('_group_membership')
                associated_group_id = associated_group_membership.get('id') if associated_group_membership else None
                associated_group_details = group_details_cache.get(associated_group_id) if associated_group_id else None
                
                # Connection details are already in connection_data (from all_connections fetch)
                connection_details = connection_data
                
                # Create synthetic connection_membership with role from group membership
                # Connections inherit permissions from groups, so use group role as connection role
                connection_role = associated_group_membership.get('role') if associated_group_membership else None
                synthetic_connection_membership = {
                    'id': connection_id,
                    'role': connection_role,
                    'created_at': connection_details.get('created_at')
                } if connection_id else None
                
                # Look up role details from roles API
                role_name_from_membership = connection_role
                role_data = lookup_role(role_name_from_membership)
                if not role_data and role_name_from_membership:
                    log.warning(f"Role lookup failed for connection role '{role_name_from_membership}' - role details will be limited")
                
                # Create a record for EACH team membership (or one record if no teams)
                if user_teams_list:
                    for team_id, team_role in user_teams_list:
                        user_team_data = teams_lookup.get(team_id)
                        # Ensure team_role from membership is included in team data
                        if user_team_data and team_role:
                            user_team_data = user_team_data.copy()
                            user_team_data['role'] = team_role
                        # Create connection-level audit record with team context
                        audit_record = create_audit_record(
                            user=user_details,
                            team=user_team_data,
                            role=role_data,
                            group_membership=associated_group_membership,
                            group_details=associated_group_details,
                            connection_membership=synthetic_connection_membership,
                            connection_details=connection_details
                        )
                        op.upsert(table="soc2_access_control", data=audit_record)
                        record_count += 1
                else:
                    # No teams - create one record for this connection
                    audit_record = create_audit_record(
                        user=user_details,
                        team=None,
                        role=role_data,
                        group_membership=associated_group_membership,
                        group_details=associated_group_details,
                        connection_membership=synthetic_connection_membership,
                        connection_details=connection_details
                    )
                    op.upsert(table="soc2_access_control", data=audit_record)
                    record_count += 1

            # Step 9: Create team-only records (users with teams but no groups/connections)
            # AND base-level records (users with no teams, groups, or connections)
            if not user_group_memberships and not user_accessible_connections:
                if user_teams_list:
                    # User has teams but no groups/connections - create a record for EACH team
                    log.info(f"  User {user_id} has {len(user_teams_list)} team(s) but no groups or connections - creating team-level records")
                    user_account_role = user_details.get('role')
                    role_data = lookup_role(user_account_role)
                    if not role_data and user_account_role:
                        log.warning(f"Role lookup failed for user account role '{user_account_role}' - role details will be limited")
                    
                    for team_id, team_role in user_teams_list:
                        user_team_data = teams_lookup.get(team_id)
                        # Ensure team_role from membership is included in team data
                        if user_team_data and team_role:
                            user_team_data = user_team_data.copy()
                            user_team_data['role'] = team_role
                        audit_record = create_audit_record(
                            user=user_details,
                            team=user_team_data,
                            role=role_data,
                            group_membership=None,
                            group_details=None,
                            connection_membership=None,
                            connection_details=None
                        )
                        op.upsert(table="soc2_access_control", data=audit_record)
                        record_count += 1
                else:
                    # User has no teams, groups, or connections - create base-level record
                    log.info(f"  User {user_id} has no teams, groups or connections - creating base-level record")
                    user_account_role = user_details.get('role')
                    role_data = lookup_role(user_account_role)
                    if not role_data and user_account_role:
                        log.warning(f"Role lookup failed for user account role '{user_account_role}' - role details will be limited")
                    
                    audit_record = create_audit_record(
                        user=user_details,
                        team=None,
                        role=role_data,
                        group_membership=None,
                        group_details=None,
                        connection_membership=None,
                        connection_details=None
                    )
                    op.upsert(table="soc2_access_control", data=audit_record)
                    record_count += 1

            # Mark user as processed
            processed_users.add(user_id)

            # Checkpoint periodically
            if record_count % CHECKPOINT_INTERVAL == 0:
                new_state = {
                    "last_sync_time": datetime.now().isoformat(),
                    "processed_users": list(processed_users)
                }
                op.checkpoint(state=new_state)
                log.info(f"Checkpointed progress: {record_count} records, {len(processed_users)} users processed")

        # Final checkpoint
        new_state = {
            "last_sync_time": datetime.now().isoformat(),
            "processed_users": list(processed_users)
        }
        op.checkpoint(state=new_state)
        
        # Summary for auditors
        log.info(f"SOC2 Compliance Connector: Completed sync")
        log.info(f"  Total users in system: {len(all_users)}")
        log.info(f"  Users processed: {len(processed_users)}")
        log.info(f"  Total audit records created: {record_count}")
        log.info(f"  Teams found: {len(teams_lookup)}")
        log.info(f"  Roles found: {len(roles_lookup)}")
        log.info(f"  Users with team memberships: {len(user_teams_mapping)}")

    except Exception as e:
        log.severe(f"SOC2 Compliance Connector failed: {str(e)}")
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
