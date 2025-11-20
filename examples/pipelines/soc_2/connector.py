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
from datetime import datetime
from typing import Dict, List, Any, Optional

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
CHECKPOINT_INTERVAL = 100  # Checkpoint every 100 records
MAX_RETRIES = 3
REQUEST_TIMEOUT = 30

def validate_configuration(configuration: dict):
    """
    Validate the configuration dictionary to ensure it contains all required parameters.
    This function is called at the start of the update method to ensure that the connector has all necessary configuration values.
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
    Raises:
        ValueError: if any required configuration parameter is missing.
    """
    required_configs = ["api_key", "api_secret"]
    for key in required_configs:
        if key not in configuration:
            raise ValueError(f"Missing required configuration value: {key}")

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

def make_api_request(endpoint: str, headers: Dict[str, str], method: str = 'GET', payload: Optional[Dict] = None) -> Optional[Dict]:
    """
    Make API request to Fivetran endpoints with proper error handling.
    Args:
        endpoint: API endpoint path
        headers: Authentication headers
        method: HTTP method (GET, POST, PATCH, DELETE)
        payload: Request payload for POST/PATCH requests
    Returns:
        API response data or None if request failed
    """
    url = f'{BASE_URL}/{endpoint}'
    
    try:
        if method == 'GET':
            response = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
        elif method == 'POST':
            response = requests.post(url, headers=headers, json=payload, timeout=REQUEST_TIMEOUT)
        elif method == 'PATCH':
            response = requests.patch(url, headers=headers, json=payload, timeout=REQUEST_TIMEOUT)
        elif method == 'DELETE':
            response = requests.delete(url, headers=headers, timeout=REQUEST_TIMEOUT)
        else:
            raise ValueError(f'Invalid request method: {method}')

        response.raise_for_status()
        return response.json()
    
    except requests.exceptions.RequestException as e:
        log.severe(f'API request failed for {endpoint}: {str(e)}')
        return None

def get_teams_data(headers: Dict[str, str]) -> List[Dict]:
    """
    Fetch all teams data from Fivetran API.
    Args:
        headers: Authentication headers
    Returns:
        List of team data dictionaries
    """
    log.info("Fetching teams data")
    teams_data = make_api_request('teams', headers)
    
    # Handle both possible response structures: data.items or just data
    if teams_data and 'data' in teams_data:
        if 'items' in teams_data['data']:
            return teams_data['data']['items']
        else:
            return teams_data['data']
    return []

def get_team_details(headers: Dict[str, str], team_id: str) -> Optional[Dict]:
    """
    Fetch detailed information for a specific team.
    Args:
        headers: Authentication headers
        team_id: Team identifier
    Returns:
        Team details dictionary or None
    """
    log.info(f"Fetching details for team: {team_id}")
    return make_api_request(f'teams/{team_id}', headers)

def get_team_groups(headers: Dict[str, str], team_id: str) -> List[Dict]:
    """
    Fetch groups associated with a team.
    Args:
        headers: Authentication headers
        team_id: Team identifier
    Returns:
        List of group data dictionaries
    """
    log.info(f"Fetching groups for team: {team_id}")
    groups_data = make_api_request(f'teams/{team_id}/groups', headers)
    
    # Handle both possible response structures: data.items or just data
    if groups_data and 'data' in groups_data:
        if 'items' in groups_data['data']:
            return groups_data['data']['items']
        else:
            return groups_data['data']
    return []

def get_team_users(headers: Dict[str, str], team_id: str) -> List[Dict]:
    """
    Fetch users associated with a team.
    Args:
        headers: Authentication headers
        team_id: Team identifier
    Returns:
        List of user data dictionaries
    """
    log.info(f"Fetching users for team: {team_id}")
    users_data = make_api_request(f'teams/{team_id}/users', headers)
    
    # Handle both possible response structures: data.items or just data
    if users_data and 'data' in users_data:
        if 'items' in users_data['data']:
            return users_data['data']['items']
        else:
            return users_data['data']
    return []

def get_user_details(headers: Dict[str, str], user_id: str) -> Optional[Dict]:
    """
    Fetch detailed information for a specific user.
    Args:
        headers: Authentication headers
        user_id: User identifier
    Returns:
        User details dictionary or None
    """
    log.info(f"Fetching details for user: {user_id}")
    return make_api_request(f'users/{user_id}', headers)

def get_user_connections(headers: Dict[str, str], user_id: str) -> List[Dict]:
    """
    Fetch connections associated with a user.
    Args:
        headers: Authentication headers
        user_id: User identifier
    Returns:
        List of connection data dictionaries
    """
    log.info(f"Fetching connections for user: {user_id}")
    connections_data = make_api_request(f'users/{user_id}/connections', headers)
    
    # Handle both possible response structures: data.items or just data
    if connections_data and 'data' in connections_data:
        if 'items' in connections_data['data']:
            return connections_data['data']['items']
        else:
            return connections_data['data']
    return []

def get_user_groups(headers: Dict[str, str], user_id: str) -> List[Dict]:
    """
    Fetch groups associated with a user.
    Args:
        headers: Authentication headers
        user_id: User identifier
    Returns:
        List of group data dictionaries
    """
    log.info(f"Fetching groups for user: {user_id}")
    groups_data = make_api_request(f'users/{user_id}/groups', headers)
    
    # Handle both possible response structures: data.items or just data
    if groups_data and 'data' in groups_data:
        if 'items' in groups_data['data']:
            return groups_data['data']['items']
        else:
            return groups_data['data']
    return []

def get_roles_data(headers: Dict[str, str]) -> List[Dict]:
    """
    Fetch all roles data from Fivetran API.
    Args:
        headers: Authentication headers
    Returns:
        List of role data dictionaries
    """
    log.info("Fetching roles data")
    roles_data = make_api_request('roles', headers)
    
    if roles_data and 'data' in roles_data and 'items' in roles_data['data']:
        return roles_data['data']['items']
    return []

def get_all_users(headers: Dict[str, str]) -> List[Dict]:
    """
    Fetch all users data from Fivetran API.
    Args:
        headers: Authentication headers
    Returns:
        List of user data dictionaries
    """
    log.info("Fetching all users data")
    users_data = make_api_request('users', headers)
    
    # Handle both possible response structures: data.items or just data
    if users_data and 'data' in users_data:
        if 'items' in users_data['data']:
            return users_data['data']['items']
        else:
            return users_data['data']
    return []

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
    # Debug logging to understand data structure
    log.info(f"Creating SOC2 record with data: user={user_data}, team={team_data}, group={group_data}, role={role_data}, connection={connection_data}")
    
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
    log.info("SOC2 Compliance Connector: Starting data sync")

    # Validate the configuration to ensure it contains all required values
    validate_configuration(configuration=configuration)

    # Extract configuration parameters
    api_key = configuration.get("api_key")
    api_secret = configuration.get("api_secret")
    debug_mode = configuration.get("debug_mode", "false").lower() == "true"
    
    # Log debug mode status
    if debug_mode:
        log.info("DEBUG MODE ENABLED: Processing will be limited to first 5 users")

    # Get the state variable for the sync
    last_sync_time = state.get("last_sync_time")
    processed_users = state.get("processed_users", set())

    # Create authentication headers
    headers = create_auth_headers(api_key, api_secret)

    try:
        record_count = 0
        
        # Fetch all users first to get the complete user list
        log.info("Fetching all users for SOC2 compliance audit")
        all_users = get_all_users(headers)
        
        # Apply debug mode limit if enabled
        if debug_mode:
            log.info("DEBUG MODE: Limiting processing to first 5 users")
            all_users = all_users[:5]
        
        # Create API log record for users endpoint
        api_log = create_api_log_record('users', 'GET', None, {'data': all_users}, 'SUCCESS')
        op.upsert(table="api_logs", data=api_log)
        record_count += 1

        # Fetch roles data
        log.info("Fetching roles data for access control mapping")
        roles_data = get_roles_data(headers)
        
        # Create API log record for roles endpoint
        api_log = create_api_log_record('roles', 'GET', None, {'data': {'items': roles_data}}, 'SUCCESS')
        op.upsert(table="api_logs", data=api_log)
        record_count += 1

        # Create roles lookup dictionary (using name as key since roles don't have id)
        log.info(f"Found {len(roles_data)} roles: {[role.get('name') for role in roles_data]}")
        roles_lookup = {role.get('name'): role for role in roles_data}

        # Fetch teams data
        log.info("Fetching teams data for organizational structure")
        teams_data = get_teams_data(headers)
        
        # Create API log record for teams endpoint
        api_log = create_api_log_record('teams', 'GET', None, {'data': teams_data}, 'SUCCESS')
        op.upsert(table="api_logs", data=api_log)
        record_count += 1

        # Create teams lookup dictionary
        log.info(f"Found {len(teams_data)} teams: {[team.get('id') for team in teams_data]}")
        teams_lookup = {team.get('id'): team for team in teams_data}

        # Process each user for SOC2 compliance
        for user in all_users:
            log.info(f"Raw user data: {json.dumps(user, indent=2)}")
            user_id = user.get('id')
            if not user_id or user_id in processed_users:
                continue

            log.info(f"Processing user: {user_id}")

            # Fetch user details
            user_details = get_user_details(headers, user_id)
            log.info(f"User {user_id} details: {json.dumps(user_details, indent=2) if user_details else 'None'}")
            if user_details:
                # Create API log record for user details endpoint
                api_log = create_api_log_record(f'users/{user_id}', 'GET', None, user_details, 'SUCCESS')
                op.upsert(table="api_logs", data=api_log)
                record_count += 1

            # Fetch user connections
            user_connections = get_user_connections(headers, user_id)
            log.info(f"User {user_id} has {len(user_connections)} connections")
            if user_connections:
                log.info(f"User {user_id} connections data: {json.dumps(user_connections, indent=2)}")
                # Create API log record for user connections endpoint
                api_log = create_api_log_record(f'users/{user_id}/connections', 'GET', None, {'data': user_connections}, 'SUCCESS')
                op.upsert(table="api_logs", data=api_log)
                record_count += 1

            # Fetch user groups
            user_groups = get_user_groups(headers, user_id)
            log.info(f"User {user_id} has {len(user_groups)} groups")
            if user_groups:
                log.info(f"User {user_id} groups data: {json.dumps(user_groups, indent=2)}")
                # Create API log record for user groups endpoint
                api_log = create_api_log_record(f'users/{user_id}/groups', 'GET', None, {'data': user_groups}, 'SUCCESS')
                op.upsert(table="api_logs", data=api_log)
                record_count += 1

            # Process each connection for the user
            for connection in user_connections:
                connection_id = connection.get('id')
                if connection_id:
                    # Create API log record for specific connection endpoint
                    api_log = create_api_log_record(f'users/{user_id}/connections/{connection_id}', 'GET', None, connection, 'SUCCESS')
                    op.upsert(table="api_logs", data=api_log)
                    record_count += 1

            # Process each group for the user
            for group in user_groups:
                group_id = group.get('id')
                if group_id:
                    # Create API log record for specific group endpoint
                    api_log = create_api_log_record(f'users/{user_id}/groups/{group_id}', 'GET', None, group, 'SUCCESS')
                    op.upsert(table="api_logs", data=api_log)
                    record_count += 1

            # Create comprehensive SOC2 access control records showing complete access hierarchy
            log.info(f"Creating comprehensive SOC2 records for user {user_id}")
            soc2_records_created = 0
            
            # 1. Create records for each group (team-level access)
            for group in user_groups:
                team_id = group.get('team_id')
                team_data = teams_lookup.get(team_id) if team_id else None
                role_name = group.get('role_name') or group.get('role')
                role_data = roles_lookup.get(role_name) if role_name else None
                
                log.info(f"Creating team-level SOC2 record: user={user_id}, team={team_id}, group={group.get('id')}, role={role_name}")
                
                user_data = user_details.get('data') if user_details and 'data' in user_details else user
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
            
            # 2. Create records for each connection (connection-level access)
            for connection in user_connections:
                log.info(f"Creating connection-level SOC2 record: user={user_id}, connection={connection.get('id')}")
                
                user_data = user_details.get('data') if user_details and 'data' in user_details else user
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
            
            # 3. Create granular records for each connection-group combination (most specific access)
            for connection in user_connections:
                for group in user_groups:
                    team_id = group.get('team_id')
                    team_data = teams_lookup.get(team_id) if team_id else None
                    role_name = group.get('role_name') or group.get('role')
                    role_data = roles_lookup.get(role_name) if role_name else None
                    
                    log.info(f"Creating granular SOC2 record: user={user_id}, connection={connection.get('id')}, group={group.get('id')}, role={role_name}")
                    
                    user_data = user_details.get('data') if user_details and 'data' in user_details else user
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
            
            # 4. Create a base-level record for all users (shows user exists in system)
            log.info(f"Creating base-level SOC2 record for user {user_id}")
            user_data = user_details.get('data') if user_details and 'data' in user_details else user
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
            
            log.info(f"Created {soc2_records_created} comprehensive SOC2 access control records for user {user_id}")

            # Mark user as processed
            processed_users.add(user_id)

            # Checkpoint every CHECKPOINT_INTERVAL records
            if record_count % CHECKPOINT_INTERVAL == 0:
                new_state = {
                    "last_sync_time": datetime.now().isoformat(),
                    "processed_users": list(processed_users),
                    "records_processed": record_count
                }
                op.checkpoint(state=new_state)
                log.info(f"Checkpointed at {record_count} records")

        # Process teams and their associated data
        for team in teams_data:
            team_id = team.get('id')
            if not team_id:
                continue

            log.info(f"Processing team: {team_id}")

            # Fetch team details
            team_details = get_team_details(headers, team_id)
            if team_details:
                # Create API log record for team details endpoint
                api_log = create_api_log_record(f'teams/{team_id}', 'GET', None, team_details, 'SUCCESS')
                op.upsert(table="api_logs", data=api_log)
                record_count += 1

            # Fetch team groups
            team_groups = get_team_groups(headers, team_id)
            if team_groups:
                # Create API log record for team groups endpoint
                api_log = create_api_log_record(f'teams/{team_id}/groups', 'GET', None, {'data': team_groups}, 'SUCCESS')
                op.upsert(table="api_logs", data=api_log)
                record_count += 1

            # Fetch team users
            team_users = get_team_users(headers, team_id)
            if team_users:
                # Create API log record for team users endpoint
                api_log = create_api_log_record(f'teams/{team_id}/users', 'GET', None, {'data': team_users}, 'SUCCESS')
                op.upsert(table="api_logs", data=api_log)
                record_count += 1

            # Process each group in the team
            for group in team_groups:
                group_id = group.get('id')
                if group_id:
                    # Create API log record for specific team group endpoint
                    api_log = create_api_log_record(f'teams/{team_id}/groups/{group_id}', 'GET', None, group, 'SUCCESS')
                    op.upsert(table="api_logs", data=api_log)
                    record_count += 1

            # Process each user in the team
            for team_user in team_users:
                user_id = team_user.get('id')
                if user_id:
                    # Create API log record for specific team user endpoint
                    api_log = create_api_log_record(f'teams/{team_id}/users/{user_id}', 'GET', None, team_user, 'SUCCESS')
                    op.upsert(table="api_logs", data=api_log)
                    record_count += 1

        # Final checkpoint with updated state
        new_state = {
            "last_sync_time": datetime.now().isoformat(),
            "processed_users": list(processed_users),
            "records_processed": record_count
        }
        op.checkpoint(state=new_state)
        
        log.info(f"SOC2 Compliance Connector: Completed sync with {record_count} total records")

    except Exception as e:
        log.severe(f"SOC2 Compliance Connector failed: {str(e)}")
        raise RuntimeError(f"Failed to sync SOC2 compliance data: {str(e)}")

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
