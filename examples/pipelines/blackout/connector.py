# This connector manages Fivetran connection blackout periods by checking connection status
# and pausing connections during specified blackout times to prevent overloading data sources.
# See the Technical Reference documentation (https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update)
# and the Best Practices documentation (https://fivetran.com/docs/connectors/connector-sdk/best-practices) for details

# Import required classes from fivetran_connector_sdk
from fivetran_connector_sdk import Connector # For supporting Connector operations like Update() and Schema()
from fivetran_connector_sdk import Logging as log # For enabling Logs in your connector code
from fivetran_connector_sdk import Operations as op # For supporting Data operations like Upsert(), Update(), Delete() and checkpoint()

# Add your source-specific imports here
import json
import requests
from requests.auth import HTTPBasicAuth
from datetime import datetime, time
import pytz
from typing import Dict, List, Optional


# Constants for blackout period parsing
WEEKDAYS = ['monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday']
TIME_FORMATS = ['%H:%M', '%I:%M%p', '%I:%M %p', '%I%p', '%I:%M%p']

def parse_connector_ids(connector_ids_str: str) -> List[str]:
    """
    Parse the comma-separated connector IDs string into a list of connector IDs.
    Expected format: "connector1,connector2,connector3"
    Args:
        connector_ids_str: Comma-separated string of connector IDs
    Returns:
        List of connector IDs, or empty list if string is empty
    """
    if not connector_ids_str or not connector_ids_str.strip():
        return []
    
    # Split by comma and clean up whitespace
    connector_ids = [cid.strip() for cid in connector_ids_str.split(',') if cid.strip()]
    
    log.info(f"Parsed {len(connector_ids)} connector ID(s) from comma-separated string")
    return connector_ids

def validate_configuration(configuration: dict):
    """
    Validate the configuration dictionary to ensure it contains all required parameters.
    This function is called at the start of the update method to ensure that the connector has all necessary configuration values.
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
    Raises:
        ValueError: if any required configuration parameter is missing.
    """
    # Validate required configuration parameters
    required_configs = ["api_key", "api_secret", "group_id", "blackout_periods", "connector_ids"]
    for key in required_configs:
        if key not in configuration:
            raise ValueError(f"Missing required configuration value: {key}")
    
    # Validate blackout periods format
    blackout_periods = configuration.get("blackout_periods", "")
    if not blackout_periods:
        raise ValueError("Blackout periods cannot be empty")
    
    # Validate connector_ids - required for scalability
    connector_ids = configuration.get("connector_ids", "")
    if not connector_ids or not connector_ids.strip():
        raise ValueError("connector_ids is required and cannot be empty")
    if not isinstance(connector_ids, str):
        raise ValueError("connector_ids must be a comma-separated string")

def parse_blackout_periods(blackout_periods: str) -> List[Dict]:
    """
    Parse the blackout periods string into structured data.
    Expected format: "monday-friday,9am-11pm;saturday,10am-6pm"
    Args:
        blackout_periods: Comma-separated string of blackout periods
    Returns:
        List of dictionaries containing parsed blackout period information
    """
    parsed_periods = []
    
    # Split by semicolon for multiple period groups
    period_groups = blackout_periods.split(';')
    
    for group in period_groups:
        if not group.strip():
            continue
            
        # Split by comma for days and time
        parts = group.split(',')
        if len(parts) != 2:
            log.warning(f"Skipping invalid blackout period format: {group}")
            continue
            
        days_part, time_part = parts[0].strip(), parts[1].strip()
        
        # Parse days (e.g., "monday-friday" or "monday")
        days = parse_days(days_part)
        log.info(f"Parsed days '{days_part}' → {days}")
        
        # Parse time range (e.g., "9am-11pm")
        time_range = parse_time_range(time_part)
        log.info(f"Parsed time range '{time_part}' → {time_range}")
        
        if days and time_range:
            parsed_periods.append({
                'days': days,
                'start_time': time_range['start'],
                'end_time': time_range['end']
            })
            log.info(f"Added blackout period: {days} {time_range['start']}-{time_range['end']}")
        else:
            log.warning(f"Failed to parse blackout period: days={days}, time_range={time_range}")
    
    return parsed_periods

def parse_days(days_str: str) -> List[str]:
    """
    Parse days string into list of weekday names.
    Args:
        days_str: String like "monday-friday" or "monday"
    Returns:
        List of weekday names
    """
    days = []
    
    if '-' in days_str:
        # Handle range like "monday-friday"
        start_day, end_day = days_str.split('-')
        start_idx = WEEKDAYS.index(start_day.lower())
        end_idx = WEEKDAYS.index(end_day.lower())
        
        if start_idx <= end_idx:
            days = WEEKDAYS[start_idx:end_idx + 1]
        else:
            # Handle wrap-around (e.g., "friday-monday")
            days = WEEKDAYS[start_idx:] + WEEKDAYS[:end_idx + 1]
    else:
        # Single day
        if days_str.lower() in WEEKDAYS:
            days = [days_str.lower()]
    
    return days

def parse_time_range(time_str: str) -> Optional[Dict]:
    """
    Parse time range string into start and end times.
    Args:
        time_str: String like "9am-11pm"
    Returns:
        Dictionary with 'start' and 'end' time objects, or None if invalid
    """
    if '-' not in time_str:
        return None
        
    start_time_str, end_time_str = time_str.split('-')
    
    start_time = parse_time(start_time_str.strip())
    end_time = parse_time(end_time_str.strip())
    
    if start_time and end_time:
        return {
            'start': start_time,
            'end': end_time
        }
    
    return None

def parse_time(time_str: str) -> Optional[time]:
    """
    Parse time string into time object.
    Args:
        time_str: String like "9am", "11pm", "14:30"
    Returns:
        time object or None if invalid
    """
    time_str = time_str.lower().strip()
    
    # Try different time formats
    for fmt in TIME_FORMATS:
        try:
            parsed = datetime.strptime(time_str, fmt).time()
            log.info(f"Successfully parsed '{time_str}' with format '{fmt}' → {parsed}")
            return parsed
        except ValueError:
            continue
    
    # Handle 24-hour format without leading zeros
    if ':' in time_str and len(time_str.split(':')[0]) == 1:
        try:
            return datetime.strptime(f"0{time_str}", "%H:%M").time()
        except ValueError:
            pass
    
    # Handle special case for "closed" or empty time
    if time_str in ['closed', 'none', '']:
        return None
    
    # Try to parse common formats manually
    try:
        # Handle "9am", "11pm" format
        if time_str.endswith('am') or time_str.endswith('pm'):
            hour_str = time_str[:-2]
            if hour_str.isdigit():
                hour = int(hour_str)
                if hour == 12:
                    hour = 0 if time_str.endswith('am') else 12
                elif time_str.endswith('pm') and hour != 12:
                    hour += 12
                parsed_time = time(hour=hour, minute=0)
                log.info(f"Manually parsed '{time_str}' → {parsed_time}")
                return parsed_time
    except (ValueError, TypeError):
        pass
    
    log.warning(f"Could not parse time: {time_str}")
    return None

def is_in_blackout_period(parsed_periods: List[Dict], current_time: datetime) -> bool:
    """
    Check if current time falls within any blackout period.
    Args:
        parsed_periods: List of parsed blackout periods
        current_time: Current datetime to check
    Returns:
        True if current time is in blackout period, False otherwise
    """
    current_weekday = current_time.strftime('%A').lower()
    current_time_obj = current_time.time()
    
    log.info(f"Checking blackout periods for {current_weekday} at {current_time_obj}")
    
    for period in parsed_periods:
        log.info(f"Checking period: {period['days']} {period['start_time']}-{period['end_time']}")
        
        if current_weekday in period['days']:
            start_time = period['start_time']
            end_time = period['end_time']
            
            log.info(f"Current day matches period. Checking time: {current_time_obj} between {start_time} and {end_time}")
            
            # Handle overnight periods (e.g., 11pm to 9am)
            if start_time > end_time:
                if current_time_obj >= start_time or current_time_obj <= end_time:
                    log.info(f"Overnight period: Current time {current_time_obj} is in blackout period")
                    return True
            else:
                if start_time <= current_time_obj <= end_time:
                    log.info(f"Standard period: Current time {current_time_obj} is in blackout period")
                    return True
                else:
                    log.info(f"Standard period: Current time {current_time_obj} is NOT in blackout period")
        else:
            log.info(f"Current day {current_weekday} does not match period days {period['days']}")
    
    log.info(f"No blackout periods match current time")
    return False

def make_fivetran_api_request(method: str, endpoint: str, api_key: str, api_secret: str, payload: dict = None) -> Optional[dict]:
    """
    Make HTTP request to Fivetran API.
    Args:
        method: HTTP method (GET, POST, PATCH, DELETE)
        endpoint: API endpoint
        api_key: Fivetran API key
        api_secret: Fivetran API secret
        payload: Request payload for POST/PATCH requests
    Returns:
        API response as dictionary or None if failed
    """
    base_url = 'https://api.fivetran.com/v1'
    headers = {
        'Authorization': f'Bearer {api_key}:{api_secret}'
    }
    url = f'{base_url}/{endpoint}'
    
    try:
        if method == 'GET':
            response = requests.get(url, headers=headers, auth=HTTPBasicAuth(api_key, api_secret))
        elif method == 'POST':
            response = requests.post(url, headers=headers, json=payload, auth=HTTPBasicAuth(api_key, api_secret))
        elif method == 'PATCH':
            response = requests.patch(url, headers=headers, json=payload, auth=HTTPBasicAuth(api_key, api_secret))
        elif method == 'DELETE':
            response = requests.delete(url, headers=headers, auth=HTTPBasicAuth(api_key, api_secret))
        else:
            raise ValueError('Invalid request method.')
        
        response.raise_for_status()
        return response.json()
        
    except requests.exceptions.RequestException as e:
        log.severe(f'Fivetran API request failed: {e}')
        return None

def get_connector_details(api_key: str, api_secret: str, connector_id: str) -> Optional[Dict]:
    """
    Get details of a specific connector by ID.
    This is more scalable than fetching all connectors and filtering.
    Args:
        api_key: Fivetran API key
        api_secret: Fivetran API secret
        connector_id: Fivetran connector ID
    Returns:
        Connector details dictionary or None if not found
    """
    endpoint = f'connectors/{connector_id}'
    response = make_fivetran_api_request('GET', endpoint, api_key, api_secret)
    
    if response and response.get('code') == 'Success':
        return response.get('data', {})
    
    return None

def get_connector_status(api_key: str, api_secret: str, group_id: str) -> List[Dict]:
    """
    Get status of all connectors in a group.
    This is used as a fallback when connector_ids are not specified.
    Args:
        api_key: Fivetran API key
        api_secret: Fivetran API secret
        group_id: Fivetran group ID
    Returns:
        List of connector status information
    """
    endpoint = f'groups/{group_id}/connectors'
    response = make_fivetran_api_request('GET', endpoint, api_key, api_secret)
    
    if response and response.get('code') == 'Success':
        return response.get('data', {}).get('items', [])
    
    return []

def pause_connector(api_key: str, api_secret: str, group_id: str, connector_id: str) -> bool:
    """
    Pause a specific connector.
    Args:
        api_key: Fivetran API key
        api_secret: Fivetran API secret
        group_id: Fivetran group ID
        connector_id: Connector ID to pause
    Returns:
        True if successful, False otherwise
    """
    endpoint = f'connectors/{connector_id}'
    payload = {
        "paused": True
    }
    
    response = make_fivetran_api_request('PATCH', endpoint, api_key, api_secret, payload)
    return response is not None and response.get('code') == 'Success'

def resume_connector(api_key: str, api_secret: str, group_id: str, connector_id: str) -> bool:
    """
    Resume a specific connector.
    Args:
        api_key: Fivetran API key
        api_secret: Fivetran API secret
        group_id: Fivetran group ID
        connector_id: Connector ID to resume
    Returns:
        True if successful, False otherwise
    """
    endpoint = f'connectors/{connector_id}'
    payload = {
        "paused": False
    }
    
    response = make_fivetran_api_request('PATCH', endpoint, api_key, api_secret, payload)
    return response is not None and response.get('code') == 'Success'

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
            "table": "blackout_log", # Name of the table in the destination, required.
            "primary_key": ["timestamp", "connector_id"]
        },
        {
            "table": "connector_status", # Name of the table in the destination, required.
            "primary_key": ["timestamp", "connector_id"]
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
    log.info("Blackout Connector: Managing Fivetran connection blackout periods")

    # Validate the configuration to ensure it contains all required values.
    validate_configuration(configuration=configuration)

    # Extract configuration parameters
    api_key = configuration.get("api_key")
    api_secret = configuration.get("api_secret")
    group_id = configuration.get("group_id")
    blackout_periods_str = configuration.get("blackout_periods")
    connector_ids_str = configuration.get("connector_ids", "")
    
    # Get timezone from configuration or default to UTC
    timezone_str = configuration.get("timezone", "UTC")
    try:
        tz = pytz.timezone(timezone_str)
    except pytz.exceptions.UnknownTimeZoneError:
        log.warning(f"Unknown timezone: {timezone_str}, using UTC")
        tz = pytz.timezone("UTC")

    # Get the last sync time from state, if available
    last_sync_time = state.get("last_sync_time")
    
    try:
        # Parse blackout periods
        log.info(f"Processing blackout periods: '{blackout_periods_str}'")
        parsed_periods = parse_blackout_periods(blackout_periods_str)
        log.info(f"Parsed {len(parsed_periods)} blackout period(s)")
        
        # Parse connector IDs - required for scalable operation
        target_connector_ids = parse_connector_ids(connector_ids_str)
        if not target_connector_ids:
            raise ValueError("No valid connector IDs found in connector_ids configuration")
        
        log.info(f"Processing {len(target_connector_ids)} connector(s) by ID: {target_connector_ids}")
        
        # Get current time in specified timezone
        current_time = datetime.now(tz)
        log.info(f"Current time: {current_time.strftime('%Y-%m-%d %H:%M:%S %Z')}")
        log.info(f"Current weekday: {current_time.strftime('%A').lower()}")
        
        # Check if currently in blackout period
        in_blackout = is_in_blackout_period(parsed_periods, current_time)
        log.info(f"Currently in blackout period: {in_blackout}")
        
        # Fetch connectors individually by ID - scalable approach
        connectors = []
        for connector_id in target_connector_ids:
            connector = get_connector_details(api_key, api_secret, connector_id)
            if connector:
                connectors.append(connector)
                log.info(f"Successfully fetched connector {connector_id}")
            else:
                log.warning(f"Failed to fetch connector {connector_id} - skipping")
        
        log.info(f"Successfully fetched {len(connectors)} out of {len(target_connector_ids)} connector(s)")
        
        # Process each connector
        for connector in connectors:
            connector_id = connector.get('id')
            connector_name = connector.get('schema', 'Unknown')
            service = connector.get('service', 'Unknown')
            schema_name = connector.get('schema', 'Unknown')
            sync_state = connector.get('status', {}).get('sync_state', 'Unknown')
            paused = connector.get('paused', False)
            sync_frequency = connector.get('sync_frequency', 0)
            
            # Log connector status
            status_record = {
                "timestamp": current_time.isoformat(),
                "connector_id": connector_id,
                "connector_name": connector_name,
                "service": service,
                "schema": schema_name,
                "sync_state": sync_state,
                "paused": paused,
                "sync_frequency": sync_frequency,
                "group_id": group_id,
                "in_blackout_period": in_blackout
            }
            
            # Upsert connector status record - direct operation call without yield
            op.upsert(table="connector_status", data=status_record)
            
            # Determine action based on blackout status
            action_taken = None
            reason = None
            blackout_period_info = None
            
            log.info(f"Processing connector {connector_name} ({connector_id}): paused={paused}, in_blackout={in_blackout}")
            
            if in_blackout and not paused:
                # Should pause connector
                log.info(f"Should pause connector {connector_name} - in blackout period and not paused")
                if pause_connector(api_key, api_secret, group_id, connector_id):
                    action_taken = "pause"
                    reason = "Blackout period active"
                    blackout_period_info = blackout_periods_str
                    log.info(f"Paused connector {connector_name} ({connector_id}) due to blackout period")
                else:
                    log.warning(f"Failed to pause connector {connector_name} ({connector_id})")
                    
            elif not in_blackout and paused:
                # Should resume connector
                log.info(f"Should resume connector {connector_name} - not in blackout period and paused")
                if resume_connector(api_key, api_secret, group_id, connector_id):
                    action_taken = "resume"
                    reason = "Blackout period ended"
                    blackout_period_info = blackout_periods_str
                    log.info(f"Resumed connector {connector_name} ({connector_id}) after blackout period")
                else:
                    log.warning(f"Failed to resume connector {connector_name} ({connector_id})")
            else:
                log.info(f"No action needed for connector {connector_name}: paused={paused}, in_blackout={in_blackout}")
            
            # Log action if taken
            if action_taken:
                action_record = {
                    "timestamp": current_time.isoformat(),
                    "connector_id": connector_id,
                    "connector_name": connector_name,
                    "action": action_taken,
                    "reason": reason,
                    "blackout_period": blackout_period_info,
                    "group_id": group_id,
                    "sync_state": sync_state
                }
                
                # Upsert action log record - direct operation call without yield
                op.upsert(table="blackout_log", data=action_record)
        
        # Update state with the current sync time for the next run
        new_state = {
            "last_sync_time": current_time.isoformat(),
            "last_blackout_check": current_time.isoformat(),
            "blackout_periods_parsed": len(parsed_periods)
        }

        # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
        # from the correct position in case of next sync or interruptions.
        # Learn more about how and where to checkpoint by reading our best practices documentation
        # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
        op.checkpoint(state=new_state)
        
        log.info(f"Successfully processed {len(connectors)} connectors. Blackout period active: {in_blackout}")

    except Exception as e:
        # In case of an exception, raise a runtime error
        log.severe(f"Failed to manage blackout periods: {str(e)}")
        raise RuntimeError(f"Failed to manage blackout periods: {str(e)}")

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
