"""PostgreSQL Data Freshness Monitor and Fivetran Sync Trigger Connector.

This connector connects to PostgreSQL, executes a data freshness query, and triggers
a Fivetran connector sync if the data is determined to be stale. It monitors the
freshness of customer data and automatically syncs the source connector when needed.

"""

# Import required classes from fivetran_connector_sdk
from fivetran_connector_sdk import Connector  # For supporting Connector operations like Update() and Schema()
from fivetran_connector_sdk import Logging as log  # For enabling Logs in your connector code
from fivetran_connector_sdk import Operations as op  # For supporting Data operations like Upsert(), Update(), Delete() and checkpoint()

# Import source-specific libraries
import json
import psycopg2
from psycopg2 import sql
from psycopg2.extras import RealDictCursor
import requests
from requests.auth import HTTPBasicAuth
from datetime import datetime, timezone
import time


def validate_configuration(configuration: dict):
    """
    Validate the configuration dictionary to ensure it contains all required parameters.
    This function is called at the start of the update method to ensure that the connector has all necessary configuration values.
    
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
    
    Raises:
        ValueError: if any required configuration parameter is missing.
    """
    
    # Validate PostgreSQL connection parameters
    postgres_configs = ["postgres_host", "postgres_port", "postgres_database", "postgres_user", "postgres_password"]
    for key in postgres_configs:
        if key not in configuration:
            raise ValueError(f"Missing required PostgreSQL configuration value: {key}")
    
    # Validate Fivetran API parameters
    fivetran_configs = ["fivetran_api_key", "fivetran_api_secret", "fivetran_connector_id"]
    for key in fivetran_configs:
        if key not in configuration:
            raise ValueError(f"Missing required Fivetran configuration value: {key}")
    
    # Validate query parameters
    if "freshness_query" not in configuration:
        raise ValueError("Missing required configuration value: freshness_query")


def get_postgres_connection(configuration: dict):
    """
    Create and return a PostgreSQL connection using the provided configuration.
    
    Args:
        configuration: Dictionary containing PostgreSQL connection parameters.
    
    Returns:
        psycopg2.extensions.connection: Active PostgreSQL connection.
    
    Raises:
        RuntimeError: If connection fails.
    """
    try:
        # Convert port to integer since Fivetran requires all config values to be strings
        port = int(configuration["postgres_port"])
        
        conn = psycopg2.connect(
            host=configuration["postgres_host"],
            port=port,
            database=configuration["postgres_database"],
            user=configuration["postgres_user"],
            password=configuration["postgres_password"]
        )
        log.info("Successfully connected to PostgreSQL")
        return conn
    except Exception as e:
        log.severe(f"Failed to connect to PostgreSQL: {e}")
        raise RuntimeError(f"PostgreSQL connection failed: {str(e)}")


def execute_freshness_query(connection, query: str):
    """
    Execute the data freshness query and return results.
    
    Args:
        connection: Active PostgreSQL connection.
        query: SQL query to execute for data freshness check.
    
    Returns:
        list: Query results as list of dictionaries.
    
    Raises:
        RuntimeError: If query execution fails.
    """
    try:
        with connection.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(query)
            
            # Fetch all results
            results = cursor.fetchall()
            
            # Convert to list of dictionaries
            data = [dict(row) for row in results]
            
            log.info(f"Successfully executed freshness query, returned {len(data)} rows")
            return data
        
    except Exception as e:
        log.severe(f"Failed to execute freshness query: {e}")
        raise RuntimeError(f"Query execution failed: {str(e)}")


def trigger_fivetran_sync(connector_id: str, api_key: str, api_secret: str):
    """
    Trigger Fivetran connector sync by unpausing and forcing a sync.
    
    Args:
        connector_id: Fivetran connector ID to sync.
        api_key: Fivetran API key.
        api_secret: Fivetran API secret.
    
    Returns:
        dict: Response from Fivetran API.
    
    Raises:
        RuntimeError: If sync trigger fails.
    """
    try:
        # Use both Authorization header and HTTPBasicAuth for compatibility
        auth = HTTPBasicAuth(api_key, api_secret)
        
        headers = {
            'Authorization': f'Bearer {api_key}:{api_secret}',
            'Content-Type': 'application/json'
        }
        
        # First unpause the connector
        unpause_url = f'https://api.fivetran.com/v1/connectors/{connector_id}'
        unpause_payload = {"paused": False}
        
        log.info(f"Unpausing connector {connector_id}")
        response = requests.patch(unpause_url, headers=headers, json=unpause_payload, auth=auth)
        response.raise_for_status()
        log.info("Connector unpaused successfully")
        
        # Wait a moment for the unpause to take effect
        time.sleep(2)
        
        # Then trigger sync
        sync_url = f'https://api.fivetran.com/v1/connectors/{connector_id}/sync'
        sync_payload = {"force": True}
        
        log.info(f"Triggering sync for connector {connector_id}")
        response = requests.post(sync_url, headers=headers, json=sync_payload, auth=auth)
        response.raise_for_status()
        
        sync_response = response.json()
        log.info("Sync triggered successfully")
        return sync_response
        
    except requests.exceptions.RequestException as e:
        log.severe(f"Failed to trigger Fivetran sync: {e}")
        raise RuntimeError(f"Fivetran sync trigger failed: {str(e)}")


def analyze_freshness_data(data: list):
    """
    Analyze the freshness data to determine if sync is needed.
    
    Args:
        data: List of dictionaries containing freshness query results.
    
    Returns:
        dict: Analysis results including sync recommendation and details.
    """
    if not data:
        return {
            "sync_needed": False,
            "reason": "No data returned from freshness query",
            "stale_records": 0,
            "total_records": 0,
            "fresh_records": 0
        }
    
    stale_records = 0
    total_records = len(data)
    
    for record in data:
        # Handle both uppercase and lowercase column names from PostgreSQL
        freshness_status = record.get("freshness_status") or record.get("FRESHNESS_STATUS")
        if freshness_status == "Stale data":
            stale_records += 1
    
    # Calculate fresh records
    fresh_records = total_records - stale_records
    
    # Determine if sync is needed based on stale data percentage
    stale_percentage = (stale_records / total_records) * 100 if total_records > 0 else 0
    
    # Sync if more than 10% of records are stale (configurable threshold)
    sync_needed = stale_percentage > 10
    
    log.info(f" Analysis results: {stale_records} stale records, {fresh_records} fresh records out of {total_records} total ({stale_percentage:.1f}% stale). Sync needed: {sync_needed}")
    
    return {
        "sync_needed": sync_needed,
        "reason": f"{stale_percentage:.1f}% of records are stale ({stale_records}/{total_records})",
        "stale_records": stale_records,
        "total_records": total_records,
        "stale_percentage": stale_percentage,
        "fresh_records": fresh_records
    }


def schema(configuration: dict):
    """
    Define the schema function which lets you configure the schema your connector delivers.
    
    This connector creates a sync_events table to track all sync trigger attempts and results.
    
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
    """
    
    return [
        {
            "table": "sync_events",
            "primary_key": ["event_timestamp", "connector_id"]
        }
    ]


def update(configuration: dict, state: dict):
    """
    Execute the data freshness monitoring workflow and trigger Fivetran syncs when needed.
    
    This function connects to PostgreSQL, executes a freshness query, analyzes the results,
    and automatically triggers Fivetran connector syncs if stale data is detected.
    
    Args:
        configuration: Dictionary containing connection and API configuration parameters
        state: Dictionary containing state information from previous execution runs
    
    Raises:
        RuntimeError: If the monitoring process fails
    """
    
    log.info("Starting PostgreSQL Data Freshness Monitor and Fivetran Sync Trigger")
    
    # Validate configuration parameters
    log.info("Validating configuration parameters")
    validate_configuration(configuration=configuration)
    log.info("Configuration validation completed successfully")
    
    # Extract configuration parameters
    freshness_query = configuration.get("freshness_query")
    fivetran_connector_id = configuration.get("fivetran_connector_id")
    fivetran_api_key = configuration.get("fivetran_api_key")
    fivetran_api_secret = configuration.get("fivetran_api_secret")
    
    # Retrieve previous execution state
    last_check_time = state.get("last_check_time")
    if last_check_time:
        log.info(f"Previous execution timestamp: {last_check_time}")
    else:
        log.info("Initial execution - no previous state found")
    
    try:
        # Establish PostgreSQL connection
        log.info("Establishing PostgreSQL connection")
        postgres_conn = get_postgres_connection(configuration)
        log.info("PostgreSQL connection established successfully")
        
        # Execute data freshness query
        log.info("Executing data freshness query")
        log.info(f"Query: {freshness_query}")
        freshness_data = execute_freshness_query(postgres_conn, freshness_query)
        log.info(f"Query execution completed. Retrieved {len(freshness_data)} records for analysis")
        
        # Analyze data freshness and determine sync requirements
        log.info("Analyzing data freshness metrics")
        analysis = analyze_freshness_data(freshness_data)
        
        current_timestamp = datetime.now(timezone.utc)
        log.info(f"Analysis completed at: {current_timestamp}")
        
        # Log analysis results
        if analysis["sync_needed"]:
            log.warning(f"Stale data detected: {analysis['reason']}")
            log.warning(f"Stale records: {analysis['stale_records']}/{analysis['total_records']} ({analysis['stale_percentage']:.1f}%)")
            log.info("Initiating Fivetran sync trigger sequence")
        else:
            log.info(f"Data freshness status: {analysis['reason']}")
            log.info(f"Fresh records: {analysis['fresh_records']}/{analysis['total_records']}")
            log.info("No sync required - data is current")
        
        # Create sync event record for tracking
        sync_event = {
            "event_timestamp": current_timestamp,
            "connector_id": fivetran_connector_id,
            "sync_triggered": analysis["sync_needed"],
            "trigger_reason": analysis["reason"],
            "stale_records_count": analysis["stale_records"],
            "total_records_count": analysis["total_records"],
            "stale_percentage": analysis["stale_percentage"],
            "api_response": "",
            "success": False
        }
        
        # Trigger Fivetran sync if stale data is detected
        if analysis["sync_needed"]:
            try:
                log.info("Triggering Fivetran connector sync")
                api_response = trigger_fivetran_sync(
                    fivetran_connector_id, 
                    fivetran_api_key, 
                    fivetran_api_secret
                )
                sync_event["api_response"] = json.dumps(api_response)
                sync_event["success"] = True
                log.info("Fivetran sync triggered successfully")
                log.info(f"API Response: {json.dumps(api_response, indent=2)}")
            except Exception as e:
                sync_event["api_response"] = str(e)
                sync_event["success"] = False
                log.severe(f"Failed to trigger Fivetran sync: {e}")
        else:
            log.info("No sync action required")
        
        # Upsert sync event record to destination table
        log.info("Recording sync event to destination")
        op.upsert(table="sync_events", data=sync_event)
        log.info("Sync event recorded successfully")
        
        # Close PostgreSQL connection
        log.info("Closing PostgreSQL connection")
        postgres_conn.close()
        log.info("PostgreSQL connection closed")
        
        # Update execution state for next run
        new_state = {
            "last_check_time": current_timestamp.isoformat(),
            "last_analysis": analysis
        }
        
        # Save checkpoint state
        log.info("Saving execution checkpoint")
        op.checkpoint(state=new_state)
        log.info("Checkpoint saved successfully")
        
        log.info("Data freshness monitoring execution completed successfully")
        
    except Exception as e:
        log.severe(f"Data freshness monitoring execution failed: {str(e)}")
        raise RuntimeError(f"Data freshness monitor failed: {str(e)}")


# Create the connector object using the schema and update functions
connector = Connector(update=update, schema=schema)

# Check if the script is being run as the main module.
# This is Python's standard entry method allowing your script to be run directly from the command line or IDE 'run' button.
# This is useful for debugging while you write your code. Note this method is not called by Fivetran when executing your connector in production.
# Please test using the Fivetran debug command prior to finalizing and deploying your connector.
if __name__ == "__main__":
    # Open the configuration.json file and load its contents
    with open("/configuration.json", 'r') as f:
        configuration = json.load(f)
    
    # Test the connector locally
    connector.debug(configuration=configuration)
