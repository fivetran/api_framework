#!/usr/bin/env python3
import sys
import os
import json
import urllib3
from typing import Dict, List, Any, Optional

# Disable insecure request warnings for local testing
urllib3.disable_warnings()

try:
    from mcp.server.fastmcp import FastMCP
except ImportError:
    try:
        from fastmcp import FastMCP
    except ImportError:
        print("Could not import FastMCP. Please ensure the mcp library is installed.", file=sys.stderr)
        sys.exit(1)

# Add the directory to sys.path so we can import the original script
current_dir = os.path.dirname(os.path.abspath(__file__))
if current_dir not in sys.path:
    sys.path.insert(0, current_dir)

from hvr_api_frame import HVRAPIClient, test_authentication

mcp = FastMCP("HVR API server")

_client = None

def get_client() -> HVRAPIClient:
    """
    Returns a configured HVRAPIClient using credentials from the config.json.
    """
    global _client
    if _client is not None:
        return _client
        
    try:
        # Configuration file for auth
        r = '/config.json'
        with open(r, "r") as i:
            l = i.read()
            y = json.loads(l)
            
        username = y['hvr']['username']
        password = y['hvr']['password']
        base_url = y['hvr']['base_url']
        
        # Test auth and get the token
        access_token = test_authentication(username, password, base_url)
        
        _client = HVRAPIClient(
            base_url=base_url,
            username=username,
            password=password,
            access_token=access_token
        )
        return _client
    except Exception as e:
        raise RuntimeError(f"Failed to initialize HVR client: {str(e)}")


# ----------------------------------------------------------------------
# Channel operations
# ----------------------------------------------------------------------

@mcp.tool()
def get_channel_details(channel_name: str) -> str:
    """Fetch details of a specific channel."""
    try:
        return json.dumps(get_client().get_channel_details(channel_name), indent=2)
    except Exception as e:
        return f"Error: {str(e)}"

@mcp.tool()
def create_channel(channel_config_json: str) -> str:
    """Create a new channel using a JSON string configuration."""
    try:
        config = json.loads(channel_config_json)
        return json.dumps(get_client().create_channel(config), indent=2)
    except Exception as e:
        return f"Error: {str(e)}"

@mcp.tool()
def update_channel(channel_name: str, channel_config_json: str) -> str:
    """Update an existing channel using a JSON string configuration."""
    try:
        config = json.loads(channel_config_json)
        return json.dumps(get_client().update_channel(channel_name, config), indent=2)
    except Exception as e:
        return f"Error: {str(e)}"

@mcp.tool()
def delete_channel(channel_name: str) -> str:
    """Delete an existing channel."""
    try:
        get_client().delete_channel(channel_name)
        return f"Successfully deleted channel '{channel_name}'"
    except Exception as e:
        return f"Error: {str(e)}"

@mcp.tool()
def get_all_channel_names() -> str:
    """Fetch a list of all existing channel names."""
    try:
        return json.dumps(get_client().get_all_channel_names(), indent=2)
    except Exception as e:
        return f"Error: {str(e)}"


# ----------------------------------------------------------------------
# Hub operations
# ----------------------------------------------------------------------

@mcp.tool()
def get_hub_definition() -> str:
    """Fetch hub definition."""
    try:
        return json.dumps(get_client().get_hub(), indent=2)
    except Exception as e:
        return f"Error: {str(e)}"

@mcp.tool()
def get_locations() -> str:
    """Fetch a list of all available locations in the hub."""
    try:
        return json.dumps(get_client().get_locations(), indent=2)
    except Exception as e:
        return f"Error: {str(e)}"

@mcp.tool()
def get_hub_overview(hub: str) -> str:
    """Fetch a comprehensive overview of the hub, including properties, channels, and locations."""
    try:
        client = get_client()
        status = client.get_hub_status(hub)
        channels = client.get_all_channel_names()
        locations = client.get_locations()
        
        overview = {
            "hub_properties": status,
            "channels": channels,
            "locations": locations
        }
        return json.dumps(overview, indent=2)
    except Exception as e:
        return f"Error: {str(e)}"

@mcp.tool()
def get_hub_status(hub: str) -> str:
    """Fetch hub properties and status."""
    try:
        return json.dumps(get_client().get_hub_status(hub), indent=2)
    except Exception as e:
        return f"Error: {str(e)}"

@mcp.tool()
def freeze_hub(hub: str) -> str:
    """Freeze a hub by setting its state to FROZEN."""
    try:
        get_client().post_hub_update(hub)
        return f"Hub '{hub}' frozen successfully."
    except Exception as e:
        return f"Error: {str(e)}"

@mcp.tool()
def unfreeze_hub(hub: str) -> str:
    """Unfreeze a hub by setting its state to LIVE."""
    try:
        get_client().post_hub_startv2(hub)
        return f"Hub '{hub}' unfrozen successfully."
    except Exception as e:
        return f"Error: {str(e)}"

@mcp.tool()
def stop_hub(hub: str) -> str:
    """Stop the hub."""
    try:
        return json.dumps(get_client().post_hub_stop(hub), indent=2)
    except Exception as e:
        return f"Error: {str(e)}"

@mcp.tool()
def start_hub(hub: str) -> str:
    """Start the hub."""
    try:
        return json.dumps(get_client().post_hub_start(hub), indent=2)
    except Exception as e:
        return f"Error: {str(e)}"

@mcp.tool()
def manage_hub_state(hub: str, hub_id: str) -> str:
    """Manage hub state intelligently based on current state and hub_id."""
    try:
        return get_client().manage_hub_state(hub, hub_id)
    except Exception as e:
        return f"Error: {str(e)}"


# ----------------------------------------------------------------------
# Refresh and Job Operations
# ----------------------------------------------------------------------

@mcp.tool()
def start_refresh_job(hub: str, channel: str) -> str:
    """Start a refresh job for a specific channel."""
    try:
        return json.dumps(get_client().start_refresh_job(hub, channel), indent=2)
    except Exception as e:
        return f"Error: {str(e)}"

@mcp.tool()
def poll_refresh_job(hub: str, channel: str, event_id: str, location: str = 'test_repo', timeout: int = 1800, poll_interval: int = 30) -> str:
    """Poll the refresh job status until completion or timeout."""
    try:
        return json.dumps(get_client().poll_refresh_job(hub, channel, event_id, location, timeout, poll_interval), indent=2)
    except Exception as e:
        return f"Error: {str(e)}"

@mcp.tool()
def execute_comprehensive_refresh(hub: str, channel: str, location: str = 'test_repo') -> str:
    """Comprehensive method to start refresh, poll, and retrieve results."""
    try:
        return json.dumps(get_client().execute_comprehensive_refresh(hub, channel, location), indent=2)
    except Exception as e:
        return f"Error: {str(e)}"


# ----------------------------------------------------------------------
# Metadata and Table Operations
# ----------------------------------------------------------------------

@mcp.tool()
def capture_source_table_definition(hub: str, source_location: str, table_name: str) -> str:
    """Capture the latest source table DDL definition."""
    try:
        return json.dumps(get_client().capture_source_table_definition(hub, source_location, table_name), indent=2)
    except Exception as e:
        return f"Error: {str(e)}"

@mcp.tool()
def import_table_definition(hub: str, channel: str, table_name: str, table_definition_json: str) -> str:
    """Import table definition JSON back into the hub definition."""
    try:
        definition = json.loads(table_definition_json)
        return json.dumps(get_client().import_table_definition(hub, channel, table_name, definition), indent=2)
    except Exception as e:
        return f"Error: {str(e)}"

@mcp.tool()
def adapt_check_tables(hub: str, channel: str, location: str, table_names_json: str, check_layout: bool = True, localize_datatypes: bool = True) -> str:
    """Compare hub table definition(s) against the layout in a source/target DB."""
    try:
        table_names = json.loads(table_names_json)
        return json.dumps(get_client().adapt_check_tables(hub, channel, location, table_names, check_layout=check_layout, localize_datatypes=localize_datatypes), indent=2)
    except Exception as e:
        return f"Error: {str(e)}"

@mcp.tool()
def adapt_apply_tables(hub: str, channel: str, location: str, table_names_json: str, add_tables: bool = True, check_layout: bool = True, localize_datatypes: bool = True) -> str:
    """Add/replace table definition info from a DB into the hub definition."""
    try:
        table_names = json.loads(table_names_json)
        return json.dumps(get_client().adapt_apply_tables(hub, channel, location, table_names, add_tables=add_tables, check_layout=check_layout, localize_datatypes=localize_datatypes), indent=2)
    except Exception as e:
        return f"Error: {str(e)}"

@mcp.tool()
def test_target_connectivity_for_table(hub: str, channel: str, location: str, table_name: str) -> str:
    """Lightweight connectivity preflight for a target location."""
    try:
        return str(get_client().test_target_connectivity_for_table(hub, channel, location, table_name))
    except Exception as e:
        return f"Error: {str(e)}"

@mcp.tool()
def create_alter_target_tables(hub: str, channel: str, table_names_json: str, target_locations_json: str, fill: bool = False) -> str:
    """Create or ALTER tables on target without copying data (if fill=False)."""
    try:
        table_names = json.loads(table_names_json)
        target_locations = json.loads(target_locations_json)
        return json.dumps(get_client().create_alter_target_tables(hub, channel, table_names, target_locations, fill), indent=2)
    except Exception as e:
        return f"Error: {str(e)}"

@mcp.tool()
def verify_create_alter_jobs(hub: str, job_type: str = "CreateAlterTargetTables") -> str:
    """Verify the status of create/alter target table jobs."""
    try:
        return json.dumps(get_client().verify_create_alter_jobs(hub, job_type), indent=2)
    except Exception as e:
        return f"Error: {str(e)}"

@mcp.tool()
def get_table_definition(hub: str, channel: str, table_name: str) -> str:
    """Get the current table definition stored in the hub for a specific channel."""
    try:
        return json.dumps(get_client().get_table_definition(hub, channel, table_name), indent=2)
    except Exception as e:
        return f"Error: {str(e)}"

@mcp.tool()
def execute_metadata_sync_workflow(hub: str, channel: str, table_name: str, source_location: str, target_locations_json: str) -> str:
    """Execute the complete metadata-only sync workflow."""
    try:
        target_locations = json.loads(target_locations_json)
        return json.dumps(get_client().execute_metadata_sync_workflow(hub, channel, table_name, source_location, target_locations), indent=2)
    except Exception as e:
        return f"Error: {str(e)}"

if __name__ == "__main__":
    mcp.run()
