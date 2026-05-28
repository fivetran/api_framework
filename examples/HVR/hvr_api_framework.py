"""
HVR API Client Tool
===================

A comprehensive Python client for interacting with HVR (High Volume Replicator) APIs.
This tool provides easy-to-use methods for managing HVR hubs, channels, locations, and data replication workflows.

HOW TO USE THIS TOOL EFFECTIVELY:
================================

1. QUICK START:
   - Ensure you have a valid config.json file with HVR credentials
   - Run the script to test authentication and basic functionality
   - Uncomment the use case sections you want to execute

2. USE CASES (Uncomment the sections you need):
   - Channel Migration: Copy and modify existing channels for new environments
   - Hub Management: Monitor and control hub states (freeze/unfreeze)
   - Data Refresh: Trigger and monitor data replication jobs
   - Channel Operations: Create, update, delete, and list channels

3. CUSTOMIZATION:
   - Modify channel names, locations, and configurations in the use case sections
   - Add your own methods to the HVRAPIClient class for specific needs
   - Extend the tool with additional API endpoints as needed

4. BEST PRACTICES:
   - Always backup existing channels before making changes
   - Test in development environment first
   - Use proper error handling for production deployments
   - Monitor job status and implement proper logging

5. EXTENDING THE TOOL:
   - Add new API methods following the existing pattern
   - Create workflow methods that combine multiple operations
   - Implement validation and error handling for new features
   - Use the existing authentication and header management

EXAMPLE WORKFLOWS:
=================

# Environment Migration
1. Get details from source channel
2. Modify configuration for target environment
3. Create new channel with updated settings
4. Verify channel creation and test data flow

# Maintenance Window
1. Check current hub status
2. Freeze hub for maintenance
3. Perform maintenance tasks
4. Unfreeze hub and verify status

# Data Refresh Pipeline
1. Start refresh job
2. Monitor progress with polling
3. Handle completion or errors
4. Generate summary report

For detailed usage instructions, see the README.md file.
"""

import requests
import json
from typing import Dict, List, Tuple
import urllib3
import time

# ONLY USE FOR LOCAL TESTING
urllib3.disable_warnings()

#configuration file for key,secret,params,etc.
r = '/config.json'
with open(r, "r") as i:
    l = i.read()
    y = json.loads(l)
username = y['hvr']['username']
password = y['hvr']['password']
base_url = y['hvr']['base_url']


#o = '/Documents/code/hvr/out.txt'


class HVRAPIClient:
    """
    A client for interacting with the HVR APIs.
    """
    def __init__(self, base_url: str, username: str, password: str, access_token:str):
        # Remove trailing slash to prevent double slashes in URLs
        self.base_url = base_url.rstrip('/')
        self.username = username
        self.password = password
        self.access_token = access_token
        # Capture details of the most recent HTTP error to inform workflow decisions
        self.last_error_info: Dict[str, str] | None = None

    def get_access_token(self) -> str:
        """
        Obtain an access token for authentication.
        """
        url = f"{self.base_url}/auth/v1/password"
        payload = {
            "username": self.username,
            "password": self.password,
            "bearer": "token"
        }
        response = requests.post(url, json=payload)
        response.raise_for_status()
        print(response)
        return response.json()["access_token"]

    def get_channel_details(self, channel_name: str) -> Dict:
        """
        Fetch details of a specific channel.
        """
        url = f"{self.base_url}/api/v6.1.0.36/hubs/hvrhub/definition/channels/{channel_name}"
        params = {
            "fetch": ["cols", "tables", "channel_actions", "loc_groups"]
        }
        response = requests.get(url,params=params, headers=self._get_auth_headers(),verify=False)
        response.raise_for_status()
        return response.json()

    def create_channel(self, channel_config: Dict) -> Dict:
        """
        Create a new channel.
        """
        url = f"{self.base_url}/api/v6.1.0.36/hubs/hvrhub/definition/channels"
        print(f"Creating channel with URL: {url}")
        print(f"Channel config: {json.dumps(channel_config, indent=2)}")
        response = requests.post(url, json=channel_config, headers=self._get_auth_headers(), verify=False)
        response.raise_for_status()
        
        # Handle empty response (common for successful POST operations)
        if response.text.strip():
            return response.json()
        else:
            return {"status": "success", "message": "Channel created successfully"}

    def update_channel(self, channel_name: str, channel_config: Dict) -> Dict:
        """
        Update an existing channel.
        """
        url = f"{self.base_url}/api/v6.1.0.36/hubs/hvrhub/definition/channels/{channel_name}"
        response = requests.put(url, json=channel_config, headers=self._get_auth_headers(), verify=False)
        response.raise_for_status()
        return response.json()

    def delete_channel(self, channel_name: str) -> None:
        """
        Delete an existing channel.
        """
        url = f"{self.base_url}/api/v6.1.0.36/hubs/hvrhub/definition/channels/{channel_name}"
        response = requests.delete(url, headers=self._get_auth_headers(), verify=False)
        response.raise_for_status()

    def get_all_channel_names(self) -> List[str]:
        """
        Fetch a list of all existing channel names.
        """
        url = f"{self.base_url}/api/v6.1.0.36/hubs/hvrhub/definition/channels"
        params = {
            "fetch": ["cols", "tables", "channel_actions", "loc_groups"]
        }
        response = requests.get(url, params=params, headers=self._get_auth_headers(),verify=False)
        #print(response.json())
        #with open(o,"w") as out:
        #total_pages = d.get(['paginationData']['numberOfPages'], 1)
        #print(total_pages)
        #    out.write(str(response.json()))

        response.raise_for_status()
        data = response.json()
        if isinstance(data, dict):
            return list(data.keys())
        elif isinstance(data, list):
            return [channel.get("table_group", "unknown") for channel in data if isinstance(channel, dict)]
        return []
    
    def get_hub(self) -> List[str]:
        """
        Fetch hub definition.
        """
        url = f"{self.base_url}/api/v6.1.0.36/hubs/hvrhub/definition"
        response = requests.get(url, headers=self._get_auth_headers(),verify=False)
        response.raise_for_status()
        return response.json()

    def get_locations(self) -> List[str]:
        """
        Fetch a list of all available locations in the hub.
        """
        url = f"{self.base_url}/api/v6.1.0.36/hubs/hvrhub/definition/locs"
        try:
            response = requests.get(url, headers=self._get_auth_headers(), verify=False)
            response.raise_for_status()
            data = response.json()
            if isinstance(data, dict):
                return list(data.keys())
            elif isinstance(data, list):
                return [d.get("loc_name", str(d)) if isinstance(d, dict) else str(d) for d in data]
            return []
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                print(f"Locations endpoint not found: {url}")
                return []
            else:
                raise e

    def get_hub_status(self, hub: str) -> List[str]:
        """
        Fetch a list of hubproperties.
        """
        url = f"{self.base_url}/api/v6.1.0.36/hubs/{hub}/props"

        params = {
            "fetch": ["Hub_Id","Description", "Hub_State", "Hub_Server_URL", "Hub_Server_Platform", "Hub_Server_HVR_Version","Hub_Server_OS_Fingerprint","Creator","Created"]
        }
        response = requests.get(url, params=params,headers=self._get_auth_headers(),verify=False)
        response.raise_for_status()
        return response.json()
    
    def post_hub_update(self, hub: str) -> List[str]:
        """
        Change a list of hubproperties.
        """
        url = f"{self.base_url}/api/v6.1.0.36/hubs/{hub}/props_modify"

        payload = {
            "hub_prop_args": [
                {
                    "key": ["Hub_State"],  # First element is the type 'prop', second is the identifier
                    "value": "FROZEN"
                }
            ]
        }

        try:
            response = requests.post(url, json=payload, headers=self._get_auth_headers(), verify=False)
            response.raise_for_status()  # This will raise an error for 4xx and 5xx responses
            return response
        except requests.exceptions.HTTPError as err:
            print(f"HTTP error occurred: {err}")  # Print the error
            print("Response content:", response.content)  # Print the response content for debugging
            return []
        
    def post_hub_startv2(self, hub: str) -> List[str]:
        """
        Change a list of hubproperties.
        """
        url = f"{self.base_url}/api/v6.1.0.36/hubs/{hub}/props_modify"

        payload = {
            "hub_prop_args": [
                {
                    "key": ["Hub_State"],  # First element is the type 'prop', second is the identifier
                    "value": "LIVE"
                }
            ]
        }

        try:
            response = requests.post(url, json=payload, headers=self._get_auth_headers(), verify=False)
            response.raise_for_status()  # This will raise an error for 4xx and 5xx responses
            return response
        except requests.exceptions.HTTPError as err:
            print(f"HTTP error occurred: {err}")  # Print the error
            print("Response content:", response.content)  # Print the response content for debugging
            return []


    def post_hub_stop(self, hub: str) -> List[str]:
        """
        Stop that hub!
        """

        url = f"{self.base_url}/api/v6.1.0.36/{hub}/stop"

        response = requests.post(url,headers=self._get_auth_headers(),verify=False)
        response.raise_for_status()
        return response.json()
    
    
    def post_hub_start(self, hub: str) -> List[str]:
        """
        Start that hub!
        """
        url = f"{self.base_url}/api/v6.1.0.36/{hub}/restart"

        response = requests.post(url,headers=self._get_auth_headers(),verify=False)
        response.raise_for_status()
        return response.json()
    

    def manage_hub_state(self, hub: str, hub_id: str) -> str:
        """
        Manages the state of a hub:
        - Unfreezes the hub if it is frozen.
        - Freezes the hub if it is live and matches the target hub_id.
        - Reports if no action is needed.

        Args:
            client (Any): API client object with hub management methods.
            hub (str): Name of the hub.
            hub_id (str): ID of the hub to manage.

        Returns:
            str: Description of the action taken.
        """
        print(f"Checking status of hub: {hub}")
        hub_status = self.get_hub_status(hub=hub)
        print(f"Current hub status:\n{json.dumps(hub_status, indent=2)}")

        hub_state = hub_status.get('Hub_State')
        current_hub_id = hub_status.get('Hub_Id')
        
        if hub_state == 'FROZEN':
            print(f"Hub is frozen. Unfreezing hub: {hub}")
            self.post_hub_startv2(hub=hub)
            action_taken = "Hub unfrozen (started)"
        elif hub_state == 'LIVE' and current_hub_id == hub_id:
            print(f"Hub is live and matches target ID. Freezing hub: {hub}")
            self.post_hub_update(hub=hub)
            action_taken = "Hub frozen"
        else:
            print("Hub state doesn't match conditions. No action taken.")
            return "Not Mapped"

        time.sleep(2)  # Allow state change to take effect
        
        updated_status = self.get_hub_status(hub=hub)
        print(f"Updated hub status:\n{json.dumps(updated_status, indent=2)}")
        
        return action_taken


    def start_refresh_job(self, hub: str, channel: str, source_loc: str = 'oracle', target_loc: str = 'eli_snow') -> Dict:
        """
        Start a refresh job for a specific channel.
        
        Args:
            hub (str): The hub name
            channel (str): The channel name
            source_loc (str): Source location name
            target_loc (str): Target location name
        
        Returns:
            Dict: Response containing posted event ID and job details
        """
        url = f"{self.base_url}/api/v6.1.0.36/hubs/{hub}/channels/{channel}/refresh"
        
        payload = {
            'source_loc': source_loc,
            'target_loc': target_loc,
            'granularity': 'bulk',
            'start_immediate': True,
           
           # 'context_variables': { 
           #     'name': 'value'
           # }
        }
        
        try:
            response = requests.post(url, json=payload, headers=self._get_auth_headers(), verify=False)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Error starting refresh job: {e}")
            return None


    def poll_refresh_job(self, hub: str, channel: str, event_id: str, 
                         location: str = 'test_repo', 
                         timeout: int = 1800,  # 30 minutes
                         poll_interval: int = 30) -> Dict:
        """
        Poll the refresh job status using the /events endpoint until completion or timeout.
        
        Args:
            hub (str): The hub name
            channel (str): The channel name
            event_id (str): The event ID from job start
            location (str): Source location for filtering
            timeout (int): Maximum time to wait in seconds (default 30 minutes)
            poll_interval (int): Time between status checks in seconds
        
        Returns:
            Dict: Final job status or None if timeout occurs
        """
        start_time = time.time()
        event_id = event_id.replace('%', ':')
        print(event_id)
        
        print(f"🕒 Starting refresh job polling for channel {channel}")
        print(f"   Event ID: {event_id}")
        print(f"   Timeout: {timeout} seconds")
        print(f"   Poll Interval: {poll_interval} seconds")
        
        while time.time() - start_time < timeout:
            try:
                # Prepare query parameters
                event_id = event_id.replace('%', ':')
                print(event_id)

                params = {
                    'channel': channel,
                    'ev_id': event_id,
                    'loc': location,
                    'current_only': True,
                    'fetch_results': True
                }
                
                #1. Construct URL for events endpoint
                #url = f"{self.base_url}/api/v6.1.0.36/hubs/{hub}/events"

                #2.
                url = f"{self.base_url}/api/v6.1.0.36/hubs/{hub}/events?channel={channel}&ev_id={event_id}"
        
                response = requests.get(url, headers=self._get_auth_headers(), verify=False)
                
                # # 1. Send GET request
                # response = requests.get(url, 
                #                          params=params, 
                #                          headers=self._get_auth_headers(), 
                #                          verify=False)
                

                response.raise_for_status()
                
                # Parse events
                events = response.json()
                
                # There should only be one event due to current_only=True
                if not events:
                    print("❓ No events found. Unexpected scenario.")
                    return None
                
                # Get the first (and only) event
                event = list(events.values())[0]
                event_state = event.get('state', 'UNKNOWN')
                
                # Print detailed status
                print(f"📡 Current Event State: {event_state}")
                
                # Handle different states
                if event_state == 'ACTIVE':
                    print("⏳ Job is still active. Waiting...")
                    time.sleep(poll_interval)
                    continue
                
                elif event_state == 'DONE':
                    print("✅ Refresh job completed successfully!")
                    return self._summarize_refresh_event(event)
                
                elif event_state in ['FAILED', 'CANCELED']:
                    print(f"❌ Refresh job {event_state.lower()}!")
                    return self._summarize_refresh_event(event)
                
                else:
                    print(f"⚠️ Unexpected event state: {event_state}")
                    return None
            
            except requests.exceptions.RequestException as e:
                print(f"🔥 Error polling job status: {e}")
                time.sleep(poll_interval)
        
        print("⏰ Refresh job polling timed out.")
        return None

    def _summarize_refresh_event(self, event: Dict) -> Dict:
        """
        Create a comprehensive summary of the refresh event.
        
        Args:
            event (Dict): The event dictionary from the /events endpoint
        
        Returns:
            Dict: Summarized event information
        """
        summary = {
            'state': event.get('state', 'UNKNOWN'),
            'type': event.get('type', 'N/A'),
            'channel': event.get('channel', 'N/A'),
            'location': event.get('loc', 'N/A'),
            'job_id': event.get('job', 'N/A'),
            'start_time': event.get('start_tstamp', 'N/A'),
            'finish_time': event.get('finish_tstamp', 'N/A'),
            'description': event.get('description', 'No description'),
            'results': []
        }
        
        # Add table-level results if available
        if event.get('results'):
            for result in event['results']:
                summary['results'].append({
                    'table': result.get('table', 'N/A'),
                    'result': result.get('result', 'N/A'),
                    'value': result.get('value', 'N/A')
                })
        
        # Pretty print the summary
        print("\n📊 Refresh Job Summary:")
        print(json.dumps(summary, indent=2))
        
        return summary

    def execute_comprehensive_refresh(self, hub: str, channel: str, location: str = 'test_repo') -> Dict:
        """
        Comprehensive method to start refresh, poll, and retrieve results.
        
        Args:
            hub (str): The hub name
            channel (str): The channel name
            location (str): Source location for the refresh
        
        Returns:
            Dict: Comprehensive refresh results
        """
        # Start the refresh job
        start_response = self.start_refresh_job(hub, channel)
        
        if not start_response:
            print("❌ Failed to start refresh job.")
            return None
        
        event_id = start_response.get('posted_ev_id')
        event_id = event_id.replace('%', ':')
        job_id = start_response.get('job')
        
        print(f"🚀 Refresh job started:")
        print(f"   Event ID: {event_id}")
        print(f"   Job ID: {job_id}")
        
        # Poll the job and retrieve results
        job_results = self.poll_refresh_job(hub, channel, event_id, location)
        
        if job_results:
            print("✨ Refresh job completed successfully.")
            return job_results
        else:
            print("❌ Refresh job failed or timed out.")
            return None

    def capture_source_table_definition(self, hub: str, source_location: str, table_name: str) -> Dict:
        """
        Capture the latest source table DDL definition.
        
        Args:
            hub (str): The hub name
            source_location (str): Source location name
            table_name (str): Table name to capture
        
        Returns:
            Dict: Table definition JSON from source
        """
        params = {"direction": "source"}

        try:
            print(f"🔍 Capturing source table definition for {table_name} from {source_location}")

            # Preflight: list tables at source location to validate location/table visibility
            list_urls = [
                # Prefer 'locations' over legacy 'locs'
                f"{self.base_url}/api/v6.1.0.36/hubs/{hub}/locations/{source_location}/tables",
                # Legacy/alternate paths
                f"{self.base_url}/api/v6.1.0.36/hubs/{hub}/locs/{source_location}/tables",
                f"{self.base_url}/api/v6.1.0.36/hubs/{hub}/definition/locs/{source_location}/tables",
            ]
            preflight_ok = False
            for lu in list_urls:
                try:
                    lr = requests.get(lu, headers=self._get_auth_headers(), verify=False)
                    if lr.status_code == 200 and isinstance(lr.json(), dict):
                        if table_name in lr.json():
                            preflight_ok = True
                            break
                except Exception:
                    pass

            if not preflight_ok:
                print("⚠️ Preflight: table not visible via locs tables listing. Proceeding to redefine attempts anyway.")

            # Ordered attempts: prefer GET 'locations' path per spec, then fall back to other variants
            attempts = [
                # Spec-compliant endpoints
                ("GET",  f"{self.base_url}/api/v6.1.0.36/hubs/{hub}/locations/{source_location}/tables/{table_name}/redefine_table", False),
                # Sometimes servers accept POST for redefine; try both
                ("POST", f"{self.base_url}/api/v6.1.0.36/hubs/{hub}/locations/{source_location}/tables/{table_name}/redefine_table", True),
                # Legacy/alternate paths
                ("GET",  f"{self.base_url}/api/v6.1.0.36/hubs/{hub}/locs/{source_location}/tables/{table_name}/redefine_table", False),
                ("GET",  f"{self.base_url}/api/v6.1.0.36/hubs/{hub}/definition/locs/{source_location}/tables/{table_name}/redefine_table", False),
                ("POST", f"{self.base_url}/api/v6.1.0.36/hubs/{hub}/definition/locs/{source_location}/tables/{table_name}/redefine_table", True),
            ]

            last_url = None
            response = None
            for method, url_try, send_json in attempts:
                last_url = url_try
                try:
                    if method == "POST":
                        if send_json:
                            response = requests.post(url_try, json=params, headers=self._get_auth_headers(), verify=False)
                        else:
                            response = requests.post(url_try, params=params, headers=self._get_auth_headers(), verify=False)
                    else:
                        response = requests.get(url_try, params=params, headers=self._get_auth_headers(), verify=False)
                except Exception:
                    continue
                if response is not None and response.status_code < 400:
                    break

            response.raise_for_status()

            table_definition = response.json()
            print(f"✅ Successfully captured table definition for {table_name}")
            print(f"   Columns: {len(table_definition.get('cols', []))}")
            print(f"   Keys: {len(table_definition.get('keys', []))}")
            return table_definition

        except requests.exceptions.RequestException as e:
            print(f"❌ Error capturing table definition: {e}")
            try:
                # Print which URL we last attempted for easier troubleshooting
                print(f"   Last attempted URL: {last_url}")
            except Exception:
                pass
            return None

    def import_table_definition(self, hub: str, channel: str, table_name: str, table_definition: Dict) -> Dict:
        """
        Import table definition JSON back into the hub definition.
        
        Args:
            hub (str): The hub name
            channel (str): Channel name
            table_name (str): Table name
            table_definition (Dict): Table definition JSON from capture_source_table_definition
        
        Returns:
            Dict: Import response
        """
        # Use the documented endpoint and payload shape for importing definition changes
        url_primary = f"{self.base_url}/api/v6.1.0.36/hubs/{hub}/definition/import"

        # Table definition should be nested under changes -> replace_table
        # using the documented shapes. We'll default to replace_table, falling back to add_table
        # if the replace variant fails with 404/400.
        payload_replace = {
            "changes": [
                {
                    "replace_table": {
                        "channel": channel,
                        "table": table_name,
                        **({k: v for k, v in table_definition.items() if k in ("base_name", "table_group", "cols")})
                    }
                }
            ]
        }
        payload_add = {
            "changes": [
                {
                    "add_table": {
                        "channel": channel,
                        "table": table_name,
                        **({k: v for k, v in table_definition.items() if k in ("base_name", "table_group", "cols")})
                    }
                }
            ]
        }
        
        try:
            print(f"📥 Importing table definition for {table_name} into channel {channel}")
            print(f"🔗 Import URL: {url_primary}")
            print(f"📦 Payload (replace): {json.dumps(payload_replace, indent=2)}")

            response = requests.post(url_primary, json=payload_replace, headers=self._get_auth_headers(), verify=False)
            print(f"📊 Response status (replace): {response.status_code}")

            if response.status_code in (400, 404):
                # Try latest with replace
                url_latest = f"{self.base_url}/api/latest/hubs/{hub}/definition/import"
                print(f"↩️ Fallback to latest (replace): {url_latest}")
                response = requests.post(url_latest, json=payload_replace, headers=self._get_auth_headers(), verify=False)
                print(f"📊 Response status (latest replace): {response.status_code}")

            if response.status_code in (400, 404):
                # Try add_table variant
                print(f"🆕 Trying add_table payload")
                print(f"📦 Payload (add): {json.dumps(payload_add, indent=2)}")
                response = requests.post(url_primary, json=payload_add, headers=self._get_auth_headers(), verify=False)
                print(f"📊 Response status (add primary): {response.status_code}")

            if response.status_code in (400, 404):
                url_latest = f"{self.base_url}/api/latest/hubs/{hub}/definition/import"
                print(f"↩️ Fallback to latest (add): {url_latest}")
                response = requests.post(url_latest, json=payload_add, headers=self._get_auth_headers(), verify=False)
                print(f"📊 Response status (latest add): {response.status_code}")

            response.raise_for_status()
            
            print(f"✅ Successfully imported table definition for {table_name}")
            return response.json() if response.text.strip() else {"status": "success", "message": "Definition imported"}
            
        except requests.exceptions.RequestException as e:
            print(f"❌ Error importing table definition: {e}")
            return None

    def adapt_check_tables(self, hub: str, channel: str, location: str, table_names: List[str], 
                           check_layout: bool = True, localize_datatypes: bool = True,
                           fetch_extra: List[str] = None, mapspec: Dict = None,
                           tables_not_matched_by_mapspec: bool = None) -> Dict:
        """
        Compare hub table definition(s) against the layout in a source/target DB.
        Uses POST /api/v6.1.0.36/hubs/{hub}/channels/{channel}/locs/{loc}/adapt/check
        """
        url = f"{self.base_url}/api/v6.1.0.36/hubs/{hub}/channels/{channel}/locs/{location}/adapt/check"
        base_tables = {
            "tables": table_names,
            "check_layout": check_layout,
            "localize_datatypes": localize_datatypes
        }
        attempts: List[Tuple[Dict[str, Any], str]] = []
        # Base
        attempts.append(({"tables_in_channel": base_tables}, "base"))
        # With tables_not_matched_by_mapspec
        tic_with_notmatched = dict(base_tables)
        tic_with_notmatched["tables_not_matched_by_mapspec"] = True
        attempts.append(({"tables_in_channel": tic_with_notmatched}, "with_tables_not_matched_by_mapspec"))
        # With mapspec if provided
        if mapspec:
            attempts.append(({"tables_in_channel": base_tables, "mapspec": mapspec}, "with_mapspec"))
        # Toggle localize_datatypes False
        tic_localize_false = dict(base_tables)
        tic_localize_false["localize_datatypes"] = False
        attempts.append(({"tables_in_channel": tic_localize_false}, "localize_false"))
        # fetch_extra (db_stats) to enrich
        if fetch_extra:
            attempts.append(({"tables_in_channel": base_tables, "fetch_extra": fetch_extra}, "with_fetch_extra"))

        last_error_text = None
        for pl, label in attempts:
            try:
                print(f"🔎 Adapt CHECK → {url} ({label})")
                print(f"📋 Payload: {json.dumps(pl, indent=2)}")
                response = requests.post(url, json=pl, headers=self._get_auth_headers(), verify=False)
                print(f"📊 Response status: {response.status_code}")
                if response.status_code >= 400:
                    try:
                        last_error_text = response.text
                        print(f"🧾 Response body: {last_error_text[:1000]}")
                        # Stash error context for higher-level handling
                        self.last_error_info = {
                            "stage": "adapt_check",
                            "hub": hub,
                            "channel": channel,
                            "location": location,
                            "label": label,
                            "status_code": str(response.status_code),
                            "url": url,
                            "body": last_error_text[:2000]
                        }
                    except Exception:
                        pass
                response.raise_for_status()
                data = response.json()
                print(f"📄 Adapt CHECK result (keys): {list(data.keys())}")
                return data
            except requests.exceptions.RequestException as e:
                print(f"❌ Adapt CHECK attempt '{label}' failed: {e}")
                continue
        return None

    def adapt_apply_tables(self, hub: str, channel: str, location: str, table_names: List[str], 
                           add_tables: bool = True, check_layout: bool = True,
                           localize_datatypes: bool = True, ignore_diff: List[str] = None,
                           mapspec: Dict = None) -> Dict:
        """
        Add/replace table definition info from a DB into the hub definition for given tables.
        Uses POST /api/v6.1.0.36/hubs/{hub}/channels/{channel}/locs/{loc}/adapt/apply
        """
        url = f"{self.base_url}/api/v6.1.0.36/hubs/{hub}/channels/{channel}/locs/{location}/adapt/apply"
        tic_base: Dict[str, Any] = {
            "tables": table_names,
            "check_layout": check_layout,
            "localize_datatypes": localize_datatypes
        }
        if ignore_diff:
            tic_base["ignore_diff"] = ignore_diff

        base_payload: Dict[str, Any] = {
            "tables_in_channel": tic_base,
            "add_tables": add_tables
        }
        attempts: List[Tuple[Dict[str, Any], str]] = []
        attempts.append((base_payload, "base"))
        # Add mapspec if provided
        if mapspec:
            p = dict(base_payload)
            p["mapspec"] = mapspec
            attempts.append((p, "with_mapspec"))
        # Toggle localize_datatypes False
        tic_localize_false = dict(tic_base)
        tic_localize_false["localize_datatypes"] = False
        attempts.append(({"tables_in_channel": tic_localize_false, "add_tables": add_tables}, "localize_false"))
        # Add broad ignore_diff
        broad_ignore = [
            "encoding_changed", "nulls_added", "nulls_removed",
            "data_type_changed", "data_type_family_changed", "col_range_bigger", "col_range_smaller"
        ]
        tic_with_ignore = dict(tic_base)
        existing_ignore = tic_with_ignore.get("ignore_diff", [])
        tic_with_ignore["ignore_diff"] = sorted(list(set(existing_ignore + broad_ignore)))
        attempts.append(({"tables_in_channel": tic_with_ignore, "add_tables": add_tables}, "with_ignore_diff"))

        last_error_text = None
        for pl, label in attempts:
            try:
                print(f"📝 Adapt APPLY → {url} ({label})")
                print(f"📦 Payload: {json.dumps(pl, indent=2)}")
                response = requests.post(url, json=pl, headers=self._get_auth_headers(), verify=False)
                print(f"📊 Response status: {response.status_code}")
                if response.status_code >= 400:
                    try:
                        last_error_text = response.text
                        print(f"🧾 Response body: {last_error_text[:1000]}")
                        # Stash error context for higher-level handling
                        self.last_error_info = {
                            "stage": "adapt_apply",
                            "hub": hub,
                            "channel": channel,
                            "location": location,
                            "label": label,
                            "status_code": str(response.status_code),
                            "url": url,
                            "body": last_error_text[:2000]
                        }
                    except Exception:
                        pass
                response.raise_for_status()
                data = response.json()
                print(f"✅ Adapt APPLY result (keys): {list(data.keys())}")
                return data
            except requests.exceptions.RequestException as e:
                print(f"❌ Adapt APPLY attempt '{label}' failed: {e}")
                continue
        return None

    def test_target_connectivity_for_table(self, hub: str, channel: str, location: str, table_name: str) -> bool:
        """
        Lightweight connectivity preflight for a target location using adapt/check.
        Returns True if the server can reach the target DB for the given table context.
        """
        print(f"🔌 Preflight connectivity check for location '{location}' using table '{table_name}'")
        res = self.adapt_check_tables(
            hub=hub,
            channel=channel,
            location=location,
            table_names=[table_name],
            check_layout=True,
            localize_datatypes=True
        )
        if res is not None:
            return True
        # Inspect last error for common connectivity patterns to surface clearer guidance
        err = (self.last_error_info or {}).get("body", "")
        if (
            "Unable to connect" in err
            or "Couldn't resolve host name" in err
            or "timeout" in err.lower()
        ):
            print("⚠️ Target connectivity issue detected. Verify network/DNS/credentials for the location.")
        return False

    def create_alter_target_tables(self, hub: str, channel: str, table_names: List[str], 
                                 target_locations: List[str], fill: bool = False) -> Dict:
        """
        Create or ALTER tables on target without copying data (metadata-only).
        
        Args:
            hub (str): The hub name
            channel (str): Channel name
            table_names (List[str]): List of table names to create/alter
            target_locations (List[str]): List of target locations
            fill (bool): Whether to refresh data (False for metadata-only)
        
        Returns:
            Dict: Create/Alter response
        """
        payload_base = {
            "channel": channel,
            "tables": table_names,
            "locations": target_locations,
            "fill": fill
        }
        payload_with_type = {"type": "CreateAlterTargetTables", **payload_base}
        
        endpoint_attempts = [
            (f"{self.base_url}/api/v6.1.0.36/hubs/{hub}/events/create-alter-target-tables", payload_base),
            (f"{self.base_url}/api/v6.1.0.36/hubs/{hub}/events/create_alter_target_tables", payload_base),
            (f"{self.base_url}/api/v6.1.0.36/hubs/{hub}/events", payload_with_type),
            (f"{self.base_url}/api/latest/hubs/{hub}/events/create-alter-target-tables", payload_base),
            (f"{self.base_url}/api/latest/hubs/{hub}/events/create_alter_target_tables", payload_base),
            (f"{self.base_url}/api/latest/hubs/{hub}/events", payload_with_type),
            (f"{self.base_url}/api/v6.1.0.36/hubs/{hub}/channels/{channel}/events", payload_with_type),
            (f"{self.base_url}/api/latest/hubs/{hub}/channels/{channel}/events", payload_with_type),
        ]
        
        try:
            operation_type = "metadata-only" if not fill else "with data refresh"
            print(f"🔧 Creating/Altering tables {table_names} on targets {target_locations} ({operation_type})")
            
            last_error_text = None
            for url_try, payload_try in endpoint_attempts:
                print(f"➡️  POST {url_try}")
                print(f"📦 Payload: {json.dumps(payload_try, indent=2)}")
                response = requests.post(url_try, json=payload_try, headers=self._get_auth_headers(), verify=False)
                print(f"📊 Response status: {response.status_code}")
                if response.status_code < 400:
                    break
                try:
                    last_error_text = response.text
                    print(f"🧾 Response body: {last_error_text[:500]}")
                except Exception:
                    pass

            response.raise_for_status()
            
            result = response.json() if response.text.strip() else {"status": "success", "message": "Tables created/altered"}
            print(f"✅ Successfully initiated create/alter operation for {len(table_names)} tables")
            
            return result
            
        except requests.exceptions.RequestException as e:
            print(f"❌ Error creating/altering target tables: {e}")
            try:
                if 'response' in dir(e) and e.response is not None:
                    print(f"🧾 Error response body: {e.response.text[:1000]}")
            except Exception:
                pass
            return None

    def verify_create_alter_jobs(self, hub: str, job_type: str = "CreateAlterTargetTables") -> Dict:
        """
        Verify the status of create/alter target table jobs.
        
        Args:
            hub (str): The hub name
            job_type (str): Type of job to check
        
        Returns:
            Dict: Job status information
        """
        url = f"{self.base_url}/api/v6.1.0.36/hubs/{hub}/jobs"
        params = {"type": job_type}
        
        try:
            print(f"🔍 Checking status of {job_type} jobs")
            response = requests.get(url, params=params, headers=self._get_auth_headers(), verify=False)
            if response.status_code == 400:
                # Try alternative filter param name
                params_alt = {"job_type": job_type}
                response = requests.get(url, params=params_alt, headers=self._get_auth_headers(), verify=False)
            response.raise_for_status()
            
            jobs = response.json()
            print(f"📊 Found {len(jobs)} {job_type} jobs")
            
            # Print job details
            for job_id, job_info in jobs.items():
                state = job_info.get('state', 'UNKNOWN')
                description = job_info.get('description', 'No description')
                print(f"   Job {job_id}: {state} - {description}")
            
            return jobs
            
        except requests.exceptions.RequestException as e:
            print(f"❌ Error checking job status: {e}")
            return None

    def get_table_definition(self, hub: str, channel: str, table_name: str) -> Dict:
        """
        Get the current table definition stored in the hub for a specific channel.
        
        Args:
            hub (str): The hub name
            channel (str): The channel name
            table_name (str): Table name
        
        Returns:
            Dict: Current table definition with columns and metadata
        """
        url = f"{self.base_url}/api/v6.1.0.36/hubs/{hub}/definition/channels/{channel}/tables"
        
        # Query parameters as per API documentation
        params = {
            'fetch': ['cols'],  # Fetch column information
            'table': [table_name]  # Specific table to fetch
        }
        
        try:
            print(f"🔍 Making request to: {url}")
            print(f"📋 Query params: {params}")
            
            response = requests.get(url, params=params, headers=self._get_auth_headers(), verify=False)
            print(f"📊 Response status: {response.status_code}")
            
            response.raise_for_status()
            
            # Extract the table definition from the response
            data = response.json()
            print(f"📄 Raw API response: {json.dumps(data, indent=2)}")
            
            if table_name in data:
                table_def = data[table_name]
                print(f"✅ Found table definition for '{table_name}'")
                return table_def
            else:
                print(f"❌ Table '{table_name}' not found in channel '{channel}'")
                print(f"Available tables: {list(data.keys())}")
                return {}
                
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                print(f"Channel '{channel}' not found in hub '{hub}'")
                return {}
            else:
                print(f"Error fetching table definition: {e}")
                raise e

    def execute_metadata_sync_workflow(self, hub: str, channel: str, table_name: str, 
                                      source_location: str, target_locations: List[str]) -> Dict:
        """
        Execute the complete metadata-only sync workflow:
        1. Capture source table definition
        2. Import definition into hub
        3. Create/Alter target tables (metadata-only)
        4. Verify results
        
        Args:
            hub (str): The hub name
            channel (str): Channel name
            table_name (str): Table name to sync
            source_location (str): Source location
            target_locations (List[str]): Target locations
        
        Returns:
            Dict: Complete workflow results
        """
        print(f"🚀 Starting metadata-only sync workflow for table {table_name}")
        print(f"   Channel: {channel}")
        print(f"   Source: {source_location}")
        print(f"   Targets: {target_locations}")
        print("=" * 60)
        
        workflow_results = {
            "table_name": table_name,
            "channel": channel,
            "source_location": source_location,
            "target_locations": target_locations,
            "steps": {}
        }
        
        # Step 1: Capture/check from source DB and update hub definition via adapt/apply
        print("\n📋 STEP 1: Capture/Check from source and UPDATE hub definition (adapt/apply)")
        print(f"   Context → hub={hub}, channel={channel}, source_location={source_location}, table={table_name}")
        # First, run adapt/check to see diffs vs source
        check_result = self.adapt_check_tables(hub, channel, source_location, [table_name])
        workflow_results["steps"]["check_source"] = {"status": "success" if check_result else "failed", "result": check_result}
        if check_result is None:
            workflow_results["status"] = "failed"
            workflow_results["error"] = "Adapt CHECK failed at source"
            return workflow_results
        # Then apply source layout into hub definition
        apply_result = self.adapt_apply_tables(hub, channel, source_location, [table_name], add_tables=True)
        workflow_results["steps"]["apply_from_source"] = {"status": "success" if apply_result else "failed", "result": apply_result}
        if apply_result is None:
            workflow_results["status"] = "failed"
            workflow_results["error"] = "Adapt APPLY failed at source"
            return workflow_results
        
        # Step 2: Validate hub definition reflects the source changes (get_table_definition)
        print("\n📥 STEP 2: Validating hub definition reflects source schema")
        before_def = self.get_table_definition(hub, channel, table_name)
        workflow_results["steps"]["hub_definition_after_apply"] = {"status": "success", "definition": before_def}
        
        # Step 3: Apply schema changes to target databases via adapt/check + adapt/apply
        print("\n🔧 STEP 3: Applying schema changes to target databases (adapt/apply)")
        print(f"   Context → hub={hub}, channel={channel}, table={table_name}, targets={target_locations}")
        targets_apply: Dict[str, Any] = {}
        # Preflight: ensure each target location is reachable before attempting schema operations
        for tgt in target_locations:
            ok = self.test_target_connectivity_for_table(hub, channel, tgt, table_name)
            if not ok:
                # Provide actionable guidance if the known connectivity error signatures are present
                guidance = (
                    "Target location is not reachable from HVR hub server. "
                    "Check network egress, DNS, and location credentials (host, database, warehouse, role)."
                )
                last_err = (self.last_error_info or {}).get("body", "")
                print(f"❌ Connectivity preflight failed for '{tgt}'.")
                if last_err:
                    print(f"   Details: {last_err[:500]}")
                workflow_results["status"] = "failed"
                workflow_results["error"] = f"Connectivity to target '{tgt}' failed"
                workflow_results["guidance"] = guidance
                return workflow_results
        # Try to derive mapspec from hub definition to help matching on targets
        hub_def = self.get_table_definition(hub, channel, table_name)
        derived_mapspec: Dict[str, Any] = None
        try:
            schema_name = hub_def.get("schema") or hub_def.get("table_group")  # table_group may not be schema, but keep minimal
            base_name = hub_def.get("base_name") or table_name
            if schema_name:
                derived_mapspec = {"tables": [{"schema": schema_name, "base_name": base_name}]}
            else:
                derived_mapspec = {"tables": [{"base_name": base_name}]}
        except Exception:
            pass
        for tgt in target_locations:
            print(f"   • Target: {tgt} → adapt CHECK")
            tgt_check = self.adapt_check_tables(hub, channel, tgt, [table_name], mapspec=derived_mapspec)
            print(f"     adapt CHECK status: {'ok' if tgt_check is not None else 'failed'}")
            print(f"   • Target: {tgt} → adapt APPLY")
            tgt_apply = self.adapt_apply_tables(hub, channel, tgt, [table_name], add_tables=True, mapspec=derived_mapspec)
            print(f"     adapt APPLY status: {'ok' if tgt_apply is not None else 'failed'}")
            targets_apply[tgt] = {
                "check": {"status": "success" if tgt_check else "failed", "result": tgt_check},
                "apply": {"status": "success" if tgt_apply else "failed", "result": tgt_apply},
            }
            if tgt_apply is None:
                workflow_results["status"] = "failed"
                workflow_results["error"] = f"Adapt APPLY failed at target {tgt}"
                if self.last_error_info:
                    workflow_results["last_error"] = self.last_error_info
                workflow_results["steps"]["apply_targets"] = targets_apply
                return workflow_results
        workflow_results["steps"]["apply_targets"] = targets_apply
        
        # Step 4: Verify results
        print("\n✅ STEP 4: Verifying results")
        time.sleep(5)  # Allow time for operations to complete
        
        # Check job status (Table_Definition_Adapt events)
        job_status = self.verify_create_alter_jobs(hub, job_type="Table_Definition_Adapt")
        workflow_results["steps"]["verify_jobs"] = {"status": "success", "jobs": job_status}
        
        # Get updated table definition and also run adapt/check on targets
        print(f"   Fetching updated table definition for verification...")
        updated_definition = self.get_table_definition(hub, channel, table_name)
        try:
            print(f"   📄 Updated definition (cols count): {len((updated_definition or {}).get('cols', {}))}")
        except Exception:
            pass
        workflow_results["steps"]["verify_definition"] = {"status": "success", "definition": updated_definition}

        print("   Running adapt CHECK against target locations to ensure schema alignment...")
        targets_check: Dict[str, Any] = {}
        for tgt in target_locations:
            tgt_res = self.adapt_check_tables(hub, channel, tgt, [table_name])
            targets_check[tgt] = {"status": "success" if tgt_res else "failed", "result": tgt_res}
        workflow_results["steps"]["check_targets"] = targets_check

        # Compare pre/post definitions for changes summary
        try:
            before_cols = set((before_def or {}).get("cols", {}).keys())
            after_cols = set((updated_definition or {}).get("cols", {}).keys())
            added_cols = sorted(list(after_cols - before_cols))
            removed_cols = sorted(list(before_cols - after_cols))
            print(f"\n🧾 Change summary for {table_name}:")
            print(f"   Added columns: {added_cols}")
            print(f"   Removed columns: {removed_cols}")
            workflow_results["steps"]["verify_definition"]["summary"] = {
                "added_cols": added_cols,
                "removed_cols": removed_cols
            }
        except Exception as diff_e:
            print(f"Could not compute diff summary: {diff_e}")
        
        workflow_results["status"] = "success"
        workflow_results["message"] = "Metadata sync workflow completed successfully"
        
        print("\n🎉 METADATA SYNC WORKFLOW COMPLETED SUCCESSFULLY!")
        print(f"   Table {table_name} definition synchronized across all locations")
        print(f"   No data refresh triggered (metadata-only operation)")
        
        return workflow_results


    """Authentication Headers"""

    def _get_auth_headers(self) -> Dict[str, str]:
        """
        Generate the authentication headers.
        """
        return {
            "Authorization": f"Bearer {self.access_token}"
        }
    

    """Authentication Main"""

def test_authentication(username: str, password: str, base_url: str = base_url):
        """
        Authenticate using the HVR API and retrieve the access token.
        """
        url = f"{base_url}/auth/v1/password"
        headers = {
            "Content-Type": "application/json"
        }
        data = {
            "username": username,
            "password": password,
            "bearer": "token"
        }
        
        try:
            response = requests.post(url, json=data, headers=headers, verify=False)
            if response.status_code == 200:
                access_token = response.json().get("access_token")
                # print("Authentication successful, access token:", access_token)
                return access_token  # Return the access token
            else:
                print("Authentication failed with status code:", response.status_code)
                print("Response:", response.json())
        
        except requests.exceptions.ConnectionError as ce:
            print("Connection error occurred:", ce)

if __name__ == "__main__":
    # Get started with auth
    access_token=test_authentication(username,password)

    # Example usage
    client = HVRAPIClient(
        base_url=base_url,
        username=username,
        password=password,
        access_token=access_token
    )

    #######################################################################################################################################################################################

    #################################################
    ## Use Case: HUB Monitor, stop, start, status check in 4 steps
    #################################################

    # #1.1
    hub_stat = client.get_hub_status(hub="hvrhub")
    print('...1.1...Hub Properties Are...')
    print(json.dumps(hub_stat, indent=2))

# time.sleep(5)

# #1.2
# hub_freeze = client.post_hub_update(hub="hvrhub")
# print('...1.2...Hub FROZEN...')

# hub_stat = client.get_hub_status(hub="hvrhub")
# print('...1.2...Hub Properties Are...')
# print(json.dumps(hub_stat, indent=2))

# time.sleep(5)

# # #1.3
# hub_start = client.post_hub_startv2(hub="hvrhub")
# print('...1.3...Started the Hub...')

# # #1.4
# hub_stat = client.get_hub_status(hub="hvrhub")
# print('...1.4...Hub Properties Are...')
# print(json.dumps(hub_stat, indent=2))

#################################################
## Use Case: Check all channels
#################################################

#all_channel_names = client.get_all_channel_names()
#print('...Listing All Channels...')
#print(str(all_channel_names))

#################################################
## Use Case: Manage hub state
#hub_state = client.manage_hub_state(hub="hvrhub", hub_id="di-ldp-hub-linux-testing~P896620~H0F1C")
#################################################

#################################################
# Use Case: Refresh, poll, summarize
# refresh, poll, summarize
#results = client.execute_comprehensive_refresh(hub="hvrhub", channel="test_ch2")
#################################################


###################################################
# Use Case: Action example
#################################################
# {
#   "action": "Restrict",
#   "channel": "your_channel_name",
#   "context": "refresh",
#   "location": "your_target_location",
#   "action_params": {
#     "RefreshCondition": "{hvr_is_deleted} = 0",
#     "Context": "refresh_keep_deletes"
#   }
# }


#################################################
# Use Case: Get details of a specific channel
#################################################
# channel_details = client.get_channel_details("test_ch2")
# print(json.dumps(channel_details, indent=2))
# config=json.dumps(channel_details, indent=2)

#################################################
# # Use Case: Create a new channel
#################################################
# new_channel_config = {
#     "channel": "restapi_new",
#     #channel_details
# }
# new_channel = client.create_channel(new_channel_config)
# print(json.dumps(new_channel, indent=2))

#################################################
# # Use Case: Update an existing channel
#################################################
# updated_channel_config = {
#     # ... (updated channel configuration)
# }
# updated_channel = client.update_channel("restapi_new", updated_channel_config)
# print(json.dumps(updated_channel, indent=2))

#################################################
## Use Case: Delete an existing channel
#################################################
#  Delete an existing channel
#client.delete_channel("restapi_new")


#################################################
## Use Case: Metadata-Only Sync Workflow
## This workflow syncs table definitions without triggering data refresh
#################################################

# Example: Sync table definition for STUDENT table across environments
# This is perfect for schema evolution scenarios where you need to:
# 1. Capture the latest source table structure
# 2. Update the hub definition with new schema
# 3. Apply schema changes to target databases
# 4. Verify the changes without moving data

# Uncomment the following lines to execute the metadata sync workflow:

# Define the parameters for the metadata sync
hub_name = "hvrhub"
channel_name = "test_ch2"  # Your channel name
table_name = "hvr_event"    # Table to sync
source_location = "test_repo"  # Source location
target_locations = ["snowflake"]  # Target locations

# print("🚀 Starting Metadata-Only Sync Workflow")
# print("=" * 50)

# # Execute the complete metadata sync workflow
# metadata_results = client.execute_metadata_sync_workflow(
#     hub=hub_name,
#     channel=channel_name,
#     table_name=table_name,
#     source_location=source_location,
#     target_locations=target_locations
# )

# # Print the complete results
# print("\n📊 METADATA SYNC WORKFLOW RESULTS:")
# print(json.dumps(metadata_results, indent=2))

# # Alternative: Execute individual steps for more control
# print("\n🔧 Executing Individual Steps:")

# # Step 1: Capture source definition
# source_def = client.capture_source_table_definition(hub_name, source_location, table_name)
# if source_def:
#     print(f"✅ Captured {len(source_def.get('cols', []))} columns from source")

# # Step 2: Import into hub
# import_result = client.import_table_definition(hub_name, channel_name, table_name, source_def)
# if import_result:
#     print("✅ Definition imported into hub")

# # Step 3: Create/Alter target tables (metadata-only)
# alter_result = client.create_alter_target_tables(
#     hub_name, channel_name, [table_name], target_locations, fill=False
# )
# if alter_result:
#     print("✅ Target tables created/altered (metadata-only)")

# # Step 4: Verify results
# time.sleep(5)  # Allow operations to complete
# job_status = client.verify_create_alter_jobs(hub_name)
# updated_def = client.get_table_definition(hub_name, table_name)

# print("\n🎯 METADATA SYNC COMPLETED!")
# print("   ✓ Source table definition captured")
# print("   ✓ Hub definition updated")
# print("   ✓ Target tables synchronized")
# print("   ✓ No data refresh triggered")

# # Edge Cases and Troubleshooting:
# # - If primary key columns change, manual resync may be required
# # - Some data type changes may fail with F_JT1478 error
# # - DDL operations may pause Integrate jobs during maintenance

# # Log queries for troubleshooting:
# # SELECT * FROM hvr_event WHERE evt_code = 'DefinitionChange' AND evt_object = 'STUDENT' ORDER BY evt_time DESC;
# # SELECT * FROM hvr_event WHERE evt_code = 'CreateAlterTargetTables' AND evt_state = 'FAILED';

#######################################################################################################################################################################################


#################################################
## Use Case: Test get_table_definition
#################################################

# print("\n🔎 Testing get_table_definition")
# try:
#     print(f"Fetching table definition for hub: {hub_name}, channel: {channel_name}, table: {table_name}")
#     tbl_def = client.get_table_definition(hub_name, channel_name, table_name)
#     print(f"Raw response: {json.dumps(tbl_def, indent=2)}")
    
#     if tbl_def:
#         print("Status: OK")
#         cols = tbl_def.get('cols', {})
#         print(f"Columns type: {type(cols)}")
#         print(f"Columns count: {len(cols)}")
        
#         # Handle cols as a dictionary (per API documentation)
#         if isinstance(cols, dict):
#             col_names = list(cols.keys())
#             print(f"Column names: {col_names}")
#             sample_cols = dict(list(cols.items())[:5])  # Get first 5 columns
#             print(json.dumps({"sample_cols": sample_cols}, indent=2))
#         else:
#             print(f"Unexpected cols format: {cols}")
#     else:
#         print("Status: FAILED (None returned)")
# except Exception as e:
#     print(f"Error during get_table_definition test: {e}")
#     import traceback
#     traceback.print_exc()


# Documentation https://fivetran.com/docs/hvr6/rest-api/rest-api-reference/610/61036#tableadaptinterface
# Git https://github.com/fivetran/api_framework/blob/main/examples/HVR/hvr_check_source_def.py

# print("\n🔎 Testing adapt/check against source DB for a single table")
# try:
#     check_url = f"{base_url}/api/v6.1.0.36/hubs/{hub_name}/channels/{channel_name}/locs/{source_location}/adapt/check/{table_name}"
#     check_payload = {
#         "localize_datatypes": True,
#         "fetch_extra": ["db_stats"]
#     }
#     print(f"POST {check_url}")
#     print(f"Payload: {json.dumps(check_payload, indent=2)}")
#     check_resp = requests.post(check_url, json=check_payload, headers=client._get_auth_headers(), verify=False)
#     print(f"Status: {check_resp.status_code}")
#     check_resp.raise_for_status()
#     check_data = check_resp.json()
#     print("\n📄 Adapt/Check (single table) response:")
#     # Print a compact summary of key fields if present
#     summary = {
#         "schema": check_data.get("schema"),
#         "base_name": check_data.get("base_name"),
#         "exists_in_db": check_data.get("exists_in_db"),
#         "diff": check_data.get("diff"),
#     }
#     print(json.dumps(summary, indent=2))
#     # Print the first few columns if available
#     cols = check_data.get("cols", [])
#     if isinstance(cols, list) and cols:
#         print("\nColumns (sample up to 5):")
#         sample_cols = cols[:5]
#         print(json.dumps(sample_cols, indent=2))
#     elif isinstance(cols, dict) and cols:
#         print("\nColumns (dict keys sample up to 5):")
#         sample_cols = dict(list(cols.items())[:5])
#         print(json.dumps(sample_cols, indent=2))
#     else:
#         print("No column details returned.")
# except Exception as e:
#     print(f"Error during per-table adapt/check test: {e}")
#     import traceback
#     traceback.print_exc()
