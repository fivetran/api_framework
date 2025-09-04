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

For detailed usage instructions, see the README.md file at https://github.com/fivetran/api_framework/blob/main/examples/HVR/hvr_README.md.
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
username = y['username']
password = y['password']
base_url = y['base_url']

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
        url = f"{self.base_url}/api/vv6.1.0.36/hubs/hvrhub/definition/channels/{channel_name}"
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
        return [channel["table_group"] for channel in response.json()]
    
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
        url = f"{self.base_url}/api/v6.1.0.36/hubs/hvrhub/definition/locations"
        try:
            response = requests.get(url, headers=self._get_auth_headers(), verify=False)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                print(f"Locations endpoint not found: {url}")
                print("Will extract location information from existing channels instead.")
                return {}
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
        hub_status = client.get_hub_status(hub=hub)
        print(f"Current hub status:\n{json.dumps(hub_status, indent=2)}")

        hub_state = hub_status.get('Hub_State')
        current_hub_id = hub_status.get('Hub_Id')
        
        if hub_state == 'FROZEN':
            print(f"Hub is frozen. Unfreezing hub: {hub}")
            client.post_hub_startv2(hub=hub)
            action_taken = "Hub unfrozen (started)"
        elif hub_state == 'LIVE' and current_hub_id == hub_id:
            print(f"Hub is live and matches target ID. Freezing hub: {hub}")
            client.post_hub_update(hub=hub)
            action_taken = "Hub frozen"
        else:
            print("Hub state doesn't match conditions. No action taken.")
            return "Not Mapped"

        time.sleep(2)  # Allow state change to take effect
        
        updated_status = client.get_hub_status(hub=hub)
        print(f"Updated hub status:\n{json.dumps(updated_status, indent=2)}")
        
        return action_taken


    def start_refresh_job(self, hub: str, channel: str) -> Dict:
        """
        Start a refresh job for a specific channel.
        
        Args:
            hub (str): The hub name
            channel (str): The channel name
        
        Returns:
            Dict: Response containing posted event ID and job details
        """
        url = f"{self.base_url}/api/v6.1.0.36/hubs/{hub}/channels/{channel}/refresh"
        
        payload = {
            'source_loc': 'test_repo',
            'target_loc': 'snowflake',
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
            # Check if the request was successful
            if response.status_code == 200:
                access_token = response.json().get("access_token")
                print("Authentication successful, access token:", access_token)
                return access_token  # Return the access token
            else:
                print("Authentication failed with status code:", response.status_code)
                print("Response:", response.json())
        
        except requests.exceptions.ConnectionError as ce:
            print("Connection error occurred:", ce)

    # Get started with auth
access_token=test_authentication(username,password)


# Example usage
client = HVRAPIClient(
    base_url=base_url,
    username=username,
    password=password,
    access_token=access_token
)




#################################################
## Use Case: Migrate channel with changes 
#################################################
#1.  Check if hub exists first
try:
    hub_info = client.get_hub()
    print('...Hub exists and is accessible...')
    
    # Try to get available locations (may not be available via API)
    try:
        locations = client.get_locations()
        if locations:
            print('...Available Locations...')
            print(f"Found {len(locations)} locations:")
            for loc_name, loc_info in locations.items():
                print(f"  - {loc_name}: {loc_info.get('type', 'N/A')} ({loc_info.get('description', 'No description')})")
    except Exception as loc_e:
        print(f"Could not fetch locations via API: {loc_e}")
        print("Will extract location information from existing channels instead.")
    
except Exception as e:
    print(f"Error accessing hub: {e}")
    exit(1)

#2.  Get details of a specific channel
channel_details = client.get_channel_details("test_ch2")
print('...Channel Details Retrieved...')
print("Channel structure includes:")
print(f"- Channel name: {channel_details.get('channel', 'N/A')}")
print(f"- Tables: {len(channel_details.get('tables', {}))} tables")
print(f"- Actions: {len(channel_details.get('actions', []))} actions")
print(f"- Location groups: {len(channel_details.get('loc_groups', {}))} location groups")

# Let's examine the actions to understand the data flow
if 'actions' in channel_details:
    print("\n📋 Current Actions:")
    for i, action in enumerate(channel_details['actions']):
        print(f"  {i+1}. Type: {action.get('type', 'N/A')}")
        print(f"     Location Scope: {action.get('loc_scope', 'N/A')}")
        print(f"     Table Scope: {action.get('table_scope', 'N/A')}")

# Let's examine the location groups
if 'loc_groups' in channel_details:
    print("\n📍 Location Groups:")
    for group_name, group_info in channel_details['loc_groups'].items():
        print(f"  Group: {group_name}")
        print(f"    Members: {group_info.get('members', [])}")

#print(json.dumps(channel_details, indent=2))


# #3.  Update channel_config to include the new channel name and configure locations
param_value = "test_api_125"  # Replace with the desired channel name

channel_config = channel_details
channel_config['channel'] = param_value  # Set the new channel name

# Configure the channel with proper extract-to-import setup
print(f"\n🔧 Configuring channel '{param_value}' with extract-to-import setup...")

# Keep the original actions but update them for the new channel
if 'actions' in channel_config:
    print(f"Original actions: {len(channel_config['actions'])}")
    
    # Update actions to work with the new channel
    updated_actions = []
    
    # Create a clean extract-to-import setup
    # 1. Capture action (extract from source)
    capture_action = {
        'type': 'Capture',
        'loc_scope': 'test_repo',  # Source location
        'table_scope': '*'  # All tables
    }
    
    # 2. Integrate action (import to target)
    integrate_action = {
        'type': 'Integrate',
        'loc_scope': 'snowflake',  # Target location
        'table_scope': '*'  # All tables
    }
    
    # Add actions in the correct order: Capture first, then Integrate
    updated_actions.append(capture_action)
    updated_actions.append(integrate_action)
    
    channel_config['actions'] = updated_actions
    print(f"Updated actions: {len(channel_config['actions'])}")
    
    # Print the configured actions
    print("\n📋 Configured Actions:")
    for i, action in enumerate(channel_config['actions']):
        print(f"  {i+1}. Type: {action.get('type', 'N/A')}")
        print(f"     Location Scope: {action.get('loc_scope', 'N/A')}")
        print(f"     Table Scope: {action.get('table_scope', 'N/A')}")

# Ensure location groups are properly configured
if 'loc_groups' not in channel_config:
    channel_config['loc_groups'] = {}

# Add source and target location groups (must be uppercase)
channel_config['loc_groups']['SOURCE'] = {
    'members': ['test_repo']  # Source location
}
channel_config['loc_groups']['TARGET'] = {
    'members': ['snowflake']  # Target location
}

print(f"\n📍 Configured Location Groups:")
for group_name, group_info in channel_config['loc_groups'].items():
    print(f"  {group_name}: {group_info.get('members', [])}")

print(f"\n✅ Channel '{param_value}' configured for extract-to-import:")
print(f"   Source: test_repo → Target: snowflake")
print(f"   Actions: Capture + Integrate")
print(f"   Tables: All tables (*)")

print(json.dumps(channel_config, indent=2))

#4.  Ensure channel_config is properly formatted JSON before creating a new channel
if isinstance(channel_config, dict):
    try:
        new_channel = client.create_channel(channel_config)  # channel_config
        print('...New Channel Created from Retrieved Data...')
        print(json.dumps(new_channel, indent=2))
    except Exception as e:
        print(f"Error creating channel: {e}")
        if hasattr(e, 'response') and e.response is not None:
            print(f"Response status: {e.response.status_code}")
            print(f"Response content: {e.response.text}")
        else:
            print("No response object available")
else:
    print("Error: channel_config is not a valid JSON object.")



#################################################
##Use Case: HUB Monitor, stop, start, status check in 4 steps
#################################################

# #1.1
# hub_stat = client.get_hub_status(hub="hvrhub")
# print('...1.1...Hub Properties Are...')
# print(json.dumps(hub_stat, indent=2))

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
#hub_state = client.manage_hub_state(hub="hvrhub", hub_id="di-hub-l-test~P89~H0FC")
#################################################

#################################################
# Use Case: Refresh, poll, summarize
# refresh, poll, summarize
#results = client.execute_comprehensive_refresh(hub="hub", channel="test_ch")
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
#     "channel": "api_new",
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
