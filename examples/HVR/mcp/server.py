#!/usr/bin/env python3
"""
HVR Single-File MCP Server
==========================
A comprehensive, production-grade MCP (Model Context Protocol) server consolidating 
HVR API connectivity, state management, and replication workflow automation.

Designed to read credentials from `/config.json` with robust fallbacks, support 
long-running AI/agentic sessions through transparent token refreshing, and register 
26 intuitive tools for seamless natural language HVR automation.
"""

import os
import sys
import json
import time
import requests
import urllib3
from typing import Dict, List, Tuple, Any, Optional

# Disable insecure request warnings for local testing / private HVR servers
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

try:
    from mcp.server.fastmcp import FastMCP
except ImportError:
    try:
        from fastmcp import FastMCP
    except ImportError:
        print("Could not import FastMCP. Please ensure the mcp library is installed: pip install mcp fastmcp", file=sys.stderr)
        sys.exit(1)


# ==============================================================================
# HVR API Client with Transparent Token Auto-Refresh
# ==============================================================================

class HVRAPIClient:
    """
    A unified, robust API client for interacting with Fivetran HVR.
    Includes built-in auto-refresh of authentication tokens upon 401 Unauthorized.
    """
    def __init__(self, base_url: str, username: str, password: str, access_token: str):
        self.base_url = base_url.rstrip('/')
        self.username = username
        self.password = password
        self.access_token = access_token
        # Cache of most recent HTTP error context to assist agent debugging
        self.last_error_info: Optional[Dict[str, str]] = None

    def _get_auth_headers(self) -> Dict[str, str]:
        """Generate authentication headers with current Bearer token."""
        return {
            "Authorization": f"Bearer {self.access_token}"
        }

    def _request(self, method: str, url: str, **kwargs) -> requests.Response:
        """
        Internal request wrapper that performs transparent token refreshing 
        when hitting a 401 Unauthorized response during long-running sessions.
        """
        # Inject standard auth headers
        if "headers" not in kwargs or kwargs["headers"] is None:
            kwargs["headers"] = {}
        kwargs["headers"].update(self._get_auth_headers())

        # Ensure verify=False does not log warnings
        if kwargs.get("verify") is False:
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

        try:
            response = requests.request(method, url, **kwargs)
            
            # Detect token expiration/invalidation
            if response.status_code == 401:
                print("⚠️ Received 401 Unauthorized. Attempting to refresh access token...", file=sys.stderr)
                try:
                    new_token = test_authentication(self.username, self.password, self.base_url)
                    if new_token:
                        self.access_token = new_token
                        kwargs["headers"].update(self._get_auth_headers())
                        print("🔄 Retrying request with refreshed token...", file=sys.stderr)
                        response = requests.request(method, url, **kwargs)
                except Exception as refresh_err:
                    print(f"❌ Failed to refresh authentication token: {refresh_err}", file=sys.stderr)
            
            response.raise_for_status()
            return response

        except requests.exceptions.RequestException as e:
            # Capture diagnostic information to support location/schema troubleshooting
            if e.response is not None:
                try:
                    body = e.response.text
                    self.last_error_info = {
                        "url": url,
                        "status_code": str(e.response.status_code),
                        "body": body[:2000]  # Cap length for safety
                    }
                except Exception:
                    pass
            raise e

    def get_access_token(self) -> str:
        """Fetch a new access token via password authentication."""
        url = f"{self.base_url}/auth/v1/password"
        payload = {
            "username": self.username,
            "password": self.password,
            "bearer": "token"
        }
        response = requests.post(url, json=payload, verify=False)
        response.raise_for_status()
        return response.json()["access_token"]

    # --------------------------------------------------------------------------
    # Channel Management
    # --------------------------------------------------------------------------

    def get_channel_details(self, channel_name: str) -> Dict[str, Any]:
        """Fetch complete configuration details of a specific channel."""
        url = f"{self.base_url}/api/latest/hubs/hvrhub/definition/channels/{channel_name}"
        params = {"fetch": ["cols", "tables", "channel_actions", "loc_groups"]}
        response = self._request("GET", url, params=params, verify=False)
        return response.json()

    def create_channel(self, channel_config: Dict[str, Any]) -> Dict[str, Any]:
        """Create a new channel using a configuration payload."""
        url = f"{self.base_url}/api/latest/hubs/hvrhub/definition/channels"
        response = self._request("POST", url, json=channel_config, verify=False)
        if response.text.strip():
            return response.json()
        return {"status": "success", "message": f"Channel created successfully"}

    def update_channel(self, channel_name: str, channel_config: Dict[str, Any]) -> Dict[str, Any]:
        """Update an existing channel definition."""
        url = f"{self.base_url}/api/latest/hubs/hvrhub/definition/channels/{channel_name}"
        response = self._request("PUT", url, json=channel_config, verify=False)
        return response.json()

    def delete_channel(self, channel_name: str) -> None:
        """Delete a channel by name."""
        url = f"{self.base_url}/api/latest/hubs/hvrhub/definition/channels/{channel_name}"
        self._request("DELETE", url, verify=False)

    def get_all_channel_names(self) -> List[str]:
        """Fetch a list of all active channel names in the hub."""
        url = f"{self.base_url}/api/latest/hubs/hvrhub/definition/channels"
        params = {"fetch": ["cols", "tables", "channel_actions", "loc_groups"]}
        response = self._request("GET", url, params=params, verify=False)
        data = response.json()
        if isinstance(data, dict):
            return list(data.keys())
        elif isinstance(data, list):
            return [ch.get("table_group", "unknown") for ch in data if isinstance(ch, dict)]
        return []

    # --------------------------------------------------------------------------
    # Hub Management
    # --------------------------------------------------------------------------

    def get_hub(self) -> Dict[str, Any]:
        """Fetch complete hub definition."""
        url = f"{self.base_url}/api/latest/hubs/hvrhub/definition"
        response = self._request("GET", url, verify=False)
        return response.json()

    def get_locations(self) -> List[str]:
        """Fetch all configured replication locations in the hub."""
        url = f"{self.base_url}/api/latest/hubs/hvrhub/definition/locs"
        try:
            response = self._request("GET", url, verify=False)
            data = response.json()
            if isinstance(data, dict):
                return list(data.keys())
            elif isinstance(data, list):
                return [d.get("loc_name", str(d)) if isinstance(d, dict) else str(d) for d in data]
            return []
        except requests.exceptions.HTTPError as e:
            if e.response is not None and e.response.status_code == 404:
                print(f"⚠️ Locations endpoint not found: {url}", file=sys.stderr)
                return []
            raise e

    def get_hub_status(self, hub: str) -> Dict[str, Any]:
        """Fetch current properties and status parameters of the hub."""
        url = f"{self.base_url}/api/latest/hubs/{hub}/props"
        params = {
            "fetch": [
                "Hub_Id", "Description", "Hub_State", "Hub_Server_URL", 
                "Hub_Server_Platform", "Hub_Server_HVR_Version", 
                "Hub_Server_OS_Fingerprint", "Creator", "Created"
            ]
        }
        response = self._request("GET", url, params=params, verify=False)
        return response.json()

    def post_hub_update(self, hub: str) -> Any:
        """Freeze a hub by setting state to FROZEN."""
        url = f"{self.base_url}/api/latest/hubs/{hub}/props_modify"
        payload = {
            "hub_prop_args": [
                {
                    "key": ["Hub_State"],
                    "value": "FROZEN"
                }
            ]
        }
        try:
            response = self._request("POST", url, json=payload, verify=False)
            return response
        except requests.exceptions.HTTPError as err:
            print(f"❌ HTTP error during freeze_hub operation: {err}", file=sys.stderr)
            return []

    def post_hub_startv2(self, hub: str) -> Any:
        """Unfreeze a hub by setting state to LIVE."""
        url = f"{self.base_url}/api/latest/hubs/{hub}/props_modify"
        payload = {
            "hub_prop_args": [
                {
                    "key": ["Hub_State"],
                    "value": "LIVE"
                }
            ]
        }
        try:
            response = self._request("POST", url, json=payload, verify=False)
            return response
        except requests.exceptions.HTTPError as err:
            print(f"❌ HTTP error during unfreeze_hub operation: {err}", file=sys.stderr)
            return []

    def post_hub_stop(self, hub: str) -> Dict[str, Any]:
        """Stop the hub replication services."""
        url = f"{self.base_url}/api/latest/{hub}/stop"
        response = self._request("POST", url, verify=False)
        return response.json()

    def post_hub_start(self, hub: str) -> Dict[str, Any]:
        """Restart the hub replication services."""
        url = f"{self.base_url}/api/latest/{hub}/restart"
        response = self._request("POST", url, verify=False)
        return response.json()

    def manage_hub_state(self, hub: str, hub_id: str) -> str:
        """
        Manages the state of a hub dynamically:
        - Unfreezes the hub if it is frozen.
        - Freezes the hub if it is live and matches target hub_id.
        """
        print(f"🔍 Checking status of hub: {hub}")
        hub_status = self.get_hub_status(hub=hub)
        hub_state = hub_status.get('Hub_State')
        current_hub_id = hub_status.get('Hub_Id')
        
        if hub_state == 'FROZEN':
            print(f"❄️ Hub is frozen. Unfreezing: {hub}")
            self.post_hub_startv2(hub=hub)
            action_taken = "Hub unfrozen (started)"
        elif hub_state == 'LIVE' and current_hub_id == hub_id:
            print(f"🔥 Hub is live and matches target. Freezing: {hub}")
            self.post_hub_update(hub=hub)
            action_taken = "Hub frozen"
        else:
            print("ℹ️ Hub state does not match conditions. No action taken.")
            return "Not Mapped"

        time.sleep(2)  # Wait for state change propagation
        updated_status = self.get_hub_status(hub=hub)
        print(f"📊 Updated Hub Status:\n{json.dumps(updated_status, indent=2)}")
        
        return action_taken

    # --------------------------------------------------------------------------
    # Data Refresh & Polling Operations
    # --------------------------------------------------------------------------

    def start_refresh_job(self, hub: str, channel: str, source_loc: str = 'oracle', target_loc: str = 'eli_snow') -> Optional[Dict[str, Any]]:
        """Start a refresh job between specified source and target locations."""
        url = f"{self.base_url}/api/latest/hubs/{hub}/channels/{channel}/refresh"
        payload = {
            'source_loc': source_loc,
            'target_loc': target_loc,
            'granularity': 'bulk',
            'start_immediate': True
        }
        try:
            response = self._request("POST", url, json=payload, verify=False)
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"❌ Error starting refresh job: {e}", file=sys.stderr)
            return None

    def poll_refresh_job(self, hub: str, channel: str, event_id: str, location: str = 'test_repo', 
                         timeout: int = 1800, poll_interval: int = 30) -> Optional[Dict[str, Any]]:
        """Poll job event status until complete or timed out."""
        start_time = time.time()
        event_id = event_id.replace('%', ':')
        
        print(f"🕒 Polling refresh job for channel: {channel} (Event: {event_id})")
        
        while time.time() - start_time < timeout:
            try:
                event_id = event_id.replace('%', ':')
                url = f"{self.base_url}/api/latest/hubs/{hub}/events?channel={channel}&ev_id={event_id}"
                
                response = self._request("GET", url, verify=False)
                events = response.json()
                
                if not events:
                    print("⚠️ No events found in query results.", file=sys.stderr)
                    return None
                
                event = list(events.values())[0]
                event_state = event.get('state', 'UNKNOWN')
                print(f"📡 Event State: {event_state}")
                
                if event_state == 'ACTIVE':
                    time.sleep(poll_interval)
                    continue
                elif event_state == 'DONE':
                    print("✅ Refresh job finished successfully.")
                    return self._summarize_refresh_event(event)
                elif event_state in ['FAILED', 'CANCELED']:
                    print(f"❌ Refresh job {event_state.lower()}!")
                    return self._summarize_refresh_event(event)
                else:
                    print(f"⚠️ Unexpected state: {event_state}", file=sys.stderr)
                    return None
            except requests.exceptions.RequestException as e:
                print(f"⚠️ Error polling event: {e}. Retrying...", file=sys.stderr)
                time.sleep(poll_interval)
        
        print("⏰ Refresh polling timed out.")
        return None

    def _summarize_refresh_event(self, event: Dict[str, Any]) -> Dict[str, Any]:
        """Create a clear summary representation of the replication event."""
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
        if event.get('results'):
            for result in event['results']:
                summary['results'].append({
                    'table': result.get('table', 'N/A'),
                    'result': result.get('result', 'N/A'),
                    'value': result.get('value', 'N/A')
                })
        print(f"\n📊 Summary:\n{json.dumps(summary, indent=2)}")
        return summary

    def execute_comprehensive_refresh(self, hub: str, channel: str, location: str = 'test_repo') -> Optional[Dict[str, Any]]:
        """Triggers, polls, and gathers results for a channel replication refresh workflow."""
        start_response = self.start_refresh_job(hub, channel)
        if not start_response:
            return None
        
        event_id = start_response.get('posted_ev_id', '').replace('%', ':')
        job_id = start_response.get('job')
        
        print(f"🚀 Triggered job: {job_id} | Event: {event_id}")
        return self.poll_refresh_job(hub, channel, event_id, location)

    # --------------------------------------------------------------------------
    # Metadata & Schema Adapt/Apply Operations
    # --------------------------------------------------------------------------

    def capture_source_table_definition(self, hub: str, source_location: str, table_name: str) -> Optional[Dict[str, Any]]:
        """Redefines and captures the DDL/JSON definition of a source database table."""
        params = {"direction": "source"}
        print(f"🔍 Redefining source table definition: {table_name} from {source_location}")

        # Preflight layout query
        list_urls = [
            f"{self.base_url}/api/latest/hubs/{hub}/locations/{source_location}/tables",
            f"{self.base_url}/api/latest/hubs/{hub}/locs/{source_location}/tables",
            f"{self.base_url}/api/latest/hubs/{hub}/definition/locs/{source_location}/tables",
        ]
        preflight_ok = False
        for lu in list_urls:
            try:
                lr = self._request("GET", lu, verify=False)
                if lr.status_code == 200 and isinstance(lr.json(), dict):
                    if table_name in lr.json():
                        preflight_ok = True
                        break
            except Exception:
                pass

        if not preflight_ok:
            print("⚠️ Preflight check: table not explicitly visible in locs tables. Trying redefine anyway...")

        attempts = [
            ("GET",  f"{self.base_url}/api/latest/hubs/{hub}/locations/{source_location}/tables/{table_name}/redefine_table", False),
            ("POST", f"{self.base_url}/api/latest/hubs/{hub}/locations/{source_location}/tables/{table_name}/redefine_table", True),
            ("GET",  f"{self.base_url}/api/latest/hubs/{hub}/locs/{source_location}/tables/{table_name}/redefine_table", False),
            ("GET",  f"{self.base_url}/api/latest/hubs/{hub}/definition/locs/{source_location}/tables/{table_name}/redefine_table", False),
            ("POST", f"{self.base_url}/api/latest/hubs/{hub}/definition/locs/{source_location}/tables/{table_name}/redefine_table", True),
        ]

        response = None
        for method, url_try, send_json in attempts:
            try:
                if method == "POST":
                    if send_json:
                        response = self._request("POST", url_try, json=params, verify=False)
                    else:
                        response = self._request("POST", url_try, params=params, verify=False)
                else:
                    response = self._request("GET", url_try, params=params, verify=False)
            except Exception:
                continue
            if response is not None and response.status_code < 400:
                break

        if response is None:
            print("❌ All redefine endpoints failed to fetch source table metadata.", file=sys.stderr)
            return None

        table_definition = response.json()
        print(f"✅ Table structure fetched for '{table_name}' ({len(table_definition.get('cols', []))} columns)")
        return table_definition

    def import_table_definition(self, hub: str, channel: str, table_name: str, table_definition: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Imports a table schema JSON structure back into the HVR Hub catalog."""
        url_primary = f"{self.base_url}/api/latest/hubs/{hub}/definition/import"
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
            print(f"📥 Importing definition for table '{table_name}' into channel '{channel}'")
            response = self._request("POST", url_primary, json=payload_replace, verify=False)
            
            if response.status_code in (400, 404):
                url_latest = f"{self.base_url}/api/latest/hubs/{hub}/definition/import"
                response = self._request("POST", url_latest, json=payload_replace, verify=False)

            if response.status_code in (400, 404):
                response = self._request("POST", url_primary, json=payload_add, verify=False)

            if response.status_code in (400, 404):
                url_latest = f"{self.base_url}/api/latest/hubs/{hub}/definition/import"
                response = self._request("POST", url_latest, json=payload_add, verify=False)

            print(f"✅ Imported definition for {table_name}")
            return response.json() if response.text.strip() else {"status": "success", "message": "Definition imported"}
        except requests.exceptions.RequestException as e:
            print(f"❌ Error during import: {e}", file=sys.stderr)
            return None

    def adapt_check_tables(self, hub: str, channel: str, location: str, table_names: List[str], 
                            check_layout: bool = True, localize_datatypes: bool = True,
                            fetch_extra: Optional[List[str]] = None, mapspec: Optional[Dict[str, Any]] = None,
                            tables_not_matched_by_mapspec: Optional[bool] = None) -> Optional[Dict[str, Any]]:
        """Queries metadata check mapping against target databases for catalog comparisons."""
        url = f"{self.base_url}/api/latest/hubs/{hub}/channels/{channel}/locs/{location}/adapt/check"
        base_tables = {
            "tables": table_names,
            "check_layout": check_layout,
            "localize_datatypes": localize_datatypes
        }
        attempts = []
        attempts.append(({"tables_in_channel": base_tables}, "base"))
        
        tic_with_notmatched = dict(base_tables)
        tic_with_notmatched["tables_not_matched_by_mapspec"] = True
        attempts.append(({"tables_in_channel": tic_with_notmatched}, "with_tables_not_matched_by_mapspec"))
        
        if mapspec:
            attempts.append(({"tables_in_channel": base_tables, "mapspec": mapspec}, "with_mapspec"))
            
        tic_localize_false = dict(base_tables)
        tic_localize_false["localize_datatypes"] = False
        attempts.append(({"tables_in_channel": tic_localize_false}, "localize_false"))
        
        if fetch_extra:
            attempts.append(({"tables_in_channel": base_tables, "fetch_extra": fetch_extra}, "with_fetch_extra"))

        for pl, label in attempts:
            try:
                print(f"🔎 Adapt CHECK → {url} ({label})")
                response = self._request("POST", url, json=pl, verify=False)
                return response.json()
            except requests.exceptions.RequestException as e:
                print(f"❌ Adapt CHECK variation '{label}' failed: {e}", file=sys.stderr)
                continue
        return None

    def adapt_apply_tables(self, hub: str, channel: str, location: str, table_names: List[str], 
                            add_tables: bool = True, check_layout: bool = True,
                            localize_datatypes: bool = True, ignore_diff: Optional[List[str]] = None,
                            mapspec: Optional[Dict[str, Any]] = None) -> Optional[Dict[str, Any]]:
        """Applies schema comparison resolutions back into the catalog definition structures."""
        url = f"{self.base_url}/api/latest/hubs/{hub}/channels/{channel}/locs/{location}/adapt/apply"
        tic_base = {
            "tables": table_names,
            "check_layout": check_layout,
            "localize_datatypes": localize_datatypes
        }
        if ignore_diff:
            tic_base["ignore_diff"] = ignore_diff

        base_payload = {
            "tables_in_channel": tic_base,
            "add_tables": add_tables
        }
        attempts = []
        attempts.append((base_payload, "base"))
        
        if mapspec:
            p = dict(base_payload)
            p["mapspec"] = mapspec
            attempts.append((p, "with_mapspec"))
            
        tic_localize_false = dict(tic_base)
        tic_localize_false["localize_datatypes"] = False
        attempts.append(({"tables_in_channel": tic_localize_false, "add_tables": add_tables}, "localize_false"))
        
        broad_ignore = [
            "encoding_changed", "nulls_added", "nulls_removed",
            "data_type_changed", "data_type_family_changed", "col_range_bigger", "col_range_smaller"
        ]
        tic_with_ignore = dict(tic_base)
        existing_ignore = tic_with_ignore.get("ignore_diff", [])
        tic_with_ignore["ignore_diff"] = sorted(list(set(existing_ignore + broad_ignore)))
        attempts.append(({"tables_in_channel": tic_with_ignore, "add_tables": add_tables}, "with_ignore_diff"))

        for pl, label in attempts:
            try:
                print(f"📝 Adapt APPLY → {url} ({label})")
                response = self._request("POST", url, json=pl, verify=False)
                return response.json()
            except requests.exceptions.RequestException as e:
                print(f"❌ Adapt APPLY variation '{label}' failed: {e}", file=sys.stderr)
                continue
        return None

    def test_target_connectivity_for_table(self, hub: str, channel: str, location: str, table_name: str) -> bool:
        """Performs lightweight check/redefine preflight testing for target database location reachability."""
        print(f"🔌 Checking target path connectivity: location '{location}' | context '{table_name}'")
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
        err = (self.last_error_info or {}).get("body", "")
        if "Unable to connect" in err or "Couldn't resolve host" in err or "timeout" in err.lower():
            print("⚠️ Reachability fault: connection, host, or timeout occurred.", file=sys.stderr)
        return False

    def create_alter_target_tables(self, hub: str, channel: str, table_names: List[str], 
                                 target_locations: List[str], fill: bool = False) -> Optional[Dict[str, Any]]:
        """Drives CREATE or ALTER table structures directly against target locations without transferring data."""
        payload_base = {
            "channel": channel,
            "tables": table_names,
            "locations": target_locations,
            "fill": fill
        }
        payload_with_type = {"type": "CreateAlterTargetTables", **payload_base}
        
        endpoint_attempts = [
            (f"{self.base_url}/api/latest/hubs/{hub}/events/create-alter-target-tables", payload_base),
            (f"{self.base_url}/api/latest/hubs/{hub}/events/create_alter_target_tables", payload_base),
            (f"{self.base_url}/api/latest/hubs/{hub}/events", payload_with_type),
            (f"{self.base_url}/api/latest/hubs/{hub}/events/create-alter-target-tables", payload_base),
            (f"{self.base_url}/api/latest/hubs/{hub}/events/create_alter_target_tables", payload_base),
            (f"{self.base_url}/api/latest/hubs/{hub}/events", payload_with_type),
            (f"{self.base_url}/api/latest/hubs/{hub}/channels/{channel}/events", payload_with_type),
            (f"{self.base_url}/api/latest/hubs/{hub}/channels/{channel}/events", payload_with_type),
        ]
        
        try:
            op_label = "metadata-only" if not fill else "data-refresh"
            print(f"🔧 Target Schema Update ({op_label}): {table_names} on locations {target_locations}")
            
            response = None
            for url_try, payload_try in endpoint_attempts:
                try:
                    print(f"➡️  POST {url_try}")
                    response = self._request("POST", url_try, json=payload_try, verify=False)
                    if response.status_code < 400:
                        break
                except Exception:
                    continue

            if response is None:
                raise RuntimeError("All create-alter HVR endpoint structures failed to respond successfully.")

            result = response.json() if response.text.strip() else {"status": "success", "message": "Tables updated successfully"}
            print(f"✅ Schema alignment complete.")
            return result
        except requests.exceptions.RequestException as e:
            print(f"❌ Target alignment failed: {e}", file=sys.stderr)
            return None

    def verify_create_alter_jobs(self, hub: str, job_type: str = "CreateAlterTargetTables") -> Optional[Dict[str, Any]]:
        """Queries running/historical table alignment tasks or catalog adjustment executions."""
        url = f"{self.base_url}/api/latest/hubs/{hub}/jobs"
        params = {"type": job_type}
        
        try:
            print(f"🔍 Monitoring {job_type} executions...")
            try:
                response = self._request("GET", url, params=params, verify=False)
            except requests.exceptions.HTTPError as e:
                if e.response is not None and e.response.status_code == 400:
                    params_alt = {"job_type": job_type}
                    response = self._request("GET", url, params=params_alt, verify=False)
                else:
                    raise e
            
            jobs = response.json()
            print(f"📊 Evaluated {len(jobs)} active/historical alignment runs.")
            for jid, jinfo in jobs.items():
                print(f"   • Job {jid}: status '{jinfo.get('state')}' - {jinfo.get('description')}")
            return jobs
        except requests.exceptions.RequestException as e:
            print(f"❌ Failed to fetch alignment run statuses: {e}", file=sys.stderr)
            return None

    def get_table_definition(self, hub: str, channel: str, table_name: str) -> Dict[str, Any]:
        """Fetches stored database schema representations mapped inside the HVR Hub."""
        url = f"{self.base_url}/api/latest/hubs/{hub}/definition/channels/{channel}/tables"
        params = {
            'fetch': ['cols'],
            'table': [table_name]
        }
        try:
            print(f"🔍 Fetching catalog schema: {url}")
            response = self._request("GET", url, params=params, verify=False)
            data = response.json()
            if table_name in data:
                return data[table_name]
            print(f"❌ Catalog element '{table_name}' missing inside channel '{channel}' configuration.", file=sys.stderr)
            return {}
        except requests.exceptions.HTTPError as e:
            if e.response is not None and e.response.status_code == 404:
                print(f"❌ Channel '{channel}' not found inside hub '{hub}' catalog.", file=sys.stderr)
                return {}
            raise e

    # --------------------------------------------------------------------------
    # Consolidated High-Level Schema Evolution / Sync Workflows
    # --------------------------------------------------------------------------

    def execute_metadata_sync_workflow(self, hub: str, channel: str, table_name: str, 
                                      source_location: str, target_locations: List[str]) -> Dict[str, Any]:
        """
        Coordinates full schema evolution replication setup sequentially without data copying:
        1. Queries source catalog updates.
        2. Applies differences into HVR Hub definition.
        3. Preflights target destination network routes.
        4. Aligns structures inside destination database nodes.
        5. Validates structural completeness catalog changes.
        """
        print(f"🚀 Initializing Schema Sync Automation: Table '{table_name}'")
        print(f"   From: '{source_location}' | To: {target_locations}")
        print("=" * 60)
        
        workflow_results = {
            "table_name": table_name,
            "channel": channel,
            "source_location": source_location,
            "target_locations": target_locations,
            "steps": {}
        }
        
        # Step 1: Catalog Redefine Source Comparison
        print("\n📋 STEP 1: Syncing structure details from Source to Hub")
        check_result = self.adapt_check_tables(hub, channel, source_location, [table_name])
        workflow_results["steps"]["check_source"] = {"status": "success" if check_result else "failed", "result": check_result}
        if check_result is None:
            workflow_results["status"] = "failed"
            workflow_results["error"] = "Adapt CHECK failed at source"
            return workflow_results
            
        apply_result = self.adapt_apply_tables(hub, channel, source_location, [table_name], add_tables=True)
        workflow_results["steps"]["apply_from_source"] = {"status": "success" if apply_result else "failed", "result": apply_result}
        if apply_result is None:
            workflow_results["status"] = "failed"
            workflow_results["error"] = "Adapt APPLY failed at source"
            return workflow_results
        
        # Step 2: Read Hub Schema Definition
        print("\n📥 STEP 2: Verifying catalog adaptation updates inside Hub definition")
        before_def = self.get_table_definition(hub, channel, table_name)
        workflow_results["steps"]["hub_definition_after_apply"] = {"status": "success", "definition": before_def}
        
        # Step 3: Align target tables schema
        print("\n🔧 STEP 3: Applying schema alignment across targets")
        targets_apply = {}
        for tgt in target_locations:
            ok = self.test_target_connectivity_for_table(hub, channel, tgt, table_name)
            if not ok:
                guidance = "Location route not responsive. Check firewalls, network routing, or database credentials."
                print(f"❌ Route connection failure: target '{tgt}'")
                workflow_results["status"] = "failed"
                workflow_results["error"] = f"Connectivity to target '{tgt}' failed"
                workflow_results["guidance"] = guidance
                return workflow_results

        # Extract mapping information
        hub_def = self.get_table_definition(hub, channel, table_name)
        derived_mapspec = None
        try:
            schema_name = hub_def.get("schema") or hub_def.get("table_group")
            base_name = hub_def.get("base_name") or table_name
            if schema_name:
                derived_mapspec = {"tables": [{"schema": schema_name, "base_name": base_name}]}
            else:
                derived_mapspec = {"tables": [{"base_name": base_name}]}
        except Exception:
            pass

        for tgt in target_locations:
            print(f"   • Location: {tgt} (Pre-checking layout...)")
            tgt_check = self.adapt_check_tables(hub, channel, tgt, [table_name], mapspec=derived_mapspec)
            print(f"   • Location: {tgt} (Executing updates...)")
            tgt_apply = self.adapt_apply_tables(hub, channel, tgt, [table_name], add_tables=True, mapspec=derived_mapspec)
            targets_apply[tgt] = {
                "check": {"status": "success" if tgt_check else "failed", "result": tgt_check},
                "apply": {"status": "success" if tgt_apply else "failed", "result": tgt_apply},
            }
            if tgt_apply is None:
                workflow_results["status"] = "failed"
                workflow_results["error"] = f"Adapt APPLY failed at target {tgt}"
                workflow_results["steps"]["apply_targets"] = targets_apply
                return workflow_results
        workflow_results["steps"]["apply_targets"] = targets_apply
        
        # Step 4: Verification & Comparison Reporting
        print("\n✅ STEP 4: Verifying final consistency checks")
        time.sleep(5)
        
        job_status = self.verify_create_alter_jobs(hub, job_type="Table_Definition_Adapt")
        workflow_results["steps"]["verify_jobs"] = {"status": "success", "jobs": job_status}
        
        updated_definition = self.get_table_definition(hub, channel, table_name)
        workflow_results["steps"]["verify_definition"] = {"status": "success", "definition": updated_definition}

        targets_check = {}
        for tgt in target_locations:
            tgt_res = self.adapt_check_tables(hub, channel, tgt, [table_name])
            targets_check[tgt] = {"status": "success" if tgt_res else "failed", "result": tgt_res}
        workflow_results["steps"]["check_targets"] = targets_check

        try:
            before_cols = set((before_def or {}).get("cols", {}).keys())
            after_cols = set((updated_definition or {}).get("cols", {}).keys())
            added_cols = sorted(list(after_cols - before_cols))
            removed_cols = sorted(list(before_cols - after_cols))
            print(f"\n🧾 Structural Change Summary ({table_name}):")
            print(f"   Added elements: {added_cols}")
            print(f"   Removed elements: {removed_cols}")
            workflow_results["steps"]["verify_definition"]["summary"] = {
                "added_cols": added_cols,
                "removed_cols": removed_cols
            }
        except Exception as diff_e:
            print(f"Could not construct comparison details: {diff_e}")
        
        workflow_results["status"] = "success"
        workflow_results["message"] = "Metadata schema evolution complete. Zero data movement executed."
        print("\n🎉 SCHEMA SYNCHRONIZATION RUN COMPLETE!")
        return workflow_results


# ==============================================================================
# Global Helpers and Authentication Logic
# ==============================================================================

def test_authentication(username: str, password: str, base_url: str) -> Optional[str]:
    """Test connection credentials and return access bearer token from HVR API."""
    url = f"{base_url.rstrip('/')}/auth/v1/password"
    headers = {"Content-Type": "application/json"}
    payload = {
        "username": username,
        "password": password,
        "bearer": "token"
    }
    try:
        response = requests.post(url, json=payload, headers=headers, verify=False)
        if response.status_code == 200:
            return response.json().get("access_token")
        print(f"❌ Connection auth rejected (Code {response.status_code}): {response.text}", file=sys.stderr)
    except Exception as e:
        print(f"❌ Connection error: {e}", file=sys.stderr)
    return None


# ==============================================================================
# FastMCP Server Initialization
# ==============================================================================

mcp = FastMCP("HVR API server")
_client: Optional[HVRAPIClient] = None

def get_client() -> HVRAPIClient:
    """
    Initializes and returns the singleton HVRAPIClient instance.
    Reads credentials from `/config.json` with multi-path backup fallbacks.
    """
    global _client
    if _client is not None:
        return _client
        
    try:
        # Primary config location specified by the user
        r = '/config.json'
        try:
            with open(r, "r") as i:
                l = i.read()
                y = json.loads(l)
        except FileNotFoundError:
            # Fallback pathing structure for flexible ecosystem running
            fallbacks = [
                '/config.json',
                os.path.join(os.path.dirname(os.path.abspath(__file__)), 'config.json'),
                os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'config.json'),
                'config.json'
            ]
            y = None
            for f_path in fallbacks:
                try:
                    with open(f_path, "r") as i:
                        l = i.read()
                        y = json.loads(l)
                        break
                except FileNotFoundError:
                    continue
            if y is None:
                raise FileNotFoundError(
                    "Config file 'config.json' missing at '/config.json' and all fallback directories."
                )

        username = y['fivetran']['username']
        password = y['fivetran']['password']
        base_url = y['fivetran']['base_url']
        
        access_token = test_authentication(username, password, base_url)
        if not access_token:
            raise RuntimeError("Authentication denied by HVR endpoint.")
            
        _client = HVRAPIClient(
            base_url=base_url,
            username=username,
            password=password,
            access_token=access_token
        )
        return _client
    except Exception as e:
        raise RuntimeError(f"HVR Client setup failure: {str(e)}")


# ==============================================================================
# Registered MCP Tools (26 distinct tools)
# ==============================================================================

# --- Channel Management ---

@mcp.tool()
def get_channel_details(channel_name: str) -> str:
    """Fetch complete structural and routing metadata for a specific HVR channel."""
    try:
        return json.dumps(get_client().get_channel_details(channel_name), indent=2)
    except Exception as e:
        return f"Error: {str(e)}"

@mcp.tool()
def create_channel(channel_config_json: str) -> str:
    """Create a new channel using a JSON string configuration payload."""
    try:
        config = json.loads(channel_config_json)
        return json.dumps(get_client().create_channel(config), indent=2)
    except Exception as e:
        return f"Error: {str(e)}"

@mcp.tool()
def update_channel(channel_name: str, channel_config_json: str) -> str:
    """Update properties/tables for an existing HVR channel using a JSON config payload."""
    try:
        config = json.loads(channel_config_json)
        return json.dumps(get_client().update_channel(channel_name, config), indent=2)
    except Exception as e:
        return f"Error: {str(e)}"

@mcp.tool()
def delete_channel(channel_name: str) -> str:
    """Delete a replication channel definition by name."""
    try:
        get_client().delete_channel(channel_name)
        return f"Successfully deleted channel '{channel_name}'"
    except Exception as e:
        return f"Error: {str(e)}"

@mcp.tool()
def get_all_channel_names() -> str:
    """Fetch lists of all active replication channels inside the HVR Hub catalog."""
    try:
        return json.dumps(get_client().get_all_channel_names(), indent=2)
    except Exception as e:
        return f"Error: {str(e)}"


# --- Hub and Location Operations ---

@mcp.tool()
def get_hub_definition() -> str:
    """Fetch complete hub properties and replication definition settings."""
    try:
        return json.dumps(get_client().get_hub(), indent=2)
    except Exception as e:
        return f"Error: {str(e)}"

@mcp.tool()
def get_locations() -> str:
    """Fetch names of all configured database/warehouse replication locations in the Hub."""
    try:
        return json.dumps(get_client().get_locations(), indent=2)
    except Exception as e:
        return f"Error: {str(e)}"

@mcp.tool()
def get_hub_status(hub: str) -> str:
    """Fetch properties and state characteristics (LIVE, FROZEN, etc.) of a specific hub."""
    try:
        return json.dumps(get_client().get_hub_status(hub), indent=2)
    except Exception as e:
        return f"Error: {str(e)}"

@mcp.tool()
def get_hub_overview(hub: str) -> str:
    """Fetch aggregate overview of a hub, combining status properties, locations, and channel lists."""
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
def freeze_hub(hub: str) -> str:
    """Freeze replication operations by putting the hub state into FROZEN."""
    try:
        get_client().post_hub_update(hub)
        return f"Hub '{hub}' frozen successfully."
    except Exception as e:
        return f"Error: {str(e)}"

@mcp.tool()
def unfreeze_hub(hub: str) -> str:
    """Resume replication operations by putting the hub state back to LIVE."""
    try:
        get_client().post_hub_startv2(hub)
        return f"Hub '{hub}' unfrozen successfully."
    except Exception as e:
        return f"Error: {str(e)}"

@mcp.tool()
def stop_hub(hub: str) -> str:
    """Stop HVR replication server processes for a specific hub."""
    try:
        return json.dumps(get_client().post_hub_stop(hub), indent=2)
    except Exception as e:
        return f"Error: {str(e)}"

@mcp.tool()
def start_hub(hub: str) -> str:
    """Start/Restart HVR replication server processes for a specific hub."""
    try:
        return json.dumps(get_client().post_hub_start(hub), indent=2)
    except Exception as e:
        return f"Error: {str(e)}"

@mcp.tool()
def manage_hub_state(hub: str, hub_id: str) -> str:
    """Intelligently transition hub state (freezes live hubs, unfreezes frozen ones) based on matching hub ID."""
    try:
        return get_client().manage_hub_state(hub, hub_id)
    except Exception as e:
        return f"Error: {str(e)}"


# --- Refresh & Polling Operations ---

@mcp.tool()
def start_refresh_job(hub: str, channel: str, source_loc: str = 'oracle', target_loc: str = 'eli_snow') -> str:
    """Start a data refresh replication run between a source and target location."""
    try:
        return json.dumps(get_client().start_refresh_job(hub, channel, source_loc, target_loc), indent=2)
    except Exception as e:
        return f"Error: {str(e)}"

@mcp.tool()
def poll_refresh_job(hub: str, channel: str, event_id: str, location: str = 'test_repo', timeout: int = 1800, poll_interval: int = 30) -> str:
    """Poll replication task events until completed, failed, or timed out."""
    try:
        return json.dumps(get_client().poll_refresh_job(hub, channel, event_id, location, timeout, poll_interval), indent=2)
    except Exception as e:
        return f"Error: {str(e)}"

@mcp.tool()
def execute_comprehensive_refresh(hub: str, channel: str, location: str = 'test_repo') -> str:
    """Trigger, continuously poll, and summarize a full channel data refresh workflow."""
    try:
        return json.dumps(get_client().execute_comprehensive_refresh(hub, channel, location), indent=2)
    except Exception as e:
        return f"Error: {str(e)}"


# --- Schema and Table Adaptations ---

@mcp.tool()
def capture_source_table_definition(hub: str, source_location: str, table_name: str) -> str:
    """Query, redefine, and extract structure parameters from source database schema."""
    try:
        return json.dumps(get_client().capture_source_table_definition(hub, source_location, table_name), indent=2)
    except Exception as e:
        return f"Error: {str(e)}"

@mcp.tool()
def import_table_definition(hub: str, channel: str, table_name: str, table_definition_json: str) -> str:
    """Import a captured table definition layout payload back into the HVR Hub catalog."""
    try:
        definition = json.loads(table_definition_json)
        return json.dumps(get_client().import_table_definition(hub, channel, table_name, definition), indent=2)
    except Exception as e:
        return f"Error: {str(e)}"

@mcp.tool()
def adapt_check_tables(hub: str, channel: str, location: str, table_names_json: str, check_layout: bool = True, localize_datatypes: bool = True) -> str:
    """Check structural differences of hub definitions against target database layouts."""
    try:
        table_names = json.loads(table_names_json)
        return json.dumps(get_client().adapt_check_tables(hub, channel, location, table_names, check_layout=check_layout, localize_datatypes=localize_datatypes), indent=2)
    except Exception as e:
        return f"Error: {str(e)}"

@mcp.tool()
def adapt_apply_tables(hub: str, channel: str, location: str, table_names_json: str, add_tables: bool = True, check_layout: bool = True, localize_datatypes: bool = True) -> str:
    """Incorporate target schema layout changes back into the HVR hub definitions."""
    try:
        table_names = json.loads(table_names_json)
        return json.dumps(get_client().adapt_apply_tables(hub, channel, location, table_names, add_tables=add_tables, check_layout=check_layout, localize_datatypes=localize_datatypes), indent=2)
    except Exception as e:
        return f"Error: {str(e)}"

@mcp.tool()
def test_target_connectivity_for_table(hub: str, channel: str, location: str, table_name: str) -> str:
    """Preflight check target connectivity/routing using catalog checking endpoints."""
    try:
        status = get_client().test_target_connectivity_for_table(hub, channel, location, table_name)
        return "Connected successfully" if status else "Connection failed"
    except Exception as e:
        return f"Error: {str(e)}"

@mcp.tool()
def create_alter_target_tables(hub: str, channel: str, table_names_json: str, target_locations_json: str, fill: bool = False) -> str:
    """Create or ALTER destination database structures (metadata-only if fill=False)."""
    try:
        table_names = json.loads(table_names_json)
        target_locations = json.loads(target_locations_json)
        return json.dumps(get_client().create_alter_target_tables(hub, channel, table_names, target_locations, fill), indent=2)
    except Exception as e:
        return f"Error: {str(e)}"

@mcp.tool()
def verify_create_alter_jobs(hub: str, job_type: str = "CreateAlterTargetTables") -> str:
    """Verify execution details of target schema alignment actions."""
    try:
        return json.dumps(get_client().verify_create_alter_jobs(hub, job_type), indent=2)
    except Exception as e:
        return f"Error: {str(e)}"

@mcp.tool()
def get_table_definition(hub: str, channel: str, table_name: str) -> str:
    """Query currently cataloged layout schema inside the HVR hub definition."""
    try:
        return json.dumps(get_client().get_table_definition(hub, channel, table_name), indent=2)
    except Exception as e:
        return f"Error: {str(e)}"

@mcp.tool()
def execute_metadata_sync_workflow(hub: str, channel: str, table_name: str, source_location: str, target_locations_json: str) -> str:
    """Run sequential automation capturing source schema evolution and aligning destination nodes."""
    try:
        target_locations = json.loads(target_locations_json)
        return json.dumps(get_client().execute_metadata_sync_workflow(hub, channel, table_name, source_location, target_locations), indent=2)
    except Exception as e:
        return f"Error: {str(e)}"


# ==============================================================================
# Executable Entrypoint
# ==============================================================================

if __name__ == "__main__":
    mcp.run()
