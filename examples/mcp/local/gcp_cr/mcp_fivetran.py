#!/usr/bin/env python3
"""
Fivetran MCP Server ( Streamable HTTP / Cloud Run / Cursor)

Key design points:
- **FastMCP Streamable HTTP**: Uses `FastMCP.streamable_http_app()` to implement
  the MCP endpoint.
- **Single MCP Endpoint**: Mounted at `/` for Cloud Run
  (e.g. `https://<service>.a.run.app/`).
- **Security Best Practices**:
  - Validates the `Origin` header against `MCP_ALLOWED_ORIGINS`
  - Optional bearer-token auth via `MCP_HTTP_AUTH_TOKEN`
  - MCP protocol version and session handling are delegated to FastMCP.
- **Configuration**:
  - Prefer `FIVETRAN_API_KEY` / `FIVETRAN_API_SECRET` env vars (Cloud Run)
  - Fallback to `/configuration.json` inside the container (for local/dev)

Exports:
- `app` (FastAPI ASGI app for Cloud Run / uvicorn)
- `mcp` (FastMCP server instance for tools/resources)
"""

# =============================================================================
# IMPORTS
# =============================================================================

import json
import logging
import os
import time
from datetime import datetime
from typing import Any, Dict, Optional, Union

import requests
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from requests.auth import HTTPBasicAuth

try:
    from mcp.server.fastmcp import FastMCP
except ImportError as exc:
    raise ImportError(
        "FastMCP is required for the HTTP MCP server. "
        "Install with: pip install mcp"
    ) from exc


# =============================================================================
# LOGGING
# =============================================================================

logger = logging.getLogger("mcp_fivetran")
if not logger.handlers:
    logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))


# =============================================================================
# MCP SERVER INITIALIZATION
# =============================================================================

# FastMCP instance used for all tools in this file
mcp = FastMCP("fivetran-mcp-demo-http")


# =============================================================================
# CONFIGURATION MANAGEMENT (from v2, container-friendly path)
# =============================================================================

# In containers (Cloud Run), use a simple root path; for local dev you can
# mount/bind a file at this location if you don't use env vars.
config_file = "/configuration.json"


def _ensure_config_file_exists() -> None:
    """
    Validate configuration file path exists (only for local development).

    Skipped when environment variables are set (Cloud Run).
    """
    if os.getenv("FIVETRAN_API_KEY") and os.getenv("FIVETRAN_API_SECRET"):
        logger.info("Using environment variables for configuration (Cloud Run mode)")
        return

    try:
        cfg_dir = os.path.dirname(config_file) or "."
        if cfg_dir:
            os.makedirs(cfg_dir, exist_ok=True)
        if not os.path.exists(config_file):
            logger.warning("Configuration file not found at: %s", config_file)
            logger.warning("Server will fail credential operations until configured.")
    except Exception as e:  # pragma: no cover - safety logging
        logger.error("Failed to validate configuration file path: %s", e)


_ensure_config_file_exists()


def _load_config(config_file_path: Optional[str] = None) -> Dict[str, Any]:
    """
    Load configuration from environment variables (Cloud Run) or JSON file.

    Priority:
    1. Environment variables (FIVETRAN_API_KEY, FIVETRAN_API_SECRET)
    2. JSON configuration file
    """
    api_key = os.getenv("FIVETRAN_API_KEY")
    api_secret = os.getenv("FIVETRAN_API_SECRET")

    if api_key and api_secret:
        logger.info("Configuration loaded from environment variables (Cloud Run)")
        return {
            "fivetran_api_key": api_key,
            "fivetran_api_secret": api_secret,
        }

    if config_file_path is None:
        config_file_path = config_file

    try:
        with open(config_file_path, "r") as f:
            config_content = f.read()
            logger.info("Configuration file loaded from: %s", config_file_path)
            config = json.loads(config_content)
            return config
    except Exception as e:
        raise Exception(f"Failed to load configuration file: {e}")


def _get_api_credentials() -> tuple[str, str]:
    """
    Retrieve Fivetran API credentials from configuration.
    """
    try:
        config = _load_config()
        api_key = config["fivetran_api_key"]
        api_secret = config["fivetran_api_secret"]
        logger.info("API credentials loaded successfully")
        return api_key, api_secret
    except Exception as e:
        raise Exception(f"Failed to load API credentials: {e}")


# =============================================================================
# FIVETRAN API CLIENT LAYER (from v2)
# =============================================================================


def _make_api_request(
    method: str,
    endpoint: str,
    payload: Dict[str, Any] | None = None,
    params: Dict[str, Any] | None = None,
    max_retries: int = 3,
) -> Optional[Dict[str, Any]]:
    """
    Execute Fivetran API request with reliability and logging.
    """
    try:
        api_key, api_secret = _get_api_credentials()
        auth = HTTPBasicAuth(api_key, api_secret)
        base_url = "https://api.fivetran.com/v1"

        url = f"{base_url}/{endpoint}"
        headers = {
            "Accept": "application/json;version=2",
            "Content-Type": "application/json",
        }

        timeout = (10, 30)  # (connect_timeout, read_timeout) in seconds

        logger.debug("Executing %s request to: %s", method, url)
        if payload:
            logger.debug("Request payload: %s", json.dumps(payload, indent=2))

        for attempt in range(max_retries):
            try:
                if method == "GET":
                    response = requests.get(
                        url, headers=headers, auth=auth, params=params, timeout=timeout
                    )
                elif method == "POST":
                    response = requests.post(
                        url, headers=headers, json=payload, auth=auth, timeout=timeout
                    )
                elif method == "PATCH":
                    response = requests.patch(
                        url, headers=headers, json=payload, auth=auth, timeout=timeout
                    )
                elif method == "DELETE":
                    response = requests.delete(
                        url, headers=headers, json=payload, auth=auth, timeout=timeout
                    )
                else:
                    raise ValueError(f"Invalid request method: {method}")

                logger.debug("Response status: %s", response.status_code)

                if response.status_code >= 400:
                    logger.error("API Error %s: %s", response.status_code, response.text)

                response.raise_for_status()

                data = response.json()
                if isinstance(data, str):
                    logger.warning("API returned string instead of JSON: %s", data)
                    return None

                return data

            except requests.exceptions.Timeout as e:
                logger.warning("Request timeout on attempt %s: %s", attempt + 1, e)
                if attempt == max_retries - 1:
                    raise
                time.sleep(2**attempt)

            except requests.exceptions.RequestException as e:
                logger.error("Request failed on attempt %s: %s", attempt + 1, e)
                if attempt == max_retries - 1:
                    raise
                time.sleep(2**attempt)

            except Exception as e:
                logger.error("Unexpected error on attempt %s: %s", attempt + 1, e)
                if attempt == max_retries - 1:
                    raise
                time.sleep(2**attempt)

        return None

    except Exception as e:
        logger.error("Failed to make API request: %s", e)
        return None


# =============================================================================
# TOOLS (INLINE COPY OF v2 TOOL SURFACE)
# =============================================================================


@mcp.tool()
def list_connectors(group_id: str | None = None) -> str:
    """
    List all Fivetran connectors with optional group filtering.
    """
    try:
        endpoint = "connections"
        params: Dict[str, Any] = {}
        if group_id:
            params["group_id"] = group_id

        response = _make_api_request("GET", endpoint, params=params)

        if response:
            api_data = response.get("data", {})
            if isinstance(api_data, dict) and "items" in api_data:
                connectors_list = api_data.get("items", [])
            elif isinstance(api_data, list):
                connectors_list = api_data
            else:
                connectors_list = []

            return json.dumps(
                {
                    "success": True,
                    "data": connectors_list,
                    "message": f"Retrieved {len(connectors_list)} connectors",
                },
                indent=2,
            )
        else:
            return json.dumps(
                {
                    "success": False,
                    "error": "Failed to retrieve connectors list",
                },
                indent=2,
            )

    except Exception as e:
        return json.dumps(
            {"success": False, "error": f"Error listing connectors: {str(e)}"},
            indent=2,
        )


@mcp.tool()
def get_connector_status(connector_id: str) -> str:
    """
    Retrieve detailed status for a specific connector.
    """
    try:
        endpoint = f"connectors/{connector_id}"
        response = _make_api_request("GET", endpoint)

        if response:
            return json.dumps(
                {
                    "success": True,
                    "data": response.get("data", {}),
                    "message": "Connector status retrieved successfully",
                },
                indent=2,
            )
        else:
            return json.dumps(
                {
                    "success": False,
                    "error": "Failed to retrieve connector status",
                },
                indent=2,
            )

    except Exception as e:
        return json.dumps(
            {"success": False, "error": f"Error getting connector status: {str(e)}"},
            indent=2,
        )


@mcp.tool()
def pause_connector(connector_id: str) -> str:
    """
    Pause a running connector (stops data syncing).
    """
    try:
        endpoint = f"connectors/{connector_id}"
        payload = {"paused": True}
        response = _make_api_request("PATCH", endpoint, payload)

        if response:
            return json.dumps(
                {
                    "success": True,
                    "data": response.get("data", {}),
                    "message": "Connector paused successfully",
                },
                indent=2,
            )
        else:
            return json.dumps(
                {"success": False, "error": "Failed to pause connector"},
                indent=2,
            )

    except Exception as e:
        return json.dumps(
            {"success": False, "error": f"Error pausing connector: {str(e)}"},
            indent=2,
        )


@mcp.tool()
def resume_connector(connector_id: str) -> str:
    """
    Resume a paused connector (resumes data syncing).
    """
    try:
        endpoint = f"connectors/{connector_id}"
        payload = {"paused": False}
        response = _make_api_request("PATCH", endpoint, payload)

        if response:
            return json.dumps(
                {
                    "success": True,
                    "data": response.get("data", {}),
                    "message": "Connector resumed successfully",
                },
                indent=2,
            )
        else:
            return json.dumps(
                {"success": False, "error": "Failed to resume connector"},
                indent=2,
            )

    except Exception as e:
        return json.dumps(
            {"success": False, "error": f"Error resuming connector: {str(e)}"},
            indent=2,
        )


@mcp.tool()
def get_connector_metadata(connector_type: str) -> str:
    """
    Retrieve connector type metadata and configuration requirements.
    """
    try:
        metadata_response = _make_api_request(
            "GET", f"metadata/connectors/{connector_type}"
        )

        if metadata_response:
            result = {
                "success": True,
                "connector_type": connector_type,
                "metadata": metadata_response.get("data", {}),
                "message": f"Metadata retrieved successfully for {connector_type}",
            }
        else:
            result = {
                "success": False,
                "error": f"Failed to retrieve metadata for connector type: {connector_type}",
                "connector_type": connector_type,
            }

        return json.dumps(result, indent=2)

    except Exception as e:
        return json.dumps(
            {
                "success": False,
                "error": f"Error retrieving connector metadata: {str(e)}",
                "connector_type": connector_type,
            },
            indent=2,
        )


@mcp.tool()
def create_dynamic_connector(
    connector_type: str,
    group_id: str,
    config: Dict[str, Any],
    auth: Dict[str, Any] | None = None,
    **kwargs,
) -> str:
    """
    Create any connector type with dynamic configuration.
    """
    try:
        payload: Dict[str, Any] = {
            "group_id": group_id,
            "service": connector_type,
            "trust_certificates": kwargs.get("trust_certificates", True),
            "trust_fingerprints": kwargs.get("trust_fingerprints", True),
            "run_setup_tests": kwargs.get("run_setup_tests", True),
            "paused": kwargs.get("paused", True),
            "pause_after_trial": kwargs.get("pause_after_trial", False),
            "sync_frequency": kwargs.get("sync_frequency", 1440),
            "data_delay_sensitivity": kwargs.get("data_delay_sensitivity", "NORMAL"),
            "data_delay_threshold": kwargs.get("data_delay_threshold", 0),
            "schedule_type": kwargs.get("schedule_type", "auto"),
            "config": config,
        }

        optional_params = [
            "daily_sync_time",
            "connect_card_config",
            "proxy_agent_id",
            "private_link_id",
            "networking_method",
            "hybrid_deployment_agent_id",
            "destination_configuration",
        ]

        for param in optional_params:
            if param in kwargs:
                payload[param] = kwargs[param]

        if auth:
            payload["auth"] = auth

        logger.info("Creating %s connector with dynamic configuration", connector_type)
        logger.debug("Payload: %s", json.dumps(payload, indent=2))

        response = _make_api_request("POST", "connections/", payload)

        if response:
            data = response.get("data", {}) or {}
            connector_id = data.get("id", "Unknown")
            status = data.get("status", {}) or {}

            result = {
                "success": True,
                "connector_id": connector_id,
                "connector_type": connector_type,
                "group_id": group_id,
                "status": status.get("setup_state", "N/A"),
                "paused": payload.get("paused", True),
                "created_at": data.get("created_at", "N/A"),
                "message": f"{connector_type} connector created successfully and paused for complete setup!",
            }
        else:
            result = {
                "success": False,
                "error": "Failed to create connector",
                "connector_type": connector_type,
            }

        return json.dumps(result, indent=2)

    except Exception as e:
        return json.dumps(
            {
                "success": False,
                "error": f"Error creating dynamic connector: {str(e)}",
                "connector_type": connector_type,
            },
            indent=2,
        )


@mcp.tool()
def test_connection() -> str:
    """
    Verify Fivetran API connectivity and credentials.
    """
    try:
        logger.debug("Testing Fivetran API connection")
        test_response = _make_api_request("GET", "groups")

        if test_response:
            return json.dumps(
                {
                    "success": True,
                    "message": "Connection test successful",
                    "groups_count": len(test_response.get("data", {}).get("items", [])),
                    "timestamp": datetime.now().isoformat(),
                },
                indent=2,
            )
        else:
            return json.dumps(
                {
                    "success": False,
                    "error": "API request failed or returned no data",
                },
                indent=2,
            )

    except Exception as e:
        return json.dumps(
            {"success": False, "error": f"Connection test failed: {str(e)}"},
            indent=2,
        )


@mcp.tool()
def get_simple_destinations() -> str:
    """
    Get end-user friendly destination list.
    """
    try:
        logger.debug("Starting get_simple_destinations function")

        response = _make_api_request("GET", "destinations")

        if response is None:
            return json.dumps(
                {
                    "success": False,
                    "error": "Failed to make API request to destinations endpoint",
                },
                indent=2,
            )

        data_section = response.get("data", {})

        if isinstance(data_section, list):
            all_items = data_section
        elif isinstance(data_section, dict):
            all_items = data_section.get("items", [])
        else:
            all_items = []

        if all_items:
            destinations = []

            for dest in all_items:
                destination_id = dest.get("id", "N/A")
                service = dest.get("service", "N/A")
                region = dest.get("region", "UNKNOWN")
                group_id = dest.get("group_id", "N/A")

                service_account_email = (
                    f"g-{group_id}@fivetran-production.iam.gserviceaccount.com"
                )

                destination_name = "N/A"
                if group_id != "N/A":
                    try:
                        group_response = _make_api_request("GET", f"groups/{group_id}")
                        if group_response and isinstance(group_response, Dict):
                            group_data = group_response.get("data", {})
                            if isinstance(group_data, Dict):
                                destination_name = group_data.get(
                                    "name", f"Group_{group_id}"
                                )
                    except Exception as e:
                        logger.warning(
                            "Failed to fetch group name for group_id %s: %s",
                            group_id,
                            e,
                        )
                        destination_name = f"Group_{group_id}"

                destinations.append(
                    {
                        "destination_name": destination_name,
                        "destination_id": destination_id,
                        "service_account_email": service_account_email,
                        "service": service,
                        "region": region,
                        "group_id": group_id,
                    }
                )

            return json.dumps(
                {
                    "success": True,
                    "destinations": destinations,
                    "total": len(destinations),
                    "message": "Destinations formatted for end user sharing",
                },
                indent=2,
            )
        else:
            return json.dumps(
                {
                    "success": False,
                    "error": "No destinations found or invalid response format",
                },
                indent=2,
            )

    except Exception as e:
        return json.dumps(
            {
                "success": False,
                "error": f"Error formatting destinations for end users: {str(e)}",
            },
            indent=2,
        )


@mcp.tool()
def check_connector_status(connector_id: str) -> str:
    """
    Comprehensive connector health check with recommendations.
    """
    try:
        status_response_str = get_connector_status(connector_id)
        status_response = json.loads(status_response_str) if status_response_str else {}

        if status_response:
            data = status_response.get("data", {}) or {}
            status = data.get("status", {}) or {}

            result: Dict[str, Any] = {
                "success": True,
                "connector_id": connector_id,
                "setup_state": status.get("setup_state", "UNKNOWN"),
                "sync_state": status.get("sync_state", "UNKNOWN"),
                "paused": data.get("paused", False),
                "service": data.get("service", "UNKNOWN"),
                "group_id": data.get("group_id", "UNKNOWN"),
                "created_at": data.get("created_at", "UNKNOWN"),
                "last_sync": status.get("last_sync", "UNKNOWN"),
                "message": "Connector status retrieved successfully",
            }

            if status.get("setup_state") == "INCOMPLETE":
                result["needs_attention"] = True
                result[
                    "recommendation"
                ] = "Connector setup is incomplete. Check configuration and authentication."
            elif not data.get("paused"):
                result["needs_attention"] = True
                result[
                    "recommendation"
                ] = "Connector is running. Consider pausing until setup is complete."
            else:
                result["needs_attention"] = False
                result[
                    "recommendation"
                ] = "Connector appears to be properly configured and paused."

        else:
            result = {
                "success": False,
                "error": "Failed to retrieve connector status",
                "connector_id": connector_id,
            }

        return json.dumps(result, indent=2)

    except Exception as e:
        return json.dumps(
            {
                "success": False,
                "error": f"Error checking connector status: {str(e)}",
                "connector_id": connector_id,
            },
            indent=2,
        )


@mcp.tool()
def migrate_connector(
    connector_id: str,
    target_group_id: str,
    new_schema_name: str | None = None,
    new_table_name: str | None = None,
) -> str:
    """
    Migrate connector to different destination with configuration preservation.
    """
    try:
        original_connector_str = get_connector_status(connector_id)
        original_connector = (
            json.loads(original_connector_str) if original_connector_str else None
        )
        if not original_connector:
            return json.dumps(
                {
                    "success": False,
                    "error": "Failed to retrieve original connector details",
                },
                indent=2,
            )

        original_data = original_connector.get("data", {}) or {}
        original_config = original_data.get("config", {}) or {}

        if original_data.get("service") == "google_sheets":
            new_config = {
                "service": "google_sheets",
                "group_id": target_group_id,
                "paused": True,
                "config": {
                    "schema": new_schema_name
                    or f"{original_config.get('schema', 'google_sheets')}_migrated",
                    "table": new_table_name
                    or f"{original_config.get('table', 'data')}_migrated",
                    "named_range": original_config.get("named_range"),
                    "sheet_id": original_config.get("sheet_id"),
                },
            }
        else:
            return json.dumps(
                {
                    "success": False,
                    "error": f"Migration not supported for service type: {original_data.get('service')}",
                },
                indent=2,
            )

        create_response = _make_api_request("POST", "connections/", new_config)

        if create_response:
            new_connector_id = create_response.get("data", {}).get("id")

            schema_response = _make_api_request(
                "GET", f"connectors/{connector_id}/schemas"
            )
            if schema_response:
                schema_data = schema_response.get("data", {}) or {}
                _make_api_request(
                    "PATCH",
                    f"connectors/{new_connector_id}/schemas",
                    schema_data,
                )

            result = {
                "success": True,
                "original_connector_id": connector_id,
                "new_connector_id": new_connector_id,
                "target_group_id": target_group_id,
                "new_schema": new_config["config"]["schema"],
                "new_table": new_config["config"]["table"],
                "message": "Connector migrated successfully",
            }
        else:
            result = {
                "success": False,
                "error": "Failed to create migrated connector",
                "original_connector_id": connector_id,
            }

        return json.dumps(result, indent=2)

    except Exception as e:
        return json.dumps(
            {
                "success": False,
                "error": f"Error migrating connector: {str(e)}",
                "connector_id": connector_id,
            },
            indent=2,
        )


@mcp.tool()
def reload_connector_schema(connector_id: str) -> str:
    """
    Refresh connector schema to detect source changes.
    """
    try:
        reload_response = _make_api_request(
            "POST", f"connectors/{connector_id}/schemas/reload"
        )

        if reload_response:
            result = {
                "success": True,
                "connector_id": connector_id,
                "message": "Schema reload triggered successfully",
                "timestamp": datetime.now().isoformat(),
            }
        else:
            result = {
                "success": False,
                "error": "Failed to reload schema",
                "connector_id": connector_id,
            }

        return json.dumps(result, indent=2)

    except Exception as e:
        return json.dumps(
            {
                "success": False,
                "error": f"Error reloading schema: {str(e)}",
                "connector_id": connector_id,
            },
            indent=2,
        )


@mcp.tool()
def get_connector_schema(connector_id: str) -> str:
    """
    Retrieve current schema configuration and sync settings.
    """
    try:
        schema_response = _make_api_request(
            "GET", f"connectors/{connector_id}/schemas"
        )

        if schema_response:
            result = {
                "success": True,
                "connector_id": connector_id,
                "schema_config": schema_response.get("data", {}),
                "message": "Schema retrieved successfully",
            }
        else:
            result = {
                "success": False,
                "error": "Failed to retrieve schema",
                "connector_id": connector_id,
            }

        return json.dumps(result, indent=2)

    except Exception as e:
        return json.dumps(
            {
                "success": False,
                "error": f"Error retrieving schema: {str(e)}",
                "connector_id": connector_id,
            },
            indent=2,
        )


@mcp.tool()
def update_connector_schema(connector_id: str, schema_config: Dict[str, Any]) -> str:
    """
    Modify connector schema configuration.
    """
    try:
        update_response = _make_api_request(
            "PATCH", f"connectors/{connector_id}/schemas", schema_config
        )

        if update_response:
            result = {
                "success": True,
                "connector_id": connector_id,
                "message": "Schema updated successfully",
                "timestamp": datetime.now().isoformat(),
            }
        else:
            result = {
                "success": False,
                "error": "Failed to update schema",
                "connector_id": connector_id,
            }

        return json.dumps(result, indent=2)

    except Exception as e:
        return json.dumps(
            {
                "success": False,
                "error": f"Error updating schema: {str(e)}",
                "connector_id": connector_id,
            },
            indent=2,
        )


@mcp.tool()
def modify_sync_frequency(connection_id: str, sync_frequency: int) -> str:
    """
    Update the sync frequency for a Fivetran connection.
    """
    VALID_SYNC_FREQUENCIES = [1, 5, 15, 30, 60, 120, 180, 360, 480, 720, 1440]

    try:
        if sync_frequency not in VALID_SYNC_FREQUENCIES:
            return json.dumps(
                {
                    "success": False,
                    "error": f"Invalid sync_frequency value: {sync_frequency}",
                    "valid_values": VALID_SYNC_FREQUENCIES,
                },
                indent=2,
            )

        endpoint = f"connections/{connection_id}"
        payload = {"sync_frequency": sync_frequency}

        response = _make_api_request("PATCH", endpoint, payload)

        if response:
            data = response.get("data", {}) or {}
            status = data.get("status", {}) or {}

            result = {
                "success": True,
                "connection_id": connection_id,
                "sync_frequency": sync_frequency,
                "sync_frequency_formatted": f"{sync_frequency} minutes ({sync_frequency // 60 if sync_frequency >= 60 else sync_frequency} {'hours' if sync_frequency >= 60 else 'minutes'})",
                "setup_state": status.get("setup_state", "N/A"),
                "sync_state": status.get("sync_state", "N/A"),
                "message": "Connection sync frequency updated successfully",
            }
        else:
            result = {
                "success": False,
                "error": "Failed to update connection sync frequency",
                "connection_id": connection_id,
            }

        return json.dumps(result, indent=2)

    except Exception as e:
        return json.dumps(
            {
                "success": False,
                "error": f"Error updating sync frequency: {str(e)}",
                "connection_id": connection_id,
            },
            indent=2,
        )


@mcp.tool()
def health_check_all_connectors(group_id: str | None = None) -> str:
    """
    Enterprise-wide connector health assessment.
    """
    try:
        connectors_response_str = list_connectors(group_id)

        if not connectors_response_str:
            return json.dumps(
                {
                    "success": False,
                    "error": "Failed to retrieve connectors list - empty response",
                }
            )

        try:
            connectors_response = json.loads(connectors_response_str)
        except json.JSONDecodeError as e:
            return json.dumps(
                {
                    "success": False,
                    "error": f"Failed to parse connectors response: {str(e)}",
                    "response_preview": connectors_response_str[:200],
                }
            )

        if not isinstance(connectors_response, dict):
            return json.dumps(
                {
                    "success": False,
                    "error": f"Invalid response type: expected dict, got {type(connectors_response).__name__}",
                    "response": str(connectors_response)[:200],
                }
            )

        if not connectors_response.get("success"):
            error_msg = connectors_response.get("error", "Unknown error")
            return json.dumps(
                {"success": False, "error": f"Failed to retrieve connectors: {error_msg}"}
            )

        data = connectors_response.get("data", [])

        if isinstance(data, list):
            connectors = data
        elif isinstance(data, dict):
            connectors = data.get("items", [])
            if not isinstance(connectors, list):
                return json.dumps(
                    {
                        "success": False,
                        "error": "Invalid connectors structure: data.items is not a list",
                        "data_type": type(connectors).__name__,
                        "data_preview": str(connectors)[:200],
                    }
                )
        else:
            return json.dumps(
                {
                    "success": False,
                    "error": f"Invalid connectors data type: expected list or dict, got {type(data).__name__}",
                    "data_type": type(data).__name__,
                    "data_preview": str(data)[:200],
                }
            )

        health_results: Dict[str, Any] = {
            "success": True,
            "total_connectors": len(connectors),
            "healthy": 0,
            "needs_attention": 0,
            "failed": 0,
            "connector_health": [],
            "summary": {},
        }

        for conn in connectors:
            if not isinstance(conn, dict):
                health_results["failed"] += 1
                health_results["connector_health"].append(
                    {
                        "connector_id": "UNKNOWN",
                        "name": "UNKNOWN",
                        "service": "UNKNOWN",
                        "health_status": "failed",
                        "issues": [
                            f"Invalid connector data type: expected dict, got {type(conn).__name__}"
                        ],
                    }
                )
                continue

            connector_id = conn.get("id")
            connector_name = conn.get("name", "N/A")
            service = conn.get("service", "N/A")

            if not connector_id:
                health_results["failed"] += 1
                health_results["connector_health"].append(
                    {
                        "connector_id": "MISSING",
                        "name": connector_name,
                        "service": service,
                        "health_status": "failed",
                        "issues": ["Missing connector ID"],
                    }
                )
                continue

            try:
                status_response_str = get_connector_status(connector_id)

                if not status_response_str:
                    health_results["failed"] += 1
                    health_results["connector_health"].append(
                        {
                            "connector_id": connector_id,
                            "name": connector_name,
                            "service": service,
                            "health_status": "failed",
                            "issues": ["Empty status response"],
                        }
                    )
                    continue

                try:
                    status_response = json.loads(status_response_str)
                except json.JSONDecodeError as e:
                    health_results["failed"] += 1
                    health_results["connector_health"].append(
                        {
                            "connector_id": connector_id,
                            "name": connector_name,
                            "service": service,
                            "health_status": "failed",
                            "issues": [f"Failed to parse status response: {str(e)}"],
                        }
                    )
                    continue

                if not isinstance(status_response, dict):
                    health_results["failed"] += 1
                    health_results["connector_health"].append(
                        {
                            "connector_id": connector_id,
                            "name": connector_name,
                            "service": service,
                            "health_status": "failed",
                            "issues": [
                                f"Invalid status response type: expected dict, got {type(status_response).__name__}"
                            ],
                        }
                    )
                    continue

                status_data = status_response.get("data", {}) or {}
                status_info = status_data.get("status", {}) or {}

                health_status = "healthy"
                issues = []

                setup_state = status_info.get("setup_state", "UNKNOWN")
                sync_state = status_info.get("sync_state", "UNKNOWN")
                paused = status_data.get("paused", False)

                if setup_state == "INCOMPLETE":
                    health_status = "needs_attention"
                    issues.append("Setup incomplete")
                elif sync_state == "FAILED":
                    health_status = "needs_attention"
                    issues.append("Sync failed")
                elif not paused and setup_state != "COMPLETE":
                    health_status = "needs_attention"
                    issues.append("Running with incomplete setup")

                connector_health = {
                    "connector_id": connector_id,
                    "name": connector_name,
                    "service": service,
                    "health_status": health_status,
                    "setup_state": setup_state,
                    "sync_state": sync_state,
                    "paused": paused,
                    "issues": issues,
                    "last_sync": status_info.get("last_sync", "N/A"),
                }

                health_results["connector_health"].append(connector_health)

                if health_status == "healthy":
                    health_results["healthy"] += 1
                elif health_status == "needs_attention":
                    health_results["needs_attention"] += 1
                else:
                    health_results["failed"] += 1

            except Exception as e:
                health_results["failed"] += 1
                health_results["connector_health"].append(
                    {
                        "connector_id": connector_id,
                        "name": connector_name,
                        "service": service,
                        "health_status": "failed",
                        "issues": [f"Error checking status: {str(e)}"],
                    }
                )

        health_results["summary"] = {
            "health_percentage": round(
                (health_results["healthy"] / health_results["total_connectors"]) * 100,
                2,
            )
            if health_results["total_connectors"] > 0
            else 0,
            "recommendations": [],
        }

        if health_results["needs_attention"] > 0:
            health_results["summary"]["recommendations"].append(
                f"{health_results['needs_attention']} connectors need attention"
            )

        if health_results["failed"] > 0:
            health_results["summary"]["recommendations"].append(
                f"{health_results['failed']} connectors failed health check"
            )

        health_results["message"] = (
            f"Health check completed: {health_results['healthy']} healthy, "
            f"{health_results['needs_attention']} need attention, "
            f"{health_results['failed']} failed"
        )

        return json.dumps(health_results, indent=2)

    except Exception as e:
        return json.dumps(
            {"success": False, "error": f"Health check failed: {str(e)}"}, indent=2
        )


@mcp.tool()
def get_connector_metrics(connector_id: str, days: int = 7) -> str:
    """
    Detailed connector performance and configuration metrics.
    """
    try:
        connector_response_str = get_connector_status(connector_id)
        connector_response = (
            json.loads(connector_response_str) if connector_response_str else None
        )
        if not connector_response:
            return json.dumps(
                {
                    "success": False,
                    "error": "Failed to retrieve connector details",
                },
                indent=2,
            )

        connector_data = connector_response.get("data", {}) or {}
        status_info = connector_data.get("status", {}) or {}

        metrics = {
            "success": True,
            "connector_id": connector_id,
            "service": connector_data.get("service", "N/A"),
            "group_id": connector_data.get("group_id", "N/A"),
            "created_at": connector_data.get("created_at", "N/A"),
            "current_status": {
                "setup_state": status_info.get("setup_state", "UNKNOWN"),
                "sync_state": status_info.get("sync_state", "UNKNOWN"),
                "paused": connector_data.get("paused", False),
                "last_sync": status_info.get("last_sync", "N/A"),
            },
            "performance_metrics": {
                "sync_frequency": connector_data.get("sync_frequency", "N/A"),
                "data_delay_sensitivity": connector_data.get(
                    "data_delay_sensitivity", "N/A"
                ),
                "data_delay_threshold": connector_data.get(
                    "data_delay_threshold", "N/A"
                ),
            },
            "configuration": {
                "trust_certificates": connector_data.get(
                    "trust_certificates", False
                ),
                "trust_fingerprints": connector_data.get(
                    "trust_fingerprints", False
                ),
                "run_setup_tests": connector_data.get("run_setup_tests", False),
            },
            "message": f"Metrics retrieved for {days} days lookback",
        }

        return json.dumps(metrics, indent=2)

    except Exception as e:
        return json.dumps(
            {
                "success": False,
                "error": f"Error retrieving metrics: {str(e)}",
                "connector_id": connector_id,
            },
            indent=2,
        )


@mcp.tool()
def get_connector_usage_report(
    group_id: str | None = None, days: int = 30
) -> str:
    """
    Enterprise connector usage analytics and reporting.
    """
    try:
        connectors_response_str = list_connectors(group_id)
        connectors_response = (
            json.loads(connectors_response_str) if connectors_response_str else None
        )
        if not connectors_response:
            return json.dumps(
                {"success": False, "error": "Failed to retrieve connectors"}, indent=2
            )

        if isinstance(connectors_response, str):
            return json.dumps(
                {
                    "success": False,
                    "error": f"API returned string instead of JSON: {connectors_response}",
                },
                indent=2,
            )

        connectors = connectors_response.get("data", []) or []

        report: Dict[str, Any] = {
            "success": True,
            "report_period_days": days,
            "group_id": group_id,
            "total_connectors": len(connectors),
            "service_breakdown": {},
            "status_breakdown": {},
            "health_summary": {"healthy": 0, "needs_attention": 0, "failed": 0},
            "connector_details": [],
        }

        for conn in connectors:
            connector_id = conn.get("id")
            service = conn.get("service", "N/A")
            paused = conn.get("paused", False)

            report["service_breakdown"].setdefault(service, 0)
            report["service_breakdown"][service] += 1

            status_key = "paused" if paused else "active"
            report["status_breakdown"].setdefault(status_key, 0)
            report["status_breakdown"][status_key] += 1

            try:
                status_response_str = get_connector_status(connector_id)
                status_response = (
                    json.loads(status_response_str) if status_response_str else None
                )
                if status_response:
                    status_data = status_response.get("data", {}) or {}
                    status_info = status_data.get("status", {}) or {}

                    setup_state = status_info.get("setup_state", "UNKNOWN")

                    if setup_state == "COMPLETE" and not paused:
                        health = "healthy"
                        report["health_summary"]["healthy"] += 1
                    elif setup_state == "INCOMPLETE":
                        health = "needs_attention"
                        report["health_summary"]["needs_attention"] += 1
                    else:
                        health = "failed"
                        report["health_summary"]["failed"] += 1

                    report["connector_details"].append(
                        {
                            "connector_id": connector_id,
                            "service": service,
                            "health": health,
                            "setup_state": setup_state,
                            "paused": paused,
                            "created_at": status_data.get("created_at", "N/A"),
                            "last_sync": status_info.get("last_sync", "N/A"),
                        }
                    )
                else:
                    report["health_summary"]["failed"] += 1
                    report["connector_details"].append(
                        {
                            "connector_id": connector_id,
                            "service": service,
                            "health": "failed",
                            "error": "Failed to retrieve status",
                        }
                    )
            except Exception as e:
                report["health_summary"]["failed"] += 1
                report["connector_details"].append(
                    {
                        "connector_id": connector_id,
                        "service": service,
                        "health": "failed",
                        "error": f"Status check failed: {str(e)}",
                    }
                )

        total = report["total_connectors"]
        if total > 0:
            report["health_percentages"] = {
                "healthy": round(
                    (report["health_summary"]["healthy"] / total) * 100, 2
                ),
                "needs_attention": round(
                    (report["health_summary"]["needs_attention"] / total) * 100, 2
                ),
                "failed": round(
                    (report["health_summary"]["failed"] / total) * 100, 2
                ),
            }

        report["message"] = (
            f"Usage report generated for {total} connectors over {days} days"
        )

        return json.dumps(report, indent=2)

    except Exception as e:
        return json.dumps(
            {"success": False, "error": f"Error generating usage report: {str(e)}"},
            indent=2,
        )


# =============================================================================
# STREAMABLE HTTP MCP APP
# =============================================================================

mcp_app = mcp.streamable_http_app()


# =============================================================================
# FASTAPI WRAPPER FOR CLOUD RUN + SECURITY MIDDLEWARE
# =============================================================================

app = FastAPI(
    title="Fivetran MCP (HTTP / Cloud Run)",
    lifespan=lambda _: mcp.session_manager.run(),
)


@app.middleware("http")
async def security_middleware(request: Request, call_next):
    """
    Enforce HTTP MCP best-practices for Cloud Run:
    - Validate Origin header against MCP_ALLOWED_ORIGINS
    - Optional bearer token via MCP_HTTP_AUTH_TOKEN
    - MCP-Protocol-Version / Mcp-Session-Id are handled by FastMCP.
    """

    allowed_origins = os.getenv("MCP_ALLOWED_ORIGINS", "")
    origin = request.headers.get("Origin")
    if origin and allowed_origins:
        allowed = {o.strip() for o in allowed_origins.split(",") if o.strip()}
        if allowed and origin not in allowed:
            logger.warning("Rejected request from disallowed Origin: %s", origin)
            return JSONResponse(
                status_code=403,
                content={"error": "Origin not allowed"},
            )

    expected_token = os.getenv("MCP_HTTP_AUTH_TOKEN")
    if expected_token:
        auth_header = request.headers.get("Authorization", "")
        if auth_header != f"Bearer {expected_token}":
            logger.warning("Rejected request with invalid Authorization header")
            return JSONResponse(
                status_code=401,
                content={"error": "Unauthorized"},
            )

    return await call_next(request)


# Mount MCP endpoint at root (single MCP endpoint per google_http.md)
app.mount("/", mcp_app)


__all__ = ["app", "mcp"]
