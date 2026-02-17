#!/usr/bin/env python3
"""Minimal MCP server for common Fivetran actions."""

import json
import os
import time
from datetime import datetime
from typing import Any, Dict, Optional

import requests
from requests.auth import HTTPBasicAuth

from mcp.server.fastmcp import FastMCP

mcp = FastMCP("fivetran-mcp-quickstart")

API_BASE_URL = "https://api.fivetran.com/v1"
API_VERSION_HEADER = "application/json;version=2"


def _get_credentials() -> tuple[str, str]:
    api_key = os.getenv("FIVETRAN_API_KEY")
    api_secret = os.getenv("FIVETRAN_API_SECRET")

    if not api_key or not api_secret:
        raise RuntimeError(
            "Missing Fivetran credentials. Set FIVETRAN_API_KEY and FIVETRAN_API_SECRET."
        )

    return api_key, api_secret


def _request(
    method: str,
    endpoint: str,
    payload: Optional[Dict[str, Any]] = None,
    params: Optional[Dict[str, Any]] = None,
    max_retries: int = 3,
) -> Optional[Dict[str, Any]]:
    api_key, api_secret = _get_credentials()
    auth = HTTPBasicAuth(api_key, api_secret)

    url = f"{API_BASE_URL}/{endpoint}"
    headers = {
        "Accept": API_VERSION_HEADER,
        "Content-Type": "application/json",
        "User-Agent": "fivetran-quickstart-mcp",
    }
    timeout = (10, 30)

    for attempt in range(max_retries):
        try:
            if method == "GET":
                response = requests.get(url, headers=headers, auth=auth, params=params, timeout=timeout)
            elif method == "POST":
                response = requests.post(url, headers=headers, auth=auth, json=payload, timeout=timeout)
            elif method == "PATCH":
                response = requests.patch(url, headers=headers, auth=auth, json=payload, timeout=timeout)
            else:
                raise ValueError(f"Unsupported method: {method}")

            response.raise_for_status()
            return response.json()
        except requests.exceptions.Timeout:
            if attempt == max_retries - 1:
                raise
            time.sleep(2 ** attempt)
        except requests.exceptions.RequestException as exc:
            if attempt == max_retries - 1:
                raise RuntimeError(f"Fivetran API request failed: {exc}") from exc
            time.sleep(2 ** attempt)

    return None


@mcp.tool()
def test_connection() -> str:
    """Validate credentials and connectivity."""
    response = _request("GET", "groups")
    groups = response.get("data", {}).get("items", []) if response else []
    return json.dumps(
        {
            "success": True,
            "groups_count": len(groups),
            "timestamp": datetime.now().isoformat(),
        },
        indent=2,
    )


@mcp.tool()
def list_connectors(group_id: Optional[str] = None) -> str:
    """List connectors, optionally filtered by group ID."""
    params = {"group_id": group_id} if group_id else None
    response = _request("GET", "connections", params=params)
    data = response.get("data", {}) if response else {}

    connectors = data.get("items", []) if isinstance(data, dict) else data
    return json.dumps(
        {
            "success": True,
            "count": len(connectors),
            "connectors": connectors,
        },
        indent=2,
    )


@mcp.tool()
def get_connector_status(connector_id: str) -> str:
    """Fetch connector status and configuration."""
    response = _request("GET", f"connectors/{connector_id}")
    return json.dumps(
        {
            "success": True,
            "data": response.get("data", {}) if response else {},
        },
        indent=2,
    )


@mcp.tool()
def pause_connector(connector_id: str) -> str:
    """Pause a connector to stop syncing."""
    response = _request("PATCH", f"connectors/{connector_id}", payload={"paused": True})
    return json.dumps(
        {
            "success": True,
            "data": response.get("data", {}) if response else {},
            "message": "Connector paused",
        },
        indent=2,
    )


@mcp.tool()
def resume_connector(connector_id: str) -> str:
    """Resume a connector to continue syncing."""
    response = _request("PATCH", f"connectors/{connector_id}", payload={"paused": False})
    return json.dumps(
        {
            "success": True,
            "data": response.get("data", {}) if response else {},
            "message": "Connector resumed",
        },
        indent=2,
    )


@mcp.tool()
def get_connector_metadata(connector_type: str) -> str:
    """Discover required fields for a connector type."""
    response = _request("GET", f"metadata/connectors/{connector_type}")
    return json.dumps(
        {
            "success": True,
            "connector_type": connector_type,
            "metadata": response.get("data", {}) if response else {},
        },
        indent=2,
    )


@mcp.tool()
def create_connector(
    connector_type: str,
    group_id: str,
    config: Dict[str, Any],
    auth: Optional[Dict[str, Any]] = None,
    paused: bool = True,
    sync_frequency: int = 1440,
) -> str:
    """Create a connector with a minimal, safe payload."""
    payload: Dict[str, Any] = {
        "group_id": group_id,
        "service": connector_type,
        "paused": paused,
        "sync_frequency": sync_frequency,
        "trust_certificates": True,
        "trust_fingerprints": True,
        "run_setup_tests": True,
        "config": config,
    }
    if auth:
        payload["auth"] = auth

    response = _request("POST", "connections", payload=payload)
    data = response.get("data", {}) if response else {}

    return json.dumps(
        {
            "success": True,
            "connector_id": data.get("id"),
            "status": data.get("status", {}).get("setup_state"),
            "paused": data.get("paused"),
            "message": "Connector created",
        },
        indent=2,
    )


@mcp.tool()
def list_destinations() -> str:
    """List destinations with a clean summary."""
    response = _request("GET", "destinations")
    data = response.get("data", {}) if response else {}

    items = data.get("items", []) if isinstance(data, dict) else data
    destinations = []
    for item in items:
        destinations.append(
            {
                "destination_id": item.get("id"),
                "group_id": item.get("group_id"),
                "service": item.get("service"),
                "region": item.get("region"),
                "name": item.get("name") or item.get("id"),
            }
        )

    return json.dumps(
        {
            "success": True,
            "count": len(destinations),
            "destinations": destinations,
        },
        indent=2,
    )


if __name__ == "__main__":
    mcp.run(transport="stdio")
