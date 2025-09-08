#contribution by Parmeet K.

#!/usr/bin/env python3
"""Consolidate single-table file merge mode connections into a new
multi-table connection.

Given a list of connection IDs for file connections operating in merge mode,
this script validates that all non-table configuration matches across the
connections, gathers each connection's ``config.files`` entries, and creates a
new multi-table connection containing all tables. The source connections are
paused to avoid duplicate ingestion.

Usage:
    export FIVETRAN_API_KEY=...
    export FIVETRAN_API_SECRET=...
    python consolidate_file_connections.py TABLE_GROUP_NAME CONN_ID [CONN_ID ...]

Requirements:
    * All connections must belong to the same group (destination) and use the
      same service type and destination schema (the portion of the ``schema``
      value before any ``.``).
    * Table-specific configuration is limited to the ``table_name`` (required),
      ``file_pattern`` and ``archive_pattern``.
"""
from __future__ import annotations

import argparse
import base64
import json
import os
import sys
from typing import Dict, List

import requests

API_BASE = "https://api.fivetran.com/v1"


def _auth_headers() -> Dict[str, str]:
    """Return headers with HTTP Basic authentication for the Fivetran API."""
    key = os.environ.get("FIVETRAN_API_KEY")
    secret = os.environ.get("FIVETRAN_API_SECRET")
    if not key or not secret:
        raise RuntimeError(
            "FIVETRAN_API_KEY and FIVETRAN_API_SECRET must be set in the environment."
        )
    token = base64.b64encode(f"{key}:{secret}".encode()).decode()
    return {"Authorization": f"Basic {token}", "Content-Type": "application/json"}


def api_request(method: str, path: str, **kwargs) -> Dict:
    """Perform a request against the Fivetran API and return the ``data`` field."""
    url = f"{API_BASE}{path}"
    headers = kwargs.pop("headers", {})
    headers.update(_auth_headers())
    response = requests.request(method, url, headers=headers, **kwargs)
    if not response.ok:
        raise RuntimeError(
            f"{method} {path} failed with {response.status_code}: {response.text}"
        )
    payload = response.json()
    return payload["data"]


def fetch_connection(conn_id: str) -> Dict:
    """Fetch details for a connection."""
    return api_request("GET", f"/connectors/{conn_id}")

def extract_file_configs(files: List[Dict]) -> List[Dict]:
    """Return normalized per-table configuration entries."""
    normalized: List[Dict] = []
    for entry in files:
        table = {"table_name": entry["table_name"]}
        if "file_pattern" in entry:
            table["file_pattern"] = entry["file_pattern"]
        if "archive_pattern" in entry:
            table["archive_pattern"] = entry["archive_pattern"]
        normalized.append(table)
    return normalized


def consolidate(table_group_name: str, connection_ids: List[str]) -> str:
    """Create a new multi-table connection from the given single-table connections.

    Returns the ID of the newly created connection and pauses the source
    connections to prevent further syncs.
    """
    if not connection_ids:
        raise RuntimeError("At least one connection ID is required")

    base_id = connection_ids[0]
    base = fetch_connection(base_id)
    base_name = base.get("name", base_id)
    group_id = base["group_id"]
    service = base["service"]
    schema_raw = base.get("schema") or ""
    dest_schema = schema_raw.split(".", 1)[0]

    base_config = base["config"]
    combined_files = extract_file_configs(base_config.get("files", []))
    common_config = {k: v for k, v in base_config.items() if k not in ("files", "schema")}

    for cid in connection_ids[1:]:
        conn = fetch_connection(cid)
        conn_name = conn.get("name", cid)
        if conn["group_id"] != group_id:
            raise RuntimeError(
                f"Connection {conn_name} ({cid}) is in group {conn['group_id']} "
                f"but connection {base_name} ({base_id}) is in group {group_id}"
            )
        if conn["service"] != service:
            raise RuntimeError(
                f"Connection {conn_name} ({cid}) uses service {conn['service']} "
                f"but connection {base_name} ({base_id}) uses {service}"
            )
        conn_schema = conn.get("schema") or ""
        conn_dest = conn_schema.split(".", 1)[0]
        if conn_dest != dest_schema:
            raise RuntimeError(
                f"Connection {conn_name} ({cid}) uses destination schema {conn_dest} "
                f"but connection {base_name} ({base_id}) uses {dest_schema}"
            )
        conn_config = conn["config"]
        conn_common = {k: v for k, v in conn_config.items() if k not in ("files", "schema")}
        if conn_common != common_config:
            raise RuntimeError(
                f"Connection {conn_name} ({cid}) config must match {base_name} ({base_id}) except for files"
            )
        combined_files.extend(extract_file_configs(conn_config.get("files", [])))

    new_config = dict(common_config)
    new_config["schema"] = dest_schema
    new_config["table_group_name"] = table_group_name
    new_config["files"] = combined_files

    created = api_request(
        "POST",
        "/connectors",
        data=json.dumps(
            {
                "group_id": group_id,
                "service": service,
                "config": new_config,
                "run_setup_tests": False,
                "paused": True
            }
        ),
    )
    new_id = created["id"]

    # Pause original single-table connections to avoid duplicate ingestion.
    for cid in connection_ids:
        api_request(
            "PATCH",
            f"/connectors/{cid}",
            data=json.dumps({"paused": True}),
        )
    return new_id


def main(argv: List[str]) -> None:
    parser = argparse.ArgumentParser(
        description="Create a new multi-table file connection from single-table connections",
    )
    parser.add_argument(
        "table_group_name", help="Table group name for the new connection"
    )
    parser.add_argument(
        "connection_ids",
        nargs="+",
        help="IDs of single-table file connections to consolidate",
    )
    args = parser.parse_args(argv)

    new_id = consolidate(args.table_group_name, args.connection_ids)
    print(
        f"Created connection {new_id} and paused {len(args.connection_ids)} connection(s)"
    )


if __name__ == "__main__":
    main(sys.argv[1:])
