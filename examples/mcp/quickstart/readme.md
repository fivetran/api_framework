# Fivetran MCP Quickstart

Enterprise-grade, minimal MCP server for common Fivetran actions. Built to be easy to run, safe by default, and ready for automation.

## What you get
- Zero-boilerplate MCP server (`server.py`)
- Safe defaults for connector creation (paused + setup tests)
- Consistent JSON responses for LLMs, scripts, and ops tools

## Prerequisites
- Python 3.10+
- `mcp` and `requests` installed
- Fivetran API credentials

## Setup
Set credentials as environment variables:

```bash
export FIVETRAN_API_KEY="your_key"
export FIVETRAN_API_SECRET="your_secret"
```

## Run
```bash
python quickstart/server.py
```

## Available tools
- `test_connection()`
- `list_connectors(group_id=None)`
- `get_connector_status(connector_id)`
- `pause_connector(connector_id)`
- `resume_connector(connector_id)`
- `get_connector_metadata(connector_type)`
- `create_connector(connector_type, group_id, config, auth=None, paused=True, sync_frequency=1440)`
- `list_destinations()`

## Typical flow
1. `test_connection()`
2. `list_destinations()`
3. `get_connector_metadata(connector_type)`
4. `create_connector(...)`
5. `get_connector_status(connector_id)`
6. `resume_connector(connector_id)`
