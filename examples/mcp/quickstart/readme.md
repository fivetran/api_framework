# Fivetran MCP Quickstart

This is a streamlined MCP server for common Fivetran actions. Built to be easy to run, safe by default, and ready for automation.

## What you get
- Boilerplate MCP server (`server.py`)
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

## Sample prompts
Use these verbatim or adapt them. They are sequenced so an agent can learn, create safely, then operate.


## Prompt 1:
```text
Test connectivity and show how many groups are available.
```

## Prompt 2:
```text
List destinations and summarize the names, services, and IDs I can use.
```

## Prompt 3:
```text
Fetch connector metadata for `google_sheets` and explain the required config fields.
```

## Prompt 4:
```text
Check the connector status for `CONNECTOR_ID` and tell me if setup is complete.
```

## Prompt 5:
```text
Resume the connector once setup is complete.
```

## Prompt 6:
```text
Create a connector for `CONNECTOR_TYPE` using metadata-driven config. Use group `GROUP_ID`, then:
- Fetch metadata for `CONNECTOR_TYPE` and list required fields.
- Build a config payload from these inputs:
  - source_identifier: SOURCE_ID
  - schema: SCHEMA_NAME
  - table: TABLE_NAME
  - named_range: RANGE_NAME (if applicable)
  - auth: AUTH_OBJECT (if required)
- Create the connector with safe defaults (paused + setup tests).
- Check status until setup is complete.
- Resume the connector.
Return a concise step-by-step log with the connector ID and any required follow-ups.
```

## Logical sequence
1. Start with `test_connection()` to validate credentials and network access.
2. Use `list_destinations()` to pick a valid `group_id`.
3. Run `get_connector_metadata(connector_type)` so the agent learns required fields and avoids trial-and-error.
4. Call `create_connector(...)` with safe defaults (paused + setup tests).
5. Poll `get_connector_status(connector_id)` to verify setup and sync readiness.
6. Activate with `resume_connector(connector_id)` once everything checks out.
