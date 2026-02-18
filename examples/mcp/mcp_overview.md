# MCP and Fivetran MCP Overview

## What is MCP
Model Context Protocol (MCP) is a standard way for AI agents to call tools and services through a consistent interface.

In practice, MCP gives you:
- A common contract for tool invocation (`discover`, `call`, `return JSON`)
- Cleaner separation between reasoning (LLM) and execution (service/API)
- Safer operations through explicit tool boundaries and auditable actions

## How MCP Optimizes Work Through Natural Language
MCP turns natural language into structured operational steps.

Example pattern:
1. User asks in plain English.
2. Agent maps intent to MCP tool calls.
3. Tools return normalized JSON.
4. Agent decides next action and reports outcomes.

This reduces manual API scripting, context-switching, and trial-and-error by letting teams operate workflows conversationally while still using deterministic backend actions.

## What Fivetran MCP Is (Quickstart Context)
Fivetran MCP is a streamlined MCP server for common Fivetran actions. It is designed to be:
- Easy to run
- Safe by default
- Automation-ready

### Included in the quickstart
- Boilerplate MCP server (`server.py`)
- Safe connector creation defaults (paused + setup tests)
- Consistent JSON responses for LLMs, scripts, and ops workflows

### Prerequisites
- Python 3.10+
- `mcp` and `requests`
- Fivetran API credentials

### Setup
```bash
export FIVETRAN_API_KEY="your_key"
export FIVETRAN_API_SECRET="your_secret"
python quickstart/server.py
```

## Available Fivetran MCP Tools
- `test_connection()`
- `list_connectors(group_id=None)`
- `get_connector_status(connector_id)`
- `pause_connector(connector_id)`
- `resume_connector(connector_id)`
- `get_connector_metadata(connector_type)`
- `create_connector(connector_type, group_id, config, auth=None, paused=True, sync_frequency=1440)`
- `list_destinations()`

## Recommended Operating Sequence
1. `test_connection()` to validate credentials and network access.
2. `list_destinations()` to identify a valid `group_id`.
3. `get_connector_metadata(connector_type)` to collect required config fields.
4. `create_connector(...)` with safe defaults (paused + setup tests).
5. `get_connector_status(connector_id)` until setup is complete.
6. `resume_connector(connector_id)` when ready.

## Why This Improves Workload Efficiency
Using Fivetran MCP with natural language helps teams:
- Provision connectors faster with fewer misconfigurations
- Standardize runbooks into repeatable prompt-driven workflows
- Keep changes safer through default paused creation and setup validation
- Reduce operator overhead by automating status checks and next-step decisions

## Prompt-to-Action Workflow
A practical natural-language sequence:
1. "Test connectivity and show groups."
2. "List destinations with service and IDs."
3. "Fetch metadata for `CONNECTOR_TYPE` and required fields."
4. "Create connector using metadata-driven config and safe defaults."
5. "Poll status until setup is complete."
6. "Resume connector and return connector ID plus follow-ups."

This is the core value: human-friendly intent at the top, reliable API execution underneath.
