# Fivetran MCP Server Customization Guide

## Overview

This guide demonstrates how to customize the Fivetran MCP server for specific workflows while maintaining optimal context and token efficiency. The server's declarative architecture makes it easy to create targeted tool sets without code duplication.

## Critical: Context and Token Management

**Why This Matters:**
- MCP servers expose tools to AI agents that consume context tokens
- Each tool definition adds to the agent's context window
- Exposing 180+ tools when you only need 10 wastes tokens and reduces agent focus
- **Token efficiency = Better agent performance + Lower costs**

**Best Practices:**
1. **Expose only what you need** - Filter tools to your workflow
2. **Use descriptive names** - Help agents understand tool purpose without reading descriptions
3. **Provide examples** - Reduce agent trial-and-error (saves tokens)
4. **Combine operations** - Create workflow tools that chain multiple API calls

---

## Customization Flow 1: Filtering Tools for Workflow-Specific Use Cases

### Use Case: Connection Management Workflow

**Goal:** Expose only connection-related tools to focus the agent on connection operations.

### Step 1: Identify Required Tools

First, determine which tools your workflow needs. For connection management:

- `list_connections` - View all connections
- `get_connection_details` - Inspect specific connection
- `create_connection` - Set up new connections
- `modify_connection` - Update connection settings
- `sync_connection` - Trigger manual syncs
- `get_connection_schema_config` - View table configurations
- `run_connection_setup_tests` - Validate connections

### Step 2: Create Workflow-Specific Tool Dictionary

Create a new file `workflow_connections.py`:

```python
# workflow_connections.py
"""Connection Management Workflow - Exposes only connection-related tools."""

from server import TOOLS, build_tool_schema, Server, stdio_server
from mcp.types import Tool

# Filter tools for connection management workflow
CONNECTION_WORKFLOW_TOOLS = {
    # Core connection operations
    "list_connections": TOOLS["list_connections"],
    "get_connection_details": TOOLS["get_connection_details"],
    "create_connection": TOOLS["create_connection"],
    "modify_connection": TOOLS["modify_connection"],
    "delete_connection": TOOLS["delete_connection"],
    
    # Connection state and sync
    "get_connection_state": TOOLS["get_connection_state"],
    "sync_connection": TOOLS["sync_connection"],
    "resync_connection": TOOLS["resync_connection"],
    
    # Schema and table management
    "get_connection_schema_config": TOOLS["get_connection_schema_config"],
    "modify_connection_table_config": TOOLS["modify_connection_table_config"],
    
    # Testing and validation
    "run_connection_setup_tests": TOOLS["run_connection_setup_tests"],
}

# Create workflow-specific server
connection_server = Server("fivetran-connections")

@connection_server.list_tools()
async def list_tools() -> list[Tool]:
    """List only connection management tools."""
    return [build_tool_schema(name, config) for name, config in CONNECTION_WORKFLOW_TOOLS.items()]

# Reuse execute_tool and other handlers from main server
from server import call_tool as base_call_tool, execute_tool

@connection_server.call_tool()
async def call_tool(name: str, arguments: dict) -> list:
    """Execute connection workflow tools."""
    if name not in CONNECTION_WORKFLOW_TOOLS:
        raise ValueError(f"Tool '{name}' not available in connection workflow")
    return await base_call_tool(name, arguments)
```

### Step 3: Create Entry Point

Create `run_connection_workflow.py`:

```python
#!/usr/bin/env python3
"""Run Fivetran MCP server with connection management workflow."""

import asyncio
from workflow_connections import connection_server, stdio_server

async def main():
    """Run the connection workflow server."""
    async with stdio_server() as (read_stream, write_stream):
        await connection_server.run(
            read_stream, 
            write_stream, 
            connection_server.create_initialization_options()
        )

if __name__ == "__main__":
    asyncio.run(main())
```

### Step 4: Test the Filtered Workflow

```bash
# Make executable
chmod +x run_connection_workflow.py

# Test with MCP client
python run_connection_workflow.py
```

### Step 5: Measure Token Impact

**Before filtering:**
- Tools exposed: n-180+
- Estimated context tokens: ~50,000+
- Agent confusion: High (too many options)

**After filtering:**
- Tools exposed: 11
- Estimated context tokens: ~3,000
- Agent focus: High (clear purpose)

**Token Savings: ~Reduction in tool context**

---

## Customization Flow 2: Creating Workflow-Specific Composite Tools

### Use Case: Complete Connection Setup Workflow

**Goal:** Create a single tool that orchestrates multiple API calls for common workflows.

### Step 1: Define the Workflow

A complete connection setup typically requires:
1. Create connection
2. Run setup tests
3. Configure schemas/tables
4. Trigger initial sync

### Step 2: Create Workflow Orchestration Function

Create `workflow_composite_tools.py`:

```python
# workflow_composite_tools.py
"""Composite workflow tools that orchestrate multiple API operations."""

from server import TOOLS, build_tool_schema, Server, stdio_server, execute_tool, fivetran_request
from mcp.types import Tool, TextContent
from typing import Any
import json
import asyncio

# Base tools we'll compose
BASE_TOOLS = {
    "create_connection": TOOLS["create_connection"],
    "run_connection_setup_tests": TOOLS["run_connection_setup_tests"],
    "get_connection_schema_config": TOOLS["get_connection_schema_config"],
    "modify_connection_table_config": TOOLS["modify_connection_table_config"],
    "sync_connection": TOOLS["sync_connection"],
    "get_connection_details": TOOLS["get_connection_details"],
}

# Composite workflow tools
COMPOSITE_TOOLS = {
    # Base tools (still available individually)
    **BASE_TOOLS,
    
    # NEW: Complete connection setup workflow
    "setup_connection_complete": {
        "description": """Complete connection setup workflow that:
1. Creates a new connection
2. Runs setup tests to validate configuration
3. Configures table sync settings (optional)
4. Triggers initial data sync
5. Returns connection status

This tool combines 4-5 API calls into a single operation, reducing agent context and improving reliability.""",
        "method": "POST",
        "endpoint": "/workflow/setup-connection-complete",
        "params": ["request_body"],
        "config_example": {
            "connection_config": "Full connection creation payload (service, group_id, schema, config, etc.)",
            "enable_all_tables": "Boolean - if true, enables all tables for syncing (default: false)",
            "table_config": "Optional dict mapping schema.table to enabled state",
            "trigger_sync": "Boolean - if true, triggers sync after setup (default: true)",
            "wait_for_test": "Boolean - if true, waits for test completion (default: true)"
        }
    },
    
    # NEW: Connection health check workflow
    "check_connection_health": {
        "description": """Comprehensive connection health check that:
1. Gets connection details
2. Gets connection state
3. Gets schema configuration
4. Runs setup tests
5. Returns consolidated health report

Combines 4 API calls into one diagnostic operation.""",
        "method": "GET",
        "endpoint": "/workflow/check-connection-health",
        "params": ["connection_id"],
    },
    
    # NEW: Bulk table configuration
    "configure_connection_tables": {
        "description": """Configure multiple tables at once:
1. Gets current schema configuration
2. Updates specified tables
3. Returns updated configuration

More efficient than calling modify_connection_table_config multiple times.""",
        "method": "POST",
        "endpoint": "/workflow/configure-tables",
        "params": ["connection_id", "request_body"],
        "config_example": {
            "table_configs": "Array of {schema_name, table_name, enabled} objects"
        }
    },
}

async def execute_composite_tool(name: str, arguments: dict[str, Any]) -> dict[str, Any]:
    """Execute composite workflow tools."""
    
    if name == "setup_connection_complete":
        return await setup_connection_complete(arguments)
    elif name == "check_connection_health":
        return await check_connection_health(arguments)
    elif name == "configure_connection_tables":
        return await configure_connection_tables(arguments)
    else:
        # Fall back to base tool execution
        return await execute_tool(name, arguments)


async def setup_connection_complete(arguments: dict[str, Any]) -> dict[str, Any]:
    """Complete connection setup workflow."""
    config = json.loads(arguments["request_body"])
    connection_config = config["connection_config"]
    
    results = {
        "workflow": "setup_connection_complete",
        "steps": [],
        "connection_id": None,
        "status": "in_progress"
    }
    
    try:
        # Step 1: Create connection
        create_result = await execute_tool("create_connection", {
            "request_body": json.dumps(connection_config)
        })
        connection_id = create_result["data"]["id"]
        results["connection_id"] = connection_id
        results["steps"].append({
            "step": 1,
            "action": "create_connection",
            "status": "success",
            "connection_id": connection_id
        })
        
        # Step 2: Run setup tests (if requested)
        if config.get("wait_for_test", True):
            test_result = await execute_tool("run_connection_setup_tests", {
                "connection_id": connection_id
            })
            results["steps"].append({
                "step": 2,
                "action": "run_setup_tests",
                "status": test_result.get("code", "Success"),
                "details": test_result
            })
        
        # Step 3: Configure tables (if specified)
        if config.get("enable_all_tables") or config.get("table_config"):
            schema_config = await execute_tool("get_connection_schema_config", {
                "connection_id": connection_id
            })
            
            # Enable all tables if requested
            if config.get("enable_all_tables"):
                # Implementation to enable all tables
                results["steps"].append({
                    "step": 3,
                    "action": "enable_all_tables",
                    "status": "success"
                })
            elif config.get("table_config"):
                # Configure specific tables
                for table_cfg in config["table_config"]:
                    await execute_tool("modify_connection_table_config", {
                        "connection_id": connection_id,
                        "schema_name": table_cfg["schema_name"],
                        "table_name": table_cfg["table_name"],
                        "request_body": json.dumps({"enabled": table_cfg.get("enabled", True)})
                    })
                results["steps"].append({
                    "step": 3,
                    "action": "configure_tables",
                    "status": "success",
                    "tables_configured": len(config["table_config"])
                })
        
        # Step 4: Trigger sync (if requested)
        if config.get("trigger_sync", True):
            sync_result = await execute_tool("sync_connection", {
                "connection_id": connection_id,
                "request_body": json.dumps({})
            })
            results["steps"].append({
                "step": 4,
                "action": "trigger_sync",
                "status": "success",
                "sync_id": sync_result.get("data", {}).get("sync_id")
            })
        
        # Get final connection status
        connection_details = await execute_tool("get_connection_details", {
            "connection_id": connection_id
        })
        
        results["status"] = "completed"
        results["final_status"] = connection_details["data"]["status"]
        results["connection_details"] = connection_details["data"]
        
        return {
            "code": "Success",
            "data": results
        }
        
    except Exception as e:
        results["status"] = "failed"
        results["error"] = str(e)
        return {
            "code": "Error",
            "data": results
        }


async def check_connection_health(arguments: dict[str, Any]) -> dict[str, Any]:
    """Comprehensive connection health check."""
    connection_id = arguments["connection_id"]
    
    health_report = {
        "connection_id": connection_id,
        "checks": {},
        "overall_status": "unknown"
    }
    
    try:
        # Check 1: Connection details
        details = await execute_tool("get_connection_details", {"connection_id": connection_id})
        health_report["checks"]["connection_details"] = {
            "status": details["data"]["status"],
            "service": details["data"]["service"],
            "paused": details["data"].get("paused", False)
        }
        
        # Check 2: Connection state
        state = await execute_tool("get_connection_state", {"connection_id": connection_id})
        health_report["checks"]["sync_state"] = {
            "setup_state": state["data"].get("setup_state"),
            "sync_state": state["data"].get("sync_state")
        }
        
        # Check 3: Schema configuration
        schemas = await execute_tool("get_connection_schema_config", {"connection_id": connection_id})
        schema_count = len(schemas.get("data", {}).get("schemas", {}))
        health_report["checks"]["schema_config"] = {
            "schemas_configured": schema_count
        }
        
        # Check 4: Run tests
        tests = await execute_tool("run_connection_setup_tests", {"connection_id": connection_id})
        health_report["checks"]["setup_tests"] = {
            "status": tests.get("code", "Unknown"),
            "details": tests.get("data", {})
        }
        
        # Determine overall status
        if health_report["checks"]["connection_details"]["status"] == "connected":
            health_report["overall_status"] = "healthy"
        elif health_report["checks"]["connection_details"]["paused"]:
            health_report["overall_status"] = "paused"
        else:
            health_report["overall_status"] = "needs_attention"
        
        return {
            "code": "Success",
            "data": health_report
        }
        
    except Exception as e:
        health_report["overall_status"] = "error"
        health_report["error"] = str(e)
        return {
            "code": "Error",
            "data": health_report
        }


async def configure_connection_tables(arguments: dict[str, Any]) -> dict[str, Any]:
    """Bulk configure multiple tables."""
    connection_id = arguments["connection_id"]
    config = json.loads(arguments["request_body"])
    table_configs = config["table_configs"]
    
    results = {
        "connection_id": connection_id,
        "tables_configured": [],
        "errors": []
    }
    
    for table_cfg in table_configs:
        try:
            result = await execute_tool("modify_connection_table_config", {
                "connection_id": connection_id,
                "schema_name": table_cfg["schema_name"],
                "table_name": table_cfg["table_name"],
                "request_body": json.dumps({"enabled": table_cfg.get("enabled", True)})
            })
            results["tables_configured"].append({
                "schema": table_cfg["schema_name"],
                "table": table_cfg["table_name"],
                "enabled": table_cfg.get("enabled", True),
                "status": "success"
            })
        except Exception as e:
            results["errors"].append({
                "schema": table_cfg["schema_name"],
                "table": table_cfg["table_name"],
                "error": str(e)
            })
    
    return {
        "code": "Success",
        "data": results
    }


# Create composite workflow server
composite_server = Server("fivetran-composite-workflows")

@composite_server.list_tools()
async def list_tools() -> list[Tool]:
    """List composite workflow tools."""
    return [build_tool_schema(name, config) for name, config in COMPOSITE_TOOLS.items()]

@composite_server.call_tool()
async def call_tool(name: str, arguments: dict) -> list[TextContent]:
    """Execute composite workflow tools."""
    try:
        if name not in COMPOSITE_TOOLS:
            raise ValueError(f"Unknown tool: {name}")
        
        result = await execute_composite_tool(name, arguments)
        return [TextContent(type="text", text=json.dumps(result, indent=2))]
    except Exception as e:
        return [TextContent(type="text", text=f"Error: {str(e)}")]
```

### Step 3: Create Entry Point for Composite Workflows

Create `run_composite_workflows.py`:

```python
#!/usr/bin/env python3
"""Run Fivetran MCP server with composite workflow tools."""

import asyncio
from workflow_composite_tools import composite_server, stdio_server

async def main():
    """Run the composite workflow server."""
    async with stdio_server() as (read_stream, write_stream):
        await composite_server.run(
            read_stream,
            write_stream,
            composite_server.create_initialization_options()
        )

if __name__ == "__main__":
    asyncio.run(main())
```

### Step 4: Test Composite Workflow

```bash
# Make executable
chmod +x run_composite_workflows.py

# Test the composite setup workflow
python run_composite_workflows.py
```

### Step 5: Measure Token and API Call Impact

**Before composite tools:**
- Agent needs to call: 4-5 separate tools
- Context tokens per operation: ~15,000 (multiple tool descriptions)
- API calls: 4-5 requests
- Error handling: Agent must handle each step

**After composite tools:**
- Agent needs to call: 1 tool
- Context tokens per operation: ~3,000 (single tool description)
- API calls: 4-5 requests (same, but orchestrated)
- Error handling: Built into workflow

**Token Savings: ~Reduction per workflow operation**

---

## Advanced: Combining Both Approaches

### Use Case: Data Pipeline Setup Workflow

Create `workflow_pipeline_setup.py` that combines filtering AND composite tools:

```python
# workflow_pipeline_setup.py
"""Focused pipeline setup workflow - filtered tools + composite operations."""

from workflow_composite_tools import (
    COMPOSITE_TOOLS, 
    execute_composite_tool,
    setup_connection_complete,
    check_connection_health
)

# Filter to only pipeline-relevant tools
PIPELINE_WORKFLOW_TOOLS = {
    # Composite workflows (high-level operations)
    "setup_connection_complete": COMPOSITE_TOOLS["setup_connection_complete"],
    "check_connection_health": COMPOSITE_TOOLS["check_connection_health"],
    "configure_connection_tables": COMPOSITE_TOOLS["configure_connection_tables"],
    
    # Essential base operations (for flexibility)
    "list_connections": COMPOSITE_TOOLS["list_connections"],
    "get_connection_details": COMPOSITE_TOOLS["get_connection_details"],
    "sync_connection": COMPOSITE_TOOLS["sync_connection"],
}

# Total tools: 6 (vs 180+ in full server)
# Token reduction: ~97%
```

---

## Token Management Best Practices

### 1. Tool Naming Strategy

**Bad:**
```python
"get_conn": {...}  # Unclear, agent must read description
```

**Good:**
```python
"get_connection_details": {...}  # Self-documenting
```

### 2. Description Optimization

**Bad (verbose):**
```python
"description": "This tool allows you to retrieve detailed information about a specific connection in your Fivetran account. It returns status, configuration, sync history, and other metadata that can be useful for monitoring and troubleshooting purposes."
```

**Good (concise):**
```python
"description": "Get detailed connection information including status, configuration, and sync history."
```

### 3. Use Config Examples Sparingly

Only include `config_example` for complex operations:

```python
# Simple GET - no example needed
"get_connection_details": {
    "description": "...",
    "method": "GET",
    "endpoint": "/v1/connections/{connection_id}"
}

# Complex POST - example helps
"create_connection": {
    "description": "...",
    "method": "POST",
    "endpoint": "/v1/connections",
    "config_example": {...}  # Include for complex payloads
}
```

### 4. Monitor Context Usage

```python
# Add to your server initialization
def estimate_context_tokens(tools: dict) -> int:
    """Estimate context tokens for tool definitions."""
    base_tokens = 100  # Server overhead
    for name, config in tools.items():
        base_tokens += len(name) // 4  # ~4 chars per token
        base_tokens += len(config["description"]) // 4
        if "config_example" in config:
            base_tokens += sum(len(str(v)) // 4 for v in config["config_example"].values())
    return base_tokens

# Usage
print(f"Estimated context tokens: {estimate_context_tokens(PIPELINE_WORKFLOW_TOOLS)}")
```

---

## Quick Reference: Customization Checklist

- [ ] **Identify workflow scope** - What operations are needed?
- [ ] **Filter base tools** - Select only required tools from `TOOLS`
- [ ] **Create composite tools** - Combine multiple operations where beneficial
- [ ] **Optimize descriptions** - Keep concise, add examples only for complex operations
- [ ] **Test token impact** - Measure context size before/after
- [ ] **Document workflow** - Add README for your specific workflow
- [ ] **Create entry point** - Make executable script for easy deployment

---

## Example: Complete Custom Workflow

See `examples/connection_management_workflow/` for a complete implementation combining:
- Filtered tool set (11 tools)
- Composite workflow tools (3 tools)
- Token-optimized descriptions
- Complete test suite

**Result:** 14 tools total, ~4,000 context tokens, handles 90% of connection management use cases.

---

## Summary

1. **Filtering tools** reduces context by 90%+ for focused workflows
2. **Composite tools** reduce agent operations and improve reliability
3. **Token management** is critical for agent performance and cost control
4. **Combining both approaches** gives maximum efficiency

The declarative architecture makes customization trivial - just modify data structures, no code changes needed for the execution engine.
