# HVR MCP server best practices guide

This guide details customization pathways, architectural implementations, and extension procedures specifically for /hvr/mcp/server.py.

---

## 1. HVR integration architecture

The HVR MCP Server exposes replication pipelines and DDL/schema evolution capabilities directly to LLM agents. The server follows a structured design:

```
                  +----------------------------------------------+
                  |                 FastMCP Server               |
                  |             [hvr_mcp_server.py]              |
                  +----------------------------------------------+
                                         ||
                      Calls singleton client helper get_client()
                                         \/
                  +----------------------------------------------+
                  |                HVRAPIClient                  |
                  |    - Token Cache                             |
                  |    - _request() with Auto-Refresh            |
                  +----------------------------------------------+
                                         ||
                     Performs HTTP Callouts via requests
                                         \/
                               +--------------------+
                               |   HVR Hub API v6   |
                               +--------------------+
```

### Key components:
* **Host & Server Initialization:** initializing the `FastMCP` protocol binding.
* **Client Singleton Setup:**  handles configuration parsing and authentication caching.
* **API Client wrapper:** Managed by REST path routing and token recovery logic.
* **Credential Cascade:** Supports local environments by resolving paths dynamically starting from the user's config file down to local environment fallbacks.

---

## 2. Best practices for HVR operations

When designing workflows or orchestrating replication pipelines, ensure the following safety measures are observed:

### Preflight check for target alterations
Before running tools that apply DDL modifications to target databases (like `create_alter_target_tables`), always test target database egress pathing:
```python
# Best practice pattern inside your orchestrator
is_connected = await client.execute_tool("test_target_connectivity_for_table", {
    "hub": "hvrhub",
    "channel": "test_ch2",
    "location": "snowflake_target",
    "table_name": "target_table"
})
if not is_connected:
    raise ConnectionError("Destination Snowflake warehouse is unreachable from the HVR Hub.")
```

### Schema drift validation
Always check layout discrepancies using `adapt_check_tables` before executing `adapt_apply_tables`. This allows the orchestrator or agent to review structural differences and warn the operator of any destructive changes (e.g., column drops or type changes).

---

## 3. How to customize and add new tools

This walkthrough demonstrates how to add a new tool to [hvr_mcp_server.py](file:///Users/elijah.davis/Documents/code/api/hvr/hvr_mcp/hvr_mcp_server.py).

### Example: Adding `get_channel_actions`

We want to add a tool to query replication actions (such as `Capture` or `Integrate`) defined on a specific HVR channel. This requires:
1. Adding an API call to the HVRAPIClient class.
2. Registering the new FastMCP tool using the `@mcp.tool()` decorator.
3. Testing the new tool using a JSON-RPC command or the MCP Inspector.

#### Step 1: Implement the client API method
Open server.py and add the following method to the HVRAPIClient class (around the channel management section):

```python
    def get_channel_actions(self, channel_name: str) -> List[Dict[str, Any]]:
        """Fetch all replication actions associated with a specific channel."""
        url = f"{self.base_url}/api/latest/hubs/hvrhub/definition/channels/{channel_name}/actions"
        response = self._request("GET", url, verify=False)
        return response.json()
```

#### Step 2: Register the FastMCP tool
Scroll to the registered tool section in server.py and add the tool declaration:

```python
@mcp.tool()
def get_channel_actions(channel_name: str) -> str:
    """Fetch all replication actions (e.g. Capture, Integrate) defined on a specific HVR channel."""
    try:
        return json.dumps(get_client().get_channel_actions(channel_name), indent=2)
    except Exception as e:
        return f"Error: {str(e)}"
```

> [!TIP]
> **Write high-quality docstrings**
> FastMCP reads your tool's docstring and exposes it to the calling LLM model. Write descriptive, clear docstrings so the agent understands exactly when to invoke your new tool.

---

## 4. Validation and testing

To verify your custom tool behaves correctly, execute the following diagnostic tests:

### CLI JSON-RPC handshake test
Execute the server locally, feeding it a JSON-RPC payload matching your new tool name over standard input:

```bash
(echo '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2024-11-05","capabilities":{},"clientInfo":{"name":"tester","version":"1.0"}}}'; echo '{"jsonrpc":"2.0","method":"notifications/initialized"}'; echo '{"jsonrpc": "2.0", "method": "tools/call", "params": {"name": "get_channel_actions", "arguments": {"channel_name": "test_ch2"}}, "id": 2}') | python3 /Users/elijah.davis/Documents/code/api/hvr/hvr_mcp/hvr_mcp_server.py
```

### Visual verification with MCP inspector
Start the MCP Inspector instance to test your new tool visually inside a local web UI:

```bash
npx -y @modelcontextprotocol/inspector python3 /Users/elijah.davis/Documents/code/api/hvr/hvr_mcp/hvr_mcp_server.py
```
This launches a browser interface listing all registered tools (including your new `get_channel_actions` tool) allowing you to execute it and review JSON responses directly.
