# Fivetran MCP Server Example

A streamlined Model Context Protocol (MCP) server for Fivetran operations, with steps for Cursor IDE integration.

## Features

- **Destination Management**: List and get details of Fivetran destinations
- **Connector Operations**: List, get details, and sync connectors
- **Metadata Retrieval**: Get connector type metadata and configuration requirements
- **Health Monitoring**: Review connector health and get recommendations
- **Object Summary**: Comprehensive overview of all Fivetran objects

## Quick Start

### 1. Prerequisites

- Python 3.11+
- Fivetran API credentials (API key and secret)
 - Cursor IDE

### 2. Configuration

**Recommended: Use Environment Variables (Secure)**

Set environment variables for your Fivetran credentials:

```bash
export FIVETRAN_API_KEY="your_fivetran_api_key_here"
export FIVETRAN_API_SECRET="your_fivetran_api_secret_here"
```

**Alternative: Configuration File**

Create a `configuration.json` file in the same directory as `mcp_simple.py` with your Fivetran credentials:

```json
{
    "fivetran_api_key": "YOUR_FIVETRAN_API_KEY",
    "fivetran_api_secret": "YOUR_FIVETRAN_API_SECRET"
}
```

**Priority Order:**
1. Environment variables (recommended for production)
2. Configuration file (fallback for development)

### 3. Cursor IDE Setup

This MCP server can be integrated directly into Cursor IDE, allowing you to interact with Fivetran through natural language commands in your conversations.

#### Step 1: Install Dependencies

First, ensure all Python dependencies are installed:

```bash
cd /path/to/mcp_simple
pip install -r requirements.txt
```

Or install the core dependency:
```bash
pip install mcp requests
```

#### Step 2: Configure Cursor MCP Settings

1. **Open Cursor Settings**: 
   - Press `Cmd+Shift+P` (Mac) or `Ctrl+Shift+P` (Windows/Linux)
   - Type "Preferences: Open User Settings (JSON)"
   - Or navigate to `Cursor → Settings → Extensions → Model Context Protocol`

2. **Edit MCP Configuration**:
   - Locate or create the MCP configuration file at `~/.cursor/mcp.json`
   - Add the following configuration:

```json
{
  "mcpServers": {
    "fivetran-simple": {
      "command": "python",
      "args": [
        "/absolute/path/to/mcp_simple/mcp_simple.py"
      ],
      "env": {
        "FIVETRAN_API_KEY": "your_fivetran_api_key_here",
        "FIVETRAN_API_SECRET": "your_fivetran_api_secret_here"
      }
    }
  }
}
```

**Important**: Replace `/absolute/path/to/mcp_simple/mcp_simple.py` with the absolute path to your `mcp_simple.py` file.

**Example configuration (with environment variables)**:
```json
{
  "mcpServers": {
    "fivetran-simple": {
      "command": "python",
      "args": [
        "/Users/elijah.davis/Documents/code/api/mcp/mcp_simple/mcp_simple.py"
      ]
    }
  }
}
```

If you're using environment variables already set in your shell, you can also use a configuration file instead:

```json
{
  "mcpServers": {
    "fivetran-simple": {
      "command": "python",
      "args": [
        "/Users/elijah.davis/Documents/code/api/mcp/mcp_simple/mcp_simple.py"
      ],
      "env": {
        "MCP_CONFIG_FILE": "/Users/elijah.davis/Documents/code/api/mcp/mcp_simple/configuration.json"
      }
    }
  }
}
```

#### Step 3: Restart Cursor

After updating the MCP configuration:
1. **Restart Cursor IDE** completely to load the new MCP server
2. Or use `Cmd+Shift+P` → "Developer: Reload Window"

#### Step 4: Verify Integration

Once Cursor has restarted, the MCP server should be available. To verify:

1. **Check MCP Status**: 
   - Look for the MCP server indicator in Cursor's status bar
   - Or check Cursor's developer console for MCP connection logs

2. **Test in Conversation**:
   - Start a new conversation in Cursor
   - Try asking: "List all my Fivetran destinations"
   - The AI should use the `list_destinations` tool from your MCP server

3. **Available Commands in Cursor**:
   You can now ask Cursor AI to:
   - "List all my Fivetran destinations"
   - "Show me details about connector [connector_id]"
   - "Sync connector [connector_id]"
   - "What's the health status of my connectors?"
   - "Get metadata for Google Sheets connector type"

#### Troubleshooting Cursor Integration

**Issue: MCP server not connecting**
- Verify the Python path is correct in `mcp.json`
- Check that the `mcp_simple.py` file is executable
- Ensure Python 3.11+ is available at the system `python` command
- Check Cursor's developer console (Help → Toggle Developer Tools) for error messages

**Issue: Authentication errors**
- Verify API credentials are correctly set in `env` section of `mcp.json` OR in `configuration.json`
- Check that environment variables are set if not using the `env` section
- Test credentials independently by running: `python mcp_simple.py` directly

**Issue: Tools not appearing**
- Restart Cursor completely after configuration changes
- Check that `mcp.json` is valid JSON (use a JSON validator)
- Verify the server name in `mcp.json` matches the server name in the code (`fivetran-simple`)

**Issue: Permission errors**
- Ensure the Python script has execute permissions: `chmod +x mcp_simple.py`
- Check that the path to `mcp_simple.py` uses absolute, not relative paths

### 4. Local Development

```bash
# Install dependencies
pip install -r requirements.txt

# Run the server locally (stdio mode for testing)
python mcp_simple.py
```

Note: When running locally, the server uses stdio transport by default. For Cursor IDE integration, the server will automatically use stdio transport when invoked through the MCP configuration.

 

## Available Tools

| Tool | Description |
|------|-------------|
| `list_destinations` | List all Fivetran destinations |
| `get_destination_details` | Get detailed information about a specific destination |
| `list_connectors` | List all connectors (optionally filtered by group) |
| `get_connector_details` | Get detailed information about a specific connector |
| `sync_connector` | Trigger a manual sync for a connector |
| `get_connector_metadata` | Get metadata for a connector type |
| `review_connector_health` | Review connector health and get recommendations |
| `get_object_summary` | Get comprehensive summary of all objects |

## Environment Variables

### Required (for authentication)
- `FIVETRAN_API_KEY`: Your Fivetran API key
- `FIVETRAN_API_SECRET`: Your Fivetran API secret

### Optional
- `MCP_CONFIG_FILE`: Path to configuration file (fallback if env vars not set)

## Architecture

- **FastMCP Framework**: Built on FastMCP for efficient MCP server implementation
- **Transport**: Uses stdio by default; optional HTTP via `MCP_TRANSPORT=http` and `MCP_PORT` env vars
- **Enterprise-Grade Reliability**: Includes retry logic, timeout protection, and comprehensive error handling

## File Structure

```
mcp_simple/
├── mcp_simple.py          # Main server implementation
├── test_mcp_simple.py     # Test script
├── requirements.txt       # Python dependencies
├── pyproject.toml         # uv package configuration
├── configuration.json    # Fivetran API credentials (optional, create if needed)
└── README_mcp_with_fivetran.md   # This file
```

**For Cursor IDE**, you only need:
- `mcp_simple.py` - The main server file
- `configuration.json` (optional) - If not using environment variables
- `requirements.txt` - For installing dependencies

## Testing

Run the test script to verify the server setup:

```bash
python test_mcp_simple.py
```

## Troubleshooting

### Common Issues

1. **Configuration File Not Found**
   - Ensure `configuration.json` exists with valid Fivetran credentials
   - Check file permissions and path

2. **API Authentication Errors**
   - Verify API key and secret are correct
   - Ensure credentials have appropriate permissions

3. **Port Already in Use**
   - If using HTTP transport, change the `MCP_PORT` environment variable
   - Check for other services using the selected port

### Logs

The server provides detailed logging for debugging:
- `[INFO]`: General information
- `[DEBUG]`: Detailed debugging information
- `[WARNING]`: Non-critical issues
- `[ERROR]`: Critical errors

## Security Notes

- **Use environment variables** for production deployments (recommended)
- Never commit `configuration.json` or `.env` files with real credentials
- For Cursor IDE: Prefer using environment variables in your system rather than hardcoding in `mcp.json`
- If using `mcp.json`, consider adding it to `.gitignore` if it contains sensitive data
- Environment variables are automatically excluded from logs and debugging output

### Cursor IDE Security Best Practices

1. **Don't commit `mcp.json` with credentials**: If you store credentials in `mcp.json`, ensure it's in your `.gitignore`
2. **Use system environment variables**: Set `FIVETRAN_API_KEY` and `FIVETRAN_API_SECRET` in your shell profile (`.zshrc`, `.bashrc`, etc.) rather than in `mcp.json`
3. **Use configuration file with proper permissions**: If using `configuration.json`, set restrictive file permissions: `chmod 600 configuration.json`

## Support

For issues and questions:
- Check the logs for detailed error messages
- Verify Fivetran API credentials and permissions
- Ensure all dependencies are properly installed
