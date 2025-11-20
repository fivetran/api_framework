# Fivetran MCP Server via Gram.ai

Model Context Protocol (MCP) server for Fivetran operations, seamlessly integrated with Cursor IDE through Gram.ai.

**Visit the installation page**: [https://app.getgram.ai/mcp/test-demo-fivetran-api-mcp/install](https://app.getgram.ai/mcp/test-demo-fivetran-api-mcp/install)

## Features

- **Destination Management**: List and get details of Fivetran destinations
- **Connector Operations**: List, get details, and sync connectors
- **Metadata Retrieval**: Get connector type metadata and configuration requirements
- **Health Monitoring**: Review connector health and get recommendations
- **Object Summary**: Comprehensive overview of all Fivetran objects

## Prerequisites

- Cursor IDE installed
- Fivetran API credentials (API key and secret)

## Quick Start

### Step 1: Install via Gram.ai

1. **Visit the installation page**: [https://app.getgram.ai/mcp/test-demo-fivetran-api-mcp/install](https://app.getgram.ai/mcp/test-demo-fivetran-api-mcp/install)

2. **Select Cursor IDE** from the available IDE options

3. **Click Install** - This will open Cursor and prompt you to configure the MCP server

### Step 2: Enter Your Fivetran Credentials in Cursor

After clicking Install, Cursor will open and ask you to configure the MCP server. You **must** enter your Fivetran API credentials in the Cursor UI:

1. **Open your MCP configuration file**: `~/.cursor/mcp.json` (or the MCP configuration dialog that appears in Cursor)

2. **Find the `fivetran_api` MCP server configuration** section

3. **Enter your Fivetran credentials** in the `headers` section. You need to fill in these two exact fields:
   - **`Mcp-Fivetran-Api-Basic-Auth-Username`**: Enter your Fivetran API username/key here
   - **`Mcp-Fivetran-Api-Basic-Auth-Password`**: Enter your Fivetran API password/secret here
   
   **Important**: Make sure both header names are spelled exactly as shown above (case-sensitive).

   The configuration should look like this:
   ```json
   {
     "fivetran_api": {
       "url": "https://app.getgram.ai/mcp/test-demo-fivetran-api-mcp",
       "headers": {
         "Mcp-Fivetran-Api-Basic-Auth-Username": "your_fivetran_api_key_here",
         "Mcp-Fivetran-Api-Basic-Auth-Password": "your_fivetran_api_secret_here"
       }
     }
   }
   ```

4. **Save the configuration** - Cursor will automatically reload the MCP server

### Step 3: Restart Cursor

After entering your credentials, **restart Cursor IDE** completely to load the new MCP server.

### Step 4: Verify Integration

Once Cursor has restarted, verify the integration:

1. **Check MCP Status**: 
   - Look for the MCP server indicator in Cursor's status bar
   - Or check Cursor's developer console for MCP connection logs

2. **Test in Conversation**:
   - Start a new conversation in Cursor
   - Try asking: "List all my Fivetran destinations"
   - The AI should use the Fivetran MCP tools automatically

## Available Commands

You can now ask Cursor AI to:
- "List all my Fivetran destinations"
- "Show me details about connector [connector_id]"
- "Sync connector [connector_id]"
- "What's the health status of my connectors?"
- "Get metadata for Google Sheets connector type"
- "Get a summary of all my Fivetran objects"

## Troubleshooting

### MCP Server Not Connecting

- Ensure Node.js is installed: `node --version`
- Check Cursor's developer console (Help → Toggle Developer Tools) for error messages
- Verify `npx` is available: `which npx` (macOS/Linux) or `where.exe npx` (Windows)

### Authentication Errors

- Verify your Fivetran API credentials are correct
- **Check that both headers are filled in** in `~/.cursor/mcp.json`:
  - `Mcp-Fivetran-Api-Basic-Auth-Username` (must not be empty)
  - `Mcp-Fivetran-Api-Basic-Auth-Password` (must not be empty)
- Ensure the header names are spelled exactly as shown (case-sensitive)
- Test credentials independently with Fivetran API
- Verify the `fivetran_api` configuration exists in your `mcp.json` file

### Tools Not Appearing

- Restart Cursor completely after installation
- Verify the MCP server is connected in Cursor's settings

## Support

For issues and questions:
- Verify Fivetran API credentials and permissions
- Ensure Node.js and `npx` are properly installed
- Book a [call](https://go.fivetran.com/demo/services) with the Professional Services team 
