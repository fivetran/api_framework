# Fivetran MCP Server via Gram Speakeasy

Enterprise-grade Model Context Protocol (MCP) server for Fivetran operations, seamlessly integrated with Cursor IDE through Gram.

## Features

- **Destination Management**: List and get details of Fivetran destinations
- **Connector Operations**: List, get details, and sync connectors
- **Metadata Retrieval**: Get connector type metadata and configuration requirements
- **Health Monitoring**: Review connector health and get recommendations
- **Object Summary**: Comprehensive overview of all Fivetran objects

## Prerequisites

- Cursor IDE installed
- Fivetran API credentials (username and password for basic auth)
- Node.js installed (for `npx` command)
- MCP_FIVETRAN_API_BASIC_AUTH_USERNAME: Your Fivetran API username
- MCP_FIVETRAN_API_BASIC_AUTH_PASSWORD: Your Fivetran API password

## Quick Start

### Step 1: Set Environment Variables (Required)

**Set these environment variables BEFORE installation** to ensure a seamless setup experience.

#### macOS / Linux

Add to your shell profile (`~/.zshrc`, `~/.bashrc`, or `~/.bash_profile`):

```bash
export MCP_FIVETRAN_API_BASIC_AUTH_USERNAME="your_fivetran_username_here"
export MCP_FIVETRAN_API_BASIC_AUTH_PASSWORD="your_fivetran_password_here"
```

Then reload your shell:
```bash
source ~/.zshrc  # or ~/.bashrc
```

#### Windows (PowerShell)

```powershell
$env:MCP_FIVETRAN_API_BASIC_AUTH_USERNAME="your_fivetran_username_here"
$env:MCP_FIVETRAN_API_BASIC_AUTH_PASSWORD="your_fivetran_password_here"
```

For permanent setup, add to System Environment Variables or use:
```powershell
[System.Environment]::SetEnvironmentVariable("MCP_FIVETRAN_API_BASIC_AUTH_USERNAME", "your_value", "User")
[System.Environment]::SetEnvironmentVariable("MCP_FIVETRAN_API_BASIC_AUTH_PASSWORD", "your_value", "User")
```

#### Verify Environment Variables Are Set

Before proceeding to installation, verify your environment variables are configured correctly:

**macOS / Linux:**
```bash
[ -n "$MCP_FIVETRAN_API_BASIC_AUTH_USERNAME" ] && echo "✓ Username: ${MCP_FIVETRAN_API_BASIC_AUTH_USERNAME:0:1}***${MCP_FIVETRAN_API_BASIC_AUTH_USERNAME: -1}" || echo "✗ Username not set"
[ -n "$MCP_FIVETRAN_API_BASIC_AUTH_PASSWORD" ] && echo "✓ Password: ${MCP_FIVETRAN_API_BASIC_AUTH_PASSWORD:0:1}***${MCP_FIVETRAN_API_BASIC_AUTH_PASSWORD: -1}" || echo "✗ Password not set"
```

**Windows (PowerShell):**
```powershell
if($env:MCP_FIVETRAN_API_BASIC_AUTH_USERNAME){$u=$env:MCP_FIVETRAN_API_BASIC_AUTH_USERNAME;Write-Host "✓ Username: $($u[0..2] -join '')***$($u[-3..-1] -join '')" -ForegroundColor Green}else{Write-Host "✗ Username not set" -ForegroundColor Red}
if($env:MCP_FIVETRAN_API_BASIC_AUTH_PASSWORD){$p=$env:MCP_FIVETRAN_API_BASIC_AUTH_PASSWORD;Write-Host "✓ Password: $($p[0..1] -join '')***$($p[-2..-1] -join '')" -ForegroundColor Green}else{Write-Host "✗ Password not set" -ForegroundColor Red}
```

**Expected Output:**
```
✓ Username: a***z
✓ Password: 1***9
```

If both show ✓, proceed to Step 2. If you see ✗, ensure variables are exported and reload your shell.

### Step 2: Install via Gram.ai

1. **Visit the installation page**: [https://app.getgram.ai/mcp/test-demo-fivetran-api-mcp/install](https://app.getgram.ai/mcp/test-demo-fivetran-api-mcp/install)

2. **Select Cursor IDE** from the available IDE options

3. **Click Install** - Gram.ai will automatically configure Cursor with the correct MCP settings

The installation will use your environment variables (`MCP_FIVETRAN_API_BASIC_AUTH_USERNAME` and `MCP_FIVETRAN_API_BASIC_AUTH_PASSWORD`) that you set in Step 1.

### Step 3: Restart Cursor

After installation:
1. **Restart Cursor IDE** completely to load the new MCP server

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

## Manual Configuration (Advanced)

If you prefer manual configuration or need to customize settings, edit `~/.cursor/mcp.json`:

```json
{
  "mcpServers": {
    "fivetran-gram": {
      "command": "npx",
      "args": [
        "mcp-remote",
        "https://app.getgram.ai/mcp/test-demo-fivetran-api-mcp",
        "--header",
        "Mcp-Fivetran-Api-Basic-Auth-Username:${MCP_FIVETRAN_API_BASIC_AUTH_USERNAME}",
        "--header",
        "Mcp-Fivetran-Api-Basic-Auth-Password:${MCP_FIVETRAN_API_BASIC_AUTH_PASSWORD}"
      ],
      "env": {
        "MCP_FIVETRAN_API_BASIC_AUTH_USERNAME": "<your-value-here>",
        "MCP_FIVETRAN_API_BASIC_AUTH_PASSWORD": "<your-value-here>"
      }
    }
  }
}
```

**Note**: If environment variables are already set system-wide, you can omit the `env` section.

## Troubleshooting

### MCP Server Not Connecting

- **First, verify environment variables are set** using the verification commands in Step 1
- Ensure Node.js is installed: `node --version`
- Check Cursor's developer console (Help → Toggle Developer Tools) for error messages
- Verify `npx` is available: `which npx` (macOS/Linux) or `where.exe npx` (Windows)

### Authentication Errors

- Verify credentials are correctly set in environment variables
- Test credentials independently with Fivetran API
- Ensure environment variables are loaded in the shell where Cursor is launched
- On macOS/Linux, ensure variables are exported (use `export` keyword)

### Tools Not Appearing

- Restart Cursor completely after configuration changes
- Verify `mcp.json` is valid JSON (use a JSON validator)
- Check that environment variables are accessible to Cursor (may require restarting Cursor after setting variables)

### Environment Variables Not Found

- **Run the verification commands from Step 1** to check if variables are set
- Ensure variables are set in the shell profile that Cursor uses
- On macOS, Cursor may use a login shell - add variables to `~/.zprofile` or `~/.zshrc`
- Verify variables persist after terminal restart: `env | grep MCP_FIVETRAN` (macOS/Linux) or `$env:MCP_FIVETRAN_API_BASIC_AUTH_USERNAME` (Windows)
- Reload your shell profile: `source ~/.zshrc` (or `~/.bashrc`) after adding variables

## Environment Variables

### Required

- `MCP_FIVETRAN_API_BASIC_AUTH_USERNAME`: Your Fivetran API username
- `MCP_FIVETRAN_API_BASIC_AUTH_PASSWORD`: Your Fivetran API password

## Security Best Practices

1. **Use environment variables**: Never hardcode credentials in `mcp.json`
2. **Restrict file permissions**: If using a config file, set permissions: `chmod 600 mcp.json`
3. **Don't commit credentials**: Add `mcp.json` to `.gitignore` if it contains sensitive data
4. **Rotate credentials regularly**: Update environment variables periodically for enhanced security
5. **Use secure storage**: Consider using a secrets manager for production environments

## Support

For issues and questions:
- Check Cursor's developer console for detailed error messages
- Verify Fivetran API credentials and permissions
- Ensure Node.js and `npx` are properly installed
- Review environment variable configuration

