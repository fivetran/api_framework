# AWS S3 Fivetran Approval MCP Server

This MCP utilizes AWS S3 as the data store for the `mcp_approvals.json` configuration

## Table of Contents
1. [Prerequisites](#prerequisites)
2. [Configuration Setup](#configuration-setup)
3. [VS Code / Cursor Integration](#vs-code--cursor-integration)
4. [Testing & Verification](#testing--verification)
5. [Administrative Approval Workflow](#administrative-approval-workflow)

---

## Prerequisites

Ensure you have the required dependencies. If they are missing, the server will attempt to auto-install them via pip:
- `mcp` (FastMCP)
- `boto3` (AWS SDK for Python)
- `requests` (Fivetran API communications)

---

## Configuration Setup

The server reads configuration from two sources: the global `/configuration.json` file and fallback environment variables.

### 1. `configuration.json` (Recommended)
Add your AWS S3 settings and Fivetran API credentials directly to the configuration file:

```json
{
  "fivetran_api_key": "YOUR_FIVETRAN_API_KEY",
  "fivetran_api_secret": "YOUR_FIVETRAN_API_SECRET",
  "aws_access_key_id": "YOUR_AWS_ACCESS_KEY_ID",
  "aws_secret_access_key": "YOUR_AWS_SECRET_ACCESS_KEY",
  "aws_region": "us-east-1",
  "aws_s3_bucket": "your-approvals-s3-bucket",
  "aws_s3_key": "mcp_approvals.json"
}
```

### 2. Environment Variables (Alternative)
If AWS keys are not provided in `configuration.json`, the server defaults to standard AWS credentials:
* `AWS_ACCESS_KEY_ID`
* `AWS_SECRET_ACCESS_KEY`
* `AWS_DEFAULT_REGION` or `AWS_REGION`
* `AWS_S3_BUCKET` (Required if not in `configuration.json`)
* `AWS_S3_KEY` (Optional; defaults to `mcp_approvals.json`)

---

## VS Code / Cursor Integration

To load this MCP server in VS Code (with Cline, Roo Code, Claude Dev, or Cursor), add the following server configurations:

### Cline / Roo Code / Roo Clinic (MCP Settings)
Usually configured at `~/Library/Application Support/Code/User/globalStorage/saoudrizwan.claude-dev/settings/cline_mcp_settings.json` (location varies by extension):

```json
{
  "mcpServers": {
    "fivetran-s3-approvals": {
      "command": "python",
      "args": ["/mcp_fivetran_s3_approve.py"],
      "env": {
        "FIVETRAN_MCP_APPROVAL": "YOUR_BASE64_BASIC_AUTH_VAL"
      }
    }
  }
}
```

> [!IMPORTANT]
> **Generating the `FIVETRAN_MCP_APPROVAL` Value**
> The `FIVETRAN_MCP_APPROVAL` environment variable is required to execute administrative actions (`approve_request`, `reject_request`, `set_approval_mode`).
>
> Construct this value by base64-encoding the string `api_key:api_secret`:
> ```bash
> echo -n "YOUR_FIVETRAN_API_KEY:YOUR_FIVETRAN_API_SECRET" | base64
> ```
> Put the output string directly as the value of `FIVETRAN_MCP_APPROVAL`.

### Cursor Desktop Setup
1. Open Cursor Settings -> Features -> **MCP**.
2. Click **+ Add New MCP Server**.
3. Fill in:
   - **Name**: `fivetran-s3-approvals`
   - **Type**: `command`
   - **Command**: `python /mcp_fivetran_s3_approve.py`
4. Click **Save**.

---

## Testing & Verification

You can test and run this MCP server manually in the command line using `mcp dev` to verify S3 permissions, connectivity, and configuration:

```bash
# Install FastMCP CLI utilities globally
pip install mcp-cli

# Run MCP server in development inspector mode
mcp dev /mcp_fivetran_s3_approve.py
```
This starts an interactive browser inspector UI where you can invoke tools manually and inspect S3 file generation.

### Quick Command-Line Execution
Run directly using Python:
```bash
python /mcp_fivetran_s3_approve.py
```
*(It will run in stdio transport mode and output logs/errors to `stderr` while keeping `stdout` reserved for JSON-RPC messages).*

---

## Administrative Approval Workflow

### 1. Intercepting Actions
When a user attempts a mutating action (e.g. `pause_connector`):
```json
{
  "success": true,
  "message": "Pause Connector request submitted successfully and is being processed.",
  "status": "pending_approval",
  "note": "Your request has been received and will be processed shortly."
}
```
This automatically writes a pending record to `s3://<your-bucket>/mcp_approvals.json`.

### 2. Listing Approvals
Admin lists pending requests:
```python
list_approval_requests(status="PENDING")
```

### 3. Approving a Request
Admin approves the request (requires the `FIVETRAN_MCP_APPROVAL` env variable to be properly configured as explained in Cline/Cursor setup):
```python
approve_request(request_id="req_1717436034123_4523")
```
This updates the status in S3 to `APPROVED`, executes the action using the Fivetran API, updates status to `EXECUTED`, and stores the response payload.
