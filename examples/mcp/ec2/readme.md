# Rapid adoption guide: Hosting MCP servers on EC2

This guide provides a method for deploying the Fivetran Quickstart Model Context Protocol (MCP) server (`server.py`) on a remote AWS EC2 instance and connecting it to your local AI assistants or IDEs.

---

## Prerequisites

- An active AWS EC2 instance (e.g., Amazon Linux 2023 or Ubuntu)
- An SSH key pair (`.pem` file) configured for access
- The `server.py` script on your local machine
- Your Fivetran API Key and API Secret

---

## Step 1: Prepare the EC2 environment

Connect to your EC2 instance and install Python and the specific packages required by the Fivetran quickstart server.

Run the following commands in your terminal:

```bash
# Connect to your instance
ssh -i /path/to/your-key.pem ec2-user@<ec2-ip-address>

# Update the system and install Python 3
sudo yum update -y
sudo yum install python3 python3-pip -y

# Install the exact packages required for the Fivetran quickstart server
pip3 install mcp requests

# Exit the SSH session to return to your local machine
exit
```

---

## Step 2: Deploy your script

Transfer the quickstart MCP `server.py` script from your local machine to the EC2 instance.

Example command from your **local machine**:

```bash
scp -i /path/to/your-key.pem /Users/name/Documents/code/api/mcp/server.py ec2-user@<ec2-ip-address>:/home/ec2-user/server.py
```

---

## Step 3: Test locally with MCP inspector

You can verify that the server runs and authenticates properly by testing it using the official MCP Inspector. Since the script uses environment variables for credentials, we pass them inline in the SSH command block.

Example command on your **local machine**:

```bash
npx -y @modelcontextprotocol/inspector ssh -i /path/to/your-key.pem ec2-user@<ec2-ip-address> "FIVETRAN_API_KEY=your_key_here FIVETRAN_API_SECRET=your_secret_here python3 /home/ec2-user/server.py"
```

Open the local URL generated (usually `http://localhost:5173`) in your browser to inspect tools and run tests.

---

## Step 4: Configure your local client

Add the hosted MCP server to your local clients by passing the environment variables inline in the SSH configuration.

### VS Code configuration
If you are using an AI coding assistant in VS Code that supports MCP, configure it via the `mcp.json` file.

File location: ~/Library/Application Support/Code/User/mcp.json

1. Open the VS Code command palette (`Cmd+Shift+P` on Mac, `Ctrl+Shift+P` on Windows/Linux).
2. Type and select **MCP Servers** (or the equivalent for your extension).
3. Add your EC2 server configuration:

```json
{
  "servers": {
    "fivetran-ec2-quickstart": {
      "command": "ssh",
      "args": [
        "-i", 
        "/path/to/your-key.pem", 
        "ec2-user@<ec2-ip-address>", 
        "FIVETRAN_API_KEY=your_key_here FIVETRAN_API_SECRET=your_secret_here python3 /home/ec2-user/server.py"
      ]
    }
  }
}
```

### Cursor configuration
1. Open Cursor Settings -> Features -> MCP
2. Click **+ Add new MCP server**
3. Configure exactly as follows:
   - **Name**: `fivetran-ec2-quickstart`
   - **Type**: `command`
   - **Command**: `ssh`
   - **Arguments**: `["-i", "/path/to/your-key.pem", "ec2-user@<ec2-ip-address>", "FIVETRAN_API_KEY=your_key_here FIVETRAN_API_SECRET=your_secret_here python3 /home/ec2-user/server.py"]`

### Claude Desktop configuration
Open your `claude_desktop_config.json` file (typically at `~/Library/Application Support/Claude/claude_desktop_config.json` on Mac):

```json
{
  "mcpServers": {
    "fivetran-ec2-quickstart": {
      "command": "ssh",
      "args": [
        "-i", 
        "/path/to/your-key.pem", 
        "ec2-user@<ec2-ip-address>", 
        "FIVETRAN_API_KEY=your_key_here FIVETRAN_API_SECRET=your_secret_here python3 /home/ec2-user/server.py"
      ]
    }
  }
}
```

---

## Step 5: Example prompts to interact with your MCP server

Once your AI assistant is connected, you can use natural language prompts to invoke the tools exposed by `server.py`:

### 1. Test Fivetran connection
> **Prompt:** "Test the Fivetran connection to verify our credentials are working."
* **What happens:** The model calls `test_connection()` which lists groups and validates credentials.

### 2. View all active connectors
> **Prompt:** "Show me a list of all my Fivetran connectors."
* **What happens:** The model calls `list_connectors()` to fetch and display configured sources.

### 3. Check the status of a specific connector
> **Prompt:** "Get the detailed status for connector `connector_id_here`."
* **What happens:** The model calls `get_connector_status(connector_id="connector_id_here")`.

### 4. Pause or resume a connector
> **Prompt:** "Pause my Fivetran connector `connector_id_here`."
* **What happens:** The model calls `pause_connector(connector_id="connector_id_here")`.

---

## Best practices & security

> [!CAUTION]
> **Never open public ports**
> Do not configure AWS Security Groups to allow inbound TCP traffic for the MCP server. SSH (Port 22) is sufficient for this `stdio` setup and provides maximum security.

> [!NOTE]
> **Persistent sessions**
> Because MCP over SSH initiates a new session when your client connects, there is no need to set up `systemd` or background process managers (like PM2). The local IDE manages the lifecycle of the remote process automatically.
