## Quick Demo README — Fivetran Connector SDK + Claude Code

This document covers cloning the repo, preparing a project, prompting Claude 4.5 Sonnet, testing with the SDK CLI, inspecting replicated data with DuckDB, and deploying a Fivetran Connector SDK.

### - 0) Prereqs

- Git installed
- Python 3.9+ and pip
- Fivetran Connector SDK & CLI (pip install fivetran-connector-sdk)
- DuckDB CLI (brew install duckdb, choco install duckdb, or download from duckdb.org)
- Access to Claude 4.5 Sonnet and Claude Code (editor/IDE)

### 1) Clone the repo & prepare a new project folder

``` python
# 1) Clone the repo and move into the SDK demo directory
git clone https://github.com/fivetran/fivetran_connector_sdk.git
cd fivetran_connector_sdk/all_things_ai/ai_agents/claude_code/.claude/agents

# 2) Ask viewer for a project name (or set one now)
export NEW_PROJECT="fda_food_enforcement_demo"   # <- change as desired

# 3) Create a fresh folder
mkdir -p "$NEW_PROJECT"

# 4) Seed a Claude prompt file from the agents content
#    (Use the ft-csdk-generate.md agent as your base prompt)
cat claude/agents/ft-csdk-generate.md > "$NEW_PROJECT/claude.md"

# 5) Add a notes file for endpoint details the agent can reference
cat > "$NEW_PROJECT/notes.txt" << 'EOF'
Endpoint: https://api.fda.gov/food/enforcement.json
Notes:
- Public FDA food enforcement recalls endpoint
- Use limit=10 for initial build to minimize API usage
- No API key required by default
- Expect nested JSON; let Fivetran infer schema beyond primary key
- Incremental upsert with state tracking by table (checkpoints)
EOF
```

### 2) Claude Code: Generate the Connector
Navigate to your new project directory and use Claude Code to generate the connector:

**Navigate to the project directory**
```bash
cd "$NEW_PROJECT"
```
**Use Claude Code with Claude 4.5 Sonnet to generate the connector**
```bash
claude code --model claude-sonnet-4-5-20250929 "Build me a Fivetran Connector SDK solution for the FDA endpoint at https://api.fda.gov/food/enforcement.json. 

Requirements:
- Incrementally upsert data and track state through checkpoints by table
- Use limit=10 for initial build to minimize API calls
- Default to no API key required
- Exit gracefully with clear comments and strategic log.info messages
- Define only the primary key and let Fivetran infer the rest of the schema

Reference files:
- Best practices: ../claude/agents/ft-csdk-generate.md
- Endpoint notes: ./notes.txt

Create all necessary files (configuration.json, connector.py, etc.) in the current directory."
```

**Alternative approach using interactive mode:**
Navigate to your new project directory and use Claude Code to generate the connector:
**Navigate to the project directory**
```bash
cd "$NEW_PROJECT"
```

**Start Claude Code in interactive mode with Claude 4.5 Sonnet**
```bash
claude code --model claude-sonnet-4-5-20250929
```
**Then paste your prompt:**
**"Build me a Fivetran Connector SDK solution... [full prompt as above]"**
Claude Code will generate the connector structure including:

- configuration.json
- connector.py
- Any additional required files

### 3) Claude Code: Test with the SDK

Open the project in Claude Code (or your IDE). Confirm the generated files are inside "$NEW_PROJECT".

From terminal:
```bash
cd "$NEW_PROJECT"
```

## (Optional) show generated files

``` bash
ls -1
```

## Run a local debug with your configuration
``` bash
fivetran debug --configuration "configuration.json"
```

**You should see logs indicating:**

- API call to https://api.fda.gov/food/enforcement.json with limit=10
- Rows discovered & written
- State/checkpoint updates
- Clean exit with log.info breadcrumbs
- If something fails, ask Claude Code inline:

``` bash
“Fix any issues preventing fivetran debug from completing successfully. Keep limit=10, no API key, maintain incremental upsert and checkpoint state by table.”
```

**Re-run:**

``` bash
fivetran debug --configuration "configuration.json"
```

### 4) Inspect replicated data with DuckDB

If the connector outputs local data files (e.g., ./data/ as CSV/Parquet) or a staging path, point DuckDB at them. Common quick patterns:

## Example: if data was written to ./tester/warehouse
``` bash
duckdb -c "SELECT * FROM '/tester.warehouse.fda_food_enforcement' LIMIT 20;"
```

### 5) Deploy

When debug looks good:

## From inside the project folder
```bash
fivetran deploy
```
What it does: packages & deploys your Connector SDK project per the configuration in the repo. If your environment requires auth or a target workspace, provide/confirm those per your standard setup before deployment.

# That’s it!
