# Fivetran Connection State Manager

A Python application built with DBOS that manages Fivetran connection state with durable execution and automatic recovery.

## Features

- **Fivetran State Management**: Get, update, and verify Fivetran connection state
- **DBOS Workflows**: Reliable execution with automatic recovery using DBOS workflows and steps
- **Error Recovery**: Automatically resumes connections if state updates fail
- **RESTful API**: Clean FastAPI endpoints for state management operations

## Setup

### 1. Install and Initialize DBOS

Run these commands in a clean directory to install DBOS Python locally:

```bash
pip install dbos
```

The first command installs the DBOS Python library in your environment.

### 2. Install Additional Dependencies

```bash
pip install fastapi uvicorn httpx pydantic
```

Or create a `requirements.txt` file:

```txt
dbos
fastapi
uvicorn[standard]
httpx
pydantic
```

Then install:

```bash
pip install -r requirements.txt
```

### 3. Set Up Database Connection

Since you have Postgres running on localhost:5432, set the environment variables:

```bash
export DBOS_SYSTEM_DATABASE_URL="postgresql://postgres:postgres@localhost:5432/dbos_system"
export DBOS_DATABASE_URL="postgresql://postgres:postgres@localhost:5432/dbos_app"
```

**Note**: Replace `postgres:postgres` with your actual Postgres username and password. If your Postgres doesn't have a password, use:

```bash
export DBOS_SYSTEM_DATABASE_URL="postgresql://postgres@localhost:5432/dbos_system"
export DBOS_DATABASE_URL="postgresql://postgres@localhost:5432/dbos_app"
```

If you don't have these databases created yet, create them using one of these methods:

**Option 1: Using pgAdmin 4**

1. Open pgAdmin 4 and connect to your PostgreSQL server (localhost:5432)
2. Right-click on "Databases" in the left sidebar
3. Select "Create" → "Database..."
4. In the "Database" field, enter `dbos_system`
5. Click "Save" to create the database
6. Repeat steps 2-5 to create `dbos_app` database

**Option 2: Using Command Line**

```bash
createdb dbos_system
createdb dbos_app
```

**Option 3: Using psql**

```sql
psql -U postgres -h localhost
CREATE DATABASE dbos_system;
CREATE DATABASE dbos_app;
\q
```

### 4. Start Your App

Start your app with this command:

```bash
python3 state_mgr.py
```

To see that your app is working, visit this URL in your browser: http://localhost:8000/

This app lets you test the reliability of DBOS. Launch a workflow, then crash the app. Restart it and watch it seamlessly recover from where it left off.

### 5. Register Your App (Optional)

To register your app with the DBOS Console:

1. Register your app with the DBOS Console using the name `fivetran-state-manager`
2. Follow the interactive guide to get your conductor key
3. Set the conductor key as an environment variable:

```bash
export DBOS_CONDUCTOR_KEY="your-conductor-key-here"
```

The app will automatically use the conductor key if it's set in the environment. This enables full DBOS Console integration and monitoring.

## Usage

### API Endpoints

#### 1. Get Current Connection State

```bash
curl -X POST "http://localhost:8000/state/get" \
  -H "Content-Type: application/json" \
  -d '{
    "api_key": "your-api-key",
    "api_secret": "your-api-secret",
    "connection_id": "your-connection-id"
  }'
```

#### 2. Update Connection State

```bash
curl -X POST "http://localhost:8000/state/update" \
  -H "Content-Type: application/json" \
  -d '{
    "api_key": "your-api-key",
    "api_secret": "your-api-secret",
    "connection_id": "your-connection-id",
    "new_state": {
      "cursor": "2025-03-06 20:20:20"
    }
  }'
```

#### 3. Get API Documentation

```bash
curl http://localhost:8000/
```

### Example: Update State Workflow

The update workflow performs these steps automatically:

1. **Get Current State** - Retrieves the current connection state
2. **Pause Connection** - Pauses the connection to allow safe state updates
3. **Update State** - Updates the connection state with new values
4. **Resume Connection** - Resumes the connection
5. **Verify Update** - Verifies that the state was updated correctly

If any step fails, the workflow automatically attempts to resume the connection to prevent leaving it in a paused state.

### Python Example

```python
import requests

# Get current state
response = requests.post(
    "http://localhost:8000/state/get",
    json={
        "api_key": "your-api-key",
        "api_secret": "your-api-secret",
        "connection_id": "your-connection-id"
    }
)
print(response.json())

# Update state
response = requests.post(
    "http://localhost:8000/state/update",
    json={
        "api_key": "your-api-key",
        "api_secret": "your-api-secret",
        "connection_id": "your-connection-id",
        "new_state": {
            "cursor": "2025-03-06 20:20:20",
            "last_sync": "2025-01-15T10:30:00Z"
        }
    }
)
print(response.json())
```

## Architecture

- **state_mgr.py**: Main FastAPI application with DBOS integration
- **DBOS Steps**: Individual operations (get state, pause, update, resume, verify) are DBOS steps
- **DBOS Workflows**: Orchestrates the complete state update process with error recovery

## DBOS Features Used

- **Workflows**: Each API operation is wrapped in a DBOS workflow for reliability
- **Steps**: Individual operations (get state, pause connection, update state, resume connection, verify) are DBOS steps
- **Automatic Recovery**: If the application crashes, DBOS will recover workflows from the last completed step
- **Durable Execution**: All workflow state is checkpointed to the database

## Environment Variables

- `DBOS_SYSTEM_DATABASE_URL`: PostgreSQL connection string for DBOS system database (optional, defaults to SQLite)
- `DBOS_DATABASE_URL`: PostgreSQL connection string for application database (optional)
- `DBOS_CONDUCTOR_KEY`: Conductor key for DBOS Console registration (optional, enables full DBOS Console integration)

## Testing Reliability

To test DBOS's reliability:

1. Start a state update workflow by making a POST request to `/state/update`
2. While the workflow is running, crash the application (Ctrl+C or kill the process)
3. Restart the application: `python3 state_mgr.py`
4. DBOS will automatically recover the workflow from the last completed step and continue execution

## License

MIT
