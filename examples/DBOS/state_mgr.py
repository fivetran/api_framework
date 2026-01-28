"""
Fivetran Connection State Manager with DBOS
Manages Fivetran connection state using DBOS workflows for reliability and durability.
"""
import os
import json
import base64
import time
from typing import Dict, Any, Optional, List

import uvicorn
import httpx
from dbos import DBOS, DBOSConfig
from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse, HTMLResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

app = FastAPI()

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# DBOS Configuration
config: DBOSConfig = {
    "name": "fivetran-state-manager",
    "system_database_url": os.environ.get("DBOS_SYSTEM_DATABASE_URL"),
    "application_database_url": os.environ.get("DBOS_DATABASE_URL"),
}
conductor_key = os.environ.get("DBOS_CONDUCTOR_KEY", None)
DBOS(fastapi=app, config=config, conductor_key=conductor_key)

# Request Models
class StateUpdateRequest(BaseModel):
    api_key: str
    api_secret: str
    connection_id: str
    new_state: Optional[Dict[str, Any]] = None

class GetStateRequest(BaseModel):
    api_key: str
    api_secret: str
    connection_id: str

class MultiStateUpdateRequest(BaseModel):
    api_key: str
    api_secret: str
    connection_ids: List[str]
    new_state: Optional[Dict[str, Any]] = None

class MultiGetStateRequest(BaseModel):
    api_key: str
    api_secret: str
    connection_ids: List[str]

##################################
#### Helper Functions
##################################

def get_headers(api_key: str, api_secret: str) -> Dict[str, str]:
    """Generate authentication headers for Fivetran API"""
    auth_string = f"{api_key}:{api_secret}"
    auth_bytes = auth_string.encode('utf-8')
    auth_b64 = base64.b64encode(auth_bytes).decode('utf-8')
    return {
        'Authorization': f'Basic {auth_b64}',
        'Accept': 'application/json;version=2',
        'Content-Type': 'application/json',
    }


def make_api_request(
    method: str,
    endpoint: str,
    api_key: str,
    api_secret: str,
    payload: Optional[Dict[str, Any]] = None,
    timeout: int = 90
) -> Dict[str, Any]:
    """
    Make HTTP request to Fivetran API with error handling
    
    Args:
        method: HTTP method (GET, POST, PATCH, DELETE)
        endpoint: API endpoint (relative to base URL)
        api_key: Fivetran API key
        api_secret: Fivetran API secret
        payload: Optional request payload
        timeout: Request timeout in seconds
    
    Returns:
        Response JSON as dictionary
    
    Raises:
        Exception: If request fails
    """
    base_url = 'https://api.fivetran.com/v1'
    url = f"{base_url}/{endpoint}"
    headers = get_headers(api_key, api_secret)
    
    try:
        with httpx.Client(timeout=timeout) as client:
            if method == 'GET':
                response = client.get(url, headers=headers)
            elif method == 'POST':
                response = client.post(url, headers=headers, json=payload)
            elif method == 'PATCH':
                response = client.patch(url, headers=headers, json=payload)
            elif method == 'DELETE':
                response = client.delete(url, headers=headers)
            else:
                raise ValueError(f'Invalid request method: {method}')
            
            response.raise_for_status()
            return response.json() if response.text else {}
    except httpx.HTTPStatusError as e:
        error_msg = f"HTTP {e.response.status_code}: {e.response.reason_phrase}"
        try:
            error_json = e.response.json()
            if 'message' in error_json:
                error_msg = f"Fivetran API Error: {error_json['message']}"
        except Exception:
            pass
        raise Exception(error_msg)
    except httpx.RequestError as e:
        raise Exception(f"Request failed: {str(e)}")


##################################
#### DBOS Steps
##################################

@DBOS.step()
def get_connection_state_step(api_key: str, api_secret: str, connection_id: str) -> Dict[str, Any]:
    """
    Step to retrieve current connection state from Fivetran API
    
    Args:
        api_key: Fivetran API key
        api_secret: Fivetran API secret
        connection_id: Fivetran connection ID
    
    Returns:
        Current connection state dictionary
    """
    DBOS.logger.info(f"Getting current state for connection: {connection_id}")
    endpoint = f'connections/{connection_id}/state'
    time.sleep(10)
    result = make_api_request('GET', endpoint, api_key, api_secret)
    DBOS.logger.info(f"Successfully retrieved state for connection: {connection_id}")
    return result


@DBOS.step()
def pause_connection_step(api_key: str, api_secret: str, connection_id: str) -> Dict[str, Any]:
    """
    Step to pause a Fivetran connection
    
    Args:
        api_key: Fivetran API key
        api_secret: Fivetran API secret
        connection_id: Fivetran connection ID
    
    Returns:
        Connection update response
    """
    DBOS.logger.info(f"Pausing connection: {connection_id}")
    endpoint = f'connections/{connection_id}'
    payload = {"paused": True}
    time.sleep(10)
    result = make_api_request('PATCH', endpoint, api_key, api_secret, payload)
    DBOS.logger.info(f"Successfully paused connection: {connection_id}")
    return result


@DBOS.step()
def update_state_step(
    api_key: str,
    api_secret: str,
    connection_id: str,
    new_state: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Step to update connection state
    
    Args:
        api_key: Fivetran API key
        api_secret: Fivetran API secret
        connection_id: Fivetran connection ID
        new_state: New state dictionary to set
    
    Returns:
        State update response
    """
    DBOS.logger.info(f"Updating state for connection: {connection_id}")
    endpoint = f'connections/{connection_id}/state'
    payload = {"state": new_state}
    time.sleep(10)
    result = make_api_request('PATCH', endpoint, api_key, api_secret, payload)
    DBOS.logger.info(f"Successfully updated state for connection: {connection_id}")
    return result


@DBOS.step()
def resume_connection_step(api_key: str, api_secret: str, connection_id: str) -> Dict[str, Any]:
    """
    Step to resume a paused Fivetran connection
    
    Args:
        api_key: Fivetran API key
        api_secret: Fivetran API secret
        connection_id: Fivetran connection ID
    
    Returns:
        Connection update response
    """
    DBOS.logger.info(f"Resuming connection: {connection_id}")
    endpoint = f'connections/{connection_id}'
    payload = {"paused": False}
    time.sleep(10)
    result = make_api_request('PATCH', endpoint, api_key, api_secret, payload)
    DBOS.logger.info(f"Successfully resumed connection: {connection_id}")
    return result


@DBOS.step()
def verify_state_step(api_key: str, api_secret: str, connection_id: str) -> Dict[str, Any]:
    """
    Step to verify the updated connection state
    
    Args:
        api_key: Fivetran API key
        api_secret: Fivetran API secret
        connection_id: Fivetran connection ID
    
    Returns:
        Current connection state dictionary
    """
    DBOS.logger.info(f"Verifying state for connection: {connection_id}")
    endpoint = f'connections/{connection_id}/state'
    time.sleep(10)
    result = make_api_request('GET', endpoint, api_key, api_secret)
    DBOS.logger.info(f"Successfully verified state for connection: {connection_id}")
    return result


##################################
#### DBOS Workflows
##################################

@app.post("/state/get")
@DBOS.workflow()
def get_state_workflow(request: GetStateRequest):
    """
    Workflow to get current connection state
    
    Args:
        request: GetStateRequest with API credentials and connection_id
    
    Returns:
        Current connection state
    """
    DBOS.logger.info(f"Starting get state workflow for connection: {request.connection_id}")
    state = get_connection_state_step(
        request.api_key,
        request.api_secret,
        request.connection_id
    )
    return state


@app.post("/state/update")
@DBOS.workflow()
def update_state_workflow(request: StateUpdateRequest):
    """
    Workflow to update connection state with full error recovery
    
    This workflow:
    1. Gets current state
    2. Pauses the connection
    3. Updates the state
    4. Resumes the connection
    5. Verifies the update
    
    If any step fails, the workflow will attempt to resume the connection
    to prevent leaving it in a paused state.
    
    Args:
        request: StateUpdateRequest with API credentials, connection_id, and new_state
    
    Returns:
        Updated state and verification result
    """
    DBOS.logger.info(f"Starting state update workflow for connection: {request.connection_id}")
    
    # Step 1: Get current state
    current_state = get_connection_state_step(
        request.api_key,
        request.api_secret,
        request.connection_id
    )
    
    # If no new state provided, return current state
    if request.new_state is None:
        DBOS.logger.info("No new state provided, returning current state")
        return {
            "current_state": current_state,
            "message": "No changes made to state"
        }
    
    # Step 2: Pause connection
    pause_connection_step(
        request.api_key,
        request.api_secret,
        request.connection_id
    )
    
    try:
        # Step 3: Update state
        update_result = update_state_step(
            request.api_key,
            request.api_secret,
            request.connection_id,
            request.new_state
        )
        
        # Step 4: Resume connection
        resume_connection_step(
            request.api_key,
            request.api_secret,
            request.connection_id
        )
        
        # Step 5: Verify update
        verified_state = verify_state_step(
            request.api_key,
            request.api_secret,
            request.connection_id
        )
        
        return {
            "current_state": current_state,
            "update_result": update_result,
            "verified_state": verified_state,
            "message": "State updated and verified successfully"
        }
    except Exception as e:
        # Attempt to resume connection if update failed
        DBOS.logger.error(f"State update failed: {str(e)}. Attempting to resume connection.")
        try:
            resume_connection_step(
                request.api_key,
                request.api_secret,
                request.connection_id
            )
        except Exception as resume_error:
            DBOS.logger.error(f"Failed to resume connection: {str(resume_error)}")
        
        raise HTTPException(
            status_code=500,
            detail=f"State update failed: {str(e)}"
        )


@app.post("/state/get-multiple")
@DBOS.workflow()
def get_state_multiple_workflow(request: MultiGetStateRequest):
    """
    Workflow to get current connection state for multiple connections
    
    Args:
        request: MultiGetStateRequest with API credentials and list of connection_ids
    
    Returns:
        Dictionary mapping connection_id to current state
    """
    DBOS.logger.info(f"Starting get state workflow for {len(request.connection_ids)} connections")
    results = {}
    for connection_id in request.connection_ids:
        state = get_connection_state_step(
            request.api_key,
            request.api_secret,
            connection_id
        )
        results[connection_id] = state
    return results


@app.post("/state/update-multiple")
@DBOS.workflow()
def update_state_multiple_workflow(request: MultiStateUpdateRequest):
    """
    Workflow to update connection state for multiple connections with all-or-nothing semantics
    
    This workflow processes each connection sequentially:
    1. Gets current state
    2. Pauses the connection
    3. Updates the state
    4. Resumes the connection
    5. Verifies the update
    
    If any step fails for any connection, the workflow will resume all previously paused
    connections and then fail. This ensures atomicity - either all connections are
    updated successfully, or none are left in a modified state.
    
    Args:
        request: MultiStateUpdateRequest with API credentials, list of connection_ids, and new_state
    
    Returns:
        Dictionary mapping connection_id to update result
    
    Raises:
        HTTPException: If any connection fails, after cleaning up all paused connections
    """
    DBOS.logger.info(f"Starting state update workflow for {len(request.connection_ids)} connections")
    results = {}
    paused_connections = []  # Track connections we've paused for cleanup
    
    try:
        for connection_id in request.connection_ids:
            # Step 1: Get current state
            current_state = get_connection_state_step(
                request.api_key,
                request.api_secret,
                connection_id
            )
            
            # If no new state provided, return current state
            if request.new_state is None:
                DBOS.logger.info(f"No new state provided for {connection_id}, returning current state")
                results[connection_id] = {
                    "current_state": current_state,
                    "message": "No changes made to state"
                }
                continue
            
            # Step 2: Pause connection
            pause_connection_step(
                request.api_key,
                request.api_secret,
                connection_id
            )
            paused_connections.append(connection_id)
            
            # Step 3: Update state
            update_result = update_state_step(
                request.api_key,
                request.api_secret,
                connection_id,
                request.new_state
            )
            
            # Step 4: Resume connection
            resume_connection_step(
                request.api_key,
                request.api_secret,
                connection_id
            )
            paused_connections.remove(connection_id)  # Successfully resumed, remove from tracking
            
            # Step 5: Verify update
            verified_state = verify_state_step(
                request.api_key,
                request.api_secret,
                connection_id
            )
            
            results[connection_id] = {
                "current_state": current_state,
                "update_result": update_result,
                "verified_state": verified_state,
                "message": "State updated and verified successfully"
            }
        
        return results
        
    except Exception as e:
        # Resume all paused connections before failing
        DBOS.logger.error(f"State update failed: {str(e)}. Resuming all paused connections.")
        for connection_id in paused_connections:
            try:
                resume_connection_step(
                    request.api_key,
                    request.api_secret,
                    connection_id
                )
                DBOS.logger.info(f"Successfully resumed connection: {connection_id}")
            except Exception as resume_error:
                DBOS.logger.error(f"Failed to resume connection {connection_id}: {str(resume_error)}")
        
        # Fail the entire workflow
        raise HTTPException(
            status_code=500,
            detail=f"State update failed for one or more connections: {str(e)}. All paused connections have been resumed."
        )


@app.get("/", include_in_schema=False)
def frontend():
    """Serve the HTML frontend"""
    html_path = os.path.join(os.path.dirname(__file__), "html", "app.html")
    with open(html_path, "r") as file:
        html = file.read()
    return HTMLResponse(html)


@app.get("/api/info")
def api_info():
    """API information endpoint"""
    return {
        "service": "Fivetran Connection State Manager",
        "version": "1.0.0",
        "status": "running",
        "endpoints": {
            "GET /": "Frontend UI",
            "GET /api/info": "This endpoint - API information",
            "GET /docs": "Interactive API documentation (Swagger UI)",
            "GET /redoc": "Alternative API documentation (ReDoc)",
            "POST /state/get": "Get current connection state (requires api_key, api_secret, connection_id in request body)",
            "POST /state/update": "Update connection state (requires api_key, api_secret, connection_id, new_state in request body)",
            "POST /state/get-multiple": "Get current connection state for multiple connections (requires api_key, api_secret, connection_ids list in request body)",
            "POST /state/update-multiple": "Update connection state for multiple connections (requires api_key, api_secret, connection_ids list, new_state in request body)",
        },
        "description": "DBOS-powered state management for Fivetran connections with durable execution and automatic recovery",
        "docs_url": "http://localhost:8000/docs"
    }


if __name__ == "__main__":
    DBOS.launch()
    uvicorn.run(app, host="0.0.0.0", port=8000)
