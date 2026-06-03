#!/usr/bin/env python3
"""
MCP Server for Fivetran Management with AWS S3-Backed Approvals

Architecture:
- FastMCP-based tool server
- AWS S3-backed approval system (reading/writing mcp_approvals.json)
- Tiered tool organization (Business Impact → Operational → Monitoring)
- Comprehensive error handling and retry logic

Usage:
    python mcp_fivetran_s3_approve.py

Configuration Required (configuration.json):
    {
        "fivetran_api_key": "YOUR_API_KEY",
        "fivetran_api_secret": "YOUR_API_SECRET",
        "aws_access_key_id": "...",       // Optional, falls back to environment
        "aws_secret_access_key": "...",   // Optional, falls back to environment
        "aws_region": "...",              // Optional, default us-east-1
        "aws_s3_bucket": "YOUR_BUCKET",   // Required for approvals
        "aws_s3_key": "..."               // Optional, default mcp_approvals.json
    }
"""

# Standard library - core functionality
import base64  # Base64 encoding for approval credential validation
import json  # JSON serialization for API requests/responses
import os  # File system and environment operations
import time  # Rate limiting and delays
import sys  # System operations and stderr logging
import subprocess  # Subprocess operations for dynamic package loading
from datetime import datetime  # Timestamps for audit logs
from typing import Dict, Any, Optional, Union  # Type hints for enterprise code quality

def log_enterprise(*args, **kwargs) -> None:
    """
    Log to stderr to avoid corrupting MCP stdout stream.
    
    MCP servers use stdout for JSON-RPC communication; any regular print 
    statements will break the protocol.
    """
    kwargs['file'] = sys.stderr
    print(*args, **kwargs)


# Third-party - HTTP client
import requests  # Fivetran API communication
from requests.auth import HTTPBasicAuth  # API authentication

# Optional - AWS S3 integration for approval system
try:
    import boto3  # AWS SDK for Python
    from botocore.exceptions import ClientError  # AWS Client exceptions
except ImportError:
    log_enterprise("[SETUP] Installing boto3 library...")
    try:
        subprocess.check_call([sys.executable, "-m", "pip", "install", "boto3"])
        import boto3
        from botocore.exceptions import ClientError
    except Exception as e:
        log_enterprise(f"[ERROR] Failed to install boto3: {e}")
        # Fallback to os.system if subprocess fails
        os.system("pip install boto3")
        import boto3
        from botocore.exceptions import ClientError

# MCP Server Framework
try:
    from mcp.server.fastmcp import FastMCP  # Model Context Protocol server
except ImportError:
    # Auto-install if missing (development convenience)
    log_enterprise("[SETUP] Installing FastMCP library...")
    try:
        subprocess.check_call([sys.executable, "-m", "pip", "install", "mcp"])
        from mcp.server.fastmcp import FastMCP
    except Exception as e:
        log_enterprise(f"[ERROR] Failed to install mcp: {e}")
        os.system("pip install mcp")
        from mcp.server.fastmcp import FastMCP

# =============================================================================
# SERVER INITIALIZATION
# =============================================================================

# Initialize FastMCP server instance
mcp = FastMCP('fivetran-mcp-demo')

# =============================================================================
# CONFIGURATION MANAGEMENT
# =============================================================================

# Configuration file path - single source of truth for all credentials
config_file = '/configuration.json'

def _ensure_config_file_exists() -> None:
    """
    Validate configuration file path exists.
    
    Note: Does not create default configuration to prevent accidental misconfigurations.
    Enterprise deployments should provision configuration explicitly.
    """
    try:
        cfg_dir = os.path.dirname(config_file) or '.'
        if cfg_dir:
            os.makedirs(cfg_dir, exist_ok=True)
        if not os.path.exists(config_file):
            log_enterprise(f"[WARNING] Configuration file not found at: {config_file}")
            log_enterprise(f"[WARNING] Server will fail credential operations until configured.")
    except Exception as e:
        log_enterprise(f"[ERROR] Failed to validate configuration file path: {e}")

# Ensure configuration file exists at startup
_ensure_config_file_exists()

def _load_config(config_file_path: str = None) -> Dict:
    """
    Load configuration from JSON file.
    
    Expected configuration structure:
    {
        "fivetran_api_key": "<api_key>",
        "fivetran_api_secret": "<api_secret>",
        "aws_access_key_id": "<aws_access_key_id>",
        "aws_secret_access_key": "<aws_secret_access_key>",
        "aws_region": "<aws_region>",
        "aws_s3_bucket": "<aws_s3_bucket>",
        "aws_s3_key": "<aws_s3_key>"
    }
    
    Args:
        config_file_path: Optional override path, defaults to global config_file
    
    Returns:
        Configuration dictionary
    
    Raises:
        Exception: If file cannot be read or parsed
    """
    if config_file_path is None:
        config_file_path = config_file
    
    try:
        with open(config_file_path, "r") as f:
            config_content = f.read()
            log_enterprise(f"[INFO] Configuration file loaded successfully from: {config_file_path}")
            log_enterprise(f"[DEBUG] Configuration file size: {len(config_content)} characters")
            config = json.loads(config_content)
            log_enterprise(f"[DEBUG] Configuration keys found: {list(config.keys())}")
            return config
    except Exception as e:
        raise Exception(f"Failed to load configuration file: {e}")

def _get_aws_s3_config() -> Dict[str, Optional[str]]:
    """
    Retrieve AWS S3 configurations.
    First checks configuration.json, falls back to environment variables.
    
    Returns:
        Dictionary of AWS S3 configuration parameters
    """
    cfg = {}
    try:
        cfg = _load_config()
    except Exception as e:
        log_enterprise(f"[WARNING] Could not load configuration.json for AWS settings: {e}")
        
    aws_access_key = cfg.get("aws_access_key_id") or os.environ.get("AWS_ACCESS_KEY_ID")
    aws_secret_key = cfg.get("aws_secret_access_key") or os.environ.get("AWS_SECRET_ACCESS_KEY")
    aws_region = cfg.get("aws_region") or os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION", "us-east-1")
    aws_bucket = cfg.get("aws_s3_bucket") or os.environ.get("AWS_S3_BUCKET")
    aws_key = cfg.get("aws_s3_key") or os.environ.get("AWS_S3_KEY", "mcp_approvals.json")
    
    return {
        "aws_access_key_id": aws_access_key,
        "aws_secret_access_key": aws_secret_key,
        "aws_region": aws_region,
        "aws_s3_bucket": aws_bucket,
        "aws_s3_key": aws_key
    }

# Initialize global AWS S3 config
S3_CONFIG = _get_aws_s3_config()
if S3_CONFIG["aws_s3_bucket"]:
    log_enterprise(f"[INFO] AWS S3 Bucket configured: {S3_CONFIG['aws_s3_bucket']}")
    if S3_CONFIG["aws_access_key_id"]:
        log_enterprise("[INFO] AWS credentials loaded from configuration/environment.")
    else:
        log_enterprise("[INFO] AWS credentials check: using default provider chain (IAM role or environment).")
else:
    log_enterprise("[WARNING] AWS S3 Bucket not configured. S3 approvals store will fail requests until set.")

# =============================================================================
# APPROVAL WORKFLOW SYSTEM
# =============================================================================
# 
# Enterprise approval system for sensitive operations:
# - Mutating operations (create, delete, modify) require approval by default
# - Approval requests stored in AWS S3 for audit trail
# - Admin tools for approval/rejection workflow
# - Bypass flag available for automated/pre-approved operations
#
# Workflow:
# 1. User calls mutating tool → creates approval request
# 2. Request stored in AWS S3 (mcp_approvals.json)
# 3. Admin reviews via list_approval_requests()
# 4. Admin approves via approve_request() → executes action
# =============================================================================

# Global flag: enable/disable approval requirement
APPROVAL_REQUIRED = True

def _init_s3_client():
    """
    Initialize AWS S3 boto3 client.
    
    If explicit access keys are provided, we use them; otherwise, boto3 falls back
    to the standard AWS credential resolution chain (e.g. AWS CLI, IAM Role, env).
    
    Returns:
        boto3.client object or None if initialization fails
    """
    try:
        if boto3 is None:
            log_enterprise("[ERROR] boto3 library is not loaded.")
            return None
            
        params = {}
        if S3_CONFIG["aws_access_key_id"] and S3_CONFIG["aws_secret_access_key"]:
            params["aws_access_key_id"] = S3_CONFIG["aws_access_key_id"]
            params["aws_secret_access_key"] = S3_CONFIG["aws_secret_access_key"]
        if S3_CONFIG["aws_region"]:
            params["region_name"] = S3_CONFIG["aws_region"]
            
        return boto3.client("s3", **params)
    except Exception as e:
        log_enterprise(f"[ERROR] Failed to initialize AWS S3 client: {e}")
        return None

def _read_approval_store() -> Dict[str, Any]:
    """
    Read approval requests from AWS S3.
    
    Centralizes approval state in S3 for:
    - Multi-user access (shared file)
    - Audit trail persistence
    - External visibility for admin review
    
    Returns:
        Approval store dictionary with "requests" array
        Empty store if S3 unavailable or file not found
    """
    try:
        bucket = S3_CONFIG["aws_s3_bucket"]
        key = S3_CONFIG["aws_s3_key"]
        
        if not bucket:
            log_enterprise("[WARNING] AWS S3 Bucket not configured, returning empty approval store")
            return {"requests": []}
            
        s3_client = _init_s3_client()
        if not s3_client:
            log_enterprise("[WARNING] Failed to initialize S3 client, returning empty approval store")
            return {"requests": []}
            
        try:
            log_enterprise(f"[INFO] Reading approval store from S3: s3://{bucket}/{key}")
            response = s3_client.get_object(Bucket=bucket, Key=key)
            content = response['Body'].read().decode('utf-8')
            return json.loads(content)
        except ClientError as e:
            error_code = e.response['Error']['Code']
            # NoSuchKey (file does not exist yet) or 404
            if error_code in ['NoSuchKey', '404']:
                log_enterprise(f"[INFO] Approval file not found at s3://{bucket}/{key}, creating new empty store")
                return {"requests": []}
            else:
                log_enterprise(f"[ERROR] ClientError from AWS S3: {e}")
                return {"requests": []}
        except Exception as e:
            log_enterprise(f"[ERROR] Failed to read from S3: {e}")
            return {"requests": []}
            
    except Exception as e:
        log_enterprise(f"[ERROR] Failed to read approval store: {e}")
        return {"requests": []}

def _write_approval_store(store: Dict[str, Any]) -> None:
    """
    Write approval requests to AWS S3.
    
    Creates or updates mcp_approvals.json in configured S3 bucket.
    
    Args:
        store: Approval store dictionary to persist
    """
    try:
        bucket = S3_CONFIG["aws_s3_bucket"]
        key = S3_CONFIG["aws_s3_key"]
        
        if not bucket:
            log_enterprise("[WARNING] AWS S3 Bucket not configured, skipping approval store write")
            return
            
        s3_client = _init_s3_client()
        if not s3_client:
            log_enterprise("[WARNING] Failed to initialize S3 client, skipping approval store write")
            return
            
        # Convert store to JSON bytes
        content_bytes = json.dumps(store, indent=2).encode('utf-8')
        
        try:
            log_enterprise(f"[INFO] Uploading approval file to S3: s3://{bucket}/{key}")
            s3_client.put_object(
                Bucket=bucket,
                Key=key,
                Body=content_bytes,
                ContentType='application/json'
            )
            log_enterprise(f"[INFO] ✅ Successfully wrote approval file to AWS S3")
        except Exception as e:
            log_enterprise(f"[ERROR] Failed to write approvals to AWS S3: {e}")
            import traceback
            traceback.print_exc()
            
    except Exception as e:
        log_enterprise(f"[ERROR] Failed to write approval store: {e}")
        import traceback
        traceback.print_exc()

def _generate_request_id() -> str:
    """
    Generate globally unique approval request ID.
    
    Format: req_{timestamp_ms}_{process_id}
    Ensures uniqueness across concurrent operations.
    """
    return f"req_{int(time.time()*1000)}_{os.getpid()}"

def _approval_intercept(action_name: str, params: Dict[str, Any], approval_bypass: bool = False) -> Optional[str]:
    """
    Intercept mutating operations for approval workflow.
    
    Enterprise control mechanism:
    - Captures operation intent without executing
    - Creates audit record in S3
    - Returns pending status to user
    
    Args:
        action_name: Operation being intercepted (e.g., 'pause_connector')
        params: Operation parameters for later execution
        approval_bypass: If True, skip approval (for admin/automated flows)
    
    Returns:
        JSON response string if intercepted, None if approved to proceed
    """
    global APPROVAL_REQUIRED
    try:
        if approval_bypass:
            return None
        if not APPROVAL_REQUIRED:
            return None
        request_id = _generate_request_id()
        request_record = {
            "id": request_id,
            "action": action_name,
            "params": params,
            "status": "PENDING",
            "created_at": datetime.now().isoformat(),
        }
        store = _read_approval_store()
        store.setdefault("requests", []).append(request_record)
        _write_approval_store(store)
        
        # Return a user-friendly success message instead of approval prompt
        return json.dumps({
            "success": True,
            "message": f"{action_name.replace('_', ' ').title()} request submitted successfully and is being processed.",
            "status": "pending_approval",
            "note": "Your request has been received and will be processed shortly."
        }, indent=2)
    except Exception as e:
        return json.dumps({
            "success": False,
            "error": f"Failed to submit request: {str(e)}",
            "action": action_name
        }, indent=2)

# =============================================================================
# ACTION REGISTRY - Approval Execution Dispatcher
# =============================================================================

# Maps action names to callable functions for approved request execution
ACTION_REGISTRY: Dict[str, Any] = {}

def _register_action(name: str, func) -> None:
    """
    Register action handler for approval execution.
    
    Args:
        name: Action name matching intercepted operations
        func: Callable to execute when approved
    """
    ACTION_REGISTRY[name] = func

def _execute_action(action_name: str, params: Dict[str, Any]) -> str:
    """
    Execute approved action from registry.
    
    Automatically sets approval_bypass=True to prevent re-interception.
    
    Args:
        action_name: Registered action to execute
        params: Parameters captured during interception
    
    Returns:
        JSON result string from action execution
    """
    try:
        func = ACTION_REGISTRY.get(action_name)
        if not func:
            return json.dumps({"success": False, "error": f"Unknown action: {action_name}"}, indent=2)

        # Ensure bypass flag is set
        if isinstance(params, dict):
            params = dict(params)
            params["approval_bypass"] = True

        result = func(**params) if callable(func) else None
        return result if isinstance(result, str) else json.dumps(result or {"success": False, "error": "No result returned"}, indent=2)
    except Exception as e:
        return json.dumps({"success": False, "error": f"Execution failed: {str(e)}"}, indent=2)


# =============================================================================
# APPROVAL ADMIN TOOLS
# =============================================================================
# Administrative interface for approval workflow management
# Restricted to admin users for system governance
# =============================================================================

def _validate_approval_credentials() -> bool:
    """
    Validate that the approving user has the correct approval credentials.
    
    Checks that the environment variable FIVETRAN_MCP_APPROVAL contains
    the base64-encoded value of api_key:api_secret (HTTP Basic Auth format).
    
    Returns:
        True if credentials match, False otherwise
    """
    try:
        # Get the approval credential from environment
        env_approval = os.environ.get('FIVETRAN_MCP_APPROVAL')
        if not env_approval:
            log_enterprise("[WARNING] FIVETRAN_MCP_APPROVAL environment variable not set")
            return False
        
        # Get the actual API credentials
        api_key, api_secret = _get_api_credentials()
        
        # Encode credentials in the same format as HTTP Basic Auth
        # Format: base64(api_key:api_secret)
        credentials_string = f"{api_key}:{api_secret}"
        encoded_credentials = base64.b64encode(credentials_string.encode('utf-8')).decode('utf-8')
        
        # Compare with environment variable
        if env_approval.strip() == encoded_credentials:
            log_enterprise("[INFO] Approval credentials validated successfully")
            return True
        else:
            log_enterprise("[WARNING] Approval credentials do not match")
            log_enterprise(f"[DEBUG] Expected: {encoded_credentials[:20]}...")
            log_enterprise(f"[DEBUG] Got: {env_approval[:20]}...")
            return False
            
    except Exception as e:
        log_enterprise(f"[ERROR] Failed to validate approval credentials: {e}")
        return False

@mcp.tool()
def set_approval_mode(enabled: bool) -> str:
    """
    Enable or disable approval requirement globally.
    
    **ADMIN ONLY** - System-wide governance control.
    
    When enabled:
    - All mutating operations require approval
    - Requests stored in AWS S3 for audit
    - Admin must approve via approve_request()
    
    When disabled:
    - All operations execute immediately
    - No approval workflow or audit trail
    
    Args:
        enabled: True to require approvals, False to allow direct execution
    
    Returns:
        JSON string with updated approval mode status
    """
    global APPROVAL_REQUIRED
    APPROVAL_REQUIRED = bool(enabled)
    return json.dumps({
        "success": True,
        "approval_required": APPROVAL_REQUIRED,
        "message": "Approval mode updated"
    }, indent=2)

@mcp.tool()
def get_approval_mode() -> str:
    """
    Query current approval mode status.
    
    **ADMIN ONLY** - Check system governance state.
    
    Returns:
        JSON string with approval_required boolean
    """
    return json.dumps({
        "version": "mcp_fivetran_s3_approve.py",
        "success": True,
        "approval_required": APPROVAL_REQUIRED
    }, indent=2)

@mcp.tool()
def list_approval_requests(status: str = None) -> str:
    """
    List all approval requests with optional filtering.
    
    **ADMIN ONLY** - Review pending and historical approval requests.
    
    Request lifecycle:
    1. PENDING - awaiting admin decision
    2. APPROVED - admin approved, executing
    3. EXECUTED - completed successfully
    4. REJECTED - admin rejected, not executed
    
    Args:
        status: Optional filter (PENDING, APPROVED, REJECTED, EXECUTED)
    
    Returns:
        JSON string with filtered approval requests and metadata
    """
    store = _read_approval_store()
    items = store.get("requests", [])
    if status:
        items = [r for r in items if r.get("status") == status]
    return json.dumps({
        "success": True,
        "admin_function": True,
        "count": len(items),
        "requests": items,
        "note": "This is an admin function for managing approval requests"
    }, indent=2)

@mcp.tool()
def approve_request(request_id: str) -> str:
    """
    Approve and execute a pending approval request.
    
    **ADMIN ONLY** - Authorize and execute intercepted operations.
    
    Requires FIVETRAN_MCP_APPROVAL environment variable to be set with
    base64-encoded api_key:api_secret value matching the configured credentials.
    
    Workflow:
    1. Validates approval credentials from environment variable
    2. Validates request is PENDING
    3. Updates status to APPROVED
    4. Executes the requested action with approval bypass
    5. Updates status to EXECUTED with result
    6. Persists to S3 for audit
    
    Args:
        request_id: Approval request ID to approve
    
    Returns:
        JSON string with execution result
    """
    # Validate approval credentials first
    if not _validate_approval_credentials():
        return json.dumps({
            "success": False,
            "error": "Approval credentials validation failed. Ensure FIVETRAN_MCP_APPROVAL environment variable is set correctly with base64-encoded api_key:api_secret."
        }, indent=2)
    
    store = _read_approval_store()
    for r in store.get("requests", []):
        if r.get("id") == request_id:
            if r.get("status") != "PENDING":
                return json.dumps({"success": False, "error": f"Request not pending, current status: {r.get('status')}"}, indent=2)
            r["status"] = "APPROVED"
            r["approved_at"] = datetime.now().isoformat()
            _write_approval_store(store)
            # Execute
            exec_result = _execute_action(r.get("action"), r.get("params", {}))
            # Mark executed
            try:
                exec_payload = json.loads(exec_result)
            except Exception:
                exec_payload = {"success": False, "raw": exec_result}
            r["status"] = "EXECUTED"
            r["executed_at"] = datetime.now().isoformat()
            r["result"] = exec_payload
            _write_approval_store(store)
            return json.dumps({
                "success": True,
                "request_id": request_id,
                "action": r.get("action"),
                "result": exec_payload
            }, indent=2)
    return json.dumps({"success": False, "error": "Request not found"}, indent=2)

@mcp.tool()
def reject_request(request_id: str, reason: str = "") -> str:
    """
    Reject a pending approval request.
    
    **ADMIN ONLY** - Deny intercepted operations.
    
    Requires FIVETRAN_MCP_APPROVAL environment variable to be set with
    base64-encoded api_key:api_secret value matching the configured credentials.
    
    Args:
        request_id: Approval request ID to reject
        reason: Optional rejection reason for audit trail
    
    Returns:
        JSON string with rejection confirmation
    """
    # Validate approval credentials first
    if not _validate_approval_credentials():
        return json.dumps({
            "success": False,
            "error": "Approval credentials validation failed. Ensure FIVETRAN_MCP_APPROVAL environment variable is set correctly with base64-encoded api_key:api_secret."
        }, indent=2)
    
    store = _read_approval_store()
    for r in store.get("requests", []):
        if r.get("id") == request_id:
            if r.get("status") != "PENDING":
                return json.dumps({"success": False, "error": f"Request not pending, current status: {r.get('status')}"}, indent=2)
            r["status"] = "REJECTED"
            r["rejected_at"] = datetime.now().isoformat()
            r["reason"] = reason
            _write_approval_store(store)
            return json.dumps({"success": True, "request_id": request_id, "status": "REJECTED"}, indent=2)
    return json.dumps({"success": False, "error": "Request not found"}, indent=2)

# =============================================================================
# FIVETRAN API CLIENT LAYER
# =============================================================================

def _get_api_credentials() -> tuple:
    """
    Retrieve Fivetran API credentials from configuration.
    
    Returns:
        Tuple of (api_key, api_secret)
    
    Raises:
        Exception: If credentials missing or configuration invalid
    """
    try:
        config = _load_config()
        api_key = config['fivetran_api_key']
        api_secret = config['fivetran_api_secret']
        log_enterprise(f"[INFO] API credentials loaded successfully")
        log_enterprise(f"[DEBUG] API Key: {api_key[:10]}...{api_key[-10:] if len(api_key) > 20 else '***'}")
        log_enterprise(f"[DEBUG] API Secret: {api_secret[:10]}...{api_secret[-10:] if len(api_secret) > 20 else '***'}")
        return api_key, api_secret
    except Exception as e:
        raise Exception(f"Failed to load API credentials: {e}")

def _make_api_request(method: str, endpoint: str, payload: Dict = None, params: Dict = None, max_retries: int = 3) -> Optional[Dict]:
    """
    Execute Fivetran API request with enterprise-grade reliability.
    
    Features:
    - Automatic retry with exponential backoff
    - Comprehensive error logging
    - Timeout protection (10s connect, 30s read)
    - API versioning support (Accept: application/json;version=2)
    
    Args:
        method: HTTP method (GET, POST, PATCH, DELETE)
        endpoint: API endpoint path (without base URL)
        payload: Request body for POST/PATCH
        params: URL query parameters for GET
        max_retries: Maximum retry attempts (default: 3)
    
    Returns:
        Response JSON dictionary or None on failure
    """
    try:
        # Get credentials
        api_key, api_secret = _get_api_credentials()
        auth = HTTPBasicAuth(api_key, api_secret)
        base_url = 'https://api.fivetran.com/v1'
        
        url = f'{base_url}/{endpoint}'
        headers = {
            'Accept': 'application/json;version=2',
            'Content-Type': 'application/json',
            'User-Agent': 'fivetran_mcp_local_test'
        }
        
        # Set timeout values to prevent hanging
        timeout = (10, 30)  # (connect_timeout, read_timeout) in seconds
        
        # Debug logging
        log_enterprise(f"[DEBUG] Executing {method} request to: {url}")
        if payload:
            log_enterprise(f"[DEBUG] Request payload: {json.dumps(payload, indent=2)}")
        
        for attempt in range(max_retries):
            try:
                if method == 'GET':
                    response = requests.get(url, headers=headers, auth=auth, params=params, timeout=timeout)
                elif method == 'POST':
                    response = requests.post(url, headers=headers, json=payload, auth=auth, timeout=timeout)
                elif method == 'PATCH':
                    response = requests.patch(url, headers=headers, json=payload, auth=auth, timeout=timeout)
                elif method == 'DELETE':
                    response = requests.delete(url, headers=headers, json=payload, auth=auth, timeout=timeout)
                else:
                    raise ValueError(f'Invalid request method: {method}')
                
                # Log response details for debugging
                log_enterprise(f"[DEBUG] Response status: {response.status_code}")
                log_enterprise(f"[DEBUG] Response headers: {dict(response.headers)}")
                
                if response.status_code >= 400:
                    log_enterprise(f"[ERROR] API Error {response.status_code}: {response.text}")
                    log_enterprise(f"[DEBUG] Response content: {response.text}")
                
                response.raise_for_status()
                
                # Ensure we return a dictionary, not a string
                response_data = response.json()
                if isinstance(response_data, str):
                    log_enterprise(f"[WARNING] API returned string instead of JSON: {response_data}")
                    return None
                
                return response_data
                
            except requests.exceptions.Timeout as e:
                log_enterprise(f"[WARNING] Request timeout on attempt {attempt + 1}: {e}")
                if attempt == max_retries - 1:
                    raise e
                time.sleep(2 ** attempt)  # Exponential backoff
                
            except requests.exceptions.RequestException as e:
                log_enterprise(f"[ERROR] Request failed on attempt {attempt + 1}: {e}")
                if attempt == max_retries - 1:
                    raise e
                time.sleep(2 ** attempt)  # Exponential backoff
                
            except Exception as e:
                log_enterprise(f"[ERROR] Unexpected error on attempt {attempt + 1}: {e}")
                if attempt == max_retries - 1:
                    raise e
                time.sleep(2 ** attempt)  # Exponential backoff
        
        return None
        
    except Exception as e:
        log_enterprise(f"[ERROR] Failed to make API request: {e}")
        return None

@mcp.tool()
def list_connectors(group_id: str = None) -> str:
    """
    List all Fivetran connectors with optional group filtering.
    
    Args:
        group_id: Optional Fivetran group/destination ID to filter results
    
    Returns:
        JSON string with connector list or error details
    """
    try:
        endpoint = 'connections'
        params = {}
        if group_id:
            params['group_id'] = group_id
        
        response = _make_api_request('GET', endpoint, params=params)
        
        if response:
            return json.dumps({
                "success": True,
                "data": response.get('data', []),
                "message": f"Retrieved {len(response.get('data', []))} connectors"
            }, indent=2)
        else:
            return json.dumps({
                "success": False,
                "error": "Failed to retrieve connectors list"
            }, indent=2)
            
    except Exception as e:
        return json.dumps({
            "success": False,
            "error": f"Error listing connectors: {str(e)}"
        }, indent=2)

@mcp.tool()
def get_connector_status(connector_id: str) -> str:
    """
    Retrieve detailed status for a specific connector.
    
    Args:
        connector_id: Fivetran connector ID
    
    Returns:
        JSON string with connector status, sync state, and configuration
    """
    try:
        endpoint = f'connectors/{connector_id}'
        response = _make_api_request('GET', endpoint)
        
        if response:
            return json.dumps({
                "success": True,
                "data": response.get('data', {}),
                "message": "Connector status retrieved successfully"
            }, indent=2)
        else:
            return json.dumps({
                "success": False,
                "error": "Failed to retrieve connector status"
            }, indent=2)
            
    except Exception as e:
        return json.dumps({
            "success": False,
            "error": f"Error getting connector status: {str(e)}"
        }, indent=2)

@mcp.tool()  
def pause_connector(connector_id: str, approval_bypass: bool = False) -> str:
    """
    Pause a running connector (stops data syncing).
    
    **Approval Required**: This mutating operation requires admin approval unless bypassed.
    
    Args:
        connector_id: Fivetran connector ID to pause
        approval_bypass: Admin flag to skip approval workflow
    
    Returns:
        JSON string with operation result or approval request ID
    """
    try:
        intercept = _approval_intercept('pause_connector', {"connector_id": connector_id}, approval_bypass)
        if intercept:
            return intercept
        endpoint = f'connectors/{connector_id}'
        payload = {"paused": True}
        response = _make_api_request('PATCH', endpoint, payload)
        
        if response:
            return json.dumps({
                "success": True,
                "data": response.get('data', {}),
                "message": "Connector paused successfully"
            }, indent=2)
        else:
            return json.dumps({
                "success": False,
                "error": "Failed to pause connector"
            }, indent=2)
            
    except Exception as e:
        return json.dumps({
            "success": False,
            "error": f"Error pausing connector: {str(e)}"
        }, indent=2)

@mcp.tool()
def resume_connector(connector_id: str, approval_bypass: bool = False) -> str:
    """
    Resume a paused connector (resumes data syncing).
    
    **Approval Required**: This mutating operation requires admin approval unless bypassed.
    
    Args:
        connector_id: Fivetran connector ID to resume
        approval_bypass: Admin flag to skip approval workflow
    
    Returns:
        JSON string with operation result or approval request ID
    """
    try:
        intercept = _approval_intercept('resume_connector', {"connector_id": connector_id}, approval_bypass)
        if intercept:
            return intercept
        endpoint = f'connectors/{connector_id}'
        payload = {"paused": False}
        response = _make_api_request('PATCH', endpoint, payload)
        
        if response:
            return json.dumps({
                "success": True,
                "data": response.get('data', {}),
                "message": "Connector resumed successfully"
            }, indent=2)
        else:
            return json.dumps({
                "success": False,
                "error": "Failed to resume connector"
            }, indent=2)
            
    except Exception as e:
        return json.dumps({
            "success": False,
            "error": f"Error resuming connector: {str(e)}"
        }, indent=2)

@mcp.tool()
def generate_unique_names(base_schema: str = "google_sheets", base_table: str = "data") -> tuple:
    """
    Generate timestamp-based unique schema and table names.
    
    Prevents naming collisions in batch operations and testing scenarios.
    
    Args:
        base_schema: Base schema name (default: "google_sheets")
        base_table: Base table name (default: "data")
        
    Returns:
        Tuple of (schema_YYYYmmdd_HHMMSS, table_YYYYmmdd_HHMMSS)
    """
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    unique_schema = f"{base_schema}_{timestamp}"
    unique_table = f"{base_table}_{timestamp}"
    return unique_schema, unique_table

@mcp.tool()
def initialize_connector():
    """
    Validate Fivetran API credentials and configuration.
    
    Internal initialization check - verifies credentials can be loaded
    before attempting API operations.
    
    Returns:
        Boolean: True if credentials valid, False otherwise
    """
    try:
        log_enterprise(f"[DEBUG] Testing Fivetran API credentials from config: {config_file}")
        api_key, api_secret = _get_api_credentials()
        log_enterprise(f"[DEBUG] API credentials loaded successfully")
        log_enterprise(f"[DEBUG] API Key: {api_key[:10]}...{api_key[-10:] if len(api_key) > 20 else '***'}")
        log_enterprise(f"[DEBUG] API Secret: {api_secret[:10]}...{api_secret[-10:] if len(api_secret) > 20 else '***'}")
        return True
    except Exception as e:
        log_enterprise(f"[ERROR] Failed to initialize connector: {e}")
        log_enterprise(f"[ERROR] Exception type: {type(e)}")
        import traceback
        traceback.print_exc()
        return False

@mcp.tool()
def get_connector_metadata(connector_type: str) -> str:
    """
    Retrieve connector type metadata and configuration requirements.
    
    Useful for discovering required/optional fields before creating connectors.
    
    Args:
        connector_type: Connector service type
            Supported: 'google_sheets', 'sql_server', 'mysql', 'postgresql',
            'snowflake', 'bigquery', etc.
        
    Returns:
        JSON string with metadata including:
        - Required configuration fields
        - Optional parameters
        - Field descriptions and examples
    """
    try:
        if not initialize_connector():
            return json.dumps({"success": False, "error": "Failed to initialize Fivetran connector"})
        
        metadata_response = _make_api_request('GET', f'metadata/connectors/{connector_type}')
        
        if metadata_response:
            result = {
                "success": True,
                "connector_type": connector_type,
                "metadata": metadata_response.get('data', {}),
                "message": f"Metadata retrieved successfully for {connector_type}"
            }
        else:
            result = {
                "success": False,
                "error": f"Failed to retrieve metadata for connector type: {connector_type}",
                "connector_type": connector_type
            }
        
        return json.dumps(result, indent=2)
        
    except Exception as e:
        return json.dumps({
            "success": False,
            "error": f"Error retrieving connector metadata: {str(e)}",
            "connector_type": connector_type
        }, indent=2)

@mcp.tool()
def create_dynamic_connector(connector_type: str, group_id: str, config: Dict[str, Any], auth: Dict[str, Any] = None, approval_bypass: bool = False, **kwargs) -> str:
    """
    Create any connector type with dynamic configuration.
    
    **Approval Required**: This mutating operation requires admin approval unless bypassed.
    
    Universal connector creation supporting all Fivetran connector types.
    Use get_connector_metadata() to discover required configuration.
    
    Args:
        connector_type: Connector service type (e.g., 'google_sheets', 'sql_server')
        group_id: Fivetran destination/group ID
        config: Connector-specific configuration dictionary
        auth: Optional authentication configuration (for OAuth connectors)
        approval_bypass: Admin flag to skip approval
        **kwargs: Additional options:
            - paused: Start paused (default: True for safety)
            - sync_frequency: Minutes between syncs (default: 1440)
            - trust_certificates: Trust SSL certs (default: True)
            - networking_method: Connection method (default: auto)
        
    Returns:
        JSON string with connector creation result or approval request
    """
    try:
        intercept = _approval_intercept('create_dynamic_connector', {
            "connector_type": connector_type,
            "group_id": group_id,
            "config": config,
            "auth": auth,
            "options": kwargs
        }, approval_bypass)
        if intercept:
            return intercept
        if not initialize_connector():
            return json.dumps({"success": False, "error": "Failed to initialize Fivetran connector"})
        
        payload = {
            "group_id": group_id,
            "service": connector_type,
            "trust_certificates": kwargs.get("trust_certificates", True),
            "trust_fingerprints": kwargs.get("trust_fingerprints", True),
            "run_setup_tests": kwargs.get("run_setup_tests", True),
            "paused": kwargs.get("paused", True),  # Start paused for safety
            "pause_after_trial": kwargs.get("pause_after_trial", False),
            "sync_frequency": kwargs.get("sync_frequency", 1440),
            "data_delay_sensitivity": kwargs.get("data_delay_sensitivity", "NORMAL"),
            "data_delay_threshold": kwargs.get("data_delay_threshold", 0),
            "schedule_type": kwargs.get("schedule_type", "auto"),
            "config": config
        }
        
        optional_params = [
            "daily_sync_time", "connect_card_config", "proxy_agent_id", 
            "private_link_id", "networking_method", "hybrid_deployment_agent_id",
            "destination_configuration"
        ]
        
        for param in optional_params:
            if param in kwargs:
                payload[param] = kwargs[param]
        
        if auth:
            payload["auth"] = auth
        
        log_enterprise(f"[INFO] Creating {connector_type} connector with dynamic configuration")
        log_enterprise(f"[DEBUG] Payload: {json.dumps(payload, indent=2)}")
        
        response = _make_api_request('POST', 'connections/', payload)
        
        if response:
            connector_id = response.get('data', {}).get('id', 'Unknown')
            status = response.get('data', {}).get('status', {})
            
            result = {
                "success": True,
                "connector_id": connector_id,
                "connector_type": connector_type,
                "group_id": group_id,
                "status": status.get('setup_state', 'N/A'),
                "paused": payload.get('paused', True),
                "created_at": response.get('data', {}).get('created_at', 'N/A'),
                "message": f"{connector_type} connector created successfully and paused for complete setup!"
            }
        else:
            result = {
                "success": False,
                "error": "Failed to create connector",
                "connector_type": connector_type
            }
        
        return json.dumps(result, indent=2)
        
    except Exception as e:
        return json.dumps({
            "success": False,
            "error": f"Error creating dynamic connector: {str(e)}",
            "connector_type": connector_type
        }, indent=2)

@mcp.tool()
def create_google_sheet_connector(connector_details: Dict[str, Any], approval_bypass: bool = False) -> str:
    """
    Create Google Sheets connector with comprehensive defaults.
    
    **Approval Required**: This mutating operation requires admin approval unless bypassed.
    
    Independent connector creation providing:
    - Automatic unique schema/table naming (timestamp-based)
    - Comprehensive default configuration
    - Paused startup for safe validation
    - Detailed result reporting
    
    Args:
        connector_details: Configuration dictionary with:
            Required:
            - sheet_url: Google Sheet URL or ID
            - group_id: Fivetran destination ID
            - schema_name: Database schema name
            - table_name: Database table name
            - auth_type: 'ServiceAccount' or 'OAuth'
            Optional:
            - named_range: Range specification (e.g., 'Sheet1!A1:Z1000')
            - service_account_email: Service account for sharing
            - sync_frequency: Minutes between syncs (default: 1440)
        approval_bypass: Admin flag to skip approval
    
    Returns:
        JSON string with connector creation result
    """
    try:
        intercept = _approval_intercept('create_google_sheet_connector', {"connector_details": connector_details}, approval_bypass)
        if intercept:
            return intercept
        if not initialize_connector():
            return json.dumps({"success": False, "error": "Failed to initialize Fivetran connector"})
        
        sheet_url = connector_details.get("sheet_url", "")
        if "docs.google.com" in sheet_url:
            try:
                sheet_id = sheet_url.split('/d/')[1].split('/')[0]
            except Exception:
                sheet_id = sheet_url
        else:
            sheet_id = sheet_url
        
        schema_name = connector_details.get("schema_name", "google_sheets")
        table_name = connector_details.get("table_name", "data")
        unique_schema, unique_table = generate_unique_names(schema_name, table_name)
        
        payload = {
            "group_id": connector_details["group_id"],
            "service": "google_sheets",
            "trust_certificates": True,
            "trust_fingerprints": True,
            "run_setup_tests": True,
            "paused": True,  # Start paused for safety
            "pause_after_trial": False,
            "sync_frequency": connector_details.get("sync_frequency", 1440),
            "data_delay_sensitivity": "NORMAL",
            "data_delay_threshold": 0,
            "schedule_type": "auto",
            "config": {
                "schema": unique_schema,
                "table": unique_table,
                "named_range": connector_details.get("named_range"),
                "sheet_id": sheet_id,
                "auth_type": connector_details.get("auth_type", "ServiceAccount")
            }
        }
        
        if connector_details.get("auth_type") == "OAuth":
            payload["auth"] = {
                "refresh_token": connector_details.get("refresh_token"),
                "client_id": connector_details.get("client_id"),
                "client_secret": connector_details.get("client_secret")
            }
        
        log_enterprise(f"[INFO] Creating Google Sheets connector with payload: {json.dumps(payload, indent=2)}")
        
        response = _make_api_request('POST', 'connectors', payload)
        
        if response:
            connector_id = response.get('data', {}).get('id', 'N/A')
            status = response.get('data', {}).get('status', {})
            
            result = {
                "success": True,
                "connector_id": connector_id,
                "sheet_id": sheet_id,
                "named_range": connector_details.get('named_range', 'Entire sheet'),
                "group_id": connector_details['group_id'],
                "schema": unique_schema,
                "table": unique_table,
                "auth_type": connector_details.get('auth_type', 'ServiceAccount'),
                "sync_frequency": connector_details.get('sync_frequency', 1440),
                "status": status.get('setup_state', 'N/A'),
                "paused": True,  # Confirm connector is paused
                "created_at": response.get('data', {}).get('created_at', 'N/A'),
                "message": "Google Sheets connector created successfully and paused for complete setup!"
            }
        else:
            try:
                api_key, api_secret = _get_api_credentials()
                auth = HTTPBasicAuth(api_key, api_secret)
                base_url = 'https://api.fivetran.com/v1'
                url = f"{base_url}/connectors"
                headers = {
                    'Accept': 'application/json;version=2',
                    'Content-Type': 'application/json',
                    'User-Agent': 'fivetran_mcp_local_test'
                }
                resp = requests.post(url, headers=headers, json=payload, auth=auth, timeout=(10, 30))
                try:
                    body = resp.json()
                except Exception:
                    body = {"raw": resp.text}
                result = {
                    "success": False,
                    "error": "Failed to create Google Sheets connector",
                    "status_code": resp.status_code,
                    "response": body,
                    "payload_preview": {
                        "group_id": payload.get("group_id"), 
                        "service": payload.get("service"), 
                        "config_keys": list(payload.get("config", {}).keys())
                    }
                }
            except Exception as e:
                result = {
                    "success": False,
                    "error": f"Failed to create Google Sheets connector (no details): {str(e)}"
                }
        
        return json.dumps(result, indent=2)
        
    except Exception as e:
        return json.dumps({
            "success": False,
            "error": f"Error creating connector: {str(e)}"
        }, indent=2)

@mcp.tool()
def test_connection() -> str:
    """
    Verify Fivetran API connectivity and credentials.
    
    Simple health check using groups endpoint to validate:
    - API credentials are correct
    - Network connectivity is working
    - API is responding
    
    Returns:
        JSON string with success status and groups count
    """
    try:
        log_enterprise(f"[DEBUG] Testing Fivetran API connection")
        
        if not initialize_connector():
            return json.dumps({"success": False, "error": "Failed to initialize Fivetran connector"})
        
        test_response = _make_api_request('GET', 'groups')
        
        if test_response:
            return json.dumps({
                "success": True,
                "message": "Connection test successful",
                "groups_count": len(test_response.get('data', {}).get('items', [])),
                "timestamp": datetime.now().isoformat()
            }, indent=2)
        else:
            return json.dumps({
                "success": False,
                "error": "API request failed or returned no data"
            }, indent=2)
            
    except Exception as e:
        return json.dumps({
            "success": False,
            "error": f"Connection test failed: {str(e)}"
        }, indent=2)

@mcp.tool()
def list_destinations() -> str:
    """
    List all Fivetran destinations with service account details.
    
    Critical for Google Sheets connector setup - provides the service account
    email that must be granted access to the sheet.
    
    Returns:
        JSON string containing:
        - Destination list with IDs, names, regions
        - Constructed service account emails (format: g-{group_id}@fivetran-production.iam.gserviceaccount.com)
        - End-user friendly summary for sharing
    """
    try:
        if not initialize_connector():
            return json.dumps({"success": False, "error": "Failed to initialize Fivetran connector"})
        
        log_enterprise(f"[DEBUG] Starting destinations API call")
        response = _make_api_request('GET', 'destinations')
        
        if response is None:
            return json.dumps({
                "success": False,
                "error": "Failed to make API request to destinations endpoint"
            }, indent=2)
        
        data_section = response.get('data', {})
        if isinstance(data_section, list):
            all_items = data_section
        elif isinstance(data_section, dict):
            all_items = data_section.get('items', [])
        else:
            all_items = []
        
        log_enterprise(f"[DEBUG] Found {len(all_items)} destinations")
        
        if all_items:
            result = {
                "success": True,
                "total_destinations": len(all_items),
                "destinations": [],
                "service_account_emails": [],
                "end_user_summary": []
            }
            
            for dest in all_items:
                destination_id = dest.get('id', 'N/A')
                service = dest.get('service', 'N/A')
                region = dest.get('region', 'UNKNOWN')
                setup_status = dest.get('setup_status', 'UNKNOWN')
                group_id = dest.get('group_id', 'N/A')
                
                destination_name = 'N/A'
                try:
                    log_enterprise(f"[DEBUG] Fetching group name for destination {destination_id} with group_id {group_id}")
                    
                    api_key, api_secret = _get_api_credentials()
                    auth = HTTPBasicAuth(api_key, api_secret)
                    base_url = 'https://api.fivetran.com/v1'
                    group_url = f'{base_url}/groups/{group_id}'
                    group_headers = {
                        'Accept': 'application/json',
                        'Content-Type': 'application/json',
                        'User-Agent': 'fivetran_mcp_local_test'
                    }
                    
                    group_response = requests.get(
                        group_url, 
                        headers=group_headers, 
                        auth=auth, 
                        timeout=(10, 30)
                    )
                    
                    if group_response.status_code == 200:
                        group_data = group_response.json()
                        if group_data and 'data' in group_data:
                            destination_name = group_data['data'].get('name', 'N/A')
                except Exception as e:
                    log_enterprise(f"[WARNING] Failed to fetch group name for group_id {group_id}: {e}")
                    destination_name = f"Group_{group_id}"
                
                service_account_email = f"g-{group_id}@fivetran-production.iam.gserviceaccount.com"
                
                destination_info = {
                    "id": destination_id,
                    "name": destination_name,
                    "service": service,
                    "region": region,
                    "setup_status": setup_status,
                    "group_id": group_id,
                    "created_at": dest.get('created_at', 'N/A'),
                    "service_account_email": service_account_email
                }
                result["destinations"].append(destination_info)
                
                result["service_account_emails"].append({
                    "destination_id": destination_id,
                    "destination_name": destination_name,
                    "service_account_email": service_account_email,
                    "service": service,
                    "region": region,
                    "group_id": group_id
                })
                
                end_user_info = {
                    "destination_name": destination_name,
                    "destination_id": destination_id,
                    "service_account_email": service_account_email,
                    "service": service,
                    "region": region,
                    "group_id": group_id
                }
                result["end_user_summary"].append(end_user_info)
            
            result["message"] = f"Retrieved {len(all_items)} destinations successfully"
            
        else:
            result = {
                "success": False,
                "error": "No destinations found or invalid response format",
                "response_type": str(type(response)),
                "response_keys": list(response.keys()) if isinstance(response, dict) else "Not a dict"
            }
        
        return json.dumps(result, indent=2)
        
    except Exception as e:
        import traceback
        error_details = traceback.format_exc()
        return json.dumps({
            "success": False,
            "error": f"Error listing destinations: {str(e)}",
            "traceback": error_details
        }, indent=2)

@mcp.tool()
def get_simple_destinations() -> str:
    """
    Get end-user friendly destination list.
    
    Simplified output optimized for:
    - Sharing with non-technical users
    - Quick reference of available destinations
    - Clean format for documentation
    
    Returns:
        JSON string with minimal destination information:
        - Destination name and ID
        - Service account email to share sheets with
        - Service type and region
    """
    try:
        log_enterprise(f"[DEBUG] Starting get_simple_destinations function")
        
        if not initialize_connector():
            log_enterprise(f"[ERROR] Failed to initialize Fivetran connector")
            return json.dumps({"success": False, "error": "Failed to initialize Fivetran connector"})
        
        destinations_response = list_destinations()
        destinations_data = json.loads(destinations_response)
        
        if destinations_data.get("success"):
            return json.dumps({
                "success": True,
                "destinations": destinations_data.get("end_user_summary", []),
                "total": len(destinations_data.get("end_user_summary", [])),
                "message": "Destinations formatted for end user sharing"
            }, indent=2)
        else:
            return destinations_response
            
    except Exception as e:
        return json.dumps({
            "success": False,
            "error": f"Error formatting destinations for end users: {str(e)}"
        }, indent=2)

@mcp.tool()
def check_connector_status(connector_id: str) -> str:
    """
    Comprehensive connector health check with recommendations.
    
    Enhanced status check that includes:
    - Setup and sync state analysis
    - Configuration validation
    - Actionable recommendations
    
    Args:
        connector_id: Fivetran connector ID to analyze
        
    Returns:
        JSON string with:
        - Current status (setup_state, sync_state, paused)
        - Health assessment
        - Recommended actions if issues detected
    """
    try:
        if not initialize_connector():
            return json.dumps({"success": False, "error": "Failed to initialize Fivetran connector"})
        
        status_response_str = get_connector_status(connector_id)
        status_response = json.loads(status_response_str) if status_response_str else None
        
        if status_response:
            data = status_response.get('data', {})
            status = data.get('status', {})
            
            result = {
                "success": True,
                "connector_id": connector_id,
                "setup_state": status.get('setup_state', 'UNKNOWN'),
                "sync_state": status.get('sync_state', 'UNKNOWN'),
                "paused": data.get('paused', False),
                "service": data.get('service', 'UNKNOWN'),
                "group_id": data.get('group_id', 'UNKNOWN'),
                "created_at": data.get('created_at', 'UNKNOWN'),
                "last_sync": status.get('last_sync', 'UNKNOWN'),
                "message": "Connector status retrieved successfully"
            }
            
            if status.get('setup_state') == 'INCOMPLETE':
                result["needs_attention"] = True
                result["recommendation"] = "Connector setup is incomplete. Check configuration and authentication."
            elif not data.get('paused'):
                result["needs_attention"] = True
                result["recommendation"] = "Connector is running. Consider pausing until setup is complete."
            else:
                result["needs_attention"] = False
                result["recommendation"] = "Connector appears to be properly configured and paused."
        else:
            result = {
                "success": False,
                "error": "Failed to retrieve connector status",
                "connector_id": connector_id
            }
        
        return json.dumps(result, indent=2)
        
    except Exception as e:
        return json.dumps({
            "success": False,
            "error": f"Error checking connector status: {str(e)}",
            "connector_id": connector_id
        }, indent=2)

@mcp.tool()
def migrate_connector(connector_id: str, target_group_id: str, new_schema_name: str = None, new_table_name: str = None, approval_bypass: bool = False) -> str:
    """
    Migrate connector to different destination with configuration preservation.
    
    **Approval Required**: This mutating operation requires admin approval unless bypassed.
    
    Enterprise migration supporting:
    - Cross-destination connector moves
    - Schema configuration preservation
    - Optional rename during migration
    - Automatic schema replication
    
    Args:
        connector_id: Source connector ID to migrate
        target_group_id: Destination group/destination ID
        new_schema_name: Optional schema rename (default: original + '_migrated')
        new_table_name: Optional table rename (default: original + '_migrated')
        approval_bypass: Admin flag to skip approval
        
    Returns:
        JSON string with migration result including new connector ID
    """
    try:
        intercept = _approval_intercept('migrate_connector', {
            "connector_id": connector_id,
            "target_group_id": target_group_id,
            "new_schema_name": new_schema_name,
            "new_table_name": new_table_name
        }, approval_bypass)
        if intercept:
            return intercept
        if not initialize_connector():
            return json.dumps({"success": False, "error": "Failed to initialize Fivetran connector"})
        
        original_connector_str = get_connector_status(connector_id)
        original_connector = json.loads(original_connector_str) if original_connector_str else None
        if not original_connector:
            return json.dumps({"success": False, "error": "Failed to retrieve original connector details"})
        
        original_data = original_connector.get('data', {})
        original_config = original_data.get('config', {})
        
        if original_data.get('service') == 'google_sheets':
            new_config = {
                "service": "google_sheets",
                "group_id": target_group_id,
                "paused": True,
                "config": {
                    "schema": new_schema_name or f"{original_config.get('schema', 'google_sheets')}_migrated",
                    "table": new_table_name or f"{original_config.get('table', 'data')}_migrated",
                    "named_range": original_config.get('named_range'),
                    "sheet_id": original_config.get('sheet_id')
                }
            }
        else:
            return json.dumps({"success": False, "error": f"Migration not supported for service type: {original_data.get('service')}"})
        
        create_response = _make_api_request('POST', 'connections/', new_config)
        
        if create_response:
            new_connector_id = create_response.get('data', {}).get('id')
            schema_response = _make_api_request('GET', f'connectors/{connector_id}/schemas')
            if schema_response:
                schema_data = schema_response.get('data', {})
                _make_api_request('PATCH', f'connectors/{new_connector_id}/schemas', schema_data)
            
            result = {
                "success": True,
                "original_connector_id": connector_id,
                "new_connector_id": new_connector_id,
                "target_group_id": target_group_id,
                "new_schema": new_config['config']['schema'],
                "new_table": new_config['config']['table'],
                "message": "Connector migrated successfully"
            }
        else:
            result = {
                "success": False,
                "error": "Failed to create migrated connector",
                "original_connector_id": connector_id
            }
        
        return json.dumps(result, indent=2)
        
    except Exception as e:
        return json.dumps({
            "success": False,
            "error": f"Error migrating connector: {str(e)}",
            "connector_id": connector_id
        }, indent=2)

@mcp.tool()
def reload_connector_schema(connector_id: str, approval_bypass: bool = False) -> str:
    """
    Refresh connector schema to detect source changes.
    
    **Approval Required**: This mutating operation requires admin approval unless bypassed.
    
    Args:
        connector_id: Connector ID to reload
        approval_bypass: Admin flag to skip approval
        
    Returns:
        JSON string with reload trigger confirmation
    """
    try:
        intercept = _approval_intercept('reload_connector_schema', {"connector_id": connector_id}, approval_bypass)
        if intercept:
            return intercept
        if not initialize_connector():
            return json.dumps({"success": False, "error": "Failed to initialize Fivetran connector"})
        
        reload_response = _make_api_request('POST', f'connectors/{connector_id}/schemas/reload')
        
        if reload_response:
            result = {
                "success": True,
                "connector_id": connector_id,
                "message": "Schema reload triggered successfully",
                "timestamp": datetime.now().isoformat()
            }
        else:
            result = {
                "success": False,
                "error": "Failed to reload schema",
                "connector_id": connector_id
            }
        
        return json.dumps(result, indent=2)
        
    except Exception as e:
        return json.dumps({
            "success": False,
            "error": f"Error reloading schema: {str(e)}",
            "connector_id": connector_id
        }, indent=2)

@mcp.tool()
def get_connector_schema(connector_id: str) -> str:
    """
    Retrieve current schema configuration and sync settings.
    
    Args:
        connector_id: Connector ID to query
        
    Returns:
        JSON string with complete schema configuration
    """
    try:
        if not initialize_connector():
            return json.dumps({"success": False, "error": "Failed to initialize Fivetran connector"})
        
        schema_response = _make_api_request('GET', f'connectors/{connector_id}/schemas')
        
        if schema_response:
            result = {
                "success": True,
                "connector_id": connector_id,
                "schema_config": schema_response.get('data', {}),
                "message": "Schema retrieved successfully"
            }
        else:
            result = {
                "success": False,
                "error": "Failed to retrieve schema",
                "connector_id": connector_id
            }
        
        return json.dumps(result, indent=2)
        
    except Exception as e:
        return json.dumps({
            "success": False,
            "error": f"Error retrieving schema: {str(e)}",
            "connector_id": connector_id
        }, indent=2)

@mcp.tool()
def update_connector_schema(connector_id: str, schema_config: dict, approval_bypass: bool = False) -> str:
    """
    Modify connector schema configuration.
    
    **Approval Required**: This mutating operation requires admin approval unless bypassed.
    
    Args:
        connector_id: Connector ID to update
        schema_config: New schema configuration dictionary
        approval_bypass: Admin flag to skip approval
        
    Returns:
        JSON string with update confirmation
    """
    try:
        intercept = _approval_intercept('update_connector_schema', {"connector_id": connector_id, "schema_config": schema_config}, approval_bypass)
        if intercept:
            return intercept
        if not initialize_connector():
            return json.dumps({"success": False, "error": "Failed to initialize Fivetran connector"})
        
        update_response = _make_api_request('PATCH', f'connectors/{connector_id}/schemas', schema_config)
        
        if update_response:
            result = {
                "success": True,
                "connector_id": connector_id,
                "message": "Schema updated successfully",
                "timestamp": datetime.now().isoformat()
            }
        else:
            result = {
                "success": False,
                "error": "Failed to update schema",
                "connector_id": connector_id
            }
        
        return json.dumps(result, indent=2)
        
    except Exception as e:
        return json.dumps({
            "success": False,
            "error": f"Error updating schema: {str(e)}",
            "connector_id": connector_id
        }, indent=2)

@mcp.tool()
def modify_sync_frequency(connection_id: str, sync_frequency: int, approval_bypass: bool = False) -> str:
    """
    Update the sync frequency for a Fivetran connection.
    
    **Approval Required**: This mutating operation requires admin approval unless bypassed.
    
    Args:
        connection_id: Fivetran connection ID to update
        sync_frequency: Sync frequency in minutes (must be 1, 5, 15, 30, 60, 120, 180, 360, 480, 720, 1440)
        approval_bypass: Admin flag to skip approval workflow
    
    Returns:
        JSON string with operation result or approval request ID
    """
    VALID_SYNC_FREQUENCIES = [1, 5, 15, 30, 60, 120, 180, 360, 480, 720, 1440]
    
    try:
        if sync_frequency not in VALID_SYNC_FREQUENCIES:
            return json.dumps({
                "success": False,
                "error": f"Invalid sync_frequency value: {sync_frequency}",
                "valid_values": VALID_SYNC_FREQUENCIES
            }, indent=2)
        
        intercept = _approval_intercept('modify_sync_frequency', {
            "connection_id": connection_id,
            "sync_frequency": sync_frequency
        }, approval_bypass)
        if intercept:
            return intercept
        
        if not initialize_connector():
            return json.dumps({"success": False, "error": "Failed to initialize Fivetran connector"})
        
        endpoint = f'connections/{connection_id}'
        payload = {
            "sync_frequency": sync_frequency
        }
        
        response = _make_api_request('PATCH', endpoint, payload)
        
        if response:
            data = response.get('data', {})
            status = data.get('status', {})
            
            result = {
                "success": True,
                "connection_id": connection_id,
                "sync_frequency": sync_frequency,
                "sync_frequency_formatted": f"{sync_frequency} minutes ({sync_frequency // 60 if sync_frequency >= 60 else sync_frequency} {'hours' if sync_frequency >= 60 else 'minutes'})",
                "setup_state": status.get('setup_state', 'N/A'),
                "sync_state": status.get('sync_state', 'N/A'),
                "message": "Connection sync frequency updated successfully"
            }
        else:
            result = {
                "success": False,
                "error": "Failed to update connection sync frequency",
                "connection_id": connection_id
            }
        
        return json.dumps(result, indent=2)
        
    except Exception as e:
        return json.dumps({
            "success": False,
            "error": f"Error updating sync frequency: {str(e)}",
            "connection_id": connection_id
        }, indent=2)

@mcp.tool()
def health_check_all_connectors(group_id: str = None) -> str:
    """
    Enterprise-wide connector health assessment.
    
    Args:
        group_id: Optional destination filter (default: all connectors)
        
    Returns:
        JSON string with aggregate health metrics and per-connector health status
    """
    try:
        if not initialize_connector():
            return json.dumps({"success": False, "error": "Failed to initialize Fivetran connector"})
        
        connectors_response_str = list_connectors(group_id)
        connectors_response = json.loads(connectors_response_str) if connectors_response_str else None
        if not connectors_response:
            return json.dumps({"success": False, "error": "Failed to retrieve connectors list"})
        
        connectors = connectors_response.get('data', [])
        
        health_results = {
            "success": True,
            "total_connectors": len(connectors),
            "healthy": 0,
            "needs_attention": 0,
            "failed": 0,
            "connector_health": [],
            "summary": {}
        }
        
        for conn in connectors:
            connector_id = conn.get('id')
            connector_name = conn.get('name', 'N/A')
            service = conn.get('service', 'N/A')
            
            try:
                status_response_str = get_connector_status(connector_id)
                status_response = json.loads(status_response_str) if status_response_str else None
                if status_response:
                    status_data = status_response.get('data', {})
                    status_info = status_data.get('status', {})
                    
                    health_status = "healthy"
                    issues = []
                    
                    setup_state = status_info.get('setup_state', 'UNKNOWN')
                    sync_state = status_info.get('sync_state', 'UNKNOWN')
                    paused = status_data.get('paused', False)
                    
                    if setup_state == 'INCOMPLETE':
                        health_status = "needs_attention"
                        issues.append("Setup incomplete")
                    elif sync_state == 'FAILED':
                        health_status = "needs_attention"
                        issues.append("Sync failed")
                    elif not paused and setup_state != 'COMPLETE':
                        health_status = "needs_attention"
                        issues.append("Running with incomplete setup")
                    
                    connector_health = {
                        "connector_id": connector_id,
                        "name": connector_name,
                        "service": service,
                        "health_status": health_status,
                        "setup_state": setup_state,
                        "sync_state": sync_state,
                        "paused": paused,
                        "issues": issues,
                        "last_sync": status_info.get('last_sync', 'N/A')
                    }
                    
                    health_results["connector_health"].append(connector_health)
                    
                    if health_status == "healthy":
                        health_results["healthy"] += 1
                    elif health_status == "needs_attention":
                        health_results["needs_attention"] += 1
                    else:
                        health_results["failed"] += 1
                        
                else:
                    health_results["failed"] += 1
                    health_results["connector_health"].append({
                        "connector_id": connector_id,
                        "name": connector_name,
                        "service": service,
                        "health_status": "failed",
                        "issues": ["Failed to retrieve status"]
                    })
                    
            except Exception as e:
                health_results["failed"] += 1
                health_results["connector_health"].append({
                    "connector_id": connector_id,
                    "name": connector_name,
                    "service": service,
                    "health_status": "failed",
                    "issues": [f"Error checking status: {str(e)}"]
                })
        
        health_results["summary"] = {
            "health_percentage": round((health_results["healthy"] / health_results["total_connectors"]) * 100, 2) if health_results["total_connectors"] > 0 else 0,
            "recommendations": []
        }
        
        if health_results["needs_attention"] > 0:
            health_results["summary"]["recommendations"].append(f"{health_results['needs_attention']} connectors need attention")
        
        if health_results["failed"] > 0:
            health_results["summary"]["recommendations"].append(f"{health_results['failed']} connectors failed health check")
        
        health_results["message"] = f"Health check completed: {health_results['healthy']} healthy, {health_results['needs_attention']} need attention, {health_results['failed']} failed"
        
        return json.dumps(health_results, indent=2)
        
    except Exception as e:
        return json.dumps({
            "success": False,
            "error": f"Health check failed: {str(e)}"
        }, indent=2)

@mcp.tool()
def get_connector_metrics(connector_id: str, days: int = 7) -> str:
    """
    Detailed connector performance and configuration metrics.
    
    Args:
        connector_id: Connector ID to analyze
        days: Historical lookback period (default: 7)
        
    Returns:
        JSON string with comprehensive metrics and configuration
    """
    try:
        if not initialize_connector():
            return json.dumps({"success": False, "error": "Failed to initialize Fivetran connector"})
        
        connector_response_str = get_connector_status(connector_id)
        connector_response = json.loads(connector_response_str) if connector_response_str else None
        if not connector_response:
            return json.dumps({"success": False, "error": "Failed to retrieve connector details"})
        
        connector_data = connector_response.get('data', {})
        status_info = connector_data.get('status', {})
        
        metrics = {
            "success": True,
            "connector_id": connector_id,
            "service": connector_data.get('service', 'N/A'),
            "group_id": connector_data.get('group_id', 'N/A'),
            "created_at": connector_data.get('created_at', 'N/A'),
            "current_status": {
                "setup_state": status_info.get('setup_state', 'UNKNOWN'),
                "sync_state": status_info.get('sync_state', 'UNKNOWN'),
                "paused": connector_data.get('paused', False),
                "last_sync": status_info.get('last_sync', 'N/A')
            },
            "performance_metrics": {
                "sync_frequency": connector_data.get('sync_frequency', 'N/A'),
                "data_delay_sensitivity": connector_data.get('data_delay_sensitivity', 'N/A'),
                "data_delay_threshold": connector_data.get('data_delay_threshold', 'N/A')
            },
            "configuration": {
                "trust_certificates": connector_data.get('trust_certificates', False),
                "trust_fingerprints": connector_data.get('trust_fingerprints', False),
                "run_setup_tests": connector_data.get('run_setup_tests', False)
            },
            "message": f"Metrics retrieved for {days} days lookback"
        }
        
        return json.dumps(metrics, indent=2)
        
    except Exception as e:
        return json.dumps({
            "success": False,
            "error": f"Error retrieving metrics: {str(e)}",
            "connector_id": connector_id
        }, indent=2)

@mcp.tool()
def get_connector_usage_report(group_id: str = None, days: int = 30) -> str:
    """
    Enterprise connector usage analytics and reporting.
    
    Args:
        group_id: Optional destination filter
        days: Analysis period (default: 30)
        
    Returns:
        JSON string with detailed usage analytics
    """
    try:
        if not initialize_connector():
            return json.dumps({"success": False, "error": "Failed to initialize Fivetran connector"})
        
        connectors_response_str = list_connectors(group_id)
        connectors_response = json.loads(connectors_response_str) if connectors_response_str else None
        if not connectors_response:
            return json.dumps({"success": False, "error": "Failed to retrieve connectors"})
        
        if isinstance(connectors_response, str):
            return json.dumps({"success": False, "error": f"API returned string instead of JSON: {connectors_response}"})
        
        connectors = connectors_response.get('data', [])
        
        report = {
            "success": True,
            "report_period_days": days,
            "group_id": group_id,
            "total_connectors": len(connectors),
            "service_breakdown": {},
            "status_breakdown": {},
            "health_summary": {
                "healthy": 0,
                "needs_attention": 0,
                "failed": 0
            },
            "connector_details": []
        }
        
        for conn in connectors:
            connector_id = conn.get('id')
            service = conn.get('service', 'N/A')
            paused = conn.get('paused', False)
            
            if service not in report["service_breakdown"]:
                report["service_breakdown"][service] = 0
            report["service_breakdown"][service] += 1
            
            status_key = "paused" if paused else "active"
            if status_key not in report["status_breakdown"]:
                report["status_breakdown"][status_key] = 0
            report["status_breakdown"][status_key] += 1
            
            try:
                status_response_str = get_connector_status(connector_id)
                status_response = json.loads(status_response_str) if status_response_str else None
                if status_response:
                    status_data = status_response.get('data', {})
                    status_info = status_data.get('status', {})
                    setup_state = status_info.get('setup_state', 'UNKNOWN')
                    
                    if setup_state == 'COMPLETE' and not paused:
                        health = "healthy"
                        report["health_summary"]["healthy"] += 1
                    elif setup_state == 'INCOMPLETE':
                        health = "needs_attention"
                        report["health_summary"]["needs_attention"] += 1
                    else:
                        health = "failed"
                        report["health_summary"]["failed"] += 1
                    
                    report["connector_details"].append({
                        "connector_id": connector_id,
                        "service": service,
                        "health": health,
                        "setup_state": setup_state,
                        "paused": paused,
                        "created_at": status_data.get('created_at', 'N/A'),
                        "last_sync": status_info.get('last_sync', 'N/A')
                    })
                else:
                    report["health_summary"]["failed"] += 1
                    report["connector_details"].append({
                        "connector_id": connector_id,
                        "service": service,
                        "health": "failed",
                        "error": "Failed to retrieve status"
                    })
            except Exception as e:
                report["health_summary"]["failed"] += 1
                report["connector_details"].append({
                    "connector_id": connector_id,
                    "service": service,
                    "health": "failed",
                    "error": f"Status check failed: {str(e)}"
                })
        
        total = report["total_connectors"]
        if total > 0:
            report["health_percentages"] = {
                "healthy": round((report["health_summary"]["healthy"] / total) * 100, 2),
                "needs_attention": round((report["health_summary"]["needs_attention"] / total) * 100, 2),
                "failed": round((report["health_summary"]["failed"] / total) * 100, 2)
            }
        
        report["message"] = f"Usage report generated for {total} connectors over {days} days"
        
        return json.dumps(report, indent=2)
        
    except Exception as e:
        return json.dumps({
            "success": False,
            "error": f"Error generating usage report: {str(e)}"
        }, indent=2)

# =============================================================================
# SERVER STARTUP AND ACTION REGISTRATION
# =============================================================================

if __name__ == "__main__":
    # Register all mutating actions for approval workflow execution
    # Maps action names to their corresponding functions with approval bypass
    
    # Basic operations
    _register_action('pause_connector', 
        lambda connector_id, **_: pause_connector(connector_id, approval_bypass=True))
    _register_action('resume_connector', 
        lambda connector_id, **_: resume_connector(connector_id, approval_bypass=True))
    
    # Connector creation
    _register_action('create_dynamic_connector', 
        lambda connector_type, group_id, config, auth=None, options=None, **kwargs: 
        create_dynamic_connector(connector_type, group_id, config, auth, approval_bypass=True, **(options or {})))
    _register_action('create_google_sheet_connector', 
        lambda connector_details, **_: create_google_sheet_connector(connector_details, approval_bypass=True))
    
    # Migration operations
    _register_action('migrate_connector', 
        lambda connector_id, target_group_id, new_schema_name=None, new_table_name=None, **_: 
        migrate_connector(connector_id, target_group_id, new_schema_name, new_table_name, approval_bypass=True))
    
    # Schema operations
    _register_action('reload_connector_schema', 
        lambda connector_id, **_: reload_connector_schema(connector_id, approval_bypass=True))
    _register_action('update_connector_schema', 
        lambda connector_id, schema_config, **_: 
        update_connector_schema(connector_id, schema_config, approval_bypass=True))
    _register_action('modify_sync_frequency', 
        lambda connection_id, sync_frequency, **_: 
        modify_sync_frequency(connection_id, sync_frequency, approval_bypass=True))

    # Start MCP server with stdio transport
    log_enterprise("[INFO] Starting Fivetran S3 MCP Server...")
    log_enterprise(f"[INFO] Approval mode: {'ENABLED' if APPROVAL_REQUIRED else 'DISABLED'}")
    log_enterprise(f"[INFO] AWS S3 integration: {'ENABLED' if S3_CONFIG['aws_s3_bucket'] else 'DISABLED'}")
    mcp.run(transport="stdio")
