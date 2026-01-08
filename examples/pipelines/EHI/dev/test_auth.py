"""
SQL Server Authentication Test Connector

This connector demonstrates how to test authentication to SQL Server using:
- Private link endpoint connectivity
- Certificate-based authentication (cdw_cert)
- Standard SQL Server authentication (username/password)

The connector performs a simple authentication test and records the results in a test table.

See the Technical Reference documentation:
https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update

And the Best Practices documentation:
https://fivetran.com/docs/connectors/connector-sdk/best-practices
"""

# Required imports
from fivetran_connector_sdk import Connector, Logging as log, Operations as op
import json
import os
import re
import tempfile
import platform
import socket
import time
import random
from datetime import datetime
from typing import Dict, Any, Optional

# Source-specific imports
try:
    import pytds
except ImportError:
    raise ImportError("pytds not installed. Install with: pip install pytds")

try:
    import OpenSSL.SSL as SSL
    import OpenSSL.crypto as crypto
    OPENSSL_AVAILABLE = True
except ImportError:
    OPENSSL_AVAILABLE = False
    log.warning("pyopenssl not available. Inline certificate support may be limited.")


def validate_configuration(configuration: dict):
    """
    Validate the configuration dictionary to ensure it contains all required parameters.
    
    Args:
        configuration: Dictionary holding configuration settings for the connector.
        
    Raises:
        ValueError: If any required configuration parameter is missing.
    """
    # Determine platform-specific keys
    is_local = platform.system() == "Darwin"
    server_key = "MSSQL_SERVER_DIR" if is_local else "MSSQL_SERVER"
    port_key = "MSSQL_PORT_DIR" if is_local else "MSSQL_PORT"
    
    required_configs = [
        server_key, port_key, "MSSQL_DATABASE", "MSSQL_USER", "MSSQL_PASSWORD"
    ]
    
    for key in required_configs:
        if key not in configuration:
            raise ValueError(f"Missing required configuration value: {key}")


def verify_dns_resolution(hostname: str) -> bool:
    """
    Verify DNS resolution for a hostname, especially important for private links.
    
    Args:
        hostname: Hostname to resolve
        
    Returns:
        True if DNS resolution succeeds, False otherwise
    """
    try:
        socket.gethostbyname(hostname)
        log.info(f"DNS resolution successful for {hostname}")
        return True
    except socket.gaierror as e:
        log.warning(f"DNS resolution failed for {hostname}: {e}")
        return False
    except Exception as e:
        log.warning(f"Error during DNS resolution for {hostname}: {e}")
        return False


def connect_to_sql_server(configuration: dict):
    """
    Connect to SQL Server using TDS with SSL certificate chain.
    
    Args:
        configuration: Configuration dictionary with connection parameters
        
    Returns:
        pytds connection object
        
    Raises:
        ValueError: If required configuration parameters are missing
        ConnectionRefusedError: If connection is refused
        TimeoutError: If connection times out
        RuntimeError: For other connection errors
    """
    # Determine platform-specific keys
    is_local = platform.system() == "Darwin"
    server_key = "MSSQL_SERVER_DIR" if is_local else "MSSQL_SERVER"
    port_key = "MSSQL_PORT_DIR" if is_local else "MSSQL_PORT"
    
    # Get connection parameters
    server = configuration.get(server_key)
    port_str = configuration.get(port_key)
    database = configuration.get("MSSQL_DATABASE")
    user = configuration.get("MSSQL_USER")
    password = configuration.get("MSSQL_PASSWORD")
    cdw_cert = configuration.get("cdw_cert", "")
    
    # Validate required parameters
    if not server:
        raise ValueError(f"Missing required configuration: {server_key}")
    if not port_str:
        raise ValueError(f"Missing required configuration: {port_key}")
    
    try:
        port = int(port_str)
    except (ValueError, TypeError):
        raise ValueError(f"Invalid port value: {port_str}. Port must be a number.")
    
    if not database:
        raise ValueError("Missing required configuration: MSSQL_DATABASE")
    if not user:
        raise ValueError("Missing required configuration: MSSQL_USER")
    if not password:
        raise ValueError("Missing required configuration: MSSQL_PASSWORD")
    
    # Check if this is a private link connection
    is_privatelink = 'privatelink' in server.lower()
    
    log.info(f"Connecting to SQL Server: {server}:{port}")
    log.info(f"Database: {database}, User: {user}")
    if is_privatelink:
        log.info("Private link connection detected")
        # Verify DNS resolution
        if not verify_dns_resolution(server):
            log.warning(f"DNS resolution warning for private link {server}, but proceeding")
    
    # Handle certificate configuration
    cafile = None
    cafile_cfg = cdw_cert.strip() if cdw_cert else None
    
    if cafile_cfg:
        log.info("cdw_cert found in configuration - using provided certificate")
        
        # Decode JSON-escaped sequences (e.g., \\n -> \n, \\t -> \t, etc.)
        # This handles certificates stored in JSON config files where newlines are escaped
        # When certificates are stored in JSON, newlines are often escaped as \\n
        # We need to convert these to actual newlines for PEM parsing to work
        original_length = len(cafile_cfg)
        # Replace escaped sequences (looking for literal backslash-n, not actual newline)
        cafile_cfg = cafile_cfg.replace('\\n', '\n').replace('\\t', '\t').replace('\\r', '\r')
        if len(cafile_cfg) != original_length:
            log.info("Decoded JSON escape sequences in certificate (\\n -> newline, \\t -> tab, \\r -> carriage return)")
        
        if cafile_cfg.lstrip().startswith("-----BEGIN"):
            # Inline certificate (PEM format)
            log.info("Using inline certificate from cdw_cert configuration")
            if not OPENSSL_AVAILABLE:
                raise RuntimeError("OpenSSL Python bindings required for inline certificate support. Install pyopenssl.")
            
            try:
                # Build a fresh X509 store and attach it to the SSL context
                ctx = SSL.Context(SSL.TLS_METHOD)
                ctx.set_verify(SSL.VERIFY_PEER, lambda conn, cert, errnum, depth, ok: bool(ok))
                
                # Look for both certificates and certificate chains
                # Support multiple PEM block types: CERTIFICATE, RSA PRIVATE KEY, etc.
                pem_blocks = re.findall(
                    r'-----BEGIN (?:CERTIFICATE|RSA PRIVATE KEY|PRIVATE KEY|PUBLIC KEY)-----.+?-----END (?:CERTIFICATE|RSA PRIVATE KEY|PRIVATE KEY|PUBLIC KEY)-----',
                    cafile_cfg, re.DOTALL
                )
                
                # If no blocks found with the broader pattern, try just certificates
                if not pem_blocks:
                    pem_blocks = re.findall(
                        r'-----BEGIN CERTIFICATE-----.+?-----END CERTIFICATE-----',
                        cafile_cfg, re.DOTALL
                    )
                
                if not pem_blocks:
                    # Log the first 200 chars for debugging
                    log.warning(f"No PEM blocks found. Certificate starts with: {cafile_cfg[:200]}")
                    raise ValueError("No valid certificates found in cdw_cert configuration")
                
                # Retrieve existing store and add each PEM certificate
                store = ctx.get_cert_store()
                if store is None:
                    raise RuntimeError("Failed to retrieve certificate store from SSL context")
                
                certificates_loaded = 0
                for pem in pem_blocks:
                    # Only process certificates, skip private keys
                    if '-----BEGIN CERTIFICATE-----' in pem:
                        certificate = crypto.load_certificate(crypto.FILETYPE_PEM, pem)
                        store.add_cert(certificate)
                        certificates_loaded += 1
                    else:
                        log.info(f"Skipping non-certificate PEM block (likely a private key)")
                
                if certificates_loaded == 0:
                    raise ValueError("No certificates found in cdw_cert configuration (only private keys or other PEM blocks)")
                
                # Configure pytds to use the custom SSL context
                pytds.tls.create_context = lambda cafile: ctx
                cafile = 'ignored'
                log.info(f"Loaded {certificates_loaded} certificate(s) from inline cdw_cert configuration")
                
            except Exception as e:
                log.severe(f"Failed to process inline certificate from cdw_cert: {e}")
                log.severe(f"Certificate preview (first 300 chars): {cafile_cfg[:300]}")
                raise RuntimeError(f"Invalid cdw_cert configuration: {e}")
                
        elif os.path.isfile(cafile_cfg):
            # Certificate file path
            log.info(f"Using certificate file from cdw_cert: {cafile_cfg}")
            cafile = cafile_cfg
        else:
            # Try to create a temp file with the content
            log.warning("cdw_cert doesn't appear to be a file path or inline PEM. Creating temp file...")
            try:
                tmp = tempfile.NamedTemporaryFile(delete=False, suffix='.pem', mode='w')
                tmp.write(cafile_cfg)
                tmp.flush()
                tmp.close()
                cafile = tmp.name
                log.info(f"Created temporary certificate file from cdw_cert: {cafile}")
            except Exception as e:
                log.severe(f"Failed to process cdw_cert value: {e}")
                raise RuntimeError(f"Invalid cdw_cert configuration: {e}")
    else:
        log.warning("No cdw_cert provided. Connection will proceed without certificate validation.")
        if is_privatelink:
            log.warning("For private links, certificate is typically required.")
    
    # Prepare connection parameters
    connection_timeout = int(configuration.get("connection_timeout", "60" if is_privatelink else "30"))
    max_connection_retries = int(configuration.get("connection_retries", "3" if is_privatelink else "1"))
    base_retry_delay = 2
    
    log.info(f"Attempting connection with timeout: {connection_timeout}s, max retries: {max_connection_retries}")
    
    conn_params = {
        'server': server,
        'database': database,
        'user': user,
        'password': password,
        'port': port,
        'validate_host': False,
        'timeout': connection_timeout
    }
    
    # Handle certificate file for authentication
    if cafile and cafile != 'ignored':
        conn_params['cafile'] = cafile
        log.info(f"Using certificate file for authentication: {cafile}")
    elif cafile == 'ignored':
        log.info("Using inline certificate for authentication")
        # Custom SSL context already configured via pytds.tls.create_context
    elif cafile is None:
        log.warning("No certificate - connection will proceed without certificate validation")
    
    # Retry loop for connection attempts
    last_error = None
    for attempt in range(max_connection_retries):
        try:
            if attempt > 0:
                delay = base_retry_delay * (2 ** (attempt - 1)) + random.uniform(0, 1)
                log.info(f"Connection attempt {attempt + 1}/{max_connection_retries} after {delay:.2f}s delay")
                time.sleep(delay)
            
            log.info(f"Connection attempt {attempt + 1}/{max_connection_retries} to {server}:{port}")
            conn = pytds.connect(**conn_params)
            log.info("Successfully connected to SQL Server database")
            return conn
            
        except (ConnectionRefusedError, TimeoutError, OSError) as e:
            last_error = e
            log.warning(f"Connection error on attempt {attempt + 1}/{max_connection_retries}: {e}")
            if attempt + 1 >= max_connection_retries:
                break
            continue
            
        except Exception as e:
            last_error = e
            log.severe(f"Non-retryable connection error: {e}")
            raise
    
    # All retries exhausted
    if last_error:
        error_str = str(last_error).lower()
        if 'connection refused' in error_str or 'errno 111' in error_str:
            raise ConnectionRefusedError(f"Connection refused to {server}:{port} after {max_connection_retries} attempts: {last_error}")
        elif 'timeout' in error_str:
            raise TimeoutError(f"Connection timeout to {server}:{port} after {max_connection_retries} attempts: {last_error}")
        else:
            raise RuntimeError(f"Failed to connect to {server}:{port} after {max_connection_retries} attempts: {last_error}")
    
    raise RuntimeError(f"Failed to connect to {server}:{port} - unknown error")


def schema(configuration: dict):
    """
    Define the schema function which lets you configure the schema your connector delivers.
    See: https://fivetran.com/docs/connectors/connector-sdk/technical-reference#schema
    
    Args:
        configuration: Dictionary containing configuration settings
        
    Returns:
        List of table definitions with table name and primary key
    """
    return [
        {
            "table": "auth_test_results",
            "primary_key": ["test_id"],
            "columns": {
                "test_id": "STRING",
                "test_timestamp": "STRING",
                "server_address": "STRING",
                "database_name": "STRING",
                "connection_status": "STRING",
                "dns_resolution": "STRING",
                "certificate_used": "STRING",
                "sql_server_version": "STRING",
                "current_database": "STRING",
                "current_user": "STRING",
                "error_message": "STRING"
            }
        },
    ]


def update(configuration: dict, state: dict):
    """
    Define the update function, which is called by Fivetran during each sync.
    See: https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update
    
    Args:
        configuration: Dictionary containing connection details
        state: Dictionary containing state information from previous runs
               (empty for first sync or for any full re-sync)
    """
    log.info("Starting authentication test")
    
    # Validate configuration
    validate_configuration(configuration=configuration)
    
    # Determine platform-specific keys
    is_local = platform.system() == "Darwin"
    server_key = "MSSQL_SERVER_DIR" if is_local else "MSSQL_SERVER"
    
    # Extract configuration parameters
    server = configuration.get(server_key)
    database = configuration.get("MSSQL_DATABASE")
    cdw_cert = configuration.get("cdw_cert", "")
    
    # Generate test ID
    test_id = f"test_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"
    test_timestamp = datetime.utcnow().isoformat() + "Z"
    
    # Initialize result data
    result_data = {
        "test_id": test_id,
        "test_timestamp": test_timestamp,
        "server_address": server or "N/A",
        "database_name": database or "N/A",
        "connection_status": "FAILED",
        "dns_resolution": "NOT_TESTED",
        "certificate_used": "No" if not cdw_cert else "Yes",
        "sql_server_version": "N/A",
        "current_database": "N/A",
        "current_user": "N/A",
        "error_message": "N/A"
    }
    
    try:
        # Test DNS resolution for private links
        is_privatelink = 'privatelink' in (server or '').lower()
        if is_privatelink:
            log.info("Testing DNS resolution for private link")
            if verify_dns_resolution(server):
                result_data["dns_resolution"] = "SUCCESS"
            else:
                result_data["dns_resolution"] = "FAILED"
                log.warning("DNS resolution failed, but proceeding with connection attempt")
        
        # Attempt connection
        log.info("Attempting to connect to SQL Server")
        conn = connect_to_sql_server(configuration)
        
        # Connection successful - test queries
        result_data["connection_status"] = "SUCCESS"
        log.info("Connection successful - executing test queries")
        
        try:
            cursor = conn.cursor()
            
            # Get SQL Server version
            cursor.execute("SELECT @@VERSION AS version")
            row = cursor.fetchone()
            if row:
                version = row[0] if isinstance(row, (list, tuple)) else row.get('version', 'N/A')
                result_data["sql_server_version"] = str(version)[:200]  # Limit length
                log.info(f"SQL Server version retrieved: {str(version)[:100]}...")
            
            # Get current database
            cursor.execute("SELECT DB_NAME() AS current_database")
            row = cursor.fetchone()
            if row:
                db_name = row[0] if isinstance(row, (list, tuple)) else row.get('current_database', 'N/A')
                result_data["current_database"] = str(db_name)
                log.info(f"Current database: {db_name}")
            
            # Get current user
            cursor.execute("SELECT SYSTEM_USER AS current_user")
            row = cursor.fetchone()
            if row:
                user_name = row[0] if isinstance(row, (list, tuple)) else row.get('current_user', 'N/A')
                result_data["current_user"] = str(user_name)
                log.info(f"Current user: {user_name}")
            
            cursor.close()
            conn.close()
            
            log.info("Authentication test completed successfully")
            
        except Exception as query_error:
            log.warning(f"Connection successful but query failed: {query_error}")
            result_data["error_message"] = f"Query error: {str(query_error)}"
            conn.close()
        
        # Upsert test result - direct operation call without yield
        op.upsert(table="auth_test_results", data=result_data)
        log.info(f"Test result recorded: {test_id}")
        
    except Exception as e:
        # Connection or other error
        error_msg = str(e)
        result_data["connection_status"] = "FAILED"
        result_data["error_message"] = error_msg
        log.severe(f"Authentication test failed: {error_msg}")
        
        # Upsert test result with error - direct operation call without yield
        op.upsert(table="auth_test_results", data=result_data)
        log.info(f"Test result recorded with error: {test_id}")
        
        # Don't raise - we want to record the failure
        log.warning("Authentication test failed, but result has been recorded")
    
    # Update state for next run
    new_state = {
        "last_test_timestamp": test_timestamp,
        "last_test_id": test_id
    }
    
    # Checkpoint state to save progress
    # This ensures the sync can resume from the correct position
    # See: https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation
    op.checkpoint(state=new_state)
    
    log.info("Authentication test sync completed")


# Initialize the connector with the defined update and schema functions
connector = Connector(update=update, schema=schema)

# Check if the script is being run as the main module.
# This is Python's standard entry method allowing your script to be run directly from the command line or IDE 'run' button.
# This is useful for debugging while you write your code. Note this method is not called by Fivetran when executing your connector in production.
# Please test using the Fivetran debug command prior to finalizing and deploying your connector.
if __name__ == "__main__":
    # Open the configuration.json file and load its contents
    current_dir = os.path.dirname(os.path.abspath(__file__))
    config_path = os.path.join(current_dir, "configuration.json")
    
    with open(config_path, 'r') as f:
        configuration = json.load(f)
    
    # Test the connector locally
    connector.debug(configuration=configuration)

