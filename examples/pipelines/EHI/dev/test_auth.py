"""
Fivetran Connector SDK - SQL Server Connector with Certificate-Based Authentication

This connector demonstrates how to connect to SQL Server using:
- Private link endpoint support
- Certificate-based authentication (cdw_cert)
- Standard SQL Server authentication (username/password)
- Automatic certificate chain generation

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
import subprocess
from typing import Dict, List, Any, Optional
from datetime import datetime

# Source-specific imports
try:
    import pytds
    PYTDS_AVAILABLE = True
except ImportError:
    PYTDS_AVAILABLE = False
    log.severe("pytds not installed. Install with: pip install pytds")

try:
    import requests
    REQUESTS_AVAILABLE = True
except ImportError:
    REQUESTS_AVAILABLE = False
    log.warning("requests not available. Certificate generation may be limited.")

try:
    import OpenSSL.SSL as SSL
    import OpenSSL.crypto as crypto
    OPENSSL_AVAILABLE = True
except ImportError:
    OPENSSL_AVAILABLE = False
    log.warning("pyopenssl not available. Inline certificate support may be limited.")

# Configuration constants
BATCH_SIZE = 5000
CHECKPOINT_INTERVAL = 100000  # Checkpoint every 100k records
CONNECTION_TIMEOUT = 60
MAX_RETRIES = 3
BASE_RETRY_DELAY = 5


def validate_configuration(configuration: dict):
    """
    Validate the configuration dictionary to ensure it contains all required parameters.
    
    Args:
        configuration: Dictionary holding configuration settings
        
    Raises:
        ValueError: If any required configuration parameter is missing
    """
    is_local = platform.system() == "Darwin"
    server_key = "MSSQL_SERVER_DIR" if is_local else "MSSQL_SERVER"
    port_key = "MSSQL_PORT_DIR" if is_local else "MSSQL_PORT"
    
    required_configs = [
        server_key,
        port_key,
        "MSSQL_DATABASE",
        "MSSQL_USER",
        "MSSQL_PASSWORD"
    ]
    
    for key in required_configs:
        if key not in configuration or not configuration[key]:
            raise ValueError(f"Missing required configuration value: {key}")


def verify_dns_resolution(hostname: str, timeout: int = 10) -> bool:
    """Verify DNS resolution for a hostname, especially important for private links.
    
    Args:
        hostname: Hostname to resolve
        timeout: DNS resolution timeout in seconds
        
    Returns:
        True if DNS resolution succeeds, False otherwise
    """
    try:
        log.info(f"Verifying DNS resolution for {hostname}...")
        socket.gethostbyname(hostname)
        log.info(f"DNS resolution successful for {hostname}")
        return True
    except socket.gaierror as e:
        log.warning(f"DNS resolution failed for {hostname}: {e}")
        return False
    except Exception as e:
        log.warning(f"Error during DNS resolution for {hostname}: {e}")
        return False


def generate_cert_chain(server: str, port: int, timeout: int = 30, allow_fallback: bool = False) -> Optional[str]:
    """Generates a certificate chain file by fetching intermediate and root certificates.
    
    Args:
        server: Server hostname or IP address
        port: Server port number
        timeout: Connection timeout in seconds (default: 30)
        allow_fallback: If True, return None instead of raising on connection errors (for private links)
        
    Returns:
        Path to temporary certificate file, or None if allow_fallback=True and connection fails
        
    Raises:
        RuntimeError: If certificate generation fails and allow_fallback=False
    """
    log.info(f"Generating certificate chain for {server}:{port}")
    
    # Try to fetch certificates with timeout
    try:
        proc = subprocess.run(
            ['openssl', 's_client', '-showcerts', '-connect', f'{server}:{port}', 
             '-servername', server, '-verify_return_error'],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            timeout=timeout
        )
        
        # Log OpenSSL output for debugging
        if proc.stderr:
            log.info(f"OpenSSL stderr: {proc.stderr[:500]}")  # First 500 chars
        
        # Check for connection errors in stderr
        if 'Connection refused' in proc.stderr or 'connect:errno=111' in proc.stderr:
            if allow_fallback:
                log.warning(f"Connection refused to {server}:{port} for certificate generation. "
                           f"This is expected for private link connections. Using fallback certificate.")
                return None
            log.severe(f"Connection refused to {server}:{port}. Check network connectivity and firewall rules.")
            raise ConnectionRefusedError(f"Cannot connect to {server}:{port} - connection refused")
        
        if 'timeout' in proc.stderr.lower() or 'timed out' in proc.stderr.lower():
            if allow_fallback:
                log.warning(f"Connection timeout to {server}:{port} for certificate generation. "
                           f"This is expected for private link connections. Using fallback certificate.")
                return None
            log.severe(f"Connection timeout to {server}:{port}. Check network connectivity.")
            raise TimeoutError(f"Cannot connect to {server}:{port} - connection timeout")
            
    except subprocess.TimeoutExpired:
        if allow_fallback:
            log.warning(f"OpenSSL command timed out for {server}:{port}. Using fallback certificate.")
            return None
        log.severe(f"OpenSSL command timed out after {timeout} seconds for {server}:{port}")
        raise TimeoutError(f"Certificate generation timed out for {server}:{port}")
    except FileNotFoundError:
        log.severe("OpenSSL not found. Please ensure OpenSSL is installed and in PATH.")
        raise RuntimeError("OpenSSL not found - cannot generate certificate chain")
    except (ConnectionRefusedError, TimeoutError):
        # Re-raise these if not allowing fallback
        if not allow_fallback:
            raise
        log.warning(f"Connection error to {server}:{port} for certificate generation. Using fallback certificate.")
        return None
    except Exception as e:
        if allow_fallback:
            log.warning(f"Error running OpenSSL: {e}. Using fallback certificate.")
            return None
        log.severe(f"Error running OpenSSL: {e}")
        raise RuntimeError(f"Failed to generate certificate chain: {e}")
    
    # Fetch root certificate
    root_pem = ""
    if REQUESTS_AVAILABLE:
        try:
            root_pem = requests.get(
                'https://cacerts.digicert.com/DigiCertGlobalRootG2.crt.pem',
                timeout=10
            ).text
        except Exception as e:
            log.warning(f"Failed to fetch root certificate from DigiCert: {e}")
            root_pem = ""
    
    # Check for DNS resolution errors in stderr and stdout
    stderr_lower = (proc.stderr or '').lower()
    stdout_lower = (proc.stdout or '').lower()
    combined_output = stderr_lower + ' ' + stdout_lower
    dns_error_patterns = [
        'name or service not known',
        'name resolution failed',
        'could not resolve hostname',
        'host not found',
        'nodename nor servname provided',
        'bio_lookup_ex',
        'bio routines:bio_lookup_ex',
        'system lib',
        'errno=0',
        'could not resolve',
        'unable to resolve'
    ]
    has_dns_error = any(pattern in combined_output for pattern in dns_error_patterns)
    
    # Log for debugging
    if has_dns_error:
        log.info(f"DNS error detected in OpenSSL output. stderr: {proc.stderr[:200] if proc.stderr else 'None'}")
    
    # Extract PEM blocks from OpenSSL output
    pem_blocks = re.findall(
        r'-----BEGIN CERTIFICATE-----.+?-----END CERTIFICATE-----',
        proc.stdout,
        re.DOTALL
    )
    
    # Handle case where no certificates were found
    if not pem_blocks:
        log.warning(f"No certificates found from OpenSSL for {server}:{port}")
        log.info("OpenSSL output preview:")
        log.info(proc.stdout[:500] if proc.stdout else "No output")
        
        # Check if this is a DNS resolution error - always use fallback for DNS errors
        if has_dns_error:
            log.warning(f"DNS resolution failed for {server}:{port}. "
                       f"This may be expected if the certificate server is not accessible from this environment.")
            log.info("DNS resolution error detected - attempting fallback certificate approach")
            if root_pem:
                # Log the root certificate content
                log.info("=" * 80)
                log.info("GENERATED ROOT CERTIFICATE CONTENT (DNS error fallback):")
                log.info("=" * 80)
                log.info(root_pem)
                log.info("=" * 80)
                
                tmp = tempfile.NamedTemporaryFile(delete=False, suffix='.pem')
                tmp.write(root_pem.encode('utf-8'))
                tmp.flush()
                tmp.close()
                log.info(f"Created certificate file: {tmp.name}")
                return tmp.name
            else:
                log.warning("No root certificate available - will attempt connection without certificate validation")
                return None
        
        # For private link connections or when allow_fallback is True, try using root certificate only
        if 'privatelink' in server.lower() or allow_fallback:
            log.info("Private link detected or fallback allowed - using root certificate only")
            if root_pem:
                # Log the root certificate content
                log.info("=" * 80)
                log.info("GENERATED ROOT CERTIFICATE CONTENT (fallback):")
                log.info("=" * 80)
                log.info(root_pem)
                log.info("=" * 80)
                
                tmp = tempfile.NamedTemporaryFile(delete=False, suffix='.pem')
                tmp.write(root_pem.encode('utf-8'))
                tmp.flush()
                tmp.close()
                log.info(f"Created certificate file: {tmp.name}")
                return tmp.name
            else:
                if allow_fallback:
                    log.warning("No root certificate available - will attempt connection without certificate validation")
                    return None
                log.warning("No root certificate available - connection may fail")
                # Create empty cert file as fallback
                tmp = tempfile.NamedTemporaryFile(delete=False, suffix='.pem')
                tmp.write(b'')
                tmp.flush()
                tmp.close()
                return tmp.name
        else:
            raise RuntimeError(f"No certificates found from OpenSSL for {server}:{port}. "
                             f"OpenSSL stderr: {proc.stderr[:200]}")
    
    # Combine intermediate and root certificates
    intermediate = pem_blocks[1] if len(pem_blocks) > 1 else pem_blocks[0]
    cert_chain = intermediate
    if root_pem:
        cert_chain = intermediate + '\n' + root_pem
    
    # Log the generated certificate content
    log.info("=" * 80)
    log.info("GENERATED CERTIFICATE CONTENT:")
    log.info("=" * 80)
    log.info(cert_chain)
    log.info("=" * 80)
    log.info(f"Certificate chain length: {len(cert_chain)} bytes")
    cert_count = len(re.findall(r'-----BEGIN CERTIFICATE-----', cert_chain))
    log.info(f"Number of certificates in chain: {cert_count}")
    
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix='.pem')
    tmp.write(cert_chain.encode('utf-8'))
    tmp.flush()
    tmp.close()
    log.info(f"Successfully created certificate chain file: {tmp.name} ({len(cert_chain)} bytes)")
    return tmp.name


def connect_to_mssql(configuration: dict):
    """Connects to MSSQL using TDS with SSL cert chain.
    
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
    if not PYTDS_AVAILABLE:
        raise RuntimeError("pytds not available - cannot connect to SQL Server")
    
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
    
    # For private links, verify DNS resolution first
    if is_privatelink:
        log.info("Private link detected - verifying DNS resolution before connection attempt")
        if not verify_dns_resolution(server):
            log.warning(f"DNS resolution warning for private link {server}, but proceeding with connection attempt")
        else:
            log.info(f"DNS resolution confirmed for private link {server}")
    
    # Handle certificate configuration
    cafile = None
    cafile_cfg = cdw_cert.strip() if cdw_cert else None
    
    if cafile_cfg:
        log.info("cdw_cert found in configuration - using provided certificate")
        
        if cafile_cfg.lstrip().startswith("-----BEGIN"):
            # Inline certificate (PEM format)
            log.info("Detected inline PEM certificate")
            if not OPENSSL_AVAILABLE:
                raise RuntimeError("OpenSSL Python bindings not available. Install pyopenssl package.")
            
            try:
                # Build a fresh X509 store and attach it to the SSL context
                ctx = SSL.Context(SSL.TLS_METHOD)
                ctx.set_verify(SSL.VERIFY_PEER, lambda conn, cert, errnum, depth, ok: bool(ok))
                
                pem_blocks = re.findall(
                    r'-----BEGIN CERTIFICATE-----.+?-----END CERTIFICATE-----',
                    cafile_cfg, re.DOTALL
                )
                
                if not pem_blocks:
                    raise ValueError("No valid certificates found in cdw_cert configuration")
                
                # Retrieve existing store and add each PEM certificate
                store = ctx.get_cert_store()
                if store is None:
                    raise RuntimeError("Failed to retrieve certificate store from SSL context")
                
                for pem in pem_blocks:
                    certificate = crypto.load_certificate(crypto.FILETYPE_PEM, pem)
                    store.add_cert(certificate)
                
                # Configure pytds to use the custom SSL context
                pytds.tls.create_context = lambda cafile: ctx
                cafile = 'ignored'
                log.info(f"Loaded {len(pem_blocks)} certificate(s) from inline cdw_cert configuration")
                
            except Exception as e:
                raise RuntimeError(f"Failed to process inline certificate from cdw_cert: {e}")
                
        elif os.path.isfile(cafile_cfg):
            # Certificate file path
            log.info(f"Using certificate file: {cafile_cfg}")
            cafile = cafile_cfg
        else:
            # Try to create a temp file with the content
            log.warning("Certificate doesn't appear to be a file path or inline PEM. Creating temp file...")
            try:
                tmp = tempfile.NamedTemporaryFile(delete=False, suffix='.pem', mode='w')
                tmp.write(cafile_cfg)
                tmp.flush()
                tmp.close()
                cafile = tmp.name
                log.info(f"Created temporary certificate file: {cafile}")
            except Exception as e:
                raise RuntimeError(f"Failed to process cdw_cert value: {e}")
    else:
        log.warning("No cdw_cert provided. Attempting to generate certificate chain...")
        if is_privatelink:
            log.warning("For private links, certificate is typically required.")
        
        # Try to generate certificate chain
        cert_timeout = int(configuration.get("cert_generation_timeout", 30))
        cert_file = generate_cert_chain(server, port, timeout=cert_timeout, allow_fallback=is_privatelink)
        if cert_file:
            cafile = cert_file
            log.info(f"Using generated certificate file: {cafile}")
            
            # Read and log the certificate content from the file
            try:
                with open(cafile, 'r') as f:
                    cert_content = f.read()
                log.info("=" * 80)
                log.info("CERTIFICATE CONTENT FROM GENERATED FILE:")
                log.info("=" * 80)
                log.info(cert_content)
                log.info("=" * 80)
            except Exception as e:
                log.warning(f"Could not read certificate file for logging: {e}")
        else:
            log.warning("Certificate generation failed or returned None. Connection will proceed without certificate validation.")
    
    # Prepare connection parameters
    connection_timeout = int(configuration.get("connection_timeout", CONNECTION_TIMEOUT))
    
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
    max_connection_retries = int(configuration.get("connection_retries", MAX_RETRIES))
    last_error = None
    
    for attempt in range(max_connection_retries):
        try:
            if attempt > 0:
                delay = BASE_RETRY_DELAY * (2 ** (attempt - 1))
                log.info(f"Retry attempt {attempt + 1}/{max_connection_retries} after {delay}s delay...")
                import time
                time.sleep(delay)
            
            log.info(f"Connection attempt {attempt + 1}/{max_connection_retries} to {server}:{port}...")
            conn = pytds.connect(**conn_params)
            log.info("✓ Connection established successfully!")
            return conn
            
        except ConnectionRefusedError as e:
            last_error = e
            log.warning(f"Connection refused: {e}")
            if attempt + 1 >= max_connection_retries:
                break
            continue
            
        except TimeoutError as e:
            last_error = e
            log.warning(f"Connection timeout: {e}")
            if attempt + 1 >= max_connection_retries:
                break
            continue
            
        except OSError as e:
            last_error = e
            error_str = str(e).lower()
            if 'connection refused' in error_str or 'errno 111' in error_str:
                log.warning(f"Connection refused (OSError): {e}")
            elif 'timeout' in error_str:
                log.warning(f"Connection timeout (OSError): {e}")
            else:
                log.warning(f"Network error (OSError): {e}")
            if attempt + 1 >= max_connection_retries:
                break
            continue
            
        except Exception as e:
            last_error = e
            log.warning(f"Connection error: {e}")
            log.warning(f"Error type: {type(e).__name__}")
            if attempt + 1 >= max_connection_retries:
                break
            continue
    
    # All retries exhausted
    error_msg = f"Failed to establish connection to {server}:{port} after {max_connection_retries} attempts"
    if last_error:
        error_msg += f": {last_error}"
    log.severe(error_msg)
    raise RuntimeError(error_msg)


def schema(configuration: dict):
    """
    Define the schema function which lets you configure the schema your connector delivers.
    See: https://fivetran.com/docs/connectors/connector-sdk/technical-reference#schema
    
    Args:
        configuration: Dictionary containing configuration settings
        
    Returns:
        List of table definitions with table name and primary key
    """
    # Example schema - replace with your actual table definitions
    # You can query the database to discover tables dynamically
    return [
        {
            "table": "example_table",
            "primary_key": ["id"],
            "columns": {
                "id": "STRING",
                "name": "STRING",
                "created_at": "STRING"
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
               (empty for first sync or full re-sync)
    """
    log.info("Starting sync")
    
    # Validate configuration
    validate_configuration(configuration=configuration)
    
    # Extract configuration parameters
    schema_name = configuration.get("MSSQL_SCHEMA", "dbo")
    batch_size = int(configuration.get("batch_size", BATCH_SIZE))
    checkpoint_interval = int(configuration.get("checkpoint_interval", CHECKPOINT_INTERVAL))
    
    # Get state variables
    last_sync_time = state.get("last_sync_time")
    
    try:
        # Connect to database
        log.info("Establishing connection to SQL Server...")
        conn = connect_to_mssql(configuration)
        
        try:
            cursor = conn.cursor()
            
            # Example: Fetch data from a table
            # Replace this with your actual query logic
            table_name = "example_table"
            qualified_table = f"{schema_name}.{table_name}" if schema_name else table_name
            
            if last_sync_time:
                # Incremental sync
                log.info(f"Performing incremental sync for {table_name} since {last_sync_time}")
                query = f"""
                    SELECT id, name, created_at 
                    FROM {qualified_table}
                    WHERE created_at > '{last_sync_time}'
                    ORDER BY created_at
                """
            else:
                # Full sync
                log.info(f"Performing full sync for {table_name}")
                query = f"""
                    SELECT id, name, created_at 
                    FROM {qualified_table}
                    ORDER BY created_at
                """
            
            log.info(f"Executing query: {query[:200]}...")
            cursor.execute(query)
            
            # Get column names
            cols = [d[0] for d in getattr(cursor, 'description', [])]
            log.info(f"Columns: {cols}")
            
            records_processed = 0
            new_sync_time = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
            
            # Process data in batches
            while True:
                rows = cursor.fetchmany(batch_size)
                if not rows:
                    break
                
                for row in rows:
                    # Convert row to dictionary
                    record = {cols[i]: row[i] for i in range(len(cols))}
                    
                    # Direct operation call without yield - per SDK best practices
                    op.upsert(table=table_name, data=record)
                    records_processed += 1
                    
                    # Checkpoint at regular intervals
                    if records_processed % checkpoint_interval == 0:
                        log.info(f"Checkpointing after {records_processed} records")
                        new_state = {"last_sync_time": new_sync_time}
                        op.checkpoint(state=new_state)
            
            # Final checkpoint
            new_state = {"last_sync_time": new_sync_time}
            op.checkpoint(state=new_state)
            
            log.info(f"Sync completed successfully. Processed {records_processed} records.")
            
            cursor.close()
            
        finally:
            conn.close()
            log.info("Connection closed")
        
    except Exception as e:
        log.severe(f"Sync failed: {str(e)}")
        raise RuntimeError(f"Failed to sync data: {str(e)}")


# Initialize the connector with the defined update and schema functions
connector = Connector(update=update, schema=schema)

# Main entry point for local testing
# This is not called by Fivetran in production
# Test using: fivetran debug --configuration configuration.json
if __name__ == "__main__":
    current_dir = os.path.dirname(os.path.abspath(__file__))
    config_path = os.path.join(current_dir, "configuration.json")
    
    with open(config_path, 'r') as f:
        configuration = json.load(f)
    
    connector.debug(configuration=configuration)

