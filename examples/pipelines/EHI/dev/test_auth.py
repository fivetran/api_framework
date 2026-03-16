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
    is_local = platform.system() == "Darwin"
    server_key = "MSSQL_SERVER_DIR" if is_local else "MSSQL_SERVER"
    cert_key = "MSSQL_CERT_SERVER_DIR" if is_local else "MSSQL_CERT_SERVER"
    port_key = "MSSQL_PORT_DIR" if is_local else "MSSQL_PORT"
    cert_port_key = "MSSQL_CERT_PORT_DIR" if is_local else "MSSQL_CERT_PORT"
    
    # Get connection parameters
    server = configuration.get(server_key)
    cert_server = configuration.get(cert_key) or server  # Fallback to server if cert_server not provided
    port_str = configuration.get(port_key)
    cert_port_str = configuration.get(cert_port_key)  # Optional - falls back to port if not provided
    # Strip whitespace from cert_port_str to handle empty strings
    if cert_port_str:
        cert_port_str = cert_port_str.strip()
        if not cert_port_str:
            cert_port_str = None
    
    # Validate required parameters
    if not server:
        raise ValueError(f"Missing required configuration: {server_key}. "
                       f"Please provide the MSSQL server address.")
    
    if not port_str:
        raise ValueError(f"Missing required configuration: {port_key}. "
                       f"Please provide the MSSQL port number.")
    
    try:
        port = int(port_str)
    except (ValueError, TypeError):
        raise ValueError(f"Invalid port value: {port_str}. Port must be a number.")
    
    # Check if this is a private link connection
    is_privatelink = 'privatelink' in server.lower()
    
    # For private links, verify DNS resolution first
    if is_privatelink:
        log.info("Private link detected - verifying DNS resolution before connection attempt")
        if not verify_dns_resolution(server):
            log.warning(f"DNS resolution warning for private link {server}, but proceeding with connection attempt")
        else:
            log.info(f"DNS resolution confirmed for private link {server}")
    
    # Get certificate port (falls back to regular port if not specified)
    # For private links, certificate port MUST be explicitly specified
    if cert_port_str:
        try:
            cert_port = int(cert_port_str)
            if cert_port < 1 or cert_port > 65535:
                raise ValueError(f"Invalid cert port: {cert_port} (must be 1-65535)")
            log.info(f"Using separate certificate port: {cert_port} (SQL port: {port})")
        except (ValueError, TypeError):
            log.warning(f"Invalid cert_port value: {cert_port_str}. Falling back to SQL port: {port}")
            cert_port = port
    else:
        # For private links, certificate port is required
        if is_privatelink:
            raise ValueError(
                f"Private link connection requires explicit certificate port. "
                f"Please set {cert_port_key} in configuration. "
                f"For private links, the certificate server typically uses a different port (e.g., 1434) "
                f"than the SQL server port ({port})."
            )
        cert_port = port
        log.info(f"Using SQL port for certificate generation: {port}")
    
    # Validate database, user, and password
    database = configuration.get("MSSQL_DATABASE")
    user = configuration.get("MSSQL_USER")
    password = configuration.get("MSSQL_PASSWORD")
    
    if not database:
        raise ValueError("Missing required configuration: MSSQL_DATABASE")
    if not user:
        raise ValueError("Missing required configuration: MSSQL_USER")
    if not password:
        raise ValueError("Missing required configuration: MSSQL_PASSWORD")
    
    # Log connection attempt (without sensitive data)
    log.info(f"Connecting to MSSQL server: {server}:{port}")
    log.info(f"Database: {database}, User: {user}")
    if 'privatelink' in server.lower():
        log.info("Private link connection detected")
    
    # Handle certificate configuration
    # PRIORITY: If cdw_cert is provided, use it for authentication and do NOT generate certificates
    cafile_cfg = configuration.get("cdw_cert", None)
    cafile = None
    
    # Log certificate source
    if cafile_cfg:
        log.info("cdw_cert found in configuration - will use provided certificate for MSSQL authentication")
    else:
        if cert_port != port or cert_server != server:
            log.info(f"Certificate will be fetched from certificate server: {cert_server}:{cert_port} (SQL server: {server}:{port})")
        else:
            log.info(f"Certificate will be fetched from: {cert_server}:{cert_port}")

    if cafile_cfg:
        # cdw_cert is provided - use it exclusively, do not generate certificates
        log.info("cdw_cert configuration found - using provided certificate and skipping certificate generation")
        
        if cafile_cfg.lstrip().startswith("-----BEGIN"):
            # Inline certificate (PEM format)
            log.info("Using inline certificate from cdw_cert configuration")
            try:
                import OpenSSL.SSL as SSL, OpenSSL.crypto as crypto, pytds.tls
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
            except ImportError:
                log.severe("OpenSSL Python bindings not available. Install pyopenssl package.")
                raise RuntimeError("OpenSSL Python bindings required for inline certificate support")
            except Exception as e:
                log.severe(f"Failed to process inline certificate from cdw_cert: {e}")
                raise RuntimeError(f"Invalid cdw_cert configuration: {e}")
        elif os.path.isfile(cafile_cfg):
            # Certificate file path
            log.info(f"Using certificate file from cdw_cert: {cafile_cfg}")
            cafile = cafile_cfg
        else:
            # cdw_cert is provided but doesn't match expected formats
            # Try to treat it as a certificate string that might work
            log.warning(f"cdw_cert provided but doesn't appear to be a file path or inline PEM certificate.")
            log.warning(f"Value appears to be: {cafile_cfg[:100]}... (first 100 chars)")
            log.warning("Attempting to use cdw_cert as-is. If connection fails, verify the certificate format.")
            
            # Try to create a temp file with the content
            try:
                tmp = tempfile.NamedTemporaryFile(delete=False, suffix='.pem', mode='w')
                tmp.write(cafile_cfg)
                tmp.flush()
                tmp.close()
                cafile = tmp.name
                log.info(f"Created temporary certificate file from cdw_cert: {cafile}")
            except Exception as e:
                log.severe(f"Failed to process cdw_cert value: {e}")
                raise RuntimeError(f"Invalid cdw_cert configuration - could not process as file or inline cert: {e}")
        
        # IMPORTANT: When cdw_cert is provided, we do NOT generate certificates
        # This is especially important for private links where certificate generation may fail
        log.info("cdw_cert is configured - skipping certificate generation (this is expected for private links)")
    else:
        # No cdw_cert provided: generate chain from server and port
        if is_privatelink:
            log.warning("Private link detected but cdw_cert not provided. Certificate generation may fail for private links.")
            log.warning("For private links, it is recommended to provide cdw_cert in configuration to avoid certificate generation issues.")
        
        if not cert_server:
            raise ValueError(f"Cannot generate cert chain: {cert_key} not provided")
        log.info(f"cdw_cert not provided - generating certificate chain for {cert_server}:{cert_port} (SQL server: {server}:{port})")
        if is_privatelink and cert_port == port:
            log.warning(f"WARNING: For private links, certificate port should typically be different from SQL port. "
                       f"Using {cert_port} for certificate generation. If this fails, ensure {cert_port_key} is set correctly.")
        # For private links or when cert server differs from SQL server, allow fallback if certificate generation fails
        is_privatelink_cert = 'privatelink' in (cert_server or server or '').lower()
        cert_server_different = cert_server and cert_server != server
        # Allow fallback for private links or when cert server is different (may not be accessible)
        allow_cert_fallback = is_privatelink_cert or cert_server_different
        
        if is_privatelink_cert:
            log.info("Private link certificate server detected - will use fallback certificate approach if generation fails")
        
        try:
            cafile = generate_cert_chain(cert_server, cert_port, allow_fallback=allow_cert_fallback)
            if cafile is None and allow_cert_fallback:
                log.info("Certificate generation returned None (connection failed) - attempting root certificate fallback")
                # Try to get root certificate as fallback
                try:
                    log.info("Fetching root certificate from DigiCert as fallback...")
                    root_pem = requests.get(
                        'https://cacerts.digicert.com/DigiCertGlobalRootG2.crt.pem',
                        timeout=10
                    ).text
                    if root_pem:
                        tmp = tempfile.NamedTemporaryFile(delete=False, suffix='.pem')
                        tmp.write(root_pem.encode('utf-8'))
                        tmp.flush()
                        tmp.close()
                        cafile = tmp.name
                        log.info(f"Successfully created root certificate fallback file: {cafile}")
                    else:
                        log.warning("Root certificate fetch returned empty content. Will attempt connection without certificate validation.")
                        cafile = None
                except Exception as root_err:
                    log.warning(f"Could not fetch root certificate: {root_err}. Will attempt connection without certificate validation.")
                    cafile = None
        except (ConnectionRefusedError, TimeoutError) as e:
            if allow_cert_fallback:
                log.warning(f"Certificate generation failed: {e}. Using fallback approach.")
                # Try root certificate fallback
                try:
                    root_pem = requests.get(
                        'https://cacerts.digicert.com/DigiCertGlobalRootG2.crt.pem',
                        timeout=10
                    ).text
                    tmp = tempfile.NamedTemporaryFile(delete=False, suffix='.pem')
                    tmp.write(root_pem.encode('utf-8'))
                    tmp.flush()
                    tmp.close()
                    cafile = tmp.name
                    log.info(f"Using root certificate fallback: {cafile}")
                except Exception as root_err:
                    log.warning(f"Could not fetch root certificate: {root_err}. Will attempt connection without certificate validation.")
                    cafile = None
            else:
                log.severe(f"Certificate generation failed: {e}")
                log.severe("Troubleshooting steps:")
                log.severe(f"  1. Verify {server_key} ({server}) and {port_key} ({port}) are correct")
                log.severe(f"  2. Verify {cert_key} ({cert_server}) and {cert_port_key} ({cert_port}) are correct")
                if cert_port != port:
                    log.severe(f"     Note: Certificate port ({cert_port}) is different from SQL port ({port})")
                log.severe(f"  3. Check network connectivity to certificate server: {cert_server}:{cert_port}")
                log.severe(f"  4. Verify firewall rules allow connections to {cert_server}:{cert_port}")
                log.severe(f"  5. For private links, ensure the certificate endpoint is accessible from this environment")
                log.severe(f"  6. Check DNS resolution: nslookup {cert_server}")
                raise
        except Exception as e:
            if allow_cert_fallback:
                log.warning(f"Certificate generation error: {e}. Using fallback approach.")
                cafile = None
            else:
                log.severe(f"Unexpected error during certificate generation: {e}")
                raise RuntimeError(f"Certificate generation failed: {e}")
    
    # Attempt connection with retry logic
    import pytds
    # For private links, use longer default timeout
    default_timeout = 60 if is_privatelink else 30
    connection_timeout = int(configuration.get("connection_timeout", default_timeout))
    
    # For private links, use more retries with exponential backoff
    max_connection_retries = int(configuration.get("connection_retries", 3 if is_privatelink else 1))
    base_retry_delay = 2  # Start with 2 seconds
    
    log.info(f"Attempting connection with timeout: {connection_timeout}s, max retries: {max_connection_retries}")
    if is_privatelink:
        log.info("Private link connection - using extended timeout and retry logic")
    
    # Prepare connection parameters
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
        # Using certificate file from cdw_cert or generated certificate
        conn_params['cafile'] = cafile
        if cafile_cfg:
            log.info(f"Using cdw_cert certificate file for MSSQL authentication: {cafile}")
            log.info(f"Connecting to {server}:{port} with certificate-based authentication")
        else:
            log.info(f"Using generated certificate file: {cafile}")
    elif cafile == 'ignored':
        # Using custom SSL context from inline cdw_cert (already configured via pytds.tls.create_context)
        if cafile_cfg:
            log.info(f"Using inline cdw_cert certificate for MSSQL authentication")
            log.info(f"Connecting to {server}:{port} with certificate-based authentication")
        # No need to set cafile - custom SSL context is already configured
    elif cafile is None:
        # No certificate - connection will proceed without certificate validation
        if cafile_cfg:
            log.warning("cdw_cert was provided but could not be processed - connection will proceed without certificate validation")
        else:
            log.warning("No certificate file available - connection will proceed without certificate validation")
        # pytds will handle this - we just don't pass cafile
    
    # Retry loop for connection attempts
    last_error = None
    for attempt in range(max_connection_retries):
        try:
            if attempt > 0:
                # Exponential backoff with jitter
                delay = base_retry_delay * (2 ** (attempt - 1)) + random.uniform(0, 1)
                log.info(f"Connection attempt {attempt + 1}/{max_connection_retries} after {delay:.2f}s delay")
                time.sleep(delay)
            
            log.info(f"Connection attempt {attempt + 1}/{max_connection_retries} to {server}:{port}")
            conn = pytds.connect(**conn_params)
            log.info("Successfully connected to MSSQL database")
            return conn
        except ConnectionRefusedError as e:
            last_error = e
            log.warning(f"Connection refused on attempt {attempt + 1}/{max_connection_retries}: {e}")
            if attempt + 1 >= max_connection_retries:
                # Final attempt failed
                break
            # Continue to retry
            continue
        except TimeoutError as e:
            last_error = e
            log.warning(f"Connection timeout on attempt {attempt + 1}/{max_connection_retries}: {e}")
            if attempt + 1 >= max_connection_retries:
                # Final attempt failed
                break
            # Continue to retry
            continue
        except OSError as e:
            last_error = e
            error_str = str(e).lower()
            # Check if it's a connection-related OSError
            if 'connection refused' in error_str or 'errno 111' in error_str:
                log.warning(f"Connection refused (OSError) on attempt {attempt + 1}/{max_connection_retries}: {e}")
                if attempt + 1 >= max_connection_retries:
                    # Final attempt failed
                    break
                # Continue to retry
                continue
            elif 'timeout' in error_str:
                log.warning(f"Connection timeout (OSError) on attempt {attempt + 1}/{max_connection_retries}: {e}")
                if attempt + 1 >= max_connection_retries:
                    # Final attempt failed
                    break
                # Continue to retry
                continue
            else:
                # Other OSError - might be network related, retry
                log.warning(f"Network error (OSError) on attempt {attempt + 1}/{max_connection_retries}: {e}")
                if attempt + 1 >= max_connection_retries:
                    # Final attempt failed
                    break
                # Continue to retry
                continue
        except Exception as e:
            # Non-retryable error
            last_error = e
            log.severe(f"Non-retryable connection error: {e}")
            raise
    
    # All retries exhausted - provide detailed error messages
    if last_error:
        error_str = str(last_error).lower()
        if 'connection refused' in error_str or 'errno 111' in error_str:
            log.severe(f"Connection refused to {server}:{port} after {max_connection_retries} attempts")
            log.severe("Troubleshooting steps:")
            log.severe(f"  1. Verify {server_key} = '{server}' is correct")
            log.severe(f"  2. Verify {port_key} = '{port}' is correct")
            log.severe(f"  3. Check network connectivity: Can you reach {server}:{port}?")
            log.severe(f"  4. Verify firewall/security group rules allow connections")
            if is_privatelink:
                log.severe(f"  5. For private links, verify:")
                log.severe(f"     - DNS resolution works: nslookup {server}")
                log.severe(f"     - Private link endpoint is accessible from this environment")
                log.severe(f"     - VPC peering/routing is configured correctly")
                log.severe(f"     - Security groups allow traffic from this source")
            else:
                log.severe(f"  5. Check if SQL Server is running and listening on port {port}")
            raise ConnectionRefusedError(f"Connection refused to {server}:{port} after {max_connection_retries} attempts: {last_error}")
        elif 'timeout' in error_str:
            log.severe(f"Connection timeout to {server}:{port} after {max_connection_retries} attempts (timeout: {connection_timeout}s)")
            log.severe("Troubleshooting steps:")
            log.severe(f"  1. Check network latency to {server}")
            log.severe(f"  2. Verify firewall rules allow connections")
            if is_privatelink:
                log.severe(f"  3. For private links, verify network routing and VPC configuration")
                log.severe(f"  4. Consider increasing connection_timeout in configuration (current: {connection_timeout}s)")
            else:
                log.severe(f"  3. Increase connection_timeout in configuration if needed (current: {connection_timeout}s)")
            raise TimeoutError(f"Connection timeout to {server}:{port} after {max_connection_retries} attempts: {last_error}")
        else:
            log.severe(f"Connection error to {server}:{port} after {max_connection_retries} attempts: {last_error}")
            if is_privatelink:
                log.severe("For private links, verify:")
                log.severe(f"  - DNS resolution: nslookup {server}")
                log.severe(f"  - Network connectivity and routing")
                log.severe(f"  - VPC peering configuration")
            raise RuntimeError(f"Failed to connect to {server}:{port} after {max_connection_retries} attempts: {last_error}")
    
    # Should not reach here, but just in case
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
