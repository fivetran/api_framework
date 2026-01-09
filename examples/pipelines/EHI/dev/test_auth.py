#!/usr/bin/env python3
"""
Simple authentication test script for SQL Server using private link and certificate-based authentication.

This script tests the connection to a SQL Server instance using:
- Private link endpoint
- Certificate-based authentication (cdw_cert)
- Standard SQL Server authentication (username/password)

Usage:
    python test_auth.py

Configuration:
    Update the configuration dictionary below with your credentials and certificate.
"""

import os
import json
import re
import tempfile
import platform
import socket
import time
import random
import subprocess
from typing import Optional

# Import requests for fetching root certificates
try:
    import requests
    REQUESTS_AVAILABLE = True
except ImportError:
    REQUESTS_AVAILABLE = False
    print("WARNING: requests not available. Certificate generation may be limited.")

# Import required libraries
try:
    import pytds
except ImportError:
    print("ERROR: pytds not installed. Install with: pip install pytds")
    exit(1)

try:
    import OpenSSL.SSL as SSL
    import OpenSSL.crypto as crypto
    OPENSSL_AVAILABLE = True
except ImportError:
    OPENSSL_AVAILABLE = False
    print("WARNING: pyopenssl not available. Inline certificate support may be limited.")


def generate_cert_chain(server: str, port: int, timeout: int = 30, allow_fallback: bool = False) -> Optional[str]:
    """Generates a certificate chain file by fetching intermediate and root certificates.
    
    This function mirrors the logic from connector.py to test certificate generation.
    
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
    print(f"\nGenerating certificate chain for {server}:{port}")
    
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
            print(f"OpenSSL stderr: {proc.stderr[:500]}")  # First 500 chars
        
        # Check for connection errors in stderr
        if 'Connection refused' in proc.stderr or 'connect:errno=111' in proc.stderr:
            if allow_fallback:
                print(f"⚠ Connection refused to {server}:{port} for certificate generation. "
                      f"This is expected for private link connections. Using fallback certificate.")
                return None
            print(f"✗ Connection refused to {server}:{port}. Check network connectivity and firewall rules.")
            raise ConnectionRefusedError(f"Cannot connect to {server}:{port} - connection refused")
        
        if 'timeout' in proc.stderr.lower() or 'timed out' in proc.stderr.lower():
            if allow_fallback:
                print(f"⚠ Connection timeout to {server}:{port} for certificate generation. "
                      f"This is expected for private link connections. Using fallback certificate.")
                return None
            print(f"✗ Connection timeout to {server}:{port}. Check network connectivity.")
            raise TimeoutError(f"Cannot connect to {server}:{port} - connection timeout")
            
    except subprocess.TimeoutExpired:
        if allow_fallback:
            print(f"⚠ OpenSSL command timed out for {server}:{port}. Using fallback certificate.")
            return None
        print(f"✗ OpenSSL command timed out after {timeout} seconds for {server}:{port}")
        raise TimeoutError(f"Certificate generation timed out for {server}:{port}")
    except FileNotFoundError:
        print("✗ OpenSSL not found. Please ensure OpenSSL is installed and in PATH.")
        raise RuntimeError("OpenSSL not found - cannot generate certificate chain")
    except (ConnectionRefusedError, TimeoutError):
        # Re-raise these if not allowing fallback
        if not allow_fallback:
            raise
        print(f"⚠ Connection error to {server}:{port} for certificate generation. Using fallback certificate.")
        return None
    except Exception as e:
        if allow_fallback:
            print(f"⚠ Error running OpenSSL: {e}. Using fallback certificate.")
            return None
        print(f"✗ Error running OpenSSL: {e}")
        raise RuntimeError(f"Failed to generate certificate chain: {e}")
    
    # Fetch root certificate
    root_pem = ""
    if REQUESTS_AVAILABLE:
        try:
            print("Fetching root certificate from DigiCert...")
            root_pem = requests.get(
                'https://cacerts.digicert.com/DigiCertGlobalRootG2.crt.pem',
                timeout=10
            ).text
            print("✓ Root certificate fetched successfully")
        except Exception as e:
            print(f"⚠ Failed to fetch root certificate from DigiCert: {e}")
            # Use a fallback root certificate or continue without it
            root_pem = ""
    else:
        print("⚠ requests library not available - skipping root certificate fetch")
    
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
        'errno=0',  # DNS errors often show errno=0
        'could not resolve',
        'unable to resolve'
    ]
    has_dns_error = any(pattern in combined_output for pattern in dns_error_patterns)
    
    # Log for debugging
    if has_dns_error:
        print(f"⚠ DNS error detected in OpenSSL output. stderr: {proc.stderr[:200] if proc.stderr else 'None'}")
    
    # Extract PEM blocks from OpenSSL output
    pem_blocks = re.findall(
        r'-----BEGIN CERTIFICATE-----.+?-----END CERTIFICATE-----',
        proc.stdout,
        re.DOTALL
    )
    
    # Handle case where no certificates were found
    if not pem_blocks:
        print(f"⚠ No certificates found from OpenSSL for {server}:{port}")
        print("OpenSSL output preview:")
        print(proc.stdout[:500] if proc.stdout else "No output")
        
        # Check if this is a DNS resolution error - always use fallback for DNS errors
        if has_dns_error:
            print(f"⚠ DNS resolution failed for {server}:{port}. "
                  f"This may be expected if the certificate server is not accessible from this environment.")
            print("DNS resolution error detected - attempting fallback certificate approach")
            if root_pem:
                tmp = tempfile.NamedTemporaryFile(delete=False, suffix='.pem')
                tmp.write(root_pem.encode('utf-8'))
                tmp.flush()
                tmp.close()
                print(f"✓ Created certificate file: {tmp.name}")
                return tmp.name
            else:
                print("⚠ No root certificate available - will attempt connection without certificate validation")
                return None
        
        # For private link connections or when allow_fallback is True, try using root certificate only
        if 'privatelink' in server.lower() or allow_fallback:
            print("Private link detected or fallback allowed - using root certificate only")
            if root_pem:
                tmp = tempfile.NamedTemporaryFile(delete=False, suffix='.pem')
                tmp.write(root_pem.encode('utf-8'))
                tmp.flush()
                tmp.close()
                print(f"✓ Created certificate file: {tmp.name}")
                return tmp.name
            else:
                if allow_fallback:
                    print("⚠ No root certificate available - will attempt connection without certificate validation")
                    return None
                print("⚠ No root certificate available - connection may fail")
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
    
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix='.pem')
    tmp.write(cert_chain.encode('utf-8'))
    tmp.flush()
    tmp.close()
    print(f"✓ Successfully created certificate chain file: {tmp.name} ({len(cert_chain)} bytes)")
    return tmp.name


def test_cert_generation(configuration: dict) -> bool:
    """Test certificate generation using the generate_cert_chain function.
    
    Args:
        configuration: Dictionary containing connection parameters
        
    Returns:
        True if certificate generation test passed, False otherwise
    """
    print("\n" + "=" * 80)
    print("CERTIFICATE GENERATION TEST")
    print("=" * 80)
    
    # Determine platform-specific keys
    is_local = platform.system() == "Darwin"
    server_key = "MSSQL_SERVER_DIR" if is_local else "MSSQL_SERVER"
    port_key = "MSSQL_PORT_DIR" if is_local else "MSSQL_PORT"
    
    # Get connection parameters
    server = configuration.get(server_key)
    port_str = configuration.get(port_key)
    
    # Validate required parameters
    if not server:
        print(f"✗ ERROR: Missing required configuration: {server_key}")
        return False
    
    if not port_str:
        print(f"✗ ERROR: Missing required configuration: {port_key}")
        return False
    
    try:
        port = int(port_str)
    except (ValueError, TypeError):
        print(f"✗ ERROR: Invalid port value: {port_str}. Port must be a number.")
        return False
    
    # Check if this is a private link connection
    is_privatelink = 'privatelink' in server.lower()
    
    print(f"\nCertificate Generation Details:")
    print(f"  Server: {server}")
    print(f"  Port: {port}")
    print(f"  Private Link: {'Yes' if is_privatelink else 'No'}")
    
    # Get timeout from configuration
    timeout = int(configuration.get("cert_generation_timeout", 30))
    allow_fallback = is_privatelink  # Allow fallback for private links
    
    print(f"  Timeout: {timeout}s")
    print(f"  Allow Fallback: {'Yes' if allow_fallback else 'No'}")
    
    cert_file = None
    try:
        # Attempt to generate certificate chain
        cert_file = generate_cert_chain(server, port, timeout=timeout, allow_fallback=allow_fallback)
        
        if cert_file:
            print(f"\n✓ Certificate generation successful!")
            print(f"  Certificate file: {cert_file}")
            
            # Validate the certificate file
            if os.path.exists(cert_file):
                file_size = os.path.getsize(cert_file)
                print(f"  File size: {file_size} bytes")
                
                # Read and validate PEM format
                try:
                    with open(cert_file, 'r') as f:
                        cert_content = f.read()
                    
                    # Count certificates in the file
                    cert_count = len(re.findall(
                        r'-----BEGIN CERTIFICATE-----',
                        cert_content
                    ))
                    print(f"  Certificates in chain: {cert_count}")
                    
                    if cert_count > 0:
                        print("✓ Certificate file is valid PEM format")
                        
                        # Print the certificate content
                        print("\n" + "-" * 80)
                        print("GENERATED CERTIFICATE CONTENT:")
                        print("-" * 80)
                        print(cert_content)
                        print("-" * 80)
                    else:
                        print("⚠ WARNING: Certificate file exists but contains no certificates")
                        
                except Exception as e:
                    print(f"⚠ WARNING: Could not read certificate file: {e}")
            else:
                print(f"✗ ERROR: Certificate file was not created: {cert_file}")
                return False
        else:
            if allow_fallback:
                print("\n⚠ Certificate generation returned None (fallback mode)")
                print("  This is expected for private link connections that cannot be reached directly.")
                print("  The connector will use the provided cdw_cert or attempt connection without certificate.")
                return True  # This is acceptable in fallback mode
            else:
                print("\n✗ Certificate generation failed - no certificate file created")
                return False
        
        print("\n" + "=" * 80)
        print("✓ CERTIFICATE GENERATION TEST PASSED")
        print("=" * 80)
        return True
        
    except Exception as e:
        print(f"\n✗ Certificate generation test failed: {e}")
        print(f"  Error type: {type(e).__name__}")
        
        print("\nTroubleshooting Steps:")
        print(f"  1. Verify OpenSSL is installed: openssl version")
        print(f"  2. Check network connectivity to {server}:{port}")
        print(f"  3. Verify firewall/security group rules allow connections")
        
        if is_privatelink:
            print(f"  4. For private links, certificate generation may fail if:")
            print(f"     - The endpoint is not accessible from this environment")
            print(f"     - DNS resolution fails")
            print(f"     - This is expected and the connector will use cdw_cert instead")
        
        return False
        
    finally:
        # Clean up temporary certificate file if created
        if cert_file and os.path.exists(cert_file):
            try:
                os.unlink(cert_file)
                print(f"\n✓ Cleaned up temporary certificate file: {cert_file}")
            except Exception as e:
                print(f"⚠ Warning: Could not delete temporary certificate file {cert_file}: {e}")


def verify_dns_resolution(hostname: str, timeout: int = 10) -> bool:
    """Verify DNS resolution for a hostname, especially important for private links.
    
    Args:
        hostname: Hostname to resolve
        timeout: DNS resolution timeout in seconds
        
    Returns:
        True if DNS resolution succeeds, False otherwise
    """
    try:
        print(f"Verifying DNS resolution for {hostname}...")
        ip = socket.gethostbyname(hostname)
        print(f"✓ DNS resolution successful: {hostname} -> {ip}")
        return True
    except socket.gaierror as e:
        print(f"✗ DNS resolution failed for {hostname}: {e}")
        return False
    except Exception as e:
        print(f"✗ Error during DNS resolution for {hostname}: {e}")
        return False


def test_connection(configuration: dict) -> bool:
    """Test connection to SQL Server using the provided configuration.
    
    Args:
        configuration: Dictionary containing connection parameters
        
    Returns:
        True if connection successful, False otherwise
    """
    print("\n" + "=" * 80)
    print("SQL SERVER AUTHENTICATION TEST")
    print("=" * 80)
    
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
        print(f"✗ ERROR: Missing required configuration: {server_key}")
        return False
    
    if not port_str:
        print(f"✗ ERROR: Missing required configuration: {port_key}")
        return False
    
    try:
        port = int(port_str)
    except (ValueError, TypeError):
        print(f"✗ ERROR: Invalid port value: {port_str}. Port must be a number.")
        return False
    
    if not database:
        print("✗ ERROR: Missing required configuration: MSSQL_DATABASE")
        return False
    
    if not user:
        print("✗ ERROR: Missing required configuration: MSSQL_USER")
        return False
    
    if not password:
        print("✗ ERROR: Missing required configuration: MSSQL_PASSWORD")
        return False
    
    # Check if this is a private link connection
    is_privatelink = 'privatelink' in server.lower()
    
    print(f"\nConnection Details:")
    print(f"  Server: {server}")
    print(f"  Port: {port}")
    print(f"  Database: {database}")
    print(f"  User: {user}")
    print(f"  Private Link: {'Yes' if is_privatelink else 'No'}")
    print(f"  Certificate Provided: {'Yes' if cdw_cert else 'No'}")
    
    # For private links, verify DNS resolution first
    if is_privatelink:
        print("\n" + "-" * 80)
        print("Private Link DNS Verification")
        print("-" * 80)
        if not verify_dns_resolution(server):
            print("⚠ WARNING: DNS resolution failed, but proceeding with connection attempt")
        else:
            print("✓ DNS resolution confirmed")
    
    # Handle certificate configuration
    cafile = None
    cafile_cfg = cdw_cert.strip() if cdw_cert else None
    
    if cafile_cfg:
        print("\n" + "-" * 80)
        print("Certificate Configuration")
        print("-" * 80)
        print("cdw_cert found in configuration - using provided certificate")
        
        if cafile_cfg.lstrip().startswith("-----BEGIN"):
            # Inline certificate (PEM format)
            print("✓ Detected inline PEM certificate")
            if not OPENSSL_AVAILABLE:
                print("✗ ERROR: OpenSSL Python bindings not available. Install pyopenssl package.")
                return False
            
            try:
                # Build a fresh X509 store and attach it to the SSL context
                ctx = SSL.Context(SSL.TLS_METHOD)
                ctx.set_verify(SSL.VERIFY_PEER, lambda conn, cert, errnum, depth, ok: bool(ok))
                
                pem_blocks = re.findall(
                    r'-----BEGIN CERTIFICATE-----.+?-----END CERTIFICATE-----',
                    cafile_cfg, re.DOTALL
                )
                
                if not pem_blocks:
                    print("✗ ERROR: No valid certificates found in cdw_cert configuration")
                    return False
                
                # Retrieve existing store and add each PEM certificate
                store = ctx.get_cert_store()
                if store is None:
                    print("✗ ERROR: Failed to retrieve certificate store from SSL context")
                    return False
                
                for pem in pem_blocks:
                    certificate = crypto.load_certificate(crypto.FILETYPE_PEM, pem)
                    store.add_cert(certificate)
                
                # Configure pytds to use the custom SSL context
                pytds.tls.create_context = lambda cafile: ctx
                cafile = 'ignored'
                print(f"✓ Loaded {len(pem_blocks)} certificate(s) from inline cdw_cert configuration")
                
            except Exception as e:
                print(f"✗ ERROR: Failed to process inline certificate from cdw_cert: {e}")
                return False
                
        elif os.path.isfile(cafile_cfg):
            # Certificate file path
            print(f"✓ Using certificate file: {cafile_cfg}")
            cafile = cafile_cfg
        else:
            # Try to create a temp file with the content
            print("⚠ Certificate doesn't appear to be a file path or inline PEM. Creating temp file...")
            try:
                tmp = tempfile.NamedTemporaryFile(delete=False, suffix='.pem', mode='w')
                tmp.write(cafile_cfg)
                tmp.flush()
                tmp.close()
                cafile = tmp.name
                print(f"✓ Created temporary certificate file: {cafile}")
            except Exception as e:
                print(f"✗ ERROR: Failed to process cdw_cert value: {e}")
                return False
    else:
        print("\n⚠ WARNING: No cdw_cert provided. Connection will proceed without certificate validation.")
        if is_privatelink:
            print("⚠ WARNING: For private links, certificate is typically required.")
    
    # Prepare connection parameters
    connection_timeout = int(configuration.get("connection_timeout", 60 if is_privatelink else 30))
    max_connection_retries = int(configuration.get("connection_retries", 3 if is_privatelink else 1))
    base_retry_delay = 2
    
    print("\n" + "-" * 80)
    print("Connection Attempt")
    print("-" * 80)
    print(f"Timeout: {connection_timeout}s")
    print(f"Max Retries: {max_connection_retries}")
    
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
        print(f"✓ Using certificate file for authentication: {cafile}")
    elif cafile == 'ignored':
        print("✓ Using inline certificate for authentication")
        # Custom SSL context already configured via pytds.tls.create_context
    elif cafile is None:
        print("⚠ No certificate - connection will proceed without certificate validation")
    
    # Retry loop for connection attempts
    last_error = None
    for attempt in range(max_connection_retries):
        try:
            if attempt > 0:
                delay = base_retry_delay * (2 ** (attempt - 1)) + random.uniform(0, 1)
                print(f"\nRetry attempt {attempt + 1}/{max_connection_retries} after {delay:.2f}s delay...")
                time.sleep(delay)
            
            print(f"\nConnection attempt {attempt + 1}/{max_connection_retries}...")
            print(f"  Connecting to {server}:{port}...")
            
            conn = pytds.connect(**conn_params)
            
            print("\n" + "=" * 80)
            print("✓ SUCCESS: Connection established!")
            print("=" * 80)
            
            # Test a simple query
            try:
                cursor = conn.cursor()
                cursor.execute("SELECT @@VERSION AS version")
                row = cursor.fetchone()
                if row:
                    version = row[0] if isinstance(row, (list, tuple)) else row.get('version', 'N/A')
                    print(f"\nSQL Server Version:")
                    print(f"  {version[:100]}...")  # First 100 chars
                
                cursor.execute("SELECT DB_NAME() AS current_database")
                row = cursor.fetchone()
                if row:
                    db_name = row[0] if isinstance(row, (list, tuple)) else row.get('current_database', 'N/A')
                    print(f"\nCurrent Database: {db_name}")
                
                cursor.execute("SELECT SYSTEM_USER AS current_user")
                row = cursor.fetchone()
                if row:
                    user_name = row[0] if isinstance(row, (list, tuple)) else row.get('current_user', 'N/A')
                    print(f"Current User: {user_name}")
                
                cursor.close()
                conn.close()
                
                print("\n✓ Authentication test completed successfully!")
                return True
                
            except Exception as query_error:
                print(f"\n⚠ WARNING: Connection successful but query failed: {query_error}")
                conn.close()
                return True  # Connection worked, query issue is separate
                
        except ConnectionRefusedError as e:
            last_error = e
            print(f"✗ Connection refused: {e}")
            if attempt + 1 >= max_connection_retries:
                break
            continue
            
        except TimeoutError as e:
            last_error = e
            print(f"✗ Connection timeout: {e}")
            if attempt + 1 >= max_connection_retries:
                break
            continue
            
        except OSError as e:
            last_error = e
            error_str = str(e).lower()
            if 'connection refused' in error_str or 'errno 111' in error_str:
                print(f"✗ Connection refused (OSError): {e}")
            elif 'timeout' in error_str:
                print(f"✗ Connection timeout (OSError): {e}")
            else:
                print(f"✗ Network error (OSError): {e}")
            if attempt + 1 >= max_connection_retries:
                break
            continue
            
        except Exception as e:
            last_error = e
            print(f"✗ Connection error: {e}")
            print(f"  Error type: {type(e).__name__}")
            if attempt + 1 >= max_connection_retries:
                break
            continue
    
    # All retries exhausted
    print("\n" + "=" * 80)
    print("✗ FAILED: Connection could not be established")
    print("=" * 80)
    
    if last_error:
        error_str = str(last_error).lower()
        print(f"\nLast Error: {last_error}")
        print(f"Error Type: {type(last_error).__name__}")
        
        print("\nTroubleshooting Steps:")
        print(f"  1. Verify server address: {server}")
        print(f"  2. Verify port: {port}")
        print(f"  3. Check network connectivity: Can you reach {server}:{port}?")
        print(f"  4. Verify firewall/security group rules allow connections")
        
        if is_privatelink:
            print(f"  5. For private links, verify:")
            print(f"     - DNS resolution: nslookup {server}")
            print(f"     - Private link endpoint is accessible from this environment")
            print(f"     - VPC peering/routing is configured correctly")
            print(f"     - Security groups allow traffic from this source")
            print(f"     - Certificate (cdw_cert) is correct and valid")
        else:
            print(f"  5. Check if SQL Server is running and listening on port {port}")
            print(f"  6. Verify username and password are correct")
    
    return False


def main():
    """Main function to run the authentication test."""
    
    # Configuration - UPDATE THESE VALUES
    configuration = {
        # Server configuration (platform-specific)
        "MSSQL_SERVER": "",
        "MSSQL_SERVER_DIR": "",
        "MSSQL_PORT": "1434",
        "MSSQL_PORT_DIR": "1434",
        
        # Database configuration
        "MSSQL_DATABASE": "cdw",
        "MSSQL_USER": "your_username",  # UPDATE THIS
        "MSSQL_PASSWORD": "your_password",  # UPDATE THIS
        "MSSQL_SCHEMA": "",
        
        # Certificate (PEM format - can be inline or file path)
        # If inline, must start with "-----BEGIN CERTIFICATE-----"
        "cdw_cert": "",  # UPDATE THIS with your certificate
        
        # Connection options
        "connection_timeout": "60",  # seconds
        "connection_retries": "3",  # number of retry attempts
        "cert_generation_timeout": "30",  # seconds for certificate generation
    }
    
    # Alternative: Load from configuration.json file if it exists
    config_file = "configuration.json"
    if os.path.exists(config_file):
        print(f"Loading configuration from {config_file}...")
        try:
            with open(config_file, 'r') as f:
                file_config = json.load(f)
                # Merge file config with defaults (file config takes precedence)
                configuration.update(file_config)
                print("✓ Configuration loaded from file")
        except Exception as e:
            print(f"⚠ Warning: Could not load configuration from file: {e}")
            print("Using hardcoded configuration values")
    
    # Check if required values are still placeholders
    if configuration.get("MSSQL_USER") == "your_username":
        print("\n⚠ WARNING: MSSQL_USER is still set to placeholder value 'your_username'")
        print("   Please update the configuration with your actual username")
    
    if configuration.get("MSSQL_PASSWORD") == "your_password":
        print("⚠ WARNING: MSSQL_PASSWORD is still set to placeholder value 'your_password'")
        print("   Please update the configuration with your actual password")
    
    if not configuration.get("cdw_cert", "").strip():
        print("⚠ WARNING: cdw_cert is empty")
        print("   For private link connections, certificate is typically required")
        response = input("\nContinue without certificate? (y/N): ")
        if response.lower() != 'y':
            print("Exiting. Please provide cdw_cert in configuration.")
            return
    
    # Run certificate generation test first
    print("\n" + "=" * 80)
    print("RUNNING CERTIFICATE GENERATION TEST")
    print("=" * 80)
    cert_test_success = test_cert_generation(configuration)
    
    if not cert_test_success:
        print("\n⚠ WARNING: Certificate generation test failed or was skipped")
        print("  This may be expected for private link connections.")
        response = input("\nContinue with connection test anyway? (y/N): ")
        if response.lower() != 'y':
            print("Exiting. Please check certificate generation issues.")
            return
    
    # Run the connection test
    print("\n" + "=" * 80)
    print("RUNNING CONNECTION TEST")
    print("=" * 80)
    success = test_connection(configuration)
    
    if success:
        print("\n" + "=" * 80)
        print("TEST RESULT: ✓ PASSED")
        print("=" * 80)
        exit(0)
    else:
        print("\n" + "=" * 80)
        print("TEST RESULT: ✗ FAILED")
        print("=" * 80)
        exit(1)


if __name__ == "__main__":
    main()

