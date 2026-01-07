
import os
import json
import re
import requests
import concurrent.futures
from datetime import datetime
import math
from fivetran_connector_sdk import Connector, Logging as log, Operations as op
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
import platform
import tempfile
import subprocess
from contextlib import contextmanager
import queue
import time
import random
import threading
from typing import Dict, List, Any, Optional, Tuple

# Try to import psutil for resource monitoring, fallback gracefully if not available
try:
    import psutil
    PSUTIL_AVAILABLE = True
except ImportError:
    PSUTIL_AVAILABLE = False
    log.warning("psutil not available - resource monitoring will be disabled")

# Configuration constants
BATCH_SIZE = 5000
PARTITION_SIZE = 50000
CHECKPOINT_INTERVAL = 1000000  # Checkpoint every 1 million records
CONNECTION_TIMEOUT_HOURS = 3  # Reconnect after 3 hours
MAX_RETRIES = 5
BASE_RETRY_DELAY = 5
MAX_RETRY_DELAY = 300  # 5 minutes max delay

# Table size thresholds for adaptive processing
SMALL_TABLE_THRESHOLD = 1000000  # 1M rows
LARGE_TABLE_THRESHOLD = 50000000  # 50M rows

# Resource monitoring thresholds
MEMORY_THRESHOLD_HIGH = 80  # 80% memory usage triggers reduction
MEMORY_THRESHOLD_CRITICAL = 90  # 90% memory usage triggers aggressive reduction
CPU_THRESHOLD_HIGH = 85  # 85% CPU usage triggers thread reduction
CPU_THRESHOLD_CRITICAL = 95  # 95% CPU usage triggers aggressive reduction

# Resource monitoring state
resource_state = {
    'memory_pressure': False,
    'cpu_pressure': False,
    'batch_size_reduced': False,
    'threads_reduced': False,
    'last_monitoring': None,
    'monitoring_interval': 3600  # Check every 1 hour
}

def monitor_resources() -> Dict[str, Any]:
    """Monitor system resources and return current status."""
    if not PSUTIL_AVAILABLE:
        return {'status': 'disabled', 'reason': 'psutil not available'}
    
    try:
        # Get current resource usage
        memory = psutil.virtual_memory()
        cpu_percent = psutil.cpu_percent(interval=1)
        
        # Calculate memory usage percentage
        memory_usage = memory.percent
        memory_available_gb = memory.available / (1024**3)
        
        # Get disk usage for the current working directory
        disk = psutil.disk_usage('.')
        disk_usage = (disk.used / disk.total) * 100
        
        # Determine resource pressure levels
        memory_pressure = memory_usage > MEMORY_THRESHOLD_HIGH
        memory_critical = memory_usage > MEMORY_THRESHOLD_CRITICAL
        cpu_pressure = cpu_percent > CPU_THRESHOLD_HIGH
        cpu_critical = cpu_percent > CPU_THRESHOLD_CRITICAL
        
        # Log resource status
        log.info(f"Resource Monitor: Memory {memory_usage:.1f}% ({memory_available_gb:.1f}GB available), "
                f"CPU {cpu_percent:.1f}%, Disk {disk_usage:.1f}%")
        
        # Log warnings for high resource usage
        if memory_critical:
            log.warning(f"CRITICAL MEMORY USAGE: {memory_usage:.1f}% - System under severe memory pressure!")
        elif memory_pressure:
            log.warning(f"HIGH MEMORY USAGE: {memory_usage:.1f}% - Consider reducing batch sizes")
        
        if cpu_critical:
            log.warning(f"CRITICAL CPU USAGE: {cpu_percent:.1f}% - System under severe CPU pressure!")
        elif cpu_pressure:
            log.warning(f"HIGH CPU USAGE: {cpu_percent:.1f}% - Consider reducing thread count")
        
        return {
            'status': 'active',
            'memory_usage': memory_usage,
            'memory_available_gb': memory_available_gb,
            'cpu_percent': cpu_percent,
            'disk_usage': disk_usage,
            'memory_pressure': memory_pressure,
            'memory_critical': memory_critical,
            'cpu_pressure': cpu_pressure,
            'cpu_critical': cpu_critical,
            'timestamp': datetime.utcnow()
        }
        
    except Exception as e:
        log.warning(f"Resource monitoring failed: {e}")
        return {'status': 'error', 'error': str(e)}

def should_reduce_batch_size(memory_usage: float, current_batch_size: int) -> Tuple[bool, int]:
    """Determine if batch size should be reduced based on memory pressure."""
    if memory_usage > MEMORY_THRESHOLD_CRITICAL:
        # Critical memory pressure - reduce by 50%
        new_batch_size = max(current_batch_size // 2, 100)
        log.warning(f"CRITICAL MEMORY PRESSURE: Reducing batch size from {current_batch_size:,} to {new_batch_size:,} records")
        return True, new_batch_size
    elif memory_usage > MEMORY_THRESHOLD_HIGH:
        # High memory pressure - reduce by 25%
        new_batch_size = max(int(current_batch_size * 0.75), 100)
        log.info(f"HIGH MEMORY PRESSURE: Reducing batch size from {current_batch_size:,} to {new_batch_size:,} records")
        return True, new_batch_size
    
    return False, current_batch_size

def should_reduce_threads(cpu_percent: float, current_threads: int) -> Tuple[bool, int]:
    """Determine if thread count should be reduced based on CPU pressure."""
    if cpu_percent > CPU_THRESHOLD_CRITICAL:
        # Critical CPU pressure - reduce to 1 thread
        new_threads = 1
        log.warning(f"CRITICAL CPU PRESSURE: Reducing threads from {current_threads} to {new_threads}")
        return True, new_threads
    elif cpu_percent > CPU_THRESHOLD_HIGH:
        # High CPU pressure - reduce by 50%
        new_threads = max(current_threads // 2, 1)
        log.info(f"HIGH CPU PRESSURE: Reducing threads from {current_threads} to {new_threads}")
        return True, new_threads
    
    return False, current_threads

def get_adaptive_parameters_with_monitoring(table_size: int, base_threads: int, base_batch_size: int) -> Dict[str, Any]:
    """Get adaptive parameters considering both table size and current resource pressure."""
    # Get base adaptive parameters
    partition_size = get_adaptive_partition_size(table_size)
    batch_size = get_adaptive_batch_size(table_size)
    threads = get_adaptive_threads(table_size)
    queue_size = get_adaptive_queue_size(table_size)
    checkpoint_interval = get_adaptive_checkpoint_interval(table_size)
    
    # Apply resource monitoring adjustments
    resource_status = monitor_resources()
    
    if resource_status['status'] == 'active':
        # Check if we need to reduce batch size due to memory pressure
        should_reduce_batch, new_batch_size = should_reduce_batch_size(
            resource_status['memory_usage'], batch_size
        )
        if should_reduce_batch:
            batch_size = new_batch_size
            resource_state['batch_size_reduced'] = True
            log.info(f"Resource monitoring adjusted batch size to {batch_size:,} for table with {table_size:,} rows")
        
        # Check if we need to reduce threads due to CPU pressure
        should_reduce_thread, new_threads = should_reduce_threads(
            resource_status['cpu_percent'], threads
        )
        if should_reduce_thread:
            threads = new_threads
            resource_state['threads_reduced'] = True
            log.info(f"Resource monitoring adjusted threads to {threads} for table with {table_size:,} rows")
        
        # Log resource-aware parameter selection
        log.info(f"Resource-aware parameters for {table_size:,} row table: "
                f"{threads} threads, {batch_size:,} batch size, {partition_size:,} partition size")
    
    return {
        'partition_size': partition_size,
        'batch_size': batch_size,
        'threads': threads,
        'queue_size': queue_size,
        'checkpoint_interval': checkpoint_interval,
        'resource_pressure': resource_status.get('memory_pressure', False) or resource_status.get('cpu_pressure', False),
        'resource_status': resource_status
    }

# Adaptive partitioning based on table size
def get_adaptive_partition_size(table_size: int) -> int:
    """Get optimal partition size based on table size."""
    if table_size < SMALL_TABLE_THRESHOLD:
        return PARTITION_SIZE  # 50K for small tables
    elif table_size < LARGE_TABLE_THRESHOLD:
        return PARTITION_SIZE // 2  # 25K for medium tables
    else:
        return PARTITION_SIZE // 10  # 5K for large tables

def get_adaptive_batch_size(table_size: int) -> int:
    """Get optimal batch size based on table size."""
    if table_size < SMALL_TABLE_THRESHOLD:
        return BATCH_SIZE  # 5K for small tables
    elif table_size < LARGE_TABLE_THRESHOLD:
        return BATCH_SIZE // 2  # 2.5K for medium tables
    else:
        return BATCH_SIZE // 5  # 1K for large tables

def get_adaptive_queue_size(table_size: int) -> int:
    """Get optimal queue size based on table size."""
    if table_size < SMALL_TABLE_THRESHOLD:
        return 10000  # 10K for small tables
    elif table_size < LARGE_TABLE_THRESHOLD:
        return 5000  # 5K for medium tables
    else:
        return 1000  # 1K for large tables

def get_adaptive_threads(table_size: int) -> int:
    """Get optimal thread count based on table size, capped at 4 threads."""
    if table_size < SMALL_TABLE_THRESHOLD:
        return 4  # 4 threads for small tables
    elif table_size < LARGE_TABLE_THRESHOLD:
        return 2  # 2 threads for medium tables
    else:
        return 1  # 1 thread for large tables to avoid overwhelming the DB



# Deadlock detection patterns
DEADLOCK_PATTERNS = [
    'deadlock',
    'lock timeout',
    'lock wait timeout',
    'transaction deadlock',
    'lock request time out period exceeded',
    'lock escalation',
    'lock conflict',
    'blocked by another transaction'
]

# Connection timeout patterns
TIMEOUT_PATTERNS = [
    'connection timeout',
    'connection reset',
    'connection lost',
    'network timeout',
    'read timeout',
    'write timeout',
    'socket timeout',
    'timeout expired'
]

# Additional timeout handling for large tables
def get_adaptive_timeout(table_size: int) -> int:
    """Get adaptive timeout based on table size."""
    if table_size < SMALL_TABLE_THRESHOLD:
        return CONNECTION_TIMEOUT_HOURS  # 3 hours for small tables
    elif table_size < LARGE_TABLE_THRESHOLD:
        return CONNECTION_TIMEOUT_HOURS * 2  # 6 hours for medium tables
    else:
        return CONNECTION_TIMEOUT_HOURS * 4  # 12 hours for large tables

def get_adaptive_checkpoint_interval(table_size: int) -> int:
    """Get adaptive checkpoint interval based on table size."""
    if table_size < SMALL_TABLE_THRESHOLD:
        return CHECKPOINT_INTERVAL  # 1M for small tables
    elif table_size < LARGE_TABLE_THRESHOLD:
        return CHECKPOINT_INTERVAL // 2  # 500K for medium tables
    else:
        return CHECKPOINT_INTERVAL // 10  # 100K for large tables

class ConnectionManager:
    """Manages database connections with timeout and deadlock detection."""
    
    def __init__(self, configuration: dict, table_size: int = 0):
        self.configuration = configuration
        self.table_size = table_size
        self.connection_start_time = None
        self.current_connection = None
        self.current_cursor = None
        self.lock = threading.Lock()
        self.timeout_hours = get_adaptive_timeout(table_size)
        
    def _is_connection_expired(self) -> bool:
        """Check if current connection has exceeded timeout limit."""
        if not self.connection_start_time:
            return True
        elapsed = datetime.utcnow() - self.connection_start_time
        return elapsed.total_seconds() > (self.timeout_hours * 3600)
    
    def _is_deadlock_error(self, error: Exception) -> bool:
        """Detect if error is related to deadlock or lock timeout."""
        error_str = str(error).lower()
        return any(pattern in error_str for pattern in DEADLOCK_PATTERNS)
    
    def _is_timeout_error(self, error: Exception) -> bool:
        """Detect if error is related to connection timeout."""
        error_str = str(error).lower()
        return any(pattern in error_str for pattern in TIMEOUT_PATTERNS)
    
    def _create_connection(self):
        """Create a new database connection."""
        try:
            log.info(f"Creating new database connection (table size: {self.table_size:,} rows)")
            conn = connect_to_mssql(self.configuration)
            self.current_connection = conn
            self.current_cursor = conn.cursor()
            self.connection_start_time = datetime.utcnow()
            log.info(f"New database connection established at {self.connection_start_time}")
            return conn, self.current_cursor
        except (ConnectionRefusedError, TimeoutError) as e:
            log.severe(f"Failed to create database connection: {e}")
            log.severe("This is a network connectivity issue. Please check:")
            log.severe("  1. Server address and port are correct")
            log.severe("  2. Network connectivity to the server")
            log.severe("  3. Firewall/security group rules")
            log.severe("  4. For private links, DNS resolution and endpoint accessibility")
            raise
        except Exception as e:
            log.severe(f"Failed to create database connection: {e}")
            log.severe(f"Error type: {type(e).__name__}")
            raise
    
    def _close_connection(self):
        """Close current database connection."""
        try:
            if self.current_cursor:
                self.current_cursor.close()
                self.current_cursor = None
            if self.current_connection:
                self.current_connection.close()
                self.current_connection = None
            self.connection_start_time = None
            log.info("Database connection closed")
        except Exception as e:
            log.warning(f"Error closing connection: {e}")
    
    @contextmanager
    def get_cursor(self):
        """Context manager for database cursor with automatic reconnection."""
        with self.lock:
            try:
                # Check if connection is expired or doesn't exist
                if self._is_connection_expired() or not self.current_connection:
                    self._close_connection()
                    self._create_connection()
                
                yield self.current_cursor
                
            except Exception as e:
                # Handle deadlock and timeout errors
                if self._is_deadlock_error(e):
                    log.warning(f"Deadlock detected: {e}")
                    self._close_connection()
                    raise DeadlockError(f"Database deadlock: {e}")
                elif self._is_timeout_error(e):
                    log.warning(f"Connection timeout detected: {e}")
                    self._close_connection()
                    raise TimeoutError(f"Database timeout: {e}")
                else:
                    log.severe(f"Database error: {e}")
                    raise

class DeadlockError(Exception):
    """Custom exception for deadlock errors."""
    pass

class TimeoutError(Exception):
    """Custom exception for timeout errors."""
    pass

def get_table_sizes(configuration: dict, conn_manager, tables: List[str]) -> Dict[str, int]:
    """Get row counts for all tables efficiently."""
    table_sizes = {}
    
    # Use a single query to get all table sizes at once
    # NOTE: sys.tables uses the column 'name' (not TABLE_NAME). We alias it to
    # TABLE_NAME for downstream code expecting that key. We also SUM partition
    # row counts because large tables can be partitioned.
    size_query = """
    SELECT 
        t.name AS TABLE_NAME,
        SUM(p.rows) AS ROW_COUNT
    FROM sys.tables t
    INNER JOIN sys.indexes i ON t.object_id = i.object_id AND i.index_id < 2 -- 0 = heap, 1 = clustered
    INNER JOIN sys.partitions p ON i.object_id = p.object_id AND i.index_id = p.index_id
    WHERE t.name IN ({})
    GROUP BY t.name
    """.format(','.join([f"'{table}'" for table in tables]))
    
    try:
        with conn_manager.get_cursor() as cursor:
            cursor.execute(size_query)
            for row in cursor.fetchall():
                table_name = row['TABLE_NAME'] if isinstance(row, dict) else row[0]
                row_count = row['ROW_COUNT'] if isinstance(row, dict) else row[1]
                table_sizes[table_name] = int(row_count) if row_count else 0
    except Exception as e:
        log.warning(f"Failed to get table sizes efficiently: {e}. Falling back to individual queries.")
        # Fallback to individual queries
        schema_name = get_schema_name(configuration)
        for table in tables:
            try:
                qualified_table = qualify_table_name(table, schema_name)
                with conn_manager.get_cursor() as cursor:
                    cursor.execute(configuration["src_val_record_count"].format(tableName=qualified_table))
                    row = cursor.fetchone()
                    # Handle both tuple and dict results, with support for column alias
                    if isinstance(row, dict):
                        count = row.get('row_count') or row.get('ROW_COUNT') or list(row.values())[0]
                    else:
                        count = row[0]
                    table_sizes[table] = int(count) if count else 0
            except Exception as e2:
                log.warning(f"Failed to get size for table {table}: {e2}")
                table_sizes[table] = 0
    
    return table_sizes

def categorize_and_sort_tables(tables: List[str], table_sizes: Dict[str, int]) -> List[Tuple[str, str, int]]:
    """Categorize tables by size and sort for optimal processing order.
    
    Returns: List of tuples (table_name, category, row_count)
    Categories: 'small', 'medium', 'large'
    """
    categorized = []
    
    for table in tables:
        row_count = table_sizes.get(table, 0)
        
        if row_count < SMALL_TABLE_THRESHOLD:
            category = 'small'
        elif row_count < LARGE_TABLE_THRESHOLD:
            category = 'medium'
        else:
            category = 'large'
        
        categorized.append((table, category, row_count))
    
    # Sort by category (small first, then medium, then large) and by row count within each category
    categorized.sort(key=lambda x: ('small', 'medium', 'large').index(x[1]) * 1000000000 + x[2])
    
    return categorized

def flatten_dict(prefix: str, d: Any, result: Dict[str, Any]) -> None:
    """Flatten nested dictionary structures for database storage."""
    if isinstance(d, dict):
        if not d:
            result[prefix] = 'N/A'
        else:
            for k, v in d.items():
                new_key = f"{prefix}_{k}" if prefix else k
                flatten_dict(new_key, v, result)
    elif isinstance(d, list):
        result[prefix] = json.dumps(d) if d else 'N/A'
    else:
        result[prefix] = d if d is not None and d != "" else 'N/A'

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
    try:
        root_pem = requests.get(
            'https://cacerts.digicert.com/DigiCertGlobalRootG2.crt.pem',
            timeout=10
        ).text
    except Exception as e:
        log.warning(f"Failed to fetch root certificate from DigiCert: {e}")
        # Use a fallback root certificate or continue without it
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
        'errno=0',  # DNS errors often show errno=0
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
    
    # Get certificate port (falls back to regular port if not specified)
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
    if cert_port != port:
        log.info(f"Certificate will be fetched from: {cert_server}:{cert_port}")
    if 'privatelink' in server.lower():
        log.info("Private link connection detected")
    
    # Handle certificate configuration
    cafile_cfg = configuration.get("cdw_cert", None)
    cafile = None

    if cafile_cfg:
        if cafile_cfg.lstrip().startswith("-----BEGIN"):
            log.info("Using inline certificate from configuration")
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
                log.info(f"Loaded {len(pem_blocks)} certificate(s) from inline configuration")
            except ImportError:
                log.severe("OpenSSL Python bindings not available. Install pyopenssl package.")
                raise RuntimeError("OpenSSL Python bindings required for inline certificate support")
            except Exception as e:
                log.severe(f"Failed to process inline certificate: {e}")
                raise RuntimeError(f"Invalid certificate configuration: {e}")
        elif os.path.isfile(cafile_cfg):
            log.info(f"Using certificate file: {cafile_cfg}")
            cafile = cafile_cfg
        else:
            # Generate certificate chain
            if not cert_server:
                raise ValueError(f"Cannot generate cert chain: {cert_key} not provided and {server_key} cannot be used")
            log.info(f"Generating certificate chain for {cert_server}:{cert_port}")
            # For private links or when cert server differs from SQL server, allow fallback if certificate generation fails
            is_privatelink = 'privatelink' in (cert_server or server or '').lower()
            cert_server_different = cert_server and cert_server != server
            # Allow fallback for private links or when cert server is different (may not be accessible)
            allow_cert_fallback = is_privatelink or cert_server_different
            try:
                cafile = generate_cert_chain(cert_server, cert_port, allow_fallback=allow_cert_fallback)
                if cafile is None and allow_cert_fallback:
                    log.info("Certificate generation failed - will use root certificate or proceed without validation")
                    # Try to get root certificate as fallback
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
                    raise
            except Exception as e:
                if allow_cert_fallback:
                    log.warning(f"Certificate generation error: {e}. Using fallback approach.")
                    cafile = None
                else:
                    log.severe(f"Unexpected error during certificate generation: {e}")
                    raise RuntimeError(f"Certificate generation failed: {e}")
    else:
        # No inline cert config: generate chain from server and port
        if not cert_server:
            raise ValueError(f"Cannot generate cert chain: {cert_key} not provided")
        log.info(f"Generating certificate chain for {cert_server}:{cert_port}")
        # For private links or when cert server differs from SQL server, allow fallback if certificate generation fails
        is_privatelink = 'privatelink' in (cert_server or server or '').lower()
        cert_server_different = cert_server and cert_server != server
        # Allow fallback for private links or when cert server is different (may not be accessible)
        allow_cert_fallback = is_privatelink or cert_server_different
        try:
            cafile = generate_cert_chain(cert_server, cert_port, allow_fallback=allow_cert_fallback)
            if cafile is None and allow_cert_fallback:
                log.info("Certificate generation failed - will use root certificate or proceed without validation")
                # Try to get root certificate as fallback
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
                log.severe(f"  1. Verify {server_key} and {port_key} are correct")
                if cert_port != port:
                    log.severe(f"  2. Verify {cert_port_key} ({cert_port}) is correct and different from SQL port ({port})")
                log.severe(f"  3. Check network connectivity to {cert_server}:{cert_port}")
                log.severe(f"  4. Verify firewall rules allow connections to {cert_server}:{cert_port}")
                log.severe(f"  5. For private links, ensure the endpoint is accessible from this environment")
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
    connection_timeout = int(configuration.get("connection_timeout", 30))
    
    log.info(f"Attempting connection with timeout: {connection_timeout}s")
    
    try:
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
        
        # Handle certificate file
        if cafile and cafile != 'ignored':
            conn_params['cafile'] = cafile
            log.info(f"Using certificate file: {cafile}")
        elif cafile is None:
            # No certificate - connection will proceed without certificate validation
            log.warning("No certificate file available - connection will proceed without certificate validation")
            # pytds will handle this - we just don't pass cafile
        # else: cafile == 'ignored' means using custom SSL context (already configured)
        
        conn = pytds.connect(**conn_params)
        log.info("Successfully connected to MSSQL database")
        return conn
    except ConnectionRefusedError as e:
        log.severe(f"Connection refused to {server}:{port}")
        log.severe("Troubleshooting steps:")
        log.severe(f"  1. Verify {server_key} = '{server}' is correct")
        log.severe(f"  2. Verify {port_key} = '{port}' is correct")
        log.severe(f"  3. Check network connectivity: Can you reach {server}:{port}?")
        log.severe(f"  4. Verify firewall/security group rules allow connections")
        log.severe(f"  5. For private links, ensure DNS resolution works: nslookup {server}")
        log.severe(f"  6. Check if SQL Server is running and listening on port {port}")
        raise
    except TimeoutError as e:
        log.severe(f"Connection timeout to {server}:{port} after {connection_timeout}s")
        log.severe("Troubleshooting steps:")
        log.severe(f"  1. Check network latency to {server}")
        log.severe(f"  2. Verify firewall rules allow connections")
        log.severe(f"  3. Increase connection_timeout in configuration if needed")
        raise
    except Exception as e:
        error_str = str(e).lower()
        if 'connection refused' in error_str or 'errno 111' in error_str:
            log.severe(f"Connection refused to {server}:{port}")
            log.severe("Troubleshooting steps:")
            log.severe(f"  1. Verify server and port are correct")
            log.severe(f"  2. Check network connectivity")
            log.severe(f"  3. Verify firewall rules")
            raise ConnectionRefusedError(f"Cannot connect to {server}:{port}: {e}")
        elif 'timeout' in error_str:
            log.severe(f"Connection timeout to {server}:{port}")
            raise TimeoutError(f"Connection timeout to {server}:{port}: {e}")
        else:
            log.severe(f"Connection error: {e}")
            raise RuntimeError(f"Failed to connect to MSSQL: {e}")

def get_schema_name(configuration: dict) -> str:
    """Extract schema name from configuration or default to 'epic'."""
    # Try to get from explicit config parameter
    if "MSSQL_SCHEMA" in configuration:
        return configuration["MSSQL_SCHEMA"]
    
    # Try to extract from schema_table_list_query
    query = configuration.get("schema_table_list_query", "")
    if "TABLE_SCHEMA = '" in query:
        match = re.search(r"TABLE_SCHEMA\s*=\s*'([^']+)'", query)
        if match:
            return match.group(1)
    
    # Default to 'epic' if not found
    return "epic"

def qualify_table_name(table_name: str, schema_name: str) -> str:
    """Qualify a table name with schema using SQL Server bracket notation."""
    # If already qualified, return as-is
    if '.' in table_name and not table_name.startswith('['):
        parts = table_name.split('.', 1)
        return f"[{parts[0]}].[{parts[1]}]"
    elif '.' in table_name:
        return table_name  # Already qualified with brackets
    
    # Qualify with schema
    return f"[{schema_name}].[{table_name}]"

def validate_configuration(configuration: dict) -> None:
    """Validate the configuration dictionary to ensure it contains all required parameters."""
    required_configs = [
        "MSSQL_DATABASE", "MSSQL_USER", "MSSQL_PASSWORD",
        "schema_table_list_query", "schema_pk_query", "schema_col_query",
        "src_upsert_records", "src_del_records", "src_val_record_count",
        "src_gen_index_column_bounds", "src_fl_records"
    ]
    
    for key in required_configs:
        if key not in configuration:
            raise ValueError(f"Missing required configuration value: {key}")
    
    # Validate connection parameters
    is_local = platform.system() == "Darwin"
    server_key = "MSSQL_SERVER_DIR" if is_local else "MSSQL_SERVER"
    port_key = "MSSQL_PORT_DIR" if is_local else "MSSQL_PORT"
    
    server = configuration.get(server_key)
    port = configuration.get(port_key)
    
    if not server:
        raise ValueError(f"Missing required configuration: {server_key}")
    if not port:
        raise ValueError(f"Missing required configuration: {port_key}")
    
    try:
        port_int = int(port)
        if port_int < 1 or port_int > 65535:
            raise ValueError(f"Invalid port number: {port_int}. Port must be between 1 and 65535")
    except (ValueError, TypeError):
        raise ValueError(f"Invalid port value: {port}. Port must be a number")

def validate_connection_parameters(configuration: dict) -> Dict[str, Any]:
    """Validate and return connection parameters with helpful error messages.
    
    Returns:
        Dictionary with validated connection parameters
    """
    is_local = platform.system() == "Darwin"
    server_key = "MSSQL_SERVER_DIR" if is_local else "MSSQL_SERVER"
    cert_key = "MSSQL_CERT_SERVER_DIR" if is_local else "MSSQL_CERT_SERVER"
    port_key = "MSSQL_PORT_DIR" if is_local else "MSSQL_PORT"
    cert_port_key = "MSSQL_CERT_PORT_DIR" if is_local else "MSSQL_CERT_PORT"
    
    server = configuration.get(server_key)
    cert_server = configuration.get(cert_key) or server
    port_str = configuration.get(port_key)
    cert_port_str = configuration.get(cert_port_key)
    
    errors = []
    if not server:
        errors.append(f"Missing {server_key}")
    if not port_str:
        errors.append(f"Missing {port_key}")
    else:
        try:
            port = int(port_str)
            if port < 1 or port > 65535:
                errors.append(f"Invalid port: {port} (must be 1-65535)")
        except (ValueError, TypeError):
            errors.append(f"Invalid port value: {port_str} (must be a number)")
    
    # Validate cert_port if provided
    cert_port = None
    if cert_port_str:
        try:
            cert_port = int(cert_port_str)
            if cert_port < 1 or cert_port > 65535:
                errors.append(f"Invalid cert_port: {cert_port} (must be 1-65535)")
        except (ValueError, TypeError):
            errors.append(f"Invalid cert_port value: {cert_port_str} (must be a number)")
    
    if errors:
        error_msg = "Connection parameter validation failed:\n  " + "\n  ".join(errors)
        if 'privatelink' in (server or '').lower():
            error_msg += "\n\nFor private link connections, ensure:"
            error_msg += "\n  1. The private link endpoint is correctly configured"
            error_msg += "\n  2. DNS resolution works for the private link hostname"
            error_msg += "\n  3. Network connectivity is available to the private link endpoint"
        raise ValueError(error_msg)
    
    # Use cert_port if provided, otherwise fall back to port
    final_cert_port = cert_port if cert_port is not None else int(port_str)
    
    return {
        'server': server,
        'cert_server': cert_server,
        'port': int(port_str),
        'cert_port': final_cert_port,
        'database': configuration.get("MSSQL_DATABASE"),
        'user': configuration.get("MSSQL_USER"),
        'is_privatelink': 'privatelink' in server.lower() if server else False
    }

def schema(configuration: dict) -> List[Dict[str, Any]]:
    """Discover tables, columns, and primary keys."""
    validate_configuration(configuration)
    
    # Validate connection parameters early with helpful error messages
    try:
        conn_params = validate_connection_parameters(configuration)
        log.info(f"Connection parameters validated: server={conn_params['server']}, "
                f"port={conn_params['port']}, database={conn_params['database']}")
        if conn_params['is_privatelink']:
            log.info("Private link connection detected")
    except ValueError as e:
        log.severe(f"Configuration validation failed: {e}")
        raise
    
    raw_debug = configuration.get("debug", False)
    debug = (isinstance(raw_debug, str) and raw_debug.lower() == 'true')
    log.info(f"Debug mode: {debug}")
    
    try:
        conn_manager = ConnectionManager(configuration)
    except (ConnectionRefusedError, TimeoutError) as e:
        log.severe(f"Failed to establish connection during schema discovery: {e}")
        log.severe("Please verify your connection parameters and network connectivity")
        raise
    except Exception as e:
        log.severe(f"Unexpected error creating connection manager: {e}")
        raise
    
    try:
        with conn_manager.get_cursor() as cursor:
            query = configuration["schema_table_list_query_debug"] if debug else configuration["schema_table_list_query"]
            log.info(f"Executing schema discovery query: {query[:200]}...")
            cursor.execute(query)
            tables = [r['TABLE_NAME'] if isinstance(r, dict) else r[0]
                      for r in cursor.fetchall()]
            log.info(f"Found {len(tables)} table(s) to process")
    except (ConnectionRefusedError, TimeoutError) as e:
        log.severe(f"Connection error during schema discovery: {e}")
        log.severe("Troubleshooting steps:")
        log.severe("  1. Verify server address and port are correct")
        log.severe("  2. Check network connectivity")
        log.severe("  3. Verify firewall/security group rules")
        log.severe("  4. For private links, ensure DNS resolution works")
        raise
    except Exception as e:
        log.severe(f"Error executing schema discovery query: {e}")
        raise
    
    result = []
    for table in tables:
        try:
            with conn_manager.get_cursor() as cursor:
                cursor.execute(configuration["schema_pk_query"].format(table=table))
                pk_rows = cursor.fetchall()
                primary_keys = [r['COLUMN_NAME'] if isinstance(r, dict) else r[0]
                                for r in pk_rows]
                cursor.execute(configuration["schema_col_query"].format(table=table))
                col_rows = cursor.fetchall()
                columns = [r['COLUMN_NAME'] if isinstance(r, dict) else r[0]
                           for r in col_rows]
            
            obj = {'table': table}
            if primary_keys:
                obj['primary_key'] = primary_keys
            if columns:
                obj['column'] = columns
            result.append(obj)
            
        except Exception as e:
            log.warning(f"Error processing schema for table {table}: {e}")
            continue
    
    log.info(f"Schema discovery completed: {len(result)} table(s) processed")
    return result

def has_timestamp_column(table: str, column_name: str, configuration: dict, conn_manager: ConnectionManager) -> bool:
    """Check if a table has a specific timestamp column for incremental sync.
    
    Args:
        table: Table name to check
        column_name: Column name to check for (e.g., '_LastUpdatedInstant')
        configuration: Configuration dictionary
        conn_manager: Connection manager instance
        
    Returns:
        True if column exists, False otherwise
    """
    schema_name = get_schema_name(configuration)
    
    try:
        with conn_manager.get_cursor() as cursor:
            # Use unqualified table name for INFORMATION_SCHEMA query
            check_query = f"""
            SELECT COUNT(*) AS col_count
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = '{schema_name}' AND TABLE_NAME = '{table}' AND COLUMN_NAME = '{column_name}'
            """
            cursor.execute(check_query)
            row = cursor.fetchone()
            if isinstance(row, dict):
                count = row.get('col_count') or list(row.values())[0]
            else:
                count = row[0]
            return int(count) > 0
    except Exception as e:
        log.warning(f"Error checking for column {column_name} in table {table}: {e}")
        return False

def process_incremental_sync(table: str, configuration: dict, state: dict, 
                           conn_manager: ConnectionManager, pk_map_full: Dict[str, List[str]]) -> int:
    """Process incremental sync for a table using direct operation calls (no yield).
    
    Args:
        table: Table name to sync
        configuration: Configuration dictionary
        state: State dictionary for incremental sync
        conn_manager: Connection manager instance
        pk_map_full: Primary key mapping for all tables
        
    Returns:
        Number of records processed
    """
    start_date = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    records_processed = 0
    
    # Get schema name and qualify table name
    schema_name = get_schema_name(configuration)
    qualified_table = qualify_table_name(table, schema_name)
    
    # Check if table has the required timestamp column for incremental sync
    timestamp_column = configuration.get("incremental_timestamp_column", "_LastUpdatedInstant")
    if not has_timestamp_column(table, timestamp_column, configuration, conn_manager):
        log.warning(f"Table {table} does not have column '{timestamp_column}'. "
                   f"Skipping incremental sync - will perform full load on next run.")
        # Return 0 to indicate no records processed, and the table will be removed from state
        return 0
    
    # Get table size for adaptive parameters
    try:
        with conn_manager.get_cursor() as cursor:
            cursor.execute(configuration["src_val_record_count"].format(tableName=qualified_table))
            row = cursor.fetchone()
            # Handle both tuple and dict results, with support for column alias
            if isinstance(row, dict):
                total_rows = row.get('row_count') or row.get('ROW_COUNT') or list(row.values())[0]
            else:
                total_rows = row[0]
            
            # Use resource-aware adaptive parameters
            adaptive_params = get_adaptive_parameters_with_monitoring(total_rows, 0, 0)
            batch_size = adaptive_params['batch_size']
            checkpoint_interval = adaptive_params['checkpoint_interval']
            
            if adaptive_params['resource_pressure']:
                log.info(f"Resource Monitor: Processing incremental sync for {table} with resource-aware parameters")
            
    except Exception as e:
        log.warning(f"Could not determine table size for {table}, using default batch size: {e}")
        batch_size = BATCH_SIZE
        checkpoint_interval = CHECKPOINT_INTERVAL
    
    with conn_manager.get_cursor() as cursor:
        # Get the timestamp column name from configuration or use default
        timestamp_column = configuration.get("incremental_timestamp_column", "_LastUpdatedInstant")
        
        # Build query - replace timestamp column name in the query template
        query_template = configuration["src_upsert_records"]
        # Replace the hardcoded _LastUpdatedInstant with the configurable column name
        query = query_template.replace("_LastUpdatedInstant", timestamp_column).format(
            tableName=qualified_table, 
            endDate=state[table], 
            startDate=start_date
        )
        log.info(f"Incremental sync for {table}: {query}")
        cursor.execute(query)
        
        cols = [d[0] for d in getattr(cursor, 'description', [])]
        while True:
            rows = cursor.fetchmany(batch_size)  # Use adaptive batch size
            if not rows:
                break
            for row in rows:
                flat_data = {}
                flatten_dict("", {cols[i]: row[i] for i in range(len(cols))}, flat_data)
                # Direct operation call without yield - per SDK best practices
                op.upsert(table=table, data=flat_data)
                records_processed += 1
                
                # Checkpoint every adaptive interval
                if records_processed % checkpoint_interval == 0:
                    log.info(f"Checkpointing {table} after {records_processed} records")
                    op.checkpoint(state=state)
        
        # Process deletes
        pk_cols = pk_map_full.get(table, [])
        if pk_cols:
            timestamp_column = configuration.get("incremental_timestamp_column", "_LastUpdatedInstant")
            del_query_template = configuration["src_del_records"]
            # Replace the hardcoded _LastUpdatedInstant with the configurable column name
            del_query = del_query_template.replace("_LastUpdatedInstant", timestamp_column).format(
                tableName=qualified_table, 
                endDate=state[table], 
                startDate=start_date,
                joincol=", ".join(pk_cols)
            )
            log.info(f"Delete sync for {table}: {del_query}")
            cursor.execute(del_query)
            while True:
                drows = cursor.fetchmany(batch_size)  # Use adaptive batch size
                if not drows:
                    break
                for drow in drows:
                    keys = {c: drow[c] for c in pk_cols} if isinstance(drow, dict) \
                        else {pk_cols[i]: drow[i] for i in range(len(pk_cols))}
                    # Direct operation call without yield - per SDK best practices
                    op.delete(table=table, keys=keys)
                    records_processed += 1
    
    return records_processed

def process_full_load(table: str, configuration: dict, conn_manager: ConnectionManager, 
                     pk_map: Dict[str, str], threads: int, max_queue_size: int, state: dict) -> int:
    """Process full load for a table using partitioned approach with direct operation calls (no yield).
    
    Args:
        table: Table name to load
        configuration: Configuration dictionary
        conn_manager: Connection manager instance
        pk_map: Primary key mapping
        threads: Number of threads to use (0 for adaptive)
        max_queue_size: Maximum queue size (0 for adaptive)
        state: State dictionary for checkpointing
        
    Returns:
        Number of records processed
    """
    records_processed = 0
    idx = pk_map.get(table)
    
    # Get schema name and qualify table name
    schema_name = get_schema_name(configuration)
    qualified_table = qualify_table_name(table, schema_name)
    
    # Get total record count
    count_q = configuration["src_val_record_count"].format(tableName=qualified_table)
    with conn_manager.get_cursor() as cursor:
        cursor.execute(count_q)
        row = cursor.fetchone()
        # Handle both tuple and dict results, with support for column alias
        if isinstance(row, dict):
            total_rows = row.get('row_count') or row.get('ROW_COUNT') or list(row.values())[0]
        else:
            total_rows = row[0]
        
        # Use adaptive parameters with resource monitoring
        adaptive_params = get_adaptive_parameters_with_monitoring(total_rows, threads, max_queue_size)
        
        partition_size = adaptive_params['partition_size']
        batch_size = adaptive_params['batch_size']
        adaptive_threads = adaptive_params['threads']
        adaptive_queue_size = adaptive_params['queue_size']
        checkpoint_interval = adaptive_params['checkpoint_interval']
        resource_pressure = adaptive_params['resource_pressure']
        
        # Override with configuration if provided, otherwise use adaptive values
        actual_threads = threads if threads > 0 else adaptive_threads
        actual_queue_size = max_queue_size if max_queue_size > 0 else adaptive_queue_size
        
        # Ensure actual_threads never exceeds 4
        if actual_threads > 4:
            log.warning(f"Table {table}: Adaptive threads ({actual_threads}) exceeds maximum allowed (4). Capping at 4 threads.")
            actual_threads = 4
        
        num_partitions = math.ceil(total_rows / partition_size)
        
        # Log table processing parameters with resource context
        log.info(f"Table {table}: {total_rows:,} rows, {num_partitions} partitions, "
                f"{actual_threads} threads, {actual_queue_size} queue size, "
                f"{partition_size:,} partition size, {batch_size:,} batch size, "
                f"{checkpoint_interval:,} checkpoint interval")
        
        if resource_pressure:
            log.info(f"Resource Monitor: Processing {table} with resource-aware parameters due to system pressure")
        if adaptive_params['resource_status']['status'] == 'active':
            log.info(f"Resource Monitor: Memory {adaptive_params['resource_status']['memory_usage']:.1f}%, "
                    f"CPU {adaptive_params['resource_status']['cpu_percent']:.1f}%")
    
    # Derive an index column if none was discovered (fallback to first column)
    if not idx:
        try:
            with conn_manager.get_cursor() as cursor:
                # Use unqualified table name for INFORMATION_SCHEMA queries
                cursor.execute(configuration["src_gen_index_column"].format(table=table))
                r = cursor.fetchone()
                if r:
                    if isinstance(r, dict):
                        idx = r.get('COLUMN_NAME') or r.get('column_name') or list(r.values())[0]
                    else:
                        idx = r[0]
                    pk_map[table] = idx  # cache for reuse
                    log.info(f"No primary key found for {table}; using first column '{idx}' as index key")
                else:
                    raise ValueError("No columns returned to derive index key")
        except Exception as e:
            log.severe(f"Failed to derive index column for {table}: {e}")
            raise
    if not idx:
        raise ValueError(f"Index key could not be determined for table {table}")

    # Generate partition bounds with validated index key
    bounds_q = configuration["src_gen_index_column_bounds"].format(
        tableName=qualified_table, indexkey=idx, threads=num_partitions
    )
    with conn_manager.get_cursor() as cursor:
        cursor.execute(bounds_q)
        parts = cursor.fetchall()

    def load_partition(partition_data, q, sentinel):
        """Load a partition into the shared queue."""
        for attempt in range(2):
            try:
                # Create a new connection manager for this partition with table size context
                partition_conn_manager = ConnectionManager(configuration, total_rows)
                with partition_conn_manager.get_cursor() as pc:
                    fl_q = configuration["src_fl_records"].format(
                        tableName=qualified_table,
                        indexkey=idx,
                        lowerbound=partition_data.get('lowerbound', partition_data[1]) if isinstance(partition_data, dict) else partition_data[1],
                        upperbound=partition_data.get('upperbound', partition_data[2]) if isinstance(partition_data, dict) else partition_data[2]
                    )
                    log.info(f"Partition query: {fl_q}")
                    pc.execute(fl_q)
                    cols = [d[0] for d in getattr(pc, 'description', [])]
                    while True:
                        rows = pc.fetchmany(batch_size)  # Use adaptive batch size
                        if not rows:
                            break
                        for row in rows:
                            rec = {cols[i]: row[i] for i in range(len(row))}
                            q.put(rec)
                break
            except Exception as e:
                log.warning(f"Partition load attempt {attempt+1} failed: {e}. Retrying connection")
                if attempt == 1:
                    raise
        q.put(sentinel)

    sentinel = object()
    q = queue.Queue(maxsize=actual_queue_size)  # Use adaptive queue size
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=actual_threads) as executor:  # Use adaptive threads
        for p in parts:
            executor.submit(load_partition, p, q, sentinel)
        
        finished = 0
        total = len(parts)
        while finished < total:
            item = q.get()
            if item is sentinel:
                finished += 1
            else:
                flat_data = {}
                flatten_dict("", item, flat_data)
                # Direct operation call without yield - per SDK best practices
                op.upsert(table=table, data=flat_data)
                records_processed += 1
                
                # Checkpoint every adaptive interval
                if records_processed % checkpoint_interval == 0:
                    log.info(f"Checkpointing {table} after {records_processed} records")
                    op.checkpoint(state=state)
    
    return records_processed

def update(configuration: dict, state: dict):
    """Main update function with enhanced error handling and state management."""
    validate_configuration(configuration)
    
    # Configuration parameters
    threads = int(configuration.get("threads", 0))  # 0 means use adaptive
    max_queue_size = int(configuration.get("max_queue_size", 0))  # 0 means use adaptive
    max_retries = int(configuration.get("max_retries", MAX_RETRIES))
    sleep_seconds = float(configuration.get("retry_sleep_seconds", BASE_RETRY_DELAY))
    
    # Ensure threads never exceed 4
    if threads > 4:
        log.warning(f"Requested threads ({threads}) exceeds maximum allowed (4). Capping at 4 threads.")
        threads = 4
    
    # Debug mode
    raw_debug = configuration.get("debug", False)
    debug = (isinstance(raw_debug, str) and raw_debug.lower() == 'true')
    
    # Get schema and table information
    schema_list = schema(configuration)
    pk_map_full = {s['table']: s.get('primary_key', []) for s in schema_list}
    pk_map = {tbl: cols[0] for tbl, cols in pk_map_full.items() if cols}
    
    # Get table list
    # Create initial connection manager for schema discovery
    initial_conn_manager = ConnectionManager(configuration)
    with initial_conn_manager.get_cursor() as cursor:
        query = configuration["schema_table_list_query_debug"] if debug else configuration["schema_table_list_query"]
        cursor.execute(query)
        tables = [r['TABLE_NAME'] if isinstance(r, dict) else r[0]
                  for r in cursor.fetchall()]
    
    if debug:
        log.info(f"Tables found: {tables}")
    
    # Get table sizes and categorize them
    log.info("Analyzing table sizes for optimal processing order...")
    table_sizes = get_table_sizes(configuration, initial_conn_manager, tables)
    categorized_tables = categorize_and_sort_tables(tables, table_sizes)
    
    # Log processing strategy
    small_count = sum(1 for _, cat, _ in categorized_tables if cat == 'small')
    medium_count = sum(1 for _, cat, _ in categorized_tables if cat == 'medium')
    large_count = sum(1 for _, cat, _ in categorized_tables if cat == 'large')
    
    log.info(f"Processing strategy: {small_count} small tables (<1M), "
            f"{medium_count} medium tables (1M-50M), {large_count} large tables (50M+)")
    
    # Display detailed processing plan
    display_processing_plan(categorized_tables)
    
    # Initialize resource monitoring
    if PSUTIL_AVAILABLE:
        log.info("Resource Monitor: System monitoring enabled - will automatically adjust parameters based on resource pressure")
        initial_status = monitor_resources()
        log.info(f"Resource Monitor: Initial system status - Memory {initial_status.get('memory_usage', 'N/A')}%, "
                f"CPU {initial_status.get('cpu_percent', 'N/A')}%, Disk {initial_status.get('disk_usage', 'N/A')}%")
    else:
        log.info("Resource Monitor: System monitoring disabled - psutil not available")
    
    # Initialize state for tables
    if "last_updated_at" in state:
        ts = state.pop("last_updated_at")
        for t in tables:
            state.setdefault(t, ts)
    
    # Process tables by category (small first, then medium, then large)
    total_tables = len(categorized_tables)
    processed_tables = 0
    
    for table, category, row_count in categorized_tables:
        processed_tables += 1
        start_date = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
        records_processed = 0
        
        log.info(f"Processing table {processed_tables}/{total_tables}: {table} "
                f"({category}, {row_count:,} rows)")
        
        # Create connection manager with table size context for adaptive timeouts
        conn_manager = ConnectionManager(configuration, row_count)
        
        # Retry loop for deadlock and timeout handling
        for attempt in range(max_retries):
            try:
                if debug:
                    log.info(f"Processing table: {table}, start_date: {start_date}, "
                            f"state: {state}, attempt {attempt+1}/{max_retries}")
                
                # Track tables that don't support incremental sync
                tables_without_timestamp = state.get("_tables_without_timestamp", [])
                if isinstance(tables_without_timestamp, str):
                    # Handle case where it's stored as a string (from JSON)
                    import ast
                    try:
                        tables_without_timestamp = ast.literal_eval(tables_without_timestamp)
                    except:
                        tables_without_timestamp = []
                if not isinstance(tables_without_timestamp, list):
                    tables_without_timestamp = list(tables_without_timestamp) if tables_without_timestamp else []
                
                if table in state and table not in tables_without_timestamp:
                    # Incremental sync - direct call (no yield per SDK best practices)
                    try:
                        records_processed = process_incremental_sync(table, configuration, state, conn_manager, pk_map_full)
                        # If incremental sync returned 0, it means the table doesn't have the required column
                        # Mark it and perform full load
                        if records_processed == 0:
                            log.info(f"Table {table} does not support incremental sync - marking and performing full load")
                            if table not in tables_without_timestamp:
                                tables_without_timestamp.append(table)
                            state["_tables_without_timestamp"] = tables_without_timestamp
                            state.pop(table, None)  # Remove timestamp state
                            # Perform full load now
                            records_processed = process_full_load(table, configuration, conn_manager, pk_map, threads, max_queue_size, state)
                    except Exception as inc_error:
                        # If incremental sync fails due to missing column, fall back to full load
                        error_str = str(inc_error).lower()
                        if "invalid column name" in error_str and ("_lastupdatedinstant" in error_str or "timestamp" in error_str):
                            log.warning(f"Table {table} does not support incremental sync (missing timestamp column). "
                                       f"Marking and falling back to full load. Error: {inc_error}")
                            if table not in tables_without_timestamp:
                                tables_without_timestamp.append(table)
                            state["_tables_without_timestamp"] = tables_without_timestamp
                            state.pop(table, None)  # Remove from state
                            records_processed = process_full_load(table, configuration, conn_manager, pk_map, threads, max_queue_size, state)
                        else:
                            raise  # Re-raise if it's a different error
                else:
                    # Full load - direct call (no yield per SDK best practices)
                    records_processed = process_full_load(table, configuration, conn_manager, pk_map, threads, max_queue_size, state)
                
                # Successful completion, exit retry loop
                log.info(f"Successfully processed table {table}: {records_processed} records")
                break
                
            except (DeadlockError, TimeoutError) as e:
                log.warning(f"{type(e).__name__} during sync for {table}, attempt {attempt+1}/{max_retries}: {e}")
                if attempt + 1 >= max_retries:
                    log.severe(f"Max retries exceeded for table {table}: {e}")
                    raise
                
                # Exponential backoff with jitter
                base_backoff = min(sleep_seconds * (2 ** attempt), MAX_RETRY_DELAY)
                delay = random.uniform(base_backoff * 0.5, base_backoff * 1.5)
                log.info(f"Retrying {table} after backoff {delay:.2f}s (attempt {attempt+1}/{max_retries})")
                time.sleep(delay)
                continue
                
            except Exception as e:
                log.severe(f"Unexpected error processing table {table}: {e}")
                raise
        
        # Update state and checkpoint after table completion
        # Only add timestamp to state if table supports incremental sync
        tables_without_timestamp = state.get("_tables_without_timestamp", [])
        if isinstance(tables_without_timestamp, str):
            import ast
            try:
                tables_without_timestamp = ast.literal_eval(tables_without_timestamp)
            except:
                tables_without_timestamp = []
        
        if table not in tables_without_timestamp:
            state[table] = start_date
        # Direct operation call without yield - per SDK best practices
        op.checkpoint(state=state)
        
        # Record count validation
        try:
            schema_name = get_schema_name(configuration)
            qualified_table = qualify_table_name(table, schema_name)
            with conn_manager.get_cursor() as cursor:
                cursor.execute(configuration["src_val_record_count"].format(tableName=qualified_table))
                row = cursor.fetchone()
                # Handle both tuple and dict results, with support for column alias
                if isinstance(row, dict):
                    count = row.get('row_count') or row.get('ROW_COUNT') or list(row.values())[0]
                else:
                    count = row[0]
            
            # Direct operation call without yield - per SDK best practices
            op.upsert(table="CDK_VALIDATION", data={
                "datetime": datetime.utcnow().isoformat() + "Z",
                "tablename": table,
                "count": count,
                "records_processed": records_processed,
                "category": category,
                "processing_order": processed_tables
            })
            
        except Exception as e:
            log.warning(f"Failed to record validation for table {table}: {e}")
        
        # Progress update
        log.info(f"Completed {processed_tables}/{total_tables} tables. "
                f"Next: {categorized_tables[processed_tables][0] if processed_tables < total_tables else 'None'}")
        
        # Periodic resource monitoring check
        if PSUTIL_AVAILABLE and processed_tables % 5 == 0:  # Check every 5 tables
            log.info("Resource Monitor: Periodic system check...")
            current_status = monitor_resources()
            if current_status.get('status') == 'active':
                log.info(f"Resource Monitor: Current status - Memory {current_status['memory_usage']:.1f}%, "
                        f"CPU {current_status['cpu_percent']:.1f}%, Disk {current_status['disk_usage']:.1f}%")

def display_processing_plan(categorized_tables: List[Tuple[str, str, int]]) -> None:
    """Display a detailed processing plan for the sync operation."""
    log.info("=" * 80)
    log.info("SYNC PROCESSING PLAN")
    log.info("=" * 80)
    
    # Group tables by category
    small_tables = [(t, c, r) for t, c, r in categorized_tables if c == 'small']
    medium_tables = [(t, c, r) for t, c, r in categorized_tables if c == 'medium']
    large_tables = [(t, c, r) for t, c, r in categorized_tables if c == 'large']
    
    # Display small tables (quick wins)
    if small_tables:
        log.info(f"\nSMALL TABLES ({len(small_tables)} tables, <1M rows each):")
        log.info("-" * 50)
        for i, (table, _, rows) in enumerate(small_tables[:10]):  # Show first 10
            log.info(f"  {i+1:2d}. {table:<40} {rows:>10,} rows")
        if len(small_tables) > 10:
            log.info(f"  ... and {len(small_tables) - 10} more small tables")
    
    # Display medium tables
    if medium_tables:
        log.info(f"\nMEDIUM TABLES ({len(medium_tables)} tables, 1M-50M rows each):")
        log.info("-" * 50)
        for i, (table, _, rows) in enumerate(medium_tables[:10]):  # Show first 10
            log.info(f"  {i+1:2d}. {table:<40} {rows:>10,} rows")
        if len(medium_tables) > 10:
            log.info(f"  ... and {len(medium_tables) - 10} more medium tables")
    
    # Display large tables (challenging ones)
    if large_tables:
        log.info(f"\nLARGE TABLES ({len(large_tables)} tables, 50M+ rows each):")
        log.info("-" * 50)
        for i, (table, _, rows) in enumerate(large_tables):
            log.info(f"  {i+1:2d}. {table:<40} {rows:>10,} rows")
    
    # Summary statistics
    total_rows = sum(rows for _, _, rows in categorized_tables)
    small_rows = sum(rows for _, _, rows in small_tables)
    medium_rows = sum(rows for _, _, rows in medium_tables)
    large_rows = sum(rows for _, _, rows in large_tables)
    
    log.info("\n" + "=" * 80)
    log.info("SUMMARY STATISTICS")
    log.info("=" * 80)
    log.info(f"Total tables: {len(categorized_tables)}")
    log.info(f"Total rows: {total_rows:,}")
    
    # Calculate percentages only if total_rows > 0 to avoid division by zero
    if total_rows > 0:
        small_pct = (small_rows/total_rows*100)
        medium_pct = (medium_rows/total_rows*100)
        large_pct = (large_rows/total_rows*100)
        log.info(f"Small tables: {len(small_tables)} tables, {small_rows:,} rows ({small_pct:.1f}%)")
        log.info(f"Medium tables: {len(medium_tables)} tables, {medium_rows:,} rows ({medium_pct:.1f}%)")
        log.info(f"Large tables: {len(large_tables)} tables, {large_rows:,} rows ({large_pct:.1f}%)")
    else:
        log.info(f"Small tables: {len(small_tables)} tables, {small_rows:,} rows (N/A%)")
        log.info(f"Medium tables: {len(medium_tables)} tables, {medium_rows:,} rows (N/A%)")
        log.info(f"Large tables: {len(large_tables)} tables, {large_rows:,} rows (N/A%)")
    
    # # Estimated processing time (rough estimates)
    # if small_tables:
    #     log.info(f"\nEstimated processing time:")
    #     log.info(f"  Small tables: ~{len(small_tables) * 2} minutes")
    # if medium_tables:
    #     log.info(f"  Medium tables: ~{len(medium_tables) * 15} minutes")
    # if large_tables:
    #     log.info(f"  Large tables: ~{sum(rows/1000000 for _, _, rows in large_tables)} hours")
    
    # log.info("=" * 80)
    # log.info("Starting sync process...")
    # log.info("=" * 80)

# Initialize the connector with the defined update and schema functions
connector = Connector(update=update, schema=schema)

if __name__ == "__main__":
    current_dir = os.path.dirname(os.path.abspath(__file__))
    config_path = os.path.join(current_dir, "configuration.json")
    with open(config_path, "r") as f:
        cfg = json.load(f)
    connector.debug(configuration=cfg)
