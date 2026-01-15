
import os
import json
import re
import requests
import concurrent.futures
from datetime import datetime, timedelta
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
import signal
import sys
import pytds

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
            conn = connect_to_mssql(self.configuration)
            self.current_connection = conn
            self.current_cursor = conn.cursor()
            self.connection_start_time = datetime.utcnow()
            log.info(f"New database connection established at {self.connection_start_time}")
            return conn, self.current_cursor
        except Exception as e:
            log.severe(f"Failed to create database connection: {e}")
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
        for table in tables:
            try:
                with conn_manager.get_cursor() as cursor:
                    cursor.execute(configuration["src_val_record_count"].format(tableName=table))
                    row = cursor.fetchone()
                    count = row[0] if not isinstance(row, dict) else list(row.values())[0]
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

def obfuscate_sensitive(value: str, show_chars: int = 4) -> str:
    """Obfuscate sensitive strings for logging purposes."""
    if not value or len(value) <= show_chars:
        return "***" if value else "N/A"
    return f"{value[:show_chars]}...{value[-show_chars:]}" if len(value) > show_chars * 2 else "***"

def obfuscate_cert(cert_content: str) -> str:
    """Obfuscate certificate content for logging."""
    if not cert_content:
        return "N/A"
    # Extract first and last few characters of each certificate block
    cert_blocks = re.findall(
        r'-----BEGIN CERTIFICATE-----.+?-----END CERTIFICATE-----',
        cert_content,
        re.DOTALL
    )
    if not cert_blocks:
        return "*** (invalid format) ***"
    
    obfuscated = []
    for i, block in enumerate(cert_blocks):
        lines = block.split('\n')
        if len(lines) > 2:
            first_line = lines[1][:8] if len(lines[1]) > 8 else lines[1]
            last_line = lines[-2][-8:] if len(lines[-2]) > 8 else lines[-2]
            obfuscated.append(f"Cert {i+1}: {first_line}...{last_line} ({len(block)} chars)")
        else:
            obfuscated.append(f"Cert {i+1}: *** ({len(block)} chars)")
    
    return f"{len(cert_blocks)} certificate(s): " + ", ".join(obfuscated)

def generate_cert_chain(server: str, port: int) -> str:
    """Generates a certificate chain file by fetching intermediate and root certificates.
    
    This function connects to the certificate server (typically on port 1434) to retrieve
    the SSL certificate chain needed for authenticating to the data server.
    """
    log.info("=" * 80)
    log.info("CERTIFICATE GENERATION PROCESS - Step 1: Connect to Certificate Server")
    log.info("=" * 80)
    log.info(f"Certificate Server: {server}")
    log.info(f"Certificate Server Port: {port} (expected: 1434 for privatelink)")
    
    try:
        log.info(f"Step 1.1: Executing OpenSSL s_client to retrieve certificates from {server}:{port}")
        proc = subprocess.run(
            ['openssl', 's_client', '-showcerts', '-connect', f'{server}:{port}'],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            timeout=30
        )
        
        if proc.returncode != 0:
            log.severe(f"OpenSSL connection failed with return code {proc.returncode}")
            log.severe(f"OpenSSL stderr: {proc.stderr[:500]}")  # Limit error output
            raise RuntimeError(f"Failed to connect to certificate server {server}:{port}: {proc.stderr}")
        
        log.info(f"Step 1.2: OpenSSL connection successful (return code: {proc.returncode})")
        log.info(f"Step 1.3: Parsing certificate blocks from OpenSSL output")
        
        pem_blocks = re.findall(
            r'-----BEGIN CERTIFICATE-----.+?-----END CERTIFICATE-----',
            proc.stdout,
            re.DOTALL
        )
        
        if not pem_blocks:
            log.severe("No certificate blocks found in OpenSSL output")
            raise RuntimeError("No certificates retrieved from certificate server")
        
        log.info(f"Step 1.4: Found {len(pem_blocks)} certificate block(s) from server")
        
        # Log obfuscated certificate info
        for i, block in enumerate(pem_blocks):
            block_preview = obfuscate_cert(block)
            log.info(f"  Certificate {i+1}: {block_preview}")
        
        intermediate = pem_blocks[1] if len(pem_blocks) > 1 else pem_blocks[0]
        log.info(f"Step 1.5: Selected {'intermediate' if len(pem_blocks) > 1 else 'server'} certificate")
        
        log.info("Step 1.6: Fetching DigiCert root certificate from public CA store")
        root_pem = requests.get(
            'https://cacerts.digicert.com/DigiCertGlobalRootG2.crt.pem',
            timeout=10
        ).text
        
        if not root_pem or 'BEGIN CERTIFICATE' not in root_pem:
            log.severe("Failed to retrieve root certificate from DigiCert")
            raise RuntimeError("Root certificate retrieval failed")
        
        log.info(f"Step 1.7: Root certificate retrieved ({len(root_pem)} chars)")
        log.info(f"  Root cert preview: {obfuscate_cert(root_pem)}")
        
        log.info("Step 1.8: Creating temporary certificate chain file")
        tmp = tempfile.NamedTemporaryFile(delete=False, suffix='.pem')
        cert_chain = intermediate + '\n' + root_pem
        tmp.write(cert_chain.encode('utf-8'))
        tmp.flush()
        tmp.close()
        
        log.info(f"Step 1.9: Certificate chain file created: {tmp.name}")
        log.info(f"  Chain file size: {len(cert_chain)} bytes")
        log.info(f"  Chain preview: {obfuscate_cert(cert_chain)}")
        log.info("=" * 80)
        log.info("CERTIFICATE GENERATION COMPLETE")
        log.info("=" * 80)
        
        return tmp.name
        
    except subprocess.TimeoutExpired:
        log.severe(f"OpenSSL connection to {server}:{port} timed out after 30 seconds")
        raise RuntimeError(f"Certificate server connection timeout: {server}:{port}")
    except requests.RequestException as e:
        log.severe(f"Failed to retrieve root certificate: {e}")
        raise RuntimeError(f"Root certificate retrieval failed: {e}")
    except Exception as e:
        log.severe(f"Certificate generation failed: {e}")
        raise


def connect_to_mssql(configuration: dict):
    """Connects to MSSQL using TDS with SSL cert chain.
    
    Authentication Flow for Privatelink:
    1. Connect to MSSQL_CERT_SERVER (port 1434) to retrieve/generate certificate
    2. Use certificate with cdw_cert configuration for authentication
    3. Connect to MSSQL_SERVER (port 1433) for data replication
    """
    log.info("=" * 80)
    log.info("MSSQL AUTHENTICATION PROCESS - Initialization")
    log.info("=" * 80)
    
    # Determine environment and configuration keys
    is_local = platform.system() == "Darwin"
    server_key = "MSSQL_SERVER_DIR" if is_local else "MSSQL_SERVER"
    cert_key = "MSSQL_CERT_SERVER_DIR" if is_local else "MSSQL_CERT_SERVER"
    port_key = "MSSQL_PORT_DIR" if is_local else "MSSQL_PORT"
    cert_port_key = "MSSQL_CERT_PORT_DIR" if is_local else "MSSQL_CERT_PORT"
    
    log.info(f"Step 0.1: Environment detection - Platform: {platform.system()}, Local: {is_local}")
    log.info(f"Step 0.2: Configuration keys - Server: {server_key}, Cert Server: {cert_key}")
    
    # Extract configuration values
    server = configuration.get(server_key)
    cert_server = configuration.get(cert_key)
    data_port = configuration.get(port_key, "1433")  # Default to 1433 for data server
    cert_port = configuration.get(cert_port_key, "1434")  # Default to 1434 for cert server
    cafile_cfg = configuration.get("cdw_cert", None)
    database = configuration.get("MSSQL_DATABASE")
    user = configuration.get("MSSQL_USER")
    password = configuration.get("MSSQL_PASSWORD")
    
    # Validate required configuration
    log.info("Step 0.3: Validating configuration parameters")
    if not server:
        log.severe(f"Missing required configuration: {server_key}")
        raise ValueError(f"Missing required configuration: {server_key}")
    if not cert_server:
        log.severe(f"Missing required configuration: {cert_key}")
        raise ValueError(f"Missing required configuration: {cert_key}")
    if not database:
        log.severe("Missing required configuration: MSSQL_DATABASE")
        raise ValueError("Missing required configuration: MSSQL_DATABASE")
    if not user:
        log.severe("Missing required configuration: MSSQL_USER")
        raise ValueError("Missing required configuration: MSSQL_USER")
    if not password:
        log.severe("Missing required configuration: MSSQL_PASSWORD")
        raise ValueError("Missing required configuration: MSSQL_PASSWORD")
    
    # Log configuration (obfuscated)
    log.info(f"Step 0.4: Configuration summary (sensitive data obfuscated):")
    log.info(f"  Data Server (MSSQL_SERVER): {server}")
    log.info(f"  Data Server Port: {data_port} (expected: 1433 for privatelink)")
    log.info(f"  Certificate Server (MSSQL_CERT_SERVER): {cert_server}")
    log.info(f"  Certificate Server Port: {cert_port} (expected: 1434 for privatelink)")
    log.info(f"  Database: {database}")
    log.info(f"  User: {obfuscate_sensitive(user)}")
    log.info(f"  CDW Cert provided: {'Yes' if cafile_cfg else 'No'}")
    if cafile_cfg:
        log.info(f"  CDW Cert type: {'PEM string' if cafile_cfg.lstrip().startswith('-----BEGIN') else 'File path' if os.path.isfile(cafile_cfg) else 'Invalid'}")
    
    log.info("=" * 80)
    log.info("MSSQL AUTHENTICATION PROCESS - Certificate Handling")
    log.info("=" * 80)
    
    # Handle certificate configuration
    if cafile_cfg:
        log.info("Step 1.1: CDW certificate configuration found, processing...")
        
        if cafile_cfg.lstrip().startswith("-----BEGIN"):
            log.info("Step 1.2: Certificate provided as PEM string, loading into SSL context")
            log.info(f"  Certificate preview: {obfuscate_cert(cafile_cfg)}")
            
            try:
                import OpenSSL.SSL as SSL, OpenSSL.crypto as crypto, pytds.tls
                
                log.info("Step 1.3: Creating SSL context with TLS method")
                ctx = SSL.Context(SSL.TLS_METHOD)
                
                log.info("Step 1.4: Setting SSL verification mode (VERIFY_PEER)")
                ctx.set_verify(SSL.VERIFY_PEER, lambda conn, cert, errnum, depth, ok: bool(ok))
                
                log.info("Step 1.5: Parsing PEM certificate blocks from configuration")
                pem_blocks = re.findall(
                    r'-----BEGIN CERTIFICATE-----.+?-----END CERTIFICATE-----',
                    cafile_cfg, re.DOTALL
                )
                
                if not pem_blocks:
                    log.severe("No valid certificate blocks found in cdw_cert configuration")
                    raise ValueError("Invalid certificate format in cdw_cert")
                
                log.info(f"Step 1.6: Found {len(pem_blocks)} certificate block(s) in configuration")
                for i, pem in enumerate(pem_blocks):
                    log.info(f"  Certificate {i+1}: {obfuscate_cert(pem)}")
                
                log.info("Step 1.7: Retrieving certificate store from SSL context")
                store = ctx.get_cert_store()
                if store is None:
                    log.severe("Failed to retrieve certificate store from SSL context")
                    raise RuntimeError("Failed to retrieve certificate store from SSL context")
                
                log.info("Step 1.8: Adding certificates to SSL context store")
                for i, pem in enumerate(pem_blocks):
                    try:
                        certificate = crypto.load_certificate(crypto.FILETYPE_PEM, pem)
                        store.add_cert(certificate)
                        log.info(f"  Added certificate {i+1} to store successfully")
                    except Exception as e:
                        log.severe(f"Failed to add certificate {i+1} to store: {e}")
                        raise
                
                log.info("Step 1.9: Configuring pytds TLS context factory")
                pytds.tls.create_context = lambda cafile: ctx
                cafile = None  # Set to None since SSL context is configured via pytds.tls.create_context
                
                log.info("Step 1.10: SSL context configured successfully (using PEM string)")
                
            except ImportError as e:
                log.severe(f"Required OpenSSL libraries not available: {e}")
                raise RuntimeError(f"OpenSSL libraries required for PEM certificate: {e}")
            except Exception as e:
                log.severe(f"Failed to process PEM certificate: {e}")
                raise
                
        elif os.path.isfile(cafile_cfg):
            log.info(f"Step 1.2: Certificate provided as file path: {cafile_cfg}")
            log.info(f"  File exists: {os.path.exists(cafile_cfg)}")
            if os.path.exists(cafile_cfg):
                file_size = os.path.getsize(cafile_cfg)
                log.info(f"  File size: {file_size} bytes")
                # Read and obfuscate cert content for logging
                try:
                    with open(cafile_cfg, 'r') as f:
                        cert_content = f.read()
                        log.info(f"  Certificate preview: {obfuscate_cert(cert_content)}")
                except Exception as e:
                    log.warning(f"  Could not read certificate file for preview: {e}")
            cafile = cafile_cfg
            log.info("Step 1.3: Using certificate file path for connection")
        else:
            log.warning(f"Step 1.2: CDW cert provided but not a valid PEM string or file path")
            log.info(f"  Attempting to generate certificate chain from certificate server")
            if not cert_server or not cert_port:
                log.severe("Cannot generate cert chain: certificate server or port missing")
                raise ValueError("Cannot generate cert chain: certificate server or port missing")
            log.info(f"  Certificate Server: {cert_server}, Port: {cert_port}")
            cafile = generate_cert_chain(cert_server, int(cert_port))
    else:
        log.info("Step 1.1: No CDW certificate configuration found")
        log.info("Step 1.2: Generating certificate chain from certificate server")
        if not cert_server or not cert_port:
            log.severe("Cannot generate cert chain: certificate server or port missing")
            raise ValueError("Cannot generate cert chain: certificate server or port missing")
        log.info(f"  Certificate Server: {cert_server}, Port: {cert_port}")
        cafile = generate_cert_chain(cert_server, int(cert_port))
    
    log.info("=" * 80)
    log.info("MSSQL AUTHENTICATION PROCESS - Database Connection")
    log.info("=" * 80)
    log.info(f"Step 2.1: Preparing connection to data server")
    log.info(f"  Server: {server}")
    log.info(f"  Port: {data_port} (expected: 1433 for privatelink)")
    log.info(f"  Database: {database}")
    log.info(f"  User: {obfuscate_sensitive(user)}")
    if cafile:
        log.info(f"  Certificate file: {cafile}")
    else:
        log.info(f"  Certificate: SSL Context (configured via pytds.tls.create_context)")
    log.info(f"  Host validation: Disabled (validate_host=False)")
    
    try:
        log.info("Step 2.2: Establishing TDS connection with SSL/TLS")
        log.info(f"  Connection parameters:")
        log.info(f"    - Server: {server}:{data_port}")
        log.info(f"    - Database: {database}")
        log.info(f"    - User: {obfuscate_sensitive(user)}")
        log.info(f"    - Certificate: {'File path' if cafile else 'SSL Context (PEM)'}")
        
        conn = pytds.connect(
            server=server,
            database=database,
            user=user,
            password=password,
            port=int(data_port),
            cafile=cafile,
            validate_host=False
        )
        
        log.info("Step 2.3: Connection established successfully")
        log.info(f"  Connection object: {type(conn).__name__}")
        
        # Test the connection
        log.info("Step 2.4: Verifying connection with test query")
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT @@VERSION")
            version = cursor.fetchone()
            cursor.close()
            log.info(f"  Server version retrieved: {obfuscate_sensitive(str(version[0]) if version else 'N/A', show_chars=50)}")
        except Exception as e:
            log.warning(f"  Connection test query failed (non-critical): {e}")
        
        log.info("=" * 80)
        log.info("MSSQL AUTHENTICATION PROCESS - COMPLETE")
        log.info("=" * 80)
        log.info(f"Successfully authenticated to {server}:{data_port}/{database}")
        log.info("Ready for data replication")
        log.info("=" * 80)
        
        return conn
        
    except pytds.tds_base.Error as e:
        log.severe(f"TDS connection error: {e}")
        log.severe(f"  Server: {server}:{data_port}")
        log.severe(f"  Database: {database}")
        log.severe(f"  User: {obfuscate_sensitive(user)}")
        raise RuntimeError(f"Failed to connect to MSSQL server: {e}")
    except Exception as e:
        log.severe(f"Unexpected connection error: {e}")
        log.severe(f"  Error type: {type(e).__name__}")
        raise RuntimeError(f"Unexpected error during MSSQL connection: {e}")

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

def schema(configuration: dict) -> List[Dict[str, Any]]:
    """Discover tables, columns, and primary keys."""
    validate_configuration(configuration)
    
    raw_debug = configuration.get("debug", False)
    debug = (isinstance(raw_debug, str) and raw_debug.lower() == 'true')
    log.info(f"Debug mode: {debug}")
    
    conn_manager = ConnectionManager(configuration)
    
    with conn_manager.get_cursor() as cursor:
        query = configuration["schema_table_list_query_debug"] if debug else configuration["schema_table_list_query"]
        cursor.execute(query)
        tables = [r['TABLE_NAME'] if isinstance(r, dict) else r[0]
                  for r in cursor.fetchall()]
    
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
    
    return result

def process_incremental_sync(table: str, configuration: dict, state: dict, 
                           conn_manager: ConnectionManager, pk_map_full: Dict[str, List[str]]):
    """Process incremental sync for a table."""
    start_date = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    records_processed = 0
    
    # Get table size for adaptive parameters
    try:
        with conn_manager.get_cursor() as cursor:
            cursor.execute(configuration["src_val_record_count"].format(tableName=table))
            row = cursor.fetchone()
            total_rows = row[0] if not isinstance(row, dict) else list(row.values())[0]
            
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
        query = configuration["src_upsert_records"].format(
            tableName=table, endDate=state[table], startDate=start_date
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
                yield op.upsert(table=table, data=flat_data)
                records_processed += 1
                
                # Checkpoint every adaptive interval
                if records_processed % checkpoint_interval == 0:
                    log.info(f"Checkpointing {table} after {records_processed} records")
                    yield op.checkpoint(state)
        
        # Process deletes
        pk_cols = pk_map_full.get(table, [])
        if pk_cols:
            del_query = configuration["src_del_records"].format(
                tableName=table, endDate=state[table], startDate=start_date,
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
                    yield op.delete(table=table, keys=keys)
                    records_processed += 1
    
    return records_processed

def process_full_load(table: str, configuration: dict, conn_manager: ConnectionManager, 
                     pk_map: Dict[str, str], threads: int, max_queue_size: int, state: dict):
    """Process full load for a table using partitioned approach."""
    records_processed = 0
    idx = pk_map.get(table)
    
    # Get total record count
    count_q = configuration["src_val_record_count"].format(tableName=table)
    with conn_manager.get_cursor() as cursor:
        cursor.execute(count_q)
        row = cursor.fetchone()
        total_rows = row[0] if not isinstance(row, dict) else list(row.values())[0]
        
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
                cursor.execute(configuration["src_gen_index_column"].format(tableName=table))
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
        tableName=table, indexkey=idx, threads=num_partitions
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
                        tableName=table,
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
                yield op.upsert(table=table, data=flat_data)
                records_processed += 1
                
                # Checkpoint every adaptive interval
                if records_processed % checkpoint_interval == 0:
                    log.info(f"Checkpointing {table} after {records_processed} records")
                    yield op.checkpoint(state)
    
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
                
                if table in state:
                    # Incremental sync
                    for operation in process_incremental_sync(table, configuration, state, conn_manager, pk_map_full):
                        yield operation
                        # Count upsert and delete operations for this table
                        if operation is not None and hasattr(operation, 'table') and operation.table == table:
                            records_processed += 1
                else:
                    # Full load
                    for operation in process_full_load(table, configuration, conn_manager, pk_map, threads, max_queue_size, state):
                        yield operation
                        # Count upsert operations for this table
                        if operation is not None and hasattr(operation, 'table') and operation.table == table:
                            records_processed += 1
                
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
        state[table] = start_date
        yield op.checkpoint(state)
        
        # Record count validation
        try:
            with conn_manager.get_cursor() as cursor:
                cursor.execute(configuration["src_val_record_count"].format(tableName=table))
                row = cursor.fetchone()
                count = row[0] if not isinstance(row, dict) else list(row.values())[0]
            
            yield op.upsert(table="CDK_VALIDATION", data={
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
    
# Initialize the connector with the defined update and schema functions
connector = Connector(update=update, schema=schema)

if __name__ == "__main__":
    current_dir = os.path.dirname(os.path.abspath(__file__))
    config_path = os.path.join(current_dir, "configuration.json")
    with open(config_path, "r") as f:
        cfg = json.load(f)
    connector.debug(configuration=cfg) 
