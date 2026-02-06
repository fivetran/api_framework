"""
Fivetran Connector SDK for SQL Server replication via PrivateLink.

This connector efficiently replicates data from SQL Server to Fivetran destination,
handling 70+ billion rows with minimal resource usage. It uses incremental replication
based on timestamp columns, tracking state per table for optimal performance.

Optimized for speed with:
- Table slicing/partitioning for large tables (>100K rows)
- Adaptive batch sizes based on table size
- Table categorization and optimal processing order
- Deadlock and timeout detection
- Connection timeout management

See: https://fivetran.com/docs/connectors/connector-sdk/technical-reference
"""

from fivetran_connector_sdk import Connector, Logging as log, Operations as op
import json
import pytds
import platform
import re
import subprocess
import tempfile
import os
import requests
import math
import queue
import time
import random
import concurrent.futures
import hashlib
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple
from contextlib import contextmanager
import threading

# Resource-optimized constants for 1GB RAM / <0.5 vCPU environment
BATCH_SIZE = 10000  # Base batch size - adaptive based on table size
PARTITION_SIZE = 100000  # Rows per partition for large tables
CHECKPOINT_INTERVAL = 2000000  # Checkpoint every 2M records (adaptive)
MAX_RETRIES = 5
BASE_RETRY_DELAY = 5
MAX_RETRY_DELAY = 300
CONNECTION_TIMEOUT_HOURS = 3

# Table size thresholds for adaptive processing
SMALL_TABLE_THRESHOLD = 1000000  # 1M rows
LARGE_TABLE_THRESHOLD = 100000000  # 100M rows
SLICE_THRESHOLD = 100000  # Slice tables >100K rows
MAX_THREADS = 4  # Maximum threads for parallel slice processing

# Deleted record detection thresholds (for scalability)
# Per agentsv2.md: State ONLY stores {table}_last_sync timestamps (concise, table-level adjustments)
# Delete detection uses SQL queries to source database (no PK sets in state)
DELETE_BATCH_SIZE = 1000  # Batch delete operations for efficiency

# Deadlock and timeout detection patterns
DEADLOCK_PATTERNS = [
    'deadlock', 'lock timeout', 'lock wait timeout', 'transaction deadlock',
    'lock request time out period exceeded', 'lock escalation', 'lock conflict',
    'blocked by another transaction'
]
TIMEOUT_PATTERNS = [
    'connection timeout', 'connection reset', 'connection lost', 'network timeout',
    'read timeout', 'write timeout', 'socket timeout', 'timeout expired'
]


def validate_configuration(configuration: dict) -> None:
    """Validate required configuration parameters."""
    required = [
        "MSSQL_SERVER", "MSSQL_DATABASE", "MSSQL_USER", "MSSQL_PASSWORD",
        "MSSQL_PORT", "MSSQL_SCHEMA", "schema_table_list_query",
        "schema_pk_query", "schema_col_query"
    ]
    missing = [k for k in required if k not in configuration or not configuration[k]]
    if missing:
        raise ValueError(f"Missing required configuration: {', '.join(missing)}")


def detect_deleted_records_sql(configuration: dict, table: str, pks: List[str], 
                                inc_col: Optional[str], start_date: Optional[datetime], 
                                end_date: datetime, conn_manager: ConnectionManager) -> int:
    """
    Detect deleted records using SQL query to source database.
    Queries for records with _IsDeleted = 1 flag (SQL-based, no state storage needed).
    
    Per agentsv2.md: State only contains {table}_last_sync timestamps (concise).
    
    Args:
        configuration: Configuration dictionary
        table: Table name
        pks: Primary key columns
        inc_col: Incremental timestamp column
        start_date: Start date for incremental sync (None for full sync)
        end_date: End date for sync
        conn_manager: Connection manager
        
    Returns:
        Number of deleted records processed
    """
    if not pks:
        return 0
    
    schema_name = configuration.get("MSSQL_SCHEMA", "dbo")
    qualified_table = f"[{schema_name}].[{table}]"
    
    # Build delete detection query
    # Query for records with _IsDeleted = 1 flag
    pk_cols = ", ".join([f"[{pk}]" for pk in pks])
    
    if inc_col and start_date:
        # Incremental sync: query deleted records in time range
        start_str = start_date.strftime('%Y-%m-%d %H:%M:%S')
        end_str = end_date.strftime('%Y-%m-%d %H:%M:%S')
        delete_query = f"""
        SELECT {pk_cols}
        FROM {qualified_table}
        WHERE [{inc_col}] >= '{start_str}' AND [{inc_col}] <= '{end_str}' 
        AND _IsDeleted = 1
        """
    else:
        # Full sync: query all deleted records
        delete_query = f"""
        SELECT {pk_cols}
        FROM {qualified_table}
        WHERE _IsDeleted = 1
        """
    
    deleted_count = 0
    try:
        with conn_manager.get_cursor() as cursor:
            cursor.execute(delete_query)
            
            delete_batch = []
            while True:
                rows = cursor.fetchmany(DELETE_BATCH_SIZE)
                if not rows:
                    break
                
                for row in rows:
                    # Convert row to delete record with primary keys
                    if isinstance(row, dict):
                        delete_record = {pk: row.get(pk) for pk in pks}
                    else:
                        delete_record = {pk: row[i] for i, pk in enumerate(pks)}
                    
                    delete_batch.append(delete_record)
                    
                    # Process in batches
                    if len(delete_batch) >= DELETE_BATCH_SIZE:
                        for record in delete_batch:
                            op.delete(table=table, data=record)
                        deleted_count += len(delete_batch)
                        delete_batch = []
            
            # Process remaining deletes after loop completes
            if delete_batch:
                for record in delete_batch:
                    op.delete(table=table, data=record)
                deleted_count += len(delete_batch)
        
        if deleted_count > 0:
            log.info(f"Table {table}: Marked {deleted_count:,} records as deleted (SQL-based detection)")
    
    except Exception as e:
        # If _IsDeleted column doesn't exist, log warning and continue
        if "_IsDeleted" in str(e) or "Invalid column name" in str(e):
            log.warning(f"Table {table}: _IsDeleted column not found, skipping delete detection: {e}")
        else:
            log.warning(f"Table {table}: Error detecting deleted records: {e}")
    
    return deleted_count


def generate_cert_chain(server: str, port: int) -> str:
    """Generate certificate chain file by fetching certificates."""
    try:
        proc = subprocess.run(
            ['openssl', 's_client', '-showcerts', '-connect', f'{server}:{port}'],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            timeout=30
        )
        root_pem = requests.get(
            'https://cacerts.digicert.com/DigiCertGlobalRootG2.crt.pem',
            timeout=30
        ).text
        pem_blocks = re.findall(
            r'-----BEGIN CERTIFICATE-----.+?-----END CERTIFICATE-----',
            proc.stdout,
            re.DOTALL
        )
        intermediate = pem_blocks[1] if len(pem_blocks) > 1 else pem_blocks[0]
        tmp = tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.pem')
        tmp.write(intermediate + '\n' + root_pem)
        tmp.flush()
        tmp.close()
        return tmp.name
    except Exception as e:
        log.warning(f"Failed to generate cert chain: {e}")
        return None


def connect_to_mssql(configuration: dict):
    """Connect to SQL Server via PrivateLink using TDS protocol."""
    is_local = platform.system() == "Darwin"
    server_key = "MSSQL_SERVER_DIR" if is_local else "MSSQL_SERVER"
    port_key = "MSSQL_PORT_DIR" if is_local else "MSSQL_PORT"
    
    server = configuration.get(server_key) or configuration.get("MSSQL_SERVER")
    port = int(configuration.get(port_key) or configuration.get("MSSQL_PORT", "1433"))
    
    # Handle certificate for PrivateLink
    cafile = None
    if configuration.get("cdw_cert"):
        cert_content = configuration["cdw_cert"]
        if cert_content.lstrip().startswith("-----BEGIN"):
            # Inline certificate - create temp file
            tmp = tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.pem')
            tmp.write(cert_content)
            tmp.flush()
            tmp.close()
            cafile = tmp.name
        elif os.path.isfile(cert_content):
            cafile = cert_content
    else:
        # Generate cert chain if cert server is provided
        cert_server = configuration.get("MSSQL_CERT_SERVER") or server
        cert_port = int(configuration.get("MSSQL_CERT_PORT", port))
        cafile = generate_cert_chain(cert_server, cert_port)
    
    try:
        conn = pytds.connect(
            server=server,
            database=configuration["MSSQL_DATABASE"],
            user=configuration["MSSQL_USER"],
            password=configuration["MSSQL_PASSWORD"],
            port=port,
            cafile=cafile,
            validate_host=False,
            timeout=300
        )
        log.info(f"Connected to SQL Server: {server}:{port}")
        return conn
    except Exception as e:
        log.severe(f"Failed to connect to SQL Server: {e}")
        raise


class DeadlockError(Exception):
    """Custom exception for deadlock errors."""
    pass

class TimeoutError(Exception):
    """Custom exception for timeout errors."""
    pass


class ConnectionManager:
    """Thread-safe connection manager with deadlock/timeout detection and adaptive timeouts."""
    
    def __init__(self, configuration: dict, table_size: int = 0):
        self.configuration = configuration
        self.table_size = table_size
        self.connection = None
        self.connection_start_time = None
        self.lock = threading.Lock()
        self.timeout_hours = self._get_adaptive_timeout(table_size)
    
    def _get_adaptive_timeout(self, table_size: int) -> int:
        """Get adaptive timeout based on table size."""
        if table_size < SMALL_TABLE_THRESHOLD:
            return CONNECTION_TIMEOUT_HOURS
        elif table_size < LARGE_TABLE_THRESHOLD:
            return CONNECTION_TIMEOUT_HOURS * 2
        else:
            return CONNECTION_TIMEOUT_HOURS * 4
    
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
    
    @contextmanager
    def get_cursor(self):
        """Get database cursor with automatic connection management and error detection."""
        with self.lock:
            try:
                if self._is_connection_expired() or self.connection is None:
                    if self.connection:
                        try:
                            self.connection.close()
                        except:
                            pass
                    self.connection = connect_to_mssql(self.configuration)
                    self.connection_start_time = datetime.utcnow()
                yield self.connection.cursor()
            except Exception as e:
                # Handle deadlock and timeout errors
                if self._is_deadlock_error(e):
                    log.warning(f"Deadlock detected: {e}")
                    if self.connection:
                        try:
                            self.connection.close()
                        except:
                            pass
                    self.connection = None
                    raise DeadlockError(f"Database deadlock: {e}")
                elif self._is_timeout_error(e):
                    log.warning(f"Connection timeout detected: {e}")
                    if self.connection:
                        try:
                            self.connection.close()
                        except:
                            pass
                    self.connection = None
                    raise TimeoutError(f"Database timeout: {e}")
                else:
                    # Reconnect on other errors
                    log.warning(f"Connection error, reconnecting: {e}")
                    if self.connection:
                        try:
                            self.connection.close()
                        except:
                            pass
                    self.connection = None
                    self.connection = connect_to_mssql(self.configuration)
                    self.connection_start_time = datetime.utcnow()
                    yield self.connection.cursor()
    
    def close(self):
        """Close database connection."""
        with self.lock:
            if self.connection:
                try:
                    self.connection.close()
                except:
                    pass
                self.connection = None
                self.connection_start_time = None


def get_table_list(configuration: dict, conn_manager: ConnectionManager) -> List[str]:
    """Discover tables to replicate."""
    with conn_manager.get_cursor() as cursor:
        query = configuration["schema_table_list_query"]
        cursor.execute(query)
        tables = [row[0] if isinstance(row, (list, tuple)) else row.get('TABLE_NAME', list(row.values())[0])
                  for row in cursor.fetchall()]
    return tables


def get_primary_keys(configuration: dict, table: str, conn_manager: ConnectionManager) -> List[str]:
    """Get primary key columns for a table."""
    with conn_manager.get_cursor() as cursor:
        query = configuration["schema_pk_query"].format(table=table)
        cursor.execute(query)
        pks = [row[0] if isinstance(row, (list, tuple)) else row.get('COLUMN_NAME', list(row.values())[0])
               for row in cursor.fetchall()]
    return pks


def get_table_columns(configuration: dict, table: str, conn_manager: ConnectionManager) -> List[str]:
    """Get all columns for a table."""
    with conn_manager.get_cursor() as cursor:
        query = configuration["schema_col_query"].format(table=table)
        cursor.execute(query)
        cols = [row[0] if isinstance(row, (list, tuple)) else row.get('COLUMN_NAME', list(row.values())[0])
                for row in cursor.fetchall()]
    return cols


def schema(configuration: dict) -> List[Dict[str, Any]]:
    """
    Define schema for all tables in the database.
    
    See: https://fivetran.com/docs/connectors/connector-sdk/technical-reference#schema
    """
    validate_configuration(configuration)
    
    conn_manager = ConnectionManager(configuration)
    try:
        tables = get_table_list(configuration, conn_manager)
        
        result = []
        for table in tables:
            try:
                pks = get_primary_keys(configuration, table, conn_manager)
                result.append({
                    "table": table,
                    "primary_key": pks if pks else None
                })
            except Exception as e:
                log.warning(f"Failed to get schema for {table}: {e}")
                # Include table even if PK discovery fails
                result.append({"table": table})
        
        log.info(f"Discovered {len(result)} tables")
        return result
    finally:
        conn_manager.close()


def get_adaptive_batch_size(table_size: int) -> int:
    """Get optimal batch size based on table size."""
    if table_size < SMALL_TABLE_THRESHOLD:
        return BATCH_SIZE  # 10K for small tables
    elif table_size < LARGE_TABLE_THRESHOLD:
        return int(BATCH_SIZE * 0.6)  # 6K for medium tables
    else:
        return BATCH_SIZE // 3  # 3.3K for large tables


def get_adaptive_checkpoint_interval(table_size: int) -> int:
    """Get adaptive checkpoint interval based on table size."""
    if table_size < SMALL_TABLE_THRESHOLD:
        return CHECKPOINT_INTERVAL  # 2M for small tables
    elif table_size < LARGE_TABLE_THRESHOLD:
        return int(CHECKPOINT_INTERVAL * 0.75)  # 1.5M for medium tables
    else:
        return CHECKPOINT_INTERVAL // 5  # 400K for large tables


def get_table_size(configuration: dict, table: str, conn_manager: ConnectionManager) -> int:
    """Get row count for a table efficiently."""
    schema_name = configuration.get("MSSQL_SCHEMA", "dbo")
    qualified_table = f"[{schema_name}].[{table}]"
    
    # Try to use sys.partitions for fast count (more efficient than COUNT(*))
    try:
        with conn_manager.get_cursor() as cursor:
            size_query = f"""
            SELECT SUM(p.rows) AS ROW_COUNT
            FROM sys.tables t
            INNER JOIN sys.indexes i ON t.object_id = i.object_id AND i.index_id < 2
            INNER JOIN sys.partitions p ON i.object_id = p.object_id AND i.index_id = p.index_id
            WHERE t.name = '{table}' AND SCHEMA_NAME(t.schema_id) = '{schema_name}'
            """
            cursor.execute(size_query)
            row = cursor.fetchone()
            if row:
                count = row[0] if isinstance(row, (list, tuple)) else row.get('ROW_COUNT', list(row.values())[0])
                return int(count) if count else 0
    except Exception as e:
        log.warning(f"Failed to get table size via sys.partitions for {table}: {e}")
    
    # Fallback to COUNT(*) if available in config
    if "src_val_record_count" in configuration:
        try:
            with conn_manager.get_cursor() as cursor:
                # Handle both {tableName} and {schema} placeholders
                query = configuration["src_val_record_count"].replace("{tableName}", qualified_table).replace("{schema}", schema_name)
                cursor.execute(query)
                row = cursor.fetchone()
                if row:
                    count = row[0] if isinstance(row, (list, tuple)) else list(row.values())[0]
                    return int(count) if count else 0
        except Exception as e:
            log.warning(f"Failed to get table size for {table}: {e}")
    
    return 0


def get_table_sizes(configuration: dict, conn_manager: ConnectionManager, tables: List[str]) -> Dict[str, int]:
    """Get row counts for all tables efficiently using single query."""
    table_sizes = {}
    schema_name = configuration.get("MSSQL_SCHEMA", "dbo")
    
    # Use single query to get all table sizes at once
    try:
        with conn_manager.get_cursor() as cursor:
            placeholders = ','.join([f"'{table}'" for table in tables])
            size_query = f"""
            SELECT 
                t.name AS TABLE_NAME,
                SUM(p.rows) AS ROW_COUNT
            FROM sys.tables t
            INNER JOIN sys.indexes i ON t.object_id = i.object_id AND i.index_id < 2
            INNER JOIN sys.partitions p ON i.object_id = p.object_id AND i.index_id = p.index_id
            WHERE t.name IN ({placeholders}) AND SCHEMA_NAME(t.schema_id) = '{schema_name}'
            GROUP BY t.name
            """
            cursor.execute(size_query)
            for row in cursor.fetchall():
                table_name = row[0] if isinstance(row, (list, tuple)) else row.get('TABLE_NAME', list(row.values())[0])
                row_count = row[1] if isinstance(row, (list, tuple)) else row.get('ROW_COUNT', list(row.values())[1])
                table_sizes[table_name] = int(row_count) if row_count else 0
    except Exception as e:
        log.warning(f"Failed to get table sizes efficiently: {e}. Falling back to individual queries.")
        # Fallback to individual queries
        for table in tables:
            table_sizes[table] = get_table_size(configuration, table, conn_manager)
    
    return table_sizes


def categorize_and_sort_tables(tables: List[str], table_sizes: Dict[str, int]) -> List[Tuple[str, str, int]]:
    """Categorize tables by size and sort for optimal processing order (small first)."""
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


def get_incremental_column(configuration: dict, table: str, conn_manager: Optional[ConnectionManager] = None) -> Optional[str]:
    """
    Get the incremental timestamp column for a table with priority order (FIFO).
    
    Priority order:
    1. Table-specific configuration (incremental_timestamp_column_{table})
    2. Global comma-separated list (incremental_timestamp_column) - checks each column in order
       until one exists in the table
    
    Args:
        configuration: Configuration dictionary
        table: Table name
        conn_manager: Connection manager (optional, used to verify column existence)
    
    Returns:
        First available column from priority list, or None if none found
    """
    # Check table-specific configuration first (highest priority)
    table_col_key = f"incremental_timestamp_column_{table}".lower()
    for key, value in configuration.items():
        if key.lower() == table_col_key and value:
            column_name = value.strip()
            # If conn_manager provided, verify column exists (case-insensitive)
            if conn_manager:
                try:
                    columns = get_table_columns(configuration, table, conn_manager)
                    # Case-insensitive matching
                    columns_lower = {col.lower(): col for col in columns}
                    if column_name.lower() in columns_lower:
                        # Return the actual column name from the table (preserves case)
                        return columns_lower[column_name.lower()]
                    else:
                        log.warning(f"Table {table}: Table-specific column '{column_name}' not found, checking global priority list")
                except Exception as e:
                    log.warning(f"Table {table}: Error verifying table-specific column '{column_name}': {e}")
            else:
                return column_name
    
    # Check global configuration - parse comma-separated list
    global_col_config = configuration.get("incremental_timestamp_column", "").strip()
    if not global_col_config:
        return None
    
    # Parse comma-separated columns (FIFO priority order)
    column_candidates = [col.strip() for col in global_col_config.split(",") if col.strip()]
    
    if not column_candidates:
        return None
    
    # If only one column specified, return it (backward compatible)
    if len(column_candidates) == 1:
        return column_candidates[0]
    
    # Multiple columns specified - check each in priority order (FIFO)
    # If conn_manager provided, verify which columns exist in the table
    if conn_manager:
        try:
            table_columns = get_table_columns(configuration, table, conn_manager)
            # Create case-insensitive lookup (preserves actual case from table)
            columns_lower = {col.lower(): col for col in table_columns}
            
            # Return first column from priority list that exists in the table (case-insensitive match)
            for candidate in column_candidates:
                if candidate.lower() in columns_lower:
                    # Return the actual column name from the table (preserves case)
                    actual_column = columns_lower[candidate.lower()]
                    log.info(f"Table {table}: Selected incremental column '{actual_column}' from priority list (matched '{candidate}')")
                    return actual_column
            
            # None of the priority columns exist
            log.warning(f"Table {table}: None of the priority columns {column_candidates} exist in table. Available columns: {table_columns[:10]}...")
            return None
        except Exception as e:
            log.warning(f"Table {table}: Error checking column existence: {e}. Using first column from priority list: {column_candidates[0]}")
            return column_candidates[0]  # Fallback to first in list
    else:
        # No conn_manager - return first column (will be verified later when conn_manager is available)
        return column_candidates[0]


def get_date_range(configuration: dict, state: dict, table: str, inc_col: Optional[str]) -> Tuple[Optional[datetime], datetime]:
    """
    Calculate date range for incremental sync.
    
    Returns: (start_date, end_date)
    - start_date: None for full sync, datetime for incremental
    - end_date: Current timestamp (exclusive)
    """
    end_date = datetime.utcnow()
    
    # Check if this is initial sync (no state for this table)
    state_key = f"{table}_last_sync"
    last_sync_str = state.get(state_key)
    
    if not last_sync_str or not inc_col:
        # Full sync - no date filtering
        return None, end_date
    
    try:
        # Parse last sync timestamp
        if isinstance(last_sync_str, str):
            # Try ISO format first
            try:
                start_date = datetime.fromisoformat(last_sync_str.replace('Z', '+00:00'))
            except:
                # Try common SQL Server formats
                for fmt in ['%Y-%m-%d %H:%M:%S', '%Y-%m-%dT%H:%M:%S', '%Y-%m-%d']:
                    try:
                        start_date = datetime.strptime(last_sync_str, fmt)
                        break
                    except:
                        continue
                else:
                    log.warning(f"Could not parse last_sync for {table}: {last_sync_str}")
                    return None, end_date
        else:
            start_date = last_sync_str
        
        log.info(f"Table {table}: Incremental sync from {start_date} to {end_date}")
        return start_date, end_date
    except Exception as e:
        log.warning(f"Error parsing last_sync for {table}: {e}")
        return None, end_date


def build_query(configuration: dict, table: str, inc_col: Optional[str], 
                start_date: Optional[datetime], end_date: datetime) -> str:
    """Build SQL query for table replication."""
    schema_name = configuration.get("MSSQL_SCHEMA", "dbo")
    qualified_table = f"[{schema_name}].[{table}]"
    
    # Build WHERE clause for incremental sync
    where_clause = ""
    if inc_col and start_date:
        # Incremental: get records updated since last sync (exclude nulls for incremental)
        start_str = start_date.strftime('%Y-%m-%d %H:%M:%S')
        end_str = end_date.strftime('%Y-%m-%d %H:%M:%S')
        where_clause = f"WHERE [{inc_col}] > '{start_str}' AND [{inc_col}] <= '{end_str}'"
    # For full sync, include ALL records including those with null timestamps
    # No WHERE clause needed - we want everything
    
    # Get all columns
    query = f"SELECT * FROM {qualified_table}"
    if where_clause:
        query += f" {where_clause}"
    if inc_col:
        query += f" ORDER BY [{inc_col}]"
    
    return query


def get_table_index_column(configuration: dict, table: str, conn_manager: ConnectionManager) -> Optional[str]:
    """Get index column for table slicing (prefer primary key, fallback to first column)."""
    pks = get_primary_keys(configuration, table, conn_manager)
    if pks:
        return pks[0]
    
    # Fallback: get first column
    try:
        cols = get_table_columns(configuration, table, conn_manager)
        return cols[0] if cols else None
    except:
        return None


def get_table_bounds(configuration: dict, table: str, index_col: str, num_slices: int, 
                    conn_manager: ConnectionManager) -> List[Tuple[Any, Any]]:
    """Generate partition bounds for table slicing."""
    schema_name = configuration.get("MSSQL_SCHEMA", "dbo")
    qualified_table = f"[{schema_name}].[{table}]"
    
    # Get min and max values for index column
    with conn_manager.get_cursor() as cursor:
        bounds_query = f"""
        SELECT 
            MIN([{index_col}]) AS min_val,
            MAX([{index_col}]) AS max_val
        FROM {qualified_table}
        """
        cursor.execute(bounds_query)
        row = cursor.fetchone()
        if not row:
            return []
        
        min_val = row[0] if isinstance(row, (list, tuple)) else row.get('min_val', list(row.values())[0])
        max_val = row[1] if isinstance(row, (list, tuple)) else row.get('max_val', list(row.values())[1])
        
        if min_val is None or max_val is None:
            return []
        
        # Generate bounds
        bounds = []
        if isinstance(min_val, (int, float)) and isinstance(max_val, (int, float)):
            step = (max_val - min_val) / num_slices
            for i in range(num_slices):
                lower = min_val + (i * step)
                upper = min_val + ((i + 1) * step) if i < num_slices - 1 else max_val
                bounds.append((lower, upper))
        else:
            # For non-numeric, use NTILE approach
            ntile_query = f"""
            SELECT 
                [{index_col}],
                NTILE({num_slices}) OVER (ORDER BY [{index_col}]) AS tile
            FROM {qualified_table}
            GROUP BY [{index_col}]
            """
            cursor.execute(ntile_query)
            # This is simplified - actual implementation would need more complex logic
            bounds.append((min_val, max_val))
        
        return bounds


def process_slice(slice_data: Tuple[int, Any, Any, dict, str, str, List[str], int, int, str, Optional[str]]) -> Tuple[int, Optional[datetime]]:
    """Process a single slice in a thread. Returns (records_processed, max_timestamp)."""
    slice_idx, lower_bound, upper_bound, configuration, table, qualified_table, columns, batch_size, checkpoint_interval, index_col, inc_col = slice_data
    
    slice_conn_manager = ConnectionManager(configuration)
    records_processed = 0
    max_timestamp = None
    
    try:
        # Build slice query - include null timestamps for full sync
        slice_query = f"SELECT * FROM {qualified_table} WHERE [{index_col}] >= '{lower_bound}' AND [{index_col}] < '{upper_bound}'"
        
        with slice_conn_manager.get_cursor() as cursor:
            cursor.execute(slice_query)
            
            while True:
                rows = cursor.fetchmany(batch_size)
                if not rows:
                    break
                
                for row in rows:
                    if isinstance(row, dict):
                        record = row
                    else:
                        record = {col: val for col, val in zip(columns, row)}
                    
                    op.upsert(table=table, data=record)
                    records_processed += 1
                    
                    # Track max timestamp
                    if inc_col and record.get(inc_col):
                        ts = record[inc_col]
                        if isinstance(ts, str):
                            try:
                                ts = datetime.fromisoformat(ts.replace('Z', '+00:00'))
                            except:
                                try:
                                    ts = datetime.strptime(ts, '%Y-%m-%d %H:%M:%S')
                                except:
                                    continue
                        if isinstance(ts, datetime):
                            if max_timestamp is None or ts > max_timestamp:
                                max_timestamp = ts
                
                # Checkpoint periodically (thread-safe, reduced logging)
                if records_processed % checkpoint_interval == 0:
                    # Strategic logging: only log every 5M records per slice
                    if records_processed % (checkpoint_interval * 5) == 0:
                        log.info(f"Table {table} slice {slice_idx+1}: Processed {records_processed:,} records")
    
    except Exception as e:
        log.severe(f"Table {table} slice {slice_idx+1}: Error - {e}")
        log.severe(f"Table {table} slice {slice_idx+1}: Query was: {slice_query[:200]}...")
        raise
    finally:
        slice_conn_manager.close()
    
    return records_processed, max_timestamp


def process_table_with_slicing(configuration: dict, state: dict, table: str, 
                               table_size: int, conn_manager: ConnectionManager) -> int:
    """Process large table with slicing/partitioning using thread pool (max 4 threads)."""
    schema_name = configuration.get("MSSQL_SCHEMA", "dbo")
    qualified_table = f"[{schema_name}].[{table}]"
    
    # Calculate number of slices (cap based on available threads)
    num_slices = max(1, min(table_size // SLICE_THRESHOLD, MAX_THREADS))  # Cap at max threads
    
    # Get index column for slicing
    index_col = get_table_index_column(configuration, table, conn_manager)
    if not index_col:
        log.warning(f"Table {table}: No index column found, processing without slicing")
        return process_table_direct(configuration, state, table, table_size, conn_manager)
    
    # Get partition bounds
    bounds = get_table_bounds(configuration, table, index_col, num_slices, conn_manager)
    if not bounds:
        log.warning(f"Table {table}: Could not generate bounds, processing without slicing")
        return process_table_direct(configuration, state, table, table_size, conn_manager)
    
    log.info(f"Table {table}: Processing with {len(bounds)} slices using up to {MAX_THREADS} threads")
    
    # Get columns
    columns = get_table_columns(configuration, table, conn_manager)
    batch_size = get_adaptive_batch_size(table_size)
    checkpoint_interval = get_adaptive_checkpoint_interval(table_size)
    inc_col = get_incremental_column(configuration, table, conn_manager)
    
    # Prepare slice data for parallel processing
    slice_data_list = []
    for slice_idx, (lower_bound, upper_bound) in enumerate(bounds):
        slice_data = (
            slice_idx + 1, lower_bound, upper_bound, configuration, table, 
            qualified_table, columns, batch_size, checkpoint_interval, index_col, inc_col
        )
        slice_data_list.append(slice_data)
    
    # Process slices in parallel using thread pool (max 4 threads)
    total_records = 0
    max_timestamp = None
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        futures = [executor.submit(process_slice, slice_data) for slice_data in slice_data_list]
        
        for future in concurrent.futures.as_completed(futures):
            try:
                slice_records, slice_max_ts = future.result()
                total_records += slice_records
                
                # Track overall max timestamp
                if slice_max_ts:
                    if max_timestamp is None or slice_max_ts > max_timestamp:
                        max_timestamp = slice_max_ts
                
                # Checkpoint after each slice completes
                if inc_col and max_timestamp:
                    state[f"{table}_last_sync"] = max_timestamp.isoformat()
                op.checkpoint(state=state)
                
            except Exception as e:
                log.severe(f"Table {table}: Slice processing failed - {e}")
                raise
    
    # Handle deleted records using SQL-based detection (no PK sets in state)
    # Per agentsv2.md: State ONLY stores {table}_last_sync timestamps (concise)
    pks = get_primary_keys(configuration, table, conn_manager)
    inc_col = get_incremental_column(configuration, table, conn_manager)
    start_date, end_date = get_date_range(configuration, state, table, inc_col)
    if pks:
        detect_deleted_records_sql(configuration, table, pks, inc_col, start_date, end_date, conn_manager)
    
    # Final checkpoint
    if inc_col and max_timestamp:
        state[f"{table}_last_sync"] = max_timestamp.isoformat()
    elif not state.get(f"{table}_last_sync") and inc_col:
        state[f"{table}_last_sync"] = datetime.utcnow().isoformat()
    
    op.checkpoint(state=state)
    log.info(f"Table {table}: Completed full sync with slicing - {total_records:,} records")
    
    return total_records


def process_table_direct(configuration: dict, state: dict, table: str, 
                        table_size: int, conn_manager: ConnectionManager) -> int:
    """Process table directly without slicing (for small tables or incremental syncs)."""
    inc_col = get_incremental_column(configuration, table, conn_manager)
    start_date, end_date = get_date_range(configuration, state, table, inc_col)
    
    is_incremental = start_date is not None
    sync_type = "incremental" if is_incremental else "full"
    
    # Get primary keys for deleted record detection
    pks = get_primary_keys(configuration, table, conn_manager)
    
    # Build query
    query = build_query(configuration, table, inc_col, start_date, end_date)
    
    # Get column names
    columns = get_table_columns(configuration, table, conn_manager)
    
    # Adaptive parameters
    batch_size = get_adaptive_batch_size(table_size)
    checkpoint_interval = get_adaptive_checkpoint_interval(table_size)
    
    records_processed = 0
    max_timestamp = None
    
    try:
        with conn_manager.get_cursor() as cursor:
            cursor.execute(query)
            
            # Process in batches
            while True:
                rows = cursor.fetchmany(batch_size)
                if not rows:
                    break
                
                # Process batch
                for row in rows:
                    # Convert row to dict
                    if isinstance(row, dict):
                        record = row
                    else:
                        record = {col: val for col, val in zip(columns, row)}
                    
                    # Upsert record
                    op.upsert(table=table, data=record)
                    records_processed += 1
                    
                    # Track max timestamp for incremental sync
                    if inc_col and record.get(inc_col):
                        ts = record[inc_col]
                        if isinstance(ts, str):
                            try:
                                ts = datetime.fromisoformat(ts.replace('Z', '+00:00'))
                            except:
                                try:
                                    ts = datetime.strptime(ts, '%Y-%m-%d %H:%M:%S')
                                except:
                                    continue
                        if isinstance(ts, datetime):
                            if max_timestamp is None or ts > max_timestamp:
                                max_timestamp = ts
                
                # Checkpoint periodically (reduced logging frequency)
                if records_processed % checkpoint_interval == 0:
                    if inc_col and max_timestamp:
                        state[f"{table}_last_sync"] = max_timestamp.isoformat()
                    op.checkpoint(state=state)
                    # Strategic logging: only log every 10M records for large tables
                    if records_processed % (checkpoint_interval * 5) == 0:
                        log.info(f"Table {table}: Processed {records_processed:,} records")
        
        # Handle deleted records using SQL-based detection (no PK sets in state)
        # Per agentsv2.md: State ONLY stores {table}_last_sync timestamps (concise)
        if pks:
            detect_deleted_records_sql(configuration, table, pks, inc_col, start_date, end_date, conn_manager)
        
        # Final checkpoint
        if inc_col and max_timestamp:
            state[f"{table}_last_sync"] = max_timestamp.isoformat()
        elif not is_incremental and inc_col:
            # Full sync completed - set to end_date for next incremental
            state[f"{table}_last_sync"] = end_date.isoformat()
        
        op.checkpoint(state=state)
        log.info(f"Table {table}: Completed {sync_type} sync - {records_processed:,} records")
        
    except Exception as e:
        log.severe(f"Table {table}: Error during sync: {e}")
        log.severe(f"Table {table}: Query was: {query[:200]}...")  # Log first 200 chars of query
        raise RuntimeError(f"Failed to sync table {table}: {str(e)}")
    
    return records_processed


def process_table(configuration: dict, state: dict, table: str, 
                 table_size: int, conn_manager: ConnectionManager) -> int:
    """
    Process a single table: fetch and upsert records.
    Uses slicing for large tables, direct processing for small tables.
    
    Returns: Number of records processed
    """
    # Check if incremental sync
    inc_col = get_incremental_column(configuration, table, conn_manager)
    start_date, _ = get_date_range(configuration, state, table, inc_col)
    is_incremental = start_date is not None
    
    # Use slicing for large full loads, direct processing for incremental or small tables
    if not is_incremental and table_size > SLICE_THRESHOLD:
        return process_table_with_slicing(configuration, state, table, table_size, conn_manager)
    else:
        return process_table_direct(configuration, state, table, table_size, conn_manager)


def update(configuration: dict, state: dict):
    """
    Main update function called by Fivetran during each sync.
    Optimized with table categorization, adaptive parameters, and error handling.
    
    See: https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update
    """
    log.info("Starting sync")
    
    validate_configuration(configuration)
    
    max_retries = int(configuration.get("max_retries", MAX_RETRIES))
    retry_delay = float(configuration.get("retry_sleep_seconds", BASE_RETRY_DELAY))
    
    # Initialize connection manager for discovery
    conn_manager = ConnectionManager(configuration)
    
    try:
        # Get list of tables to replicate
        tables = get_table_list(configuration, conn_manager)
        
        if not tables:
            log.warning("No tables found to replicate")
            return
        
        log.info(f"Discovered {len(tables)} tables")
        
        # Get table sizes and categorize
        table_sizes = get_table_sizes(configuration, conn_manager, tables)
        categorized_tables = categorize_and_sort_tables(tables, table_sizes)
        
        # Log processing strategy
        small_count = sum(1 for _, cat, _ in categorized_tables if cat == 'small')
        medium_count = sum(1 for _, cat, _ in categorized_tables if cat == 'medium')
        large_count = sum(1 for _, cat, _ in categorized_tables if cat == 'large')
        log.info(f"Processing {len(categorized_tables)} tables: {small_count} small, {medium_count} medium, {large_count} large")
        
        # Process tables in optimal order (small first)
        total_records = 0
        processed = 0
        
        for table, category, row_count in categorized_tables:
            processed += 1
            
            # Create connection manager with table size context for adaptive timeouts
            table_conn_manager = ConnectionManager(configuration, row_count)
            
            # Retry loop with deadlock/timeout handling
            for attempt in range(max_retries):
                try:
                    records = process_table(configuration, state, table, row_count, table_conn_manager)
                    total_records += records
                    
                    # Progress update every 10 tables
                    if processed % 10 == 0:
                        log.info(f"Progress: {processed}/{len(categorized_tables)} tables, {total_records:,} total records")
                    break
                
                except (DeadlockError, TimeoutError) as e:
                    log.warning(f"{type(e).__name__} for {table}, attempt {attempt+1}/{max_retries}: {e}")
                    if attempt + 1 >= max_retries:
                        log.severe(f"Max retries exceeded for table {table}: {e}")
                        raise
                    
                    # Exponential backoff with jitter
                    base_backoff = min(retry_delay * (2 ** attempt), MAX_RETRY_DELAY)
                    delay = random.uniform(base_backoff * 0.5, base_backoff * 1.5)
                    time.sleep(delay)
                    continue
                
                except Exception as e:
                    log.severe(f"Failed to process table {table}: {e}")
                    if attempt + 1 >= max_retries:
                        raise
                    time.sleep(retry_delay)
                    continue
            
            table_conn_manager.close()
        
        log.info(f"Sync completed: {total_records:,} total records replicated across {processed} tables")
        
    finally:
        conn_manager.close()


# Initialize connector
connector = Connector(update=update, schema=schema)

# Main entry point for local testing
if __name__ == "__main__":
    config_path = os.path.join(os.path.dirname(__file__), "configuration.json")
    with open(config_path, 'r') as f:
        configuration = json.load(f)
    connector.debug(configuration=configuration)
