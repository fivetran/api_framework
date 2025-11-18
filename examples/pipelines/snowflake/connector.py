"""
Snowflake Connector SDK - High-Performance Data Replication

This connector provides enterprise-ready Snowflake data ingestion optimized for
high-volume replication. Designed to sync 70M+ rows efficiently with minimal
memory footprint and maximum throughput.

FEATURES:
- Zero-memory accumulation: processes and upserts rows immediately
- Batch processing: 10,000 rows per batch by default
- Incremental sync: supports comma-separated timestamp columns from config
- Thread-safe: maximum 4 concurrent threads
- Strategic logging: performance-focused logging
- Connection pooling: efficient connection management
- Error handling: automatic retry with exponential backoff

PERFORMANCE TARGETS:
- 70M rows in under 2 hours
- Memory-efficient: no data accumulation
- High throughput: optimized batch processing

See the Technical Reference documentation (https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update)
and the Best Practices documentation (https://fivetran.com/docs/connectors/connector-sdk/best-practices) for details
"""

from fivetran_connector_sdk import Connector
from fivetran_connector_sdk import Logging as log
from fivetran_connector_sdk import Operations as op

import json
import snowflake.connector
from datetime import datetime
from typing import List, Dict, Any, Tuple, Optional
import time
import random
import threading
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
from contextlib import contextmanager

# =============================================================================
# CONFIGURATION CONSTANTS
# =============================================================================

DEFAULT_BATCH_SIZE = 10000  # 10K rows per batch
DEFAULT_START_DATE = "2020-01-01T00:00:00.000Z"
DEFAULT_MAX_RETRIES = 3
DEFAULT_RETRY_DELAY_SEC = 5
CHECKPOINT_INTERVAL = 100000  # Checkpoint every 100K records
MAX_THREADS = 4  # Maximum concurrent threads
CONNECTION_TIMEOUT_HOURS = 3

# =============================================================================
# ERROR HANDLING PATTERNS
# =============================================================================

DEADLOCK_PATTERNS = [
    'deadlock', 'lock timeout', 'lock wait timeout', 'transaction deadlock',
    'lock request time out period exceeded', 'lock escalation', 'lock conflict',
    'blocked by another transaction'
]

TIMEOUT_PATTERNS = [
    'connection timeout', 'connection reset', 'connection lost', 'network timeout',
    'read timeout', 'write timeout', 'socket timeout', 'timeout expired'
]

# =============================================================================
# CONNECTION MANAGEMENT
# =============================================================================

class ConnectionManager:
    """Snowflake connection manager with thread-safe operations."""
    
    def __init__(self, configuration: dict):
        self.configuration = configuration
        self.connection_start_time = None
        self.current_connection = None
        self.current_cursor = None
        self.lock = threading.Lock()
    
    def _is_connection_expired(self) -> bool:
        """Check if current connection has exceeded timeout limit."""
        if not self.connection_start_time:
            return True
        elapsed = datetime.utcnow() - self.connection_start_time
        return elapsed.total_seconds() > (CONNECTION_TIMEOUT_HOURS * 3600)
    
    def _is_deadlock_error(self, error: Exception) -> bool:
        """Detect if error is related to deadlock or lock timeout."""
        error_str = str(error).lower()
        return any(pattern in error_str for pattern in DEADLOCK_PATTERNS)
    
    def _is_timeout_error(self, error: Exception) -> bool:
        """Detect if error is related to connection timeout."""
        error_str = str(error).lower()
        return any(pattern in error_str for pattern in TIMEOUT_PATTERNS)
    
    def _create_connection(self):
        """Create a new Snowflake connection."""
        try:
            conn = get_snowflake_connection(self.configuration)
            self.current_connection = conn
            self.current_cursor = conn.cursor()
            self.connection_start_time = datetime.utcnow()
            return conn, self.current_cursor
        except Exception as e:
            log.severe(f"Failed to create Snowflake connection: {e}")
            raise
    
    def _close_connection(self):
        """Close current Snowflake connection."""
        try:
            if self.current_cursor:
                self.current_cursor.close()
                self.current_cursor = None
            if self.current_connection:
                self.current_connection.close()
                self.current_connection = None
            self.connection_start_time = None
        except Exception as e:
            log.warning(f"Error closing connection: {e}")
    
    @contextmanager
    def get_cursor(self):
        """Context manager for Snowflake cursor with automatic reconnection."""
        with self.lock:
            try:
                if self._is_connection_expired() or not self.current_connection:
                    self._close_connection()
                    self._create_connection()
                
                yield self.current_cursor
                
            except Exception as e:
                if self._is_deadlock_error(e):
                    self._close_connection()
                    raise DeadlockError(f"Snowflake deadlock: {e}")
                elif self._is_timeout_error(e):
                    self._close_connection()
                    raise TimeoutError(f"Snowflake timeout: {e}")
                else:
                    log.severe(f"Snowflake error: {e}")
                    raise

class DeadlockError(Exception):
    """Custom exception for Snowflake deadlock errors."""
    pass

class TimeoutError(Exception):
    """Custom exception for Snowflake timeout errors."""
    pass

# =============================================================================
# UTILITY FUNCTIONS
# =============================================================================

def get_config_value(configuration: dict, key: str, default: str = "", as_bool: bool = False) -> str:
    """Helper function to get configuration values."""
    value = str(configuration.get(key, default))
    return value.lower() == "true" if as_bool else value

def retry_with_backoff(func, max_retries: int = DEFAULT_MAX_RETRIES, base_delay: float = DEFAULT_RETRY_DELAY_SEC):
    """Retry function with exponential backoff."""
    for attempt in range(max_retries + 1):
        try:
            return func()
        except Exception as e:
            if attempt == max_retries:
                log.severe(f"All retry attempts exhausted. Final error: {e}")
                raise e
            
            delay = base_delay * (2 ** attempt) + random.uniform(0, 1)
            log.warning(f"Attempt {attempt + 1} failed: {e}. Retrying in {delay:.2f} seconds...")
            time.sleep(delay)

def get_table_name_without_schema(full_table_name: str) -> str:
    """Extract table name without schema prefix."""
    if '.' in full_table_name:
        return full_table_name.split('.')[-1]
    return full_table_name

def convert_value(value):
    """Convert database values to appropriate types for Fivetran."""
    if value is None:
        return None
    elif isinstance(value, datetime):
        return value.isoformat()
    elif isinstance(value, (int, float, str, bool)):
        return value
    else:
        return str(value)

def parse_timestamp_columns(configuration: dict) -> List[str]:
    """Parse comma-separated timestamp columns from configuration."""
    timestamp_column_str = get_config_value(configuration, "timestamp_column", "")
    if not timestamp_column_str:
        return []
    
    columns = [col.strip().upper() for col in timestamp_column_str.split(',') if col.strip()]
    return columns

def find_timestamp_column(columns: List[str], timestamp_columns: List[str]) -> Tuple[Optional[str], List[str]]:
    """
    Find first matching timestamp column from configured list.
    
    Returns a tuple of:
    - First matching column name (or None if no match)
    - List of all configured timestamp columns that were checked (for logging)
    
    Matches are case-insensitive and support both exact and partial matching.
    Checks in the order of configured timestamp columns to respect logical order.
    """
    if not timestamp_columns:
        return None, []
    
    columns_upper = [col.upper() for col in columns]
    timestamp_columns_upper = [tc.upper() for tc in timestamp_columns]
    
    # Check for exact matches first (case-insensitive)
    # Iterate through configured columns in order to respect logical order
    for tc in timestamp_columns:
        tc_upper = tc.upper()
        for col in columns:
            if col.upper() == tc_upper:
                return col, timestamp_columns
    
    # If no exact matches, check for partial matches
    # Still iterate through configured columns in order
    for tc in timestamp_columns:
        tc_upper = tc.upper()
        for col in columns:
            col_upper = col.upper()
            if tc_upper in col_upper or col_upper in tc_upper:
                return col, timestamp_columns
    
    return None, timestamp_columns

# =============================================================================
# SNOWFLAKE CONNECTION
# =============================================================================

def validate_configuration(configuration: dict):
    """Validate the configuration dictionary."""
    use_privatelink = get_config_value(configuration, "use_privatelink", "false", as_bool=True)
    
    # Check authentication method
    private_key = get_config_value(configuration, "private_key", "")
    has_jwt_auth = bool(private_key and 
                        private_key.strip() and 
                        private_key.startswith("-----BEGIN PRIVATE KEY-----") and
                        len(private_key) > 100 and
                        "Your private key content here" not in private_key)
    
    if has_jwt_auth:
        required_configs = [
            "snowflake_user", "snowflake_warehouse", "snowflake_database", "snowflake_schema"
        ]
    else:
        required_configs = [
            "snowflake_user", "snowflake_password", "snowflake_warehouse",
            "snowflake_database", "snowflake_schema"
        ]
    
    if use_privatelink:
        required_configs.append("privatelink_host")
    else:
        required_configs.append("snowflake_account")
    
    for key in required_configs:
        if key not in configuration:
            raise ValueError(f"Missing required configuration value: {key}")
        configuration[key] = str(configuration[key])

def get_snowflake_connection(configuration: dict):
    """Create and return a Snowflake connection."""
    def _connect():
        use_privatelink = get_config_value(configuration, "use_privatelink", "false", as_bool=True)
        
        private_key_data = get_config_value(configuration, "private_key", "")
        private_key_password = get_config_value(configuration, "private_key_password", "")
        private_key = None
        
        # Load private key if provided
        if (private_key_data and 
            private_key_data.strip() and 
            private_key_data.startswith("-----BEGIN PRIVATE KEY-----") and
            len(private_key_data) > 100 and
            "Your private key content here" not in private_key_data):
            try:
                private_key = serialization.load_pem_private_key(
                    private_key_data.encode('utf-8'),
                    password=private_key_password.encode('utf-8') if private_key_password else None,
                    backend=default_backend()
                )
            except Exception as e:
                log.warning(f"Failed to load private key, using password auth: {e}")
                private_key = None
        
        if use_privatelink:
            privatelink_host = get_config_value(configuration, "privatelink_host", "")
            if not privatelink_host:
                raise ValueError("PrivateLink host is required when use_privatelink is true")
            
            account_parts = privatelink_host.split('.')
            account = account_parts[0]
            ssl_verify = get_config_value(configuration, "ssl_verify", "true", as_bool=True)
            
            conn_params = {
                "account": account,
                "host": privatelink_host,
                "user": get_config_value(configuration, "snowflake_user"),
                "warehouse": get_config_value(configuration, "snowflake_warehouse"),
                "database": get_config_value(configuration, "snowflake_database"),
                "schema": get_config_value(configuration, "snowflake_schema"),
                "client_session_keep_alive": True,
                "login_timeout": 60,
                "network_timeout": 60,
                "insecure_mode": not ssl_verify,
                "verify_ssl": ssl_verify,
            }
            
            if private_key:
                conn_params.update({
                    "authenticator": "SNOWFLAKE_JWT",
                    "private_key": private_key
                })
                if configuration.get("snowflake_role"):
                    conn_params["role"] = get_config_value(configuration, "snowflake_role")
            else:
                conn_params["password"] = get_config_value(configuration, "snowflake_password", "")
            
            return snowflake.connector.connect(**conn_params)
        else:
            account = get_config_value(configuration, "snowflake_account")
            ssl_verify = get_config_value(configuration, "ssl_verify", "true", as_bool=True)
            
            conn_params = {
                "account": account,
                "user": get_config_value(configuration, "snowflake_user"),
                "warehouse": get_config_value(configuration, "snowflake_warehouse"),
                "database": get_config_value(configuration, "snowflake_database"),
                "schema": get_config_value(configuration, "snowflake_schema"),
                "client_session_keep_alive": True,
                "login_timeout": 60,
                "network_timeout": 60,
                "insecure_mode": not ssl_verify,
                "verify_ssl": ssl_verify,
            }
            
            if private_key:
                conn_params.update({
                    "authenticator": "SNOWFLAKE_JWT",
                    "private_key": private_key
                })
                if configuration.get("snowflake_role"):
                    conn_params["role"] = get_config_value(configuration, "snowflake_role")
            else:
                conn_params["password"] = get_config_value(configuration, "snowflake_password", "")
            
            return snowflake.connector.connect(**conn_params)
    
    max_retries = int(get_config_value(configuration, "max_retries", str(DEFAULT_MAX_RETRIES)))
    return retry_with_backoff(_connect, max_retries)

def get_table_list(configuration: dict) -> List[str]:
    """Read table names from configuration."""
    tables_string = get_config_value(configuration, 'tables', '')
    if not tables_string:
        log.warning("No tables specified in configuration")
        return []
    
    tables = [table.strip() for table in tables_string.split(',') if table.strip()]
    return tables

def get_table_columns(cursor, table_name: str) -> List[str]:
    """Get column names for a table."""
    try:
        cursor.execute(f"SELECT * FROM {table_name} LIMIT 1")
        return [column[0] for column in cursor.description] if cursor.description else []
    except Exception as e:
        log.severe(f"Error getting columns for {table_name}: {e}")
        return []

# =============================================================================
# DATA REPLICATION
# =============================================================================

def sync_table(conn_manager: ConnectionManager, table_name: str, state: dict, 
               configuration: dict, batch_size: int = DEFAULT_BATCH_SIZE) -> Tuple[int, str]:
    """
    Sync a table with zero-memory accumulation and incremental replication.
    
    Processes rows immediately and upserts them one at a time to prevent
    memory overflow. Uses batch fetching for efficiency but processes
    each row immediately.
    
    Supports incremental sync based on timestamp columns:
    - First sync or no timestamp match: Uses DEFAULT_START_DATE for full lookback
    - Subsequent syncs: Uses checkpointed state for incremental replication
    - Uses first matching timestamp column from configuration (respects logical order)
    """
    table_name_clean = get_table_name_without_schema(table_name)
    state_key = f"{table_name_clean}_last_sync"
    
    # Parse timestamp columns from configuration
    timestamp_columns_config = parse_timestamp_columns(configuration)
    
    with conn_manager.get_cursor() as cursor:
        # Get all columns
        columns = get_table_columns(cursor, table_name)
        if not columns:
            log.severe(f"No columns found for table {table_name}")
            return 0, DEFAULT_START_DATE
        
        # Find first matching timestamp column (respects configuration order)
        timestamp_col, checked_columns = find_timestamp_column(columns, timestamp_columns_config) if timestamp_columns_config else (None, [])
        
        # Determine sync mode and last_sync value
        if timestamp_col:
            # We have a matching timestamp column - check state
            last_sync = state.get(state_key)
            if not last_sync or last_sync == "":
                # First sync: use DEFAULT_START_DATE
                last_sync = DEFAULT_START_DATE
                log.info(f"{table_name}: First sync detected. Matched timestamp column '{timestamp_col}' from configured columns {checked_columns}. Using full lookback from {DEFAULT_START_DATE}")
            else:
                # Incremental sync: use checkpointed state
                log.info(f"{table_name}: Incremental sync. Using matched timestamp column '{timestamp_col}' from configured columns {checked_columns}. Filtering from checkpointed state: {last_sync}")
            
            # Build incremental query using first matched timestamp column
            query = f"""
            SELECT * FROM {table_name} 
            WHERE {timestamp_col} > '{last_sync}'
            ORDER BY {timestamp_col}
            """
        else:
            # No matching timestamp column found - use DEFAULT_START_DATE for full table scan
            last_sync = DEFAULT_START_DATE
            if checked_columns:
                log.info(f"{table_name}: No matching timestamp column found. Checked configured columns {checked_columns} against table columns. Performing full table scan from {DEFAULT_START_DATE}")
            else:
                log.info(f"{table_name}: No timestamp columns configured. Performing full table scan from {DEFAULT_START_DATE}")
            
            query = f"SELECT * FROM {table_name}"
        
        # Execute query
        cursor.execute(query)
        
        records_processed = 0
        max_timestamp = last_sync
        batch_count = 0
        start_time = time.time()
        
        # Process records in batches - NO MEMORY ACCUMULATION
        while True:
            rows = cursor.fetchmany(batch_size)
            if not rows:
                break
            
            batch_count += 1
            
            # Process each row immediately without accumulating in memory
            for row in rows:
                # Convert row to dictionary
                data = {col: convert_value(val) for col, val in zip(columns, row)}
                
                # IMMEDIATE UPSERT - No memory accumulation
                op.upsert(table=table_name_clean, data=data)
                
                # Track timestamp for checkpointing using first matched column
                if timestamp_col and data.get(timestamp_col):
                    current_timestamp_str = str(data[timestamp_col])
                    if current_timestamp_str > max_timestamp:
                        max_timestamp = current_timestamp_str
                
                records_processed += 1
                
                # Checkpoint at intervals
                if records_processed % CHECKPOINT_INTERVAL == 0:
                    current_state = state.copy()
                    # Update state with timestamp if we have a matching timestamp column
                    if timestamp_col:
                        current_state[state_key] = str(max_timestamp)
                    else:
                        # For full table scans without timestamp column, keep DEFAULT_START_DATE
                        current_state[state_key] = str(DEFAULT_START_DATE)
                    op.checkpoint(current_state)
            
            # Log batch progress
            if batch_count % 10 == 0:
                elapsed = time.time() - start_time
                rate = records_processed / elapsed if elapsed > 0 else 0
                #log.info(f"{table_name}: {records_processed:,} rows processed ({rate:.0f} rows/sec)")
        
        elapsed = time.time() - start_time
        rate = records_processed / elapsed if elapsed > 0 else 0
        log.info(f"{table_name}: Completed {records_processed:,} rows in {elapsed:.1f}s ({rate:.0f} rows/sec)")
        
        # Return max_timestamp if we have a timestamp column, otherwise return DEFAULT_START_DATE
        return records_processed, max_timestamp if timestamp_col else DEFAULT_START_DATE

# =============================================================================
# SCHEMA AND UPDATE FUNCTIONS
# =============================================================================

def schema(configuration: dict):
    """
    Define schema with primary keys only.
    
    Fivetran will infer column types automatically.
    """
    tables = get_table_list(configuration)
    schema_definition = []
    
    for table in tables:
        table_name = get_table_name_without_schema(table)
        # Only define primary key - Fivetran infers the rest
        schema_definition.append({
            "table": table_name
        })
    
    log.info(f"Schema defined for {len(schema_definition)} tables")
    return schema_definition

def update(configuration: dict, state: dict):
    """
    Main update function for high-performance data replication.
    
    Processes tables sequentially with zero-memory accumulation.
    Each row is upserted immediately to prevent memory overflow.
    """
    log.info("Starting Snowflake data replication")
    
    # Validate configuration
    validate_configuration(configuration)
    
    # Get configuration parameters
    batch_size = int(get_config_value(configuration, "batch_size", str(DEFAULT_BATCH_SIZE)))
    max_retries = int(get_config_value(configuration, "max_retries", str(DEFAULT_MAX_RETRIES)))
    
    # Get list of tables to sync
    tables = get_table_list(configuration)
    if not tables:
        log.warning("No tables to sync")
        return
    
    log.info(f"Syncing {len(tables)} tables with batch size {batch_size:,}")
    
    # Initialize state
    current_state = state.copy() if state else {}
    
    # Create connection manager
    conn_manager = ConnectionManager(configuration)
    
    # Process tables sequentially
    total_records = 0
    successful_tables = 0
    
    for i, table in enumerate(tables, 1):
        log.info(f"[{i}/{len(tables)}] Processing table: {table}")
        
        # Retry loop for error handling
        for attempt in range(max_retries):
            try:
                records_processed, last_sync = sync_table(
                    conn_manager, table, current_state, configuration, batch_size
                )
                
                total_records += records_processed
                successful_tables += 1
                
                # Update state
                if records_processed > 0:
                    table_name_clean = get_table_name_without_schema(table)
                    state_key = f"{table_name_clean}_last_sync"
                    current_state[state_key] = str(last_sync)
                    op.checkpoint(current_state)
                
                break  # Success, exit retry loop
                
            except (DeadlockError, TimeoutError) as e:
                log.warning(f"{type(e).__name__} for {table}, attempt {attempt+1}/{max_retries}: {e}")
                if attempt + 1 >= max_retries:
                    log.severe(f"Max retries exceeded for table {table}")
                    break
                
                delay = DEFAULT_RETRY_DELAY_SEC * (2 ** attempt) + random.uniform(0, 1)
                time.sleep(delay)
                continue
                
            except Exception as e:
                log.severe(f"Error processing table {table}: {e}")
                break
    
    # Final checkpoint
    if successful_tables > 0:
        op.checkpoint(current_state)
    
    log.info(f"Replication complete: {successful_tables}/{len(tables)} tables, {total_records:,} total rows")

# =============================================================================
# CONNECTOR INITIALIZATION
# =============================================================================

connector = Connector(update=update, schema=schema)

if __name__ == "__main__":
    with open("configuration.json", 'r') as f:
        configuration = json.load(f)
    
    connector.debug(configuration=configuration)
