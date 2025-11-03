"""
Data Validation Connector for Fivetran

This connector validates data between source and destination systems by:
1. Fetching table schemas from Fivetran API
2. Connecting to source database (PostgreSQL)
3. Connecting to destination database (Snowflake)
4. Running validation queries on both systems
5. Comparing results and creating audit records
6. Upserting validation results to Fivetran

The connector supports customizable validation queries and provides detailed audit trails.
"""

from fivetran_connector_sdk import Connector
from fivetran_connector_sdk import Operations as op
from fivetran_connector_sdk import Logging as log
import requests
from requests.auth import HTTPBasicAuth
import json
import pyodbc
import snowflake.connector
import psycopg2
from datetime import datetime, timezone
import time
from typing import Dict, List, Any, Optional
import hashlib


def ensure_string_configuration(configuration: dict) -> dict:
    """Ensure all configuration values are strings for Fivetran SDK compatibility"""
    def convert_to_strings(obj):
        if isinstance(obj, dict):
            return {key: convert_to_strings(value) for key, value in obj.items()}
        elif isinstance(obj, list):
            return [convert_to_strings(item) for item in obj]
        else:
            return str(obj)
    
    return convert_to_strings(configuration)


def convert_value_for_fivetran(value):
    """Convert database values to Fivetran-compatible format"""
    if value is None:
        return None
    elif hasattr(value, 'isoformat'):
        # Convert datetime objects to ISO format
        return value.isoformat()
    elif isinstance(value, (list, tuple)):
        # Convert lists/tuples to JSON strings for Fivetran compatibility
        return json.dumps(value)
    elif isinstance(value, dict):
        # Convert dictionaries to JSON strings for Fivetran compatibility
        return json.dumps(value)
    elif isinstance(value, (int, float, str, bool)):
        # These types are already Fivetran-compatible
        return value
    else:
        # Convert any other type to string
        return str(value)


def ensure_fivetran_compatible_dict(data_dict: dict) -> dict:
    """Ensure all values in a dictionary are Fivetran-compatible"""
    cleaned_dict = {}
    for key, value in data_dict.items():
        cleaned_dict[key] = convert_value_for_fivetran(value)
    return cleaned_dict


def unflatten_configuration(flat_config: dict) -> dict:
    """Convert flattened configuration (dot notation) back to nested structure"""
    nested_config = {}
    
    for key, value in flat_config.items():
        keys = key.split('.')
        current = nested_config
        
        for i, k in enumerate(keys[:-1]):
            if k not in current:
                current[k] = {}
            current = current[k]
        
        current[keys[-1]] = value
    
    return nested_config


def parse_configuration(configuration: dict) -> dict:
    """Parse configuration values from strings to appropriate types"""
    # First unflatten the configuration if it's in flat format
    if any('.' in key for key in configuration.keys()):
        configuration = unflatten_configuration(configuration)
    
    parsed_config = configuration.copy()
    
    # Parse source configuration
    if 'source' in parsed_config:
        source_config = parsed_config['source']
        if 'port' in source_config:
            source_config['port'] = int(source_config['port'])
    
    # Parse destination configuration
    if 'destination' in parsed_config:
        dest_config = parsed_config['destination']
        if 'use_privatelink' in dest_config:
            dest_config['use_privatelink'] = dest_config['use_privatelink'].lower() == 'true'
    
    # Parse validation configuration
    if 'validation' in parsed_config:
        validation_config = parsed_config['validation']
        if 'batch_size' in validation_config:
            validation_config['batch_size'] = int(validation_config['batch_size'])
    
    # Parse audit configuration
    if 'audit' in parsed_config:
        audit_config = parsed_config['audit']
        if 'retention_days' in audit_config:
            audit_config['retention_days'] = int(audit_config['retention_days'])
    
    return parsed_config


class FivetranAPIClient:
    """Client for interacting with Fivetran API"""
    
    def __init__(self, api_key: str, api_secret: str):
        self.api_key = api_key
        self.api_secret = api_secret
        self.base_url = 'https://api.fivetran.com/v1'
        self.auth = HTTPBasicAuth(api_key, api_secret)
    
    def get_connection_schemas(self, connection_id: str) -> Dict[str, Any]:
        """Get schemas and tables for a Fivetran connection"""
        endpoint = f'connections/{connection_id}/schemas'
        headers = {
            'Authorization': f'Bearer {self.api_key}:{self.api_secret}'
        }
        
        log.info(f"🔍 Fetching connection schemas from Fivetran API for connection ID: {connection_id}")
        log.info(f"📡 API Endpoint: {self.base_url}/{endpoint}")
        
        try:
            response = requests.get(
                f'{self.base_url}/{endpoint}',
                headers=headers,
                auth=self.auth
            )
            response.raise_for_status()
            
            response_data = response.json()
            log.info(f"✅ Fivetran API response received successfully")
            log.info(f"📊 Response status code: {response.status_code}")
            log.info(f"📋 Response data structure: {list(response_data.keys()) if isinstance(response_data, dict) else 'Not a dict'}")
            
            if 'data' in response_data and 'schemas' in response_data['data']:
                schema_count = len(response_data['data']['schemas'])
                log.info(f"🏗️  Found {schema_count} schemas in connection")
                
                for schema_name, schema_info in response_data['data']['schemas'].items():
                    enabled_tables = [name for name, info in schema_info.get('tables', {}).items() if info.get('enabled', False)]
                    log.info(f"   📋 Schema '{schema_name}': {len(enabled_tables)} enabled tables")
            
            return response_data
        except requests.exceptions.RequestException as e:
            log.severe(f'❌ Fivetran API request failed: {e}')
            raise RuntimeError(f'Failed to fetch connection schemas: {e}')
    
    def get_enabled_tables(self, connection_id: str) -> List[str]:
        """Get list of enabled tables from Fivetran connection"""
        log.info(f"🔍 Extracting enabled tables from connection schemas...")
        schemas_data = self.get_connection_schemas(connection_id)
        enabled_tables = []
        
        log.info(f"📋 Processing schemas to find enabled tables...")
        for schema_name, schema_info in schemas_data.get('data', {}).get('schemas', {}).items():
            if schema_info.get('enabled', False):
                log.info(f"   ✅ Schema '{schema_name}' is enabled")
                for table_name, table_info in schema_info.get('tables', {}).items():
                    if table_info.get('enabled', False):
                        # Format: schema_name.table_name
                        full_table_name = f"{schema_name}.{table_name}"
                        enabled_tables.append(full_table_name)
                        log.info(f"      ✅ Table '{full_table_name}' is enabled")
                    else:
                        log.info(f"      ❌ Table '{schema_name}.{table_name}' is disabled")
            else:
                log.info(f"   ❌ Schema '{schema_name}' is disabled")
        
        log.info(f"📊 Summary: Found {len(enabled_tables)} enabled tables")
        log.info(f"📋 Enabled tables: {enabled_tables}")
        return enabled_tables


class DatabaseConnector:
    """Base class for database connections"""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.connection = None
    
    def connect(self):
        """Establish database connection"""
        raise NotImplementedError
    
    def disconnect(self):
        """Close database connection"""
        if self.connection:
            self.connection.close()
            self.connection = None
    
    def execute_query(self, query: str, params: Optional[tuple] = None) -> List[Dict[str, Any]]:
        """Execute a query and return results as list of dictionaries"""
        raise NotImplementedError
    
    def get_table_count(self, table_name: str) -> int:
        """Get row count for a table"""
        query = f"SELECT COUNT(*) as count FROM {table_name}"
        result = self.execute_query(query)
        return result[0]['count'] if result else 0


class PostgresConnector(DatabaseConnector):
    """PostgreSQL database connector"""
    
    def connect(self):
        """Connect to PostgreSQL database"""
        log.info(f"🔌 Connecting to PostgreSQL database...")
        log.info(f"   📍 Host: {self.config['host']}")
        log.info(f"   🚪 Port: {self.config['port']}")
        log.info(f"   🗄️  Database: {self.config['database']}")
        log.info(f"   👤 User: {self.config['username']}")
        
        try:
            self.connection = psycopg2.connect(
                host=self.config['host'],
                port=self.config['port'],
                database=self.config['database'],
                user=self.config['username'],
                password=self.config['password']
            )
            log.info("✅ Successfully connected to PostgreSQL database")
        except Exception as e:
            log.severe(f"❌ Failed to connect to PostgreSQL: {e}")
            raise RuntimeError(f"PostgreSQL connection failed: {e}")
    
    def execute_query(self, query: str, params: Optional[tuple] = None) -> List[Dict[str, Any]]:
        """Execute PostgreSQL query"""
        if not self.connection:
            self.connect()
        
        log.info(f"🔍 Executing PostgreSQL query...")
        log.info(f"   📝 Query: {query}")
        if params:
            log.info(f"   📋 Parameters: {params}")
        
        try:
            cursor = self.connection.cursor()
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            
            # Get column names
            columns = [desc[0] for desc in cursor.description]
            log.info(f"   📊 Query returned {len(columns)} columns: {columns}")
            
            # Fetch results
            rows = cursor.fetchall()
            log.info(f"   📈 Query returned {len(rows)} rows")
            
            results = []
            for i, row in enumerate(rows):
                row_dict = {}
                for j, value in enumerate(row):
                    # Convert values to Fivetran-compatible format
                    converted_value = convert_value_for_fivetran(value)
                    row_dict[columns[j]] = converted_value
                results.append(row_dict)
                
                # Log first few rows for debugging
                if i < 3:  # Log first 3 rows
                    log.info(f"      📋 Row {i+1}: {row_dict}")
                elif i == 3:
                    log.info(f"      ... (showing first 3 rows only)")
            
            cursor.close()
            log.info(f"✅ PostgreSQL query executed successfully")
            return results
            
        except Exception as e:
            log.severe(f"❌ PostgreSQL query execution failed: {e}")
            raise RuntimeError(f"Query execution failed: {e}")


class SnowflakeConnector(DatabaseConnector):
    """Snowflake database connector"""
    
    def connect(self):
        """Connect to Snowflake database"""
        log.info(f"🔌 Connecting to Snowflake database...")
        
        try:
            use_privatelink = self.config.get('use_privatelink', False)
            
            if use_privatelink:
                privatelink_host = self.config['privatelink_host']
                account = privatelink_host.split('.')[0]  # Extract account from host
                
                log.info(f"   🔗 Using PrivateLink connection")
                log.info(f"   📍 PrivateLink Host: {privatelink_host}")
                log.info(f"   🏢 Account: {account}")
                log.info(f"   👤 User: {self.config['snowflake_user']}")
                log.info(f"   🏭 Warehouse: {self.config['snowflake_warehouse']}")
                log.info(f"   🗄️  Database: {self.config['snowflake_database']}")
                log.info(f"   📋 Schema: {self.config['snowflake_schema']}")
                
                self.connection = snowflake.connector.connect(
                    account=account,
                    host=privatelink_host,
                    user=self.config['snowflake_user'],
                    password=self.config['snowflake_password'],
                    warehouse=self.config['snowflake_warehouse'],
                    database=self.config['snowflake_database'],
                    schema=self.config['snowflake_schema']
                )
            else:
                log.info(f"   🌐 Using standard Snowflake connection")
                log.info(f"   🏢 Account: {self.config['snowflake_account']}")
                log.info(f"   👤 User: {self.config['snowflake_user']}")
                log.info(f"   🏭 Warehouse: {self.config['snowflake_warehouse']}")
                log.info(f"   🗄️  Database: {self.config['snowflake_database']}")
                log.info(f"   📋 Schema: {self.config['snowflake_schema']}")
                
                self.connection = snowflake.connector.connect(
                    account=self.config['snowflake_account'],
                    user=self.config['snowflake_user'],
                    password=self.config['snowflake_password'],
                    warehouse=self.config['snowflake_warehouse'],
                    database=self.config['snowflake_database'],
                    schema=self.config['snowflake_schema']
                )
            
            log.info("✅ Successfully connected to Snowflake database")
            
        except Exception as e:
            log.severe(f"❌ Failed to connect to Snowflake: {e}")
            raise RuntimeError(f"Snowflake connection failed: {e}")
    
    def execute_query(self, query: str, params: Optional[tuple] = None) -> List[Dict[str, Any]]:
        """Execute Snowflake query"""
        if not self.connection:
            self.connect()
        
        log.info(f"🔍 Executing Snowflake query...")
        log.info(f"   📝 Query: {query}")
        if params:
            log.info(f"   📋 Parameters: {params}")
        
        try:
            cursor = self.connection.cursor()
            cursor.execute(query)
            
            # Get column names
            columns = [desc[0] for desc in cursor.description]
            log.info(f"   📊 Query returned {len(columns)} columns: {columns}")
            
            # Fetch results
            rows = cursor.fetchall()
            log.info(f"   📈 Query returned {len(rows)} rows")
            
            results = []
            for i, row in enumerate(rows):
                row_dict = {}
                for j, value in enumerate(row):
                    # Convert values to Fivetran-compatible format
                    converted_value = convert_value_for_fivetran(value)
                    row_dict[columns[j]] = converted_value
                results.append(row_dict)
                
                # Log first few rows for debugging
                if i < 3:  # Log first 3 rows
                    log.info(f"      📋 Row {i+1}: {row_dict}")
                elif i == 3:
                    log.info(f"      ... (showing first 3 rows only)")
            
            cursor.close()
            log.info(f"✅ Snowflake query executed successfully")
            return results
            
        except Exception as e:
            log.severe(f"❌ Snowflake query execution failed: {e}")
            raise RuntimeError(f"Query execution failed: {e}")


class DataValidator:
    """Data validation engine"""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.validation_config = config.get('validation', {})
        self.audit_config = config.get('audit', {})
    
    def get_validation_query(self, table_name: str, query_type: str, db_type: str = None) -> str:
        """Get validation query for a table and query type"""
        log.info(f"🔍 Generating validation query for table '{table_name}' (type: {query_type}, db: {db_type})")
        
        # Check for custom validation first
        custom_validations = self.validation_config.get('custom_validations', {})
        if table_name in custom_validations:
            log.info(f"   📋 Using custom validation for table '{table_name}'")
            custom_query = custom_validations[table_name].get(query_type, "")
            log.info(f"   📝 Custom query: {custom_query}")
            return custom_query
        
        # Use default validation query
        default_queries = self.validation_config.get('validation_queries', {})
        query_template = default_queries.get(query_type, "")
        log.info(f"   📋 Using default validation query template: {query_template}")
        
        # Handle table names based on database type
        if db_type == 'snowflake' and '.' in table_name:
            log.info(f"   ❄️  Processing Snowflake table name: {table_name}")
            # For Snowflake, use the full table path (database.schema.table)
            # table_name format: database.schema.table
            table_parts = table_name.split('.')
            log.info(f"   📋 Table parts: {table_parts}")
            
            if len(table_parts) >= 3:
                # Use the full table name as is for Snowflake
                final_query = query_template.format(table_name=table_name)
                log.info(f"   ✅ Snowflake full path query: {final_query}")
                return final_query
            else:
                # Fallback to just the table name
                final_query = query_template.format(table_name=table_parts[-1])
                log.info(f"   ⚠️  Snowflake fallback query: {final_query}")
                return final_query
                
        elif db_type == 'postgres' and '.' in table_name:
            log.info(f"   🐘 Processing PostgreSQL table name: {table_name}")
            # For PostgreSQL, use the full schema.table format since no schema is set in connection
            # table_name format: schema.table or database.schema.table
            table_parts = table_name.split('.')
            log.info(f"   📋 Table parts: {table_parts}")
            
            if len(table_parts) >= 2:
                # Handle both schema.table and database.schema.table formats
                if len(table_parts) >= 3:
                    # database.schema.table format - use schema.table
                    schema_name = 'public'
                    table_name_only = table_parts[-1]
                    log.info(f"   📋 PostgreSQL: database.schema.table format detected")
                    log.info(f"   📋 Using schema '{schema_name}' and table '{table_name_only}'")
                else:
                    # schema.table format
                    schema_name = 'public'
                    table_name_only = table_parts[1]
                    log.info(f"   📋 PostgreSQL: schema.table format detected")
                    log.info(f"   📋 Using schema '{schema_name}' and table '{table_name_only}'")
                
                # Quote schema and table names separately to avoid issues with special characters
                schema_table = f'"{schema_name}"."{table_name_only}"'
                final_query = query_template.format(table_name=schema_table)
                log.info(f"   ✅ PostgreSQL query: {final_query}")
                return final_query
            else:
                # Fallback to original table name
                final_query = query_template.format(table_name=table_name)
                log.info(f"   ⚠️  PostgreSQL fallback query: {final_query}")
                return final_query
        else:
            # For other databases or simple table names, use the table name as provided
            final_query = query_template.format(table_name=table_name)
            log.info(f"   ✅ Standard query for {db_type} table {table_name}: {final_query}")
            return final_query
    
    def validate_table(self, source_conn: DatabaseConnector, dest_conn: DatabaseConnector, 
                      table_name: str, source_db_type: str = None, dest_db_type: str = None) -> Dict[str, Any]:
        """Validate a single table between source and destination"""
        timestamp = datetime.now(timezone.utc).isoformat()
        
        log.info(f"🔍 Starting validation for table: {table_name}")
        log.info(f"   📅 Validation timestamp: {timestamp}")
        log.info(f"   🗄️  Source database type: {source_db_type}")
        log.info(f"   🗄️  Destination database type: {dest_db_type}")
        
        try:
            # Get validation queries
            log.info(f"📝 Generating validation queries...")
            source_query = self.get_validation_query(table_name, 'source_validation', source_db_type)
            dest_query = self.get_validation_query(table_name, 'destination_validation', dest_db_type)
            
            if not source_query or not dest_query:
                log.warning(f"⚠️  No validation queries found for table {table_name}")
                skipped_result = {
                    'table_name': table_name,
                    'validation_timestamp': timestamp,
                    'source_row_count': 0,
                    'destination_row_count': 0,
                    'validation_status': 'SKIPPED',
                    'error_message': 'No validation queries configured'
                }
                log.info(f"⏭️  Skipping validation for table {table_name}")
                return ensure_fivetran_compatible_dict(skipped_result)
            
            # Execute validation queries
            log.info(f"🚀 Executing validation queries...")
            log.info(f"   📊 Executing source validation query...")
            source_result = source_conn.execute_query(source_query)
            log.info(f"   📊 Source query result: {source_result}")
            
            log.info(f"   📊 Executing destination validation query...")
            dest_result = dest_conn.execute_query(dest_query)
            log.info(f"   📊 Destination query result: {dest_result}")
            
            # Extract row counts and ensure they are integers
            # Handle case-insensitive column names
            def get_row_count(result_row, possible_names=['row_count', 'ROW_COUNT', 'Row_Count']):
                log.info(f"   🔍 Looking for row count in columns: {list(result_row.keys())}")
                for name in possible_names:
                    if name in result_row:
                        value = int(result_row[name])
                        log.info(f"   ✅ Found row count in column '{name}': {value}")
                        return value
                log.warning(f"   ⚠️  No row count column found. Available columns: {list(result_row.keys())}")
                return 0
            
            source_row_count = get_row_count(source_result[0]) if source_result else 0
            dest_row_count = get_row_count(dest_result[0]) if dest_result else 0
            
            log.info(f"📊 Row count comparison:")
            log.info(f"   📈 Source row count: {source_row_count}")
            log.info(f"   📈 Destination row count: {dest_row_count}")
            
            # Determine validation status
            if source_row_count == dest_row_count:
                validation_status = 'PASSED'
                error_message = None
                log.info(f"✅ Validation PASSED: Row counts match ({source_row_count} = {dest_row_count})")
            else:
                validation_status = 'FAILED'
                error_message = f"Row count mismatch: source={source_row_count}, destination={dest_row_count}"
                log.warning(f"❌ Validation FAILED: Row count mismatch ({source_row_count} ≠ {dest_row_count})")
            
            validation_result = {
                'table_name': table_name,
                'validation_timestamp': timestamp,
                'source_row_count': source_row_count,
                'destination_row_count': dest_row_count,
                'validation_status': validation_status,
                'error_message': error_message
            }
            
            log.info(f"📋 Final validation result: {validation_result}")
            
            # Ensure all values are Fivetran-compatible
            return ensure_fivetran_compatible_dict(validation_result)
            
        except Exception as e:
            log.severe(f"❌ Validation failed for table {table_name}: {e}")
            error_result = {
                'table_name': table_name,
                'validation_timestamp': timestamp,
                'source_row_count': 0,
                'destination_row_count': 0,
                'validation_status': 'ERROR',
                'error_message': str(e)
            }
            log.info(f"📋 Error validation result: {error_result}")
            return ensure_fivetran_compatible_dict(error_result)


def get_database_connector(config: Dict[str, Any], db_type: str) -> DatabaseConnector:
    """Factory function to create appropriate database connector"""
    if db_type == 'postgres':
        return PostgresConnector(config)
    elif db_type == 'snowflake':
        return SnowflakeConnector(config)
    else:
        raise ValueError(f"Unsupported database type: {db_type}")


def schema(configuration: dict):
    """Define the schema for the validation audit table"""
    # Ensure configuration is all strings for SDK compatibility
    string_config = ensure_string_configuration(configuration)
    
    # Handle both flat and nested configuration formats
    if any('.' in key for key in string_config.keys()):
        # Flat configuration - get audit table name directly
        audit_table = string_config.get('audit.table_name', 'data_validation_audit')
    else:
        # Nested configuration - unflatten first
        nested_config = unflatten_configuration(string_config)
        audit_table = nested_config.get('audit', {}).get('table_name', 'data_validation_audit')
    
    return [
        {
            "table": audit_table,
            "primary_key": ["table_name", "validation_timestamp"]
        }
    ]


def update(configuration: dict, state: dict):
    """Main update function that orchestrates the validation process"""
    log.info("🚀 Starting data validation process")
    log.info("=" * 60)
    
    try:
        # Ensure configuration is all strings for SDK compatibility, then parse for internal use
        log.info("📋 Processing configuration...")
        string_config = ensure_string_configuration(configuration)
        parsed_config = parse_configuration(string_config)
        log.info("✅ Configuration processed successfully")
        
        # Initialize API client
        log.info("🔌 Initializing Fivetran API client...")
        api_config = parsed_config.get('fivetran_api', {})
        api_client = FivetranAPIClient(
            api_key=api_config['api_key'],
            api_secret=api_config['api_secret']
        )
        log.info("✅ Fivetran API client initialized")
        
        # Get enabled tables from Fivetran
        log.info("📊 Fetching enabled tables from Fivetran...")
        connection_id = api_config['connection_id']
        log.info(f"🔗 Connection ID: {connection_id}")
        enabled_tables = api_client.get_enabled_tables(connection_id)
        
        if not enabled_tables:
            log.warning("⚠️  No enabled tables found in Fivetran connection")
            log.info("🏁 Ending validation process - no tables to validate")
            return
        
        log.info(f"📋 Found {len(enabled_tables)} tables to validate")
        
        # Initialize database connectors
        log.info("🔌 Initializing database connections...")
        source_config = parsed_config.get('source', {})
        dest_config = parsed_config.get('destination', {})
        
        log.info(f"🗄️  Source database type: {source_config.get('type', 'unknown')}")
        log.info(f"🗄️  Destination database type: {dest_config.get('type', 'unknown')}")
        
        source_conn = get_database_connector(source_config, source_config['type'])
        dest_conn = get_database_connector(dest_config, dest_config['type'])
        log.info("✅ Database connectors initialized")
        
        # Initialize validator
        log.info("🔍 Initializing data validator...")
        validator = DataValidator(parsed_config)
        log.info("✅ Data validator initialized")
        
        # Validate each table
        log.info("=" * 60)
        log.info("🔍 Starting table validation process...")
        
        for i, table_name in enumerate(enabled_tables, 1):
            try:
                log.info(f"📋 Processing table {i}/{len(enabled_tables)}: {table_name}")
                log.info("-" * 40)
                
                # Pass the full table name to the validator - it will handle the appropriate formatting
                # based on the database type (schema.table for PostgreSQL, full path for Snowflake)
                validation_table_name = table_name
                log.info(f"🔍 Using table name for validation: {validation_table_name}")
                
                # Perform validation
                validation_result = validator.validate_table(
                    source_conn, dest_conn, validation_table_name,
                    source_db_type=source_config['type'],
                    dest_db_type=dest_config['type']
                )
                
                # Yield upsert operation for audit table
                audit_table = parsed_config.get('audit', {}).get('table_name', 'data_validation_audit')
                log.info(f"💾 Upserting validation result to audit table: {audit_table}")
                yield op.upsert(audit_table, validation_result)
                
                log.info(f"✅ Validation completed for {table_name}: {validation_result['validation_status']}")
                log.info("-" * 40)
                
            except Exception as e:
                log.severe(f"❌ Failed to validate table {table_name}: {e}")
                # Create error record
                error_result = {
                    'table_name': table_name,
                    'validation_timestamp': datetime.now(timezone.utc).isoformat(),
                    'source_row_count': 0,
                    'destination_row_count': 0,
                    'validation_status': 'ERROR',
                    'error_message': str(e)
                }
                audit_table = parsed_config.get('audit', {}).get('table_name', 'data_validation_audit')
                log.info(f"💾 Upserting error result to audit table: {audit_table}")
                yield op.upsert(audit_table, ensure_fivetran_compatible_dict(error_result))
        
        # Cleanup connections
        log.info("🔌 Closing database connections...")
        source_conn.disconnect()
        dest_conn.disconnect()
        log.info("✅ Database connections closed")
        
        log.info("=" * 60)
        log.info("🎉 Data validation process completed successfully")
        
    except Exception as e:
        log.severe(f"❌ Data validation process failed: {e}")
        raise e


# Create connector instance
connector = Connector(update=update, schema=schema)


# Main execution for local testing
if __name__ == "__main__":
    with open("/configuration.json", 'r') as f:
        configuration = json.load(f)
    
    # Configuration is already in the correct format (all strings, flat structure)
    connector.debug(configuration=configuration)
