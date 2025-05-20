from fivetran_connector_sdk import Connector
from fivetran_connector_sdk import Operations as op
from fivetran_connector_sdk import Logging as log
import pyodbc
import time
import json

# Permissions used in this demo

# CREATE USER fivetran WITH PASSWORD 'your_password';
# GRANT CONNECT ON DATABASE sample_db TO fivetran;
# GRANT USAGE ON SCHEMA public TO fivetran;
# GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO fivetran;
# ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO fivetran;

def get_primary_keys(cursor, table_name, schema_name):
    """Get primary key columns for a table"""
    cursor.execute("""
        SELECT kcu.column_name
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu 
            ON tc.constraint_name = kcu.constraint_name
            AND tc.table_schema = kcu.table_schema
        WHERE tc.constraint_type = 'PRIMARY KEY'
            AND tc.table_name = ?
            AND tc.table_schema = ?
    """, (table_name, schema_name))

    pk_columns = [row[0] for row in cursor.fetchall()]
    return pk_columns if pk_columns else ["id"]  # Default to 'id' if no PK found


def get_table_columns(cursor, table_name, schema_name):
    """Get column names and types for a table"""
    cursor.execute("""
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_name = ?
        AND table_schema = ?
    """, (table_name, schema_name))
    return cursor.fetchall()


def get_table_comment(cursor, table_name, schema_name):
    """
    Fetches the comment for a given table in PostgreSQL using pyodbc.
    Args:
        cursor: pyodbc cursor object
        table_name (str): The name of the table
        schema_name (str): The schema of the table
    Returns:
        str: The comment on the table, if any.
    """
    query = """
        SELECT obj_description(('"' || ? || '"."' || ? || '"')::regclass, 'pg_class') AS table_comment;
    """
    cursor.execute(query, (schema_name, table_name))
    result = cursor.fetchone()
    return result[0] if result else None


def get_column_comment(cursor, table_name, schema_name, column_name):
    """
    Fetches the comment for a given column in PostgreSQL using pyodbc.
    Args:
        cursor: pyodbc cursor object
        table_name (str): The name of the table
        schema_name (str): The schema of the table
        column_name (str): The name of the column
    Returns:
        str: The comment on the column, if any.
    """
    query = """
        SELECT pgd.description
        FROM pg_catalog.pg_statio_all_tables as st
        INNER JOIN pg_catalog.pg_description pgd ON (pgd.objoid=st.relid)
        INNER JOIN information_schema.columns c ON (
            c.table_schema=st.schemaname AND c.table_name=st.relname
            AND ordinal_position=pgd.objsubid
        )
        WHERE c.table_name = ? AND c.table_schema = ? AND c.column_name = ?
    """
    cursor.execute(query, (table_name, schema_name, column_name))
    result = cursor.fetchone()
    return result[0] if result else None


def should_process_table(table_name, schema_name, configuration):
    """Determine if table should be processed based on configuration"""
    # Remove the exclusion of specific tables
    # excluded_tables = {'pg_stat_statements', 'pg_stat_statements_info'}

    # if table_name in excluded_tables:
    #     return False

    # Remove the filtering logic based on configuration
    # table_filters = configuration.get('table_filters', {})
    # table_key = f"{schema_name}.{table_name}"

    # if table_filters and table_key not in table_filters:
    #     return False

    return True  # Always return True to process all tables


def apply_transformations(row_dict,table_name,configuration):
    if table_name.lower()=='orders':
        if row_dict['region']=='fdx55':
            row_dict['region']=configuration.get('region')  #pull region value from config.json
        elif row_dict['region']=='weg45':
            row_dict['region']='Europe'  #hard code example
    return row_dict




def get_filtered_query(table_name, schema_name, column_names):
    """Get the appropriate query based on table"""
    base_query = f"SELECT {', '.join(column_names)} FROM {schema_name}.{table_name}"
    return base_query


def schema(configuration: dict):
    """Dynamically generate schema including metadata and source tables"""
    conn_str = (
        f"DRIVER=/opt/homebrew/lib/psqlodbcw.so;"
        f"SERVER={configuration['host']};"
        f"PORT={configuration['port']};"
        f"DATABASE={configuration['database']};"
        f"UID={configuration['username']};"
        f"PWD={configuration['password']};"
    )

    schema_entries = [
        {
            "table": "postgres_tables",
            "primary_key": ["table_name", "timestamp"]
        },
        {
            "table": "postgres_columns",
            "primary_key": ["table_name", "column_name", "timestamp"]
        }
    ]

    try:
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()

        # Get all user tables
        cursor.execute("""
            SELECT table_name, table_schema
            FROM information_schema.tables
            WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
            AND table_type = 'BASE TABLE'
        """)

        for table_name, schema_name in cursor.fetchall():
            if should_process_table(table_name, schema_name, configuration):
                primary_keys = get_primary_keys(cursor, table_name, schema_name)
                schema_entries.append({
                    "table": f"source_{schema_name}_{table_name}",
                    "primary_key": primary_keys,
                    "columns": {
                        "region": "string"  # Add region column to schema
                    }
                })

        print(schema_entries)
        return schema_entries

    except pyodbc.Error as e:
        log.info(f'Schema generation error: {str(e)}')
        raise e
    finally:
        if 'conn' in locals():
            conn.close()


def update(configuration: dict, state: dict):
    conn_str = (
        f"DRIVER=/opt/homebrew/lib/psqlodbcw.so;"
        f"SERVER={configuration['host']};"
        f"PORT={configuration['port']};"
        f"DATABASE={configuration['database']};"
        f"UID={configuration['username']};"
        f"PWD={configuration['password']};"
    )

    try:
        log.info('Connecting to PostgreSQL database...')
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()
        timestamp = time.time()

        # 1. Collect and upsert metadata about tables
        log.info('Collecting table metadata...')
        cursor.execute("""
                    SELECT 
                        table_name,
                        table_schema,
                        table_type
                    FROM information_schema.tables
                    WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
                    AND table_type = 'BASE TABLE'
                """)

        tables = cursor.fetchall()
        for table in tables:
            if should_process_table(table[0], table[1], configuration):
                # Fetch table comment
                comment = get_table_comment(cursor, table[0], table[1])
                yield op.upsert("postgres_tables", {
                    "table_name": table[0],
                    "schema_name": table[1],
                    "table_type": table[2],
                    "timestamp": timestamp,
                    "comment": comment
                })

        # 2. Collect and upsert metadata about columns
        log.info('Collecting column metadata...')
        cursor.execute("""
                    SELECT 
                        table_name,
                        column_name,
                        data_type,
                        column_default,
                        is_nullable,
                        table_schema
                    FROM information_schema.columns
                    WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
                """)

        for column in cursor.fetchall():
            if should_process_table(column[0], column[5], configuration):
                # Fetch column comment
                comment = get_column_comment(cursor, column[0], column[5], column[1])
                yield op.upsert("postgres_columns", {
                    "table_name": column[0],
                    "column_name": column[1],
                    "data_type": column[2],
                    "column_default": column[3],
                    "is_nullable": column[4],
                    "timestamp": timestamp,
                    "comment": comment
                })

        # 3. Collect and upsert data from each source table
        for table_name, schema_name, _ in tables:
            log.info(f'Processing table: {schema_name}.{table_name}')

            # Get column information for the table
            columns = get_table_columns(cursor, table_name, schema_name)
            column_names = [col[0] for col in columns]

            # Build and execute query without filters
            query = get_filtered_query(table_name, schema_name, column_names)
            cursor.execute(query)

            # Upsert each row with transformations
            while True:
                rows = cursor.fetchmany(1000)  # Process in batches of 1000
                if not rows:
                    break

                for row in rows:
                    # Create dictionary of column names and values
                    row_dict = {
                        col_name: value
                        for col_name, value in zip(column_names, row)
                    }
                    # Add timestamp to track sync
                    row_dict['sync_timestamp'] = timestamp

                    # Apply transformations
                    row_dict = apply_transformations(row_dict, table_name, configuration=configuration)

                    # Filter based on the transformed region value
                    if 'region' in row_dict and row_dict['region'] == configuration.get('region'):
                        # Upsert to destination table based on region value in config
                        yield op.upsert(
                            f"source_{schema_name}_{table_name}",
                            row_dict
                        )
                    elif table_name.lower() != 'orders':
                        yield op.upsert(
                            f"source_{schema_name}_{table_name}",
                            row_dict
                        )

        log.info('Data extraction completed successfully.')

    except pyodbc.Error as e:
        log.warning((f'Database error occurred: {str(e)}'))
        raise e
    finally:
        if 'conn' in locals():
            conn.close()
            log.info('Database connection closed.')

# Create connector instance
connector = Connector(update=update, schema=schema)

# Main execution
if __name__ == "__main__":
    with open("/config.json", 'r') as f:
        configuration = json.load(f)

    connector.debug(configuration=configuration)
