# Data Validation Connector SDK

A comprehensive Fivetran connector SDK solution for validating data between source and destination systems. This connector automatically fetches table schemas from Fivetran API, connects to source and destination databases, runs customizable validation queries, and creates detailed audit trails.

## Features

- **Automated Table Discovery**: Fetches enabled tables from Fivetran API
- **Multi-Database Support**: Supports PostgreSQL (source) and Snowflake (destination)
- **Customizable Validation Queries**: Configurable SQL validation queries per table
- **Comprehensive Audit Trail**: Detailed validation results with timestamps
- **Error Handling**: Robust error handling and logging
- **Incremental Validation**: Support for incremental validation modes
- **PrivateLink Support**: Full Snowflake PrivateLink support

## Architecture

```
Fivetran API → Table Discovery → Source DB → Validation → Destination DB → Audit Results
```

## Setup Instructions

### 1. Install Dependencies

```bash
pip install -r requirements.txt
```

### 2. Configure the Connector

**Important**: All configuration values must be strings. The connector will automatically parse them to appropriate types.

Edit `configuration.json` with your specific settings:

```json
{
  "fivetran_api": {
    "api_key": "your_fivetran_api_key",
    "api_secret": "your_fivetran_api_secret",
    "connection_id": "your_connection_id"
  },
  "source": {
    "type": "postgres",
    "host": "your_postgres_host",
    "port": "5432",
    "database": "your_database",
    "username": "your_username",
    "password": "your_password"
  },
  "destination": {
    "type": "snowflake",
    "snowflake_account": "your_snowflake_account",
    "snowflake_user": "your_snowflake_user",
    "snowflake_password": "your_snowflake_password",
    "snowflake_warehouse": "your_warehouse",
    "snowflake_database": "your_database",
    "snowflake_schema": "your_schema"
  }
}
```

### 3. Configure Validation Queries

#### Default Validation Queries

The connector uses default validation queries that can be customized:

```json
"validation_queries": {
  "source_validation": "SELECT COUNT(*) as row_count, COUNT(DISTINCT id) as distinct_ids FROM {table_name}",
  "destination_validation": "SELECT COUNT(*) as row_count, COUNT(DISTINCT id) as distinct_ids FROM {table_name}"
}
```

#### Custom Validation Queries

For specific tables, you can define custom validation queries:

```json
"custom_validations": {
  "orders": {
    "source_validation": "SELECT COUNT(*) as row_count, SUM(total_amount) as total_revenue FROM orders WHERE created_date >= CURRENT_DATE - INTERVAL '1 day'",
    "destination_validation": "SELECT COUNT(*) as row_count, SUM(total_amount) as total_revenue FROM orders WHERE created_date >= CURRENT_DATE - INTERVAL '1 day'"
  }
}
```

### 4. Run the Connector

#### Local Testing

```bash
python connector.py
```

#### Fivetran Deployment

Deploy the connector to Fivetran using the standard connector deployment process.

## Configuration Options

**Note**: All configuration values must be strings. The connector automatically parses them to appropriate types (integers, booleans, etc.).

### Fivetran API Configuration

| Parameter | Description | Required |
|-----------|-------------|----------|
| `api_key` | Fivetran API key | Yes |
| `api_secret` | Fivetran API secret | Yes |
| `connection_id` | Fivetran connection ID to validate | Yes |

### Source Database Configuration

| Parameter | Description | Required |
|-----------|-------------|----------|
| `type` | Database type (postgres) | Yes |
| `host` | Database host | Yes |
| `port` | Database port | Yes |
| `database` | Database name | Yes |
| `username` | Database username | Yes |
| `password` | Database password | Yes |

### Destination Database Configuration

| Parameter | Description | Required |
|-----------|-------------|----------|
| `type` | Database type (snowflake) | Yes |
| `snowflake_account` | Snowflake account identifier | Yes* |
| `snowflake_user` | Snowflake username | Yes |
| `snowflake_password` | Snowflake password | Yes |
| `snowflake_warehouse` | Snowflake warehouse | Yes |
| `snowflake_database` | Snowflake database | Yes |
| `snowflake_schema` | Snowflake schema | Yes |
| `use_privatelink` | Use PrivateLink connection | No |
| `privatelink_host` | PrivateLink host | Yes** |

*Required for regular Snowflake connections
**Required when use_privatelink=true

### Validation Configuration

| Parameter | Description | Default |
|-----------|-------------|---------|
| `batch_size` | Records per batch | 1000 |
| `sync_mode` | Sync mode (incremental/full) | incremental |
| `validation_queries` | Default validation queries | See config |
| `custom_validations` | Table-specific validations | {} |

### Audit Configuration

| Parameter | Description | Default |
|-----------|-------------|---------|
| `table_name` | Audit table name | data_validation_audit |
| `retention_days` | Audit retention period | 30 |

## Audit Table Schema

The connector creates an audit table with the following schema:

| Column | Type | Description |
|--------|------|-------------|
| `table_name` | String | Name of the validated table |
| `validation_timestamp` | Timestamp | When validation was performed |
| `source_row_count` | Integer | Row count from source database |
| `destination_row_count` | Integer | Row count from destination database |
| `validation_status` | String | PASSED/FAILED/ERROR/SKIPPED |
| `error_message` | String | Error details if validation failed |
| `source_validation_result` | JSON | Full source validation results |
| `destination_validation_result` | JSON | Full destination validation results |

## Validation Statuses

- **PASSED**: Source and destination row counts match
- **FAILED**: Row count mismatch detected
- **ERROR**: Validation process encountered an error
- **SKIPPED**: No validation queries configured for table

## Example Usage

### Basic Row Count Validation

```json
"validation_queries": {
  "source_validation": "SELECT COUNT(*) as row_count FROM {table_name}",
  "destination_validation": "SELECT COUNT(*) as row_count FROM {table_name}"
}
```

### Advanced Validation with Business Logic

```json
"custom_validations": {
  "orders": {
    "source_validation": "SELECT COUNT(*) as row_count, SUM(total_amount) as revenue, AVG(total_amount) as avg_order FROM orders WHERE status = 'completed'",
    "destination_validation": "SELECT COUNT(*) as row_count, SUM(total_amount) as revenue, AVG(total_amount) as avg_order FROM orders WHERE status = 'completed'"
  }
}
```

### Data Quality Validation

```json
"custom_validations": {
  "customers": {
    "source_validation": "SELECT COUNT(*) as total, COUNT(DISTINCT email) as unique_emails, COUNT(CASE WHEN email IS NOT NULL THEN 1 END) as non_null_emails FROM customers",
    "destination_validation": "SELECT COUNT(*) as total, COUNT(DISTINCT email) as unique_emails, COUNT(CASE WHEN email IS NOT NULL THEN 1 END) as non_null_emails FROM customers"
  }
}
```

## Error Handling

The connector includes comprehensive error handling:

- **API Errors**: Logs and continues with next table
- **Database Connection Errors**: Detailed error messages and retry logic
- **Query Execution Errors**: Graceful handling with error records
- **Validation Errors**: Creates audit records with error details

## Logging

The connector provides detailed logging at multiple levels:

- **INFO**: General process information
- **WARNING**: Non-critical issues
- **SEVERE**: Critical errors that may affect validation

## Best Practices

1. **Use Specific Validation Queries**: Create table-specific validation queries for better accuracy
2. **Monitor Audit Table**: Regularly check the audit table for validation failures
3. **Set Appropriate Batch Sizes**: Adjust batch size based on table sizes and performance requirements
4. **Use Incremental Validation**: Configure incremental validation for large tables
5. **Secure Credentials**: Store sensitive credentials securely and use environment variables when possible

## Troubleshooting

### Common Issues

1. **API Authentication Errors**: Verify API key and secret are correct
2. **Database Connection Failures**: Check network connectivity and credentials
3. **Query Execution Errors**: Verify SQL syntax and table permissions
4. **Validation Failures**: Review custom validation queries for correctness

### Debug Mode

Run the connector in debug mode for detailed logging:

```bash
python connector.py
```

## Support

For issues and questions:

1. Check the logs for detailed error messages
2. Verify configuration parameters
3. Test database connections independently
4. Review validation query syntax

## License

This connector is provided as-is for data validation purposes. 
