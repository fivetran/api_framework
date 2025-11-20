# SOC2 Compliance Connector for Fivetran

This connector demonstrates how to fetch SOC2 compliance data from Fivetran API endpoints and create audit trails for access control monitoring and compliance reporting.

## Overview

The SOC2 Compliance Connector is designed to help organizations maintain compliance with SOC2 requirements by:

- Fetching comprehensive user access data from Fivetran API endpoints
- Creating detailed audit trails of all API requests and responses
- Generating SOC2-compliant access control reports
- Providing clear visibility into who has access to what resources

## Features

- **Complete API Coverage**: Fetches data from all specified Fivetran API endpoints
- **Audit Trail**: Creates detailed logs of all API requests and responses
- **SOC2 Compliance**: Generates formatted access control records for compliance reporting
- **Incremental Sync**: Supports checkpointing for efficient incremental data synchronization
- **Error Handling**: Comprehensive error handling and logging for production reliability
- **Rate Limiting**: Built-in request timeout and retry mechanisms
- **Debug Mode**: Optional debug mode that limits processing to first 5 users for testing

## API Endpoints Covered

The connector fetches data from the following Fivetran API endpoints:

- `GET /v1/teams` - All teams
- `GET /v1/teams/{teamId}` - Team details
- `GET /v1/teams/{teamId}/groups` - Team groups
- `GET /v1/teams/{teamId}/users` - Team users
- `GET /v1/teams/{teamId}/groups/{groupId}` - Specific team group
- `GET /v1/teams/{teamId}/users/{userId}` - Specific team user
- `GET /v1/roles` - All roles
- `GET /v1/users` - All users
- `GET /v1/users/{userId}` - User details
- `GET /v1/users/{userId}/connections` - User connections
- `GET /v1/users/{userId}/groups` - User groups
- `GET /v1/users/{userId}/connections/{connectionId}` - Specific user connection
- `GET /v1/users/{userId}/groups/{groupId}` - Specific user group

## Data Tables

### 1. api_logs
Contains detailed audit trail of all API requests and responses:
- `log_id`: Unique identifier for each API call
- `timestamp`: When the API call was made
- `endpoint`: The API endpoint that was called
- `method`: HTTP method used (GET, POST, PATCH, DELETE)
- `request_data`: Request payload (if any)
- `response_data`: Response data from the API
- `status`: Request status (SUCCESS, FAILED)
- `record_count`: Number of records returned

### 2. soc2_access_control
Contains SOC2-compliant access control records:
- `access_record_id`: Unique identifier for each access record
- `user_id`: User identifier
- `user_email`: User email address
- `user_name`: Full name of the user
- `team_id`: Team identifier
- `team_name`: Team name
- `group_id`: Group identifier
- `group_name`: Group name
- `role_id`: Role identifier
- `role_name`: Role name
- `connection_id`: Connection identifier
- `connection_name`: Connection name
- `access_level`: Access level/permission level
- `permissions`: List of specific permissions
- `last_accessed`: Last access timestamp
- `compliance_status`: Compliance status (ACTIVE, INACTIVE, etc.)
- `audit_timestamp`: Audit timestamp for compliance tracking

## Setup Instructions

### 1. Prerequisites

- Python 3.9 or higher
- Fivetran API credentials (API Key and API Secret)
- Access to Fivetran API endpoints

### 2. Installation

1. Clone or download the connector files
2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

### 3. Configuration

1. Update `configuration.json` with your Fivetran API credentials:
   ```json
   {
       "api_key": "your_fivetran_api_key_here",
       "api_secret": "your_fivetran_api_secret_here",
       "debug_mode": "false"
   }
   ```

2. Replace the placeholder values with your actual API credentials

3. **Debug Mode**: Set `debug_mode` to `"true"` to limit processing to the first 5 users only. This is useful for testing and development.

### 4. Testing

Test the connector locally using the Fivetran debug command:

```bash
fivetran debug --configuration configuration.json
```

This will:
- Validate your configuration
- Test API connectivity
- Run a sample sync
- Generate a local DuckDB database (`warehouse.db`) with the results

## Usage

### Local Testing

Run the connector locally for testing:

```bash
python connector.py
```

### Production Deployment

For production deployment, follow the standard Fivetran Connector SDK deployment process:

1. Package your connector
2. Deploy to your Fivetran environment
3. Configure the connector in the Fivetran UI
4. Set up scheduling and monitoring

## SOC2 Compliance Features

### Access Control Monitoring

The connector creates comprehensive access control records that include:
- User identification and contact information
- Organizational structure (teams, groups)
- Role-based permissions
- Resource access (connections)
- Audit timestamps for compliance tracking

### Audit Trail

Every API request is logged with:
- Complete request/response data
- Timestamps for audit purposes
- Status tracking for monitoring
- Record counts for data validation

### Compliance Reporting

The `soc2_access_control` table provides:
- Clear visibility into user permissions
- Access level documentation
- Compliance status tracking
- Audit timestamps for regulatory requirements

## Monitoring and Troubleshooting

### Logs

The connector provides detailed logging at multiple levels:
- `INFO`: Status updates, progress tracking
- `WARNING`: Potential issues, rate limits
- `SEVERE`: Errors, failures, critical issues

### Common Issues

1. **Authentication Errors**: Verify your API credentials in `configuration.json`
2. **Rate Limiting**: The connector includes built-in timeout handling
3. **Network Issues**: Check your network connectivity to Fivetran API
4. **Data Validation**: Review the `api_logs` table for request/response details

### Performance Optimization

- The connector uses checkpointing every 100 records for efficient processing
- Large datasets are processed incrementally
- API requests include proper timeout handling
- State management prevents duplicate processing

## Security Considerations

- API credentials are stored in configuration files (ensure proper file permissions)
- All API communications use HTTPS
- Sensitive data is properly handled and logged
- Audit trails provide complete request/response visibility

## Best Practices

1. **Regular Monitoring**: Monitor the `api_logs` table for any failed requests
2. **Data Validation**: Review the `soc2_access_control` table for completeness
3. **Backup Configuration**: Keep secure backups of your configuration
4. **Access Control**: Limit access to configuration files containing API credentials
5. **Regular Updates**: Keep dependencies updated for security patches

## Support

For issues or questions:
1. Check the logs for error details
2. Review the `api_logs` table for request/response information
3. Verify your API credentials and permissions
4. Consult the Fivetran Connector SDK documentation

## References

- [Fivetran Connector SDK Documentation](https://fivetran.com/docs/connector-sdk)
- [SDK Examples Repository](https://github.com/fivetran/fivetran_connector_sdk/tree/main/examples)
- [Technical Reference](https://fivetran.com/docs/connector-sdk/technical-reference)
- [Best Practices Guide](https://fivetran.com/docs/connector-sdk/best-practices)
