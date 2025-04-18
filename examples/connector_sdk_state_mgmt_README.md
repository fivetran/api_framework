# Fivetran API State Management Tool

## Overview
This tool provides a secure and user-friendly interface for managing Fivetran connection states through their REST API. It allows users to safely pause connections, update their state, and resume operations with proper error handling and verification steps.

## Features
- Secure API authentication using Basic Auth
- Interactive command-line interface
- Safe state management workflow:
  1. Pause connection
  2. Update state
  3. Resume connection
  4. Verify changes
- JSON state validation
- Error handling and recovery
- Color-coded console output for better visibility

## Prerequisites
- Python 3.x
- Required Python packages:
  - `requests`
  - `colorama`
  - `json`

## Configuration
1. Create a `config.json` file in your workspace root with the following structure:
```json
{
    "fivetran": {
        "api_key_demo_sand": "your_api_key",
        "api_secret_demo_sand": "your_api_secret"
    }
}
```

## Usage
1. Navigate to the tool directory:
```bash
cd code/api/fivetran
```

2. Run the script:
```bash
python api_interact_state.py
```

3. Follow the interactive prompts:
   - Enter the connection ID when prompted
   - View the current state
   - Enter new state information in JSON format (or press Enter to keep current state)
   - Monitor the process through color-coded console output

## State Update Format
The state should be provided as a JSON dictionary. Examples:
```json
{"cursor": "2025-03-06 20:20:20"}
{"last_sync": "2025-03-06T20:20:20Z"}
```

## Security Considerations
- API credentials are stored in a separate configuration file
- Credentials are encoded using Base64 for API authentication
- The tool uses HTTPS for all API communications

## Error Handling
The tool includes comprehensive error handling for:
- Invalid API credentials
- Network connectivity issues
- Invalid JSON input
- API response errors
- Connection state management failures

## Output Colors
- Cyan: Information and prompts
- Green: Successful operations
- Yellow: Warnings and no changes
- Red: Errors and failures

## Best Practices
1. Always verify the current state before making changes
2. Keep a backup of the current state before updates
3. Monitor the verification step to ensure changes were applied correctly
4. Use the tool in a controlled environment to prevent accidental state changes

## Support
For issues or questions, please contact your system administrator or Fivetran support.

## License
This tool is provided as-is and should be used in accordance with Fivetran's API usage policies. 
