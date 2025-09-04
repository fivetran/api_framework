# HVR Base Name Configuration Tool

A Python utility for automatically configuring table base names and schemas in HVR channel configurations. This tool helps standardize naming conventions and schema assignments across multiple tables in your HVR replication workflows.

## 🎯 What This Tool Does

The HVR Base Name Configuration Tool addresses common challenges in HVR channel setup:

### **Problem Solved:**
- **Manual Configuration**: Eliminates the need to manually set base names for each table
- **Naming Consistency**: Ensures consistent naming patterns across all tables
- **Schema Management**: Automatically assigns schemas to tables
- **Bulk Updates**: Updates multiple tables at once with standardized configurations

### **Key Features:**
- **Prefix/Suffix Support**: Add consistent prefixes or suffixes to table names
- **Schema Assignment**: Automatically assign schemas to all tables
- **Duplicate Prevention**: Avoids creating duplicate actions for existing tables
- **Configuration-Driven**: Uses JSON configuration for easy customization
- **Validation**: Ensures required configuration parameters are provided

## 🚀 Quick Start

### Prerequisites
- Python 3.6+
- HVR channel configuration JSON file
- Valid configuration file (`config.json`)

### Installation
1. Ensure you have the `hvr_base_name_v2.py` file
2. Create a `config.json` file with your settings
3. Prepare your HVR channel configuration JSON file

### Configuration Setup

Create a `config.json` file in your working directory:

```json
{
  "prefix": "MY_PREFIX_",
  "suffix": "_SUFFIX",
  "schema": "MY_SCHEMA",
  "file_path": "/path/to/your/channel_config.json"
}
```

**Configuration Parameters:**
- `prefix`: Optional prefix to add to all table base names
- `suffix`: Optional suffix to add to all table base names  
- `schema`: Schema name to assign to all tables
- `file_path`: Path to your HVR channel configuration JSON file

## 📖 Use Cases

### Use Case 1: Standardize Table Naming

**Scenario**: You want all tables in your channel to follow a consistent naming pattern.

```json
// config.json
{
  "prefix": "PROD_",
  "suffix": "_V1",
  "schema": "PRODUCTION_SCHEMA",
  "file_path": "./channel_config.json"
}
```

**Result**: Tables like `customers`, `orders`, `products` become:
- `PROD_customers_V1`
- `PROD_orders_V1` 
- `PROD_products_V1`

### Use Case 2: Environment-Specific Configuration

**Scenario**: You need different naming patterns for different environments.

```json
// Development Environment
{
  "prefix": "DEV_",
  "schema": "DEV_SCHEMA",
  "file_path": "./dev_channel_config.json"
}

// Production Environment  
{
  "prefix": "PROD_",
  "schema": "PROD_SCHEMA",
  "file_path": "./prod_channel_config.json"
}
```

### Use Case 3: Schema Migration

**Scenario**: You're migrating tables to a new schema and need to update all base names.

```json
{
  "prefix": "NEW_",
  "schema": "MIGRATED_SCHEMA",
  "file_path": "./migration_config.json"
}
```

## 🛠️ How It Works

### Input Format
The tool expects an HVR channel configuration JSON file with this structure:

```json
{
  "changes": [
    {
      "add_channel": {
        "tables": {
          "table1": {
            "base_name": "original_table1"
          },
          "table2": {
            "base_name": "original_table2"
          }
        },
        "actions": [
          {
            "loc_scope": "TARGET",
            "table_scope": "table1",
            "type": "TableProperties",
            "params": {
              "BaseName": "existing_base_name",
              "Schema": "existing_schema"
            }
          }
        ]
      }
    }
  ]
}
```

### Processing Logic
1. **Extract Base Names**: Reads existing table base names from the configuration
2. **Analyze Existing Actions**: Identifies existing TableProperties actions
3. **Generate New Actions**: Creates new actions for tables without existing configurations
4. **Update Existing Actions**: Modifies existing actions with new naming patterns
5. **Apply Schema**: Assigns the specified schema to all tables
6. **Save Changes**: Writes the updated configuration back to the file

### Output Format
The tool updates your configuration file with new TableProperties actions:

```json
{
  "changes": [
    {
      "add_channel": {
        "tables": {
          "table1": {
            "base_name": "original_table1"
          }
        },
        "actions": [
          {
            "loc_scope": "TARGET",
            "table_scope": "table1",
            "type": "TableProperties",
            "params": {
              "BaseName": "PREFIX_table1_SUFFIX",
              "Schema": "MY_SCHEMA"
            }
          }
        ]
      }
    }
  ]
}
```

## 🔧 Configuration Examples

### Basic Configuration
```json
{
  "schema": "DEFAULT_SCHEMA",
  "file_path": "./channel.json"
}
```

### With Prefix Only
```json
{
  "prefix": "STAGING_",
  "schema": "STAGING_SCHEMA", 
  "file_path": "./staging_channel.json"
}
```

### With Suffix Only
```json
{
  "suffix": "_BACKUP",
  "schema": "BACKUP_SCHEMA",
  "file_path": "./backup_channel.json"
}
```

### Full Configuration
```json
{
  "prefix": "PROD_",
  "suffix": "_V2",
  "schema": "PRODUCTION_SCHEMA",
  "file_path": "./production_channel.json"
}
```

## 🎯 Advanced Usage

### Batch Processing Multiple Files
```python
import json
import os

configs = [
    {"prefix": "DEV_", "schema": "DEV_SCHEMA", "file_path": "./dev.json"},
    {"prefix": "STAGE_", "schema": "STAGE_SCHEMA", "file_path": "./stage.json"},
    {"prefix": "PROD_", "schema": "PROD_SCHEMA", "file_path": "./prod.json"}
]

for config in configs:
    with open('config.json', 'w') as f:
        json.dump(config, f)
    
    # Run the tool
    os.system('python hvr_base_name_v2.py')
```

### Integration with HVR API Tool
```python
# After running the base name tool, use the updated config with HVR API
from hvr_api_frame import HVRAPIClient

# Load the updated configuration
with open('updated_channel_config.json', 'r') as f:
    channel_config = json.load(f)

# Create channel using HVR API
client = HVRAPIClient(base_url, username, password, access_token)
new_channel = client.create_channel(channel_config)
```

## 🚨 Troubleshooting

### Common Issues

1. **Configuration File Not Found**
   ```
   FileNotFoundError: [Errno 2] No such file or directory: 'config.json'
   ```
   **Solution**: Ensure `config.json` exists in the working directory

2. **Invalid JSON Format**
   ```
   json.JSONDecodeError: Expecting property name enclosed in double quotes
   ```
   **Solution**: Check JSON syntax in your configuration files

3. **Missing Required Configuration**
   ```
   ValueError: Error: Either prefix or suffix must be provided.
   ```
   **Solution**: Provide at least one of prefix, suffix, or ensure existing actions exist

4. **File Path Issues**
   ```
   FileNotFoundError: [Errno 2] No such file or directory: '/path/to/file.json'
   ```
   **Solution**: Verify the file_path in config.json is correct

### Debug Mode
Add debug logging to understand what the tool is doing:

```python
# Add to the script for debugging
print(f"Processing file: {file_path}")
print(f"Found {len(base_names)} base names: {base_names}")
print(f"Existing actions: {len(existing_actions)}")
print(f"New actions to add: {len(new_actions)}")
```

## 🔧 Extending the Tool

### Adding New Parameters
```python
# Add new configuration parameters
config = json.load(config_file)
new_param = config.get('new_param', 'default_value')

# Use in action generation
new_action = {
    "loc_scope": "TARGET",
    "table_scope": base_name,
    "type": "TableProperties", 
    "params": {
        "BaseName": f"{prefix}{base_name}{suffix}",
        "Schema": schema,
        "NewParam": new_param  # Add new parameter
    }
}
```

### Custom Naming Logic
```python
# Implement custom naming patterns
def generate_custom_name(base_name, prefix, suffix, schema):
    # Add your custom logic here
    if base_name.startswith('temp_'):
        return f"{prefix}TEMP_{base_name[5:]}{suffix}"
    else:
        return f"{prefix}{base_name}{suffix}"

# Use in action generation
custom_base_name = generate_custom_name(base_name, prefix, suffix, schema)
```

### Validation Functions
```python
def validate_config(config):
    """Validate configuration parameters."""
    errors = []
    
    if not config.get('schema'):
        errors.append("Schema is required")
    
    if not config.get('file_path'):
        errors.append("File path is required")
    
    if not os.path.exists(config.get('file_path', '')):
        errors.append(f"File not found: {config.get('file_path')}")
    
    return errors

# Use validation
errors = validate_config(config)
if errors:
    raise ValueError(f"Configuration errors: {errors}")
```

## 📚 Best Practices

1. **Backup Your Files**: Always backup original configurations before running the tool
2. **Test in Development**: Test naming patterns in development before production
3. **Version Control**: Keep configurations in version control for tracking changes
4. **Documentation**: Document your naming conventions for team consistency
5. **Validation**: Validate generated configurations before deploying to HVR

## 🤝 Integration with Other Tools

### HVR API Integration
This tool works seamlessly with the [HVR API Client Tool](https://github.com/fivetran/api_framework/blob/main/examples/HVR/hvr_api_framework.py):
1. Use this tool to prepare channel configurations
2. Use the HVR API tool to deploy the configurations
3. Automate the entire process with scripts

```bash
#!/bin/bash
# Example script
python hvr_channel_actions.py
python hvr_api_framework.py
```

## 📚 Additional Resources

- HVR Documentation
- JSON Schema Validation
- Python JSON Processing
- HVR Channel Configuration Guide

---

**Happy HVR Configuration Management! 🚀**
