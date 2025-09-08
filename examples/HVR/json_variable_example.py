import json

# Define your JSON file path here
json_file_path = 'C:/temp/python/Export_Channel_2025-09-03T194540.json'

# Load configuration from json_file_path
with open(json_file_path, 'r') as config_file:
    config = json.load(config_file)

# Read prefix, suffix, schema, and file_path from config
prefix = config.get('prefix', 'dev_')
suffix = config.get('suffix', '')
schema = config.get('schema', '')
file_path = config.get('file_path', json_file_path)  # Default to json_file_path if not provided

# Load main data from file_path
with open(file_path, 'r') as file:
    data = json.load(file)

# Validate prefix and suffix
if not prefix and not suffix:
    raise ValueError("Error: Either prefix or suffix must be provided.")

# Extract the base names from add_channel tables
base_names = [
    channel['base_name'] 
    for change in data['changes'] 
    if 'add_channel' in change 
    for channel in change['add_channel']['tables'].values()
]

# Extract the prefix from the first BaseName in existing actions
existing_actions = []
if data['changes']:
    for change in data['changes']:
        if 'add_channel' in change:
            existing_actions = change['add_channel']['actions']
            break

# Get the prefix from the first BaseName if it exists
if existing_actions and 'params' in existing_actions[0] and 'BaseName' in existing_actions[0]['params']:
    first_base_name = existing_actions[0]['params']['BaseName']
    prefix = first_base_name.split('_')[0] + '_'
else:
    prefix = prefix  # Keeps the original

# Create a set of existing BaseNames for quick lookup
existing_base_names = {
    action['params']['BaseName'] 
    for action in existing_actions 
    if 'params' in action and 'BaseName' in action['params']
}

table_scope_map = {action['table_scope']: action for action in existing_actions}

# Create new actions for each base_name that doesn't already exist
new_actions = []
for base_name in base_names:
    new_action = {
        "loc_scope": "TARGET",
        "table_scope": base_name,
        "type": "TableProperties",
        "params": {
            "BaseName": f"{prefix}{'_' if prefix else ''}{base_name}{'_' + suffix if suffix else ''}",
            "Schema": schema  
        }
    }
    if new_action['params']['BaseName'] not in existing_base_names:
        new_actions.append(new_action)

for change in data['changes']:
    if 'add_channel' in change:
        for table_name in change['add_channel']['tables']:
            table_action = {
                "loc_scope": "TARGET",
                "table_scope": table_name,
                "type": "TableProperties",
                "params": {
                    "BaseName": f"{prefix}{'_' if prefix else ''}{table_name}{'_' + suffix if suffix else ''}",  
                    "Schema": schema 
                }
            }
            if table_name in table_scope_map:
                table_scope_map[table_name]['params']['BaseName'] = table_action['params']['BaseName']
            else:
                change['add_channel']['actions'].append(table_action)
                table_scope_map[table_name] = table_action

# Save the updated JSON back to the file
with open(file_path, 'w') as file:
    json.dump(data, file, indent=2)

print("Channel actions updated successfully.")

#Contributed by - David K.
