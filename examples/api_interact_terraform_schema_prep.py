import json
import requests
from requests.auth import HTTPBasicAuth
import json
import colorama
from colorama import Fore, Back, Style

#configuration file for key,secret,params,etc.
r = '/config.json'
with open(r, "r") as i:
    l = i.read()
    y = json.loads(l)
api_key = y['fivetran']['api_key']
api_secret = y['fivetran']['api_secret']
a = HTTPBasicAuth(api_key, api_secret)

#api_key = ''
#api_secret = ''
#a = HTTPBasicAuth(api_key, api_secret)

#review - terra_schema_template_output.txt
#review - https://registry.terraform.io/providers/fivetran/fivetran/latest/docs/resources/connector_schema_config
#review - logic BLOCK_ALL: all schemas, tables and columns are DISABLED by default, the configuration only specifies ENABLED items
#prepare - .tf prereqs: 
## resource "fivetran_connector_schema_config" "my_connector_schema_config" {
## connector_id = fivetran_connector.resource_name.id
## schema_change_handling = "BLOCK_ALL" | "ALLOW_COLUMNS"
## }


def atlas(method, endpoint, payload=None):

    base_url = 'https://api.fivetran.com/v1'
    h = {
        'Authorization': f'Bearer {api_key}:{api_secret}'
    }
    url = f'{base_url}/{endpoint}'

    try:
        if method == 'GET':
            response = requests.get(url, headers=h, auth=a)
        elif method == 'POST':
            response = requests.post(url, headers=h, json=payload, auth=a)
        elif method == 'PATCH':
            response = requests.patch(url, headers=h, json=payload, auth=a)
        elif method == 'DELETE':
            response = requests.delete(url, headers=h, auth=a)
        else:
            raise ValueError('Invalid request method.')
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f'Request failed: {e}')
        return None

#Request
connector_id = ''
method = 'GET'
endpoint = 'connectors/'+ connector_id + '/schemas'
#payload = ''

response = atlas(method, endpoint)

#Review
if response is not None:
    print(response)

    def convert_to_template(output_file, exclude_columns):
        data = response
        template_output = ""
        
        for schema, schema_data in data["data"]["schemas"].items():
            if schema_data["enabled"]:
                template_output += f"schema {{\n    name = \"{schema}\"\n"
                for table, table_data in schema_data["tables"].items():
                    template_output += f"    table {{\n        name = \"{table}\"\n        enabled = \"true\"\n"
                    template_output += "    }\n"
                if not exclude_columns:
                    for column, column_data in table_data["columns"].items():
                            template_output += f"        column {{\n            name = \"{column}\"\n            enabled = \"true\"\n"
                            if column_data.get("hashed"):
                                template_output += f"            hashed = \"{column_data['hashed']}\"\n"
                            template_output += "        }\n"
                    template_output += "    }\n"
                template_output += "}\n"
        with open(output_file, 'w') as f:
            f.write(template_output)
            
    convert_to_template('terraform_schema_output.txt', exclude_columns=True)
    print('Script Complete. ' + 'Terraform schema formatted for ' + connector_id)
