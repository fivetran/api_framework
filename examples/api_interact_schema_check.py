import requests
from requests.auth import HTTPBasicAuth
import json
from datetime import datetime, timedelta

#configuration file
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

#Copy a Connector
def atlas(method, endpoint, payload):

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
method = 'POST'
connector_id = '' 
# Before 02/2025
endpoint = 'connectors/'+ connector_id +'/schemas/reload' 
# After 02/2025
# connection_id = '' 
# endpoint = 'connections/'+ connection_id +'/schemas/reload' 
payload = {"exclude_mode": "PRESERVE"}

#Submit
response = atlas(method, endpoint, payload)

if __name__ == '__main__':
    if response is not None:

        for schema_name, schema_info in response['data']['schemas'].items():
            #print(f"Schema Name: {schema_name}, Enabled: {schema_info['enabled']}")
            if schema_info['enabled'] == False:
                print(f"Schema Name: {schema_name}, Enabled: {schema_info['enabled']}")
