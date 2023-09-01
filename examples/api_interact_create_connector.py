import requests
from requests.auth import HTTPBasicAuth
import json
import colorama
from colorama import Fore, Back, Style


api_key = ''
api_secret = ''
a = HTTPBasicAuth(api_key, api_secret)

#new SQL server connector (n times)

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
        response.raise_for_status()  # Raise exception for 4xx or 5xx responses
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f'Request failed: {e}')
        return None

#Request
p = ''                     #source auth
destination = ''          #destination ID
new_schema = ["s_011", "s_012","s_013"]        #new schema name(s)
method = 'POST'                 #'PATCH' 'GET' 'DELETE'
endpoint = 'connectors/'

for new_schema in new_schema:
    payload = {
                "service": "sql_server_rds",
                "group_id": destination,
                "trust_certificates": "true",
                "run_setup_tests": "true",
                "paused": "true",
                "pause_after_trial": "true",
                "config": { "schema_prefix": new_schema,
                            "host":  "",
                            "port": 1433,
                            "database": "sqlserver",
                            "user": "fivetran",
                            "password": p       
                }}
#Submit
    print(Fore.CYAN + "Submitting Connector") 
    response = atlas(method, endpoint, payload)

#Review
    if response is not None:
        print('Call: ' + method + ' ' + endpoint + ' ' + str(payload))
        print(response['code'] + ' ' + response['message'])
        print(Fore.MAGENTA + "Connector: " + response['data']['id']  + " successfully created in " + str(destination))
