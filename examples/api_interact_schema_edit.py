import requests
from requests.auth import HTTPBasicAuth
import json
import datetime
import colorama
from colorama import Fore, Back, Style

#configuration file for key,secret,params,etc.
#r = 'config.json'
#with open(r, "r") as i:
#    l = i.read()
#    y = json.loads(l)
#api_key = y['API_KEY']
#api_secret = y['API_SECRET']
#a = HTTPBasicAuth(api_key, api_secret)

api_key = ''
api_secret = ''
a = HTTPBasicAuth(api_key, api_secret)

#modify existing connector schema, sync 

def atlas(method, endpoint, payload):

    base_url = 'https://api.fivetran.com/v1'
    h = {
        'Authorization': f'Bearer {api_key}:{api_secret}'
    }
    url = f'{base_url}/{endpoint}'

    print(datetime.datetime.now())
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
        print(datetime.datetime.now())
        return response.json()
        
    except requests.exceptions.RequestException as e:
        print(f'Request failed: {e}')
        print(datetime.datetime.now())
        return None

# Example:
connector_id = ''
group_id = ''
schema = ''
method = 'PATCH'  #'POST' 'PATCH' 'GET'
# Before 02/2025
endpoint = 'connectors/' + connector_id + '/schemas/'+ schema 
# After 02/2025
# connection_id = ''
#endpoint = 'connections/' + connection_id + '/schemas'
#PATCH https://api.fivetran.com/v1/connectors/{connector_id}/schemas

# Before 02/2025
payload = {
        "enabled": True,
        "tables": {
            "": {
                "enabled": False
            }
        }
} 

# After 02/2025
#V2
# payload = {
#     "schemas": {"covid19": {
#             "enabled": False,
#             "tables": {"anomalies": {
#                     "enabled": False
#                 }
#         },
#         "colleges": {
#             "enabled": False,
#             "tables": {"anomalies": {
#                     "enabled": False
#                 }
#         }        
#         },
#     "schema_change_handling": "BLOCK_ALL"
#             }
#         }
#     }

response = atlas(method, endpoint, payload)

if response is not None:
    print(Fore.CYAN + 'Call: ' + method + ' ' + endpoint + ' ' + str(payload))
    print(Fore.GREEN +  'Response: ' + response['code'])
    print(Fore.MAGENTA + str(response))
