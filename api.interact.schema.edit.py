import requests
from requests.auth import HTTPBasicAuth
import json
import datetime
import colorama
from colorama import Fore, Back, Style

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
endpoint = 'connectors/' + connector_id + '/schemas/'+ schema 
#PATCH https://api.fivetran.com/v1/connectors/{connector_id}/schemas/{schema}
payload = {
        "enabled": True,
        "tables": {
            "": {
                "enabled": False
            }
        }
} 

response = atlas(method, endpoint, payload)

if response is not None:
    print(Fore.CYAN + 'Call: ' + method + ' ' + endpoint + ' ' + str(payload))
    print(Fore.GREEN +  'Response: ' + response['code'])
    print(Fore.MAGENTA + str(response))
