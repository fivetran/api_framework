import requests
from requests.auth import HTTPBasicAuth
import json
import colorama
from colorama import Fore, Back, Style

api_key = ''
api_secret = ''
a = HTTPBasicAuth(api_key, api_secret)

#check status of connector and process x activity.

def atlas(method, endpoint, payload):

    base_url = 'https://api.fivetran.com/v1'
    h = {
        'Authorization': f'Bearer {api_key}:{api_secret}'
    }
    url = f'{base_url}/{endpoint}'

    try:
        if method == 'GET':
            response = requests.get(url, headers=h, auth=a)
            print(response)
        elif method == 'POST':
            response = requests.post(url, headers=h, json=payload, auth=a)
        elif method == 'PATCH':
            response = requests.patch(url, headers=h, json=payload, auth=a)
        elif method == 'DELETE':
            response = requests.delete(url, headers=h, json=payload, auth=a)
        else:
            raise ValueError('Invalid request method.')

        response.raise_for_status()  # Raise exception for 4xx or 5xx responses

        return response.json()
    except requests.exceptions.RequestException as e:
        print(f'Request failed: {e}')
        return None

#Request:
connector_id = ''
method = 'GET'
endpoint = 'connectors/' + connector_id 
payload = ''

#Submit
response = atlas(method, endpoint, payload)

#Review
if response is not None:
    print(Fore.CYAN + 'Call: ' + method + ' ' + endpoint + ' ' + str(payload))
    print(Fore.GREEN + 'Response: ' + response['code'])
    a = response['data']['status']['sync_state']
    if a != 'scheduled':
        print(Fore.MAGENTA + 'Connector Current state: ' + response['data']['status']['sync_state'])
    else:
        print(Fore.MAGENTA + 'Requirement met. Connector is: ' + response['data']['status']['sync_state'])
