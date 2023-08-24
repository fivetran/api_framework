import requests
from requests.auth import HTTPBasicAuth
import json
import colorama
from colorama import Fore, Back, Style

api_key = ''
api_secret = ''
a = HTTPBasicAuth(api_key, api_secret)

#new BQ destination + group

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
method = 'POST' #'PATCH' 'GET' 'DELETE'
endpoint = 'destinations/'
gendpoint = 'groups/'
gpayload = {
    "name": ""
}

#Submit group
gresp = atlas(method, gendpoint, gpayload)
if gresp is not None:
    print(Fore.CYAN + 'Call: ' + method + ' ' + gendpoint + ' ' + str(gpayload))
    print(Fore.GREEN +  'Response: ' + gresp['code'])
    print(Fore.MAGENTA + str(gresp))
    payload = {
      "group_id":  gresp['data']['id'],
      "service": "big_query",
      "region": "US",
      "time_zone_offset": "-5",
      "config" : 
          {
            "project_id": "",
            "data_set_location": "US"
          }
  }
# #Submit destination
    response = atlas(method, endpoint, payload)
    if response is not None:
        print(Fore.CYAN + 'Call: ' + method + ' ' + endpoint + ' ' + str(payload))
        print(Fore.GREEN +  'Response: ' + response['code'])
        print(Fore.MAGENTA + str(response))
