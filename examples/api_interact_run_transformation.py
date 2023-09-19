import requests
from requests.auth import HTTPBasicAuth
import json
import colorama
from colorama import Fore

#configuration file
#r = 'config.json'
#with open(r, "r") as i:
#    l = i.read()
#    y = json.loads(l)
#api_key = y['API_KEY']
#api_secret = y['API_SECRET']
#a = HTTPBasicAuth(api_key, api_secret)

#api_key = ''
#api_secret = ''

#run a dbt transformation

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
        response.raise_for_status()  # Raise exception
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f'Request failed: {e}')
        return None
#Request
transformation_id = ''
method = 'POST'  #'PATCH' 'DELETE' 'GET'
endpoint = 'dbt/transformations/' + transformation_id + '/run'
payload = ''

#Submit
response = atlas(method, endpoint, payload)
#Review
if response is not None:
    print(Fore.CYAN + 'Call: ' + method + ' ' + endpoint + ' ' + str(payload))
    print(Fore.GREEN +  'Response: ' + response['code'])
    print(Fore.MAGENTA + str(response))
