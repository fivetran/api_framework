import requests
from requests.auth import HTTPBasicAuth
import json
import colorama
from colorama import Fore, Back, Style
import time

#configuration file for key,secret,params,etc.
r = 'config.json'
with open(r, "r") as i:
    l = i.read()
    y = json.loads(l)
api_key = y['fivetran']['api_key']
api_secret = y['fivetran']['api_secret']
a = HTTPBasicAuth(api_key, api_secret)

#api_key = ''
#api_secret = ''
#a = HTTPBasicAuth(api_key, api_secret)

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
method = 'GET'
endpoint = 'connectors/' + y['fivetran']['c']
payload = ''
t = {"paused": False} #activate
j = {"force": True}  #resync
m = {"schedule_type": "manual"} #control scheduling via API

#Submit
response = atlas(method, endpoint, payload)

#Review
if response is not None:
    stat = response['data']['status']['sync_state']
    print(stat)
    if stat != 'syncing':
        mu = "https://api.fivetran.com/v1/connectors/"
        syncer = mu + y['fivetran']['c'] + "/sync"
        modi = mu + y['fivetran']['c']
        #activate
        sz = requests.patch(modi,auth=a,json=t)
        time.sleep(10)
        print("Connector active")
        #sw = requests.patch(modi,auth=a,json=m)
        #sync
        sy = requests.post(syncer,auth=a,json=j)
        time.sleep(20)
    statupdt = atlas(method, endpoint, payload)
    stat2 = statupdt['data']['status']['sync_state']
    print(stat2)
