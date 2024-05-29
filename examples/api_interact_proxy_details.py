import requests
from requests.auth import HTTPBasicAuth
import json
import colorama
from colorama import Fore, Back, Style
from datetime import datetime


#configuration file
r = '/config.json'
with open(r, "r") as i:
    l = i.read()
    y = json.loads(l)
api_key = y['fivetran']['api_key']
api_secret = y['fivetran']['api_secret']
a = HTTPBasicAuth(api_key, api_secret)
agents_out = []

#api_key = ''
#api_secret = ''
#a = HTTPBasicAuth(api_key, api_secret)
current_date = datetime.now().strftime("%m/%d/%Y")
since_id = None

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

        response.raise_for_status()  # Raise exception for 4xx or 5xx responses

        return response.json()
    except requests.exceptions.RequestException as e:
        print(f'Request failed: {e}')
        return None

method = 'GET'  #'POST' 'PATCH' 'DELETE'
endpoint = 'proxy'
payload = {}


#Submit
response = atlas(method, endpoint, payload)
#Review
if response is not None:
    data_list =  response['data']
    timeline  =  data_list['items']

    for i in timeline:
        agents_out.append({
                    "account_id": i['account_id'],
                    "registered_at": i['registered_at'],
                    "display_name": i['display_name'],
                    "region": i['region'],
                    "token": i['token'],
                    "salt": i['salt'],
                    "created_by": i['created_by']
            })
    

    ans = {
                "state": {
                    since_id: current_date
                },
                "schema" : {
                    "agent_info" : {
                    "primary_key" : "agent_id"
                        }
                },
                "insert": {
                    "agent_info": agents_out
                },
                "hasMore" : False
            }
    
print(ans)
