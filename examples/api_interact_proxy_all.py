import requests
from requests.auth import HTTPBasicAuth
import json
from datetime import datetime
import colorama
from colorama import Fore, Back, Style

#configuration file
r = '/config.json'
with open(r, "r") as i:
    l = i.read()
    y = json.loads(l)
api_key = y['fivetran']['api_key']
api_secret = y['fivetran']['api_secret']
a = HTTPBasicAuth(api_key, api_secret)

current_date = datetime.now().strftime("%m/%d/%Y")
since_id = None
agents_out = []
details_out = []
connections_out = []

#api_key = ''
#api_secret = ''
#a = HTTPBasicAuth(api_key, api_secret)

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


method = 'GET'  #'POST' 'PATCH' 'DELETE'
endpoint = 'proxy'
payload = {}


#Submit
response = atlas(method, endpoint, payload)
#Review
if response is not None:
    data_list =  response['data']
    timeline  =  data_list['items']

    # Loop through n agents
    for i in range(5):
        agent = timeline[i]
        agents_out.append({
            "account_id": agent['account_id'],
            "agent_id": agent['id'],
            "registered_at": agent['registered_at'],
            "display_name": agent['display_name']
        })

        agent_id = agent['id']
        payload = agent_id
        agent_dets = atlas(method, endpoint, payload)
        data = agent_dets['data']

        details_out.append({
            "account_id": agent['account_id'],
            "agent_id": agent['id'],
            "registered_at": agent['registered_at'],
            "display_name": agent['display_name'],
            "region": agent['region'],
            "token": agent['token'],
            "salt": agent['salt'],
            "created_by": agent['created_by']
        })

        endpoint = 'proxy/' + agent_id + '/connections'
        payload = {}
        connection_dets = atlas(method, endpoint, payload)
        if connection_dets is not None and 'items' in connection_dets:
            connection_list = connection_dets['data']
            connection_items = connection_dets['items']
        else:
            connection_items = None

        connections_out.append({
            "account_id": agent['account_id'],
            "agent_id": agent['id'],
            "registered_at": agent['registered_at'],
            "display_name": agent['display_name'],
            "connection_id": connection_items
        })

        ans = {
                "state": {
                    since_id: current_date
                },
                "schema" : {
                    "agent_info" : {
                    "primary_key" : "agent_id"
                        },
                    "agent_details" : {
                    "primary_key" : "agent_id"
                        },
                    "agent_connections" : {
                    "primary_key" : "agent_id"
                        }
                },
                "insert": {
                    "agent_info": agents_out,
                    "agent_details" : details_out,
                    "agent_connections":connections_out
                },
                "hasMore" : False
            }

    #print(agents_out)
    #print(details_out)
    #print(connections_out)
print(ans)
