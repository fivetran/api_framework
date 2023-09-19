import requests
from requests.auth import HTTPBasicAuth
import json
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

#Request connector details to copy to new destination
connector_id = ''
method = 'GET'  #'POST' 'PATCH' 'DELETE'
endpoint = 'connectors/' + connector_id 
payload = ''

#Submit
response = atlas(method, endpoint, payload)
#Review
if response is not None:
    print(Fore.CYAN + 'Call: ' + method + ' ' + endpoint + ' ' + str(payload))
    print(Fore.GREEN +  'Response: ' + response['code'])
    #print(Fore.MAGENTA + str(response))
    print(Fore.MAGENTA + 'Processing Connector Migration for connector id ' + connector_id + ' to destination id ' + y['DEST'])

    #Migrate Connector
    spw = '' #source pw
    dest = ["", "", ""]
    ns = '' #new connector name
    j = {"force": True} #initiate the sync
    t = {"force": False} #initiate the sync
    mu = "https://api.fivetran.com/v1/connectors/" #main url
    session = requests.Session()
    u_0 = mu + "{}"
    u_1 = mu

    #1
    data_list = response['data']
    #validate connector data to migrate
    #print(data_list)

    #create new connector in new destination(s) using response data

for dest in dest:
    c = {"service": data_list['service'],
            "group_id": dest,
            "trust_certificates": "true",
            "run_setup_tests": "true",
            "paused": "true",  #Trigger sync
            "pause_after_trial": "true",
            "sync_frequency": data_list['sync_frequency'],
            "config": { "schema_prefix": ns,
                         "host": data_list['config']['host'],
                          "port": data_list['config']['port'], 
                          "database": data_list['config']['database'],
                          "user": data_list['config']['user'],
                          "password": spw}}         
   
    #2 create the connector in the new destination and review the results
    print(Fore.CYAN + "Submitting Connector")  
    x = requests.post(u_1,auth=a,json=c)
    z = x.json()
    #print(z)
    resp = z['data']
    print(Fore.GREEN + "Connector Created")
    #print(Fore.GREEN + x.text + " ***Connector Created***")
    #print(resp)

    #prepare to configure the schema
    u_2 = mu + "{}" + "/schemas"
    u_3 = mu + resp['id'] + "/schemas/reload"
    u_4 = mu + resp['id'] + "/schemas"
    u_5 = mu + resp['id'] + "/sync"
    
    #3 validate existing config
    print(Fore.CYAN + "Validating Original Schema")  
    sresponse =session.get(url=u_2.format(connector_id), auth=a).json()
    d = sresponse['data']
    #print(d)

    #4 load the schema config on the new connector
    print(Fore.CYAN + "Loading New Schema")  
    o = requests.post(u_3,auth=a)
    print(Fore.GREEN + "Connector Schema Loaded")

    #5 configure the new connector
    print(Fore.CYAN + "Submitting Connector Schema Configuration")  
    q = requests.patch(u_4,auth=a,json=d)
    print(Fore.GREEN + "Connector Schema Configured")

    print(Fore.CYAN + "Validating Original Schema")  
    sssresponse =session.get(url=u_2.format(resp['id']), auth=a).json()
    q = sssresponse['data']

    #6 sync the new connector
    #s = requests.post(u_5,auth=a,json=j)
    #print(Fore.GREEN + "Connector Sync Started")
    #v = requests.post(u_5,auth=a,json=j)
    #print(Fore.GREEN + "Connector Sync paused")

    #success
    print(Fore.MAGENTA + "Connector: " + ns + " successfully created in " + str(dest))
