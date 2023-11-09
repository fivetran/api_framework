import requests
from requests.auth import HTTPBasicAuth
import json
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

#api_key = ''
#api_secret = ''
#a = HTTPBasicAuth(api_key, api_secret)

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

#Request to get connector details from a given group
group_id = ''  
new_group = ''        
method = 'GET'  #'POST' 'PATCH' 'DELETE'
endpoint = 'groups/' + group_id + '/connectors' 
payload = ''

#Submit
response = atlas(method, endpoint, payload)
#Review
if response is not None:

    #Migrate Connectors
    j = {"force": True} #initiate the sync
    mu = "https://api.fivetran.com/v1/connectors/" #main url
    session = requests.Session()
    u_0 = mu + "{}"
    u_1 = mu
    data_list = response['data']
    #print(data_list)
    migration_objects = data_list['items']
    #print(migration_objects)
    print(Fore.CYAN + 'Call: ' + method + ' ' + endpoint + ' ' + str(payload))
    print(Fore.GREEN +  'Response: ' + response['code'])

    for i in migration_objects:

        print(Fore.MAGENTA + 'Processing Connector Migration for connector id ' + i['id'] + ' to destination id ' + new_group)
        #validate connector data to migrate
        
        cresponse=session.get(url=u_0.format(i['id']), auth=a).json()
        ct =  cresponse['data']
        #print(ct)
        if ct['service'] == 'google_sheets':
            c = {
                    "service": ct['service'],
                    "group_id": new_group,
                    "paused": "true",  #Trigger sync
                    "config": {
                        "schema": "google_sheets_migrated",
                        "table": "table_test",
                        "named_range": ct['config']['named_range'],
                        "sheet_id": ct['config']['sheet_id']
                    }
                }
            #print(c)
        elif ct['service'] == 'sql_server':
            c = {"service": ct['service'],
                "group_id": new_group,
                "trust_certificates": "true",
                "run_setup_tests": "true",
                "paused": "true",  #Trigger sync
                "pause_after_trial": "true",
                "sync_frequency": ct['sync_frequency'],
                "config": { "schema_prefix": ct['schema'] + '_migrated_test',
                            "host": ct['config']['host'],
                            "port": ct['config']['port'], 
                            "database": ct['config']['database'],
                            "user": ct['config']['user'],
                            "password": y['fivetran']['spw']}
                }
            #source not defined   
        else: print('Atlas has no map to this source. Please refer to the Fivetran documenation linked here: https://fivetran.com/docs/rest-api/connectors')
           
        #create the connector in the new destination and review the results
        print(Fore.CYAN + "Submitting Connector")  
        x = requests.post(u_1,auth=a,json=c)
        z = x.json()
        #print(x)
        #print(z)
        resp = z['data']
        print(Fore.GREEN + "Connector " + resp['id'] + " Created. Type " + ct['service'])

        #prepare to configure the schema
        u_2 = mu + "{}" + "/schemas"
        u_3 = mu + resp['id'] + "/schemas/reload"
        u_4 = mu + resp['id'] + "/schemas"
        u_5 = mu + resp['id'] + "/sync"
        
        #validate existing config
        print(Fore.CYAN + "Validating Original Schema for " + ct['id'])  
        sresponse =session.get(url=u_2.format(ct['id']), auth=a).json()
        d = sresponse['data']

        #load the schema config on the new connector
        print(Fore.CYAN + "Loading New Schema for " + resp['id'])  
        o = requests.post(u_3,auth=a)
        print(Fore.GREEN + "Connector Schema Loaded")

        #configure the new connector
        print(Fore.CYAN + "Submitting Connector Schema Configuration for " + resp['id'])  
        q = requests.patch(u_4,auth=a,json=d)
        print(Fore.GREEN + "Connector Schema Configured")

        #sync the new connector
        #s = requests.post(u_5,auth=a,json=j)
        #print(Fore.GREEN + "Connector Sync Started")

        #success
        print(Fore.MAGENTA + "Connector: " + resp['id'] + " successfully created in " + str(new_group))
