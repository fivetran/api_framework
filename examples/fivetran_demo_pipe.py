import requests
from requests.auth import HTTPBasicAuth
import json
import colorama
from colorama import Fore, Back, Style
import time

#configuration file
r = '/config.json'
with open(r, "r") as i:
    l = i.read()
    y = json.loads(l)
api_key = y['API_KEY']
api_secret = y['API_SECRET']
a = HTTPBasicAuth(api_key, api_secret)

#Create a new group, destination, webhook, connectors, and execute a transformation.
def atlas(method, endpoint, payload):

    base_url = 'https://api.fivetran.com/v1'
    url = f'{base_url}/{endpoint}'

    try:
        if method == 'GET':
            response = requests.get(url,auth=a)
        elif method == 'POST':
            response = requests.post(url, json=payload, auth=a)
        elif method == 'PATCH':
            response = requests.patch(url,json=payload, auth=a)
        elif method == 'DELETE':
            response = requests.delete(url, auth=a)
        else:
            raise ValueError('Invalid request method.')
        response.raise_for_status()  # Raise exception for 4xx or 5xx responses
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f'Request failed: {e}')
        return None
#Group and destination params
method = 'POST'         
endpoint = 'destinations/'
gendpoint = 'groups/'
gpayload = {
    "name": "im_group"
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
    #Submit destination
    dresponse = atlas(method, endpoint, payload)
    if dresponse is not None:
        print(Fore.CYAN + 'Call: ' + method + ' ' + endpoint + ' ' + str(payload))
        print(Fore.GREEN +  'Response: ' + dresponse['code'])
        print(Fore.MAGENTA + str(dresponse))
        #New Webhook from response data
        wgid = gresp['data']['id']
        wmethod = 'POST' 
        wendpoint = 'webhooks/group/' + wgid
        wpayload = { 
        "url": "https.ngrok-free.app",
        "events": [ "connection_successful",
        "connection_failure",
        "create_connector",
        "pause_connector",
        "resume_connector",
        "edit_connector",
        "delete_connector",
        "force_update_connector",
        "resync_connector",
        "resync_table"
        ]
        }
        #Submit Webhook
        print(Fore.CYAN + "Submitting Webhook") 
        wresponse = atlas(wmethod, wendpoint, wpayload)
        #Create Connectors
        p = y['T']                               #source auth
        new_schema = ["t_400", "t_401","t_402"]  #connector names
        smethod = 'POST'                      
        sendpoint = 'connectors/'
        for new_schema in new_schema:
            spayload = {
                        "service": "sql_server_rds",
                        "group_id": wgid,
                        "trust_certificates": "true",
                        "run_setup_tests": "true",
                        "paused": "false",
                        "pause_after_trial": "true",
                        "config": { "schema_prefix": new_schema,
                                    "host":  "",
                                    "port": 1433,
                                    "database": "sqlserver",
                                    "user": "fivetran",
                                    "password": p       
                        }}
        #Submit Connectors
            print(Fore.CYAN + "Submitting Connector") 
            cresponse = atlas(smethod, sendpoint, spayload)
        #Review Connector Response
            if cresponse is not None:
                print(Fore.MAGENTA + "Connector: " + cresponse['data']['id']  + " successfully created in " + str(wgid))
        #Pause for 30 seconds. Then, Pause the connector. Then, edit schema.
                time.sleep(30)
        #Pause the new connector
                u_2 = 'https://api.fivetran.com/v1' + '/connectors/' + cresponse['data']['id']
                pc = requests.patch(u_2,auth=a,json={"paused": True})
                print(Fore.GREEN + "Connector Paused")
        #Load the schema of the new connector
                u_3 = 'https://api.fivetran.com/v1' + cresponse['data']['id'] + "/schemas/reload"
                o = requests.post(u_3,auth=a)
                print(Fore.GREEN + "Connector Schema Loaded")
        #Configure the Schemas 
        #PATCH https://api.fivetran.com/v1/connectors/{connector_id}/schemas/{schema}
                sgroup_id = wgid
                ssmethod = 'PATCH'
                ssendpoint = 'connectors/' + cresponse['data']['id'] + '/schemas/hr' 
                sspayload = {
                                "enabled": True,
                                "tables": {
                                    "employees": {
                                        "enabled": True
                                    },
                                    "events": {
                                        "enabled": False
                                    }
                                }
                            }                            
                sresponse = atlas(ssmethod, ssendpoint, sspayload)
        #Sync the connectors
                if sresponse is not None:
                    print(Fore.MAGENTA + "Connector: " + cresponse['data']['id']  + " successfully configured in " + str(wgid))
        #Access to the destination must be granted first.
                    u_5 = 'https://api.fivetran.com/v1' + cresponse['data']['id'] + "/sync"
                    j = {"force": True} #initiate the sync
                    s = requests.post(u_5,auth=a,json=j)
                    print(Fore.GREEN + "Connector Sync Started")
#Execute a transformation
transformation_id = ''
tmethod = 'POST'
tendpoint = 'dbt/transformations/' + transformation_id + '/run'
tpayload = ''
#Submit Transfromation
tresponse = atlas(tmethod, tendpoint, tpayload)
#Review
if tresponse is not None:
   print(Fore.CYAN + 'Call: ' + tmethod + ' ' + tendpoint + ' ' + str(tpayload))
   print(Fore.GREEN +  'Response: ' + tresponse['code'])
   print(Fore.MAGENTA + str(tresponse))
#Script Complete
print(Fore.BLUE + 'Script Complete. Check resources')
