import json
import fileinput
import requests
from requests.auth import HTTPBasicAuth
import colorama
from colorama import Fore

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
b = "/code_to_update.sql"

#re-write sql file using metadata

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
connector_id = ''
method = 'GET'  #'POST' 'PATCH' 'DELETE' 'GET'
endpoint = 'metadata/connectors/' + connector_id + "/schemas"
payload = ''

#Submit
response = atlas(method, endpoint, payload)
#Review
if response is not None:
    print(Fore.GREEN +  'Atlas Response Code: ' + response['code'])
    #Define variables
    mu = "https://api.fivetran.com/v1/metadata/connectors/" 
    session = requests.Session()
    u_  = mu + "{}" + "/schemas"
    u_0 = mu + "{}" + "/tables"
    u__ = mu + "{}" + "/columns"
    sresponse=session.get(url=u_.format(connector_id), auth=a).json()
    tresponse=session.get(url=u_0.format(connector_id), auth=a).json()
    cresponse=session.get(url=u__.format(connector_id), auth=a).json()
    sdata_list =  sresponse['data']
    tdata_list =  tresponse['data']
    cdata_list =  cresponse['data']
    stimeline  =  sdata_list['items']
    timeline   =  tdata_list['items']
    ctimeline  =  cdata_list['items']
    #Begin
    try:
        for s in stimeline:
            with fileinput.FileInput(b,inplace=True) as file:
                    u = str(s['name_in_source'])
                    y = str(s['name_in_destination'])
                    for line in file:
                        print(line.replace(u,y),end='')
            for t in timeline:
                with fileinput.FileInput(b,inplace=True) as file:
                    w = str(t['name_in_source'])
                    o = str(t['name_in_destination'])
                    for line in file:
                        print(line.replace(w,o),end='')
                for c in ctimeline:
                    with fileinput.FileInput(b,inplace=True) as file:
                        e = str(c['name_in_source'])
                        f = str(c['name_in_destination'])
                        for line in file:
                            print(line.replace(e,f),end='')                      
    except:
        print(Fore.RED + "Error matching Metadata Elements. Review " + b)
#Fin
with open(b,"a") as g:
    g.write('\n' + '--Fivetran Metadata Normalized Query' +'\n' +'--Endpoints Utilized: ' + u_ + ' | ' + u_0 + ' | ' + u__)
print(Fore.GREEN + 'SQL Writer Response Code: ' + response['code'])
print(Fore.CYAN +  'SQL objects rewritten using metadata response data. Review ' + Fore.YELLOW + b)
