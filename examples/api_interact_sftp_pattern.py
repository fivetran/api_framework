import requests
from requests.auth import HTTPBasicAuth
import json
import colorama
from colorama import Fore, Back, Style
import time
import datetime
from datetime import datetime, timedelta


#configuration file for key,secret,params,etc.
r = ‘=config.json'
with open(r, "r") as i:
    l = i.read()
    y = json.loads(l)
api_key = y['fivetran']['api_key']
api_secret = y['fivetran']['api_secret']
a = HTTPBasicAuth(api_key, api_secret)

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
        response.raise_for_status()  # Raise exception for 4xx or 5xx responses
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f'Request failed: {e}')
        return None

current_day = '_112023'
day = int(current_day[3:5])
month = int(current_day[1:3])
year = int(current_day[5:])

# Calculate the next day
current_date = datetime(year, month, day)
next_date = current_date + timedelta(days=1)

# Format the updated day in _MMDDYY format
updated_day = next_date.strftime("_%m%d%y")
print(updated_day)

#Request
method = 'GET'
endpoint = 'connectors/' + y['fivetran']['c']
payload = ''
t = {"config":{"pattern": str(updated_day) + "-\\d{6}.csv"}}

#Submit
response = atlas(method, endpoint, payload)

#Review
if response is not None:
    
    stat = response['data']['config']['pattern']
    print(stat)
    if stat != 'syncing':
        mu = "https://api.fivetran.com/v1/connectors/"
        modi = mu + y['fivetran']['c']
        #activate
        sz = requests.patch(modi,auth=a,json=t)
        time.sleep(3)
        print("Connector active")
        #sw = requests.patch(modi,auth=a,json=m)
    statupdt = atlas(method, endpoint, payload)
    stat2 = statupdt['data']['config']['pattern']
    print(stat2)
