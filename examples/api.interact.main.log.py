import requests
from requests.auth import HTTPBasicAuth
import json
import colorama
from colorama import Fore
import os
import logging
from logging.handlers import RotatingFileHandler

api_key = ''
api_secret = ''
a = HTTPBasicAuth(api_key, api_secret)

#logging example for pausing a connector

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

        logger.info(f'Successful {method} request to {url}')
        return response.json()
    except requests.exceptions.RequestException as e:
        logger.error(f'Request failed: {e}')
        print(f'Request failed: {e}')
        return None


#logger
log_file = "/api_framework.log"
log_size = 10 * 1024 * 1024  # 10 MB

# Check if the log file size exceeds 10MB
if os.path.exists(log_file) and os.path.getsize(log_file) >= log_size:
    # If it does, overwrite the file
    open(log_file, 'w').close()

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Add a rotating handler
handler = RotatingFileHandler(log_file, maxBytes=log_size, backupCount=3)
logger.addHandler(handler)

#Request
connector_id = ''
method = 'PATCH'  #'POST' 'PATCH' 'DELETE' 'GET'
endpoint = 'connectors/' + connector_id 
payload = {"paused": True}

#Submit
response = atlas(method, endpoint, payload)
#Review
if response is not None:
    print(Fore.CYAN + 'Call: ' + method + ' ' + endpoint + ' ' + str(payload))
    print(Fore.GREEN +  'Response: ' + response['code'])
    print(Fore.MAGENTA + str(response))
