import requests
from requests.auth import HTTPBasicAuth
import json
import colorama
from colorama import Fore

api_key = ''
api_secret = ''
a = HTTPBasicAuth(api_key, api_secret)

def atlas(method, endpoint, payload):
    
    # Base URL for the Fivetran API
    base_url = 'https://api.fivetran.com/v1'
    
    # Set up headers with authorization
    h = {
        'Authorization': f'Bearer {api_key}:{api_secret}'
    }
    # Construct full URL
    url = f'{base_url}/{endpoint}'

    try:
        # Make the request using the specified method
        # If method is not one of the expected values, raise a ValueError
        # If the request fails for any reason, catch the exception and print an error message
        # If the HTTP status code indicates an error, raise an exception
        # If the request is successful, return the JSON response
        
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


if __name__ == '__main__':
    
    #define parameters
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
