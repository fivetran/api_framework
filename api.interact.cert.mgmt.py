import requests
from requests.auth import HTTPBasicAuth
import json

api_key = ''
api_secret = ''
a = HTTPBasicAuth(api_key, api_secret)

#automate certificates

def atlas(method, endpoint, payload):

    base_url = 'https://api.fivetran.com/v1'
    h = {
        'Authorization': f'Bearer {api_key}:{api_secret}'
    }
    url = f'{base_url}/{endpoint}'

    try:
        if method == 'GET':
            response = requests.get(url, headers=h, auth=a, params=p)
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
group_id = ''
method = 'GET'  #'POST' 'PATCH' 'DELETE'
endpoint = 'groups/' + group_id + '/connectors'
payload = ''
limit =      #example 1-1000
p = {"limit": limit}

#Submit
response = atlas(method, endpoint, payload)
#Process
if response is not None:
    conn_list = response["data"]['items']

    while "next_cursor" in response["data"]:
        print("paged")
        params = {"limit": limit, "cursor": response["data"]["next_cursor"]}
        url = "https://api.fivetran.com/v1/groups/{}/connectors".format(group_id)
        response_paged = requests.get(url=url, auth=a, params=params).json()
        if any(response_paged["data"]["items"]) == True:
            conn_list.extend(response_paged["data"]['items'])
        response = response_paged

    for conn in conn_list:
        print("Connector " + conn["schema"] + " has status: " + conn["status"]["setup_state"])
        if conn["status"]["setup_state"] == 'broken':
            print(">>> Running setup tests for " + conn["schema"])
            conn_url = "https://api.fivetran.com/v1/connectors/{}/test".format(conn["id"])
            response = requests.post(url=conn_url, auth=a, json={"trust_certificates": True,"trust_fingerprints": True}).json()
            print("")
            print("Test Results:")
            for test in response['data']['setup_tests']:
                print(test["title"]+ ": " +test["status"])
                if test["status"] == "FAILED":
                    print(test["message"])
            print("")
        else: 
            print(">>> Skipping tests")
