# Fivetran api_framework
Fivetran API Python Script

This script provides a simple way to interact with the Fivetran API using Python. It can be used to leverage information from any endpoint. Test out a few of the example use cases and determine the best path forward for automating Fivetran activity/monitoring.

- The function atlas is a general-purpose function to interact with the Fivetran API. It takes three parameters: method, endpoint, and payload.
- The method parameter determines the HTTP method to use (GET, POST, PATCH, DELETE). The endpoint parameter specifies the API endpoint to interact with. The payload parameter is used to send data in the case of POST or PATCH requests.
- The function constructs the full URL for the API request, sets up the headers including the authorization, and makes the request using the requests library.
- If the request is successful, it returns the JSON response. If the request fails, it prints an error message and returns None.
- The function uses exception handling to catch any errors that occur during the request and to raise an exception if the HTTP status code indicates an error.

## To use the script, you will need to:

- Install libraries:
  - requests
  - json
  - colorama
- Set the api_key and api_secret variables to your Fivetran API credentials.
- Run the script with the desired method, endpoint, and payload.

## For example, to pause a connector, you would run the script like this:

>python atlas.py PATCH connectors/my-connector-id paused=True

## Example use cases:
- Create new connectors
- Create new connectors in multiple destinations (1:many)
- Update connector settings
- Certificate management
- Delete connectors
- Get connector status
- Get a list of all connectors in a specific status
- Create new destinations
- Manage schema configurations
- Re-write SQL files


## Example: api.interact.connection.status.py

This Python script is designed to interact with an API, specifically the Fivetran API, to retrieve and display the status of connectors. It uses the requests library to send HTTP requests and the colorama library to colorize the output.
Step-by-step Breakdown

1. Import necessary libraries: The script begins by importing the necessary Python libraries. These include requests for making HTTP requests, json for handling JSON data, and colorama for colorizing the terminal output.
   ```python
    import requests
    from requests.auth import HTTPBasicAuth
    import json
    import colorama
    from colorama import Fore, Back, Style

2. Define the atlas function: This function is used to send HTTP requests to the Fivetran API. It takes three parameters: method (the HTTP method), endpoint (the API endpoint), and payload (the request body for POST and PATCH requests). It constructs the request, sends it, and returns the response as a JSON object.
   ```python
       def atlas(method, endpoint, payload):
        base_url = 'https://api.fivetran.com/v1'
        h = {
            'Authorization': f'Bearer {api_key}:{api_secret}'
        }
        url = f'{base_url}/{endpoint}'
        ...

3. Specify the request parameters: The script then specifies the parameters for the API request. In this case, it's making a GET request to the 'groups/{group_id}/connectors' endpoint.
   ```python
    group_id = ''
    method = 'GET'
    endpoint = 'groups/' + group_id + '/connectors'
    payload = ''
   
4. Call the atlas function: The script calls the atlas function with the specified parameters and stores the response.
   ```python
   response = atlas(method, endpoint, payload)
   
5. Process and display the response: Finally, the script checks if the response is not None, prints the request and response details, and iterates over the 'items' in the response data, printing the 'service', 'sync_state', and 'sync_frequency' for each item.
  ```python
       if response is not None:
        print(Fore.CYAN + 'Call: ' + method + ' ' + endpoint + ' ' + str(payload))
        print(Fore.GREEN + 'Response: ' + response['code'])
        cdata_list =  response['data']
        ctimeline  =  cdata_list['items']
    for c in ctimeline:
        print(Fore.MAGENTA + 'Type:' + c['service'] + Fore.BLUE + ' Status:' + c['status']['sync_state'] + Fore.YELLOW + ' Frequency:' + str(c['sync_frequency']))
   
