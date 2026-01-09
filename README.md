# Fivetran API Python Framework

This repository is owned and maintained by the [Fivetran Professional Services](https://www.fivetran.com/professional-services) team. It provides a Python-based framework for interacting with the Fivetran REST API to support automation, monitoring, and advanced integration workflows.

The repository also includes example solutions from customer engagements covering:

- AI-driven workflows
- HVR (High Volume Replication) automation
- MCP-based orchestration patterns
- Fivetran Connector SDK integrations

These examples are designed to be educational and practical, helping teams understand what’s possible with the Fivetran platform and accelerate real-world implementations.

If you’d like guidance, customization, or hands-on support, book a demo with [Fivetran Professional Services](https://go.fivetran.com/demo/services) to connect with our experts.

## Disclaimer

This repository is provided as a reference implementation to help users better understand and work with Fivetran automations. It is intended as a starting point for custom solutions that may require additional design, hardening, or scaling for enterprise use.

## Resources:
- [Fivetran API Docs](https://fivetran.com/docs/rest-api)
- [Postman Collection](https://fivetran.com/docs/rest-api/api-tools#fivetranpostmancollection)
- [REST API FAQs](https://fivetran.com/docs/rest-api/faq)

## Overview:
- The function 'atlas' is a general-purpose function to interact with the Fivetran API. It takes three parameters: method, endpoint, and payload.
- The method parameter determines the HTTP method to use (GET, POST, PATCH, DELETE). The endpoint parameter specifies the API endpoint to interact with. The payload parameter is used to send data in the case of POST or PATCH requests.
- The function constructs the full URL for the API request, sets up the headers including the authorization, and makes the request using the requests library.
- If the request is successful, it returns the JSON response. If the request fails, it prints an error message and returns None.
- The function uses exception handling to catch any errors that occur during the request and to raise an exception if the HTTP status code indicates an error.

## To use the framework, you will need to:

- Install libraries:
  - requests
  - json
  - colorama
- Set the api_key and api_secret variables to your Fivetran API credentials.
- Run the script with the desired method, endpoint, and payload.

## Example use cases:

- [Create New User](examples/api_interact_new_user.py)
- [Create New Team](examples/api_interact_new_team.py)
- [Create New Group Webhook](examples/api_interact_new_group_webhook.py)
- [Certificate Management](examples/api_interact_cert_mgmt.py)
- [Create New Connector](examples/api_interact_create_connector.py)
- [Resync Table](examples/api_interact_table_sync.py)
- [Create New Connector in Multiple Destinations](examples/api_interact_one_connector_in_many_destinations.py)
- [Create New Destinations](examples/api_interact_new_group_destination.py)
- [Run a dbt Transformation](examples/api_interact_run_transformation.py)
- [Update Connector Settings](examples/api_interact_main.py)
- [Delete Connector](examples/api_interact_delete_connector.py)
- [Get Connector Status](examples/api_interact_connector_status.py)
- [Get Connector Status using Pagination](examples/api_interact_status_pagination.py)
- [Get a list of connectors in a specific status](examples/api_interact_connection_status.py)
- [Manage Schema Configurations](examples/api_interact_schema_edit.py)
- [Pause or Sync connectors and log the action(s)](examples/api_interact_main_log.py)
- [Re-write SQL files](examples/api_interact_sql_writer.py)

# Example: api_interact_connection_status.py

This Python script is designed to interact with an API, specifically the Fivetran API, to retrieve and display the status of connectors. It uses the requests library to send HTTP requests and the colorama library to colorize the output.
Step-by-step Breakdown

## 1. Import necessary libraries: 
The script begins by importing the necessary Python libraries. These include requests for making HTTP requests, json for handling JSON data, and colorama for colorizing the terminal output.
```python
        import requests
        from requests.auth import HTTPBasicAuth
        import json
        import colorama
        from colorama import Fore, Back, Style
```
## 2. Define the atlas function: 
This function is used to send HTTP requests to the Fivetran API. It takes three parameters: method (the HTTP method), endpoint (the API endpoint), and payload (the request body for POST and PATCH requests). It constructs the request, sends it, and returns the response as a JSON object.
```python
        def atlas(method, endpoint, payload):
        base_url = 'https://api.fivetran.com/v1'
        h = {
        'Authorization': f'Bearer {api_key}:{api_secret}'
        }
        url = f'{base_url}/{endpoint}'
        ...
```
## 3. Specify the request parameters: 
The script then specifies the parameters for the API request. In this case, it's making a GET request to the 'groups/{group_id}/connectors' endpoint.
```python
        group_id = ''
        method = 'GET'
        endpoint = 'groups/' + group_id + '/connectors'
        payload = ''
```  
## 4. Call the atlas function: 
The script calls the atlas function with the specified parameters and stores the response.
   ```python
   response = atlas(method, endpoint, payload)
```
## 5. Process and display the response:
Finally, the script checks if the response is not None, prints the request and response details, and iterates over the 'items' in the response data, printing the 'service', 'sync_state', and 'sync_frequency' for each item.
```python
     if response is not None:
      print(Fore.CYAN + 'Call: ' + method + ' ' + endpoint + ' ' + str(payload))
      print(Fore.GREEN + 'Response: ' + response['code'])
      cdata_list =  response['data']
      ctimeline  =  cdata_list['items']
  for c in ctimeline:
      print(Fore.MAGENTA + 'Type:' + c['service'] + Fore.BLUE + ' Status:' + c['status']['sync_state'] + Fore.YELLOW + ' Frequency:' + str(c['sync_frequency']))
```
# Example: api_interact_main_log.py
This Python script is designed to interact with an API, specifically the Fivetran API, to pause a given connector and log the actions. It uses the requests library to send HTTP requests and the colorama library to colorize the output.
Step-by-step Breakdown

## 1. Import necessary modules: 
  ```python
    import requests
    from requests.auth import HTTPBasicAuth
    import json
    import colorama
    from colorama import Fore
    import os
    import logging
    from logging.handlers import RotatingFileHandler
```
## 2. Define the atlas function: 
This function is used to make HTTP requests to the Fivetran API. It takes three parameters: the HTTP method (GET, POST, PATCH, DELETE), the API endpoint, and the payload (data to send with the request). The function constructs the request, sends it, and logs the result.
  ```python
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
 ```  
## 3. Set up logging: 
The script sets up a logger that writes to a file (api_framework.log). If the log file exceeds 10MB, it is overwritten. The logger is set to log INFO level messages and above. A rotating file handler is added to the logger, which keeps the last 3 log files when the current log file reaches 10MB.
  ```python
     log_file = "/api_framework.log"
     log_size = 10 * 1024 * 1024  # 10 MB
      
      #Check if the log file size exceeds 10MB
      if os.path.exists(log_file) and os.path.getsize(log_file) >= log_size:
          # If it does, overwrite the file
          open(log_file, 'w').close()
      
      logger = logging.getLogger(__name__)
      logger.setLevel(logging.INFO)
      
      #Add a rotating handler
      handler = RotatingFileHandler(log_file, maxBytes=log_size, backupCount=3)
      logger.addHandler(handler)
```
## 4. Make a request: 
The script constructs a request to the Fivetran API to pause a connector (identified by connector_id). The HTTP method is PATCH, the endpoint is connectors/{connector_id}, and the payload is {"paused": True}.
  ```python
      connector_id = ''
      method = 'PATCH'  #'POST' 'PATCH' 'DELETE' 'GET'
      endpoint = 'connectors/' + connector_id 
      payload = {"paused": True}
      
      #Submit
      response = atlas(method, endpoint, payload)
```
## 5. Handle the response: 
The script calls the atlas function to send the request and get the response. If the response is not None, it prints the request details and response in different colors. In this example, we successfully paused a connector and logged the activity.
  ```python
      if response is not None:
        print(Fore.CYAN + 'Call: ' + method + ' ' + endpoint + ' ' + str(payload))
        print(Fore.GREEN +  'Response: ' + response['code'])
        print(Fore.MAGENTA + str(response))
```

## Questions?
[Book a consultation with a Fivetran Services expert.](https://go.fivetran.com/demo/services?_gl=1*18htro9*_ga*MTUxNDcyNDcxMy4xNjY5OTA2MDg3*_ga_NE72Z5F3GB*MTY5NDA5OTIzNC4xMDU1LjEuMTY5NDA5OTc1MC40NC4wLjA.) 




## Credits 
- This solution was written by: Elijah Davis
- Special Thanks: Angel Hernandez & Jimmy Hooker
