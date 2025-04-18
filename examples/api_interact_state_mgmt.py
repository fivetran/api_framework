import requests
import json
import base64
import colorama
from colorama import Fore, Style

# Configuration file
r = '/config.json'
with open(r, "r") as i:
    l = i.read()
    y = json.loads(l)
api_key = y['fivetran']['api_key']
api_secret = y['fivetran']['api_secret']


# README https://github.com/fivetran/api_framework/blob/main/examples/connector_sdk_state_mgmt_README.md

# Atlas function for API interactions
def atlas(method, endpoint, payload=None):
    # Base64 authentication
    base_url = 'https://api.fivetran.com/v1'
    credentials = f'{api_key}:{api_secret}'.encode('utf-8')
    b64_credentials = base64.b64encode(credentials).decode('utf-8')
    h = {
        'Authorization': f'Basic {b64_credentials}',
        'Accept': 'application/json;version=2'
    }
    url = f'{base_url}/{endpoint}'

    try:
        if method == 'GET':
            response = requests.get(url, headers=h, json=payload)
        elif method == 'POST':
            response = requests.post(url, headers=h, json=payload)
        elif method == 'PATCH':
            response = requests.patch(url, headers=h, json=payload)
        elif method == 'DELETE':
            response = requests.delete(url, headers=h, json=payload)
        else:
            raise ValueError('Invalid request method.')

        response.raise_for_status()  # Raise exception for 4xx or 5xx responses
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f'Request failed: {e}')
        return None

def main():
    # Initialize colorama
    colorama.init(autoreset=True)
    
    # Get connection ID from user
    connection_id = input(Fore.CYAN + "Enter connection ID: ")
    
    # Step 1: GET the current state
    print(Fore.CYAN + "\n=== Getting current connection state ===")
    endpoint = f'connections/{connection_id}/state'
    current_state = atlas('GET', endpoint)
    
    if current_state:
        print(Fore.GREEN + "Current state retrieved successfully:")
        print(json.dumps(current_state, indent=4))
        
        # Step 2: Allow user to input new state
        print(Fore.CYAN + "\n=== Enter new state information ===")
        print("Enter the new state as a JSON dictionary (e.g., {\"cursor\": \"2025-03-06 20:20:20\"}):")
        print("Or press Enter to keep current state:")
        user_input = input()
        
        if user_input.strip():
            try:
                # Try to parse as JSON directly
                try:
                    new_state_value = json.loads(user_input)
                except json.JSONDecodeError:
                    # If direct parsing fails, try to fix common input errors
                    if ":" in user_input and not user_input.strip().startswith("{"):
                        # User likely entered a key-value pair without braces
                        fixed_input = "{" + user_input + "}"
                        new_state_value = json.loads(fixed_input)
                    else:
                        raise  # Re-raise the exception if we can't fix it
                
                # Step 3: Pause the connection first
                print(Fore.CYAN + "\n=== Pausing connection ===")
                pause_endpoint = f'connections/{connection_id}'
                pause_payload = {"paused": True}
                pause_response = atlas('PATCH', pause_endpoint, pause_payload)
                
                if pause_response:
                    print(Fore.GREEN + "Connection paused successfully")
                    
                    # Step 4: PATCH to update the state
                    print(Fore.CYAN + "\n=== Updating connection state ===")
                    state_payload = {"state": new_state_value}
                    update_response = atlas('PATCH', endpoint, state_payload)
                    
                    if update_response:
                        print(Fore.GREEN + "State updated successfully:")
                        print(json.dumps(update_response, indent=4))
                        
                        # Step 5: Resume the connection
                        print(Fore.CYAN + "\n=== Resuming connection ===")
                        resume_payload = {"paused": False}
                        resume_response = atlas('PATCH', pause_endpoint, resume_payload)
                        
                        if resume_response:
                            print(Fore.GREEN + "Connection resumed successfully")
                        else:
                            print(Fore.RED + "Failed to resume connection.")
                        
                        # Step 6: GET to verify the update
                        print(Fore.CYAN + "\n=== Verifying updated state ===")
                        updated_state = atlas('GET', endpoint)
                        
                        if updated_state:
                            print(Fore.GREEN + "Updated state verified:")
                            print(json.dumps(updated_state, indent=4))
                        else:
                            print(Fore.RED + "Failed to verify updated state.")
                    else:
                        print(Fore.RED + "Failed to update state.")
                        
                        # Try to resume the connection even if state update failed
                        print(Fore.CYAN + "\n=== Resuming connection ===")
                        resume_payload = {"paused": False}
                        resume_response = atlas('PATCH', pause_endpoint, resume_payload)
                else:
                    print(Fore.RED + "Failed to pause connection.")
            except json.JSONDecodeError:
                print(Fore.RED + "Invalid JSON format. State update cancelled.")
        else:
            print(Fore.YELLOW + "No changes made to state.")
    else:
        print(Fore.RED + "Failed to retrieve current state.")

if __name__ == "__main__":
    main()
