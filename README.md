# Fivetran api_framework
Fivetran API Python Script

This script provides a simple way to interact with the Fivetran API using Python. It can be used to check connector details, pause connectors, or sync connectors.

## To use the script, you will need to:

- Install libraries:
  - requests
  - json
  - colorama
- Set the api_key and api_secret variables to your Fivetran API credentials.
- Run the script with the desired method, endpoint, and payload.

## For example, to pause a connector, you would run the script like this:

>python atlas.py PATCH connectors/my-connector-id paused=True

## Other use cases include:
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
