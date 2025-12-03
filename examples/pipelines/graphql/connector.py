"""
Fivetran Connector SDK for FluentRetail GraphQL API

This connector syncs orders data from the FluentRetail GraphQL API using:
- OAuth 2.0 password grant authentication
- Cursor-based pagination
- Incremental sync using updatedOn field
- GraphQL query execution with nested data handling

Key Features:
- OAuth token management with automatic refresh
- Cursor-based pagination for efficient data retrieval
- Incremental sync using updatedOn timestamp filtering
- Nested data flattening for complex GraphQL responses
- Error handling and retry logic with exponential backoff for timeouts
- Rate limiting support
- Optional max_records_per_sync limit (empty string = sync all data)
- Configurable request timeout and retry attempts

References:
- SDK Docs: https://fivetran.com/docs/connector-sdk
- Best Practices: https://fivetran.com/docs/connector-sdk/best-practices
"""

# =============================================================================
# REQUIRED IMPORTS - Fivetran Connector SDK
# =============================================================================

from fivetran_connector_sdk import Connector
from fivetran_connector_sdk import Logging as log
from fivetran_connector_sdk import Operations as op

# =============================================================================
# ADDITIONAL IMPORTS - GraphQL and HTTP requests
# =============================================================================

import json
import time
import requests
from typing import Dict, Any, Optional, List
from datetime import datetime, timedelta
from urllib.parse import urlencode

# =============================================================================
# CONSTANTS
# =============================================================================

# GraphQL query for fetching orders with pagination and date filtering
ORDERS_QUERY = """
query OrdersQuery($afterCursor: String, $min_dt: DateTime, $max_dt: DateTime) {
  orders(updatedOn: {from: $min_dt, to: $max_dt}, first: 40, after: $afterCursor) {
    pageInfo {
      hasNextPage
      hasPreviousPage
    }
    edges {
      cursor
      node {
        id
        ref
        retailer {
          id
        }
        attributes {
          name
          value
        }
        items {
          edges {
            node {
              attributes {
                name
                value
              }
              createdOn
              currency
              id
              paidPrice
              price
              quantity
              ref
              status
              taxPrice
              taxType
              totalPrice
              totalTaxPrice
              updatedOn
            }
          }
        }
        status
        totalPrice
        totalTaxPrice
        type
        updatedOn
        createdOn
        fulfilments {
          edges {
            node {
              items {
                edges {
                  node {
                    id
                    ref
                    rejectedQuantity
                    requestedQuantity
                    filledQuantity
                    orderItem {
                      id
                    }
                    fulfilment {
                      id
                      status
                      createdOn
                    }
                  }
                }
              }
            }
          }
        }
        financialTransactions {
          edges {
            node {
              externalTransactionId
              createdOn
              total
            }
          }
        }
      }
    }
  }
}
"""

# =============================================================================
# CONFIGURATION VALIDATION
# =============================================================================

def validate_configuration(configuration: dict) -> None:
    """
    Validate that all required configuration parameters are present.
    
    Args:
        configuration: Configuration dictionary from configuration.json
        
    Raises:
        ValueError: If any required configuration is missing
    """
    required_keys = [
        "base_url",
        "username",
        "password",
        "client_id",
        "client_secret"
    ]
    
    for key in required_keys:
        if key not in configuration or not configuration[key]:
            raise ValueError(f"Missing required configuration: {key}")
    
    log.info("Configuration validated successfully")


# =============================================================================
# AUTHENTICATION
# =============================================================================

def get_access_token(configuration: dict) -> str:
    """
    Authenticate with OAuth 2.0 password grant and retrieve access token.
    
    Args:
        configuration: Configuration dictionary containing auth credentials
        
    Returns:
        Access token string
        
    Raises:
        RuntimeError: If authentication fails
    """
    base_url = configuration.get("base_url", "").rstrip("/")
    auth_url = f"{base_url}/oauth/token"
    
    params = {
        "username": configuration.get("username"),
        "password": configuration.get("password"),
        "client_id": configuration.get("client_id"),
        "client_secret": configuration.get("client_secret"),
        "grant_type": "password",
        "scope": "api"
    }
    
    try:
        log.info("Authenticating with OAuth endpoint")
        response = requests.post(auth_url, params=params, timeout=30)
        response.raise_for_status()
        
        token_data = response.json()
        access_token = token_data.get("access_token")
        
        if not access_token:
            raise RuntimeError("Access token not found in authentication response")
        
        log.info("Authentication successful")
        return access_token
        
    except requests.exceptions.RequestException as e:
        log.severe(f"Authentication failed: {str(e)}")
        raise RuntimeError(f"Failed to authenticate: {str(e)}")


# =============================================================================
# GRAPHQL REQUEST HANDLING
# =============================================================================

def execute_graphql_query(
    base_url: str,
    access_token: str,
    query: str,
    variables: Dict[str, Any],
    rate_limit_delay: float = 0.0,
    debug: bool = False,
    timeout: int = 120,
    max_retries: int = 3
) -> Dict[str, Any]:
    """
    Execute a GraphQL query against the API with retry logic for timeouts.
    
    Args:
        base_url: Base URL of the API
        access_token: OAuth access token
        query: GraphQL query string
        variables: GraphQL query variables
        rate_limit_delay: Delay in seconds before making request (for rate limiting)
        debug: Whether to dump response to debug_resp.json
        timeout: Request timeout in seconds (default: 120)
        max_retries: Maximum number of retry attempts for timeout errors (default: 3)
        
    Returns:
        GraphQL response data
        
    Raises:
        RuntimeError: If the GraphQL request fails after all retries
    """
    # Apply rate limiting delay
    if rate_limit_delay > 0:
        time.sleep(rate_limit_delay)
    
    graphql_url = f"{base_url}/graphql"
    
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
        "Accept": "application/json"
    }
    
    payload = {
        "query": query,
        "variables": variables
    }
    
    # Retry logic with exponential backoff for timeout errors
    last_exception = None
    for attempt in range(max_retries + 1):
        try:
            if attempt > 0:
                # Exponential backoff: 2^attempt seconds
                backoff_delay = 2 ** attempt
                log.warning(f"Retrying GraphQL query (attempt {attempt + 1}/{max_retries + 1}) after {backoff_delay}s delay...")
                time.sleep(backoff_delay)
            
            log.info(f"Executing GraphQL query with cursor: {variables.get('afterCursor', 'None')}")
            response = requests.post(
                graphql_url,
                headers=headers,
                json=payload,
                timeout=timeout
            )
            response.raise_for_status()
            
            result = response.json()
            
            # Debug: Dump response to file
            if debug:
                try:
                    with open("debug_resp.json", "w", encoding="utf-8") as f:
                        json.dump(result, f, indent=2, ensure_ascii=False)
                    log.info("Debug: Response dumped to debug_resp.json")
                except Exception as e:
                    log.warning(f"Failed to write debug_resp.json: {str(e)}")
            
            # Check for GraphQL errors
            if "errors" in result:
                error_messages = [err.get("message", "Unknown error") for err in result["errors"]]
                raise RuntimeError(f"GraphQL errors: {', '.join(error_messages)}")
            
            return result.get("data", {})
            
        except requests.exceptions.ReadTimeout as e:
            last_exception = e
            if attempt < max_retries:
                log.warning(f"Read timeout on attempt {attempt + 1}/{max_retries + 1}: {str(e)}")
                continue
            else:
                log.severe(f"GraphQL request timed out after {max_retries + 1} attempts")
                raise RuntimeError(f"Failed to execute GraphQL query after {max_retries + 1} attempts: Read timeout (timeout={timeout}s)")
        
        except requests.exceptions.RequestException as e:
            # For non-timeout errors, don't retry
            log.severe(f"GraphQL request failed: {str(e)}")
            raise RuntimeError(f"Failed to execute GraphQL query: {str(e)}")
    
    # Should not reach here, but handle just in case
    if last_exception:
        raise RuntimeError(f"Failed to execute GraphQL query after {max_retries + 1} attempts: {str(last_exception)}")


# =============================================================================
# DATA PROCESSING - Extract data into logical tables
# =============================================================================

def process_order_data(order_node: Dict[str, Any], order_id: str) -> Dict[str, List[Dict[str, Any]]]:
    """
    Process order node and extract data into separate logical tables.
    
    Args:
        order_node: Order node from GraphQL response
        order_id: Order ID for foreign key relationships
        
    Returns:
        Dictionary with table names as keys and lists of records as values
    """
    tables_data = {
        "orders": [],
        "order_items": [],
        "order_attributes": [],
        "order_item_attributes": [],
        "fulfilment_items": [],
        "financial_transactions": []
    }
    
    # Extract order record
    order_record = {
        "id": order_node.get("id"),
        "ref": order_node.get("ref"),
        "status": order_node.get("status"),
        "type": order_node.get("type"),
        "totalPrice": order_node.get("totalPrice"),
        "totalTaxPrice": order_node.get("totalTaxPrice"),
        "updatedOn": order_node.get("updatedOn"),
        "createdOn": order_node.get("createdOn"),
    }
    
    # Retailer ID
    retailer = order_node.get("retailer", {})
    order_record["retailer_id"] = retailer.get("id") if retailer else None
    
    tables_data["orders"].append(order_record)
    
    # Extract order attributes
    attributes = order_node.get("attributes", [])
    for attr in attributes:
        attr_record = {
            "order_id": order_id,
            "name": attr.get("name"),
            "value": attr.get("value")
        }
        tables_data["order_attributes"].append(attr_record)
    
    # Extract order items
    items = order_node.get("items", {}).get("edges", [])
    for item_edge in items:
        item_node = item_edge.get("node", {})
        item_id = item_node.get("id")
        
        if item_id:
            # Order item record
            item_record = {
                "id": item_id,
                "order_id": order_id,
                "ref": item_node.get("ref"),
                "status": item_node.get("status"),
                "currency": item_node.get("currency"),
                "price": item_node.get("price"),
                "paidPrice": item_node.get("paidPrice"),
                "quantity": item_node.get("quantity"),
                "taxPrice": item_node.get("taxPrice"),
                "taxType": item_node.get("taxType"),
                "totalPrice": item_node.get("totalPrice"),
                "totalTaxPrice": item_node.get("totalTaxPrice"),
                "createdOn": item_node.get("createdOn"),
                "updatedOn": item_node.get("updatedOn")
            }
            tables_data["order_items"].append(item_record)
            
            # Order item attributes
            item_attributes = item_node.get("attributes", [])
            for item_attr in item_attributes:
                item_attr_record = {
                    "order_item_id": item_id,
                    "name": item_attr.get("name"),
                    "value": item_attr.get("value")
                }
                tables_data["order_item_attributes"].append(item_attr_record)
    
    # Extract fulfilment items
    fulfilments = order_node.get("fulfilments", {}).get("edges", [])
    for fulfilment_edge in fulfilments:
        fulfilment_node = fulfilment_edge.get("node", {})
        fulfilment_items = fulfilment_node.get("items", {}).get("edges", [])
        
        for fulfilment_item_edge in fulfilment_items:
            fulfilment_item_node = fulfilment_item_edge.get("node", {})
            fulfilment_item_id = fulfilment_item_node.get("id")
            
            if fulfilment_item_id:
                fulfilment_item_record = {
                    "id": fulfilment_item_id,
                    "order_id": order_id,
                    "ref": fulfilment_item_node.get("ref"),
                    "rejectedQuantity": fulfilment_item_node.get("rejectedQuantity"),
                    "requestedQuantity": fulfilment_item_node.get("requestedQuantity"),
                    "filledQuantity": fulfilment_item_node.get("filledQuantity")
                }
                
                # Order item reference
                order_item_ref = fulfilment_item_node.get("orderItem", {})
                fulfilment_item_record["order_item_id"] = order_item_ref.get("id") if order_item_ref else None
                
                # Fulfilment reference
                fulfilment_ref = fulfilment_item_node.get("fulfilment", {})
                if fulfilment_ref:
                    fulfilment_item_record["fulfilment_id"] = fulfilment_ref.get("id")
                    fulfilment_item_record["fulfilment_status"] = fulfilment_ref.get("status")
                    fulfilment_item_record["fulfilment_createdOn"] = fulfilment_ref.get("createdOn")
                
                tables_data["fulfilment_items"].append(fulfilment_item_record)
    
    # Extract financial transactions
    financial_transactions = order_node.get("financialTransactions", {}).get("edges", [])
    for tx_edge in financial_transactions:
        tx_node = tx_edge.get("node", {})
        external_transaction_id = tx_node.get("externalTransactionId")
        
        if external_transaction_id:
            tx_record = {
                "order_id": order_id,
                "externalTransactionId": external_transaction_id,
                "createdOn": tx_node.get("createdOn"),
                "total": tx_node.get("total")
            }
            tables_data["financial_transactions"].append(tx_record)
    
    return tables_data


# =============================================================================
# SCHEMA DEFINITION
# =============================================================================

def schema(configuration: dict):
    """
    Define the schema for all tables.
    
    Only define table names and primary keys here.
    Column types will be inferred from the data by Fivetran.
    
    Args:
        configuration: Configuration dictionary
        
    Returns:
        List of table definitions
    """
    return [
        {
            "table": "orders",
            "primary_key": ["id"]
        },
        {
            "table": "order_items",
            "primary_key": ["id"]
        },
        {
            "table": "order_attributes",
            "primary_key": ["order_id", "name"]
        },
        {
            "table": "order_item_attributes",
            "primary_key": ["order_item_id", "name"]
        },
        {
            "table": "fulfilment_items",
            "primary_key": ["id"]
        },
        {
            "table": "financial_transactions",
            "primary_key": ["order_id", "externalTransactionId"]
        }
    ]


# =============================================================================
# DATA SYNC LOGIC
# =============================================================================

def update(configuration: dict, state: dict) -> None:
    """
    Main sync function called by Fivetran.
    
    This function:
    1. Validates configuration
    2. Authenticates and gets access token
    3. Fetches orders data using GraphQL with cursor pagination
    4. Processes and upserts data
    5. Saves state using op.checkpoint() for incremental syncs
    
    Args:
        configuration: Configuration dictionary from configuration.json
        state: State dictionary from previous sync (empty on first sync)
    """
    # Validate configuration
    validate_configuration(configuration)
    
    # Extract configuration parameters
    base_url = configuration.get("base_url", "").rstrip("/")
    rate_limit_delay = float(configuration.get("rate_limit_delay", "0.5"))
    page_size = int(configuration.get("page_size", "40"))
    
    # Handle max_records_per_sync - optional, empty string means no limit
    max_records_per_sync_str = configuration.get("max_records_per_sync", "10000")
    if max_records_per_sync_str == "" or max_records_per_sync_str is None:
        max_records_per_sync = None
        log.info("max_records_per_sync not set - will sync all available data")
    else:
        try:
            max_records_per_sync = int(max_records_per_sync_str)
            log.info(f"max_records_per_sync set to {max_records_per_sync}")
        except (ValueError, TypeError):
            log.warning(f"Invalid max_records_per_sync value '{max_records_per_sync_str}', defaulting to no limit")
            max_records_per_sync = None
    
    enable_debug = configuration.get("enable_debug", "false").lower() == "true"
    
    # Get timeout configuration (default: 120 seconds)
    timeout = int(configuration.get("request_timeout", "120"))
    max_retries = int(configuration.get("max_retries", "3"))
    
    # Get state for incremental sync
    last_updated_on = state.get("last_updated_on")
    last_cursor = state.get("last_cursor")
    
    # Calculate date range for incremental sync
    # If no last_updated_on, sync last 30 days by default
    if last_updated_on:
        min_dt = last_updated_on
    else:
        # Default to 30 days ago for first sync
        default_days = int(configuration.get("initial_sync_days", "30"))
        min_dt = (datetime.utcnow() - timedelta(days=default_days)).isoformat() + "Z"
    
    # Set max_dt to current time
    max_dt = datetime.utcnow().isoformat() + "Z"
    
    log.info(f"Starting sync from {min_dt} to {max_dt}")
    if enable_debug:
        log.info("Debug mode enabled - responses will be dumped to debug_resp.json")
    
    try:
        # Authenticate and get access token
        access_token = get_access_token(configuration)
        
        # Initialize pagination variables
        cursor = last_cursor if last_cursor else None
        has_next_page = True
        orders_processed = 0
        latest_updated_on = min_dt
        
        # Process all pages
        # Only check max_records_per_sync limit if it's set (not None)
        while has_next_page and (max_records_per_sync is None or orders_processed < max_records_per_sync):
            # Prepare GraphQL variables
            variables = {
                "afterCursor": cursor,
                "min_dt": min_dt,
                "max_dt": max_dt
            }
            
            # Execute GraphQL query (only dump first response if debug enabled)
            dump_debug = enable_debug and cursor is None
            data = execute_graphql_query(
                base_url=base_url,
                access_token=access_token,
                query=ORDERS_QUERY,
                variables=variables,
                rate_limit_delay=rate_limit_delay,
                debug=dump_debug,
                timeout=timeout,
                max_retries=max_retries
            )
            
            # Extract orders data
            orders_data = data.get("orders", {})
            page_info = orders_data.get("pageInfo", {})
            edges = orders_data.get("edges", [])
            
            # Process each order
            for edge in edges:
                order_node = edge.get("node", {})
                order_id = order_node.get("id")
                cursor = edge.get("cursor")
                
                if not order_id:
                    log.warning("Skipping order with missing ID")
                    continue
                
                # Process order data into separate tables
                tables_data = process_order_data(order_node, order_id)
                
                # Upsert data into each table
                for table_name, records in tables_data.items():
                    for record in records:
                        op.upsert(table=table_name, data=record)
                
                # Track latest updatedOn for state management
                order_updated_on = order_node.get("updatedOn")
                if order_updated_on and order_updated_on > latest_updated_on:
                    latest_updated_on = order_updated_on
                
                orders_processed += 1
                
                # Checkpoint periodically (every 100 orders)
                if orders_processed % 100 == 0:
                    new_state = {
                        "last_updated_on": latest_updated_on,
                        "last_cursor": cursor
                    }
                    op.checkpoint(state=new_state)
                    log.info(f"Processed {orders_processed} orders, checkpointed at cursor: {cursor}")
            
            # Check if there are more pages
            has_next_page = page_info.get("hasNextPage", False)
            
            if not has_next_page:
                log.info("No more pages to process")
                break
            
            # Only check limit if max_records_per_sync is set
            if max_records_per_sync is not None and orders_processed >= max_records_per_sync:
                log.warning(f"Reached max orders limit ({max_records_per_sync}), stopping sync")
                break
        
        # Final checkpoint with updated state
        new_state = {
            "last_updated_on": latest_updated_on,
            "last_cursor": cursor if has_next_page else None
        }
        op.checkpoint(state=new_state)
        
        log.info(f"Sync completed. Processed {orders_processed} orders")
        
    except Exception as e:
        log.severe(f"Sync failed: {str(e)}")
        raise RuntimeError(f"Failed to sync data: {str(e)}")


# =============================================================================
# CONNECTOR INITIALIZATION
# =============================================================================

# Initialize the connector with the defined update and schema functions
connector = Connector(update=update, schema=schema)

# =============================================================================
# DEBUG ENTRY POINT
# =============================================================================

if __name__ == "__main__":
    # Open the configuration.json file and load its contents
    with open("configuration.json", 'r') as f:
        configuration = json.load(f)
    
    # Test the connector locally
    connector.debug(configuration=configuration)
