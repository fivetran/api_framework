"""
Clarity orchestration example
========================================================

This DAG serves as a generic template for integrating Airflow with Fivetran,
specifically for enterprise replication workflows (like Clarity EHR).

Features:
- Secured credential management via Airflow Connections.
- Dataset-driven scheduling (Triggered by upstream ETL results).
- Dynamic Fivetran connector synchronization.
- Support for regular syncs AND historical resyncs using the Fivetran API.

Placeholders:
- <FIVETRAN_CONN_ID>: The Airflow Connection ID for Fivetran (default: 'fivetran').
- <DESTINATION_NAME>: Your Destination database name.
- <CONNECTOR_ID_VAR>: Airflow Variable containing Fivetran Connection ID.
"""

import json
from datetime import timedelta
from typing import Any, Dict, List

import pendulum
import requests
from requests.auth import HTTPBasicAuth

from airflow import DAG, Dataset
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.fivetran.hooks.fivetran import FivetranHook
from airflow.providers.fivetran.operators.fivetran import FivetranOperator
from airflow.operators.email import EmailOperator

# --- CONFIGURATION & CONSTANTS ---
# Use pendulum for timezone-aware scheduling
START_DATE = pendulum.datetime(2025, 1, 1, tz="UTC")

# Default arguments applied to all tasks
DEFAULT_ARGS = {
    'owner': 'data_engineering',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

DAG_ID = "fivetran_generic_replication_template"

# This DAG will trigger when the upstream ETL dataset is updated.
UPSTREAM_ETL_DATASET = Dataset("fivetran://upstream/etl/process")

# Define the target dataset this DAG "produces" upon completion
TARGET_DATASET = Dataset("fivetran://<DESTINATION_NAME>/replication_complete")

# --- FUNCTIONS ---

def get_connector_configs() -> List[Dict[str, Any]]:
    """
    Retrieves Fivetran connector settings from Airflow Variables.
    Storing IDs in Variables makes the DAG code environments-agnostic.
    """
    # Example JSON Variable format: 
    # {"connectors": [{"id": "connector_id_1", "name": "ehr_records", "type": "regular"}]}
    raw_config = Variable.get("fivetran_connector_list", default_var='{"connectors": []}')
    return json.loads(raw_config).get("connectors", [])

def trigger_historical_resync(connector_id: str, fivetran_conn_id: str = 'fivetran'):
    """
    Triggers a Fivetran historical resync via REST API.
    
    Why use Python instead of the standard operator?
    The standard FivetranOperator usually handles incremental syncs. 
    Manual resyncs often require specific API calls to the /resync endpoint.
    """
    hook = FivetranHook(fivetran_conn_id=fivetran_conn_id)
    conn = hook.get_connection(fivetran_conn_id)
    
    # Credentials from Connection
    auth = HTTPBasicAuth(conn.login, conn.password)
    resync_url = f"https://api.fivetran.com/v1/connections/{connector_id}/resync"
    
    print(f"Triggering resync for: {connector_id}")
    response = requests.post(resync_url, auth=auth, timeout=30)
    response.raise_for_status()
    print("Resync successfully triggered.")

# --- DAG ---

with DAG(
    dag_id=DAG_ID,
    start_date=START_DATE,
    default_args=DEFAULT_ARGS,
    schedule=[UPSTREAM_ETL_DATASET], # Triggered by Dataset
    catchup=False,
    max_active_runs=1,
    tags=["template", "fivetran", "replication"],
) as dag:

    # 1. Start Marker
    begin = EmptyOperator(task_id="begin")

    # 2. Dynamic Replication Logic
    # We iterate through connectors defined in our Airflow Variables
    configs = get_connector_configs()
    
    sync_tasks = []
    for cfg in configs:
        c_id = cfg["id"]
        c_name = cfg["name"]
        sync_type = cfg.get("type", "regular")
        
        if sync_type == "historical_resync":
            # Flow: Trigger API -> Wait for Completion
            trigger = PythonOperator(
                task_id=f"trigger_resync_{c_name}",
                python_callable=trigger_historical_resync,
                op_kwargs={"connector_id": c_id}
            )
            
            wait = FivetranOperator(
                task_id=f"wait_resync_{c_name}",
                fivetran_conn_id='fivetran',
                connector_id=c_id,
                poll_frequency=300 # Poll every 5 mins
            )
            trigger >> wait
            sync_tasks.append(wait)
        else:
            # Standard Sync
            sync = FivetranOperator(
                task_id=f"sync_{c_name}",
                fivetran_conn_id='fivetran',
                connector_id=c_id
            )
            sync_tasks.append(sync)

    # 3. Notification & Output
    # Use 'one_failed' for failure alerts and 'all_success' for completion
    success_msg = EmailOperator(
        task_id="notify_success",
        to="data_team@example.com",
        subject="Fivetran Sync Success: {{ dag.dag_id }}",
        html_content="All replication tasks completed successfully.",
        trigger_rule="all_success"
    )

    mark_done = EmptyOperator(
        task_id="mark_datasets_complete",
        outlets=[TARGET_DATASET], # Mark the Dataset as ready
        trigger_rule="all_success"
    )

    # 4. End Marker
    end = EmptyOperator(task_id="end", trigger_rule="all_done")

    # begin -> [Parallel Sync Tasks] -> success_msg -> mark_done -> end
    begin >> sync_tasks >> success_msg >> mark_done >> end
