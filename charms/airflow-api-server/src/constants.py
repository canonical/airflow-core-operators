"""Shared constants for the Airflow API Server charm."""

SERVICE_NAME = "airflow"
CONTAINER_NAME = "airflow-api-server"
AIRFLOW_COMPONENT = "api-server"
AIRFLOW_COORDINATOR_RELATION_NAME = "airflow-coordinator"
AIRFLOW_HOME = "/opt/airflow"
AIRFLOW_CONFIG_PATH = f"{AIRFLOW_HOME}/airflow.cfg"

PEER_RELATION_NAME = "airflow-api-server-peers"
HAS_EVER_BEEN_READY_KEY = "has_ever_been_ready"

