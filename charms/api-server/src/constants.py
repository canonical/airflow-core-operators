"""Shared constants for the Airflow API Server charm."""

SERVICE_NAME = "airflow"
CONTAINER_NAME = "airflow-api-server"
AIRFLOW_COMPONENT = "api-server"

AIRFLOW_COORDINATOR_RELATION_ENDPOINT = "airflow-coordinator"
AIRFLOW_API_SERVER_RELATION_ENDPOINT = "airflow-api-server"
TRAEFIK_INGRESS_RELATION_ENDPOINT = "ingress"
AIRFLOW_HOME = "/opt/airflow"
AIRFLOW_CONFIG_PATH = f"{AIRFLOW_HOME}/airflow.cfg"
WORKLOAD_USER = "ubuntu"
WORKLOAD_GROUP = "ubuntu"
