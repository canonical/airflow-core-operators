"""Shared constants for the Airflow Triggerer charm."""

SERVICE_NAME = "airflow"
CONTAINER_NAME = "airflow-triggerer"
AIRFLOW_COMPONENT = "triggerer"
AIRFLOW_COORDINATOR_RELATION_NAME = "airflow-coordinator"
AIRFLOW_HOME = "/opt/airflow"
AIRFLOW_CONFIG_PATH = f"{AIRFLOW_HOME}/airflow.cfg"
