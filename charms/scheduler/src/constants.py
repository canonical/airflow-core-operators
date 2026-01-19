"""Constants for the scheduler charm to use."""

AIRFLOW_HOME = "/opt/airflow/"
AIRFLOW_COMPONENT = "scheduler"
AIRFLOW_CONFIG_PATH = f"{AIRFLOW_HOME}/airflow.cfg"
AIRFLOW_COORDINATOR_RELATION_NAME = "airflow-coordinator"
CONTAINER_NAME = "airflow-scheduler"
SERVICE_NAME = "airflow"
