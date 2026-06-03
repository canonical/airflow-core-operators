"""Shared constants for the Airflow API Server charm."""

SERVICE_NAME = "airflow"
CONTAINER_NAME = "airflow-api-server"
AIRFLOW_COMPONENT = "api-server"

AIRFLOW_COORDINATOR_RELATION_ENDPOINT = "airflow-coordinator"
AIRFLOW_API_SERVER_RELATION_ENDPOINT = "airflow-api-server"
TRAEFIK_INGRESS_RELATION_ENDPOINT = "ingress"
AIRFLOW_HOME = "/opt/airflow"
AIRFLOW_CONFIG_PATH = f"{AIRFLOW_HOME}/airflow.cfg"
WEBSERVER_CONFIG_PATH = f"{AIRFLOW_HOME}/webserver_config.py"
WORKLOAD_USER = "ubuntu"
WORKLOAD_GROUP = "ubuntu"

FAILED_TO_CHECK_WEBSERVER_CONFIG_UPDATE_MESSAGE = (
    "Failed to check webserver_config.py needs update."
)
FAILED_TO_WRITE_WEBSERVER_CONFIG_MESSAGE = (
    "Failed to write webserver_config.py to workload container"
)
FAILED_TO_CHECK_WEBSERVER_CONFIG_EXISTS_MESSAGE = (
    "Failed to check if webserver_config.py exists in workload container"
)
FAILED_TO_REMOVE_WEBSERVER_CONFIG_MESSAGE = (
    "Failed to remove webserver_config.py in workload container"
)
