"""Shared constants and helpers for integration tests."""

import os
import re
from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parents[3]
INTEGRATION_DAGS_DIR = REPO_ROOT / "tests" / "integration" / "dags"
FUNCTIONAL_DAG_TEMPLATE = INTEGRATION_DAGS_DIR / "functional_test_dag.py"
FUNCTIONAL_DAG_ID = "test_functional_dag"


def _parse_charmcraft_yaml(charm_dir: str) -> dict:
    """Parse charmcraft.yaml for a given charm directory."""
    charmcraft_path = REPO_ROOT / "charms" / charm_dir / "charmcraft.yaml"
    if not charmcraft_path.exists():
        return {}
    with open(charmcraft_path) as f:
        return yaml.safe_load(f)


def _get_container_name(charm_dir: str) -> str:
    """Extract container name from charmcraft.yaml."""
    config = _parse_charmcraft_yaml(charm_dir)
    containers = config.get("containers", {})
    if containers:
        return list(containers.keys())[0]
    raise ValueError(f"No containers defined in charmcraft.yaml for {charm_dir}")


IMAGE = os.environ.get("AIRFLOW_IMAGE", "ubuntu/airflow:3.1-24.04_edge")

CORE_CHARMS = {
    "api-server": "airflow-api-server-k8s",
    "dag-processor": "airflow-dag-processor-k8s",
    "scheduler": "airflow-scheduler-k8s",
    "triggerer": "airflow-triggerer-k8s",
}
CORE_COMPONENTS = CORE_CHARMS.keys()
CORE_APPS = CORE_CHARMS.values()
CORE_APP_BY_COMPONENT = {component: app for component, app in CORE_CHARMS.items()}

CONTAINER_NAMES = {
    app: _get_container_name(component) for component, app in CORE_CHARMS.items()
}

POSTGRES_APP = "postgresql-k8s"
PGBOUNCER_APP = os.environ.get("PGBOUNCER_APP", "pgbouncer-k8s")
COORDINATOR_APP = "airflow-coordinator-k8s"

ALL_APPS = [POSTGRES_APP, PGBOUNCER_APP, COORDINATOR_APP] + list(CORE_APPS)

POSTGRES_CHANNEL = os.environ.get("POSTGRES_CHANNEL", "14/stable")
POSTGRES_PROFILE = os.environ.get("POSTGRES_PROFILE", "testing")
PGBOUNCER_CHANNEL = "1/stable"
COORDINATOR_CHANNEL = os.environ.get("COORDINATOR_CHANNEL", "3.1/edge")

COORD_REL = os.environ.get("COORD_REL", "airflow-coordinator")
AIRFLOW_CONFIG_PATH = os.environ.get("AIRFLOW_CONFIG_PATH", "/opt/airflow/airflow.cfg")
DEFAULT_DAGS_PATH = os.environ.get("DAGS_PATH", "/opt/airflow/dags")

AUTH_FILE = "/opt/airflow/simple_auth_manager_passwords.json.generated"

# TODO: Update the constant once the issue https://github.com/canonical/airflow-coordinator-k8s-operator/issues/16 is resolved
DAGS_FILE = "/dags/test_dag.py"

PEBBLE_SERVICE_NAME = "airflow"

ANSI_RE = re.compile(r"\x1b\[[0-9;]*m")


def get_core_app(component: str) -> str:
    """Return the application name for a core component."""
    return CORE_APP_BY_COMPONENT[component]
