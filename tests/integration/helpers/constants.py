"""Shared constants and helpers for integration tests."""

from __future__ import annotations

import os
import re
from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parents[3]


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
        # Return the first container name
        return list(containers.keys())[0]
    return charm_dir  # fallback


CORE_CHARMS = [
    ("api-server", "airflow-api-server-k8s"),
    ("dag-processor", "airflow-dag-processor-k8s"),
    ("scheduler", "airflow-scheduler-k8s"),
    ("triggerer", "airflow-triggerer-k8s"),
]
CORE_COMPONENTS = [component for component, _ in CORE_CHARMS]
CORE_APPS = [app for _, app in CORE_CHARMS]
CORE_APP_BY_COMPONENT = {component: app for component, app in CORE_CHARMS}

# Container names for each charm
CONTAINER_NAMES = {
    app: _get_container_name(component) for component, app in CORE_CHARMS
}

POSTGRES_APP = "postgresql-k8s"
PGBOUNCER_APP = os.environ.get("PGBOUNCER_APP", "pgbouncer-k8s")
COORDINATOR_APP = "airflow-coordinator-k8s"

# All applications in the deployment
ALL_APPS = [POSTGRES_APP, PGBOUNCER_APP, COORDINATOR_APP] + CORE_APPS

POSTGRES_CHANNEL = os.environ.get("POSTGRES_CHANNEL", "14/stable")
POSTGRES_PROFILE = os.environ.get("POSTGRES_PROFILE", "testing")
PGBOUNCER_CHANNEL = os.environ.get("PGBOUNCER_CHANNEL")
COORDINATOR_CHANNEL = os.environ.get("COORDINATOR_CHANNEL", "3.1/edge")

COORD_REL = os.environ.get("COORD_REL", "airflow-coordinator")
AIRFLOW_CONFIG_PATH = os.environ.get("AIRFLOW_CONFIG_PATH", "/opt/airflow/airflow.cfg")
DEFAULT_DAGS_PATH = os.environ.get("DAGS_PATH", "/opt/airflow/dags")

AUTH_FILE = "/opt/airflow/simple_auth_manager_passwords.json.generated"
DAGS_FILE = "/dags/test_dag.py"

PEBBLE_SERVICE_NAME = "airflow"

ANSI_RE = re.compile(r"\x1b\[[0-9;]*m")


def get_core_app(component: str) -> str:
    """Return the application name for a core component."""
    return CORE_APP_BY_COMPONENT[component]
