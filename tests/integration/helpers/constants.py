"""Shared constants and helpers for integration tests."""

from __future__ import annotations

import os
import re
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]

CORE_CHARMS = [
    ("api-server", "airflow-api-server-k8s"),
    ("dag-processor", "airflow-dag-processor-k8s"),
    ("scheduler", "airflow-scheduler-k8s"),
    ("triggerer", "airflow-triggerer-k8s"),
]
CORE_COMPONENTS = [component for component, _ in CORE_CHARMS]
CORE_APPS = [app for _, app in CORE_CHARMS]
CORE_APP_BY_COMPONENT = {component: app for component, app in CORE_CHARMS}

POSTGRES_APP = "postgresql-k8s"
COORDINATOR_APP = "airflow-coordinator-k8s"

POSTGRES_CHANNEL = os.environ.get("POSTGRES_CHANNEL", "14/stable")
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
