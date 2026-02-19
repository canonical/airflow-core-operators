# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.
#
# The integration tests use the Jubilant library. See https://documentation.ubuntu.com/jubilant/
# To learn more about testing, see https://documentation.ubuntu.com/ops/latest/explanation/testing/

"""Shared constants and helpers for integration tests."""

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
    return yaml.safe_load(charmcraft_path.read_text())


def _get_container_name(charm_dir: str) -> str:
    """Extract container name from charmcraft.yaml."""
    config = _parse_charmcraft_yaml(charm_dir)
    containers = config.get("containers", {})
    if containers:
        return list(containers.keys())[0]
    raise ValueError(f"No containers defined in charmcraft.yaml for {charm_dir}")


IMAGE = "ubuntu/airflow:3.1-24.04_edge"

CORE_CHARMS = {
    "api-server": "airflow-api-server-k8s",
    "dag-processor": "airflow-dag-processor-k8s",
    "scheduler": "airflow-scheduler-k8s",
    "triggerer": "airflow-triggerer-k8s",
}
CORE_CHARMS_RESOURCES = {
    component: {app.replace("-k8s","-image"): IMAGE} for component,app in CORE_CHARMS.items()
}
CORE_COMPONENTS = CORE_CHARMS.keys()
CORE_APPS = CORE_CHARMS.values()

CONTAINER_NAMES = {
    component: _get_container_name(component) for component, app in CORE_CHARMS.items()
}

CORE_JOB_CHECKS = [
    ("triggerer", "TriggererJob"),
    ("dag-processor", "DagProcessorJob"),
    ("scheduler", "SchedulerJob"),
]
AIRFLOW_HOME = "/opt/airflow"
POSTGRES_APP = "postgresql-k8s"
PGBOUNCER_APP = "pgbouncer-k8s"
COORDINATOR_APP = "airflow-coordinator-k8s"

ALL_APPS = [POSTGRES_APP, PGBOUNCER_APP, COORDINATOR_APP] + list(CORE_APPS)

POSTGRES_CHANNEL = "14/stable"
COORDINATOR_CHANNEL = "3.1/edge"

COORD_REL = "airflow-coordinator"

EXPECTED_RELATIONS = [
    (COORDINATOR_APP, "postgres"),
    *[(app, COORD_REL) for _, app in CORE_CHARMS.items()],
]

AIRFLOW_CONFIG_PATH = f"{AIRFLOW_HOME}/airflow.cfg"
DEFAULT_DAGS_PATH = f"{AIRFLOW_HOME}/dags"

AUTH_FILE = f"{AIRFLOW_HOME}/simple_auth_manager_passwords.json.generated"

# TODO: Update the constant once the issue https://github.com/canonical/airflow-coordinator-k8s-operator/issues/16 is resolved
DAGS_FILE = "/dags/test_dag.py"

PEBBLE_SERVICE_NAME = "airflow"

ANSI_RE = re.compile(r"\x1b\[[0-9;]*m")
