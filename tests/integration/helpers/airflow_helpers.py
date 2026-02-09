"""Helpers for Airflow CLI, config parsing, and test DAG templates."""

from __future__ import annotations

import configparser
import json
import shlex
import textwrap
import time

import jubilant
from tests.integration.helpers.constants import AIRFLOW_CONFIG_PATH, ANSI_RE, PEBBLE_SERVICE_NAME

def clean_ansi(text: str) -> str:
    """Strip ANSI escape sequences from CLI output."""
    return ANSI_RE.sub("", text)


def json_from_airflow(out: str) -> list | dict:
    """Parse JSON output from Airflow CLI commands."""
    clean = clean_ansi(out).strip()
    return json.loads(clean)


def read_airflow_config(
    juju: jubilant.Juju,
    unit: str,
    container: str,
    path: str = AIRFLOW_CONFIG_PATH,
) -> configparser.ConfigParser:
    """Read the rendered airflow.cfg from the workload container."""
    from tests.integration.conftest import ssh

    output = ssh(juju, unit, container, "bash -lc " + shlex.quote(f"cat {path}"))
    parser = configparser.ConfigParser()
    parser.read_string(output)
    return parser


def get_airflow_config_value(
    juju: jubilant.Juju,
    app: str,
    section: str,
    key: str,
) -> str:
    """Return a config value from the Airflow CLI."""
    from tests.integration.conftest import ssh, unit_name, workload_container_for_app

    cmd = f"airflow config get-value {section} {key}"
    out = ssh(
        juju,
        unit_name(app),
        workload_container_for_app(app),
        "bash -lc " + shlex.quote(cmd),
    )
    return clean_ansi(out).strip()


def ensure_db_migrated(juju: jubilant.Juju, app: str) -> None:
    """Ensure the Airflow database migrations are fully applied."""
    from tests.integration.conftest import (
        pebble_service_is_running,
        pebble_services_text,
        ssh,
        unit_name,
        workload_container_for_app,
    )

    unit = unit_name(app)
    container = workload_container_for_app(app)

    deadline = time.time() + 5 * 60
    last_error: Exception | None = None
    while time.time() < deadline:
        try:
            services_text = pebble_services_text(juju, unit, container)
            if pebble_service_is_running(services_text, PEBBLE_SERVICE_NAME):
                break
        except Exception as exc:
            last_error = exc
        time.sleep(5)
    else:
        if last_error is not None:
            raise last_error
        raise RuntimeError(
            f"Timed out waiting for '{PEBBLE_SERVICE_NAME}' service in {app}"
        )

    cmd = "airflow db migrate"
    ssh(
        juju,
        unit,
        container,
        "bash -lc " + shlex.quote(cmd),
    )


def set_coordinator_load_examples(
    juju: jubilant.Juju, coordinator_unit: str, load_examples: bool
) -> None:
    from tests.integration.conftest import ssh_unit

    unit_path = coordinator_unit.replace("/", "-")
    template_path = (
        f"/var/lib/juju/agents/unit-{unit_path}/charm/src/templates/airflow_config.j2"
    )
    value = "True" if load_examples else "False"
    cmd = f"sed -i 's/^load_examples = .*/load_examples = {value}/' {template_path}"
    ssh_unit(juju, coordinator_unit, "bash -lc " + shlex.quote(cmd))


def restart_airflow_service(juju: jubilant.Juju, app: str) -> None:
    """Restart the airflow service in the workload container via Pebble."""
    from tests.integration.conftest import ssh, unit_name, workload_container_for_app

    ssh(
        juju,
        unit_name(app),
        workload_container_for_app(app),
        "pebble restart airflow",
    )


def airflow_dags_reserialize(juju: jubilant.Juju, app: str) -> None:
    """Reserialize DAGs to ensure new config is applied to parsing."""
    from tests.integration.conftest import ssh, unit_name, workload_container_for_app

    ssh(
        juju,
        unit_name(app),
        workload_container_for_app(app),
        "bash -lc " + shlex.quote("airflow dags reserialize"),
    )


def functional_test_dag(dag_id: str) -> str:
    """Return the DAG used by integration functional tests."""
    return textwrap.dedent(
        f"""
            from airflow import DAG
            from airflow.operators.bash import BashOperator
            from datetime import datetime

            with DAG(
                dag_id="{dag_id}",
                start_date=datetime(2023, 1, 1),
                schedule=None,
                catchup=False,
            ) as dag:
                BashOperator(task_id="ping", bash_command="echo pong")
            """
    ).lstrip()
