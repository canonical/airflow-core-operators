"""Helpers for Airflow CLI, config parsing, and test DAG templates."""

import configparser
import json
import shlex
import jubilant

import tests.integration.helpers.constants as constants 
from tenacity import (
    retry,
    stop_after_attempt,
    wait_fixed,
    retry_if_exception_type,
)
import logging
import textwrap

logger = logging.getLogger(__name__)


class ServiceNotReadyError(RuntimeError):
    """Raised when the Pebble service is not ready yet."""

def clean_ansi(text: str) -> str:
    """Strip ANSI escape sequences from CLI output."""
    return constants.ANSI_RE.sub("", text)


def json_from_airflow(out: str) -> list | dict:
    """Parse JSON output from Airflow CLI commands."""
    clean = clean_ansi(out).strip()
    return json.loads(clean)


def read_airflow_config(
    juju: jubilant.Juju,
    unit: str,
    container: str,
    path: str = constants.AIRFLOW_CONFIG_PATH,
) -> configparser.ConfigParser:
    """Read the rendered airflow.cfg from the workload container."""

    output = juju.cli("ssh", "--container", container, unit, "bash -lc " + shlex.quote(f"cat {path}"))
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

    cmd = f"airflow config get-value {section} {key}"
    out = juju.cli("ssh", "--container", app.replace("-k8s", ""), f"{app}/0", "bash -lc " + shlex.quote(cmd))
    return clean_ansi(out).strip()

@retry(
    stop=stop_after_attempt(30),
    wait=wait_fixed(5),
    reraise=True,
    retry=retry_if_exception_type(ServiceNotReadyError),
)
def ensure_db_migrated(juju: jubilant.Juju, app: str) -> None:
    """Ensure the Airflow database migrations are fully applied."""
    from tests.integration.conftest import (
        pebble_service_is_running,
    )

    unit = f"{app}/0"
    container = app.replace("-k8s", "")
    
    try:
        services_text = juju.cli(
            "ssh",
            "--container",
            container,
            unit,
            "pebble services || true",
        )
    except Exception as exc:
        logger.info("Pebble service not ready yet, retrying...")
        raise ServiceNotReadyError(
            f"Failed checking Pebble services for {app}"
        ) from exc

    if not pebble_service_is_running(services_text, constants.PEBBLE_SERVICE_NAME):
        logger.info("Pebble service not ready yet, retrying...")
        raise ServiceNotReadyError(
            f"Timed out waiting for '{constants.PEBBLE_SERVICE_NAME}' service in {app}"
        )

    cmd = "airflow db migrate"
    juju.cli("ssh","--container",container,unit,"bash -lc " + shlex.quote(cmd))

def set_coordinator_load_examples(
    juju: jubilant.Juju, coordinator_unit: str, load_examples: bool
) -> None:

    unit_path = coordinator_unit.replace("/", "-")
    template_path = (
        f"/var/lib/juju/agents/unit-{unit_path}/charm/src/templates/airflow_config.j2"
    )
    value = "True" if load_examples else "False"
    cmd = f"sed -i 's/^load_examples = .*/load_examples = {value}/' {template_path}"
    juju.cli("ssh", coordinator_unit, "bash -lc " + shlex.quote(cmd))


def restart_airflow_service(juju: jubilant.Juju, app: str) -> None:
    """Restart the airflow service in the workload container via Pebble."""

    juju.cli(
        "ssh",
        "--container",
        app.replace("-k8s", ""), 
        f"{app}/0",
        "pebble restart airflow",
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