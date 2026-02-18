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
    retry_if_result,
)
import logging

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

    output = juju.ssh(
        unit,
        "bash -lc " + shlex.quote(f"cat {path}"),
        container=container,
    )
    parser = configparser.ConfigParser()
    parser.read_string(output)
    return parser


def get_airflow_config_value(
    juju: jubilant.Juju,
    component: str,
    app: str,
    section: str,
    key: str,
) -> str:
    """Return a config value from the Airflow CLI."""

    cmd = f"airflow config get-value {section} {key}"
    container = constants.CONTAINER_NAMES[component]
    out = juju.ssh(
        f"{app}/0",
        "bash -lc " + shlex.quote(cmd),
        container=container,
    )
    return clean_ansi(out).strip()


@retry(
    stop=stop_after_attempt(30),
    wait=wait_fixed(5),
    reraise=True,
    retry=retry_if_exception_type(ServiceNotReadyError)
    | retry_if_result(lambda result: result is False),
)
def ensure_db_migrated(juju: jubilant.Juju, component: str, app: str) -> None:
    """Ensure the Airflow database migrations are fully applied."""
    from tests.integration.conftest import (
        pebble_service_is_running,
    )

    unit = f"{app}/0"
    container = constants.CONTAINER_NAMES[component]

    try:
        service_ready = pebble_service_is_running(
            juju,
            unit,
            constants.PEBBLE_SERVICE_NAME,
        )
    except Exception as exc:
        logger.info("Pebble service not ready yet, retrying...")
        raise ServiceNotReadyError(
            f"Failed checking Pebble services for {app}"
        ) from exc

    if not service_ready:
        logger.info("Pebble service not ready yet, retrying...")
        raise ServiceNotReadyError(
            f"Timed out waiting for '{constants.PEBBLE_SERVICE_NAME}' service in {app}"
        )

    cmd = "airflow db migrate; echo __EXIT:$?"
    out = juju.ssh(unit, "bash -lc " + shlex.quote(cmd), container=container)
    exit_code = None
    for line in out.splitlines():
        if line.startswith("__EXIT:"):
            exit_code = line.split(":", 1)[1].strip()
            break
    success = exit_code == "0"
    if not success:
        logger.info("Airflow DB migration failed for %s: %s", app, out)
    return success


def set_coordinator_config_value(
    juju: jubilant.Juju,
    coordinator_unit: str,
    key: str,
    value: str | bool,
) -> None:
    """Update a key in the coordinator airflow_config.j2 template."""
    unit_path = coordinator_unit.replace("/", "-")
    template_path = (
        f"/var/lib/juju/agents/unit-{unit_path}/charm/src/templates/airflow_config.j2"
    )
    rendered_value = (
        "True" if value is True else "False" if value is False else str(value)
    )
    cmd = f"sed -i 's/^{key} = .*/{key} = {rendered_value}/' {template_path}"
    juju.ssh(coordinator_unit, "bash -lc " + shlex.quote(cmd))


# def restart_airflow_service(juju: jubilant.Juju, app: str) -> None:
#     """Restart the airflow service in the workload container via Pebble."""

#     juju.cli(
#         "ssh",
#         "--container",
#         app.replace("-k8s", ""),
#         f"{app}/0",
#         "pebble restart airflow",
#         container=container,
#     )
