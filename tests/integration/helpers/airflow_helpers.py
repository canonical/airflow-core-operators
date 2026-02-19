# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.
#
# The integration tests use the Jubilant library. See https://documentation.ubuntu.com/jubilant/
# To learn more about testing, see https://documentation.ubuntu.com/ops/latest/explanation/testing/

"""Helpers for Airflow CLI, config parsing, and test DAG templates."""

import configparser
import json
import shlex
import jubilant

import tests.integration.helpers.constants as constants
import logging


logger = logging.getLogger(__name__)


class ServiceNotReadyError(RuntimeError):
    """Raised when the Pebble service is not ready yet."""


def json_from_airflow(out: str) -> list | dict:
    """Parse JSON output from Airflow CLI commands."""
    clean = constants.ANSI_RE.sub("", out).strip()
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
