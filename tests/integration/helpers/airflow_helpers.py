# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.
#
# The integration tests use the Jubilant library. See https://documentation.ubuntu.com/jubilant/
# To learn more about testing, see https://documentation.ubuntu.com/ops/latest/explanation/testing/

"""Helpers for Airflow CLI, config parsing, and test DAG templates."""

import configparser
import json
import logging
import tempfile
import jubilant
import tests.integration.helpers.constants as constants


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

    parser = configparser.ConfigParser()

    with tempfile.NamedTemporaryFile(mode="w+", encoding="utf-8") as f:
        juju.scp(f"{unit}:{path}", f.name, container=container)
        f.seek(0)
        parser.read_file(f)  # returns None; parser is mutated
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
    with tempfile.NamedTemporaryFile(mode="w+", encoding="utf-8") as source_file:
        juju.scp(f"{coordinator_unit}:{template_path}", source_file.name)
        source_file.seek(0)
        lines = source_file.readlines()

    updated_lines: list[str] = []
    replaced = False
    for line in lines:
        if line.startswith(f"{key} = "):
            line_ending = "\n" if line.endswith("\n") else ""
            updated_lines.append(f"{key} = {rendered_value}{line_ending}")
            replaced = True
        else:
            updated_lines.append(line)

    if not replaced:
        logger.warning("Key '%s' not found in template %s", key, template_path)

    with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8") as updated_file:
        updated_file.writelines(updated_lines)
        updated_file.flush()
        juju.scp(updated_file.name, f"{coordinator_unit}:{template_path}")
