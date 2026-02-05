"""Helpers for Airflow CLI, config parsing, and test DAG templates."""

from __future__ import annotations

import configparser
import json
import shlex
import textwrap

import jubilant

from tests.integration.helpers.constants import AIRFLOW_CONFIG_PATH, ANSI_RE


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
    run_in,
    path: str = AIRFLOW_CONFIG_PATH,
) -> configparser.ConfigParser:
    """Read the rendered airflow.cfg from the workload container."""
    output = run_in(juju, unit, container, "bash -lc " + shlex.quote(f"cat {path}"))
    parser = configparser.ConfigParser()
    parser.read_string(output)
    return parser


def get_airflow_config_value(
    juju: jubilant.Juju,
    app: str,
    section: str,
    key: str,
    run_in,
    unit,
    container_for,
) -> str:
    """Return a config value from the Airflow CLI."""
    cmd = f"airflow config get-value {section} {key}"
    out = run_in(
        juju,
        unit(app),
        container_for(app),
        "bash -lc " + shlex.quote(cmd),
    )
    return clean_ansi(out).strip()


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
