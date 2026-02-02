# tests/integration/helpers/dags.py
from __future__ import annotations

import textwrap


def functional_test_dag(dag_id: str) -> str:
    """
    Returns the DAG used by integration functional tests.
    """
    return (
        textwrap.dedent(
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
        )
        .lstrip()
    )
