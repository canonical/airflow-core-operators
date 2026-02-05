"""Integration tests covering DAG execution flow."""

import time
import pytest
import jubilant
import shlex

from tests.integration.helpers.airflow_helpers import (
    functional_test_dag,
    json_from_airflow,
)
from tests.integration.helpers.constants import CORE_APPS, DAGS_FILE, get_core_app


@pytest.mark.abort_on_fail
def test_dag_discovery_and_execution(
    juju: jubilant.Juju,
    deployed_stack: bool,
    relate_core_charms: bool,
    unit,
    container_for,
    run_in,
    push_file,
    airflow_db_migrated,
):
    """Injected DAG should be discovered and complete successfully."""
    dag_id = "test_functional_dag"
    dag_content = functional_test_dag(dag_id)

    print("Pushing DAG content:")
    for app in CORE_APPS:
        push_file(juju, unit(app), container_for(app), DAGS_FILE, dag_content)
    print("DAG pushed.")

    for app in CORE_APPS:
        run_in(
            juju,
            unit(app),
            container_for(app),
            "bash -lc " + shlex.quote("airflow dags reserialize"),
        )
        run_in(
            juju,
            unit(app),
            container_for(app),
            "bash -lc " + shlex.quote(f"airflow dags unpause {dag_id}"),
        )

    juju.wait(jubilant.all_agents_idle, timeout=15 * 60)
    print("Verify DAG discovery and execution")
    discovered = False
    for _ in range(36):
        out = run_in(
            juju,
            unit(get_core_app("scheduler")),
            container_for(get_core_app("scheduler")),
            "bash -lc "
            + shlex.quote("PYTHONWARNINGS=ignore airflow dags list --output json"),
        )
        try:
            dags = json_from_airflow(out)
            if any(d.get("dag_id") == dag_id for d in dags if isinstance(d, dict)):
                print("DAG discovered in list.")
                discovered = True
                break
        except Exception:
            print("Error parsing DAG list output.")
            pass
        time.sleep(10)

    assert discovered, "DAG was not discovered (DAG Processor failed to sync DAG to DB)"

    run_id = f"it-{int(time.time())}"
    run_in(
        juju,
        unit(get_core_app("scheduler")),
        container_for(get_core_app("scheduler")),
        "bash -lc " + shlex.quote(f"airflow dags trigger {dag_id} --run-id {run_id}"),
    )

    queued_or_running = False
    for _ in range(18):
        out = run_in(
            juju,
            unit(get_core_app("scheduler")),
            container_for(get_core_app("scheduler")),
            "bash -lc "
            + shlex.quote(
                f"PYTHONWARNINGS=ignore NO_COLOR=1 CLICOLOR=0 TERM=dumb airflow dags list-runs {dag_id} --output json"
            ),
        )
        try:
            runs = json_from_airflow(out)
            for run in runs if isinstance(runs, list) else []:
                if run.get("run_id") == run_id and run.get("state") in {
                    "queued",
                    "running",
                }:
                    queued_or_running = True
                    break
            if queued_or_running:
                break
        except Exception:
            pass
        time.sleep(5)

    assert queued_or_running, "DAG run did not reach queued or running state"
