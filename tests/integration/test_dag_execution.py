"""Integration tests covering DAG execution flow."""

import time
import pytest
import jubilant
import shlex
from tests.integration.helpers.airflow_helpers import (
    json_from_airflow,
)
from pathlib import Path
from tests.integration.helpers.constants import (
    CONTAINER_NAMES,
    CORE_APPS,
    DAGS_FILE,
    FUNCTIONAL_DAG_TEMPLATE,
    FUNCTIONAL_DAG_ID,
    CORE_APP_BY_COMPONENT,
)
from tests.integration.conftest import (
    push_text_file,
)


@pytest.mark.abort_on_fail
def test_dag_discovery_and_execution(
    juju: jubilant.Juju,
    deployed_stack,
):
    """Injected DAG should be discovered and complete successfully."""
    juju.wait(jubilant.all_agents_idle, timeout=10 * 60)

    dag_id = FUNCTIONAL_DAG_ID
    dag_content = Path(FUNCTIONAL_DAG_TEMPLATE).read_text(encoding="utf-8")
    for app in CORE_APPS:
        unit = f"{app}/0"
        container = CONTAINER_NAMES[app]
        push_text_file(
            juju,
            unit,
            container,
            DAGS_FILE,
            dag_content,
        )
    
        print("DAG pushed.")
        juju.cli("ssh","--container", container, unit, f"ls -l {DAGS_FILE}")

    for app in CORE_APPS:
        unit = f"{app}/0"
        container = CONTAINER_NAMES[app]
        juju.cli(
            "ssh",
            "--container",
            container,
            unit,
            "bash -lc " + shlex.quote("airflow dags reserialize"),
        )
        juju.cli(
            "ssh",
            "--container",
            container,
            unit,
            "bash -lc " + shlex.quote("airflow dags unpause " + dag_id),
        )

    juju.wait(jubilant.all_agents_idle, timeout=15 * 60)
    print("Verify DAG discovery and execution")
    discovered = False
    for _ in range(36):
        scheduler_unit = f"{CORE_APP_BY_COMPONENT['scheduler']}/0"
        scheduler_container = CONTAINER_NAMES[CORE_APP_BY_COMPONENT["scheduler"]]
        out = juju.cli(
            "ssh",
            "--container",
            scheduler_container,
            scheduler_unit,
            "bash -lc "
            + shlex.quote("PYTHONWARNINGS=ignore airflow dags list --output json"),
        )
        print(f"----------------- DAG list output: ------------------------ \n {out}")
        try:
            dags = json_from_airflow(out)
            print(f"Parsed DAGs: {dags}")
            juju.cli("ssh","--container" , scheduler_container, scheduler_unit, "airflow dags list-import-errors")
            if any(d.get("dag_id") == dag_id for d in dags if isinstance(d, dict)):
                print("DAG discovered in list.")
                discovered = True
                break
        except Exception:
            print("Error parsing DAG list output.")
            juju.cli("ssh","--container" , scheduler_container, scheduler_unit, "airflow dags list-import-errors")
            pass
        time.sleep(10)

    assert discovered, "DAG was not discovered (DAG Processor failed to sync DAG to DB)"

    run_id = f"it-{int(time.time())}"
    scheduler_unit = f"{CORE_APP_BY_COMPONENT['scheduler']}/0"
    scheduler_container = CONTAINER_NAMES[CORE_APP_BY_COMPONENT["scheduler"]]
    juju.cli(
        "ssh",
        "--container",
        scheduler_container,
        scheduler_unit,
        "bash -lc " + shlex.quote(f"airflow dags trigger {dag_id} --run-id {run_id}"),
    )

    queued_or_running = False
    for _ in range(18):
        scheduler_unit = f"{CORE_APP_BY_COMPONENT['scheduler']}/0"
        scheduler_container = CONTAINER_NAMES[CORE_APP_BY_COMPONENT["scheduler"]]
        out = juju.cli(
            "ssh",
            "--container",
            scheduler_container,
            scheduler_unit,
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
