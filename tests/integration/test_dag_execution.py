# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.
#
# The integration tests use the Jubilant library. See https://documentation.ubuntu.com/jubilant/
# To learn more about testing, see https://documentation.ubuntu.com/ops/latest/explanation/testing/

"""Integration tests covering DAG execution flow."""

import time
import jubilant
import shlex
import tenacity
from tests.integration.helpers.airflow_helpers import (
    json_from_airflow,
)
from pathlib import Path
import tests.integration.helpers.constants as constants
from tests.integration.conftest import (
    push_text_file,
)


def test_dag_discovery_and_execution(
    juju: jubilant.Juju,
    deployed_stack,
):
    """Injected DAG should be discovered and complete successfully."""

    dag_content = Path(constants.FUNCTIONAL_DAG_TEMPLATE).read_text(encoding="utf-8")
    for component, app in constants.CORE_CHARMS.items():
        unit = f"{app}/0"
        container = constants.CONTAINER_NAMES[component]
        push_text_file(
            juju,
            unit,
            container,
            constants.DAGS_FILE,
            dag_content,
        )

        juju.ssh(unit, f"ls -l {constants.DAGS_FILE}", container=container)

    for component, app in constants.CORE_CHARMS.items():
        unit = f"{app}/0"
        container = constants.CONTAINER_NAMES[component]
        juju.ssh(
            unit,
            "bash -lc " + shlex.quote("airflow dags reserialize"),
            container=container,
        )

    juju.wait(jubilant.all_agents_idle, timeout=15 * 60)

    for attempt in tenacity.Retrying(
        stop=tenacity.stop_after_attempt(36),
        wait=tenacity.wait_fixed(10),
        reraise=True,
    ):
        with attempt:
            out = juju.ssh(
                f"{constants.CORE_CHARMS['scheduler']}/0",
                "bash -lc "
                + shlex.quote("PYTHONWARNINGS=ignore airflow dags list --output json"),
                container=constants.CONTAINER_NAMES["scheduler"],
            )

            dags = json_from_airflow(out)
            if not any(
                d.get("dag_id") == constants.FUNCTIONAL_DAG_ID
                for d in dags
                if isinstance(d, dict)
            ):
                raise AssertionError("DAG not discovered yet")

    run_id = f"it-{int(time.time())}"
    juju.ssh(
        f"{constants.CORE_CHARMS['scheduler']}/0",
        "bash -lc "
        + shlex.quote(
            f"airflow dags trigger {constants.FUNCTIONAL_DAG_ID} --run-id {run_id}"
        ),
        container=constants.CONTAINER_NAMES["scheduler"],
    )

    queued_or_running = False
    for attempt in tenacity.Retrying(
        stop=tenacity.stop_after_attempt(36),
        wait=tenacity.wait_fixed(10),
        reraise=True,
    ):
        with attempt:
            out = juju.ssh(
                f"{constants.CORE_CHARMS['scheduler']}/0",
                "bash -lc "
                + shlex.quote(
                    f"PYTHONWARNINGS=ignore NO_COLOR=1 CLICOLOR=0 TERM=dumb airflow dags list-runs {constants.FUNCTIONAL_DAG_ID} --output json"
                ),
                container=constants.CONTAINER_NAMES["scheduler"],
            )
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

    assert queued_or_running, "DAG run did not reach queued or running state"
