# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.
#
# The integration tests use the Jubilant library. See https://documentation.ubuntu.com/jubilant/
# To learn more about testing, see https://documentation.ubuntu.com/ops/latest/explanation/testing/

"""Integration tests for configuration and relation behavior."""

import shlex
import time
import pytest
import jubilant
from tenacity import Retrying, stop_after_attempt, wait_fixed

from tests.integration.conftest import pebble_service_is_running, get_pebble_service_status
from tests.integration.helpers.airflow_helpers import (
    json_from_airflow,
    read_airflow_config,
    set_coordinator_config_value,
)
import tests.integration.helpers.constants as constants


@pytest.mark.parametrize("component, app", list(constants.CORE_CHARMS.items()))
def test_airflow_config_options_present_and_rewritten_on_relation_change(
    juju: jubilant.Juju,
    deployed_stack,
    component: str,
    app: str,
):
    """Airflow config should be removed on relation break and restored on rejoin."""
    target_unit = f"{app}/0"
    target_container = constants.CONTAINER_NAMES[component]

    cfg = read_airflow_config(juju, target_unit, target_container)

    assert cfg.get("core", "dags_folder") == "dags"
    assert cfg.get("core", "executor") == "LocalExecutor"
    assert cfg.get("core", "load_examples") == "False"
    assert cfg.get("database", "sql_alchemy_conn").startswith("postgresql+psycopg2://")
    assert cfg.get("api", "port") == "8080"
    assert cfg.get("logging", "base_log_folder") == "logs"

    juju.remove_relation(
        f"{constants.COORDINATOR_APP}:{constants.COORD_REL}",
        f"{app}:{constants.COORD_REL}",
    )

    juju.wait(jubilant.all_agents_idle, timeout=5 * 60)

    output = juju.ssh(
        target_unit,
        f"test -f {shlex.quote(constants.AIRFLOW_CONFIG_PATH)} && echo OK || echo MISSING",
        container=target_container,
    )
    assert "OK" not in output

    juju.integrate(
        f"{constants.COORDINATOR_APP}:{constants.COORD_REL}",
        f"{app}:{constants.COORD_REL}",
    )

    juju.wait(jubilant.all_active, timeout=5 * 60)

    cfg = read_airflow_config(juju, target_unit, target_container)
    assert cfg.get("core", "executor") == "LocalExecutor"
    assert cfg.get("database", "sql_alchemy_conn").startswith("postgresql+psycopg2://")


def test_database_connectivity_from_scheduler(
    juju: jubilant.Juju,
):
    """Exec into the scheduler container and confirm DB connectivity."""

    out = juju.ssh(
        f"{constants.CORE_CHARMS['scheduler']}/0",
        "bash -lc "
        + shlex.quote("airflow db check && echo DB_CHECK_OK || echo DB_CHECK_FAILED"),
        container=constants.CONTAINER_NAMES["scheduler"],
    )
    assert "DB_CHECK_OK" in out, f"Failed to connect to the DB: {out}"


def test_config_change_propagates_and_dags_reserialize(
    juju: jubilant.Juju,
):
    """Config changes in coordinator should propagate and allow DAG reserialize."""

    set_coordinator_config_value(
        juju, f"{constants.COORDINATOR_APP}/0", "load_examples", True
    )

    # Stop all components to ensure conflictless singular dag reserialization
    for component, app in constants.CORE_CHARMS.items():
        juju.ssh(
            f"{app}/0",
            "pebble stop airflow",
            container=constants.CONTAINER_NAMES[component],
        )

        service_status = get_pebble_service_status(juju, component, f"{app}/0", "airflow")
        assert service_status["current"] == "inactive", f"Issue stopping service for {component}"

    juju.ssh(
        f"{constants.CORE_CHARMS['dag-processor']}/0",
        "bash -lc " + shlex.quote("airflow dags reserialize"),
        container=constants.CONTAINER_NAMES["dag-processor"],
    )

    # Start all components after dag reserialization
    for component, app in constants.CORE_CHARMS.items():
        juju.ssh(
            f"{app}/0",
            "pebble start airflow",
            container=constants.CONTAINER_NAMES[component],
        )

        service_status = get_pebble_service_status(juju, component, f"{app}/0", "airflow")
        assert service_status["current"] == "active", f"Issue starting service for {component}"


    for component, app in constants.CORE_CHARMS.items():
        cfg = read_airflow_config(
            juju, f"{app}/0", constants.CONTAINER_NAMES[component]
        )
        assert cfg.get("core", "load_examples") == "True", (
            f"Expected load_examples=True in {app} config"
        )

    out = juju.ssh(
        f"{constants.CORE_CHARMS['scheduler']}/0",
        "bash -lc "
        + shlex.quote("PYTHONWARNINGS=ignore airflow dags list --output json"),
        container=constants.CONTAINER_NAMES["scheduler"],
    )
    assert (
        isinstance(json_from_airflow(out), list) and len(json_from_airflow(out)) > 0
    ), f"Expected DAG list output, got: {out}"


def test_scheduler_scale_and_resilience(
    juju: jubilant.Juju,
):
    """Scheduler should scale up and down while remaining healthy."""

    dag_id = "latest_only_with_trigger"

    try:
        juju.add_unit(constants.CORE_CHARMS["scheduler"], num_units=2)
        juju.wait(
            ready=lambda st: jubilant.all_active(st)
            and len(st.apps[constants.CORE_CHARMS["scheduler"]].units) == 3,
            timeout=10 * 60,
        )

        juju.ssh(
            f"{constants.CORE_CHARMS['scheduler']}/0",
            "bash -lc " + shlex.quote(f"airflow dags unpause {dag_id}"),
            container=constants.CONTAINER_NAMES["scheduler"],
        )

        status = juju.status()
        for unit_name, unit_status in status.apps[
            constants.CORE_CHARMS["scheduler"]
        ].units.items():
            assert unit_status.is_active, (
                f"{unit_name} should be active, got {unit_status.workload_status.current}"
            )
            assert pebble_service_is_running(
                juju,
                unit_name,
                "scheduler",
                constants.PEBBLE_SERVICE_NAME,
            ), (
                f"{unit_name}: pebble service '{constants.PEBBLE_SERVICE_NAME}' not active."
            )

            run_id = f"scale-{unit_name.replace('/', '-')}-{int(time.time())}"
            juju.ssh(
                unit_name,
                "bash -lc "
                + shlex.quote(f"airflow dags trigger {dag_id} --run-id {run_id}"),
                container=constants.CONTAINER_NAMES["scheduler"],
            )

            for attempt in Retrying(
                stop=stop_after_attempt(6), wait=wait_fixed(30), reraise=True
            ):
                with attempt:
                    out = juju.ssh(
                        f"{constants.CORE_CHARMS['scheduler']}/0",
                        "bash -lc "
                        + shlex.quote(
                            f"PYTHONWARNINGS=ignore NO_COLOR=1 CLICOLOR=0 TERM=dumb "
                            f"airflow dags list-runs {dag_id} --output json"
                        ),
                        container=constants.CONTAINER_NAMES["scheduler"],
                    )
                    runs = json_from_airflow(out)
                    # TODO: remove "failed" from accepted run state after
                    # https://github.com/canonical/airflow-core-operators/issues/17
                    # resolved, i.e. we can reliably ensure successful runs
                    if not any(
                        run.get("run_id") == run_id
                        and run.get("state") in {"queued", "running", "failed", "success"}
                        for run in runs
                        if isinstance(runs, list)
                    ):
                        raise AssertionError(
                            f"DAG run {run_id} did not get queued or begin execution"
                        )
    finally:
        juju.remove_unit(constants.CORE_CHARMS["scheduler"], num_units=2)
        juju.wait(
            lambda st: len(st.apps[constants.CORE_CHARMS["scheduler"]].units) == 1,
            timeout=10 * 60,
        )
