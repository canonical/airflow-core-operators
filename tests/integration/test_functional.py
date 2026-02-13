"""Integration tests for configuration and relation behavior."""

import shlex
import time

import pytest
import jubilant
from tenacity import Retrying, stop_after_attempt, wait_fixed

from tests.integration.conftest import (
    file_exists,
    pebble_service_is_running,
)
from tests.integration.helpers.airflow_helpers import (
    json_from_airflow,
    read_airflow_config,
    set_coordinator_config_value,
)
import tests.integration.helpers.constants as constants


@pytest.mark.abort_on_fail
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

    juju.cli(
        "remove-relation",
        f"{constants.COORDINATOR_APP}:{constants.COORD_REL}",
        f"{app}:{constants.COORD_REL}",
    )

    juju.wait(jubilant.all_agents_idle, timeout=10 * 60)

    assert not file_exists(
        juju, target_unit, target_container, constants.AIRFLOW_CONFIG_PATH
    )

    juju.integrate(
        f"{constants.COORDINATOR_APP}:{constants.COORD_REL}",
        f"{app}:{constants.COORD_REL}",
    )

    juju.wait(jubilant.all_agents_idle, timeout=20 * 60)

    cfg = read_airflow_config(juju, target_unit, target_container)
    assert cfg.get("core", "executor") == "LocalExecutor"
    assert cfg.get("database", "sql_alchemy_conn").startswith("postgresql+psycopg2://")


@pytest.mark.abort_on_fail
def test_database_connectivity_from_scheduler(
    juju: jubilant.Juju,
):
    """Exec into the scheduler container and confirm DB connectivity."""
    scheduler_unit = f"{constants.CORE_CHARMS['scheduler']}/0"
    scheduler_container = constants.CONTAINER_NAMES["scheduler"]
    check_cmd = "airflow db check || echo 'DB check failed'"
    out = juju.cli(
        "ssh",
        "--container",
        scheduler_container,
        scheduler_unit,
        "bash -lc " + shlex.quote(check_cmd),
    )
    assert "Connection successful" in out, f"Failed to connect to the DB: {out}"


@pytest.mark.abort_on_fail
def test_config_change_propagates_and_dags_reserialize(
    juju: jubilant.Juju,
):
    """Config changes in coordinator should propagate and allow DAG reserialize."""
    coordinator_unit = f"{constants.COORDINATOR_APP}/0"
    set_coordinator_config_value(juju, coordinator_unit, "load_examples", True)

    # TODO: Update once the issue https://github.com/canonical/airflow-core-operators/issues/19 is resolved
    for _, app in constants.CORE_CHARMS.items():
        juju.cli(
            "ssh",
            "--container",
            app.replace("-k8s", ""),
            f"{app}/0",
            "pebble restart airflow",
        )

    for _, app in constants.CORE_CHARMS.items():
        juju.cli(
            "ssh",
            "--container",
            app.replace("-k8s", ""),
            f"{app}/0",
            "bash -lc " + shlex.quote("airflow dags reserialize"),
        )

    for component, app in constants.CORE_CHARMS.items():
        cfg = read_airflow_config(
            juju, f"{app}/0", constants.CONTAINER_NAMES[component]
        )
        assert cfg.get("core", "load_examples") == "True", (
            f"Expected load_examples=True in {app} config"
        )

    scheduler_unit = f"{constants.CORE_CHARMS['scheduler']}/0"
    scheduler_container = constants.CONTAINER_NAMES["scheduler"]
    out = juju.cli(
        "ssh",
        "--container",
        scheduler_container,
        scheduler_unit,
        "bash -lc "
        + shlex.quote("PYTHONWARNINGS=ignore airflow dags list --output json"),
    )
    assert isinstance(json_from_airflow(out), list)


@pytest.mark.abort_on_fail
def test_scheduler_scale_and_resilience(
    juju: jubilant.Juju,
):
    """Scheduler should scale up and down while remaining healthy."""
    scheduler_app = constants.CORE_CHARMS["scheduler"]
    scheduler_container = constants.CONTAINER_NAMES["scheduler"]
    dag_id = "latest_only_with_trigger"

    try:
        juju.cli("scale-application", scheduler_app, str(3))
        juju.wait(
            ready=lambda st: st.apps[scheduler_app].is_active
            and len(st.apps[scheduler_app].units) == 3,
            timeout=15 * 60,
        )

        scheduler_unit = f"{scheduler_app}/0"
        juju.cli(
            "ssh",
            "--container",
            scheduler_container,
            scheduler_unit,
            "bash -lc " + shlex.quote(f"airflow dags unpause {dag_id}"),
        )

        status = juju.status()
        for unit_name, unit_status in status.apps[scheduler_app].units.items():
            assert unit_status.is_active, (
                f"{unit_name} should be active, got {unit_status.workload_status.current}"
            )
            assert pebble_service_is_running(
                juju,
                unit_name,
                constants.PEBBLE_SERVICE_NAME,
            ), (
                f"{unit_name}: pebble service '{constants.PEBBLE_SERVICE_NAME}' not active."
            )

            run_id = f"scale-{unit_name.replace('/', '-')}-{int(time.time())}"
            juju.cli(
                "ssh",
                "--container",
                scheduler_container,
                unit_name,
                "bash -lc "
                + shlex.quote(f"airflow dags trigger {dag_id} --run-id {run_id}"),
            )

            for attempt in Retrying(
                stop=stop_after_attempt(6), wait=wait_fixed(30), reraise=True
            ):
                with attempt:
                    out = juju.cli(
                        "ssh",
                        "--container",
                        scheduler_container,
                        scheduler_unit,
                        "bash -lc "
                        + shlex.quote(
                            f"PYTHONWARNINGS=ignore NO_COLOR=1 CLICOLOR=0 TERM=dumb "
                            f"airflow dags list-runs {dag_id} --output json"
                        ),
                    )
                    runs = json_from_airflow(out)
                    if not any(
                        run.get("run_id") == run_id
                        and run.get("state") in {"queued", "running"}
                        for run in runs
                        if isinstance(runs, list)
                    ):
                        raise AssertionError(
                            f"DAG run {run_id} did not reach queued/running"
                        )
    finally:
        juju.cli("scale-application", scheduler_app, "1")
        juju.wait(
            ready=lambda st: st.apps[scheduler_app].is_active
            and len(st.apps[scheduler_app].units) == 1,
            timeout=15 * 60,
        )
