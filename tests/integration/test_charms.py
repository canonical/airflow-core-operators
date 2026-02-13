"""Integration tests validating core charm behavior."""
# from tenacity import retry, stop_after_attempt

import dataclasses
import pytest
import jubilant
import shlex
from tenacity import Retrying, stop_after_attempt, wait_fixed

from tests.integration.conftest import (
    file_exists,
    get_pebble_service_current,
    get_pebble_service_startup,
    pebble_service_is_running,
)
from tests.integration.helpers.airflow_helpers import get_airflow_config_value
import tests.integration.helpers.constants as constants


@pytest.mark.abort_on_fail
def test_full_stack_goes_active_and_core_services_run(
    juju: jubilant.Juju,
    deployed_stack,
):
    """Full stack should go active and core services should be running."""
    juju.wait(
        ready=lambda st: jubilant.all_active(juju.status(), *constants.ALL_APPS),
        timeout=5 * 60,
    )
    
    status = juju.status()
    for app in constants.ALL_APPS:
        app_status = status.apps[app]
        assert app_status.is_active, (
            f"{app} should be active, but got status {app_status.app_status.current}"
        )

    for _, app in constants.CORE_CHARMS.items():
        assert pebble_service_is_running(
            juju, f"{app}/0", constants.PEBBLE_SERVICE_NAME
        ), (
            f"{app}: pebble service '{constants.PEBBLE_SERVICE_NAME}' not active after recovery."
        )


@pytest.mark.abort_on_fail
def test_pebble_services_and_config_exist(
    juju: jubilant.Juju,
):
    """Pebble services should be active and config should be present."""
    service_name = constants.PEBBLE_SERVICE_NAME
    for _, app in constants.CORE_CHARMS.items():
        unit = f"{app}/0"
        container = app.replace("-k8s", "")

        assert file_exists(juju, unit, container, constants.AIRFLOW_CONFIG_PATH), (
            f"{app}: expected {constants.AIRFLOW_CONFIG_PATH} to exist"
        )

        assert pebble_service_is_running(juju, unit, service_name), (
            f"{app}: pebble service '{service_name}' not active."
        )


@pytest.mark.abort_on_fail
def test_api_health_endpoint_if_available(
    juju: jubilant.Juju,
):
    """API server health endpoint should return healthy when available."""
    status_dict = dataclasses.asdict(juju.status())
    model_info = status_dict.get("model") or {}
    model_name = (
        model_info.get("name")
        or status_dict.get("model_name")
        or status_dict.get("model")
    )
    if not model_name:
        raise RuntimeError("Unable to determine Juju model name from status")

    api_app = constants.CORE_CHARMS["api-server"]
    api_unit = f"{api_app}/0"
    service_host = f"{api_app}-endpoints.{model_name}.svc.cluster.local:8080"

    check_cmd = (
        "curl -s http://localhost:8080/api/v2/monitor/health; echo '---'; "
        f"curl -s http://{service_host}/api/v2/monitor/health"
    )
    out = juju.cli("ssh", api_unit, "bash -lc " + shlex.quote(check_cmd))

    parts = out.split("---", 1)
    if len(parts) != 2:
        raise AssertionError(f"Unexpected API health output:\n{out}")

    assert "healthy" in parts[0], "API unhealthy from localhost"
    assert "healthy" in parts[1], "API unhealthy in cluster"


@pytest.mark.abort_on_fail
def test_triggerer_health(
    juju: jubilant.Juju,
):
    """Triggerer job should report a healthy status."""
    juju.wait(jubilant.all_agents_idle, timeout=10 * 60)

    out = juju.cli(
        "ssh",
        "--container",
        constants.CONTAINER_NAMES["triggerer"],
        f"{constants.CORE_CHARMS['triggerer']}/0",
        "bash -lc " + shlex.quote("airflow jobs check --job-type TriggererJob || true"),
    )

    assert (
        "No issues found" in out
        or "Found one alive job" in out
        or "Found 1 alive job" in out
        or "Found" in out
        and "alive job" in out
    ), f"Triggerer check did not pass:\n{out}"


@pytest.mark.abort_on_fail
def test_airflow_config_cli_values(
    juju: jubilant.Juju,
):
    """Airflow CLI should return expected config values."""
    # TODO: Update the assertions related to dags and logs folder oncer the issue https://github.com/canonical/airflow-coordinator-k8s-operator/issues/16 is resolved
    for _, app in constants.CORE_CHARMS.items():
        assert (
            get_airflow_config_value(
                juju,
                app,
                "core",
                "executor",
            )
            == "LocalExecutor"
        )
        assert (
            get_airflow_config_value(
                juju,
                app,
                "api",
                "port",
            )
            == "8080"
        )
        assert (
            get_airflow_config_value(
                juju,
                app,
                "logging",
                "base_log_folder",
            )
            == "logs"
        )
        assert (
            get_airflow_config_value(
                juju,
                app,
                "core",
                "dags_folder",
            )
            == "dags"
        )


@pytest.mark.abort_on_fail
def test_charm_statuses_on_missing_relation(
    juju: jubilant.Juju,
):
    """Scheduler and coordinator should block if their relation is removed."""
    juju.cli(
        "remove-relation",
        f"{constants.COORDINATOR_APP}:{constants.COORD_REL}",
        f"{constants.CORE_CHARMS['scheduler']}:{constants.COORD_REL}",
    )

    juju.wait(jubilant.all_agents_idle, timeout=10 * 60)

    status = juju.status()
    coord_app = status.apps[constants.COORDINATOR_APP]
    sched_app = status.apps[constants.CORE_CHARMS["scheduler"]]

    assert coord_app.is_blocked, (
        f"Expected coordinator blocked, got {coord_app.app_status.current}"
    )
    assert sched_app.is_blocked, (
        f"Expected scheduler blocked, got {sched_app.app_status.current}"
    )

    waiting_components = {"api-server", "triggerer", "dag-processor"}
    for component, app in constants.CORE_CHARMS.items():
        if component in waiting_components:
            app_status = status.apps[app]
            assert app_status.is_waiting, (
                f"Expected {app} waiting, got {app_status.app_status.current}"
            )
    juju.integrate(
        f"{constants.COORDINATOR_APP}:{constants.COORD_REL}",
        f"{constants.CORE_CHARMS['scheduler']}:{constants.COORD_REL}",
    )

    juju.wait(jubilant.all_agents_idle, timeout=5 * 60, successes=3, delay=20)

    status = juju.status()
    for app in constants.ALL_APPS:
        app_status = status.apps[app]
        assert app_status.is_active, (
            f"{app} should be active, but got status {app_status.app_status.current}"
        )


@pytest.mark.abort_on_fail
def test_core_charms_wait_when_database_unavailable(
    juju: jubilant.Juju,
):
    """Core charms should go waiting if Postgres is scaled down or removed."""

    juju.cli(
        "remove-relation",
        f"{constants.PGBOUNCER_APP}:database",
        f"{constants.COORDINATOR_APP}:postgres",
    )

    juju.wait(jubilant.all_agents_idle, timeout=10 * 60)

    juju.wait(
        ready=lambda st: all(
            st.apps[app].is_waiting for _, app in constants.CORE_CHARMS.items()
        ),
        timeout=15 * 60,
    )

    st = juju.status()
    for component, app in constants.CORE_CHARMS.items():
        app_status = st.apps[app]
        assert app_status.is_waiting, (
            f"Expected {app} waiting, got {app_status.app_status.current}"
        )
        expected_state = "active" if component == "api-server" else "backoff"

        for attempt in Retrying(
            stop=stop_after_attempt(3), wait=wait_fixed(20), reraise=True
        ):
            with attempt:
                if expected_state == "active":
                    startup = get_pebble_service_startup(
                        juju, f"{app}/0", constants.PEBBLE_SERVICE_NAME
                    )
                    current = get_pebble_service_current(
                        juju, f"{app}/0", constants.PEBBLE_SERVICE_NAME
                    )
                    assert startup == "enabled" and current == "active", (
                        f"{app}: pebble service '{constants.PEBBLE_SERVICE_NAME}' not active while waiting."
                    )
                else:
                    current = get_pebble_service_current(
                        juju, f"{app}/0", constants.PEBBLE_SERVICE_NAME
                    )
                    assert current == "backoff", (
                        f"{app}: pebble service '{constants.PEBBLE_SERVICE_NAME}' not in backoff while waiting."
                    )
    juju.integrate(
        f"{constants.PGBOUNCER_APP}:database",
        f"{constants.COORDINATOR_APP}:postgres",
    )
    juju.wait(jubilant.all_agents_idle, timeout=5 * 60, successes=3, delay=20)

    status = juju.status()
    for app in constants.ALL_APPS:
        app_status = status.apps[app]
        assert app_status.is_active, (
            f"{app} should be active, but got status {app_status.app_status.current}"
        )
