"""Integration tests validating core charm behavior."""

from __future__ import annotations

import pytest
import jubilant
import shlex

from tests.integration.conftest import (
    file_exists,
    pebble_services_text,
    pebble_service_is_running,
    ssh,
    ssh_unit,
    unit_name,
    workload_container_for_app,
    remove_relation_if_exists,
    integrate_if_missing,
)
from tests.integration.helpers.airflow_helpers import get_airflow_config_value
from tests.integration.helpers.constants import (
    AIRFLOW_CONFIG_PATH,
    ALL_APPS,
    COORDINATOR_APP,
    COORD_REL,
    CORE_CHARMS,
    PEBBLE_SERVICE_NAME,
    POSTGRES_APP,
    get_core_app,
)


@pytest.mark.abort_on_fail
def test_full_stack_goes_active_and_core_services_run(
    juju: jubilant.Juju,
    deployed_stack,
):
    """Full stack should go active and core services should be running."""
    juju.wait(lambda st: jubilant.all_active(st, *ALL_APPS), timeout=5 * 60)

    status = juju.status()
    for app in ALL_APPS:
        app_status = status.apps[app]
        assert app_status.is_active, (
            f"{app} should be active, but got status {app_status.app_status.current}"
        )


@pytest.mark.abort_on_fail
def test_pebble_services_and_config_exist(
    juju: jubilant.Juju,
):
    """Pebble services should be active and config should be present."""
    service_name = PEBBLE_SERVICE_NAME
    for _, app in CORE_CHARMS:
        u = unit_name(app)
        c = workload_container_for_app(app)

        assert file_exists(juju, u, c, AIRFLOW_CONFIG_PATH), (
            f"{app}: expected {AIRFLOW_CONFIG_PATH} to exist"
        )

        services_text = pebble_services_text(juju, u, c)
        assert pebble_service_is_running(services_text, service_name), (
            f"{app}: pebble service '{service_name}' not active.\n{services_text}"
        )


@pytest.mark.abort_on_fail
def test_api_health_endpoint_if_available(
    juju: jubilant.Juju,
):
    """API server health endpoint should return healthy when available."""
    api_unit = unit_name(get_core_app("api-server"))

    check_cmd = "command -v curl >/dev/null && curl -s http://localhost:8080/api/v2/monitor/health || echo NO_CURL"
    out = ssh_unit(juju, api_unit, "bash -lc " + shlex.quote(check_cmd))

    compact = out.replace(" ", "").replace("\n", "")
    assert '"status":"healthy"' in compact, (
        f"API health endpoint unhealthy. Output:\n{out}"
    )


@pytest.mark.abort_on_fail
def test_triggerer_health(
    juju: jubilant.Juju,
):
    """Triggerer job should report a healthy status."""
    juju.wait(jubilant.all_agents_idle, timeout=10 * 60)

    out = ssh(
        juju,
        unit_name(get_core_app("triggerer")),
        workload_container_for_app(get_core_app("triggerer")),
        "bash -lc 'airflow jobs check --job-type TriggererJob || true'",
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
    assert (
        get_airflow_config_value(
            juju,
            get_core_app("scheduler"),
            "core",
            "executor",
        )
        == "LocalExecutor"
    )
    assert (
        get_airflow_config_value(
            juju,
            get_core_app("api-server"),
            "api",
            "port",
        )
        == "8080"
    )
    assert (
        get_airflow_config_value(
            juju,
            get_core_app("triggerer"),
            "logging",
            "base_log_folder",
        )
        == "logs"
    )
    assert (
        get_airflow_config_value(
            juju,
            get_core_app("dag-processor"),
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
    remove_relation_if_exists(
        juju,
        f"{COORDINATOR_APP}:{COORD_REL}",
        f"{get_core_app('scheduler')}:{COORD_REL}",
    )

    juju.wait(jubilant.all_agents_idle, timeout=10 * 60)

    st = juju.status()
    coord_app = st.apps[COORDINATOR_APP]
    sched_app = st.apps[get_core_app("scheduler")]

    assert coord_app.is_blocked, (
        f"Expected coordinator blocked, got {coord_app.app_status.current}"
    )
    assert sched_app.is_blocked, (
        f"Expected scheduler blocked, got {sched_app.app_status.current}"
    )

    waiting_components = {"api-server", "triggerer", "dag-processor"}
    for component, app in CORE_CHARMS:
        if component in waiting_components:
            app_status = st.apps[app]
            assert app_status.is_waiting, (
                f"Expected {app} waiting, got {app_status.app_status.current}"
            )


@pytest.mark.abort_on_fail
def test_core_charms_wait_when_postgres_scaled_down(
    juju: jubilant.Juju,
):
    """Core charms should go waiting if Postgres is scaled down or removed."""
    integrate_if_missing(
        juju,
        f"{COORDINATOR_APP}:{COORD_REL}",
        f"{get_core_app('scheduler')}:{COORD_REL}",
    )
    juju.cli("remove-application", POSTGRES_APP, "--no-prompt", "--force")

    juju.wait(jubilant.all_agents_idle, timeout=10 * 60)

    juju.wait(
        ready=lambda st: all(st.apps[app].is_waiting for _, app in CORE_CHARMS),
        timeout=15 * 60,
    )

    st = juju.status()
    for _, app in CORE_CHARMS:
        app_status = st.apps[app]
        assert app_status.is_waiting, (
            f"Expected {app} waiting, got {app_status.app_status.current}"
        )
