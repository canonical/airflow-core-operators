"""Integration tests validating core charm behavior."""

from __future__ import annotations

import pytest
import jubilant
import shlex

from tests.integration.helpers.airflow_helpers import get_airflow_config_value
from tests.integration.helpers.constants import (
    AIRFLOW_CONFIG_PATH,
    COORDINATOR_APP,
    COORD_REL,
    CORE_APPS,
    CORE_CHARMS,
    PEBBLE_SERVICE_NAME,
    POSTGRES_APP,
    get_core_app,
)

@pytest.mark.abort_on_fail
def test_full_stack_goes_active_and_core_services_run(
    juju: jubilant.Juju,
    deployed_stack: bool,
    relate_core_charms: bool,
):
    """Full stack should go active and core services should be running."""
    juju.wait(
        ready=lambda st: jubilant.all_active(
            st, POSTGRES_APP, COORDINATOR_APP, *CORE_APPS
        ),
        error=jubilant.any_error,
        timeout=60 * 60,
    )

    all_apps = [POSTGRES_APP, COORDINATOR_APP] + [app for _, app in CORE_CHARMS]
    for app in all_apps:
        app_status = juju.status().apps[app]
        assert app_status.is_active, (
            f"{app} should be active, but got status {app_status.app_status.current}"
        )



@pytest.mark.abort_on_fail
def test_pebble_services_and_config_exist(
    juju: jubilant.Juju,
    file_exists_fn,
    pebble_services,
    pebble_running,
    unit,
    container_for,
):
    """Pebble services should be active and config should be present."""
    service_name = PEBBLE_SERVICE_NAME
    for _, app in CORE_CHARMS:
        u = unit(app)
        c = container_for(app)

        assert file_exists_fn(juju, u, c, AIRFLOW_CONFIG_PATH), (
            f"{app}: expected {AIRFLOW_CONFIG_PATH} to exist"
        )

        services_text = pebble_services(juju, u, c)
        assert pebble_running(services_text, service_name), (
            f"{app}: pebble service '{service_name}' not active.\n{services_text}"
        )


@pytest.mark.abort_on_fail
def test_api_health_endpoint_if_available(
    juju: jubilant.Juju,
    unit,
    run_in_unit,
):
    """API server health endpoint should return healthy when available."""
    api_unit = unit(get_core_app("api-server"))

    check_cmd = "command -v curl >/dev/null && curl -s http://localhost:8080/api/v2/monitor/health || echo NO_CURL"
    out = run_in_unit(juju, api_unit, "bash -lc " + shlex.quote(check_cmd))

    compact = out.replace(" ", "").replace("\n", "")
    assert '"status":"healthy"' in compact, (
        f"API health endpoint unhealthy. Output:\n{out}"
    )


@pytest.mark.abort_on_fail
def test_triggerer_health(
    juju: jubilant.Juju,
    unit,
    container_for,
    run_in,
):
    """Triggerer job should report a healthy status."""
    out = run_in(
        juju,
        unit(get_core_app("triggerer")),
        container_for(get_core_app("triggerer")),
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
    run_in,
    unit,
    container_for,
):
    """Airflow CLI should return expected config values."""
    assert (
        get_airflow_config_value(
            juju,
            get_core_app("scheduler"),
            "core",
            "executor",
            run_in,
            unit,
            container_for,
        )
        == "LocalExecutor"
    )
    assert (
        get_airflow_config_value(
            juju,
            get_core_app("api-server"),
            "api",
            "port",
            run_in,
            unit,
            container_for,
        )
        == "8080"
    )
    assert (
        get_airflow_config_value(
            juju,
            get_core_app("triggerer"),
            "logging",
            "base_log_folder",
            run_in,
            unit,
            container_for,
        )
        == "logs"
    )
    assert (
        get_airflow_config_value(
            juju,
            get_core_app("dag-processor"),
            "core",
            "dags_folder",
            run_in,
            unit,
            container_for,
        )
        == "dags"
    )


@pytest.mark.abort_on_fail
def test_charm_statuses_on_missing_relation(
    juju: jubilant.Juju,
    remove_relation,
):
    """Scheduler and coordinator should block if their relation is removed."""
    remove_relation(
        juju,
        f"{COORDINATOR_APP}:{COORD_REL}",
        f"{get_core_app('scheduler')}:{COORD_REL}",
    )

    juju.wait(jubilant.all_agents_idle)

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
    integrate_relation,
):
    """Core charms should go waiting if Postgres is scaled down or removed."""
    for _, app in CORE_CHARMS:
        integrate_relation(
            juju, f"{COORDINATOR_APP}:{COORD_REL}", f"{app}:{COORD_REL}"
        )

    juju.cli("remove-application", POSTGRES_APP, "--no-prompt", "--force")

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
