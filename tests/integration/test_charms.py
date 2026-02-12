"""Integration tests validating core charm behavior."""
# from tenacity import retry, stop_after_attempt

import pytest
import jubilant
import shlex

from tests.integration.conftest import (
    file_exists,
    pebble_service_is_running,
)
from tests.integration.helpers.airflow_helpers import get_airflow_config_value, ensure_db_migrated
from tests.integration.helpers.constants import (
    AIRFLOW_CONFIG_PATH,
    ALL_APPS,
    COORDINATOR_APP,
    COORD_REL,
    CORE_CHARMS,
    PEBBLE_SERVICE_NAME,
    PGBOUNCER_APP,
    CORE_APP_BY_COMPONENT,
    CONTAINER_NAMES,
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
    for _, app in CORE_CHARMS.items():
        unit = f"{app}/0"
        container = app.replace("-k8s", "")

        assert file_exists(juju, unit, container, AIRFLOW_CONFIG_PATH), (
            f"{app}: expected {AIRFLOW_CONFIG_PATH} to exist"
        )

        services_text = juju.cli("ssh", "--container", container, unit, "pebble services || true")
        assert pebble_service_is_running(services_text, service_name), (
            f"{app}: pebble service '{service_name}' not active.\n{services_text}"
        )


@pytest.mark.abort_on_fail
def test_api_health_endpoint_if_available(
    juju: jubilant.Juju,
):
    """API server health endpoint should return healthy when available."""
    status = juju.status()
    model = getattr(status, "model", None)
    model_name = getattr(model, "name", None) if model is not None else None
    if model_name is None:
        model_name = getattr(status, "model_name", None)
    if model_name is None:
        model_name = getattr(status, "model", None)
    if model_name is None:
        raise RuntimeError("Unable to determine Juju model name from status")

    api_app = CORE_APP_BY_COMPONENT["api-server"]
    api_unit = f"{api_app}/0"
    service_host = f"{api_app}-endpoints.{model_name}.svc.cluster.local:8080"

    check_cmd = (
        "command -v curl >/dev/null || { echo NO_CURL; exit 0; }; "
        "curl -s http://localhost:8080/api/v2/monitor/health; echo '---'; "
        f"curl -s http://{service_host}/api/v2/monitor/health"
    )
    out = juju.cli("ssh", api_unit, "bash -lc " + shlex.quote(check_cmd))
    if "NO_CURL" in out:
        pytest.skip("curl not available in API server container")

    parts = out.split("---", 1)
    if len(parts) != 2:
        raise AssertionError(f"Unexpected API health output:\n{out}")

    for label, response in [("localhost", parts[0]), ("cluster", parts[1])]:
        compact = response.replace(" ", "").replace("\n", "")
        assert '"status":"healthy"' in compact, (
            f"API health endpoint unhealthy from {label}. Output:\n{response}"
        )

@pytest.mark.abort_on_fail
def test_triggerer_health(
    juju: jubilant.Juju,
):
    """Triggerer job should report a healthy status."""
    juju.wait(jubilant.all_agents_idle, timeout=10 * 60)

    out = juju.cli(
        "ssh",
        "--container",
        CORE_APP_BY_COMPONENT["triggerer"].replace("-k8s", ""),
        f"{CORE_APP_BY_COMPONENT['triggerer']}/0",
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
    for _, app in CORE_CHARMS.items():
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
        f"{COORDINATOR_APP}:{COORD_REL}",
        f"{CORE_APP_BY_COMPONENT['scheduler']}:{COORD_REL}",
    )

    juju.wait(jubilant.all_agents_idle, timeout=10 * 60)

    st = juju.status()
    coord_app = st.apps[COORDINATOR_APP]
    sched_app = st.apps[CORE_APP_BY_COMPONENT["scheduler"]]

    assert coord_app.is_blocked, (
        f"Expected coordinator blocked, got {coord_app.app_status.current}"
    )
    assert sched_app.is_blocked, (
        f"Expected scheduler blocked, got {sched_app.app_status.current}"
    )

    waiting_components = {"api-server", "triggerer", "dag-processor"}
    for component, app in CORE_CHARMS.items():
        if component in waiting_components:
            app_status = st.apps[app]
            assert app_status.is_waiting, (
                f"Expected {app} waiting, got {app_status.app_status.current}"
            )
    juju.integrate(
        f"{COORDINATOR_APP}:{COORD_REL}",
        f"{CORE_APP_BY_COMPONENT['scheduler']}:{COORD_REL}",
    )

    juju.wait(
        ready=lambda st: jubilant.all_active(st, *ALL_APPS),
        timeout=10 * 60,
    )
    status = juju.status()
    for app in ALL_APPS:
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
        f"{PGBOUNCER_APP}:database",
        f"{COORDINATOR_APP}:postgres",
    )

    juju.wait(jubilant.all_agents_idle, timeout=10 * 60)

    juju.wait(
        ready=lambda st: all(st.apps[app].is_waiting for _, app in CORE_CHARMS.items()),
        timeout=15 * 60,
    )

    st = juju.status()
    for _, app in CORE_CHARMS.items():
        app_status = st.apps[app]
        assert app_status.is_waiting, (
            f"Expected {app} waiting, got {app_status.app_status.current}"
        )
        container = CONTAINER_NAMES[app]
        services_text = juju.cli("ssh", "--container", container, f"{app}/0", "pebble services || true")
        assert pebble_service_is_running(services_text, PEBBLE_SERVICE_NAME), (
            f"{app}: pebble service '{PEBBLE_SERVICE_NAME}' not active while waiting.\n{services_text}"
        )
    juju.integrate(
        f"{PGBOUNCER_APP}:database",
        f"{COORDINATOR_APP}:postgres",
    )
    juju.wait(
        ready=lambda st: jubilant.all_active(st, *ALL_APPS),
        timeout=10 * 60,
    )
    for _, app in CORE_CHARMS.items():
        ensure_db_migrated(juju, app)
    status = juju.status()
    for app in ALL_APPS:
        app_status = status.apps[app]
        assert app_status.is_active, (
            f"{app} should be active, but got status {app_status.app_status.current}"
        )
