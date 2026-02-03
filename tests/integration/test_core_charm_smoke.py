"""Smoke tests for core Airflow charms.

These are lightweight checks intended to catch obvious problems quickly
and complement the existing functional integration tests.

Possible checks to run for each core charm:
- Verify the charm has at least one unit deployed
- Verify the Pebble service is enabled and active
- Verify Airflow config file exists in the container
- Check API health endpoint (if `curl` is available in the unit)
- Basic port listen check for API (optional)
- Verify no obvious crashlooping by looking for `ERROR` in recent logs (not implemented here)

The tests below implement the first four checks using the existing fixtures
provided by `tests/integration/conftest.py`.
"""

from __future__ import annotations

import shlex
import pytest
import jubilant

from tests.integration.helpers.charm_prep import CORE_CHARMS, POSTGRES_APP, COORDINATOR_APP

API_APP = "airflow-api-server-k8s"
AIRFLOW_CONFIG_PATH = "/opt/airflow/airflow.cfg"

@pytest.mark.abort_on_fail
def test_core_apps_have_units(juju: jubilant.Juju, deployed_stack: bool, relate_core_charms: bool):
    st = juju.status()
    for _, app in CORE_CHARMS:
        assert app in st.apps, f"Expected app {app} in Juju status"
        units = st.apps[app].units
        assert units, f"Expected at least one unit for {app}"

@pytest.mark.abort_on_fail
def test_pebble_services_and_config_exist(
    juju: jubilant.Juju,
    deployed_stack: bool,
    relate_core_charms: bool,
    file_exists_fn,
    pebble_services,
    pebble_running,
    unit,
    container_for,
):
    service_name = "airflow"
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
    deployed_stack: bool,
    relate_core_charms: bool,
    unit,
    container_for,
    run_in_unit,
):
    api_unit = unit(API_APP)

    check_cmd = "command -v curl >/dev/null && curl -s http://localhost:8080/api/v2/monitor/health || echo NO_CURL"
    out = run_in_unit(juju, api_unit, "bash -lc " + shlex.quote(check_cmd))

    compact = out.replace(" ", "").replace("\n", "")
    assert '"status":"healthy"' in compact, f"API health endpoint unhealthy. Output:\n{out}"
