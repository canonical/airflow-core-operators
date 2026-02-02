from __future__ import annotations

import os
import pytest
import jubilant

from tests.integration.helpers.charm_prep import (
    CORE_CHARMS,
    COORDINATOR_APP,
    POSTGRES_APP,
)

COORD_REL = os.environ.get("COORD_REL", "airflow-coordinator")
AIRFLOW_CONFIG_PATH = os.environ.get("AIRFLOW_CONFIG_PATH", "/opt/airflow/airflow.cfg")

PEBBLE_SERVICE = {
    "airflow-api-server-k8s": "airflow",
    "airflow-dag-processor-k8s": "airflow",
    "airflow-scheduler-k8s": "airflow",
    "airflow-triggerer-k8s": "airflow",
}


@pytest.mark.abort_on_fail
def test_coordinator_blocks_until_some_core_components_related(
    juju: jubilant.Juju,
    deployed_stack: bool,
    remove_relation,
):
    remove_relation(
        juju,
        f"{COORDINATOR_APP}:{COORD_REL}",
        f"airflow-scheduler-k8s:{COORD_REL}",
    )

    juju.wait(jubilant.all_agents_idle)

    st = juju.status()
    coord_app = st.apps[COORDINATOR_APP]
    assert coord_app.is_blocked, f"Expected blocked, got {coord_app.status}"


@pytest.mark.abort_on_fail
def test_full_stack_goes_active_and_core_services_run(
    juju: jubilant.Juju,
    deployed_stack: bool,
    relate_core_charms: bool,
    file_exists_fn,
    pebble_services,
    pebble_running,
    unit,
    container_for,
):
        
    juju.wait(
        ready=lambda st: jubilant.all_active(st, POSTGRES_APP, COORDINATOR_APP, *core_apps),
        error=jubilant.any_error,
        timeout=60 * 60,
    )

    for _, app in CORE_CHARMS:
        u = unit(app)
        c = container_for(app)
        service = PEBBLE_SERVICE[app]

        assert file_exists_fn(juju, u, c, AIRFLOW_CONFIG_PATH), (
            f"{app}: expected {AIRFLOW_CONFIG_PATH} to exist"
        )

        services_text = pebble_services(juju, u, c)
        assert pebble_running(services_text, service), (
            f"{app}: pebble service '{service}' not active.\n{services_text}"
        )
