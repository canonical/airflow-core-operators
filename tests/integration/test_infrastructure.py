import pytest
import jubilant
import shlex

from tests.integration.helpers.charm_prep import COORDINATOR_APP

API_APP = "airflow-api-server-k8s"
SCHED_APP = "airflow-scheduler-k8s"
TRIG_APP = "airflow-triggerer-k8s"

@pytest.mark.abort_on_fail
def test_api_health_endpoint(
    juju: jubilant.Juju,
    deployed_stack: bool,
    relate_core_charms: bool,
    unit,
    container_for,
    run_in,
):
    api_unit = unit(API_APP)
    api_container = container_for(API_APP)

    cmd = "curl -s http://localhost:8080/api/v2/monitor/health || true" 

    out = run_in(
        juju,
        api_unit,
        api_container,
        "bash -lc" + shlex.quote(cmd),
    ).lower()

    compact = out.replace(" ", "").replace("\n", "")
    assert '"status":"healthy"' in compact, f"Health endpoint unhealthy. Output:\n{out}"



@pytest.mark.abort_on_fail
def test_triggerer_health(
    juju: jubilant.Juju,
    deployed_stack: bool,
    relate_core_charms: bool,
    unit,
    container_for,
    run_in,
    airflow_db_migrated,
):
    airflow_db_migrated(juju, SCHED_APP)

    out = run_in(juju,unit(TRIG_APP),container_for(TRIG_APP),
        "bash -lc 'airflow jobs check --job-type TriggererJob || true'",)

    assert (
        "No issues found" in out
        or "Found one alive job" in out
        or "Found 1 alive job" in out
        or "Found" in out and "alive job" in out
    ), f"Triggerer check did not pass:\n{out}"
