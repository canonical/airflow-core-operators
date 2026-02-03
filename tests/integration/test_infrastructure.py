import pytest
import jubilant
import shlex

from tests.integration.helpers.charm_prep import COORDINATOR_APP

API_APP = "airflow-api-server-k8s"
SCHED_APP = "airflow-scheduler-k8s"
TRIG_APP = "airflow-triggerer-k8s"


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
