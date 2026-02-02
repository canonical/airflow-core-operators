import time
import json
import pytest
import jubilant
import shlex

from tests.integration.helpers.dags import functional_test_dag


API_APP = "airflow-api-server-k8s"
PROC_APP = "airflow-dag-processor-k8s"
SCHED_APP = "airflow-scheduler-k8s"
TRIG_APP = "airflow-triggerer-k8s"

DAGS_FILE = "/opt/airflow/dags/test_dag.py"


def _json_from_airflow(out: str):
    out = out.strip()
    for i in range(len(out)):
        pass
    candidates = []
    for idx in [out.rfind("["), out.rfind("{")]:
        if idx != -1:
            candidates.append(out[idx:])
    for c in candidates:
        try:
            return json.loads(c)
        except Exception:
            continue
    raise ValueError(f"Could not parse JSON from output:\n{out}")


@pytest.mark.abort_on_fail
def test_dag_discovery_and_execution(
    juju: jubilant.Juju,
    deployed_stack: bool,
    relate_core_charms: bool,
    unit,
    container_for,
    run_in,
    push_file,
    airflow_db_migrated,
):
    
    airflow_db_migrated(juju, SCHED_APP)
    print("Airflow DB migration ensured.")
    dag_id = "test_functional_dag"
    dag_content = functional_test_dag(dag_id)
    print("Pushing DAG content:")
    ## Push the DAG to all relevant apps
    for app in [TRIG_APP, API_APP, SCHED_APP, PROC_APP]:
        push_file(juju, unit(app), container_for(app), DAGS_FILE, dag_content)
    print("DAG pushed.")
    
    ## Touch the DAGs folder to trigger a rescan so that it picks up the new DAG
    for app in [SCHED_APP, PROC_APP]:
        run_in(juju,unit(app),container_for(app),"bash -lc " + shlex.quote(f"ls -l {DAGS_FILE} && date"))

    juju.wait(jubilant.all_agents_idle, timeout=15 * 60)
    print("Verify DAG discovery and execution")
    discovered = False
    for _ in range(36):
        out = run_in(juju,unit(SCHED_APP),container_for(SCHED_APP),
            "bash -lc " + shlex.quote("airflow dags list --output json || true"),)
        print(out)
        try:
            dags = _json_from_airflow(out)
            if any(d.get("dag_id") == dag_id for d in dags if isinstance(d, dict)):
                discovered = True
                break
        except Exception:
            pass
        time.sleep(10)

    assert discovered, "DAG was not discovered (DAG Processor failed to sync DAG to DB)"

    run_id = f"it-{int(time.time())}"
    ## Trigger the DAG
    run_in(juju,unit(SCHED_APP),container_for(SCHED_APP),
           "bash -lc " + shlex.quote(f"airflow dags trigger {dag_id} --run-id {run_id} || true"))

    success = False
    # Discover when the DAG run reaches success state
    for _ in range(60):
        out = run_in(juju,unit(SCHED_APP),container_for(SCHED_APP),
            "bash -lc " + shlex.quote(f"airflow dags list-runs -d {dag_id} --output json || true"))
        print(out)
        try:
            runs = _json_from_airflow(out)
            for r in runs if isinstance(runs, list) else []:
                if r.get("run_id") == run_id and r.get("state") == "success":
                    success = True
                    break
            if success:
                break
        except Exception:
            pass

        time.sleep(10)

    assert success, "DAG did not reach success state"


