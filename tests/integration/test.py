from pathlib import Path

content = Path("./dags/functional_test_dag.py").read_text(encoding="utf-8")
print("Yes, ", content)
print(type(content))

CORE_CHARMS = {
    "api-server": "airflow-api-server-k8s",
    "dag-processor": "airflow-dag-processor-k8s",
    "scheduler": "airflow-scheduler-k8s",
    "triggerer": "airflow-triggerer-k8s",
}
CORE_COMPONENTS = CORE_CHARMS.keys()
CORE_APPS = CORE_CHARMS.values()
CORE_APP_BY_COMPONENT = {component: app for component, app in CORE_CHARMS.items()}

print(CORE_APP_BY_COMPONENT)
