output "application" {
  value = juju_application.airflow_dag_processor_k8s
}

output "provides" {
  value = {}
}

output "requires" {
  value = {
    airflow_coordinator = "airflow-coordinator"
  }
}
