output "application" {
  value = juju_application.airflow_api_server_k8s
}

output "provides" {
  value = {
    airflow_api_server = "airflow-api-server"
  }
}

output "requires" {
  value = {
    airflow_coordinator = "airflow-coordinator"
  }
}
