# airflow-api-server

This charm deploys and manages the Apache Airflow API Server as part of a Charmed Airflow deployment.
An API Server exposes the REST API endpoints that allow clients to programmatically interact with Airflow resources such as DAGs, runs, and task metadata.

## Overview

The Airflow API Server charm:

- Runs the Airflow `api-server` service inside a workload container
- Receives rendered Airflow configuration and secrets from the Airflow Coordinator
- Participates as one of the Airflow core components alongside:
  - Scheduler
  - Triggerer
  - DAG Processor

# Usage

```
juju deploy airflow-api-server-k8s
juju deploy airflow-coordinator-k8s
juju integrate airflow-api-server-k8s airflow-coordinator-k8s
```
- API server goes into BlockedStatus, when relation with `airflow-coordinator-k8s` is missing.
- The charm stays in Waiting status until all required Airflow core charms are related to the coordinator and configuration has been generated.
- The charm transitions to Active once the configuration is written, the Pebble layer is added, and a successful replan occurs.


## OCI Images

This charm uses the official Airflow OCI image: `ubuntu/airflow`.
