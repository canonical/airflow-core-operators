# airflow-api-server

Charmhub package name: airflow-api-server
More information: https://charmhub.io/airflow-api-server

This charm deploys and manages the Apache Airflow API Server as part of a Charmed Airflow deployment.
It is designed to work only in coordination with the Airflow Coordinator charm, which centralizes
configuration, secrets, and cluster-wide validation.

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


## Other resources

- [Contributing](CONTRIBUTING.md)

- See the [Juju documentation](https://documentation.ubuntu.com/juju/3.6/howto/manage-charms/) for more information about developing and improving charms.
