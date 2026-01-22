# airflow-triggerer

Charmhub package name: airflow-triggerer-k8s
More information: https://charmhub.io/airflow-triggerer

This charm deploys and manages the Apache Airflow Triggerer as part of a Charmed Airflow deployment.
It is designed to work only in coordination with the Airflow Coordinator charm, which centralizes
configuration, secrets, and cluster-wide validation.

## Overview

The Airflow Triggerer charm:

- Runs the Airflow `triggerer` service inside a workload container
- Receives rendered Airflow configuration and secrets from the Airflow Coordinator
- Participates as one of the Airflow core components alongside:
  - Scheduler
  - API Server
  - DAG Processor

# Usage

```
juju deploy airflow-triggerer-k8s
juju deploy airflow-coordinator-k8s
juju integrate airflow-triggerer-k8s airflow-coordinator-k8s
```
- Triggerer goes into BlockedStatus, when relation with `airflow-coordinator-k8s` is missing.
- The charm stays in Waiting status until all required Airflow core charms are related to the coordinator and configuration has been generated.
- The charm transitions to Active once the configuration is written, the Pebble layer is added, and a successful replan occurs.


## Other resources

- [Contributing](CONTRIBUTING.md)

- See the [Juju documentation](https://documentation.ubuntu.com/juju/3.6/howto/manage-charms/) for more information about developing and improving charms.