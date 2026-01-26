# airflow-scheduler-k8s

A Juju charm for deploying and managing Apache Airflow Scheduler on Kubernetes. The Airflow Scheduler monitors all tasks and DAGs, then triggers task instances following their dependencies.

## Overview

The Airflow Scheduler charm:

- Runs the Airflow `scheduler` service inside a workload container
- Receives rendered Airflow configuration and secrets from the Airflow Coordinator
- Participates as one of the Airflow core components alongside:
  - API Server
  - Triggerer
  - DAG Processor

## Usage

```
juju deploy airflow-scheduler-k8s
juju deploy airflow-coordinator-k8s
juju integrate airflow-scheduler-k8s airflow-coordinator-k8s
```

The scheduler will enter a blocked state until all required Airflow core components are deployed and related to the Airflow coordinator charm.

## OCI Images

This charm uses the official Airflow OCI image: `ubuntu/airflow`.
