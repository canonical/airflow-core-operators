# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

import dataclasses
import logging

import ops.testing
import pytest

import constants
from charm import AirflowDagProcessorCharm

logger = logging.getLogger(__name__)

AIRFLOW_VERSION = "3.1.0"
LAYER_NAME = "dag-processor-base"


@pytest.fixture
def airflow_dag_processor_charm():
    yield AirflowDagProcessorCharm


@pytest.fixture(scope="function")
def container():
    """Fixture to create a mock container."""
    return ops.testing.Container(name="airflow-dag-processor", can_connect=True)


@pytest.fixture(scope="function")
def container_with_layer(container):
    """Fixture for a mock container with the dag-processor Pebble layer already present."""
    existing_layer = ops.pebble.Layer(
        {
            "services": {
                "airflow-dag-processor": {
                    "override": "replace",
                    "command": "airflow dag-processor",
                    "startup": "enabled",
                }
            }
        }
    )
    return dataclasses.replace(container, layers={"dag-processor-base": existing_layer})


@pytest.fixture(scope="function")
def running_container():
    """Container whose service is already defined and running in Pebble."""
    return ops.testing.Container(
        name="airflow-dag-processor",
        can_connect=True,
        layers={
            LAYER_NAME: ops.pebble.Layer(
                {
                    "services": {
                        constants.SERVICE_NAME: {
                            "override": "replace",
                            "command": "airflow dag-processor",
                            "startup": "enabled",
                        }
                    }
                }
            )
        },
        service_statuses={constants.SERVICE_NAME: ops.pebble.ServiceStatus.ACTIVE},
    )


@pytest.fixture(scope="function")
def context(airflow_dag_processor_charm):
    return ops.testing.Context(charm_type=airflow_dag_processor_charm)


def core_component_metadata(
    component: str, airflow_version: str = AIRFLOW_VERSION, workload_image_hash: str = "somehash"
) -> dict[str, str]:
    return {
        "airflow_version": airflow_version,
        "workload_image_hash": workload_image_hash,
        "component": component,
    }


@pytest.fixture(scope="function")
def api_server_data():
    return core_component_metadata("api-server")


@pytest.fixture(scope="function")
def scheduler_data():
    return core_component_metadata("scheduler")


@pytest.fixture(scope="function")
def triggerer_data():
    return core_component_metadata("triggerer")


@pytest.fixture(scope="function")
def dag_processor_data():
    return core_component_metadata("dag-processor")


@pytest.fixture(scope="function")
def api_server_relation(api_server_data):
    return ops.testing.Relation("airflow-coordinator", remote_app_data=api_server_data)


@pytest.fixture(scope="function")
def scheduler_relation(scheduler_data):
    return ops.testing.Relation("airflow-coordinator", remote_app_data=scheduler_data)


@pytest.fixture(scope="function")
def triggerer_relation(triggerer_data):
    return ops.testing.Relation("airflow-coordinator", remote_app_data=triggerer_data)


@pytest.fixture(scope="function")
def dag_processor_relation(dag_processor_data):
    return ops.testing.Relation("airflow-coordinator", remote_app_data=dag_processor_data)


@pytest.fixture(scope="function")
def all_required_relations(
    api_server_relation,
    scheduler_relation,
    triggerer_relation,
    dag_processor_relation,
):
    return [
        api_server_relation,
        scheduler_relation,
        triggerer_relation,
        dag_processor_relation,
    ]


@pytest.fixture(scope="function")
def state(all_required_relations, container):
    return ops.testing.State(
        leader=True,
        relations=all_required_relations,
        containers=[container],
    )
