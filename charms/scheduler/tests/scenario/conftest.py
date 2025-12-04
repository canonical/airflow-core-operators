import logging

import ops.testing
import pytest

import constants
from charm import AirflowSchedulerCharm

AIRFLOW_VERSION = "3.1.0"
logger = logging.getLogger(__name__)


@pytest.fixture
def airflow_scheduler_charm():
    yield AirflowSchedulerCharm


@pytest.fixture
def container():
    """Fixture to create a mock container."""
    return ops.testing.Container(name=constants.CONTAINER_NAME, can_connect=True)


@pytest.fixture
def context(airflow_scheduler_charm):
    return ops.testing.Context(charm_type=airflow_scheduler_charm)


def _component_relation(component: str) -> ops.testing.Relation:
    """Create a relation with component metadata."""
    return ops.testing.Relation(
        "airflow-coordinator",
        remote_app_data={
            "airflow_version": AIRFLOW_VERSION,
            "workload_image_hash": "somehash",
            "component": component,
        },
    )


@pytest.fixture
def api_server_relation():
    return _component_relation("api-server")


@pytest.fixture
def scheduler_relation():
    return _component_relation("scheduler")


@pytest.fixture
def triggerer_relation():
    return _component_relation("triggerer")


@pytest.fixture
def dag_processor_relation():
    return _component_relation("dag-processor")


@pytest.fixture
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


@pytest.fixture
def state(all_required_relations, container):
    return ops.testing.State(
        leader=True,
        relations=all_required_relations,
        containers=[container],
    )
