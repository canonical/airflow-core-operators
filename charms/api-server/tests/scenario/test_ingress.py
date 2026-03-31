# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""Scenario tests for Traefik ingress integration with the Airflow API Server charm."""

import dataclasses
import unittest.mock

import ops
import ops.testing
from charms.airflow_coordinator_k8s.v0.airflow_coordinator import AirflowCoordinatorCoreRequires
from charms.traefik_k8s.v2.ingress import IngressPerAppRequirer
from conftest import ingress_relation_with_url

import constants


def _mock_coordinator_config():
    """Context managers that mock the coordinator to allow config writes."""
    return (
        unittest.mock.patch.object(
            AirflowCoordinatorCoreRequires,
            "can_write_airflow_config",
            new_callable=unittest.mock.PropertyMock,
            return_value=True,
        ),
        unittest.mock.patch.object(
            AirflowCoordinatorCoreRequires,
            "airflow_config_needs_update",
            return_value=False,
        ),
        unittest.mock.patch.object(
            AirflowCoordinatorCoreRequires,
            "write_airflow_config",
            return_value=None,
        ),
    )


def test_active_status_with_ingress_relation(
    context, state, container, api_server_relation, ingress_relation
):
    """Charm reaches ActiveStatus when both coordinator and ingress relations exist."""
    state_in = dataclasses.replace(
        state,
        relations=[api_server_relation, ingress_relation],
        containers=[container],
    )

    mock_can_write, mock_needs_update, mock_write = _mock_coordinator_config()
    with mock_can_write, mock_needs_update, mock_write:
        state_out = context.run(context.on.pebble_ready(container), state_in)

    assert state_out.unit_status == ops.ActiveStatus()

    out_container = state_out.get_container(constants.CONTAINER_NAME)
    layer = out_container.layers["api-server-base"]
    assert constants.SERVICE_NAME in layer.services
    assert layer.services[constants.SERVICE_NAME].command == "airflow api-server"
    assert not layer.services[constants.SERVICE_NAME].environment


def test_active_status_without_ingress_relation(context, state, container, api_server_relation):
    """Ingress is optional — charm reaches ActiveStatus without it."""
    state_in = dataclasses.replace(
        state,
        relations=[api_server_relation],
        containers=[container],
    )

    mock_can_write, mock_needs_update, mock_write = _mock_coordinator_config()
    with mock_can_write, mock_needs_update, mock_write:
        state_out = context.run(context.on.pebble_ready(container), state_in)

    assert state_out.unit_status == ops.ActiveStatus()


def test_ingress_requirer_initialized(context, state, container, api_server_relation):
    """The charm creates an IngressPerAppRequirer instance with port=80."""
    state_in = dataclasses.replace(
        state,
        relations=[api_server_relation],
        containers=[container],
    )

    mock_can_write, mock_needs_update, mock_write = _mock_coordinator_config()
    with mock_can_write, mock_needs_update, mock_write:
        with context(context.on.pebble_ready(container), state_in) as manager:
            manager.run()
            assert hasattr(manager.charm, "_ingress")
            assert isinstance(manager.charm._ingress, IngressPerAppRequirer)


def test_ingress_ready_path_routing(context, state, container, api_server_relation):
    """With path-based routing, the charm extracts and shares the ingress path."""
    ingress_rel = ingress_relation_with_url("http://traefik:8080/test-airflow-api-server-k8s")
    api_server_provides_rel = ops.testing.Relation(
        "airflow-api-server",
        remote_app_data={},
    )
    state_in = dataclasses.replace(
        state,
        relations=[api_server_relation, ingress_rel, api_server_provides_rel],
        containers=[container],
    )

    mock_can_write, mock_needs_update, mock_write = _mock_coordinator_config()
    with mock_can_write, mock_needs_update, mock_write:
        state_out = context.run(context.on.relation_changed(ingress_rel), state_in)

    assert state_out.unit_status == ops.ActiveStatus()

    # Verify ingress path is shared via the airflow-api-server relation
    api_server_provides_out = state_out.get_relations("airflow-api-server")[0]
    assert (
        api_server_provides_out.local_app_data.get("ingress_path") == "test-airflow-api-server-k8s"
    )

    # Verify proxy-headers and environment are set when ingress is active
    out_container = state_out.get_container(constants.CONTAINER_NAME)
    layer = out_container.layers["api-server-base"]
    assert layer.services[constants.SERVICE_NAME].command == "airflow api-server --proxy-headers"
    assert layer.services[constants.SERVICE_NAME].environment == {"FORWARDED_ALLOW_IPS": "*"}


def test_ingress_ready_subdomain_routing(context, state, container, api_server_relation):
    """With subdomain-based routing, no path is shared."""
    ingress_rel = ingress_relation_with_url("http://test-airflow-api-server-k8s.example.com/")
    api_server_provides_rel = ops.testing.Relation(
        "airflow-api-server",
        remote_app_data={},
    )
    state_in = dataclasses.replace(
        state,
        relations=[api_server_relation, ingress_rel, api_server_provides_rel],
        containers=[container],
    )

    mock_can_write, mock_needs_update, mock_write = _mock_coordinator_config()
    with mock_can_write, mock_needs_update, mock_write:
        state_out = context.run(context.on.relation_changed(ingress_rel), state_in)

    assert state_out.unit_status == ops.ActiveStatus()

    # Verify no ingress path is shared (subdomain routing has no path prefix)
    api_server_provides_out = state_out.get_relations("airflow-api-server")[0]
    assert "ingress_path" not in api_server_provides_out.local_app_data

    # Verify proxy-headers and environment are still set (ingress is active)
    out_container = state_out.get_container(constants.CONTAINER_NAME)
    layer = out_container.layers["api-server-base"]
    assert layer.services[constants.SERVICE_NAME].command == "airflow api-server --proxy-headers"
    assert layer.services[constants.SERVICE_NAME].environment == {"FORWARDED_ALLOW_IPS": "*"}


def test_ingress_revoked_on_relation_broken(context, state, container, api_server_relation):
    """When the ingress relation is broken, the charm handles revocation gracefully."""
    ingress_rel = ingress_relation_with_url("http://traefik:8080/test-airflow-api-server-k8s")
    api_server_provides_rel = ops.testing.Relation(
        "airflow-api-server",
        local_app_data={"ingress_path": "test-airflow-api-server-k8s"},
        remote_app_data={},
    )
    state_in = dataclasses.replace(
        state,
        relations=[api_server_relation, ingress_rel, api_server_provides_rel],
        containers=[container],
    )

    mock_can_write, mock_needs_update, mock_write = _mock_coordinator_config()
    with mock_can_write, mock_needs_update, mock_write:
        state_out = context.run(context.on.relation_broken(ingress_rel), state_in)

    # Charm should not crash; still active because coordinator relation is present
    assert state_out.unit_status == ops.ActiveStatus()

    # Verify ingress path is cleared from the airflow-api-server relation
    api_server_provides_out = state_out.get_relations("airflow-api-server")[0]
    assert "ingress_path" not in api_server_provides_out.local_app_data

    # Verify proxy-headers and environment are removed when ingress is revoked
    out_container = state_out.get_container(constants.CONTAINER_NAME)
    layer = out_container.layers["api-server-base"]
    assert layer.services[constants.SERVICE_NAME].command == "airflow api-server"
    assert not layer.services[constants.SERVICE_NAME].environment
