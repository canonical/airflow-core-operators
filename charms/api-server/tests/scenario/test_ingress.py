# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""Scenario tests for Traefik ingress integration with the Airflow API Server charm."""

import dataclasses
import unittest.mock

import ops
from charms.airflow_coordinator_k8s.v0.airflow_coordinator import AirflowCoordinatorCoreRequires
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

def test_ingress_joined_but_not_ready(
    context, state, container, api_server_relation, ingress_relation
):
    """Verify that an incomplete ingress relation does not alter the base configuration.

    1. Simulate container startup (`pebble-ready`) with only the coordinator relation
       to establish the initial `api-server-base` layer.
    2. Introduce an `ingress` relation that lacks a URL in its databag and trigger a
       `relation-changed` event.
    3. Assert that the charm safely ignores the incomplete ingress data, maintains
       `ActiveStatus`, and leaves the base Pebble layer untouched (no proxy headers added).
    """
    state_in = dataclasses.replace(
        state,
        relations=[api_server_relation],
        containers=[container],
    )

    mock_can_write, mock_needs_update, mock_write = _mock_coordinator_config()
    with mock_can_write, mock_needs_update, mock_write:
        state_mid = context.run(context.on.pebble_ready(container), state_in)

    state_mid = dataclasses.replace(state_mid, relations=[api_server_relation, ingress_relation])

    with mock_can_write, mock_needs_update, mock_write:
        state_out = context.run(context.on.relation_changed(ingress_relation), state_mid)

    assert state_out.unit_status == ops.ActiveStatus()

    out_container = state_out.get_container(constants.CONTAINER_NAME)
    layer = out_container.layers["api-server-base"]

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


def test_ingress_ready_subdomain_routing(context, state, container, api_server_relation):
    """When ingress relation exists and started serving with subdomain-based routing."""
    ingress_relation = ingress_relation_with_url("http://test-airflow-api-server-k8s.example.com/")
    state_in = dataclasses.replace(
        state,
        relations=[api_server_relation, ingress_relation],
        containers=[container],
    )

    mock_can_write, mock_needs_update, mock_write = _mock_coordinator_config()
    with mock_can_write, mock_needs_update, mock_write:
        state_out = context.run(context.on.relation_changed(ingress_relation), state_in)

    assert state_out.unit_status == ops.ActiveStatus()

    out_container = state_out.get_container(constants.CONTAINER_NAME)
    layer = out_container.layers["api-server-base"]
    assert layer.services[constants.SERVICE_NAME].command == "airflow api-server --proxy-headers"
    assert layer.services[constants.SERVICE_NAME].environment == {"FORWARDED_ALLOW_IPS": "*"}


def test_ingress_revoked_on_relation_broken(context, state, container, api_server_relation):
    """When the ingress relation is broken, the charm handles revocation gracefully."""
    ingress_relation = ingress_relation_with_url("http://test-airflow-api-server-k8s.example.com/")
    state_in = dataclasses.replace(
        state,
        relations=[api_server_relation, ingress_relation],
        containers=[container],
    )

    mock_can_write, mock_needs_update, mock_write = _mock_coordinator_config()
    with mock_can_write, mock_needs_update, mock_write:
        state_out = context.run(context.on.relation_broken(ingress_relation), state_in)

    assert state_out.unit_status == ops.ActiveStatus()

    out_container = state_out.get_container(constants.CONTAINER_NAME)
    layer = out_container.layers["api-server-base"]
    assert layer.services[constants.SERVICE_NAME].command == "airflow api-server"
    assert not layer.services[constants.SERVICE_NAME].environment
