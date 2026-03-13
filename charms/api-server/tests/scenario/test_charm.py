# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

import dataclasses
import unittest.mock

import ops
from charms.airflow_coordinator_k8s.v0.airflow_coordinator import (
    AirflowCoordinatorCoreRequires as AirflowCoordinatorRequires,
)

import constants


def test_pebble_connection_failure_scenario(context, state, container, api_server_relation):
    """When the container cannot connect to Pebble."""
    container = dataclasses.replace(container, can_connect=False)
    state_in = dataclasses.replace(state, relations=[api_server_relation], containers=[container])

    state_out = context.run(context.on.pebble_ready(container), state_in)

    assert state_out.unit_status == ops.MaintenanceStatus("Cannot connect to workload container")


def test_missing_relation_status_scenario(context, state, container):
    state_in = dataclasses.replace(state, relations=[])

    with (
        unittest.mock.patch("ops.model.Container.stop", autospec=True),
        unittest.mock.patch("ops.model.Container.exists", autospec=True, return_value=False),
    ):
        state_out = context.run(context.on.pebble_ready(container), state_in)

    assert state_out.unit_status == ops.BlockedStatus("Missing airflow-coordinator relation")

    out_container = state_out.get_container(constants.CONTAINER_NAME)
    assert "api-server-base" not in out_container.layers


def test_missing_relation_with_cleanup_config_exists_scenario(context, state, container):
    """Missing relation; stop succeeds; config exists.

    Expect: service is stopped, config is removed, and unit is Blocked due to the
    missing airflow-coordinator relation.
    """
    state_in = dataclasses.replace(state, relations=[])

    with (
        unittest.mock.patch(
            "ops.model.Container.stop", autospec=True, return_value=None
        ) as stop_mock,
        unittest.mock.patch("ops.model.Container.exists", autospec=True, return_value=True),
        unittest.mock.patch("ops.model.Container.remove_path", autospec=True) as remove_mock,
    ):
        state_out = context.run(context.on.pebble_ready(container), state_in)

    stop_mock.assert_called_once()
    remove_mock.assert_called_once()

    assert remove_mock.call_args.kwargs.get("recursive") is False

    assert state_out.unit_status == ops.BlockedStatus("Missing airflow-coordinator relation")

    out_container = state_out.get_container(constants.CONTAINER_NAME)
    assert "api-server-base" not in out_container.layers


def test_stop_service_pebble_api_error_scenario(context, state, container):
    """When relation is missing and stopping the service raises Pebble APIError.

    Charm goes Blocked with "Failed to stop pebble service"
    """
    state_in = dataclasses.replace(state, relations=[])

    svc = unittest.mock.Mock()
    svc.is_running.return_value = True

    with (
        unittest.mock.patch(
            "ops.model.Container.stop",
            autospec=True,
            side_effect=ops.pebble.APIError(
                body={}, code=500, status="Internal Server Error", message="stop failed"
            ),
        ),
    ):
        state_out = context.run(context.on.pebble_ready(container), state_in)

    assert state_out.unit_status == ops.BlockedStatus("Failed to stop pebble service")

    out_container = state_out.get_container(constants.CONTAINER_NAME)
    assert "api-server-base" not in out_container.layers


def test_waiting_when_cannot_write_airflow_config(context, state, container, api_server_relation):
    """When coordinator hasn't provided config yet (can_write_airflow_config=False).

    Charm goes Waiting.
    """
    state_in = dataclasses.replace(state, relations=[api_server_relation])

    with unittest.mock.patch.object(
        AirflowCoordinatorRequires,
        "can_write_airflow_config",
        new_callable=unittest.mock.PropertyMock,
        return_value=False,
    ):
        state_out = context.run(context.on.pebble_ready(container), state_in)

    assert state_out.unit_status == ops.WaitingStatus("Waiting for relation data from coordinator")

    out_container = state_out.get_container(constants.CONTAINER_NAME)
    assert "api-server-base" not in out_container.layers


def test_failed_airflow_config_write_pebble_error_scenario(
    context, state, container, api_server_relation
):
    """When writing config fails with a Pebble error.

    Charm goes Blocked.
    """
    state_in = dataclasses.replace(state, relations=[api_server_relation])

    with (
        unittest.mock.patch.object(
            AirflowCoordinatorRequires,
            "can_write_airflow_config",
            new_callable=unittest.mock.PropertyMock,
            return_value=True,
        ),
        unittest.mock.patch.object(
            AirflowCoordinatorRequires,
            "write_airflow_config",
            side_effect=ops.pebble.ConnectionError("Write failed"),
        ),
    ):
        state_out = context.run(context.on.pebble_ready(container), state_in)

    assert state_out.unit_status == ops.BlockedStatus("Failed to write config file: Pebble Error")


def test_failed_airflow_config_write_generic_exception_scenario(
    context, state, container, api_server_relation
):
    """When writing config fails with a non-Pebble exception.

    Charm goes Blocked.
    """
    state_in = dataclasses.replace(state, relations=[api_server_relation])

    with (
        unittest.mock.patch.object(
            AirflowCoordinatorRequires,
            "can_write_airflow_config",
            new_callable=unittest.mock.PropertyMock,
            return_value=True,
        ),
        unittest.mock.patch.object(
            AirflowCoordinatorRequires,
            "write_airflow_config",
            side_effect=RuntimeError("Unexpected error"),
        ),
    ):
        state_out = context.run(context.on.pebble_ready(container), state_in)

    assert state_out.unit_status == ops.BlockedStatus(
        "Failed to write config file to workload container"
    )


def test_replan_failure_scenario(context, state, container, api_server_relation):
    """When Pebble replan fails, charm goes Blocked."""
    state_in = dataclasses.replace(state, relations=[api_server_relation])

    fake_change = unittest.mock.Mock()
    fake_change.id = "1"
    fake_change.kind = "replan"
    fake_change.summary = "replan failed"
    fake_change.tasks = []

    with (
        unittest.mock.patch.object(
            AirflowCoordinatorRequires,
            "can_write_airflow_config",
            new_callable=unittest.mock.PropertyMock,
            return_value=True,
        ),
        unittest.mock.patch.object(
            AirflowCoordinatorRequires,
            "write_airflow_config",
            return_value=None,
        ),
        unittest.mock.patch(
            "ops.model.Container.replan",
            side_effect=ops.pebble.ChangeError(err="x", change=fake_change),
        ),
    ):
        state_out = context.run(context.on.pebble_ready(container), state_in)

    assert state_out.unit_status == ops.BlockedStatus("Failed to replan Pebble services")


def test_active_status_flow_scenario(context, state, container, api_server_relation):
    """When relation exists.

    config is writable
    replan works
    charm goes Active
    charm defines the service.
    """
    state_in = dataclasses.replace(state, relations=[api_server_relation])

    with (
        unittest.mock.patch.object(
            AirflowCoordinatorRequires,
            "can_write_airflow_config",
            new_callable=unittest.mock.PropertyMock,
            return_value=True,
        ),
        unittest.mock.patch.object(
            AirflowCoordinatorRequires,
            "write_airflow_config",
            return_value=None,
        ),
    ):
        state_out = context.run(context.on.pebble_ready(container), state_in)

    assert state_out.unit_status == ops.ActiveStatus()

    out_container = state_out.get_container(constants.CONTAINER_NAME)
    layer = out_container.layers["api-server-base"]
    assert constants.SERVICE_NAME in layer.services
    assert layer.services[constants.SERVICE_NAME].command == "airflow api-server"
    assert layer.services[constants.SERVICE_NAME].startup == "enabled"
    assert layer.services[constants.SERVICE_NAME].override == "merge"
