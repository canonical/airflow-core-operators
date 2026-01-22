import dataclasses
import unittest.mock

import ops
from charms.airflow_coordinator_k8s.v0.airflow_coordinator import AirflowCoordinatorRequires

import constants


def test_pebble_connection_failure_scenario(context, state, container):
    """Test the scenario when the container cannot connect to Pebble."""
    container = dataclasses.replace(container, can_connect=False)
    state_in = dataclasses.replace(state, relations=[], containers=[container])
    state_out = context.run(context.on.pebble_ready(container), state_in)

    assert state_out.unit_status == ops.MaintenanceStatus("Cannot connect to workload container")


def test_missing_relation_status_scenario(context, state, container):
    """Test that charm sets blocked status when missing the coordinator relation."""
    state_in = dataclasses.replace(state, relations=[])
    state_out = context.run(context.on.pebble_ready(container), state_in)

    assert state_out.unit_status == ops.BlockedStatus("Missing airflow-coordinator relation")

    out_container = state_out.get_container(constants.CONTAINER_NAME)
    layer = out_container.layers["api-server-base"]
    assert layer.services[constants.SERVICE_NAME].startup == "disabled"


def test_missing_core_components_before_first_ready(
    context, state, container, api_server_relation
):
    """When coordinator reports missing core components on first bring-up."""
    state_in = dataclasses.replace(state, relations=[api_server_relation])

    with unittest.mock.patch.object(
        AirflowCoordinatorRequires,
        "missing_core_components_exist",
        new_callable=unittest.mock.PropertyMock,
        return_value=True,
    ):
        state_out = context.run(context.on.pebble_ready(container), state_in)

    assert state_out.unit_status == ops.WaitingStatus("Waiting for relation data from coordinator")


def test_cannot_write_airflow_config_disables_service_first_bringup(
    context, state, container, api_server_relation
):
    """When config isn't writable on first bring-up."""
    state_in = dataclasses.replace(state, relations=[api_server_relation])

    with unittest.mock.patch.object(
        AirflowCoordinatorRequires,
        "can_write_airflow_config",
        new_callable=unittest.mock.PropertyMock,
        return_value=False,
    ):
        state_out = context.run(context.on.pebble_ready(container), state_in)

    assert state_out.unit_status == ops.WaitingStatus(
        "Cannot write config file to workload container."
    )

    out_container = state_out.get_container(constants.CONTAINER_NAME)
    layer = out_container.layers["api-server-base"]
    assert layer.services[constants.SERVICE_NAME].startup == "disabled"


def test_cannot_write_airflow_config_after_first_ready_stays_active(
    context, state, container, api_server_relation
):
    """After first readiness, if config isn't writable, stay Active."""
    state_in = dataclasses.replace(state, relations=[api_server_relation])

    with (
        unittest.mock.patch.object(
            AirflowCoordinatorRequires,
            "can_write_airflow_config",
            new_callable=unittest.mock.PropertyMock,
            return_value=True,
        ),
        unittest.mock.patch.object(
            AirflowCoordinatorRequires, "write_airflow_config", return_value=None
        ),
        unittest.mock.patch.object(
            AirflowCoordinatorRequires,
            "missing_core_components_exist",
            new_callable=unittest.mock.PropertyMock,
            return_value=False,
        ),
    ):
        state_mid = context.run(context.on.pebble_ready(container), state_in)

    assert state_mid.unit_status == ops.ActiveStatus()

    with (
        unittest.mock.patch.object(
            AirflowCoordinatorRequires,
            "can_write_airflow_config",
            new_callable=unittest.mock.PropertyMock,
            return_value=False,
        ),
        unittest.mock.patch.object(
            AirflowCoordinatorRequires,
            "missing_core_components_exist",
            new_callable=unittest.mock.PropertyMock,
            return_value=True,
        ),
    ):
        state_out = context.run(context.on.pebble_ready(container), state_mid)

    assert state_out.unit_status == ops.ActiveStatus()

    out_container = state_out.get_container(constants.CONTAINER_NAME)
    layer = out_container.layers["api-server-base"]
    assert layer.services[constants.SERVICE_NAME].startup == "enabled"


def test_failed_airflow_config_write_scenario(context, state, container, api_server_relation):
    """Test the scenario when the configuration file cannot be written to the container."""
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
            "missing_core_components_exist",
            new_callable=unittest.mock.PropertyMock,
            return_value=False,
        ),
        unittest.mock.patch.object(
            AirflowCoordinatorRequires,
            "write_airflow_config",
            side_effect=ops.pebble.ConnectionError("Write failed"),
        ),
    ):
        state_out = context.run(context.on.pebble_ready(container), state_in)

    assert state_out.unit_status == ops.BlockedStatus(
        "Failed to write config file to workload container"
    )


def test_replan_failure_scenario(context, state, container, api_server_relation):
    """Test status when pebble replan fails."""
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
            "missing_core_components_exist",
            new_callable=unittest.mock.PropertyMock,
            return_value=False,
        ),
        unittest.mock.patch.object(
            AirflowCoordinatorRequires, "write_airflow_config", return_value=None
        ),
        unittest.mock.patch(
            "ops.model.Container.replan",
            side_effect=ops.pebble.ChangeError(err="x", change=fake_change),
        ),
    ):
        state_out = context.run(context.on.pebble_ready(container), state_in)

    assert state_out.unit_status == ops.BlockedStatus("Failed to replan Pebble services")


def test_active_status_flow_scenario(context, state, container, api_server_relation):
    """When relation exists + config is writable charm should go Active."""
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
            "missing_core_components_exist",
            new_callable=unittest.mock.PropertyMock,
            return_value=False,
        ),
        unittest.mock.patch.object(
            AirflowCoordinatorRequires, "write_airflow_config", return_value=None
        ),
    ):
        state_out = context.run(context.on.pebble_ready(container), state_in)

    assert state_out.unit_status == ops.ActiveStatus()

    out_container = state_out.get_container(constants.CONTAINER_NAME)
    layer = out_container.layers["api-server-base"]
    assert constants.SERVICE_NAME in layer.services
    assert layer.services[constants.SERVICE_NAME].command == "airflow api-server"
    assert layer.services[constants.SERVICE_NAME].startup == "enabled"
