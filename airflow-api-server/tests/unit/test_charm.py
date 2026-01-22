import dataclasses
from unittest.mock import PropertyMock, patch

import ops
from charms.airflow_coordinator_k8s.v0.airflow_coordinator import AirflowCoordinatorRequires


def test_pebble_connection_failure_scenario(context, state, container, api_server_relation):
    """Test the scenario when the container cannot connect to Pebble."""
    container = dataclasses.replace(container, can_connect=False)
    state_in = dataclasses.replace(state, relations=[api_server_relation], containers=[container])
    state_out = context.run(context.on.pebble_ready(container), state_in)

    assert state_out.unit_status == ops.MaintenanceStatus("Cannot connect to workload container")


def test_missing_relation_status_scenario(context, state, container):
    """Test the 'Missing relation' block when NOT integrated."""
    state_in = dataclasses.replace(state, relations=[])
    state_out = context.run(context.on.pebble_ready(container), state_in)

    assert state_out.unit_status == ops.BlockedStatus("Missing airflow-coordinator relation")


def test_relation_present_but_not_ready_scenario(context, state, container, api_server_relation):
    """Test status when relation is present but library says 'not ready'."""
    state_in = dataclasses.replace(state, relations=[api_server_relation])
    with patch.object(
        AirflowCoordinatorRequires, "_ready", new_callable=PropertyMock, return_value=False
    ):
        state_out = context.run(context.on.pebble_ready(container), state_in)

    assert state_out.unit_status == ops.WaitingStatus("Waiting for relation data")


def test_cannot_write_airflow_config_scenario(context, state, container, api_server_relation):
    state_in = dataclasses.replace(state, relations=[api_server_relation])

    with (
        patch.object(
            AirflowCoordinatorRequires, "_ready", new_callable=PropertyMock, return_value=True
        ),
        patch.object(
            AirflowCoordinatorRequires,
            "can_write_airflow_config",
            new_callable=PropertyMock,
            return_value=False,
        ),
    ):
        state_out = context.run(context.on.pebble_ready(container), state_in)

    assert state_out.unit_status == ops.BlockedStatus(
        "Cannot write airflow config to workload container"
    )


def test_failed_airflow_config_write_scenario(context, state, container, api_server_relation):
    """Test the scenario when the configuration file cannot be written to the container."""
    container = dataclasses.replace(container)
    state_in = dataclasses.replace(state, relations=[api_server_relation])
    with (
        patch.object(
            AirflowCoordinatorRequires, "_ready", new_callable=PropertyMock, return_value=True
        ),
        patch.object(
            AirflowCoordinatorRequires,
            "can_write_airflow_config",
            new_callable=PropertyMock,
            return_value=True,
        ),
        patch.object(
            AirflowCoordinatorRequires,
            "write_airflow_config",
            side_effect=Exception("Write failed"),
        ),
    ):
        state_out = context.run(context.on.pebble_ready(container), state_in)

    assert state_out.unit_status == ops.BlockedStatus(
        "Failed to write to config file to workload container"
    )


def test_replan_failure_scenario(context, state, container, api_server_relation):
    state_in = dataclasses.replace(state, relations=[api_server_relation])
    with (
        patch.object(
            AirflowCoordinatorRequires, "_ready", new_callable=PropertyMock, return_value=True
        ),
        patch.object(
            AirflowCoordinatorRequires,
            "can_write_airflow_config",
            new_callable=PropertyMock,
            return_value=True,
        ),
        patch.object(AirflowCoordinatorRequires, "write_airflow_config", return_value=None),
        patch("ops.model.Container.replan", side_effect=ops.pebble.ChangeError("x", "y")),
    ):
        state_out = context.run(context.on.pebble_ready(container), state_in)

    assert state_out.unit_status == ops.BlockedStatus("Failed to replan Pebble services")


def test_active_status_flow_scenario(context, state, container, api_server_relation):
    """Test full flow to ActiveStatus using the Scenario framework."""
    state_in = dataclasses.replace(state, relations=[api_server_relation])
    with (
        patch.object(
            AirflowCoordinatorRequires, "_ready", new_callable=PropertyMock, return_value=True
        ),
        patch.object(
            AirflowCoordinatorRequires,
            "can_write_airflow_config",
            new_callable=PropertyMock,
            return_value=True,
        ),
        patch.object(AirflowCoordinatorRequires, "write_airflow_config", return_value=None),
        patch("ops.model.Container.replan", autospec=True) as replan_mock,
    ):
        state_out = context.run(context.on.pebble_ready(container), state_in)

    assert state_out.unit_status == ops.ActiveStatus()
    replan_mock.assert_called_once()

    out_container = state_out.get_container("airflow-api-server")
    plan = out_container.layers["api-server-base"]
    assert "airflow" in plan.services
    assert plan.services["airflow"].command == "airflow api-server"
