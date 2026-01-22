import dataclasses
from unittest.mock import PropertyMock, patch

import ops
from charms.airflow_coordinator_k8s.v0.airflow_coordinator import AirflowCoordinatorRequires


def test_pebble_connection_failure_scenario(context, state, container, api_server_relation):
    """Test the scenario when the container cannot connect to Pebble."""
    container = dataclasses.replace(container, can_connect=False)
    state_in = dataclasses.replace(state, relations=[api_server_relation])
    state_out = context.run(context.on.pebble_ready(container), state_in)

    assert state_out.unit_status == ops.WaitingStatus("Waiting for relation data")


def test_missing_relation_status_scenario(context, state, container, api_server_relation):
    """Test the 'Missing relation' block when NOT integrated."""
    state_in = dataclasses.replace(state, relations=[api_server_relation])
    state_out = context.run(context.on.pebble_ready(container), state_in)

    if not state_out.relations:
        assert state_out.unit_status == ops.BlockedStatus(
            "Missing airflow-coordinator relation: airflow-coordinator"
        )
    else:
        assert state_out.unit_status == ops.WaitingStatus("Waiting for relation data")


def test_relation_present_but_not_ready_scenario(context, state, container, api_server_relation):
    """Test status when relation is present but library says 'not ready'."""
    state_in = dataclasses.replace(state, relations=[api_server_relation])
    state_out = context.run(context.on.pebble_ready(container), state_in)

    assert state_out.unit_status == ops.WaitingStatus("Waiting for relation data")


def test_failed_airflow_config_write_scenario(context, state, container, api_server_relation):
    """Test the scenario when the configuration file cannot be written to the container."""
    container = dataclasses.replace(container, can_connect=True)
    state_in = dataclasses.replace(state, relations=[api_server_relation])
    with (
        patch.object(AirflowCoordinatorRequires, "ready", return_value=True),
        patch.object(
            AirflowCoordinatorRequires,
            "write_airflow_config",
            side_effect=Exception("Simulated write failure"),
        ),
    ):
        state_out = context.run(context.on.pebble_ready(container), state_in)

    assert state_out.unit_status == ops.BlockedStatus(
        "Failed to write to config file to workload container"
    )


def test_active_status_flow_scenario(context, state, container, api_server_relation):
    """Test full flow to ActiveStatus using the Scenario framework."""
    state_in = dataclasses.replace(state, relations=[api_server_relation])
    with (
        patch.object(
            AirflowCoordinatorRequires, "ready", new_callable=PropertyMock(return_value=True)
        ),
        patch.object(AirflowCoordinatorRequires, "write_airflow_config", return_value=True),
    ):
        state_out = context.run(context.on.pebble_ready(container), state_in)

    assert state_out.unit_status == ops.ActiveStatus()

    out_container = state_out.get_container("airflow-api-server")
    plan = out_container.layers["api-server-base"]
    assert "airflow-api-server" in plan.services
    assert plan.services["airflow-api-server"].command == "airflow api-server"
