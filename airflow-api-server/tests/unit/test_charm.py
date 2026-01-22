import dataclasses
import unittest.mock

import ops
from charms.airflow_coordinator_k8s.v0.airflow_coordinator import AirflowCoordinatorRequires


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


def test_cannot_write_airflow_config_scenario(context, state, container, api_server_relation):
    """Test status when relation exists but it's not yet safe to write config."""
    state_in = dataclasses.replace(state, relations=[api_server_relation])

    with unittest.mock.patch.object(
        AirflowCoordinatorRequires,
        "can_write_airflow_config",
        new_callable=unittest.mock.PropertyMock,
        return_value=False,
    ):
        state_out = context.run(context.on.pebble_ready(container), state_in)

    assert state_out.unit_status == ops.WaitingStatus(
        "Cannot write airflow config to workload container: Waiting for relation data"
    )


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
            "write_airflow_config",
            side_effect=ops.pebble.ConnectionError("Write failed"),
        ),
    ):
        state_out = context.run(context.on.pebble_ready(container), state_in)

    assert state_out.unit_status == ops.BlockedStatus(
        "Failed to write to config file to workload container"
    )


class _FakeChange:
    tasks = []  # ChangeError.__str__ iterates over change.tasks


def test_replan_failure_scenario(context, state, container, api_server_relation):
    """Test status when pebble replan fails."""
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
        unittest.mock.patch(
            "ops.model.Container.replan",
            side_effect=ops.pebble.ChangeError(err="x", change=_FakeChange()),
        ),
    ):
        state_out = context.run(context.on.pebble_ready(container), state_in)

    assert state_out.unit_status == ops.BlockedStatus("Failed to replan Pebble services")


def test_active_status_flow_scenario(context, state, container, api_server_relation):
    """Test full flow to ActiveStatus using the Scenario framework."""
    state_in = dataclasses.replace(state, relations=[api_server_relation])

    # Make layer comparison deterministic: start from an empty plan so we replan exactly once.
    class _FakePlan:
        def to_dict(self):
            return {}

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
        unittest.mock.patch("ops.model.Container.get_plan", return_value=_FakePlan()),
        unittest.mock.patch("ops.model.Container.replan", autospec=True) as replan_mock,
    ):
        state_out = context.run(context.on.pebble_ready(container), state_in)

    assert state_out.unit_status == ops.ActiveStatus()
    replan_mock.assert_called_once()

    out_container = state_out.get_container("airflow-api-server")
    plan = out_container.layers["api-server-base"]
    assert "airflow" in plan.services
    assert plan.services["airflow"].command == "airflow api-server"
    assert plan.services["airflow"].startup == "enabled"
