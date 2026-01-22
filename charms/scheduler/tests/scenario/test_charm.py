# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

import dataclasses
import unittest.mock

import ops
from charms.airflow_coordinator_k8s.v0.airflow_coordinator import (
    AirflowCoordinatorRequires,
)


def test_pebble_connection_failure_scenario(
    context, state, container, scheduler_relation
):
    """Test the scenario when the container cannot connect to Pebble."""
    container = dataclasses.replace(container, can_connect=False)
    state_in = dataclasses.replace(
        state, relations=[scheduler_relation], containers=[container]
    )
    state_out = context.run(context.on.pebble_ready(container), state_in)

    assert state_out.unit_status == ops.MaintenanceStatus(
        "Cannot connect to workload container"
    )


def test_missing_relation_status_scenario(context, state, container):
    """Test the 'Missing relation' block when NOT integrated."""
    state_in = dataclasses.replace(state, relations=[])
    with (
        unittest.mock.patch("ops.model.Container.stop", autospec=True),
        unittest.mock.patch(
            "ops.model.Container.exists", autospec=True, return_value=False
        ),
    ):
        state_out = context.run(context.on.pebble_ready(container), state_in)

    assert state_out.unit_status == ops.BlockedStatus(
        "Missing airflow-coordinator relation"
    )


def test_missing_relation_with_cleanup_scenario(context, state, container):
    """Test cleanup happens when relation is missing and config exists."""
    state_in = dataclasses.replace(state, relations=[])
    with (
        unittest.mock.patch("ops.model.Container.stop", autospec=True) as stop_mock,
        unittest.mock.patch(
            "ops.model.Container.exists", autospec=True, return_value=True
        ),
        unittest.mock.patch(
            "ops.model.Container.remove_path", autospec=True
        ) as remove_mock,
    ):
        state_out = context.run(context.on.pebble_ready(container), state_in)

    # Verify service stop was called
    stop_mock.assert_called_once()

    # Verify config cleanup was called with correct parameters
    remove_mock.assert_called_once()
    call_args = remove_mock.call_args
    # Check that recursive=False was passed
    assert call_args.kwargs.get("recursive") is False

    assert state_out.unit_status == ops.BlockedStatus(
        "Missing airflow-coordinator relation"
    )


def test_cannot_write_airflow_config_scenario(
    context, state, container, scheduler_relation
):
    """Test WaitingStatus when can't write config (validation failures or missing data)."""
    state_in = dataclasses.replace(state, relations=[scheduler_relation])

    with unittest.mock.patch.object(
        AirflowCoordinatorRequires,
        "can_write_airflow_config",
        new_callable=unittest.mock.PropertyMock,
        return_value=False,
    ):
        state_out = context.run(context.on.pebble_ready(container), state_in)

    assert state_out.unit_status == ops.WaitingStatus("Waiting for relation data")


def test_failed_airflow_config_write_pebble_error_scenario(
    context, state, container, scheduler_relation
):
    """Test BlockedStatus when writing config fails due to Pebble error."""
    state_in = dataclasses.replace(state, relations=[scheduler_relation])
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
            side_effect=ops.pebble.ConnectionError("Connection failed"),
        ),
    ):
        state_out = context.run(context.on.pebble_ready(container), state_in)

    assert state_out.unit_status == ops.BlockedStatus(
        "Failed to write config: Pebble connection error"
    )


def test_failed_airflow_config_write_generic_scenario(
    context, state, container, scheduler_relation
):
    """Test BlockedStatus when writing config fails with generic exception."""
    state_in = dataclasses.replace(state, relations=[scheduler_relation])
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
        "Failed to write config to workload container"
    )


def test_replan_failure_scenario(context, state, container, scheduler_relation):
    """Test BlockedStatus when Pebble replan fails."""
    state_in = dataclasses.replace(state, relations=[scheduler_relation])
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
            "ops.model.Container.replan", side_effect=ops.pebble.ChangeError("x", "y")
        ),
    ):
        state_out = context.run(context.on.pebble_ready(container), state_in)

    assert state_out.unit_status == ops.BlockedStatus(
        "Failed to replan Pebble services"
    )


def test_active_status_flow_scenario(context, state, container, scheduler_relation):
    """Test full flow to ActiveStatus (service starts automatically on replan)."""
    state_in = dataclasses.replace(state, relations=[scheduler_relation])
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
        unittest.mock.patch("ops.model.Container.replan", autospec=True) as replan_mock,
    ):
        state_out = context.run(context.on.pebble_ready(container), state_in)

    assert state_out.unit_status == ops.ActiveStatus()
    replan_mock.assert_called_once()

    out_container = state_out.get_container("airflow-scheduler")
    plan = out_container.layers["scheduler-base"]
    assert "airflow" in plan.services
    assert plan.services["airflow"].command == "airflow scheduler"
    assert plan.services["airflow"].startup == "enabled"


def test_stop_service_pebble_api_error_scenario(context, state, container):
    """Test BlockedStatus when stopping service fails with Pebble API error."""
    state_in = dataclasses.replace(state, relations=[])
    with (
        unittest.mock.patch(
            "ops.model.Container.stop",
            autospec=True,
            side_effect=ops.pebble.APIError(
                body={}, code=500, status="Internal Server Error", message="API error"
            ),
        ),
    ):
        state_out = context.run(context.on.pebble_ready(container), state_in)

    assert state_out.unit_status == ops.BlockedStatus(
        "Failed to stop service: Pebble API error"
    )
