# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

import dataclasses
import hashlib
import json
import logging
import unittest.mock

import ops
from charms.airflow_coordinator_k8s.v0.airflow_coordinator import (
    AirflowCoordinatorCoreRequires,
)

import constants


def test_pebble_connection_failure_scenario(context, state, container, scheduler_relation):
    """Test the scenario when the container cannot connect to Pebble."""
    container = dataclasses.replace(container, can_connect=False)
    state_in = dataclasses.replace(state, relations=[scheduler_relation], containers=[container])
    state_out = context.run(context.on.pebble_ready(container), state_in)

    assert state_out.unit_status == ops.MaintenanceStatus("Cannot connect to workload container")


def test_missing_relation_status_scenario(context, state, container):
    """Test the 'Missing relation' block when NOT integrated."""
    state_in = dataclasses.replace(state, relations=[])
    with (
        unittest.mock.patch("ops.model.Container.stop", autospec=True),
        unittest.mock.patch("ops.model.Container.exists", autospec=True, return_value=False),
    ):
        state_out = context.run(context.on.pebble_ready(container), state_in)

    assert state_out.unit_status == ops.BlockedStatus("Missing airflow-coordinator relation")


def test_missing_relation_with_cleanup_scenario(context, state, container):
    """Test cleanup happens when relation is missing and config exists."""
    state_in = dataclasses.replace(state, relations=[])
    with (
        unittest.mock.patch("ops.model.Container.stop", autospec=True) as stop_mock,
        unittest.mock.patch("ops.model.Container.exists", autospec=True, return_value=True),
        unittest.mock.patch("ops.model.Container.remove_path", autospec=True) as remove_mock,
    ):
        state_out = context.run(context.on.pebble_ready(container), state_in)

    # Verify service stop was called
    stop_mock.assert_called_once()

    # Verify config cleanup was called with correct parameters
    remove_mock.assert_called_once()
    call_args = remove_mock.call_args
    # Check that recursive=False was passed
    assert call_args.kwargs.get("recursive") is False

    assert state_out.unit_status == ops.BlockedStatus("Missing airflow-coordinator relation")


def test_cannot_write_airflow_config_scenario(context, state, container, scheduler_relation):
    """Test WaitingStatus when can't write config (validation failures or missing data)."""
    state_in = dataclasses.replace(state, relations=[scheduler_relation])

    with unittest.mock.patch.object(
        AirflowCoordinatorCoreRequires,
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
            AirflowCoordinatorCoreRequires,
            "can_write_airflow_config",
            new_callable=unittest.mock.PropertyMock,
            return_value=True,
        ),
        unittest.mock.patch.object(
            AirflowCoordinatorCoreRequires,
            "airflow_config_needs_update",
            return_value=True,
        ),
        unittest.mock.patch.object(
            AirflowCoordinatorCoreRequires,
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
            AirflowCoordinatorCoreRequires,
            "can_write_airflow_config",
            new_callable=unittest.mock.PropertyMock,
            return_value=True,
        ),
        unittest.mock.patch.object(
            AirflowCoordinatorCoreRequires,
            "airflow_config_needs_update",
            return_value=True,
        ),
        unittest.mock.patch.object(
            AirflowCoordinatorCoreRequires,
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
    fake_change = unittest.mock.Mock()
    fake_change.id = "1"
    fake_change.kind = "replan"
    fake_change.summary = "replan failed"
    fake_change.tasks = []
    with (
        unittest.mock.patch.object(
            AirflowCoordinatorCoreRequires,
            "can_write_airflow_config",
            new_callable=unittest.mock.PropertyMock,
            return_value=True,
        ),
        unittest.mock.patch.object(
            AirflowCoordinatorCoreRequires,
            "airflow_config_needs_update",
            return_value=True,
        ),
        unittest.mock.patch.object(
            AirflowCoordinatorCoreRequires, "write_airflow_config", return_value=None
        ),
        unittest.mock.patch(
            "ops.model.Container.replan",
            side_effect=ops.pebble.ChangeError(err="x", change=fake_change),
        ),
    ):
        state_out = context.run(context.on.pebble_ready(container), state_in)

    assert state_out.unit_status == ops.BlockedStatus("Failed to replan Pebble services")


def test_active_status_flow_scenario(context, state, container, scheduler_relation):
    """Test full flow to ActiveStatus (service starts automatically on replan)."""
    state_in = dataclasses.replace(state, relations=[scheduler_relation])
    with (
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
            AirflowCoordinatorCoreRequires, "write_airflow_config", return_value=None
        ),
        unittest.mock.patch("ops.model.Container.replan", autospec=True) as replan_mock,
    ):
        state_out = context.run(context.on.pebble_ready(container), state_in)

    assert state_out.unit_status == ops.ActiveStatus()
    replan_mock.assert_called_once()

    out_container = state_out.get_container(constants.CONTAINER_NAME)
    plan = out_container.layers["scheduler-base"]
    assert "airflow" in plan.services
    assert plan.services["airflow"].command == "true"
    assert plan.services["airflow"].startup == "disabled"
    assert plan.services["airflow"].override == "replace"
    assert "airflow-scheduler" in plan.services
    assert plan.services["airflow-scheduler"].command == "airflow scheduler"
    assert plan.services["airflow-scheduler"].startup == "enabled"
    assert plan.services["airflow-scheduler"].override == "replace"
    assert plan.services["airflow-scheduler"].user == "ubuntu"
    assert plan.services["airflow-scheduler"].group == "ubuntu"
    assert not plan.log_targets


def test_restart_when_existing_config_changes(
    context, state, running_container, scheduler_relation, caplog
):
    """Restart an already running service, and log it, when its config changes."""
    state_in = dataclasses.replace(
        state, relations=[scheduler_relation], containers=[running_container]
    )
    with (
        caplog.at_level(logging.WARNING, logger="security_logging"),
        unittest.mock.patch.object(
            AirflowCoordinatorCoreRequires,
            "can_write_airflow_config",
            new_callable=unittest.mock.PropertyMock,
            return_value=True,
        ),
        unittest.mock.patch.object(
            AirflowCoordinatorCoreRequires,
            "airflow_config_needs_update",
            return_value=True,
        ),
        unittest.mock.patch.object(
            AirflowCoordinatorCoreRequires,
            "write_airflow_config",
            return_value=None,
        ),
        unittest.mock.patch("ops.model.Container.restart", autospec=True) as restart_mock,
    ):
        state_out = context.run(context.on.pebble_ready(running_container), state_in)

    assert state_out.unit_status == ops.ActiveStatus()
    restart_mock.assert_called_once()
    _assert_single_security_event(
        caplog, "sys_restart:juju-charm", trigger="configuration_changed"
    )


def test_no_restart_when_config_unchanged(context, state, container, scheduler_relation):
    """Do not restart when airflow config content is unchanged."""
    state_in = dataclasses.replace(state, relations=[scheduler_relation])
    with (
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
        unittest.mock.patch("ops.model.Container.restart", autospec=True) as restart_mock,
    ):
        state_out = context.run(context.on.pebble_ready(container), state_in)

    assert state_out.unit_status == ops.ActiveStatus()
    restart_mock.assert_not_called()


def test_pod_spec_no_op_when_not_available_scenario(context, state, container, scheduler_relation):
    """Test that when pod spec is not in databag and file doesn't exist, it's a no-op."""
    state_in = dataclasses.replace(state, relations=[scheduler_relation])
    with (
        unittest.mock.patch.object(
            AirflowCoordinatorCoreRequires,
            "can_write_airflow_config",
            new_callable=unittest.mock.PropertyMock,
            return_value=True,
        ),
        unittest.mock.patch.object(
            AirflowCoordinatorCoreRequires, "write_airflow_config", return_value=None
        ),
        unittest.mock.patch.object(
            AirflowCoordinatorCoreRequires,
            "airflow_config_needs_update",
            return_value=False,
        ),
        unittest.mock.patch.object(
            AirflowCoordinatorCoreRequires,
            "can_write_kubernetes_executor_pod_spec",
            new_callable=unittest.mock.PropertyMock,
            return_value=False,
        ),
        unittest.mock.patch.object(
            AirflowCoordinatorCoreRequires, "write_kubernetes_executor_pod_spec"
        ) as write_pod_spec_mock,
        unittest.mock.patch("ops.model.Container.exists", autospec=True, return_value=False),
        unittest.mock.patch("ops.model.Container.replan", autospec=True),
    ):
        state_out = context.run(context.on.pebble_ready(container), state_in)

    assert state_out.unit_status == ops.ActiveStatus()
    write_pod_spec_mock.assert_not_called()


def test_pod_spec_removed_when_not_in_databag_scenario(
    context, state, container, scheduler_relation
):
    """Test that a stale pod spec file is removed when pod spec is no longer in databag."""
    state_in = dataclasses.replace(state, relations=[scheduler_relation])
    with (
        unittest.mock.patch.object(
            AirflowCoordinatorCoreRequires,
            "can_write_airflow_config",
            new_callable=unittest.mock.PropertyMock,
            return_value=True,
        ),
        unittest.mock.patch.object(
            AirflowCoordinatorCoreRequires, "write_airflow_config", return_value=None
        ),
        unittest.mock.patch.object(
            AirflowCoordinatorCoreRequires,
            "airflow_config_needs_update",
            return_value=False,
        ),
        unittest.mock.patch.object(
            AirflowCoordinatorCoreRequires,
            "can_write_kubernetes_executor_pod_spec",
            new_callable=unittest.mock.PropertyMock,
            return_value=False,
        ),
        unittest.mock.patch.object(
            AirflowCoordinatorCoreRequires, "write_kubernetes_executor_pod_spec"
        ) as write_pod_spec_mock,
        unittest.mock.patch("ops.model.Container.exists", autospec=True, return_value=True),
        unittest.mock.patch("ops.model.Container.remove_path", autospec=True) as remove_mock,
        unittest.mock.patch("ops.model.Container.replan", autospec=True),
    ):
        state_out = context.run(context.on.pebble_ready(container), state_in)

    assert state_out.unit_status == ops.ActiveStatus()
    write_pod_spec_mock.assert_not_called()
    remove_mock.assert_called_once()
    call_args = remove_mock.call_args
    assert call_args.kwargs.get("recursive") is False


def test_pod_spec_written_successfully_scenario(context, state, container, scheduler_relation):
    """Test that pod spec is written when available, and charm reaches ActiveStatus."""
    state_in = dataclasses.replace(state, relations=[scheduler_relation])
    with (
        unittest.mock.patch.object(
            AirflowCoordinatorCoreRequires,
            "can_write_airflow_config",
            new_callable=unittest.mock.PropertyMock,
            return_value=True,
        ),
        unittest.mock.patch.object(
            AirflowCoordinatorCoreRequires, "write_airflow_config", return_value=None
        ),
        unittest.mock.patch.object(
            AirflowCoordinatorCoreRequires,
            "airflow_config_needs_update",
            return_value=False,
        ),
        unittest.mock.patch.object(
            AirflowCoordinatorCoreRequires,
            "can_write_kubernetes_executor_pod_spec",
            new_callable=unittest.mock.PropertyMock,
            return_value=True,
        ),
        unittest.mock.patch.object(
            AirflowCoordinatorCoreRequires, "write_kubernetes_executor_pod_spec"
        ) as write_pod_spec_mock,
        unittest.mock.patch("ops.model.Container.replan", autospec=True),
    ):
        state_out = context.run(context.on.pebble_ready(container), state_in)

    assert state_out.unit_status == ops.ActiveStatus()
    write_pod_spec_mock.assert_called_once_with(
        filepath=constants.AIRFLOW_POD_TEMPLATE_FILE_PATH,
        user=constants.WORKLOAD_USER,
        group=constants.WORKLOAD_GROUP,
    )


def test_pod_spec_write_pebble_error_scenario(context, state, container, scheduler_relation):
    """Test BlockedStatus when writing pod spec fails due to Pebble error."""
    state_in = dataclasses.replace(state, relations=[scheduler_relation])
    with (
        unittest.mock.patch.object(
            AirflowCoordinatorCoreRequires,
            "can_write_airflow_config",
            new_callable=unittest.mock.PropertyMock,
            return_value=True,
        ),
        unittest.mock.patch.object(
            AirflowCoordinatorCoreRequires, "write_airflow_config", return_value=None
        ),
        unittest.mock.patch.object(
            AirflowCoordinatorCoreRequires,
            "can_write_kubernetes_executor_pod_spec",
            new_callable=unittest.mock.PropertyMock,
            return_value=True,
        ),
        unittest.mock.patch.object(
            AirflowCoordinatorCoreRequires,
            "write_kubernetes_executor_pod_spec",
            side_effect=ops.pebble.ConnectionError("Connection failed"),
        ),
    ):
        state_out = context.run(context.on.pebble_ready(container), state_in)

    assert state_out.unit_status == ops.BlockedStatus(
        "Failed to write pod spec: Pebble connection error"
    )


def test_pod_spec_write_generic_error_scenario(context, state, container, scheduler_relation):
    """Test BlockedStatus when writing pod spec fails with generic exception."""
    state_in = dataclasses.replace(state, relations=[scheduler_relation])
    with (
        unittest.mock.patch.object(
            AirflowCoordinatorCoreRequires,
            "can_write_airflow_config",
            new_callable=unittest.mock.PropertyMock,
            return_value=True,
        ),
        unittest.mock.patch.object(
            AirflowCoordinatorCoreRequires, "write_airflow_config", return_value=None
        ),
        unittest.mock.patch.object(
            AirflowCoordinatorCoreRequires,
            "can_write_kubernetes_executor_pod_spec",
            new_callable=unittest.mock.PropertyMock,
            return_value=True,
        ),
        unittest.mock.patch.object(
            AirflowCoordinatorCoreRequires,
            "write_kubernetes_executor_pod_spec",
            side_effect=RuntimeError("Unexpected error"),
        ),
    ):
        state_out = context.run(context.on.pebble_ready(container), state_in)

    assert state_out.unit_status == ops.BlockedStatus(
        "Failed to write pod spec to workload container: Unexpected error"
    )


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

    assert state_out.unit_status == ops.BlockedStatus("Failed to stop service: Pebble API error")


def test_otel_config_rendered_and_otlp_endpoint_exposed(context, state, container):
    """Verify OTel config renders component app name and exposes otlp_endpoint."""
    secret = ops.testing.Secret(
        {"sensitive-data": json.dumps({"secret": "s3cret"})},
    )
    config_template = (
        "[core]\nfernet_key = {{ secret }}\n\n"
        "[metrics]\notel_on = True\notel_host = 1.2.3.4\notel_port = 4318\n"
        "otel_service = {{ otel_service }}\notel_ssl_active = False\n\n"
        "[traces]\notel_on = True\notel_host = 1.2.3.4\notel_port = 4318\n"
        "otel_service = {{ otel_service }}\notel_ssl_active = False\n"
    )
    relation = ops.testing.Relation(
        "airflow-coordinator",
        remote_app_data={
            "config-template": config_template,
            "secret-sensitive-data": secret.id,
            "otlp-endpoint": "http://1.2.3.4:4318",
        },
    )
    state_in = dataclasses.replace(
        state,
        relations=[relation],
        secrets=[secret],
    )

    with (
        unittest.mock.patch("ops.model.Container.replan", autospec=True),
        unittest.mock.patch("ops.model.Container.restart", autospec=True),
        context(context.on.pebble_ready(container), state_in) as manager,
    ):
        state_out = manager.run()
        assert manager.charm._config_requires.otlp_endpoint == "http://1.2.3.4:4318"
        app_name = manager.charm.app.name
        charm_name = manager.charm.meta.name
        unit_name = manager.charm.unit.name

    assert state_out.unit_status == ops.ActiveStatus()
    out_container = state_out.get_container(constants.CONTAINER_NAME)
    fs = out_container.get_filesystem(context)
    written_cfg = (fs / constants.AIRFLOW_CONFIG_PATH.lstrip("/")).read_text()
    assert f"otel_service = {app_name}" in written_cfg

    layer = out_container.layers["scheduler-base"]
    expected_instance_id = f"{state_out.model.name}_{state_out.model.uuid}_{app_name}_{unit_name}"
    assert layer.services["airflow-scheduler"].environment == {
        "OTEL_RESOURCE_ATTRIBUTES": (
            f"juju_application={app_name},"
            f"juju_charm={charm_name},"
            f"juju_model={state_out.model.name},"
            f"juju_model_uuid={state_out.model.uuid},"
            f"juju_unit={unit_name},"
            f"service.instance.id={expected_instance_id}"
        )
    }
    target_name = "airflow-scheduler-logs"
    assert target_name in layer.log_targets
    target = layer.log_targets[target_name]
    assert target.type == "opentelemetry"
    assert target.location == "http://1.2.3.4:4318"
    assert target.override == "replace"
    assert target.services == ["airflow-scheduler"]
    assert target.labels == {
        "juju_model": state_out.model.name,
        "juju_model_uuid": state_out.model.uuid,
        "juju_application": app_name,
        "juju_charm": charm_name,
        "juju_unit": unit_name,
        "loki.resource.labels": (
            "juju_application, juju_charm, juju_model, juju_model_uuid, juju_unit"
        ),
    }


def test_log_target_disabled_and_rotated_on_endpoint_changes(context, state, container):
    """Verify Pebble log-targets disable old targets and create new ones on rotation."""
    secret = ops.testing.Secret(
        {"sensitive-data": json.dumps({"secret": "s3cret"})},
    )
    config_template = "[core]\nfernet_key = {{ secret }}\n"
    base_target = "airflow-scheduler-logs"
    existing_layer = ops.pebble.Layer(
        {
            "log-targets": {
                base_target: {
                    "override": "replace",
                    "type": "opentelemetry",
                    "location": "http://1.2.3.4:4318",
                    "services": ["airflow-scheduler"],
                },
                "other-custom-target": {
                    "override": "replace",
                    "type": "loki",
                    "location": "http://loki:3100/loki/api/v1/push",
                    "services": ["airflow-scheduler"],
                },
            }
        }
    )
    container_with_target = dataclasses.replace(container, layers={"other-layer": existing_layer})

    # 1. Endpoint unset: existing target is disabled with services: []
    relation_unset = ops.testing.Relation(
        "airflow-coordinator",
        remote_app_data={
            "config-template": config_template,
            "secret-sensitive-data": secret.id,
        },
    )
    state_unset = dataclasses.replace(
        state,
        relations=[relation_unset],
        secrets=[secret],
        containers=[container_with_target],
    )

    with (
        unittest.mock.patch("ops.model.Container.replan", autospec=True),
        unittest.mock.patch("ops.model.Container.restart", autospec=True),
    ):
        state_out = context.run(context.on.pebble_ready(container_with_target), state_unset)

    assert state_out.unit_status == ops.ActiveStatus()
    layer = state_out.get_container(constants.CONTAINER_NAME).layers["scheduler-base"]
    assert not layer.services["airflow-scheduler"].environment
    assert base_target in layer.log_targets
    assert "other-custom-target" not in layer.log_targets
    target = layer.log_targets[base_target]
    assert target.override == "replace"
    assert target.type == "opentelemetry"
    assert target.location == "http://1.2.3.4:4318"
    assert target.services == []
    assert not target.labels

    # 2. Endpoint rotated: old target remains disabled, new hashed target is active
    new_endpoint = "http://5.6.7.8:4318"
    relation_rotated = ops.testing.Relation(
        "airflow-coordinator",
        remote_app_data={
            "config-template": config_template,
            "secret-sensitive-data": secret.id,
            "otlp-endpoint": new_endpoint,
        },
    )
    state_rotated = dataclasses.replace(
        state_out,
        relations=[relation_rotated],
        secrets=[secret],
    )

    with (
        unittest.mock.patch("ops.model.Container.replan", autospec=True),
        unittest.mock.patch("ops.model.Container.restart", autospec=True),
    ):
        state_out_rotated = context.run(
            context.on.pebble_ready(container_with_target), state_rotated
        )

    assert state_out_rotated.unit_status == ops.ActiveStatus()
    rotated_layer = state_out_rotated.get_container(constants.CONTAINER_NAME).layers[
        "scheduler-base"
    ]
    assert rotated_layer.log_targets[base_target].services == []
    suffix = hashlib.sha256(new_endpoint.encode()).hexdigest()[:8]
    rotated_target_name = f"{base_target}-{suffix}"
    assert rotated_target_name in rotated_layer.log_targets
    rotated_target = rotated_layer.log_targets[rotated_target_name]
    assert rotated_target.location == new_endpoint
    assert rotated_target.services == ["airflow-scheduler"]

    # 3. Rotate back to original endpoint (A -> B -> A): base target is re-enabled, B disabled
    container_after_b = dataclasses.replace(
        container,
        layers={"other-layer": existing_layer, "scheduler-base": rotated_layer},
    )
    relation_back_to_a = ops.testing.Relation(
        "airflow-coordinator",
        remote_app_data={
            "config-template": config_template,
            "secret-sensitive-data": secret.id,
            "otlp-endpoint": "http://1.2.3.4:4318",
        },
    )
    state_back_to_a = dataclasses.replace(
        state_out_rotated,
        relations=[relation_back_to_a],
        secrets=[secret],
        containers=[container_after_b],
    )

    with (
        unittest.mock.patch("ops.model.Container.replan", autospec=True),
        unittest.mock.patch("ops.model.Container.restart", autospec=True),
    ):
        state_out_a_again = context.run(
            context.on.pebble_ready(container_after_b), state_back_to_a
        )

    assert state_out_a_again.unit_status == ops.ActiveStatus()
    layer_a_again = state_out_a_again.get_container(constants.CONTAINER_NAME).layers[
        "scheduler-base"
    ]
    assert layer_a_again.log_targets[base_target].location == "http://1.2.3.4:4318"
    assert layer_a_again.log_targets[base_target].services == ["airflow-scheduler"]
    assert layer_a_again.log_targets[rotated_target_name].services == []


def _security_events(caplog):
    return [
        json.loads(record.getMessage())["owasp_event"]
        for record in caplog.records
        if record.name == "security_logging"
    ]


def _assert_single_security_event(caplog, event_name, **data):
    events = _security_events(caplog)
    assert [event["event"] for event in events] == [event_name]
    assert events[0]["appid"] == constants.APP_ID
    assert events[0]["target"] == constants.SERVICE_NAME
    for key, value in data.items():
        assert events[0]["data"][key] == value


def _coordinator_relation_with_config():
    """Coordinator relation with enough data for the charm to render a real config."""
    secret = ops.testing.Secret({"sensitive-data": json.dumps({"secret": "s3cret"})})
    relation = ops.testing.Relation(
        "airflow-coordinator",
        remote_app_data={
            "config-template": "[core]\nfernet_key = {{ secret }}\n",
            "secret-sensitive-data": secret.id,
        },
    )
    return relation, secret


def test_first_deploy_logs_startup_not_restart(context, state, container, caplog):
    """On a fresh deploy the config file is new and the service has never run.

    Replan starts the service, so this is a startup, not a charm restart.
    """
    relation, secret = _coordinator_relation_with_config()
    state_in = dataclasses.replace(state, relations=[relation], secrets=[secret])

    with (
        caplog.at_level(logging.WARNING, logger="security_logging"),
        unittest.mock.patch("ops.model.Container.restart", autospec=True) as restart_mock,
    ):
        state_out = context.run(context.on.pebble_ready(container), state_in)

    assert state_out.unit_status == ops.ActiveStatus()
    restart_mock.assert_not_called()
    _assert_single_security_event(caplog, "sys_startup:pebble", trigger="pebble_replan")


def test_unchanged_config_while_running_logs_nothing(
    context, state, running_container, scheduler_relation, caplog
):
    state_in = dataclasses.replace(
        state, relations=[scheduler_relation], containers=[running_container]
    )

    with (
        caplog.at_level(logging.WARNING, logger="security_logging"),
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
    ):
        state_out = context.run(context.on.pebble_ready(running_container), state_in)

    assert state_out.unit_status == ops.ActiveStatus()
    assert not _security_events(caplog)


def test_missing_relation_while_running_logs_shutdown(context, state, running_container, caplog):
    state_in = dataclasses.replace(state, relations=[], containers=[running_container])

    with caplog.at_level(logging.WARNING, logger="security_logging"):
        state_out = context.run(context.on.pebble_ready(running_container), state_in)

    assert state_out.unit_status == ops.BlockedStatus("Missing airflow-coordinator relation")
    out_container = state_out.get_container(constants.CONTAINER_NAME)
    assert out_container.service_statuses[constants.SERVICE_NAME] == (
        ops.pebble.ServiceStatus.INACTIVE
    )
    _assert_single_security_event(
        caplog, "sys_shutdown:juju-charm", trigger="required_relation_missing"
    )


def test_missing_relation_while_stopped_logs_nothing(context, state, running_container, caplog):
    stopped_container = dataclasses.replace(
        running_container,
        service_statuses={constants.SERVICE_NAME: ops.pebble.ServiceStatus.INACTIVE},
    )
    state_in = dataclasses.replace(state, relations=[], containers=[stopped_container])

    with caplog.at_level(logging.WARNING, logger="security_logging"):
        state_out = context.run(context.on.pebble_ready(stopped_container), state_in)

    assert state_out.unit_status == ops.BlockedStatus("Missing airflow-coordinator relation")
    assert not _security_events(caplog)


def test_stop_failure_logs_no_shutdown(context, state, running_container, caplog):
    state_in = dataclasses.replace(state, relations=[], containers=[running_container])

    with (
        caplog.at_level(logging.WARNING, logger="security_logging"),
        unittest.mock.patch(
            "ops.model.Container.stop",
            autospec=True,
            side_effect=ops.pebble.APIError(
                body={}, code=500, status="Internal Server Error", message="stop failed"
            ),
        ),
    ):
        state_out = context.run(context.on.pebble_ready(running_container), state_in)

    assert isinstance(state_out.unit_status, ops.BlockedStatus)
    assert not _security_events(caplog)


def test_replan_failure_with_service_in_error_logs_crash(
    context, state, running_container, scheduler_relation, caplog
):
    crashed_container = dataclasses.replace(
        running_container,
        service_statuses={constants.SERVICE_NAME: ops.pebble.ServiceStatus.ERROR},
    )
    state_in = dataclasses.replace(
        state, relations=[scheduler_relation], containers=[crashed_container]
    )
    fake_change = unittest.mock.Mock()
    fake_change.id = "1"
    fake_change.kind = "replan"
    fake_change.summary = "replan failed"
    fake_change.tasks = []

    with (
        caplog.at_level(logging.WARNING, logger="security_logging"),
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
        unittest.mock.patch(
            "ops.model.Container.replan",
            side_effect=ops.pebble.ChangeError(err="x", change=fake_change),
        ),
    ):
        state_out = context.run(context.on.pebble_ready(crashed_container), state_in)

    assert state_out.unit_status == ops.BlockedStatus("Failed to replan Pebble services")
    _assert_single_security_event(caplog, "sys_crash:PebbleServiceError", status="error")
