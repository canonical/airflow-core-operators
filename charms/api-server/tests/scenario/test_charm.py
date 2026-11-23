# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

import dataclasses
import hashlib
import json
import logging
import unittest.mock

import ops
from charms.airflow_coordinator_k8s.v0.airflow_coordinator import AirflowCoordinatorCoreRequires

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
        AirflowCoordinatorCoreRequires,
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
    ):
        state_out = context.run(context.on.pebble_ready(container), state_in)

    assert state_out.unit_status == ops.ActiveStatus()

    out_container = state_out.get_container(constants.CONTAINER_NAME)
    layer = out_container.layers["api-server-base"]
    assert "airflow" in layer.services
    assert layer.services["airflow"].command == "true"
    assert layer.services["airflow"].startup == "disabled"
    assert layer.services["airflow"].override == "replace"
    assert "airflow-api-server" in layer.services
    assert layer.services["airflow-api-server"].command == "airflow api-server"
    assert layer.services["airflow-api-server"].startup == "enabled"
    assert layer.services["airflow-api-server"].override == "replace"
    assert layer.services["airflow-api-server"].user == "ubuntu"
    assert layer.services["airflow-api-server"].group == "ubuntu"
    assert not layer.services["airflow-api-server"].environment
    assert not layer.log_targets


def test_restart_when_existing_config_changes(
    context, state, running_container, api_server_relation, caplog
):
    """Restart an already running service, and log it, when its config changes."""
    state_in = dataclasses.replace(
        state, relations=[api_server_relation], containers=[running_container]
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


def test_no_restart_when_config_unchanged(context, state, container, api_server_relation):
    """Do not restart when airflow config content is unchanged."""
    state_in = dataclasses.replace(state, relations=[api_server_relation])

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
    context, state, running_container, api_server_relation, caplog
):
    state_in = dataclasses.replace(
        state, relations=[api_server_relation], containers=[running_container]
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
    context, state, running_container, api_server_relation, caplog
):
    crashed_container = dataclasses.replace(
        running_container,
        service_statuses={constants.SERVICE_NAME: ops.pebble.ServiceStatus.ERROR},
    )
    state_in = dataclasses.replace(
        state, relations=[api_server_relation], containers=[crashed_container]
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
