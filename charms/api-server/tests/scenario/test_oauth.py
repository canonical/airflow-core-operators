# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""Unit tests for webserver_config.py handling for OAuth in the API Server charm."""

import dataclasses
import unittest.mock

import ops
import ops.testing
from charms.airflow_coordinator_k8s.v0.airflow_coordinator import AirflowCoordinatorCoreRequires

import constants


def _patch_coordinator_ready():
    """Patch AirflowCoordinatorCoreRequires so the coordinator appears fully
    ready and the webserver config is NOT active (default baseline).
    """
    return unittest.mock.patch.multiple(
        AirflowCoordinatorCoreRequires,
        can_write_airflow_config=unittest.mock.PropertyMock(return_value=True),
        airflow_config_needs_update=unittest.mock.MagicMock(return_value=False),
        write_airflow_config=unittest.mock.MagicMock(return_value=None),
        can_write_webserver_config=unittest.mock.PropertyMock(return_value=False),
    )


def _patch_coordinator_ready_with_webserver(needs_update: bool = True):
    """Patch so the coordinator is fully ready AND webserver config is active."""
    return unittest.mock.patch.multiple(
        AirflowCoordinatorCoreRequires,
        can_write_airflow_config=unittest.mock.PropertyMock(return_value=True),
        airflow_config_needs_update=unittest.mock.MagicMock(return_value=False),
        write_airflow_config=unittest.mock.MagicMock(return_value=None),
        can_write_webserver_config=unittest.mock.PropertyMock(return_value=True),
        webserver_config_needs_update=unittest.mock.MagicMock(return_value=needs_update),
        write_webserver_config=unittest.mock.MagicMock(return_value=None),
    )


class TestWebserverConfigHandling:
    def test_template_only_relation_change_rewrites_config_and_restarts(
        self, context, state, container, api_server_relation, tmp_path
    ):
        """A coordinator template update is applied without a new Pebble-ready event."""
        secret = ops.testing.Secret({"sensitive-data": '{"client_id": "test-client"}'})
        original_template = 'CLIENT_ID = "{{ client_id }}"\nAUTH_USER_REGISTRATION = True'
        updated_template = original_template.replace("True", "False")
        relation = dataclasses.replace(
            api_server_relation,
            remote_app_data={
                "config-template": "[core]\n",
                "secret-sensitive-data": secret.id,
                "webserver-config-template": original_template,
            },
        )
        workload = dataclasses.replace(
            container,
            mounts={
                "airflow-home": ops.testing.Mount(location=constants.AIRFLOW_HOME, source=tmp_path)
            },
        )
        initial_state = dataclasses.replace(
            state, relations=[relation], secrets=[secret], containers=[workload]
        )
        state_out = context.run(
            context.on.relation_changed(relation, remote_unit=0), initial_state
        )

        assert state_out.unit_status == ops.ActiveStatus()
        config_path = tmp_path / "webserver_config.py"
        assert (
            config_path.read_text() == 'CLIENT_ID = "test-client"\nAUTH_USER_REGISTRATION = True'
        )

        updated_relation = dataclasses.replace(
            state_out.get_relation(relation.id),
            remote_app_data={
                **relation.remote_app_data,
                "webserver-config-template": updated_template,
            },
        )
        updated_state = dataclasses.replace(state_out, relations=[updated_relation])

        with unittest.mock.patch("ops.model.Container.restart", autospec=True) as restart_mock:
            state_out = context.run(
                context.on.relation_changed(updated_relation, remote_unit=0), updated_state
            )

        assert state_out.unit_status == ops.ActiveStatus()
        assert (
            config_path.read_text() == 'CLIENT_ID = "test-client"\nAUTH_USER_REGISTRATION = False'
        )
        restart_mock.assert_called_once_with(unittest.mock.ANY, constants.SERVICE_NAME)

    def test_webserver_config_written_when_active_and_needs_update(
        self, context, state, container, api_server_relation
    ):
        """When can_write_webserver_config is True and needs_update is True,
        write_webserver_config is called and the charm reaches ActiveStatus.
        """
        state_in = dataclasses.replace(
            state,
            relations=[api_server_relation],
        )

        with (
            _patch_coordinator_ready_with_webserver(needs_update=True),
            unittest.mock.patch.object(
                AirflowCoordinatorCoreRequires,
                "write_webserver_config",
            ) as write_mock,
        ):
            state_out = context.run(context.on.pebble_ready(container), state_in)

        assert state_out.unit_status == ops.ActiveStatus()

        write_mock.assert_called_once_with(
            filepath=constants.WEBSERVER_CONFIG_PATH,
            user=constants.WORKLOAD_USER,
            group=constants.WORKLOAD_GROUP,
        )

    def test_webserver_config_not_written_when_content_unchanged(
        self, context, state, container, api_server_relation
    ):
        """When can_write_webserver_config is True but needs_update is False,
        write_webserver_config is not called and the service is not restarted.
        """
        state_in = dataclasses.replace(
            state,
            relations=[api_server_relation],
        )

        with (
            _patch_coordinator_ready_with_webserver(needs_update=False),
            unittest.mock.patch.object(
                AirflowCoordinatorCoreRequires,
                "write_webserver_config",
            ) as write_mock,
            unittest.mock.patch("ops.model.Container.restart", autospec=True) as restart_mock,
        ):
            state_out = context.run(context.on.pebble_ready(container), state_in)

        assert state_out.unit_status == ops.ActiveStatus()

        write_mock.assert_not_called()
        restart_mock.assert_not_called()

    def test_service_restarted_when_webserver_config_written(
        self, context, state, container, api_server_relation
    ):
        """When write_webserver_config is called the service is restarted."""
        state_in = dataclasses.replace(
            state,
            relations=[api_server_relation],
        )

        with (
            _patch_coordinator_ready_with_webserver(needs_update=True),
            unittest.mock.patch("ops.model.Container.restart", autospec=True) as restart_mock,
        ):
            state_out = context.run(context.on.pebble_ready(container), state_in)

        assert state_out.unit_status == ops.ActiveStatus()

        restart_mock.assert_called_once()

    def test_stale_webserver_config_removed_when_oauth_inactive(
        self, context, state, container, api_server_relation
    ):
        """When can_write_webserver_config is False but the file exists on
        disk, the file is removed and the service restarts.
        """
        state_in = dataclasses.replace(
            state,
            relations=[api_server_relation],
        )

        with (
            _patch_coordinator_ready(),
            unittest.mock.patch("ops.model.Container.exists", autospec=True, return_value=True),
            unittest.mock.patch("ops.model.Container.remove_path", autospec=True) as remove_mock,
            unittest.mock.patch("ops.model.Container.restart", autospec=True) as restart_mock,
        ):
            state_out = context.run(context.on.pebble_ready(container), state_in)

        assert state_out.unit_status == ops.ActiveStatus()

        remove_mock.assert_called_once()
        restart_mock.assert_called_once()

    def test_no_action_when_oauth_inactive_and_no_file_on_disk(
        self, context, state, container, api_server_relation
    ):
        """When can_write_webserver_config is False and no file exists on
        disk, no remove and no restart.
        """
        state_in = dataclasses.replace(
            state,
            relations=[api_server_relation],
        )

        with (
            _patch_coordinator_ready(),
            unittest.mock.patch("ops.model.Container.exists", autospec=True, return_value=False),
            unittest.mock.patch("ops.model.Container.remove_path", autospec=True) as remove_mock,
            unittest.mock.patch("ops.model.Container.restart", autospec=True) as restart_mock,
        ):
            state_out = context.run(context.on.pebble_ready(container), state_in)

        assert state_out.unit_status == ops.ActiveStatus()

        remove_mock.assert_not_called()
        restart_mock.assert_not_called()

    def test_error_on_needs_update_check_goes_blocked(
        self, context, state, container, api_server_relation
    ):
        """A ConnectionError during webserver_config_needs_update results in
        BlockedStatus.
        """
        state_in = dataclasses.replace(
            state,
            relations=[api_server_relation],
        )

        with (
            unittest.mock.patch.multiple(
                AirflowCoordinatorCoreRequires,
                can_write_airflow_config=unittest.mock.PropertyMock(return_value=True),
                airflow_config_needs_update=unittest.mock.MagicMock(return_value=False),
                write_airflow_config=unittest.mock.MagicMock(return_value=None),
                can_write_webserver_config=unittest.mock.PropertyMock(return_value=True),
                webserver_config_needs_update=unittest.mock.MagicMock(
                    side_effect=ops.pebble.ConnectionError("cannot connect")
                ),
            ),
        ):
            state_out = context.run(context.on.pebble_ready(container), state_in)

        assert state_out.unit_status == ops.BlockedStatus(
            constants.FAILED_TO_CHECK_WEBSERVER_CONFIG_UPDATE_MESSAGE
        )

    def test_error_on_write_webserver_config_goes_blocked(
        self, context, state, container, api_server_relation
    ):
        """A ConnectionError during webserver_config_needs_update results in
        BlockedStatus.
        """
        state_in = dataclasses.replace(
            state,
            relations=[api_server_relation],
        )

        with (
            unittest.mock.patch.multiple(
                AirflowCoordinatorCoreRequires,
                can_write_airflow_config=unittest.mock.PropertyMock(return_value=True),
                airflow_config_needs_update=unittest.mock.MagicMock(return_value=False),
                write_airflow_config=unittest.mock.MagicMock(return_value=True),
                can_write_webserver_config=unittest.mock.PropertyMock(return_value=True),
                webserver_config_needs_update=unittest.mock.MagicMock(return_value=True),
                write_webserver_config=unittest.mock.MagicMock(
                    side_effect=ops.pebble.ConnectionError("cannot connect")
                ),
            ),
        ):
            state_out = context.run(context.on.pebble_ready(container), state_in)

        assert state_out.unit_status == ops.BlockedStatus(
            constants.FAILED_TO_WRITE_WEBSERVER_CONFIG_MESSAGE
        )

    def test_error_on_check_webserver_config_exists_goes_blocked(
        self, context, state, container, api_server_relation
    ):
        """A ConnectionError during webserver_config_needs_update results in
        BlockedStatus.
        """
        state_in = dataclasses.replace(
            state,
            relations=[api_server_relation],
        )

        with (
            unittest.mock.patch.multiple(
                AirflowCoordinatorCoreRequires,
                can_write_airflow_config=unittest.mock.PropertyMock(return_value=True),
                airflow_config_needs_update=unittest.mock.MagicMock(return_value=False),
                write_airflow_config=unittest.mock.MagicMock(return_value=True),
                can_write_webserver_config=unittest.mock.PropertyMock(return_value=False),
            ),
            unittest.mock.patch.object(
                ops.Container,
                "exists",
                side_effect=unittest.mock.MagicMock(
                    side_effect=ops.pebble.ConnectionError("cannot connect")
                ),
            ),
        ):
            state_out = context.run(context.on.pebble_ready(container), state_in)

        assert state_out.unit_status == ops.BlockedStatus(
            constants.FAILED_TO_CHECK_WEBSERVER_CONFIG_EXISTS_MESSAGE
        )

    def test_error_on_remove_webserver_config_goes_blocked(
        self, context, state, container, api_server_relation
    ):
        """A ConnectionError during webserver_config_needs_update results in
        BlockedStatus.
        """
        state_in = dataclasses.replace(
            state,
            relations=[api_server_relation],
        )

        with (
            unittest.mock.patch.multiple(
                AirflowCoordinatorCoreRequires,
                can_write_airflow_config=unittest.mock.PropertyMock(return_value=True),
                airflow_config_needs_update=unittest.mock.MagicMock(return_value=False),
                write_airflow_config=unittest.mock.MagicMock(return_value=True),
                can_write_webserver_config=unittest.mock.PropertyMock(return_value=False),
            ),
            unittest.mock.patch.multiple(
                ops.Container,
                exists=unittest.mock.MagicMock(return_value=True),
                remove_path=unittest.mock.MagicMock(
                    side_effect=ops.pebble.ConnectionError("cannot connect")
                ),
            ),
        ):
            state_out = context.run(context.on.pebble_ready(container), state_in)

        assert state_out.unit_status == ops.BlockedStatus(
            constants.FAILED_TO_REMOVE_WEBSERVER_CONFIG_MESSAGE
        )
