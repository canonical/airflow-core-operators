# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""Unit tests for webserver_config.py handling in the API Server charm."""

import unittest.mock

import ops
import ops.testing
from charms.airflow_coordinator_k8s.v0.airflow_coordinator import AirflowCoordinatorCoreRequires

import constants

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


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


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestWebserverConfigHandling:
    def test_webserver_config_written_when_active_and_needs_update(
        self, context, state, container, api_server_relation
    ):
        """When can_write_webserver_config is True and needs_update is True,
        write_webserver_config is called and the charm reaches ActiveStatus.
        """
        state_in = state.__class__(
            leader=True,
            relations=[api_server_relation],
            containers=[container],
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
        state_in = state.__class__(
            leader=True,
            relations=[api_server_relation],
            containers=[container],
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
        state_in = state.__class__(
            leader=True,
            relations=[api_server_relation],
            containers=[container],
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
        state_in = state.__class__(
            leader=True,
            relations=[api_server_relation],
            containers=[container],
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
        state_in = state.__class__(
            leader=True,
            relations=[api_server_relation],
            containers=[container],
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

    def test_pebble_connection_error_on_needs_update_check_goes_blocked(
        self, context, state, container, api_server_relation
    ):
        """A ConnectionError during webserver_config_needs_update results in
        BlockedStatus.
        """
        state_in = state.__class__(
            leader=True,
            relations=[api_server_relation],
            containers=[container],
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
            "Failed to check webserver_config.py: Pebble error"
        )

    def test_pebble_error_on_write_goes_blocked(
        self, context, state, container, api_server_relation
    ):
        """A Pebble error during write_webserver_config results in
        BlockedStatus.
        """
        state_in = state.__class__(
            leader=True,
            relations=[api_server_relation],
            containers=[container],
        )

        with (
            unittest.mock.patch.multiple(
                AirflowCoordinatorCoreRequires,
                can_write_airflow_config=unittest.mock.PropertyMock(return_value=True),
                airflow_config_needs_update=unittest.mock.MagicMock(return_value=False),
                write_airflow_config=unittest.mock.MagicMock(return_value=None),
                can_write_webserver_config=unittest.mock.PropertyMock(return_value=True),
                webserver_config_needs_update=unittest.mock.MagicMock(return_value=True),
                write_webserver_config=unittest.mock.MagicMock(
                    side_effect=ops.pebble.ConnectionError("push failed")
                ),
            ),
        ):
            state_out = context.run(context.on.pebble_ready(container), state_in)

        assert state_out.unit_status == ops.BlockedStatus(
            "Failed to write webserver_config.py: Pebble error"
        )

    def test_pebble_connection_error_on_exists_check_goes_blocked(
        self, context, state, container, api_server_relation
    ):
        """A ConnectionError while checking if the stale file exists results
        in BlockedStatus.
        """
        state_in = state.__class__(
            leader=True,
            relations=[api_server_relation],
            containers=[container],
        )

        with (
            _patch_coordinator_ready(),
            unittest.mock.patch(
                "ops.model.Container.exists",
                autospec=True,
                side_effect=ops.pebble.ConnectionError("cannot connect"),
            ),
        ):
            state_out = context.run(context.on.pebble_ready(container), state_in)

        assert state_out.unit_status == ops.BlockedStatus(
            "Failed to check webserver_config.py: Pebble connection error"
        )

    def test_pebble_error_on_remove_goes_blocked(
        self, context, state, container, api_server_relation
    ):
        """A Pebble error while removing a stale file results in BlockedStatus."""
        state_in = state.__class__(
            leader=True,
            relations=[api_server_relation],
            containers=[container],
        )

        with (
            _patch_coordinator_ready(),
            unittest.mock.patch("ops.model.Container.exists", autospec=True, return_value=True),
            unittest.mock.patch(
                "ops.model.Container.remove_path",
                autospec=True,
                side_effect=ops.pebble.PathError("not-found", "cannot remove"),
            ),
        ):
            state_out = context.run(context.on.pebble_ready(container), state_in)

        assert state_out.unit_status == ops.BlockedStatus(
            "Failed to remove webserver_config.py: Pebble error"
        )
