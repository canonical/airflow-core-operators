# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

import dataclasses
import logging

import ops
import ops.testing
import pytest
from charms.airflow_api_server_k8s.v0.airflow_api_server import (
    AirflowAPIServerProvides,
    AirflowAPIServerRequires,
)

import constants

logger = logging.getLogger(__name__)


class AirflowAPIServerMockCharm(ops.CharmBase):
    """Mock Airflow API Server charm."""

    def __init__(self, *args):
        super().__init__(*args)

        self.provides = AirflowAPIServerProvides(
            self,
            constants.AIRFLOW_API_SERVER_RELATION_ENDPOINT,
            "test-host",
            "test-port",
        )

        self.unit.status = ops.ActiveStatus()


class AirflowAPIServerRequirerMockCharm(ops.CharmBase):
    """Mock charm that requires the airflow-api-server relation."""

    def __init__(self, *args):
        super().__init__(*args)

        self.requires = AirflowAPIServerRequires(
            self,
            constants.AIRFLOW_API_SERVER_RELATION_ENDPOINT,
            self.reconcile,
        )

        self.unit.status = ops.BlockedStatus()

    def reconcile(self, event) -> None:
        logger.info(f"§Requirer reacting to event: {type(event)}")

        self.unit.status = ops.ActiveStatus()


@pytest.fixture(scope="function")
def provides_application_context():
    return ops.testing.Context(
        charm_type=AirflowAPIServerMockCharm,
        meta={
            "name": "airflow-api-server",
            "provides": {
                constants.AIRFLOW_API_SERVER_RELATION_ENDPOINT: {
                    "interface": "airflow_api_server",
                    "limit": 1,
                },
            },
        },
    )


@pytest.fixture(scope="function")
def requires_application_context():
    return ops.testing.Context(
        charm_type=AirflowAPIServerRequirerMockCharm,
        meta={
            "name": "mock-airflow-api-server-requirer",
            "requires": {
                constants.AIRFLOW_API_SERVER_RELATION_ENDPOINT: {
                    "interface": "airflow_api_server",
                    "limit": 1,
                },
            },
        },
    )


@pytest.fixture(scope="function")
def airflow_application_provides_relation():
    return ops.testing.Relation(
        constants.AIRFLOW_API_SERVER_RELATION_ENDPOINT,
        interface="airflow_api_server",
    )


@pytest.fixture(scope="function")
def airflow_api_server_requires_relation():
    return ops.testing.Relation(
        constants.AIRFLOW_API_SERVER_RELATION_ENDPOINT,
        interface="airflow_api_server",
        remote_app_data={
            "host": "test-receiving-host",
            "port": "test-receiving-port",
        },
    )


@pytest.fixture(scope="function")
def provides_application_state(airflow_application_provides_relation):
    return ops.testing.State(
        leader=True,
        relations=[airflow_application_provides_relation],
    )


@pytest.fixture(scope="function")
def requires_application_state(airflow_api_server_requires_relation):
    return ops.testing.State(
        leader=True,
        relations=[airflow_api_server_requires_relation],
    )


class TestAirflowAPIServerProvides:
    def test_missing_relation(self, provides_application_context, provides_application_state):
        """Confirms that the _reconcile handler not run if the relation is missing."""
        provides_application_state = dataclasses.replace(provides_application_state, relations=[])

        state_out = provides_application_context.run(
            provides_application_context.on.start(), provides_application_state
        )
        assert state_out.unit_status == ops.ActiveStatus()

    def test_non_leader_noop(self, provides_application_context, provides_application_state):
        """Confirms the _reconcile handler no-ops if it is not the leader unit."""
        provides_application_state = dataclasses.replace(provides_application_state, leader=False)

        state_out = provides_application_context.run(
            provides_application_context.on.start(), provides_application_state
        )

        assert state_out.unit_status == ops.ActiveStatus()
        assert (
            state_out.get_relations(constants.AIRFLOW_API_SERVER_RELATION_ENDPOINT)[
                0
            ].local_app_data
            == {}
        )

    def test_successful_set_of_data(
        self, provides_application_context, provides_application_state
    ):
        """Ensures proper write of data in the relation databag."""
        state_out = provides_application_context.run(
            provides_application_context.on.start(), provides_application_state
        )

        assert state_out.unit_status == ops.ActiveStatus()
        assert sorted(
            state_out.get_relations(constants.AIRFLOW_API_SERVER_RELATION_ENDPOINT)[
                0
            ].local_app_data
        ) == sorted(
            {
                "host": "test-host",
                "port": "test-port",
            }
        )


class TestAirflowAPIServerRequires:
    def get_juju_log_line(self, log_level: str, event: ops.EventBase):
        """Composes specific expected logs from mock charms."""
        return ops.testing.JujuLogLine(
            level=log_level, message=f"§Requirer reacting to event: {event}"
        )

    def test_missing_relation(self, requires_application_context, requires_application_state):
        """Confirms proper null data when relation is missing."""
        requires_application_state = dataclasses.replace(requires_application_state, relations=[])

        with requires_application_context(
            requires_application_context.on.start(), requires_application_state
        ) as manager:
            state_out = manager.run()

            assert (
                self.get_juju_log_line("INFO", ops.StartEvent)
                not in requires_application_context.juju_log
            )

            assert state_out.unit_status == ops.BlockedStatus()
            assert manager.charm.requires.api_server_host is None
            assert manager.charm.requires.api_server_port is None

    def test_valid_relation(
        self,
        requires_application_context,
        requires_application_state,
        airflow_api_server_requires_relation,
    ):
        """Confirms proper data access when first available and subsequently updated."""
        with requires_application_context(
            requires_application_context.on.relation_changed(airflow_api_server_requires_relation),
            requires_application_state,
        ) as manager:
            state_out = manager.run()

            assert (
                self.get_juju_log_line("INFO", ops.RelationChangedEvent)
                in requires_application_context.juju_log
            )

            assert state_out.unit_status == ops.ActiveStatus()
            assert manager.charm.requires.api_server_host == "test-receiving-host"
            assert manager.charm.requires.api_server_port == "test-receiving-port"

        updated_relation = dataclasses.replace(
            airflow_api_server_requires_relation,
            remote_app_data={"host": "updated-receiving-host", "port": "updated-receiving-port"},
        )
        updated_state = dataclasses.replace(state_out, relations=[updated_relation])
        updated_state = dataclasses.replace(updated_state, unit_status=ops.WaitingStatus())

        with requires_application_context(
            requires_application_context.on.relation_changed(updated_relation), updated_state
        ) as manager:
            state_out = manager.run()

            assert (
                self.get_juju_log_line("INFO", ops.RelationChangedEvent)
                in requires_application_context.juju_log
            )

            assert state_out.unit_status == ops.ActiveStatus()
            assert manager.charm.requires.api_server_host == "updated-receiving-host"
            assert manager.charm.requires.api_server_port == "updated-receiving-port"

    def test_relation_break(
        self,
        requires_application_context,
        requires_application_state,
        airflow_api_server_requires_relation,
    ):
        """Confirms null data access when relation broken."""
        with requires_application_context(
            requires_application_context.on.relation_broken(airflow_api_server_requires_relation),
            requires_application_state,
        ) as manager:
            state_out = manager.run()

            assert (
                self.get_juju_log_line("INFO", ops.RelationBrokenEvent)
                in requires_application_context.juju_log
            )

            assert state_out.unit_status == ops.ActiveStatus()
            assert manager.charm.requires.api_server_host is None
            assert manager.charm.requires.api_server_port is None
