#!/usr/bin/env python3
# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""Charm of the Airflow scheduler workload."""

import logging

import ops
from charms.airflow_coordinator_k8s.v0.airflow_coordinator import (
    AirflowCoordinatorRequires,
)
from ops.pebble import LayerDict

import constants

logger = logging.getLogger(__name__)


# TODO: abstract this to a diff module so all charms in this repo can use it
class ExitWithStatusError(Exception):
    """Base class of exceptions for when a method has an opinion on the unit status."""

    def __init__(self, msg: str, status_type):
        super().__init__(str(msg))
        self.msg = str(msg)
        self.status_type = status_type

    @property
    def status(self):
        """Returns an instance of self.status_type with a message."""
        return self.status_type(self.msg)


class AirflowSchedulerCharm(ops.CharmBase):
    """Charm of the Airflow scheduler workload."""

    def __init__(self, framework: ops.Framework):
        super().__init__(framework)

        framework.observe(
            self.on[constants.CONTAINER_NAME].pebble_ready, self._reconcile
        )

        self._container = self.unit.get_container(constants.CONTAINER_NAME)

        # Create config requires object for handling Airflow configurations
        self.config_requires = AirflowCoordinatorRequires(
            charm=self,
            relation_name=constants.AIRFLOW_COORDINATOR_RELATION_NAME,
            component=constants.AIRFLOW_COMPONENT,
            workload_container=self._container,
            callback=self._reconcile,
        )

    @property
    def _airflow_scheduler_layer(self) -> LayerDict:
        """Return the service Pebble layer."""
        layer: LayerDict = {
            "services": {
                constants.SERVICE_NAME: {
                    "override": "replace",
                    "summary": "The airflow scheduler service.",
                    "command": "airflow scheduler",
                    "startup": "enabled",
                }
            }
        }
        return layer

    def _check_container_can_connect(self) -> None:
        """Verify connection to the container; otherwise raise."""
        if not self._container.can_connect():
            raise ExitWithStatusError(
                "Cannot connect to workload container", ops.MaintenanceStatus
            )

    def _check_required_relation(self) -> None:
        """Verify the coordinator relation exists, otherwise raise."""
        relation = self.model.get_relation(constants.AIRFLOW_COORDINATOR_RELATION_NAME)
        if not relation:
            raise ExitWithStatusError(
                "Missing airflow-coordinator relation", ops.BlockedStatus
            )

    def _check_relation_ready_and_can_write_config(self) -> None:
        """Verify the relation is ready and Airflow config can be written, otherwise raise.

        Raises:
            ExitWithStatusError: If the coordinator is yet to provide config data.
                This could be due to validation issues for this component, missing other components,
                or the coordinator still preparing the configuration.
        """
        # Check if THIS component has validation failures
        if self.config_requires.validation_failure_messages:
            raise ExitWithStatusError(
                "Waiting for relation data", ops.WaitingStatus
            )

        # Check if we can write the config
        # If not, the coordinator hasn't provided config yet (temporary condition)
        if not self.config_requires.can_write_airflow_config:
            raise ExitWithStatusError(
                "Waiting for relation data", ops.WaitingStatus
            )

    def _write_airflow_config(self, config_path) -> None:
        """Write the airflow configuration file inside the workload container given a path."""
        try:
            self.config_requires.write_airflow_config(config_path=config_path)
        except (ops.pebble.ConnectionError, ops.pebble.Error) as e:
            # TODO: is BlockedStatus the best status here? I don't think there's
            # too much a human operator can actually do to resolve the issue.
            raise ExitWithStatusError(
                "Failed to write config: Pebble connection error", ops.BlockedStatus
            ) from e
        except Exception as e:
            raise ExitWithStatusError(
                "Failed to write config to workload container", ops.BlockedStatus
            ) from e

    def _add_layer_and_replan(self) -> None:
        """Add the Pebble layer and replan the services only when needed."""
        current_services = self._container.get_plan().to_dict().get("services", {})
        desired_services = self._airflow_scheduler_layer.get("services", {})

        if current_services == desired_services:
            return

        self._container.add_layer(
            "scheduler-base", self._airflow_scheduler_layer, combine=True
        )

        try:
            self._container.replan()
        except ops.pebble.ChangeError as e:
            raise ExitWithStatusError(
                "Failed to replan Pebble services",
                ops.BlockedStatus,
            ) from e

    def _reconcile(self, _) -> None:
        """Reconcile the state of the charm for any event by running all operations."""
        # Try/except the actual operations of the reconciler
        try:
            self._check_container_can_connect()
            self._check_required_relation()
            self._check_relation_ready_and_can_write_config()
            self._write_airflow_config(
                config_path=f"{constants.AIRFLOW_HOME}/airflow.cfg"
            )
            self._add_layer_and_replan()
        except ExitWithStatusError as e:
            self.unit.status = e.status
            return

        self.unit.status = ops.ActiveStatus()


if __name__ == "__main__":  # pragma: nocover
    ops.main(AirflowSchedulerCharm)
