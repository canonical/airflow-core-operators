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

        framework.observe(self.on[constants.CONTAINER_NAME].pebble_ready, self._reconcile)

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

    def _stop_service(self) -> None:
        """Stop the scheduler service (idempotent)."""
        try:
            logger.info(f"Stopping {constants.SERVICE_NAME} service")
            self._container.stop(constants.SERVICE_NAME)
        except ops.pebble.APIError as e:
            raise ExitWithStatusError(
                "Failed to stop service: Pebble API error",
                ops.BlockedStatus,
            ) from e

    def _cleanup_airflow_home(self) -> None:
        """Cleanup the Airflow home directory."""
        # Remove config file
        config_path = constants.AIRFLOW_CONFIG_PATH
        if self._container.exists(config_path):
            logger.info("Removing airflow home...")
            self._container.remove_path(config_path, recursive=False)

    def _check_container_can_connect(self) -> None:
        """Verify connection to the container; otherwise raise."""
        if not self._container.can_connect():
            raise ExitWithStatusError(
                "Cannot connect to workload container", ops.MaintenanceStatus
            )

    def _check_required_relation_and_act(self) -> None:
        """Verify the coordinator relation exists, otherwise raise.

        If the relation does not exist, the charm will attempt to
        stop the service (if started) and remove the Airflow configuration
        file from the Airflow home directory (if present).

        Raises:
            ExitWithStatusError: If the relation with the airflow coordinator
                charm is not present.
        """
        relation = self.model.get_relation(constants.AIRFLOW_COORDINATOR_RELATION_NAME)
        if not relation:
            # Always attempt to stop the service and remove the airflow home
            # if the relation is not present
            self._stop_service()
            self._cleanup_airflow_home()
            raise ExitWithStatusError(
                "Missing airflow-coordinator relation", ops.BlockedStatus
            )

    def _write_airflow_config(self, config_path) -> None:
        """Write the airflow configuration file inside the workload container given a path.

        This method checks if the configuration can be written; otherwise raises.

        Raises:
            ExitWithStatusError: if the configuration cannot be written or if
                the operation failed due to issues with the write operation.
        """
        # Check if we can write the config
        # If not, the coordinator hasn't provided config yet (temporary condition)
        if not self.config_requires.can_write_airflow_config:
            raise ExitWithStatusError("Waiting for relation data", ops.WaitingStatus)

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
        """Add the Pebble layer and replan.

        The service will start automatically since startup is enabled.

        Raises:
            ExitWithStatusError: If the service cannot be replanned.
        """
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
        try:
            self._check_container_can_connect()
            self._check_required_relation_and_act()
            self._write_airflow_config(
                config_path=constants.AIRFLOW_CONFIG_PATH,
            )
            self._add_layer_and_replan()
        except ExitWithStatusError as e:
            self.unit.status = e.status
            return

        self.unit.status = ops.ActiveStatus()


if __name__ == "__main__":  # pragma: nocover
    ops.main(AirflowSchedulerCharm)
