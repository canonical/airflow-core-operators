#!/usr/bin/env python3
# Copyright 2026 Ubuntu
# See LICENSE file for licensing details.

"""Charm the Airflow Triggerer."""

import logging

import ops
from charms.airflow_coordinator_k8s.v0.airflow_coordinator import AirflowCoordinatorCoreRequires

import constants

logger = logging.getLogger(__name__)


class ExitWithStatusError(Exception):
    """Exception raised to exit with a specific status."""

    def __init__(self, msg: str, status_type):
        super().__init__(str(msg))
        self.msg = str(msg)
        self.status_type = status_type

    @property
    def status(self):
        """Return the Juju unit status represented by this exception."""
        return self.status_type(self.msg)


class AirflowTriggererCharm(ops.CharmBase):
    """Charm the Airflow Triggerer."""

    def __init__(self, framework: ops.Framework):
        super().__init__(framework)

        self.framework.observe(self.on[constants.CONTAINER_NAME].pebble_ready, self._reconcile)

        self._container = self.unit.get_container(constants.CONTAINER_NAME)
        self._config_requires = AirflowCoordinatorCoreRequires(
            charm=self,
            relation_name=constants.AIRFLOW_COORDINATOR_RELATION_NAME,
            component=constants.AIRFLOW_COMPONENT,
            workload_container=self._container,
            callback=self._reconcile,
        )

    def _stop_service_and_remove_config(self) -> None:
        try:
            logger.info(f"Stopping service {constants.SERVICE_NAME}")
            self._container.stop(constants.SERVICE_NAME)
        except ops.pebble.APIError:
            raise ExitWithStatusError(
                "Failed to stop service",
                ops.BlockedStatus,
            )
        config_path = constants.AIRFLOW_CONFIG_PATH
        if self._container.exists(config_path):
            self._container.remove_path(config_path, recursive=False)

    def _check_pebble_connection(self) -> None:
        """Verify connection to the container; otherwise raise."""
        if not self._container.can_connect():
            raise ExitWithStatusError(
                "Cannot connect to workload container", ops.MaintenanceStatus
            )

    def _check_required_relations(self) -> None:
        """Check if required relations are established."""
        if not self.model.get_relation(constants.AIRFLOW_COORDINATOR_RELATION_NAME):
            self._stop_service_and_remove_config()
            raise ExitWithStatusError(
                "Missing airflow-coordinator relation",
                ops.BlockedStatus,
            )

    def _write_airflow_config(self, config_path: str) -> bool:
        """Write configuration files and return whether service restart is required."""
        if not self._config_requires.can_write_airflow_config:
            raise ExitWithStatusError(
                "Waiting for relation data from coordinator",
                ops.WaitingStatus,
            )
        try:
            should_restart = self._config_requires.airflow_config_needs_update(config_path=config_path)
            if should_restart:
                self._config_requires.write_airflow_config(config_path=config_path)
        except (
            ops.pebble.ConnectionError,
            ops.pebble.Error,
        ):
            raise ExitWithStatusError(
                "Failed to write config file: Pebble Error",
                ops.BlockedStatus,
            )
        except Exception:
            raise ExitWithStatusError(
                "Failed to write config file to workload container",
                ops.BlockedStatus,
            )
        return should_restart

    @property
    def _triggerer_layer(self) -> ops.pebble.LayerDict:
        """Define the Pebble layer for the workload."""
        layer: ops.pebble.LayerDict = {
            "services": {
                constants.SERVICE_NAME: {
                    "override": "replace",
                    "summary": "A service that runs the triggerer workload.",
                    "command": "airflow triggerer",
                    "startup": "enabled",
                }
            }
        }
        return layer

    def _add_layer_and_replan(self, restart_service: bool = False) -> None:
        """Add the Pebble layer and replan the services.

        The service starts automatically after replanning as startup is enabled.

        Raises:
            ExitWithStatusError: If replanning fails.
        """
        self._container.add_layer("triggerer-base", self._triggerer_layer, combine=True)
        try:
            self._container.replan()
            if restart_service:
                self._container.restart(constants.SERVICE_NAME)

        except ops.pebble.ChangeError as e:
            logger.exception("Pebble replan failed for triggerer service: %s", e)
            raise ExitWithStatusError(
                "Failed to replan Pebble services",
                ops.BlockedStatus,
            )
        except ops.pebble.APIError as e:
            logger.exception("Pebble restart failed for triggerer service: %s", e)
            raise ExitWithStatusError(
                "Failed to replan Pebble services",
                ops.BlockedStatus,
            )

    def _reconcile(self, _) -> None:
        """Reconcile the charm state."""
        try:
            self._check_pebble_connection()
            self._check_required_relations()
            restart_service = self._write_airflow_config(config_path=constants.AIRFLOW_CONFIG_PATH)
            self._add_layer_and_replan(restart_service=restart_service)
        except ExitWithStatusError as e:
            self.unit.status = e.status
            return

        self.unit.status = ops.ActiveStatus()


if __name__ == "__main__":
    ops.main(AirflowTriggererCharm)
