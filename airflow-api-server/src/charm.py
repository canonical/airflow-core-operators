#!/usr/bin/env python3
# Copyright 2025 Ubuntu
# See LICENSE file for licensing details.

"""Charm the application."""

import logging

import ops
from charms.airflow_coordinator_k8s.v0.airflow_coordinator import AirflowCoordinatorRequires

import constants

logger = logging.getLogger(__name__)


class ExitWithStatusError(Exception):
    """Exception raised to exit with a specific status."""

    def __init__(self, msg: str, status_type):
        """Initialize the exception with a status."""
        super().__init__(str(msg))
        self.msg = str(msg)
        self.status_type = status_type

    @property
    def status(self):
        """Get the status."""
        return self.status_type(self.msg)


class AirflowApiServerCharm(ops.CharmBase):
    """Charm the application."""

    def __init__(self, framework: ops.Framework):
        super().__init__(framework)

        self.framework.observe(self.on[constants.CONTAINER_NAME].pebble_ready, self._reconcile)

        self.container = self.unit.get_container(constants.CONTAINER_NAME)
        self.config_requires = AirflowCoordinatorRequires(
            charm=self,
            relation_name=constants.AIRFLOW_COORDINATOR_RELATION_NAME,
            component=constants.AIRFLOW_COMPONENT,
            workload_container=self.container,
            callback=self._reconcile,
        )

    def _check_pebble_connection(self) -> None:
        """Check if we can connect to the Pebble API."""
        if not self.container.can_connect():
            raise ExitWithStatusError(
                "Cannot connect to workload container",
                ops.MaintenanceStatus,
            )

    def _stop_workload(self) -> None:
        """Stop the workload service if it's running."""
        try:
            self.container.stop(constants.SERVICE_NAME)
        except Exception:
            logger.debug("Workload stop skipped/failed (service may not exist yet).")

    def _check_required_relations(self) -> None:
        """Check if all required relations are established."""
        relation = self.model.get_relation(constants.AIRFLOW_COORDINATOR_RELATION_NAME)
        if not relation:
            raise ExitWithStatusError(
                "Missing airflow-coordinator relation",
                ops.BlockedStatus,
            )
        if not self.config_requires._ready:
            self._stop_workload()
            raise ExitWithStatusError(
                "Waiting for relation data",
                ops.WaitingStatus,
            )

    def _write_airflow_config(self, config_path) -> None:
        """Write configuration files to the workload container."""
        if not self.config_requires.can_write_airflow_config:
            raise ExitWithStatusError(
                "Cannot write airflow config to workload container",
                ops.BlockedStatus,
            )
        try:
            self.config_requires.write_airflow_config(config_path=config_path)
        except Exception:
            raise ExitWithStatusError(
                "Failed to write to config file to workload container",
                ops.BlockedStatus,
            )

    @property
    def _api_server_layer(self) -> ops.pebble.LayerDict:
        """Define the Pebble layer for the workload container."""
        layer: ops.pebble.LayerDict = {
            "services": {
                constants.SERVICE_NAME: {
                    "override": "replace",
                    "summary": "A service that runs the api-server workload container",
                    "command": "airflow api-server",
                    "startup": "enabled",
                }
            }
        }
        return layer

    def _layer_changed(self, label: str, new_layer: ops.pebble.LayerDict) -> bool:
        """Return True if the currently running Pebble plan differs from new_layer."""
        plan = self.container.get_plan()

        current = plan.to_dict() if hasattr(plan, "to_dict") else plan.to_yaml()

        current_services = current.get("services", {}) if isinstance(current, dict) else {}
        desired_services = new_layer.get("services", {})

        return current_services != desired_services

    def _add_layer_and_replan(self) -> None:
        """Add the Pebble layer and replan the services."""
        if not self._layer_changed("api-server-base", self._api_server_layer):
            return

        self.container.add_layer("api-server-base", self._api_server_layer, combine=True)
        try:
            self.container.replan()
        except ops.pebble.ChangeError:
            raise ExitWithStatusError(
                "Failed to replan Pebble services",
                ops.BlockedStatus,
            )

    def _reconcile(self, _) -> None:
        """Reconcile the charm state."""
        try:
            self._check_pebble_connection()
            self._check_required_relations()
            self._write_airflow_config(config_path=f"{constants.AIRFLOW_CONFIG_PATH}")
            self._add_layer_and_replan()
        except ExitWithStatusError as e:
            self.unit.status = e.status
            return
        self.unit.status = ops.ActiveStatus()


if __name__ == "__main__":
    ops.main(AirflowApiServerCharm)
