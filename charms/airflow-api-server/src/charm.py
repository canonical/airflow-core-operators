#!/usr/bin/env python3
# Copyright 2023 Ubuntu
# See LICENSE file for licensing details.

"""Charm the Airflow API Server."""

import json
import logging

import jinja2
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
    """Charm the Airflow API Server."""

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
        """Check if the Pebble API is reachable in the workload container."""
        if not self.container.can_connect():
            raise ExitWithStatusError(
                "Cannot connect to workload container",
                ops.MaintenanceStatus,
            )

    def _stop_workload(self) -> None:
        """Stop the workload service if it's running."""
        services = self.container.get_services()

        service = services.get(constants.SERVICE_NAME)
        if service and service.is_running():
            self.container.stop(constants.SERVICE_NAME)

    def _check_required_relations(self) -> None:
        """Check if all required relations are established."""
        relation = self.model.get_relation(constants.AIRFLOW_COORDINATOR_RELATION_NAME)
        if not relation:
            self._stop_workload()
            self._add_layer_and_replan(startup="disabled")
            raise ExitWithStatusError(
                "Missing airflow-coordinator relation",
                ops.BlockedStatus,
            )

    def _write_airflow_config(self, config_path) -> None:
        """Write configuration files to the workload."""
        if not self.config_requires.can_write_airflow_config:
            self._stop_workload()
            self._add_layer_and_replan(startup="disabled")
            raise ExitWithStatusError(
                "Cannot write airflow config to workload container: Waiting for relation data",
                ops.WaitingStatus,
            )
        try:
            self.config_requires.write_airflow_config(config_path=config_path)
        except (
            ops.pebble.ConnectionError,
            ops.pebble.APIError,
            json.JSONDecodeError,
            jinja2.TemplateError,
        ):
            raise ExitWithStatusError(
                "Failed to write to config file to workload container",
                ops.BlockedStatus,
            )

    def _api_server_layer(self, startup: str = "enabled") -> ops.pebble.LayerDict:
        """Define the Pebble layer for the workload container."""
        layer: ops.pebble.LayerDict = {
            "services": {
                constants.SERVICE_NAME: {
                    "override": "replace",
                    "summary": "A service that runs the api-server workload container",
                    "command": "airflow api-server",
                    "startup": startup,
                }
            }
        }
        return layer

    def _add_layer_and_replan(self, startup: str = "enabled") -> None:
        """Add the Pebble layer and replan the services."""
        current_services = self.container.get_plan().to_dict().get("services", {})
        layer = self._api_server_layer(startup=startup)
        desired_services = layer.get("services", {})

        if current_services == desired_services:
            return

        self.container.add_layer("api-server-base", layer, combine=True)
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
            self._write_airflow_config(config_path=constants.AIRFLOW_CONFIG_PATH)
            self._add_layer_and_replan(startup="enabled")
        except ExitWithStatusError as e:
            self.unit.status = e.status
            return
        self.unit.status = ops.ActiveStatus()


if __name__ == "__main__":
    ops.main(AirflowApiServerCharm)
