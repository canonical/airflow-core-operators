#!/usr/bin/env python3
# Copyright 2026 Ubuntu
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
        super().__init__(str(msg))
        self.msg = str(msg)
        self.status_type = status_type

    @property
    def status(self):
        return self.status_type(self.msg)


class AirflowApiServerCharm(ops.CharmBase):
    """Charm the Airflow API Server."""

    _stored = ops.StoredState()

    def __init__(self, framework: ops.Framework):
        super().__init__(framework)

        self._stored.set_default(has_ever_been_ready=False)

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

    def _check_required_relations(self) -> None:
        """Check if required relations are established."""
        if not self.model.get_relation(constants.AIRFLOW_COORDINATOR_RELATION_NAME):
            self._stored.has_ever_been_ready = False
            self._add_layer_and_replan(startup="disabled")
            raise ExitWithStatusError(
                "Missing airflow-coordinator relation",
                ops.BlockedStatus,
            )
        if self.config_requires.missing_core_components_exist and not self._stored.has_ever_been_ready:
            raise ExitWithStatusError(
                "Waiting for relation data from coordinator",
                ops.WaitingStatus,
            )

    def _write_airflow_config(self, config_path: str) -> None:
        """Write configuration files to the workload."""

        if not self.config_requires.can_write_airflow_config:
            if not self._stored.has_ever_been_ready:
                self._add_layer_and_replan(startup="disabled")
                raise ExitWithStatusError(
                    "Cannot write config file to workload container.",
                    ops.WaitingStatus,
                )
            return
        try:
            self.config_requires.write_airflow_config(config_path=config_path)
        except (
            ops.pebble.ConnectionError,
            ops.pebble.APIError,
            json.JSONDecodeError,
            jinja2.TemplateError,
        ):
            self._add_layer_and_replan(startup="disabled")
            raise ExitWithStatusError(
                "Failed to write config file to workload container",
                ops.BlockedStatus,
            )

    def _api_server_layer(self, startup: str = "enabled") -> ops.pebble.LayerDict:
        """Define the Pebble layer for the workload."""
        layer: ops.pebble.LayerDict = {
            "services": {
                constants.SERVICE_NAME: {
                    "override": "replace",
                    "summary": "A service that runs the api-server workload.",
                    "command": "airflow api-server",
                    "startup": startup,
                }
            }
        }
        return layer

    def _add_layer_and_replan(self, startup: str = "enabled") -> None:
        """Add the Pebble layer and replan the services."""
        
        service = self.container.get_services().get(constants.SERVICE_NAME)
        if service and service.startup == startup:
            return

        layer = self._api_server_layer(startup=startup)
        self.container.add_layer("api-server-base", layer, combine=True)

        try:
            self.container.replan()
        except ops.pebble.ChangeError as e:
            logger.exception("Failed to replan Pebble services: %s", e)
            raise ExitWithStatusError(
                "Failed to replan Pebble services",
                ops.BlockedStatus,
            )


    def _reconcile(self, event) -> None:
        """Reconcile the charm state."""
        try:
            self._check_pebble_connection()
            self._check_required_relations()
            self._write_airflow_config(config_path=constants.AIRFLOW_CONFIG_PATH)
            self._add_layer_and_replan()
            self._stored.has_ever_been_ready = True # Happy path

        except ExitWithStatusError as e:
            self.unit.status = e.status
            return

        self.unit.status = ops.ActiveStatus()


if __name__ == "__main__":
    ops.main(AirflowApiServerCharm)
