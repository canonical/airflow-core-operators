#!/usr/bin/env python3
# Copyright 2026 Ubuntu
# See LICENSE file for licensing details.

"""Charm the Airflow API Server."""

import logging

import ops
from charms.airflow_api_server_k8s.v0.airflow_api_server import AirflowAPIServerProvides
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


class AirflowApiServerCharm(ops.CharmBase):
    """Charm the Airflow API Server."""

    def __init__(self, framework: ops.Framework):
        super().__init__(framework)

        self.framework.observe(self.on[constants.CONTAINER_NAME].pebble_ready, self._reconcile)

        self._container = self.unit.get_container(constants.CONTAINER_NAME)
        self._config_requires = AirflowCoordinatorCoreRequires(
            charm=self,
            relation_name=constants.AIRFLOW_COORDINATOR_RELATION_ENDPOINT,
            component=constants.AIRFLOW_COMPONENT,
            workload_container=self._container,
            callback=self._reconcile,
        )

        self._api_server_provides = AirflowAPIServerProvides(
            self,
            constants.AIRFLOW_API_SERVER_RELATION_ENDPOINT,
            self._airflow_api_server_host,
            str(self._airflow_api_server_port),
        )

    @property
    def _airflow_api_server_host(self) -> str:
        """Airflow API Server hostname."""
        # Hard-coded, but subject to change with addition of features like
        # ingress or configurable options of the type of K8s service for the application
        return f"{self.app.name}-endpoints.{self.model.name}.svc.cluster.local"

    @property
    def _airflow_api_server_port(self) -> int:
        """Airflow API Server port."""
        return 8080

    def _stop_service_and_remove_config(self) -> None:
        try:
            logger.info(f"Stopping service {constants.SERVICE_NAME}")
            self._container.stop(constants.SERVICE_NAME)
        except ops.pebble.APIError:
            raise ExitWithStatusError(
                "Failed to stop pebble service",
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
        if not self.model.get_relation(constants.AIRFLOW_COORDINATOR_RELATION_ENDPOINT):
            self._stop_service_and_remove_config()
            raise ExitWithStatusError(
                "Missing airflow-coordinator relation",
                ops.BlockedStatus,
            )

    def _write_airflow_config(self, config_path: str) -> None:
        """Write the airflow config to the workload container."""
        if not self._config_requires.can_write_airflow_config:
            raise ExitWithStatusError(
                "Waiting for relation data from coordinator",
                ops.WaitingStatus,
            )
        try:
            self._config_requires.write_airflow_config(
                config_path=config_path,
                user=constants.WORKLOAD_USER,
                group=constants.WORKLOAD_GROUP,
            )
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

    @property
    def _api_server_layer(self) -> ops.pebble.LayerDict:
        """Define the Pebble layer for the workload."""
        layer: ops.pebble.LayerDict = {
            "services": {
                constants.SERVICE_NAME: {
                    "override": "replace",
                    "summary": "A service that runs the api-server workload.",
                    "command": "airflow api-server",
                    "startup": "enabled",
                    "user": constants.WORKLOAD_USER,
                    "group": constants.WORKLOAD_GROUP,
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
        self._container.add_layer("api-server-base", self._api_server_layer, combine=True)
        try:
            self._container.replan()
            if restart_service:
                self._container.restart(constants.SERVICE_NAME)

        except ops.pebble.ChangeError as e:
            logger.exception("Pebble replan failed for api-server service: %s", e)
            raise ExitWithStatusError(
                "Failed to replan Pebble services",
                ops.BlockedStatus,
            )
        except ops.pebble.APIError as e:
            logger.exception("Pebble restart failed for api-server service: %s", e)
            raise ExitWithStatusError(
                "Failed to replan Pebble services",
                ops.BlockedStatus,
            )

    def _reconcile(self, _) -> None:
        """Reconcile the charm state."""
        try:
            self._check_pebble_connection()
            self._check_required_relations()

            if not self._config_requires.can_write_airflow_config:
                raise ExitWithStatusError(
                    "Waiting for relation data from coordinator",
                    ops.WaitingStatus,
                )

            airflow_config_updated = self._config_requires.airflow_config_needs_update(
                config_path=constants.AIRFLOW_CONFIG_PATH
            )
            if airflow_config_updated:
                self._write_airflow_config(config_path=constants.AIRFLOW_CONFIG_PATH)

            if self._config_requires.can_write_tls_ca_chain:
                self._config_requires.write_tls_ca_chains(constants.WORKLOAD_USER, constants.WORKLOAD_GROUP)

            self._add_layer_and_replan(restart_service=airflow_config_updated)
        except ExitWithStatusError as e:
            self.unit.status = e.status
            return

        self.unit.status = ops.ActiveStatus()


if __name__ == "__main__":
    ops.main(AirflowApiServerCharm)
