#!/usr/bin/env python3
# Copyright 2026 Ubuntu
# See LICENSE file for licensing details.

"""Charm the Airflow API Server."""

import logging
from urllib.parse import urlparse

import ops
from charms.airflow_api_server_k8s.v0.airflow_api_server import AirflowAPIServerProvides
from charms.airflow_coordinator_k8s.v0.airflow_coordinator import AirflowCoordinatorCoreRequires
from charms.traefik_k8s.v2.ingress import (
    IngressPerAppRequirer,
)

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

        self._ingress = IngressPerAppRequirer(
            self,
            host=self._airflow_api_server_host,
            port=self._airflow_api_server_port,
        )
        self._api_server_provides = AirflowAPIServerProvides(
            self,
            constants.AIRFLOW_API_SERVER_RELATION_ENDPOINT,
            self._airflow_api_server_host,
            str(self._airflow_api_server_port),
        )

        self.framework.observe(self._ingress.on.ready, self._reconcile)
        self.framework.observe(self._ingress.on.revoked, self._reconcile)

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

    def _handle_ingress(self) -> None:
        """Extract the ingress path and share it to configure Airflow's base_url.

        The extracted path is passed to the airflow-coordinator, which uses it
        to construct the global `base_url` configuration for the Airflow cluster.

        - Clears the path if the relation is broken or unready.
        - Extracts the path prefix (for routing_mode=path) from the
           URL and sets it in the relation databag.
        - Clears the path if the extracted path is empty (for routing_mode=subdomain).
        """
        if (
            not self.model.get_relation(constants.TRAEFIK_INGRESS_RELATION_ENDPOINT)
            or not self._ingress.url
        ):
            self._api_server_provides.clear_ingress_path()
            return
        ingress_path = urlparse(self._ingress.url).path.strip("/") or None
        if ingress_path:
            self._api_server_provides.set_ingress_path(ingress_path)
        else:
            self._api_server_provides.clear_ingress_path()

    def _write_airflow_config(self, config_path: str) -> None:
        """Write configuration files to the workload container."""
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
        service: dict = {
            "override": "replace",
            "summary": "A service that runs the api-server workload.",
            "command": "airflow api-server",
            "startup": "enabled",
            "user": constants.WORKLOAD_USER,
            "group": constants.WORKLOAD_GROUP,
        }

        if self._ingress.url:
            service["command"] += " --proxy-headers"
            # Using the '*' wildcard prevents a brute-force cycle of restarting the
            # Airflow API server every time the proxy IP changes.
            # This is safe because traffic reaching this container is already
            # gated by the cluster's internal network and the Ingress controller.
            service["environment"] = {"FORWARDED_ALLOW_IPS": "*"}

        layer: ops.pebble.LayerDict = {"services": {constants.SERVICE_NAME: service}}
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

    def _handle_webserver_config(self) -> bool:
        """Write or remove webserver_config.py based on coordinator relation data.

        Delegates to AirflowCoordinatorCoreRequires when the coordinator has shared
        a webserver config template; falls back to removing any stale file when OAuth
        is no longer active.

        Returns:
            True if the file was created, updated, or removed (service restart
            warranted), False otherwise.

        Raises:
            ExitWithStatusError: If a Pebble operation fails.
        """
        if self._config_requires.can_write_webserver_config:
            try:
                needs_update = self._config_requires.webserver_config_needs_update(
                    constants.WEBSERVER_CONFIG_PATH
                )
            except (ops.pebble.ConnectionError, ops.pebble.PathError) as e:
                logger.exception("Failed to check webserver_config.py: %s", e)
                raise ExitWithStatusError(
                    constants.FAILED_TO_CHECK_WEBSERVER_CONFIG_UPDATE_MESSAGE,
                    ops.BlockedStatus,
                )

            if not needs_update:
                return False

            try:
                self._config_requires.write_webserver_config(
                    filepath=constants.WEBSERVER_CONFIG_PATH,
                    user=constants.WORKLOAD_USER,
                    group=constants.WORKLOAD_GROUP,
                )
            except (ops.pebble.ConnectionError, ops.pebble.Error) as e:
                logger.exception("Failed to write webserver_config.py: %s", e)
                raise ExitWithStatusError(
                    constants.FAILED_TO_WRITE_WEBSERVER_CONFIG_MESSAGE,
                    ops.BlockedStatus,
                )

            return True

        # OAuth not active — remove any stale file from a previous OAuth relation.
        try:
            file_exists = self._container.exists(constants.WEBSERVER_CONFIG_PATH)
        except ops.pebble.ConnectionError as e:
            logger.exception("Pebble connection error checking webserver_config.py: %s", e)
            raise ExitWithStatusError(
                constants.FAILED_TO_CHECK_WEBSERVER_CONFIG_EXISTS_MESSAGE,
                ops.BlockedStatus,
            )

        if not file_exists:
            return False

        try:
            self._container.remove_path(constants.WEBSERVER_CONFIG_PATH)
        except (ops.pebble.ConnectionError, ops.pebble.PathError) as e:
            logger.exception("Failed to remove webserver_config.py: %s", e)
            raise ExitWithStatusError(
                constants.FAILED_TO_REMOVE_WEBSERVER_CONFIG_MESSAGE,
                ops.BlockedStatus,
            )

        return True

    def _reconcile(self, _) -> None:
        """Reconcile the charm state."""
        try:
            self._check_pebble_connection()
            self._handle_ingress()
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

            webserver_config_updated = self._handle_webserver_config()

            if self._config_requires.can_write_tls_ca_chain:
                self._config_requires.write_tls_ca_chains(
                    constants.WORKLOAD_USER, constants.WORKLOAD_GROUP
                )

            self._add_layer_and_replan(
                restart_service=airflow_config_updated or webserver_config_updated
            )
        except ExitWithStatusError as e:
            self.unit.status = e.status
            return

        self.unit.status = ops.ActiveStatus()


if __name__ == "__main__":
    ops.main(AirflowApiServerCharm)
