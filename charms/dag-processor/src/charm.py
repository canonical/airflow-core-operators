#!/usr/bin/env python3
# Copyright 2026 Ubuntu
# See LICENSE file for licensing details.

"""Charm the Airflow Dag Processor."""

import hashlib
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


class AirflowDagProcessorCharm(ops.CharmBase):
    """Charm the Airflow Dag Processor."""

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
        if not self.model.get_relation(constants.AIRFLOW_COORDINATOR_RELATION_NAME):
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

    def _log_targets_config(self) -> dict[str, ops.pebble.LogTargetDict]:
        """Build the Pebble log-targets configuration for the current OTLP endpoint."""
        otlp_endpoint = self._config_requires.otlp_endpoint
        base_target_name = f"{constants.SERVICE_NAME}-logs"
        existing_targets = self._container.get_plan().log_targets
        log_targets: dict[str, ops.pebble.LogTargetDict] = {}

        active_target_name: str | None = None
        if otlp_endpoint:
            base_existing = existing_targets.get(base_target_name)
            if base_existing is None or base_existing.location == otlp_endpoint:
                active_target_name = base_target_name
            else:
                suffix = hashlib.sha256(otlp_endpoint.encode()).hexdigest()[:8]
                active_target_name = f"{base_target_name}-{suffix}"

        for name, existing in existing_targets.items():
            if name != active_target_name:
                log_targets[name] = {
                    "override": "replace",
                    "type": existing.type,
                    "location": existing.location,
                    "services": [],
                }

        if otlp_endpoint and active_target_name:
            log_targets[active_target_name] = {
                "override": "replace",
                "type": "opentelemetry",
                "location": otlp_endpoint,
                "services": [constants.SERVICE_NAME],
                "labels": {
                    "juju_model": self.model.name,
                    "juju_model_uuid": self.model.uuid,
                    "juju_application": self.app.name,
                    "juju_charm": self.meta.name,
                    "juju_unit": self.unit.name,
                },
            }

        return log_targets

    @property
    def _dag_processor_layer(self) -> ops.pebble.LayerDict:
        """Define the Pebble layer for the workload."""
        layer: ops.pebble.LayerDict = {
            "services": {
                constants.ROCK_SERVICE_NAME: {
                    "override": "replace",
                    "command": "true",
                    "startup": "disabled",
                },
                constants.SERVICE_NAME: {
                    "override": "replace",
                    "summary": "A service that runs the dag-processor workload.",
                    "command": "airflow dag-processor",
                    "startup": "enabled",
                    "user": constants.WORKLOAD_USER,
                    "group": constants.WORKLOAD_GROUP,
                },
            },
        }
        if log_targets := self._log_targets_config():
            layer["log-targets"] = log_targets
        return layer

    def _add_layer_and_replan(self, restart_service: bool = False) -> None:
        """Add the Pebble layer and replan the services.

        The service starts automatically after replanning as startup is enabled.

        Raises:
            ExitWithStatusError: If replanning fails.
        """
        self._container.add_layer("dag-processor-base", self._dag_processor_layer, combine=True)
        try:
            self._container.replan()
            if restart_service:
                self._container.restart(constants.SERVICE_NAME)

        except ops.pebble.ChangeError as e:
            logger.exception("Pebble replan failed for dag-processor service: %s", e)
            raise ExitWithStatusError(
                "Failed to replan Pebble services",
                ops.BlockedStatus,
            )
        except ops.pebble.APIError as e:
            logger.exception("Pebble restart failed for dag-processor service: %s", e)
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
                self._config_requires.write_tls_ca_chains(
                    constants.WORKLOAD_USER, constants.WORKLOAD_GROUP
                )

            self._add_layer_and_replan(restart_service=airflow_config_updated)
        except ExitWithStatusError as e:
            self.unit.status = e.status
            return

        self.unit.status = ops.ActiveStatus()


if __name__ == "__main__":
    ops.main(AirflowDagProcessorCharm)
