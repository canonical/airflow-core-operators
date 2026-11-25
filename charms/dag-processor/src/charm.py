#!/usr/bin/env python3
# Copyright 2026 Ubuntu
# See LICENSE file for licensing details.

"""Charm the Airflow Dag Processor."""

import hashlib
import logging

import ops
from charms.airflow_coordinator_k8s.v0.airflow_coordinator import AirflowCoordinatorCoreRequires

import constants
from security_logging import PebbleServiceSecurityLogger

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
        self._security_events = PebbleServiceSecurityLogger(
            container=self._container,
            appid=constants.APP_ID,
            service_name=constants.SERVICE_NAME,
        )
        self._config_requires = AirflowCoordinatorCoreRequires(
            charm=self,
            relation_name=constants.AIRFLOW_COORDINATOR_RELATION_NAME,
            component=constants.AIRFLOW_COMPONENT,
            workload_container=self._container,
            callback=self._reconcile,
        )

    def _stop_service_and_remove_config(self) -> None:
        try:
            was_running = self._security_events.is_running()
            if constants.SERVICE_NAME in self._container.get_plan().services:
                logger.info(f"Stopping service {constants.SERVICE_NAME}")
                self._container.stop(constants.SERVICE_NAME)
        except ops.pebble.APIError:
            raise ExitWithStatusError(
                "Failed to stop pebble service",
                ops.BlockedStatus,
            )
        logger.info("Stopped service %s", constants.SERVICE_NAME)
        self._security_events.log_shutdown_if_running(was_running, "required_relation_missing")
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
            if name != active_target_name and (
                name == base_target_name or name.startswith(f"{base_target_name}-")
            ):
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
                    "loki.resource.labels": (
                        "juju_application, juju_charm, juju_model, juju_model_uuid, juju_unit"
                    ),
                },
            }

        return log_targets

    def _otel_resource_attributes(self) -> str:
        """Build the OTEL_RESOURCE_ATTRIBUTES value with Juju topology and instance ID."""
        instance_id = f"{self.model.name}_{self.model.uuid}_{self.app.name}_{self.unit.name}"
        return (
            f"juju_application={self.app.name},"
            f"juju_charm={self.meta.name},"
            f"juju_model={self.model.name},"
            f"juju_model_uuid={self.model.uuid},"
            f"juju_unit={self.unit.name},"
            f"service.instance.id={instance_id}"
        )

    @property
    def _dag_processor_layer(self) -> ops.pebble.LayerDict:
        """Define the Pebble layer for the workload."""
        service: ops.pebble.ServiceDict = {
            "override": "replace",
            "summary": "A service that runs the dag-processor workload.",
            "command": "airflow dag-processor",
            "startup": "enabled",
            "user": constants.WORKLOAD_USER,
            "group": constants.WORKLOAD_GROUP,
        }
        if self._config_requires.otlp_endpoint:
            service["environment"] = {"OTEL_RESOURCE_ATTRIBUTES": self._otel_resource_attributes()}
        layer: ops.pebble.LayerDict = {
            "services": {
                constants.ROCK_SERVICE_NAME: {
                    "override": "replace",
                    "command": "true",
                    "startup": "disabled",
                },
                constants.SERVICE_NAME: service,
            },
        }
        if log_targets := self._log_targets_config():
            layer["log-targets"] = log_targets
        return layer

    def _add_layer_and_replan(self, restart_service: bool = False) -> None:
        """Add the Pebble layer and replan the services.

        The service starts automatically after replanning as startup is enabled.
        It is only restarted if it was already running, since a service that
        replan just started has already picked up the new configuration.

        Raises:
            ExitWithStatusError: If replanning fails.
        """
        was_running = self._security_events.is_running()
        self._container.add_layer("dag-processor-base", self._dag_processor_layer, combine=True)
        try:
            self._container.replan()
            if restart_service and was_running:
                self._container.restart(constants.SERVICE_NAME)
                self._security_events.log_restart("configuration_changed")
            else:
                self._security_events.log_startup_if_transitioned(was_running)

        except ops.pebble.ChangeError as e:
            self._security_events.log_crash_if_error()
            logger.exception("Pebble replan failed for dag-processor service: %s", e)
            raise ExitWithStatusError(
                "Failed to replan Pebble services",
                ops.BlockedStatus,
            )
        except ops.pebble.APIError as e:
            self._security_events.log_crash_if_error()
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
