#!/usr/bin/env python3
# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""Charm of the Airflow scheduler workload."""

import logging

import ops
from charms.airflow_coordinator_k8s.v0.airflow_coordinator import (
    AirflowCoordinatorCoreRequires,
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
        self.config_requires = AirflowCoordinatorCoreRequires(
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
                    "user": constants.WORKLOAD_USER,
                    "group": constants.WORKLOAD_GROUP,
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

    def _cleanup_airflow_home_contents(self) -> None:
        """Cleanup the contents of the Airflow home directory."""
        # Remove config file
        config_path = constants.AIRFLOW_CONFIG_PATH
        if self._container.exists(config_path):
            logger.info("Cleaning up contents of Airflow home...")
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
            self._cleanup_airflow_home_contents()
            raise ExitWithStatusError("Missing airflow-coordinator relation", ops.BlockedStatus)

    def _write_airflow_config(self, config_path: str) -> None:
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
            self.config_requires.write_airflow_config(
                config_path=config_path,
                user=constants.WORKLOAD_USER,
                group=constants.WORKLOAD_GROUP,
            )
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

    def _remove_stale_kubernetes_executor_pod_spec(
        self, filepath: str = constants.AIRFLOW_POD_TEMPLATE_FILE_PATH
    ) -> None:
        """Remove the pod spec if it no longer exists in the relation databag.

        Args:
            filepath: Path inside the workload container where the pod spec is assumed to live.
              Defaults to: AIRFLOW_HOME/pod_templates/worker_pod_template.yaml.
        """
        # This means that the pod spec exists in the databag and
        # there are no issues writing it to the workload container
        if self.config_requires.can_write_kubernetes_executor_pod_spec:
            return

        if self._container.exists(filepath):
            logger.info("Removing Kubernetes Executor pod spec.")
            self._container.remove_path(filepath, recursive=False)

    def _write_kubernetes_executor_pod_spec(
        self, filepath: str = constants.AIRFLOW_POD_TEMPLATE_FILE_PATH
    ) -> None:
        """Write the K8s executor pod spec to the workload container if available.

        This is a no-op when the KubernetesExecutor is not configured (i.e. when
        no pod spec has been shared by the coordinator).

        Args:
            filepath: Path inside the workload container where the pod spec will
                be written. Defaults to AIRFLOW_HOME/pod_templates/worker_pod_template.yaml.

        Raises:
            ExitWithStatusError: if the write operation fails.
        """
        # This means that either the podspec does not exist or there are issues
        # trying to write the file in the workload container
        # FIXME: if the AirflowCoordinatorCoreRequires object had a way to access
        # the value of the k8s executor pod spec, we could improve this check to
        # correctly identify the reason why we cannot write the file.
        if not self.config_requires.can_write_kubernetes_executor_pod_spec:
            logger.info("The Kubernetes Executor pod spec file was not written.")
            return

        try:
            self.config_requires.write_kubernetes_executor_pod_spec(
                filepath=filepath,
                user=constants.WORKLOAD_USER,
                group=constants.WORKLOAD_GROUP,
            )
        except (ops.pebble.ConnectionError, ops.pebble.Error) as e:
            raise ExitWithStatusError(
                "Failed to write pod spec: Pebble connection error", ops.BlockedStatus
            ) from e
        except Exception as e:
            logger.exception("Failed to write pod spec to workload container")
            raise ExitWithStatusError(
                f"Failed to write pod spec to workload container: {e}", ops.BlockedStatus
            ) from e

    def _add_layer_and_replan(self) -> None:
        """Add the Pebble layer and replan.

        The service will start automatically since startup is enabled.

        Raises:
            ExitWithStatusError: If the service cannot be replanned.
        """
        self._container.add_layer("scheduler-base", self._airflow_scheduler_layer, combine=True)

        try:
            self._container.replan()
            if restart_service:
                self._container.restart(constants.SERVICE_NAME)
        except ops.pebble.ChangeError as e:
            logger.exception("Pebble replan failed for scheduler service: %s", e)
            raise ExitWithStatusError(
                "Failed to replan Pebble services",
                ops.BlockedStatus,
            ) from e
        except ops.pebble.APIError as e:
            logger.exception("Pebble restart failed for scheduler service: %s", e)
            raise ExitWithStatusError(
                "Failed to replan Pebble services",
                ops.BlockedStatus,
            ) from e

    def _reconcile(self, _) -> None:
        """Reconcile the state of the charm for any event by running all operations."""
        try:
            self._check_container_can_connect()
            self._check_required_relation_and_act()
            self._write_kubernetes_executor_pod_spec()
            if not self.config_requires.can_write_airflow_config:
                raise ExitWithStatusError("Waiting for relation data", ops.WaitingStatus)
            restart_service = self.config_requires.airflow_config_needs_update(
                config_path=constants.AIRFLOW_CONFIG_PATH,
            )
            if restart_service:
                self._write_airflow_config(config_path=constants.AIRFLOW_CONFIG_PATH)
            self._add_layer_and_replan(restart_service=restart_service)
            self._remove_stale_kubernetes_executor_pod_spec()
        except ExitWithStatusError as e:
            self.unit.status = e.status
            return

        self.unit.status = ops.ActiveStatus()


if __name__ == "__main__":  # pragma: nocover
    ops.main(AirflowSchedulerCharm)
