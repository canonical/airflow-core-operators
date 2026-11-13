"""Security event logging for Airflow Pebble-managed services."""

import json
import logging
from datetime import datetime, timezone
from typing import Any

import ops

# The unique Charmhub library identifier, never change it
LIBID = "8d7f0e2c6a1947ad9b74d3e2f1c0a5b6"

# Increment this major API version when introducing breaking changes
LIBAPI = 0

# Increment this PATCH version before using `charmcraft publish-lib`
LIBPATCH = 1


class PebbleServiceSecurityLogger:
    """Emit system security events for a Pebble-managed Airflow service."""

    def __init__(
        self,
        container: ops.model.Container,
        appid: str,
        component: str,
        service_name: str,
    ) -> None:
        self._container = container
        self._appid = appid
        self._component = component
        self._service_name = service_name
        self._logger = logging.getLogger(__name__)

    def is_running(self) -> bool | None:
        """Return the service state, or None when Pebble cannot be queried."""
        service = self._get_service_info()
        return bool(service and service.is_running())

    def _get_service_info(self) -> Any | None:
        """Return Pebble service info when it is available."""
        try:
            services = self._container.get_services(self._service_name)
        except (ops.pebble.APIError, ops.pebble.ConnectionError):
            return None
        return services.get(self._service_name)

    def log_startup_if_transitioned(self, was_running: bool | None) -> None:
        """Log startup only when a stopped service becomes running."""
        if was_running is False and self.is_running() is True:
            self._emit(
                "sys_startup",
                "pebble",
                "pebble",
                "Service started by Pebble after charm reconciliation.",
                trigger="pebble_replan",
            )

    def log_shutdown_if_running(self, was_running: bool | None, trigger: str) -> None:
        """Log a successful charm-requested stop of a running service."""
        if was_running:
            self._emit(
                "sys_shutdown",
                "juju-charm",
                "juju-charm",
                "Service stopped by the charm.",
                trigger=trigger,
            )

    def log_restart(self, trigger: str) -> None:
        """Log a restart after the charm's Pebble restart call succeeds."""
        self._emit(
            "sys_restart",
            "juju-charm",
            "juju-charm",
            "Service restarted by the charm.",
            trigger=trigger,
        )

    def log_crash_if_error(self) -> None:
        """Log a crash only when Pebble reports the service in ERROR state."""
        service = self._get_service_info()
        if service and service.current == ops.pebble.ServiceStatus.ERROR:
            self._emit(
                "sys_crash",
                "PebbleServiceError",
                "pebble",
                "Pebble reports the service in an error state.",
                status=service.current.value,
            )

    def _emit(
        self,
        event_type: str,
        event_parameter: str,
        actor: str,
        description: str,
        **data: str,
    ) -> None:
        payload = {
            "datetime": datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
            "appid": self._appid,
            "event": f"{event_type}:{event_parameter}",
            "level": "WARN",
            "actor": actor,
            "target": self._service_name,
            "description": f"Airflow {self._component}: {description}",
            "data": data,
        }
        self._logger.warning(json.dumps(payload, separators=(",", ":")))
