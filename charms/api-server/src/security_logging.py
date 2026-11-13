"""Airflow lifecycle security events using the approved OWASP logger."""

import json
import logging
from datetime import datetime, timezone
from typing import Any

import ops
from owasp_logger import OWASPLogger

_OWASP_EVENT_KEY = "owasp_event"


class _ContextLogger(logging.LoggerAdapter):
    """Add Airflow context fields to the OWASP logger's structured event."""

    def process(self, message: str, kwargs: dict[str, Any]) -> tuple[str, dict[str, Any]]:
        extra = dict(kwargs.get("extra") or {})
        event = extra.get(_OWASP_EVENT_KEY)
        if event is not None:
            event = dict(event)
            event.update(self.extra)
            event["datetime"] = datetime.now(timezone.utc).isoformat(timespec="milliseconds")
            if event.get("level") == "WARNING":
                event["level"] = "WARN"
            extra[_OWASP_EVENT_KEY] = event
            kwargs["extra"] = extra
            message = json.dumps({_OWASP_EVENT_KEY: event}, separators=(",", ":"))
        return message, kwargs


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
        self._component = component
        self._service_name = service_name
        self._context_logger = _ContextLogger(
            logging.getLogger(__name__),
            {"target": service_name, "actor": "", "action": "", "outcome": "", "data": {}},
        )
        self._owasp_logger = OWASPLogger(appid=appid, logger=self._context_logger)

    def is_running(self) -> bool | None:
        """Return the service state, or None when Pebble cannot be queried."""
        service = self._get_service_info()
        return bool(service and service.is_running())

    def _get_service_info(self):
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
                "start",
                "success",
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
                "stop",
                "success",
                "Service stopped by the charm.",
                trigger=trigger,
            )

    def log_restart(self, trigger: str) -> None:
        """Log a restart after the charm's Pebble restart call succeeds."""
        self._emit(
            "sys_restart",
            "juju-charm",
            "juju-charm",
            "restart",
            "success",
            "Service restarted by the charm.",
            trigger=trigger,
        )

    def log_crash_if_error(self) -> None:
        """Log a crash only when Pebble reports the service in ERROR state."""
        service = self._get_service_info()
        if service and service.current.value == "error":
            self._emit(
                "sys_crash",
                "PebbleServiceError",
                "pebble",
                "crash",
                "failure",
                "Pebble reports the service in an error state.",
                status=service.current.value,
            )

    def _emit(
        self,
        method_name: str,
        identity: str,
        actor: str,
        action: str,
        outcome: str,
        description: str,
        **data: str,
    ) -> None:
        self._context_logger.extra.update(
            {
                "target": self._service_name,
                "actor": actor,
                "action": action,
                "outcome": outcome,
                "data": data,
            }
        )
        try:
            getattr(self._owasp_logger, method_name)(identity, description=description)
        finally:
            self._context_logger.extra.update(
                {"actor": "", "action": "", "outcome": "", "data": {}}
            )
