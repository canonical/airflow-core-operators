"""Tests for the shared Pebble system security event logger."""

import json
import logging
from collections import UserDict
from datetime import datetime
from unittest.mock import Mock

import ops
import pytest

import constants
from security_logging import PebbleServiceSecurityLogger


@pytest.mark.parametrize(
    ("method_name", "args", "event_name"),
    [
        ("log_startup_if_transitioned", (False,), "sys_startup:pebble"),
        ("log_shutdown_if_running", (True, "relation_removed"), "sys_shutdown:juju-charm"),
        ("log_restart", ("configuration_changed",), "sys_restart:juju-charm"),
        ("log_crash_if_error", (), "sys_crash:PebbleServiceError"),
    ],
)
def test_emits_system_event_json(caplog, method_name, args, event_name):
    service = Mock()
    service.is_running.return_value = True
    service.current = ops.pebble.ServiceStatus.ERROR
    container = Mock()
    container.get_services.return_value = UserDict({"api-server": service})
    security_logger = PebbleServiceSecurityLogger(
        container=container,
        appid=constants.APP_ID,
        component="api-server",
        service_name="api-server",
    )

    with caplog.at_level(
        logging.WARNING,
        logger="security_logging",
    ):
        getattr(security_logger, method_name)(*args)

    record = next(
        record
        for record in caplog.records
        if record.name == "security_logging"
    )
    event = json.loads(record.getMessage())["owasp_event"]
    datetime.fromisoformat(event["datetime"])
    assert event["appid"] == "airflow.api-server"
    assert event["type"] == "security"
    assert event["event"] == event_name
    assert event["level"] == "WARN"
    assert event["actor"]
    assert event["target"] == "api-server"
    assert event["action"]
    assert event["outcome"]
    assert event["data"]


def test_startup_is_not_logged_without_a_state_transition(caplog):
    service = Mock()
    service.is_running.return_value = True
    container = Mock()
    container.get_services.return_value = UserDict({"api-server": service})
    security_logger = PebbleServiceSecurityLogger(
        container=container,
        appid=constants.APP_ID,
        component="api-server",
        service_name="api-server",
    )

    with caplog.at_level(
        logging.WARNING,
        logger="security_logging",
    ):
        security_logger.log_startup_if_transitioned(was_running=True)

    assert not caplog.records
