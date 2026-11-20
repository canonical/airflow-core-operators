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


def _security_logger(is_running=True, current=ops.pebble.ServiceStatus.ACTIVE):
    service = Mock()
    service.is_running.return_value = is_running
    service.current = current
    container = Mock()
    container.get_services.return_value = UserDict({constants.SERVICE_NAME: service})
    return PebbleServiceSecurityLogger(
        container=container,
        appid=constants.APP_ID,
        service_name=constants.SERVICE_NAME,
    )


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
    security_logger = _security_logger(current=ops.pebble.ServiceStatus.ERROR)

    with caplog.at_level(logging.WARNING, logger="security_logging"):
        getattr(security_logger, method_name)(*args)

    record = next(record for record in caplog.records if record.name == "security_logging")
    event = json.loads(record.getMessage())["owasp_event"]
    datetime.fromisoformat(event["datetime"])
    assert event["appid"] == constants.APP_ID
    assert event["type"] == "security"
    assert event["event"] == event_name
    assert event["level"] == "WARN"
    assert event["actor"]
    assert event["target"] == constants.SERVICE_NAME
    assert "action" not in event
    assert "outcome" not in event
    assert event["data"]


@pytest.mark.parametrize(
    ("method_name", "args", "is_running", "current"),
    [
        pytest.param(
            "log_startup_if_transitioned",
            (True,),
            True,
            ops.pebble.ServiceStatus.ACTIVE,
            id="startup-when-already-running",
        ),
        pytest.param(
            "log_shutdown_if_running",
            (False, "relation_removed"),
            False,
            ops.pebble.ServiceStatus.INACTIVE,
            id="shutdown-when-not-running",
        ),
        pytest.param(
            "log_crash_if_error",
            (),
            False,
            ops.pebble.ServiceStatus.INACTIVE,
            id="crash-when-not-in-error",
        ),
    ],
)
def test_no_event_without_matching_state(caplog, method_name, args, is_running, current):
    security_logger = _security_logger(is_running=is_running, current=current)

    with caplog.at_level(logging.WARNING, logger="security_logging"):
        getattr(security_logger, method_name)(*args)

    assert not caplog.records
