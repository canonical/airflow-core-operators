"""Tests for the shared Pebble system security event logger."""

import json
import logging
from collections import UserDict
from datetime import datetime
from unittest.mock import Mock

import ops
import pytest
from charms.airflow_security.v0.security_logging import PebbleServiceSecurityLogger

import constants


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
        logger="charms.airflow_security.v0.security_logging",
    ):
        getattr(security_logger, method_name)(*args)

    record = next(
        record
        for record in caplog.records
        if record.name == "charms.airflow_security.v0.security_logging"
    )
    event = json.loads(record.getMessage())
    datetime.fromisoformat(event["datetime"])
    assert event["appid"] == "airflow.api-server"
    assert event["event"] == event_name
    assert event["level"] == "WARN"
    assert event["target"] == "api-server"
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
        logger="charms.airflow_security.v0.security_logging",
    ):
        security_logger.log_startup_if_transitioned(was_running=True)

    assert not caplog.records
