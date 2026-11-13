# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.
#
# The integration tests use the Jubilant library. See https://documentation.ubuntu.com/jubilant/
# To learn more about testing, see https://documentation.ubuntu.com/ops/latest/explanation/testing/

"""Integration tests validating core charm behavior."""

import json
import re
import pytest
import jubilant
import shlex
from tenacity import Retrying, stop_after_attempt, wait_fixed

from tests.integration.conftest import (
    get_pebble_service_status,
    pebble_service_is_running,
)
from tests.integration.helpers.airflow_helpers import read_airflow_config
import tests.integration.helpers.constants as constants


@pytest.mark.parametrize("component, app", list(constants.CORE_CHARMS.items()))
def test_pebble_services_and_config_exist(
    juju: jubilant.Juju,
    deployed_stack,
    component: str,
    app: str,
):
    """Pebble services should be active and config should be present."""

    for attempt in Retrying(
        stop=stop_after_attempt(6), wait=wait_fixed(30), reraise=True
    ):
        with attempt:
            output = juju.ssh(
                f"{app}/0",
                f"test -f {shlex.quote(constants.AIRFLOW_CONFIG_PATH)} && echo OK || echo MISSING",
                container=constants.CONTAINER_NAMES[component],
            )
            assert "OK" in output, (
                f"{app}: expected {constants.AIRFLOW_CONFIG_PATH} to exist"
            )

            ownership = juju.ssh(
                f"{app}/0",
                f"stat -c '%U:%G' {shlex.quote(constants.AIRFLOW_CONFIG_PATH)}",
                container=constants.CONTAINER_NAMES[component],
            ).strip()
            assert (
                ownership == f"{constants.WORKLOAD_USER}:{constants.WORKLOAD_GROUP}"
            ), (
                f"{app}: expected {constants.AIRFLOW_CONFIG_PATH} to be owned by "
                f"{constants.WORKLOAD_USER}:{constants.WORKLOAD_GROUP}, got {ownership}"
            )

            assert pebble_service_is_running(
                juju, f"{app}/0", component, constants.PEBBLE_SERVICE_NAME
            ), f"{app}: pebble service '{constants.PEBBLE_SERVICE_NAME}' not active."


def test_airflow_cluster_health_via_api_endpoint(
    juju: jubilant.Juju,
):
    """Airflow API health endpoint should report all core components healthy."""

    service_host = (
        f"{constants.CORE_CHARMS['api-server']}-endpoints."
        f"{juju.model}.svc.cluster.local:8080"
    )

    check_cmd = (
        "curl -s http://localhost:8080/api/v2/monitor/health; echo '---'; "
        f"curl -s http://{service_host}/api/v2/monitor/health || true"
    )
    out = juju.ssh(
        f"{constants.CORE_CHARMS['api-server']}/0", "bash -lc " + shlex.quote(check_cmd)
    )

    parts = out.split("---", 1)
    if len(parts) != 2:
        raise AssertionError(f"Unexpected API health output:\n{out}")

    assert all(v["status"] == "healthy" for v in json.loads(parts[0]).values()), (
        f"API unhealthy from localhost:\n{parts[0]}"
    )

    assert all(v["status"] == "healthy" for v in json.loads(parts[1]).values()), (
        f"API unhealthy in cluster:\n{parts[1]}"
    )


@pytest.mark.parametrize("component, app", list(constants.CORE_CHARMS.items()))
def test_airflow_config_cli_values(
    juju: jubilant.Juju,
    component: str,
    app: str,
):
    """Airflow CLI should return expected config values."""

    cfg = read_airflow_config(juju, f"{app}/0", constants.CONTAINER_NAMES[component])

    assert cfg.get("core", "executor") == "LocalExecutor"
    assert cfg.get("api", "port") == "8080"


def test_charm_statuses_on_missing_relation(
    juju: jubilant.Juju,
):
    """Scheduler and coordinator should block if their relation is removed."""
    juju.remove_relation(
        f"{constants.COORDINATOR_APP}:{constants.COORD_REL}",
        f"{constants.CORE_CHARMS['scheduler']}:{constants.COORD_REL}",
    )

    juju.wait(jubilant.all_agents_idle, timeout=10 * 60)

    status = juju.status()
    coord_app = status.apps[constants.COORDINATOR_APP]
    sched_app = status.apps[constants.CORE_CHARMS["scheduler"]]

    assert coord_app.is_blocked, (
        f"Expected coordinator blocked, got {coord_app.app_status.current}"
    )
    assert sched_app.is_blocked, (
        f"Expected scheduler blocked, got {sched_app.app_status.current}"
    )

    assert all(
        status.apps[app].is_waiting
        for component, app in constants.CORE_CHARMS.items()
        if component in {"api-server", "triggerer", "dag-processor"}
    ), "Expected api-server, triggerer, and dag-processor to be waiting"

    juju.integrate(
        f"{constants.COORDINATOR_APP}:{constants.COORD_REL}",
        f"{constants.CORE_CHARMS['scheduler']}:{constants.COORD_REL}",
    )

    juju.wait(jubilant.all_agents_idle, timeout=5 * 60, successes=3, delay=20)


def test_core_charms_wait_when_database_unavailable(
    juju: jubilant.Juju,
):
    """Core charms should go waiting if Postgres is scaled down or removed."""

    juju.remove_relation(
        f"{constants.PGBOUNCER_APP}:database",
        f"{constants.COORDINATOR_APP}:postgres",
    )

    juju.wait(jubilant.all_agents_idle, timeout=10 * 60)

    juju.wait(
        ready=lambda st: all(
            st.apps[app].is_waiting for _, app in constants.CORE_CHARMS.items()
        ),
        timeout=15 * 60,
    )

    for component, app in constants.CORE_CHARMS.items():
        app_status = juju.status().apps[app]
        assert app_status.is_waiting, (
            f"Expected {app} waiting, got {app_status.app_status.current}"
        )
        expected_state = "active" if component == "api-server" else "backoff"

        for attempt in Retrying(
            stop=stop_after_attempt(6), wait=wait_fixed(30), reraise=True
        ):
            with attempt:
                if expected_state == "active":
                    assert pebble_service_is_running(
                        juju, f"{app}/0", component, constants.PEBBLE_SERVICE_NAME
                    ), (
                        f"{app}: pebble service '{constants.PEBBLE_SERVICE_NAME}' not active while waiting."
                    )
                else:
                    current = get_pebble_service_status(
                        juju, component, f"{app}/0", constants.PEBBLE_SERVICE_NAME
                    )["current"]
                    assert current == "backoff", (
                        f"{app}: pebble service '{constants.PEBBLE_SERVICE_NAME}' not in backoff while waiting."
                    )
    juju.integrate(
        f"{constants.PGBOUNCER_APP}:database",
        f"{constants.COORDINATOR_APP}:postgres",
    )
    juju.wait(jubilant.all_agents_idle, timeout=5 * 60, successes=3, delay=30)


def test_send_otlp_propagates_otel_config_to_core_charms(
    juju: jubilant.Juju,
):
    """Integrating coordinator with otelcol writes OTel config to all core charms."""
    juju.deploy(
        "opentelemetry-collector-k8s",
        app="otelcol",
        channel="0.130/stable",
        trust=True,
    )

    try:
        juju.integrate(
            f"{constants.COORDINATOR_APP}:send-otlp",
            "otelcol:receive-otlp",
        )

        juju.wait(
            lambda status: (
                jubilant.all_active(
                    status,
                    constants.COORDINATOR_APP,
                    *constants.CORE_APPS,
                )
                and jubilant.all_blocked(status, "otelcol")
                and jubilant.all_agents_idle(status)
            ),
            error=jubilant.any_error,
            timeout=10 * 60,
            successes=3,
            delay=5,
        )

        for component, app in constants.CORE_CHARMS.items():
            for attempt in Retrying(
                stop=stop_after_attempt(12), wait=wait_fixed(10), reraise=True
            ):
                with attempt:
                    cfg = read_airflow_config(
                        juju, f"{app}/0", constants.CONTAINER_NAMES[component]
                    )
                    for section in ("metrics", "traces"):
                        assert cfg.has_section(section)
                        assert cfg.get(section, "otel_on") == "True"
                        assert cfg.get(section, "otel_host").startswith("otelcol.")
                        assert cfg.get(section, "otel_port") == "4318"
                        assert cfg.get(section, "otel_service") == app
                        assert cfg.get(section, "otel_ssl_active") == "False"

        # Verify real Airflow metrics from the core charms reach the OTel collector.
        metric_line_re = re.compile(
            r"^otelcol_receiver_accepted_metric_points(?:\{([^}]*)\})?\s+([0-9.eE+-]+)$",
            re.MULTILINE,
        )
        for attempt in Retrying(
            stop=stop_after_attempt(12), wait=wait_fixed(10), reraise=True
        ):
            with attempt:
                metrics_out = juju.ssh(
                    "otelcol/0", "curl -s http://localhost:8888/metrics"
                )
                points = next(
                    (
                        float(m.group(2))
                        for m in metric_line_re.finditer(metrics_out)
                        if 'receiver="otlp' in (m.group(1) or "")
                        and 'transport="http"' in (m.group(1) or "")
                    ),
                    None,
                )
                assert points is not None and points > 0

        juju.remove_relation(
            f"{constants.COORDINATOR_APP}:send-otlp",
            "otelcol:receive-otlp",
        )

        juju.wait(
            lambda status: (
                jubilant.all_active(
                    status,
                    constants.COORDINATOR_APP,
                    *constants.CORE_APPS,
                )
                and jubilant.all_agents_idle(status)
                and "send-otlp" not in status.apps[constants.COORDINATOR_APP].relations
            ),
            error=jubilant.any_error,
            timeout=10 * 60,
        )

        for component, app in constants.CORE_CHARMS.items():
            for attempt in Retrying(
                stop=stop_after_attempt(12), wait=wait_fixed(10), reraise=True
            ):
                with attempt:
                    cfg = read_airflow_config(
                        juju, f"{app}/0", constants.CONTAINER_NAMES[component]
                    )
                    assert not cfg.has_section("metrics")
                    assert not cfg.has_section("traces")
    finally:
        juju.remove_application("otelcol", force=True)
        juju.wait(lambda status: "otelcol" not in status.apps, timeout=5 * 60)
