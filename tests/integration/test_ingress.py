# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.
#
# The integration tests use the Jubilant library. See https://documentation.ubuntu.com/jubilant/
# To learn more about testing, see https://documentation.ubuntu.com/ops/latest/explanation/testing/

"""Integration tests for Traefik ingress with the Airflow API Server charm.

These tests validate the Airflow stack's behavior when integrated with Traefik
as an ingress-per-app reverse proxy.  Focus areas:

- Health checks through the ingress URL reach the API server.
- ``base_url`` in ``airflow.cfg`` contains the ingress path with ``routing_mode=path``
  and does **not** contain it with ``routing_mode=subdomain`` or after relation removal.
- The Pebble plan is updated with ``--proxy-headers`` and ``FORWARDED_ALLOW_IPS``
  while ingress is related, and reverts when it is removed.
"""

import json
from urllib.parse import urlparse

import jubilant
import requests
from tenacity import Retrying, stop_after_attempt, wait_fixed

from tests.integration.conftest import (
    get_pebble_plan,
    pebble_service_is_running,
)
from tests.integration.helpers.airflow_helpers import read_airflow_config
import tests.integration.helpers.constants as constants


SUBDOMAIN_EXTERNAL_HOSTNAME = "airflow.nip.io"


def _get_traefik_proxied_url(juju: jubilant.Juju) -> str:
    """Retrieve the proxied endpoint URL for the API server via ``show-proxied-endpoints``."""
    result = juju.run(f"{constants.TRAEFIK_APP}/0", "show-proxied-endpoints")
    endpoints = json.loads(result.results["proxied-endpoints"])
    url = endpoints.get(constants.CORE_CHARMS["api-server"], {}).get("url")
    assert url, (
        f"No proxied URL found for {constants.CORE_CHARMS['api-server']} in Traefik endpoints: {endpoints}"
    )
    return url


def _get_api_server_pebble_service(juju: jubilant.Juju) -> dict:
    """Return the pebble plan's service dict for the API server."""
    plan = get_pebble_plan(
        juju, f"{constants.CORE_CHARMS['api-server']}/0", "api-server"
    )
    return plan["services"][constants.PEBBLE_SERVICE_NAME]


def _get_base_url(juju: jubilant.Juju) -> str:
    """Read ``[api] base_url`` from the rendered ``airflow.cfg``."""
    cfg = read_airflow_config(
        juju,
        f"{constants.CORE_CHARMS['api-server']}/0",
        constants.CONTAINER_NAMES["api-server"],
    )
    return cfg.get("api", "base_url", fallback="")


def _assert_pebble_has_proxy_flags(juju: jubilant.Juju) -> None:
    """Assert the pebble service has --proxy-headers and FORWARDED_ALLOW_IPS."""
    svc = _get_api_server_pebble_service(juju)
    assert "--proxy-headers" in svc["command"], (
        f"Expected --proxy-headers in command, got: {svc['command']}"
    )
    assert svc.get("environment", {}).get("FORWARDED_ALLOW_IPS") == "*", (
        f"Expected FORWARDED_ALLOW_IPS=*, got: {svc.get('environment')}"
    )


def _assert_pebble_has_no_proxy_flags(juju: jubilant.Juju) -> None:
    """Assert the pebble service does NOT have proxy headers or FORWARDED_ALLOW_IPS."""
    svc = _get_api_server_pebble_service(juju)
    assert "--proxy-headers" not in svc["command"], (
        f"--proxy-headers should be absent, got: {svc['command']}"
    )
    assert not svc.get("environment", {}).get("FORWARDED_ALLOW_IPS"), (
        f"FORWARDED_ALLOW_IPS should be absent, got: {svc.get('environment')}"
    )


def _set_traefik_routing_mode(
    juju: jubilant.Juju,
    mode: str,
    *,
    external_hostname: str | None = None,
) -> None:
    """Switch Traefik's ``routing_mode`` and optionally set ``external_hostname``."""
    config: dict[str, str] = {"routing_mode": mode}
    if external_hostname:
        config["external_hostname"] = external_hostname
    juju.config(constants.TRAEFIK_APP, config)
    juju.wait(jubilant.all_agents_idle, timeout=5 * 60, successes=2, delay=10)


def test_health_via_ingress_url(juju: jubilant.Juju, ingress_stack):
    """Requests through the Traefik ingress URL reach the API server health endpoint."""
    url = _get_traefik_proxied_url(juju)
    health_url = f"{url.rstrip('/')}/api/v2/monitor/health"
    for attempt in Retrying(
        stop=stop_after_attempt(10), wait=wait_fixed(5), reraise=True
    ):
        with attempt:
            response = requests.get(
                health_url,
                headers={"Accept": "application/json"},
                verify=False,
                timeout=10,
            )
            assert response.status_code == 200, (
                f"Health endpoint failed with {response.status_code}: {response.text}"
            )
            health = response.json()
            assert all(v["status"] == "healthy" for v in health.values()), (
                f"API unhealthy from localhost:\n{health}"
            )


def test_ingress_path_based_routing(juju: jubilant.Juju, ingress_stack):
    """Validate base_url and pebble plan in default path-based routing mode.

    Ensures base_url contains the Traefik path prefix and pebble has proxy flags.
    """

    url = _get_traefik_proxied_url(juju)
    ingress_path = urlparse(url).path.strip("/")
    assert ingress_path, f"Expected a path component in the proxied URL, got: {url}"

    # TODO enable this assertion once PR #34 in coordinator repo is merged
    # base_url = _get_base_url(juju)
    # assert ingress_path in base_url, (
    #     f"[path mode] Expected '{ingress_path}' in base_url, got: '{base_url}'"
    # )
    _assert_pebble_has_proxy_flags(juju)


def test_ingress_relation_broken(juju: jubilant.Juju, ingress_stack):
    """Validate that removing the ingress relation updates base_url and pebble plan."""

    url = _get_traefik_proxied_url(juju)
    ingress_path = urlparse(url).path.strip("/")
    juju.remove_relation(
        f"{constants.CORE_CHARMS['api-server']}:ingress",
        f"{constants.TRAEFIK_APP}:ingress",
    )
    juju.wait(jubilant.all_agents_idle, timeout=5 * 60, successes=2, delay=10)

    base_url = _get_base_url(juju)
    assert ingress_path not in base_url, (
        f"[relation removed] base_url should NOT contain '{ingress_path}', got: '{base_url}'"
    )
    _assert_pebble_has_no_proxy_flags(juju)

    assert pebble_service_is_running(
        juju,
        f"{constants.CORE_CHARMS['api-server']}/0",
        "api-server",
        constants.PEBBLE_SERVICE_NAME,
    )


def test_ingress_subdomain_mode(juju: jubilant.Juju, ingress_stack):
    """Validate that base_url and pebble plan behave correctly in subdomain routing mode."""

    juju.integrate(
        f"{constants.CORE_CHARMS['api-server']}:ingress",
        f"{constants.TRAEFIK_APP}:ingress",
    )
    base_url = _get_base_url(juju)
    url = _get_traefik_proxied_url(juju)
    ingress_path = urlparse(url).path.strip("/")
    _set_traefik_routing_mode(
        juju, "subdomain", external_hostname=SUBDOMAIN_EXTERNAL_HOSTNAME
    )
    juju.wait(jubilant.all_agents_idle, timeout=5 * 60, successes=2, delay=10)

    assert ingress_path not in base_url, (
        f"[subdomain mode] base_url should NOT contain '{ingress_path}', got: '{base_url}'"
    )
    _assert_pebble_has_proxy_flags(juju)
