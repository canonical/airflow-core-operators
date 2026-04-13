# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.
#
# The integration tests use the Jubilant library. See https://documentation.ubuntu.com/jubilant/
# To learn more about testing, see https://documentation.ubuntu.com/ops/latest/explanation/testing/

"""Integration tests for Traefik ingress with the Airflow API Server charm.

These tests validate the Airflow stack's behavior when integrated with Traefik
as an ingress-per-app reverse proxy.  Focus areas:

- Standard HTTP routing and health checks through the Traefik ingress URL.
- HTTPS routing and TLS termination when Traefik is integrated with a certificates operator.
"""

import json

import jubilant
import requests
from tenacity import Retrying, stop_after_attempt, wait_fixed

import tests.integration.helpers.constants as constants


def _get_traefik_proxied_url(juju: jubilant.Juju) -> str:
    """Retrieve the proxied endpoint URL for the API server via ``show-proxied-endpoints``."""
    result = juju.run(f"{constants.TRAEFIK_APP}/0", "show-proxied-endpoints")
    endpoints = json.loads(result.results["proxied-endpoints"])
    url = endpoints.get(constants.CORE_CHARMS["api-server"], {}).get("url")
    assert url, (
        f"No proxied URL found for {constants.CORE_CHARMS['api-server']} in Traefik endpoints: {endpoints}"
    )
    return url


def _health_check_via_url(url: str):
    """Perform a health check request to the given URL and validate the response."""
    health_url = f"{url.rstrip('/')}/api/v2/monitor/health"
    for attempt in Retrying(
        stop=stop_after_attempt(10), wait=wait_fixed(5), reraise=True
    ):
        with attempt:
            resp = requests.get(
                health_url,
                headers={"Accept": "application/json"},
                timeout=10,
                verify=False,
            )
            assert resp.status_code == 200, (
                f"Health endpoint returned {resp.status_code}: {resp.text[:200]}"
            )

        content_type = resp.headers.get("Content-Type", "")
        assert "json" in content_type, (
            f"Expected JSON response, got Content-Type={content_type}: {resp.text[:200]}"
        )
        health = resp.json()
        assert all(v["status"] == "healthy" for v in health.values()), (
            f"Not all components healthy: {health}"
        )


def test_http_health_check_via_ingress(juju: jubilant.Juju, traefik_ingress_stack):
    """Requests through the Traefik ingress URL reach the API server health endpoint."""
    url = _get_traefik_proxied_url(juju)
    _health_check_via_url(url)


def test_https_health_check_via_tls_ingress(
    juju: jubilant.Juju, traefik_https_ingress_stack
):
    """Requests through the Traefik ingress URL reach the API server health endpoint."""
    url = _get_traefik_proxied_url(juju)
    assert url.startswith("https://"), f"Expected HTTPS URL, got: {url}"
    _health_check_via_url(url)
