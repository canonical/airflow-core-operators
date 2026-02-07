"""Pytest fixtures for integration tests."""

from __future__ import annotations

import base64
import logging
import os
import pathlib
import shlex
import sys
import time
import re

import jubilant
import pytest

from tests.integration.helpers.constants import (
    ALL_APPS,
    COORDINATOR_APP,
    COORDINATOR_CHANNEL,
    CORE_CHARMS,
    POSTGRES_APP,
    POSTGRES_CHANNEL,
    COORD_REL,
    REPO_ROOT,
)
from tests.integration.helpers.airflow_helpers import ensure_db_migrated

logger = logging.getLogger(__name__)


def charm_dir(name: str) -> pathlib.Path:
    """Path to charm directory in repo /charms folder."""
    return REPO_ROOT / "charms" / name


def image_resources() -> dict[str, dict[str, str]]:
    """Return OCI image resource mappings for core charms."""
    tag = os.environ.get("AIRFLOW_IMAGE_TAG", "3.1-24.04_edge")
    base = os.environ.get("AIRFLOW_IMAGE_BASE", "ubuntu/airflow")

    return {
        "airflow-api-server-k8s": {"airflow-api-server-image": f"{base}:{tag}"},
        "airflow-dag-processor-k8s": {"airflow-dag-processor-image": f"{base}:{tag}"},
        "airflow-scheduler-k8s": {"airflow-scheduler-image": f"{base}:{tag}"},
        "airflow-triggerer-k8s": {"airflow-triggerer-image": f"{base}:{tag}"},
    }


@pytest.fixture(scope="module")
def juju(request: pytest.FixtureRequest):
    """Create a temporary Juju model for running tests."""
    if "JUJU_MODEL" in os.environ:
        # Ensure the named model exists when JUJU_MODEL is set.
        juju = jubilant.Juju(wait_timeout=20 * 60)
        juju.add_model(os.environ["JUJU_MODEL"], config={"update-status-hook-interval": "10s"})
        yield juju

        if request.session.testsfailed:
            logger.info("Collecting Juju logs...")
            time.sleep(0.5)
            log = juju.debug_log(limit=1000)
            print(log, end="", file=sys.stderr)
        return

    with jubilant.temp_model(config={"update-status-hook-interval": "10s"}) as juju:
        juju.wait_timeout = 20 * 60
        yield juju

        if request.session.testsfailed:
            logger.info("Collecting Juju logs...")
            time.sleep(0.5)
            log = juju.debug_log(limit=1000)
            print(log, end="", file=sys.stderr)


@pytest.fixture(scope="module")
def coordinator_charm():
    """Return the coordinator charm reference."""
    # Use local charm if available, otherwise use charmhub
    coordinator_path = pathlib.Path("../airflow-coordinator-k8s-operator")
    if coordinator_path.exists():
        charm_files = list(coordinator_path.glob("airflow-coordinator-k8s*.charm"))
        if charm_files:
            return str(charm_files[0].resolve())

    # Fallback to charmhub
    return f"ch:{COORDINATOR_APP}"


@pytest.fixture(scope="module")
def core_charms():
    """Return paths to already-packed core charms."""
    charm_paths = {}
    for dir_name, app in CORE_CHARMS:
        charm_dir_path = charm_dir(dir_name)
        charm_files = list(charm_dir_path.glob("*.charm"))
        if not charm_files:
            raise FileNotFoundError(
                f"No .charm file found in {charm_dir_path}. Run 'just pack-charms' first."
            )
        # Use the most recent .charm file
        charm_paths[app] = max(charm_files, key=lambda p: p.stat().st_mtime)
    return charm_paths


@pytest.fixture(scope="module")
def deployed_stack(juju: jubilant.Juju, coordinator_charm: str, core_charms: dict):
    """Deploy the full Airflow stack."""
    logger.info("Deploying PostgreSQL...")
    juju.deploy(POSTGRES_APP, channel=POSTGRES_CHANNEL, trust=True)

    # Wait for PostgreSQL to be ready
    logger.info("Waiting for PostgreSQL to be active...")
    juju.wait(lambda st: jubilant.all_active(st, POSTGRES_APP), timeout=10 * 60)

    logger.info("Deploying Airflow Coordinator...")
    if coordinator_charm.startswith("ch:"):
        juju.deploy(
            coordinator_charm.replace("ch:", ""),
            app=COORDINATOR_APP,
            channel=COORDINATOR_CHANNEL,
        )
    else:
        juju.deploy(coordinator_charm, app=COORDINATOR_APP)

    logger.info("Deploying core charms...")
    resources_map = image_resources()
    for component, app in CORE_CHARMS:
        charm_path = str(core_charms[app])
        resources = resources_map.get(app, {})
        juju.deploy(charm_path, app=app, resources=resources)

    logger.info("Integrating coordinator <-> postgres")
    juju.integrate(f"{COORDINATOR_APP}:postgres", f"{POSTGRES_APP}:database")

    logger.info("Waiting for coordinator-postgres relation to be ready...")
    juju.wait(jubilant.all_agents_idle, timeout=10 * 60)

    logger.info("Integrating all core charms")
    for _, app in CORE_CHARMS:
        juju.integrate(f"{COORDINATOR_APP}:{COORD_REL}", f"{app}:{COORD_REL}")

    logger.info("Waiting for all core charm relations to be ready...")
    juju.wait(jubilant.all_agents_idle, timeout=15 * 60)

    ensure_db_migrated(juju, "airflow-api-server-k8s")


def unit_name(app: str, n: int = 0) -> str:
    """Return unit name for app."""
    return f"{app}/{n}"


def workload_container_for_app(app: str) -> str:
    """Get container name for app."""
    # Remove k8s suffix and return workload container name
    return app.replace("-k8s", "")


def ssh(juju: jubilant.Juju, unit: str, container: str, cmd: str) -> str:
    """Run command in unit container via SSH."""
    return juju.cli("ssh", "--container", container, unit, cmd)


def ssh_unit(juju: jubilant.Juju, unit: str, cmd: str) -> str:
    """Run command in unit without container specification."""
    return juju.cli("ssh", unit, cmd)


def file_exists(juju: jubilant.Juju, unit: str, container: str, path: str) -> bool:
    """Check if file exists in container."""
    output = ssh(
        juju, unit, container, f"test -f {shlex.quote(path)} && echo OK || echo MISSING"
    )
    return "OK" in output


def pebble_services_text(juju: jubilant.Juju, unit: str, container: str) -> str:
    """Get pebble services output."""
    return ssh(juju, unit, container, "pebble services || true")


def pebble_service_is_running(services_text: str, service: str) -> bool:
    """Return True if a Pebble service is active in the services output."""
    pattern = rf"^{re.escape(service)}\s+enabled\s+active\s+"
    return re.search(pattern, services_text, flags=re.MULTILINE) is not None


def push_text_file(
    juju: jubilant.Juju,
    unit: str,
    container: str,
    dst_path: str,
    content: str,
) -> None:
    """Push text content to a file inside a unit container."""
    parent_dir = os.path.dirname(dst_path) or "/"
    parent_q = shlex.quote(parent_dir)
    dst_q = shlex.quote(dst_path)

    payload_b64 = base64.b64encode(content.encode("utf-8")).decode("ascii")
    payload_q = shlex.quote(payload_b64)

    cmd = "bash -lc " + shlex.quote(
        f"mkdir -p {parent_q} && echo {payload_q} | base64 -d > {dst_q}"
    )
    ssh(juju, unit, container, cmd)


def remove_relation_if_exists(
    juju: jubilant.Juju, endpoint_a: str, endpoint_b: str
) -> None:
    """Remove a relation if it exists, ignoring missing relations."""
    try:
        juju.cli("remove-relation", endpoint_a, endpoint_b)
    except Exception:
        pass


def integrate_if_missing(juju: jubilant.Juju, endpoint_a: str, endpoint_b: str) -> None:
    """Integrate two endpoints if they are not already related."""
    try:
        juju.integrate(endpoint_a, endpoint_b)
    except Exception:
        pass
