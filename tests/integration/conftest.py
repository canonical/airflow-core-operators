"""Pytest fixtures for integration tests."""

# from __future__ import annotations
from pathlib import Path
import logging
import os
import shlex
import sys
import time
import re

import jubilant
import pytest
import base64

from tests.integration.helpers.constants import (
    IMAGE,
    ALL_APPS,
    COORDINATOR_APP,
    COORDINATOR_CHANNEL,
    CORE_CHARMS,
    PGBOUNCER_APP,
    PGBOUNCER_CHANNEL,
    POSTGRES_APP,
    POSTGRES_CHANNEL,
    POSTGRES_PROFILE,
    COORD_REL,
    REPO_ROOT,
)
from tests.integration.helpers.airflow_helpers import (
    ensure_db_migrated,
)

logger = logging.getLogger(__name__)

EXPECTED_RELATIONS = [
    (COORDINATOR_APP, "postgres"),
    *[(app, COORD_REL) for _, app in CORE_CHARMS.items()],
]

def image_resources() -> dict[str, dict[str, str]]:
    """Return OCI image resource mappings for core charms."""


    return {
        "airflow-api-server-k8s": {"airflow-api-server-image": IMAGE},
        "airflow-dag-processor-k8s": {"airflow-dag-processor-image": IMAGE},
        "airflow-scheduler-k8s": {"airflow-scheduler-image": IMAGE},
        "airflow-triggerer-k8s": {"airflow-triggerer-image": IMAGE},
    }


@pytest.fixture(scope="module")
def juju(request: pytest.FixtureRequest):
    """Create a temporary Juju model for running tests."""
    if "JUJU_MODEL" in os.environ:
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
    coordinator_path = Path("../airflow-coordinator-k8s-operator")
    if coordinator_path.exists():
        charm_files = list(coordinator_path.glob("airflow-coordinator-k8s*.charm"))
        if charm_files:
            return str(charm_files[0].resolve())

    return f"ch:{COORDINATOR_APP}"


@pytest.fixture(scope="module")
def core_charms():
    """Return paths to already-packed core charms."""
    charm_paths = {}
    for dir_name, app in CORE_CHARMS.items():
        charm_dir_path = REPO_ROOT / "charms" / dir_name
        charm_files = list(charm_dir_path.glob("*.charm"))
        if not charm_files:
            raise FileNotFoundError(
                f"No .charm file found in {charm_dir_path}. Run 'just pack-charms' first."
            )
        charm_paths[app] = max(charm_files, key=lambda p: p.stat().st_mtime)
    return charm_paths

@pytest.fixture(scope="module")
def deployed_stack(juju: jubilant.Juju, coordinator_charm: str, core_charms: dict):
    """Deploy the full Airflow stack with PgBouncer in front of PostgreSQL."""
    logger.info("Deploying PostgreSQL...")
    juju.deploy(
        POSTGRES_APP,
        channel=POSTGRES_CHANNEL,
        trust=True,
        config={"profile": POSTGRES_PROFILE},
    )

    logger.info("Waiting for PostgreSQL to be active...")
    juju.wait(lambda st: jubilant.all_active(st, POSTGRES_APP), timeout=30 * 60, successes = 3, delay = 30)

    logger.info("Deploying PgBouncer...")
    pgbouncer_kwargs = {"app": PGBOUNCER_APP, "trust": True}
    if PGBOUNCER_CHANNEL:
        pgbouncer_kwargs["channel"] = PGBOUNCER_CHANNEL
    juju.deploy(PGBOUNCER_APP, **pgbouncer_kwargs)

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
    for _, app in CORE_CHARMS.items():
        charm_path = str(core_charms[app])
        resources = resources_map.get(app, {})
        juju.deploy(charm_path, app=app, resources=resources)

    logger.info("Integrating coordinator <-> pgbouncer")
    juju.integrate(f"{COORDINATOR_APP}:postgres", f"{PGBOUNCER_APP}:database")

    logger.info("Integrating pgbouncer <-> postgres")
    juju.integrate(f"{PGBOUNCER_APP}:backend-database", f"{POSTGRES_APP}:database")

    juju.wait(lambda st: jubilant.all_active(st, POSTGRES_APP), timeout=30 * 60, successes = 3, delay = 30)

    logger.info("Integrating all core charms")
    for _, app in CORE_CHARMS.items():
        juju.integrate(f"{COORDINATOR_APP}:{COORD_REL}", f"{app}:{COORD_REL}")

    logger.info("Waiting for all core charm relations to be ready...")
    juju.wait(jubilant.all_agents_idle, timeout=30 * 60)
    juju.wait(
        ready=lambda st: jubilant.all_active(st, *ALL_APPS),
        timeout=30 * 60,
    )

    ensure_db_migrated(juju, "airflow-api-server-k8s")


# @pytest.fixture(autouse=True)
# def invariant_checker(juju: jubilant.Juju):
#     all_apps_deployed = all(app in juju.status().apps for app in ALL_APPS)

#     expected_relations_present = all(
#         juju.status().apps.get(relation_info[0])
#         and len(juju.status().apps[relation_info[0]].relations.get(relation_info[1], []))
#         for relation_info in EXPECTED_RELATIONS
#     )

#     if not all_apps_deployed or not expected_relations_present:
#         logger.info("Skipping invariant pre-check as model (apps + ready) not present yet")
#     else:
#         assert jubilant.all_active(juju.status())

#     yield

#     all_apps_deployed = all(app in juju.status().apps for app in ALL_APPS)

#     expected_relations_present = all(
#         juju.status().apps.get(relation_info[0])
#         and len(juju.status().apps[relation_info[0]].relations.get(relation_info[1], []))
#         for relation_info in EXPECTED_RELATIONS
#     )

#     if not all_apps_deployed or not expected_relations_present:
#         logger.info("Skipping invariant post-check as model (apps + ready) not present yet")
#         return

#     assert jubilant.all_active(juju.status())

def file_exists(juju: jubilant.Juju, unit: str, container: str, path: str) -> bool:
    """Check if file exists in container."""
    output = juju.cli("ssh", "--container", container, unit, f"test -f {shlex.quote(path)} && echo OK || echo MISSING")
    return "OK" in output

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
    destination_path = Path(dst_path)
    destination_directory = destination_path.parent
    destination_directory_arg = shlex.quote(str(destination_directory))
    destination_path_arg = shlex.quote(dst_path)

    encoded_payload = base64.b64encode(content.encode("utf-8")).decode("ascii")
    encoded_payload_arg = shlex.quote(encoded_payload)

    command = "bash -lc " + shlex.quote(
        f"mkdir -p {destination_directory_arg} "
        f"&& echo {encoded_payload_arg} | base64 -d > {destination_path_arg}"
    )
    juju.cli("ssh", "--container", container, unit, command)
