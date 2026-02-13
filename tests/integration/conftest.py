"""Pytest fixtures for integration tests."""

from pathlib import Path
import logging
import shlex
import sys
import time

import jubilant
import pytest
import base64

import tests.integration.helpers.constants as constants

from tests.integration.helpers.airflow_helpers import (
    ensure_db_migrated,
)

logger = logging.getLogger(__name__)

EXPECTED_RELATIONS = [
    (constants.COORDINATOR_APP, "postgres"),
    *[(app, constants.COORD_REL) for _, app in constants.CORE_CHARMS.items()],
]


@pytest.fixture(scope="module")
def juju(request: pytest.FixtureRequest):
    """Create a temporary Juju model for running tests."""

    with jubilant.temp_model(config={"update-status-hook-interval": "10s"}) as juju:
        juju.wait_timeout = 20 * 60
        yield juju

        if request.session.testsfailed:
            logger.info("Collecting Juju logs...")
            time.sleep(0.5)
            log = juju.debug_log(limit=1000)
            print(log, end="", file=sys.stderr)


@pytest.fixture(scope="module")
def core_charms():
    """Return paths to already-packed core charms."""
    charm_paths = {}
    for dir_name, app in constants.CORE_CHARMS.items():
        charm_dir_path = constants.REPO_ROOT / "charms" / dir_name
        charm_files = list(charm_dir_path.glob("*.charm"))
        if len(charm_files) > 1:
            raise FileNotFoundError(
                f"Multiple .charm files found in {charm_dir_path}. Run 'just pack-charms' first."
            )
        charm_paths[app] = charm_files[0]
    return charm_paths


@pytest.fixture(scope="module")
def deployed_stack(juju: jubilant.Juju, core_charms: dict):
    """Deploy the full Airflow stack with PgBouncer in front of PostgreSQL."""
    logger.info("Deploying PostgreSQL...")
    juju.deploy(
        "postgresql-k8s",
        channel=constants.POSTGRES_CHANNEL,
        trust=True,
        config={"profile": "testing"},
    )

    logger.info("Waiting for PostgreSQL to be active...")
    juju.wait(
        lambda st: jubilant.all_active(st, constants.POSTGRES_APP),
        timeout=10 * 60,
        successes=3,
        delay=30,
    )

    logger.info("Deploying PgBouncer...")
    juju.deploy(constants.PGBOUNCER_APP, app=constants.PGBOUNCER_APP, trust=True)
    logger.info("Integrating pgbouncer <-> postgres")
    juju.integrate(
        f"{constants.PGBOUNCER_APP}:backend-database",
        f"{constants.POSTGRES_APP}:database",
    )
    juju.wait(
        lambda st: jubilant.all_active(st, constants.PGBOUNCER_APP),
        timeout=5 * 60,
    )

    logger.info("Deploying Airflow Coordinator...")

    juju.deploy(
        constants.COORDINATOR_APP,
        app=constants.COORDINATOR_APP,
        channel=constants.COORDINATOR_CHANNEL,
    )

    logger.info("Deploying core charms...")
    for _, app in constants.CORE_CHARMS.items():
        charm_path = str(core_charms[app])
        resources = {app.replace("-k8s", "-image"): constants.IMAGE}
        juju.deploy(charm_path, app=app, resources=resources)

    logger.info("Integrating coordinator <-> pgbouncer")
    juju.integrate(
        f"{constants.COORDINATOR_APP}:postgres", f"{constants.PGBOUNCER_APP}:database"
    )
    juju.wait(
        lambda st: jubilant.all_active(st, constants.POSTGRES_APP),
        timeout=10 * 60,
        successes=3,
        delay=30,
    )

    logger.info("Integrating all core charms")
    for _, app in constants.CORE_CHARMS.items():
        juju.integrate(
            f"{constants.COORDINATOR_APP}:{constants.COORD_REL}",
            f"{app}:{constants.COORD_REL}",
        )

    logger.info("Waiting for all core charm relations to be ready...")
    juju.wait(jubilant.all_agents_idle, timeout=5 * 60)
    juju.wait(
        ready=lambda st: jubilant.all_active(st, *constants.ALL_APPS),
        timeout=10 * 60,
    )
    ensure_db_migrated(juju, "airflow-api-server-k8s")


@pytest.fixture(autouse=True)
def invariant_checker(juju: jubilant.Juju):
    all_apps_deployed = all(app in juju.status().apps for app in constants.ALL_APPS)

    expected_relations_present = all(
        juju.status().apps.get(application)
        and len(juju.status().apps[application].relations.get(relation_endpoint, []))
        for application, relation_endpoint in EXPECTED_RELATIONS
    )

    if not all_apps_deployed or not expected_relations_present:
        logger.info(
            "Skipping invariant pre-check as model (apps + ready) not present yet"
        )
    else:
        juju.wait(jubilant.all_agents_idle, timeout=5 * 60)
        assert jubilant.all_active(juju.status())

    yield

    all_apps_deployed = all(app in juju.status().apps for app in constants.ALL_APPS)

    expected_relations_present = all(
        juju.status().apps.get(application)
        and len(juju.status().apps[application].relations.get(relation_endpoint, []))
        for application, relation_endpoint in EXPECTED_RELATIONS
    )

    if not all_apps_deployed or not expected_relations_present:
        logger.info(
            "Skipping invariant post-check as model (apps + ready) not present yet"
        )
        return
    juju.wait(jubilant.all_agents_idle, timeout=5 * 60)
    assert jubilant.all_active(juju.status())


def file_exists(juju: jubilant.Juju, unit: str, container: str, path: str) -> bool:
    """Check if file exists in container."""
    output = juju.cli(
        "ssh",
        "--container",
        container,
        unit,
        f"test -f {shlex.quote(path)} && echo OK || echo MISSING",
    )
    return "OK" in output


def pebble_service_is_running(
    juju: jubilant.Juju,
    unit: str,
    service_name: str,
) -> bool:
    """Return True if a Pebble service is active in a unit container."""
    startup = get_pebble_service_startup(juju, unit, service_name)
    current = get_pebble_service_current(juju, unit, service_name)
    return startup == "enabled" and current == "active"


def get_pebble_service_startup(
    juju: jubilant.Juju,
    unit: str,
    service_name: str,
) -> str:
    """Return the Pebble service startup value for a unit."""
    return _get_pebble_service_status(juju, unit, service_name)["startup"]


def get_pebble_service_current(
    juju: jubilant.Juju,
    unit: str,
    service_name: str,
) -> str:
    """Return the Pebble service current value for a unit."""
    return _get_pebble_service_status(juju, unit, service_name)["current"]


def _get_pebble_service_status(
    juju: jubilant.Juju,
    unit: str,
    service_name: str,
) -> dict[str, str]:
    app = unit.split("/")[0]
    container = constants.CONTAINER_NAMES.get(app) or app.replace("-k8s", "")
    services_text = juju.cli(
        "ssh",
        "--container",
        container,
        unit,
        "pebble services || true",
    )
    lines = [line for line in services_text.splitlines() if line.strip()]
    for line in lines[1:]:
        parts = line.split()
        if len(parts) >= 3 and parts[0] == service_name:
            return {"startup": parts[1], "current": parts[2]}
    raise ValueError(
        f"Service '{service_name}' not found in Pebble services output for {unit}.\n"
        f"Output:\n{services_text}"
    )


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
