# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.
#
# The integration tests use the Jubilant library. See https://documentation.ubuntu.com/jubilant/
# To learn more about testing, see https://documentation.ubuntu.com/ops/latest/explanation/testing/

"""Pytest fixtures for integration tests."""

from pathlib import Path
import logging
import os
import sys
import time

import jubilant
import pytest

import tests.integration.helpers.constants as constants


logger = logging.getLogger(__name__)


@pytest.fixture(scope="module")
def juju(request: pytest.FixtureRequest):
    """Create a temporary Juju model for running tests."""
    if "JUJU_MODEL" in os.environ:
        juju = jubilant.Juju(wait_timeout=20 * 60)
        juju.add_model(
            os.environ["JUJU_MODEL"],
            config={"update-status-hook-interval": "10s"},
        )
        yield juju

        if request.session.testsfailed:
            time.sleep(0.5)
            log = juju.debug_log(limit=1000)
            print(log, end="", file=sys.stderr)
        return

    with jubilant.temp_model(config={"update-status-hook-interval": "10s"}) as juju:
        juju.wait_timeout = 20 * 60
        yield juju

        if request.session.testsfailed:
            time.sleep(0.5)
            log = juju.debug_log(limit=1000)
            print(log, end="", file=sys.stderr)


@pytest.fixture(scope="module")
def core_charms():
    """Return paths to already-packed core charms."""
    charm_paths = {}
    for dir_name, app in constants.CORE_CHARMS.items():
        charm_dir_path = constants.REPO_ROOT / "charms" / dir_name
        charm_paths[app] = next(charm_dir_path.glob("*.charm"))
    return charm_paths


@pytest.fixture(scope="module")
def deployed_stack(juju: jubilant.Juju, core_charms: dict):
    """Deploy the full Airflow stack with PgBouncer in front of PostgreSQL."""

    juju.deploy(
        constants.POSTGRES_APP,
        channel=constants.POSTGRES_CHANNEL,
        trust=True,
        config={"profile": "testing"},
    )

    juju.deploy(constants.PGBOUNCER_APP, trust=True)

    juju.integrate(
        f"{constants.PGBOUNCER_APP}:backend-database",
        f"{constants.POSTGRES_APP}:database",
    )

    juju.deploy(
        constants.COORDINATOR_APP,
        app=constants.COORDINATOR_APP,
        channel=constants.COORDINATOR_CHANNEL,
    )

    for component, app in constants.CORE_CHARMS.items():
        juju.deploy(
            core_charms[app], resources=constants.CORE_CHARMS_RESOURCES[component]
        )

    juju.integrate(
        f"{constants.COORDINATOR_APP}:postgres", f"{constants.PGBOUNCER_APP}:database"
    )

    juju.integrate(
        f"{constants.COORDINATOR_APP}:airflow-api-server",
        f"{constants.CORE_CHARMS['api-server']}:airflow-api-server",
    )

    for _, app in constants.CORE_CHARMS.items():
        juju.integrate(
            f"{constants.COORDINATOR_APP}:{constants.COORD_REL}",
            f"{app}:{constants.COORD_REL}",
        )

    juju.wait(jubilant.all_active, timeout=10 * 60, successes=2, delay=20)


@pytest.fixture(autouse=True)
def invariant_checker(juju: jubilant.Juju):
    """Fail fast when core app invariants are broken before or after tests."""
    all_apps_deployed = all(app in juju.status().apps for app in constants.ALL_APPS)

    expected_relations_present = all(
        juju.status().apps.get(application)
        and len(juju.status().apps[application].relations.get(relation_endpoint, []))
        for application, relation_endpoint in constants.EXPECTED_RELATIONS
    )

    if not all_apps_deployed or not expected_relations_present:
        logger.info(
            "Skipping invariant pre-check as model (apps + ready) not present yet"
        )
    else:
        assert jubilant.all_active(juju.status())

    yield

    all_apps_deployed = all(app in juju.status().apps for app in constants.ALL_APPS)

    expected_relations_present = all(
        juju.status().apps.get(application)
        and len(juju.status().apps[application].relations.get(relation_endpoint, []))
        for application, relation_endpoint in constants.EXPECTED_RELATIONS
    )

    if not all_apps_deployed or not expected_relations_present:
        logger.info(
            "Skipping invariant post-check as model (apps + ready) not present yet"
        )
    else:
        assert jubilant.all_active(juju.status())


def pebble_service_is_running(
    juju: jubilant.Juju,
    unit: str,
    component: str,
    service_name: str,
) -> bool:
    """Return True if a Pebble service is active in a unit container."""
    pebble_service_status = get_pebble_service_status(
        juju, component, unit, service_name
    )
    return (
        pebble_service_status["startup"] == "enabled"
        and pebble_service_status["current"] == "active"
    )


def get_pebble_service_status(
    juju: jubilant.Juju,
    component: str,
    unit: str,
    service_name: str,
) -> dict[str, str]:
    """Return Pebble service status fields for a unit container."""
    container = constants.CONTAINER_NAMES[component]
    services_text = juju.ssh(unit, "pebble services || true", container=container)
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
    destination_path: str,
    local_path: str,
) -> None:
    """Push text content to a file inside a unit container."""
    local_path = Path(local_path)

    if not local_path.exists():
        raise FileNotFoundError(local_path)

    # Ensure destination directory exists
    destination_directory = Path(destination_path).parent
    juju.ssh(
        unit,
        f"mkdir -p {destination_directory}",
        container=container,
    )

    # Copy the file
    juju.scp(
        str(local_path),
        f"{unit}:{destination_path}",
        container=container,
    )
