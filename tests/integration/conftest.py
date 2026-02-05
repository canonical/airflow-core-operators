"""Pytest fixtures for integration tests."""

from __future__ import annotations

import base64
import os
import shlex
import uuid
import pytest
import jubilant
import re

from tests.integration.helpers.charm_packaging_helpers import (
    coordinator_charm_ref,
    image_resources,
    pack_all_core_charms,
)
from tests.integration.helpers.constants import (
    CORE_CHARMS,
    COORDINATOR_APP,
    COORDINATOR_CHANNEL,
    COORD_REL,
    POSTGRES_APP,
    POSTGRES_CHANNEL,
)


def _new_model_name() -> str:
    """Return a unique model name unless JUJU_MODEL is provided."""
    return os.environ.get("JUJU_MODEL") or f"jubilant-{uuid.uuid4().hex[:8]}"


def unit_name(app: str, n: int = 0) -> str:
    """Return a unit name for a given app and index."""
    return f"{app}/{n}"


def workload_container_for_app(app: str) -> str:
    """Given app like airflow-api-server-k8s -> airflow-api-server"""
    return app[:-4] if app.endswith("-k8s") else app


def ssh(juju: jubilant.Juju, unit: str, container: str, cmd: str) -> str:
    """Run a command in a unit container via Juju SSH."""
    return juju.cli("ssh", "--container", container, unit, cmd)


def ssh_unit(juju: jubilant.Juju, unit: str, cmd: str) -> str:
    """Run `juju ssh <unit> <cmd>` (no --container). Use when the workload
    binary (e.g. curl) is available on the unit's default shell but not in a
    specific container."""
    return juju.cli("ssh", unit, cmd)


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


def ensure_airflow_db_migrated(
    juju: jubilant.Juju,
    app: str,
) -> None:
    """
    Make sure Airflow metadata DB has tables.
    We run this from a core unit (scheduler is usually best).
    """
    unit = unit_name(app)
    container = workload_container_for_app(app)

    cmd = "bash -lc " + shlex.quote("airflow db migrate || airflow db upgrade || true")
    ssh(juju, unit, container, cmd)
    juju.wait(jubilant.all_agents_idle, timeout=30 * 60)


def file_exists(juju: jubilant.Juju, unit: str, container: str, path: str) -> bool:
    """Return True if a file exists in the workload container."""
    q = shlex.quote(path)
    out = ssh(juju, unit, container, f"test -f {q} && echo OK || echo MISSING")
    return "OK" in out


def pebble_services_text(juju: jubilant.Juju, unit: str, container: str) -> str:
    """Return the output of `pebble services` for the container."""
    return ssh(juju, unit, container, "pebble services || true")


def pebble_service_is_running(services_text: str, service: str) -> bool:
    """Return True if a Pebble service is active in the services output."""
    pattern = rf"^{re.escape(service)}\s+enabled\s+active\s+"
    return re.search(pattern, services_text, flags=re.MULTILINE) is not None


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


@pytest.fixture(scope="session")
def juju() -> jubilant.Juju:
    """Return a Jubilant client for the configured model."""
    model = _new_model_name()
    wait_timeout = float(os.environ.get("JUJU_WAIT_TIMEOUT", "600"))
    return jubilant.Juju(
        model=model,
        wait_timeout=wait_timeout,
        cli_binary=os.environ.get("JUJU", "juju"),
    )


@pytest.fixture(scope="session")
def keep_model() -> bool:
    """Return True if the model should be kept after tests."""
    return os.environ.get("KEEP_MODEL", "0") == "1"


@pytest.fixture(scope="session", autouse=True)
def ensure_model_lifecycle(juju: jubilant.Juju, keep_model: bool):
    """Create a test model and optionally destroy it after tests."""
    user_model = os.environ.get("JUJU_MODEL")
    if user_model:
        yield
        return

    juju.add_model(juju.model)
    yield

    if not keep_model:
        try:
            juju.destroy_model(juju.model, destroy_storage=True)
        except TypeError:
            juju.cli(
                "destroy-model",
                juju.model,
                "--no-prompt",
                "--destroy-storage",
                include_model=False,
            )


@pytest.fixture(scope="session")
def deployed_stack(juju: jubilant.Juju):
    """Deploy Postgres, the coordinator, and core charms."""
    core_charm_files = pack_all_core_charms()
    resources_map = image_resources()

    coord_ref = coordinator_charm_ref()

    juju.deploy(POSTGRES_APP, app=POSTGRES_APP, channel=POSTGRES_CHANNEL, trust=True)
    juju.wait(
        ready=lambda st: jubilant.all_active(st, POSTGRES_APP),
        error=jubilant.any_error,
        timeout=30 * 60,
    )

    print("Deploying coordinator...")
    juju.deploy(coord_ref, app=COORDINATOR_APP, channel=COORDINATOR_CHANNEL, trust=True)

    for _, app in CORE_CHARMS:
        charm_path = str(core_charm_files[app])
        resources = resources_map.get(app, {})
        print(f"Deploying core charm {app}...")
        juju.deploy(charm_path, app=app, trust=True, resources=resources)

    print("Integrating coordinator <-> postgres")
    juju.integrate(f"{COORDINATOR_APP}:postgres", f"{POSTGRES_APP}:database")
    return True


@pytest.fixture(scope="session")
def relate_core_charms(juju: jubilant.Juju, deployed_stack: bool):
    """Relate all core charms to the coordinator and wait for readiness."""
    for _, app in CORE_CHARMS:
        try:
            print(f"Integrating coordinator <-> {app}")
            juju.integrate(f"{COORDINATOR_APP}:{COORD_REL}", f"{app}:{COORD_REL}")
        except Exception:
            pass

    core_apps = [app for _, app in CORE_CHARMS]

    juju.wait(
        ready=lambda st: jubilant.all_active(
            st, POSTGRES_APP, COORDINATOR_APP, *core_apps
        ),
        error=jubilant.any_error,
        timeout=15 * 60,
    )
    return True


@pytest.fixture
def file_exists_fn():
    """Fixture returning file existence helper."""
    return file_exists


@pytest.fixture
def pebble_services():
    """Fixture returning pebble services helper."""
    return pebble_services_text


@pytest.fixture
def pebble_running():
    """Fixture returning pebble service status checker."""
    return pebble_service_is_running


@pytest.fixture
def remove_relation():
    """Fixture returning relation removal helper."""
    return remove_relation_if_exists


@pytest.fixture
def integrate_relation():
    """Fixture returning relation integrate helper."""
    return integrate_if_missing


@pytest.fixture
def unit():
    """Fixture returning unit name helper."""
    return unit_name


@pytest.fixture
def container_for():
    """Fixture returning workload container name helper."""
    return workload_container_for_app


@pytest.fixture
def run_in():
    """Fixture returning container command executor."""
    return ssh


@pytest.fixture
def run_in_unit():
    """Fixture returning unit command executor."""
    return ssh_unit


@pytest.fixture
def push_file():
    """Fixture returning file push helper."""
    return push_text_file


@pytest.fixture
def airflow_db_migrated():
    """Fixture returning Airflow DB migration helper."""
    return ensure_airflow_db_migrated
