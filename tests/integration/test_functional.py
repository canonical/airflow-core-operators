"""Integration tests for configuration and relation behavior."""

import shlex
import time

import pytest
import jubilant

from tests.integration.conftest import (
    file_exists,
)
from tests.integration.helpers.airflow_helpers import (
    json_from_airflow,
    read_airflow_config,
    set_coordinator_load_examples,
)
from tests.integration.helpers.constants import (
    AIRFLOW_CONFIG_PATH,
    CONTAINER_NAMES,
    COORDINATOR_APP,
    COORD_REL,
    CORE_CHARMS,
    CORE_APP_BY_COMPONENT,
)
from tests.integration.helpers.juju_helpers import find_component_metadata


@pytest.mark.abort_on_fail
def test_airflow_config_options_present_and_rewritten_on_relation_change(
    juju: jubilant.Juju,
    deployed_stack,
):
    """Airflow config should be removed on relation break and restored on rejoin."""
    target_app = CORE_APP_BY_COMPONENT["scheduler"]
    target_unit = f"{target_app}/0"
    target_container = CONTAINER_NAMES[target_app]

    cfg = read_airflow_config(juju, target_unit, target_container)

    assert cfg.get("core", "dags_folder") == "dags"
    assert cfg.get("core", "executor") == "LocalExecutor"
    assert cfg.get("core", "load_examples") == "False"
    assert cfg.get("database", "sql_alchemy_conn").startswith("postgresql+psycopg2://")
    assert cfg.get("api", "port") == "8080"
    assert cfg.get("logging", "base_log_folder") == "logs"

    juju.cli(
        "remove-relation",
        f"{COORDINATOR_APP}:{COORD_REL}",
        f"{target_app}:{COORD_REL}",
    )

    juju.wait(jubilant.all_agents_idle, timeout=10 * 60)

    assert not file_exists(juju, target_unit, target_container, AIRFLOW_CONFIG_PATH)

    juju.integrate(
        f"{COORDINATOR_APP}:{COORD_REL}",
        f"{target_app}:{COORD_REL}",
    )

    juju.wait(jubilant.all_agents_idle, timeout=20 * 60)

    cfg = read_airflow_config(juju, target_unit, target_container)
    assert cfg.get("core", "executor") == "LocalExecutor"
    assert cfg.get("database", "sql_alchemy_conn").startswith("postgresql+psycopg2://")


@pytest.mark.abort_on_fail
def test_relation_databag_contains_core_metadata(
    juju: jubilant.Juju,
):
    """Each core charm should publish component metadata to the relation databag."""
    juju.wait(jubilant.all_agents_idle, timeout=10 * 60)
    for expected_component, app in CORE_CHARMS:
        matching = find_component_metadata(
            juju,
            f"{app}/0",
            COORD_REL,
            expected_component,
        )

        assert matching is not None, (
            f"Missing component metadata for {expected_component}"
        )


@pytest.mark.abort_on_fail
def test_airflow_cli_stress_dags_list(
    juju: jubilant.Juju,
):
    """Airflow CLI should remain responsive under repeated list calls."""
    juju.wait(jubilant.all_agents_idle, timeout=10 * 60)

    for _ in range(6):
        scheduler_unit = f"{CORE_APP_BY_COMPONENT['scheduler']}/0"
        scheduler_container = CONTAINER_NAMES[CORE_APP_BY_COMPONENT["scheduler"]]
        out = juju.cli(
            "ssh",
            "--container",
            scheduler_container,
            scheduler_unit,
            "bash -lc "
            + shlex.quote("PYTHONWARNINGS=ignore airflow dags list --output json"),
        )
        parsed = json_from_airflow(out)
        assert parsed is not None
        time.sleep(5)


@pytest.mark.abort_on_fail
def test_database_connectivity_from_scheduler(
    juju: jubilant.Juju,
):
    """Exec into the scheduler container and confirm DB connectivity."""
    scheduler_unit = f"{CORE_APP_BY_COMPONENT['scheduler']}/0"
    scheduler_container = CONTAINER_NAMES[CORE_APP_BY_COMPONENT["scheduler"]]

    check_cmd = "airflow db check || echo 'DB check failed'"
    out = juju.cli(
        "ssh",
        "--container",
        scheduler_container,
        scheduler_unit,
        "bash -lc " + shlex.quote(check_cmd),
    )
    assert "DB check failed" not in out, f"Failed to connect to the DB: {out}"


@pytest.mark.abort_on_fail
def test_config_change_propagates_and_dags_reserialize(
    juju: jubilant.Juju,
):
    """Config changes in coordinator should propagate and allow DAG reserialize."""
    coordinator_unit = f"{COORDINATOR_APP}/0"
    set_coordinator_load_examples(juju, coordinator_unit, True)

    juju.wait(jubilant.all_agents_idle, timeout=15 * 60)

    for _, app in CORE_CHARMS.items():
        juju.cli(
            "ssh",
            "--container",
            app.replace("-k8s", ""), 
            f"{app}/0",
            "pebble restart airflow",
        )

    for _, app in CORE_CHARMS.items():
        juju.cli(
            "ssh",
            "--container",
            app.replace("-k8s", ""), 
            f"{app}/0",
            "bash -lc " + shlex.quote("airflow dags reserialize"),
        )

    for _, app in CORE_CHARMS.items():
        cfg = read_airflow_config(juju, f"{app}/0", CONTAINER_NAMES[app])
        assert cfg.get("core", "load_examples") == "True", (
            f"Expected load_examples=True in {app} config"
        )

    scheduler_unit = f"{CORE_APP_BY_COMPONENT['scheduler']}/0"
    scheduler_container = CONTAINER_NAMES[CORE_APP_BY_COMPONENT["scheduler"]]
    out = juju.cli(
        "ssh",
        "--container",
        scheduler_container,
        scheduler_unit,
        "bash -lc " + shlex.quote("PYTHONWARNINGS=ignore airflow dags list --output json"),
    )
    assert isinstance(json_from_airflow(out), list)
