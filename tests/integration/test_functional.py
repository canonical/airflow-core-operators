"""Integration tests for configuration and relation behavior."""

from __future__ import annotations

import shlex
import time

import pytest
import jubilant

from tests.integration.helpers.airflow_helpers import (
    json_from_airflow,
    read_airflow_config,
)
from tests.integration.helpers.constants import (
    AIRFLOW_CONFIG_PATH,
    COORDINATOR_APP,
    COORD_REL,
    CORE_CHARMS,
    get_core_app,
)
from tests.integration.helpers.juju_helpers import find_component_metadata


@pytest.mark.abort_on_fail
def test_airflow_config_options_present_and_rewritten_on_relation_change(
    juju: jubilant.Juju,
    deployed_stack: bool,
    relate_core_charms: bool,
    remove_relation,
    integrate_relation,
    unit,
    container_for,
    run_in,
):
    """Airflow config should be removed on relation break and restored on rejoin."""
    target_app = get_core_app("scheduler")
    target_unit = unit(target_app)
    target_container = container_for(target_app)

    cfg = read_airflow_config(juju, target_unit, target_container, run_in)

    assert cfg.get("core", "dags_folder") == "dags"
    assert cfg.get("core", "executor") == "LocalExecutor"
    assert cfg.get("core", "load_examples") == "False"
    assert cfg.get("database", "sql_alchemy_conn").startswith("postgresql+psycopg2://")
    assert cfg.get("api", "port") == "8080"
    assert cfg.get("logging", "base_log_folder") == "logs"

    remove_relation(
        juju,
        f"{COORDINATOR_APP}:{COORD_REL}",
        f"{target_app}:{COORD_REL}",
    )

    juju.wait(jubilant.all_agents_idle, timeout=10 * 60)

    missing = run_in(
        juju,
        target_unit,
        target_container,
        "bash -lc "
        + shlex.quote(f"test -f {AIRFLOW_CONFIG_PATH} && echo OK || echo MISSING"),
    )
    assert "MISSING" in missing

    integrate_relation(
        juju,
        f"{COORDINATOR_APP}:{COORD_REL}",
        f"{target_app}:{COORD_REL}",
    )

    juju.wait(jubilant.all_agents_idle, timeout=20 * 60)

    cfg = read_airflow_config(juju, target_unit, target_container, run_in)
    assert cfg.get("core", "executor") == "LocalExecutor"
    assert cfg.get("database", "sql_alchemy_conn").startswith("postgresql+psycopg2://")


@pytest.mark.abort_on_fail
def test_relation_databag_contains_core_metadata(
    juju: jubilant.Juju,
    deployed_stack: bool,
    relate_core_charms: bool,
    unit,
):
    """Each core charm should publish component metadata to the relation databag."""
    for expected_component, app in CORE_CHARMS:
        matching = find_component_metadata(
            juju,
            unit(app),
            COORD_REL,
            expected_component,
        )

        assert matching is not None, (
            f"Missing component metadata for {expected_component}"
        )


@pytest.mark.abort_on_fail
def test_airflow_cli_stress_dags_list(
    juju: jubilant.Juju,
    deployed_stack: bool,
    relate_core_charms: bool,
    run_in,
    unit,
    container_for,
    airflow_db_migrated,
):
    """Airflow CLI should remain responsive under repeated list calls."""
    airflow_db_migrated(juju, get_core_app("scheduler"))

    for _ in range(6):
        out = run_in(
            juju,
            unit(get_core_app("scheduler")),
            container_for(get_core_app("scheduler")),
            "bash -lc "
            + shlex.quote("PYTHONWARNINGS=ignore airflow dags list --output json"),
        )
        parsed = json_from_airflow(out)
        assert parsed is not None
        time.sleep(5)


@pytest.mark.abort_on_fail
def test_database_connectivity_from_scheduler(
    juju: jubilant.Juju,
    deployed_stack: bool,
    relate_core_charms: bool,
    unit,
    container_for,
    run_in_unit,
):
    """Exec into the scheduler container and confirm DB connectivity."""
    # Exec into the scheduler unit's container and run airflow db check (or similar)
    scheduler_unit = unit(get_core_app("scheduler"))
    scheduler_container = container_for(get_core_app("scheduler"))

    check_cmd = "airflow db check || echo 'DB check failed'"
    out = run_in_unit(
        juju, scheduler_unit, scheduler_container, "bash -lc " + shlex.quote(check_cmd)
    )

    assert "DB check failed" not in out, f"Failed to connect to the DB: {out}"
