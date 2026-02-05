"""Charm packaging helpers for integration tests."""

from __future__ import annotations

import os
import subprocess
from pathlib import Path

from tests.integration.helpers.constants import (
    CORE_CHARMS,
    REPO_ROOT,
)


def coordinator_charm_ref() -> str:
    """Return the charmhub reference for the coordinator."""
    return "ch:airflow-coordinator-k8s"


def charm_dir(name: str) -> Path:
    """Path to charm directory in repo /charms folder."""
    return REPO_ROOT / "charms" / name


def pack_charm(charm_path: Path) -> Path:
    """Pack a charmcraft project and return the resulting .charm file path."""
    print(f"--- Packing charm in {charm_path} ---")
    subprocess.run(["charmcraft", "pack"], cwd=str(charm_path), check=True)
    charms = sorted(
        charm_path.glob("*.charm"), key=lambda p: p.stat().st_mtime, reverse=True
    )
    if not charms:
        raise FileNotFoundError(f"No .charm produced in {charm_path}")
    return charms[0]


def pack_all_core_charms() -> dict[str, Path]:
    """Pack all core charms and return a map of app name to charm path."""
    return {app: pack_charm(charm_dir(dir_name)) for dir_name, app in CORE_CHARMS}


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
