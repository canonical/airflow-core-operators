from __future__ import annotations

import os
import subprocess
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]

CORE_CHARMS = [
    ("api-server", "airflow-api-server-k8s"),
    ("dag-processor", "airflow-dag-processor-k8s"),
    ("scheduler", "airflow-scheduler-k8s"),
    ("triggerer", "airflow-triggerer-k8s"),
]

POSTGRES_APP = "postgresql-k8s"
COORDINATOR_APP = "airflow-coordinator-k8s"

POSTGRES_CHANNEL = os.environ.get("POSTGRES_CHANNEL", "14/stable")
COORDINATOR_CHANNEL = os.environ.get("COORDINATOR_CHANNEL", "3.1/edge")

COORD_REL = "airflow-coordinator"

def coordinator_charm_ref() -> str:
    return "ch:airflow-coordinator-k8s"

def charm_dir(name: str) -> Path:
    """Path to charm directory in repo /charms folder."""
    return REPO_ROOT / "charms" / name

def pack_charm(charm_path: Path) -> Path:
    print(f"--- Packing charm in {charm_path} ---")
    subprocess.run(["charmcraft", "pack"], cwd=str(charm_path), check=True)
    charms = sorted(charm_path.glob("*.charm"), key=lambda p: p.stat().st_mtime, reverse=True)
    if not charms:
        raise FileNotFoundError(f"No .charm produced in {charm_path}")
    return charms[0]

def pack_all_core_charms() -> dict[str, Path]:
    return {app: pack_charm(charm_dir(dir_name)) for dir_name, app in CORE_CHARMS}

def image_resources() -> dict[str, dict[str, str]]:
    tag = os.environ.get("AIRFLOW_IMAGE_TAG", "3.1-24.04_edge")
    base = os.environ.get("AIRFLOW_IMAGE_BASE", "ubuntu/airflow")

    return {
        "airflow-api-server-k8s": {"airflow-api-server-image": f"{base}:{tag}"},
        "airflow-dag-processor-k8s": {"airflow-dag-processor-image": f"{base}:{tag}"},
        "airflow-scheduler-k8s": {"airflow-scheduler-image": f"{base}:{tag}"},
        "airflow-triggerer-k8s": {"airflow-triggerer-image": f"{base}:{tag}"},
    }
