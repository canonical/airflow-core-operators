"""Juju CLI and relation helpers for integration tests."""

from __future__ import annotations

from typing import Any

import json
import jubilant
import yaml


def show_unit_yaml(juju: jubilant.Juju, unit: str) -> dict[str, Any]:
    """Return YAML output from `juju show-unit` parsed into a dict."""
    raw = juju.cli("show-unit", unit, "--format", "yaml")
    return yaml.safe_load(raw) or {}


def relation_info_for_endpoint(
    unit_yaml: dict[str, Any],
    endpoint: str,
) -> dict[str, Any] | None:
    """Return relation-info entry for a given endpoint if present."""
    if not unit_yaml:
        return None

    unit_data = next(iter(unit_yaml.values()), {})
    for info in unit_data.get("relation-info", []):
        if info.get("endpoint") == endpoint:
            return info
    return None


def extract_relation_data(relation_info: dict[str, Any]) -> list[dict[str, Any]]:
    """Collect application and unit relation data dicts from relation-info."""
    data: list[dict[str, Any]] = []
    if not relation_info:
        return data

    if isinstance(relation_info.get("application-data"), dict):
        data.append(relation_info["application-data"])

    for unit_data in (relation_info.get("related-units") or {}).values():
        for key in ["application-data", "relation-data", "data"]:
            if isinstance(unit_data.get(key), dict):
                data.append(unit_data[key])

    return data


def find_component_metadata(
    juju: jubilant.Juju,
    unit: str,
    endpoint: str,
    expected_component: str,
) -> dict[str, Any] | None:
    """Find component metadata for a relation in the unit relation data."""
    unit_yaml = show_unit_yaml(juju, unit)
    relation_info = relation_info_for_endpoint(unit_yaml, endpoint)
    if not relation_info:
        return None

    relation_data = extract_relation_data(relation_info)

    for data in relation_data:
        if data.get("component") == expected_component:
            return data

        data_blob = data.get("data")
        if isinstance(data_blob, str):
            try:
                parsed = json.loads(data_blob)
            except json.JSONDecodeError:
                continue
            component = parsed.get("fixed_request_id", {}).get("component")
            if component == expected_component:
                return data

    return None
