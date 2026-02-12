"""Juju CLI and relation helpers for integration tests."""

from typing import Any, Iterable

import json
import jubilant


def _as_mapping(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if hasattr(value, "__dict__"):
        return value.__dict__
    return {}


def _iter_relation_entries(relations: Any) -> Iterable[Any]:
    if isinstance(relations, dict):
        for entry in relations.values():
            if isinstance(entry, list):
                yield from entry
            else:
                yield entry
    elif isinstance(relations, list):
        yield from relations


def _extract_relation_data(relation_entry: Any) -> list[dict[str, Any]]:
    data: list[dict[str, Any]] = []
    relation_map = _as_mapping(relation_entry)

    for key in ["application_data", "application-data"]:
        value = relation_map.get(key)
        if isinstance(value, dict):
            data.append(value)

    related_units = relation_map.get("related_units") or relation_map.get("related-units") or {}
    related_units_map = _as_mapping(related_units)
    for unit_data in related_units_map.values():
        unit_map = _as_mapping(unit_data)
        for key in ["application_data", "application-data", "relation_data", "relation-data", "data"]:
            value = unit_map.get(key)
            if isinstance(value, dict):
                data.append(value)

    return data


def find_component_metadata(
    juju: jubilant.Juju,
    unit: str,
    endpoint: str,
    expected_component: str,
) -> dict[str, Any] | None:
    """Find component metadata for a relation in the unit relation data."""
    status = juju.status()
    app = unit.split("/")[0]
    app_status = status.apps.get(app)
    if not app_status:
        return None

    relations = getattr(app_status, "relations", {})
    relation_entries = list(_iter_relation_entries(relations.get(endpoint, [])))

    if not relation_entries:
        units = getattr(app_status, "units", {})
        unit_status = units.get(unit) or units.get(unit.split("/")[-1])
        if unit_status is not None:
            unit_relations = getattr(unit_status, "relations", {})
            relation_entries = list(_iter_relation_entries(unit_relations.get(endpoint, [])))

    for entry in relation_entries:
        for data in _extract_relation_data(entry):
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
