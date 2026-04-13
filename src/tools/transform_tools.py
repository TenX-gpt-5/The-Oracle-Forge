"""
transform_tools.py

Local transformation helpers for joins, extraction, and aggregation.
"""

from __future__ import annotations

from typing import Any

from src.kb.join_key_store import JoinKeyStore
from src.tools.remote_sandbox import RemoteSandboxClient


def normalize_join_key(entity: str, raw_value: Any) -> str:
    return JoinKeyStore().normalize_value(entity, raw_value)


def run_python_transform(script: str, use_remote: bool = False, cwd: str | None = None) -> dict[str, Any]:
    if use_remote:
        client = RemoteSandboxClient()
        return client.run_python(script, cwd=cwd)
    namespace: dict[str, Any] = {}
    exec(script, namespace, namespace)
    return {"ok": True, "mode": "local", "namespace_keys": sorted(namespace.keys())}


def extract_structured_facts(text: str, schema: dict | None = None) -> dict[str, Any]:
    lowered = text.lower()
    negative_keywords = ("upset", "late", "angry", "issue", "problem", "failed")
    urgency_keywords = ("urgent", "high", "immediately")
    return {
        "negative_sentiment": any(keyword in lowered for keyword in negative_keywords),
        "urgent": any(keyword in lowered for keyword in urgency_keywords),
        "mentions_billing": "billing" in lowered,
        "mentions_delivery": "delivery" in lowered,
    }


def extract_rows_with_facts(rows: list[dict[str, Any]], text_field: str) -> list[dict[str, Any]]:
    extracted_rows = []
    for row in rows:
        facts = extract_structured_facts(str(row.get(text_field, "")))
        enriched = dict(row)
        enriched.update(facts)
        extracted_rows.append(enriched)
    return extracted_rows


def join_on_normalized_key(
    left_rows: list[dict[str, Any]],
    right_rows: list[dict[str, Any]],
    left_key: str,
    right_key: str,
    entity: str,
) -> list[dict[str, Any]]:
    right_lookup: dict[str, list[dict[str, Any]]] = {}
    for row in right_rows:
        normalized = normalize_join_key(entity, row.get(right_key))
        right_lookup.setdefault(normalized, []).append(row)

    joined: list[dict[str, Any]] = []
    for left in left_rows:
        normalized = normalize_join_key(entity, left.get(left_key))
        matches = right_lookup.get(normalized, [])
        if not matches:
            continue
        for right in matches:
            merged = dict(left)
            merged.update(right)
            merged["customer_id"] = normalized
            joined.append(merged)
    return joined


def aggregate_by_field(
    rows: list[dict[str, Any]],
    group_field: str,
    metric_fields: list[str],
) -> list[dict[str, Any]]:
    grouped: dict[str, dict[str, Any]] = {}
    for row in rows:
        key = str(row.get(group_field, "unknown"))
        bucket = grouped.setdefault(key, {group_field: key})
        for metric in metric_fields:
            value = row.get(metric, 0) or 0
            if isinstance(value, (int, float)):
                bucket[metric] = bucket.get(metric, 0) + value
            else:
                bucket[metric] = bucket.get(metric, 0)
    return sorted(grouped.values(), key=lambda item: str(item.get(group_field, "")))
