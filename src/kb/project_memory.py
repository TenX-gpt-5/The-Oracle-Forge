"""
project_memory.py

Persistent project memory containing domain definitions and validated fixes.
"""

from __future__ import annotations

import json
from pathlib import Path


class ProjectMemory:
    def __init__(self):
        root = Path(__file__).resolve().parents[2]
        self.path = root / ".oracle_forge" / "project_memory.json"
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.data = self._load()

    def _load(self) -> dict:
        if self.path.exists():
            return json.loads(self.path.read_text())
        data = {
            "domain_definitions": {
                "repeat_purchase_rate": "Percentage of customers with two or more completed orders.",
                "active_user": "A user who logged in within the last 30 days unless a query overrides the definition.",
                "revenue": "Sum of completed order amounts from the authoritative transactions table.",
                "support_ticket_volume": "Count of support tickets per entity over the selected time window.",
            },
            "corrections_log": [],
        }
        self.path.write_text(json.dumps(data, indent=2))
        return data

    def _save(self) -> None:
        self.path.write_text(json.dumps(self.data, indent=2))

    def get_domain_definitions(self, terms: list[str] | None = None) -> dict:
        domain_definitions = self.data.get("domain_definitions", {})
        if not terms:
            return dict(domain_definitions)
        return {term: domain_definitions[term] for term in terms if term in domain_definitions}

    def get_corrections(self, entities: list[str] | None = None) -> list[dict]:
        corrections = list(self.data.get("corrections_log", []))
        if not entities:
            return corrections
        return [
            correction
            for correction in corrections
            if any(entity in correction.get("problem_signature", "") for entity in entities)
        ]

    def add_correction(self, correction: dict) -> None:
        self.data.setdefault("corrections_log", []).append(correction)
        self._save()
