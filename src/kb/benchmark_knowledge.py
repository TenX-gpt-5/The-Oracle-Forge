"""
benchmark_knowledge.py

Loads benchmark-specific, KB-backed routing rules so the execution router
does not own dataset answer logic directly.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any


class BenchmarkKnowledge:
    def __init__(self):
        root = Path(__file__).resolve().parents[2]
        self.rules_dir = root / "kb" / "domain"
        self.data = self._load()

    def _load(self) -> dict[str, Any]:
        datasets: dict[str, list[dict[str, Any]]] = {}
        for path in sorted(self.rules_dir.glob("*_benchmark_rules.json")):
            payload = json.loads(path.read_text())
            dataset = str(payload.get("dataset", "")).lower()
            if not dataset:
                continue
            datasets.setdefault(dataset, []).extend(payload.get("rules", []))
        return {"datasets": datasets}

    def match(self, dataset: str, question_lower: str) -> dict[str, Any] | None:
        dataset_rules = self.data.get("datasets", {}).get(dataset.lower(), [])
        if not dataset_rules:
            return None
        for rule in dataset_rules:
            required_all = rule.get("question_contains_all", [])
            required_any = rule.get("question_contains_any", [])
            if any(token.lower() not in question_lower for token in required_all):
                continue
            if required_any and not any(token.lower() in question_lower for token in required_any):
                continue
            return rule
        return None
