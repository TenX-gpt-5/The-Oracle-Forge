"""
consolidator.py

Extracts durable memory candidates from raw experiences.
"""

from __future__ import annotations


class Consolidator:
    def consolidate_experiences(self, raw_logs: list[dict]) -> list[dict]:
        candidates: list[dict] = []
        for log in raw_logs:
            validation = log.get("validation", {})
            if log.get("success") and log.get("retries", 0) > 0:
                candidates.append(
                    {
                        "scope": "project",
                        "type": "correction",
                        "confidence": "high",
                        "problem_signature": log["question"][:120],
                        "lesson": "A retry was needed before validation passed; preserve the final fix path.",
                        "failure_class": validation.get("failure_class", "none"),
                    }
                )
            if log.get("success"):
                candidates.append(
                    {
                        "scope": "global",
                        "type": "lesson",
                        "confidence": "medium",
                        "problem_signature": log["question"][:120],
                        "lesson": "Validated answers should be grounded in explicit evidence and trace artifacts.",
                    }
                )
        return candidates
