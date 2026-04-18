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
            failure_class = validation.get("failure_class", "none")

            # Persist retry-resolved wins as project-level correction recipes.
            if log.get("success") and log.get("retries", 0) > 0:
                candidates.append(
                    {
                        "scope": "project",
                        "type": "correction",
                        "confidence": "high",
                        "problem_signature": log["question"][:120],
                        "lesson": "A retry was needed before validation passed; preserve the final fix path.",
                        "failure_class": failure_class,
                    }
                )

            # Persist categorized failures so the project memory can guide future runs
            # even when no successful retry happened in the same turn.
            if not log.get("success") and failure_class not in {"none", "unknown_failure"}:
                candidates.append(
                    {
                        "scope": "project",
                        "type": "correction",
                        "confidence": "high",
                        "problem_signature": log["question"][:120],
                        "lesson": (
                            "Encountered a classified failure. Reuse this signature and "
                            "apply targeted mitigation before synthesis."
                        ),
                        "failure_class": failure_class,
                        "validation_errors": validation.get("errors", []),
                    }
                )

            if log.get("success"):
                candidates.append(
                    {
                        "scope": "global",
                        "type": "lesson",
                        "confidence": "medium",
                        "problem_signature": log["question"][:120],
                        "lesson": (
                            "Validated answers should be grounded in explicit evidence and trace artifacts; "
                            "never inspect validators, ground-truth files, or prior solved artifacts as an input to solving."
                        ),
                    }
                )
        return candidates
