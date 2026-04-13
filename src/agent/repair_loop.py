"""
repair_loop.py

Classifies failures and prepares targeted retry instructions.
"""

from __future__ import annotations


class RepairLoop:
    def handle_failure(self, state_snapshot: dict) -> dict:
        validation = state_snapshot.get("validation", {})
        failure_class = validation.get("failure_class", "unknown_failure")
        retry_count = state_snapshot.get("retries", 0) + 1

        payload = {
            "is_retry": True,
            "retry_count": retry_count,
            "failure_class": failure_class,
            "diagnostics": "; ".join(validation.get("errors", ["Unknown validation issue"])),
        }

        if failure_class in {"schema_missing", "routing_failure", "execution_failure"}:
            payload["force_schema_inspection"] = True
        if failure_class == "join_or_aggregation_failure":
            payload["prefer_sources"] = ["postgres", "sqlite", "mongodb"]
        if failure_class == "extraction_failure":
            payload["force_text_extraction"] = True
        if failure_class == "benchmark_external_validation_failed":
            diagnostics = payload["diagnostics"].lower()
            if "missing category" in diagnostics:
                payload["force_text_extraction"] = True
            if "number" in diagnostics:
                payload["enforce_numeric_answer_shape"] = True

        return payload
