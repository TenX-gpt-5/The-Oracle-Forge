"""
planner.py

Rule-based planning for Oracle Forge. It intentionally uses deterministic
logic so the architecture is runnable without external model dependencies.
"""

from __future__ import annotations

from typing import Any

from src.kb.schema_index import SchemaIndex


class Planner:
    def __init__(self) -> None:
        self.schema_index = SchemaIndex()

    def generate_plan(
        self,
        user_question: str,
        repair_context: dict[str, Any] | None = None,
        benchmark_context: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        question = user_question.lower()
        repair_context = repair_context or {}
        benchmark_context = benchmark_context or {}
        dataset = str(benchmark_context.get("dataset", "")).lower()
        dataset_schema = self.schema_index.get_schema_for_dataset(dataset) if dataset else {}
        benchmark_db_types = sorted(
            {
                self._normalize_source_type(config.get("db_type", ""))
                for config in benchmark_context.get("db_clients", {}).values()
                if config.get("db_type")
            }
        )
        dataset_source_types = [
            self._normalize_source_type(source.get("db_type", ""))
            for source in dataset_schema.get("sources", {}).values()
            if source.get("db_type")
        ]

        required_sources: list[str] = []
        if dataset_source_types:
            required_sources.extend(dataset_source_types)
        elif any(token in question for token in ("postgres", "order", "orders", "revenue", "user", "users", "purchase")):
            required_sources.append("postgres")
        if any(token in question for token in ("sqlite", "segment", "segments", "cache")):
            required_sources.append("sqlite")
        if any(token in question for token in ("duckdb", "metric", "metrics", "trend", "analytical")):
            required_sources.append("duckdb")
        if any(token in question for token in ("mongo", "mongodb", "ticket", "tickets", "support", "crm", "note", "notes")):
            required_sources.append("mongodb")
        if not required_sources:
            required_sources = benchmark_db_types or ["postgres"]

        entities: list[str] = []
        if any(token in question for token in ("customer", "user", "users")):
            entities.append("customer")
        if any(token in question for token in ("ticket", "support", "crm")):
            entities.append("support_ticket")
        if any(token in question for token in ("lead", "opportunity", "case")):
            entities.append("crm_record")
        if any(token in question for token in ("business", "store", "restaurant", "repo", "repository")):
            entities.append("business")
        if any(token in question for token in ("order", "purchase", "revenue")):
            entities.append("order")
        if any(token in question for token in ("segment", "segments")):
            entities.append("segment")
        if dataset_schema.get("default_entities"):
            entities = list(dict.fromkeys(dataset_schema.get("default_entities", []) + entities))
        if not entities:
            entities = ["dataset"]

        join_keys: list[str] = []
        join_keys.extend(dataset_schema.get("join_keys", []))
        if not dataset_schema.get("join_keys") and (len(required_sources) > 1 or "customer" in entities):
            join_keys.append("customer_id")

        needs_text_extraction = any(
            token in question for token in ("note", "notes", "review", "reviews", "comment", "comments", "sentiment")
        )

        needs_domain_resolution: list[str] = []
        domain_terms = {
            "repeat_purchase_rate": ("repeat purchase", "repeat_purchase", "repeat-purchase"),
            "active_user": ("active user", "active users", "active customer", "active customers"),
            "revenue": ("revenue",),
            "support_ticket_volume": ("support ticket", "ticket volume", "support volume"),
        }
        for term, triggers in domain_terms.items():
            if any(trigger in question for trigger in triggers):
                needs_domain_resolution.append(term)

        if any(token in question for token in ("schema", "table", "tables", "columns")):
            question_type = "schema_discovery"
            expected_output_shape = "schema_listing"
        elif any(token in question for token in ("how many", "count", "number of")):
            question_type = "count_query"
            expected_output_shape = "count_summary"
        elif len(required_sources) > 1:
            question_type = "cross_db_aggregation"
            expected_output_shape = "ranked_segments_plus_explanation"
        else:
            question_type = "single_source_summary"
            expected_output_shape = "tabular_summary"

        if dataset or (
            benchmark_db_types
            and any(
                token in question
                for token in (
                    "average rating",
                    "located in",
                    "highest number of reviews",
                    "business parking",
                    "credit card payments",
                    "offer wifi",
                    "registered on yelp",
                    "business categories",
                )
            )
        ):
            if any(token in question for token in ("how many", "count", "number of")):
                question_type = "count_query"
            else:
                question_type = "cross_db_aggregation" if len(required_sources) > 1 else "single_source_summary"
            expected_output_shape = "benchmark_answer"

        planner_notes: list[str] = []
        if repair_context.get("force_schema_inspection"):
            planner_notes.append("Retry should begin with schema inspection before executing analytical queries.")
        if repair_context.get("prefer_sources"):
            required_sources = repair_context["prefer_sources"]
            planner_notes.append("Using repaired source preference from the previous failed attempt.")
        if repair_context.get("failure_class"):
            planner_notes.append(f"Previous failure class: {repair_context['failure_class']}.")

        return {
            "question_type": question_type,
            "required_sources": list(dict.fromkeys(required_sources)),
            "entities": list(dict.fromkeys(entities)),
            "join_keys": list(dict.fromkeys(join_keys)),
            "needs_text_extraction": needs_text_extraction,
            "needs_domain_resolution": needs_domain_resolution,
            "expected_output_shape": expected_output_shape,
            "planner_notes": planner_notes,
        }

    def _normalize_source_type(self, db_type: str) -> str:
        if db_type == "mongo":
            return "mongodb"
        return db_type
