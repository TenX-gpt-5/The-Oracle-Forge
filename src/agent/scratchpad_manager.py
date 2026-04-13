"""
scratchpad_manager.py

Creates bounded scratchpads that help decompose work without letting
intermediate tasks answer the user directly.
"""

from __future__ import annotations


class ScratchpadManager:
    def create_scratchpads(
        self,
        question: str,
        plan: dict,
        context: dict,
    ) -> list[dict]:
        scratchpads: list[dict] = []

        if plan.get("question_type") == "schema_discovery":
            scratchpads.append(
                {
                    "name": "schema-exploration",
                    "goal": "Inspect available tables or collections before analytical execution.",
                    "constraints": {"read_only": True},
                    "tool_hints": ["inspect_schema", "inspect_sample_values"],
                }
            )

        for source in plan.get("required_sources", []):
            scratchpads.append(
                {
                    "name": f"source-{source}",
                    "goal": f"Retrieve source-specific evidence from {source}.",
                    "constraints": {"source": source, "read_only": True},
                    "tool_hints": [f"run_sql_{source}" if source != "mongodb" else "run_mongo_pipeline"],
                }
            )

        if len(plan.get("required_sources", [])) > 1 and plan.get("join_keys"):
            scratchpads.append(
                {
                    "name": "join-normalization",
                    "goal": "Normalize cross-database join keys before merging results.",
                    "constraints": {"entity": plan["entities"][0] if plan.get("entities") else "customer"},
                    "tool_hints": ["normalize_join_key", "run_python_transform"],
                }
            )

        if plan.get("needs_text_extraction"):
            scratchpads.append(
                {
                    "name": "text-extraction",
                    "goal": "Extract structured facts from free text fields before aggregation.",
                    "constraints": {"read_only": True},
                    "tool_hints": ["extract_structured_facts"],
                }
            )

        return scratchpads
