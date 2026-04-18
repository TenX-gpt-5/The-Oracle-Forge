"""
context_cortex.py

Selects the minimum useful context for a given turn.
"""

from __future__ import annotations

from typing import Any

from src.kb.domain_store import DomainStore
from src.kb.global_memory import GlobalMemory
from src.kb.join_key_store import JoinKeyStore
from src.kb.project_memory import ProjectMemory
from src.kb.schema_index import SchemaIndex
from src.kb.text_inventory import TextInventory
from src.memory.episodic_recall import EpisodicRecall
from src.memory.experience_store import ExperienceStore


class ContextCortex:
    def __init__(
        self,
        global_memory: GlobalMemory | None = None,
        project_memory: ProjectMemory | None = None,
        experience_store: ExperienceStore | None = None,
    ):
        self.global_memory = global_memory or GlobalMemory()
        self.project_memory = project_memory or ProjectMemory()
        self.experience_store = experience_store or ExperienceStore()
        self.schema_index = SchemaIndex()
        self.join_key_store = JoinKeyStore()
        self.domain_store = DomainStore(self.project_memory)
        self.text_inventory = TextInventory()
        self.episodic_recall = EpisodicRecall(self.experience_store)

    def retrieve_context(
        self,
        question: str,
        plan: dict[str, Any],
        repair_context: dict[str, Any] | None = None,
        benchmark_context: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        repair_context = repair_context or {}
        benchmark_context = benchmark_context or {}
        entities = plan.get("entities", [])
        required_sources = plan.get("required_sources", [])
        domain_terms = plan.get("needs_domain_resolution", [])
        dataset = benchmark_context.get("dataset", "")
        dataset_schema = self.schema_index.get_schema_for_dataset(dataset) if dataset else {}

        if dataset_schema.get("sources"):
            schema_context = dataset_schema["sources"]
        else:
            schema_context = {source: self.schema_index.get_schema_for_db(source) for source in required_sources}
        join_context = {
            entity: self.join_key_store.get_normalization_method(entity)
            for entity in entities
            if entity == "customer"
        }
        text_context = self.text_inventory.find_relevant_fields(
            required_sources=required_sources,
            entities=entities,
            needs_text_extraction=plan.get("needs_text_extraction", False),
        )
        domain_context = self.domain_store.resolve_terms(domain_terms)
        corrections = self.project_memory.get_corrections(entities)
        episodic_hits = self.episodic_recall.find_similar(question, limit=3)

        return {
            "global_rules": self.global_memory.get_agent_rules(),
            "project_memory": {
                "domain_definitions": self.project_memory.get_domain_definitions(domain_terms),
                "corrections": corrections,
            },
            "dataset_schema": dataset_schema,
            "schemas": schema_context,
            "join_key_intelligence": join_context,
            "text_field_inventory": text_context,
            "domain_rules": domain_context,
            "episodic_recall": episodic_hits,
            "repair_context": repair_context,
            "benchmark_context": benchmark_context,
        }
