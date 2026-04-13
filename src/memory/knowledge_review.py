"""
knowledge_review.py

Applies simple promotion rules before durable memory is updated.
"""

from __future__ import annotations

from src.kb.global_memory import GlobalMemory
from src.kb.project_memory import ProjectMemory


class KnowledgeReview:
    def __init__(self, global_memory: GlobalMemory, project_memory: ProjectMemory):
        self.global_memory = global_memory
        self.project_memory = project_memory

    def review_and_promote(self, candidate_memories: list[dict]) -> list[dict]:
        promoted: list[dict] = []
        for candidate in candidate_memories:
            if candidate.get("scope") == "project" and candidate.get("confidence") == "high":
                self.project_memory.add_correction(candidate)
                promoted.append(candidate)
            elif candidate.get("scope") == "global":
                self.global_memory.add_lesson(candidate)
                promoted.append(candidate)
        return promoted
