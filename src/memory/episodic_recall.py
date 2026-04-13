"""
episodic_recall.py

Selective episodic lookup over the experience store.
"""

from __future__ import annotations

from src.memory.experience_store import ExperienceStore


class EpisodicRecall:
    def __init__(self, experience_store: ExperienceStore):
        self.experience_store = experience_store

    def find_similar(self, question: str, limit: int = 3) -> list[dict]:
        return self.experience_store.find_similar_experiences(question, limit=limit)
