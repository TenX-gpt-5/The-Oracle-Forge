"""
trace_logger.py

Persists completed traces into the experience store.
"""

from __future__ import annotations

from src.memory.experience_store import ExperienceStore


class TraceLogger:
    def __init__(self, store: ExperienceStore | None = None):
        self.store = store or ExperienceStore()

    def log_trace(self, trace: dict) -> int:
        return self.store.log_experience(trace)
