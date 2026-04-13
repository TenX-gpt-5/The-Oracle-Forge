"""
global_memory.py

Persistent global rules and reusable lessons for Oracle Forge.
"""

from __future__ import annotations

import json
from pathlib import Path


class GlobalMemory:
    def __init__(self):
        root = Path(__file__).resolve().parents[2]
        self.path = root / ".oracle_forge" / "global_memory.json"
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.data = self._load()

    def _load(self) -> dict:
        if self.path.exists():
            return json.loads(self.path.read_text())
        data = {
            "agent_rules": [
                "Prefer schema inspection before guessing table names.",
                "Use explicit normalization before cross-database joins.",
                "Validate evidence before answering the user.",
                "Store only reviewed lessons in durable memory.",
            ],
            "lessons": [],
        }
        self.path.write_text(json.dumps(data, indent=2))
        return data

    def _save(self) -> None:
        self.path.write_text(json.dumps(self.data, indent=2))

    def get_agent_rules(self) -> dict:
        return {
            "agent_rules": list(self.data.get("agent_rules", [])),
            "lessons": list(self.data.get("lessons", [])),
        }

    def add_lesson(self, lesson: dict) -> None:
        self.data.setdefault("lessons", []).append(lesson)
        self._save()
