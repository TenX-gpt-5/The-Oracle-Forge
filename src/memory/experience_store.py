"""
experience_store.py

Persistent episodic store for traces and outcomes.
"""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path


class ExperienceStore:
    def __init__(self, db_path: str | None = None):
        root = Path.home() / ".oracle_forge"
        self.db_path = str(root / "oracle_forge_experiences.db") if db_path is None else db_path
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    def _init_db(self) -> None:
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS experiences (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                    question TEXT,
                    plan_json TEXT,
                    retrieved_context_json TEXT,
                    tool_calls_json TEXT,
                    trace_json TEXT,
                    validation_json TEXT,
                    final_answer TEXT,
                    retries INTEGER,
                    success BOOLEAN
                )
                """
            )
            conn.commit()

    def log_experience(self, turn_data: dict) -> int:
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(
                """
                INSERT INTO experiences (
                    question,
                    plan_json,
                    retrieved_context_json,
                    tool_calls_json,
                    trace_json,
                    validation_json,
                    final_answer,
                    retries,
                    success
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    turn_data.get("question", ""),
                    json.dumps(turn_data.get("plan", {})),
                    json.dumps(turn_data.get("retrieved_context", {})),
                    json.dumps(turn_data.get("tool_calls", [])),
                    json.dumps(turn_data.get("trace", [])),
                    json.dumps(turn_data.get("validation", {})),
                    turn_data.get("final_answer", ""),
                    int(turn_data.get("retries", 0)),
                    bool(turn_data.get("success", True)),
                ),
            )
            conn.commit()
            return int(cursor.lastrowid)

    def find_similar_experiences(self, question: str, limit: int = 3) -> list[dict]:
        query_terms = {term for term in question.lower().split() if len(term) > 3}
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                """
                SELECT id, question, plan_json, validation_json, final_answer, retries, success
                FROM experiences
                ORDER BY id DESC
                LIMIT 50
                """
            ).fetchall()

        scored: list[tuple[int, dict]] = []
        for row in rows:
            candidate_terms = {term for term in row["question"].lower().split() if len(term) > 3}
            overlap = len(query_terms.intersection(candidate_terms))
            if overlap == 0:
                continue
            scored.append(
                (
                    overlap,
                    {
                        "id": row["id"],
                        "question": row["question"],
                        "plan": json.loads(row["plan_json"] or "{}"),
                        "validation": json.loads(row["validation_json"] or "{}"),
                        "final_answer": row["final_answer"],
                        "retries": row["retries"],
                        "success": bool(row["success"]),
                    },
                )
            )
        return [item for _, item in sorted(scored, key=lambda pair: pair[0], reverse=True)[:limit]]
