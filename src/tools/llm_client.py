"""
llm_client.py

Thin OpenRouter client for KB-driven query generation and answer synthesis.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any


def _load_env_files() -> None:
    """Load .env files from known locations if dotenv is unavailable."""
    try:
        from dotenv import load_dotenv  # type: ignore
        repo_root = Path(__file__).resolve().parents[2]
        for candidate in [repo_root / ".env", repo_root / "src" / "tools" / ".env"]:
            if candidate.exists():
                load_dotenv(candidate, override=False)
        return
    except ImportError:
        pass
    # Manual fallback: parse key=value lines
    repo_root = Path(__file__).resolve().parents[2]
    for candidate in [repo_root / ".env", repo_root / "src" / "tools" / ".env"]:
        if not candidate.exists():
            continue
        for line in candidate.read_text().splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, value = line.partition("=")
            key = key.strip()
            value = value.strip().strip('"').strip("'")
            if key and key not in os.environ:
                os.environ[key] = value


_load_env_files()


class LLMClient:
    def __init__(self):
        self.api_key = os.getenv("OPENROUTER_API_KEY", "")
        self.model = os.getenv("LLM_MODEL", "anthropic/claude-sonnet-4-5")
        self.base_url = "https://openrouter.ai/api/v1"
        self._client = None

    def _get_client(self):
        if self._client is None:
            try:
                from openai import OpenAI  # type: ignore
                self._client = OpenAI(base_url=self.base_url, api_key=self.api_key)
            except ImportError:
                raise RuntimeError("openai package required: pip install openai")
        return self._client

    def complete(self, messages: list[dict[str, str]], temperature: float = 0.0) -> str:
        client = self._get_client()
        response = client.chat.completions.create(
            model=self.model,
            messages=messages,
            temperature=temperature,
        )
        return response.choices[0].message.content or ""

    def generate_queries(
        self,
        question: str,
        db_description: str,
        schema_context: dict[str, Any],
        db_clients: dict[str, Any],
    ) -> dict[str, str]:
        schema_str = json.dumps(schema_context, indent=2) if schema_context else ""
        db_names = list(db_clients.keys())

        system_prompt = (
            "You are a data agent. Given a question and database schemas, "
            "return ONLY a JSON object mapping each database name to a SQL query "
            "(or MongoDB pipeline JSON string) that retrieves the data needed. "
            "For SQLite/PostgreSQL use SQL. For MongoDB use a JSON pipeline. "
            "Do not explain. Return only valid JSON."
        )
        user_prompt = (
            f"Question: {question}\n\n"
            f"Database descriptions:\n{db_description}\n\n"
            f"Schema KB:\n{schema_str}\n\n"
            f"Available databases: {db_names}\n\n"
            "Return a JSON object like: "
            '{"books_database": "SELECT ...", "review_database": "SELECT ..."}\n'
            "Only include databases needed to answer the question."
        )

        raw = self.complete([
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ])

        try:
            # Strip markdown code fences if present
            cleaned = raw.strip()
            if cleaned.startswith("```"):
                cleaned = "\n".join(cleaned.split("\n")[1:])
            if cleaned.endswith("```"):
                cleaned = "\n".join(cleaned.split("\n")[:-1])
            return json.loads(cleaned.strip())
        except (json.JSONDecodeError, ValueError):
            return {}

    def synthesize_answer(
        self,
        question: str,
        query_results: dict[str, Any],
        db_description: str,
    ) -> str:
        results_str = json.dumps(query_results, indent=2, default=str)

        system_prompt = (
            "You are a data agent. Given a question and query results from databases, "
            "return ONLY the final answer as a plain string — no explanation, no markdown. "
            "If the answer is a list of book titles, return them comma-separated. "
            "If it is a single value (decade, number, name), return just that value."
        )
        user_prompt = (
            f"Question: {question}\n\n"
            f"Database descriptions:\n{db_description}\n\n"
            f"Query results:\n{results_str}\n\n"
            "Return only the final answer."
        )

        return self.complete([
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ]).strip()
