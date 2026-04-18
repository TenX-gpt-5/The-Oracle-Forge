"""
llm_client.py

Thin OpenRouter client for KB-driven query generation and answer synthesis.
Used as an optional dynamic fallback when the deterministic runtime does not
have a specialized solver for a query.
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
    def __init__(self) -> None:
        self.api_key = os.getenv("OPENROUTER_API_KEY", "")
        base_url = os.getenv("OPENROUTER_BASE_URL", "https://openrouter.ai/api/v1").rstrip("/")
        if base_url.endswith("/api"):
            base_url = f"{base_url}/v1"
        self.base_url = base_url
        self.model = os.getenv(
            "OPENROUTER_MODEL",
            os.getenv("LLM_MODEL", "anthropic/claude-sonnet-4-5"),
        )
        self._client = None

    def _get_client(self):
        if self._client is None:
            try:
                from openai import OpenAI  # type: ignore

                self._client = OpenAI(base_url=self.base_url, api_key=self.api_key)
            except ImportError as exc:
                raise RuntimeError("openai package required: pip install openai") from exc
        return self._client

    def complete(self, messages: list[dict[str, str]], temperature: float = 0.0) -> str:
        client = self._get_client()
        response = client.chat.completions.create(
            model=self.model,
            messages=messages,
            temperature=temperature,
        )
        return response.choices[0].message.content or ""

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
            "If the answer is a list of names, return them comma-separated or newline-separated "
            "in the same order as the evidence. If it is a single value, return just that value. "
            "Use only the provided schema and query results; do not invent fields, joins, or entities."
        )
        user_prompt = (
            f"Question: {question}\n\n"
            f"Database descriptions:\n{db_description}\n\n"
            f"Query results:\n{results_str}\n\n"
            "Return only the final answer."
        )

        return self.complete(
            [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ]
        ).strip()

    def build_benchmark_artifact(
        self,
        question: str,
        query_results: dict[str, Any],
        db_description: str,
    ) -> dict[str, Any]:
        results_str = json.dumps(query_results, indent=2, default=str)

        system_prompt = (
            "You are a benchmark answer synthesizer. "
            "Return ONLY valid JSON with these keys: dataset, answer_kind, formatted_answer, numeric_answer, review_count. "
            "Include any extra useful keys when relevant, such as matched_business_count, state_abbr, business_name, categories, top_categories, or repo names. "
            "Do not include markdown or explanation."
        )
        user_prompt = (
            f"Question: {question}\n\n"
            f"Database descriptions:\n{db_description}\n\n"
            f"Query results:\n{results_str}\n\n"
            "Return a JSON object for the benchmark answer artifact."
        )
        response = self.complete(
            [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ]
        ).strip()
        if response.startswith("```"):
            response = response.strip("`")
            if response.lower().startswith("json"):
                response = response[4:].strip()
        try:
            payload = json.loads(response)
        except json.JSONDecodeError:
            return {}
        if not isinstance(payload, dict):
            return {}
        payload.setdefault("dataset", "")
        payload.setdefault("answer_kind", "llm_synthesized")
        payload.setdefault("formatted_answer", "")
        payload.setdefault("numeric_answer", None)
        payload.setdefault("review_count", 0)
        return payload

    def classify_benchmark_rule(
        self,
        question: str,
        dataset: str,
        dataset_schema: dict[str, Any],
        rule_catalog: list[dict[str, Any]],
    ) -> str | None:
        allowed_rule_ids = [
            str(rule.get("rule_id", "")).strip()
            for rule in rule_catalog
            if str(rule.get("rule_id", "")).strip()
        ]
        if not allowed_rule_ids:
            return None

        schema_summary = json.dumps(dataset_schema, indent=2, default=str)
        rules_summary = json.dumps(
            [
                {
                    "rule_id": rule.get("rule_id"),
                    "hint": rule.get("reasoning_hint"),
                    "output": rule.get("output_expectation"),
                }
                for rule in rule_catalog
            ],
            indent=2,
            default=str,
        )
        system_prompt = (
            "You are a benchmark query classifier. "
            "Return ONLY valid JSON with keys rule_id, confidence, and reason. "
            "Choose rule_id from the allowed list or null if none fit. "
            "Prefer the most specific matching rule."
        )
        user_prompt = (
            f"Dataset: {dataset}\n"
            f"Allowed rule IDs: {', '.join(allowed_rule_ids)}\n\n"
            f"Dataset schema:\n{schema_summary}\n\n"
            f"Rule catalog:\n{rules_summary}\n\n"
            f"Question:\n{question}\n\n"
            "Return a JSON object like {\"rule_id\": \"...\", \"confidence\": 0.0, \"reason\": \"...\"}."
        )
        response = self.complete(
            [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ]
        ).strip()
        if response.startswith("```"):
            response = response.strip("`")
            if response.lower().startswith("json"):
                response = response[4:].strip()
        try:
            payload = json.loads(response)
        except json.JSONDecodeError:
            return None
        rule_id = str(payload.get("rule_id", "")).strip()
        if not rule_id or rule_id not in allowed_rule_ids:
            return None
        return rule_id

    def generate_queries(
        self,
        question: str,
        db_description: str,
        schema_context: dict[str, Any],
        db_clients: dict[str, Any],
    ) -> dict[str, str]:
        allowed_dbs = [
            name for name in db_clients.keys()
        ]
        if not allowed_dbs:
            return {}

        schema_summary = json.dumps(schema_context, indent=2, default=str)
        db_summary = json.dumps(db_clients, indent=2, default=str)
        system_prompt = (
            "You are a benchmark query generator. "
            "Return ONLY valid JSON with a queries object mapping each allowed database name "
            "to a single query string for that database. "
            "Use the provided schema and descriptions to infer the right query. "
            "Prefer the smallest correct query for each database. "
            "Do not include explanations, markdown, or extra keys."
        )
        user_prompt = (
            f"Question: {question}\n\n"
            f"Database description: {db_description}\n\n"
            f"Allowed database names: {', '.join(allowed_dbs)}\n\n"
            f"Database clients:\n{db_summary}\n\n"
            f"Schema context:\n{schema_summary}\n\n"
            "Use the schema hints for dataset-specific keys, normalized identifiers, and date or category filters. "
            "Return one query per allowed database that retrieves only the rows needed to answer the question.\n\n"
            "Return JSON like {\"queries\": {\"db_name\": \"SELECT ...\"}}."
        )
        response = self.complete(
            [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ]
        ).strip()
        if response.startswith("```"):
            response = response.strip("`")
            if response.lower().startswith("json"):
                response = response[4:].strip()
        try:
            payload = json.loads(response)
        except json.JSONDecodeError:
            return {}
        queries = payload.get("queries", payload)
        if not isinstance(queries, dict):
            return {}
        normalized: dict[str, str] = {}
        for db_name, query in queries.items():
            db_key = str(db_name).strip()
            if db_key not in allowed_dbs:
                continue
            if isinstance(query, str) and query.strip():
                normalized[db_key] = query.strip()
        return normalized
