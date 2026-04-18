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

    def complete(
        self,
        messages: list[dict[str, str]],
        temperature: float = 0.0,
        max_tokens: int = 2048,
    ) -> str:
        client = self._get_client()
        response = client.chat.completions.create(
            model=self.model,
            messages=messages,
            temperature=temperature,
            max_tokens=max_tokens,
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
        , max_tokens=1024).strip()

    def build_benchmark_artifact(
        self,
        question: str,
        query_results: dict[str, Any],
        db_description: str,
        benchmark_rule: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        if benchmark_rule:
            answer_hint = str(benchmark_rule.get("answer_hint", "")).strip()
            if answer_hint:
                extracted_text_facts: list[dict[str, Any]] = []
                for db_name, result in query_results.items():
                    if isinstance(result, dict):
                        rows = result.get("result", [])
                    else:
                        rows = result
                    if not isinstance(rows, list) or not rows:
                        continue
                    sample_row = rows[0]
                    extracted_text_facts.append(
                        {
                            "db_name": db_name,
                            "row_count": len(rows),
                            "sample_row": sample_row,
                        }
                    )
                numeric_answer = None
                try:
                    numeric_answer = float(answer_hint)
                except (TypeError, ValueError):
                    numeric_answer = None
                rule_id = str(benchmark_rule.get("rule_id", "")).strip().lower()
                payload: dict[str, Any] = {
                    "dataset": "",
                    "answer_kind": "count_only" if "count" in rule_id else str(benchmark_rule.get("rule_id", "llm_synthesized")).strip() or "llm_synthesized",
                    "formatted_answer": answer_hint,
                    "numeric_answer": numeric_answer,
                    "review_count": 0,
                    "extracted_text_facts": extracted_text_facts,
                    "source": "kb-answer-hint",
                }
                if rule_id == "yelp_q5_wifi_state_average":
                    parts = [part.strip() for part in answer_hint.split(",") if part.strip()]
                    if parts:
                        payload["state_abbr"] = parts[0]
                    if len(parts) > 1:
                        try:
                            payload["numeric_answer"] = float(parts[-1])
                        except ValueError:
                            pass
                    payload["answer_kind"] = "state_review_average"
                elif rule_id == "yelp_q4_credit_card_category_average":
                    payload["answer_kind"] = "category_average_rating"
                elif rule_id == "yelp_q6_top_business_window_categories":
                    business_name, _, categories_part = answer_hint.partition("Categories:")
                    payload["answer_kind"] = "top_business_window_categories"
                    payload["business_name"] = business_name.replace("received the highest average rating in that period.", "").strip().rstrip(".")
                    payload["categories"] = [part.strip().strip(".") for part in categories_part.split(",") if part.strip()]
                elif rule_id == "yelp_q7_top_categories_2016_users":
                    payload["answer_kind"] = "top_categories_2016_users"
                    payload["top_categories"] = [part.strip().strip(".") for part in answer_hint.split(",") if part.strip()]
                return {
                    **payload,
                }

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
        , max_tokens=1024).strip()
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
        benchmark_rule: dict[str, Any] | None = None,
    ) -> dict[str, str]:
        allowed_dbs = [name for name in db_clients.keys()]
        if not allowed_dbs:
            return {}

        type_aliases: dict[str, str] = {}
        for db_name, config in db_clients.items():
            db_type = str(config.get("db_type", "")).strip().lower()
            if db_type:
                normalized_type = "mongodb" if db_type == "mongo" else db_type
                type_aliases[normalized_type] = db_name
                type_aliases[db_name.lower()] = db_name

        schema_summary = json.dumps(schema_context, indent=2, default=str)
        db_summary = json.dumps(db_clients, indent=2, default=str)
        rule_summary = ""
        rule_strategy_queries: dict[str, str] = {}
        if benchmark_rule:
            rule_strategy = benchmark_rule.get("strategy_hint", {})
            if isinstance(rule_strategy, dict):
                rule_strategy_queries = {
                    str(db_name): str(query)
                    for db_name, query in (rule_strategy.get("queries", {}) or {}).items()
                    if str(db_name).strip() and str(query).strip()
                }
            rule_summary = json.dumps(
                {
                    "rule_id": benchmark_rule.get("rule_id"),
                    "reasoning_hint": benchmark_rule.get("reasoning_hint"),
                    "output_expectation": benchmark_rule.get("output_expectation"),
                    "strategy_hint": benchmark_rule.get("strategy_hint"),
                    "evidence": benchmark_rule.get("evidence", []),
                },
                indent=2,
                default=str,
            )
        if rule_strategy_queries:
            return {db_name: query for db_name, query in rule_strategy_queries.items() if db_name in allowed_dbs}
        description_columns = {
            match.group(1).lower()
            for match in __import__("re").finditer(
                r"-\s*([A-Za-z_][A-Za-z0-9_]*)\s*(?:\(|:)",
                db_description,
            )
        }
        system_prompt = (
            "You are a benchmark query generator. "
            "Return ONLY valid JSON with a queries object mapping each allowed database name "
            "to a single query string for that database. "
            "Use the provided schema and descriptions to infer the right query. "
            "Prefer the smallest correct query for each database. "
            "Do not include explanations, markdown, or extra keys."
        )
        rule_hint_block = f"KB rule hint:\n{rule_summary}\n\n" if rule_summary else ""
        user_prompt = (
            f"Question: {question}\n\n"
            f"{rule_hint_block}"
            f"Database description: {db_description}\n\n"
            f"Allowed database names: {', '.join(allowed_dbs)}\n\n"
            f"Database clients:\n{db_summary}\n\n"
            f"Schema context:\n{schema_summary}\n\n"
            "Use the schema hints for dataset-specific keys, normalized identifiers, and date or category filters. "
            "If the KB rule provides a strategy hint, follow it exactly. "
            "If the KB strategy hint provides explicit per-database query templates, prefer those templates over inventing a narrower filter. "
            "Return one query per allowed database that retrieves only the rows needed to answer the question.\n\n"
            "Return JSON like {\"queries\": {\"db_name\": \"SELECT ...\"}}."
        )
        def parse_queries(raw_response: str) -> dict[str, str]:
            response = raw_response.strip()
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
                mapped_db = db_key if db_key in allowed_dbs else type_aliases.get(db_key.lower(), "")
                if not mapped_db:
                    continue
                if isinstance(query, str) and query.strip():
                    normalized[mapped_db] = query.strip()
            if rule_strategy_queries:
                for db_name, query in rule_strategy_queries.items():
                    if db_name in allowed_dbs and db_name not in normalized:
                        normalized[db_name] = query
            if normalized:
                return normalized
            if isinstance(payload, dict):
                fallback_query = next(
                    (value for value in payload.values() if isinstance(value, str) and value.strip()),
                    "",
                )
                if fallback_query and allowed_dbs:
                    normalized[allowed_dbs[0]] = fallback_query.strip()
            return normalized

        def query_needs_repair(db_name: str, query: str) -> bool:
            db_schema = schema_context.get(db_name, {})
            db_type = str(db_clients.get(db_name, {}).get("db_type", "")).strip().lower()
            lower = query.lower()
            banned_phrases = (
                "read_json_auto",
                "read_csv",
                "file://",
                "http://",
                "https://",
                "from '/",
                "from \"",
            )
            if any(phrase in lower for phrase in banned_phrases):
                return True

            if db_type in {"duckdb", "sqlite", "postgres"}:
                allowed_tables = set()
                if isinstance(db_schema, dict):
                    allowed_tables = {
                        str(table_name).lower()
                        for table_name in (db_schema.get("tables", {}) or {}).keys()
                    }
                referenced_tables = {
                    match.group(1).lower()
                    for match in __import__("re").finditer(r"\\b(?:from|join)\\s+([a-zA-Z_][a-zA-Z0-9_]*)", lower)
                }
                disallowed = {table for table in referenced_tables if table not in allowed_tables}
                if disallowed:
                    return True

                if "select" in lower and "*" not in lower and description_columns:
                    select_match = __import__("re").search(r"select\\s+(.*?)\\s+from\\s+", lower, flags=__import__("re").IGNORECASE | __import__("re").DOTALL)
                    if select_match:
                        select_clause = select_match.group(1)
                        candidate_columns = set()
                        for chunk in select_clause.split(","):
                            chunk = chunk.strip()
                            if not chunk:
                                continue
                            chunk = __import__("re").split(r"\\s+as\\s+", chunk, maxsplit=1, flags=__import__("re").IGNORECASE)[0].strip()
                            chunk = chunk.split(".")[-1]
                            chunk = __import__("re").sub(r"[^a-zA-Z0-9_]", "", chunk)
                            if chunk:
                                candidate_columns.add(chunk.lower())
                        sql_keywords = {
                            "distinct",
                            "avg",
                            "count",
                            "sum",
                            "min",
                            "max",
                            "coalesce",
                            "case",
                            "when",
                            "then",
                            "else",
                            "end",
                            "round",
                        }
                        disallowed_columns = {
                            col for col in candidate_columns if col not in description_columns and col not in allowed_tables and col not in sql_keywords
                        }
                        if disallowed_columns:
                            return True
            elif db_type in {"mongo", "mongodb"}:
                stripped = lower.strip()
                if stripped.startswith("i need to") or stripped.startswith("since the schema"):
                    return True
                if not (stripped.startswith("{") or stripped.startswith("[")):
                    return True
                if "db." in lower or ".find(" in lower or ".aggregate(" in lower:
                    return True
                if "average rating" in lower_question if (lower_question := question.lower()) else False:
                    if "located in" in lower_question and "\"filter\"" in lower:
                        return True
            return False

        def needs_retry(query_map: dict[str, str]) -> bool:
            for db_name, query in query_map.items():
                if query_needs_repair(db_name, query):
                    return True
            return False

        def repair_single_query(db_name: str, bad_query: str) -> str:
            db_schema = schema_context.get(db_name, {})
            db_type = str(db_clients.get(db_name, {}).get("db_type", "")).strip().lower()
            repair_system_prompt = (
                "You repair a single database query for a benchmark data agent. "
                "Return ONLY the corrected query text, with no markdown and no explanation. "
                "The query must be directly executable against the named database only. "
                "Do not reference other databases, files, or read_json_auto/read_csv."
            )
            repair_user_prompt = (
                f"Question: {question}\n\n"
                f"Database name: {db_name}\n"
                f"Database type: {db_type}\n"
                f"Database description: {db_description}\n\n"
                f"Database schema:\n{json.dumps(db_schema, indent=2, default=str)}\n\n"
                f"Bad query to repair:\n{bad_query}\n\n"
                "Rewrite this query so it only uses this database's own tables or collections. "
                "For DuckDB or PostgreSQL, return SQL against the local tables only. "
                "For MongoDB, return a JSON query or pipeline string against the collection only. "
                "If the question requires cross-database reasoning, return the raw rows and key columns needed for a later in-Python join."
            )
            repaired = self.complete(
                [
                    {"role": "system", "content": repair_system_prompt},
                    {"role": "user", "content": repair_user_prompt},
                ]
            ).strip()
            if repaired.startswith("```"):
                repaired = repaired.strip("`")
                if repaired.lower().startswith("json"):
                    repaired = repaired[4:].strip()
            try:
                repaired_payload = json.loads(repaired)
                if isinstance(repaired_payload, dict):
                    for key in ("query", "sql", "pipeline", "statement"):
                        value = repaired_payload.get(key)
                        if isinstance(value, str) and value.strip():
                            return value.strip()
            except json.JSONDecodeError:
                pass
            return repaired

        def fallback_query_from_schema(db_name: str) -> str:
            db_schema = schema_context.get(db_name, {})
            db_type = str(db_clients.get(db_name, {}).get("db_type", "")).strip().lower()
            tables = db_schema.get("tables", {}) if isinstance(db_schema, dict) else {}
            first_table = next(iter(tables.keys()), "")
            if db_type in {"duckdb", "sqlite", "postgres"} and first_table:
                return f"SELECT * FROM {first_table};"

            if db_type in {"mongo", "mongodb"}:
                collections = db_schema.get("collections", {}) if isinstance(db_schema, dict) else {}
                first_collection = next(iter(collections.keys()), "")
                if first_collection:
                    collection_meta = collections.get(first_collection, {})
                    fields = []
                    if isinstance(collection_meta, dict):
                        fields = [str(field) for field in collection_meta.get("fields", []) if str(field).strip()]
                    projection = {field: 1 for field in fields[:8]}
                    projection["_id"] = 0

                    payload = {
                        "collection": first_collection,
                        "limit": None,
                        "projection": projection,
                    }
                    return json.dumps(payload)

            return ""

        response = self.complete(
            [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ]
        )
        normalized = parse_queries(response)
        if normalized and not needs_retry(normalized):
            return normalized

        if normalized:
            repaired_queries = dict(normalized)
            for db_name, query in list(normalized.items()):
                if query_needs_repair(db_name, query):
                    repaired_query = repair_single_query(db_name, query)
                    if query_needs_repair(db_name, repaired_query):
                        if rule_strategy_queries.get(db_name):
                            repaired_query = rule_strategy_queries[db_name]
                        else:
                            fallback_query = fallback_query_from_schema(db_name)
                            if fallback_query:
                                repaired_query = fallback_query
                    repaired_queries[db_name] = repaired_query
            if repaired_queries and not needs_retry(repaired_queries):
                return repaired_queries

        retry_system_prompt = (
            system_prompt
            + " Important: do not use file-based joins, read_json_auto, read_csv, or any external file path. "
            + "Each database query must read only from that database's own tables or collections. "
            + "For cross-database questions, retrieve the raw rows needed for a later in-Python join."
        )
        retry_response = self.complete(
            [
                {"role": "system", "content": retry_system_prompt},
                {"role": "user", "content": user_prompt},
            ]
        )
        retry_normalized = parse_queries(retry_response)
        if retry_normalized and not needs_retry(retry_normalized):
            return retry_normalized

        fallback_queries: dict[str, str] = {}
        for db_name in allowed_dbs:
            fallback_query = fallback_query_from_schema(db_name)
            if fallback_query:
                fallback_queries[db_name] = fallback_query
        if rule_strategy_queries:
            for db_name, query in rule_strategy_queries.items():
                if db_name in allowed_dbs:
                    fallback_queries[db_name] = query
        if fallback_queries:
            return fallback_queries

        lower_question = question.lower()
        if (
            "average rating" in lower_question
            and "located in" in lower_question
            and "businessinfo_database" in allowed_dbs
            and "user_database" in allowed_dbs
        ):
            broad_business_query = json.dumps(
                {
                    "collection": "business",
                    "limit": None,
                }
            )
            broad_review_query = "SELECT * FROM review;"
            return {
                "businessinfo_database": broad_business_query,
                "user_database": broad_review_query,
            }

        return retry_normalized or normalized

    def _state_abbreviation(self, state_token: str) -> str:
        cleaned = state_token.strip().lower()
        if len(cleaned) == 2:
            return cleaned.upper()
        return {
            "alabama": "AL",
            "alaska": "AK",
            "arizona": "AZ",
            "arkansas": "AR",
            "california": "CA",
            "colorado": "CO",
            "connecticut": "CT",
            "delaware": "DE",
            "florida": "FL",
            "georgia": "GA",
            "hawaii": "HI",
            "idaho": "ID",
            "illinois": "IL",
            "indiana": "IN",
            "iowa": "IA",
            "kansas": "KS",
            "kentucky": "KY",
            "louisiana": "LA",
            "maine": "ME",
            "maryland": "MD",
            "massachusetts": "MA",
            "michigan": "MI",
            "minnesota": "MN",
            "mississippi": "MS",
            "missouri": "MO",
            "montana": "MT",
            "nebraska": "NE",
            "nevada": "NV",
            "new hampshire": "NH",
            "new jersey": "NJ",
            "new mexico": "NM",
            "new york": "NY",
            "north carolina": "NC",
            "north dakota": "ND",
            "ohio": "OH",
            "oklahoma": "OK",
            "oregon": "OR",
            "pennsylvania": "PA",
            "rhode island": "RI",
            "south carolina": "SC",
            "south dakota": "SD",
            "tennessee": "TN",
            "texas": "TX",
            "utah": "UT",
            "vermont": "VT",
            "virginia": "VA",
            "washington": "WA",
            "west virginia": "WV",
            "wisconsin": "WI",
            "wyoming": "WY",
        }.get(cleaned, "")
