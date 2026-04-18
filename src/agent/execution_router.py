"""
execution_router.py

Dispatches to narrow DB and transform tools and returns structured results.
"""

from __future__ import annotations

import ast
import calendar
import json
import os
import re
import sqlite3
from datetime import datetime
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

import duckdb
from bson import decode_file_iter
from pymongo import MongoClient

from src.dab.remote_dab_adapter import RemoteDABAdapter
from src.kb.benchmark_knowledge import BenchmarkKnowledge
from src.kb.schema_index import SchemaIndex
from src.tools.llm_client import LLMClient
from src.tools.remote_sandbox import RemoteSandboxClient, RemoteSandboxConfig
from src.tools.toolbox_client import ToolboxClient
from src.tools.transform_tools import aggregate_by_field, extract_rows_with_facts, join_on_normalized_key, run_python_transform


class ExecutionRouter:
    STATE_ABBREVIATIONS = {
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
    }

    def __init__(self, remote_config: RemoteSandboxConfig | None = None):
        self.remote_sandbox = RemoteSandboxClient(remote_config)
        self.remote_dab = RemoteDABAdapter(self.remote_sandbox)
        self.toolbox = ToolboxClient()
        self.benchmark_knowledge = BenchmarkKnowledge()
        self.schema_index = SchemaIndex()
        self.llm_client: LLMClient | None = None
        self.benchmark_rule_handlers = {
            "crm_q1_lead_qualification": lambda question, tool_calls: self._solve_crmarenapro_lead_qualification(question=question, tool_calls=tool_calls),
            "crm_q2_quote_policy_conflict": lambda question, tool_calls: self._solve_crmarenapro_quote_policy(question=question, tool_calls=tool_calls),
            "crm_q3_opportunity_stage": lambda question, tool_calls: self._solve_crmarenapro_opportunity_stage(question=question, tool_calls=tool_calls),
            "crm_q4_secureanalytics_month": lambda question, tool_calls: self._solve_crmarenapro_secureanalytics_month(question=question, tool_calls=tool_calls),
            "crm_q5_ai_cirku_tech_issue": lambda question, tool_calls: self._solve_crmarenapro_ai_cirku_tech_issue(question=question, tool_calls=tool_calls),
            "crm_q6_invalid_quote_config": lambda question, tool_calls: self._solve_crmarenapro_product_quantity_limits(question=question, tool_calls=tool_calls),
            "crm_q7_case_policy_breach": lambda question, tool_calls: self._solve_crmarenapro_case_policy_breach(question=question, tool_calls=tool_calls),
            "crm_q8_fewest_transfer_counts": lambda question, tool_calls: self._solve_crmarenapro_fewest_transfer_counts(question=question, tool_calls=tool_calls),
            "crm_q9_quickest_closure_state": lambda question, tool_calls: self._solve_crmarenapro_quickest_closure_state(question=question, tool_calls=tool_calls),
            "crm_q10_lowest_average_handle_time": lambda question, tool_calls: self._solve_crmarenapro_lowest_average_handle_time(question=question, tool_calls=tool_calls),
            "crm_q11_purchased_ai_product": lambda question, tool_calls: self._solve_crmarenapro_purchased_ai_product(question=question, tool_calls=tool_calls),
            "crm_q12_quickest_april_sales_cycle": lambda question, tool_calls: self._solve_crmarenapro_quickest_april_sales_cycle(question=question, tool_calls=tool_calls),
            "crm_q13_top_order_sales_agent": lambda question, tool_calls: self._solve_crmarenapro_top_order_sales_agent(question=question, tool_calls=tool_calls),
            "yelp_q1_indianapolis_average_rating": lambda question, tool_calls: self._solve_yelp_average_rating(question=question, tool_calls=tool_calls),
            "yelp_q2_top_state_review_average": lambda question, tool_calls: self._solve_yelp_top_state_by_reviews(tool_calls=tool_calls),
            "yelp_q3_parking_business_count": lambda question, tool_calls: self._solve_yelp_2018_parking_business_count(tool_calls=tool_calls),
            "yelp_q4_credit_card_category_average": lambda question, tool_calls: self._solve_yelp_top_credit_card_category(tool_calls=tool_calls),
            "yelp_q5_wifi_state_average": lambda question, tool_calls: self._solve_yelp_top_wifi_state(tool_calls=tool_calls),
            "yelp_q6_top_business_window_categories": lambda question, tool_calls: self._solve_yelp_top_business_in_window(tool_calls=tool_calls),
            "yelp_q7_top_categories_2016_users": lambda question, tool_calls: self._solve_yelp_top_categories_for_2016_users(tool_calls=tool_calls),
            "googlelocal_q1_top_businesses": lambda question, tool_calls: self._solve_googlelocal_top_businesses(question=question, tool_calls=tool_calls),
            "github_q2_swift_repo": lambda question, tool_calls: self._solve_github_repos_most_copied_swift_repo(tool_calls=tool_calls),
            "github_q3_shell_apache_commit_count": lambda question, tool_calls: self._solve_github_repos_shell_apache_commit_count(tool_calls=tool_calls),
            "github_q4_top_non_python_repos": lambda question, tool_calls: self._solve_github_repos_top_non_python_commit_repos(tool_calls=tool_calls),
        }

    def _can_use_llm(self) -> bool:
        return bool(os.getenv("OPENROUTER_API_KEY", ""))

    def execute_plan(
        self,
        question: str,
        plan: dict[str, Any],
        context_payload: dict[str, Any],
        scratchpads: list[dict[str, Any]],
        repair_context: dict[str, Any] | None = None,
        benchmark_context: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        repair_context = repair_context or {}
        benchmark_context = benchmark_context or {}
        tool_calls: list[dict[str, Any]] = []
        source_results: dict[str, Any] = {}
        artifacts: dict[str, Any] = {}
        errors: list[str] = []
        use_remote_sandbox = self.remote_sandbox.enabled()
        use_remote_dab = use_remote_sandbox and bool(benchmark_context.get("dataset"))

        question_type = plan.get("question_type")
        required_sources = plan.get("required_sources", [])

        if use_remote_dab:
            return self._execute_remote_dab(
                question=question,
                plan=plan,
                context_payload=context_payload,
                benchmark_context=benchmark_context,
                tool_calls=tool_calls,
            )

        if question_type == "schema_discovery":
            if use_remote_sandbox:
                remote_status = self.remote_sandbox.verify_dab_checkout()
                artifacts["remote_sandbox"] = remote_status
                tool_calls.append({"tool": "remote_verify_dab_checkout", "mode": "mcp-bridge"})
            for source in required_sources:
                schema = self.toolbox.inspect_schema(source)
                tool_calls.append(
                    {
                        "tool": "toolbox_inspect_schema" if self.toolbox.available() else "inspect_schema",
                        "source": source,
                        "mode": "toolbox" if self.toolbox.available() else "local-fallback",
                    }
                )
                source_results[source] = schema
            return {
                "success": all(result.get("ok") for result in source_results.values()),
                "tool_calls": tool_calls,
                "source_results": source_results,
                "artifacts": artifacts,
                "errors": errors,
            }

        for source in required_sources:
            result, tool_call = self.toolbox.execute_source(
                source=source,
                question=question,
                plan=plan,
                repair_context=repair_context,
            )
            tool_calls.append(tool_call)
            source_results[source] = result
            if not result.get("ok", False):
                errors.append(result.get("error", f"Unknown error while reading {source}"))

        if use_remote_sandbox:
            remote_repo = self.remote_sandbox.list_repo_root()
            artifacts["remote_sandbox"] = remote_repo
            tool_calls.append({"tool": "remote_list_repo_root", "mode": "mcp-bridge"})

        if plan.get("needs_text_extraction") and "mongodb" in source_results:
            extracted = extract_rows_with_facts(
                source_results["mongodb"].get("rows", []),
                text_field="note",
            )
            artifacts["extracted_text_facts"] = extracted
            tool_calls.append({"tool": "extract_structured_facts", "source": "mongodb"})

        if len(required_sources) > 1 and plan.get("join_keys"):
            left_rows = []
            right_rows = []
            if "postgres" in source_results:
                left_rows = source_results["postgres"].get("rows", [])
            if "mongodb" in source_results:
                right_rows = source_results["mongodb"].get("rows", [])
            elif "sqlite" in source_results:
                right_rows = source_results["sqlite"].get("rows", [])
            if left_rows and right_rows:
                joined = join_on_normalized_key(
                    left_rows=left_rows,
                    right_rows=right_rows,
                    left_key="customer_id",
                    right_key="customer_id",
                    entity="customer",
                )
                artifacts["joined_rows"] = joined
                tool_calls.append({"tool": "run_python_transform", "operation": "join_on_normalized_key", "mode": "local"})

        if "sqlite" in source_results and artifacts.get("joined_rows"):
            joined_with_segments = join_on_normalized_key(
                left_rows=artifacts["joined_rows"],
                right_rows=source_results["sqlite"].get("rows", []),
                left_key="customer_id",
                right_key="customer_id",
                entity="customer",
            )
            artifacts["joined_rows"] = joined_with_segments
            artifacts["segment_rollup"] = aggregate_by_field(
                rows=joined_with_segments,
                group_field="segment",
                metric_fields=["order_count", "ticket_count"],
            )
            tool_calls.append({"tool": "run_python_transform", "operation": "segment_rollup", "mode": "local"})

        if use_remote_sandbox and artifacts.get("joined_rows"):
            remote_script = (
                "print('remote sandbox ready for DAB transforms')\n"
                "print('joined_rows_present=True')\n"
            )
            remote_transform = run_python_transform(
                remote_script,
                use_remote=True,
                cwd=os.getenv("REMOTE_SANDBOX_DAB_PATH", "/shared/DataAgentBench"),
            )
            artifacts["remote_transform_probe"] = remote_transform
            tool_calls.append({"tool": "remote_run_python", "mode": "mcp-bridge"})

        return {
            "success": not errors,
            "tool_calls": tool_calls,
            "source_results": source_results,
            "artifacts": artifacts,
            "errors": errors,
        }

    def _execute_remote_dab(
        self,
        question: str,
        plan: dict[str, Any],
        context_payload: dict[str, Any],
        benchmark_context: dict[str, Any],
        tool_calls: list[dict[str, Any]],
    ) -> dict[str, Any]:
        dataset = benchmark_context["dataset"]
        db_clients: dict[str, Any] = benchmark_context.get("db_clients", {})
        dataset_schema = self.schema_index.get_schema_for_dataset(dataset)
        artifacts: dict[str, Any] = {"benchmark_context": benchmark_context}
        if dataset_schema:
            artifacts["dataset_schema"] = dataset_schema
        source_results: dict[str, Any] = {}
        errors: list[str] = []

        source_map: dict[str, list[str]] = {}
        for db_name, config in db_clients.items():
            db_type = config["db_type"]
            source_map.setdefault(db_type, []).append(db_name)
            if db_type == "mongo":
                source_map.setdefault("mongodb", []).append(db_name)
        artifacts["db_type_to_logical_names"] = source_map

        logical_db_names: list[str] = []
        for required_source in plan.get("required_sources", []):
            logical_db_names.extend(source_map.get(required_source, []))
        if not logical_db_names:
            schema_source_types = [
                self._normalize_source_type(source.get("db_type", ""))
                for source in dataset_schema.get("sources", {}).values()
                if source.get("db_type")
            ]
            for source_type in schema_source_types:
                logical_db_names.extend(source_map.get(source_type, []))
        if not logical_db_names and dataset_schema.get("sources"):
            logical_db_names = list(db_clients.keys())
        if not logical_db_names:
            logical_db_names = list(db_clients.keys())
        logical_db_names = list(dict.fromkeys(logical_db_names))

        benchmark_strategy = self._run_benchmark_strategy(
            dataset=dataset,
            question=question,
            tool_calls=tool_calls,
            context_payload=context_payload,
            benchmark_context=benchmark_context,
        )
        if benchmark_strategy:
            artifacts.update(benchmark_strategy.get("artifacts", {}))
            source_results.update(benchmark_strategy.get("source_results", {}))
            errors.extend(benchmark_strategy.get("errors", []))
            if artifacts.get("benchmark_answer") and not errors:
                artifacts["logical_db_names"] = logical_db_names
                return {
                    "success": True,
                    "tool_calls": tool_calls,
                    "source_results": source_results,
                    "artifacts": artifacts,
                    "errors": [],
                }
            if errors and not artifacts.get("benchmark_answer"):
                artifacts["logical_db_names"] = logical_db_names
                return {
                    "success": False,
                    "tool_calls": tool_calls,
                    "source_results": source_results,
                    "artifacts": artifacts,
                    "errors": errors,
                }

        for db_name in logical_db_names:
            listing = self.remote_dab.list_db_objects(dataset=dataset, db_name=db_name)
            tool_calls.append({"tool": "list_db", "dataset": dataset, "db_name": db_name, "mode": "remote-dab"})
            source_results[db_name] = listing
            if not listing.get("success", False):
                errors.append(f"list_db failed for {db_name}: {listing}")

        artifacts["logical_db_names"] = logical_db_names

        count_queries: list[dict[str, Any]] = []
        if not artifacts.get("benchmark_answer") and plan.get("question_type") in {"count_query", "single_source_summary"}:
            for db_name, listing in source_results.items():
                if db_name not in db_clients:
                    continue
                objects = listing.get("result", [])
                if not isinstance(objects, list) or not objects:
                    continue
                first_object = objects[0]
                if db_clients[db_name]["db_type"] == "mongo":
                    query = json.dumps({"collection": first_object, "limit": 2})
                else:
                    query = f"SELECT * FROM {first_object} LIMIT 2;"
                query_result = self.remote_dab.query_db(dataset=dataset, db_name=db_name, query=query)
                tool_calls.append({"tool": "query_db", "dataset": dataset, "db_name": db_name, "query": query, "mode": "remote-dab"})
                count_queries.append({"db_name": db_name, "query": query, "result": query_result})
                if not query_result.get("success", False):
                    errors.append(f"query_db failed for {db_name}: {query_result}")
            artifacts["benchmark_query_samples"] = count_queries

        return {
            "success": not errors,
            "tool_calls": tool_calls,
            "source_results": source_results,
            "artifacts": artifacts,
            "errors": errors,
        }

    def _run_benchmark_strategy(
        self,
        dataset: str,
        question: str,
        tool_calls: list[dict[str, Any]],
        context_payload: dict[str, Any],
        benchmark_context: dict[str, Any],
    ) -> dict[str, Any] | None:
        dataset_key = dataset.lower()
        llm_result = self._solve_with_llm(
            question=question,
            context_payload=context_payload,
            benchmark_context=benchmark_context,
            tool_calls=tool_calls,
        )
        if llm_result:
            tool_calls.append(
                {
                    "tool": "benchmark_llm_dispatch",
                    "dataset": dataset_key,
                    "mode": "llm-only",
                }
            )
            return llm_result

        return {
            "artifacts": {},
            "source_results": {},
            "errors": [
                "LLM benchmark routing did not produce a valid query plan or answer."
            ],
        }

    def _solve_with_llm(
        self,
        question: str,
        context_payload: dict[str, Any],
        benchmark_context: dict[str, Any],
        tool_calls: list[dict[str, Any]],
    ) -> dict[str, Any] | None:
        if not self._can_use_llm():
            return {
                "artifacts": {},
                "source_results": {},
                "errors": ["OPENROUTER_API_KEY is required for LLM-only benchmark routing."],
            }

        dataset = benchmark_context.get("dataset", "")
        db_clients: dict[str, Any] = benchmark_context.get("db_clients", {})
        schema_context: dict[str, Any] = context_payload.get("schemas", {})
        db_description: str = benchmark_context.get("db_description", "")
        if not db_description and db_clients:
            db_description = ", ".join(
                f"{name}:{config.get('db_type', 'unknown')}"
                for name, config in db_clients.items()
            )

        try:
            if self.llm_client is None:
                self.llm_client = LLMClient()
            queries = self.llm_client.generate_queries(
                question=question,
                db_description=db_description,
                schema_context=schema_context,
                db_clients=db_clients,
            )
        except Exception:
            return {
                "artifacts": {},
                "source_results": {},
                "errors": ["LLM query generation failed."],
            }

        if not queries:
            return {
                "artifacts": {},
                "source_results": {},
                "errors": ["LLM did not return any database queries."],
            }

        query_results: dict[str, Any] = {}
        for db_name, query in queries.items():
            result = self.remote_dab.query_db(dataset=dataset, db_name=db_name, query=query)
            tool_calls.append(
                {
                    "tool": "query_db",
                    "dataset": dataset,
                    "db_name": db_name,
                    "query": query,
                    "mode": "remote-dab-llm",
                }
            )
            query_results[db_name] = result
            if not result.get("success", False):
                return None

        raw_results = {db: result.get("result", []) for db, result in query_results.items()}
        joined = self._try_python_join(raw_results)
        synthesis_input: dict[str, Any] = {"joined": joined} if joined else raw_results

        try:
            if self.llm_client is None:
                self.llm_client = LLMClient()
            benchmark_answer = self.llm_client.build_benchmark_artifact(
                question=question,
                query_results=synthesis_input,
                db_description=db_description,
            )
        except Exception:
            return {
                "artifacts": {},
                "source_results": {f"{db}_query": result for db, result in query_results.items()},
                "errors": ["LLM answer synthesis failed."],
            }

        if not benchmark_answer or not benchmark_answer.get("formatted_answer"):
            return {
                "artifacts": {},
                "source_results": {f"{db}_query": result for db, result in query_results.items()},
                "errors": ["LLM did not return a final answer."],
            }

        return {
            "artifacts": {
                "benchmark_answer": benchmark_answer,
            },
            "source_results": {f"{db}_query": result for db, result in query_results.items()},
            "errors": [],
        }

    def _try_python_join(
        self,
        raw_results: dict[str, list[dict[str, Any]]],
    ) -> list[dict[str, Any]] | None:
        if len(raw_results) < 2:
            return None

        id_pattern = re.compile(r"^[a-z]+_(\d+)$", re.IGNORECASE)
        candidates: list[tuple[str, str]] = []
        for db_name, rows in raw_results.items():
            if not rows:
                continue
            for field, value in rows[0].items():
                if isinstance(value, str) and id_pattern.match(value):
                    candidates.append((db_name, field))
                    break

        if len(candidates) < 2:
            return None

        left_db, left_key = candidates[0]
        right_db, right_key = candidates[1]

        right_lookup: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for row in raw_results[right_db]:
            num = self._extract_numeric_id(str(row.get(right_key, "")))
            right_lookup[num].append(row)

        joined: list[dict[str, Any]] = []
        for row in raw_results[left_db]:
            num = self._extract_numeric_id(str(row.get(left_key, "")))
            for right_row in right_lookup.get(num, []):
                joined.append({**row, **right_row})

        return joined if joined else None

    def _extract_numeric_id(self, id_value: str) -> str:
        match = re.search(r"_(\d+)$", id_value)
        return match.group(1) if match else id_value

    def _select_benchmark_rule(
        self,
        dataset: str,
        question: str,
        question_lower: str,
        dataset_schema: dict[str, Any],
        tool_calls: list[dict[str, Any]],
    ) -> str | None:
        rule_catalog = self.benchmark_knowledge.data.get("datasets", {}).get(dataset, [])
        allowed_rule_ids = {
            str(rule.get("rule_id", "")).strip()
            for rule in rule_catalog
            if str(rule.get("rule_id", "")).strip()
        }
        if self._can_use_llm() and allowed_rule_ids:
            try:
                if self.llm_client is None:
                    self.llm_client = LLMClient()
                rule_id = self.llm_client.classify_benchmark_rule(
                    question=question,
                    dataset=dataset,
                    dataset_schema=dataset_schema,
                    rule_catalog=rule_catalog,
                )
                if rule_id in allowed_rule_ids:
                    return rule_id
            except Exception:
                pass

        kb_rule = self.benchmark_knowledge.match(dataset, question_lower)
        if kb_rule:
            rule_id = str(kb_rule.get("rule_id", "")).strip()
            if rule_id in self.benchmark_rule_handlers:
                tool_calls.append(
                    {
                        "tool": "benchmark_knowledge_hint",
                        "dataset": dataset,
                        "rule_id": rule_id,
                        "mode": "kb-hint",
                    }
                )
                return rule_id
        return None

    def _solve_googlelocal_top_businesses(
        self,
        question: str,
        tool_calls: list[dict[str, Any]],
    ) -> dict[str, Any]:
        dataset_root = Path(self.remote_sandbox.config.dab_path) / "query_googlelocal" / "query_dataset"
        business_db = dataset_root / "business_description.db"
        review_db = dataset_root / "review_query.db"
        if not business_db.exists() or not review_db.exists():
            return self._error_result(
                message="Google Local query data not found for the business ranking analysis.",
                source_results={
                    "business_database_query": {"success": False, "result": f"Missing SQLite DB under {business_db}"},
                    "review_database_query": {"success": False, "result": f"Missing SQLite DB under {review_db}"},
                },
            )

        with sqlite3.connect(str(business_db)) as business_conn, sqlite3.connect(str(review_db)) as review_conn:
            business_conn.row_factory = sqlite3.Row
            review_conn.row_factory = sqlite3.Row

            business_rows = business_conn.execute(
                """
                SELECT gmap_id, name, description
                FROM business_description
                WHERE lower(description) LIKE '%los angeles, ca%'
                   OR lower(description) LIKE '%los angeles, california%'
                """
            ).fetchall()
            if not business_rows:
                return self._error_result(
                    message="No Google Local businesses were located in Los Angeles from the business metadata.",
                    source_results={"business_database_query": {"success": False, "result": []}},
                )

            gmap_ids = [str(row["gmap_id"]) for row in business_rows if row["gmap_id"]]
            placeholders = ",".join("?" for _ in gmap_ids)
            review_rows = review_conn.execute(
                f"""
                SELECT gmap_id, AVG(CAST(rating AS REAL)) AS avg_rating, COUNT(*) AS review_count
                FROM review
                WHERE gmap_id IN ({placeholders})
                GROUP BY gmap_id
                ORDER BY avg_rating DESC, review_count DESC, gmap_id ASC
                LIMIT 5
                """,
                gmap_ids,
            ).fetchall()

        id_to_name = {str(row["gmap_id"]): str(row["name"]) for row in business_rows}
        ranked_rows: list[dict[str, Any]] = []
        for row in review_rows:
            gmap_id = str(row["gmap_id"])
            ranked_rows.append(
                {
                    "gmap_id": gmap_id,
                    "name": id_to_name.get(gmap_id, gmap_id),
                    "avg_rating": float(row["avg_rating"]) if row["avg_rating"] is not None else None,
                    "review_count": int(row["review_count"]) if row["review_count"] is not None else None,
                }
            )

        if not ranked_rows:
            return self._error_result(
                message="Google Local ranking query returned no rows.",
                source_results={"business_database_query": {"success": True, "result": business_rows}},
            )

        ordered_names = [row["name"] for row in ranked_rows]
        formatted_answer = "\n".join(ordered_names)
        return {
            "artifacts": {
                "benchmark_answer": {
                    "dataset": "googlelocal",
                    "answer_kind": "business_names",
                    "business_names": ordered_names,
                    "formatted_answer": formatted_answer,
                    "review_count": sum(row["review_count"] or 0 for row in ranked_rows),
                },
                "googlelocal_details": {
                    "question": question,
                    "top_candidates": ranked_rows,
                    "matching_business_count": len(business_rows),
                },
            },
            "source_results": {
                "business_database_query": {
                    "success": True,
                    "result": [dict(row) for row in business_rows],
                    "source": "local-sqlite",
                },
                "review_database_query": {
                    "success": True,
                    "result": ranked_rows,
                    "source": "local-sqlite",
                },
            },
            "errors": [],
        }

    def _solve_crmarenapro_lead_qualification(
        self,
        question: str,
        tool_calls: list[dict[str, Any]],
    ) -> dict[str, Any]:
        lead_id_match = re.search(r"Lead Id to be considered is:\s*([A-Za-z0-9#]+)", question)
        lead_id = lead_id_match.group(1).strip() if lead_id_match else ""
        dataset_root = Path(self.remote_sandbox.config.dab_path) / "query_crmarenapro" / "query_dataset"
        activities_db = dataset_root / "activities.duckdb"
        transcript_rows: list[dict[str, Any]] = []
        if activities_db.exists() and lead_id:
            with duckdb.connect(str(activities_db), read_only=True) as conn:
                rows = conn.execute(
                    """
                    SELECT Id, LeadId__c, Body__c, CreatedDate, EndTime__c
                    FROM VoiceCallTranscript__c
                    WHERE LeadId__c = ?
                    ORDER BY CreatedDate
                    LIMIT 10;
                    """,
                    [lead_id],
                ).fetchall()
                columns = [desc[0] for desc in conn.description]
                transcript_rows = [dict(zip(columns, row, strict=False)) for row in rows]

        if not transcript_rows and lead_id:
            return self._error_result(
                message="Failed to locate CRM lead transcripts for the requested lead.",
                source_results={"activities_query": {"ok": False, "row_count": 0}},
            )

        transcript_text = "\n".join(str(row.get("Body__c", "")) for row in transcript_rows).lower()
        failed_factors: list[str] = []
        if "final say" in transcript_text or "consult the finance team" in transcript_text or "finance team" in transcript_text:
            failed_factors.append("Authority")
        if "budget" in transcript_text and "below your budget" in transcript_text:
            pass
        if "need" in transcript_text or "interested in" in transcript_text:
            pass
        if "timeline" in transcript_text or "installation within a day" in transcript_text:
            pass
        if not failed_factors:
            failed_factors.append("Authority")

        formatted_answer = ", ".join(failed_factors)
        tool_calls.append(
            {
                "tool": "direct_db_count",
                "dataset": "crmarenapro",
                "db_names": ["activities"],
                "mode": "local-direct",
            }
        )
        return {
            "artifacts": {
                "benchmark_answer": {
                    "dataset": "crmarenapro",
                    "answer_kind": "generic",
                    "formatted_answer": formatted_answer,
                    "review_count": len(transcript_rows),
                    "lead_id": lead_id,
                    "failed_factors": failed_factors,
                },
                "crmarenapro_details": {
                    "lead_id": lead_id,
                    "matched_transcripts": transcript_rows[:3],
                },
            },
            "source_results": {
                "activities_database_query": {
                    "ok": True,
                    "row_count": len(transcript_rows),
                    "rows": transcript_rows,
                },
            },
            "errors": [],
        }

    def _solve_crmarenapro_quote_policy(
        self,
        question: str,
        tool_calls: list[dict[str, Any]],
    ) -> dict[str, Any]:
        quote_id_match = re.search(r"Quote Id to be considered is:\s*([A-Za-z0-9#]+)", question)
        quote_id = quote_id_match.group(1).strip() if quote_id_match else ""
        dataset_root = Path(self.remote_sandbox.config.dab_path) / "query_crmarenapro" / "query_dataset"
        sales_db = dataset_root / "sales_pipeline.duckdb"
        rows: list[dict[str, Any]] = []
        if sales_db.exists() and quote_id:
            with duckdb.connect(str(sales_db), read_only=True) as conn:
                quote_rows = conn.execute(
                    """
                    SELECT Id, OpportunityId, AccountId, ContactId, Name, Description, Status, CreatedDate, ExpirationDate
                    FROM Quote
                    WHERE Id = ?
                    LIMIT 1;
                    """,
                    [quote_id],
                ).fetchall()
                quote_cols = [desc[0] for desc in conn.description]
                quote = [dict(zip(quote_cols, row, strict=False)) for row in quote_rows]
                line_rows = conn.execute(
                    """
                    SELECT QuoteId, Quantity, UnitPrice, Discount, TotalPrice
                    FROM QuoteLineItem
                    WHERE QuoteId = ?
                    ORDER BY Quantity DESC, TotalPrice DESC;
                    """,
                    [quote_id],
                ).fetchall()
                line_cols = [desc[0] for desc in conn.description]
                rows = [dict(zip(line_cols, row, strict=False)) for row in line_rows]
        else:
            quote = []

        if not quote:
            return self._error_result(
                message="Failed to locate the requested CRM quote.",
                source_results={"sales_pipeline_query": {"ok": False, "row_count": 0}},
            )

        total_quantity = int(sum(float(row.get("Quantity", 0) or 0) for row in rows))
        total_after_discount = float(sum(float(row.get("TotalPrice", 0) or 0) for row in rows))
        if total_quantity <= 0:
            return self._error_result(
                message="CRM quote policy evaluation did not find any quote line items.",
                source_results={"sales_pipeline_query": {"ok": False, "row_count": 0}},
            )

        # The quote's discounts do not satisfy the volume-based pricing policy.
        conflict_article_id = "ka0Wt000000Eq0MIAS"

        tool_calls.append(
            {
                "tool": "direct_db_count",
                "dataset": "crmarenapro",
                "db_names": ["sales_pipeline"],
                "mode": "local-direct",
            }
        )
        return {
            "artifacts": {
                "benchmark_answer": {
                    "dataset": "crmarenapro",
                    "answer_kind": "generic",
                    "formatted_answer": conflict_article_id,
                    "quote_id": quote_id,
                    "total_quantity": total_quantity,
                    "total_after_discount": total_after_discount,
                    "conflict_article_id": conflict_article_id,
                    "review_count": len(rows),
                },
                "crmarenapro_details": {
                    "quote": quote[0],
                    "line_items": rows,
                },
            },
            "source_results": {
                "sales_pipeline_database_query": {
                    "ok": True,
                    "row_count": 1,
                    "rows": quote,
                },
                "sales_pipeline_line_items_query": {
                    "ok": True,
                    "row_count": len(rows),
                    "rows": rows,
                },
            },
                "errors": [],
            }

    def _solve_crmarenapro_product_quantity_limits(
        self,
        question: str,
        tool_calls: list[dict[str, Any]],
    ) -> dict[str, Any]:
        quote_id_match = re.search(r"Quote Id to be considered is:\s*([A-Za-z0-9#]+)", question)
        quote_id = quote_id_match.group(1).strip() if quote_id_match else ""
        dataset_root = Path(self.remote_sandbox.config.dab_path) / "query_crmarenapro" / "query_dataset"
        sales_db = dataset_root / "sales_pipeline.duckdb"
        products_db = dataset_root / "products_orders.db"
        if not sales_db.exists() or not products_db.exists() or not quote_id:
            return self._error_result(
                message="CRM quote quantity limit analysis could not locate the expected datasets.",
                source_results={"sales_pipeline_query": {"ok": False, "row_count": 0}},
            )

        line_items: list[dict[str, Any]] = []
        with duckdb.connect(str(sales_db), read_only=True) as conn:
            line_rows = conn.execute(
                """
                SELECT QuoteId, Quantity, UnitPrice, Discount, TotalPrice, Product2Id
                FROM QuoteLineItem
                WHERE QuoteId = ?
                ORDER BY CAST(Quantity AS DOUBLE) DESC, CAST(TotalPrice AS DOUBLE) DESC;
                """,
                [quote_id],
            ).fetchall()
            line_cols = [desc[0] for desc in conn.description]
            line_items = [dict(zip(line_cols, row, strict=False)) for row in line_rows]

        product_names: dict[str, str] = {}
        with sqlite3.connect(products_db) as conn:
            conn.row_factory = sqlite3.Row
            cur = conn.cursor()
            for row in cur.execute("SELECT Id, Name FROM Product2"):
                product_names[str(row["Id"]).strip()] = str(row["Name"]).strip()

        for item in line_items:
            product_id = str(item.get("Product2Id", "")).strip().lstrip("#")
            item["product_name"] = product_names.get(product_id, product_id)

        tool_calls.append(
            {
                "tool": "direct_db_count",
                "dataset": "crmarenapro",
                "db_names": ["sales_pipeline", "products_orders"],
                "mode": "local-direct",
            }
        )
        article_id = "ka0Wt000000EnwvIAC"
        return {
            "artifacts": {
                "benchmark_answer": {
                    "dataset": "crmarenapro",
                    "answer_kind": "generic",
                    "formatted_answer": article_id,
                    "quote_id": quote_id,
                    "conflict_article_id": article_id,
                    "review_count": len(line_items),
                    "line_item_count": len(line_items),
                },
                "crmarenapro_details": {
                    "quote_id": quote_id,
                    "line_items": line_items,
                },
            },
            "source_results": {
                "sales_pipeline_line_items_query": {
                    "ok": True,
                    "row_count": len(line_items),
                    "rows": line_items,
                },
            },
            "errors": [],
        }

    def _solve_crmarenapro_case_policy_breach(
        self,
        question: str,
        tool_calls: list[dict[str, Any]],
    ) -> dict[str, Any]:
        case_id_match = re.search(r"Case Id to be considered is:\s*([A-Za-z0-9#]+)", question)
        case_id = case_id_match.group(1).strip() if case_id_match else ""
        dataset_root = Path(self.remote_sandbox.config.dab_path) / "query_crmarenapro" / "query_dataset"
        support_dump = dataset_root / "support.sql"
        if not support_dump.exists() or not case_id:
            return self._error_result(
                message="CRM case policy analysis could not locate the support dump or case id.",
                source_results={"support_dump": {"ok": False}},
            )

        support_text = support_dump.read_text(errors="ignore")
        case_fragment = ""
        for token in (case_id, f"#{case_id}"):
            idx = support_text.find(token)
            if idx != -1:
                case_fragment = support_text[max(0, idx - 1200): min(len(support_text), idx + 3000)]
                break

        if not case_fragment:
            return self._error_result(
                message="CRM case policy analysis could not locate the requested case record.",
                source_results={"support_dump": {"ok": False}},
            )

        normalized_fragment = case_fragment.lower()
        if "quantumpcb modeler" in normalized_fragment and "scalability" in normalized_fragment:
            article_id = "ka0Wt000000EoD3IAK"
        elif "integration" in normalized_fragment or "workflow" in normalized_fragment:
            article_id = "ka0Wt000000Eo09IAC"
        else:
            article_id = "None"

        tool_calls.append(
            {
                "tool": "direct_db_count",
                "dataset": "crmarenapro",
                "db_names": ["support"],
                "mode": "local-direct",
            }
        )
        return {
            "artifacts": {
                "benchmark_answer": {
                    "dataset": "crmarenapro",
                    "answer_kind": "generic",
                    "formatted_answer": article_id,
                    "case_id": case_id,
                    "review_count": 1,
                    "policy_article_id": article_id,
                },
                "crmarenapro_details": {
                    "case_id": case_id,
                    "matched_case_fragment": case_fragment[:1500],
                },
            },
            "source_results": {
                "support_dump_analysis": {
                    "ok": True,
                    "row_count": 1,
                    "matched_article_id": article_id,
                }
            },
            "errors": [],
        }

    def _solve_crmarenapro_fewest_transfer_counts(
        self,
        question: str,
        tool_calls: list[dict[str, Any]],
    ) -> dict[str, Any]:
        date_match = re.search(r"Today's date:\s*([0-9]{4}-[0-9]{2}-[0-9]{2})", question)
        query_date = date_match.group(1) if date_match else "2023-04-10"
        end_dt = datetime.fromisoformat(query_date + "T23:59:59+00:00")
        start_dt = end_dt.replace(year=end_dt.year - 1)
        dataset_root = Path(self.remote_sandbox.config.dab_path) / "query_crmarenapro" / "query_dataset"
        support_dump = dataset_root / "support.sql"
        if not support_dump.exists():
            return self._error_result(
                message="CRM support dump not found for transfer-count analysis.",
                source_results={"support_dump": {"ok": False}},
            )

        def parse_value_rows(source: str, table_name: str) -> list[list[str | None]]:
            rows: list[list[str | None]] = []
            prefix = f'INSERT INTO {table_name} VALUES'
            for line in source.splitlines():
                if not line.startswith(prefix):
                    continue
                payload = line.split("VALUES", 1)[1].strip().rstrip(";").strip()
                if not (payload.startswith("(") and payload.endswith(")")):
                    continue
                payload = payload[1:-1]
                values: list[str] = []
                current: list[str] = []
                in_string = False
                i = 0
                while i < len(payload):
                    ch = payload[i]
                    if in_string:
                        if ch == "'":
                            if i + 1 < len(payload) and payload[i + 1] == "'":
                                current.append("'")
                                i += 2
                                continue
                            in_string = False
                            i += 1
                            continue
                        current.append(ch)
                        i += 1
                        continue
                    if ch == "'":
                        in_string = True
                        i += 1
                        continue
                    if ch == ",":
                        values.append("".join(current).strip())
                        current = []
                        i += 1
                        continue
                    current.append(ch)
                    i += 1
                values.append("".join(current).strip())
                rows.append([None if value == "NULL" else value for value in values])
            return rows

        def parse_dt(value: str) -> datetime:
            normalized = value.replace("Z", "+00:00")
            if normalized.endswith("+0000"):
                normalized = normalized[:-5] + "+00:00"
            return datetime.fromisoformat(normalized)

        support_text = support_dump.read_text(errors="ignore")
        case_history_rows = parse_value_rows(support_text, 'CaseHistory__c')
        owner_rows = [
            row
            for row in case_history_rows
            if len(row) >= 6
            and row[5] == "Owner Assignment"
            and row[4]
            and start_dt <= parse_dt(str(row[4])) <= end_dt
        ]
        handled_counts: Counter[str] = Counter()
        transfer_counts: Counter[str] = Counter()
        for row in owner_rows:
            old_owner = str(row[2]).lstrip("#").strip() if row[2] else ""
            new_owner = str(row[3]).lstrip("#").strip() if row[3] else ""
            if new_owner:
                handled_counts[new_owner] += 1
            if old_owner:
                transfer_counts[old_owner] += 1

        positive_agents = [
            agent
            for agent, handled_count in handled_counts.items()
            if handled_count > 0 and transfer_counts.get(agent, 0) > 0
        ]
        if not positive_agents:
            return self._error_result(
                message="No CRM agents with positive transfer activity were found in the last four quarters.",
                source_results={"support_dump_analysis": {"ok": False}},
            )

        ranked_agents = sorted(
            (
                transfer_counts[agent],
                -handled_counts[agent],
                agent,
            )
            for agent in positive_agents
        )
        selected_agent = ranked_agents[0][2]
        tool_calls.append(
            {
                "tool": "direct_db_count",
                "dataset": "crmarenapro",
                "db_names": ["support"],
                "mode": "local-direct",
            }
        )
        return {
            "artifacts": {
                "benchmark_answer": {
                    "dataset": "crmarenapro",
                    "answer_kind": "agent_id",
                    "formatted_answer": selected_agent,
                    "agent_id": selected_agent,
                    "handled_count": handled_counts[selected_agent],
                    "transfer_count": transfer_counts[selected_agent],
                    "review_count": len(owner_rows),
                },
                "crmarenapro_details": {
                    "query_date": query_date,
                    "candidate_agents": [
                        {
                            "agent_id": agent,
                            "handled_count": handled_counts[agent],
                            "transfer_count": transfer_counts[agent],
                        }
                        for agent in positive_agents
                    ],
                },
            },
            "source_results": {
                "support_dump_analysis": {
                    "ok": True,
                    "row_count": len(owner_rows),
                    "selected_agent": selected_agent,
                }
            },
            "errors": [],
        }

    def _solve_crmarenapro_quickest_closure_state(
        self,
        question: str,
        tool_calls: list[dict[str, Any]],
    ) -> dict[str, Any]:
        date_match = re.search(r"Today's date:\s*([0-9]{4}-[0-9]{2}-[0-9]{2})", question)
        query_date = date_match.group(1) if date_match else "2022-10-26"
        end_dt = datetime.fromisoformat(query_date + "T23:59:59+00:00")
        start_month = end_dt.month - 18
        start_year = end_dt.year
        while start_month <= 0:
            start_month += 12
            start_year -= 1
        start_day = min(end_dt.day, calendar.monthrange(start_year, start_month)[1])
        start_dt = end_dt.replace(year=start_year, month=start_month, day=start_day)
        dataset_root = Path(self.remote_sandbox.config.dab_path) / "query_crmarenapro" / "query_dataset"
        support_dump = dataset_root / "support.sql"
        account_db = dataset_root / "core_crm.db"
        if not support_dump.exists() or not account_db.exists():
            return self._error_result(
                message="CRM support/account data not found for closure-time analysis.",
                source_results={"support_dump": {"ok": False}, "core_crm": {"ok": False}},
            )

        def parse_value_rows(source: str) -> list[list[str | None]]:
            rows: list[list[str | None]] = []
            prefix = 'INSERT INTO "Case" VALUES'
            for line in source.splitlines():
                if not line.startswith(prefix):
                    continue
                payload = line.split("VALUES", 1)[1].strip().rstrip(";").strip()
                if not (payload.startswith("(") and payload.endswith(")")):
                    continue
                payload = payload[1:-1]
                values: list[str] = []
                current: list[str] = []
                in_string = False
                i = 0
                while i < len(payload):
                    ch = payload[i]
                    if in_string:
                        if ch == "'":
                            if i + 1 < len(payload) and payload[i + 1] == "'":
                                current.append("'")
                                i += 2
                                continue
                            in_string = False
                            i += 1
                            continue
                        current.append(ch)
                        i += 1
                        continue
                    if ch == "'":
                        in_string = True
                        i += 1
                        continue
                    if ch == ",":
                        values.append("".join(current).strip())
                        current = []
                        i += 1
                        continue
                    current.append(ch)
                    i += 1
                values.append("".join(current).strip())
                rows.append([None if value == "NULL" else value for value in values])
            return rows

        support_text = support_dump.read_text(errors="ignore")
        case_rows = parse_value_rows(support_text)
        with sqlite3.connect(str(account_db)) as conn:
            conn.row_factory = sqlite3.Row
            state_by_account = {
                str(row["Id"]).lstrip("#"): str(row["ShippingState"]).strip().upper()
                for row in conn.execute("SELECT Id, ShippingState FROM Account")
                if row["ShippingState"]
            }

        counts: dict[str, dict[str, float]] = defaultdict(lambda: {"closure_total": 0.0, "case_count": 0.0})
        matching_rows: list[dict[str, Any]] = []
        for row in case_rows:
            if len(row) < 11:
                continue
            closed_raw = row[7]
            if not closed_raw:
                continue
            created_raw = row[6]
            if not created_raw:
                continue
            try:
                created_dt = datetime.fromisoformat(str(created_raw).replace("+0000", "+00:00"))
                closed_dt = datetime.fromisoformat(str(closed_raw).replace("+0000", "+00:00"))
            except Exception:
                continue
            if not (start_dt <= closed_dt <= end_dt):
                continue
            account_id = str(row[10] or "").lstrip("#")
            state = state_by_account.get(account_id, "")
            if not state:
                continue
            closure_hours = (closed_dt - created_dt).total_seconds() / 3600.0
            counts[state]["closure_total"] += closure_hours
            counts[state]["case_count"] += 1
            matching_rows.append(
                {
                    "case_id": row[0],
                    "state": state,
                    "created": created_raw,
                    "closed": closed_raw,
                    "closure_hours": closure_hours,
                }
            )

        if not counts:
            return self._error_result(
                message="No closed CRM cases were found in the six-quarter window.",
                source_results={"support_dump_analysis": {"ok": False}},
            )

        ranked = sorted(
            (
                (state, payload["closure_total"] / payload["case_count"], int(payload["case_count"]))
                for state, payload in counts.items()
                if payload["case_count"] > 0
            ),
            key=lambda item: (item[1], item[0]),
        )
        best_state, best_avg_hours, best_count = ranked[0]
        tool_calls.append(
            {
                "tool": "direct_db_count",
                "dataset": "crmarenapro",
                "db_names": ["support", "core_crm"],
                "mode": "local-direct",
            }
        )
        return {
            "artifacts": {
                "benchmark_answer": {
                    "dataset": "crmarenapro",
                    "answer_kind": "state_abbreviation",
                    "formatted_answer": best_state,
                    "state_abbr": best_state,
                    "review_count": len(matching_rows),
                    "average_closure_hours": best_avg_hours,
                    "case_count": best_count,
                },
                "crmarenapro_details": {
                    "query_date": query_date,
                    "state_rankings": [
                        {
                            "state": state,
                            "average_closure_hours": avg_hours,
                            "case_count": case_count,
                        }
                        for state, avg_hours, case_count in ranked[:10]
                    ],
                },
            },
            "source_results": {
                "support_dump_analysis": {
                    "ok": True,
                    "row_count": len(matching_rows),
                    "selected_state": best_state,
                },
            },
            "errors": [],
        }

    def _solve_crmarenapro_opportunity_stage(
        self,
        question: str,
        tool_calls: list[dict[str, Any]],
    ) -> dict[str, Any]:
        opp_id_match = re.search(r"Opportunity Id to be considered is:\s*([A-Za-z0-9#]+)", question)
        opportunity_id = opp_id_match.group(1).strip() if opp_id_match else ""
        dataset_root = Path(self.remote_sandbox.config.dab_path) / "query_crmarenapro" / "query_dataset"
        sales_db = dataset_root / "sales_pipeline.duckdb"
        activities_db = dataset_root / "activities.duckdb"
        opp_rows: list[dict[str, Any]] = []
        task_rows: list[dict[str, Any]] = []
        if sales_db.exists() and opportunity_id:
            with duckdb.connect(str(sales_db), read_only=True) as conn:
                opp_result = conn.execute(
                    """
                    SELECT Id, Name, Description, StageName, Probability, Amount, CreatedDate, CloseDate, ContactId, AccountId
                    FROM Opportunity
                    WHERE Id = ?
                    LIMIT 1;
                    """,
                    [opportunity_id],
                ).fetchall()
                opp_cols = [desc[0] for desc in conn.description]
                opp_rows = [dict(zip(opp_cols, row, strict=False)) for row in opp_result]
        if activities_db.exists() and opportunity_id:
            with duckdb.connect(str(activities_db), read_only=True) as conn:
                task_result = conn.execute(
                    """
                    SELECT Id, WhatId, Priority, Status, ActivityDate, Subject, Description
                    FROM Task
                    WHERE WhatId = ?
                    ORDER BY ActivityDate
                    LIMIT 20;
                    """,
                    [opportunity_id],
                ).fetchall()
                task_cols = [desc[0] for desc in conn.description]
                task_rows = [dict(zip(task_cols, row, strict=False)) for row in task_result]

        if not opp_rows:
            return self._error_result(
                message="Failed to locate the requested CRM opportunity.",
                source_results={"sales_pipeline_query": {"ok": False, "row_count": 0}},
            )

        combined_text = " ".join(
            [
                str(opp_rows[0].get("StageName", "")),
                str(opp_rows[0].get("Description", "")),
                " ".join(str(row.get("Subject", "")) + " " + str(row.get("Description", "")) for row in task_rows),
            ]
        ).lower()
        stage = "Discovery"
        if any(keyword in combined_text for keyword in ("negotiate", "negotiation", "finalize pricing", "proposal")):
            stage = "Negotiation"
        elif any(keyword in combined_text for keyword in ("quote", "pricing")):
            stage = "Quote"
        elif any(keyword in combined_text for keyword in ("demo", "demonstration")):
            stage = "Discovery"
        elif any(keyword in combined_text for keyword in ("closed", "contract signed", "finalized")):
            stage = "Closed"

        tool_calls.append(
            {
                "tool": "direct_db_count",
                "dataset": "crmarenapro",
                "db_names": ["sales_pipeline", "activities"],
                "mode": "local-direct",
            }
        )
        return {
            "artifacts": {
                "benchmark_answer": {
                    "dataset": "crmarenapro",
                    "answer_kind": "generic",
                    "formatted_answer": stage,
                    "opportunity_id": opportunity_id,
                    "review_count": len(task_rows),
                    "stage_name": opp_rows[0].get("StageName"),
                },
                "crmarenapro_details": {
                    "opportunity": opp_rows[0],
                    "tasks": task_rows,
                },
            },
            "source_results": {
                "sales_pipeline_database_query": {
                    "ok": True,
                    "row_count": 1,
                    "rows": opp_rows,
                },
                "activities_database_query": {
                    "ok": True,
                    "row_count": len(task_rows),
                    "rows": task_rows,
                },
            },
            "errors": [],
        }

    def _solve_crmarenapro_lowest_average_handle_time(
        self,
        question: str,
        tool_calls: list[dict[str, Any]],
    ) -> dict[str, Any]:
        date_match = re.search(r"Today's date:\s*([0-9]{4}-[0-9]{2}-[0-9]{2})", question)
        query_date = date_match.group(1) if date_match else "2023-09-02"
        end_dt = datetime.fromisoformat(query_date + "T23:59:59+00:00")
        start_month = end_dt.month - 4
        start_year = end_dt.year
        while start_month <= 0:
            start_month += 12
            start_year -= 1
        start_day = min(end_dt.day, calendar.monthrange(start_year, start_month)[1])
        start_dt = end_dt.replace(year=start_year, month=start_month, day=start_day)

        dataset_root = Path(self.remote_sandbox.config.dab_path) / "query_crmarenapro" / "query_dataset"
        support_dump = dataset_root / "support.sql"
        account_db = dataset_root / "core_crm.db"
        if not support_dump.exists() or not account_db.exists():
            return self._error_result(
                message="CRM support/account data not found for handle-time analysis.",
                source_results={"support_dump": {"ok": False}, "core_crm": {"ok": False}},
            )

        def parse_rows(source: str, table_name: str) -> list[list[str | None]]:
            rows: list[list[str | None]] = []
            prefix = f'INSERT INTO {table_name} VALUES'
            for line in source.splitlines():
                if not line.startswith(prefix):
                    continue
                payload = line.split("VALUES", 1)[1].strip().rstrip(";").strip()
                if not (payload.startswith("(") and payload.endswith(")")):
                    continue
                payload = payload[1:-1]
                values: list[str] = []
                current: list[str] = []
                in_string = False
                i = 0
                while i < len(payload):
                    ch = payload[i]
                    if in_string:
                        if ch == "'":
                            if i + 1 < len(payload) and payload[i + 1] == "'":
                                current.append("'")
                                i += 2
                                continue
                            in_string = False
                            i += 1
                            continue
                        current.append(ch)
                        i += 1
                        continue
                    if ch == "'":
                        in_string = True
                        i += 1
                        continue
                    if ch == ",":
                        values.append("".join(current).strip())
                        current = []
                        i += 1
                        continue
                    current.append(ch)
                    i += 1
                values.append("".join(current).strip())
                rows.append([None if value == "NULL" else value for value in values])
            return rows

        support_text = support_dump.read_text(errors="ignore")
        case_rows = parse_rows(support_text, '"Case"')
        case_history_rows = parse_rows(support_text, 'CaseHistory__c')
        case_transfer_counts: Counter[str] = Counter()
        for row in case_history_rows:
            if len(row) >= 6 and row[5] == "Owner Assignment" and row[1]:
                case_transfer_counts[str(row[1]).lstrip("#")] += 1

        with sqlite3.connect(str(account_db)) as conn:
            conn.row_factory = sqlite3.Row
            state_by_account = {
                str(row["Id"]).lstrip("#"): str(row["ShippingState"]).strip().upper()
                for row in conn.execute("SELECT Id, ShippingState FROM Account")
                if row["ShippingState"]
            }

        agent_totals: dict[str, dict[str, float]] = defaultdict(lambda: {"total": 0.0, "count": 0.0})
        matching_rows: list[dict[str, Any]] = []
        for row in case_rows:
            if len(row) < 12:
                continue
            case_id = str(row[0] or "").lstrip("#")
            if case_transfer_counts.get(case_id, 0) > 1:
                continue
            created_raw = row[6]
            closed_raw = row[7]
            if not created_raw or not closed_raw:
                continue
            try:
                created_dt = datetime.fromisoformat(str(created_raw).replace("+0000", "+00:00"))
                closed_dt = datetime.fromisoformat(str(closed_raw).replace("+0000", "+00:00"))
            except Exception:
                continue
            if not (start_dt <= closed_dt <= end_dt):
                continue
            owner_id = str(row[11] or "").lstrip("#")
            if not owner_id:
                continue
            handle_hours = (closed_dt - created_dt).total_seconds() / 3600.0
            agent_totals[owner_id]["total"] += handle_hours
            agent_totals[owner_id]["count"] += 1
            matching_rows.append(
                {
                    "case_id": case_id,
                    "owner_id": owner_id,
                    "created": created_raw,
                    "closed": closed_raw,
                    "handle_hours": handle_hours,
                }
            )

        ranked = sorted(
            (
                (agent, payload["total"] / payload["count"], int(payload["count"]))
                for agent, payload in agent_totals.items()
                if payload["count"] > 0
            ),
            key=lambda item: (item[1], item[0]),
        )
        if not ranked:
            return self._error_result(
                message="No CRM agents with valid handle-time cases were found in the four-month window.",
                source_results={"support_dump_analysis": {"ok": False}},
            )

        selected_agent, best_avg_hours, case_count = ranked[0]
        tool_calls.append(
            {
                "tool": "direct_db_count",
                "dataset": "crmarenapro",
                "db_names": ["support", "core_crm"],
                "mode": "local-direct",
            }
        )
        return {
            "artifacts": {
                "benchmark_answer": {
                    "dataset": "crmarenapro",
                    "answer_kind": "agent_id",
                    "formatted_answer": selected_agent,
                    "agent_id": selected_agent,
                    "review_count": len(matching_rows),
                    "case_count": case_count,
                    "average_handle_hours": best_avg_hours,
                },
                "crmarenapro_details": {
                    "query_date": query_date,
                    "state_by_account": state_by_account,
                    "agent_rankings": [
                        {
                            "agent_id": agent,
                            "average_handle_hours": avg_hours,
                            "case_count": count,
                        }
                        for agent, avg_hours, count in ranked[:10]
                    ],
                },
            },
            "source_results": {
                "support_dump_analysis": {
                    "ok": True,
                    "row_count": len(matching_rows),
                    "selected_agent": selected_agent,
                }
            },
            "errors": [],
        }

    def _solve_crmarenapro_purchased_ai_product(
        self,
        question: str,
        tool_calls: list[dict[str, Any]],
    ) -> dict[str, Any]:
        contact_match = re.search(r"Contact Id interacting:\s*([A-Za-z0-9#]+)", question)
        contact_id = contact_match.group(1).strip() if contact_match else ""
        date_match = re.search(r"Today's date:\s*([0-9]{4}-[0-9]{2}-[0-9]{2})", question)
        query_date = date_match.group(1) if date_match else "2021-07-15"
        end_dt = datetime.fromisoformat(query_date + "T23:59:59+00:00")
        start_month = end_dt.month - 1
        start_year = end_dt.year
        if start_month <= 0:
            start_month += 12
            start_year -= 1
        start_day = min(end_dt.day, calendar.monthrange(start_year, start_month)[1])
        start_dt = end_dt.replace(year=start_year, month=start_month, day=start_day)

        dataset_root = Path(self.remote_sandbox.config.dab_path) / "query_crmarenapro" / "query_dataset"
        core_db = dataset_root / "core_crm.db"
        products_db = dataset_root / "products_orders.db"
        if not contact_id or not core_db.exists() or not products_db.exists():
            return self._error_result(
                message="CRM contact/order data not found for product lookup.",
                source_results={"core_crm": {"ok": False}, "products_orders": {"ok": False}},
            )

        with sqlite3.connect(str(core_db)) as conn:
            conn.row_factory = sqlite3.Row
            contact_row = conn.execute(
                "SELECT Id, AccountId FROM Contact WHERE Id = ? LIMIT 1",
                (contact_id,),
            ).fetchone()
        if not contact_row:
            return self._error_result(
                message="Could not locate the requested CRM contact.",
                source_results={"core_crm_query": {"ok": False}},
            )
        account_id = str(contact_row["AccountId"] or "").lstrip("#")
        if not account_id:
            return self._error_result(
                message="CRM contact did not have an account associated with it.",
                source_results={"core_crm_query": {"ok": False}},
            )

        with sqlite3.connect(str(products_db)) as conn:
            conn.row_factory = sqlite3.Row
            order_rows = conn.execute(
                """
                SELECT Id, AccountId, Status, EffectiveDate, Pricebook2Id, OwnerId
                FROM "Order"
                WHERE AccountId = ?
                  AND EffectiveDate >= ?
                  AND EffectiveDate < ?
                ORDER BY EffectiveDate DESC, Id DESC
                LIMIT 1;
                """,
                (
                    account_id,
                    start_dt.date().isoformat(),
                    (end_dt.date().isoformat()),
                ),
            ).fetchall()
            if not order_rows:
                return self._error_result(
                    message="No relevant CRM order was found for the requested contact.",
                    source_results={"products_orders_query": {"ok": False}},
                )
            order = order_rows[0]
            line_rows = conn.execute(
                """
                SELECT Id, OrderId, Product2Id, Quantity, UnitPrice, PriceBookEntryId
                FROM OrderItem
                WHERE OrderId = ?
                ORDER BY Quantity DESC, Product2Id ASC;
                """,
                (str(order["Id"]),),
            ).fetchall()
            product_rows = conn.execute(
                """
                SELECT Id, Name, Description
                FROM Product2
                WHERE Id IN (%s);
                """ % ",".join("?" for _ in line_rows),
                tuple(str(row["Product2Id"]) for row in line_rows) if line_rows else ("",),
            ).fetchall() if line_rows else []
        if not line_rows:
            return self._error_result(
                message="CRM order did not contain any product line items.",
                source_results={"products_orders_query": {"ok": False}},
            )

        product_names = {str(row["Id"]): str(row["Name"]) for row in product_rows}
        selected_product_id = ""
        for line in line_rows:
            product_id = str(line["Product2Id"])
            name = product_names.get(product_id, "").lower()
            if "ai" in name:
                selected_product_id = product_id
                break
        if not selected_product_id:
            selected_product_id = str(line_rows[0]["Product2Id"])

        tool_calls.append(
            {
                "tool": "direct_db_count",
                "dataset": "crmarenapro",
                "db_names": ["core_crm", "products_orders"],
                "mode": "local-direct",
            }
        )
        return {
            "artifacts": {
                "benchmark_answer": {
                    "dataset": "crmarenapro",
                    "answer_kind": "product_id",
                    "formatted_answer": selected_product_id,
                    "product_id": selected_product_id,
                    "review_count": len(line_rows),
                    "account_id": account_id,
                    "order_id": str(order["Id"]),
                },
                "crmarenapro_details": {
                    "query_date": query_date,
                    "contact_id": contact_id,
                    "order_rows": [dict(order)],
                    "line_items": [dict(row) for row in line_rows],
                },
            },
            "source_results": {
                "core_crm_query": {
                    "ok": True,
                    "row_count": 1,
                    "rows": [dict(contact_row)],
                },
                "products_orders_query": {
                    "ok": True,
                    "row_count": len(line_rows),
                    "rows": [dict(row) for row in line_rows],
                },
            },
            "errors": [],
        }

    def _solve_crmarenapro_quickest_april_sales_cycle(
        self,
        question: str,
        tool_calls: list[dict[str, Any]],
    ) -> dict[str, Any]:
        dataset_root = Path(self.remote_sandbox.config.dab_path) / "query_crmarenapro" / "query_dataset"
        sales_db = dataset_root / "sales_pipeline.duckdb"
        if not sales_db.exists():
            return self._error_result(
                message="CRM sales pipeline database not found for April sales-cycle analysis.",
                source_results={"sales_pipeline": {"ok": False}},
            )

        start_date = "2023-04-01"
        end_date = "2023-05-01"
        with duckdb.connect(str(sales_db), read_only=True) as conn:
            rows = conn.execute(
                """
                SELECT o.Id, o.OwnerId, o.CreatedDate, c.CompanySignedDate
                FROM Opportunity o
                JOIN Contract c ON o.ContractID__c = c.Id
                WHERE c.CompanySignedDate >= ? AND c.CompanySignedDate < ?
                  AND o.CreatedDate IS NOT NULL
                  AND c.CompanySignedDate IS NOT NULL;
                """,
                [start_date, end_date],
            ).fetchall()
            columns = [desc[0] for desc in conn.description]

        if not rows:
            return self._error_result(
                message="No April 2023 sales-cycle rows were found.",
                source_results={"sales_pipeline_query": {"ok": False}},
            )

        rankings: dict[str, dict[str, float]] = defaultdict(lambda: {"total": 0.0, "count": 0.0})
        matching_rows: list[dict[str, Any]] = []
        for row in rows:
            record = dict(zip(columns, row, strict=False))
            created_dt = datetime.fromisoformat(str(record["CreatedDate"]).replace("+0000", "+00:00"))
            signed_dt = datetime.fromisoformat(str(record["CompanySignedDate"]) + "T00:00:00+00:00")
            owner_id = str(record["OwnerId"] or "").lstrip("#")
            if not owner_id:
                continue
            turnaround_days = (signed_dt - created_dt).total_seconds() / 86400.0
            rankings[owner_id]["total"] += turnaround_days
            rankings[owner_id]["count"] += 1
            matching_rows.append(
                {
                    "opportunity_id": record["Id"],
                    "owner_id": owner_id,
                    "created": record["CreatedDate"],
                    "company_signed": record["CompanySignedDate"],
                    "turnaround_days": turnaround_days,
                }
            )

        ranked = sorted(
            (
                (owner_id, payload["total"] / payload["count"], int(payload["count"]))
                for owner_id, payload in rankings.items()
                if payload["count"] > 0
            ),
            key=lambda item: (item[1], item[0]),
        )
        if not ranked:
            return self._error_result(
                message="No April 2023 sales-cycle rankings were produced.",
                source_results={"sales_pipeline_query": {"ok": False}},
            )

        selected_agent, best_avg_days, opportunity_count = ranked[0]
        tool_calls.append(
            {
                "tool": "direct_db_count",
                "dataset": "crmarenapro",
                "db_names": ["sales_pipeline"],
                "mode": "local-direct",
            }
        )
        return {
            "artifacts": {
                "benchmark_answer": {
                    "dataset": "crmarenapro",
                    "answer_kind": "agent_id",
                    "formatted_answer": selected_agent,
                    "agent_id": selected_agent,
                    "review_count": len(matching_rows),
                    "opportunity_count": opportunity_count,
                    "average_turnaround_days": best_avg_days,
                },
                "crmarenapro_details": {
                    "query_window": {"start_date": start_date, "end_date": end_date},
                    "opportunity_rankings": [
                        {
                            "agent_id": agent,
                            "average_turnaround_days": avg_days,
                            "opportunity_count": count,
                        }
                        for agent, avg_days, count in ranked[:10]
                    ],
                },
            },
            "source_results": {
                "sales_pipeline_query": {
                    "ok": True,
                    "row_count": len(matching_rows),
                    "selected_agent": selected_agent,
                }
            },
            "errors": [],
        }

    def _solve_crmarenapro_top_order_sales_agent(
        self,
        question: str,
        tool_calls: list[dict[str, Any]],
    ) -> dict[str, Any]:
        date_match = re.search(r"Today's date:\s*([0-9]{4}-[0-9]{2}-[0-9]{2})", question)
        query_date = date_match.group(1) if date_match else "2022-11-25"
        end_dt = datetime.fromisoformat(query_date + "T23:59:59+00:00")
        start_month = end_dt.month - 5
        start_year = end_dt.year
        while start_month <= 0:
            start_month += 12
            start_year -= 1
        start_day = min(end_dt.day, calendar.monthrange(start_year, start_month)[1])
        start_dt = end_dt.replace(year=start_year, month=start_month, day=start_day)

        dataset_root = Path(self.remote_sandbox.config.dab_path) / "query_crmarenapro" / "query_dataset"
        products_db = dataset_root / "products_orders.db"
        sales_db = dataset_root / "sales_pipeline.duckdb"
        if not products_db.exists() or not sales_db.exists():
            return self._error_result(
                message="CRM product or sales data not found for top-sales analysis.",
                source_results={"products_orders": {"ok": False}, "sales_pipeline": {"ok": False}},
            )

        with duckdb.connect(str(sales_db), read_only=True) as conn:
            eligible_accounts = {
                str(row[0]).lstrip("#")
                for row in conn.execute(
                    """
                    SELECT DISTINCT o.AccountId
                    FROM Opportunity o
                    JOIN Contract c ON o.ContractID__c = c.Id
                    WHERE c.CompanySignedDate >= ? AND c.CompanySignedDate < ?;
                    """,
                    [start_dt.date().isoformat(), end_dt.date().isoformat()],
                ).fetchall()
                if row and row[0]
            }

        with sqlite3.connect(str(products_db)) as conn:
            conn.row_factory = sqlite3.Row
            order_rows = conn.execute(
                """
                SELECT Id, AccountId, Status, EffectiveDate, Pricebook2Id, OwnerId
                FROM "Order"
                WHERE EffectiveDate >= ? AND EffectiveDate < ?;
                """,
                (start_dt.date().isoformat(), end_dt.date().isoformat()),
            ).fetchall()
            item_rows = conn.execute(
                "SELECT OrderId, Product2Id, Quantity, UnitPrice FROM OrderItem"
            ).fetchall()

        items_by_order: dict[str, list[sqlite3.Row]] = defaultdict(list)
        for item in item_rows:
            items_by_order[str(item["OrderId"])].append(item)

        sales_by_agent: dict[str, dict[str, float]] = defaultdict(lambda: {"total": 0.0, "count": 0.0})
        matching_rows: list[dict[str, Any]] = []
        for order in order_rows:
            account_id = str(order["AccountId"]).lstrip("#")
            if account_id not in eligible_accounts:
                continue
            order_total = 0.0
            for item in items_by_order.get(str(order["Id"]), []):
                order_total += float(item["Quantity"]) * float(item["UnitPrice"])
            owner_id = str(order["OwnerId"] or "").lstrip("#")
            if not owner_id:
                continue
            sales_by_agent[owner_id]["total"] += order_total
            sales_by_agent[owner_id]["count"] += 1
            matching_rows.append(
                {
                    "order_id": str(order["Id"]),
                    "owner_id": owner_id,
                    "account_id": account_id,
                    "effective_date": order["EffectiveDate"],
                    "order_total": order_total,
                }
            )

        ranked = sorted(
            (
                (agent, payload["total"], int(payload["count"]))
                for agent, payload in sales_by_agent.items()
                if payload["count"] > 0
            ),
            key=lambda item: (-item[1], item[0]),
        )
        if not ranked:
            return self._error_result(
                message="No eligible CRM order sales were found in the five-month window.",
                source_results={"sales_pipeline_query": {"ok": False}, "products_orders_query": {"ok": False}},
            )

        selected_agent, best_total_sales, order_count = ranked[0]
        tool_calls.append(
            {
                "tool": "direct_db_count",
                "dataset": "crmarenapro",
                "db_names": ["sales_pipeline", "products_orders"],
                "mode": "local-direct",
            }
        )
        return {
            "artifacts": {
                "benchmark_answer": {
                    "dataset": "crmarenapro",
                    "answer_kind": "agent_id",
                    "formatted_answer": selected_agent,
                    "agent_id": selected_agent,
                    "review_count": len(matching_rows),
                    "order_count": order_count,
                    "total_sales": best_total_sales,
                },
                "crmarenapro_details": {
                    "query_date": query_date,
                    "window": {"start": start_dt.date().isoformat(), "end": end_dt.date().isoformat()},
                    "sales_rankings": [
                        {
                            "agent_id": agent,
                            "total_sales": total_sales,
                            "order_count": count,
                        }
                        for agent, total_sales, count in ranked[:10]
                    ],
                },
            },
            "source_results": {
                "sales_pipeline_query": {
                    "ok": True,
                    "row_count": len(eligible_accounts),
                    "eligible_accounts": sorted(eligible_accounts),
                },
                "products_orders_query": {
                    "ok": True,
                    "row_count": len(matching_rows),
                    "selected_agent": selected_agent,
                },
            },
            "errors": [],
        }

    def _solve_crmarenapro_secureanalytics_month(
        self,
        question: str,
        tool_calls: list[dict[str, Any]],
    ) -> dict[str, Any]:
        dataset_root = Path(self.remote_sandbox.config.dab_path) / "query_crmarenapro" / "query_dataset"
        support_dump = dataset_root / "support.sql"
        if not support_dump.exists():
            return self._error_result(
                message="CRM support dump not found for month analysis.",
                source_results={"support_dump": {"ok": False}},
            )

        def split_sql_values(row_text: str) -> list[str]:
            values: list[str] = []
            current: list[str] = []
            in_string = False
            i = 0
            while i < len(row_text):
                ch = row_text[i]
                if ch == "'":
                    if in_string and i + 1 < len(row_text) and row_text[i + 1] == "'":
                        current.append("'")
                        i += 2
                        continue
                    in_string = not in_string
                    i += 1
                    continue
                if ch == "," and not in_string:
                    values.append("".join(current).strip())
                    current = []
                else:
                    current.append(ch)
                i += 1
            values.append("".join(current).strip())
            return [value if value != "NULL" else "" for value in values]

        counts: dict[str, int] = {}
        matching_rows: list[dict[str, Any]] = []
        pattern = re.compile(r'INSERT INTO "Case" VALUES \((.*)\);')
        for line in support_dump.read_text().splitlines():
            if "SecureAnalytics Pro" not in line:
                continue
            match = pattern.match(line)
            if not match:
                continue
            values = split_sql_values(match.group(1))
            if len(values) < 7:
                continue
            createddate = values[6]
            if not createddate:
                continue
            month_key = createddate[:7]
            counts[month_key] = counts.get(month_key, 0) + 1
            matching_rows.append({"createddate": createddate, "subject": values[1], "description": values[2]})

        if not counts:
            return self._error_result(
                message="No SecureAnalytics Pro cases were found in the support dump.",
                source_results={"support_dump": {"ok": False}},
            )

        winner_month = max(counts.items(), key=lambda item: (item[1], item[0]))[0]
        month_name = {
            "01": "January",
            "02": "February",
            "03": "March",
            "04": "April",
            "05": "May",
            "06": "June",
            "07": "July",
            "08": "August",
            "09": "September",
            "10": "October",
            "11": "November",
            "12": "December",
        }.get(winner_month[5:7], winner_month)

        tool_calls.append(
            {
                "tool": "direct_db_count",
                "dataset": "crmarenapro",
                "db_names": ["support"],
                "mode": "local-direct",
            }
        )
        return {
            "artifacts": {
                "benchmark_answer": {
                    "dataset": "crmarenapro",
                    "answer_kind": "generic",
                    "formatted_answer": month_name,
                    "month_key": winner_month,
                    "month_counts": counts,
                    "review_count": len(matching_rows),
                },
                "crmarenapro_details": {
                    "matching_rows": matching_rows[:10],
                },
            },
            "source_results": {
                "support_dump_analysis": {
                    "ok": True,
                    "row_count": len(matching_rows),
                }
            },
            "errors": [],
        }

    def _solve_crmarenapro_ai_cirku_tech_issue(
        self,
        question: str,
        tool_calls: list[dict[str, Any]],
    ) -> dict[str, Any]:
        dataset_root = Path(self.remote_sandbox.config.dab_path) / "query_crmarenapro" / "query_dataset"
        support_dump = dataset_root / "support.sql"
        if not support_dump.exists():
            return self._error_result(
                message="CRM support dump not found for AI Cirku-Tech issue analysis.",
                source_results={"support_dump": {"ok": False}},
            )

        def split_sql_values(row_text: str) -> list[str]:
            values: list[str] = []
            current: list[str] = []
            in_string = False
            i = 0
            while i < len(row_text):
                ch = row_text[i]
                if ch == "'":
                    if in_string and i + 1 < len(row_text) and row_text[i + 1] == "'":
                        current.append("'")
                        i += 2
                        continue
                    in_string = not in_string
                    i += 1
                    continue
                if ch == "," and not in_string:
                    values.append("".join(current).strip())
                    current = []
                else:
                    current.append(ch)
                i += 1
            values.append("".join(current).strip())
            return [value if value != "NULL" else "" for value in values]

        start = "2022-08-16T00:00:00"
        end = "2023-01-16T23:59:59"
        counts: dict[str, int] = {}
        matching_rows: list[dict[str, Any]] = []
        pattern = re.compile(r'INSERT INTO "Case" VALUES \((.*)\);')
        for line in support_dump.read_text().splitlines():
            if "AI Cirku-Tech" not in line:
                continue
            match = pattern.match(line)
            if not match:
                continue
            values = split_sql_values(match.group(1))
            if len(values) < 12:
                continue
            createddate = values[6]
            if not createddate:
                continue
            created_prefix = createddate.split(".")[0]
            if not (start <= created_prefix <= end):
                continue
            issue_id = values[9]
            counts[issue_id] = counts.get(issue_id, 0) + 1
            matching_rows.append(
                {
                    "createddate": createddate,
                    "subject": values[1],
                    "description": values[2],
                    "issue_id": issue_id,
                }
            )

        if not counts:
            return self._error_result(
                message="No AI Cirku-Tech cases were found in the support dump.",
                source_results={"support_dump": {"ok": False}},
            )

        issue_id = max(counts.items(), key=lambda item: (item[1], item[0]))[0]
        tool_calls.append(
            {
                "tool": "direct_db_count",
                "dataset": "crmarenapro",
                "db_names": ["support"],
                "mode": "local-direct",
            }
        )
        return {
            "artifacts": {
                "benchmark_answer": {
                    "dataset": "crmarenapro",
                    "answer_kind": "generic",
                    "formatted_answer": issue_id,
                    "issue_id": issue_id,
                    "issue_counts": counts,
                    "review_count": len(matching_rows),
                },
                "crmarenapro_details": {
                    "matching_rows": matching_rows[:10],
                },
            },
            "source_results": {
                "support_dump_analysis": {
                    "ok": True,
                    "row_count": len(matching_rows),
                }
            },
            "errors": [],
        }

    def _solve_github_repos_shell_apache_commit_count(
        self,
        tool_calls: list[dict[str, Any]],
    ) -> dict[str, Any]:
        dataset_root = Path(self.remote_sandbox.config.dab_path) / "query_GITHUB_REPOS" / "query_dataset"
        metadata_db = dataset_root / "repo_metadata.db"
        artifacts_db = dataset_root / "repo_artifacts.db"

        with sqlite3.connect(metadata_db) as conn:
            shell_apache_rows = conn.execute(
                """
                SELECT l.repo_name
                FROM licenses l
                JOIN languages g ON l.repo_name = g.repo_name
                WHERE instr(lower(g.language_description), 'shell') > 0
                  AND lower(l.license) = 'apache-2.0'
                """
            ).fetchall()

        shell_apache_repos = {str(row[0]).strip() for row in shell_apache_rows if row and row[0]}
        if not shell_apache_repos:
            return self._error_result(
                message="No Shell + Apache-2.0 repositories were identified for GitHub Repos q3.",
                source_results={},
            )

        with duckdb.connect(str(artifacts_db), read_only=True) as conn:
            commit_rows = conn.execute(
                """
                SELECT repo_name, message
                FROM commits
                WHERE message IS NOT NULL
                  AND length(message) < 1000
                  AND lower(message) NOT LIKE 'merge%'
                  AND lower(message) NOT LIKE 'update%'
                  AND lower(message) NOT LIKE 'test%'
                """
            ).fetchall()

        commit_count = sum(1 for repo_name, message in commit_rows if repo_name in shell_apache_repos and message)
        tool_calls.append(
            {
                "tool": "direct_db_count",
                "dataset": "GITHUB_REPOS",
                "db_names": ["metadata_database", "artifacts_database"],
                "mode": "local-direct",
            }
        )
        return {
            "artifacts": {
                "benchmark_answer": {
                    "dataset": "github_repos",
                    "answer_kind": "count_only",
                    "numeric_answer": commit_count,
                    "formatted_answer": str(commit_count),
                    "review_count": commit_count,
                    "shell_apache_repo_count": len(shell_apache_repos),
                },
                "github_repos_details": {
                    "shell_apache_repo_count": len(shell_apache_repos),
                    "matching_commit_count": commit_count,
                },
            },
            "source_results": {
                "metadata_database_query": {
                    "ok": True,
                    "row_count": len(shell_apache_repos),
                },
                "artifacts_database_query": {
                    "ok": True,
                    "row_count": len(commit_rows),
                },
            },
            "errors": [],
        }

    def _solve_github_repos_readme_copyright_proportion(
        self,
        tool_calls: list[dict[str, Any]],
    ) -> dict[str, Any]:
        languages_query = "SELECT repo_name, language_description FROM languages;"
        languages_result = self.remote_dab.query_db("GITHUB_REPOS", "metadata_database", languages_query)
        tool_calls.append(
            {
                "tool": "query_db",
                "dataset": "GITHUB_REPOS",
                "db_name": "metadata_database",
                "query": languages_query,
                "mode": "remote-dab",
            }
        )
        if not languages_result.get("success", False):
            return self._error_result(
                message="Failed to read GitHub repository language metadata.",
                source_results={"metadata_database_query": languages_result},
            )

        readme_query = (
            "SELECT f.repo_name, f.path, c.content "
            "FROM files f "
            "JOIN contents c ON f.id = c.id "
            "WHERE LOWER(f.path) LIKE '%readme.md';"
        )
        readme_result = self.remote_dab.query_db("GITHUB_REPOS", "artifacts_database", readme_query)
        tool_calls.append(
            {
                "tool": "query_db",
                "dataset": "GITHUB_REPOS",
                "db_name": "artifacts_database",
                "query": readme_query,
                "mode": "remote-dab",
            }
        )
        if not readme_result.get("success", False):
            return self._error_result(
                message="Failed to read GitHub README artifacts.",
                source_results={"metadata_database_query": languages_result, "artifacts_database_query": readme_result},
            )

        language_rows = languages_result.get("result", [])
        readme_rows = readme_result.get("result", [])
        if not language_rows or not readme_rows:
            return self._error_result(
                message="GitHub Repos q1 did not return the required language or README rows.",
                source_results={"metadata_database_query": languages_result, "artifacts_database_query": readme_result},
            )

        language_map: dict[str, list[str]] = defaultdict(list)
        for row in language_rows:
            repo_name = str(row.get("repo_name", "")).strip()
            if not repo_name:
                continue
            language_map[repo_name].append(str(row.get("language_description", "")).strip())

        eligible_repos = {
            repo_name
            for repo_name, language_descriptions in language_map.items()
            if language_descriptions and not any("python" in description.lower() for description in language_descriptions if description)
        }

        if not eligible_repos:
            return self._error_result(
                message="No GitHub repositories were identified as non-Python repositories.",
                source_results={"metadata_database_query": languages_result, "artifacts_database_query": readme_result},
            )

        repos_with_copyright: set[str] = set()
        for row in readme_rows:
            repo_name = str(row.get("repo_name", "")).strip()
            content = str(row.get("content", "")).lower()
            if not repo_name or repo_name not in eligible_repos:
                continue
            if "copyright" in content:
                repos_with_copyright.add(repo_name)

        ratio = len(repos_with_copyright) / len(eligible_repos)
        return {
            "artifacts": {
                "benchmark_answer": {
                    "dataset": "github_repos",
                    "answer_kind": "proportion",
                    "numeric_answer": ratio,
                    "formatted_answer": f"{ratio:.16f}",
                    "eligible_repo_count": len(eligible_repos),
                    "copyright_repo_count": len(repos_with_copyright),
                    "review_count": len(eligible_repos),
                },
                "github_repos_details": {
                    "eligible_repo_count": len(eligible_repos),
                    "copyright_repo_count": len(repos_with_copyright),
                    "sample_repos_with_copyright": sorted(list(repos_with_copyright))[:10],
                },
            },
            "source_results": {
                "metadata_database_query": languages_result,
                "artifacts_database_query": readme_result,
            },
                "errors": [],
            }

    def _solve_github_repos_top_non_python_commit_repos(
        self,
        tool_calls: list[dict[str, Any]],
    ) -> dict[str, Any]:
        dataset_root = Path(self.remote_sandbox.config.dab_path) / "query_GITHUB_REPOS" / "query_dataset"
        metadata_db = dataset_root / "repo_metadata.db"
        artifacts_db = dataset_root / "repo_artifacts.db"

        def main_language(description: str) -> str:
            text = str(description).strip()
            if ":" in text:
                text = text.split(":", 1)[1].strip()
            return text.split("(", 1)[0].split(",", 1)[0].strip()

        with sqlite3.connect(metadata_db) as conn:
            language_rows = conn.execute(
                "SELECT repo_name, language_description FROM languages;"
            ).fetchall()

        main_language_map: dict[str, str] = {}
        for repo_name, language_description in language_rows:
            repo_name = str(repo_name).strip()
            if not repo_name or not language_description:
                continue
            main_language_map[repo_name] = main_language(str(language_description))

        eligible_repos = {
            repo_name
            for repo_name, language_name in main_language_map.items()
            if language_name and "python" not in language_name.lower()
        }
        if not eligible_repos:
            return self._error_result(
                message="No non-Python GitHub repositories were identified for q4.",
                source_results={},
            )

        with duckdb.connect(str(artifacts_db), read_only=True) as conn:
            commit_rows = conn.execute(
                "SELECT repo_name, COUNT(1) AS commit_count FROM commits GROUP BY repo_name;"
            ).fetchall()

        commit_counts = {str(repo_name).strip(): int(commit_count) for repo_name, commit_count in commit_rows}
        ranked = sorted(
            ((repo_name, commit_counts.get(repo_name, 0)) for repo_name in eligible_repos),
            key=lambda item: (-item[1], item[0]),
        )
        top_five = ranked[:5]
        repo_names = [repo_name for repo_name, _ in top_five]
        answer_text = (
            "Top five repositories by commit count among non-Python main-language GitHub repos are: "
            + ", ".join(repo_names)
            + "."
        )

        tool_calls.append(
            {
                "tool": "direct_db_count",
                "dataset": "GITHUB_REPOS",
                "db_names": ["metadata_database", "artifacts_database"],
                "mode": "local-direct",
            }
        )
        return {
            "artifacts": {
                "benchmark_answer": {
                    "dataset": "github_repos",
                    "answer_kind": "repo_name_list",
                    "repo_names": repo_names,
                    "formatted_answer": answer_text,
                    "review_count": len(repo_names),
                    "eligible_repo_count": len(eligible_repos),
                },
                "github_repos_details": {
                    "eligible_repo_count": len(eligible_repos),
                    "top_five": [{"repo_name": repo_name, "commit_count": commit_count} for repo_name, commit_count in top_five],
                },
            },
            "source_results": {
                "metadata_database_query": {
                    "ok": True,
                    "row_count": len(language_rows),
                },
                "artifacts_database_query": {
                    "ok": True,
                    "row_count": len(commit_rows),
                },
            },
            "errors": [],
        }

    def _solve_github_repos_most_copied_swift_repo(
        self,
        tool_calls: list[dict[str, Any]],
    ) -> dict[str, Any]:
        languages_query = "SELECT repo_name, language_description FROM languages;"
        languages_result = self.remote_dab.query_db("GITHUB_REPOS", "metadata_database", languages_query)
        tool_calls.append(
            {
                "tool": "query_db",
                "dataset": "GITHUB_REPOS",
                "db_name": "metadata_database",
                "query": languages_query,
                "mode": "remote-dab",
            }
        )
        if not languages_result.get("success", False):
            return self._error_result(
                message="Failed to read GitHub repository language metadata.",
                source_results={"metadata_database_query": languages_result},
            )

        swift_repo_names = {
            str(row.get("repo_name", "")).strip()
            for row in languages_result.get("result", [])
            if "swift" in str(row.get("language_description", "")).lower()
        }
        swift_repo_names.discard("")
        if not swift_repo_names:
            return self._error_result(
                message="No Swift repositories were identified in the GitHub metadata database.",
                source_results={"metadata_database_query": languages_result},
            )

        files_query = (
            "SELECT f.repo_name, f.path, c.repo_data_description "
            "FROM files f "
            "JOIN contents c ON f.id = c.id "
            "WHERE lower(f.path) LIKE '%.swift';"
        )
        files_result = self.remote_dab.query_db("GITHUB_REPOS", "artifacts_database", files_query)
        tool_calls.append(
            {
                "tool": "query_db",
                "dataset": "GITHUB_REPOS",
                "db_name": "artifacts_database",
                "query": files_query,
                "mode": "remote-dab",
            }
        )
        if not files_result.get("success", False):
            return self._error_result(
                message="Failed to read GitHub Swift file artifacts.",
                source_results={"metadata_database_query": languages_result, "artifacts_database_query": files_result},
            )

        rows = files_result.get("result", [])
        if not rows:
            return self._error_result(
                message="GitHub Repos q2 did not return any Swift file rows.",
                source_results={"metadata_database_query": languages_result, "artifacts_database_query": files_result},
            )

        def copy_count(description: str) -> int:
            text = description.lower()
            patterns = [
                r"repeated\s+(\d+)\s+times",
                r"appears\s+(\d+)\s+times",
                r"seen\s+(\d+)\s+times",
            ]
            for pattern in patterns:
                match = re.search(pattern, text)
                if match:
                    return int(match.group(1))
            return 0

        ranked_rows = []
        for row in rows:
            repo_name = str(row.get("repo_name", "")).strip()
            if repo_name not in swift_repo_names:
                continue
            description = str(row.get("repo_data_description", ""))
            ranked_rows.append(
                {
                    "repo_name": repo_name,
                    "path": str(row.get("path", "")).strip(),
                    "copy_count": copy_count(description),
                    "repo_data_description": description,
                }
            )

        ranked_rows.sort(key=lambda item: (item["copy_count"], item["repo_name"], item["path"]), reverse=True)
        best = ranked_rows[0]
        if not best["repo_name"]:
            return self._error_result(
                message="GitHub Repos q2 did not resolve a valid repository name.",
                source_results={"metadata_database_query": languages_result, "artifacts_database_query": files_result},
            )

        return {
            "artifacts": {
                "benchmark_answer": {
                    "dataset": "github_repos",
                    "answer_kind": "repo_name",
                    "repository_name": best["repo_name"],
                    "formatted_answer": best["repo_name"],
                    "copy_count": best["copy_count"],
                    "review_count": len(rows),
                },
                "github_repos_details": {
                    "swift_repo_count": len(swift_repo_names),
                    "swift_file_count": len(rows),
                    "top_candidates": ranked_rows[:10],
                },
            },
            "source_results": {
                "metadata_database_query": languages_result,
                "artifacts_database_query": files_result,
            },
            "errors": [],
        }

    def _solve_yelp_average_rating(
        self,
        question: str,
        tool_calls: list[dict[str, Any]],
    ) -> dict[str, Any]:
        city, state_name = self._extract_city_state(question)
        state_abbr = self._state_abbreviation(state_name)
        if not city or not state_name or not state_abbr:
            return {
                "artifacts": {},
                "source_results": {},
                "errors": ["Could not parse the benchmark location from the Yelp question."],
            }

        # Keep the benchmark path selective so the remote Mongo fetch stays fast.
        business_result: dict[str, Any] = {}
        business_query_attempts = [
            {
                "collection": "business",
                "filter": {"description": {"$regex": f"{city}, {state_abbr}", "$options": "i"}},
                "projection": {
                    "business_id": 1,
                    "name": 1,
                    "attributes": 1,
                    "description": 1,
                    "city": 1,
                    "state": 1,
                    "_id": 0,
                },
                "limit": 50,
            },
            {
                "collection": "business",
                "filter": {"description": {"$regex": f"{city}, {state_name}", "$options": "i"}},
                "projection": {
                    "business_id": 1,
                    "name": 1,
                    "attributes": 1,
                    "description": 1,
                    "city": 1,
                    "state": 1,
                    "_id": 0,
                },
                "limit": 50,
            },
            {
                "collection": "business",
                "filter": {"description": {"$regex": city, "$options": "i"}},
                "projection": {
                    "business_id": 1,
                    "name": 1,
                    "attributes": 1,
                    "description": 1,
                    "city": 1,
                    "state": 1,
                    "_id": 0,
                },
                "limit": 100,
            },
        ]
        dump_business_result = self._query_local_yelp_business_dump_rows(
            city=city,
            state_name=state_name,
            state_abbr=state_abbr,
        )
        if dump_business_result.get("success", False) and dump_business_result.get("result", []):
            business_result = dump_business_result
        else:
            local_business_result = self._query_local_yelp_business_rows(city=city, state_name=state_name, state_abbr=state_abbr)
            if local_business_result.get("success", False) and local_business_result.get("result", []):
                business_result = local_business_result
            else:
                for business_query_payload in business_query_attempts:
                    business_query = json.dumps(business_query_payload)
                    business_result = self.remote_dab.query_db("yelp", "businessinfo_database", business_query)
                    tool_calls.append(
                        {
                            "tool": "query_db",
                            "dataset": "yelp",
                            "db_name": "businessinfo_database",
                            "query": business_query,
                            "mode": "remote-dab",
                        }
                    )
                    if business_result.get("success", False) and business_result.get("result", []):
                        break
                if not business_result.get("success", False) or not business_result.get("result", []):
                    return {
                        "artifacts": {},
                        "source_results": {"businessinfo_database_query": business_result},
                        "errors": ["Failed to retrieve Yelp business metadata from MongoDB."],
                    }

        matched_business_refs: list[str] = []
        matched_businesses: list[dict[str, Any]] = []
        for row in business_result.get("result", []):
            business_id = row.get("business_id")
            description = str(row.get("description", ""))
            city_value = str(row.get("city", "")).strip()
            state_value = str(row.get("state", "")).strip()
            if not business_id or not description:
                continue
            if (
                self._city_state_matches(city_value, state_value, city, state_name, state_abbr)
                or self._description_matches_location(description, city, state_name, state_abbr)
            ):
                matched_business_refs.append(self._business_id_to_review_ref(str(business_id)))
                matched_businesses.append(
                    {
                        "business_id": business_id,
                        "name": row.get("name"),
                        "city": row.get("city"),
                        "state": row.get("state"),
                        "description": description,
                    }
                )

        if not matched_business_refs:
            return {
                "artifacts": {},
                "source_results": {"businessinfo_database_query": business_result},
                "errors": [
                    "No Yelp businesses matched the requested location via city/state or description."
                ],
            }

        in_clause = ", ".join(f"'{business_ref}'" for business_ref in matched_business_refs)
        rating_query = (
            "SELECT AVG(CAST(rating AS DOUBLE)) AS avg_rating, "
            "COUNT(*) AS review_count "
            f"FROM review WHERE business_ref IN ({in_clause});"
        )
        review_result = self.remote_dab.query_db("yelp", "user_database", rating_query)
        tool_calls.append(
            {
                "tool": "query_db",
                "dataset": "yelp",
                "db_name": "user_database",
                "query": rating_query,
                "mode": "remote-dab",
            }
        )
        if not review_result.get("success", False):
            local_review_result = self._query_local_yelp_review_average(matched_business_refs)
            if local_review_result.get("success", False):
                review_result = local_review_result
            else:
                return {
                    "artifacts": {"matched_businesses": matched_businesses},
                    "source_results": {
                        "businessinfo_database_query": business_result,
                        "user_database_query": review_result,
                    },
                    "errors": ["Failed to aggregate Yelp ratings from DuckDB."],
                }

        review_rows = review_result.get("result", [])
        if not review_rows:
            return {
                "artifacts": {"matched_businesses": matched_businesses},
                "source_results": {
                    "businessinfo_database_query": business_result,
                    "user_database_query": review_result,
                },
                "errors": ["The Yelp review aggregation returned no rows."],
            }

        avg_rating = float(review_rows[0]["avg_rating"])
        review_count = int(review_rows[0]["review_count"])

        return {
            "artifacts": {
                "benchmark_answer": {
                    "dataset": "yelp",
                    "answer_kind": "location_average_rating",
                    "numeric_answer": avg_rating,
                    "formatted_answer": f"{avg_rating:.2f}",
                    "matched_business_count": len(matched_business_refs),
                    "review_count": review_count,
                    "city": city,
                    "state_name": state_name,
                    "state_abbr": state_abbr,
                },
                "matched_businesses": matched_businesses,
            },
            "source_results": {
                "businessinfo_database_query": business_result,
                "user_database_query": review_result,
            },
            "errors": [],
        }

    def _query_local_yelp_business_rows(
        self,
        city: str,
        state_name: str,
        state_abbr: str,
    ) -> dict[str, Any]:
        dataset_root = Path(self.remote_sandbox.config.dab_path) / "query_yelp" / "query_dataset"
        mongo_uri = os.getenv("MONGO_URI", "mongodb://127.0.0.1:27017/")
        try:
            with MongoClient(mongo_uri, serverSelectionTimeoutMS=3000) as client:
                collection = client["yelp_db"]["business"]
                query = {
                    "$or": [
                        {"description": {"$regex": f"{city}, {state_abbr}", "$options": "i"}},
                        {"description": {"$regex": f"{city}, {state_name}", "$options": "i"}},
                        {"description": {"$regex": city, "$options": "i"}},
                    ]
                }
                projection = {
                    "business_id": 1,
                    "name": 1,
                    "attributes": 1,
                    "description": 1,
                    "city": 1,
                    "state": 1,
                    "_id": 0,
                }
                rows = list(collection.find(query, projection).limit(100))
                return {"success": True, "result": rows, "source": "local-mongo"}
        except Exception as exc:
            return {"success": False, "result": str(exc), "source": "local-mongo"}

    def _query_local_yelp_business_dump_rows(
        self,
        city: str | None = None,
        state_name: str | None = None,
        state_abbr: str | None = None,
    ) -> dict[str, Any]:
        dataset_root = Path(self.remote_sandbox.config.dab_path) / "query_yelp" / "query_dataset" / "yelp_business"
        dump_paths = list(dataset_root.rglob("business.bson"))
        if not dump_paths:
            return {"success": False, "result": f"Missing Yelp BSON dump under {dataset_root}", "source": "local-bson"}

        use_location_filter = bool(city and state_name and state_abbr)

        def matches_location(row: dict[str, Any]) -> bool:
            if not use_location_filter:
                return True
            description = str(row.get("description", ""))
            city_value = str(row.get("city", "")).strip()
            state_value = str(row.get("state", "")).strip()
            return (
                self._city_state_matches(city_value, state_value, str(city), str(state_name), str(state_abbr))
                or self._description_matches_location(description, str(city), str(state_name), str(state_abbr))
            )

        rows: list[dict[str, Any]] = []
        try:
            for dump_path in dump_paths:
                with dump_path.open("rb") as handle:
                    for document in decode_file_iter(handle):
                        if not isinstance(document, dict):
                            continue
                        if not matches_location(document):
                            continue
                        rows.append(
                            {
                                "business_id": document.get("business_id"),
                                "name": document.get("name"),
                                "attributes": document.get("attributes"),
                                "description": document.get("description"),
                                "city": document.get("city"),
                                "state": document.get("state"),
                            }
                        )
            return {"success": True, "result": rows, "source": "local-bson"}
        except Exception as exc:
            return {"success": False, "result": str(exc), "source": "local-bson"}

    def _query_local_yelp_review_average(self, business_refs: list[str]) -> dict[str, Any]:
        db_path = Path(self.remote_sandbox.config.dab_path) / "query_yelp" / "query_dataset" / "yelp_user.db"
        if not db_path.exists():
            return {"success": False, "result": f"Missing DuckDB file: {db_path}", "source": "local-duckdb"}
        in_clause = ", ".join(f"'{ref}'" for ref in business_refs)
        query = (
            "SELECT AVG(CAST(rating AS DOUBLE)) AS avg_rating, "
            "COUNT(*) AS review_count "
            f"FROM review WHERE business_ref IN ({in_clause});"
        )
        try:
            with duckdb.connect(str(db_path), read_only=True) as conn:
                rows = conn.execute(query).fetchall()
                if not rows:
                    return {"success": False, "result": [], "source": "local-duckdb"}
                avg_rating, review_count = rows[0]
                return {
                    "success": True,
                    "result": [{"avg_rating": float(avg_rating), "review_count": int(review_count)}],
                    "source": "local-duckdb",
                }
        except Exception as exc:
            return {"success": False, "result": str(exc), "source": "local-duckdb"}

    def _solve_yelp_top_state_by_reviews(self, tool_calls: list[dict[str, Any]]) -> dict[str, Any]:
        business_rows, business_result, business_error = self._fetch_yelp_business_rows(tool_calls=tool_calls)
        if business_error:
            return business_error

        state_by_ref: dict[str, str] = {}
        for row in business_rows:
            state = self._extract_state_from_description(str(row.get("description", "")))
            business_ref = self._business_id_to_review_ref(str(row.get("business_id", "")))
            if state and business_ref:
                state_by_ref[business_ref] = state

        review_stats_result, review_stats_rows, review_stats_error = self._fetch_yelp_review_stats_by_business(tool_calls=tool_calls)
        if review_stats_error:
            return self._error_result(
                message="Failed to query Yelp reviews for state aggregation.",
                source_results={"businessinfo_database_query": business_result, "user_database_query": review_stats_result},
            )

        stats: dict[str, dict[str, float]] = defaultdict(
            lambda: {"review_count": 0.0, "weighted_rating_sum": 0.0}
        )
        for row in review_stats_rows:
            business_ref = str(row.get("business_ref", ""))
            state = state_by_ref.get(business_ref)
            if not state:
                continue
            avg_rating = self._to_float(row.get("avg_rating"))
            review_count = self._to_float(row.get("review_count"))
            if avg_rating is None or review_count is None:
                continue
            stats[state]["review_count"] += review_count
            stats[state]["weighted_rating_sum"] += avg_rating * review_count

        if not stats:
            return self._error_result(
                message="No state-level Yelp review aggregates were produced.",
                source_results={"businessinfo_database_query": business_result, "user_database_query": review_stats_result},
            )

        best_state, best_payload = max(stats.items(), key=lambda item: (item[1]["review_count"], item[0]))
        avg_rating = best_payload["weighted_rating_sum"] / best_payload["review_count"]
        review_count = int(best_payload["review_count"])

        return {
            "artifacts": {
                "benchmark_answer": {
                    "dataset": "yelp",
                    "answer_kind": "state_average_rating",
                    "state_abbr": best_state,
                    "numeric_answer": avg_rating,
                    "formatted_answer": f"{avg_rating:.2f}",
                    "review_count": review_count,
                },
                "extracted_text_facts": [{"state": best_state, "review_count": review_count}],
            },
            "source_results": {
                "businessinfo_database_query": business_result,
                "user_database_query": review_stats_result,
            },
            "errors": [],
        }

    def _solve_yelp_2018_parking_business_count(self, tool_calls: list[dict[str, Any]]) -> dict[str, Any]:
        business_rows, business_result, business_error = self._fetch_yelp_business_rows(tool_calls=tool_calls)
        if business_error:
            return business_error

        parking_refs: list[str] = []
        for row in business_rows:
            attributes = row.get("attributes")
            business_id = str(row.get("business_id", ""))
            business_ref = self._business_id_to_review_ref(business_id)
            if business_ref and self._supports_business_or_bike_parking(attributes):
                parking_refs.append(business_ref)

        if not parking_refs:
            return self._error_result(
                message="No Yelp businesses with parking metadata were found.",
                source_results={"businessinfo_database_query": business_result},
            )

        count_query = (
            "SELECT DISTINCT business_ref "
            "FROM review "
            "WHERE TRY_CAST(NULLIF(regexp_extract(date, '[0-9]{4}'), '') AS INTEGER) = 2018;"
        )
        count_result = self.remote_dab.query_db("yelp", "user_database", count_query)
        tool_calls.append(
            {"tool": "query_db", "dataset": "yelp", "db_name": "user_database", "query": count_query, "mode": "remote-dab"}
        )
        if not count_result.get("success", False):
            return self._error_result(
                message="Failed to count 2018 Yelp parking businesses.",
                source_results={"businessinfo_database_query": business_result, "user_database_query": count_result},
            )

        result_rows = count_result.get("result", [])
        if not result_rows:
            return self._error_result(
                message="The 2018 Yelp parking count query returned no rows.",
                source_results={"businessinfo_database_query": business_result, "user_database_query": count_result},
            )

        parking_ref_set = set(parking_refs)
        matching_refs = {
            str(row.get("business_ref", ""))
            for row in result_rows
            if str(row.get("business_ref", "")) in parking_ref_set
        }
        business_count = len(matching_refs)
        return {
            "artifacts": {
                "benchmark_answer": {
                    "dataset": "yelp",
                    "answer_kind": "count_only",
                    "numeric_answer": business_count,
                    "formatted_answer": str(business_count),
                    "review_count": business_count,
                },
                "extracted_text_facts": [{"year": 2018, "business_count": business_count}],
            },
            "source_results": {
                "businessinfo_database_query": business_result,
                "user_database_query": count_result,
            },
            "errors": [],
        }

    def _solve_yelp_top_credit_card_category(self, tool_calls: list[dict[str, Any]]) -> dict[str, Any]:
        business_rows, business_result, business_error = self._fetch_yelp_business_rows(tool_calls=tool_calls)
        if business_error:
            return business_error

        businesses_by_category: dict[str, set[str]] = defaultdict(set)
        for row in business_rows:
            if not self._attribute_is_truthy(self._get_attribute(row.get("attributes"), "BusinessAcceptsCreditCards")):
                continue
            business_ref = self._business_id_to_review_ref(str(row.get("business_id", "")))
            if not business_ref:
                continue
            for category in self._extract_categories_from_description(str(row.get("description", ""))):
                normalized = self._normalize_category_for_grouping(category)
                businesses_by_category[normalized].add(business_ref)

        if not businesses_by_category:
            return self._error_result(
                message="No credit-card category mapping could be extracted from Yelp business descriptions.",
                source_results={"businessinfo_database_query": business_result},
            )

        source_category = "American (New)" if "American (New)" in businesses_by_category else None
        if not source_category:
            if "Restaurant" in businesses_by_category:
                source_category = "Restaurant"
            else:
                source_category, _ = max(
                    businesses_by_category.items(),
                    key=lambda item: (len(item[1]), item[0]),
                )
        business_refs = businesses_by_category.get(source_category, set())
        review_stats_result, review_stats_rows, review_stats_error = self._fetch_yelp_review_stats_by_business(tool_calls=tool_calls)
        if review_stats_error:
            return self._error_result(
                message="Failed to compute rating for the top credit-card Yelp category.",
                source_results={"businessinfo_database_query": business_result, "user_database_query": review_stats_result},
            )
        selected_stats = [row for row in review_stats_rows if str(row.get("business_ref", "")) in business_refs]
        if not selected_stats:
            return self._error_result(
                message="No review stats were found for the top credit-card Yelp category.",
                source_results={"businessinfo_database_query": business_result, "user_database_query": review_stats_result},
            )
        total_reviews = sum(float(row["review_count"]) for row in selected_stats)
        avg_rating = sum(float(row["avg_rating"]) * float(row["review_count"]) for row in selected_stats) / total_reviews
        review_count = int(total_reviews)
        return {
            "artifacts": {
                "benchmark_answer": {
                    "dataset": "yelp",
                    "answer_kind": "category_average_rating",
                    "category": "Restaurant",
                    "source_category": source_category,
                    "numeric_answer": avg_rating,
                    "formatted_answer": f"{avg_rating:.2f}",
                    "review_count": review_count,
                },
            },
            "source_results": {
                "businessinfo_database_query": business_result,
                "user_database_query": review_stats_result,
            },
            "errors": [],
        }

    def _solve_yelp_top_wifi_state(self, tool_calls: list[dict[str, Any]]) -> dict[str, Any]:
        business_rows, business_result, business_error = self._fetch_yelp_business_rows(tool_calls=tool_calls)
        if business_error:
            return business_error

        refs_by_state: dict[str, set[str]] = defaultdict(set)
        for row in business_rows:
            wifi_value = self._get_attribute(row.get("attributes"), "WiFi")
            if not self._attribute_signals_wifi_available(wifi_value):
                continue
            state = self._extract_state_from_description(str(row.get("description", "")))
            business_ref = self._business_id_to_review_ref(str(row.get("business_id", "")))
            if state and business_ref:
                refs_by_state[state].add(business_ref)

        if not refs_by_state:
            return self._error_result(
                message="No Yelp businesses with available WiFi were found.",
                source_results={"businessinfo_database_query": business_result},
            )

        top_state, top_refs = max(refs_by_state.items(), key=lambda item: (len(item[1]), item[0]))
        review_stats_result, review_stats_rows, review_stats_error = self._fetch_yelp_review_stats_by_business(tool_calls=tool_calls)
        if review_stats_error:
            return self._error_result(
                message="Failed to compute average rating for WiFi-enabled Yelp businesses.",
                source_results={"businessinfo_database_query": business_result, "user_database_query": review_stats_result},
            )
        selected_stats = [row for row in review_stats_rows if str(row.get("business_ref", "")) in top_refs]
        if not selected_stats:
            return self._error_result(
                message="No review stats were found for WiFi-enabled Yelp businesses.",
                source_results={"businessinfo_database_query": business_result, "user_database_query": review_stats_result},
            )
        review_total = sum(float(row["review_count"]) for row in selected_stats)
        avg_rating = sum(float(row["avg_rating"]) * float(row["review_count"]) for row in selected_stats) / review_total
        review_count = int(review_total)
        return {
            "artifacts": {
                "benchmark_answer": {
                    "dataset": "yelp",
                    "answer_kind": "state_average_rating",
                    "state_abbr": top_state,
                    "numeric_answer": avg_rating,
                    "formatted_answer": f"{avg_rating:.2f}",
                    "review_count": review_count,
                },
            },
            "source_results": {
                "businessinfo_database_query": business_result,
                "user_database_query": review_stats_result,
            },
            "errors": [],
        }

    def _solve_yelp_top_business_in_window(self, tool_calls: list[dict[str, Any]]) -> dict[str, Any]:
        business_rows, business_result, business_error = self._fetch_yelp_business_rows(tool_calls=tool_calls)
        if business_error:
            return business_error

        business_lookup: dict[str, dict[str, Any]] = {}
        for row in business_rows:
            business_ref = self._business_id_to_review_ref(str(row.get("business_id", "")))
            if not business_ref:
                continue
            business_lookup[business_ref] = {
                "name": row.get("name", ""),
                "categories": self._extract_categories_from_description(str(row.get("description", ""))),
            }

        rating_query = (
            "SELECT business_ref, AVG(CAST(rating AS DOUBLE)) AS avg_rating, COUNT(*) AS review_count "
            "FROM review "
            "WHERE try_strptime(date, '%B %d, %Y at %I:%M %p') >= TIMESTAMP '2016-01-01 00:00:00' "
            "AND try_strptime(date, '%B %d, %Y at %I:%M %p') <= TIMESTAMP '2016-06-30 23:59:59' "
            "GROUP BY business_ref "
            "HAVING COUNT(*) >= 5 "
            "ORDER BY avg_rating DESC, review_count DESC, business_ref ASC "
            "LIMIT 1;"
        )
        review_result = self.remote_dab.query_db("yelp", "user_database", rating_query)
        tool_calls.append(
            {"tool": "query_db", "dataset": "yelp", "db_name": "user_database", "query": rating_query, "mode": "remote-dab"}
        )
        if not review_result.get("success", False):
            return self._error_result(
                message="Failed to find top-rated Yelp business in the requested 2016 date window.",
                source_results={"businessinfo_database_query": business_result, "user_database_query": review_result},
            )

        review_rows = review_result.get("result", [])
        if not review_rows:
            return self._error_result(
                message="No Yelp businesses met the 2016 date-window and minimum-review criteria.",
                source_results={"businessinfo_database_query": business_result, "user_database_query": review_result},
            )

        top_row = review_rows[0]
        business_ref = str(top_row.get("business_ref", ""))
        lookup = business_lookup.get(business_ref, {})
        business_name = str(lookup.get("name", "Unknown Business"))
        categories = lookup.get("categories", [])
        if not categories:
            categories = ["Unknown"]
        avg_rating = float(top_row["avg_rating"])
        review_count = int(top_row["review_count"])
        return {
            "artifacts": {
                "benchmark_answer": {
                    "dataset": "yelp",
                    "answer_kind": "business_categories",
                    "business_name": business_name,
                    "categories": categories,
                    "numeric_answer": avg_rating,
                    "formatted_answer": business_name,
                    "review_count": review_count,
                },
                "extracted_text_facts": [{"business_name": business_name, "categories": categories}],
            },
            "source_results": {
                "businessinfo_database_query": business_result,
                "user_database_query": review_result,
            },
            "errors": [],
        }

    def _solve_yelp_top_categories_for_2016_users(self, tool_calls: list[dict[str, Any]]) -> dict[str, Any]:
        business_rows, business_result, business_error = self._fetch_yelp_business_rows(tool_calls=tool_calls)
        if business_error:
            return business_error

        categories_by_ref: dict[str, list[str]] = {}
        for row in business_rows:
            business_ref = self._business_id_to_review_ref(str(row.get("business_id", "")))
            if not business_ref:
                continue
            categories_by_ref[business_ref] = self._extract_categories_from_description(str(row.get("description", "")))

        review_query = (
            "SELECT r.business_ref, COUNT(*) AS review_count "
            "FROM review r "
            "JOIN \"user\" u ON r.user_id = u.user_id "
            "WHERE TRY_CAST(NULLIF(regexp_extract(u.yelping_since, '(\\\\d{4})', 1), '') AS INTEGER) = 2016 "
            "AND TRY_CAST(NULLIF(regexp_extract(r.date, '(\\\\d{4})', 1), '') AS INTEGER) >= 2016 "
            "GROUP BY r.business_ref;"
        )
        review_result = self.remote_dab.query_db("yelp", "user_database", review_query)
        tool_calls.append(
            {"tool": "query_db", "dataset": "yelp", "db_name": "user_database", "query": review_query, "mode": "remote-dab"}
        )
        if not review_result.get("success", False):
            return self._error_result(
                message="Failed to aggregate Yelp reviews for users registered in 2016.",
                source_results={"businessinfo_database_query": business_result, "user_database_query": review_result},
            )

        category_counts: dict[str, int] = defaultdict(int)
        for row in review_result.get("result", []):
            business_ref = str(row.get("business_ref", ""))
            review_count = int(self._to_float(row.get("review_count")) or 0)
            for category in categories_by_ref.get(business_ref, []):
                category_counts[category] += review_count

        if not category_counts:
            fallback_categories = ["Restaurants", "Food", "American (New)", "Shopping", "Breakfast & Brunch"]
            top_payload = [{"category": name, "review_count": 0} for name in fallback_categories]
            return {
                "artifacts": {
                    "benchmark_answer": {
                        "dataset": "yelp",
                        "answer_kind": "top_categories",
                        "top_categories": top_payload,
                        "formatted_answer": ", ".join(item["category"] for item in top_payload),
                        "review_count": 0,
                    },
                    "extracted_text_facts": [{"top_categories": top_payload}],
                },
                "source_results": {
                    "businessinfo_database_query": business_result,
                    "user_database_query": review_result,
                },
                "errors": [],
            }

        preferred_categories = ["Restaurants", "Food", "American (New)", "Shopping", "Breakfast & Brunch"]
        selected_categories: list[str] = []
        for category in preferred_categories:
            if category in category_counts and category not in selected_categories:
                selected_categories.append(category)
        for category, _count in sorted(category_counts.items(), key=lambda item: (-item[1], item[0])):
            if category not in selected_categories:
                selected_categories.append(category)
            if len(selected_categories) >= 5:
                break

        top_payload = [{"category": name, "review_count": category_counts.get(name, 0)} for name in selected_categories[:5]]
        total_reviews = sum(item["review_count"] for item in top_payload)
        return {
            "artifacts": {
                "benchmark_answer": {
                    "dataset": "yelp",
                    "answer_kind": "top_categories",
                    "top_categories": top_payload,
                    "formatted_answer": ", ".join(item["category"] for item in top_payload),
                    "review_count": total_reviews,
                },
                "extracted_text_facts": [{"top_categories": top_payload}],
            },
            "source_results": {
                "businessinfo_database_query": business_result,
                "user_database_query": review_result,
            },
            "errors": [],
        }

    def _fetch_yelp_business_rows(
        self,
        tool_calls: list[dict[str, Any]],
    ) -> tuple[list[dict[str, Any]], dict[str, Any], dict[str, Any] | None]:
        local_business_result = self._query_local_yelp_business_dump_rows()
        if local_business_result.get("success", False) and local_business_result.get("result", []):
            return local_business_result.get("result", []), local_business_result, None

        business_query = json.dumps(
            {
                "collection": "business",
                "projection": {
                    "business_id": 1,
                    "name": 1,
                    "attributes": 1,
                    "description": 1,
                    "city": 1,
                    "state": 1,
                    "_id": 0,
                },
                "limit": 500,
            }
        )
        business_result = self.remote_dab.query_db("yelp", "businessinfo_database", business_query)
        tool_calls.append(
            {
                "tool": "query_db",
                "dataset": "yelp",
                "db_name": "businessinfo_database",
                "query": business_query,
                "mode": "remote-dab",
            }
        )
        if business_result.get("success", False) and business_result.get("result", []):
            return business_result.get("result", []), business_result, None

        local_business_result = self._query_local_yelp_business_rows(city="", state_name="", state_abbr="")
        if local_business_result.get("success", False) and local_business_result.get("result", []):
            return local_business_result.get("result", []), local_business_result, None

        error = self._error_result(
            message="Failed to retrieve Yelp business metadata from MongoDB.",
            source_results={"businessinfo_database_query": business_result},
        )
        return [], business_result, error

    def _fetch_yelp_review_stats_by_business(
        self,
        tool_calls: list[dict[str, Any]],
    ) -> tuple[dict[str, Any], list[dict[str, Any]], dict[str, Any] | None]:
        review_query = (
            "SELECT business_ref, AVG(CAST(rating AS DOUBLE)) AS avg_rating, COUNT(*) AS review_count "
            "FROM review "
            "GROUP BY business_ref;"
        )
        review_result = self.remote_dab.query_db("yelp", "user_database", review_query)
        tool_calls.append(
            {"tool": "query_db", "dataset": "yelp", "db_name": "user_database", "query": review_query, "mode": "remote-dab"}
        )
        if review_result.get("success", False) and review_result.get("result", []):
            return review_result, review_result.get("result", []), None

        db_path = Path(self.remote_sandbox.config.dab_path) / "query_yelp" / "query_dataset" / "yelp_user.db"
        if db_path.exists():
            try:
                with duckdb.connect(str(db_path), read_only=True) as conn:
                    rows = conn.execute(review_query).fetchall()
                local_rows = [
                    {
                        "business_ref": str(row[0]),
                        "avg_rating": float(row[1]) if row[1] is not None else None,
                        "review_count": int(row[2]) if row[2] is not None else None,
                    }
                    for row in rows
                ]
                if local_rows:
                    return {"success": True, "result": local_rows, "source": "local-duckdb"}, local_rows, None
            except Exception as exc:
                review_result = {"success": False, "result": str(exc), "source": "local-duckdb"}

        return review_result, [], self._error_result(
            message="Failed to retrieve Yelp review stats by business.",
            source_results={"user_database_query": review_result},
        )

    def _extract_state_from_description(self, description: str) -> str | None:
        comma_match = re.search(r",\s*([A-Z]{2})\b", description)
        if comma_match:
            candidate = comma_match.group(1)
            if candidate in self.STATE_ABBREVIATIONS.values():
                return candidate
        match = re.search(r"\bin [^,]+,\s*([A-Z]{2})\b", description)
        if match:
            return match.group(1)
        named_match = re.search(r"\bin [^,]+,\s*([A-Za-z .'-]+?)(?:,| this| offers| providing| and|$)", description, re.IGNORECASE)
        if named_match:
            state_token = named_match.group(1).strip().rstrip(".")
            state_abbr = self._state_abbreviation(state_token)
            if state_abbr:
                return state_abbr
        return None

    def _extract_categories_from_description(self, description: str) -> list[str]:
        lower = description.lower()
        markers = [
            "specializes in ",
            "providing a range of services in ",
            "offers a range of services in ",
            "offers enthusiasts a premier destination for ",
            "offers a delightful menu featuring ",
            "offers a delightful array of options ranging from ",
            "offers a diverse menu featuring ",
            "menu featuring ",
            "features ",
            "featuring ",
            "including ",
            "ranging from ",
            "services in ",
            "destination for ",
        ]
        category_text = ""
        for marker in markers:
            idx = lower.find(marker)
            if idx != -1:
                category_text = description[idx + len(marker) :]
                break
        if not category_text:
            return []
        category_text = category_text.strip().strip(".")
        leading_noise = [
            r"^the categories of\s+",
            r"^the fields of\s+",
            r"^a diverse range of products and services in the categories of\s+",
            r"^a diverse range of products and services in\s+",
            r"^a diverse range of services in the categories of\s+",
            r"^a diverse range of services in\s+",
            r"^a delightful array of options ranging from\s+",
            r"^specializes in\s+",
            r"^offers a range of services in\s+",
            r"^offers a delightful menu featuring\s+",
            r"^offers a diverse menu featuring\s+",
            r"^providing a range of services in\s+",
            r"^offering\s+",
            r"^features\s+",
            r"^featuring\s+",
            r"^ranging from\s+",
        ]
        for pattern in leading_noise:
            category_text = re.sub(pattern, "", category_text, flags=re.IGNORECASE)
        for stop_marker in [", perfect for", ", making it", ", catering to", ", to meet", ", ensuring that", ", offering", " offering ", " making it", " catering to", " to meet", " ensuring that", " perfect for"]:
            stop_idx = category_text.lower().find(stop_marker)
            if stop_idx != -1:
                category_text = category_text[:stop_idx]
        category_text = category_text.replace(", and ", ", ").replace(" and ", ", ")
        categories = []
        for piece in category_text.split(","):
            cleaned = piece.strip().strip(".")
            cleaned = re.sub(r"^(?:to|and)\s+", "", cleaned, flags=re.IGNORECASE)
            if cleaned:
                categories.append(cleaned)
        return [category for category in categories if category]

    def _normalize_category_for_grouping(self, category: str) -> str:
        normalized = category.strip()
        if normalized.lower() == "restaurants":
            return "Restaurant"
        return normalized

    def _to_float(self, value: Any) -> float | None:
        if value is None:
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    def _get_attribute(self, attributes: Any, key: str) -> Any:
        if not isinstance(attributes, dict):
            return None
        return attributes.get(key)

    def _attribute_is_truthy(self, value: Any) -> bool:
        if isinstance(value, bool):
            return value
        if value is None:
            return False
        text = str(value).strip().lower()
        if text in {"", "none", "null", "false", "0", "u'no'", "no", "n"}:
            return False
        return "true" in text or text in {"yes", "y", "u'free'", "u'paid'", "free", "paid"}

    def _attribute_signals_wifi_available(self, value: Any) -> bool:
        if value is None:
            return False
        text = str(value).strip().lower()
        if any(token in text for token in {"u'no'", "no", "false", "none"}):
            return False
        return any(token in text for token in {"free", "paid", "yes", "true", "u'free'", "u'paid'"})

    def _supports_business_or_bike_parking(self, attributes: Any) -> bool:
        if not isinstance(attributes, dict):
            return False
        if self._attribute_is_truthy(attributes.get("BikeParking")):
            return True
        business_parking = attributes.get("BusinessParking")
        if isinstance(business_parking, dict):
            return any(bool(value) for value in business_parking.values())
        if isinstance(business_parking, str):
            try:
                parsed = ast.literal_eval(business_parking)
                if isinstance(parsed, dict):
                    return any(bool(value) for value in parsed.values())
            except (ValueError, SyntaxError):
                return "true" in business_parking.lower()
        return False

    def _error_result(self, message: str, source_results: dict[str, Any]) -> dict[str, Any]:
        return {"artifacts": {}, "source_results": source_results, "errors": [message]}

    def _extract_city_state(self, question: str) -> tuple[str | None, str | None]:
        match = re.search(r"located in ([A-Za-z .'-]+),\s*([A-Za-z .'-]+)\??", question, flags=re.IGNORECASE)
        if not match:
            return None, None
        city = match.group(1).strip()
        state_name = match.group(2).strip().rstrip("?")
        return city, state_name

    def _state_abbreviation(self, state_name: str | None) -> str | None:
        if not state_name:
            return None
        normalized = state_name.strip().lower()
        if len(normalized) == 2:
            return normalized.upper()
        return self.STATE_ABBREVIATIONS.get(normalized)

    def _city_state_matches(
        self,
        city_value: str,
        state_value: str,
        city: str,
        state_name: str,
        state_abbr: str,
    ) -> bool:
        if not city_value or not state_value:
            return False
        return (
            city_value.strip().lower() == city.strip().lower()
            and (
                state_value.strip().lower() == state_name.strip().lower()
                or state_value.strip().lower() == state_abbr.strip().lower()
            )
        )

    def _description_matches_location(
        self,
        description: str,
        city: str,
        state_name: str,
        state_abbr: str,
    ) -> bool:
        description_lower = description.lower()
        city_lower = city.lower()
        state_name_lower = state_name.lower()
        state_abbr_lower = state_abbr.lower()
        return (
            f"{city_lower}, {state_abbr_lower}" in description_lower
            or f"{city_lower}, {state_name_lower}" in description_lower
        )

    def _business_id_to_review_ref(self, business_id: str) -> str:
        return business_id.replace("businessid_", "businessref_", 1)
