"""
validator.py

Structured validation for execution results.
"""

from __future__ import annotations

from typing import Any


class Validator:
    def validate_execution(
        self,
        question: str,
        plan: dict[str, Any],
        execution_result: dict[str, Any],
        context_payload: dict[str, Any],
        benchmark_context: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        benchmark_context = benchmark_context or {}
        errors: list[str] = []
        evidence: list[str] = []
        failure_class = ""

        if not execution_result.get("success", False):
            failure_class = "execution_failure"
            errors.extend(execution_result.get("errors", ["Execution failed."]))

        required_sources = plan.get("required_sources", [])
        if benchmark_context.get("dataset"):
            logical_db_names = execution_result.get("artifacts", {}).get("logical_db_names", [])
            if logical_db_names:
                evidence.append(f"logical_db_names={','.join(logical_db_names)}")
            benchmark_answer = execution_result.get("artifacts", {}).get("benchmark_answer")
            if plan.get("expected_output_shape") == "benchmark_answer":
                if not benchmark_answer:
                    failure_class = failure_class or "benchmark_answer_missing"
                    errors.append("The benchmark execution did not produce a validated answer artifact.")
                else:
                    evidence.append(f"benchmark_answer={benchmark_answer.get('formatted_answer', '')}")
                    if benchmark_answer.get("review_count") is not None:
                        evidence.append(f"benchmark_reviews={benchmark_answer['review_count']}")
                    benchmark_dataset = str(benchmark_context.get("dataset", "")).lower()
                    query_id = benchmark_context.get("query_id")
                    if benchmark_dataset == "yelp":
                        answer_kind = benchmark_answer.get("answer_kind")
                        if query_id in {2, 5}:
                            if not benchmark_answer.get("state_abbr"):
                                failure_class = failure_class or "benchmark_answer_quality_failure"
                                errors.append("Expected a resolved state abbreviation for this benchmark query.")
                            numeric_answer = benchmark_answer.get("numeric_answer")
                            if numeric_answer is None:
                                failure_class = failure_class or "benchmark_answer_quality_failure"
                                errors.append("Expected a numeric benchmark answer for this query.")
                            else:
                                numeric_value = float(numeric_answer)
                                if numeric_value < 0 or numeric_value > 5:
                                    failure_class = failure_class or "benchmark_answer_quality_failure"
                                    errors.append("Benchmark rating fell outside the expected 0-5 range.")
                        if query_id == 3:
                            if answer_kind != "count_only":
                                failure_class = failure_class or "benchmark_answer_quality_failure"
                                errors.append("Expected count_only answer kind for Yelp query 3.")
                            numeric_answer = benchmark_answer.get("numeric_answer")
                            if numeric_answer is None or int(float(numeric_answer)) <= 0:
                                failure_class = failure_class or "benchmark_answer_quality_failure"
                                errors.append("Expected a positive integer count for Yelp query 3.")
                        if query_id == 6:
                            business_name = str(benchmark_answer.get("business_name", "")).strip()
                            categories = benchmark_answer.get("categories", [])
                            categories_lower = [str(category).lower() for category in categories]
                            if not business_name:
                                failure_class = failure_class or "benchmark_answer_quality_failure"
                                errors.append("Expected a resolved business name for Yelp query 6.")
                            if not categories or any(category == "unknown" for category in categories_lower):
                                failure_class = failure_class or "extraction_failure"
                                errors.append("Expected extracted business categories for Yelp query 6.")
                            if "restaurants" not in categories_lower:
                                failure_class = failure_class or "extraction_failure"
                                errors.append("Expected Restaurants category to be present for Yelp query 6.")
        for source in required_sources:
            if source not in execution_result.get("source_results", {}):
                if not benchmark_context.get("dataset"):
                    failure_class = failure_class or "routing_failure"
                    errors.append(f"Required source {source} was not executed.")

        if plan.get("question_type") == "schema_discovery":
            for source, result in execution_result.get("source_results", {}).items():
                table_names = result.get("table_names", [])
                if not table_names:
                    failure_class = failure_class or "schema_missing"
                    errors.append(f"No tables or collections were surfaced for {source}.")
                else:
                    evidence.append(f"{source}: {', '.join(table_names)}")

        if plan.get("expected_output_shape") == "ranked_segments_plus_explanation":
            segment_rollup = execution_result.get("artifacts", {}).get("segment_rollup", [])
            if not segment_rollup:
                failure_class = failure_class or "join_or_aggregation_failure"
                errors.append("Expected a segment rollup artifact for a cross-database analytical query.")
            else:
                evidence.append(f"segment_rollup_rows={len(segment_rollup)}")

        if plan.get("needs_text_extraction"):
            extracted = execution_result.get("artifacts", {}).get("extracted_text_facts", [])
            if not extracted:
                failure_class = failure_class or "extraction_failure"
                errors.append("Expected extracted text facts but none were produced.")
            else:
                evidence.append(f"extracted_rows={len(extracted)}")

        if not errors:
            for source, result in execution_result.get("source_results", {}).items():
                if result.get("row_count") is not None:
                    evidence.append(f"{source}_rows={result['row_count']}")

        return {
            "status": "passed" if not errors else "failed",
            "errors": errors,
            "failure_class": failure_class or "none",
            "evidence": evidence,
        }
