"""
toolbox_client.py

Thin abstraction for database access that can evolve toward a true
Toolbox-first runtime without breaking the current benchmark flow.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
from ast import literal_eval
from pathlib import Path
from typing import Any

from src.tools.db_tools import inspect_schema, run_mongo_pipeline, run_sql_duckdb, run_sql_postgres, run_sql_sqlite
from src.tools.mcp_stdio_client import MCPStdIOClient


class ToolboxClient:
    """
    Gateway for standard DB access.

    Today this class prefers local fallbacks because the current workspace is a
    hybrid architecture. As Toolbox coverage becomes complete, callers do not
    need to change; only this class needs to gain fuller native invocations.
    """

    def __init__(
        self,
        toolbox_path: str | None = None,
        tools_file: str | None = None,
        mcp_config_file: str | None = None,
    ) -> None:
        self.toolbox_path = toolbox_path or os.getenv("TOOLBOX_PATH", "toolbox")
        configured_tools_file = tools_file or os.getenv("TOOLBOX_TOOLS_FILE", "mcp/tools.yaml")
        self.tools_file = str(Path(configured_tools_file))
        configured_mcp_file = mcp_config_file or os.getenv("MCP_CONFIG_FILE", "mcp/local_yelp_mcp.json")
        self.mcp_config_file = str(Path(configured_mcp_file))
        self.duckdb_mcp = MCPStdIOClient(self.mcp_config_file, server_name="duckdb")

    def configured(self) -> bool:
        return Path(self.tools_file).exists()

    def available(self) -> bool:
        return shutil.which(self.toolbox_path) is not None and self.configured()

    def inspect_schema(self, source: str) -> dict[str, Any]:
        if self.available():
            toolbox_result = self._inspect_schema_via_toolbox(source)
            if toolbox_result is not None:
                return toolbox_result
        if source == "duckdb":
            duckdb_schema = self._inspect_schema_via_mcp(source)
            if duckdb_schema is not None:
                return duckdb_schema
        return inspect_schema(source)

    def execute_source(
        self,
        source: str,
        question: str,
        plan: dict[str, Any],
        repair_context: dict[str, Any],
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        if source == "postgres":
            query = self._build_postgres_query(question, plan, repair_context)
            if self.available():
                toolbox_result = self._execute_sql_via_toolbox(source, query)
                if toolbox_result is not None:
                    return toolbox_result, {"tool": "toolbox_sql", "source": source, "query": query, "mode": "toolbox"}
            return run_sql_postgres(query), {"tool": "run_sql_postgres", "source": source, "query": query, "mode": "local-fallback"}

        if source == "sqlite":
            query = self._build_sqlite_query(question, plan)
            if self.available():
                toolbox_result = self._execute_sql_via_toolbox(source, query)
                if toolbox_result is not None:
                    return toolbox_result, {"tool": "toolbox_sql", "source": source, "query": query, "mode": "toolbox"}
            return run_sql_sqlite("", query), {"tool": "run_sql_sqlite", "source": source, "query": query, "mode": "local-fallback"}

        if source == "duckdb":
            query = self._build_duckdb_query(question, plan)
            if self.available():
                toolbox_result = self._execute_sql_via_toolbox(source, query)
                if toolbox_result is not None:
                    return toolbox_result, {"tool": "toolbox_sql", "source": source, "query": query, "mode": "toolbox"}
            mcp_result = self._execute_sql_via_mcp(source, query)
            if mcp_result is not None:
                return mcp_result, {"tool": "mcp_sql", "source": source, "query": query, "mode": "mcp-stdio"}
            return run_sql_duckdb(query), {"tool": "run_sql_duckdb", "source": source, "query": query, "mode": "local-fallback"}

        if source == "mongodb":
            pipeline = self._build_mongo_pipeline(question, plan)
            if self.available():
                toolbox_result = self._execute_mongo_via_toolbox(pipeline)
                if toolbox_result is not None:
                    return toolbox_result, {"tool": "toolbox_mongo", "source": source, "pipeline": pipeline, "mode": "toolbox"}
            return run_mongo_pipeline(pipeline), {"tool": "run_mongo_pipeline", "source": source, "pipeline": pipeline, "mode": "local-fallback"}

        return {"ok": False, "error": f"Unsupported source: {source}"}, {"tool": "unsupported_source", "source": source}

    def _inspect_schema_via_toolbox(self, source: str) -> dict[str, Any] | None:
        tool_name = {
            "postgres": "postgres-list-tables",
        }.get(source)
        if not tool_name:
            return None
        response = self._invoke_toolbox(tool_name)
        if not response.get("ok"):
            return None
        parsed = self._parse_toolbox_output(response["stdout"])
        if not isinstance(parsed, list):
            return None
        return {"ok": True, "source": source, "table_names": parsed, "schema": {"tables": parsed}}

    def _inspect_schema_via_mcp(self, source: str) -> dict[str, Any] | None:
        if source != "duckdb" or not self.duckdb_mcp.available():
            return None
        result = self._execute_sql_via_mcp(source, "SHOW TABLES;")
        if result is None or not result.get("ok"):
            return None
        table_names = []
        for row in result.get("rows", []):
            if isinstance(row, dict):
                table_name = row.get("name") or row.get("table_name") or row.get("value")
                if table_name:
                    table_names.append(str(table_name))
        return {"ok": True, "source": source, "table_names": table_names, "schema": {"tables": table_names}}

    def _execute_sql_via_toolbox(self, source: str, query: str) -> dict[str, Any] | None:
        tool_name = {
            "postgres": "postgres-execute",
        }.get(source)
        if not tool_name:
            return None
        response = self._invoke_toolbox(tool_name, {"query": query, "sql": query, "statement": query})
        if not response.get("ok"):
            return None
        parsed = self._parse_toolbox_output(response["stdout"])
        rows = self._extract_rows(parsed)
        return {
            "ok": True,
            "source": source,
            "query": query,
            "rows": rows,
            "row_count": len(rows),
            "columns": list(rows[0].keys()) if rows and isinstance(rows[0], dict) else [],
            "toolbox_raw": parsed,
        }

    def _execute_sql_via_mcp(self, source: str, query: str) -> dict[str, Any] | None:
        if source != "duckdb" or not self.duckdb_mcp.available():
            return None
        try:
            response = self.duckdb_mcp.call_first_matching_tool(
                preferred_names=[
                    "duckdb-execute",
                    "execute_sql",
                    "query",
                    "run_query",
                ],
                argument_candidates=[
                    {"query": query},
                    {"sql": query},
                    {"statement": query},
                    {"sql_query": query},
                ],
            )
        except Exception:
            return None
        if not response.get("ok"):
            return None

        payload = response.get("payload")
        rows = self._extract_rows(payload)
        if not rows:
            rows = self._rows_from_scalar_payload(payload)
        if not rows and isinstance(payload, list) and all(not isinstance(item, dict) for item in payload):
            rows = [{"value": item} for item in payload]

        return {
            "ok": True,
            "source": source,
            "query": query,
            "rows": rows,
            "row_count": len(rows),
            "columns": list(rows[0].keys()) if rows and isinstance(rows[0], dict) else [],
            "mcp_raw": payload,
            "mcp_tool_name": response.get("tool_name"),
        }

    def _execute_mongo_via_toolbox(self, pipeline: dict[str, Any]) -> dict[str, Any] | None:
        collection = pipeline.get("collection")
        tool_name = {
            "business": "mongodb-find-businesses",
            "checkin": "mongodb-find-checkins",
        }.get(collection)
        if not tool_name:
            return None
        response = self._invoke_toolbox(tool_name)
        if not response.get("ok"):
            return None
        parsed = self._parse_toolbox_output(response["stdout"])
        rows = self._extract_rows(parsed)
        return {
            "ok": True,
            "source": "mongodb",
            "pipeline": pipeline,
            "rows": rows,
            "row_count": len(rows),
            "toolbox_raw": parsed,
        }

    def _invoke_toolbox(self, tool_name: str, args: dict[str, Any] | None = None) -> dict[str, Any]:
        cmd = [self.toolbox_path, "--tools-file", self.tools_file, "invoke", tool_name]
        if args:
            # The current implementation only attempts native invocation when a
            # no-surprises path is known. Structured args are held here for the
            # next migration step.
            return {"ok": False, "stdout": "", "stderr": f"Structured Toolbox args not wired yet for {tool_name}"}
        try:
            result = subprocess.run(cmd, capture_output=True, text=True)
        except Exception as exc:
            return {"ok": False, "stdout": "", "stderr": str(exc)}
        return {
            "ok": result.returncode == 0,
            "stdout": result.stdout.strip(),
            "stderr": result.stderr.strip(),
            "returncode": result.returncode,
        }

    def _parse_toolbox_output(self, stdout: str) -> Any:
        if not stdout:
            return []
        try:
            return json.loads(stdout)
        except json.JSONDecodeError:
            return stdout

    def _extract_rows(self, payload: Any) -> list[dict[str, Any]]:
        if isinstance(payload, list):
            return [row for row in payload if isinstance(row, dict)]
        if isinstance(payload, dict):
            for key in ("result", "rows", "items", "data"):
                value = payload.get(key)
                if isinstance(value, list):
                    return [row for row in value if isinstance(row, dict)]
        return []

    def _rows_from_scalar_payload(self, payload: Any) -> list[dict[str, Any]]:
        parsed = payload
        if isinstance(payload, str):
            try:
                parsed = literal_eval(payload)
            except (ValueError, SyntaxError):
                return []

        if not isinstance(parsed, list):
            return []

        rows: list[dict[str, Any]] = []
        for item in parsed:
            if isinstance(item, dict):
                rows.append(item)
            elif isinstance(item, (list, tuple)):
                if len(item) == 1:
                    rows.append({"value": item[0]})
                else:
                    rows.append({f"value_{index}": value for index, value in enumerate(item)})
        return rows

    def _build_postgres_query(
        self,
        question: str,
        plan: dict[str, Any],
        repair_context: dict[str, Any],
    ) -> str:
        lower = question.lower()
        if repair_context.get("force_schema_inspection"):
            return "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';"
        if "how many" in lower and "user" in lower:
            return "SELECT COUNT(*) AS user_count FROM users;"
        if len(plan.get("required_sources", [])) > 1:
            return (
                "SELECT customer_id, COUNT(*) AS order_count, "
                "SUM(amount) AS revenue FROM orders WHERE status = 'completed' "
                "GROUP BY customer_id;"
            )
        return "SELECT customer_id, name, status FROM users;"

    def _build_sqlite_query(self, question: str, plan: dict[str, Any]) -> str:
        if "segment" in question.lower() or len(plan.get("required_sources", [])) > 1:
            return "SELECT customer_id, segment FROM customer_segments;"
        return "SELECT customer_id, segment FROM customer_segments;"

    def _build_duckdb_query(self, question: str, plan: dict[str, Any]) -> str:
        return "SELECT metric_date, metric_name, metric_value FROM daily_metrics;"

    def _build_mongo_pipeline(self, question: str, plan: dict[str, Any]) -> dict[str, Any]:
        if plan.get("needs_text_extraction"):
            return {"collection": "support_tickets", "operation": "notes_with_facts"}
        return {"collection": "support_tickets", "operation": "count_by_customer"}
