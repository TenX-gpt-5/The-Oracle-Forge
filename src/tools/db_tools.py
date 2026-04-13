"""
db_tools.py

Narrow DB-facing tools with structured fallbacks so the repo is runnable
without external infrastructure.
"""

from __future__ import annotations

import os
import sqlite3
from typing import Any

try:
    import psycopg  # type: ignore
except Exception:  # pragma: no cover - optional dependency
    psycopg = None


MOCK_SQL_DATA = {
    "postgres": {
        "users": [
            {"customer_id": 1, "name": "Alice", "status": "active", "last_login": "2026-04-02"},
            {"customer_id": 2, "name": "Bob", "status": "active", "last_login": "2026-04-05"},
            {"customer_id": 3, "name": "Carla", "status": "inactive", "last_login": "2025-12-10"},
        ],
        "orders": [
            {"order_id": 1001, "customer_id": 1, "amount": 120.0, "status": "completed", "ordered_at": "2026-03-01"},
            {"order_id": 1002, "customer_id": 1, "amount": 60.0, "status": "completed", "ordered_at": "2026-03-14"},
            {"order_id": 1003, "customer_id": 2, "amount": 75.0, "status": "completed", "ordered_at": "2026-03-20"},
            {"order_id": 1004, "customer_id": 2, "amount": 30.0, "status": "pending", "ordered_at": "2026-03-29"},
        ],
    },
    "sqlite": {
        "customer_segments": [
            {"customer_id": 1, "segment": "enterprise"},
            {"customer_id": 2, "segment": "mid_market"},
            {"customer_id": 3, "segment": "smb"},
        ]
    },
    "duckdb": {
        "daily_metrics": [
            {"metric_date": "2026-04-01", "metric_name": "repeat_purchase_rate", "metric_value": 0.42},
            {"metric_date": "2026-04-02", "metric_name": "ticket_volume", "metric_value": 12},
        ]
    },
}

MOCK_MONGO_DATA = {
    "support_tickets": [
        {"ticket_id": "T-1", "customer_id": "CUST-001", "note": "Customer is upset about late delivery", "status": "open", "priority": "high"},
        {"ticket_id": "T-2", "customer_id": "CUST-001", "note": "Billing issue resolved quickly", "status": "closed", "priority": "medium"},
        {"ticket_id": "T-3", "customer_id": "CUST-002", "note": "User asked for onboarding help", "status": "open", "priority": "low"},
    ]
}


def _init_mock_conn(db_name: str) -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    for table_name, rows in MOCK_SQL_DATA[db_name].items():
        if not rows:
            continue
        columns = rows[0].keys()
        ddl_columns = []
        for column in columns:
            sample = rows[0][column]
            if isinstance(sample, int):
                ddl_columns.append(f"{column} INTEGER")
            elif isinstance(sample, float):
                ddl_columns.append(f"{column} REAL")
            else:
                ddl_columns.append(f"{column} TEXT")
        cur.execute(f"CREATE TABLE {table_name} ({', '.join(ddl_columns)});")
        placeholders = ", ".join(["?"] * len(columns))
        insert_sql = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders});"
        for row in rows:
            cur.execute(insert_sql, tuple(row[column] for column in columns))
    conn.commit()
    return conn


def inspect_schema(db_name: str) -> dict[str, Any]:
    if db_name == "mongodb":
        collections = list(MOCK_MONGO_DATA.keys())
        return {"ok": True, "source": db_name, "table_names": collections, "schema": {"collections": collections}}

    schema = MOCK_SQL_DATA.get(db_name, {})
    table_names = list(schema.keys())
    return {"ok": True, "source": db_name, "table_names": table_names, "schema": schema}


def inspect_sample_values(db_name: str, table_or_collection: str) -> dict[str, Any]:
    if db_name == "mongodb":
        rows = MOCK_MONGO_DATA.get(table_or_collection, [])[:2]
        return {"ok": True, "source": db_name, "rows": rows, "row_count": len(rows)}
    rows = MOCK_SQL_DATA.get(db_name, {}).get(table_or_collection, [])[:2]
    return {"ok": True, "source": db_name, "rows": rows, "row_count": len(rows)}


def run_sql_postgres(query: str) -> dict[str, Any]:
    pg_uri = os.getenv("PG_URI", "")
    if pg_uri and psycopg is not None:
        try:
            with psycopg.connect(pg_uri) as conn:
                with conn.cursor() as cur:
                    cur.execute(query)
                    rows = []
                    columns = [desc.name for desc in cur.description] if cur.description else []
                    if columns:
                        rows = [dict(zip(columns, row)) for row in cur.fetchall()]
                    return {
                        "ok": True,
                        "source": "postgres",
                        "query": query,
                        "columns": columns,
                        "rows": rows,
                        "row_count": len(rows),
                    }
        except Exception as exc:  # pragma: no cover - depends on external infra
            return {"ok": False, "source": "postgres", "error": str(exc), "query": query}
    return _run_mock_sql("postgres", query)


def run_sql_sqlite(db_path: str, query: str) -> dict[str, Any]:
    if db_path and os.path.exists(db_path):
        try:
            with sqlite3.connect(db_path) as conn:
                conn.row_factory = sqlite3.Row
                cur = conn.cursor()
                cur.execute(query)
                rows = [dict(row) for row in cur.fetchall()]
                return {
                    "ok": True,
                    "source": "sqlite",
                    "query": query,
                    "columns": list(rows[0].keys()) if rows else [],
                    "rows": rows,
                    "row_count": len(rows),
                }
        except Exception as exc:
            return {"ok": False, "source": "sqlite", "error": str(exc), "query": query}
    return _run_mock_sql("sqlite", query)


def run_sql_duckdb(query: str) -> dict[str, Any]:
    return _run_mock_sql("duckdb", query)


def run_mongo_pipeline(pipeline: dict[str, Any]) -> dict[str, Any]:
    collection = pipeline.get("collection", "support_tickets")
    operation = pipeline.get("operation", "count_by_customer")
    documents = MOCK_MONGO_DATA.get(collection, [])
    if operation == "count_by_customer":
        counts: dict[str, int] = {}
        for doc in documents:
            customer_id = doc["customer_id"]
            counts[customer_id] = counts.get(customer_id, 0) + 1
        rows = [{"customer_id": customer_id, "ticket_count": count} for customer_id, count in counts.items()]
    else:
        rows = list(documents)
    return {
        "ok": True,
        "source": "mongodb",
        "pipeline": pipeline,
        "rows": rows,
        "row_count": len(rows),
    }


def _run_mock_sql(db_name: str, query: str) -> dict[str, Any]:
    lower = query.lower().strip()
    if "information_schema.tables" in lower:
        table_names = list(MOCK_SQL_DATA.get(db_name, {}).keys())
        rows = [{"table_name": name} for name in table_names]
        return {
            "ok": True,
            "source": db_name,
            "query": query,
            "columns": ["table_name"],
            "rows": rows,
            "row_count": len(rows),
        }

    try:
        conn = _init_mock_conn(db_name)
        cur = conn.cursor()
        cur.execute(query)
        rows = [dict(row) for row in cur.fetchall()] if cur.description else []
        columns = [description[0] for description in cur.description] if cur.description else []
        return {
            "ok": True,
            "source": db_name,
            "query": query,
            "columns": columns,
            "rows": rows,
            "row_count": len(rows),
        }
    except Exception as exc:
        return {"ok": False, "source": db_name, "error": str(exc), "query": query}
