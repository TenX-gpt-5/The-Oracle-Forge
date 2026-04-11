"""
Schema Introspection Utility

Extracts table/column/type metadata from PostgreSQL, SQLite, MongoDB, and DuckDB
into a normalized JSON manifest for the Context Cortex's Schema & Usage Index.
"""

import json
from typing import Any


def introspect_schema(db_type: str, connection_string: str = "", db_path: str = "") -> list[dict[str, Any]]:
    """
    Introspect a database and return a normalized schema manifest.

    Args:
        db_type: One of "postgresql", "sqlite", "mongodb", "duckdb"
        connection_string: Connection string (for postgresql, mongodb)
        db_path: File path (for sqlite, duckdb)

    Returns:
        List of table/collection descriptors:
        [
            {
                "table": "table_name",
                "db_type": "postgresql",
                "columns": [
                    {"name": "col_name", "type": "integer", "nullable": True, "is_primary_key": False}
                ],
                "row_count_estimate": 10000,
                "sample_values": {"col_name": ["val1", "val2", "val3"]}
            }
        ]
    """
    dispatch = {
        "postgresql": _introspect_postgres,
        "sqlite": _introspect_sqlite,
        "mongodb": _introspect_mongodb,
        "duckdb": _introspect_duckdb,
    }

    if db_type not in dispatch:
        raise ValueError(f"Unsupported db_type: {db_type}. Must be one of {list(dispatch.keys())}")

    return dispatch[db_type](connection_string or db_path)


def _introspect_postgres(connection_string: str) -> list[dict[str, Any]]:
    """Introspect PostgreSQL using information_schema."""
    try:
        import psycopg2
    except ImportError:
        raise ImportError("psycopg2 required for PostgreSQL introspection: pip install psycopg2-binary")

    manifest = []
    conn = psycopg2.connect(connection_string)
    cur = conn.cursor()

    # Get all user tables
    cur.execute("""
        SELECT table_name FROM information_schema.tables
        WHERE table_schema = 'public' AND table_type = 'BASE TABLE'
        ORDER BY table_name
    """)
    tables = [row[0] for row in cur.fetchall()]

    for table in tables:
        # Get columns
        cur.execute("""
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = %s
            ORDER BY ordinal_position
        """, (table,))
        columns = [
            {"name": row[0], "type": row[1], "nullable": row[2] == "YES", "is_primary_key": False}
            for row in cur.fetchall()
        ]

        # Mark primary keys
        cur.execute("""
            SELECT a.attname
            FROM pg_index i
            JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
            WHERE i.indrelid = %s::regclass AND i.indisprimary
        """, (table,))
        pk_cols = {row[0] for row in cur.fetchall()}
        for col in columns:
            if col["name"] in pk_cols:
                col["is_primary_key"] = True

        # Estimate row count
        cur.execute(f"SELECT reltuples::bigint FROM pg_class WHERE relname = %s", (table,))
        row_count = cur.fetchone()
        row_count_estimate = int(row_count[0]) if row_count else 0

        # Sample values (first 3 distinct per column, limit 5 columns)
        sample_values = {}
        for col in columns[:5]:
            try:
                cur.execute(
                    f'SELECT DISTINCT "{col["name"]}" FROM "{table}" WHERE "{col["name"]}" IS NOT NULL LIMIT 3'
                )
                sample_values[col["name"]] = [str(r[0]) for r in cur.fetchall()]
            except Exception:
                sample_values[col["name"]] = []

        manifest.append({
            "table": table,
            "db_type": "postgresql",
            "columns": columns,
            "row_count_estimate": row_count_estimate,
            "sample_values": sample_values,
        })

    cur.close()
    conn.close()
    return manifest


def _introspect_sqlite(db_path: str) -> list[dict[str, Any]]:
    """Introspect SQLite using pragma commands."""
    import sqlite3

    manifest = []
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name")
    tables = [row[0] for row in cur.fetchall()]

    for table in tables:
        cur.execute(f"PRAGMA table_info('{table}')")
        columns = [
            {
                "name": row[1],
                "type": row[2] or "TEXT",
                "nullable": row[3] == 0,
                "is_primary_key": row[5] == 1,
            }
            for row in cur.fetchall()
        ]

        cur.execute(f"SELECT COUNT(*) FROM '{table}'")
        row_count_estimate = cur.fetchone()[0]

        sample_values = {}
        for col in columns[:5]:
            try:
                cur.execute(f'SELECT DISTINCT "{col["name"]}" FROM "{table}" WHERE "{col["name"]}" IS NOT NULL LIMIT 3')
                sample_values[col["name"]] = [str(r[0]) for r in cur.fetchall()]
            except Exception:
                sample_values[col["name"]] = []

        manifest.append({
            "table": table,
            "db_type": "sqlite",
            "columns": columns,
            "row_count_estimate": row_count_estimate,
            "sample_values": sample_values,
        })

    conn.close()
    return manifest


def _introspect_mongodb(connection_string: str) -> list[dict[str, Any]]:
    """Introspect MongoDB by sampling documents to infer schema."""
    try:
        from pymongo import MongoClient
    except ImportError:
        raise ImportError("pymongo required for MongoDB introspection: pip install pymongo")

    client = MongoClient(connection_string)
    db_name = client.get_default_database().name if client.get_default_database() else connection_string.split("/")[-1].split("?")[0]
    db = client[db_name]

    manifest = []
    for collection_name in sorted(db.list_collection_names()):
        collection = db[collection_name]

        # Sample documents to infer schema
        sample_docs = list(collection.find().limit(50))
        if not sample_docs:
            continue

        # Infer columns from all sampled documents
        field_types: dict[str, set] = {}
        for doc in sample_docs:
            for key, value in doc.items():
                if key == "_id":
                    continue
                type_name = type(value).__name__
                field_types.setdefault(key, set()).add(type_name)

        columns = [
            {
                "name": field,
                "type": "/".join(sorted(types)),
                "nullable": True,
                "is_primary_key": False,
            }
            for field, types in sorted(field_types.items())
        ]

        row_count_estimate = collection.estimated_document_count()

        # Sample values
        sample_values = {}
        for col in columns[:5]:
            vals = set()
            for doc in sample_docs[:10]:
                if col["name"] in doc and doc[col["name"]] is not None:
                    vals.add(str(doc[col["name"]]))
            sample_values[col["name"]] = list(vals)[:3]

        manifest.append({
            "table": collection_name,
            "db_type": "mongodb",
            "columns": columns,
            "row_count_estimate": row_count_estimate,
            "sample_values": sample_values,
        })

    client.close()
    return manifest


def _introspect_duckdb(db_path: str) -> list[dict[str, Any]]:
    """Introspect DuckDB using information_schema."""
    try:
        import duckdb
    except ImportError:
        raise ImportError("duckdb required for DuckDB introspection: pip install duckdb")

    manifest = []
    conn = duckdb.connect(db_path)

    tables = conn.execute("""
        SELECT table_name FROM information_schema.tables
        WHERE table_schema = 'main'
        ORDER BY table_name
    """).fetchall()

    for (table,) in tables:
        cols = conn.execute("""
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_schema = 'main' AND table_name = ?
            ORDER BY ordinal_position
        """, [table]).fetchall()

        columns = [
            {"name": row[0], "type": row[1], "nullable": row[2] == "YES", "is_primary_key": False}
            for row in cols
        ]

        row_count = conn.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0]

        sample_values = {}
        for col in columns[:5]:
            try:
                result = conn.execute(
                    f'SELECT DISTINCT "{col["name"]}" FROM "{table}" WHERE "{col["name"]}" IS NOT NULL LIMIT 3'
                ).fetchall()
                sample_values[col["name"]] = [str(r[0]) for r in result]
            except Exception:
                sample_values[col["name"]] = []

        manifest.append({
            "table": table,
            "db_type": "duckdb",
            "columns": columns,
            "row_count_estimate": row_count,
            "sample_values": sample_values,
        })

    conn.close()
    return manifest


def manifest_to_compact_text(manifest: list[dict[str, Any]]) -> str:
    """Convert a schema manifest to compact text suitable for LLM context injection."""
    lines = []
    for entry in manifest:
        col_desc = ", ".join(
            f"{c['name']}({c['type']}{'*' if c['is_primary_key'] else ''})"
            for c in entry["columns"]
        )
        lines.append(f"[{entry['db_type']}] {entry['table']} (~{entry['row_count_estimate']} rows): {col_desc}")
    return "\n".join(lines)
