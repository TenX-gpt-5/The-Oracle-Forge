"""
schema_index.py

Curated schema and usage metadata for the local Oracle Forge runtime.
"""

from __future__ import annotations


class SchemaIndex:
    def __init__(self):
        self.schemas = {
            "postgres": {
                "tables": {
                    "users": {
                        "columns": ["customer_id", "name", "status", "last_login"],
                        "primary_key": "customer_id",
                    },
                    "orders": {
                        "columns": ["order_id", "customer_id", "amount", "status", "ordered_at"],
                        "primary_key": "order_id",
                    },
                },
                "clues": [
                    "Use orders for revenue and purchase behavior.",
                    "Use users for customer status and activity windows.",
                ],
            },
            "sqlite": {
                "tables": {
                    "customer_segments": {
                        "columns": ["customer_id", "segment"],
                        "primary_key": "customer_id",
                    }
                },
                "clues": ["Use customer_segments for segmentation rollups."],
            },
            "duckdb": {
                "tables": {
                    "daily_metrics": {
                        "columns": ["metric_date", "metric_name", "metric_value"],
                        "primary_key": None,
                    }
                },
                "clues": ["Use daily_metrics for analytical trend lookups."],
            },
            "mongodb": {
                "collections": {
                    "support_tickets": {
                        "fields": ["ticket_id", "customer_id", "note", "status", "priority"],
                        "primary_key": "ticket_id",
                    }
                },
                "clues": [
                    "support_tickets stores CRM and support data.",
                    "customer_id in MongoDB often uses a prefixed string format such as CUST-001.",
                ],
            },
        }

    def get_schema_for_db(self, db_name: str) -> dict:
        return self.schemas.get(db_name, {"tables": {}, "clues": []})

    def list_sources(self) -> list[str]:
        return list(self.schemas.keys())
