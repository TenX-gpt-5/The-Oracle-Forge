"""
schema_index.py

Curated schema and usage metadata for the local Oracle Forge runtime.
"""

from __future__ import annotations


class SchemaIndex:
    def __init__(self):
        self.dataset_schemas = {
            "bookreview": {
                "books_database": {
                    "db_type": "postgres",
                    "db_name": "bookreview_db",
                    "tables": {
                        "books_info": {
                            "columns": [
                                "title", "subtitle", "author", "rating_number",
                                "features", "description", "price", "store",
                                "categories", "details", "book_id",
                            ],
                            "primary_key": "book_id",
                        }
                    },
                    "clues": [
                        "book_id format: 'bookid_N' — normalize to numeric suffix before joining.",
                        "'description', 'categories', 'features' are stored as stringified list/dict — parse before use.",
                        "Publication year is embedded in natural-language 'details' field.",
                        "Language is mentioned in the 'details' field (e.g., 'written in English').",
                    ],
                },
                "review_database": {
                    "db_type": "sqlite",
                    "db_path": "query_dataset/review_query.db",
                    "tables": {
                        "review": {
                            "columns": [
                                "rating", "title", "text", "purchase_id",
                                "review_time", "helpful_vote", "verified_purchase",
                            ],
                            "primary_key": "purchase_id",
                        }
                    },
                    "clues": [
                        "purchase_id format: 'purchaseid_N' — normalize to numeric suffix to join with books_info.book_id.",
                        "rating is 1.0–5.0 scale (float).",
                        "verified_purchase is a boolean.",
                    ],
                },
                "_hints": [
                    "Join books_info.book_id ↔ review.purchase_id by stripping prefix and matching numeric suffix.",
                    "Use categories field in books_info to filter by genre (e.g., 'Literature & Fiction').",
                    "Use details field to extract publication decade for temporal aggregation.",
                ],
            },
        }

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

    def get_schema_for_dataset(self, dataset: str) -> dict:
        return self.dataset_schemas.get(dataset.lower(), {})

    def list_sources(self) -> list[str]:
        return list(self.schemas.keys())
