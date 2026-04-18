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
                    },
                    "articles": {
                        "fields": ["article_id", "title", "description"],
                        "primary_key": "article_id",
                        "notes": "AG News articles. Category (World/Sports/Business/Science/Technology) must be inferred from title and description text.",
                    },
                },
                "clues": [
                    "support_tickets stores CRM and support data.",
                    "customer_id in MongoDB often uses a prefixed string format such as CUST-001.",
                    "articles stores AG News content; category is not a stored field — classify by title/description keywords.",
                ],
            },
            "articles_database": {
                "collections": {
                    "articles": {
                        "fields": ["article_id", "title", "description"],
                        "primary_key": "article_id",
                        "notes": "AG News articles. Category must be inferred from title/description. Categories: World, Sports, Business, Science/Technology.",
                    }
                },
                "clues": [
                    "articles_database is a MongoDB database for AG News.",
                    "No category field exists — classify articles by matching keywords in title and description.",
                ],
            },
            "metadata_database": {
                "tables": {
                    "authors": {
                        "columns": ["author_id", "name"],
                        "primary_key": "author_id",
                    },
                    "article_metadata": {
                        "columns": ["article_id", "author_id", "region", "publication_date"],
                        "primary_key": "article_id",
                        "notes": "publication_date format: YYYY-MM-DD. region values include: Europe, Asia, North America, South America, Africa, Oceania.",
                    },
                },
                "clues": [
                    "metadata_database is a SQLite database for AG News metadata.",
                    "Filter by region and strftime('%Y', publication_date) for year-based aggregations.",
                    "Join article_metadata.article_id with articles_database articles collection on article_id.",
                ],
            },
        }

    def get_schema_for_db(self, db_name: str) -> dict:
        return self.schemas.get(db_name, {"tables": {}, "clues": []})

    def list_sources(self) -> list[str]:
        return list(self.schemas.keys())
