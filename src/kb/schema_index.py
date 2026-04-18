"""
schema_index.py

Curated source schemas plus dataset-specific schema metadata.
This keeps the runtime deterministic while letting the planner and
context layer behave more like the driver2 architecture.
"""

from __future__ import annotations


class SchemaIndex:
    def __init__(self) -> None:
        self.source_schemas = {
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

        self.dataset_schemas = {
            "bookreview": {
                "source_types": ["postgres", "sqlite"],
                "default_entities": ["book", "review"],
                "join_keys": ["book_id", "purchase_id"],
                "sources": {
                    "books_database": {
                        "db_type": "postgres",
                        "db_name": "bookreview_db",
                        "tables": {
                            "books_info": {
                                "columns": [
                                    "title",
                                    "subtitle",
                                    "author",
                                    "rating_number",
                                    "features",
                                    "description",
                                    "price",
                                    "store",
                                    "categories",
                                    "details",
                                    "book_id",
                                ],
                                "primary_key": "book_id",
                            }
                        },
                        "clues": [
                            "book_id format: 'bookid_N' — normalize to numeric suffix before joining.",
                            "'description', 'categories', 'features' are stored as stringified list/dict — parse before use.",
                        ],
                    },
                    "review_database": {
                        "db_type": "sqlite",
                        "db_path": "query_dataset/review_query.db",
                        "tables": {
                            "review": {
                                "columns": [
                                    "rating",
                                    "title",
                                    "text",
                                    "purchase_id",
                                    "review_time",
                                    "helpful_vote",
                                    "verified_purchase",
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
                },
                "clues": [
                    "Join books_info.book_id ↔ review.purchase_id by stripping the prefix and matching the numeric suffix.",
                    "Use categories to filter by genre and details to extract publication decade.",
                ],
            },
            "yelp": {
                "source_types": ["mongo", "duckdb"],
                "default_entities": ["business", "review"],
                "join_keys": ["business_id", "business_ref"],
                "sources": {
                    "businessinfo_database": {
                        "db_type": "mongo",
                        "db_name": "yelp_db",
                        "dump_folder": "query_dataset/yelp_business",
                        "collections": {
                            "business": {
                                "fields": [
                                    "business_id",
                                    "name",
                                    "city",
                                    "state",
                                    "attributes",
                                    "categories",
                                    "description",
                                ],
                                "primary_key": "business_id",
                            }
                        },
                        "clues": [
                            "business_id values are normalized prefixed identifiers.",
                            "attributes/categories/description are the key text fields for matching location and amenities.",
                        ],
                    },
                    "user_database": {
                        "db_type": "duckdb",
                        "db_path": "query_dataset/yelp_user.db",
                        "tables": {
                            "review": {
                                "columns": ["business_ref", "rating", "review_date", "text", "user_id"],
                                "primary_key": None,
                            }
                        },
                        "clues": [
                            "business_ref links review aggregates back to business_id after normalization.",
                            "rating is the numeric signal used for averages.",
                        ],
                    },
                },
                "clues": [
                    "Use business metadata for location/category filters and review DB for rating aggregation.",
                ],
            },
            "googlelocal": {
                "source_types": ["sqlite"],
                "default_entities": ["business", "review"],
                "join_keys": ["gmap_id"],
                "sources": {
                    "business_database": {
                        "db_type": "sqlite",
                        "db_path": "query_dataset/business_description.db",
                        "tables": {
                            "business_description": {
                                "columns": [
                                    "name",
                                    "gmap_id",
                                    "description",
                                    "num_of_reviews",
                                    "hours",
                                    "MISC",
                                    "state",
                                ],
                                "primary_key": "gmap_id",
                            }
                        },
                        "clues": [
                            "description contains the city/state hints used for location filtering.",
                            "gmap_id is the shared key across business and review data.",
                        ],
                    },
                    "review_database": {
                        "db_type": "sqlite",
                        "db_path": "query_dataset/review_query.db",
                        "tables": {
                            "review": {
                                "columns": ["gmap_id", "rating", "text", "time", "reviewer_name"],
                                "primary_key": None,
                            }
                        },
                        "clues": [
                            "Average rating is computed from review.rating grouped by gmap_id.",
                        ],
                    },
                },
                "clues": [
                    "Location is often encoded in description text rather than normalized columns.",
                    "The response order is determined by average rating descending.",
                ],
            },
            "crmarenapro": {
                "source_types": ["sqlite", "duckdb", "postgres"],
                "default_entities": ["lead", "case", "opportunity"],
                "join_keys": ["customer_id", "lead_id", "case_id", "opportunity_id"],
                "sources": {
                    "core_crm": {
                        "db_type": "sqlite",
                        "db_path": "query_dataset/core_crm.db",
                        "clues": ["Canonical CRM entities and identifiers live here."],
                    },
                    "sales_pipeline": {
                        "db_type": "duckdb",
                        "db_path": "query_dataset/sales_pipeline.duckdb",
                        "clues": ["Use for sales stage and pipeline timing analysis."],
                    },
                    "support": {
                        "db_type": "postgres",
                        "db_name": "crm_support",
                        "sql_file": "query_dataset/support.sql",
                        "clues": ["Use for support cases, transcripts, and policy checks."],
                    },
                    "products_orders": {
                        "db_type": "sqlite",
                        "db_path": "query_dataset/products_orders.db",
                        "clues": ["Use for product/order linkage and quantity checks."],
                    },
                    "activities": {
                        "db_type": "duckdb",
                        "db_path": "query_dataset/activities.duckdb",
                        "clues": ["Use for call transcripts and activity timelines."],
                    },
                    "territory": {
                        "db_type": "sqlite",
                        "db_path": "query_dataset/territory.db",
                        "clues": ["Use for regional assignment and state rollups."],
                    },
                },
                "clues": [
                    "Lead qualification often depends on transcripts plus BANT policy hints.",
                    "Transfer-count and handle-time questions require activity history windows.",
                ],
            },
            "github_repos": {
                "source_types": ["sqlite", "duckdb", "postgres"],
                "default_entities": ["repo", "file", "language"],
                "join_keys": ["repo_name", "repo_id"],
                "sources": {
                    "metadata_database": {
                        "db_type": "sqlite",
                        "db_path": "query_dataset/metadata.db",
                        "clues": ["Use for repository metadata and language classifications."],
                    },
                    "artifacts_database": {
                        "db_type": "duckdb",
                        "db_path": "query_dataset/artifacts.duckdb",
                        "clues": ["Use for file contents, commit counts, and artifact text."],
                    },
                },
                "clues": [
                    "README content, language filters, and repo counts drive the q2-q4 family.",
                ],
            },
        }

    def get_schema_for_db(self, db_name: str) -> dict:
        return self.source_schemas.get(db_name, {"tables": {}, "clues": []})

    def get_schema_for_dataset(self, dataset: str) -> dict:
        return self.dataset_schemas.get(dataset.lower(), {})

    def list_sources(self) -> list[str]:
        return list(self.source_schemas.keys())

    def list_datasets(self) -> list[str]:
        return list(self.dataset_schemas.keys())
