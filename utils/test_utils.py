"""
Tests for Oracle Forge shared utility library.

Run with: python -m pytest utils/test_utils.py -v
"""

import os
import json
import tempfile
import sqlite3
import pytest

# ---------------------------------------------------------------------------
# Test: Join Key Resolver
# ---------------------------------------------------------------------------

from utils.join_key_resolver import (
    detect_format,
    normalize_key,
    validate_overlap,
    resolve_and_normalize,
)


class TestJoinKeyResolver:
    """Tests for join key format detection, normalization, and overlap validation."""

    # --- detect_format ---

    def test_detect_prefixed_integer(self):
        assert detect_format(["CUST-12345", "CUST-00089", "CUST-999"]) == "prefixed_integer"

    def test_detect_plain_integer(self):
        assert detect_format(["12345", "67890", "111"]) == "integer"

    def test_detect_uuid(self):
        assert detect_format(["550e8400-e29b-41d4-a716-446655440000"]) == "uuid"

    def test_detect_phone_dashed(self):
        assert detect_format(["123-456-7890", "987-654-3210"]) == "phone_dashed"

    def test_detect_phone_e164(self):
        assert detect_format(["+11234567890", "+19876543210"]) == "phone_e164"

    def test_detect_zero_padded(self):
        assert detect_format(["00456", "00123", "00789"]) == "zero_padded_integer"

    def test_detect_empty_returns_unknown(self):
        assert detect_format([]) == "unknown"

    def test_detect_mixed_returns_majority(self):
        # 3 integers vs 1 prefixed — should detect integer
        assert detect_format(["123", "456", "789", "CUST-1"]) == "integer"

    # --- normalize_key ---

    def test_normalize_prefixed_to_integer(self):
        assert normalize_key("CUST-12345", "prefixed_integer", "integer") == 12345

    def test_normalize_integer_to_prefixed(self):
        assert normalize_key("12345", "integer", "prefixed_integer") == "CUST-12345"

    def test_normalize_zero_padded_to_integer(self):
        assert normalize_key("00456", "zero_padded_integer", "integer") == 456

    def test_normalize_phone_dashed_to_e164(self):
        result = normalize_key("123-456-7890", "phone_dashed", "phone_e164")
        assert result == "+11234567890"

    def test_normalize_to_string_default(self):
        result = normalize_key("CUST-99", "prefixed_integer", "string")
        assert result == "99"
        assert isinstance(result, str)

    # --- validate_overlap ---

    def test_full_overlap(self):
        result = validate_overlap([1, 2, 3], [1, 2, 3])
        assert result["matched"] == 3
        assert result["overlap_pct"] == 1.0
        assert result["warning"] is None

    def test_partial_overlap(self):
        result = validate_overlap([1, 2, 3], [2, 3, 4])
        assert result["matched"] == 2
        assert result["left_only"] == 1
        assert result["right_only"] == 1

    def test_no_overlap_warns(self):
        result = validate_overlap([1, 2, 3], [4, 5, 6])
        assert result["matched"] == 0
        assert result["overlap_pct"] == 0.0
        assert "CRITICAL" in result["warning"]

    def test_low_overlap_warns(self):
        left = list(range(100))
        right = list(range(5)) + list(range(200, 300))
        result = validate_overlap(left, right)
        assert result["warning"] is not None

    # --- resolve_and_normalize (end-to-end) ---

    def test_resolve_prefixed_vs_integer(self):
        left = ["CUST-1", "CUST-2", "CUST-3"]
        right = ["1", "2", "3"]
        result = resolve_and_normalize(left, right)
        assert result["left_format"] == "prefixed_integer"
        assert result["right_format"] == "integer"
        assert result["overlap"]["matched"] == 3
        assert result["overlap"]["overlap_pct"] == 1.0

    def test_resolve_no_match(self):
        left = ["CUST-1", "CUST-2"]
        right = ["99", "100"]
        result = resolve_and_normalize(left, right)
        assert result["overlap"]["matched"] == 0


# ---------------------------------------------------------------------------
# Test: Multi-Pass Retrieval
# ---------------------------------------------------------------------------

from utils.multi_pass_retrieval import (
    estimate_tokens,
    load_kb_file,
    search_kb_for_terms,
    retrieve_context,
)


class TestMultiPassRetrieval:
    """Tests for multi-pass KB retrieval."""

    @pytest.fixture
    def mock_kb(self, tmp_path):
        """Create a temporary KB directory with test files."""
        # domain/dab_schema.md
        domain_dir = tmp_path / "domain"
        domain_dir.mkdir()
        (domain_dir / "dab_schema.md").write_text(
            "# DAB Schema\nYelp dataset has transactions table with customer_id integer column."
        )
        (domain_dir / "domain_terms.md").write_text(
            "# Domain Terms\nActive customer: purchased in last 90 days.\nChurn: >180 days inactive."
        )
        (domain_dir / "join_keys.md").write_text(
            "# Join Keys\nYelp PostgreSQL customer_id is integer. MongoDB is CUST-prefixed string."
        )
        (domain_dir / "unstructured_fields.md").write_text(
            "# Unstructured Fields\nYelp reviews.text contains free-text review content."
        )

        # corrections/corrections_log.md
        corrections_dir = tmp_path / "corrections"
        corrections_dir.mkdir()
        (corrections_dir / "corrections_log.md").write_text(
            "# Corrections\nQuery: repeat customer margin → Agent over-counted, fix: filter >1 purchase in 90 days."
        )

        # architecture
        arch_dir = tmp_path / "architecture"
        arch_dir.mkdir()
        (arch_dir / "claude_code_memory.md").write_text(
            "# Claude Code Memory\nThree-layer system: MEMORY.md index, topic files, session transcripts."
        )
        (arch_dir / "openai_data_agent.md").write_text(
            "# OpenAI Data Agent\nSix-layer context: schema, column stats, ER graphs, join keys, metrics, repairs."
        )

        return str(tmp_path)

    def test_estimate_tokens(self):
        assert estimate_tokens("hello world") > 0
        assert estimate_tokens("a" * 400) == 100  # ~4 chars per token

    def test_load_kb_file_exists(self, mock_kb):
        result = load_kb_file(mock_kb, "domain/dab_schema.md")
        assert result["path"] == "domain/dab_schema.md"
        assert "Yelp" in result["content"]
        assert result["tokens"] > 0

    def test_load_kb_file_missing(self, mock_kb):
        result = load_kb_file(mock_kb, "domain/nonexistent.md")
        assert result == {}

    def test_search_relevance_high(self):
        content = "Active customer means purchased in last 90 days in the Yelp dataset."
        question = "What is an active customer in the Yelp dataset?"
        score = search_kb_for_terms(content, question)
        assert score > 0.3

    def test_search_relevance_low(self):
        content = "MongoDB aggregation pipelines require explicit field projection."
        question = "What is the fiscal year boundary?"
        score = search_kb_for_terms(content, question)
        assert score < 0.2

    def test_retrieve_context_basic(self, mock_kb):
        result = retrieve_context(
            question="What is an active customer in the Yelp dataset?",
            kb_path=mock_kb,
            max_tokens=4000,
        )
        assert len(result["sources_used"]) > 0
        assert result["total_tokens"] > 0
        assert "context_text" in result

    def test_retrieve_context_always_includes_schema(self, mock_kb):
        result = retrieve_context(
            question="Something completely unrelated to anything",
            kb_path=mock_kb,
            max_tokens=4000,
        )
        assert "domain/dab_schema.md" in result["sources_used"]

    def test_retrieve_context_always_includes_corrections(self, mock_kb):
        result = retrieve_context(
            question="Something completely unrelated",
            kb_path=mock_kb,
            max_tokens=4000,
        )
        assert "corrections/corrections_log.md" in result["sources_used"]

    def test_retrieve_context_respects_budget(self, mock_kb):
        result = retrieve_context(
            question="active customer churn Yelp",
            kb_path=mock_kb,
            max_tokens=50,  # Very tight budget
        )
        # Should skip files that exceed budget
        assert len(result["sources_skipped"]) > 0


# ---------------------------------------------------------------------------
# Test: Schema Introspection (SQLite only - no external DB needed)
# ---------------------------------------------------------------------------

from utils.schema_introspection import introspect_schema, manifest_to_compact_text


class TestSchemaIntrospection:
    """Tests for schema introspection using SQLite (no external DB required)."""

    @pytest.fixture
    def sample_sqlite_db(self, tmp_path):
        """Create a temporary SQLite database with test data."""
        db_path = str(tmp_path / "test.db")
        conn = sqlite3.connect(db_path)
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE customers (
                customer_id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                email TEXT,
                signup_date TEXT
            )
        """)
        cur.execute("""
            CREATE TABLE orders (
                order_id INTEGER PRIMARY KEY,
                customer_id INTEGER,
                amount REAL,
                status TEXT,
                FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
            )
        """)
        cur.executemany(
            "INSERT INTO customers VALUES (?, ?, ?, ?)",
            [
                (1, "Alice", "alice@example.com", "2025-01-15"),
                (2, "Bob", "bob@example.com", "2025-02-20"),
                (3, "Carol", None, "2025-03-10"),
            ],
        )
        cur.executemany(
            "INSERT INTO orders VALUES (?, ?, ?, ?)",
            [
                (101, 1, 99.99, "completed"),
                (102, 1, 149.50, "completed"),
                (103, 2, 25.00, "pending"),
            ],
        )
        conn.commit()
        conn.close()
        return db_path

    def test_introspect_sqlite(self, sample_sqlite_db):
        manifest = introspect_schema("sqlite", db_path=sample_sqlite_db)
        assert len(manifest) == 2

        table_names = [m["table"] for m in manifest]
        assert "customers" in table_names
        assert "orders" in table_names

    def test_sqlite_columns_correct(self, sample_sqlite_db):
        manifest = introspect_schema("sqlite", db_path=sample_sqlite_db)
        customers = next(m for m in manifest if m["table"] == "customers")
        col_names = [c["name"] for c in customers["columns"]]
        assert "customer_id" in col_names
        assert "name" in col_names
        assert "email" in col_names

    def test_sqlite_row_count(self, sample_sqlite_db):
        manifest = introspect_schema("sqlite", db_path=sample_sqlite_db)
        customers = next(m for m in manifest if m["table"] == "customers")
        assert customers["row_count_estimate"] == 3

    def test_sqlite_primary_key_detected(self, sample_sqlite_db):
        manifest = introspect_schema("sqlite", db_path=sample_sqlite_db)
        customers = next(m for m in manifest if m["table"] == "customers")
        pk_col = next(c for c in customers["columns"] if c["name"] == "customer_id")
        assert pk_col["is_primary_key"] is True

    def test_sqlite_sample_values(self, sample_sqlite_db):
        manifest = introspect_schema("sqlite", db_path=sample_sqlite_db)
        customers = next(m for m in manifest if m["table"] == "customers")
        assert "customer_id" in customers["sample_values"]
        assert len(customers["sample_values"]["customer_id"]) > 0

    def test_unsupported_db_type_raises(self):
        with pytest.raises(ValueError, match="Unsupported db_type"):
            introspect_schema("mysql", connection_string="mysql://localhost")

    def test_manifest_to_compact_text(self, sample_sqlite_db):
        manifest = introspect_schema("sqlite", db_path=sample_sqlite_db)
        text = manifest_to_compact_text(manifest)
        assert "[sqlite]" in text
        assert "customers" in text
        assert "customer_id" in text
