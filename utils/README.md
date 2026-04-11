# Oracle Forge — Shared Utility Library

Reusable modules for the Oracle Forge data analytics agent.

## Modules

### 1. `schema_introspection.py`
Extracts table/column/type metadata from PostgreSQL, SQLite, MongoDB, and DuckDB into a normalized JSON manifest.

**Usage:**
```python
from utils.schema_introspection import introspect_schema

# Get schema for a PostgreSQL database
manifest = introspect_schema("postgresql", connection_string="postgresql://user:pass@localhost/yelp")
# Returns: [{"table": "transactions", "columns": [{"name": "customer_id", "type": "integer"}, ...]}]
```

**Test:** `python -m pytest utils/test_utils.py::TestSchemaIntrospection`

---

### 2. `join_key_resolver.py`
Detects format mismatches between join keys across databases and normalizes them to a canonical form.

**Usage:**
```python
from utils.join_key_resolver import detect_format, normalize_key, validate_overlap

# Detect format of a key sample
fmt = detect_format(["CUST-12345", "CUST-00089"])  # Returns: "prefixed_integer"

# Normalize keys to canonical integer form
normalized = normalize_key("CUST-12345", source_format="prefixed_integer", target_format="integer")
# Returns: 12345

# Validate overlap before joining
overlap = validate_overlap(left_keys=[1, 2, 3], right_keys=[2, 3, 4])
# Returns: {"matched": 2, "left_only": 1, "right_only": 1, "overlap_pct": 0.5}
```

**Test:** `python -m pytest utils/test_utils.py::TestJoinKeyResolver`

---

### 3. `multi_pass_retrieval.py`
Retrieves context from the KB in priority order: schema → domain → corrections. Assembles a minimal context payload for the agent's working memory.

**Usage:**
```python
from utils.multi_pass_retrieval import retrieve_context

# Retrieve relevant context for a query
context = retrieve_context(
    question="What is the repeat purchase rate by segment?",
    kb_path="kb/",
    max_tokens=2000
)
# Returns: {"schema": "...", "domain_terms": "...", "corrections": "...", "sources_used": [...]}
```

**Test:** `python -m pytest utils/test_utils.py::TestMultiPassRetrieval`

## Running All Tests

```bash
cd /path/to/The-Oracle-Forge
python -m pytest utils/test_utils.py -v
```
