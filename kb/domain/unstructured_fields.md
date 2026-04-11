# DataAgentBench Unstructured Field Inventory

Here is which fields across DAB datasets contain free text requiring extraction before use in queries.

**Yelp Dataset:**
- `reviews.text` (MongoDB): Free-text customer reviews. Contains sentiment, topic mentions (e.g., "clean bathrooms," "slow service"), and structured facts embedded in prose. Extraction needed for: sentiment classification, topic extraction, entity mentions.
- `tips.text` (MongoDB): Short-form user tips. Contains location-specific advice and keyword-rich fragments.
- `business.attributes` (PostgreSQL JSONB): Semi-structured key-value pairs stored as JSON. Requires explicit JSONB field projection before aggregation.

**Support/CRM Datasets:**
- `support_tickets.notes` (MongoDB): Agent-written resolution notes. Contains: resolution category (implicit), root cause description, customer sentiment indicators. Extraction needed for: categorization, resolution clustering, sentiment scoring.
- `support_tickets.customer_message` (MongoDB): Raw customer complaint text. Variations include "missing package," "never arrived," "lost shipment" for the same issue. Simple `LIKE` matching fails — use NER or classification pipeline.

**Healthcare Dataset:**
- `patient_notes.clinical_text` (PostgreSQL TEXT): Clinical encounter notes. Contains diagnosis mentions, medication references, and procedure descriptions embedded in narrative. Extraction needed for: structured coding, diagnosis counting.

**Retail/E-commerce Datasets:**
- `products.description` (PostgreSQL TEXT): Marketing copy with embedded specifications. Requires extraction to pull structured attributes (size, color, material) before joining with behavioral data.
- `clickstream.search_query` (DuckDB): Raw user search strings. Requires normalization and intent classification before aggregation.

**Rule:** Never aggregate on a free-text field directly. Always run a structured extraction step first and aggregate on the extracted output. Route to Python sandbox or `extract_structured_facts` tool.

## Injection Test
**Q:** "Which fields in the Yelp dataset contain unstructured text, and what extraction is needed?"
**Expected:** "reviews.text in MongoDB needs sentiment/topic extraction. tips.text needs keyword extraction. business.attributes in PostgreSQL needs JSONB field projection."
**Result:** PASS
**Date:** 2026-04-11
