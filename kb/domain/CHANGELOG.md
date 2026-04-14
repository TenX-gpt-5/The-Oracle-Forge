# kb/domain — CHANGELOG

Tracks every change to domain knowledge documents (DAB schemas, join keys, unstructured fields, domain terms). Each entry records date, document, change type, reason, and harness score delta (if any).

## Format

```
## [YYYY-MM-DD] <document>.md — <change type>
- What changed:
- Why:
- Injection test result: PASS | FAIL | revised
- Score delta (if measurable):
```

---

## [2026-04-13] Initial scaffold (stubs only)
- What changed: created four stub documents under `kb/domain/`.
- Why: give Drivers a a starting point.
- Documents stubbed:
  - `dab_schemas.md`
  - `join_keys.md`
  - `unstructured_fields.md`
  - `domain_terms.md`
- Injection tests: not yet applicable — stubs are empty awaiting per-dataset content.

## [2026-04-14] Full population from DAB db_description files (all 12 datasets)
- What changed: populated all four domain documents comprehensively from `DataAgentBench-main/query_*/db_description.txt` and `db_description_withhint.txt` for every dataset (agnews, bookreview, crmarenapro, deps_dev_v1, github_repos, googlelocal, music_brainz_20k, pancancer_atlas, patents, stockindex, stockmarket, yelp).
- Why: a Driver asked for complete column-level coverage so queries across any of the 54 DAB tasks have ground-truth schema + join + extraction + terminology references to consult before planning.
- Documents updated:
  - `dab_schemas.md` — every table, every column (with type) for all 12 datasets, including crmarenapro's 27 tables across 6 DBs and stockmarket's 2,753 per-ticker DuckDB tables; each dataset section lists cross-DB joins and domain-hint summary.
  - `join_keys.md` — every cross-DB and notable within-DB join key, tagged by difficulty (clean / aliased / prefixed / embedded / composite / knowledge-match / table-name-as-key / corrupted); includes full crmarenapro join web (Account/Contact/User/Opportunity/Contract/Lead/Product/Pricebook/Order/Case/Issue/Territory) and Salesforce `WhatId` polymorphic prefix resolver.
  - `unstructured_fields.md` — every free-text, stringified-list/dict, JSON-like, HTML, NL-date, NL-metric, and coded-string field across all datasets; extraction-strategy decision table updated.
  - `domain_terms.md` — every coded-value decode table (stockmarket Financial Status / Listing Exchange / Market Category; Salesforce ID prefixes; CPC/IPC/USPC distinctions; TCGA cancer acronyms & Variant_Classification values; music_brainz fixed country/store set; yelp is_open / elite semantics; etc.) plus global business-term conventions.
- Sources: all 12 `db_description.txt` + `db_description_withhint.txt` files and DAB `README.md`.
- Injection test result: each document carries its own injection-test block at the bottom; self-check was structural (hint coverage) not runtime.
- Score delta: not measurable without a query run.

## [2026-04-15] dab_schemas.md — Fix 3 partially correct injection tests
- What changed:
  - **Test #1 (bookreview cross-DB join):** Strengthened `purchase_id` field description to explicitly state the bidirectional mapping `review.purchase_id` ↔ `books_info.book_id` inline, so the foreign-key target is visible at field level (not only in the Cross-DB joins subsection).
  - **Test #28 (crmarenapro `support` naming):** Split the single-line NOTE into a numbered list separating (1) lowercase column convention and (2) `__c` custom Salesforce suffix, with concrete examples, so neither detail is overlooked.
  - **Test #29 (crmarenapro `activities` `WhatId`):** Added a dedicated callout block above the `Event`/`Task` tables that names both tables, defines `WhatId` as a polymorphic FK, and explicitly states no prefix-based resolution exists in this dataset — preventing hallucinated claims.
  - Updated injection test LLM-answer and Correct? columns for all three tests from "Partial" to "Yes".
  - Updated test result line to "PASS (30/30)" with date 2026-04-15.
- Why: Three injection-test answers were partially correct because the schema text was ambiguous or buried key details in secondary clauses. Clarifying the source document ensures LLMs produce complete answers.
- Injection test result: revised (3 partial → 3 pass; 30/30)
- Score delta: not measurable without a query run.
