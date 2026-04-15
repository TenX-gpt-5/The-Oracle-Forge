# DataAgentBench Unstructured Field Inventory

**Injection Test Evidence:**
*   **Test Query:** "Can I aggregate directly on the review text field?"
*   **Expected Answer:** "No, you must never aggregate on a free-text field directly. You must run a structured extraction step first and aggregate on the extracted output."

Here is which fields across DAB datasets contain free text requiring extraction before use in queries.

**Rule:** Never aggregate on a free-text field directly. Always run a structured extraction step first and aggregate on the extracted output. Route to Python sandbox or `extract_structured_facts` tool.

---
name: Unstructured fields inventory
description: Every field across the 12 DAB datasets that contains free text, stringified JSON/list/dict, HTML, or NL-formatted metadata — with required extraction strategy.
type: domain
status: populated from DAB db_description.txt for all 12 datasets. Refine per-field as Drivers hit specific queries.
source: DataAgentBench-main/query_*/db_description{,_withhint}.txt
---

# Unstructured fields inventory

DAB's third hard requirement: queries often need structured facts extracted from free text, stringified containers, HTML, or natural-language metadata. The agent must detect when a field requires extraction **before** aggregating, filtering, or joining.

Type taxonomy:
- **free-text** — prose; extract entities/sentiment via LLM (regex works only for well-bounded patterns).
- **stringified-list** / **stringified-dict** — Python repr serialized to str; parse with `ast.literal_eval` first, then `json.loads` with quote normalization.
- **json-like** — JSON-shaped text (arrays or objects); parse with `json.loads`.
- **nl-date** — natural-language date string (e.g., `"March 15th, 2020"`); parse with `dateutil.parser` or LLM.
- **nl-metric** — natural-language numeric metadata (e.g., byte counts inside a description); regex for digits+units, LLM fallback.
- **html** — HTML content; strip tags with `BeautifulSoup(...).get_text()` before LLM.
- **coded-string** — short code whose meaning is elsewhere (e.g., single letters like `D`/`E`); see `domain_terms.md`.

## Entry format

```
### <dataset>.<table_or_collection>.<field>  [type]
- Stored as:
- Contains:
- Extraction needed for: <kinds of queries>
- Tool: <primary → fallback>
```

---

## 1. agnews

### agnews.articles.title  [free-text]
- Stored as: str.
- Contains: article title.
- Extraction needed for: topic classification (the four DAB categories: World, Sports, Business, Science/Technology).
- Tool: LLM classifier on title+description.

### agnews.articles.description  [free-text]
- Stored as: str.
- Contains: article body / description.
- Extraction needed for: topic classification; entity/topic extraction.
- Tool: LLM.

---

## 2. bookreview

### bookreview.books_info.features  [stringified-list]
- Stored as: str (Python repr of a list).
- Contains: bullet-list of book features.
- Tool: `ast.literal_eval` → fallback `json.loads` after quote normalization.

### bookreview.books_info.description  [stringified-list]
- Stored as: str (Python repr — confirmed by hint: "appears to be in list or dictionary format, but actually stored as strings").
- Contains: prose description, often split into list of paragraphs.
- Tool: `ast.literal_eval` → LLM for entity extraction from resulting strings.

### bookreview.books_info.categories  [stringified-list]
- Stored as: str (Python repr).
- Contains: category hierarchy (Amazon-style).
- Extraction needed for: category filters ("all books in `Children's Books` > `Literature`").
- Tool: `ast.literal_eval` → substring filter.

### bookreview.books_info.details  [stringified-dict]
- Stored as: str (Python repr of dict).
- Contains: misc key-value metadata (ISBN, publisher, dimensions, ...).
- Extraction needed for: price tier, publisher filters, dimensions.
- Tool: `ast.literal_eval`.

### bookreview.review.text  [free-text]
- Stored as: str.
- Contains: review body.
- Extraction needed for: sentiment, topic, entity mentions.
- Tool: LLM.

### bookreview.review.title  [free-text]
- Stored as: str.
- Contains: review title — short but still unstructured.

---

## 3. crmarenapro

### crmarenapro.core_crm.Account.Description  [free-text]
- Stored as: str.
- Contains: company description.
- Extraction needed for: industry/keyword filters beyond `Industry` field.

### crmarenapro.sales_pipeline.Contract.Description  [free-text]
### crmarenapro.sales_pipeline.Opportunity.Description  [free-text]
### crmarenapro.sales_pipeline.Quote.Description  [free-text]
- Stored as: str (sales-deal notes).
- Extraction needed for: qualitative deal classification.

### crmarenapro.support.Case.subject  [free-text]
### crmarenapro.support.Case.description  [free-text]
- Stored as: str.
- Contains: support-ticket subject + body — canonical DAB unstructured probe (see Challenge Brief).
- Extraction needed for: sentiment, issue-category counts.
- Tool: LLM.

### crmarenapro.support.knowledge__kav.title / summary / faq_answer__c  [free-text]
- Stored as: str.
- Contains: Salesforce knowledge-article content.
- Extraction needed for: FAQ lookup / content classification.

### crmarenapro.support.issue__c.description__c  [free-text]
### crmarenapro.support.casehistory__c.oldvalue__c / newvalue__c  [free-text or coded]
- Stored as: str.
- Contains: change history — may include IDs or status codes as values; treat case-by-case.

### crmarenapro.support.emailmessage.subject / textbody  [free-text]
- Stored as: str.
- Contains: email subject + body.
- Tool: LLM (subject may be enough for routing/classification).

### crmarenapro.support.livechattranscript.body  [free-text]
- Stored as: str.
- Contains: full chat transcript.
- Tool: LLM.

### crmarenapro.products_orders.Product2.Description  [free-text]
### crmarenapro.products_orders.Pricebook2.Description  [free-text]
### crmarenapro.territory.Territory2.Description  [free-text]
- Stored as: str.
- Contains: catalog / territory descriptions.

### crmarenapro.activities.Event.Description / Event.Subject / Event.Location  [free-text]
### crmarenapro.activities.Task.Description / Task.Subject  [free-text]
- Stored as: str.
- Contains: calendar/task notes.

### crmarenapro.activities.VoiceCallTranscript__c.Body__c  [free-text]
- Stored as: str — heavy extraction target.
- Contains: full voice-call transcript.
- Tool: LLM only.

---

## 4. deps_dev_v1

### deps_dev_v1.packageinfo.Licenses  [json-like]
- Stored as: str (JSON-like array of license identifiers).
- Tool: `json.loads`.

### deps_dev_v1.packageinfo.Links  [json-like]
- Stored as: JSON-like list of link objects.
- Contains: origin / documentation / source-code URLs.
- Tool: `json.loads` → filter by link type.

### deps_dev_v1.packageinfo.Advisories  [json-like]
- Stored as: JSON-like list of security advisories.
- Extraction needed for: vulnerability counts, CVE lookups.
- Tool: `json.loads`.

### deps_dev_v1.packageinfo.VersionInfo  [json-like]
- Stored as: JSON-like object (`IsRelease`, `Ordinal`, ...).
- Tool: `json.loads`.

### deps_dev_v1.packageinfo.Hashes  [json-like]
- Stored as: JSON-like list of file hashes.
- Tool: `json.loads`.

### deps_dev_v1.packageinfo.Registries  [json-like]
- Stored as: JSON-like list of registries.
- Tool: `json.loads`.

### deps_dev_v1.packageinfo.UpstreamIdentifiers  [json-like]
- Stored as: JSON-like list of upstream identifier objects.
- Tool: `json.loads`.

### deps_dev_v1.project_info.Licenses  [json-like]
- Stored as: JSON-like array.

### deps_dev_v1.project_info.Project_Information  [free-text + nl-metric]
- Stored as: str (natural language).
- Contains: project description **plus GitHub stars count, fork count, name, owner** (per hint).
- Extraction needed for: repo metrics queries; also holds the project identifier that join_keys resolves.
- Tool: regex for digit patterns (`\d+ stars`, `\d+ forks`) → LLM fallback.

### deps_dev_v1.project_info.Description  [free-text]
- Stored as: str — separate from `Project_Information`.

---

## 5. github_repos

### github_repos.metadata_database.languages.language_description  [nl-metric / free-text]
- Stored as: str in natural language, multiple languages per repo.
- Contains: languages + byte counts (NL form; per hint "compare the relative number of bytes").
- Extraction needed for: primary language detection.
- Tool: regex for `<language> <N> bytes` patterns → LLM fallback.

### github_repos.artifacts_database.contents.content  [free-text]
- Stored as: str — raw file content; large/binary may be truncated.
- Extraction needed for: code search, license detection, keyword counts.
- Tool: direct substring / regex for code; LLM for semantic extraction.

### github_repos.artifacts_database.contents.repo_data_description  [nl-metric / free-text]
- Stored as: NL metadata string.
- Contains: original `size`, `binary`, `copies`, `mode` attributes in prose form (per hint).
- Extraction needed for: file-attribute filters.
- Tool: regex → LLM fallback.

### github_repos.artifacts_database.commits.parent  [json-like]
- Stored as: JSON-like list of parent SHAs (length > 1 for merge commits).

### github_repos.artifacts_database.commits.author  [json-like]
### github_repos.artifacts_database.commits.committer  [json-like]
- Stored as: JSON-like object `{name, email, timestamp}`.

### github_repos.artifacts_database.commits.trailer  [json-like]
- Stored as: JSON-like trailer metadata.

### github_repos.artifacts_database.commits.difference  [json-like]
- Stored as: JSON-like structure describing file changes.
- Tool: `json.loads`.

### github_repos.artifacts_database.commits.subject / message  [free-text]
- Stored as: str (commit log text).
- Extraction needed for: conventional-commit parsing, bug-fix detection.

---

## 6. googlelocal

### googlelocal.business_description.description  [free-text]
- Stored as: str.
- Contains: business description, often including **location info** (per hint) — city/state may only appear here.
- Extraction needed for: geographic filters.
- Tool: regex for well-known city/state patterns → LLM fallback.

### googlelocal.business_description.hours  [stringified-list or list]
- Stored as: list representation (may be stringified).
- Contains: operating hours across days.
- Tool: parse list → structured hours.

### googlelocal.business_description.MISC  [stringified-dict or dict]
- Stored as: dict (keys vary across businesses — schema-less).
- Extraction needed for: any feature/attribute filter.
- Tool: dict access; enumerate distinct keys first if schema unknown.

### googlelocal.review.text  [free-text]
- Stored as: str.
- Extraction needed for: sentiment / topic queries.
- Tool: LLM.

---

## 7. music_brainz_20k

### music_brainz_20k.tracks.title / artist / album  [free-text]
- Stored as: str — mostly clean but with cross-source duplicates needing entity resolution.
- Extraction needed for: deduplication via fuzzy matching.
- Tool: string normalization + fuzzy matching (`rapidfuzz`, Levenshtein).

### music_brainz_20k.tracks.year / length / language  [semi-structured str]
- Stored as: str — may have multiple formats (year `"1999"` vs `"1999-01-01"`; length `"210"` seconds vs `"3:30"`).
- Tool: normalize during entity resolution.

---

## 8. pancancer_atlas

### pancancer_atlas.clinical_info.Patient_description  [free-text]
- Stored as: str.
- Contains: patient-level free text with `uuid`, `barcode`, `gender`, `vital status` and more embedded.
- Extraction needed for: barcode join key (→ `ParticipantBarcode`); demographic filters.
- Tool: regex for barcode pattern (TCGA-style) → LLM fallback.

### pancancer_atlas.clinical_info (remaining ~100 attributes)  [typed columns]
- Stored as: mostly structured columns (demographics, diagnosis, treatment). Each attribute is a distinct column — inventory when hitting a specific query.

---

## 9. patents

### patents.publicationinfo.Patents_info  [free-text]
- Stored as: NL summary string.
- Contains: `application_number`, `publication_number`, `assignee_harmonized`, `country_code` embedded in prose.
- Extraction needed for: anything keyed by publication/application number.
- Tool: regex for number patterns → LLM fallback.

### patents.publicationinfo.title_localized / abstract_localized  [free-text]
- Stored as: str.
- Extraction needed for: topic, keyword classification.
- Tool: LLM.

### patents.publicationinfo.claims_localized_html  [html]
### patents.publicationinfo.description_localized_html  [html]
- Stored as: HTML.
- Extraction needed for: full-text search, claim-count, description semantics.
- Tool: `BeautifulSoup(..., "html.parser").get_text()` → LLM on plain text.

### patents.publicationinfo.publication_date  [nl-date]
### patents.publicationinfo.filing_date  [nl-date]
### patents.publicationinfo.grant_date  [nl-date]
### patents.publicationinfo.priority_date  [nl-date]
- Stored as: str like `"March 15th, 2020"` (per db_description).
- Extraction needed for: date range filters, ordering.
- Tool: `dateutil.parser.parse` (handles ordinals on most locales) → LLM fallback.

### patents.publicationinfo.cpc  [json-like]
- Stored as: JSON-like list of CPC entries (code + metadata).
- Tool: `json.loads` (or `ast.literal_eval`) then explode per code.

### patents.publicationinfo.priority_claim / inventor_harmonized / citation / parent / child  [stringified-list or json-like]
- Stored as: list-like strings.
- Tool: `ast.literal_eval` → `json.loads` fallback.

### patents.publicationinfo.uspc / ipc  [coded-string]
- Stored as: classification codes (may be list-like if multiple).

### patents.cpc_definition.childGroups / children / parents  [json-like]
- Stored as: JSON-like hierarchy lists.
- Tool: `json.loads`.

### patents.cpc_definition.dateRevised  [nl-date]
- Stored as: NL date.

### patents.cpc_definition.definition / glossary / rules / synonyms / titleFull / titlePart  [free-text]
- Stored as: str (CPC reference content).

---

## 10. stockindex

### stockindex.index_info.Exchange  [free-text / knowledge-key]
- Stored as: str — full exchange name.
- Extraction needed for: knowledge-based join with `index_trade.Index` abbreviation.

(Other fields in stockindex are numeric/structured.)

---

## 11. stockmarket

### stockmarket.stockinfo."Company Description"  [free-text]
- Stored as: str — company name + description concatenated.
- Extraction needed for: company-name extraction, sector/keyword filters.
- Tool: split on common delimiters → LLM fallback.

### stockmarket.stockinfo."Financial Status"  [coded-string]
### stockmarket.stockinfo."Listing Exchange"  [coded-string]
### stockmarket.stockinfo."Market Category"  [coded-string]
- Stored as: str, single-letter codes (see `domain_terms.md`).

---

## 12. yelp

### yelp.business.description  [free-text]
- Stored as: str (MongoDB).
- Contains: business description **with location info** (city/state).
- Extraction needed for: location filters (e.g., "businesses in Indianapolis, Indiana" — query_yelp/query1).
- Tool: regex for `City, State` patterns → LLM fallback.

### yelp.business.attributes  [dict or null]
- Stored as: dict (MongoDB) or null.
- Contains: parking, WiFi, service flags.
- Tool: direct dict lookup; handle null.

### yelp.business.hours  [dict or null]
- Stored as: dict (MongoDB) or null.
- Contains: operating hours per weekday.
- Tool: direct dict lookup.

### yelp.checkin.date  [list of str]
- Stored as: list of timestamp strings.
- Tool: iterate; parse with `datetime.fromisoformat` or `dateutil`.

### yelp.review.text  [free-text]
### yelp.tip.text  [free-text]
- Stored as: str (DuckDB).
- Extraction needed for: sentiment, topic mentions, entity extraction.
- Tool: LLM.

### yelp.user.elite  [coded-string / list-as-string]
- Stored as: str — comma-separated list of years (e.g., `"2016,2017,2019"`); empty if never elite.
- Extraction needed for: elite tenure, filtering.
- Tool: `s.split(',')` after trim; handle empty.

### yelp.user.yelping_since  [date-str]
- Stored as: str timestamp — usually parseable directly.

---

## Extraction strategy decision table

| Content | First try | Fallback |
|---|---|---|
| Stringified list/dict (Python repr) | `ast.literal_eval` | `json.loads` after quote normalization |
| Stringified JSON / JSON-like | `json.loads` | `ast.literal_eval` |
| HTML | `BeautifulSoup(..).get_text()` | regex tag strip |
| NL date | `dateutil.parser.parse` | LLM extraction |
| Free text + specific entity (city, number, barcode, SHA) | regex | LLM extraction |
| Free text + sentiment / topic / classification | LLM | — |
| NL metric (bytes, stars, forks) in prose | regex for `\d+[ ]?<unit>` | LLM extraction |
| Coded single-letter fields (`D`/`E`/`Q`) | lookup table (see `domain_terms.md`) | — |
| Polymorphic Salesforce `WhatId` | ID-prefix lookup (`001`/`003`/`006`) | — |

## Injection test
**Question 1:** "Which fields in the Yelp dataset contain unstructured text, and what extraction is needed?"
**Expected:** "reviews.text in MongoDB needs sentiment/topic extraction. tips.text needs keyword extraction. business.attributes in PostgreSQL needs JSONB field projection."
**Result:** PASS
**Date:** 2026-04-11

**Question 2:** "In the patents dataset, what two storage formats require parsing before the agent can aggregate by date, and what tools should it use?" 

### Expected Answer: 
(1) `publicationinfo.{publication_date, filing_date, grant_date, priority_date}` are natural-language strings ("March 15th, 2020") — parse with `dateutil.parser`; (2) `publicationinfo.cpc` is a JSON-like list — parse with `json.loads` (or `ast.literal_eval`) and explode per code. HTML fields (`claims_localized_html`, `description_localized_html`) must be stripped with BeautifulSoup before any textual analysis.

### LLM Answer (Correct):
Format 1: patents.publicationinfo.publication_date / filing_date / grant_date / priority_date are nl-date strings and must be parsed with dateutil.parser (LLM fallback).
Format 2: patents.publicationinfo.cpc is json-like and must be parsed with json.loads (or ast.literal_eval) before exploding codes for aggregation.
**Result:** PASS
**Date:** 2026-04-16
