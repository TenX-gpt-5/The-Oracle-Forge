# DataAgentBench Join Key Glossary

**Injection Test Evidence:**
*   **Test Query:** "Join Yelp ratings to the primary DuckDB metrics table."
*   **Expected Answer:** "Agent must normalize `businessid_X` to `businessref_X` before joining, returning a valid float rating rather than a 0-row empty result."

# Join Keys

Oracle Forge should assume that shared business or customer identity may be represented differently across databases even when the entities are logically the same.

The strongest verified example in the current repo is the Yelp path:

- MongoDB business metadata uses `business_id` values like `businessid_52`
- DuckDB review rows use `business_ref` values like `businessref_52`

Those values do not match directly. Oracle Forge must normalize them before joining or filtering across sources. In the current implementation, the working Yelp strategy converts `businessid_*` to `businessref_*` before running the DuckDB aggregation.

The general rule for Oracle Forge is:
- never assume raw identifiers align across systems
- inspect format before joining

---
name: Ill-formatted join keys glossary
description: Every cross-database (and notable within-database) join key across all 12 DAB datasets — how the key appears in each DB and how to resolve.
type: domain
status: populated from DAB db_description.txt + db_description_withhint.txt for all 12 datasets. Refine as Drivers encounter real breakage.
source: DataAgentBench-main/query_*/db_description{,_withhint}.txt; DAB paper §3
---

# Ill-formatted join keys

The "same" entity carries different string formats across DBMSes. Joining naïvely returns zero rows. The agent must detect the mismatch and normalise **before** the join.

Difficulty taxonomy (used throughout):
- **clean** — same field name, same format; joining just works. Documented for completeness.
- **aliased** — same value, different column name (rename only).
- **prefixed** — same integer/id suffix, different string prefix.
- **embedded** — join key is embedded in free text or JSON-like blob; requires extraction.
- **composite** — multi-column join.
- **knowledge-match** — requires external domain knowledge (no syntactic resolver).
- **table-name-as-key** — join key is literally a DuckDB/SQLite table name; needs dynamic SQL.
- **corrupted** — key has corruption (extra `#`, trailing whitespace) that must be cleaned.

## Entry format

```
### <dataset> — <entity>  [difficulty]
- In <DB A> (<DBMS>): <field name> = <format example>
- In <DB B> (<DBMS>): <field name> = <format example>
- Relationship: <how they relate>
- Resolver: <code snippet or rule>
- First observed: <source>
```

---

## 1. agnews

### agnews — article identity  [clean]
- In `articles_database` (MongoDB, `articles`): `article_id` (int)
- In `metadata_database` (SQLite, `article_metadata`): `article_id` (int)
- Relationship: identical integer.
- Resolver: direct equality.

### agnews — author identity  [clean]
- In `metadata_database.authors`: `author_id` (int)
- In `metadata_database.article_metadata`: `author_id` (int)
- Relationship: within SQLite, direct.

---

## 2. bookreview

### bookreview — book identity  [aliased]
- In `books_database.books_info` (PostgreSQL): `book_id` (str)
- In `review_database.review` (SQLite): `purchase_id` (str) — "Unique identifier linking to book_id"
- Relationship: same value, different column name.
- Resolver: alias rename in the JOIN; no transform needed.
- Source: bookreview db_description + hint.

---

## 3. crmarenapro

### crmarenapro — Salesforce-style IDs (universal)  [corrupted]
- Field shape: Salesforce 18-char string IDs (e.g., `001Wt00000PFj4zIAD`).
- **Corruption #1:** ~25% of ID-like fields carry a leading `#` (e.g., `#001Wt00000PFj4zIAD`).
- **Corruption #2:** ~20% of text fields have trailing whitespace (e.g., `"Company Name "`).
- Affected columns: `Id`, `AccountId`, `ContactId`, `Name`, `FirstName`, `LastName`, `Email`, `Subject`, `Status`.
- Resolver: `strip('#').strip()` (or `TRIM(LEADING '#' FROM col)` then `RTRIM(col)`) on **both sides of every join**. This is mandatory, not optional — naïve joins silently drop ~25% of matches.
- Source: crmarenapro db_description_withhint.txt.

### crmarenapro — case sensitivity across DBMSes  [aliased]
- `core_crm` (SQLite), `sales_pipeline` / `activities` (DuckDB), `products_orders` / `territory` (SQLite) use **mixed-case** columns (`AccountId`, `ContactId`).
- `support` (PostgreSQL) uses **lowercase** (`accountid`, `contactid`).
- Resolver: alias in SQL (`SELECT accountid AS "AccountId" ...`) or be case-aware in joins.

### crmarenapro — Account ↔ ...  [prefixed + corrupted]
- `core_crm.Account.Id` ↔ `sales_pipeline.Opportunity.AccountId` ↔ `sales_pipeline.Contract.AccountId` ↔ `sales_pipeline.Quote.AccountId` ↔ `sales_pipeline.Lead.ConvertedAccountId` ↔ `support.Case.accountid` ↔ `support.livechattranscript.accountid` ↔ `products_orders.Order.AccountId`.

### crmarenapro — Contact ↔ ...
- `core_crm.Contact.Id` ↔ `Contact.AccountId` (self, to Account) ↔ `sales_pipeline.Opportunity.ContactId` ↔ `sales_pipeline.Quote.ContactId` ↔ `sales_pipeline.Lead.ConvertedContactId` ↔ `support.Case.contactid` ↔ `support.livechattranscript.contactid`.

### crmarenapro — User/Owner ↔ ...
- `core_crm.User.Id` ↔ every `OwnerId` field across DBs (`Opportunity.OwnerId`, `Lead.OwnerId`, `Order.OwnerId`, `Case.ownerid`, `livechattranscript.ownerid`, `Event.OwnerId`, `Task.OwnerId`) ↔ `territory.UserTerritory2Association.UserId`.

### crmarenapro — Opportunity ↔ ...
- `sales_pipeline.Opportunity.Id` ↔ `OpportunityLineItem.OpportunityId` ↔ `Quote.OpportunityId` ↔ `activities.VoiceCallTranscript__c.OpportunityId__c`.

### crmarenapro — Contract ↔ Opportunity.ContractID__c
- `sales_pipeline.Contract.Id` ↔ `sales_pipeline.Opportunity.ContractID__c` (Salesforce custom-FK field).

### crmarenapro — Lead ↔ VoiceCallTranscript
- `sales_pipeline.Lead.Id` ↔ `activities.VoiceCallTranscript__c.LeadId__c`.

### crmarenapro — Product & Pricebook web
- `products_orders.Product2.Id` ↔ `OpportunityLineItem.Product2Id`, `QuoteLineItem.Product2Id`, `OrderItem.Product2Id`, `ProductCategoryProduct.ProductId`, `PricebookEntry.Product2Id`.
- `products_orders.Pricebook2.Id` ↔ `PricebookEntry.Pricebook2Id`, `Order.Pricebook2Id`.
- `products_orders.PricebookEntry.Id` ↔ `OpportunityLineItem.PricebookEntryId`, `QuoteLineItem.PricebookEntryId`, `OrderItem.PriceBookEntryId` (**note inconsistent casing**: `PriceBookEntryId` in OrderItem vs `PricebookEntryId` elsewhere).
- `products_orders.ProductCategory.Id` ↔ `ProductCategoryProduct.ProductCategoryId`.

### crmarenapro — Order / OrderItem / Case.orderitemid__c
- `products_orders.Order.Id` ↔ `OrderItem.OrderId`.
- `products_orders.OrderItem.Id` ↔ `support.Case.orderitemid__c` (cross-DB custom FK).

### crmarenapro — Quote line-items
- `sales_pipeline.Quote.Id` ↔ `QuoteLineItem.QuoteId`.
- `sales_pipeline.OpportunityLineItem.Id` ↔ `QuoteLineItem.OpportunityLineItemId`.

### crmarenapro — Support internals
- `support.Case.id` ↔ `casehistory__c.caseid__c`, `livechattranscript.caseid`.
- `support.issue__c.id` ↔ `support.Case.issueid__c`.

### crmarenapro — Territory
- `territory.Territory2.Id` ↔ `UserTerritory2Association.Territory2Id`.

### crmarenapro — Activities polymorphic (`WhatId`)  [embedded]
- `activities.Event.WhatId` and `activities.Task.WhatId` are **polymorphic** — they can reference any record type (Opportunity, Account, Contact, Case, ...). The prefix of the ID value indicates the object type (`001...` = Account, `003...` = Contact, `006...` = Opportunity per standard Salesforce ID prefixes).
- Resolver: parse first 3 chars, route the join to the right table.

---

## 4. deps_dev_v1

### deps_dev_v1 — package identity  [composite]
- In `package_database.packageinfo` (SQLite): `(System, Name, Version)`
- In `project_database.project_packageversion` (DuckDB): `(System, Name, Version)`
- Relationship: 3-column composite join; match all three.
- Resolver: `JOIN ... USING (System, Name, Version)`.

### deps_dev_v1 — project identity  [embedded]
- In `project_database.project_packageversion`: `ProjectName` (str, e.g., `"owner/repo"`)
- In `project_database.project_info`: `Project_Information` (str) — free text containing the project name **plus** stars/forks/description.
- Relationship: `ProjectName` appears **inside** the free-text `Project_Information` — likely prefix or substring match.
- Resolver: regex/substring search of `ProjectName` in `Project_Information` (or LLM extraction). Fragile — expect false matches for generic names.
- Source: deps_dev_v1 hint.

---

## 5. github_repos

### github_repos — repo identity (universal)  [aliased]
- In `metadata_database.{languages, licenses, repos}` (SQLite): `repo_name` (str, `"owner/repo"`)
- In `artifacts_database.contents` (DuckDB): `sample_repo_name` (str)
- In `artifacts_database.{commits, files}` (DuckDB): `repo_name` (str)
- Relationship: same value; `contents` uses a different column alias.
- Resolver: `contents.sample_repo_name = X.repo_name`; otherwise direct equality.

### github_repos — blob / file linkage  [clean]
- `artifacts_database.files.id` ↔ `artifacts_database.contents.id` (blob id).

### github_repos — ref + path per file (within-repo)  [composite]
- `files.(repo_name, ref, path)` ↔ `contents.(sample_repo_name, sample_ref, sample_path)` when you need file-at-commit.

---

## 6. googlelocal

### googlelocal — business identity  [clean]
- In `review_database.review` (SQLite): `gmap_id` (str)
- In `business_database.business_description` (PostgreSQL): `gmap_id` (str)
- Relationship: shared key, shared name.
- Resolver: direct equality. (Documented as negative case — no mismatch observed.)

---

## 7. music_brainz_20k

### music_brainz_20k — track identity  [clean]
- In `tracks_database.tracks` (SQLite): `track_id` (int) — **unique in this DB** (per-row id).
- In `sales_database.sales` (DuckDB): `track_id` (int) — references `tracks.track_id`.
- Resolver: direct equality.
- **Gotcha:** `tracks.source_track_id` is NOT unique (duplicates from multiple sources). Never join on it.

### music_brainz_20k — entity resolution over tracks  [embedded — semantic]
- Within `tracks`, multiple `track_id` rows can represent the same real-world track (different `source_id`).
- Resolver: fuzzy match on `(title, artist, album, year)` with tolerance for year-format / minor-text variants. Pure string equality will under-count.
- Source: music_brainz_20k hint.

---

## 8. pancancer_atlas

### pancancer_atlas — patient barcode  [embedded]
- In `clinical_database.clinical_info` (PostgreSQL): `Patient_description` (str) — free text containing `uuid`, `barcode`, `gender`, `vital status`, etc.
- In `molecular_database.Mutation_Data` / `RNASeq_Expression` (SQLite): `ParticipantBarcode` (str) — direct id.
- Relationship: `ParticipantBarcode` is **embedded inside** `Patient_description` free text.
- Resolver: extract barcode from `Patient_description` via regex/LLM, then equality-join on barcode.
- Source: pancancer_atlas hint.

### pancancer_atlas — sample/aliquot chain  [clean]
- Within `molecular_database`: `Mutation_Data.Tumor_SampleBarcode`/`Tumor_AliquotBarcode` and `RNASeq_Expression.SampleBarcode`/`AliquotBarcode` are direct strings; join on equality when needed.

---

## 9. patents

### patents — CPC code  [embedded]
- In `publication_database.publicationinfo` (SQLite): `cpc` (str) — **JSON-like list of CPC entries** (code + metadata).
- In `CPCDefinition_database.cpc_definition` (PostgreSQL): `symbol` (str) — one CPC code per row.
- Relationship: parse `cpc` JSON in `publicationinfo`, explode per code, then join each code to `cpc_definition.symbol`.
- Resolver: `json.loads(cpc)` (or `ast.literal_eval`), iterate, equality-join.
- Source: patents hint.

### patents — patent family + citation graph  [within-DB, JSON-like]
- `publicationinfo.family_id` — groups publications in the same family.
- `publicationinfo.citation` — JSON-like list of cited patents (match against other rows' `publication_number` embedded inside `Patents_info`).
- `publicationinfo.{parent, child}` — parent/child application links.

---

## 10. stockindex

### stockindex — exchange ↔ index  [knowledge-match]
- In `indexinfo_database.index_info` (SQLite): `Exchange` (str, **full name**, e.g., "Tokyo Stock Exchange").
- In `indextrade_database.index_trade` (DuckDB): `Index` (str, **abbreviation**, e.g., `N225`, `HSI`, `000001.SS`).
- Relationship: requires world-knowledge mapping (Tokyo SE → N225; Hong Kong SE → HSI; Shanghai SE → 000001.SS; NYSE → ^NYA or DJI depending on definition, etc.).
- Resolver: maintain or prompt an LLM for an {Exchange ↔ Index} dictionary; **no syntactic resolver**.
- Source: stockindex hint.

---

## 11. stockmarket

### stockmarket — ticker ↔ trade table  [table-name-as-key]
- In `stockinfo_database.stockinfo` (SQLite): `Symbol` (str, ticker).
- In `stocktrade_database` (DuckDB): ticker values appear as **DuckDB table names** (2,753 tables, one per stock).
- Relationship: `stockinfo.Symbol` is **literally the name** of the corresponding DuckDB table.
- Resolver: programmatic. Example (DuckDB via Python):
  ```python
  import duckdb
  con = duckdb.connect(duckdb_path, read_only=True)
  tables = {r[0] for r in con.execute("SHOW TABLES").fetchall()}
  for sym in stockinfo_symbols:
      if sym in tables:
          df = con.execute(f'SELECT * FROM "{sym}"').df()
  ```
  Always quote table names (tickers may contain special characters).
- Symbol normalization may be needed: some tickers are case-variant or carry suffixes.

---

## 12. yelp

### yelp — business identity  [prefixed]
- In `businessinfo_database.business` (MongoDB): `business_id` (str) = `"businessid_<N>"`.
- In `businessinfo_database.checkin` (MongoDB): `business_id` (str) = `"businessid_<N>"` (within Mongo — direct).
- In `user_database.review`, `user_database.tip` (DuckDB): `business_ref` (str) = `"businessref_<N>"`.
- Relationship: same integer suffix `<N>`, prefix differs.
- Resolver: strip prefix to shared integer.
  ```python
  def yelp_business_key(s: str) -> int:
      # "businessid_123" or "businessref_123" → 123
      return int(s.split('_', 1)[1])
  ```
- Source: yelp db_description_withhint.txt.

### yelp — user identity  [clean]
- In `user_database.user`: `user_id` (str)
- In `user_database.review`, `user_database.tip`: `user_id` (str or null)
- Resolver: direct equality; handle null `user_id` in review/tip.

---

## Cross-dataset patterns to remember

- **Prefixed keys** (yelp) — strip prefix, compare integer part.
- **Aliased keys** (bookreview `book_id`↔`purchase_id`; github_repos `repo_name`↔`sample_repo_name`) — rename in the JOIN.
- **Corrupted keys** (crmarenapro `#` prefix + trailing whitespace) — clean both sides before every join.
- **Embedded keys** (pancancer_atlas barcode-in-free-text; deps_dev_v1 project-name-in-free-text; patents CPC-in-JSON) — extract first.
- **Composite keys** (deps_dev_v1 `(System,Name,Version)`) — join on all columns.
- **Knowledge-match** (stockindex exchange↔index) — no syntactic resolver; needs an LLM / lookup.
- **Table-name-as-key** (stockmarket ticker → DuckDB table) — dynamic SQL; enumerate tables.

## Injection test

**Question 1:**
How does Oracle Forge reconcile Yelp business identity across MongoDB and DuckDB in the current verified path?
Expected answer:
It maps MongoDB `business_id` values from the `businessid_*` format to DuckDB `business_ref` values in the `businessref_*` format before aggregation.
Status: pass
Last verified: 2026-04-11

**Question 2:** "In the crmarenapro dataset, what two kinds of corruption affect ID/text fields, and what is the mandatory pre-join normalization?"

### Expected Answer: 
~25% have leading `#`, ~20% have trailing whitespace; before every join, apply `strip('#').strip()` (or SQL `RTRIM(LTRIM(col, '#'))`) on both sides. Applies to `Id`, `AccountId`, `ContactId`, `Name`, `FirstName`, `LastName`, `Email`, `Subject`, `Status`.

### LLM Answer:
Corruption 1: ~25% of ID-like fields have a leading #.
Corruption 2: ~20% of text fields have trailing whitespace.
Mandatory pre-join normalization: strip the leading # and trim whitespace on both sides of every join key before joining.
**Result:** PASS
**Date:** 2026-04-14