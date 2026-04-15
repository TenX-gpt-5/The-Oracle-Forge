# DataAgentBench Schema Guidelines & Definitions

**Injection Test Evidence:**
*   **Test Query:** "What must you do before combining MongoDB aggregation pipeline results with PostgreSQL query results?"
*   **Expected Answer:** "You must require explicit field projection (`$project`) before combining."

Here are known query patterns and schema pitfalls working across DAB database types.

**Query Patterns That Work:**
- *MongoDB Aggregation Pipelines:* Always require explicit field projection (`$project`) before combining with PostgreSQL results. If left un-projected, the returned document payload volume will crash the context builder.
- *PostgreSQL JSONB:* Always explicitly cast JSONB fields before using them in `WHERE` clauses. Use `->>'key'` for text extraction, `->` for nested object access.
- *Cross-DB Aggregation:* Execute each database query independently, collect results into Python, then merge and aggregate. Never attempt cross-driver joins in SQL.
- *DuckDB Analytical SQL:* Supports window functions and CTEs natively. Use for heavy aggregation workloads when the data is available in DuckDB format.
- *SQLite:* Limited type system — all join keys should be cast to TEXT for comparison to avoid silent type coercion.

**Schema Navigation Rules:**
- Always run `inspect_schema` on both databases before writing a cross-DB query.
- Always check for `NULL` rates in key columns before joining — high NULLs signal data quality issues that will silently reduce result counts.
- Always check column types match expectations — a `customer_id` column may be INTEGER in one table and TEXT in another within the same database.

**See also:** `domain_terms.md` for business term definitions, `unstructured_fields.md` for free-text field inventory, `join_keys.md` for cross-database key format mappings.
**Expected:** "Apply explicit field projection ($project) in the MongoDB pipeline first. If left un-projected, the document payload will crash the context builder."
**Result:** PASS
**Date:** 2026-04-11

---
name: DAB dataset schemas
description: Per-dataset schema notes for all 12 DataAgentBench datasets — every table, every column, every cross-DB reference.
type: domain
status: populated from DAB db_description.txt files (all 12 datasets). Refine as Drivers hit specific queries.
source: DataAgentBench-main/query_*/db_description.txt and db_description_withhint.txt; DAB README.md
---

# DAB dataset schemas definitions

**12 datasets, 4 DBMSes (PostgreSQL, MongoDB, SQLite, DuckDB), 9 domains, 54 queries.** For each dataset the agent must know: which DBMSes are involved, table/collection names, every field name + type, and which fields cross-reference each other.

## Dataset index (from DAB README)

| Dataset | #DBs | DBMSes | #Tables | #Queries |
|---|---|---|---|---|
| agnews | 2 | MongoDB, SQLite | 3 | 4 |
| bookreview | 2 | PostgreSQL, SQLite | 2 | 3 |
| crmarenapro | 6 | DuckDB, PostgreSQL, SQLite | 27 | 13 |
| deps_dev_v1 | 2 | DuckDB, SQLite | 3 | 2 |
| github_repos | 2 | DuckDB, SQLite | 6 | 4 |
| googlelocal | 2 | PostgreSQL, SQLite | 2 | 4 |
| music_brainz_20k | 2 | DuckDB, SQLite | 2 | 3 |
| pancancer_atlas | 2 | DuckDB, PostgreSQL | 3 | 3 |
| patents | 2 | PostgreSQL, SQLite | 2 | 3 |
| stockindex | 2 | DuckDB, SQLite | 2 | 3 |
| stockmarket | 2 | DuckDB, SQLite | 2754 | 5 |
| yelp | 2 | DuckDB, MongoDB | 5 | 7 |

---

## 1. agnews

**Domain:** news articles. **DBMSes:** MongoDB + SQLite.

### `articles_database` (MongoDB)
- **`articles`** collection — news article content.
  - `_id` — MongoDB object id
  - `article_id` (int) — Unique identifier for the article
  - `title` (str) — Title of the news article
  - `description` (str) — Description of the news article

### `metadata_database` (SQLite)
- **`authors`** table — author records.
  - `author_id` (int) — Unique identifier for the author
  - `name` (str) — Full name of the author
- **`article_metadata`** table — links article ↔ author + publication info.
  - `article_id` (int) — Article identifier linking to `articles` collection
  - `author_id` (int) — Author identifier linking to `authors` table
  - `region` (str) — Geographic region where the article was published
  - `publication_date` (str) — Publication date in format `YYYY-MM-DD`

### Cross-DB joins
- `articles.article_id` ↔ `article_metadata.article_id`
- `article_metadata.author_id` ↔ `authors.author_id`

### Domain hints (from `db_description_withhint.txt`)
- Category is derivable **only from title+description** (LLM classification). Four categories: **World, Sports, Business, Science/Technology**.

---

## 2. bookreview

**Domain:** Amazon book reviews (up to 2023). **DBMSes:** PostgreSQL + SQLite.

### `books_database` (PostgreSQL)
- **`books_info`** table — Amazon book metadata.
  - `title` (str) — Book title
  - `subtitle` (str) — Book subtitle
  - `author` (str) — Book author(s)
  - `rating_number` (int) — Total number of ratings received
  - `features` (str) — Book features, stored as **stringified list/dict**
  - `description` (str) — Book description, stored as **stringified list/dict**
  - `price` (float) — Book price
  - `store` (str) — Store information
  - `categories` (str) — Book categories, stored as **stringified list/dict**
  - `details` (str) — Additional book details (stringified dict)
  - `book_id` (str) — Unique book identifier

### `review_database` (SQLite)
- **`review`** table — Amazon review content.
  - `rating` (float) — Rating given by reviewer (1.0–5.0)
  - `title` (str) — Review title
  - `text` (str) — Review text content (free text)
  - `purchase_id` (str) — Foreign key to PostgreSQL; `review.purchase_id` ↔ `books_info.book_id` (same value, different column name)
  - `review_time` (str) — Timestamp when review was posted
  - `helpful_vote` (int) — Number of helpful votes received
  - `verified_purchase` (bool) — Whether purchase was verified

### Cross-DB joins
- `books_info.book_id` ↔ `review.purchase_id` (same value, different column name — "fuzzy" naming mismatch).

### Domain hints
- `description`, `categories`, `features` look like list/dict but are **stored as strings** — parse before use.
- Some queries require extracting facts from `categories` or `details`.

---

## 3. crmarenapro

**Domain:** Salesforce-like CRM (CRMArena). **DBMSes:** SQLite + DuckDB + PostgreSQL across **6 databases**, **27 tables**.

### Db 3.1: `core_crm` (SQLite) — users, accounts, contacts
- **`User`** (sales team)
  - `Id`, `FirstName`, `LastName`, `Email`, `Phone`, `Username`, `Alias`, `LanguageLocaleKey`, `EmailEncodingKey`, `TimeZoneSidKey`, `LocaleSidKey`
- **`Account`** (company/customer)
  - `Id`, `Name`, `Phone`, `Industry`, `Description`, `NumberOfEmployees`, `ShippingState`
- **`Contact`** (individual contacts)
  - `Id`, `FirstName`, `LastName`, `Email`, `AccountId` (→ `Account.Id`)

### Db 3.2: `sales_pipeline` (DuckDB) — opportunities, quotes, contracts, leads
- **`Contract`** (signed contracts)
  - `Id`, `AccountId` (→ `Account.Id`), `Status`, `StartDate`, `CustomerSignedDate`, `CompanySignedDate`, `Description`, `ContractTerm`
- **`Lead`** (sales leads)
  - `Id`, `FirstName`, `LastName`, `Email`, `Phone`, `Company`, `Status`, `ConvertedContactId` (→ `Contact.Id`), `ConvertedAccountId` (→ `Account.Id`), `Title`, `CreatedDate`, `ConvertedDate`, `IsConverted`, `OwnerId` (→ `User.Id`)
- **`Opportunity`** (sales deals)
  - `Id`, `ContractID__c` (→ `Contract.Id`), `AccountId` (→ `Account.Id`), `ContactId` (→ `Contact.Id`), `OwnerId` (→ `User.Id`), `Probability`, `Amount`, `StageName`, `Name`, `Description`, `CreatedDate`, `CloseDate`
- **`OpportunityLineItem`** (deal line items)
  - `Id`, `OpportunityId` (→ `Opportunity.Id`), `Product2Id` (→ `Product2.Id`), `PricebookEntryId` (→ `PricebookEntry.Id`), `Quantity`, `TotalPrice`
- **`Quote`** (price quotes)
  - `Id`, `OpportunityId` (→ `Opportunity.Id`), `AccountId` (→ `Account.Id`), `ContactId` (→ `Contact.Id`), `Name`, `Description`, `Status`, `CreatedDate`, `ExpirationDate`
- **`QuoteLineItem`** (quote details)
  - `Id`, `QuoteId` (→ `Quote.Id`), `OpportunityLineItemId` (→ `OpportunityLineItem.Id`), `Product2Id` (→ `Product2.Id`), `PricebookEntryId` (→ `PricebookEntry.Id`), `Quantity`, `UnitPrice`, `Discount`, `TotalPrice`

### Db 3.3: `support` (PostgreSQL) — cases, knowledge, comms
**NOTE — two naming conventions differ from other databases:**
1. Column names are **lowercase** (Postgres convention) vs mixed-case elsewhere.
2. Custom Salesforce fields end in **`__c`** (e.g., `orderitemid__c`, `faq_answer__c`, `description__c`).
- **`Case`** (support cases)
  - `id`, `priority`, `subject` (free text), `description` (free text), `status`, `contactid` (→ `Contact.Id`), `createddate`, `closeddate`, `orderitemid__c` (→ `OrderItem.Id`), `issueid__c` (→ `issue__c.id`), `accountid` (→ `Account.Id`), `ownerid` (→ `User.Id`)
- **`knowledge__kav`** (knowledge articles)
  - `id`, `title`, `faq_answer__c` (free text), `summary` (free text), `urlname`
- **`issue__c`** (custom issues)
  - `id`, `name`, `description__c` (free text)
- **`casehistory__c`** (case history)
  - `id`, `caseid__c` (→ `Case.id`), `oldvalue__c`, `newvalue__c`, `createddate`, `field__c`
- **`emailmessage`** (email communications)
  - `id`, `subject`, `textbody` (free text), `parentid`, `fromaddress`, `toids`, `messagedate`, `relatedtoid`
- **`livechattranscript`** (chat logs)
  - `id`, `caseid` (→ `Case.id`), `accountid` (→ `Account.Id`), `ownerid` (→ `User.Id`), `body` (free text), `endtime`, `livechatvisitorid`, `contactid` (→ `Contact.Id`)

### Db 3.4: `products_orders` (SQLite) — catalog + orders
- **`ProductCategory`**
  - `Id`, `Name`, `CatalogId`
- **`Product2`** (product catalog)
  - `Id`, `Name`, `Description`, `IsActive`, `External_ID__c`
- **`ProductCategoryProduct`** (category↔product mapping)
  - `Id`, `ProductCategoryId` (→ `ProductCategory.Id`), `ProductId` (→ `Product2.Id`)
- **`Pricebook2`** (pricebooks)
  - `Id`, `Name`, `Description`, `IsActive`, `ValidFrom`, `ValidTo`
- **`PricebookEntry`** (price per product per book)
  - `Id`, `Pricebook2Id` (→ `Pricebook2.Id`), `Product2Id` (→ `Product2.Id`), `UnitPrice`
- **`Order`** (customer orders)
  - `Id`, `AccountId` (→ `Account.Id`), `Status`, `EffectiveDate`, `Pricebook2Id` (→ `Pricebook2.Id`), `OwnerId` (→ `User.Id`)
- **`OrderItem`** (order line items)
  - `Id`, `OrderId` (→ `Order.Id`), `Product2Id` (→ `Product2.Id`), `Quantity`, `UnitPrice`, `PriceBookEntryId` (→ `PricebookEntry.Id`)

### Db 3.5: `activities` (DuckDB) — tasks, events, call transcripts
**`WhatId`** is a **polymorphic foreign key** that appears on both **`Event`** and **`Task`**. It can reference any record type (Account, Opportunity, etc.) — there is no prefix-based resolution in this dataset; treat it as an opaque FK.
- **`Event`** (calendar events)
  - `Id`, `WhatId` (polymorphic FK → any record), `OwnerId` (→ `User.Id`), `StartDateTime`, `Subject`, `Description`, `DurationInMinutes`, `Location`, `IsAllDayEvent`
- **`Task`** (activities and tasks)
  - `Id`, `WhatId` (polymorphic FK → any record), `OwnerId` (→ `User.Id`), `Priority`, `Status`, `ActivityDate`, `Subject`, `Description`
- **`VoiceCallTranscript__c`** (call records)
  - `Id`, `OpportunityId__c` (→ `Opportunity.Id`), `LeadId__c` (→ `Lead.Id`), `Body__c` (free text transcript), `CreatedDate`, `EndTime__c`

### Db 3.6: `territory` (SQLite) — territory management
- **`Territory2`**
  - `Id`, `Name`, `Description`
- **`UserTerritory2Association`**
  - `Id`, `UserId` (→ `User.Id`), `Territory2Id` (→ `Territory2.Id`)

### Domain hints (corruption!)
- ~25% of ID-like fields may include a **leading `#`** (e.g., `#001Wt00000PFj4zIAD`) — strip before join.
- ~20% of text fields may contain **trailing whitespace** (e.g., `"Company Name "`) — rstrip before compare.
- Corruption may appear in: `Id`, `AccountId`, `ContactId`, `Name`, `FirstName`, `LastName`, `Email`, `Subject`, `Status`.
- Salesforce custom-field suffix `__c` appears in `support` and `activities` — treat as regular columns.

---

## 4. deps_dev_v1

**Domain:** software-package dependency metadata (Google deps.dev). **DBMSes:** SQLite + DuckDB.

### `package_database` (SQLite)
- **`packageinfo`** — package metadata.
  - `System` (str) — Package ecosystem (NPM, Maven, ...)
  - `Name` (str) — Package name
  - `Version` (str) — Version string
  - `Licenses` (str) — JSON-like array of licenses
  - `Links` (str) — JSON-like list of relevant links
  - `Advisories` (str) — JSON-like list of security advisories
  - `VersionInfo` (str) — JSON-like object with release metadata (`IsRelease`, `Ordinal`)
  - `Hashes` (str) — JSON-like list of file hashes
  - `DependenciesProcessed` (bool) — whether dependencies processed successfully
  - `DependencyError` (bool) — whether a dep processing error occurred
  - `UpstreamPublishedAt` (float) — Unix ms timestamp of upstream release
  - `Registries` (str) — JSON-like list of registries
  - `SLSAProvenance` (float) — SLSA provenance level if available
  - `UpstreamIdentifiers` (str) — JSON-like list of upstream IDs
  - `Purl` (float) — Package URL in purl format (typed as float, likely stored loosely)

### `project_database` (DuckDB)
- **`project_packageversion`** — project ↔ package version map.
  - `System` (str), `Name` (str), `Version` (str) — join with `packageinfo`
  - `ProjectType` (str) — e.g., `GITHUB`
  - `ProjectName` (str) — repo path in `owner/repo` format
  - `RelationProvenance` (str)
  - `RelationType` (str) — e.g., source repository type
- **`project_info`** — GitHub project info.
  - `Project_Information` (str) — Free text description **plus star/fork counts** (per hint)
  - `Licenses` (str) — JSON-like array
  - `Description` (str) — project description (differs from `Project_Information`)
  - `Homepage` (str)
  - `OSSFuzz` (float) — OSSFuzz status indicator

### Cross-DB joins
- Composite: `packageinfo.(System, Name, Version)` ↔ `project_packageversion.(System, Name, Version)`.
- Then `project_packageversion.ProjectName` ↔ `project_info.Project_Information` (**by name search inside the free-text `Project_Information`**).

### Domain hints
- GitHub stars/forks are embedded in `project_info.Project_Information` free text — extract before aggregating.

---

## 5. github_repos

**Domain:** GitHub-repo artifacts + metadata. **DBMSes:** SQLite + DuckDB.

### `metadata_database` (SQLite)
- **`languages`**
  - `repo_name` (str) — `owner/repo`
  - `language_description` (str) — languages in **natural language** (byte counts implicit)
- **`licenses`**
  - `repo_name` (str)
  - `license` (str) — e.g., `apache-2.0`, `mit`
- **`repos`**
  - `repo_name` (str)
  - `watch_count` (int) — watchers on GitHub

### `artifacts_database` (DuckDB)
- **`contents`** — file contents.
  - `id` (str) — blob id
  - `content` (str) — file content (textual; large/binary may be truncated)
  - `sample_repo_name` (str) — `owner/repo`
  - `sample_ref` (str) — branch or commit SHA
  - `sample_path` (str) — file path inside repo
  - `sample_symlink_target` (str) — symlink target, if any
  - `repo_data_description` (str) — **natural language** metadata (size, binary, copies, mode)
- **`commits`** — commit history.
  - `commit` (str) — SHA
  - `tree` (str) — tree SHA
  - `parent` (str) — JSON-like list of parent SHAs (merges have >1)
  - `author` (str) — JSON-like object (name, email, timestamp)
  - `committer` (str) — JSON-like object
  - `subject` (str) — short subject line
  - `message` (str) — full commit message
  - `trailer` (str) — JSON-like trailer metadata
  - `difference` (str) — JSON-like file changes
  - `difference_truncated` (bool)
  - `repo_name` (str)
  - `encoding` (str)
- **`files`** — file metadata (pointer to `contents.id`).
  - `repo_name` (str)
  - `ref` (str) — branch or commit SHA
  - `path` (str)
  - `mode` (int) — file mode (normal / executable / symlink)
  - `id` (str) — blob id (→ `contents.id`)
  - `symlink_target` (str)

### Cross-DB joins
- `repo_name` is the universal join key across all six tables (with alias `sample_repo_name` in `contents`).
- `files.id` ↔ `contents.id` (blob lookup).

### Domain hints
- Primary language = language with most bytes in `language_description` — **must parse the NL string**.
- `repo_data_description` drives file-attribute filters — parse NL for `size`, `binary`, `copies`, `mode`.

---

## 6. googlelocal

**Domain:** Google-Maps businesses + reviews (US, up to 2021-09). **DBMSes:** SQLite + PostgreSQL.

### `review_database` (SQLite)
- **`review`**
  - `name` (str) — reviewer name
  - `time` (str) — review timestamp
  - `rating` (int) — 1–5
  - `text` (str) — review text
  - `gmap_id` (str) — Google Maps business id

### `business_database` (PostgreSQL)
- **`business_description`**
  - `name` (str) — business name
  - `gmap_id` (str) — Google Maps business id (join key)
  - `description` (str) — **free text** (location info may live here)
  - `num_of_reviews` (int) — total reviews
  - `hours` (list) — operating hours
  - `MISC` (dict) — misc attributes (keys vary)
  - `state` (str) — operational status (`open`, `closed`, `temporarily closed`)

### Cross-DB joins
- `review.gmap_id` ↔ `business_description.gmap_id` (same name, same format).

### Domain hints
- `description` may contain location detail needed for geographic filters.

---

## 7. music_brainz_20k

**Domain:** music tracks + sales. **DBMSes:** SQLite + DuckDB.

### `tracks_database` (SQLite)
- **`tracks`** — track records with cross-source duplicates.
  - `track_id` (int) — **Unique in this DB** (per row id)
  - `source_id` (int) — source system id
  - `source_track_id` (str) — original id in the source — **NOT unique** across DB
  - `title` (str), `artist` (str), `album` (str)
  - `year` (str), `length` (str), `language` (str)

### `sales_database` (DuckDB)
- **`sales`** — per-sale transactions.
  - `sale_id` (int) — unique sale id
  - `track_id` (int) — links to `tracks.track_id`
  - `country` (str) — USA, UK, Canada, Germany, France
  - `store` (str) — iTunes, Spotify, Apple Music, Amazon Music, Google Play
  - `units_sold` (int)
  - `revenue_usd` (double)

### Cross-DB joins
- `tracks.track_id` ↔ `sales.track_id` (integer, clean).

### Domain hints
- `tracks` has **duplicates across sources**; entity resolution on `(title, artist, album, year)` needed — tolerate format variants (year formats, minor spellings).

---

## 8. pancancer_atlas

**Domain:** cancer clinical + molecular data. **DBMSes:** PostgreSQL + SQLite.

### `clinical_database` (PostgreSQL)
- **`clinical_info`** — patient-level clinical metadata.
  - **Over 100 attributes** per patient (demographics, diagnosis, treatment outcomes, survival status, etc.) — not individually listed in DAB description.
  - Key referenced field: `Patient_description` (str) — free-text blob containing `uuid`, `barcode`, `gender`, `vital status`, etc.
  - Cancer-type acronym field (standard: `acronym`) — e.g., `LGG` (Brain lower grade glioma), `BRCA` (Breast Invasive Carcinoma).

### `molecular_database` (SQLite)
- **`Mutation_Data`**
  - `ParticipantBarcode` (str) — patient id
  - `Tumor_SampleBarcode` (str), `Tumor_AliquotBarcode` (str)
  - `Normal_SampleBarcode` (str), `Normal_AliquotBarcode` (str)
  - `Normal_SampleTypeLetterCode` (str)
  - `Hugo_Symbol` (str) — gene (TP53, CDH1, ...)
  - `HGVSp_Short` (str) — protein-level annotation
  - `Variant_Classification` (str) — `Missense_Mutation`, `Nonsense_Mutation`, ...
  - `HGVSc` (str) — coding DNA annotation
  - `CENTERS` (str) — sequencing center
  - `FILTER` (str) — filter status (`PASS` = reliable)
- **`RNASeq_Expression`**
  - `ParticipantBarcode` (str)
  - `SampleBarcode` (str), `AliquotBarcode` (str)
  - `SampleTypeLetterCode` (str), `SampleType` (str)
  - `Symbol` (str) — gene symbol
  - `Entrez` (str) — Entrez gene id
  - `normalized_count` (float) — normalized RNA expression

### Cross-DB joins
- `clinical_info.Patient_description` ↔ `Mutation_Data.ParticipantBarcode` (and `RNASeq_Expression.ParticipantBarcode`) — **barcode is embedded in free text** `Patient_description`, must extract.

### Domain hints
- Average log-expression = mean of `log10(normalized_count + 1)`.
- Chi-square: χ² = Σ (Oij − Eij)² / Eij, Eij = (row_total × col_total) / grand_total.

---

## 9. patents

**Domain:** patent publications + CPC classification. **DBMSes:** SQLite + PostgreSQL.

### `publication_database` (SQLite)
- **`publicationinfo`** — patent publication records.
  - `Patents_info` (str) — **NL summary** containing `application_number`, `publication_number`, `assignee_harmonized`, `country_code`
  - `kind_code` (str) — description of the publication kind
  - `application_kind` (str) — e.g., "utility patent application"
  - `pct_number` (str) — PCT number if applicable
  - `family_id` (str) — family grouping id
  - `title_localized` (str) — localized title
  - `abstract_localized` (str) — abstract (NL)
  - `claims_localized_html` (str) — claims as HTML
  - `description_localized_html` (str) — description as HTML
  - `publication_date` (str) — **NL date** (e.g., "March 15th, 2020")
  - `filing_date` (str) — NL date
  - `grant_date` (str) — NL date
  - `priority_date` (str) — NL date
  - `priority_claim` (str) — list of claimed priority applications
  - `inventor_harmonized` (str) — harmonized list of inventors
  - `examiner` (str)
  - `uspc` (str) — US Patent Classification code(s)
  - `ipc` (str) — International Patent Classification code(s)
  - `cpc` (str) — **JSON-like list** of CPC entries (code + metadata)
  - `citation` (str) — list of cited patents + non-patent lit
  - `parent` (str) — parent applications
  - `child` (str) — child applications
  - `entity_status` (str) — legal entity status ("small entity", "large entity")
  - `art_unit` (str) — USPTO art unit

### `CPCDefinition_database` (PostgreSQL)
- **`cpc_definition`** — CPC hierarchy + definitions.
  - `applicationReferences` (str)
  - `breakdownCode` (bool)
  - `childGroups` (str) — JSON-like list of child symbols
  - `children` (str) — additional JSON-like children
  - `dateRevised` (str) — NL date
  - `definition` (str)
  - `glossary` (str)
  - `informativeReferences` (str)
  - `ipcConcordant` (str) — IPC concordance mapping
  - `level` (int) — hierarchy level (1–5)
  - `limitingReferences` (str)
  - `notAllocatable` (bool) — whether assignable to a patent
  - `parents` (str) — JSON-like list of parent symbols
  - `precedenceLimitingReferences` (str)
  - `residualReferences` (str)
  - `rules` (str)
  - `scopeLimitingReferences` (str)
  - `status` (str) — "active" / "deleted"
  - `symbol` (str) — CPC code (join key)
  - `synonyms` (str)
  - `titleFull` (str) — full descriptive title
  - `titlePart` (str) — abbreviated title

### Cross-DB joins
- `publicationinfo.cpc` (JSON-like) ↔ `cpc_definition.symbol` — parse `cpc` JSON, match each code against `symbol`.

### Domain hints
- All dates are **natural-language** — parse before sorting/filtering.
- Citations are in `publicationinfo.citation` as a list.
- `Patents_info` free text is the source of identifier/assignee/country info.

---

## 10. stockindex

**Domain:** stock-market indices worldwide. **DBMSes:** SQLite + DuckDB.

### `indexinfo_database` (SQLite)
- **`index_info`**
  - `Exchange` (str) — full exchange name (e.g., "Tokyo Stock Exchange")
  - `Currency` (str) — trading currency

### `indextrade_database` (DuckDB)
- **`index_trade`**
  - `Index` (str) — abbreviated **index symbol** (e.g., `N225`, `HSI`, `000001.SS`)
  - `Date` (str) — trading date
  - `Open`, `High`, `Low`, `Close`, `Adj Close` (float)
  - `CloseUSD` (float) — closing price in USD

### Cross-DB joins
- `index_info.Exchange` (full name) ↔ `index_trade.Index` (abbreviation) — **no deterministic mapping**; requires knowledge-based matching (e.g., "Tokyo Stock Exchange" ↔ `N225`, "Hong Kong Stock Exchange" ↔ `HSI`).

### Domain hints
- Region (Asia/Europe/North America) must be **inferred** from geographic knowledge.
- "Up day" = `Close > Open`; "down day" = `Close < Open`.
- Intraday volatility = `(High − Low) / Open`; average across period.

---

## 11. stockmarket

**Domain:** US individual stocks + ETFs. **DBMSes:** SQLite + DuckDB. **Extreme schema:** 2,754 tables.

### `stockinfo_database` (SQLite)
- **`stockinfo`** — ticker metadata.
  - `Nasdaq Traded` (str) — whether traded on NASDAQ
  - `Symbol` (str) — ticker symbol (links to table name in `stocktrade_database`)
  - `Listing Exchange` (str) — coded (`A`=NYSE MKT, `N`=NYSE, `P`=NYSE ARCA, `Z`=BATS, `V`=IEXG, `Q`=NASDAQ Global Select)
  - `Market Category` (str) — coded (`Q`=Global Select, `G`=Global Market, `S`=Capital Market)
  - `ETF` (str) — whether the security is an ETF
  - `Round Lot Size` (float)
  - `Test Issue` (str)
  - `Financial Status` (str or null) — coded (`D`=Deficient, `E`=Delinquent, `Q`=Bankrupt, `N`=Normal, `G`=Def+Bnkrpt, `H`=Def+Del, `J`=Del+Bnkrpt, `K`=Def+Del+Bnkrpt)
  - `NextShares` (str) — NextShares designation
  - `Company Description` (str) — **free text** (company name + description)

### `stocktrade_database` (DuckDB)
- **2,753 tables**, one per ticker. Each table is named after its ticker (e.g., `AAPL`). Each has identical fields:
  - `Date` (str) — trading date
  - `Open`, `High`, `Low`, `Close`, `Adj Close` (float)
  - `Volume` (int)

### Cross-DB joins
- `stockinfo.Symbol` ↔ **DuckDB table name** in `stocktrade_database`. **Table-name-as-key** pattern — dynamic SQL required.

### Domain hints
- Financially troubled = `Financial Status` ∈ {`D`, `E`, `G`, `H`, `J`, `K`} (deficient OR delinquent).
- See `join_keys.md` for the table-name-as-key resolver.

---

## 12. yelp

**Domain:** Yelp businesses + user activity. **DBMSes:** MongoDB + DuckDB.

### `businessinfo_database` (MongoDB)
- **`business`** collection
  - `_id` — MongoDB object id
  - `business_id` (str) — unique id (format `businessid_<N>`)
  - `name` (str)
  - `review_count` (int) — total reviews
  - `is_open` (int) — 1=operational, 0=permanently closed (**not** hours-open)
  - `attributes` (dict or null) — parking, WiFi, service flags
  - `hours` (dict or null) — operating hours per day
  - `description` (str) — **free text** including location (city/state)
- **`checkin`** collection
  - `_id`
  - `business_id` (str) — links to `business.business_id`
  - `date` (list of str) — check-in timestamps

### `user_database` (DuckDB)
- **`review`** — user reviews.
  - `review_id` (str), `user_id` (str or null), `business_ref` (str — format `businessref_<N>`)
  - `rating` (int, 1–5), `useful`, `funny`, `cool` (int votes)
  - `text` (str) — review body (free text)
  - `date` (str)
- **`tip`** — short tips.
  - `user_id` (str or null), `business_ref` (str)
  - `text` (str), `date` (str), `compliment_count` (int)
- **`user`** — user profiles.
  - `user_id` (str)
  - `name` (str)
  - `review_count` (int) — total reviews
  - `yelping_since` (str) — registration date
  - `useful`, `funny`, `cool` (int votes received lifetime)
  - `elite` (str) — **comma-separated years of elite status** (e.g., `"2016,2017,2019"`), empty if never elite

### Cross-DB joins
- `business.business_id` (MongoDB, `businessid_<N>`) ↔ `review.business_ref` / `tip.business_ref` (DuckDB, `businessref_<N>`) — **prefix differs, integer suffix matches**.
- `review.user_id` / `tip.user_id` ↔ `user.user_id` (within DuckDB, direct).

### Domain hints
- Location info is inside `business.description` free text — regex or LLM extraction needed.
- `attributes` is already structured (Mongo dict) but may be null.
- `is_open` is operational status, not hours-open.
- `user.elite` is a string, not a bool — parse the comma-separated year list for tenure.

---

## Population guidance

When updating any dataset section after a driver hits a query:
1. Confirm every listed field still exists (columns may have been renamed in the physical DB).
2. Add any field we missed — compare with schema introspection.
3. Flag **every** stringified JSON / list / dict field in `unstructured_fields.md`.
4. Flag **every** cross-DB reference in `join_keys.md`.
5. Flag **every** coded-value or NL-ambiguous field in `domain_terms.md`.
6. Run the injection test before committing.

---

## Injection tests

### Cross-DB join keys

| # | Question | Expected answer | LLM Answer | Result |
|---|----------|-----------------|------------|--------|
| 1 | What column in the SQLite `review` table joins to `books_info` in PostgreSQL for the **bookreview** dataset? | `review.purchase_id` ↔ `books_info.book_id` | `review.purchase_id` ↔ `books_info.book_id` | PASS — field description and cross-DB join section both state the bidirectional mapping |
| 2 | In **yelp**, the MongoDB `business_id` format is `businessid_<N>`. What is the corresponding column and format in DuckDB? | `review.business_ref` / `tip.business_ref`, format `businessref_<N>` (prefix differs, integer suffix matches) | `review.business_ref` / `tip.business_ref` with format `businessref_<N>` | PASS |
| 3 | How do you join `clinical_info` to `Mutation_Data` in **pancancer_atlas**? | Extract `ParticipantBarcode` from the free-text `Patient_description` column in `clinical_info`, then match to `Mutation_Data.ParticipantBarcode` | Extract `ParticipantBarcode` from `clinical_info.Patient_description` and join to `Mutation_Data.ParticipantBarcode` | PASS |
| 4 | What is the composite join key between the two databases in **deps_dev_v1**? | `(System, Name, Version)` across `packageinfo` (SQLite) and `project_packageversion` (DuckDB) | `(System, Name, Version)` | PASS |
| 5 | In **stockmarket**, how does `stockinfo` join to trade data in DuckDB? | `stockinfo.Symbol` maps to the **DuckDB table name** (one table per ticker) — table-name-as-key pattern | `stockinfo.Symbol` matches the DuckDB table name in `stocktrade_database` | PASS |
| 6 | What makes the **stockindex** cross-DB join non-trivial? | `index_info.Exchange` is a full name (e.g., "Tokyo Stock Exchange") while `index_trade.Index` is an abbreviation (e.g., `N225`) — requires knowledge-based matching, no deterministic mapping | `index_info.Exchange` is a full exchange name while `index_trade.Index` is an abbreviation; the mapping requires knowledge-based matching | PASS |
| 7 | In **agnews**, which two joins connect articles to authors? | `articles.article_id` ↔ `article_metadata.article_id`, then `article_metadata.author_id` ↔ `authors.author_id` | `articles.article_id` → `article_metadata.article_id`, then `article_metadata.author_id` → `authors.author_id` | PASS |

### DBMS identification

| # | Question | Expected answer | LLM Answer | Result |
|---|----------|-----------------|------------|--------|
| 8 | Which DBMS hosts `sales` for **music_brainz_20k**? | DuckDB (`sales_database`) | DuckDB | PASS |
| 9 | Which DBMS hosts the `business_description` table in **googlelocal**? | PostgreSQL (`business_database`) | PostgreSQL | PASS |
| 10 | Which DBMSes does **crmarenapro** span? | DuckDB, PostgreSQL, and SQLite (across 6 databases) | DuckDB, PostgreSQL, SQLite | PASS |
| 11 | In **yelp**, which DBMS stores the `business` collection? | MongoDB (`businessinfo_database`) | MongoDB | PASS |

### Data quirks and domain hints

| # | Question | Expected answer | LLM Answer | Result |
|---|----------|-----------------|------------|--------|
| 12 | What data corruption must you handle before joining IDs in **crmarenapro**? | ~25% of ID fields may have a leading `#` (e.g., `#001Wt00000PFj4zIAD`) — strip before join; ~20% of text fields may have trailing whitespace — rstrip before compare | Leading `#` on ID-like fields and trailing whitespace on text fields | PASS — captures both key issues, omits percentages but the core guidance is correct |
| 13 | How are dates stored in the **patents** dataset? | As natural-language strings (e.g., "March 15th, 2020") — must parse before sorting/filtering | As natural-language date strings, e.g. `"March 15th, 2020"` | PASS |
| 14 | In **bookreview**, the `description`, `categories`, and `features` columns look like structured data. What's the catch? | They are **stored as strings** (stringified list/dict) — must parse before use | They are stored as strings, not as actual structured list/dict data | PASS |
| 15 | How do you determine an article's category in **agnews**? | Category is derivable **only from title + description** via LLM classification. Four categories: World, Sports, Business, Science/Technology | From `title` + `description` via LLM classification | PASS — omits the four category names but correctly identifies the method |
| 16 | What does `is_open` mean in the **yelp** `business` collection? | Operational status (1 = operational, 0 = permanently closed) — **not** hours-open | Operational status: `1` = operational, `0` = permanently closed; not hours-open | PASS |
| 17 | How is the `elite` field stored in **yelp** `user` table? | As a comma-separated string of years (e.g., `"2016,2017,2019"`), empty string if never elite — not a boolean | As a comma-separated years string, empty if never elite | PASS |
| 18 | What is the formula for average log-expression in **pancancer_atlas**? | `mean(log10(normalized_count + 1))` | Mean of `log10(normalized_count + 1)` | PASS |
| 19 | What defines a "financially troubled" stock in **stockmarket**? | `Financial Status` ∈ {`D`, `E`, `G`, `H`, `J`, `K`} (deficient OR delinquent, including combinations with bankrupt) | `Financial Status` ∈ `{D, E, G, H, J, K}` | PASS |
| 20 | How many tables exist in the **stockmarket** `stocktrade_database`? | 2,753 tables (one per ticker symbol) | `2,753` tables | PASS |

### Field location

| # | Question | Expected answer | LLM Answer | Result |
|---|----------|-----------------|------------|--------|
| 21 | Where do you find GitHub star/fork counts in **deps_dev_v1**? | Embedded in the free-text `project_info.Project_Information` column — must extract before aggregating | In `project_info.Project_Information` free text | PASS |
| 22 | Where is location information for a **yelp** business? | Inside the free-text `business.description` field — requires regex or LLM extraction | In `business.description` free text | PASS |
| 23 | Where is location information for a **googlelocal** business? | In the free-text `business_description.description` field | In `business_description.description` free text | PASS |
| 24 | In **github_repos**, how do you determine a repo's primary language? | Parse the natural-language `languages.language_description` string — primary language = language with most bytes | Parse `language_description` NL text and take the language with the most bytes | PASS |
| 25 | In **patents**, where are `application_number`, `assignee_harmonized`, and `country_code` stored? | Inside the free-text `publicationinfo.Patents_info` column | In `publicationinfo.Patents_info` free-text summary | PASS |

### Schema structure

| # | Question | Expected answer | LLM Answer | Result |
|---|----------|-----------------|------------|--------|
| 26 | What is the universal join key across all six tables in **github_repos**? | `repo_name` (aliased as `sample_repo_name` in the `contents` table) | `repo_name` (alias `sample_repo_name` in `contents`) | PASS |
| 27 | How many databases and tables does **crmarenapro** have? | 6 databases, 27 tables | `6` databases and `27` tables | PASS |
| 28 | In **crmarenapro** `support` (PostgreSQL), what naming convention differs from other databases? | Column names are **lowercase** (Postgres convention) vs mixed-case elsewhere; custom Salesforce fields end in `__c` | Column names are lowercase (Postgres convention) vs mixed-case elsewhere; custom Salesforce fields end in `__c` | PASS — numbered NOTE now separates the two conventions clearly |
| 29 | What is `WhatId` in the **crmarenapro** `activities` database? | A **polymorphic** foreign key on `Event` and `Task` that can reference any record type | A polymorphic foreign key on `Event` and `Task` that can reference any record type; no prefix-based resolution in this dataset | PASS — dedicated `WhatId` callout block now names both tables and clarifies no prefix resolution |
| 30 | In **music_brainz_20k**, is `source_track_id` a reliable unique key? | No — `source_track_id` is **not unique** across the DB (duplicates across sources); `track_id` is the unique per-row key | No — `source_track_id` is not unique | PASS — correctly identifies non-uniqueness; omits the alternative (`track_id`) but answers the question asked |

**Result:** PASS (30/30 — 3 formerly partial answers now correct after schema clarifications)
**Date:** 2026-04-15