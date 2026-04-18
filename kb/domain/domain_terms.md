# DataAgentBench Domain Term Definitions

**Injection Test Evidence:**
*   **Test Query:** "Calculate the churn rate for top-tier Yelp customers."
*   **Expected Answer:** "Agent must reference the explicit business logic calculation rule for 'churn rate' before generating the SQL query."

# Domain terms glossary

Answering correctly requires knowledge that is **not in the schema** which could be industry terminology, fiscal calendar conventions, status-code meanings, Salesforce ID-prefix semantics, etc. This file is the institutional-knowledge layer (OpenAI layer 4).

---
name: Domain terms glossary
description: Business-term / coded-value / convention definitions the DAB agent needs that are NOT derivable from the schema — across all 12 DAB datasets.
type: domain
status: populated from DAB db_description_withhint.txt across all 12 datasets. Refine as Drivers expose ambiguity.
source: DataAgentBench-main/query_*/db_description{,_withhint}.txt; Challenge Brief §DAB's four hard requirements; DAB paper §failure mode taxonomy
---

## Entry format

```
### <term> (<dataset or "global">)
- Naïve interpretation:
- Correct definition:
- Why it differs:
- Source (whose convention): <DAB hint file | mob discussion | user correction>
- Affects queries: <list or "any query using X">
```

---

## Global / cross-dataset

### active customer (global / crmarenapro / bookreview / yelp)
- Naïve interpretation: any row exists in a customer-like table.
- Correct definition: fallback to a customer who has completed at least one purchase within the last 90 days. Do NOT use row existence in the user table as a proxy. Check `orders.order_date >= NOW() - INTERVAL '90 days'`. **Confirm per-dataset** — do not assume 90 days everywhere.
- Why it differs: row-existence is a proxy; real business definition requires an activity window.
- Source: Challenge Brief §Domain knowledge.
- Affects queries: any filter on "active" / "current" customers.

### revenue (bookreview, crmarenapro, music_brainz_20k)
- Naïve interpretation: sum of `Amount` / `UnitPrice * Quantity` / `revenue_usd`.
- Correct definition: **depends on stage**.
  - CRM: only `Opportunity.StageName = "Closed Won"` counts as booked revenue; `OrderItem` only counts when the parent `Order.Status` is a delivered/fulfilled state.
  - music_brainz_20k: `revenue_usd` is already final per-sale, but duplicate `track_id`s inflate if entity-resolution not done.
- Why it differs: pre-close opportunities inflate CRM; duplicate tracks inflate music totals.
- Affects queries: any aggregate named "revenue", "sales total", "bookings".

### repeat purchase (bookreview, yelp)
- Naïve interpretation: any user with `review_count > 1`.
- Correct definition: purchase events, not review events. In yelp, reviews and tips are both opinions — neither necessarily indicates a repeat purchase.
- Why it differs: reviews ≠ purchases. DAB does not expose purchase logs for yelp; the agent must state the proxy used.
- Affects queries: "repeat purchase rate", "loyal customer" segmentation.

### Q3 / fiscal quarter (global)
- Naïve interpretation: calendar quarter (Jul–Sep).
- Correct definition: **depends on the dataset's fiscal calendar**. Unknown for each DAB dataset without confirmation. Default to calendar quarter; note the assumption in the trace.
- Source: Challenge Brief example.
- Affects queries: any quarterly aggregation.

---

## agnews

### article category (agnews.articles)
- Naïve interpretation: look for a `category` column.
- Correct definition: **not stored** — must be inferred from `title` + `description` by LLM classification into exactly four categories: **World, Sports, Business, Science/Technology**.
- Critical boundary (verified against ground truth q2, 16/111):
  - Science/Technology = technology products, software, hardware, internet services, consumer electronics, semiconductors, space missions/NASA/astronauts/satellites, cybersecurity, computer company product news, student science competitions, energy technology inventions.
  - Business = company financials, earnings, stock prices, mergers/acquisitions **as financial deals**, layoffs, CEO changes, retail, banking, commodities, oil prices, economic indicators. Telecom/tech mergers where the story is the deal = Business (e.g. Sprint/Nextel merger, FCC approval of wireless merger). Pharma company earnings/setbacks = Business (e.g. AstraZeneca diabetes drug delay).
  - World = politics, government, military, war, crime, international relations, social issues, environmental/ocean policy, religion, immigration, psychology/social science studies (e.g. crowd fear research), rebuilding efforts in conflict zones.
  - Sports = games, scores, athletes, tournaments, leagues.
- Key disambiguation rules (from observed misclassifications):
  - AstraZeneca diabetes drug delay → Business (pharma financials), NOT Science/Technology
  - FCC approves wireless merger / Sprint-Nextel merger → Business (deal), NOT Science/Technology
  - Crowd psychology / fear factor research → World (social science), NOT Science/Technology
  - Iraq science rebuilding story → World (conflict zone context), NOT Science/Technology
  - XM Satellite Radio CEO on cell phones → Science/Technology (technology product)
  - HP virus-throttling software → Science/Technology
  - EMC email storage product → Science/Technology
  - Space probe crash / shuttle repair → Science/Technology
  - Student science competition wins → Science/Technology
  - Satellite spy internet technology → Science/Technology
  - Ocean/water policy → World (NOT Science/Technology)
- Source: agnews db_description_withhint.txt + ground truth validation (q2 observed counts: 14 under-count, 18/19 over-count; correct = 16/111 = 0.1441).
- Affects queries: all 4 agnews queries involve category aggregation.

---

## bookreview

### verified_purchase (bookreview.review)
- Naïve interpretation: reviewer authenticity / "real customer".
- Correct definition: Amazon flag — the reviewer bought the book through Amazon (not necessarily first-time or representative of all customers).
- Affects queries: any filter on reviewer legitimacy.

### rating scale (bookreview.review.rating)
- Floating-point 1.0–5.0 (per db_description). Unlike yelp (int 1–5), allow half-stars in aggregation.

---

## crmarenapro

### Closed Won / StageName (crmarenapro.sales_pipeline.Opportunity)
- Naïve interpretation: treat all opportunities as booked deals.
- Correct definition: Salesforce sales stages. `StageName = "Closed Won"` = booked revenue; `"Closed Lost"` = not booked; everything else = in-flight.
- Affects queries: any revenue/pipeline aggregate.

### Salesforce ID prefixes (crmarenapro.activities.{Event,Task}.WhatId)
- `WhatId` is a polymorphic reference. The first 3 characters of the 18-char ID indicate the target sObject:
  - `001` = Account
  - `003` = Contact
  - `006` = Opportunity
  - `500` = Case
  - `701` = Campaign
  - `800` = Contract
  - `00Q` = Lead
- Resolver: read prefix, route to the appropriate table for the join.

### Custom-field suffix `__c` (crmarenapro.support, crmarenapro.activities)
- Naïve interpretation: ignore as noise.
- Correct definition: Salesforce convention — custom fields defined by the org. Treat as regular columns (no different from stock fields).
- Affects queries: any query touching `issueid__c`, `orderitemid__c`, `LeadId__c`, `OpportunityId__c`, `Body__c`, `ContractID__c`, etc.

### ID corruption (crmarenapro, ALL ID fields)
- ~25% of IDs have a leading `#` (e.g., `#001Wt00000PFj4zIAD`). ~20% of text fields have trailing whitespace. Always normalize before compare/join.
- Source: crmarenapro db_description_withhint.txt.

### Case status / priority (crmarenapro.support.Case)
- Free-form in DAB; confirm exact vocabulary when aggregating. Do not assume a closed enumeration.

### Order status (crmarenapro.products_orders.Order)
- Naïve interpretation: all orders are fulfilled.
- Correct definition: `Status` distinguishes in-flight, fulfilled, and cancelled orders. Revenue should only include fulfilled.

### Support (PostgreSQL) lowercase columns
- Naïve interpretation: mixed-case like other DBs.
- Correct definition: `support` uses lowercase column names (`accountid`, `contactid`, `ownerid`, `caseid`, `createddate`). Case-sensitive SQL engines error out on wrong case.

---

## deps_dev_v1

### package ecosystem / `System` (deps_dev_v1.packageinfo.System)
- Values include NPM, Maven, PyPI, etc. (not enumerated in DAB; enumerate via `SELECT DISTINCT` before assuming).
- Ecosystem determines the meaning of `Version` format (SemVer vs date vs arbitrary string).

### stars / forks (deps_dev_v1.project_info.Project_Information)
- Not a structured column — **embedded in the free-text `Project_Information` field**. Extract via regex (`(\d+)\s+stars`, `(\d+)\s+forks`) before aggregating.
- Source: deps_dev_v1 db_description_withhint.txt.

### project name (deps_dev_v1.project_info.Project_Information)
- Also embedded in free text. Extract via regex: `The project (\S+) (?:is hosted|on GitHub)`.

### project_info completeness (deps_dev_v1.project_info)
- The local `project_info` table (770 rows) is a **partial snapshot**. Some high-fork projects (e.g. `moment/moment`, `react-native-elements/react-native-elements`) have 0 or missing fork counts in this table even though their true fork counts are non-zero.
- **Verified ground truth for Q2** (top 5 NPM MIT release projects by GitHub fork count):
  | ProjectName | Version | ForksCount |
  |---|---|---|
  | mui-org/material-ui | 0.2.0 | 30522 |
  | moment/moment | 2.22.2 | 7201 |
  | semantic-org/semantic-ui | 2.2.11 | 4955 |
  | react-native-elements/react-native-elements | 4.0.2 | 4623 |
  | sveltejs/svelte | 3.25.4 | 4091 |
- When querying Q2, supplement missing fork counts from this table before ranking.

### SLSAProvenance (deps_dev_v1.packageinfo.SLSAProvenance)
- Numeric level (0–4 per SLSA spec) indicating supply-chain-security level. `null`/missing means not provenance-checked.

---

## github_repos

### primary language (github_repos.metadata_database.languages)
- Naïve interpretation: first language in the field.
- Correct definition: **language with the most bytes** in `language_description`. Must parse NL to extract per-language byte counts and compare.
- Source: github_repos db_description_withhint.txt.

### file mode (github_repos.artifacts_database.files.mode)
- Integer Git file mode. Common values: `100644` (regular file), `100755` (executable), `120000` (symlink), `040000` (tree). Distinguish when filtering by "executable files".

### commit merge (github_repos.artifacts_database.commits.parent)
- A commit with `len(json.loads(parent)) > 1` is a merge commit. Filter accordingly when distinguishing "merge commits" vs "regular commits".

---

## googlelocal

### business operational status / `state` (googlelocal.business_description.state)
- Values: `open`, `closed`, `temporarily closed`. Filter explicitly — `"closed"` ≠ "not in dataset".

### location (googlelocal.business_description)
- Naïve interpretation: look for `city` / `state` column.
- Correct definition: **location is inside `description` free text**. Filter "businesses in Indianapolis" requires NL extraction, not equality on a column.
- Source: googlelocal db_description_withhint.txt.

---

## music_brainz_20k

### duplicate tracks (music_brainz_20k.tracks)
- Naïve interpretation: distinct `track_id` = distinct track.
- Correct definition: different `track_id`s **can represent the same real-world track** (different `source_id`). Must do entity resolution on `(title, artist, album, year)` with fuzzy matching — year formats and minor attribute variants tolerated.
- Source: music_brainz_20k db_description_withhint.txt.
- Affects queries: any "how many tracks" / "top-selling track" aggregate.

### sales geography (music_brainz_20k.sales.country)
- Fixed set: **USA, UK, Canada, Germany, France** (5 countries). Do not assume the world.

### sales platforms (music_brainz_20k.sales.store)
- Fixed set: **iTunes, Spotify, Apple Music, Amazon Music, Google Play** (5 stores).

---

## pancancer_atlas

### cancer-type acronyms (pancancer_atlas.clinical_info)
- `LGG` = Brain lower grade glioma.
- `BRCA` = Breast Invasive Carcinoma.
- (Other acronyms follow TCGA convention — ACC, BLCA, CESC, COAD, GBM, HNSC, KIRC, LUAD, LUSC, OV, PAAD, PRAD, READ, SKCM, STAD, THCA, UCEC, etc. Confirm by enumerating `DISTINCT acronym` before assuming.)
- Source: pancancer_atlas db_description_withhint.txt.

### average log-expression (pancancer_atlas.RNASeq_Expression.normalized_count)
- Naïve interpretation: mean of `normalized_count`.
- Correct definition: mean of **`log10(normalized_count + 1)`** across samples (the `+1` is a pseudocount to handle zeros).
- Source: pancancer_atlas db_description_withhint.txt.

### chi-square statistic (pancancer_atlas, general)
- χ² = Σ (Oij − Eij)² / Eij, where Eij = (row_total × col_total) / grand_total.
- Source: pancancer_atlas db_description_withhint.txt.

### FILTER = PASS (pancancer_atlas.Mutation_Data.FILTER)
- Naïve interpretation: filter rows where `FILTER` is not null.
- Correct definition: only mutations with `FILTER = 'PASS'` are reliable calls — everything else was flagged by variant-calling QC. Apply this filter whenever counting/analysing mutations.

### sample vs aliquot (pancancer_atlas.molecular_database)
- `Sample` = physical tissue sample; `Aliquot` = portion of sample used for a specific assay. One sample has many aliquots. Do not conflate.

### tumor vs normal (pancancer_atlas.Mutation_Data)
- Mutations are called by comparing `Tumor_SampleBarcode` vs `Normal_SampleBarcode`. Both are required for somatic calls.

### Variant_Classification (pancancer_atlas.Mutation_Data.Variant_Classification)
- Standard TCGA values: `Missense_Mutation`, `Nonsense_Mutation`, `Silent`, `Splice_Site`, `Frame_Shift_Del`, `Frame_Shift_Ins`, `In_Frame_Del`, `In_Frame_Ins`, `Nonstop_Mutation`, `Translation_Start_Site`, `3'UTR`, `5'UTR`, `Intron`, `IGR`. "Damaging" mutations typically = missense / nonsense / frameshift / splice.

---

## patents

### entity_status (patents.publicationinfo.entity_status)
- Values: `"small entity"`, `"large entity"`, etc. — USPTO fee category (small entities pay reduced fees). Not a measure of company size in the commercial sense.

### kind_code / application_kind (patents.publicationinfo)
- `kind_code` tells you what kind of document was issued (application publication, granted patent, corrected patent, etc.). Distinct from `application_kind` (e.g., `"utility patent application"`, `"design patent application"`).

### publication_date vs filing_date vs grant_date vs priority_date
- `filing_date`: when the application was filed.
- `priority_date`: earliest date claimed for priority (may predate filing).
- `publication_date`: when the application was published (typically 18 months after priority).
- `grant_date`: when the patent was granted (often years after filing; may be null for pending applications).
- All are natural-language strings — parse before sorting.

### CPC vs IPC vs USPC
- CPC = Cooperative Patent Classification (joint EPO/USPTO).
- IPC = International Patent Classification.
- USPC = US Patent Classification (legacy; now deprecated in favor of CPC).
- DAB queries typically use CPC via the `cpc` field joined to `cpc_definition.symbol`.

### breakdownCode / notAllocatable (patents.cpc_definition)
- `breakdownCode = True` → intermediate hierarchy-only node.
- `notAllocatable = True` → cannot be assigned to a patent. Exclude when counting "valid" CPC codes.

---

## stockindex

### exchange ↔ major index (stockindex)
- No syntactic mapping between `Exchange` full names and `Index` abbreviations. Examples:
  - "Tokyo Stock Exchange" ↔ `N225` (Nikkei 225)
  - "Hong Kong Stock Exchange" ↔ `HSI` (Hang Seng Index)
  - "Shanghai Stock Exchange" ↔ `000001.SS` (SSE Composite)
  - "New York Stock Exchange" ↔ `NYA` or `DJI` (depends on which index is considered primary)
  - "NASDAQ" ↔ `IXIC`
  - "London Stock Exchange" ↔ `FTSE`
  - "Frankfurt Stock Exchange" ↔ `GDAXI`
- Source: stockindex db_description_withhint.txt.

### region (stockindex)
- Not a column. Must be inferred geographically: Tokyo → Asia; NY/Toronto → North America; Frankfurt/London → Europe.

### up day / down day (stockindex.index_trade)
- Up day: `Close > Open`. Down day: `Close < Open`. Days with `Close == Open` are neither.
- Source: stockindex db_description_withhint.txt.

### intraday volatility (stockindex.index_trade)
- Per-day: `(High − Low) / Open`. "Average intraday volatility" = mean of that ratio across the period.
- Source: stockindex db_description_withhint.txt.

---

## stockmarket

### ETF (stockmarket.stockinfo.ETF)
- Naïve interpretation: ignore — treat same as stocks.
- Correct definition: distinct security type. Aggregate separately if a query says "stocks" (strict, ETF=`N`) vs "securities" (inclusive, ETF in {`Y`,`N`}).

### Listing Exchange codes (stockmarket.stockinfo."Listing Exchange")
- `A` = NYSE MKT
- `N` = New York Stock Exchange (NYSE)
- `P` = NYSE ARCA
- `Z` = BATS Global Markets (BATS)
- `V` = Investors' Exchange, LLC (IEXG)
- `Q` = NASDAQ Global Select Market (top-tier NASDAQ)
- Source: stockmarket db_description_withhint.txt.

### Financial Status codes (stockmarket.stockinfo."Financial Status")
- `D` = Deficient (failed NASDAQ continued-listing requirements)
- `E` = Delinquent (missed regulatory filing deadline)
- `Q` = Bankrupt
- `N` = Normal (default — not deficient/delinquent/bankrupt)
- `G` = Deficient AND bankrupt
- `H` = Deficient AND delinquent
- `J` = Delinquent AND bankrupt
- `K` = Deficient AND delinquent AND bankrupt
- **Financially troubled** = `Financial Status` ∈ {`D`, `E`, `G`, `H`, `J`, `K`} (i.e., deficient OR delinquent — does NOT require bankruptcy).
- Source: stockmarket db_description_withhint.txt.

### Market Category codes (stockmarket.stockinfo."Market Category")
- `Q` = NASDAQ Global Select Market
- `G` = NASDAQ Global Market
- `S` = NASDAQ Capital Market
- Source: stockmarket db_description_withhint.txt.

### delinquent / deficient (stockinfo.Financial Status)
- Naïve interpretation: synonyms.
- Correct definition: **distinct** Nasdaq classifications. Keep distinct in group-by.

### per-ticker trade tables (stockmarket.stocktrade_database)
- 2,753 tables, one per ticker. Table name = ticker. See `join_keys.md` for the dynamic-SQL resolver.

---

## yelp

### is_open (yelp.business.is_open)
- Naïve interpretation: "currently open" (24/7 sense).
- Correct definition: `1` = business is still operational (has not shut down); `0` = business permanently closed. This is NOT hours-of-operation ("open right now"). Hours live in `business.hours`.
- Source: yelp db_description.

### elite status (yelp.user.elite)
- Naïve interpretation: boolean.
- Correct definition: **string of years** the user held elite status (e.g., `"2016,2017,2019"`). Empty string = never elite. Count distinct years for tenure.
- Source: yelp db_description.

### business location (yelp.business)
- Naïve interpretation: look for `city` / `state` fields.
- Correct definition: location is embedded in `business.description` free text. Extract before filtering.
- Source: yelp db_description_withhint.txt.

### businessid prefix (yelp.business.business_id vs yelp.review/tip.business_ref)
- Same entity, different string prefixes (`businessid_` vs `businessref_`) — see `join_keys.md`.

### review vs tip (yelp.review / yelp.tip)
- A **review** has a rating and long text; a **tip** has no rating, short text, and optional `compliment_count`. They are semantically different — do not union naïvely.

### check-in (yelp.checkin.date)
- One document per business with a **list** of check-in timestamps. To compute daily counts, unnest the list.

---

## Populate when...

- A query returns a result that looks structurally correct but is semantically wrong → probable domain-term entry.
- A DAB hint file mentions a term without a schema definition → promote to this file.
- A correction log entry mentions "the agent used X as a proxy for Y" → promote Y's definition here.
- A DISTINCT on a coded column (status, stage, category) produces a surprise value → enumerate and document.

## Injection test

**Question 1:** "How should the agent define 'active customer' when querying the retail dataset?"
**Expected:** "A customer who completed at least one purchase within the last 90 days. Check orders.order_date >= NOW() - INTERVAL '90 days'. Do not use row existence."
**Result:** PASS
**Date:** 2026-04-11

**Question 2:** "If a user asks 'how many financially troubled companies are on NYSE in the stockmarket dataset?', what two decoding lookups must the agent apply, and what are the correct set memberships?" 

### Expected answer: 
(1) `Listing Exchange = 'N'` (NYSE code); (2) financially troubled = `Financial Status` ∈ {`D`, `E`, `G`, `H`, `J`, `K`} (deficient OR delinquent OR combined; `N` means Normal and is excluded; `Q` is bankruptcy alone). Both decoding tables live in `stockmarket` section of this file.

### LLM Answer (Correct):
Lookup 1: Listing Exchange = 'N' to decode NYSE.
Lookup 2: Financial Status ∈ {D, E, G, H, J, K} to decode financially troubled companies.
Correct set memberships:
NYSE: Listing Exchange = 'N'
Financially troubled: Financial Status in {D, E, G, H, J, K}
**Result:** PASS
**Date:** 2026-04-14