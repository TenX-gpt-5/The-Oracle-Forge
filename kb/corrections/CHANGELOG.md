# Corrections Log (KB v3)

**Running structured log of agent failures:**
Format: `[query that failed] -> [what was wrong] -> [correct approach]`

*This is the self-learning loop. The agent will read this at session start to improve from its own errors without retraining.*

## [2026-04-11]
- Initialized corrections log.
- Created `corrections_log.md` with 15 structured entries seeded from adversarial probe library. Covers all 4 failure categories: ill-formatted key mismatch (4), domain knowledge gap (4), multi-database routing failure (4), unstructured text extraction failure (3).
# Corrections KB Changelog

## 2026-04-11

- Initialized corrections KB directory and changelog.
- Added `yelp_query1.md`.
