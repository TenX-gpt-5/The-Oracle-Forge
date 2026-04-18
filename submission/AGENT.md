# Oracle Forge v3 Agent Summary

## Overview

Oracle Forge v3 is an orchestrated data-agent runtime for DataAgentBench. It launches a query-specific agent loop, routes tool calls to the available database backends, and uses benchmark-aware validation to keep outputs aligned with DAB expectations.

## What the agent does

- Loads the dataset query and database description.
- Chooses between schema inspection, SQL/Mongo querying, and Python-based post-processing.
- Tracks prior tool outputs in the agent loop so later turns can reuse earlier evidence.
- Returns a final answer that the DAB validator can score.

## Evaluation posture

The current submission evidence is based on the remote-local Yelp benchmark path running on the team host. The important properties are:

- `yelp` `q1` through `q7` were each run for `50` trials.
- Every query achieved `pass_at_1 = 1.0`.
- Every query achieved `trial_pass_rate = 1.0`.
- The benchmark-accepted answers were stable across the full sweep.

GitHub Repos is being evaluated in strict mode:

- `q2`, `q3`, and `q4` are confirmed with benchmark evidence.
- `q1` is still under the full 50-trial rerun and remains the last open query in that family.
- We avoid using ground-truth answer files or prior solved artifacts when reasoning about GitHub queries.

## Key design choices

- Keep the remote sandbox interpreter explicit and fall back to `python3` when needed.
- Prefer exact Yelp city/state matching before description-text fallback.
- Normalize Yelp review joins with `businessid_*` -> `businessref_*`.
- Record benchmark evidence in the score log and KB changelogs so the run history is auditable.

## What worked

- Remote-local Yelp execution on the team host.
- DuckDB-backed review access.
- MongoDB-backed business metadata access.
- 50-trial benchmark sweeps for `q1` through `q7`.

## What did not need further work for the Yelp submission

- The verified Yelp runs no longer depend on ad hoc smoke checks.
- The benchmark outputs are now captured in the submission artifacts and local score logs.
