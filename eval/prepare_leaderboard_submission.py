"""Flatten a benchmark results JSON into the leaderboard submission format.

The leaderboard expects a single JSON array with one entry per run:

[
  {"dataset": "yelp", "query": "1", "run": 0, "answer": "3.55"},
  ...
]

This helper preserves the run order from the benchmark artifact and emits only
the required fields so the final submission payload stays compact.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


def main() -> None:
    parser = argparse.ArgumentParser(description="Flatten benchmark results for leaderboard submission.")
    parser.add_argument("--input", required=True, help="Path to the nested benchmark results JSON.")
    parser.add_argument("--output", required=True, help="Path to the flat leaderboard JSON array.")
    args = parser.parse_args()

    payload = json.loads(Path(args.input).read_text(encoding="utf-8"))
    flat: list[dict[str, Any]] = []

    for query_result in payload.get("results", []):
        dataset = str(query_result.get("dataset", "")).strip()
        query_id = str(query_result.get("query_id", "")).strip()
        for trial in query_result.get("trial_results", []):
            flat.append(
                {
                    "dataset": dataset,
                    "query": query_id,
                    "run": int(trial.get("trial_index", 0)) - 1,
                    "answer": trial.get("answer"),
                }
            )

    flat.sort(key=lambda row: (row["dataset"], int(row["query"]) if str(row["query"]).isdigit() else row["query"], row["run"]))
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(flat, indent=2), encoding="utf-8")
    print(json.dumps({"input": str(args.input), "output": str(output_path), "rows": len(flat)}, indent=2))


if __name__ == "__main__":
    main()
