"""Submission-facing harness entry script for ad-hoc trial execution."""

from __future__ import annotations

import argparse
import json

from src.eval.harness import Harness


def main() -> None:
    parser = argparse.ArgumentParser(description="Run one harness trial and print trace-aware output.")
    parser.add_argument("--question", required=True)
    parser.add_argument("--expected", action="append", default=[])
    args = parser.parse_args()

    harness = Harness()
    result = harness.run_trial(
        {
            "question": args.question,
            "expected_contains": args.expected,
        }
    )
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
