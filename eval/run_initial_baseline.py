"""Run a small held-out baseline and write score + trace artifacts to results/."""

from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.eval.harness import Harness
from src.eval.score_tracker import ScoreTracker

TRIALS = [
    {
        "id": "yelp-q2",
        "question": "Which U.S. state has the highest number of reviews, and what is the average rating of businesses in that state?",
        "expected_contains": ["PA"],
    },
    {
        "id": "yelp-q3",
        "question": "During 2018, how many businesses that received reviews offered either business parking or bike parking?",
        "expected_contains": ["35"],
    },
    {
        "id": "yelp-q6",
        "question": "Which business received the highest average rating between January 1, 2016 and June 30, 2016, and what category does it belong to? Consider only businesses with at least 5 reviews.",
        "expected_contains": ["Coffee House Too Cafe"],
    },
]


def main() -> None:
    harness = Harness()
    tracker = ScoreTracker()

    trial_results = []
    for trial in TRIALS:
        result = harness.run_trial(trial)
        result["trial_id"] = trial["id"]
        trial_results.append(result)

    scores = tracker.calculate_scores(trial_results)
    payload = {
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "trial_count": len(TRIALS),
        "scores": scores,
        "trial_results": trial_results,
    }

    out_dir = Path("results")
    out_dir.mkdir(parents=True, exist_ok=True)
    out_file = out_dir / "initial_baseline_with_trace.json"
    out_file.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    print(json.dumps({"output": str(out_file), "scores": scores}, indent=2))


if __name__ == "__main__":
    main()
