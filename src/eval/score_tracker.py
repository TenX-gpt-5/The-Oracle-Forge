"""
score_tracker.py

Computes summary metrics over harness trial outputs.
"""

from __future__ import annotations


class ScoreTracker:
    def calculate_scores(self, trial_results: list[dict]) -> dict:
        if not trial_results:
            return {"pass_at_1": 0.0, "total_trials": 0, "failure_classes": {}}

        passed = sum(1 for result in trial_results if result.get("passed"))
        failure_classes: dict[str, int] = {}
        for result in trial_results:
            failure_class = result.get("failure_class", "none")
            failure_classes[failure_class] = failure_classes.get(failure_class, 0) + 1
        return {
            "pass_at_1": passed / len(trial_results),
            "total_trials": len(trial_results),
            "failure_classes": failure_classes,
        }
