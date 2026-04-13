"""
harness.py

Lightweight evaluation harness for running repeatable trials.
"""

from __future__ import annotations

from src.agent.orchestrator import Orchestrator


class Harness:
    def __init__(self, agent: Orchestrator | None = None):
        self.agent = agent or Orchestrator()

    def run_trial(self, trial_spec: dict) -> dict:
        result = self.agent.execute_turn(trial_spec["question"])
        expected_contains = trial_spec.get("expected_contains", [])
        answer = result.get("final_answer", "")
        passed = all(fragment.lower() in answer.lower() for fragment in expected_contains)
        return {
            "question": trial_spec["question"],
            "result": result,
            "passed": passed and result["validation"]["status"] == "passed",
            "failure_class": result["validation"]["failure_class"],
        }
