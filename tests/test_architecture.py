import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.agent.orchestrator import Orchestrator
from src.eval.harness import Harness
from src.eval.score_tracker import ScoreTracker


def test_orchestrator_executes_full_architecture_path():
    agent = Orchestrator()
    result = agent.execute_turn(
        "Which customer segments had order activity and how does that compare with support ticket volume?"
    )

    assert "plan" in result
    assert "retrieved_context" in result
    assert "scratchpads" in result
    assert "execution_result" in result
    assert "validation" in result
    assert "final_answer" in result
    assert "experience_id" in result

    steps = [node.get("step") for node in result["trace"]]
    assert "planner" in steps
    assert "context_cortex" in steps
    assert "scratchpad_manager" in steps
    assert "execution_router" in steps
    assert "validator" in steps
    assert "answer_synthesizer" in steps


def test_context_cortex_loads_multiple_context_layers():
    agent = Orchestrator()
    result = agent.execute_turn("Tell me what tables exist in Postgres.")
    context = result["retrieved_context"]

    assert "global_rules" in context
    assert "project_memory" in context
    assert "schemas" in context
    assert "join_key_intelligence" in context
    assert "domain_rules" in context
    assert "episodic_recall" in context


def test_harness_and_score_tracker_produce_metrics():
    harness = Harness()
    tracker = ScoreTracker()
    trial = harness.run_trial(
        {
            "question": "Tell me what tables exist in Postgres.",
            "expected_contains": ["postgres", "users", "orders"],
        }
    )
    scores = tracker.calculate_scores([trial])

    assert "pass_at_1" in scores
    assert scores["total_trials"] == 1
