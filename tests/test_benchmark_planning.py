import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.agent.execution_router import ExecutionRouter
from src.planning.planner import Planner


def test_planner_uses_benchmark_sources_for_yelp_question():
    planner = Planner()
    plan = planner.generate_plan(
        "What is the average rating of all businesses located in Indianapolis, Indiana?",
        benchmark_context={
            "db_clients": {
                "businessinfo_database": {"db_type": "mongo"},
                "user_database": {"db_type": "duckdb"},
            }
        },
    )
    assert set(plan["required_sources"]) == {"mongodb", "duckdb"}
    assert plan["question_type"] == "cross_db_aggregation"
    assert plan["expected_output_shape"] == "benchmark_answer"


def test_execution_router_extracts_location_and_join_keys_for_yelp():
    router = ExecutionRouter()
    city, state_name = router._extract_city_state(
        "What is the average rating of all businesses located in Indianapolis, Indiana?"
    )
    assert city == "Indianapolis"
    assert state_name == "Indiana"
    assert router._state_abbreviation(state_name) == "IN"
    assert router._business_id_to_review_ref("businessid_52") == "businessref_52"


def test_execution_router_matches_location_in_business_description():
    router = ExecutionRouter()
    assert router._description_matches_location(
        "Located at 5000 W 96th St in Indianapolis, IN, this establishment offers antiques.",
        "Indianapolis",
        "Indiana",
        "IN",
    )
