import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.agent.execution_router import ExecutionRouter
from src.tools.toolbox_client import ToolboxClient


def test_toolbox_client_falls_back_when_toolbox_binary_is_unavailable():
    client = ToolboxClient(toolbox_path="/definitely/missing/toolbox")
    assert client.available() is False

    result = client.inspect_schema("postgres")
    assert result["ok"] is True
    assert "users" in result["table_names"]


def test_execution_router_uses_toolbox_client_abstraction_with_local_fallback():
    router = ExecutionRouter()
    router.toolbox = ToolboxClient(toolbox_path="/definitely/missing/toolbox")

    result = router.execute_plan(
        question="Which customer segments had order activity and how does that compare with support ticket volume?",
        plan={
            "question_type": "cross_db_aggregation",
            "required_sources": ["postgres", "sqlite", "mongodb"],
            "entities": ["customer", "support_ticket", "segment"],
            "join_keys": ["customer_id"],
            "needs_text_extraction": False,
            "needs_domain_resolution": [],
            "expected_output_shape": "ranked_segments_plus_explanation",
        },
        context_payload={},
        scratchpads=[],
        repair_context={},
    )

    assert result["success"] is True
    assert any(call.get("mode") == "local-fallback" for call in result["tool_calls"])
    assert result["artifacts"]["segment_rollup"]
