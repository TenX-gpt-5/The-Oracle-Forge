import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.dab.remote_dab_adapter import RemoteDABAdapter
from src.agent.execution_router import ExecutionRouter
from src.tools.remote_sandbox import DEFAULT_REMOTE_DAB_PATH, DEFAULT_REMOTE_HOST, DEFAULT_REMOTE_PYTHON, RemoteSandboxClient


def test_remote_sandbox_defaults_match_provided_server_details():
    client = RemoteSandboxClient()
    assert client.config.host == DEFAULT_REMOTE_HOST
    assert client.config.dab_path == DEFAULT_REMOTE_DAB_PATH
    assert client.config.python_executable == DEFAULT_REMOTE_PYTHON


def test_execution_router_keeps_remote_sandbox_optional():
    os.environ.pop("REMOTE_SANDBOX_ENABLED", None)
    router = ExecutionRouter()
    result = router.execute_plan(
        question="Tell me what tables exist in Postgres.",
        plan={
            "question_type": "schema_discovery",
            "required_sources": ["postgres"],
            "entities": ["customer"],
            "join_keys": [],
            "needs_text_extraction": False,
            "needs_domain_resolution": [],
            "expected_output_shape": "schema_listing",
        },
        context_payload={},
        scratchpads=[],
        repair_context={},
    )
    assert result["success"] is True
    assert "remote_sandbox" not in result["artifacts"]


def test_remote_dab_adapter_uses_remote_dab_root():
    adapter = RemoteDABAdapter()
    assert adapter.client.config.dab_path == DEFAULT_REMOTE_DAB_PATH
