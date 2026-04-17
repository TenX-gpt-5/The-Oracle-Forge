import os
import sys
import json
import tempfile

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


def test_toolbox_client_uses_duckdb_mcp_config_instead_of_direct_file():
    with tempfile.TemporaryDirectory() as temp_dir:
        server_script = os.path.join(temp_dir, "fake_duckdb_mcp_server.py")
        with open(server_script, "w", encoding="utf-8") as f:
            f.write(
                """
import json
import sys


def send(payload):
    body = json.dumps(payload)
    sys.stdout.write(f"Content-Length: {len(body.encode('utf-8'))}\\r\\n\\r\\n{body}")
    sys.stdout.flush()


while True:
    headers = {}
    while True:
        line = sys.stdin.readline()
        if not line:
            raise SystemExit(0)
        stripped = line.strip()
        if not stripped:
            break
        name, value = stripped.split(":", 1)
        headers[name.lower()] = value.strip()

    length = int(headers["content-length"])
    message = json.loads(sys.stdin.read(length))
    method = message.get("method")
    request_id = message.get("id")

    if method == "initialize":
        send({"jsonrpc": "2.0", "id": request_id, "result": {"protocolVersion": "2024-11-05", "capabilities": {}}})
    elif method == "notifications/initialized":
        continue
    elif method == "tools/list":
        send(
            {
                "jsonrpc": "2.0",
                "id": request_id,
                "result": {
                    "tools": [
                        {
                            "name": "duckdb-execute",
                            "description": "Execute SQL against fake DuckDB",
                            "inputSchema": {"type": "object"},
                        }
                    ]
                },
            }
        )
    elif method == "tools/call":
        query = message["params"]["arguments"].get("query") or message["params"]["arguments"].get("sql")
        if query == "SHOW TABLES;":
            payload = [{"name": "review"}, {"name": "user"}]
        else:
            payload = [{"metric_name": "ticket_volume", "metric_value": 12}]
        send(
            {
                "jsonrpc": "2.0",
                "id": request_id,
                "result": {
                    "content": [
                        {
                            "type": "text",
                            "text": json.dumps(payload),
                        }
                    ]
                },
            }
        )
    else:
        send({"jsonrpc": "2.0", "id": request_id, "error": {"code": -32601, "message": f"Unknown method {method}"}})
""".strip()
            )

        config_path = os.path.join(temp_dir, "mcp.json")
        with open(config_path, "w", encoding="utf-8") as f:
            json.dump(
                {
                    "servers": {
                        "duckdb": {
                            "command": sys.executable,
                            "args": [server_script],
                            "type": "stdio",
                        }
                    }
                },
                f,
            )

        client = ToolboxClient(toolbox_path="/definitely/missing/toolbox", mcp_config_file=str(config_path))
        schema = client.inspect_schema("duckdb")
        assert schema["ok"] is True
        assert schema["table_names"] == ["review", "user"]

        result, tool_call = client.execute_source(
            source="duckdb",
            question="Show me the latest analytical metric.",
            plan={"required_sources": ["duckdb"]},
            repair_context={},
        )
        assert result["ok"] is True
        assert result["rows"] == [{"metric_name": "ticket_volume", "metric_value": 12}]
        assert tool_call["mode"] == "mcp-stdio"


def test_toolbox_client_normalizes_tuple_text_payload_from_duckdb_mcp():
    with tempfile.TemporaryDirectory() as temp_dir:
        server_script = os.path.join(temp_dir, "fake_duckdb_tuple_server.py")
        with open(server_script, "w", encoding="utf-8") as f:
            f.write(
                """
import json
import sys

def send(payload):
    body = json.dumps(payload)
    sys.stdout.write(f"Content-Length: {len(body.encode('utf-8'))}\\r\\n\\r\\n{body}")
    sys.stdout.flush()


while True:
    headers = {}
    while True:
        line = sys.stdin.readline()
        if not line:
            raise SystemExit(0)
        stripped = line.strip()
        if not stripped:
            break
        name, value = stripped.split(":", 1)
        headers[name.lower()] = value.strip()

    length = int(headers["content-length"])
    message = json.loads(sys.stdin.read(length))
    method = message.get("method")
    request_id = message.get("id")

    if method == "initialize":
        send({"jsonrpc": "2.0", "id": request_id, "result": {"protocolVersion": "2025-11-25", "capabilities": {}}})
    elif method == "notifications/initialized":
        continue
    elif method == "tools/list":
        send(
            {
                "jsonrpc": "2.0",
                "id": request_id,
                "result": {
                    "tools": [
                        {
                            "name": "query",
                            "description": "Execute SQL against fake DuckDB",
                            "inputSchema": {"type": "object"},
                        }
                    ]
                },
            }
        )
    elif method == "tools/call":
        query = message["params"]["arguments"].get("query")
        payload = "[('review',), ('tip',), ('user',)]" if query == "SHOW TABLES;" else "[(1, 'ok')]"
        send(
            {
                "jsonrpc": "2.0",
                "id": request_id,
                "result": {
                    "content": [
                        {
                            "type": "text",
                            "text": payload,
                        }
                    ]
                },
            }
        )
    else:
        send({"jsonrpc": "2.0", "id": request_id, "error": {"code": -32601, "message": f"Unknown method {method}"}})
""".strip()
            )

        config_path = os.path.join(temp_dir, "mcp.json")
        with open(config_path, "w", encoding="utf-8") as f:
            json.dump(
                {
                    "servers": {
                        "duckdb": {
                            "command": sys.executable,
                            "args": [server_script],
                            "type": "stdio",
                        }
                    }
                },
                f,
            )

        client = ToolboxClient(toolbox_path="/definitely/missing/toolbox", mcp_config_file=str(config_path))
        schema = client.inspect_schema("duckdb")
        assert schema["ok"] is True
        assert schema["table_names"] == ["review", "tip", "user"]
