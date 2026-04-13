"""
remote_dab_adapter.py

Adapter for the remote DataAgentBench checkout accessed through the remote
sandbox bridge.
"""

from __future__ import annotations

import json
from typing import Any

from src.tools.remote_sandbox import DEFAULT_REMOTE_DAB_PATH, RemoteSandboxClient


class RemoteDABAdapter:
    def __init__(self, client: RemoteSandboxClient | None = None):
        self.client = client or RemoteSandboxClient()

    def enabled(self) -> bool:
        return self.client.enabled()

    def get_query_bundle(
        self,
        dataset: str,
        query_id: int,
        use_hints: bool = False,
    ) -> dict[str, Any]:
        script = f"""
from pathlib import Path
import json
from common_scaffold.tools.db_utils.db_config import load_db_clients

root = Path("{DEFAULT_REMOTE_DAB_PATH}")
dataset_dir = root / "query_{dataset}"
query_dir = dataset_dir / "query{query_id}"
query_info = json.loads((query_dir / "query.json").read_text())
if isinstance(query_info, dict):
    query_text = query_info.get("query", "")
else:
    query_text = query_info
db_description = (dataset_dir / "db_description.txt").read_text().strip()
if {str(use_hints)}:
    hint_path = dataset_dir / "db_description_withhint.txt"
    if hint_path.exists():
        db_description += "\\n\\n" + hint_path.read_text().strip()
db_clients = load_db_clients(dataset_dir / "db_config.yaml")
payload = {{
    "dataset": "{dataset}",
    "query_id": {query_id},
    "query_dir": str(query_dir),
    "dataset_dir": str(dataset_dir),
    "query_text": query_text,
    "db_description": db_description,
    "db_config_path": str(dataset_dir / "db_config.yaml"),
    "db_clients": db_clients,
}}
print(json.dumps(payload, default=str))
"""
        return self._run_json_script(script)

    def list_db_objects(self, dataset: str, db_name: str) -> dict[str, Any]:
        script = f"""
from pathlib import Path
import json
from common_scaffold.tools.ListDBTool import ListDBTool

root = Path("{DEFAULT_REMOTE_DAB_PATH}")
dataset_dir = root / "query_{dataset}"
log_path = dataset_dir / ".oracle_forge_list_db.jsonl"
tool = ListDBTool(log_path=log_path, name="list_db", db_config_path=dataset_dir / "db_config.yaml", check_load=True)
try:
    result = tool.exec({{"db_name": "{db_name}"}})
    print(json.dumps(result, default=str))
finally:
    tool.clean_up()
"""
        return self._run_json_script(script)

    def query_db(self, dataset: str, db_name: str, query: str) -> dict[str, Any]:
        query_json = json.dumps(query)
        script = f"""
from pathlib import Path
import json
from common_scaffold.tools.QueryDBTool import QueryDBTool

root = Path("{DEFAULT_REMOTE_DAB_PATH}")
dataset_dir = root / "query_{dataset}"
log_path = dataset_dir / ".oracle_forge_query_db.jsonl"
tool = QueryDBTool(log_path=log_path, name="query_db", db_config_path=dataset_dir / "db_config.yaml", check_load=True)
try:
    result = tool.exec({{"db_name": "{db_name}", "query": {query_json}}})
    print(json.dumps(result, default=str))
finally:
    tool.clean_up()
"""
        return self._run_json_script(script)

    def validate_answer(self, dataset: str, query_id: int, answer: str) -> dict[str, Any]:
        answer_json = json.dumps(answer)
        script = f"""
from pathlib import Path
import json
from common_scaffold.validate.validate import validate

root = Path("{DEFAULT_REMOTE_DAB_PATH}")
query_dir = root / "query_{dataset}" / "query{query_id}"
result = validate(query_dir=query_dir, llm_answer={answer_json})
print(json.dumps(result, default=str))
"""
        return self._run_json_script(script)

    def _run_json_script(self, script: str) -> dict[str, Any]:
        response = self.client.run_python(script, cwd=DEFAULT_REMOTE_DAB_PATH)
        if not response.get("ok"):
            return {
                "ok": False,
                "error": response.get("stderr") or response.get("stdout") or "Unknown remote execution failure",
                "transport": response,
            }
        stdout = response.get("stdout", "").strip()
        try:
            return json.loads(stdout)
        except json.JSONDecodeError:
            return {
                "ok": False,
                "error": "Remote script did not return valid JSON",
                "stdout": stdout,
                "transport": response,
            }
