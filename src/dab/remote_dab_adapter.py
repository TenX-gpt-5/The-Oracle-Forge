"""
remote_dab_adapter.py

Adapter for the remote DataAgentBench checkout accessed through the remote
sandbox bridge.
"""

from __future__ import annotations

import json
import sqlite3
import textwrap
from typing import Any

from src.tools.remote_sandbox import DEFAULT_REMOTE_DAB_PATH, RemoteSandboxClient, RemoteSandboxConfig


class RemoteDABAdapter:
    def __init__(
        self,
        client: RemoteSandboxClient | None = None,
        config: RemoteSandboxConfig | None = None,
    ):
        self.client = client or RemoteSandboxClient(config or RemoteSandboxConfig())
        self.config = self.client.config

    def enabled(self) -> bool:
        return self.client.enabled()

    def get_query_bundle(
        self,
        dataset: str,
        query_id: int,
        use_hints: bool = False,
    ) -> dict[str, Any]:
        script = textwrap.dedent(
            f"""
            from pathlib import Path
            import json
            from common_scaffold.tools.db_utils.db_config import load_db_clients

            root = Path("{self.config.dab_path}")
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
        )
        return self._run_json_script(script)

    def list_db_objects(self, dataset: str, db_name: str) -> dict[str, Any]:
        fallback = self._run_sql_file_sqlite(dataset=dataset, db_name=db_name, query=None, mode="list_db")
        if fallback.get("ok"):
            return fallback
        script = textwrap.dedent(
            f"""
            from pathlib import Path
            import json
            from common_scaffold.tools.ListDBTool import ListDBTool

            root = Path("{self.config.dab_path}")
            dataset_dir = root / "query_{dataset}"
            log_path = dataset_dir / ".oracle_forge_list_db.jsonl"
            tool = ListDBTool(log_path=log_path, name="list_db", db_config_path=dataset_dir / "db_config.yaml", check_load=False)
            try:
                result = tool.exec({{"db_name": "{db_name}"}})
                print(json.dumps(result, default=str))
            finally:
                tool.clean_up()
            """
        )
        return self._run_json_script(script)

    def query_db(self, dataset: str, db_name: str, query: str) -> dict[str, Any]:
        fallback = self._run_sql_file_sqlite(dataset=dataset, db_name=db_name, query=query, mode="query_db")
        if fallback.get("ok"):
            return fallback
        query_json = json.dumps(query)
        script = textwrap.dedent(
            f"""
            from pathlib import Path
            import json
            from common_scaffold.tools.QueryDBTool import QueryDBTool

            root = Path("{self.config.dab_path}")
            dataset_dir = root / "query_{dataset}"
            log_path = dataset_dir / ".oracle_forge_query_db.jsonl"
            tool = QueryDBTool(log_path=log_path, name="query_db", db_config_path=dataset_dir / "db_config.yaml", check_load=False)
            try:
                result = tool.exec({{"db_name": "{db_name}", "query": {query_json}}})
                print(json.dumps(result, default=str))
            finally:
                tool.clean_up()
            """
        )
        result = self._run_json_script(script)
        if result.get("success"):
            return result
        mongo_fallback = self._run_mongo_bson_fallback(dataset=dataset, db_name=db_name, query=query)
        if mongo_fallback.get("ok"):
            return mongo_fallback
        return result

    def validate_answer(self, dataset: str, query_id: int, answer: str) -> dict[str, Any]:
        answer_json = json.dumps(answer)
        script = textwrap.dedent(
            f"""
            from pathlib import Path
            import json
            from common_scaffold.validate.validate import validate

            root = Path("{self.config.dab_path}")
            query_dir = root / "query_{dataset}" / "query{query_id}"
            result = validate(query_dir=query_dir, llm_answer={answer_json})
            print(json.dumps(result, default=str))
            """
        )
        return self._run_json_script(script)

    def _run_json_script(self, script: str) -> dict[str, Any]:
        response = self.client.run_python(script, cwd=self.config.code_path)
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

    def _run_sql_file_sqlite(
        self,
        dataset: str,
        db_name: str,
        query: str | None,
        mode: str,
    ) -> dict[str, Any]:
        script = textwrap.dedent(
            f"""
            from pathlib import Path
            import json
            import sqlite3
            from common_scaffold.tools.db_utils.db_config import load_db_clients

            root = Path("{self.config.dab_path}")
            dataset_dir = root / "query_{dataset}"
            db_clients = load_db_clients(dataset_dir / "db_config.yaml")
            db_client = db_clients.get("{db_name}", {{}})
            sql_file = Path(db_client.get("sql_file", ""))
            if sql_file and not sql_file.is_absolute():
                sql_file = (dataset_dir / sql_file).resolve()
            if not sql_file.exists():
                print(json.dumps({{"ok": False, "success": False, "error": "fallback unavailable"}}))
            else:
                local_db_path = dataset_dir / ".oracle_forge_{db_name}.sqlite"
                needs_init = not local_db_path.exists() or local_db_path.stat().st_size == 0
                if needs_init:
                    if local_db_path.exists():
                        local_db_path.unlink()
                    with sqlite3.connect(local_db_path) as conn:
                        conn.executescript(sql_file.read_text())
                        conn.commit()
                with sqlite3.connect(local_db_path) as conn:
                    conn.row_factory = sqlite3.Row
                    if "{mode}" == "list_db":
                        rows = conn.execute(
                            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name;"
                        ).fetchall()
                        payload = {{
                            "ok": True,
                            "success": True,
                            "result": [row[0] for row in rows],
                            "table_names": [row[0] for row in rows],
                            "schema": {{"tables": [row[0] for row in rows]}},
                        }}
                    else:
                        rows = conn.execute({json.dumps(query or "")}).fetchall()
                        columns = [desc[0] for desc in conn.execute({json.dumps(query or "")}).description] if rows else []
                        payload = {{
                            "ok": True,
                            "success": True,
                            "result": [dict(row) for row in rows],
                            "columns": columns,
                            "row_count": len(rows),
                            "source": "sqlite-sqlfile-fallback",
                        }}
                    print(json.dumps(payload, default=str))
            """
        )
        response = self.client.run_python(script, cwd=self.config.code_path)
        if not response.get("ok"):
            return {"ok": False, "error": response.get("stderr") or response.get("stdout") or "fallback failed"}
        stdout = response.get("stdout", "").strip()
        try:
            result = json.loads(stdout)
            if result.get("ok") and result.get("success", result.get("ok")):
                return result
        except json.JSONDecodeError:
            pass
        return {"ok": False, "error": "fallback unavailable"}

    def _run_mongo_bson_fallback(
        self,
        dataset: str,
        db_name: str,
        query: str | None,
    ) -> dict[str, Any]:
        query_json = json.dumps(query or "")
        script = textwrap.dedent(
            f"""
            from pathlib import Path
            import json
            from bson import decode_all
            from common_scaffold.tools.db_utils.db_config import load_db_clients

            root = Path("{self.config.dab_path}")
            dataset_dir = root / "query_{dataset}"
            db_clients = load_db_clients(dataset_dir / "db_config.yaml")
            db_client = db_clients.get("{db_name}", {{}})
            dump_folder = Path(db_client.get("dump_folder", ""))
            if dump_folder and not dump_folder.is_absolute():
                dump_folder = (dataset_dir / dump_folder).resolve()
            try:
                query = json.loads({query_json})
            except Exception:
                print(json.dumps({{"ok": False, "success": False, "error": "invalid mongo query"}}))
            else:
                collection = str(query.get("collection", "")).strip()
                limit = query.get("limit")
                projection = query.get("projection", {{}})
                candidate_files = [
                    dump_folder / f"{{collection}}.bson",
                    dump_folder / db_client.get("db_name", "") / f"{{collection}}.bson",
                ]
                collection_file = next((path for path in candidate_files if path.exists()), None)
                if not collection_file:
                    print(json.dumps({{"ok": False, "success": False, "error": f"Collection does not exist: {{collection}}"}}))
                else:
                    docs = decode_all(collection_file.read_bytes())
                    if isinstance(limit, int) and limit >= 0:
                        docs = docs[:limit]
                    if isinstance(projection, dict) and projection:
                        projected = []
                        for doc in docs:
                            row = {{}}
                            include_any = any(bool(v) for k, v in projection.items() if k != "_id")
                            for field, flag in projection.items():
                                if field == "_id":
                                    continue
                                if flag:
                                    row[field] = doc.get(field)
                            if not include_any:
                                row = dict(doc)
                            projected.append(row)
                        docs = projected
                    print(json.dumps({{"ok": True, "success": True, "result": docs, "row_count": len(docs), "source": "bson-dump-fallback"}}, default=str))
            """
        )
        response = self.client.run_python(script, cwd=self.config.code_path)
        if not response.get("ok"):
            return {"ok": False, "error": response.get("stderr") or response.get("stdout") or "mongo bson fallback failed"}
        stdout = response.get("stdout", "").strip()
        try:
            result = json.loads(stdout)
            if result.get("ok") and result.get("success", result.get("ok")):
                return result
        except json.JSONDecodeError:
            pass
        return {"ok": False, "error": "mongo bson fallback unavailable"}
