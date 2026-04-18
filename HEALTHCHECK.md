# Oracle Forge Healthcheck

Use this file to quickly verify that the shared Oracle Forge environment is healthy on the remote server.

## 1. Join The Shared Session

```bash
ssh trp-gpt5
tmux -S /shared/tmux/oracle-forge.sock attach -t oracle-forge-gpt5
```

## 2. Main Smoke Test

This is the best end-to-end validation of the current shared setup.

```bash
cd /shared/DataAgentBench/oracle_forge_v3
source .venv/bin/activate
python3 run_benchmark_query.py --dataset yelp --query-id 1 --validate-answer
```

Healthy result:

```text
"is_valid": true
```

That confirms:

- Oracle Forge code is available
- remote DAB integration is working
- MongoDB + DuckDB access is working
- planner, execution, validation, and synthesis are working
- the official DAB validator accepts the answer

## 3. Per-Database Quick Tests

Run these from:

```bash
cd /shared/DataAgentBench/oracle_forge_v3
source .venv/bin/activate
```

### MongoDB

```bash
python3 - <<'PY'
import json
from src.dab.remote_dab_adapter import RemoteDABAdapter

adapter = RemoteDABAdapter()
query = json.dumps({
    "collection": "business",
    "projection": {"business_id": 1, "name": 1, "description": 1, "_id": 0},
    "limit": 3,
})
print(adapter.query_db("yelp", "businessinfo_database", query))
PY
```

Expected signal:

- `success: True`
- list of Yelp business documents

### DuckDB

```bash
python3 - <<'PY'
from src.dab.remote_dab_adapter import RemoteDABAdapter

adapter = RemoteDABAdapter()
print(adapter.query_db("yelp", "user_database", "SELECT business_ref, rating, text FROM review LIMIT 3;"))
PY
```

Expected signal:

- `success: True`
- rows from the `review` table

### SQLite

```bash
python3 - <<'PY'
from src.dab.remote_dab_adapter import RemoteDABAdapter

adapter = RemoteDABAdapter()
print(adapter.list_db_objects("crmarenapro", "core_crm"))
PY
```

Expected signal:

- `success: True`
- list of SQLite tables

Optional query:

```bash
python3 - <<'PY'
from src.dab.remote_dab_adapter import RemoteDABAdapter

adapter = RemoteDABAdapter()
print(adapter.query_db("crmarenapro", "core_crm", "SELECT * FROM User LIMIT 3;"))
PY
```

### PostgreSQL

```bash
python3 - <<'PY'
from src.dab.remote_dab_adapter import RemoteDABAdapter

adapter = RemoteDABAdapter()
print(adapter.list_db_objects("crmarenapro", "support"))
PY
```

Expected signal:

- `success: True`
- list of PostgreSQL tables

Optional query:

```bash
python3 - <<'PY'
from src.dab.remote_dab_adapter import RemoteDABAdapter

adapter = RemoteDABAdapter()
print(adapter.query_db("crmarenapro", "support", "SELECT * FROM tickets LIMIT 3;"))
PY
```

If `tickets` is not the real table name, replace it with one returned by `list_db_objects`.

## 4. Toolbox Quick Checks

The shared Toolbox server runs in the `toolbox` tmux window.

Check that the server is alive:

```bash
curl http://127.0.0.1:5000/
```

Expected result:

```text
🧰 Hello, World! 🧰
```

Invoke a Toolbox tool directly:

```bash
cd /shared/DataAgentBench/oracle_forge_v3
./bin/toolbox --tools-file mcp/tools.yaml invoke mongodb-find-businesses | head
```

Expected signal:

- initialized tools
- returned Yelp business documents

## 5. Shared tmux Checks

Confirm the shared session exists:

```bash
tmux -S /shared/tmux/oracle-forge.sock list-sessions
```

Expected session name:

```text
oracle-forge-gpt5
```

Confirm the windows exist:

```bash
tmux -S /shared/tmux/oracle-forge.sock list-windows -t oracle-forge-gpt5
```

Expected windows:

```text
1: dab
2: agent
3: toolbox
```

## 6. If Something Fails

Start with these checks:

```bash
whoami
pwd
tmux -S /shared/tmux/oracle-forge.sock list-windows -t oracle-forge-gpt5
cd /shared/DataAgentBench/oracle_forge_v3
source .venv/bin/activate
python3 -c "from src.dab.remote_dab_adapter import RemoteDABAdapter; print('ok')"
```

If the final line prints:

```text
ok
```

then the Oracle Forge Python import path is healthy.
