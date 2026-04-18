# Oracle Forge

Production-style data-agent workspace for the Oracle Forge challenge.

## Team And Roles

| Name | Role |
|------|------|
| Eyobed Feleke | Driver |
| Gersum Asfaw | Driver |
| Samuel Lachisa | Intelligence Officer |
| Kidane Gebremedhin| Intelligence Officer |
| Bethelhem Abay | Signal Corps |
| Lidya Dagnew | Signal Corps |
- Team `oracle-forge-gpt5` shared session: architecture review, mob approvals, and integration validation

## Live Agent Access

- Live agent access guide: [TEAM_JOIN.md](./TEAM_JOIN.md)
- Shared server access: `ssh trp-gpt5`
- Shared tmux socket: `/shared/tmux/oracle-forge.sock`
- Shared tmux session: `oracle-forge-gpt5`
- Agent runtime path on server: `/shared/DataAgentBench/oracle_forge_v3`

## Architecture Diagram

- Diagram source docs: [ARCHITECTURE.md](./ARCHITECTURE.md), [ARCHITECTURE_V2.md](./ARCHITECTURE_V2.md), [ARCHITECTURE_V3.md](./ARCHITECTURE_V3.md)
- Diagram image artifact: ![Architecture](./image.png)

## Fresh Machine Setup

```bash
git clone https://github.com/TenX-gpt-5/The-Oracle-Forge.git
cd The-Oracle-Forge
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Quick smoke command after setup:

```bash
python3 run_benchmark_query.py --dataset yelp --query-id 1 --validate-answer
```

This repo currently includes:

- Oracle Forge architecture docs
- a runnable benchmark-oriented agent skeleton
- remote DataAgentBench integration
- a shared tmux collaboration workflow
- a shared MCP Toolbox configuration

## Fast Start

Join the shared server:

```bash
ssh trp-gpt5
tmux -S /shared/tmux/oracle-forge.sock attach -t oracle-forge-gpt5
```

Shared windows:

```text
1: dab
2: agent
3: toolbox
```

## Team Validation

Check the shared tmux session:

```bash
tmux -S /shared/tmux/oracle-forge.sock list-windows -t oracle-forge-gpt5
```

Run the benchmark starter query:

```bash
cd /shared/DataAgentBench/oracle_forge_v3
source .venv/bin/activate
python3 run_benchmark_query.py --dataset yelp --query-id 1 --validate-answer
```

Healthy output should end with:

```text
"is_valid": true
```

## Database Access Tests

Run these from the shared server.

### MongoDB

Yelp business metadata sample:

```bash
cd /shared/DataAgentBench/oracle_forge_v3
source .venv/bin/activate
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

### DuckDB

Yelp review rows sample:

```bash
cd /shared/DataAgentBench/oracle_forge_v3
source .venv/bin/activate
python3 - <<'PY'
from src.dab.remote_dab_adapter import RemoteDABAdapter

adapter = RemoteDABAdapter()
sql = "SELECT business_ref, rating, text FROM review LIMIT 3;"
print(adapter.query_db("yelp", "user_database", sql))
PY
```

### SQLite

List SQLite objects in `crmarenapro`:

```bash
cd /shared/DataAgentBench/oracle_forge_v3
source .venv/bin/activate
python3 - <<'PY'
from src.dab.remote_dab_adapter import RemoteDABAdapter

adapter = RemoteDABAdapter()
print(adapter.list_db_objects("crmarenapro", "core_crm"))
PY
```

Query a SQLite table after confirming the name from the listing:

```bash
cd /shared/DataAgentBench/oracle_forge_v3
source .venv/bin/activate
python3 - <<'PY'
from src.dab.remote_dab_adapter import RemoteDABAdapter

adapter = RemoteDABAdapter()
print(adapter.query_db("crmarenapro", "core_crm", "SELECT * FROM User LIMIT 3;"))
PY
```

### PostgreSQL

List PostgreSQL objects in `crmarenapro`:

```bash
cd /shared/DataAgentBench/oracle_forge_v3
source .venv/bin/activate
python3 - <<'PY'
from src.dab.remote_dab_adapter import RemoteDABAdapter

adapter = RemoteDABAdapter()
print(adapter.list_db_objects("crmarenapro", "support"))
PY
```

Then query a table after confirming the real name from the listing:

```bash
cd /shared/DataAgentBench/oracle_forge_v3
source .venv/bin/activate
python3 - <<'PY'
from src.dab.remote_dab_adapter import RemoteDABAdapter

adapter = RemoteDABAdapter()
print(adapter.query_db("crmarenapro", "support", "SELECT * FROM tickets LIMIT 3;"))
PY
```

If `tickets` is not the real table name, replace it with the name returned by `list_db_objects`.

## Toolbox Checks

The shared Toolbox config is in [mcp/tools.yaml](./mcp/tools.yaml).

In this repo, `mcp/tools.yaml` currently defines native Toolbox connections for:

- PostgreSQL
- SQLite
- MongoDB

DuckDB is currently handled through the Oracle Forge remote DAB path rather than a native Toolbox source.

If you need to start Toolbox manually:

```bash
cd /shared/DataAgentBench/oracle_forge_v3
./bin/toolbox serve --tools-file mcp/tools.yaml
```

Quick checks:

```bash
curl http://127.0.0.1:5000/
```

```bash
cd /shared/DataAgentBench/oracle_forge_v3
./bin/toolbox --tools-file mcp/tools.yaml invoke mongodb-find-businesses | head
```

```bash
cd /shared/DataAgentBench/oracle_forge_v3
./bin/toolbox --tools-file mcp/tools.yaml invoke postgres-list-tables
```

Important note:

- the current Toolbox runtime in this environment responds to `/` for health checks
- use `toolbox invoke ...` rather than `/v1/tools` as the main validation path

## Notes

- DuckDB is currently accessed through the Oracle Forge remote DAB path, not as a native MCP Toolbox source.
- The shared tmux socket is `/shared/tmux/oracle-forge.sock`.
- The shared team session name is `oracle-forge-gpt5`.
- If you want to stop a command inside tmux, use `Ctrl-C`, not `exit`.

## Tenai Sandbox Fallback

If the tenai sandbox VM launches successfully but the automated test pipeline fails on editable install or missing test modules, use this recovery flow inside the VM:

```bash
cd ~/tenai-infra
uv venv
uv sync
uv pip install setuptools wheel python-dotenv pytest fastapi starlette pydantic uvicorn httpx anyio
.venv/bin/python3 -m pytest -q
```

Interpretation:

- if VM launch fails before tests, the problem is sandbox provisioning
- if tests run but fail on missing modules, the problem is project packaging/dependency metadata
- passing `pytest` in the VM confirms the sandbox itself is healthy and usable for team experiments

## Related Docs

- [TEAM_JOIN.md](./TEAM_JOIN.md)
- [ARCHITECTURE.md](./ARCHITECTURE.md)
- [ARCHITECTURE_V2.md](./ARCHITECTURE_V2.md)
- [ARCHITECTURE_V3.md](./ARCHITECTURE_V3.md)
- [ORACLE_FORGE_WIN_PLAN.md](./ORACLE_FORGE_WIN_PLAN.md)
