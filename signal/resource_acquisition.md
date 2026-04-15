# Resource Acquisition Report

**Prepared by:** Signal Corps (Bethelhem Abay, Lidya Dagnew)
**Period:** Week 8 — April 8–14, 2026

---

## Summary

Signal Corps applied for free-tier compute and API resources on Day 1 (April 8) as required by the challenge. Results and access instructions are documented below for team use.

---

## 1. Cloudflare Workers — Free Tier

**Application date:** April 8, 2026
**Status:** Available — no application required
**Outcome:** Cloudflare Workers free tier is available immediately on account creation. No waitlist or approval process.

**Free tier limits:**
- 100,000 requests per day
- 10ms CPU time per request
- Suitable for the Oracle Forge Python sandbox execution worker

**Access instructions for the team:**
```bash
# Install Wrangler CLI
npm install -g wrangler

# Log in with your Cloudflare account (free account at cloudflare.com)
wrangler login

# Deploy the sandbox worker (from repo root)
cd workers
wrangler deploy

# Set the sandbox URL in your .env
SANDBOX_URL=https://sandbox.[your-worker-name].workers.dev
```

**Developer programme credits:** Cloudflare does not currently offer a dedicated AI/ML developer credit programme beyond the free tier limits. The free tier is sufficient for Oracle Forge sandbox workloads at current query volumes.

---

## 2. Anthropic API Credits

**Application date:** April 8, 2026
**Status:** Using existing programme allocation
**Outcome:** The TRP1 FDE Programme provides Anthropic API access via the programme infrastructure. No separate application required.

**Access instructions for the team:**
- API key is provisioned through the programme's shared environment
- Set `ANTHROPIC_API_KEY` in your `.env` file from the shared credentials document
- Default model for Oracle Forge agent calls: `claude-sonnet-4-6`

---

## 3. Shared Server Compute — trp-gpt5

**Application date:** April 8, 2026
**Status:** Active — provisioned through tenai-infra
**Outcome:** Shared server `trp-gpt5` is operational. All team members have access via Tailscale mesh.

**Access instructions for the team:**
```bash
# Connect via SSH
ssh trp-gpt5

# Attach to shared tmux session
tmux -S /shared/tmux/oracle-forge.sock attach -t oracle-forge-gpt5

# Agent runtime is at
cd /shared/DataAgentBench/oracle_forge_v3
source venv/bin/activate
```

---

## 4. Google MCP Toolbox — Free Tier

**Application date:** April 9, 2026
**Status:** Downloaded and operational
**Outcome:** Google MCP Toolbox for Databases (v0.30.0) is available as a free binary download. No account or application required.

**Access instructions for the team:**
```bash
# Binary is already at
/shared/DataAgentBench/oracle_forge_v3/bin/toolbox

# Start Toolbox with the team config
./bin/toolbox serve --tools-file mcp/tools.yaml

# Health check
curl http://127.0.0.1:5000/
```

---

## 5. DataAgentBench Access

**Application date:** April 8, 2026
**Status:** Public repository — no application required
**Outcome:** DAB datasets and evaluation scripts are publicly accessible. Subscribed to repository for notifications on new submissions and leaderboard updates.

**Access instructions for the team:**
```bash
git clone https://github.com/ucbepic/DataAgentBench.git
cd DataAgentBench
pip install -r requirements.txt
```

---

## Outstanding Applications

| Resource | Applied | Status | Blocker |
|---|---|---|---|
| Cloudflare AI Gateway (beta) | April 8 | Not yet approved | Beta programme — monitoring |
| Additional Anthropic API quota | April 8 | Sufficient with current allocation | No action needed |

---

## Notes for Team

- All critical resources for the Week 8–9 build are operational.
- Cloudflare Workers is available if the team wants to move the Python sandbox off the shared server — instructions above.
- Monitor Cloudflare AI Gateway beta — useful for caching repeated LLM calls during the 54-query benchmark run.
