import asyncio
import json
import os
import re
import sqlite3
import time
import uuid
from typing import Any, Dict, List, Optional
from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse
from pydantic import BaseModel

app = FastAPI(title="CheckpointMCP — Stateful Agent Task Runner")
DB_PATH = "checkpoints.db"

# ---------------------------------------------------------------------------
# 1. MCP TOOL REGISTRY (25 Realistic Tools across Read / Stage / Mutate)
# ---------------------------------------------------------------------------
MCP_TOOLS: List[Dict[str, Any]] = [
    {"name": "sentry_fetch_error_trace", "tier": "READ", "domain": "monitoring", "keywords": ["bug", "error", "crash", "trace", "sentry", "logs", "500", "exception"], "schema_tokens": 115, "description": "Fetches stack trace and affected user count from Sentry."},
    {"name": "postgres_query_users", "tier": "READ", "domain": "database", "keywords": ["user", "customer", "sql", "query", "account", "billing", "affected", "db"], "schema_tokens": 120, "description": "Read-only SQL lookup on production customer records."},
    {"name": "stripe_list_charges", "tier": "READ", "domain": "payments", "keywords": ["stripe", "charge", "payment", "invoice", "duplicate", "billing", "transaction"], "schema_tokens": 110, "description": "Lists recent payment intents and failed/duplicate charges."},
    {"name": "github_search_commits", "tier": "READ", "domain": "engineering", "keywords": ["commit", "pr", "deploy", "code", "github", "repo", "diff", "bug"], "schema_tokens": 105, "description": "Searches recent repository commits and PR diffs."},
    {"name": "linear_create_draft_issue", "tier": "STAGE", "domain": "project", "keywords": ["ticket", "linear", "issue", "draft", "triage", "roadmap", "task"], "schema_tokens": 110, "description": "Stages a draft engineering ticket in Linear without notifying team."},
    {"name": "gmail_draft_customer_apology", "tier": "STAGE", "domain": "comms", "keywords": ["email", "draft", "apology", "customer", "outreach", "reply", "message"], "schema_tokens": 105, "description": "Stages a draft email in Gmail for human review before sending."},
    {"name": "stripe_issue_refund", "tier": "MUTATE", "domain": "payments", "keywords": ["refund", "stripe", "money", "reimburse", "chargeback", "return", "payment"], "schema_tokens": 130, "description": "Executes an irreversible monetary refund via Stripe API."},
    {"name": "slack_post_incident_alert", "tier": "MUTATE", "domain": "comms", "keywords": ["slack", "notify", "channel", "incident", "alert", "broadcast", "team"], "schema_tokens": 100, "description": "Broadcasts a live alert to #incident-response Slack channel."},
    {"name": "postgres_update_subscription", "tier": "MUTATE", "domain": "database", "keywords": ["update", "mutate", "write", "subscription", "tier", "credits", "db"], "schema_tokens": 125, "description": "Mutates customer subscription state and credits in production DB."},
    {"name": "github_trigger_rollback", "tier": "MUTATE", "domain": "engineering", "keywords": ["rollback", "revert", "deploy", "production", "action", "workflow"], "schema_tokens": 120, "description": "Triggers GitHub Action to roll back production deployment."},
    # Additional 15 MCP tools representing typical workspace bloat
    {"name": "notion_search_wiki", "tier": "READ", "domain": "docs", "keywords": ["notion", "wiki", "runbook", "sop", "doc"], "schema_tokens": 110, "description": "Searches internal Notion runbooks."},
    {"name": "notion_append_block", "tier": "STAGE", "domain": "docs", "keywords": ["notion", "write", "notes", "summary"], "schema_tokens": 115, "description": "Appends draft summary block to Notion page."},
    {"name": "calendar_check_availability", "tier": "READ", "domain": "schedule", "keywords": ["calendar", "slot", "free", "meeting", "time"], "schema_tokens": 105, "description": "Checks Google Calendar availability."},
    {"name": "calendar_schedule_invite", "tier": "MUTATE", "domain": "schedule", "keywords": ["invite", "book", "calendar", "zoom"], "schema_tokens": 120, "description": "Sends external calendar invites to participants."},
    {"name": "datadog_query_metrics", "tier": "READ", "domain": "monitoring", "keywords": ["cpu", "latency", "datadog", "p99", "memory"], "schema_tokens": 125, "description": "Queries p99 latency and CPU telemetry."},
    {"name": "hubspot_get_contact", "tier": "READ", "domain": "crm", "keywords": ["crm", "lead", "hubspot", "deal", "enterprise"], "schema_tokens": 110, "description": "Fetches enterprise deal metadata from HubSpot."},
    {"name": "hubspot_update_deal_stage", "tier": "MUTATE", "domain": "crm", "keywords": ["crm", "stage", "close", "pipeline"], "schema_tokens": 120, "description": "Updates deal pipeline stage in HubSpot."},
    {"name": "aws_s3_list_buckets", "tier": "READ", "domain": "cloud", "keywords": ["s3", "bucket", "storage", "backup", "aws"], "schema_tokens": 100, "description": "Lists S3 storage buckets and backup objects."},
    {"name": "aws_ecs_restart_service", "tier": "MUTATE", "domain": "cloud", "keywords": ["ecs", "restart", "container", "cluster"], "schema_tokens": 130, "description": "Restarts production ECS container cluster."},
    {"name": "jira_search_epics", "tier": "READ", "domain": "project", "keywords": ["jira", "epic", "sprint", "backlog"], "schema_tokens": 115, "description": "Searches Jira sprint epics."},
    {"name": "intercom_fetch_conversations", "tier": "READ", "domain": "support", "keywords": ["intercom", "chat", "support", "ticket"], "schema_tokens": 110, "description": "Reads open customer support threads."},
    {"name": "intercom_reply_user", "tier": "MUTATE", "domain": "support", "keywords": ["intercom", "reply", "send", "customer"], "schema_tokens": 115, "description": "Sends live chat reply to customer."},
    {"name": "bigquery_run_analytics", "tier": "READ", "domain": "analytics", "keywords": ["cohort", "retention", "bigquery", "warehouse"], "schema_tokens": 130, "description": "Runs analytical cohort query in BigQuery."},
    {"name": "cloudflare_purge_cache", "tier": "MUTATE", "domain": "cloud", "keywords": ["cdn", "cache", "cloudflare", "dns"], "schema_tokens": 110, "description": "Purges global edge cache on Cloudflare."},
    {"name": " pagerduty_trigger_incident", "tier": "MUTATE", "domain": "monitoring", "keywords": ["pagerduty", "oncall", "wake", "page"], "schema_tokens": 115, "description": "Pages on-call engineer via PagerDuty."}
]

TOTAL_ALL_SCHEMA_TOKENS = sum(t["schema_tokens"] for t in MCP_TOOLS)  # ~2,870 tokens per step

# ---------------------------------------------------------------------------
# 2. PRESET WORKFLOW TEMPLATES (Realistic Multi-Step Agent Tasks)
# ---------------------------------------------------------------------------
WORKFLOW_TEMPLATES = {
    "incident_refund": {
        "title": "Billing Incident Triage & Auto-Refund",
        "goal": "Investigate checkout 500 error in Sentry, find duplicate Stripe charges in Postgres, stage a Linear root-cause ticket, and issue a $49 refund to affected customer.",
        "steps": [
            {
                "step_index": 0,
                "intent": "Fetch recent checkout 500 stack trace and error frequency from Sentry logs",
                "target_tool": "sentry_fetch_error_trace",
                "tier": "READ",
                "payload": {"service": "checkout-api", "window": "1h", "env": "prod"}
            },
            {
                "step_index": 1,
                "intent": "Query Postgres database and Stripe charges for duplicate billing on affected customer usr_9481",
                "target_tool": "stripe_list_charges",
                "tier": "READ",
                "payload": {"customer_id": "usr_9481", "status": "succeeded", "limit": 5}
            },
            {
                "step_index": 2,
                "intent": "Stage a draft engineering bug ticket in Linear with the stack trace and affected charge ID",
                "target_tool": "linear_create_draft_issue",
                "tier": "STAGE",
                "payload": {"team": "PAYMENTS", "title": "Fix race condition on webhook retry (ch_3P9x)", "priority": 1}
            },
            {
                "step_index": 3,
                "intent": "Execute an irreversible $49.00 refund on duplicate charge ch_3P9x via Stripe API",
                "target_tool": "stripe_issue_refund",
                "tier": "MUTATE",
                "payload": {"charge_id": "ch_3P9x", "amount_usd": 49.00, "reason": "duplicate_webhook_charge"}
            },
            {
                "step_index": 4,
                "intent": "Broadcast resolution summary to #incident-response channel on Slack",
                "target_tool": "slack_post_incident_alert",
                "tier": "MUTATE",
                "payload": {"channel": "#incident-response", "message": "Resolved duplicate charge ch_3P9x ($49 refunded) + Linear draft staged."}
            }
        ]
    },
    "zombie_accounts": {
        "title": "Scheduled Zombie Subscription Cleanup & DB Sync",
        "goal": "Scan Postgres for inactive enterprise trials, check HubSpot deal stage, draft customer follow-up in Gmail, and mutate subscription state in production DB.",
        "steps": [
            {
                "step_index": 0,
                "intent": "Query Postgres customer database for expired enterprise trial accounts with zero activity",
                "target_tool": "postgres_query_users",
                "tier": "READ",
                "payload": {"sql": "SELECT id, email FROM accounts WHERE plan='enterprise_trial' AND last_active < NOW() - INTERVAL '14 days'"}
            },
            {
                "step_index": 1,
                "intent": "Check HubSpot CRM deal stage for account ac_772 to verify no active contract pending",
                "target_tool": "hubspot_get_contact",
                "tier": "READ",
                "payload": {"account_id": "ac_772", "include_deals": True}
            },
            {
                "step_index": 2,
                "intent": "Stage a draft re-engagement email in Gmail before downgrading account",
                "target_tool": "gmail_draft_customer_apology",
                "tier": "STAGE",
                "payload": {"to": "ops@acme.io", "subject": "Your trial workspace is scheduled for archive", "offer_extension": True}
            },
            {
                "step_index": 3,
                "intent": "Mutate customer subscription tier in production Postgres database to free_archived",
                "target_tool": "postgres_update_subscription",
                "tier": "MUTATE",
                "payload": {"account_id": "ac_772", "new_tier": "free_archived", "revoke_api_keys": True}
            }
        ]
    }
}

# ---------------------------------------------------------------------------
# 3. DATABASE INITIALIZATION (SQLite Stateful Checkpoint Store)
# ---------------------------------------------------------------------------
def init_db():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("""
        CREATE TABLE IF NOT EXISTS task_runs (
            run_id TEXT PRIMARY KEY,
            title TEXT,
            goal TEXT,
            schema_pruning INTEGER,
            inject_failure_step INTEGER,
            status TEXT,
            current_step INTEGER,
            tokens_used INTEGER,
            tokens_saved_by_pruning INTEGER,
            tokens_saved_by_checkpoint INTEGER,
            created_at REAL
        )
    """)
    c.execute("""
        CREATE TABLE IF NOT EXISTS step_checkpoints (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id TEXT,
            step_index INTEGER,
            intent TEXT,
            tool_name TEXT,
            tier TEXT,
            payload_json TEXT,
            pruned_tools_json TEXT,
            schema_tokens_injected INTEGER,
            latency_ms INTEGER,
            status TEXT,
            output_json TEXT,
            error_msg TEXT,
            updated_at REAL,
            UNIQUE(run_id, step_index)
        )
    """)
    conn.commit()
    conn.close()

init_db()

# ---------------------------------------------------------------------------
# 4. DYNAMIC MCP SCHEMA-PRUNING ROUTER
# ---------------------------------------------------------------------------
def route_and_prune_schemas(intent: str, target_tool: str, prune_enabled: bool, top_k: int = 3) -> Dict[str, Any]:
    """
    Scores all 25 MCP tool definitions against the step's natural-language intent.
    If prune_enabled=True, injects only Top-K schemas (~340 tokens) instead of all 25 (~2,870 tokens).
    """
    if not prune_enabled:
        return {
            "selected_tools": [t["name"] for t in MCP_TOOLS],
            "tokens_injected": TOTAL_ALL_SCHEMA_TOKENS,
            "tokens_saved": 0,
            "simulated_latency_ms": 2150  # Heavy prompt prefill + high hallucination surface
        }

    words = set(re.findall(r"\w+", intent.lower()))
    scored = []
    for tool in MCP_TOOLS:
        score = sum(3 for kw in tool["keywords"] if kw in words)
        if tool["name"] == target_tool:
            score += 10  # Ensure target tool is ranked in Top-K
        scored.append((score, tool))

    scored.sort(key=lambda x: x[0], reverse=True)
    top_tools = [item[1] for item in scored[:top_k]]
    tokens_injected = sum(t["schema_tokens"] for t in top_tools)
    tokens_saved = TOTAL_ALL_SCHEMA_TOKENS - tokens_injected

    return {
        "selected_tools": [t["name"] for t in top_tools],
        "tokens_injected": tokens_injected,
        "tokens_saved": tokens_saved,
        "simulated_latency_ms": 580  # 1.57s faster step execution due to lean context
    }

# ---------------------------------------------------------------------------
# 5. ASYNC STEP EXECUTION ENGINE WITH CHECKPOINT PERSISTENCE
# ---------------------------------------------------------------------------
async def execute_workflow_from_checkpoint(run_id: str, start_step: int = 0):
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()

    run = c.execute("SELECT * FROM task_runs WHERE run_id = ?", (run_id,)).fetchone()
    if not run:
        conn.close()
        return

    prune_enabled = bool(run["schema_pruning"])
    inject_fail = run["inject_failure_step"]

    steps = c.execute(
        "SELECT * FROM step_checkpoints WHERE run_id = ? ORDER BY step_index ASC", (run_id,)
    ).fetchall()
    conn.close()

    for step in steps:
        idx = step["step_index"]
        if idx < start_step or step["status"] == "COMPLETED":
            continue

        # 1. Dynamic MCP Schema Routing
        routing = route_and_prune_schemas(step["intent"], step["tool_name"], prune_enabled)

        # 2. Check Risk Tier Guardrail BEFORE running MUTATE calls (unless already approved)
        if step["tier"] == "MUTATE" and step["status"] != "APPROVED_TO_RUN":
            _update_step(
                run_id, idx,
                status="AWAITING_APPROVAL",
                pruned_tools=routing["selected_tools"],
                schema_tokens=routing["tokens_injected"],
                latency_ms=0,
                output={"guardrail": "PAUSED_BY_TIER_3_POLICY", "reason": "Write/Mutate MCP call requires 1-click human approval."}
            )
            _update_run_status(run_id, "AWAITING_APPROVAL", idx)
            return  # Halt async workflow safely; state is checkpointed in SQLite!

        # 3. Mark step RUNNING
        _update_step(
            run_id, idx,
            status="RUNNING",
            pruned_tools=routing["selected_tools"],
            schema_tokens=routing["tokens_injected"],
            latency_ms=0
        )
        _update_run_status(run_id, "RUNNING", idx)

        await asyncio.sleep(routing["simulated_latency_ms"] / 1000.0)

        # 4. Check if user toggled simulated failure on this step to test Checkpoint Replay
        payload = json.loads(step["payload_json"])
        if inject_fail == idx and not payload.get("_fixed_by_user"):
            _update_step(
                run_id, idx,
                status="FAILED",
                pruned_tools=routing["selected_tools"],
                schema_tokens=routing["tokens_injected"],
                latency_ms=routing["simulated_latency_ms"],
                error_msg=f"MCP ToolTimeoutError on `{step['tool_name']}`: Invalid parameter schema or upstream 503. Edit checkpoint payload & replay from Step {idx + 1}."
            )
            _update_run_status(run_id, "FAILED_AT_CHECKPOINT", idx)
            return

        # 5. Execute Tool & Persist Checkpoint Output
        output_data = _simulate_mcp_tool_output(step["tool_name"], step["tier"], payload)
        _update_step(
            run_id, idx,
            status="COMPLETED",
            pruned_tools=routing["selected_tools"],
            schema_tokens=routing["tokens_injected"],
            latency_ms=routing["simulated_latency_ms"],
            output=output_data,
            error_msg=None
        )
        _increment_run_tokens(run_id, routing["tokens_injected"], routing["tokens_saved"])

    _update_run_status(run_id, "COMPLETED", len(steps) - 1)


def _simulate_mcp_tool_output(tool_name: str, tier: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "mcp_tool": tool_name,
        "risk_tier": tier,
        "executed_with_payload": payload,
        "result": "SUCCESS",
        "timestamp": time.strftime("%H:%M:%S UTC", time.gmtime()),
        "artifact": (
            "Draft staged in workspace buffer (no external side-effect)"
            if tier == "STAGE"
            else "State-changing API call committed after human guardrail approval"
            if tier == "MUTATE"
            else "Read-only query returned 2 matching records"
        )
    }


def _update_step(
    run_id: str,
    step_index: int,
    status: str,
    pruned_tools: List[str],
    schema_tokens: int,
    latency_ms: int,
    output: Optional[Dict] = None,
    error_msg: Optional[str] = None
):
    conn = sqlite3.connect(DB_PATH)
    conn.execute(
        """
        UPDATE step_checkpoints
        SET status = ?, pruned_tools_json = ?, schema_tokens_injected = ?,
            latency_ms = ?, output_json = ?, error_msg = ?, updated_at = ?
        WHERE run_id = ? AND step_index = ?
        """,
        (
            status,
            json.dumps(pruned_tools),
            schema_tokens,
            latency_ms,
            json.dumps(output) if output else None,
            error_msg,
            time.time(),
            run_id,
            step_index
        )
    )
    conn.commit()
    conn.close()


def _update_run_status(run_id: str, status: str, current_step: int):
    conn = sqlite3.connect(DB_PATH)
    conn.execute(
        "UPDATE task_runs SET status = ?, current_step = ? WHERE run_id = ?",
        (status, current_step, run_id)
    )
    conn.commit()
    conn.close()


def _increment_run_tokens(run_id: str, used: int, saved_pruning: int, saved_checkpoint: int = 0):
    conn = sqlite3.connect(DB_PATH)
    conn.execute(
        """
        UPDATE task_runs
        SET tokens_used = tokens_used + ?,
            tokens_saved_by_pruning = tokens_saved_by_pruning + ?,
            tokens_saved_by_checkpoint = tokens_saved_by_checkpoint + ?
        WHERE run_id = ?
        """,
        (used, saved_pruning, saved_checkpoint, run_id)
    )
    conn.commit()
    conn.close()


# ---------------------------------------------------------------------------
# 6. API ENDPOINTS
# ---------------------------------------------------------------------------
class StartRunRequest(BaseModel):
    template_key: str = "incident_refund"
    schema_pruning: bool = True
    inject_failure_step: int = 2  # Default: fails at Step 3 (index 2) to demonstrate Checkpoint Replay!

class ReplayStepRequest(BaseModel):
    step_index: int
    updated_payload: Dict[str, Any]

class ApproveStepRequest(BaseModel):
    step_index: int
    updated_payload: Optional[Dict[str, Any]] = None


@app.get("/", response_class=HTMLResponse)
async def serve_ui():
    with open("index.html", "r", encoding="utf-8") as f:
        return f.read()


@app.get("/api/tools")
async def get_mcp_tools():
    return {"total_tools": len(MCP_TOOLS), "total_unpruned_tokens": TOTAL_ALL_SCHEMA_TOKENS, "tools": MCP_TOOLS}


@app.post("/api/runs")
async def create_and_start_run(req: StartRunRequest):
    tpl = WORKFLOW_TEMPLATES.get(req.template_key, WORKFLOW_TEMPLATES["incident_refund"])
    run_id = f"run_{uuid.uuid4().hex[:8]}"

    conn = sqlite3.connect(DB_PATH)
    conn.execute(
        """
        INSERT INTO task_runs
        (run_id, title, goal, schema_pruning, inject_failure_step, status, current_step,
         tokens_used, tokens_saved_by_pruning, tokens_saved_by_checkpoint, created_at)
        VALUES (?, ?, ?, ?, ?, 'STARTING', 0, 0, 0, 0, ?)
        """,
        (run_id, tpl["title"], tpl["goal"], int(req.schema_pruning), req.inject_failure_step, time.time())
    )

    for s in tpl["steps"]:
        conn.execute(
            """
            INSERT INTO step_checkpoints
            (run_id, step_index, intent, tool_name, tier, payload_json, pruned_tools_json,
             schema_tokens_injected, latency_ms, status, output_json, error_msg, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, '[]', 0, 0, 'PENDING', NULL, NULL, ?)
            """,
            (run_id, s["step_index"], s["intent"], s["target_tool"], s["tier"], json.dumps(s["payload"]), time.time())
        )
    conn.commit()
    conn.close()

    asyncio.create_task(execute_workflow_from_checkpoint(run_id, start_step=0))
    return {"run_id": run_id}


@app.get("/api/runs/{run_id}")
async def get_run_state(run_id: str):
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    run = conn.execute("SELECT * FROM task_runs WHERE run_id = ?", (run_id,)).fetchone()
    if not run:
        conn.close()
        raise HTTPException(status_code=404, detail="Run not found")

    steps = conn.execute(
        "SELECT * FROM step_checkpoints WHERE run_id = ? ORDER BY step_index ASC", (run_id,)
    ).fetchall()
    conn.close()

    return {
        "run": dict(run),
        "steps": [
            {
                **dict(s),
                "payload": json.loads(s["payload_json"]),
                "pruned_tools": json.loads(s["pruned_tools_json"] or "[]"),
                "output": json.loads(s["output_json"]) if s["output_json"] else None
            }
            for s in steps
        ]
    }


@app.post("/api/runs/{run_id}/replay")
async def replay_from_checkpoint(run_id: str, req: ReplayStepRequest):
    """
    Edits a failed step's payload and resumes the workflow from that exact step_index
    WITHOUT re-running prior completed steps—calculating exact token spend saved.
    """
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    # Calculate tokens already spent in earlier completed steps that we AVOID re-running
    prior_tokens = conn.execute(
        "SELECT COALESCE(SUM(schema_tokens_injected), 0) as saved FROM step_checkpoints WHERE run_id = ? AND step_index < ?",
        (run_id, req.step_index)
    ).fetchone()["saved"]

    payload = req.updated_payload
    payload["_fixed_by_user"] = True

    conn.execute(
        """
        UPDATE step_checkpoints
        SET payload_json = ?, status = 'PENDING', error_msg = NULL
        WHERE run_id = ? AND step_index = ?
        """,
        (json.dumps(payload), run_id, req.step_index)
    )
    conn.commit()
    conn.close()

    _increment_run_tokens(run_id, used=0, saved_pruning=0, saved_checkpoint=prior_tokens)
    asyncio.create_task(execute_workflow_from_checkpoint(run_id, start_step=req.step_index))
    return {"resumed_from_step": req.step_index, "tokens_saved_by_checkpoint": prior_tokens}


@app.post("/api/runs/{run_id}/approve")
async def approve_mutate_step(run_id: str, req: ApproveStepRequest):
    """
    Approves a Tier-3 MUTATE tool call (with optional payload edits) and resumes async execution.
    """
    conn = sqlite3.connect(DB_PATH)
    if req.updated_payload is not None:
        conn.execute(
            "UPDATE step_checkpoints SET payload_json = ?, status = 'APPROVED_TO_RUN' WHERE run_id = ? AND step_index = ?",
            (json.dumps(req.updated_payload), run_id, req.step_index)
        )
    else:
        conn.execute(
            "UPDATE step_checkpoints SET status = 'APPROVED_TO_RUN' WHERE run_id = ? AND step_index = ?",
            (run_id, req.step_index)
        )
    conn.commit()
    conn.close()

    asyncio.create_task(execute_workflow_from_checkpoint(run_id, start_step=req.step_index))
    return {"approved_step": req.step_index}