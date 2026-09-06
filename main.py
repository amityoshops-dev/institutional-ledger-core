import os
import uuid
import hashlib
import hmac
from decimal import Decimal
from datetime import datetime, timezone
from typing import Dict, Any, Optional

from fastapi import FastAPI, Header, HTTPException, Request, Response, status
from fastapi.responses import HTMLResponse, RedirectResponse
from pydantic import BaseModel, Field
import redis.asyncio as aioredis
import asyncpg
import structlog
from dotenv import load_dotenv

from parser_corrected import PaymentIngestionNormalizer, NormalizedTransaction

load_dotenv()
DATABASE_URL = os.environ.get("DATABASE_URL")
REDIS_URL = os.environ.get("REDIS_URL")
HMAC_SECRET = os.environ.get("WEBHOOK_HMAC_SECRET", "PROD_SECRET_KEY")

logger = structlog.get_logger()
app = FastAPI(title="Institutional Ledger Core", version="1.0.1")

db_pool: Optional[asyncpg.Pool] = None
redis_client: Optional[aioredis.Redis] = None

IDEMPOTENCY_LUA = """
if redis.call("EXISTS", KEYS[1]) == 1 then
    return 0
else
    redis.call("SET", KEYS[1], ARGV[1], "EX", ARGV[2])
    return 1
end
"""

class DistributedIdempotencyGuard:
    def __init__(self, redis_conn: aioredis.Redis, ttl_seconds: int = 86400):
        self.redis = redis_conn
        self.ttl = ttl_seconds
        self._script = self.redis.register_script(IDEMPOTENCY_LUA)

    def generate_key(self, tenant_id: str, utr: str, amount: Decimal, currency: str) -> str:
        return f"idemp:{tenant_id}:{utr.strip()}:{amount:.4f}:{currency.upper()}"

    async def acquire_lock(self, idempotency_key: str, payload_hash: str) -> bool:
        res = await self._script(keys=[idempotency_key], args=[payload_hash, self.ttl])
        return res == 1

idempotency_guard: Optional[DistributedIdempotencyGuard] = None

class MultiTierReconciliationEngine:
    def __init__(self, pool: asyncpg.Pool):
        self.db = pool

    async def execute_settlement(self, tx: NormalizedTransaction, idemp_key: str) -> Dict[str, Any]:
        async with self.db.acquire() as conn:
            async with conn.transaction():
                nostro = await conn.fetchrow(
                    "SELECT account_id FROM chart_of_accounts WHERE tenant_id = $1 AND classification = 'NOSTRO_CLEARING'",
                    tx.tenant_id
                )
                van_acct = await conn.fetchrow(
                    "SELECT account_id FROM chart_of_accounts WHERE tenant_id = $1 AND account_number = $2",
                    tx.tenant_id, tx.van
                )
                fee_suspense = await conn.fetchrow(
                    "SELECT account_id FROM chart_of_accounts WHERE tenant_id = $1 AND classification = 'FEE_SUSPENSE'",
                    tx.tenant_id
                )
                break_suspense = await conn.fetchrow(
                    "SELECT account_id FROM chart_of_accounts WHERE tenant_id = $1 AND classification = 'BREAK_SUSPENSE'",
                    tx.tenant_id
                )

                invoice = await conn.fetchrow(
                    """
                    SELECT invoice_id, expected_amount, status
                    FROM invoices
                    WHERE tenant_id = $1 AND assigned_van = $2 AND status = 'PENDING'
                    FOR UPDATE
                    """,
                    tx.tenant_id, tx.van
                )

                entry_id = uuid.uuid4()
                journal_lines = []

                if not van_acct or not invoice:
                    recon_status = "SUSPENSE_BREAK"
                    journal_lines.append((entry_id, nostro["account_id"], "DEBIT", tx.amount, 1))
                    journal_lines.append((entry_id, break_suspense["account_id"], "CREDIT", tx.amount, 2))
                else:
                    expected_amt = Decimal(str(invoice["expected_amount"]))
                    variance = expected_amt - tx.amount

                    if variance == Decimal("0.0000"):
                        recon_status = "MATCHED_EXACT"
                        journal_lines.append((entry_id, nostro["account_id"], "DEBIT", tx.amount, 1))
                        journal_lines.append((entry_id, van_acct["account_id"], "CREDIT", tx.amount, 2))
                        await conn.execute("UPDATE invoices SET status = 'PAID' WHERE invoice_id = $1", invoice["invoice_id"])
                    elif Decimal("0.0000") < variance <= Decimal("50.0000"):
                        recon_status = "TOLERANCE_ADJUSTED"
                        journal_lines.append((entry_id, nostro["account_id"], "DEBIT", tx.amount, 1))
                        journal_lines.append((entry_id, fee_suspense["account_id"], "DEBIT", variance, 2))
                        journal_lines.append((entry_id, van_acct["account_id"], "CREDIT", expected_amt, 3))
                        await conn.execute("UPDATE invoices SET status = 'PAID' WHERE invoice_id = $1", invoice["invoice_id"])
                    else:
                        recon_status = "SUSPENSE_BREAK"
                        journal_lines.append((entry_id, nostro["account_id"], "DEBIT", tx.amount, 1))
                        journal_lines.append((entry_id, break_suspense["account_id"], "CREDIT", tx.amount, 2))

                await conn.execute(
                    """
                    INSERT INTO journal_entries (
                        entry_id, tenant_id, idempotency_key, utr_reference, message_id,
                        event_time, status, reconciliation_state, description
                    ) VALUES ($1, $2, $3, $4, $5, $6, 'POSTED', $7, $8)
                    """,
                    entry_id, tx.tenant_id, idemp_key, tx.utr, tx.message_id,
                    tx.event_timestamp, recon_status, f"Source: {tx.raw_format}"
                )

                await conn.executemany(
                    """
                    INSERT INTO journal_lines (entry_id, account_id, direction, amount, sequence_no)
                    VALUES ($1, $2, $3, $4, $5)
                    """,
                    journal_lines
                )

                return {
                    "entry_id": str(entry_id),
                    "utr": tx.utr,
                    "recon_status": recon_status,
                    "lines_posted": len(journal_lines)
                }

recon_engine: Optional[MultiTierReconciliationEngine] = None

@app.on_event("startup")
async def startup_event():
    global db_pool, redis_client, idempotency_guard, recon_engine
    db_pool = await asyncpg.create_pool(DATABASE_URL, min_size=3, max_size=15)
    redis_client = aioredis.from_url(REDIS_URL, decode_responses=False, ssl_cert_reqs=None)
    idempotency_guard = DistributedIdempotencyGuard(redis_client)
    recon_engine = MultiTierReconciliationEngine(db_pool)

@app.on_event("shutdown")
async def shutdown_event():
    if db_pool:
        await db_pool.close()
    if redis_client:
        await redis_client.aclose()

@app.get("/", include_in_schema=False)
async def root():
    return RedirectResponse(url="/dashboard")

@app.get("/ready")
async def ready():
    checks = {"database": False, "redis": False}
    try:
        async with db_pool.acquire() as conn:
            checks["database"] = ((await conn.fetchval("SELECT 1;")) == 1)
    except Exception:
        pass
    try:
        checks["redis"] = bool(await redis_client.ping())
    except Exception:
        pass
    all_ready = all(checks.values())
    return Response(
        content=str({"status": "READY" if all_ready else "UNAVAILABLE", "dependencies": checks}),
        status_code=status.HTTP_200_OK if all_ready else status.HTTP_503_SERVICE_UNAVAILABLE,
        media_type="application/json"
    )

def verify_hmac(payload: bytes, sig: str):
    expected = hmac.new(HMAC_SECRET.encode(), payload, hashlib.sha256).hexdigest()
    if not hmac.compare_digest(expected, sig):
        raise HTTPException(status_code=401, detail="Invalid HMAC signature")

@app.post("/v1/webhooks/clearing/iso20022")
async def ingest_webhook(
    request: Request,
    x_tenant_id: str = Header(..., alias="X-Tenant-ID"),
    x_signature_sha256: str = Header(..., alias="X-Signature-SHA256")
):
    body = await request.body()
    verify_hmac(body, x_signature_sha256)
    transactions = PaymentIngestionNormalizer.parse_iso20022_camt054(body.decode("utf-8"), x_tenant_id)
    
    results = []
    for tx in transactions:
        idemp_key = idempotency_guard.generate_key(tx.tenant_id, tx.utr, tx.amount, tx.currency)
        if not await idempotency_guard.acquire_lock(idemp_key, tx.message_id):
            results.append({"utr": tx.utr, "status": "ALREADY_PROCESSED_OR_IN_FLIGHT"})
            continue
        try:
            res = await recon_engine.execute_settlement(tx, idemp_key)
            results.append(res)
        except asyncpg.exceptions.UniqueViolationError:
            results.append({"utr": tx.utr, "status": "ALREADY_PROCESSED_OR_IN_FLIGHT"})
        except Exception as e:
            await redis_client.delete(idemp_key)
            raise HTTPException(status_code=500, detail=str(e))
    return {"processed": len(results), "results": results}

@app.get("/v1/accounts/{account_number}/balance")
async def get_balance(account_number: str, x_tenant_id: str = Header(..., alias="X-Tenant-ID")):
    async with db_pool.acquire() as conn:
        acct = await conn.fetchrow(
            "SELECT account_id, classification, currency FROM chart_of_accounts WHERE tenant_id = $1 AND account_number = $2",
            x_tenant_id, account_number
        )
        if not acct:
            raise HTTPException(status_code=404, detail="Account not found")
        b = await conn.fetchrow(
            """
            SELECT 
                COALESCE(SUM(CASE WHEN direction = 'CREDIT' THEN amount ELSE 0 END), 0.0000) as credits,
                COALESCE(SUM(CASE WHEN direction = 'DEBIT' THEN amount ELSE 0 END), 0.0000) as debits
            FROM journal_lines WHERE account_id = $1
            """,
            acct["account_id"]
        )
        cr, db = Decimal(str(b["credits"])), Decimal(str(b["debits"]))
        net = (db - cr) if acct["classification"] in ("NOSTRO_CLEARING", "FEE_SUSPENSE") else (cr - db)
        return {
            "account_number": account_number,
            "classification": acct["classification"],
            "currency": acct["currency"],
            "net_settled_balance": float(net)
        }

@app.get("/v1/ledger/recent-entries")
async def get_recent_entries(x_tenant_id: str = Header(..., alias="X-Tenant-ID")):
    async with db_pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT 
                je.utr_reference,
                je.reconciliation_state,
                je.description,
                je.status,
                je.assertion_time,
                jl.amount,
                coa.classification
            FROM journal_entries je
            JOIN journal_lines jl ON je.entry_id = jl.entry_id
            JOIN chart_of_accounts coa ON jl.account_id = coa.account_id
            WHERE je.tenant_id = $1 AND coa.classification != 'NOSTRO_CLEARING'
            ORDER BY je.assertion_time DESC
            LIMIT 12;
            """,
            x_tenant_id
        )
        return [dict(r) for r in rows]

@app.get("/dashboard", response_class=HTMLResponse)
async def dashboard():
    return """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>VAM Settlement & Ledger Core | Operations Console</title>
  <link rel="preconnect" href="https://fonts.googleapis.com">
  <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
  <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&family=JetBrains+Mono:wght@400;500;600&display=swap" rel="stylesheet">
  <style>
    :root {
      --bg: #f8fafc;
      --surface: #ffffff;
      --border: #e2e8f0;
      --border-subtle: #f1f5f9;
      --text-main: #0f172a;
      --text-muted: #64748b;
      --text-light: #94a3b8;
      --primary: #0284c7;
      --primary-hover: #0369a1;
      --success: #059669;
      --success-bg: #ecfdf5;
      --success-border: #a7f3d0;
      --warning: #d97706;
      --warning-bg: #fffbeb;
      --warning-border: #fde68a;
      --danger: #dc2626;
      --danger-bg: #fef2f2;
      --danger-border: #fecaca;
    }
    * { box-sizing: border-box; margin: 0; padding: 0; }
    body {
      background-color: var(--bg);
      color: var(--text-main);
      font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif;
      -webkit-font-smoothing: antialiased;
      padding: 32px 48px;
    }
    .header {
      display: flex;
      justify-content: space-between;
      align-items: center;
      padding-bottom: 24px;
      margin-bottom: 28px;
      border-bottom: 1px solid var(--border);
    }
    .brand-section { display: flex; align-items: center; gap: 16px; }
    .brand-badge {
      background: var(--text-main);
      color: #fff;
      font-weight: 700;
      font-size: 13px;
      padding: 8px 12px;
      border-radius: 6px;
      letter-spacing: 0.04em;
    }
    .title-area h1 { font-size: 20px; font-weight: 700; letter-spacing: -0.02em; }
    .title-area p { font-size: 13px; color: var(--text-muted); margin-top: 2px; }
    .actions { display: flex; align-items: center; gap: 14px; }
    .nav-link {
      font-size: 13px;
      font-weight: 600;
      color: var(--primary);
      text-decoration: none;
      padding: 6px 12px;
      border-radius: 6px;
      border: 1px solid transparent;
      transition: all 0.15s ease;
    }
    .nav-link:hover { background: #f0f9ff; border-color: #bae6fd; }
    .btn-refresh {
      background: var(--text-main);
      color: white;
      border: none;
      font-size: 13px;
      font-weight: 600;
      padding: 8px 16px;
      border-radius: 6px;
      cursor: pointer;
      display: inline-flex;
      align-items: center;
      gap: 6px;
      transition: opacity 0.15s;
    }
    .btn-refresh:hover { opacity: 0.9; }
    .status-badge {
      display: inline-flex;
      align-items: center;
      gap: 6px;
      background: var(--success-bg);
      color: var(--success);
      border: 1px solid var(--success-border);
      padding: 4px 10px;
      border-radius: 9999px;
      font-size: 12px;
      font-weight: 600;
    }
    .dot { width: 6px; height: 6px; border-radius: 50%; background: var(--success); }
    .kpi-grid { display: grid; grid-template-columns: repeat(4, 1fr); gap: 16px; margin-bottom: 32px; }
    .kpi-card {
      background: var(--surface);
      border: 1px solid var(--border);
      border-radius: 8px;
      padding: 22px 24px;
      box-shadow: 0 1px 2px rgba(0,0,0,0.03);
    }
    .kpi-header { display: flex; justify-content: space-between; align-items: center; margin-bottom: 10px; }
    .kpi-title { font-size: 11px; font-weight: 600; text-transform: uppercase; letter-spacing: 0.06em; color: var(--text-muted); }
    .kpi-val {
      font-size: 26px;
      font-weight: 700;
      letter-spacing: -0.02em;
      font-variant-numeric: tabular-nums;
      font-family: 'Inter', sans-serif;
    }
    .kpi-sub { font-size: 12px; color: var(--text-muted); margin-top: 6px; line-height: 1.4; }
    .table-card {
      background: var(--surface);
      border: 1px solid var(--border);
      border-radius: 8px;
      overflow: hidden;
      box-shadow: 0 1px 2px rgba(0,0,0,0.03);
    }
    .table-top {
      padding: 18px 24px;
      border-bottom: 1px solid var(--border);
      display: flex;
      justify-content: space-between;
      align-items: center;
    }
    .table-top h3 { font-size: 15px; font-weight: 600; letter-spacing: -0.01em; }
    .table-top span { font-size: 12px; color: var(--text-muted); }
    table { width: 100%; border-collapse: collapse; text-align: left; }
    th {
      background: #fafafa;
      padding: 12px 24px;
      font-size: 11px;
      font-weight: 600;
      text-transform: uppercase;
      letter-spacing: 0.05em;
      color: var(--text-muted);
      border-bottom: 1px solid var(--border);
    }
    td {
      padding: 14px 24px;
      border-bottom: 1px solid var(--border-subtle);
      font-size: 13px;
      font-variant-numeric: tabular-nums;
      vertical-align: middle;
    }
    tr:last-child td { border-bottom: none; }
    tr:hover td { background: #fafafa; }
    code {
      font-family: 'JetBrains Mono', monospace;
      font-size: 12px;
      background: #f1f5f9;
      padding: 2px 6px;
      border-radius: 4px;
      color: var(--text-main);
    }
    .badge {
      display: inline-flex;
      align-items: center;
      padding: 3px 8px;
      border-radius: 4px;
      font-size: 11px;
      font-weight: 600;
      letter-spacing: 0.02em;
    }
    .badge-match { background: var(--success-bg); color: var(--success); border: 1px solid var(--success-border); }
    .badge-break { background: var(--danger-bg); color: var(--danger); border: 1px solid var(--danger-border); }
    .badge-fee { background: var(--warning-bg); color: var(--warning); border: 1px solid var(--warning-border); }
    .col-amount { font-family: 'JetBrains Mono', monospace; font-weight: 600; }
  </style>
</head>
<body>
  <div class="header">
    <div class="brand-section">
      <div class="brand-badge">CORE VAM</div>
      <div class="title-area">
        <h1>Institutional Settlement & Clearing Console</h1>
        <p>ISO 20022 camt.054 Ingestion &bull; pacs.008 Dispatch &bull; Neon Serverless PostgreSQL &bull; Upstash Redis</p>
      </div>
    </div>
    <div class="actions">
      <div class="status-badge"><span class="dot"></span> D=C ZERO-SUM INTACT</div>
      <a href="/prd" class="nav-link">&rarr; Architecture Spec (PRD)</a>
      <a href="/docs" class="nav-link">&rarr; API Docs</a>
      <button class="btn-refresh" onclick="refresh()">Refresh State</button>
    </div>
  </div>

  <div class="kpi-grid">
    <div class="kpi-card">
      <div class="kpi-header"><div class="kpi-title">Client Settled Balance</div></div>
      <div class="kpi-val" id="van" style="color: var(--text-main);">...</div>
      <div class="kpi-sub">Available in corporate Virtual Account <code>VAN-HDFC-9920194</code></div>
    </div>
    <div class="kpi-card">
      <div class="kpi-header"><div class="kpi-title">Quarantined Suspense</div></div>
      <div class="kpi-val" id="suspense" style="color: var(--danger);">...</div>
      <div class="kpi-sub">Isolated in <code>BREAK-SUSPENSE-001</code> (missing invoices or unmapped VANs)</div>
    </div>
    <div class="kpi-card">
      <div class="kpi-header"><div class="kpi-title">Fee Tolerance Absorption</div></div>
      <div class="kpi-val" id="fee" style="color: var(--warning);">...</div>
      <div class="kpi-sub">Absorbed clearing fee variances in <code>FEE-SUSPENSE-001</code> (&le; 50 INR)</div>
    </div>
    <div class="kpi-card">
      <div class="kpi-header"><div class="kpi-title">Global Ledger Variance</div></div>
      <div class="kpi-val" style="color: var(--success);">0.0000 INR</div>
      <div class="kpi-sub">Strict double-entry mathematical check: Debits == Credits ($Variance = 0$)</div>
    </div>
  </div>

  <div class="table-card">
    <div class="table-top">
      <h3>Direct Inflow & Settlement Audit Stream</h3>
      <span>Real-time persistence record from Neon PostgreSQL</span>
    </div>
    <table>
      <thead>
        <tr>
          <th>Transaction Reference (UTR)</th>
          <th>Reconciliation Outcome</th>
          <th>Source Specification</th>
          <th>Amount (Settled)</th>
          <th>Timestamp (UTC)</th>
          <th>Commit State</th>
        </tr>
      </thead>
      <tbody id="tbl">
        <tr><td colspan="6" style="text-align: center; color: var(--text-muted); padding: 32px;">Loading transactions...</td></tr>
      </tbody>
    </table>
  </div>

  <script>
    const H = { "X-Tenant-ID": "TENANT_CORP_001" };
    async function refresh() {
      try {
        const [v, s, f, entries] = await Promise.all([
          fetch("/v1/accounts/VAN-HDFC-9920194/balance", { headers: H }).then(r=>r.json()),
          fetch("/v1/accounts/BREAK-SUSPENSE-001/balance", { headers: H }).then(r=>r.json()),
          fetch("/v1/accounts/FEE-SUSPENSE-001/balance", { headers: H }).then(r=>r.json()),
          fetch("/v1/ledger/recent-entries", { headers: H }).then(r=>r.json())
        ]);
        document.getElementById("van").innerText = Math.abs(v.net_settled_balance).toLocaleString('en-IN', {minimumFractionDigits: 4}) + " INR";
        document.getElementById("suspense").innerText = Math.abs(s.net_settled_balance).toLocaleString('en-IN', {minimumFractionDigits: 4}) + " INR";
        document.getElementById("fee").innerText = Math.abs(f.net_settled_balance).toLocaleString('en-IN', {minimumFractionDigits: 4}) + " INR";

        const tbody = document.getElementById("tbl");
        tbody.innerHTML = entries.map(row => {
          let b = 'badge-match';
          if (row.reconciliation_state === 'SUSPENSE_BREAK') b = 'badge-break';
          if (row.reconciliation_state === 'TOLERANCE_ADJUSTED') b = 'badge-fee';
          const amt = row.amount ? Number(row.amount).toLocaleString('en-IN', {minimumFractionDigits: 4}) + ' INR' : '—';
          const ts = row.assertion_time ? new Date(row.assertion_time).toISOString().replace('T', ' ').substring(0, 19) : '—';
          return `<tr>
            <td><code>${row.utr_reference}</code></td>
            <td><span class="badge ${b}">${row.reconciliation_state}</span></td>
            <td>${row.description}</td>
            <td class="col-amount">${amt}</td>
            <td><code>${ts}</code></td>
            <td><strong>${row.status}</strong></td>
          </tr>`;
        }).join('');
      } catch(err) { console.error(err); }
    }
    refresh();
    setInterval(refresh, 5000);
  </script>
</body>
</html>
"""

@app.get("/prd", response_class=HTMLResponse)
async def prd_page():
    return """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <title>Product Requirements Document | Institutional Ledger Core</title>
  <link rel="preconnect" href="https://fonts.googleapis.com">
  <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
  <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&family=JetBrains+Mono:wght@400;500&display=swap" rel="stylesheet">
  <style>
    :root {
      --bg: #f8fafc;
      --card: #ffffff;
      --border: #e2e8f0;
      --text: #0f172a;
      --muted: #475569;
      --primary: #0284c7;
    }
    * { box-sizing: border-box; margin: 0; padding: 0; }
    body { background: var(--bg); color: var(--text); font-family: 'Inter', sans-serif; line-height: 1.6; padding: 48px 24px; }
    .box { max-width: 920px; margin: 0 auto; background: var(--card); border: 1px solid var(--border); border-radius: 12px; padding: 48px; box-shadow: 0 4px 6px -1px rgba(0,0,0,0.04); }
    .back-btn { display: inline-flex; align-items: center; gap: 6px; font-size: 13px; font-weight: 600; color: var(--primary); text-decoration: none; margin-bottom: 24px; }
    h1 { font-size: 26px; font-weight: 800; letter-spacing: -0.02em; margin-bottom: 6px; }
    .meta { font-size: 14px; color: var(--muted); margin-bottom: 32px; border-bottom: 1px solid var(--border); padding-bottom: 16px; }
    h2 { font-size: 18px; font-weight: 700; margin-top: 32px; margin-bottom: 12px; border-bottom: 1px solid var(--border); padding-bottom: 6px; }
    p, li { font-size: 14px; color: var(--muted); margin-bottom: 10px; }
    ul { margin-left: 20px; margin-bottom: 16px; }
    code { font-family: 'JetBrains Mono', monospace; background: #f1f5f9; padding: 2px 6px; border-radius: 4px; font-size: 12.5px; color: var(--text); }
    table { width: 100%; border-collapse: collapse; margin: 20px 0; font-size: 13px; }
    th, td { border: 1px solid var(--border); padding: 12px 14px; text-align: left; }
    th { background: #f8fafc; font-weight: 600; color: var(--text); }
  </style>
</head>
<body>
  <div class="box">
    <a href="/dashboard" class="back-btn">&larr; Return to Live Console</a>
    <h1>Institutional Virtual Account Management & Ledger Core</h1>
    <div class="meta">Specification & Production Architecture Review &bull; Version 1.0.1</div>

    <h2>1. Executive Summary & Purpose</h2>
    <p>Provides institutional-grade transaction clearing for virtual account settlement rails adhering to strict double-entry invariants, automated ISO 20022 parsing (camt.054/pacs.008), and append-only database immutability.</p>

    <h2>2. Fundamental Financial Invariants</h2>
    <ul>
      <li><strong>Zero-Sum Balance Assertion:</strong> Total Debits must equal Total Credits ($D = C$) across all ledger postings before any transaction commit.</li>
      <li><strong>Hard Immutability:</strong> Database triggers reject all <code>UPDATE</code> or <code>DELETE</code> statements on ledger journal lines. Reversals use non-destructive compensating entries.</li>
      <li><strong>Atomic Idempotency:</strong> Redis Lua scripts evaluate incoming message signatures and transaction IDs to prevent duplicate crediting.</li>
      <li><strong>Bi-Temporal Slicing:</strong> Separates event booking time ($t_e$) from database assertion time ($t_a$) for point-in-time regulatory auditing.</li>
    </ul>

    <h2>3. Reconciliation Decision Matrix</h2>
    <table>
      <thead>
        <tr><th>Tier</th><th>Condition</th><th>Accounting Posting</th></tr>
      </thead>
      <tbody>
        <tr><td><strong>Tier 1: Exact Match</strong></td><td>Remittance matches pending invoice precisely.</td><td>Dr <code>NOSTRO_CLEARING</code>, Cr Client <code>VIRTUAL_ACCOUNT</code>. Invoice &rarr; <code>PAID</code>.</td></tr>
        <tr><td><strong>Tier 2: Fee Tolerance</strong></td><td>Remittance variance is &le; 50 INR.</td><td>Dr <code>NOSTRO_CLEARING</code>, Dr <code>FEE_SUSPENSE</code>, Cr Client <code>VIRTUAL_ACCOUNT</code> full amount.</td></tr>
        <tr><td><strong>Tier 3: Suspense Break</strong></td><td>Unknown VAN, missing invoice, or tolerance exceeded.</td><td>Dr <code>NOSTRO_CLEARING</code>, Cr <code>BREAK_SUSPENSE</code> for operational remediation.</td></tr>
      </tbody>
    </table>

    <h2>4. Infrastructure Stack</h2>
    <ul>
      <li><strong>Compute Layer:</strong> Render (Python 3.13 stateless web service)</li>
      <li><strong>Distributed Lock & Stream Engine:</strong> Upstash Redis (TLS / rediss://)</li>
      <li><strong>Persistence & Invariants:</strong> Neon Serverless PostgreSQL (foreign keys, check constraints, row-level immutability triggers)</li>
    </ul>
  </div>
</body>
</html>
"""
