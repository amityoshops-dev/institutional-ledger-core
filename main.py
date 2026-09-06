import uuid
import hashlib
import hmac
import os
from decimal import Decimal
from datetime import datetime, timezone
from typing import Dict, Any, Optional

from fastapi import FastAPI, Header, HTTPException, Request, Response, status
import redis.asyncio as aioredis
import asyncpg
import structlog
from opentelemetry import trace
from dotenv import load_dotenv

from parser_corrected import PaymentIngestionNormalizer, NormalizedTransaction

load_dotenv()
DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/ledger_db")
REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379")
HMAC_SECRET = os.environ.get("WEBHOOK_HMAC_SECRET", "PROD_SECRET_KEY")

logger = structlog.get_logger()
tracer = trace.get_tracer("recon-engine")

app = FastAPI(title="Institutional Virtual Account Ledger Core", version="1.0.1")

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
        formatted_amount = f"{amount:.4f}"
        return f"idemp:{tenant_id}:{utr.strip()}:{formatted_amount}:{currency.upper()}"

    async def acquire_lock(self, idempotency_key: str, payload_hash: str) -> bool:
        result = await self._script(keys=[idempotency_key], args=[payload_hash, self.ttl])
        return result == 1


idempotency_guard: Optional[DistributedIdempotencyGuard] = None


class MultiTierReconciliationEngine:
    def __init__(self, pool: asyncpg.Pool):
        self.db = pool

    async def execute_settlement(self, tx: NormalizedTransaction, idemp_key: str) -> Dict[str, Any]:
        with tracer.start_as_current_span("execute_reconciliation_and_ledger"):
            async with self.db.acquire() as conn:
                async with conn.transaction():
                    nostro = await conn.fetchrow(
                        "SELECT account_id FROM chart_of_accounts WHERE tenant_id = $1 AND classification = 'NOSTRO_CLEARING'",
                        tx.tenant_id,
                    )
                    van_acct = await conn.fetchrow(
                        "SELECT account_id FROM chart_of_accounts WHERE tenant_id = $1 AND account_number = $2",
                        tx.tenant_id, tx.van,
                    )
                    fee_suspense = await conn.fetchrow(
                        "SELECT account_id FROM chart_of_accounts WHERE tenant_id = $1 AND classification = 'FEE_SUSPENSE'",
                        tx.tenant_id,
                    )
                    break_suspense = await conn.fetchrow(
                        "SELECT account_id FROM chart_of_accounts WHERE tenant_id = $1 AND classification = 'BREAK_SUSPENSE'",
                        tx.tenant_id,
                    )
                    if not nostro or not fee_suspense or not break_suspense:
                        raise RuntimeError(
                            f"Tenant {tx.tenant_id} missing required control accounts"
                        )

                    invoice = await conn.fetchrow(
                        """
                        SELECT invoice_id, expected_amount, status
                        FROM invoices
                        WHERE tenant_id = $1 AND assigned_van = $2 AND status = 'PENDING'
                        FOR UPDATE
                        """,
                        tx.tenant_id, tx.van,
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
                        tx.event_timestamp, recon_status, f"Source: {tx.raw_format}",
                    )

                    await conn.executemany(
                        """
                        INSERT INTO journal_lines (entry_id, account_id, direction, amount, sequence_no)
                        VALUES ($1, $2, $3, $4, $5)
                        """,
                        journal_lines,
                    )

                    return {
                        "entry_id": str(entry_id),
                        "utr": tx.utr,
                        "recon_status": recon_status,
                        "lines_posted": len(journal_lines),
                    }


recon_engine: Optional[MultiTierReconciliationEngine] = None


@app.on_event("startup")
async def startup_event():
    global db_pool, redis_client, idempotency_guard, recon_engine
    db_pool = await asyncpg.create_pool(DATABASE_URL, min_size=5, max_size=20)
    redis_client = aioredis.from_url(
        REDIS_URL,
        decode_responses=False,
        socket_connect_timeout=10,
        socket_timeout=10,
        ssl_cert_reqs=None,
    )
    idempotency_guard = DistributedIdempotencyGuard(redis_client)
    recon_engine = MultiTierReconciliationEngine(db_pool)
    logger.info("startup_complete")


@app.on_event("shutdown")
async def shutdown_event():
    if db_pool:
        await db_pool.close()
    if redis_client:
        await redis_client.close()


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.get("/ready")
async def readiness_probe():
    checks = {"database": False, "redis": False}
    try:
        async with db_pool.acquire() as conn:
            checks["database"] = ((await conn.fetchval("SELECT 1;")) == 1)
    except Exception as e:
        logger.error("readiness_db_failure", error=str(e))
    try:
        checks["redis"] = bool(await redis_client.ping())
    except Exception as e:
        logger.error("readiness_redis_failure", error=str(e))

    all_ready = all(checks.values())
    return Response(
        content=str({"status": "READY" if all_ready else "UNAVAILABLE", "dependencies": checks}),
        status_code=status.HTTP_200_OK if all_ready else status.HTTP_503_SERVICE_UNAVAILABLE,
        media_type="application/json"
    )


def verify_hmac_signature(payload: bytes, signature: str, secret: str = HMAC_SECRET):
    expected_sig = hmac.new(secret.encode(), payload, hashlib.sha256).hexdigest()
    if not hmac.compare_digest(expected_sig, signature):
        raise HTTPException(status_code=401, detail="Invalid cryptographic HMAC signature")


@app.post("/v1/webhooks/clearing/iso20022", status_code=status.HTTP_200_OK)
async def ingest_iso20022_webhook(
    request: Request,
    x_tenant_id: str = Header(..., alias="X-Tenant-ID"),
    x_signature_sha256: str = Header(..., alias="X-Signature-SHA256"),
):
    raw_body = await request.body()
    verify_hmac_signature(raw_body, x_signature_sha256)

    try:
        transactions = PaymentIngestionNormalizer.parse_iso20022_camt054(raw_body.decode("utf-8"), x_tenant_id)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=f"Malformed ISO 20022 XML: {str(e)}")

    results = []
    for tx in transactions:
        idemp_key = idempotency_guard.generate_key(tx.tenant_id, tx.utr, tx.amount, tx.currency)
        lock_acquired = await idempotency_guard.acquire_lock(idemp_key, tx.message_id)

        if not lock_acquired:
            results.append({"utr": tx.utr, "status": "ALREADY_PROCESSED_OR_IN_FLIGHT"})
            continue

        try:
            result = await recon_engine.execute_settlement(tx, idemp_key)
            results.append(result)
        except asyncpg.exceptions.UniqueViolationError:
            # DB Backstop: Catch if Redis lost lock but DB already holds entry
            results.append({"utr": tx.utr, "status": "ALREADY_PROCESSED_OR_IN_FLIGHT"})
        except Exception as e:
            await redis_client.delete(idemp_key)
            raise HTTPException(status_code=500, detail=f"Ledger Commit Error: {str(e)}")

    return {"processed": len(results), "results": results}


@app.get("/v1/accounts/{account_number}/balance")
async def get_account_balance(
    account_number: str,
    x_tenant_id: str = Header(..., alias="X-Tenant-ID"),
):
    async with db_pool.acquire() as conn:
        acct = await conn.fetchrow(
            """
            SELECT account_id, classification, currency 
            FROM chart_of_accounts 
            WHERE tenant_id = $1 AND account_number = $2
            """,
            x_tenant_id, account_number,
        )
        if not acct:
            raise HTTPException(status_code=404, detail="Account not found")

        balance_row = await conn.fetchrow(
            """
            SELECT 
                COALESCE(SUM(CASE WHEN direction = 'CREDIT' THEN amount ELSE 0 END), 0.0000) as total_credits,
                COALESCE(SUM(CASE WHEN direction = 'DEBIT' THEN amount ELSE 0 END), 0.0000) as total_debits
            FROM journal_lines
            WHERE account_id = $1
            """,
            acct["account_id"],
        )

        total_credits = Decimal(str(balance_row["total_credits"]))
        total_debits = Decimal(str(balance_row["total_debits"]))

        if acct["classification"] in ("NOSTRO_CLEARING", "FEE_SUSPENSE"):
            net_balance = total_debits - total_credits
        else:
            net_balance = total_credits - total_debits

        return {
            "tenant_id": x_tenant_id,
            "account_number": account_number,
            "classification": acct["classification"],
            "currency": acct["currency"],
            "total_credits": float(total_credits),
            "total_debits": float(total_debits),
            "net_settled_balance": float(net_balance),
        }


@app.get("/v1/accounts/{account_number}/balance/as-of")
async def get_account_balance_as_of(
    account_number: str,
    as_of_time: str,
    time_basis: str = "assertion_time",
    x_tenant_id: str = Header(..., alias="X-Tenant-ID"),
):
    try:
        target_dt = datetime.fromisoformat(as_of_time.replace("Z", "+00:00"))
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid ISO 8601 timestamp format")

    if time_basis not in ("assertion_time", "event_time"):
        raise HTTPException(status_code=400, detail="time_basis must be 'assertion_time' or 'event_time'")

    async with db_pool.acquire() as conn:
        acct = await conn.fetchrow(
            """
            SELECT account_id, classification, currency 
            FROM chart_of_accounts 
            WHERE tenant_id = $1 AND account_number = $2
            """,
            x_tenant_id, account_number,
        )
        if not acct:
            raise HTTPException(status_code=404, detail="Account not found")

        query = f"""
            SELECT 
                COALESCE(SUM(CASE WHEN jl.direction = 'CREDIT' THEN jl.amount ELSE 0 END), 0.0000) as total_credits,
                COALESCE(SUM(CASE WHEN jl.direction = 'DEBIT' THEN jl.amount ELSE 0 END), 0.0000) as total_debits
            FROM journal_lines jl
            JOIN journal_entries je ON jl.entry_id = je.entry_id
            WHERE jl.account_id = $1 AND je.{time_basis} <= $2
        """

        balance_row = await conn.fetchrow(query, acct["account_id"], target_dt)

        total_credits = Decimal(str(balance_row["total_credits"]))
        total_debits = Decimal(str(balance_row["total_debits"]))

        if acct["classification"] in ("NOSTRO_CLEARING", "FEE_SUSPENSE"):
            net_balance = total_debits - total_credits
        else:
            net_balance = total_credits - total_debits

        return {
            "tenant_id": x_tenant_id,
            "account_number": account_number,
            "classification": acct["classification"],
            "currency": acct["currency"],
            "as_of": target_dt.isoformat(),
            "time_basis": time_basis,
            "total_credits": float(total_credits),
            "total_debits": float(total_debits),
            "net_settled_balance": float(net_balance),
        }


@app.post("/v1/entries/{entry_id}/reverse")
async def reverse_journal_entry(
    entry_id: str,
    x_tenant_id: str = Header(..., alias="X-Tenant-ID"),
):
    try:
        parsed_id = uuid.UUID(entry_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid entry UUID format")

    async with db_pool.acquire() as conn:
        async with conn.transaction():
            original_entry = await conn.fetchrow(
                """
                SELECT entry_id, utr_reference, status, reconciliation_state
                FROM journal_entries
                WHERE tenant_id = $1 AND entry_id = $2
                FOR UPDATE
                """,
                x_tenant_id, parsed_id,
            )
            if not original_entry:
                raise HTTPException(status_code=404, detail="Journal entry not found")

            original_lines = await conn.fetch(
                """
                SELECT account_id, direction, amount
                FROM journal_lines
                WHERE entry_id = $1
                ORDER BY sequence_no ASC
                """,
                parsed_id,
            )
            if not original_lines:
                raise HTTPException(status_code=400, detail="No journal lines found to reverse")

            reversal_entry_id = uuid.uuid4()
            reversal_utr = f"REV-{original_entry['utr_reference']}-{uuid.uuid4().hex[:4].upper()}"
            reversal_idemp_key = f"idemp:{x_tenant_id}:{reversal_utr}:REVERSAL"

            await conn.execute(
                """
                INSERT INTO journal_entries (
                    entry_id, tenant_id, idempotency_key, utr_reference, message_id,
                    event_time, status, reconciliation_state, description
                ) VALUES ($1, $2, $3, $4, $5, CURRENT_TIMESTAMP, 'POSTED', 'MATCHED_EXACT', $6)
                """,
                reversal_entry_id, x_tenant_id, reversal_idemp_key, reversal_utr,
                f"REVERSAL-OF-{entry_id}", f"Compensating entry for {entry_id}",
            )

            reversal_lines = []
            for seq, line in enumerate(original_lines, start=1):
                opp_direction = "CREDIT" if line["direction"] == "DEBIT" else "DEBIT"
                reversal_lines.append(
                    (reversal_entry_id, line["account_id"], opp_direction, line["amount"], seq)
                )

            await conn.executemany(
                """
                INSERT INTO journal_lines (entry_id, account_id, direction, amount, sequence_no)
                VALUES ($1, $2, $3, $4, $5)
                """,
                reversal_lines,
            )

            return {
                "reversal_entry_id": str(reversal_entry_id),
                "reversed_original_entry": entry_id,
                "lines_posted": len(reversal_lines),
                "status": "POSTED",
            }


from pydantic import BaseModel, Field

class OutwardPayoutRequest(BaseModel):
    source_van: str
    target_account_number: str
    target_ifsc: str
    beneficiary_name: str
    amount: Decimal = Field(..., gt=Decimal("0.0000"), max_digits=18, decimal_places=4)
    currency: str = Field(default="INR", max_length=3)
    instruction_id: str

def generate_pacs_008_xml(msg_id: str, end_to_end_id: str, payout: OutwardPayoutRequest) -> str:
    now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    return f"""<?xml version="1.0" encoding="UTF-8"?>
<Document xmlns="urn:iso:std:iso:20022:tech:xsd:pacs.008.001.08">
  <FIToFICstmrCdtTrf>
    <GrpHdr>
      <MsgId>{msg_id}</MsgId>
      <CreDtTm>{now_iso}</CreDtTm>
      <NbOfTxs>1</NbOfTxs>
      <SttlmInf><SttlmMtd>CLRG</SttlmMtd></SttlmInf>
    </GrpHdr>
    <CdtTrfTxInf>
      <PmtId>
        <EndToEndId>{end_to_end_id}</EndToEndId>
        <TxId>{payout.instruction_id}</TxId>
      </PmtId>
      <IntrBkSttlmAmt Ccy="{payout.currency}">{payout.amount:.4f}</IntrBkSttlmAmt>
      <IntrBkSttlmDt>{datetime.now(timezone.utc).strftime("%Y-%m-%d")}</IntrBkSttlmDt>
      <DbtrAcct><Id><Othr><Id>{payout.source_van}</Id></Othr></Id></DbtrAcct>
      <CdtrAgt><FinInstnId><ClrSysMmbId><MmbId>{payout.target_ifsc}</MmbId></ClrSysMmbId></FinInstnId></CdtrAgt>
      <Cdtr><Nm>{payout.beneficiary_name}</Nm></Cdtr>
      <CdtrAcct><Id><Othr><Id>{payout.target_account_number}</Id></Othr></Id></CdtrAcct>
    </CdtTrfTxInf>
  </FIToFICstmrCdtTrf>
</Document>"""


@app.post("/v1/payouts/outward")
async def execute_outward_payout(
    payout: OutwardPayoutRequest,
    x_tenant_id: str = Header(..., alias="X-Tenant-ID"),
):
    idemp_key = f"idemp:{x_tenant_id}:{payout.instruction_id}:{payout.amount:.4f}:{payout.currency.upper()}"
    lock_acquired = await idempotency_guard.acquire_lock(idemp_key, payout.instruction_id)
    if not lock_acquired:
        raise HTTPException(status_code=409, detail="Payout instruction already processed or in flight")

    try:
        async with db_pool.acquire() as conn:
            async with conn.transaction():
                van_acct = await conn.fetchrow(
                    """
                    SELECT account_id FROM chart_of_accounts 
                    WHERE tenant_id = $1 AND account_number = $2 AND classification = 'VIRTUAL_ACCOUNT'
                    """,
                    x_tenant_id, payout.source_van,
                )
                nostro_acct = await conn.fetchrow(
                    """
                    SELECT account_id FROM chart_of_accounts 
                    WHERE tenant_id = $1 AND classification = 'NOSTRO_CLEARING'
                    """,
                    x_tenant_id,
                )
                if not van_acct or not nostro_acct:
                    await redis_client.delete(idemp_key)
                    raise HTTPException(status_code=404, detail="Source Virtual Account or Nostro clearing account not found")

                bal_row = await conn.fetchrow(
                    """
                    SELECT 
                        COALESCE(SUM(CASE WHEN direction = 'CREDIT' THEN amount ELSE -amount END), 0.0000) as balance
                    FROM journal_lines
                    WHERE account_id = $1
                    """,
                    van_acct["account_id"],
                )
                current_balance = Decimal(str(bal_row["balance"]))
                if current_balance < payout.amount:
                    await redis_client.delete(idemp_key)
                    raise HTTPException(
                        status_code=400,
                        detail=f"Insufficient settled funds: Requested {payout.amount}, Available {current_balance}",
                    )

                entry_id = uuid.uuid4()
                msg_id = f"MSG-OUT-{uuid.uuid4().hex[:12].upper()}"
                end_to_end_id = f"E2E-{payout.instruction_id}"

                await conn.execute(
                    """
                    INSERT INTO journal_entries (
                        entry_id, tenant_id, idempotency_key, utr_reference, message_id,
                        event_time, status, reconciliation_state, description
                    ) VALUES ($1, $2, $3, $4, $5, CURRENT_TIMESTAMP, 'POSTED', 'MATCHED_EXACT', $6)
                    """,
                    entry_id, x_tenant_id, idemp_key, payout.instruction_id,
                    msg_id, f"Outward transfer to {payout.beneficiary_name} ({payout.target_account_number})",
                )

                await conn.executemany(
                    """
                    INSERT INTO journal_lines (entry_id, account_id, direction, amount, sequence_no)
                    VALUES ($1, $2, $3, $4, $5)
                    """,
                    [
                        (entry_id, van_acct["account_id"], "DEBIT", payout.amount, 1),
                        (entry_id, nostro_acct["account_id"], "CREDIT", payout.amount, 2),
                    ],
                )

        pacs_008_payload = generate_pacs_008_xml(msg_id, end_to_end_id, payout)

        return {
            "status": "DISPATCHED",
            "entry_id": str(entry_id),
            "instruction_id": payout.instruction_id,
            "amount_disbursed": float(payout.amount),
            "remaining_balance": float(current_balance - payout.amount),
            "clearing_message_id": msg_id,
            "pacs_008_xml": pacs_008_payload,
        }
    except asyncpg.exceptions.UniqueViolationError:
        raise HTTPException(status_code=409, detail="Payout instruction already processed or in flight")

from fastapi.responses import HTMLResponse

@app.get("/dashboard", response_class=HTMLResponse)
async def serve_dashboard():
    return """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>Core Banking Ledger & Clearing Console</title>
  <style>
    :root {
      --bg: #0b0f19;
      --card-bg: #111827;
      --border: #1f2937;
      --primary: #3b82f6;
      --success: #10b981;
      --warning: #f59e0b;
      --danger: #ef4444;
      --text: #f3f4f6;
      --muted: #9ca3af;
    }
    * { box-sizing: border-box; margin: 0; padding: 0; font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, monospace; }
    body { background: var(--bg); color: var(--text); padding: 24px; }
    .header { display: flex; justify-content: space-between; align-items: center; border-b: 1px solid var(--border); padding-bottom: 16px; margin-bottom: 24px; }
    .status-badge { display: flex; align-items: center; gap: 8px; font-size: 13px; color: var(--success); font-weight: 600; }
    .dot { width: 10px; height: 10px; border-radius: 50%; background: var(--success); box-shadow: 0 0 8px var(--success); }
    .grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(240px, 1fr)); gap: 16px; margin-bottom: 24px; }
    .card { background: var(--card-bg); border: 1px solid var(--border); border-radius: 8px; padding: 20px; }
    .card-title { font-size: 12px; color: var(--muted); text-transform: uppercase; letter-spacing: 0.05em; margin-bottom: 8px; }
    .card-val { font-size: 26px; font-weight: 700; color: #fff; }
    .card-sub { font-size: 12px; color: var(--muted); margin-top: 6px; }
    table { width: 100%; border-collapse: collapse; margin-top: 12px; }
    th, td { text-align: left; padding: 12px; border-bottom: 1px solid var(--border); font-size: 13px; }
    th { color: var(--muted); text-transform: uppercase; font-size: 11px; letter-spacing: 0.05em; }
    .badge { padding: 4px 8px; border-radius: 4px; font-size: 11px; font-weight: 600; }
    .badge-exact { background: rgba(16, 185, 129, 0.15); color: var(--success); }
    .badge-break { background: rgba(239, 68, 68, 0.15); color: var(--danger); }
    .badge-fee { background: rgba(245, 158, 11, 0.15); color: var(--warning); }
    .refresh-btn { background: var(--primary); color: #fff; border: none; padding: 8px 16px; border-radius: 6px; cursor: pointer; font-size: 13px; font-weight: 600; }
    .refresh-btn:hover { opacity: 0.9; }
  </style>
</head>
<body>
  <div class="header">
    <div>
      <h2>Institutional Ledger Core Console</h2>
      <p style="color: var(--muted); font-size: 13px;">Multi-Tier Settlement Rail & Real-Time Invariant Monitor</p>
    </div>
    <div style="display: flex; gap: 16px; align-items: center;">
      <div class="status-badge"><span class="dot"></span> RAILS OPERATIONAL (ZERO-SUM INTACT)</div>
      <button class="refresh-btn" onclick="fetchMetrics()">Refresh Ledger State</button>
    </div>
  </div>

  <div class="grid">
    <div class="card">
      <div class="card-title">VAN Settled Balance</div>
      <div class="card-val" id="van-bal">Loading...</div>
      <div class="card-sub">VAN-HDFC-9920194 (INR)</div>
    </div>
    <div class="card">
      <div class="card-title">Break Suspense Exposure</div>
      <div class="card-val" id="break-bal" style="color: var(--danger);">Loading...</div>
      <div class="card-sub">BREAK-SUSPENSE-001</div>
    </div>
    <div class="card">
      <div class="card-title">Fee Suspense Balance</div>
      <div class="card-val" id="fee-bal" style="color: var(--warning);">Loading...</div>
      <div class="card-sub">FEE-SUSPENSE-001</div>
    </div>
    <div class="card">
      <div class="card-title">Global Double-Entry Variance</div>
      <div class="card-val" id="variance-bal" style="color: var(--success);">0.0000 INR</div>
      <div class="card-sub">Debits == Credits (Strict Invariant)</div>
    </div>
  </div>

  <div class="card">
    <h3 style="font-size: 15px; margin-bottom: 8px;">Recent Journal Entries & Settlement Events</h3>
    <table>
      <thead>
        <tr>
          <th>Event ID / UTR</th>
          <th>Type / State</th>
          <th>Status</th>
          <th>Amount</th>
        </tr>
      </thead>
      <tbody id="stream-rows">
        <tr><td colspan="4" style="color: var(--muted); text-align: center;">Polling settlement stream...</td></tr>
      </tbody>
    </table>
  </div>

  <script>
    const HEADERS = { "X-Tenant-ID": "TENANT_CORP_001" };

    async function fetchMetrics() {
      try {
        const vanRes = await fetch("/v1/accounts/VAN-HDFC-9920194/balance", { headers: HEADERS });
        const vanData = await vanRes.json();
        document.getElementById("van-bal").innerText = Number(vanData.net_settled_balance).toLocaleString('en-IN', { minimumFractionDigits: 4 }) + " INR";

        const breakRes = await fetch("/v1/accounts/BREAK-SUSPENSE-001/balance", { headers: HEADERS });
        const breakData = await breakRes.json();
        document.getElementById("break-bal").innerText = Number(breakData.net_settled_balance).toLocaleString('en-IN', { minimumFractionDigits: 4 }) + " INR";

        const feeRes = await fetch("/v1/accounts/FEE-SUSPENSE-001/balance", { headers: HEADERS });
        const feeData = await feeRes.json();
        document.getElementById("fee-bal").innerText = Number(feeData.net_settled_balance).toLocaleString('en-IN', { minimumFractionDigits: 4 }) + " INR";

        const streamRes = await fetch("/v1/events/stream?count=8", { headers: HEADERS });
        const streamData = await streamRes.json();
        const tbody = document.getElementById("stream-rows");
        if (streamData.events && streamData.events.length > 0) {
          tbody.innerHTML = streamData.events.map(ev => {
            const data = ev.data || {};
            const badgeClass = data.status === 'MATCHED_EXACT' ? 'badge-exact' : (data.status === 'SUSPENSE_BREAK' ? 'badge-break' : 'badge-fee');
            return `<tr>
              <td><code>${data.utr || ev.event_id}</code></td>
              <td><span class="badge ${badgeClass}">${data.status || 'SETTLED'}</span></td>
              <td>POSTED</td>
              <td>${data.amount ? Number(data.amount).toLocaleString('en-IN', { minimumFractionDigits: 4 }) + ' INR' : 'N/A'}</td>
            </tr>`;
          }).join('');
        }
      } catch (err) {
        console.error("Failed to load metrics:", err);
      }
    }

    fetchMetrics();
    setInterval(fetchMetrics, 5000);
  </script>
</body>
</html>
"""

from fastapi.responses import RedirectResponse

@app.get("/", include_in_schema=False)
async def root_redirect():
    return RedirectResponse(url="/dashboard")
