import requests
import time
from datetime import datetime, timezone, timedelta
import uuid
import hashlib
import hmac
import asyncio
import os
import asyncpg
from dotenv import load_dotenv

load_dotenv()
BASE_URL = "http://localhost:8000"
HEADERS = {"X-Tenant-ID": "TENANT_CORP_001"}
HMAC_SECRET = "PROD_SECRET_KEY"

print("=" * 70)
print("SELF-CONTAINED BI-TEMPORAL AUDIT TEST")
print("=" * 70)

TEST_VAN = f"VAN-AUDIT-{uuid.uuid4().hex[:6].upper()}"
PAYMENT_AMT = 50000.0000

# 1. Setup isolated test account and matching invoice
async def setup_isolated_account():
    conn = await asyncpg.connect(os.environ["DATABASE_URL"])
    await conn.execute(
        """
        INSERT INTO chart_of_accounts (tenant_id, account_number, classification, currency)
        VALUES ('TENANT_CORP_001', $1, 'VIRTUAL_ACCOUNT', 'INR')
        ON CONFLICT DO NOTHING;
        """,
        TEST_VAN
    )
    await conn.execute(
        """
        INSERT INTO invoices (tenant_id, assigned_van, expected_amount, status)
        VALUES ('TENANT_CORP_001', $1, $2, 'PENDING');
        """,
        TEST_VAN, PAYMENT_AMT
    )
    await conn.close()

asyncio.run(setup_isolated_account())

# 2. Ingest payment webhook (Will match exact -> credits TEST_VAN)
audit_utr = f"UTR-AUDIT-{uuid.uuid4().hex[:6].upper()}"
xml_body = f"""<?xml version="1.0" encoding="UTF-8"?>
<Document xmlns="urn:iso:std:iso:20022:tech:xsd:camt.054.001.08">
  <BkToCstmrDbtCdtNtfctn>
    <GrpHdr><MsgId>MSG-AUDIT-{uuid.uuid4().hex[:6]}</MsgId></GrpHdr>
    <Ntfctn>
      <Acct><Id><Othr><Id>{TEST_VAN}</Id></Othr></Id></Acct>
      <Ntry>
        <Amt Ccy="INR">{PAYMENT_AMT:.4f}</Amt>
        <BookgDt><DtTm>2026-09-06T12:00:00Z</DtTm></BookgDt>
        <NtryDtls><TxDtls><Refs><TxId>{audit_utr}</TxId></Refs>
          <RltdPties><Dbtr><Nm>Audit Inflow</Nm></Dbtr></RltdPties>
        </TxDtls></NtryDtls>
      </Ntry>
    </Ntfctn>
  </BkToCstmrDbtCdtNtfctn>
</Document>"""

body_bytes = xml_body.encode("utf-8")
sig = hmac.new(HMAC_SECRET.encode(), body_bytes, hashlib.sha256).hexdigest()
resp_in = requests.post(
    f"{BASE_URL}/v1/webhooks/clearing/iso20022",
    data=body_bytes,
    headers={
        "X-Tenant-ID": "TENANT_CORP_001",
        "X-Signature-SHA256": sig,
        "Content-Type": "application/xml"
    }
).json()

entry_id = resp_in["results"][0]["entry_id"]
recon_status = resp_in["results"][0].get("recon_status")
print(f"Ingested Entry: {entry_id} | Recon Status: {recon_status}")

# Fetch the exact assertion_time stored by the database for this entry
async def get_entry_assertion_time(eid):
    conn = await asyncpg.connect(os.environ["DATABASE_URL"])
    val = await conn.fetchval(
        "SELECT assertion_time FROM journal_entries WHERE entry_id = $1;",
        uuid.UUID(eid)
    )
    await conn.close()
    return val

t_entry = asyncio.run(get_entry_assertion_time(entry_id))
# Slice time 1 second after assertion
t_inflow = (t_entry + timedelta(seconds=1)).isoformat()

# 3. Post compensating reversal
rev_resp = requests.post(f"{BASE_URL}/v1/entries/{entry_id}/reverse", headers=HEADERS)
reversal_id = rev_resp.json()["reversal_entry_id"]

t_rev_entry = asyncio.run(get_entry_assertion_time(reversal_id))
# Slice time 1 second after reversal assertion
t_post_rev = (t_rev_entry + timedelta(seconds=1)).isoformat()

# 4. Compare point-in-time balance reconstruction
b_pre = requests.get(
    f"{BASE_URL}/v1/accounts/{TEST_VAN}/balance/as-of",
    params={"as_of_time": t_inflow, "time_basis": "assertion_time"},
    headers=HEADERS
).json()["net_settled_balance"]

b_post = requests.get(
    f"{BASE_URL}/v1/accounts/{TEST_VAN}/balance/as-of",
    params={"as_of_time": t_post_rev, "time_basis": "assertion_time"},
    headers=HEADERS
).json()["net_settled_balance"]

print(f"Balance AS-OF {t_inflow} (Pre-Reversal) : {b_pre:>12.4f} INR")
print(f"Balance AS-OF {t_post_rev} (Post-Reversal): {b_post:>12.4f} INR")

assert b_pre == PAYMENT_AMT, f"Expected pre-reversal balance {PAYMENT_AMT}, got {b_pre}"
assert b_post == 0.0, f"Expected post-reversal balance 0.0, got {b_post}"
print("\nPASS: Bi-temporal reconstruction confirmed historical ledger balance successfully.")
