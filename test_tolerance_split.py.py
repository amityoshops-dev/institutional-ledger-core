import asyncio
import os
import hashlib
import hmac
import requests
import asyncpg
from dotenv import load_dotenv

load_dotenv()
DATABASE_URL = os.environ.get("DATABASE_URL")
HMAC_SECRET = "PROD_SECRET_KEY"

async def seed_pending_invoice():
    conn = await asyncpg.connect(DATABASE_URL)
    # Seed invoice of 1,500,000.0000 for VAN-HDFC-9920194
    await conn.execute(
        """
        INSERT INTO invoices (tenant_id, assigned_van, expected_amount, status)
        VALUES ('TENANT_CORP_001', 'VAN-HDFC-9920194', 1500000.0000, 'PENDING');
        """
    )
    await conn.close()

def run_test():
    # Send payment of 1,499,975.0000 (short by 25.00 INR -> within <= 50.00 tolerance)
    xml_body = """<?xml version="1.0" encoding="UTF-8"?>
<Document xmlns="urn:iso:std:iso:20022:tech:xsd:camt.054.001.08">
  <BkToCstmrDbtCdtNtfctn>
    <GrpHdr><MsgId>MSG-TOLERANCE-001</MsgId></GrpHdr>
    <Ntfctn>
      <Acct><Id><Othr><Id>VAN-HDFC-9920194</Id></Othr></Id></Acct>
      <Ntry>
        <Amt Ccy="INR">1499975.0000</Amt>
        <BookgDt><DtTm>2026-09-05T04:00:00Z</DtTm></BookgDt>
        <NtryDtls><TxDtls><Refs><TxId>UTR-TOLERANCE-TEST-001</TxId></Refs>
          <RltdPties><Dbtr><Nm>Acme Enterprises Pvt Ltd</Nm></Dbtr></RltdPties>
        </TxDtls></NtryDtls>
      </Ntry>
    </Ntfctn>
  </BkToCstmrDbtCdtNtfctn>
</Document>"""

    body_bytes = xml_body.encode("utf-8")
    signature = hmac.new(HMAC_SECRET.encode(), body_bytes, hashlib.sha256).hexdigest()

    resp = requests.post(
        "http://localhost:8000/v1/webhooks/clearing/iso20022",
        data=body_bytes,
        headers={
            "X-Tenant-ID": "TENANT_CORP_001",
            "X-Signature-SHA256": signature,
            "Content-Type": "application/xml",
        },
    )
    print("Status:", resp.status_code)
    print("Response:", resp.json())

if __name__ == "__main__":
    asyncio.run(seed_pending_invoice())
    run_test()