"""Sends ONE camt.054 notification containing TWO <Ntry> elements.
This is the exact shape that the original parser silently mishandled
(it used .find() instead of .findall() and only ever saw entry #1).
Expected result now: processed: 2, with both UTRs settled independently."""
import hashlib
import hmac
import requests

HMAC_SECRET = "PROD_SECRET_KEY"

xml_body = """<?xml version="1.0" encoding="UTF-8"?>
<Document xmlns="urn:iso:std:iso:20022:tech:xsd:camt.054.001.08">
  <BkToCstmrDbtCdtNtfctn>
    <GrpHdr><MsgId>MSG-BATCH-001</MsgId></GrpHdr>
    <Ntfctn>
      <Acct><Id><Othr><Id>VAN-HDFC-9920194</Id></Othr></Id></Acct>
      <Ntry>
        <Amt Ccy="INR">750000.0000</Amt>
        <BookgDt><DtTm>2026-09-05T02:00:00Z</DtTm></BookgDt>
        <NtryDtls><TxDtls><Refs><TxId>UTR-BATCH-A</TxId></Refs>
          <RltdPties><Dbtr><Nm>Acme Enterprises Pvt Ltd</Nm></Dbtr></RltdPties>
        </TxDtls></NtryDtls>
      </Ntry>
      <Ntry>
        <Amt Ccy="INR">300000.0000</Amt>
        <BookgDt><DtTm>2026-09-05T02:05:00Z</DtTm></BookgDt>
        <NtryDtls><TxDtls><Refs><TxId>UTR-BATCH-B</TxId></Refs>
          <RltdPties><Dbtr><Nm>Beta Traders LLP</Nm></Dbtr></RltdPties>
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
print(resp.status_code, resp.json())
print()
print("Expected: processed: 2 (both UTR-BATCH-A and UTR-BATCH-B settled).")
print("Note: both will come back SUSPENSE_BREAK since no invoice exists for")
print("these amounts against VAN-HDFC-9920194 — that's fine, this test is")
print("only checking that BOTH entries are seen and processed, not that they")
print("match an invoice.")
