"""Sends a webhook for a VAN with no matching open invoice.
Expected result: recon_status: SUSPENSE_BREAK, funds routed to BREAK_SUSPENSE."""
import hashlib
import hmac
import requests

HMAC_SECRET = "PROD_SECRET_KEY"

xml_body = """<?xml version="1.0" encoding="UTF-8"?>
<Document xmlns="urn:iso:std:iso:20022:tech:xsd:camt.054.001.08">
  <BkToCstmrDbtCdtNtfctn>
    <GrpHdr><MsgId>MSG-UNKNOWN-001</MsgId></GrpHdr>
    <Ntfctn>
      <Acct><Id><Othr><Id>VAN-UNKNOWN-999</Id></Othr></Id></Acct>
      <Ntry>
        <Amt Ccy="INR">42000.0000</Amt>
        <BookgDt><DtTm>2026-09-05T03:00:00Z</DtTm></BookgDt>
        <NtryDtls><TxDtls><Refs><TxId>UTR-UNKNOWN-001</TxId></Refs>
          <RltdPties><Dbtr><Nm>Unrecognized Remitter</Nm></Dbtr></RltdPties>
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
print("Expected: recon_status: SUSPENSE_BREAK")
