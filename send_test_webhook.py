"""Sends one realistic camt.054 test webhook to the running local server."""
import hashlib
import hmac
import requests

HMAC_SECRET = "PROD_SECRET_KEY"

xml_body = """<?xml version="1.0" encoding="UTF-8"?>
<Document xmlns="urn:iso:std:iso:20022:tech:xsd:camt.054.001.08">
  <BkToCstmrDbtCdtNtfctn>
    <GrpHdr><MsgId>MSG-TEST-001</MsgId></GrpHdr>
    <Ntfctn>
      <Acct><Id><Othr><Id>VAN-HDFC-9920194</Id></Othr></Id></Acct>
      <Ntry>
        <Amt Ccy="INR">1500000.0000</Amt>
        <BookgDt><DtTm>2026-09-05T01:29:45Z</DtTm></BookgDt>
        <NtryDtls><TxDtls><Refs><TxId>UTR-RBI-RTGS-TEST-001</TxId></Refs>
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
print(resp.status_code, resp.json())
