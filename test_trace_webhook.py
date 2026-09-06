import hashlib
import hmac
import requests
import uuid

HMAC_SECRET = "PROD_SECRET_KEY"
unique_utr = f"UTR-TRACE-{uuid.uuid4().hex[:8].upper()}"

xml_body = f"""<?xml version="1.0" encoding="UTF-8"?>
<Document xmlns="urn:iso:std:iso:20022:tech:xsd:camt.054.001.08">
  <BkToCstmrDbtCdtNtfctn>
    <GrpHdr><MsgId>MSG-TRACE-001</MsgId></GrpHdr>
    <Ntfctn>
      <Acct><Id><Othr><Id>VAN-HDFC-9920194</Id></Othr></Id></Acct>
      <Ntry>
        <Amt Ccy="INR">10000.0000</Amt>
        <BookgDt><DtTm>2026-09-06T10:00:00Z</DtTm></BookgDt>
        <NtryDtls><TxDtls><Refs><TxId>{unique_utr}</TxId></Refs>
          <RltdPties><Dbtr><Nm>Tracing Test Corp</Nm></Dbtr></RltdPties>
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

print("Status Code:", resp.status_code)
print("Response   :", resp.json())
