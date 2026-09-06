import hashlib
import hmac
import requests
import uuid

HMAC_SECRET = "PROD_SECRET_KEY"
URL = "http://localhost:8000/v1/webhooks/clearing/iso20022"

def send(xml_body: str, tenant: str = "TENANT_CORP_001"):
    body_bytes = xml_body.encode("utf-8")
    signature = hmac.new(HMAC_SECRET.encode(), body_bytes, hashlib.sha256).hexdigest()
    return requests.post(
        URL,
        data=body_bytes,
        headers={
            "X-Tenant-ID": tenant,
            "X-Signature-SHA256": signature,
            "Content-Type": "application/xml",
        },
    )

results = {}

# TEST 1: Idempotency (Expect ALREADY_PROCESSED_OR_IN_FLIGHT)
idemp_xml = """<?xml version="1.0" encoding="UTF-8"?>
<Document xmlns="urn:iso:std:iso:20022:tech:xsd:camt.054.001.08">
  <BkToCstmrDbtCdtNtfctn>
    <GrpHdr><MsgId>MSG-TEST-001-RETRY</MsgId></GrpHdr>
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

print("=" * 70)
print("TEST 1: IDEMPOTENCY")
print("=" * 70)
resp1 = send(idemp_xml)
passed1 = resp1.json().get("results", [{}])[0].get("status") == "ALREADY_PROCESSED_OR_IN_FLIGHT"
results["idempotency"] = passed1
print("PASS" if passed1 else f"FAIL: {resp1.json()}")

# TEST 2: Batch Entries (Unique UTRs)
utr_b1 = f"UTR-B1-{uuid.uuid4().hex[:6].upper()}"
utr_b2 = f"UTR-B2-{uuid.uuid4().hex[:6].upper()}"
batch_xml = f"""<?xml version="1.0" encoding="UTF-8"?>
<Document xmlns="urn:iso:std:iso:20022:tech:xsd:camt.054.001.08">
  <BkToCstmrDbtCdtNtfctn>
    <GrpHdr><MsgId>MSG-BATCH-{uuid.uuid4().hex[:6]}</MsgId></GrpHdr>
    <Ntfctn>
      <Acct><Id><Othr><Id>VAN-HDFC-9920194</Id></Othr></Id></Acct>
      <Ntry>
        <Amt Ccy="INR">750000.0000</Amt>
        <BookgDt><DtTm>2026-09-05T02:00:00Z</DtTm></BookgDt>
        <NtryDtls><TxDtls><Refs><TxId>{utr_b1}</TxId></Refs>
          <RltdPties><Dbtr><Nm>Acme Enterprises</Nm></Dbtr></RltdPties>
        </TxDtls></NtryDtls>
      </Ntry>
      <Ntry>
        <Amt Ccy="INR">300000.0000</Amt>
        <BookgDt><DtTm>2026-09-05T02:05:00Z</DtTm></BookgDt>
        <NtryDtls><TxDtls><Refs><TxId>{utr_b2}</TxId></Refs>
          <RltdPties><Dbtr><Nm>Beta Traders</Nm></Dbtr></RltdPties>
        </TxDtls></NtryDtls>
      </Ntry>
    </Ntfctn>
  </BkToCstmrDbtCdtNtfctn>
</Document>"""

print("=" * 70)
print("TEST 2: BATCH ENTRIES")
print("=" * 70)
resp2 = send(batch_xml)
passed2 = resp2.json().get("processed") == 2
results["batch_entries"] = passed2
print("PASS" if passed2 else f"FAIL: {resp2.json()}")

# TEST 3: Suspense Break (Unique UTR)
utr_unk = f"UTR-UNK-{uuid.uuid4().hex[:6].upper()}"
suspense_xml = f"""<?xml version="1.0" encoding="UTF-8"?>
<Document xmlns="urn:iso:std:iso:20022:tech:xsd:camt.054.001.08">
  <BkToCstmrDbtCdtNtfctn>
    <GrpHdr><MsgId>MSG-UNK-{uuid.uuid4().hex[:6]}</MsgId></GrpHdr>
    <Ntfctn>
      <Acct><Id><Othr><Id>VAN-UNKNOWN-999</Id></Othr></Id></Acct>
      <Ntry>
        <Amt Ccy="INR">42000.0000</Amt>
        <BookgDt><DtTm>2026-09-05T03:00:00Z</DtTm></BookgDt>
        <NtryDtls><TxDtls><Refs><TxId>{utr_unk}</TxId></Refs>
          <RltdPties><Dbtr><Nm>Unknown Party</Nm></Dbtr></RltdPties>
        </TxDtls></NtryDtls>
      </Ntry>
    </Ntfctn>
  </BkToCstmrDbtCdtNtfctn>
</Document>"""

print("=" * 70)
print("TEST 3: SUSPENSE BREAK")
print("=" * 70)
resp3 = send(suspense_xml)
passed3 = resp3.json().get("results", [{}])[0].get("recon_status") == "SUSPENSE_BREAK"
results["suspense_break"] = passed3
print("PASS" if passed3 else f"FAIL: {resp3.json()}")

print("=" * 70)
for k, v in results.items():
    print(f"  {k:<20} {'PASS' if v else 'FAIL'}")
