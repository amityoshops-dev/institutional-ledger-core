import asyncio
import hashlib
import hmac
import time
import uuid
import httpx

HMAC_SECRET = "PROD_SECRET_KEY"
URL = "http://localhost:8000/v1/webhooks/clearing/iso20022"
CONCURRENCY = 500

def create_payload(idx: int):
    utr = f"UTR-STRESS-{uuid.uuid4().hex[:6].upper()}-{idx}"
    xml = f"""<?xml version="1.0" encoding="UTF-8"?>
<Document xmlns="urn:iso:std:iso:20022:tech:xsd:camt.054.001.08">
  <BkToCstmrDbtCdtNtfctn>
    <GrpHdr><MsgId>MSG-LOAD-{idx}</MsgId></GrpHdr>
    <Ntfctn>
      <Acct><Id><Othr><Id>VAN-HDFC-9920194</Id></Othr></Id></Acct>
      <Ntry>
        <Amt Ccy="INR">100.0000</Amt>
        <BookgDt><DtTm>2026-09-06T12:00:00Z</DtTm></BookgDt>
        <NtryDtls><TxDtls><Refs><TxId>{utr}</TxId></Refs>
          <RltdPties><Dbtr><Nm>Stress Runner</Nm></Dbtr></RltdPties>
        </TxDtls></NtryDtls>
      </Ntry>
    </Ntfctn>
  </BkToCstmrDbtCdtNtfctn>
</Document>"""
    body = xml.encode("utf-8")
    sig = hmac.new(HMAC_SECRET.encode(), body, hashlib.sha256).hexdigest()
    return body, sig

async def send_worker(client: httpx.AsyncClient, idx: int, sem: asyncio.Semaphore):
    body, sig = create_payload(idx)
    async with sem:
        try:
            resp = await client.post(
                URL,
                content=body,
                headers={
                    "X-Tenant-ID": "TENANT_CORP_001",
                    "X-Signature-SHA256": sig,
                    "Content-Type": "application/xml",
                },
                timeout=30.0,
            )
            return resp.status_code
        except Exception as e:
            return 599

async def main():
    print("=" * 75)
    print(f"COMMENCING CONCURRENCY STRESS BENCHMARK ({CONCURRENCY} INBOUND CLEARING EVENTS)")
    print("=" * 75)

    sem = asyncio.Semaphore(50)  # Max 50 active socket connections
    limits = httpx.Limits(max_connections=100, max_keepalive_connections=50)

    async with httpx.AsyncClient(limits=limits) as client:
        start = time.perf_counter()
        tasks = [send_worker(client, i, sem) for i in range(CONCURRENCY)]
        results = await asyncio.gather(*tasks)
        elapsed = time.perf_counter() - start

    success_count = results.count(200)
    conflict_count = results.count(409)
    error_count = len(results) - (success_count + conflict_count)

    print(f"Total Transactions Processed : {len(results)}")
    print(f"Successful Ingestions (200)   : {success_count}")
    print(f"Idempotency Guard Hits (409) : {conflict_count}")
    print(f"System/Pool Failures         : {error_count}")
    print(f"Elapsed Time                 : {elapsed:.2f} seconds")
    print(f"Throughput                   : {len(results) / elapsed:.2f} transactions/sec")
    print("=" * 75)

if __name__ == "__main__":
    asyncio.run(main())
