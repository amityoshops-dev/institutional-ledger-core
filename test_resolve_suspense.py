import requests
import asyncio
import os
import asyncpg
from dotenv import load_dotenv

load_dotenv()
BASE_URL = "http://localhost:8000"
HEADERS = {
    "X-Tenant-ID": "TENANT_CORP_001",
    "Content-Type": "application/json"
}

async def get_break_entry():
    conn = await asyncpg.connect(os.environ.get("DATABASE_URL"))
    row = await conn.fetchrow(
        """
        SELECT entry_id, utr_reference 
        FROM journal_entries 
        WHERE tenant_id = 'TENANT_CORP_001' AND reconciliation_state = 'SUSPENSE_BREAK' 
        ORDER BY created_at ASC LIMIT 1;
        """
    )
    await conn.close()
    return str(row["entry_id"]), row["utr_reference"]

target_id, target_utr = asyncio.run(get_break_entry())
print("=" * 70)
print(f"RESOLVING SUSPENSE BREAK: Entry {target_id} (UTR: {target_utr})")
print("=" * 70)

payload = {
    "break_entry_id": target_id,
    "target_van": "VAN-HDFC-9920194"
}

resp = requests.post(f"{BASE_URL}/v1/suspense/resolve", json=payload, headers=HEADERS)
print("Status Code:", resp.status_code)

if resp.status_code == 200:
    data = resp.json()
    print("Resolution Entry ID :", data["resolution_entry_id"])
    print("Amount Reallocated  :", data["amount_reallocated"], "INR")
    print("Target Account      :", data["target_account"])
    print("Status              :", data["status"])
    print("\nPASS: Suspense funds safely cleared into customer Virtual Account.")
else:
    print("Error:", resp.text)
