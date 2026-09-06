import requests
import asyncio
import os
import asyncpg
from dotenv import load_dotenv

load_dotenv()

BASE_URL = "http://localhost:8000"
HEADERS = {"X-Tenant-ID": "TENANT_CORP_001"}

async def get_clean_target():
    conn = await asyncpg.connect(os.environ.get("DATABASE_URL"))
    # Select an entry that is not a reversal and not a resolution
    row = await conn.fetchrow(
        """
        SELECT entry_id FROM journal_entries 
        WHERE tenant_id = 'TENANT_CORP_001' 
          AND reconciliation_state = 'MATCHED_EXACT' 
          AND utr_reference NOT LIKE 'REV-%' 
          AND utr_reference NOT LIKE 'RES-%'
        ORDER BY created_at DESC LIMIT 1;
        """
    )
    await conn.close()
    return str(row["entry_id"]) if row else None

target_id = asyncio.run(get_clean_target())

if not target_id:
    print("No reversible entries found, skipping reversal test.")
else:
    print("=" * 70)
    print(f"REVERSING JOURNAL ENTRY: {target_id}")
    print("=" * 70)
    resp = requests.post(f"{BASE_URL}/v1/entries/{target_id}/reverse", headers=HEADERS)
    print("Status:", resp.status_code)
    print("Response:", resp.json() if resp.status_code == 200 else resp.text)
