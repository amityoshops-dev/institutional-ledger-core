import asyncio
import os
import asyncpg
from dotenv import load_dotenv

load_dotenv()
DATABASE_URL = os.environ.get("DATABASE_URL")

async def check():
    conn = await asyncpg.connect(DATABASE_URL)

    print("=" * 60)
    print("LAST 5 JOURNAL ENTRIES")
    print("=" * 60)
    entries = await conn.fetch(
        """
        SELECT entry_id, utr_reference, reconciliation_state, status, created_at 
        FROM journal_entries 
        ORDER BY created_at DESC 
        LIMIT 5;
        """
    )
    for e in entries:
        print(dict(e))

    print("\n" + "=" * 60)
    print("LEDGER BALANCE SUMMARY (TOTAL DEBITS VS CREDITS)")
    print("=" * 60)
    lines = await conn.fetch(
        """
        SELECT direction, SUM(amount) as total 
        FROM journal_lines 
        GROUP BY direction;
        """
    )
    totals = {row["direction"]: row["total"] for row in lines}
    print(totals)

    debits = totals.get("DEBIT", 0)
    credits = totals.get("CREDIT", 0)
    if debits == credits and debits > 0:
        print(f"\nPASS: Double-entry invariant preserved! Debits ({debits}) == Credits ({credits})")
    else:
        print(f"\nFAIL: Imbalance detected! Debits: {debits}, Credits: {credits}")

    await conn.close()

if __name__ == "__main__":
    asyncio.run(check())