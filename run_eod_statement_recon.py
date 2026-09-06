import asyncio
import os
import asyncpg
from decimal import Decimal
from dotenv import load_dotenv

load_dotenv()
DATABASE_URL = os.environ.get("DATABASE_URL")

# Simulated MT940 / camt.053 closing balance summary from clearing bank
BANK_STATEMENT_EOD = {
    "account_number": "NOSTRO-RBI-RTGS",
    "closing_settled_balance": Decimal("5000000.0000"),
    "currency": "INR",
}

async def reconcile_nostro():
    conn = await asyncpg.connect(DATABASE_URL)
    
    print("=" * 75)
    print("END-OF-DAY NOSTRO CLEARING RECONCILIATION")
    print("=" * 75)

    acct = await conn.fetchrow(
        "SELECT account_id FROM chart_of_accounts WHERE account_number = $1;",
        BANK_STATEMENT_EOD["account_number"],
    )
    if not acct:
        print("Error: Nostro account not found.")
        await conn.close()
        return

    # Nostro is an Asset: Total Debits (inflows) - Total Credits (outflows)
    bal_row = await conn.fetchrow(
        """
        SELECT 
            COALESCE(SUM(CASE WHEN direction = 'DEBIT' THEN amount ELSE 0 END), 0.0000) as debits,
            COALESCE(SUM(CASE WHEN direction = 'CREDIT' THEN amount ELSE 0 END), 0.0000) as credits
        FROM journal_lines
        WHERE account_id = $1;
        """,
        acct["account_id"],
    )

    internal_nostro_balance = Decimal(str(bal_row["debits"])) - Decimal(str(bal_row["credits"]))
    statement_balance = BANK_STATEMENT_EOD["closing_settled_balance"]
    variance = internal_nostro_balance - statement_balance

    print(f"Internal Nostro Balance (GL) : {internal_nostro_balance:>14.4f} INR")
    print(f"Bank Statement Balance (EOD) : {statement_balance:>14.4f} INR")
    print(f"Reconciliation Variance      : {variance:>14.4f} INR")

    if variance == Decimal("0.0000"):
        print("Status                       : MATCHED_CLEAN (Zero Nostro Break)")
    else:
        print(f"Status                       : NOSTRO_VARIANCE_DETECTED")
        print(f"Action                       : Post adjustment to FEE_SUSPENSE or investigate clearing in-flight.")

    await conn.close()

if __name__ == "__main__":
    asyncio.run(reconcile_nostro())
