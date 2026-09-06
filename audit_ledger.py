import asyncio
import os
import asyncpg
from dotenv import load_dotenv

load_dotenv()
DATABASE_URL = os.environ.get("DATABASE_URL")

async def run_audit():
    conn = await asyncpg.connect(DATABASE_URL)
    
    print("=" * 75)
    print("GLOBAL DOUBLE-ENTRY INVARIANT AUDIT")
    print("=" * 75)
    
    # 1. Total debits vs credits across the entire ledger
    totals = await conn.fetchrow(
        """
        SELECT 
            COALESCE(SUM(CASE WHEN direction = 'DEBIT' THEN amount ELSE 0 END), 0.0000) AS total_debits,
            COALESCE(SUM(CASE WHEN direction = 'CREDIT' THEN amount ELSE 0 END), 0.0000) AS total_credits,
            COALESCE(SUM(CASE WHEN direction = 'DEBIT' THEN amount ELSE -amount END), 0.0000) AS net_imbalance
        FROM journal_lines;
        """
    )
    print(f"Total Debits : {totals['total_debits']:>18.4f}")
    print(f"Total Credits: {totals['total_credits']:>18.4f}")
    print(f"Net Variance : {totals['net_imbalance']:>18.4f}")
    
    if totals["net_imbalance"] == 0 and totals["total_debits"] > 0:
        print("Status       : PASS (Zero-Sum Invariant Strictly Maintained)")
    else:
        print("Status       : FAIL (Ledger Imbalance Detected)")

    print("\n" + "=" * 75)
    print("CURRENT SUSPENSE ACCOUNT BALANCES")
    print("=" * 75)
    
    # 2. Balances in Fee and Break Suspense
    suspense_rows = await conn.fetch(
        """
        SELECT 
            coa.account_number,
            coa.classification,
            COALESCE(SUM(CASE WHEN jl.direction = 'DEBIT' THEN jl.amount ELSE -jl.amount END), 0.0000) AS net_balance
        FROM chart_of_accounts coa
        LEFT JOIN journal_lines jl ON coa.account_id = jl.account_id
        WHERE coa.classification IN ('BREAK_SUSPENSE', 'FEE_SUSPENSE')
        GROUP BY coa.account_number, coa.classification;
        """
    )
    for row in suspense_rows:
        print(f"Account: {row['account_number']:<20} | Type: {row['classification']:<16} | Net Balance: {row['net_balance']:>14.4f}")

    await conn.close()

if __name__ == "__main__":
    asyncio.run(run_audit())
