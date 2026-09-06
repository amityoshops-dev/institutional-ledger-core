import asyncio
import os
import asyncpg
from dotenv import load_dotenv

load_dotenv()
DATABASE_URL = os.environ.get("DATABASE_URL")

async def test_trigger():
    conn = await asyncpg.connect(DATABASE_URL)
    try:
        await conn.execute("UPDATE journal_lines SET amount = 999999.0000 WHERE sequence_no = 1;")
        print("FAIL: Mutation allowed! Trigger is missing or broken.")
    except asyncpg.exceptions.RaiseError as e:
        print("PASS: Immutability trigger fired successfully.")
        print("Database error message:", e.message)
    finally:
        await conn.close()

if __name__ == "__main__":
    asyncio.run(test_trigger())
