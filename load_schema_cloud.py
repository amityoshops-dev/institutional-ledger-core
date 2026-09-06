"""Loads schema_corrected.sql then seed_test_data.sql against the DATABASE_URL
in .env — replaces the `docker exec ... psql` step from the local Docker setup,
since there's no local container to exec into anymore."""
import os
import asyncio
import asyncpg
from dotenv import load_dotenv

load_dotenv()
DATABASE_URL = os.environ.get("DATABASE_URL")

if not DATABASE_URL or "YOUR_NEON" in DATABASE_URL:
    raise SystemExit(
        "DATABASE_URL is not set correctly in .env — open .env and paste your "
        "real Neon connection string in place of the placeholder."
    )


async def run_sql_file(conn: asyncpg.Connection, path: str):
    with open(path, "r", encoding="utf-8") as f:
        sql = f.read()
    print(f"--- Running {path} ---")
    await conn.execute(sql)
    print(f"--- {path} completed ---\n")


async def main():
    conn = await asyncpg.connect(DATABASE_URL)
    try:
        await run_sql_file(conn, "schema_corrected.sql")
        await run_sql_file(conn, "seed_test_data.sql")
        print("Schema and seed data loaded successfully.")
    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(main())
