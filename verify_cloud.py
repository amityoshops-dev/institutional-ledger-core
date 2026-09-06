import asyncio
import os
import asyncpg
import redis.asyncio as aioredis
from dotenv import load_dotenv

load_dotenv()

async def verify():
    print("=" * 60)
    print("TESTING CLOUD INFRASTRUCTURE CONNECTIVITY")
    print("=" * 60)
    try:
        p = await asyncpg.connect(os.environ['DATABASE_URL'])
        res = await p.fetchval('SELECT 1;')
        print(f"Neon Postgres Connection : {'PASS' if res == 1 else 'FAIL'}")
        await p.close()
    except Exception as e:
        print(f"Neon Postgres Connection : FAIL ({e})")

    try:
        r = aioredis.from_url(os.environ['REDIS_URL'], ssl_cert_reqs=None)
        pong = await r.ping()
        print(f"Upstash Redis Connection : {'PASS' if pong else 'FAIL'}")
        await r.aclose()
    except Exception as e:
        print(f"Upstash Redis Connection : FAIL ({e})")
    print("=" * 60)

if __name__ == "__main__":
    asyncio.run(verify())
