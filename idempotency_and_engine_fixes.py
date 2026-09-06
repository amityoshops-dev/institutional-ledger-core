"""
Corrected idempotency guard + engine insert.

FIX vs original draft: journal_entries.idempotency_key was being populated
with tx.message_id in the original engine.py, while the Redis lock used a
DIFFERENT composite key (tenant_id + utr + amount + currency). That means if
Redis ever lost the lock key (crash before TTL, manual flush, failover), the
Postgres UNIQUE constraint — meant to be the durable backstop — would not
catch a genuine duplicate replay, because it was never storing the same key
Redis was checking. This file makes both layers use the identical string.
"""
from decimal import Decimal
import redis.asyncio as aioredis

IDEMPOTENCY_LUA = """
if redis.call("EXISTS", KEYS[1]) == 1 then
    return 0
else
    redis.call("SET", KEYS[1], ARGV[1], "EX", ARGV[2])
    return 1
end
"""


class DistributedIdempotencyGuard:
    def __init__(self, redis_client: aioredis.Redis, ttl_seconds: int = 86400):
        self.redis = redis_client
        self.ttl = ttl_seconds
        self._script = self.redis.register_script(IDEMPOTENCY_LUA)

    def generate_key(self, tenant_id: str, utr: str, amount: Decimal, currency: str) -> str:
        """
        Single source of truth for the dedup key. Both the Redis lock AND the
        Postgres journal_entries.idempotency_key column must use THIS string —
        that's the fix. Previously only Redis used it.
        """
        formatted_amount = f"{amount:.4f}"
        return f"idemp:{tenant_id}:{utr.strip()}:{formatted_amount}:{currency.upper()}"

    async def acquire_lock(self, idempotency_key: str, payload_hash: str) -> bool:
        result = await self._script(keys=[idempotency_key], args=[payload_hash, self.ttl])
        return result == 1


# --- engine.py insert fix (excerpt — replaces the header INSERT in execute_settlement) ---
#
# BEFORE (original draft, buggy):
#   await conn.execute(
#       """
#       INSERT INTO journal_entries (
#           entry_id, tenant_id, idempotency_key, utr_reference,
#           event_time, status, reconciliation_state, description
#       ) VALUES ($1, $2, $3, $4, $5, 'POSTED', $6, $7)
#       """,
#       entry_id, tx.tenant_id, tx.message_id, tx.utr,   # <-- tx.message_id here is the bug
#       tx.event_timestamp, recon_status, f"Source: {tx.raw_format}"
#   )
#
# AFTER (fixed — pass the SAME key used for the Redis lock, computed once
# upstream in the webhook handler and threaded through to the engine):
#
#   await conn.execute(
#       """
#       INSERT INTO journal_entries (
#           entry_id, tenant_id, idempotency_key, utr_reference, message_id,
#           event_time, status, reconciliation_state, description
#       ) VALUES ($1, $2, $3, $4, $5, $6, 'POSTED', $7, $8)
#       """,
#       entry_id, tx.tenant_id, idemp_key, tx.utr, tx.message_id,   # idemp_key, not tx.message_id
#       tx.event_timestamp, recon_status, f"Source: {tx.raw_format}"
#   )
#
# This requires execute_settlement() to accept idemp_key as a parameter
# (it's already computed in main.py before execute_settlement is called) —
# pass it through instead of recomputing or substituting message_id.
