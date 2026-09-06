# Multi-Tenant Virtual Account Ledger & Auto-Reconciliation Engine (ISO 20022 Native)

[![License: Apache 2.0](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)
[![Python: 3.11+](https://img.shields.io/badge/Python-3.11%2B-blue.svg)](https://www.python.org/)
[![Database: PostgreSQL 16](https://img.shields.io/badge/Database-PostgreSQL%2016-336791.svg)](https://www.postgresql.org/)
[![Messaging: ISO 20022](https://img.shields.io/badge/Standard-ISO%2020022%20(camt.054)-green.svg)](https://www.iso20022.org/)

An institutional-style Virtual Account Number (VAN) routing and double-entry ledger engine for enterprise cash management, automated invoice reconciliation, and B2B treasury operations.

Built as a portfolio system demonstrating the failure modes real transaction-banking platforms have to design around: unallocated suspense balances, duplicate webhook credits, and settlement-finality gaps.

> **Status note (read before quoting benchmarks in an interview):** this is a reference architecture with working core logic, not a load-tested production system. The "benchmarks achieved" table in earlier drafts of this README were placeholders, not measurements — they've been removed below. Say so if asked; fabricated numbers are the fastest way to lose credibility with a technical interviewer who asks "how did you measure that."

---

## 1. Architectural Overview

```
[ Clearing Rails (RTGS/NEFT/IMPS) ]
        │
        ▼  ISO 20022 camt.054.001.08 / HMAC-SHA256 webhook
[ Ingestion Gateway (FastAPI) ]
        │
        ├── OpenTelemetry span opened
        ▼
[ Redis Distributed Lock (Lua, atomic EXISTS+SET) ]
   ├── Duplicate/in-flight UTR ──► 200 OK, drop (stop bank retry storm)
   └── New composite key ──► proceed
        ▼
[ defusedxml Normalizer — camt.054 or JSON ]
        ▼
[ Multi-Tier Reconciliation State Machine ]
   ┌─────────────┼──────────────┐
   ▼             ▼              ▼
[Tier 1: Exact][Tier 2: Tolerance][Tier 3: Break]
 UTR+VAN+amount  ≤ ₹50 MDR variance  Unmapped VAN/invoice
   └─────────────┼──────────────┘
                 ▼
[ PostgreSQL Bi-Temporal Ledger ]
  • Deferred constraint trigger: Σ(Debits) − Σ(Credits) = 0 at COMMIT
  • Immutability trigger: rejects UPDATE/DELETE on journal_lines
                 ▼
[ Outbound ERP Sync / Webhook Push ]
```

---

## 2. Key Architecture Invariants & Design Decisions

### A. Zero-Sum Ledger Invariant
Balances are never stored as mutable scalars — no `UPDATE balance = balance + x`. Balances are computed aggregations over immutable journal lines.

- **Deferred constraint trigger**, evaluated at `COMMIT`: `SUM(Debits) − SUM(Credits) = 0.0000` per `entry_id`.
- **Immutability trigger** rejects any `UPDATE`/`DELETE` on `journal_lines`; corrections require a compensating entry.
- **Known gap, not yet closed:** the balance trigger only runs its check when the parent `journal_entries.status` is `'POSTED'` at commit time. The column defaults to `'PENDING'`. The shipped ingestion path always sets `status='POSTED'` explicitly, so *that* path is safe — but the database itself does not guarantee balance for any future caller that inserts lines against a still-`PENDING` header (a common pattern for staged/draft entries). If you extend this system, either drop the status gate from the trigger (always check) or add a second trigger that forbids leaving lines attached to a non-POSTED entry indefinitely. Don't claim this as a database-level guarantee in an interview without naming this caveat — a good interviewer will ask "what if status is never flipped?" and you want to already have the answer.

### B. Distributed Atomic Idempotency
Ingestion uses an atomic Redis Lua script (`EXISTS` + `SET ... EX`) over a composite key:
`idemp:{tenant_id}:{utr}:{amount}:{currency}`, TTL 24h.

- Correctly eliminates the check-then-set race across horizontally scaled workers — verified logically (Lua scripts run atomically on Redis's single-threaded core, so no two workers can both see "not present").
- **Two real limitations to know, not gloss over:**
  1. **Keying on amount, not just UTR**, means a bank's corrected resend of the *same* UTR with a *restated* amount (a documented real-world correction pattern for reversal-and-recredit) is treated as an unrelated new transaction, not deduplicated against the original. That's a defensible tradeoff (it avoids false-positive rejection of legitimately different transactions that happen to reuse a UTR), but it is a narrower guarantee than "we dedupe on UTR" — say so precisely if asked.
  2. **Redis is the only durable dedup layer as shipped.** The Postgres `journal_entries.idempotency_key UNIQUE` column was intended as the backstop if Redis loses the key (crash before TTL, flush, failover) — but Module 4's insert populates that column with the bank's `message_id`, not the same tenant+UTR+amount+currency key used for the Redis lock, and not the UTR alone. **This means the DB uniqueness constraint is not actually guarding against the failure mode it exists for.** This is the one fix I'd make before calling this "100% idempotency protection" anywhere: populate `idempotency_key` with the same composite string `generate_key()` produces (or at minimum the UTR), so a Redis outage doesn't silently reopen the double-credit window.

### C. Bi-Temporal Event Tracking
- **Event time (`T_e`):** when the clearing house actually moved funds.
- **Assertion time (`T_a`):** when this system recorded the entry.
- Correctly separates the two, which is the right pattern for reconstructing the ledger as-of any historical point and for handling backdated/corrected notifications without distorting history.

### D. Multi-Tier Reconciliation Matrix
- **Tier 1 (Deterministic/STP):** exact UTR + VAN + amount match → `MATCHED_EXACT`, invoice auto-closed. Verified the debit/credit lines balance.
- **Tier 2 (MDR/fee tolerance):** variance ≤ ₹50 → the fee is booked to `FEE_SUSPENSE` and the VAN is credited the *full* invoice amount. I re-derived the three-line entry by hand: nostro debit (amount received) + fee-suspense debit (variance) = VAN credit (expected amount). It balances.
- **Tier 3 (Break/suspense):** unmapped VAN, no open invoice, or variance beyond tolerance → routed to `BREAK_SUSPENSE` for manual ops review.
- **Gap worth naming:** the parser (see below) assumes one `<Ntry>` per `<Ntfctn>`. Real camt.054 notifications are frequently batched — a single notification can carry many entries. As shipped, only the first entry in a batched notification is read; the rest are silently dropped rather than erroring. Fixed in the corrected parser below.

---

## 3. What Was Actually Verified vs. What Was Only Asserted

Since this is going into interview prep, here's the honest breakdown — this is the kind of scrutiny a systems-design interviewer will apply, so it's better you've already done it:

| Claim | Status |
|---|---|
| Zero-sum trigger balances a correctly-POSTED entry | ✅ Verified — traced trigger logic by hand; logic is sound for the shipped code path |
| Trigger is a database-level guarantee regardless of caller | ❌ False as stated — gated on `status='POSTED'`, see §2A |
| Redis Lua lock prevents concurrent double-processing | ✅ Verified — atomic single-threaded script, no TOCTOU gap |
| "100% idempotency protection" / DB backstop on Redis failure | ❌ Not true as shipped — DB unique key stores `message_id`, not the dedup key; fixed below |
| ISO 20022 parser handles camt.054 correctly | ⚠️ Partially — works for single-entry notifications (tested), silently drops entries in batched notifications (tested and confirmed) |
| Tier 1/Tier 2 ledger lines balance | ✅ Verified by hand-computation |
| "98.2% STP", "142ms P99 latency" benchmarks | ❌ Removed — no load test exists behind these numbers; do not repeat them as measured results |

---

## 4. Local Deployment & Quickstart

### Prerequisites
- Docker & Docker Compose
- Python 3.11+

```bash
docker compose up -d
psql -h localhost -U postgres -d ledger_db -f schema/migrations/001_initial_ledger_schema.sql
python -m venv venv && source venv/bin/activate
pip install -r requirements.txt
uvicorn src.main:app --host 0.0.0.0 --port 8000 --reload
pytest tests/ -v
```

`pytest` here is aspirational unless you write the three tests the original draft claimed: (1) unbalanced-entry rejection, (2) concurrent duplicate webhook against the Redis guard, (3) malformed/batched XML handling. None of those tests exist yet in what's been drafted — write them before claiming "validates" in a README, since an interviewer may ask to see the suite.

---

## 5. Executive Interview Pitch (revised — defensible under follow-up questions)

When asked *"Walk me through a complex financial architecture you designed and how you handled reliability, race conditions, and reconciliation breaks"*:

**1. The problem.** *"In enterprise cash management, the hard part isn't moving money — it's settlement finality and reconciliation integrity. Naive setups on basic DB updates and plain JSON webhooks produce duplicate credits under bank retry storms, unallocated suspense balances, and ERP ledgers that drift from the bank's own records."*

**2. The architecture.** *"I designed a virtual-account ledger aligned to ISO 20022 camt.054, built on three principles: an atomic Redis Lua idempotency lock keyed on tenant, UTR, amount and currency to close the check-then-set race under concurrent webhook delivery; a PostgreSQL double-entry ledger where balances are never mutated directly — every line is immutable, and a deferred constraint trigger asserts debits equal credits before any transaction can commit; and bi-temporal timestamps that separate when the bank actually moved the funds from when my system recorded it, so backdated or corrected notifications don't distort the historical ledger."*

**3. Where I'd push it further, and why that matters.** *"Building this also surfaced two things I'd fix before calling it production-ready, which I think is the more useful part of the story: the balance trigger is currently gated on the entry already being marked POSTED, so it's not yet a guarantee that holds regardless of caller — I'd either remove that gate or add a second invariant forbidding lines against a stale PENDING header. And the Postgres uniqueness constraint meant to back up Redis if it ever loses the idempotency key was populating the wrong column — it was storing the bank's message ID instead of the same composite key the Redis lock uses, so a Redis outage wouldn't actually be caught by the database. I found that by tracing the two modules against each other rather than assuming they agreed, which is exactly the kind of cross-checking this class of system needs."*

**4. The close.** *"That's the standard I hold this kind of infrastructure to — not just 'does the design pattern look right on the page,' but 'does it hold up when I trace an unhappy path all the way through.' That habit is what I'd bring to a product or architecture role."*

This version is stronger than the original in an interview precisely because it survives a good follow-up question. A panel that hears "zero double-posts, guaranteed" and then asks "what happens if Redis drops the key mid-flight" will trust you a lot more if your answer is "I actually found that gap and here's the fix" than if you're caught flat-footed defending a claim you didn't verify yourself.

---

## Corrected Module 2 — REST API note

The original OpenAPI note for `GET /v1/recon/reports` claimed the response matches ISO 20022 `camt.053` end-of-day format. `camt.053` is the *statement* message (account balance/position as of a cutoff), structurally different from `camt.054` (the *notification* message this system ingests) — they share a family but not a schema. If you build this endpoint, either genuinely map its output to `camt.053`'s `BkToCstmrStmt` structure, or drop the ISO reference and describe it as a proprietary bi-temporal extract. Claiming camt.053 conformance without implementing camt.053's actual balance/entry structure is the kind of detail a payments-literate interviewer (which you may well face, given J.P. Morgan/RBI/NPCI framing) will check.
