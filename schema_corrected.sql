-- ============================================================================
-- CORRECTED LEDGER SCHEMA
-- Changes from the original draft are marked with "-- FIX:" comments.
-- ============================================================================

CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- ============================================================================
-- 1. ENUMS
-- ============================================================================

CREATE TYPE account_classification AS ENUM (
    'NOSTRO_CLEARING',
    'VIRTUAL_ACCOUNT',
    'ESCROW_SETTLEMENT',
    'FEE_SUSPENSE',
    'BREAK_SUSPENSE'
);

CREATE TYPE entry_status AS ENUM (
    'PENDING',
    'POSTED',
    'REJECTED'
);

CREATE TYPE recon_state AS ENUM (
    'UNRECONCILED',
    'MATCHED_EXACT',
    'TOLERANCE_ADJUSTED',
    'SUSPENSE_BREAK'
);

CREATE TYPE entry_direction AS ENUM (
    'DEBIT',
    'CREDIT'
);

-- ============================================================================
-- 2. MULTI-TENANT CHART OF ACCOUNTS
-- ============================================================================

CREATE TABLE chart_of_accounts (
    account_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id VARCHAR(64) NOT NULL,
    account_number VARCHAR(64) NOT NULL,
    classification account_classification NOT NULL,
    currency CHAR(3) DEFAULT 'INR' NOT NULL,
    is_active BOOLEAN DEFAULT TRUE NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP NOT NULL,
    CONSTRAINT uq_tenant_account UNIQUE (tenant_id, account_number)
);

-- ============================================================================
-- 3. BI-TEMPORAL JOURNAL HEADERS
-- ============================================================================

CREATE TABLE journal_entries (
    entry_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id VARCHAR(64) NOT NULL,

    -- FIX: this column must hold the SAME composite key the Redis lock uses
    -- (tenant_id + utr + amount + currency), not the bank's message_id.
    -- The original Module 4 code populated this with tx.message_id, which
    -- silently breaks the "DB is the backstop if Redis loses the key" guarantee.
    -- Populate as: idemp:{tenant_id}:{utr}:{amount:.4f}:{currency} — identical
    -- string your idempotency_guard.generate_key() already produces.
    idempotency_key VARCHAR(160) UNIQUE NOT NULL,

    utr_reference VARCHAR(64) NOT NULL,
    message_id VARCHAR(128),                              -- FIX: bank MsgId kept separately, for traceability only — never used for dedup

    event_time TIMESTAMP WITH TIME ZONE NOT NULL,
    assertion_time TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP NOT NULL,

    status entry_status DEFAULT 'PENDING' NOT NULL,
    reconciliation_state recon_state DEFAULT 'UNRECONCILED' NOT NULL,
    description TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP NOT NULL
);

-- FIX: index the UTR directly since it's now distinct from idempotency_key
CREATE INDEX idx_entries_utr ON journal_entries (tenant_id, utr_reference);

-- ============================================================================
-- 4. IMMUTABLE JOURNAL LINES
-- ============================================================================

CREATE TABLE journal_lines (
    line_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    entry_id UUID NOT NULL REFERENCES journal_entries(entry_id) ON DELETE RESTRICT,
    account_id UUID NOT NULL REFERENCES chart_of_accounts(account_id) ON DELETE RESTRICT,
    direction entry_direction NOT NULL,
    amount NUMERIC(18, 4) NOT NULL,
    sequence_no INT NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP NOT NULL,

    CONSTRAINT chk_positive_amount CHECK (amount > 0.0000),
    CONSTRAINT uq_entry_sequence UNIQUE (entry_id, sequence_no)
);

-- ============================================================================
-- 5. ZERO-SUM INVARIANT TRIGGER
-- FIX: original trigger only checked balance when status = 'POSTED', which
-- means a header left in 'PENDING' with lines attached would NEVER be checked,
-- silently defeating the "database guarantees zero-sum" claim for any future
-- caller that doesn't follow the exact insert order Module 4 used. Below,
-- the check runs unconditionally at commit, for every entry that has ANY
-- lines attached — regardless of header status. A genuinely draft/staged
-- entry should have zero lines until it's ready to post, not a PENDING
-- header with partial lines sitting unchecked.
-- ============================================================================

CREATE OR REPLACE FUNCTION verify_journal_entry_balance()
RETURNS TRIGGER AS $$
DECLARE
    v_total_debit NUMERIC(18, 4);
    v_total_credit NUMERIC(18, 4);
BEGIN
    SELECT
        COALESCE(SUM(CASE WHEN direction = 'DEBIT' THEN amount ELSE 0 END), 0.0000),
        COALESCE(SUM(CASE WHEN direction = 'CREDIT' THEN amount ELSE 0 END), 0.0000)
    INTO
        v_total_debit,
        v_total_credit
    FROM journal_lines
    WHERE entry_id = NEW.entry_id;

    IF (v_total_debit <> v_total_credit) THEN
        RAISE EXCEPTION 'Ledger Invariant Violation: Entry % does not balance! Total Debits: %, Total Credits: %',
            NEW.entry_id, v_total_debit, v_total_credit;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE CONSTRAINT TRIGGER trg_assert_double_entry_balance
AFTER INSERT OR UPDATE ON journal_lines
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW
EXECUTE FUNCTION verify_journal_entry_balance();

-- ============================================================================
-- 6. IMMUTABILITY SAFEGUARD (unchanged — this part of the original was correct)
-- ============================================================================

CREATE OR REPLACE FUNCTION prevent_ledger_tampering()
RETURNS TRIGGER AS $$
BEGIN
    RAISE EXCEPTION 'Financial Ledger Immutability Violation: Ledger lines cannot be UPDATED or DELETED. Post a compensating journal entry instead.';
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_no_update_or_delete_lines
BEFORE UPDATE OR DELETE ON journal_lines
FOR EACH ROW
EXECUTE FUNCTION prevent_ledger_tampering();

-- ============================================================================
-- 7. PERFORMANCE INDEXES
-- ============================================================================

CREATE INDEX idx_lines_account_balance
ON journal_lines (account_id, direction, amount);

CREATE INDEX idx_entries_idempotency
ON journal_entries (idempotency_key);

CREATE INDEX idx_entries_bitemporal
ON journal_entries (tenant_id, event_time, assertion_time);
