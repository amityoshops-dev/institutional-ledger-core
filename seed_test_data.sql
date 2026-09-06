-- Minimal seed data so a test webhook has somewhere to land.
-- Run AFTER schema_corrected.sql.

INSERT INTO chart_of_accounts (tenant_id, account_number, classification, currency)
VALUES
  ('TENANT_CORP_001', 'NOSTRO-RBI-RTGS', 'NOSTRO_CLEARING', 'INR'),
  ('TENANT_CORP_001', 'FEE-SUSPENSE-001', 'FEE_SUSPENSE', 'INR'),
  ('TENANT_CORP_001', 'BREAK-SUSPENSE-001', 'BREAK_SUSPENSE', 'INR'),
  ('TENANT_CORP_001', 'VAN-HDFC-9920194', 'VIRTUAL_ACCOUNT', 'INR');

-- Minimal invoices table (referenced by main.py's engine but not in the original
-- Module 3 DDL -- it was assumed to already exist. Creating it here so the app
-- actually runs end-to-end.)
CREATE TABLE IF NOT EXISTS invoices (
    invoice_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id VARCHAR(64) NOT NULL,
    assigned_van VARCHAR(64) NOT NULL,
    expected_amount NUMERIC(18,4) NOT NULL,
    status VARCHAR(16) NOT NULL DEFAULT 'PENDING'
);

INSERT INTO invoices (tenant_id, assigned_van, expected_amount, status)
VALUES ('TENANT_CORP_001', 'VAN-HDFC-9920194', 1500000.0000, 'PENDING');
