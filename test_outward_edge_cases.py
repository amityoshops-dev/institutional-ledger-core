import requests
import uuid

BASE_URL = "http://localhost:8000"
HEADERS = {
    "X-Tenant-ID": "TENANT_CORP_001",
    "Content-Type": "application/json"
}

print("=" * 70)
print("TEST 1: OUTWARD PAYOUT IDEMPOTENCY LOCK")
print("=" * 70)

unique_instruction = f"OUT-{uuid.uuid4().hex[:8].upper()}"
payout_payload = {
    "source_van": "VAN-HDFC-9920194",
    "target_account_number": "91802001928374",
    "target_ifsc": "HDFC0000240",
    "beneficiary_name": "Supplier Logistics Pvt Ltd",
    "amount": "100.0000",
    "currency": "INR",
    "instruction_id": unique_instruction
}

resp_first = requests.post(f"{BASE_URL}/v1/payouts/outward", json=payout_payload, headers=HEADERS)
resp_dup = requests.post(f"{BASE_URL}/v1/payouts/outward", json=payout_payload, headers=HEADERS)

print("Duplicate Status Code:", resp_dup.status_code)
assert resp_dup.status_code == 409, f"Expected 409 Conflict, got {resp_dup.status_code}: {resp_dup.text}"
print("PASS: Replayed payout successfully rejected.")

print("\n" + "=" * 70)
print("TEST 2: INSUFFICIENT SETTLED BALANCE HARD-STOP")
print("=" * 70)

insufficient_payload = {
    "source_van": "VAN-HDFC-9920194",
    "target_account_number": "91802001928374",
    "target_ifsc": "HDFC0000240",
    "beneficiary_name": "Mega Vendor Corporation",
    "amount": "999999999.0000",
    "currency": "INR",
    "instruction_id": f"OUT-OD-{uuid.uuid4().hex[:6].upper()}"
}

resp_od = requests.post(f"{BASE_URL}/v1/payouts/outward", json=insufficient_payload, headers=HEADERS)
print("Overdraft Status Code:", resp_od.status_code)
assert resp_od.status_code == 400, f"Expected 400 Bad Request, got {resp_od.status_code}: {resp_od.text}"
print("PASS: Overdraft attempt blocked before touching the database.")
