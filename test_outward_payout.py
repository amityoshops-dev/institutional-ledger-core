import requests

BASE_URL = "http://localhost:8000"
HEADERS = {
    "X-Tenant-ID": "TENANT_CORP_001",
    "Content-Type": "application/json"
}

# 1. Check balance before outward payout
pre_bal = requests.get(f"{BASE_URL}/v1/accounts/VAN-HDFC-9920194/balance", headers=HEADERS).json()
print("=" * 70)
print(f"BALANCE BEFORE OUTWARD PAYOUT: {pre_bal['net_settled_balance']} {pre_bal['currency']}")
print("=" * 70)

# 2. Issue an outward payout of 500,000 INR
payout_payload = {
    "source_van": "VAN-HDFC-9920194",
    "target_account_number": "91802001928374",
    "target_ifsc": "HDFC0000240",
    "beneficiary_name": "Supplier Logistics Pvt Ltd",
    "amount": "500000.0000",
    "currency": "INR",
    "instruction_id": "OUT-PAY-TEST-001"
}

resp = requests.post(f"{BASE_URL}/v1/payouts/outward", json=payout_payload, headers=HEADERS)
print("Payout Response Status:", resp.status_code)

if resp.status_code == 200:
    data = resp.json()
    print("Status            :", data["status"])
    print("Entry ID          :", data["entry_id"])
    print("Disbursed Amount  :", data["amount_disbursed"])
    print("Remaining Balance :", data["remaining_balance"])
    print("\nGenerated ISO 20022 pacs.008 Document:")
    print("-" * 70)
    print(data["pacs_008_xml"])
    print("-" * 70)
else:
    print("Error:", resp.text)

# 3. Check balance after outward payout
post_bal = requests.get(f"{BASE_URL}/v1/accounts/VAN-HDFC-9920194/balance", headers=HEADERS).json()
print("=" * 70)
print(f"BALANCE AFTER OUTWARD PAYOUT:  {post_bal['net_settled_balance']} {post_bal['currency']}")
print("=" * 70)
