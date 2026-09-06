import requests

BASE_URL = "http://localhost:8000/v1/accounts"
HEADERS = {"X-Tenant-ID": "TENANT_CORP_001"}

accounts = [
    "VAN-HDFC-9920194",
    "NOSTRO-RBI-RTGS",
    "FEE-SUSPENSE-001",
    "BREAK-SUSPENSE-001"
]

print("=" * 70)
print("REAL-TIME LEDGER ACCOUNT BALANCES")
print("=" * 70)

for acct in accounts:
    resp = requests.get(f"{BASE_URL}/{acct}/balance", headers=HEADERS)
    if resp.status_code == 200:
        data = resp.json()
        print(f"Account: {data['account_number']:<20} | Type: {data['classification']:<18} | Net Balance: {data['net_settled_balance']:>14.4f} {data['currency']}")
    else:
        print(f"Account: {acct:<20} | Error {resp.status_code}: {resp.text}")
