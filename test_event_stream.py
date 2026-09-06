import requests

BASE_URL = "http://localhost:8000"
HEADERS = {"X-Tenant-ID": "TENANT_CORP_001"}

# Read back from stream endpoint
resp = requests.get(f"{BASE_URL}/v1/events/stream", headers=HEADERS)
print("Stream Status:", resp.status_code)
print("Stream Events Found:", len(resp.json().get("events", [])))
