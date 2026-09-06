# Cloud Setup (No Docker) — Neon + Upstash

Replaces the local Docker Postgres/Redis containers with free-tier cloud equivalents. No background service runs on your machine; nothing to install beyond Python packages.

## 1. Create the two cloud databases

**Postgres (Neon):**
1. Go to https://neon.tech, sign up, create a new project.
2. On the project dashboard, copy the connection string — it looks like:
   `postgresql://user:password@ep-xxx.region.aws.neon.tech/dbname?sslmode=require`

**Redis (Upstash):**
1. Go to https://upstash.com, sign up, create a new Redis database (choose a region close to you).
2. Copy the connection string in `rediss://` format (note the extra `s` — this means TLS, required by Upstash):
   `rediss://default:password@xxx.upstash.io:6379`

## 2. Configure this project

Copy `.env.example` to `.env`:
```powershell
Copy-Item .env.example .env
```
Open `.env` in VS Code and paste your two real connection strings in place of the placeholders, so it looks like:
```
DATABASE_URL=postgresql://user:password@ep-xxx.region.aws.neon.tech/dbname?sslmode=require
REDIS_URL=rediss://default:password@xxx.upstash.io:6379
```
Save the file.

## 3. Install packages (includes python-dotenv now)

```powershell
C:\Users\manka\AppData\Local\Programs\Python\Python313\python.exe -m pip install -r requirements.txt requests
```

## 4. Load the schema — via Python instead of docker exec

Since there's no local container to exec into, run this instead:

```powershell
C:\Users\manka\AppData\Local\Programs\Python\Python313\python.exe load_schema_cloud.py
```

(This script is provided below — it connects to your Neon URL from `.env` and runs both SQL files.)

## 5. Start the API

```powershell
C:\Users\manka\AppData\Local\Programs\Python\Python313\python.exe -m uvicorn main:app --reload --port 8000
```
Wait for `startup_complete`. First connection to Neon/Upstash may take 1-3 seconds longer than localhost did — that's expected network latency, not an error.

## 6. Run all tests exactly as before

```powershell
C:\Users\manka\AppData\Local\Programs\Python\Python313\python.exe send_test_webhook.py
C:\Users\manka\AppData\Local\Programs\Python\Python313\python.exe run_all_remaining_tests.py
```

Nothing else changes — same expected outputs as the local Docker run (`MATCHED_EXACT`, `ALREADY_PROCESSED_OR_IN_FLIGHT`, `processed: 2`, `SUSPENSE_BREAK`).

## Honest tradeoffs vs. local Docker

- **Lighter on your machine** — no Docker Desktop background service (which does consume real CPU/RAM even idle), no local containers.
- **Slower per-request** — every DB/Redis call now goes over the internet instead of localhost. For a portfolio demo this is invisible; if you were ever benchmarking real P99 latency numbers (don't — see the earlier note about the fabricated benchmark table), a cloud round-trip would no longer represent an on-prem clearing-house deployment's actual latency profile.
- **Free tiers have limits** — Neon's free tier suspends the database after inactivity (auto-resumes on next query, with a short cold-start delay) and Upstash's free tier caps daily commands. Fine for this project's testing purposes; not something to quote in an interview as "production infrastructure."
