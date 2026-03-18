---
description: critical safety rules for working in this codebase — read before making any changes
---

# ⚠️ CRITICAL SAFETY RULES — Read First

## 1. NEVER touch the `.env` file

The `.env` file contains production Railway database credentials.
**Do NOT edit, read out loud, or modify `.env` under any circumstances unless the user explicitly says "edit .env".**

Specifically forbidden without explicit instruction:
- Changing `DB_HOST`, `DB_PORT`, `DB_NAME`, `DB_USER`, `DB_PASSWORD`
- Adding or removing any `DATABASE_URL`, `SQLALCHEMY_DATABASE_URI`
- Renaming `RAILWAY_DB_*` variables to `DB_*`

The app connects to **localhost MySQL** in development via the `DB_*` vars.
The `RAILWAY_DB_*` vars are for emergency CLI access only — they are NOT used by the app.

## 2. NEVER run migrations or seed commands against Railway

The following commands are ONLY safe to run locally (against localhost):
```
flask db migrate
flask db upgrade
flask seed-initial-data
flask seed-admin
```

To run migrations against Railway, the user must explicitly provide the Railway DATABASE_URL as a one-off env override and confirm. **Do not assume it is safe to run migrations.**

## 3. Safe commands you CAN run freely

```bash
# Read-only DB inspection of Railway (safe)
mysql -h switchback.proxy.rlwy.net -P 43685 -u root -p<password> railway -e "SELECT ..."

# Local flask commands (always safe)
./venv/bin/flask routes
./venv/bin/flask shell
```

## 4. Two separate databases — keep them separate

| Environment | Host | Used by |
|---|---|---|
| **Local dev** | localhost:3306 / ongc_digital_workspace | Flask app locally |
| **Railway prod** | switchback.proxy.rlwy.net:43685 / railway | Deployed Railway app |

Data flows **one way only**: local dev → push code → Railway auto-deploys. Never push local DB data to Railway.

## 5. Before running any `flask db` or seed command

Always verify which DB the app is pointing to:
```bash
grep "DB_HOST" .env
```
It must say `localhost`. If it says anything else, **stop and ask the user**.
