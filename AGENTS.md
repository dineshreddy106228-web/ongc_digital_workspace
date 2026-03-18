# AGENTS.md

Repository-wide instructions for any agent working in this repo.

## Priority

These rules are safety-critical. Follow them before touching configuration,
database access, deployment, backups, migrations, or admin restore flows.

## .env Safety

- Never edit `.env` unless the user explicitly says `edit .env`.
- Never print or expose secrets from `.env`.
- Never rename or remap Railway credential variables into the app's active
  local database variables without explicit user approval.
- Treat changes to `DB_HOST`, `DB_PORT`, `DB_NAME`, `DB_USER`, `DB_PASSWORD`,
  `DATABASE_URL`, and `SQLALCHEMY_DATABASE_URI` as high risk.

## Two Databases

This repo uses two separate databases. Keep them separate.

- Local app database:
  - Host: `localhost`
  - Port: `3306`
  - Used by the Flask app locally through `DB_*` variables
- Railway production database:
  - Used by the deployed Railway app
  - Railway access variables may exist separately for emergency/manual access

Do not point the local Flask app at Railway unless the user explicitly asks for
that and confirms it.

## Before Any DB Command

Before running any command that could read from or write to the app database,
verify what the app is pointing to.

Minimum check:

```bash
grep '^DB_HOST=' .env
```

If `DB_HOST` is not `localhost` or `127.0.0.1`, stop and ask the user before
continuing with any DB-affecting action.

## Railway Safety

- Never assume Railway environment variables match local `.env`.
- Never run destructive DB operations against Railway without explicit user
  approval.
- Never restore a SQL backup into Railway casually or as part of debugging.
- Never push local database contents to Railway.

## Migrations And Seed Commands

Treat all migration and seed commands as local-only unless the user explicitly
asks to target Railway.

Examples:

- `flask db migrate`
- `flask db upgrade`
- `flask seed-initial-data`
- `flask seed-admin`

Before running any of the above, verify the target database is local.

## Backups And Restore

- Prefer taking a fresh backup before risky operations, deploys, restores, or
  schema work.
- Remember that the app's backup download is a full MySQL backup of the current
  configured database.
- Restore/import flows are destructive and overwrite current data.
- If the user is about to deploy and is worried about data, recommend a backup
  first.

## Deploy Safety

- `git push` deploys code, not local `.env`.
- Do not claim a deploy is safe unless you have checked whether startup scripts,
  migrations, restore hooks, or admin actions could affect production data.
- If asked whether a push can lose data, inspect deploy config and app startup
  behavior before answering.

## Existing Safety Notes

There is an additional local safety note at:

- `.agents/workflows/safety-rules.md`

Agents may read it for extra context, but the rules in this `AGENTS.md` are the
minimum required behavior.
