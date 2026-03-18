# ONGC Digital Workspace

A modular institutional web application for ONGC offices.
**Current version: 1.1** — Includes authentication, role-based access, audit logging, workspace-wide announcements & polls, and the CSC Workflow module under Material Master Management.

## Stack

| Layer          | Technology                  |
|----------------|-----------------------------|
| Backend        | Flask (Python 3.9-3.12)     |
| Database       | MySQL 8.x                   |
| ORM            | SQLAlchemy + Flask-Migrate  |
| Authentication | Flask-Login + Werkzeug      |
| Frontend       | Jinja2 + HTML/CSS/JS        |

---

## Setup Instructions

### 1. Clone the repository

```bash
git clone <repo-url>
cd ongc_digital_workspace
```

### 2. Create a Python virtual environment

Use Python `3.9` through `3.12`. Do not use Python `3.13+` for this repo.
If `python run.py` appears to hang during `import flask` or `import werkzeug`,
the venv was likely created with an unsupported interpreter.

```bash
/usr/bin/python3 -m venv venv
source venv/bin/activate        # Linux / macOS
# venv\Scripts\activate         # Windows
```

### 3. Install dependencies

```bash
pip install -r requirements.txt
```

### 4. Create the MySQL database

```sql
CREATE DATABASE ongc_workspace CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
CREATE USER 'ongc_app'@'localhost' IDENTIFIED BY 'your_strong_password';
GRANT ALL PRIVILEGES ON ongc_workspace.* TO 'ongc_app'@'localhost';
FLUSH PRIVILEGES;
```

### 5. Configure environment variables

```bash
cp .env.example .env
```

Edit `.env` and fill in your actual database credentials and a strong `SECRET_KEY`.  
Generate a secret key:

```bash
python3 -c "import secrets; print(secrets.token_hex(32))"
```

### 6. Run database migrations

```bash
flask db init
flask db migrate -m "Initial schema"
flask db upgrade
```

### 7. Seed initial data

```bash
flask seed-initial-data
```

This creates default roles, the pilot office (Corporate Chemistry, Dehradun), and the bootstrap admin account from `.env` values.

### 8. Run the application

```bash
python run.py
```

Optional runtime overrides:

```bash
HOST=0.0.0.0 PORT=5001 FLASK_DEBUG=0 python run.py
```

Visit **http://localhost:5000** and log in with the bootstrap admin credentials.

> The bootstrap admin is forced to change their password on first login.
> `run.py` now fails fast with a clear error if you start it on an unsupported Python version.

---

## Project Structure

```
ongc_digital_workspace/
├── run.py                  # Entry point
├── config.py               # Environment-based configuration
├── requirements.txt
├── .env.example
├── .gitignore
├── README.md
├── app/
│   ├── __init__.py         # App factory
│   ├── extensions.py       # SQLAlchemy, Migrate, Login, CSRF
│   ├── models/             # ORM models
│   ├── auth/               # Login / logout / change-password
│   ├── main/               # Dashboard
│   ├── admin/              # User management
│   ├── cli/                # Seed commands
│   ├── utils/              # Decorators (roles_required)
│   ├── templates/          # Jinja2 templates
│   └── static/             # CSS, JS
└── migrations/             # Alembic migrations (auto-generated)
```

---

## Security Notes

- All passwords are hashed with PBKDF2-SHA256 (Werkzeug)
- CSRF protection is enabled on all forms (Flask-WTF)
- Session cookies are HTTP-only with SameSite=Lax
- Security headers (X-Content-Type-Options, X-Frame-Options, etc.) set on every response
- Audit logs record login attempts, password changes, and logouts
- Bootstrap admin credentials are used only once during seeding; all users live in MySQL
- Cache-Control headers prevent sensitive pages from being cached

---

## Deployment Readiness Checks

Run these before deployment:

```bash
# Syntax/import sanity
python3 -m compileall app config.py run.py

# Migration status
export FLASK_APP=run.py
flask db heads
flask db current

# Apply pending migrations (if current != head)
flask db upgrade
```

---

## Changelog

### v1.1 — 2026-03-18

**New Features**

- **Announcements & Polling** — Superusers can now create and publish workspace-wide announcements or polls. On publication, every active user receives a modal popup alert on their next page load, and a background polling mechanism surfaces new broadcasts to users already on a loaded page without requiring a refresh. Users can dismiss or open the full announcement; poll responses are recorded and results are visible to superusers in real time.

- **CSC Workflow (Material Master Management Module)** — A structured digital workflow for the Corporate Specification Committees (CSC) has been introduced. This enables the Specification Review Committees and the Material Handling Committee to submit, route, and track structured inputs within the platform.

**Processes Digitized**

- Review Committee Workflow — The end-to-end review cycle for specification committees is now tracked within the system, replacing manual paper-based or email-driven coordination.

**Processes Automated**

- Compilation of Specifications for the 12th Edition of Corporate Specifications of Oil Field Chemicals — The aggregation and compilation step for the 12th Edition is now handled by the platform, eliminating manual data collation across committee inputs.

---

### v1.0 — Initial Release

- Foundation platform: authentication, role-based access control, audit logging, and dashboard shell.
- Admin module: user management, office/module configuration, role assignment.
- Notification system: per-user alerts for assignment and access changes.
- Security hardening: CSRF protection, security response headers, PBKDF2 password hashing, session management.
