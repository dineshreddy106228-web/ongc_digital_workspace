# ONGC Digital Workspace

A modular institutional web application for ONGC offices.  
**Phase-1** delivers the foundation: authentication, role-based access, audit logging, and a dashboard shell.

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
