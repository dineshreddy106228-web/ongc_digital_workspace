# User Governance V2 — Deployment & Testing Guide

**ONGC Digital Workspace · HCC Pilot**
_Generated after implementation of Governance V2 changes_

---

## 1. Deployment Steps (Run Once)

Run these commands from your project root after activating the virtualenv:

```bash
# 1. Apply the new database migration
flask db upgrade

# 2. Seed the new super_user role (safe to re-run)
flask seed-initial-data

# 3. Assign default module permissions to all existing users
flask seed-module-permissions
```

**What `flask seed-module-permissions` does:**

| Role | Modules Granted |
|------|----------------|
| super_admin | dashboard, tasks, inventory, csc, reports |
| super_user | dashboard, tasks, inventory, csc, reports |
| admin | dashboard, admin_users |
| owner / supporting_officer / viewer | dashboard, tasks |

> ⚠️ After this step, individual users can have their modules customised via **Admin → Users → Edit User**.

---

## 2. New Database Columns Reference

### `users` table (added)
| Column | Type | Description |
|--------|------|-------------|
| `controlling_officer_id` | BIGINT FK → users.id | The officer who directly controls this user's day-to-day tasks |
| `reviewing_officer_id` | BIGINT FK → users.id | The reviewing/reporting officer (one level above) |

### `tasks` table (added)
| Column | Type | Default | Description |
|--------|------|---------|-------------|
| `task_scope` | VARCHAR(50) | `MY` | Task visibility: `MY`, `TEAM`, or `GLOBAL` |

### `user_module_permissions` table (new)
| Column | Type | Description |
|--------|------|-------------|
| `user_id` | BIGINT FK | Reference to user |
| `module_code` | VARCHAR(100) | One of: dashboard, tasks, inventory, csc, reports, admin_users |
| `can_access` | BOOLEAN | `True` = access granted |

---

## 3. Task Visibility Rules

| Scope | Who Can See It |
|-------|---------------|
| **MY** | The task owner + their controlling officer + privileged roles (super_admin, super_user, admin) |
| **TEAM** | The task owner + their controlling officer + their reviewing officer + privileged roles |
| **GLOBAL** | All users who have `tasks` module access |

Only **super_admin**, **super_user**, and **admin** can create `GLOBAL` tasks.

---

## 4. Step-by-Step Testing Guide

### Test 1: Create a user with only CSC access

1. Log in as **super_admin** or **admin**.
2. Go to **Admin → Users → Create User**.
3. Fill in name, username, password. Set **Role** to `user`.
4. Under **Reporting Hierarchy** — leave both dropdowns as `(none)`.
5. Under **Module Access** — check only **CSC Workflow (`csc`)**.
6. Save. Log out.
7. Log in as the new CSC-only user.

**Expected outcome:**
- Navigation shows only **Dashboard** (no Task Tracker, no Users link).
- Dashboard shows only the CSC module card and the Dashboard card.
- Navigating directly to `/tasks/` redirects with "You do not have access to this module."

---

### Test 2: Confirm CSC user sees only CSC + Dashboard

Continuing from Test 1 while logged in as the CSC user:

1. Check the nav bar — only **Dashboard** and **Change Password** links are visible.
2. On the Dashboard, confirm only **Dashboard** card and **CSC Workflow** card are shown.
3. Attempt direct URL access: `/tasks/`, `/admin/users/` — both should return 403 / redirect with flash error.
4. Log back in as admin to verify the user's module permissions are correctly saved (Edit User → Module Access section should show only CSC checked).

---

### Test 3: Create a user with tasks access and hierarchy mapping

1. Log in as **super_admin** or **admin**.
2. Create three users (or use existing ones):
   - **UserA** — role `user`, modules: `dashboard`, `tasks`. No hierarchy set.
   - **UserB** — role `user`, modules: `dashboard`, `tasks`. Set **Controlling Officer** = UserA.
   - **UserC** — role `user`, modules: `dashboard`, `tasks`. Set **Controlling Officer** = UserA, **Reviewing Officer** = an admin/super user.
3. As super_admin, create sample tasks:
   - Task "Global Notice" — scope: **GLOBAL**
   - Task "My Personal Task" — scope: **MY**, owner: UserB
   - Task "Team Task" — scope: **TEAM**, owner: UserB

---

### Test 4: Confirm My / Team / Global task visibility

**Log in as UserB (controlling officer = UserA):**
- ✓ Should see "Global Notice" under **Global Tasks** section.
- ✓ Should see "My Personal Task" under **My Tasks** section (UserB is the owner).
- ✓ Should see "Team Task" under **Team & Other Tasks** section (UserB is owner).

**Log in as UserA (is UserB's controlling officer):**
- ✓ Should see "Global Notice" under **Global Tasks**.
- ✓ Should see "My Personal Task" under **My Tasks** (as controlling officer of UserB).
- ✓ Should see "Team Task" under **Team & Other Tasks** (as controlling officer).
- ✗ Should NOT see tasks owned by unrelated users.

**Log in as UserC (reviewing officer set):**
- ✓ Should see "Global Notice".
- ✓ As reviewing officer for UserB's tasks scoped TEAM, should see "Team Task".
- ✗ Should NOT see "My Personal Task" (MY scope — only owner + controlling officer can see).

**Log in as super_admin or super_user:**
- ✓ Should see ALL tasks regardless of scope or owner.

---

### Test 5: Confirm unauthorized modules are hidden and blocked

For each scenario below, verify both the **UI hides the link** and **direct URL access is blocked**:

| User type | Accessible | Blocked |
|-----------|-----------|---------|
| CSC-only user | `/`, `/csc/` | `/tasks/`, `/admin/users/` |
| Tasks-only user | `/`, `/tasks/` | `/admin/users/`, `/csc/` |
| Admin (no tasks module) | `/`, `/admin/users/` | `/tasks/` (unless tasks module was also assigned) |
| super_user | All business modules | `/admin/users/` (admin_users is admin-only) |
| super_admin | Everything | — |

**How to test a blocked route:**
1. Log in as the restricted user.
2. Navigate directly to the blocked URL (type in browser address bar).
3. Expected: flash message "You do not have access to this module" and redirect to dashboard, OR HTTP 403 page.

---

## 5. Quick Smoke Test (CLI)

After deployment, you can run this one-liner to confirm the model structure is intact:

```bash
flask shell <<'EOF'
from app.models import User, Role, Task
from app.models.user_module_permission import UserModulePermission, SUPPORTED_MODULES
from app.utils.decorators import module_access_required
from app.models.task import TASK_SCOPES

print("Models:", User, Role, Task, UserModulePermission)
print("TASK_SCOPES:", TASK_SCOPES)
print("SUPPORTED_MODULES:", [code for code, _ in SUPPORTED_MODULES])

u = User.query.first()
if u:
    print(f"User '{u.username}' — has_module_access('tasks'):", u.has_module_access('tasks'))
    print(f"  is_super_admin: {u.is_super_admin()}, is_super_user: {u.is_super_user()}, is_admin: {u.is_admin_user()}")
    print(f"  accessible_modules: {u.get_accessible_module_codes()}")
else:
    print("No users yet — run flask seed-initial-data first")
EOF
```

---

## 6. Summary of All Changed Files

| File | Change Type | Description |
|------|-------------|-------------|
| `app/models/user_module_permission.py` | **NEW** | UserModulePermission model + module constants |
| `app/models/user.py` | Modified | Hierarchy FKs, governance helper methods |
| `app/models/task.py` | Modified | `task_scope` column + `TASK_SCOPES` constant |
| `app/models/__init__.py` | Modified | Import UserModulePermission for Alembic detection |
| `app/utils/decorators.py` | Modified | Added `module_access_required(module_code)` decorator |
| `app/admin/routes.py` | Modified | Governance V2 create/edit user flows, audit logs |
| `app/tasks/routes.py` | Modified | Scope-based visibility query, task_scope field |
| `app/templates/base.html` | Modified | Conditional nav links, role badge |
| `app/templates/main/dashboard.html` | Modified | Conditional module cards, hierarchy info |
| `app/templates/admin/create_user.html` | Modified | Hierarchy dropdowns + module checkboxes |
| `app/templates/admin/edit_user.html` | Modified | Hierarchy dropdowns + module checkboxes |
| `app/templates/tasks/list.html` | Modified | Scope-sectioned layout with Jinja macros |
| `app/templates/tasks/create.html` | Modified | `task_scope` select field |
| `app/templates/tasks/edit.html` | Modified | `task_scope` select field |
| `migrations/versions/3f9a21c85d4e_user_governance_v2.py` | **NEW** | Alembic migration (no table drops) |
| `app/cli/seed.py` | Modified | super_user role + seed-module-permissions command |
| `app/cli/__init__.py` | Modified | Registers new CLI command |
| `app/static/css/admin.css` | Modified | Governance V2 CSS classes |
