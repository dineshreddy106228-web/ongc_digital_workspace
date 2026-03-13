"""Import all models so Flask-Migrate / Alembic can detect them."""

from app.models.role import Role                                    # noqa: F401
from app.models.office import Office                                # noqa: F401
from app.models.user import User                                    # noqa: F401
from app.models.user_module_permission import UserModulePermission  # noqa: F401
from app.models.audit_log import AuditLog                          # noqa: F401
from app.models.task import Task                                    # noqa: F401
from app.models.task_update import TaskUpdate                       # noqa: F401
from app.models.activity_log import ActivityLog                     # noqa: F401
from app.models.notification import Notification                    # noqa: F401
from app.models.backup_snapshot import BackupSnapshot              # noqa: F401
