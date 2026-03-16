"""Import all models so Flask-Migrate / Alembic can detect them."""

from app.models.core.role import Role                                    # noqa: F401
from app.models.office.office import Office                                # noqa: F401
from app.models.core.user import User                                    # noqa: F401
from app.models.core.user_module_permission import UserModulePermission  # noqa: F401
from app.models.core.audit_log import AuditLog                          # noqa: F401
from app.models.tasks.task import Task                                    # noqa: F401
from app.models.tasks.task_collaborator import TaskCollaborator          # noqa: F401
from app.models.tasks.recurring_task_template import RecurringTaskTemplate  # noqa: F401
from app.models.tasks.recurring_task_collaborator import RecurringTaskCollaborator  # noqa: F401
from app.models.tasks.task_update import TaskUpdate                       # noqa: F401
from app.models.core.activity_log import ActivityLog                     # noqa: F401
from app.models.core.notification import Notification                    # noqa: F401
from app.models.core.backup_snapshot import BackupSnapshot              # noqa: F401
from app.models.inventory.inventory_upload import InventoryUpload            # noqa: F401
from app.models.inventory.inventory_record import InventoryRecord            # noqa: F401
from app.models.inventory.inventory_consumption_seed import InventoryConsumptionSeed  # noqa: F401
from app.models.inventory.inventory_procurement_seed import InventoryProcurementSeed  # noqa: F401
from app.models.inventory.material_master import MaterialMaster              # noqa: F401
