"""Record which laboratories are authorised to test against a specification.

Corporate Specifications Management now carries an Administration page holding
the data kept against a specification rather than inside it. Standard Testing
Time already lives on the corporate register that QC Laboratory Monitoring
maintains, so it is read from there; the authorised testing laboratories had
nowhere to live and are recorded here.

The row is keyed by the catalogue reference the module already addresses a
chemical by — ``r-<register row id>`` for a register chemical, ``s-<record id>``
for a specification held off the register.

Revision ID: c3d4e5f6a7b8
Revises: b1c2d3e4f5a6
"""
import sqlalchemy as sa
from alembic import op

revision = "c3d4e5f6a7b8"
down_revision = "b1c2d3e4f5a6"
branch_labels = None
depends_on = None


def upgrade():
    inspector = sa.inspect(op.get_bind())
    if "csc_authorized_labs" in set(inspector.get_table_names()):
        return
    op.create_table(
        "csc_authorized_labs",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("entry_ref", sa.String(length=64), nullable=False),
        sa.Column("lab_code", sa.String(length=64), nullable=False),
        sa.Column("updated_by", sa.BigInteger(), nullable=True),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.ForeignKeyConstraint(["updated_by"], ["users.id"], ondelete="SET NULL"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("entry_ref", "lab_code", name="uq_csc_authorized_lab_entry_lab"),
    )
    op.create_index("ix_csc_authorized_labs_entry_ref", "csc_authorized_labs", ["entry_ref"])


def downgrade():
    inspector = sa.inspect(op.get_bind())
    if "csc_authorized_labs" not in set(inspector.get_table_names()):
        return
    op.drop_index("ix_csc_authorized_labs_entry_ref", table_name="csc_authorized_labs")
    op.drop_table("csc_authorized_labs")
