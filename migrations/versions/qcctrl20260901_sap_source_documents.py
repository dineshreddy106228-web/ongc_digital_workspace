"""Hold each SAP upload's workbooks once, and keep only the current pair.

A central upload writes one batch per laboratory, and every one of them was
made from the same two files, so the bytes were stored six or seven times over.
They move to a shared document each batch points at.

The retention rule changes with them.  The SAP exports were kept for the
fifteen-day rollback window, but an earlier pair cannot re-create the current
position -- re-importing it would push SAP's fields backwards -- so only the
newest upload is retained. This migration therefore keeps the bytes of the most
recent upload and discards the rest, which is the state the new rule maintains.
Inventory and the weekly QC workbook are untouched and keep their window.

Revision ID: qcctrl20260901_sap_docs
Revises: qcctrl20260901_sap_fy
"""

import datetime

from alembic import op
import sqlalchemy as sa


revision = "qcctrl20260901_sap_docs"
down_revision = "qcctrl20260901_sap_fy"
branch_labels = None
depends_on = None


def upgrade():
    bind = op.get_bind()
    longblob = sa.LargeBinary().with_variant(
        sa.dialects.mysql.LONGBLOB(), "mysql",
    )

    op.create_table(
        "qc_sap_source_documents",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("inspection_filename", sa.String(length=255), nullable=False),
        sa.Column("inspection_content_type", sa.String(length=120), nullable=False),
        sa.Column("inspection_file_size", sa.BigInteger(), nullable=False, server_default="0"),
        sa.Column("inspection_source_data", longblob, nullable=False),
        sa.Column("notification_filename", sa.String(length=255), nullable=False),
        sa.Column("notification_content_type", sa.String(length=120), nullable=False),
        sa.Column("notification_file_size", sa.BigInteger(), nullable=False, server_default="0"),
        sa.Column("notification_source_data", longblob, nullable=False),
        sa.Column("purged_at", sa.DateTime(), nullable=True),
        sa.Column("uploaded_by", sa.BigInteger(), nullable=True),
        sa.Column("uploaded_at", sa.DateTime(), nullable=False),
        sa.ForeignKeyConstraint(["uploaded_by"], ["users.id"], ondelete="SET NULL"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_qc_sap_source_documents_uploaded_at", "qc_sap_source_documents", ["uploaded_at"],
    )
    op.add_column(
        "qc_sap_upload_batches", sa.Column("source_document_id", sa.BigInteger(), nullable=True),
    )
    op.create_foreign_key(
        "fk_qc_sap_upload_batches_source_document",
        "qc_sap_upload_batches", "qc_sap_source_documents",
        ["source_document_id"], ["id"], ondelete="SET NULL",
    )

    # Carry over the newest upload only. Batches of one upload share an
    # uploaded_at second and the same two file names.
    newest = bind.execute(sa.text(
        """
        SELECT id, inspection_filename, inspection_content_type, inspection_file_size,
               notification_filename, notification_content_type, notification_file_size,
               uploaded_by, uploaded_at
          FROM qc_sap_upload_batches
         WHERE source_purged_at IS NULL
           AND inspection_source_data IS NOT NULL
           AND LENGTH(inspection_source_data) > 0
         ORDER BY uploaded_at DESC, id DESC
         LIMIT 1
        """
    )).mappings().first()

    if newest is not None:
        bind.execute(sa.text(
            """
            INSERT INTO qc_sap_source_documents (
                inspection_filename, inspection_content_type, inspection_file_size,
                inspection_source_data,
                notification_filename, notification_content_type, notification_file_size,
                notification_source_data,
                purged_at, uploaded_by, uploaded_at
            )
            SELECT inspection_filename, inspection_content_type, inspection_file_size,
                   inspection_source_data,
                   notification_filename, notification_content_type, notification_file_size,
                   notification_source_data,
                   NULL, uploaded_by, uploaded_at
              FROM qc_sap_upload_batches
             WHERE id = :batch_id
            """
        ), {"batch_id": newest["id"]})
        document_id = bind.execute(
            sa.text("SELECT id FROM qc_sap_source_documents ORDER BY id DESC LIMIT 1")
        ).scalar()
        # Every batch written by that same upload shares the document.  One
        # upload writes a batch per laboratory over a couple of seconds, so the
        # group is matched on its file names within a window rather than on an
        # exact uploaded_at, which would leave the earlier seconds unlinked.
        bind.execute(sa.text(
            """
            UPDATE qc_sap_upload_batches
               SET source_document_id = :document_id
             WHERE inspection_filename = :inspection_filename
               AND notification_filename = :notification_filename
               AND uploaded_at BETWEEN :window_start AND :uploaded_at
            """
        ), {
            "document_id": document_id,
            "uploaded_at": newest["uploaded_at"],
            "window_start": newest["uploaded_at"] - datetime.timedelta(minutes=10),
            "inspection_filename": newest["inspection_filename"],
            "notification_filename": newest["notification_filename"],
        })

    op.drop_column("qc_sap_upload_batches", "inspection_source_data")
    op.drop_column("qc_sap_upload_batches", "notification_source_data")
    op.drop_column("qc_sap_upload_batches", "source_purged_at")

    # InnoDB keeps the dropped blobs' pages until the table is rebuilt, so the
    # space this migration exists to release is only returned here.
    if bind.dialect.name == "mysql":
        op.execute("OPTIMIZE TABLE qc_sap_upload_batches")


def downgrade():
    longblob = sa.LargeBinary().with_variant(sa.dialects.mysql.LONGBLOB(), "mysql")
    op.add_column("qc_sap_upload_batches", sa.Column("source_purged_at", sa.DateTime(), nullable=True))
    op.add_column("qc_sap_upload_batches", sa.Column("notification_source_data", longblob, nullable=False))
    op.add_column("qc_sap_upload_batches", sa.Column("inspection_source_data", longblob, nullable=False))
    op.drop_constraint(
        "fk_qc_sap_upload_batches_source_document", "qc_sap_upload_batches", type_="foreignkey",
    )
    op.drop_column("qc_sap_upload_batches", "source_document_id")
    op.drop_index("ix_qc_sap_source_documents_uploaded_at", table_name="qc_sap_source_documents")
    op.drop_table("qc_sap_source_documents")
