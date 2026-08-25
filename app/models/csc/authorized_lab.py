"""Laboratories authorised to test one chemical against its corporate specification.

Standard Testing Time lives on the corporate register (``qc_testing_standards``)
because QC Laboratory Monitoring measures turnaround against it. Which
laboratories may issue a test report against a specification is a specification
decision, so it is recorded here, keyed by the catalogue reference the
Corporate Specifications pages already address a chemical by.
"""

from datetime import datetime, timezone

from app.extensions import db


class CSCAuthorizedLab(db.Model):
    __tablename__ = "csc_authorized_labs"
    __table_args__ = (
        db.UniqueConstraint("entry_ref", "lab_code", name="uq_csc_authorized_lab_entry_lab"),
        db.Index("ix_csc_authorized_labs_entry_ref", "entry_ref"),
    )

    id = db.Column(db.BigInteger, primary_key=True, autoincrement=True)
    # The catalogue reference: "r-<register row id>" or "s-<specification record id>".
    entry_ref = db.Column(db.String(64), nullable=False)
    lab_code = db.Column(db.String(64), nullable=False)
    updated_by = db.Column(db.BigInteger, db.ForeignKey("users.id", ondelete="SET NULL"), nullable=True)
    updated_at = db.Column(
        db.DateTime,
        nullable=False,
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
    )

    def __repr__(self):
        return f"<CSCAuthorizedLab {self.entry_ref} -> {self.lab_code}>"
