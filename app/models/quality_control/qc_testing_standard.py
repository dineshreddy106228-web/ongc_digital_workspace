from datetime import datetime, timezone
from app.extensions import db


class QCTestingStandard(db.Model):
    __tablename__ = "qc_testing_standards"
    id = db.Column(db.BigInteger, primary_key=True, autoincrement=True)
    chemical_name = db.Column(db.String(255), nullable=False)
    normalized_name = db.Column(db.String(255), nullable=False, unique=True)
    specification_no = db.Column(db.String(255), nullable=True)
    material_code = db.Column(db.String(100), nullable=True)
    standard_days = db.Column(db.Integer, nullable=True)
    remarks = db.Column(db.Text, nullable=True)
    updated_by = db.Column(db.BigInteger, db.ForeignKey("users.id", ondelete="SET NULL"), nullable=True)
    updated_at = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc), onupdate=lambda: datetime.now(timezone.utc), nullable=False)
