from app.extensions import db

class CSCConfig(db.Model):
    __tablename__ = "csc_config"
    id = db.Column(db.Integer, primary_key=True)
    directory_json = db.Column(db.Text, nullable=True)

class CSCOfficeOrderFile(db.Model):
    __tablename__ = "csc_office_order_files"
    slug = db.Column(db.String(100), primary_key=True)
    file_name = db.Column(db.String(255), nullable=False)
    file_data = db.Column(db.LargeBinary(length=(2**32)-1), nullable=False)
