# app/models.py
from flask_sqlalchemy import SQLAlchemy
from datetime import datetime
import uuid  # <--- NEW: Import UUID library

db = SQLAlchemy()

class Job(db.Model):
    __tablename__ = 'jobs'

    # Core Identifiers
    # NEW: Added 'default=...' to automatically generate a unique ID
    id = db.Column(db.String, primary_key=True, default=lambda: str(uuid.uuid4()))
    
    source = db.Column(db.String(50), nullable=False)
    source_job_id = db.Column(db.String, nullable=False)

    # Job Details
    title = db.Column(db.String, nullable=False)
    company = db.Column(db.String)
    location = db.Column(db.String)
    url = db.Column(db.Text, nullable=False)
    description = db.Column(db.Text)
    
    # Filtering & Logic
    job_type = db.Column(db.String(50))
    posted_date = db.Column(db.Date)
    
    # The "Smart" Columns
    is_active = db.Column(db.Boolean, default=True)
    first_seen_at = db.Column(db.DateTime, default=datetime.utcnow)
    last_seen_at = db.Column(db.DateTime, default=datetime.utcnow)

    # Unique Constraint
    __table_args__ = (
        db.UniqueConstraint('source', 'source_job_id', name='unique_job_source'),
    )

    def to_dict(self):
        return {
            'id': self.id,
            'title': self.title,
            'company': self.company,
            'location': self.location,
            'url': self.url,
            'source': self.source,
            'posted_date': self.posted_date.isoformat() if self.posted_date else None
        }