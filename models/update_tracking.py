#models/update_tracking.py
import datetime

from sqlalchemy import Column, Integer, String, DateTime, JSON
from gitignore.config import POSTGRES_HOST, POSTGRES_PORT, POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_DB
from support.base import Base

class Update_Tracking(Base):
    __tablename__ = 'update_tracking'

    id = Column(Integer, primary_key=True)
    table_name = Column(String(255), nullable=False, unique=True)
    frequency = Column(JSON, nullable=True)
    last_updated = Column(DateTime, default=datetime.datetime.utcnow, nullable=False)
    backup1_date = Column(DateTime)
    backup1_path = Column(String)
    backup2_date = Column(DateTime)
    backup2_path = Column(String)
    source = Column(String)

    def __repr__(self):
        return f"<Update_Tracking(table_name='{self.table_name}', frequency={self.frequency}, last_update='{self.last_update}', backup1_path='{self.backup1_path}', backup2_path='{self.backup2_path}', backup1_date='{self.backup1_date}', backup2_date='{self.backup2_date}', source='{self.source}')>"