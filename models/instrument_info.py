# models/instrument_info.py
from sqlalchemy import Column, Integer, String, DateTime
from sqlalchemy.ext.declarative import declarative_base
import datetime
from support.base import Base


class Instrument_Info(Base):
    __tablename__ = 'instrument_info'

    id = Column(Integer, primary_key=True)
    cusip = Column(String(20), nullable=False)
    symbol = Column(String(10), nullable=False)
    security_type = Column(String(20), nullable=False)
    description = Column(String(100), nullable=False)
    exchange = Column(String(50), nullable=False)
    asset_type = Column(String(20), nullable=False)
    created_at = Column(DateTime, nullable=False)
    source = Column(String, nullable=False)
    last_updated = Column(DateTime, default=datetime.datetime.utcnow, nullable=False)
    updated_by = Column(String, nullable=False)
