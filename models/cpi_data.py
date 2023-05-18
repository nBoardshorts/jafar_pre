# models/cpi_data.py
from sqlalchemy import Column, Integer, String, Float, DateTime
from support.base import Base


class CPI_Data(Base):
    __tablename__ = 'cpi_data'

    id = Column(Integer, primary_key=True)
    series_id = Column(String, nullable=False)
    area_code = Column(String, nullable=False)
    value = Column(Float)
    date = Column(DateTime, nullable=False)
