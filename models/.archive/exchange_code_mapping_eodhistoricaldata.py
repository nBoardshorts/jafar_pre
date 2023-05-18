# models/exchange_code_mapping_eodhistoricaldata.py

from sqlalchemy import Column, String, Integer, ForeignKey
from sqlalchemy.orm import relationship
from support.base import Base

# For mapping symbols to country code for iterating for api requests to eodhistoricaldata.com

class Exchange_Code_Mapping_EODHistoricalData(Base):
    __tablename__ = 'exchange_code_mapping_eodhistoricaldata'

    id = Column(Integer, primary_key=True)
    exchange = Column(String, nullable=False, unique=True)
    exchange_code = Column(String, nullable=False)
    symbols = relationship("Symbol_EODHistoricalData", back_populates="exchange_code_mapping_eodhistoricaldata")

    def __repr__(self):
        return f"<Exchange_Code_Mapping_EODHistoricalData(exchange='{self.exchange}', exchange_code='{self.exchange_code}')>"

