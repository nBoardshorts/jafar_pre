# models/echange_eodhistoricaldata.py
import pandas as pd
from sqlalchemy import Column, String, Integer, Boolean, JSON
from sqlalchemy.orm import relationship
from support.base import Base


class Exchange_EODHistoricalData(Base):
    __tablename__ = 'exchanges_eodhistoricaldata'

    id = Column(Integer, primary_key=True)
    Name = Column(String, nullable=False)
    Code = Column(String, nullable=False)
    OperatingMIC = Column(String, nullable=True)
    Country = Column(String, nullable=False)
    Currency = Column(String, nullable=False)
    CountryISO2 = Column(String, nullable=True)
    CountryISO3 = Column(String, nullable=True)
    Timezone = Column(String, nullable=True)
    trading_hours = Column(JSON, nullable=True)
    holidays = Column(JSON, nullable=True)
    # Relationships
    entities = relationship("Entity", back_populates="exchange_data")
    
    
    def __repr__(self):
        return f"<Exchange(Name='{self.Name}', Code='{self.Code}', OperatingMIC='{self.OperatingMIC}', Country='{self.Country}', Currency='{self.Currency}', CountryISO2='{self.CountryISO2}', CountryISO3='{self.CountryISO3}')>"
