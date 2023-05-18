# models/symbol_eodhistoricaldata.py
from sqlalchemy import Column, String, Integer, DateTime, CheckConstraint, ForeignKey
from sqlalchemy.orm import relationship
import datetime
from support.base import Base


# Purpose: This file defines the Symbol_EODHistoricalData model, representing a single symbol in the database.
#
# Workflow:
# 1. Import necessary dependencies from SQLAlchemy and helper modules.
# 2. Define the Symbol_EODHistoricalData class, inheriting from Base.
# 3. Define the columns and relationships for the Symbol_EODHistoricalData model.
# 4. Implement class methods for handling symbol data (e.g., from_eodhistorical).
#
# Criteria:
# 1. The model should include the following fields: id, symbol, name, country, exchange, exhange_code, currency,
#    assetType, isin, status, source, last_updated, and updated_by.
# 2. The 'status' field should be a string, with possible values 'active' or 'inactive'.
# 3. The 'last_updated' field should store a DateTime object representing the last time the symbol was updated.
# 4. The 'updated_by' field should store a string representing the name of the updater that performed the last update.
# 5. The 'exhange_code' field is determined by a helper function at helpers/symbol_eodhistoricaldata_helpers.py - clean_symbols_from_eodhistorical() 
#
# Usage Notes:
# - Use the from_eodhistorical method to create a new Symbol_EODHistoricalData instance from EOD Historical Data.


class Symbol_EODHistoricalData(Base):
    __tablename__ = 'symbols_eodhistoricaldata'


    id = Column(Integer, primary_key=True)
    symbol = Column(String, nullable=False)
    name = Column(String, nullable=False)
    country = Column(String, nullable=True)
    exchange = Column(String, nullable=True)
    currency = Column(String, nullable=False)
    assetType = Column(String, nullable=True)
    isin = Column(String, nullable=True)
    status = Column(String, CheckConstraint("status IN ('active', 'inactive')"), nullable=False)
    source = Column(String, nullable=False)
    last_updated = Column(DateTime, default=datetime.datetime.utcnow, nullable=False)
    updated_by = Column(String, nullable=False)
    # Relationships
    code = Column(String(50), ForeignKey('entities.code'))
    entity = relationship("Entity", back_populates="symbols_eodhistoricaldata")

    def __repr__(self):
        return f"<Symbol_EODHistoricalData(symbol='{self.symbol}', name='{self.name}', country='{self.country}', exchange='{self.exchange}', currency='{self.currency}', assetType='{self.assetType}', isin='{self.isin}', status='{self.status}', source='{self.source}', last_updated='{self.last_updated}', updated_by='{self.updated_by}')>"
    
