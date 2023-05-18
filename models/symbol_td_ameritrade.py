# models/td_ameritrade_symbol.py
import json
from sqlalchemy import Column, String, Integer, DateTime, CheckConstraint, ForeignKey
from sqlalchemy.orm import relationship
import datetime
from support.base import Base
from helpers.data_helper import remove_unknowns_and_blank_remnants_to_none
from helpers.logging_helper import configure_logging, logger, log_exception
#from models.historical_price_data import Historical_Price_Data

# See symbols_handler.py in support folder

# Purpose: This file defines the Symbol model, which represents a single symbol in the database.
#
# Workflow:
# 1. Import necessary dependencies from SQLAlchemy.
# 2. Define the Symbol class, inheriting from Base.
# 3. Define the columns and relationships for the Symbol model.
# 4. Implement class methods for handling symbol data (e.g., from_td_ameritrade, handle_exchanges, handle_duplicate_cusip).
#
# Criteria:
# 1. The model should include the following fields: id, symbol, cusip, description, exchanges, assetType, status, source, last_updated, and updated_by.
# 2. The 'status' field should be a string, with possible values 'active' or 'inactive'.
# 3. The 'last_updated' field should store a DateTime object representing the last time the symbol was updated.
# 4. The 'updated_by' field should store a string representing the name of the updater that performed the last update.
#
# Usage Notes:
# - Use the from_td_ameritrade method to create a new Symbol instance from TD Ameritrade data.
# - Use the handle_exchanges method to update the exchanges attribute based on existing and new exchange data.
# - Use the handle_duplicate_cusip method to check for duplicate CUSIPs and handle them as needed.

configure_logging()

class Symbol_TD_Ameritrade(Base):
    __tablename__ = 'symbols_td_ameritrade'

    id = Column(Integer, primary_key=True)
    symbol = Column(String, nullable=False, unique=True)
    cusip = Column(String, nullable=True)
    description = Column(String, nullable=True)
    exchanges = Column(String, nullable=True)  # Store exchanges as JSON string
    assetType = Column(String, nullable=True)
    status = Column(String, CheckConstraint("status IN ('active', 'inactive')"), nullable=False)
    source = Column(String, nullable=False)
    last_updated = Column(DateTime, default=datetime.datetime.utcnow, nullable=False)
    updated_by = Column(String, nullable=False)
    # Relationships
    code = Column(String(50), ForeignKey('entities.code'))
    entity = relationship("Entity", back_populates="symbols_td_ameritrade")


    

    @classmethod
    def from_td_ameritrade(cls, td_ameritrade_data, source, last_updated=None, updated_by=None):
        # Clean data
        cleaned_data = remove_unknowns_and_blank_remnants_to_none(td_ameritrade_data)

        # Commented out the below lines because i decided just to let exchanges be because we will instead
        # have to join on symbol with other tables and drop duplicate columns because the format for exchanges is not the same
        # and too much time to find a way to normalize, etc.
        # Call handle_exchanges to update the exchanges attribute based on existing and new exchange data.
        #try:
        #    exchanges = cleaned_data.get('exchange')
        #    cleaned_data['exchanges'] = TD_Ameritrade_Symbol.handle_exchanges(existing_exchanges=None, new_exchange=exchanges)
        #except Exception as e:
        #    log_exception(e)
        #    logger.warning(f'Error handling exchanges for symbol {cleaned_data.get("symbol")} from TD Ameritrade data: {e}')

        # Create a new Symbol instance
        symbol_instance = cls(
            symbol=cleaned_data.get('symbol'),
            cusip=cleaned_data.get('cusip'),
            description=cleaned_data.get('description'),
            exchanges=cleaned_data.get('exchange'),
            assetType=cleaned_data.get('assetType'),
            status=cleaned_data.get('status'),
            source=source,
            last_updated=last_updated,
            updated_by=updated_by,
        )
        
        return symbol_instance

    
    @classmethod
    def handle_exchanges(cls, existing_exchanges, new_exchange):
        if existing_exchanges:
            existing_exchanges_list = json.loads(existing_exchanges)
        else:
            existing_exchanges_list = []

        if new_exchange and new_exchange not in existing_exchanges_list:
            existing_exchanges_list.append(new_exchange)

        if existing_exchanges_list:
            return json.dumps(existing_exchanges_list)
        else:
            return None

 #   def __repr__(self):
 #       return f"<Symbol(symbol='{self.symbol}', cusip='{self.cusip}', description='{self.description}', exchange='{self.exchange}', assetType='{self.assetType}', status='{self.status}', source='{self.source}', last_updated='{self.last_updated}', updated_by='{self.updated_by}')>"


