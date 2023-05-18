# helpers/exchange_code_mapping_helper.py
from models import Exchange_Code_Mapping_EODHistoricalData
from models import Exchange_EODHistoricalData
from support.db import DB

def map_exchanges_to_exchange_code():
    db = DB()

    with db.session_scope() as session:
        # Delete all existing data from Exchange_Code_Mapping_EODHistoricalData table
        session.query(Exchange_Code_Mapping_EODHistoricalData).delete()

        # Get all unique exchanges from Exchange_EODHistoricalData table
        exchanges = session.query(Exchange_EODHistoricalData).distinct(Exchange_EODHistoricalData.Code)

        # Create Exchange_Code_Mapping_EODHistoricalData objects for each exchange
        for exchange in exchanges:
            exchange_code_mapping = Exchange_Code_Mapping_EODHistoricalData(
                exchange=exchange.Name,
                exchange_code=exchange.Code
            )
            
            # Add Exchange_Code_Mapping_EODHistoricalData object to session
            session.add(exchange_code_mapping)
    
