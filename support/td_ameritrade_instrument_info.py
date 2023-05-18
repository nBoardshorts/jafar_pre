# support/td_ameritrade_instrument.py
import json
from support.db import DB
from helpers.time_helper import get_current_utc_datetime
from support.td_client_wrapper import TD_Client_Wrapper
from models.instrument_info import Instrument_Info
from helpers.db_query_helper import get_symbol_cusip
from helpers.logging_helper import configure_logging, log_exception, logger


# Initialize logging for the td_ameritrade_instrument script
configure_logging()

class TD_Ameritrade_Instrument_Info:
    def __init__(self, user='TD_Ameritrade_Instrument_Updater'):
        self.td_client = TD_Client_Wrapper.get_instance().get_client()
        self.source = "TD Ameritrade"  # Hardcoded source
        self.updater_name = user
        # Create database session
        self.db = DB()

    def run_tda_enum(self, enum_type, enum_value):
        '''Run an input value through the TD Ameritrade API enum logic.'''
        try:
            enum_class = getattr(self.td_client.Instrument, enum_type)
            enum_value = enum_class(enum_value)
            return enum_value
        except Exception as e:
            log_exception(e)
            logger.error(f"Error running input through TD Ameritrade API enum logic: {e}")
            return None

    def search_instruments(self, symbols, projection='symbol-search'):
        """
        Search or retrieve instrument data, including fundamental data.
        """
        try:
            projection = self.run_tda_enum('Projection', projection) if projection is not None else None

            response = self.td_client.search_instruments(
                symbols=symbols,
                projection=projection
            )

            if response.status_code == 200:
                instrument_data = response.json()
                if instrument_data.get('error'):
                    logger.error(f"Error fetching instrument data for {symbols}: {instrument_data['error']}")
                    return None
                return instrument_data
            else:
                logger.error(f"Error fetching instrument data for {symbols}: {response.status_code}")
                return None
        except Exception as e:
            logger.error(f"Error fetching instrument data for {symbols}: {e}")
            log_exception(e)
            return None

    def get_instrument_by_cusip(self, cusip):
        """
        Get an instrument by CUSIP.
        """
        try:
            response = self.td_client.get_instrument(
                cusip=cusip
            )

            if response.status_code == 200:
                instrument_data = response.json()

                # Check if any dictionary in the list has an error
                errors = [item.get('error') for item in instrument_data if 'error' in item]
                if errors:
                    logger.error(f"Error fetching instrument data for {cusip}: {', '.join(errors)}")
                    return None
                return instrument_data
            else:
                logger.error(f"Error fetching instrument data for {cusip}: {response.status_code}")
                return None
        except Exception as e:
            logger.error(f"Error fetching instrument data for {cusip}: {e}")
            log_exception(e)
            return None
        
    def get_instrument_by_symbol(self, symbol):
        """
        Get an instrument by symbol.
        """
        cusip = get_symbol_cusip(symbol)
        if cusip is None:
            logger.error(f"Error fetching instrument data for {symbol}: no cusip found")
            return None
        return self.get_instrument_by_cusip(cusip)
    
    def update_instrument_info(self, symbol, data, source=''):
        if source == '':
            source = self.source

        with self.db.session_scope() as session:
            # Get the Instrument_Info instance for the symbol
            existing_data = session.query(Instrument_Info).filter_by(symbol=symbol).first()

            # If the Instrument_Info instance exists, update
            if existing_data:
                existing_data.data = json.dumps(data)
                existing_data.last_updated = get_current_utc_datetime()
                existing_data.updated_by = self.updater_name
                existing_data.source = source

