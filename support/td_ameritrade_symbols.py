# support/td_ameritrade_symbols.py
from support.db import DB
from support.td_client_wrapper import TD_Client_Wrapper
from models.symbol_td_ameritrade import Symbol_TD_Ameritrade
from helpers.data_helper import dict_list_to_csv
from helpers.db_query_helper import get_symbol_cusip
from helpers.logging_helper import configure_logging, log_exception, logger

# Purpose: from the TD Ameritrade API and updating the symbols table in the database. The class handles inserting new symbols,
# updating the status of existing symbols, and tracking the last_updated and updated_by fields in the Symbol model.

# Workflow:
# 1. The Updater class initializes an instance of TD_Ameritrade_Symbols with a TD_Client_Wrapper and updater_name.
# 2. TD_Ameritrade_Symbols fetches the symbols from the TD Ameritrade API.
# 3. The source is hardcoded within the TD_Ameritrade_Symbols class and returned to the Updater using the get_source() method.
# 4. The Updater calls set_symbol_update_info to update the update_tracking table with the source and updater_name.
# 5. The TD_Ameritrade_Symbols instance calls the update_symbols method to update the symbols table in the database with the new symbols.
# 6. If a symbol already exists in the database, its status is updated to 'active' if it is currently 'inactive'.
# 7. If a symbol exists in the database but does not exist in the tdameritrade response, its status is updated to 'inactive'
#
# Criteria:
# 1. Fetch symbols from the TD Ameritrade API
# 2. Insert new symbols into the database (including cusip, source, and status fields)
# 3. Update symbol update information in the Update_Tracking table
# 4. Update symbols in the database using the TD Ameritrade API
# 5. Track the last_updated and updated_by fields in the Symbol model



# Initialize logging for the td_ameritrade_symbols script
configure_logging()

class TD_Ameritrade_Symbols:
    def __init__(self, user='TD Ameritrade Symbols Class'):
        self.td_client = TD_Client_Wrapper.get_instance().get_client()
        self.source = "TD Ameritrade"  # Hardcoded source
        self.updater_name = user
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
        
    @classmethod
    def search_instruments(cls, symbols, projection='symbol-search'):
        """
        Search or retrieve instrument data, including fundamental data.
        """
        try:
            projection = cls.run_tda_enum('Projection', projection) if projection is not None else None

            response = cls.td_client.search_instruments(
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
    
    def get_all_symbols(self):
        """
        Fetches a list of all symbols from the TD Ameritrade API.
        """
        logger.info('Retrieving symbols from TD Ameritrade Server')
        symbols = []
        search_characters = list(range(48, 58)) + list(range(65, 91))
        for c in search_characters:
            symbol_regex = f'{chr(c)}.*'
            try:
                # Call TD Ameritrade API to search for instruments
                response = self.td_client.search_instruments(symbols=symbol_regex, projection=self.td_client.Instrument.Projection.SYMBOL_REGEX)
                try:
                    instruments = response.json()
                    if isinstance(instruments, dict):
                        symbols.extend(instruments.values())
                    else:
                        logger.error(f"Unexpected response while retrieving symbols from the TD Ameritrade Server for {symbol_regex}: {instruments}")
                except Exception as e:
                    logger.exception(f'Error fetching instruments for {symbol_regex}: {e}')
            except Exception as e:
                logger.exception(f"Error fetching instruments for {symbol_regex}: {e}")
        dict_list_to_csv(symbols, csv_file_name='td_ameritrade_symbols_before.csv')
        logger.info('Symbols downloaded successfully from the TD Ameritrade Server.')
        return symbols
    
    def update_symbols(self, update_time, source=''):
        """
        Updates the `symbols` table in the database with new symbol data obtained from the TD Ameritrade API. 

        Args:
            new_symbols (list): A list of symbol data obtained from the TD Ameritrade API.
            update_time (datetime): The time at which the symbols were last updated.
            source (str, optional): The source of the symbol data. If not provided, the `source` attribute of the class instance will be used.

        Returns:
            None
            
        Raises:
            None

        The method retrieves all existing symbols from the `symbols` table in the database and stores them in a dictionary for efficient lookup. For each symbol in `new_symbols`, the method checks whether it already exists in the database. If it does, the method updates its `status`, `last_updated`, `updated_by`, and `exchanges` attributes. If it does not exist, the method adds it to the database with a `status` of 'active' if it has at least one exchange and 'inactive' otherwise. 

        After all new symbols have been added or updated, the method sets the `status`, `last_updated`, and `updated_by` attributes of any symbols in the database that were not updated to reflect their new status as 'inactive'. 

        Finally, the method logs a message indicating the number of symbols added or updated and the time at which the update was performed.
        """
        new_symbols=self.get_all_symbols()
        logger.info('Updating symbols in database table td_ameritrade_symbols')
        if source == '':
            source = self.source

        with self.db.session_scope() as session:
            # Get all existing symbols from the database
            existing_symbols = session.query(Symbol_TD_Ameritrade).all()
            existing_symbols_dict = {s.symbol: s for s in existing_symbols}

            # Add new symbols and update the status of existing symbols to 'active'
            for symbol_data in new_symbols:
                symbol = Symbol_TD_Ameritrade.from_td_ameritrade(symbol_data, source, update_time, self.updater_name)
                existing_symbol = existing_symbols_dict.get(symbol.symbol)

                if existing_symbol:
                    existing_symbol.status = 'active'
                    existing_symbol.last_updated = update_time
                    existing_symbol.updated_by = self.updater_name
                    # existing_symbol.exchanges = TD_Ameritrade_Symbol.handle_exchanges(existing_symbol.exchanges, symbol.exchange)
                else: # uncomment if reverting back to using from_td_ameritrade, the above if statement wasnt altered
                    symbol.status = 'active' if symbol.exchanges and len(symbol.exchanges) > 0 else 'inactive'
                session.add(symbol)


            # Set the status of the remaining symbols to 'inactive'
            for symbol in existing_symbols:
                if symbol.symbol not in existing_symbols_dict:
                    symbol.status = 'inactive'
                    symbol.last_updated = update_time
                    symbol.updated_by = self.updater_name
            logger.info(f"Symbols update complete. {len(new_symbols)} symbols added/updated at {update_time}")