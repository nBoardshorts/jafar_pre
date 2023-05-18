# support/eodhistoricaldata_symbols.py
from gitignore.config import EOD_HISTORICAL_DATA_API_KEY
from helpers.logging_helper import configure_logging, logger
from helpers.db_query_helper import get_all_exchange_codes_from_exchanges_eodhistoricaldata
from helpers.symbol_eodhistoricaldata_helper import clean_symbols_from_eodhistorical
from helpers.time_helper import get_current_utc_datetime
from models.symbol_eodhistoricaldata import Symbol_EODHistoricalData
from datetime import datetime
import pandas as pd
from eodhd import APIClient
from support.db import DB

configure_logging()


class EODHistoricalData_Symbols:
    def __init__(self, user='EODHistoricalData_Symbols Class'):
        self.eod_client = APIClient(EOD_HISTORICAL_DATA_API_KEY)
        self.source = 'eodhistoricaldata.com'
        self.updater_name = user
        self.db = DB()
        

    def get_symbol_code(self, symbol):
        with self.db.session_scope() as session:
            symbols_eodhistoricaldata = session.query(Symbol_EODHistoricalData).filter_by(Symbol=symbol).first()
            if symbols_eodhistoricaldata is None:
                raise ValueError(f"No data found for symbol: {symbol}")
            
            if symbols_eodhistoricaldata.country == "USA":
                return f"{symbol}.US"
            else:
                exchange_code = symbols_eodhistoricaldata.exchange
                return f"{symbol}.{exchange_code}"



    def update_symbols(self, update_time, source=''):
        logger.info('Starting update on Symbols_eodhistoricaldata table.')
        if source == '':
            source = self.source

        # comment out the 4th line down and uncomment the following 3 lines to read the data from the saved CSV file and comment the line after it that calls get_all_symbols_from_all_exchanges():
        #na_values = ["", "#N/A", "#N/A N/A", "#NA", "-1.#IND", "-1.#QNAN", "-NaN", "-nan", "1.#IND", "1.#QNAN", "<NA>", "N/A", "NULL", "NaN", "n/a", "nan", "null"]
        #all_symbols = pd.read_csv('symbols_from_eodhistoricaldata.csv', keep_default_na=False, na_values=na_values)
        all_symbols = self.get_all_symbols_from_all_exchanges()

        try:
            with self.db.session_scope() as session:
                # Get all existing symbols from the database
                existing_symbols = session.query(Symbol_EODHistoricalData).all()
                existing_symbols_dict = {s.symbol: s for s in existing_symbols}


                # Add new symbols and update the status of existing symbols to 'active'
                for index, row in all_symbols.iterrows():
                    try:
                        symbol_code = row['Code']
                        symbol = clean_symbols_from_eodhistorical(row, source, update_time, self.updater_name)
                        existing_symbol = existing_symbols_dict.get(symbol_code)

                        if existing_symbol:
                            existing_symbol.status = 'active'
                            existing_symbol.last_updated = update_time
                            existing_symbol.updated_by = self.updater_name
                        else:
                            symbol.status = 'active' if symbol.exchange and len(symbol.exchange) > 0 else 'inactive'
                            session.add(symbol)  # Add new symbols to the session
                    except Exception as e:
                        print(f"Error: {e}")
                        print(f"Problematic row: {row}")

                # Set the status of the remaining symbols to 'inactive'
                all_symbols_set = set(all_symbols['Code'].tolist())

                for existing_symbol in existing_symbols:
                    if existing_symbol.symbol not in all_symbols_set:
                        existing_symbol.status = 'inactive'
                        existing_symbol.last_updated = update_time
                        existing_symbol.updated_by = self.updater_name

        except Exception as e:
            logger.exception(f'Error while saving symbols to symbols_eodhistoricaldata table: {e}')


    def get_all_symbols_from_all_exchanges(self):
        exchange_codes = get_all_exchange_codes_from_exchanges_eodhistoricaldata()
        
        all_symbols = pd.DataFrame()

        for (exchange_code,) in exchange_codes: #By adding the parentheses and the comma, you will unpack the tuple directly in the loop, so exchange_code will be a string instead of a tuple containing a string. 
            exchange_code = exchange_code.strip().upper()
            symbols = self.get_all_symbols_in_exchange(exchange_code)
            all_symbols = pd.concat([all_symbols, symbols], ignore_index=True)
         # Uncomment the below thre lines to Save to csv in case database injection messes up we dont have to call on api again
         # Pandas keeps interpreting the symbol 'NA' as not accilicable or something
        all_symbols.to_csv(f'symbols_from_eodhistoricaldata.csv', index=False, na_rep='NA')
        return all_symbols

    def get_all_symbols_in_exchange(self, exchange_code):
        try:
            response = self.eod_client.get_exchange_symbols(exchange_code)
            
            # Pandas keeps interpreting the symbol 'NA' as not applicable or something
            na_values = ["", "#N/A", "#N/A N/A", "#NA", "-1.#IND", "-1.#QNAN", "-NaN", "-nan", "1.#IND", "1.#QNAN", "<NA>", "N/A", "NULL", "NaN", "n/a", "nan", "null"]

            response.replace(na_values, pd.NA, inplace=True)  # Replace the values treated as NaN with pandas.NA
            response.fillna("NA", inplace=True)  # Fill the remaining NaN values with 'NA'
            
            return response

        except Exception as e:
            logger.exception(f'Error retrieving all symbols from exchange: {exchange_code} from eodhistoricaldata.com: {e}')
            return pd.DataFrame() # Return an empty DataFrame in case of an error
    
def main():
    eodhistoricaldata_symbols = EODHistoricalData_Symbols()
    eodhistoricaldata_symbols.update_symbols(update_time=get_current_utc_datetime())
    

if __name__ == '__main__':
    main()
