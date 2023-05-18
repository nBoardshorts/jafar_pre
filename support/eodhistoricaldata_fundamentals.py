# support/eodhistoricaldata_fundamentals
from eod import EodHistoricalData
import os
import json
from helpers.data_helper import json_to_df, save_data_to_csv
from gitignore.config import EOD_HISTORICAL_DATA_API_KEY
from helpers.db_query_helper import get_all_us_based_symbols_for_td_ameritrade_and_eodhistoricaldata
from helpers.logging_helper import configure_logging, logger
from helpers.data_helper import camel_to_snake_case, read_last_processed_symbol, update_last_processed_symbol
from models import Category_For_Metric, Metric, Metric_Value, Symbol_EODHistoricalData, Symbol_TD_Ameritrade
from models import Entity
from datetime import datetime
import time
from support.db import DB
from retry import retry
import requests

configure_logging()

class EODHistoricalData_Fundamentals:
    def __init__(self, user='EOD Historical Data Fundamentals Class'):
        #Create instance using eod library (eod-data on github)
        self.client = EodHistoricalData(EOD_HISTORICAL_DATA_API_KEY)
        self.source = 'eodhistoricaldata.com' # Hardcoded source
        self.updater_name = user
        self.db = DB()
        self.exchanges = ['NASDAQ', 'NYSE', 'BATS', 'AMEX']
        self.output_dir = './data/eodhistorical_fundamentals'
  
    @retry(requests.exceptions.RequestException, tries=3, delay=2, backoff=2)
    def get_fundamentals_for_symbol(self, eod_symbol, use_local_files=False):
        json_file_path = f'./data/fundamentals/{eod_symbol}.fundamentals.json'
        if use_local_files and os.path.exists(json_file_path):
            # Load the data from the local file
            with open(json_file_path, 'r') as json_file:
                fundamentals = json.load(json_file)
        else:
            try:
                #logger.info(f'Requesting fundamental data for {eod_symbol}')
                
                fundamentals = self.client.get_fundamental_equity(symbol=eod_symbol)
                #logger.info(f'Saving JSON response to a file')
                with open(f'./data/fundamentals/{eod_symbol}.fundamentals.json', 'w') as json_file:
                    json.dump(fundamentals, json_file)          
                return fundamentals
            except Exception as e:
                logger.error(f'Error while downloading/saving fundamental data for {eod_symbol}: {e}')
        return fundamentals
    
    @classmethod
    def eod_symbol_code_to_symbol(cls, symbol_code):
        # Split the eod_symbol to get the actual symbol
        actual_symbol = symbol_code.split('.')[0]
        return actual_symbol

    def update_entities_table(self, symbol_list, row_or_section=None):
        update_time = datetime.utcnow()

        if row_or_section is not None:
            if isinstance(row_or_section, int):
                symbol_list = symbol_list[row_or_section:row_or_section+1]
            elif isinstance(row_or_section, str):
                symbol_list = [symbol for symbol in symbol_list if row_or_section in symbol]

        self.update_general_section_for_each_symbol_in_list(symbol_list, update_time)

    def parse_and_store_general_section(self, json_data, source, update_time, updater_name=''):
        if source == '':
            source = self.source
        if updater_name == '':
            updater_name = self.updater_name

        general_data = json_data.get('General')

        if general_data:
            # Get the actual symbol from the 'code' field in the general_data
            symbol = general_data.get('Code')

        with self.db.session_scope() as session:
            eodhistoricaldata_obj = session.query(Symbol_EODHistoricalData).filter_by(symbol=symbol).first()
            td_ameritrade_obj = session.query(Symbol_TD_Ameritrade).filter_by(symbol=symbol).first()

            # Create or update the Entity object with the general_data
            entity_obj = session.query(Entity).filter_by(code=symbol).first()
            if not entity_obj:
                entity_obj = Entity(code=symbol)
                session.add(entity_obj)

            # track data changes to determine if updated by/time needs to be injected
            data_changed = False
            for key, value in general_data.items():
                snake_case_key = camel_to_snake_case(key)
                current_value = getattr(entity_obj, snake_case_key, None)
                
                # Check if the new value is different from the current value
                if current_value != value:
                    data_changed = True
                    #print(entity_obj)
                    #print(type(entity_obj))
                    #print(f"snake_case_key: {snake_case_key}")
                    #print(f"value: {value}")
                    #print(f"value type: {type(value)}")
                    #print("Attribute name (camel_case):", key)
                    #print("Attribute name (snake_case):", snake_case_key)
                    #print("Value:", value)

                    setattr(entity_obj, snake_case_key, value)
                
            # Update the related Symbol_EODHistoricalData and Symbol_TD_Ameritrade objects
            if eodhistoricaldata_obj:
                eodhistoricaldata_obj.entity = entity_obj
            if td_ameritrade_obj:
                td_ameritrade_obj.entity = entity_obj

            # Only if Data Changed!, Set the source, last_updated, and updated_by fields for both new and existing entity_obj
            if data_changed:
                entity_obj.source = source
                entity_obj.last_updated = update_time
                entity_obj.updated_by = updater_name

            # Call session.flush() after setting the required fields
            session.flush()



    def parse_and_store_fundamentals(self, json_data, eod_symbol, source=''):
        if source == '':
            source = self.source

        with self.db.session_scope() as session:
            # Loop through the JSON data
            for metric_section, metrics in json_data.items():
                if metric_section != 'General':
                    # Check if the category exists in the database, and if not, create it
                    category_obj = session.query(Category_For_Metric).filter_by(name=metric_section).first()
                    if not category_obj:
                        category_obj = Category_For_Metric(name=metric_section)
                        session.add(category_obj)
                        session.flush()

                    # Loop through the metrics in the current section
                    for metric, values in metrics.items():
                        # Check if the metric exists in the database, and if not, create it
                        metric_obj = session.query(Metric).filter_by(name=metric).first()
                        if not metric_obj:
                            metric_obj = Metric(name=metric, source=source, category_id=category_obj.id)
                            session.add(metric_obj)
                            session.flush()
                        else:
                            metric_obj.source = source

                        # Check if the values object is an integer and convert it to a string if necessary
                        if isinstance(values, int):
                            values = str(values)

                        # Loop through the metric values and store them in the Metric_Value table
                        for date, value in values.items():
                            # Split the eod_symbol to get the actual symbol
                            symbol = self.eod_symbol_code_to_symbol(eod_symbol)
                            # Query the Symbol_EODHistoricalData and Symbol_TD_Ameritrade tables to get the IDs
                            eodhistoricaldata_id = session.query(Symbol_EODHistoricalData).filter_by(symbol=symbol).first().id
                            td_ameritrade_id = session.query(Symbol_TD_Ameritrade).filter_by(symbol=symbol).first().id

                            metric_value_obj = Metric_Value(
                                eodhistoricaldata_id=eodhistoricaldata_id,  
                                td_ameritrade_id=td_ameritrade_id,      
                                metric_id=metric_obj.id,
                                timestamp=datetime.strptime(date, "%Y-%m-%d"),
                                value=value
                            )
                            session.add(metric_value_obj)
    
    def update_general_section_for_each_symbol_in_list(self, symbol_list, update_time):
        # Read the last processed symbol from the checkpoint file
        checkpoint_filename = 'data\\symbol_lists\\temp\\last_processed_symbol.txt'
        last_processed_symbol = read_last_processed_symbol(checkpoint_filename)
        start_from_next_symbol = last_processed_symbol is None or last_processed_symbol == ''

        for symbol in symbol_list:
            if start_from_next_symbol:
                try:
                    json_data = self.get_fundamentals_for_symbol(symbol, use_local_files=True)
                    if json_data:
                        self.parse_and_store_general_section(json_data, symbol, update_time)
                        logger.info(f"Processed symbol: {symbol}")
                    else:
                        logger.warning(f"No data found for symbol: {symbol}")

                    # Update the checkpoint file with the latest processed symbol
                    update_last_processed_symbol(checkpoint_filename, symbol)
                except requests.exceptions.ConnectionError as ce:
                    logger.error(f"ConnectionError occurred while fetching data for {symbol}: {ce}")
                    time.sleep(5 * 60)
                    self.update_general_section_for_each_symbol_in_list(symbol_list, update_time)
                except Exception as e:
                    logger.error(f"Error processing symbol {symbol}: {e}")
                    continue

            elif symbol == last_processed_symbol:
                start_from_next_symbol = True


    def get_fundamentals_all_symbols(self):
        eod_symbol_list = get_all_us_based_symbols_for_td_ameritrade_and_eodhistoricaldata()
        for symbol in eod_symbol_list:
            fundamentals, eod_symbol, session = self.get_fundamentals_for_symbol(symbol)
            #TODO

    def get_all_bulk_fundamentals(self, exchanges):
        for exchange in exchanges:
            try:
                logger.info(f'Requesting bulk fundamental data for {exchange}')
                bulk_fundamentals = self.client.get_fundamentals_bulk(exchange=exchange)
                print(type(bulk_fundamentals))
                logger.info(f'Converting json response to dataframe')
                df = json_to_df(bulk_fundamentals)
                logger.info(f'Saving response to csv file')
                save_data_to_csv(df, f'bulk{exchange}.csv', f'./data/bulk_fundamentals/{exchange}')
            except Exception as e:
                logger.error(f'Error while requesting bulk fundamental data for {exchange}: {e}')

    def fetch_data_for_symbol(self, eod_symbol):
        file_path = f'./data/fundamentals/{eod_symbol}.fundamentals.json'

        # Check if the file exists
        if os.path.isfile(file_path):
            try:
                # Read the JSON data from the file
                with open(file_path, 'r') as json_file:
                    json_data = json.load(json_file)
                return json_data
            except Exception as e:
                logger.error(f"Error reading JSON data from file {file_path}: {e}")
                return None
        else:
            try:
                # Request data from the API
                json_data = self.get_fundamentals_for_symbol(eod_symbol)
                return json_data
            except Exception as e:
                logger.error(f"Error fetching data from the API for {eod_symbol}: {e}")
                return None

def get_fundamentals_by_symbol_test():
    eod_fundamentals = EODHistoricalData_Fundamentals()
    fundamentals, eod_symbol, session = eod_fundamentals.get_fundamentals_for_symbol(eod_symbol="TSLA.US")
    eod_fundamentals.parse_and_store_fundamentals(json_data=fundamentals, eod_symbol=eod_symbol, session=session)
    print(fundamentals)

def main():
    get_all_us_based_symbols_for_td_ameritrade_and_eodhistoricaldata()

    
if __name__ == '__main__':
    main()   
