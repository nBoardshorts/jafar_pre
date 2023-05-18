from eod import EodHistoricalData
import os
import json

import pandas as pd
from helpers.data_helper import read_symbols_from_csv
from gitignore.config import EOD_HISTORICAL_DATA_API_KEY
from helpers.db_query_helper import get_exchange_for_symbol
from helpers.time_helper import get_current_utc_datetime
from helpers.logging_helper import configure_logging, logger
from helpers.symbol_eodhistoricaldata_helper import extract_symbol
from helpers.data_helper import add_source_and_updated_info
from models import Category_For_Metric, Metric, Metric_Value, Symbol_EODHistoricalData, Symbol_TD_Ameritrade
from models.historical_price_data import Historical_Price_Data
from models import Entity
from datetime import datetime, timedelta
import time
from support.db import DB
from retry import retry
import requests

configure_logging()

class EODHistoricalData_Historical_Price_Data:
    def __init__(self, user='EOD Historical Data Historical Price Data Class'):
        #Create instance using eod library (eod-data on github)
        self.client = EodHistoricalData(EOD_HISTORICAL_DATA_API_KEY)
        self.source = 'eodhistoricaldata.com' # Hardcoded source
        self.updater_name = user
        self.db = DB()
        self.exchanges = ['NASDAQ', 'NYSE', 'BATS', 'AMEX']
        self.output_dir = './data/eodhistorical_historical_price_data'
    
    def get_bulk_data(self, exchange, date=None, data_type=None, symbols=None, fmt='json', filter=None):
        """
        This method takes the following parameters:

        exchange: The exchange symbol, e.g., 'US', 'NYSE', 'NASDAQ', etc.
        date: (optional) The specific date you want the data for. If not provided, it will download data for the last trading day.
        data_type: (optional) Can be either 'splits' or 'dividends'. If not provided, it will download EOD data.
        symbols: (optional) A list of specific symbols you want the data for.
        fmt: (optional) The output format, default is 'json'.
        filter: (optional) Use 'extended' to get an extended dataset.
        The method returns the requested bulk data as a JSON object. If you have any questions or need further modifications, please let me know.
        """
        base_url = 'https://eodhistoricaldata.com/api/eod-bulk-last-day/'
        url = f"{base_url}{exchange}?api_token={EOD_HISTORICAL_DATA_API_KEY}&fmt={fmt}"

        if date:
            url += f"&date={date}"

        if data_type:
            url += f"&type={data_type}"

        if symbols:
            url += f"&symbols={','.join(symbols)}"

        if filter:
            url += f"&filter={filter}"

        response = requests.get(url)

        if response.status_code == 404:
            logger.error(f"Resource not found for exchange '{exchange}'.")
            return
        else:
            response.raise_for_status()

        bulk_data = response.json()

        return bulk_data

    @retry(tries=3, delay=2, backoff=2)
    def fetch_eod_data(self, symbol, period='d', order='a', _from='', to=''):
        """
        Notes from API:
        period - use 'd' for daily, 'w' for weekly, 'm' for monthly prices. By default, daily prices will be shown.
        order - use 'a' for ascending dates (from old to new), 'd' for descending dates (from new to old). By default, dates are shown in ascending order.
        from and to - the format is 'YYYY-MM-DD'. If you need data from Jan 5, 2017, to Feb 10, 2017, you should use from=2017-01-05 and to=2017-02-10.
        """
        base_url = 'https://eodhistoricaldata.com/api/eod/'
        output_dir = self.output_dir

        url = f'{base_url}{symbol}?api_token={EOD_HISTORICAL_DATA_API_KEY}&period={period}&order={order}&fmt=json'
        if _from and to:
            url += f'&from={_from}&to={to}'

        file_path = os.path.join(output_dir, f'{symbol}_{period}_{_from}_{to}_data.json')

        # Check if the file already exists
        if os.path.exists(file_path):
            logger.info(f'File already exists for {period} period data for symbol: {symbol} from {_from} to {to}. Skipping download.')
            return [], None

        response = requests.get(url)

        if response.status_code == 404:
            logger.error(f"Resource not found for symbol '{symbol}'. Skipping to the next symbol.")
            return [], None
        else:
            response.raise_for_status()

        eod_data = response.json()

        # Add 'timestamp' column
        eod_data = Historical_Price_Data.dates_to_timestamp_ms(eod_data)
        last_updated = get_current_utc_datetime()
        eod_data = add_source_and_updated_info(eod_data, self.source, last_updated, updated_by=self.updater_name)

        # Create the output directory if it doesn't exist
        os.makedirs(output_dir, exist_ok=True)

        # Save to file for later use if needed (for programming/debugging phase, will be commented out when in production)
        with open(file_path, 'w') as f:
            json.dump(eod_data, f)
        logger.info(f'Successfully saved {period} period data for symbol: {symbol} from {_from} to {to}')

        if eod_data:
            last_date = eod_data[-1]['date']
        else:
            last_date = None

        return eod_data, last_date

    def get_eod_data_for_list_of_symbols(self, symbol_list, start_date=None, end_date=None, period='d', order='a', granularity=None, gics_sector=None, entity_id=None):
        last_processed_symbol, last_processed_date = self.read_last_processed_symbol('data\\symbol_lists\\temp\\eodhist_eod_data_download_last_processed_symbol.txt')
        start_from_next_symbol = last_processed_symbol is None or last_processed_symbol == ''

        all_eod_data = []  # Initialize an empty list to compile all eod_data

        for symbol in symbol_list:
            if start_from_next_symbol or symbol == last_processed_symbol:
                start_from_next_symbol = True
            else:
                continue

            if start_date is None and end_date is None:
                start_date = "1900-01-01"
                end_date = get_current_utc_datetime().strftime('%Y-%m-%d')

            try:
                eod_data, last_timestamp = self.fetch_eod_data(symbol, period, order, start_date, end_date)
                all_eod_data.append(eod_data)

                # Save the fetched data to the database
                #save_data_to_db(eod_data)

                self.update_last_processed_symbol('data\\symbol_lists\\temp\\eodhist_eod_data_download_last_processed_symbol.txt', symbol, last_timestamp)

            except Exception as e:
                logger.error(f"Failed to fetch {period} period data for symbol: {symbol}. Error: {e}")

            time.sleep(1)  # To avoid overwhelming the API with requests

        # Update the checkpoint file with the current symbol as the last processed symbol
        self.update_last_processed_symbol('data\\symbol_lists\\temp\\eodhist_eod_data_download_last_processed_symbol.txt', symbol, last_timestamp)

        # Return the compiled EOD data along with other required parameters if they are not None
        if granularity is not None and gics_sector is not None and entity_id is not None:
            return all_eod_data, granularity, gics_sector, entity_id
        else:
            return all_eod_data



    @retry(tries=3, delay=2, backoff=2)
    def fetch_intraday_data(self, symbol, interval, start_datetime_utc, end_datetime_utc):
        base_url = "https://eodhistoricaldata.com/api/intraday/"
        fmt = "json"
        output_dir = self.output_dir

        symbol_exchange = symbol # formatted {SYMBOL_NAME}.{EXCHANGE_ID}
        url = f"{base_url}{symbol_exchange}?api_token={EOD_HISTORICAL_DATA_API_KEY}&interval={interval}&from={start_datetime_utc}&to={end_datetime_utc}&fmt={fmt}"
        
        file_path = os.path.join(output_dir, f"{symbol}_{interval}_{start_datetime_utc}_{end_datetime_utc}_data.json")

        # Check if the file already exists
        if os.path.exists(file_path):
            logger.info(f"File already exists for {interval} interval data for symbol: {symbol} from {start_datetime_utc} to {end_datetime_utc}. Skipping download.")
            return

        response = requests.get(url)

        if response.status_code == 404:
            logger.error(f"Resource not found for symbol '{symbol}'. Skipping to the next symbol.")
            return
        else:
            response.raise_for_status()

        minute_data = response.json()

        # Create the output directory if it doesn't exist
        os.makedirs(output_dir, exist_ok=True)

        with open(file_path, "w") as f:
            json.dump(minute_data, f)
        logger.info(f"Successfully saved {interval} interval data for symbol: {symbol} from {start_datetime_utc} to {end_datetime_utc}")

        if minute_data:
            last_timestamp = minute_data[-1]['timestamp']
        else:
            last_timestamp = None

        return minute_data, last_timestamp

    # When used in main get_data function need to remember that this one only will provide intradday data for the following frequency types below
    def get_intraday_data_for_list_of_symbols(self, symbol_list, start_date=None, end_date=None, frequency=None, frequency_type=None, period=None, period_type=None, need_extended_hours_data=True, granularity=None, gics_sector=None, entity_id=None):
        last_processed_symbol, last_processed_date = self.read_last_processed_symbol('data\\symbol_lists\\temp\\eodhist_minute_data_download_last_processed_symbol.txt')
        start_from_next_symbol = last_processed_symbol is None or last_processed_symbol == ''

        max_periods = {
            '1m': 120,
            '5m': 600,
            '1h': 7200
        }

        all_intraday_data = []  # Initialize an empty list to compile all intraday_data

        for symbol in symbol_list:
            if start_from_next_symbol or symbol == last_processed_symbol:
                start_from_next_symbol = True
            else:
                continue

            trunc_symbol = extract_symbol(symbol)
            exchange = get_exchange_for_symbol(symbol=trunc_symbol)

            if exchange in ["NYSE", "NASDAQ"]:
                intervals = ["1m", "5m", "1h"]
            elif exchange in ["FOREX", "CC", "MOEX"]:
                intervals = ["1m", "5m", "1h"]
            else:
                intervals = ["5m", "1h"]

            for interval in intervals:
                try:
                    if start_date is None and end_date is None and period is not None and period_type is not None:
                        start_date, end_date = self.process_period_and_period_type(period, period_type)
                    elif start_date is None and end_date is None:
                        if interval == "1m" and exchange in ["NYSE", "NASDAQ"]:
                            start_date = "2004-01-01"
                        elif interval == "1m" and exchange in ["FOREX", "CC", "MOEX"]:
                            start_date = "2009-01-01"
                        else:
                            start_date = "2020-10-01"
                        end_date = get_current_utc_datetime().strftime('%Y-%m-%d')

                    start_date = datetime.strptime(start_date, '%Y-%m-%d')
                    end_date = datetime.strptime(end_date, '%Y-%m-%d')

                    current_start_date = last_processed_date if last_processed_date else start_date
                    # Define a mapping for interval durations in seconds
                    interval_durations = {
                        "1m": 60,
                        "5m": 300,
                        "1h": 3600
                    }

                    while current_start_date <= end_date:
                        current_end_date = current_start_date + timedelta(days=max_periods[interval]) - timedelta(days=1)
                        if current_end_date > end_date:
                            current_end_date = end_date

                        start_date_unix = int(current_start_date.timestamp())
                        end_date_unix = int(current_end_date.timestamp())

                        intraday_data = self.fetch_and_save_intraday_data(symbol, interval, start_date_unix, end_date_unix)

                        all_intraday_data.append(intraday_data)

                        self.update_last_processed_symbol('data\\symbol_lists\\temp\\eodhist_minute_data_download_last_processed_symbol.txt', symbol, current_start_date.strftime('%Y-%m-%d'))

                        # Add the interval duration in seconds to the timestamp
                        new_start_timestamp = end_date_unix + interval_durations[interval]
                        current_start_date = datetime.fromtimestamp(new_start_timestamp)

                        # Add this line to ensure the start_date_unix is not greater than end_date_unix
                        if start_date_unix >= end_date_unix:
                            break

                    # Check if all the required date ranges are covered in the downloaded data
                    missing_dates = self.check_data_coverage(all_intraday_data, start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d'))

                    if missing_dates:
                        # Request the missing data
                        for missing_date in missing_dates:
                            missing_start_date = datetime.strptime(missing_date, '%Y-%m-%d')
                            missing_end_date = missing_start_date
                            missing_start_date_unix = int(missing_start_date.timestamp())
                            missing_end_date_unix = int(missing_end_date.timestamp())

                            intraday_data = self.fetch_and_save_intraday_data(symbol, interval, missing_start_date_unix, missing_end_date_unix)

                            # Check if the missing data is still missing after the request
                            missing_dates_after_request = self.check_data_coverage(intraday_data, missing_date, missing_date)
                            if missing_dates_after_request:
                                logger.error(f"Data still missing for symbol {symbol} on date {missing_date}")
                            else:
                                all_intraday_data.append(intraday_data)
                        
                        self.update_last_processed_symbol('data\\symbol_lists\\temp\\eodhist_minute_data_download_last_processed_symbol.txt', symbol, current_start_date.strftime('%Y-%m-%d'))

                        current_start_date = current_end_date + timedelta(days=1)

                        
                    # Check if all the required date ranges are covered in the downloaded data
                    self.check_data_coverage(all_intraday_data, start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d'))
                except Exception as e:
                    logger.error(f"Failed to fetch {interval} interval data for symbol: {symbol}. Error: {e}")
                time.sleep(1)  # To avoid overwhelming the API with requests

        # Update the checkpoint file with the current symbol as the last processed symbol
        self.update_last_processed_symbol('data\\symbol_lists\\temp\\eodhist_minute_data_download_last_processed_symbol.txt', symbol)

        # Return the compiled intraday data along with other required parameters if they are not None
        if granularity is not None and gics_sector is not None and entity_id is not None:
            return all_intraday_data, granularity, gics_sector, entity_id
        else:
            return all_intraday_data

    # read_last_processed_symbol function
    def read_last_processed_symbol(self, file_path):
        if os.path.exists(file_path):
            with open(file_path, 'r') as f:
                content = f.readline().strip().split(',')
                if len(content) == 2:
                    symbol, date_str = content
                    date_obj = datetime.strptime(date_str, '%Y-%m-%d')
                    return symbol, date_obj
                else:
                    return content[0], None
        else:
            return None, None
        
    # update_last_processed_symbol function
    def update_last_processed_symbol(self, file_path, symbol, date_str):
        with open(file_path, 'w') as f:
            f.write(f"{symbol},{date_str}")

    def check_data_coverage(self, all_intraday_data, start_date, end_date):
        date_set = set(pd.date_range(start_date, end_date, freq='B').strftime('%Y-%m-%d'))
        dates_in_data = set()

        for intraday_data in all_intraday_data:
            for entry in intraday_data:
                date = entry['datetime'][:10]
                dates_in_data.add(date)

        missing_dates = date_set - dates_in_data
        if missing_dates:
            logger.warning(f"Missing data for dates: {', '.join(sorted(list(missing_dates)))}")
        else:
            logger.info(f"All required date ranges are covered in the downloaded data.")
        return missing_dates

def main():
    # Instantiate class
    eodhist_data = EODHistoricalData_Historical_Price_Data()
    # Read symbol list into variable 
    #symbol_list = read_symbols_from_csv('C:\\Users\\mitch\\OneDrive\\io\\git\\backtests\\data\\symbol_lists\\master_td_eod_symbol_list_4_24_23.csv')
    # get_intraday_data for each symbol in a symbol list, this would return something, but we dont care right now, we are just saving the files.
    #eodhist_data.get_intraday_data_for_list_of_symbols(symbol_list)
    symbol_list = ['GE.US', 'TLT.US', 'T.US']
    eodhist_data.get_eod_data_for_list_of_symbols(symbol_list)
    

if __name__ == '__main__':
    main()
