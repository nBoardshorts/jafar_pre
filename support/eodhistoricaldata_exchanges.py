from gitignore.config import EOD_HISTORICAL_DATA_API_KEY
from helpers.logging_helper import configure_logging, logger
import requests
import pandas as pd
import datetime
import pytz
from eodhd import APIClient
from support.db import DB
from models import Exchange_EODHistoricalData

configure_logging()


class EODHistoricalData_Exchanges:
    def __init__(self, user='EOD Historical Data Class'):
        self.eod_client = APIClient(EOD_HISTORICAL_DATA_API_KEY)
        self.source = 'eodhistoricaldata.com'
        self.updater_name = user
        self.db = DB()
        

    #### Database query logic below ####
    def get_exchange_details_from_db(self, exchange_code):
        with self.db.session_scope() as session:
            exchange = session.query(Exchange_EODHistoricalData).filter_by(Code=exchange_code).first()
            if not exchange:
                return None

            return {
                'Name': exchange.Name,
                'Code': exchange.Code,
                'OperatingMIC': exchange.OperatingMIC,
                'Country': exchange.Country,
                'Currency': exchange.Currency,
                'CountryISO2': exchange.CountryISO2,
                'CountryISO3': exchange.CountryISO3,
                'Timezone': exchange.Timezone,
                'trading_hours': exchange.trading_hours,
                'holidays': exchange.holidays
            }

    def is_exchange_open(self, exchange_code):
        exchange_details = self.get_exchange_details_from_db(exchange_code)
        if not exchange_details:
            return None

        timezone_str = exchange_details.get('Timezone')
        if not timezone_str:
            logger.warning(f"Timezone not available for exchange {exchange_code}")
            return None

        timezone = pytz.timezone(timezone_str)
        now = datetime.datetime.now(timezone)

        # Check if today is a holiday
        holidays = exchange_details.get('holidays', [])
        today_str = now.strftime('%Y-%m-%d')
        if today_str in holidays:
            return False

        # Check if the exchange is open based on trading hours
        trading_hours = exchange_details.get('trading_hours', {})
        if not trading_hours:
            logger.warning(f"Trading hours not available for exchange {exchange_code}")
            return None

        weekday_str = str(now.weekday())
        if weekday_str not in trading_hours:
            return False

        open_time_str, close_time_str = trading_hours[weekday_str].split('-')
        open_time = datetime.datetime.strptime(open_time_str, '%H:%M').time()
        close_time = datetime.datetime.strptime(close_time_str, '%H:%M').time()

        return open_time <= now.time() <= close_time
    

    #### Database population logic below ####
    def get_all_exchanges(self):
            # Get all Exchanges
            # Uncomment the following lines to fetch the all exchanges from the API and save it to a CSV file:
            logger.info(f'Retrieving all exchanges from eodhistoricaldata.com to update exchanges table')
            try:
                response = self.eod_client.get_exchanges()
            except Exception as e:
                logger.exception(f'Error retrieving all exchanges from eodhistoricaldata.com: {e}')
            
            #response.to_csv('api_response.csv', index=False)
            # Comment out the above lines and uncomment the following line to read the data from the saved CSV file:
            # response = pd.read_csv('api_response.csv')

            logger.info('Connecting to database session to inject data')
            with self.db.session_scope() as session:
                table_exists = session.connection().dialect.has_table(session.connection(), 'exchanges')

                if table_exists:
                    existing_data = pd.read_sql_table('exchanges_eodhistoricaldata', session.connection())
                else:
                    existing_data = pd.DataFrame(columns=['id', 'Name', 'Code', 'OperatingMIC', 'Country', 'Currency', 'CountryISO2', 'CountryISO3'])

                new_data = pd.concat([existing_data, response]).drop_duplicates(subset='Code', keep='first')

                new_data = new_data.drop(columns=['id'])  # Remove the 'id' column from the DataFrame
                try:
                    new_data.to_sql('exchanges_eodhistoricaldata', session.connection(), index=False, if_exists='append')
                except Exception as e:
                    logger.exception(f'Error writing data to database: {e}')
                logger.info(f'Exchanges table updated successfully!')
            #print(new_data)


    def fetch_exchange_details(self, exchange_code):
        url = f'https://eodhistoricaldata.com/api/exchange-details/{exchange_code}?api_token={EOD_HISTORICAL_DATA_API_KEY}&fmt=json'

        try:
            response = requests.get(url)
            if response.status_code == 200:
                response_json = response.json()
                return response_json
            else:
                logger.error(f'Error fetching exchange details for {exchange_code}: {response.content}')
                return None
        except Exception as e:
            logger.exception(f'Error fetching exchange details for {exchange_code}: {e}')
            return None

    def parse_exchange_details(self, response_json, code):
        if response_json is None:
            logger.error(f'Response JSON for {code} is None, cannot parse exchange details')
            return None
            
        try:
            trading_hours = {}
            holidays = {}

            if 'TradingHours' in response_json:
                trading_hours = response_json['TradingHours']

            if 'ExchangeHolidays' in response_json:
                holidays = {i: response_json['ExchangeHolidays'][str(i)] for i in range(len(response_json['ExchangeHolidays']))}

            return {
                'Name': response_json['Name'],
                'Code': response_json['Code'],
                'OperatingMIC': response_json.get('OperatingMIC'),
                'Country': response_json['Country'],
                'Currency': response_json['Currency'],
                'Timezone': response_json.get('Timezone'),
                'isOpen': response_json.get('isOpen'),
                'trading_hours': trading_hours,
                'holidays': holidays
            }
        except Exception as e:
            logger.exception(f'Error while parsing exchange details for {code}: {str(e)}')
            return 1

    def exchange_details_to_db(self, exchange_details, code):
        logger.info(f'Writing exchange details for {code} to database...')
        with self.db.session_scope() as session:
            # Filter exchange details by code, narrowing down to only one line for each code
            exchange = session.query(Exchange_EODHistoricalData).filter_by(Code=code).first()
            # If the exchange doesnt have an existing record in the database
            if not exchange:
                # Create an instance of exchange as per the database model
                exchange = Exchange_EODHistoricalData()
            # Populate the instance with the data
            exchange.Name = exchange_details['Name']
            exchange.Code = exchange_details['Code']
            exchange.OperatingMIC = exchange_details['OperatingMIC']
            exchange.Country = exchange_details['Country']
            exchange.Currency = exchange_details['Currency']
            exchange.Timezone = exchange_details['Timezone']
            exchange.trading_hours = exchange_details['trading_hours']
            exchange.holidays = exchange_details['holidays']
            session.merge(exchange)

            logger.info(f'Details for exchange {code} updated in database successfully!')
    
    def get_exchange_details_for_all(self):
        # Start session
        with self.db.session_scope() as session:
            # Get codes for exchanges
            codes = session.query(Exchange_EODHistoricalData.Code).all()
            # For each code 
            for code in codes:
                try:
                    # Get exchange details from server
                    logger.info(f'Retrieving details for exchange {code[0]} from eodhistoricaldata.com')
                    exchange_details = self.fetch_exchange_details(code[0])
                    exchange_details = self.parse_exchange_details(exchange_details, code[0])
                    if exchange_details:
                        self.exchange_details_to_db(exchange_details, code[0])
                    else:
                        logger.error(f'Could not write details for exchange {code} to the database')

                except Exception as e:
                    logger.exception(f'Error retrieving exchange details for {code[0]}: {e}')


def main():
    eod_exch = EODHistoricalData_Exchanges()

    eod_exch.get_exchange_details_for_all()
    

if __name__ == '__main__':
    main()
