# support/historical_price_data_manager.py
from sqlalchemy import Index, Table, inspect
import pytz
import json
import os
from typing import List, Optional, Tuple, Union, Dict
from collections import defaultdict
import pandas as pd
from models import Entity
from models import Historical_Price_Data
from support.base import Base
from support.db import DB
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from support.td_ameritrade_historical import TD_Ameritrade_Historical
#from support.eodhistoricaldata_historical_price_data import EODHistoricalData_Historical_Price_Data
from helpers.time_helper import datetime_utc_to_timestamp_utc_ms, get_current_datetime_utc, date_or_date_str_to_datetime_utc, timestamp_utc_ms_to_datetime_utc, get_start_of_current_year
from helpers.db_query_helper import get_entity_id_from_symbol, get_symbols_by_gics_sector, get_market_hours
from helpers.logging_helper import configure_logging, logger
from sqlalchemy import MetaData, select


"""
Currently working out import and export logic of paritions. In particular of what we are working on at this moment
we are adding details so that we can parition by gics_sector in addition to timeframes. We have decided to utilize the partition
segments as per td_ameritrades default settings for timeframe on charts assuming they partitioned similar to their defaults. Once 
I get the revised functions to look correct from chatgpt, then we need to move on to how we are going to programattically 
store and retreive this data within our backtests or any other things we do that require this data. We will then need to make a basic
script to test with minimal data. 

last i was working to pass around variables instead of having to run a method a couple different times to query for entity_id, or figure out the parition name
i dont think i am done, but close, need to check after updating everything. I got distracted with that task when i was updatting get_data_from_database
to use the args passed instead of getting them thru another method call when i realized that we were not doing anythign with 
after hours True/False and that we need to implement this logic. Chatgpt was generating an answer when i quit today`

IM ALL OVER THE PLACE, BUT WHAT i recall i am working to incorporate eodhistorical into this data gathering effort, I 
first need to finish adjusting the code work as expected and simmilarly in the loop as td ameritrade does, also working to
add ability to request the eod timeframe data as well as intraday. 

5/02/23: Working on get_data_from_database_first and plan to work my way all the way through the flow of get_data to make 
sure all logic is in place, right now a major focus is on making sure everything will be ran thru partitions correctly, that
we find missing data from whatever source and request it from the next available source, etc. I am currently in process of making sure to 
implement need regular trade hours or not logic into the database request. See chatgpt "Partitions" for further info

today we revised get_data() and pretty much finished our implementation of get_data_from_database (or whatever its named)
okay so we were in historical price manager doing our thing when i realized that we will need to figure out the limitation
per api for maximum time we can request of data per period. Currently we are fixing the logic in td ameritrade so that it will understand
its limitations and then we are going to also create a function that will use the maximum request date window (10 days for anything less than a day, 10 years for anything a day or more)
to determeine how to efficiently group missing data dates so that we can make as few requests to the api as apossible while retrieving all missing data

"""


"""
I think i may have the parition logic setup, and i may have the td_ameritrade logic set up EXCEPT WE NEED TO GROUP MISSING DATES TOGETHER FOR FEWER API REQUESTS, now just need to create the same funciton as below for eod_historicaldata source and then start to test 
after reviewing all code and determining it looks as if it is ready)

"""
class Historical_Price_Data_Mangager:
    def __init__(self, user='Historical Price Data Manager'):
        configure_logging()
        self.updater_name = user
        self.db = DB()
        self.td_hist_data = TD_Ameritrade_Historical()
        #self.eod_historical_data = EODHistoricalData_Historical_Price_Data()
        self.sources_used = [] # tracking of sources in gather_data() effort
        

    def get_data(self, symbol=None, symbols=None, gics_sector=None, start_date_str: str = None, end_date_str: str = None, period=None, period_type=None, frequency=None, frequency_type=None, need_extended_hours_data=False, adjusted_close=True, timezone='US/Eastern', **filters):
        """
        Retrieve data based on provided parameters. The user can either provide date ranges (start_date, end_date) or
        period and period_type to fetch the data. Additional filtering options can be passed as keyword arguments.

        Args:
            start_date: The start date of the data range.
            end_date: The end date of the data range.
            need_extended_hours_data: Whether to include extended hours data.
            symbol: The stock symbol to filter by.
            gics_sector: The GICS sector to filter by.
            period: The number of periods to show.
            period_type: The type of period (day, month, year, ytd).
            frequency: The frequency of the data.
            frequency_type: The type of frequency (minute, hour).
            **filters: Additional filtering options based on columns available in entity table or historical price table partitions.

        Returns:
            A list of data points based on the provided filtering options.
        """
        # Make sure we understand what the user expects the method to accomplish, they may have provided too many details, making it confusing how to handle
        if (symbol and symbols) or (symbol and gics_sector) or (symbols and gics_sector):
            raise ValueError("You can only provide one of the following variables: symbol, symbols or gics_sector")

        # Validate input parameters
        if not (start_date_str and end_date_str and frequency and frequency_type) and not (period and period_type and frequency and frequency_type):
            raise ValueError("You must provide either start_date, end_date, frequency, and frequency_type or period, period_type, frequency, and frequency_type.")

        frequency_type = self.standardize_frequency_type(frequency_type)
        
        # Convert period/period-type to start/end dates
        if start_date_str is None and end_date_str is None:
            start_datetime_utc, end_datetime_utc = self.convert_period_to_datetime_utc(period, period_type)  # You will need to create this method
        else: 
            start_datetime_utc = date_or_date_str_to_datetime_utc(start_date_str, timezone)
            end_datetime_utc = date_or_date_str_to_datetime_utc(end_date_str, timezone)

        # If the user wants data by sector
        if gics_sector:
            # fetch symbol list for the sector, name variable symbols so that we can process either this or a list of symbols in the next step
            symbols = get_symbols_by_gics_sector(gics_sector)
        
        # If the user has provided a list of symbols or if the user wanted data by sector and we created a list from the sector
        if symbols:
                # for each symbol in the list
                for symbol in symbols:
                     # reset sources used because this is a new symbol and we may have different results for this search than the prior
                    self.sources_used = [] 
                    # Get data thru various sources, gather_data will request the data, track sources used, make sure any new data is written to database before returning the data
                    data, missing_data_ranges = self.gather_data(symbol=symbol, start_datetime_utc=start_datetime_utc, end_datetime_utc=end_datetime_utc, frequency=frequency, frequency_type=frequency_type, need_extended_hours_data=need_extended_hours_data)
        # The user only provided one symbol, so they only need data for the one symbol
        else:
            # reset sources used because this is a new symbol and we may have different results for this search than the prior
            self.sources_used = []
            # Get data thru various sources, gather_data will request the data, track sources used, make sure any new data is written to database before returning the data
            data, missing_data_ranges = self.gather_data(symbol=symbol, start_datetime_utc=start_datetime_utc, end_datetime_utc=end_datetime_utc, frequency=frequency, frequency_type=frequency_type, need_extended_hours_data=need_extended_hours_data)
        # If there are any missing data ranges, we will log the missing data ranges because we have exhausted all endpoints
        if missing_data_ranges:
            logger.error(f"Error retrieving data from database and backup servers, missing the following date ranges: {', '.join([f'({start}, {end})' for start, end in missing_data_ranges])}")
        # Return data and any missing data ranges to the user
        return data, missing_data_ranges


    def gather_data(self, symbol, start_datetime_utc: datetime, end_datetime_utc: datetime, frequency, frequency_type, need_extended_hours_data):
        # methods we will utilize for each endpoint that will be attempted for the data request
        sources = [self.get_historical_data_from_td_ameritrade_and_write_to_database,]
        
        while True:
            # Fetch data from the database (changed declaration from data, missing_data_ranges to just 'data', also revised get_historical_price_data_from_database to only return the data)
            data = self.get_historical_price_data_from_database(
                start_datetime_utc=start_datetime_utc, end_datetime_utc=end_datetime_utc, frequency=frequency, frequency_type=frequency_type, symbol=symbol,
                need_extended_hours_data=need_extended_hours_data)
            
            # Check for and find missing data ranges
            missing_data_ranges = self.find_missing_data_ranges(data, start_datetime_utc, end_datetime_utc, frequency=frequency, frequency_type=frequency_type)

            # If there are missing data ranges
            if missing_data_ranges:
                # Reset variable 'source'
                source = None
                # for each source in sources
                for s in sources:
                    # if source is not yet used
                    if s not in self.sources_used:
                        # source = source
                        source = s
                        # break from loop so we can use the source
                        break
                # Made it through above for/if loop and determined that all sources have been used, or that there are no sources, so we should break out of the if missing_data_ranges statement and move to the else: part of the statment
                if source is None:
                    break
                # Get data from api source and write to database, we are not returning data here because we want all data to come from the database even if we first had to retreive from api
                source(symbol=symbol, start_datetime_utc=start_datetime_utc, end_datetime_utc=end_datetime_utc, frequency=frequency, frequency_type=frequency_type, need_extended_hours_data=need_extended_hours_data, missing_data_ranges=missing_data_ranges)   
                # Add source to list of sources we have used for the request thus far
                self.sources_used.append(source)
                # Recursive call to check database and see if all data is present
                self.gather_data(symbol=symbol, start_datetime_utc=start_datetime_utc, end_datetime_utc=end_datetime_utc, frequency=frequency, frequency_type=frequency_type, need_extended_hours_data=need_extended_hours_data)

            else:
                # Made it through all api sources, here is the final data and missing_data_ranges we found
                return data, missing_data_ranges
            
        return data, missing_data_ranges


    def get_historical_price_data_from_database(self, start_datetime_utc: datetime, end_datetime_utc: datetime, frequency: str, frequency_type: str, need_extended_hours_data: bool, symbol: Optional[str] = None, gics_sector: Optional[str] = None, adjusted_close: bool = True, **filters) -> List[Dict[str, Union[int, str, float]]]:
        """
        Fetches historical price data from the database for a given symbol and specified date range and frequency.

        Parameters:
            start_date :
            end_date :
            frequency (str): The frequency of the historical price data ('1min', '5min', '30min', '1hour', '4hour', 'daily', 'weekly', 'monthly').
            frequency_type (str): The type of frequency for the historical price data ('minute', 'hour', 'day', 'week', 'month').
            need_extended_hours_data (bool): Whether to include extended hours data in the historical price data.
            symbol (str, optional): The stock or ETF symbol. If not provided, data for all entities will be returned.
            gics_sector (str, optional): The GICS sector of the entity. If provided, data for entities in the specified sector will be returned.
            adjusted_close (bool, optional): Whether to use the adjusted close price. Defaults to True.
            **filters: Additional filters to apply to the query. The keyword argument should be the column name and the value should be the filter value.

        Returns:
            list: A list of dictionaries containing the historical price data for the specified symbol and parameters. Each dictionary contains the following keys: 'timestamp', 'symbol', 'open', 'high', 'low', 'close', and 'volume'.
        """
        if symbol:
            entity_id = get_entity_id_from_symbol(symbol)
        else:
            entity_id = None

        # Commented out 5/10/23, if we get this workign without it can be deleted, this effort is being moved earlier in the stack to 
        # get_data so that we more uniformly pass the timestamps to other methods/functions
        #start_date_dt = parse_date_string(start_date)
        #end_date_dt = parse_date_string(end_date)
        #start_timestamp = datetime_to_timestamp(start_date_dt)
        #end_timestamp = datetime_to_timestamp(end_date_dt)


        # Determine the partition type using existing method'
        partition_type = Historical_Price_Data.determine_partition_type(frequency, frequency_type)

        # Use partition type to determine the table to query from
        # Reflect the partition table from the database
        metadata = MetaData()

        # Check if the table exists
        inspector = inspect(self.db.engine)

        if not inspector.has_table(f'Historical_Price_Data_{partition_type.capitalize()}'):
            print(f"Table Historical_Price_Data_{partition_type.capitalize()} does not exist.")
            return []

        query_table = Table(f'Historical_Price_Data_{partition_type.capitalize()}', metadata, autoload_with=self.db.engine)
        entity_table = Table('Entity', metadata, autoload_with=self.db.engine)
            
        # Construct the base query
        if adjusted_close:
            query = select([
                query_table.c.timestamp,
                entity_table.c.symbol,
                query_table.c.open,
                query_table.c.high,
                query_table.c.low,
                query_table.c.adjusted_close.label('close'),
                query_table.c.volume
            ]).select_from(
                query_table.join(entity_table, query_table.c.entity_id == entity_table.c.id)
            )
        else:
            query = select([
                query_table.c.timestamp,
                entity_table.c.symbol,
                query_table.c.open,
                query_table.c.high,
                query_table.c.low,
                query_table.c.close,
                query_table.c.volume
            ]).select_from(
                query_table.join(entity_table, query_table.c.entity_id == entity_table.c.id)
            )

        start_date_timestamp = datetime_utc_to_timestamp_utc_ms(start_datetime_utc)
        end_date_timestamp = datetime_utc_to_timestamp_utc_ms(end_datetime_utc)

        # Apply the start and end date filters
        query = query.where(query_table.c.timestamp.between(start_date_timestamp, end_date_timestamp))

        # Apply the entity_id filter if a symbol was provided
        if entity_id:
            query = query.where(query_table.c.entity_id == entity_id)

        # Apply the gics_sector filter if provided
        if gics_sector:
            query = query.where(entity_table.c.gics_sector == gics_sector)

        # Apply the need_extended_hours_data filter
        if not need_extended_hours_data:
            query = query.where(query_table.c.is_regular_trading_hours)

        # Apply additional filters provided as keyword arguments
        for column, value in filters.items():
            query = query.where(getattr(query_table.c, column) == value)

        # Execute the query and return the results
        with self.db.session_scope() as session:
            result = session.execute(query)
            data = [dict(row) for row in result]
        
        return data


    def find_missing_data_ranges(self, data: List[dict], start_datetime_utc: datetime, end_datetime_utc: datetime, frequency: int, frequency_type: str) -> List[Tuple[int, int]]:
        """
        Finds and returns the missing data ranges (by timestamp) between the provided data and the specified start and end timestamps.

        Args:
            data (list[dict]): A list of dictionaries containing historical price data.
            start_timestamp (int): The start timestamp for the data range.
            end_timestamp (int): The end timestamp for the data range.
            frequency (int): The frequency of the data ('1min', '5min', '30min', '1hour', '4hour', 'daily', 'weekly', 'monthly').
            frequency_type (str): The frequency type of the data ('minute', 'hour', 'day', 'week', 'month').

        Returns:
            list[tuple[int, int]]: A list of tuples containing the missing data ranges as start and end timestamps.
        """
        if not data:
            return [(start_datetime_utc, end_datetime_utc)]

        missing_data_ranges = []

        def process_gap(gap_start, gap_end):
            while gap_start <= gap_end:
                if self.is_during_trading_hours(gap_start):
                    gap_start += delta
                else:
                    gap_start += timedelta(minutes=1)
                if gap_start > gap_end:
                    break
                missing_data_ranges.append((gap_start, gap_end))

        data_start_timestamp = data[0].timestamp
        data_end_timestamp = data[-1].timestamp

        start_timestamp = datetime_utc_to_timestamp_utc_ms(start_datetime_utc)
        if data_start_timestamp > start_timestamp:
            process_gap(start_timestamp, data_start_timestamp - timedelta(minutes=1))
        end_timestamp = datetime_utc_to_timestamp_utc_ms(end_datetime_utc)
        if data_end_timestamp < end_timestamp:
            process_gap(data_end_timestamp + timedelta(minutes=1), end_timestamp)

        for i in range(len(data) - 1):
            current_data_timestamp = data[i].timestamp
            next_data_timestamp = data[i + 1].timestamp

            delta = None
            if frequency_type == "minute":
                delta = timedelta(minutes=frequency)
            elif frequency_type == "hour":
                delta = timedelta(hours=frequency)
            elif frequency_type == "daily":
                delta = timedelta(days=frequency)
            elif frequency_type == "weekly":
                delta = timedelta(weeks=frequency)
            elif frequency_type == "monthly":
                delta = timedelta(days=frequency * 30)

            if current_data_timestamp + delta < next_data_timestamp:
                process_gap(current_data_timestamp + delta, next_data_timestamp - delta)

        return missing_data_ranges



    def get_historical_data_from_td_ameritrade_and_write_to_database(self, symbol: str, start_datetime_utc: datetime = None, end_datetime_utc: datetime = None, frequency: Union[str, int] = None, frequency_type: Optional[str] = None, need_extended_hours_data: bool = True, missing_data_ranges = None) -> List:   
        start_timestamp = datetime_utc_to_timestamp_utc_ms(start_datetime_utc)
        end_timestamp = datetime_utc_to_timestamp_utc_ms(end_datetime_utc)

        open_time, close_time = get_market_hours(symbol)
        # Convert open and close times to timestamps for the comparison day
        open_timestamp = datetime.combine(start_datetime_utc.date(), open_time).timestamp() * 1000
        close_timestamp = datetime.combine(start_datetime_utc.date(), close_time).timestamp() * 1000

        data = self.td_hist_data.get_historical_data_from_td_ameritrade(symbol=symbol, start_timestamp=start_timestamp, end_timestamp=end_timestamp, frequency=frequency, frequency_type=frequency_type, need_extended_hours_data=need_extended_hours_data, missing_data_ranges=missing_data_ranges)

        # Define variables related to the data source, entity_id, and user
        data_source = 'TD Ameritrade'
        entity_id = get_entity_id_from_symbol(symbol)
        last_updated = get_current_datetime_utc()
        updated_by = self.updater_name  # Set the appropriate user
        
        # Determine partition type based on frequency and frequency_type
        partition_type = Historical_Price_Data.determine_partition_type(frequency, frequency_type)  # Implement this function based on your requirements

         # Initialize partition start and end
        first_timestamp = data[0]['datetime']
        current_partition_start, current_partition_end = Historical_Price_Data.get_partition_start_end(first_timestamp, partition_type)

        # Group data points by partition
        grouped_data = defaultdict(list)
        current_parition_end_timestamp = datetime_utc_to_timestamp_utc_ms(current_partition_end)
        # For each bar of data from TD Ameritrade api
        for data_point in data:
            # Determine if the timestamp falls within trading hours
            is_regular_trading_hours = open_timestamp <= data_point['datetime'] < close_timestamp


            # Create an instance of Historical_Price_Data for this bar using from_td_ameritrade method
            instance = Historical_Price_Data.from_td_ameritrade(data_point, entity_id, data_source, last_updated, updated_by, is_regular_trading_hours)
            
            timestamp = instance.timestamp
            # If the current bars timestamp is outside of the current partition date range
            if timestamp >= current_parition_end_timestamp:
                # Write instances to the partition table
                Historical_Price_Data.bulk_write_to_partition(grouped_data[current_partition_start], self.db, partition_type)

                # Clear the data points for the previous partition
                grouped_data[current_partition_start].clear()

                # Update partition start and end
                current_partition_start, current_partition_end = Historical_Price_Data.get_partition_start_end(timestamp, partition_type)
            # append instance to grouped_data
            grouped_data[current_partition_start].append(instance)

        # Write instances for the last partition
        Historical_Price_Data.bulk_write_to_partition(grouped_data[current_partition_start], self.db, partition_type)

    """ # Deprecated
    def get_data_from_eod_historical_data(self, symbol, start_timestamp, end_timestamp, period_type):
        logger.info(f"Fetching data for symbol {symbol} from EOD Historical Data API")
        if period_type == "day":
            price_data = self.eod_historical_data.get_eod_data_for_list_of_symbols([symbol], start_timestamp=start_timestamp, end_timestamp=end_timestamp)
        else:
            price_data = self.eod_historical_data.get_intraday_data_for_list_of_symbols([symbol], start_timestamp=start_timestamp, end_timestamp=end_timestamp)
        
        if price_data:
            # Save the data to the local database
            self.insert_data(price_data)
            return price_data
        return None
    """

    def convert_period_to_datetime_utc(self, period: int, period_type: str) -> Tuple[datetime, datetime]:
        """
        Convert a period type and length into a start and end datetime in UTC.

        This function takes a period length and type (e.g., 7 days, 2 weeks, 1 year, etc.) and
        calculates a start and end datetime based on that period. The start and end datetimes are 
        calculated relative to the current date and time, as determined by datetime.utcnow().

        The resulting start and end datetimes are timezone-aware and are in UTC. If the period type 
        is 'ytd' (year to date), the start date will be the beginning of the current year.

        Args:
            period (int): The length of the period.
            period_type (str): The type of the period (e.g., "day", "month", "year", etc.).

        Returns:
            Tuple[datetime, datetime]: A tuple containing the start and end datetime for the specified period.
        """
        utc = pytz.UTC

        if period_type == "second":
            delta = timedelta(seconds=period)
        elif period_type == "minute":
            delta = timedelta(minutes=period)
        elif period_type == "hour":
            delta = timedelta(hours=period)
        elif period_type == "day":
            delta = timedelta(days=period)
        elif period_type == "week":
            delta = timedelta(weeks=period)
        elif period_type == "month":
            delta = timedelta(days=period * 30)  # Assuming 30 days in a month
        elif period_type == "year":
            delta = timedelta(days=period * 365)  # Assuming 365 days in a year
        elif period_type == "ytd":
            now = get_current_datetime_utc()
            start_of_year = get_start_of_current_year()
            delta = now - start_of_year  # Days from the beginning of the year
        else:
            raise ValueError(f"Invalid period_type: {period_type}")

        end_date = get_current_datetime_utc()
        start_date = end_date - delta

        return start_date, end_date
        
    # Not currently being used
    def is_trading_hours(self, symbol, datetime_utc: datetime = None) -> bool:
        """
        Check if the given symbol is currently trading during its regular trading hours.
        
        Args:
            symbol (str): The symbol for which to check trading hours.
            utc_datetime (datetime, optional): The datetime at which to check trading hours. Defaults to the current time.
        
        Returns:
            bool: True if the symbol is currently trading during its regular trading hours, False otherwise.
        """
        with self.db.session_scope as session:
            if datetime_utc is None:
                datetime_utc = get_current_datetime_utc()

            # Get entity associated with the symbol
            entity = session.query(Entity).filter(Entity.code == symbol).one_or_none()

            if not entity:
                raise ValueError(f"Symbol not found: {symbol}")

            # Get associated exchange
            exchange = entity.exchange_data

            # Check if the current date is a holiday
            holidays = exchange.holidays
            current_date = datetime_utc.date()
            for _, holiday_data in holidays.items():
                holiday_date = datetime.strptime(holiday_data["Date"], "%Y-%m-%d").date()
                if holiday_date == current_date:
                    return False

            # Check if it's a working day
            working_days = exchange.trading_hours["WorkingDays"].split(',')
            if datetime_utc.strftime("%a") not in working_days:
                return False

            # Check if within trading hours
            open_time = datetime.strptime(exchange.trading_hours["Open"], "%H:%M:%S").time()
            close_time = datetime.strptime(exchange.trading_hours["Close"], "%H:%M:%S").time()
            current_time = datetime_utc.time()

            return open_time <= current_time <= close_time

# Test the function
#symbol = "TSLA"
#print(is_trading_hours(symbol))


    # Not currently being used
    def filter_trading_hours(self, data, symbol):
        with self.db.session_scope() as session:
            # Get entity associated with the symbol
            entity = session.query(Entity).filter(Entity.code == symbol).one_or_none()

            if not entity:
                raise ValueError(f"Symbol not found: {symbol}")

            # Get associated exchange
            exchange = entity.exchange_data

            # Get trading hours and working days
            trading_hours = exchange.trading_hours
            working_days = trading_hours["WorkingDays"].split(',')
            open_time = datetime.strptime(trading_hours["Open"], "%H:%M:%S").time()
            close_time = datetime.strptime(trading_hours["Close"], "%H:%M:%S").time()

            # Get holidays
            holidays = exchange.holidays
            holiday_dates = [datetime.strptime(holiday_data["Date"], "%Y-%m-%d").date() for _, holiday_data in holidays.items()]

            # Convert data to a pandas DataFrame if it's not already
            if not isinstance(data, pd.DataFrame):
                data = pd.DataFrame(data)

            # Filter data based on trading hours, working days, and holidays
            data['date'] = pd.to_datetime(data['date'])
            data['day_of_week'] = data['date'].dt.strftime('%a')
            data['time'] = data['date'].dt.time

            # Filter out non-working days and holidays
            filtered_data = data[data['day_of_week'].isin(working_days) & ~data['date'].dt.date.isin(holiday_dates)]

            # Filter out data outside of trading hours
            filtered_data = filtered_data[(filtered_data['time'] >= open_time) & (filtered_data['time'] <= close_time)]

            # Drop temporary columns
            filtered_data = filtered_data.drop(columns=['day_of_week', 'time'])

            return filtered_data

# Test the function with a sample dataset
#sample_data = [
#    {"date": "2023-05-01 07:59:00", "price": 100},
#    {"date": "2023-05-01 08:00:00", "price": 101},
#    {"date": "2023-05-01 20:00:00", "price": 102},
#    {"date": "2023-05-01 20:01:00", "price": 103},
#    {"date": "2023-05-06 08:00:00", "price": 104},  # Non-working day
#    {"date": "2023-05-07 08:00:00", "price": 105},  # Non-working day
#    {"date": "2023-05-08 08:00:00", "price": 106},
#]
#
#symbol = "TSLA"
#filtered_data = filter_trading_hours(sample_data, symbol)
#print(filtered_data)
    
    def insert_data(self, data_list, entity_id, period, period_type, frequency, frequency_type, granularity, gics_sector):
        # Before inserting data, ensure the appropriate partition exists
        # Use the inspect method to check if the table exists
        inspector = inspect(self.db.engine)

        with self.db.session_scope() as session:
            for data in data_list:
                partition_name = self.get_partition_name(data['timestamp'], granularity, gics_sector)
                
                if not inspector.has_table(partition_name):
                    self.create_partition_table(partition_name)

                # Insert data into the partition table
                data_to_insert = data.copy()
                data_to_insert['entity_id'] = entity_id

                if data['source'] == 'TD Ameritrade':
                    historical_price_data_instance = Historical_Price_Data.from_td_ameritrade(data_to_insert)
                elif data['source'] == 'eodhistoricaldata.com':
                    historical_price_data_instance = Historical_Price_Data.from_eod_historical(data_to_insert)
                else:
                    raise ValueError(f"Invalid source: {data['source']}")

                session.add(historical_price_data_instance)

    # Modify the create_partition function to accept granularity
    def create_partition_table(self, partition_name):
        partition_table = Table(
            partition_name,
            Base.metadata,
            *(c.copy() for c in Historical_Price_Data.__table__.c),
            extend_existing=True
        )

        index_name = f"ix_{partition_name}_timestamp"
        new_index = Index(index_name, partition_table.c.timestamp)
        partition_table.indexes.add(new_index)

        Base.metadata.create_all(self.db.engine, tables=[partition_table])

        

    def read_local_data(self, file_path):
        with open(file_path, "r") as f:
            data = json.load(f)

        formatted_data = []
        for record in data:
            formatted_record = {
                "entity_id": get_entity_id_from_symbol(record["symbol"]),
                "datetime_utc": datetime.strptime(record["timestamp"], "%Y-%m-%d %H:%M:%S").replace(tzinfo=pytz.UTC),  # Add timezone
                "open": record["open"],
                "high": record["high"],
                "low": record["low"],
                "close": record["close"],
                "adjusted_close": record.get("adjusted_close"),  # Add this line
                "volume": record["volume"],
                "source": record["source"],
                "last_updated": record["last_updated"],
                "updated_by": record["updated_by"],
            }
            formatted_data.append(formatted_record)

        return formatted_data

    def next_open_close_times(self, symbol, datetime_utc=None):
        with self.db.session_scope() as session:
            if datetime_utc is None:
                datetime_utc = get_current_datetime_utc()

            # Get entity associated with the symbol
            entity = session.query(Entity).filter(Entity.code == symbol).one_or_none()

            if not entity:
                raise ValueError(f"Symbol not found: {symbol}")

            # Get associated exchange
            exchange = entity.exchange_data

            # Get trading hours and working days
            trading_hours = exchange.trading_hours
            working_days = trading_hours["WorkingDays"].split(',')
            open_time = datetime.strptime(trading_hours["Open"], "%H:%M:%S").time()
            close_time = datetime.strptime(trading_hours["Close"], "%H:%M:%S").time()

            # Get holidays
            holidays = exchange.holidays

            # Find the next open and close times
            next_open = None
            next_close = None
            day_delta = timedelta(days=1)

            while not next_open or not next_close:
                # Check if it's a holiday
                is_holiday = False
                for _, holiday_data in holidays.items():
                    holiday_date = datetime.strptime(holiday_data["Date"], "%Y-%m-%d").date()
                    if holiday_date == datetime_utc.date():
                        is_holiday = True
                        break

                if not is_holiday and datetime_utc.strftime("%a") in working_days:
                    if not next_open and datetime_utc.time() < open_time:
                        next_open = datetime.combine(datetime_utc.date(), open_time)

                    if not next_close and datetime_utc.time() < close_time:
                        next_close = datetime.combine(datetime_utc.date(), close_time)

                # Check the next day
                datetime_utc += day_delta

            return next_open, next_close
        


    def standardize_frequency_type(self, frequency_type):
        frequency_type = frequency_type.lower()
        if frequency_type in ["minute", "minutes", "min", "mins"]:
            return "minute"
        elif frequency_type in ["day", "days", "daily", "d"]:
            return "daily"
        elif frequency_type in ["week", "weeks", "weekly", "w"]:
            return "weekly"
        elif frequency_type in ["month", "months", "monthly", "m"]:
            return "monthly"
        else:
            raise ValueError(f"Invalid frequency type: {frequency_type}")
# Test the function
#symbol = "TSLA"
#next_open, next_close = next_open_close_times(symbol)
#print("Next open time:", next_open)
#print("Next close time:", next_close)

def main():
    # Instantiate the manager
    manager = Historical_Price_Data_Mangager()

    # Read the local data directory path
    # local_data_directory = './data/eodhistorical_historical_price_data'

    # symbol_list = read_symbols_from_csv('C:\\Users\\mitch\\OneDrive\\io\\git\\backtests\\data\\symbol_lists\\master_td_eod_symbol_list_4_24_23.csv')
    # intervals = ["1m", "5m", "1h"]

    symbol_list = ['TSLA', 'TLT', 'MSFT']
    period = 10
    period_type = 'year'
    frequency = 1
    frequency_type = 'day'
    data = manager.get_data(symbols=symbol_list, period=period, period_type=period_type, frequency=frequency, frequency_type=frequency_type)
    print(type(data))

if __name__ == '__main__':
    main()