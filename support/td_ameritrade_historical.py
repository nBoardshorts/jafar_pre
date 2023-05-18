# support/td_ameritrade_historical.py
import pandas as pd
import json
from datetime import datetime, timedelta
from typing import List, Dict, Union, Optional,Tuple, Any
from support.db import DB
from support.td_client_wrapper import TD_Client_Wrapper
from helpers.logging_helper import configure_logging, log_exception, logger


# Initialize logging for the td_ameritrade_historical script
configure_logging()

# Instantiate TD_Client_Wrapper
td_client = TD_Client_Wrapper.get_instance()


class TD_Ameritrade_Historical:
    def __init__(self, user='TD_Ameritrade_Historical'):
        self.td_client = TD_Client_Wrapper.get_instance().get_client()
        self.source = "TD Ameritrade"  # Hardcoded source
        self.updater_name = user
        # Create database session
        self.db = DB()


   
    # Convert this so that it takes either dates or timestamp, 
    def get_historical_data_from_td_ameritrade(self, symbol: str, frequency: Union[str, int], frequency_type: str, start_timestamp: Optional[int] = None, end_timestamp: Optional[int] = None, start_datetime_utc: Optional[datetime] = None, end_datetime_utc: Optional[datetime] = None, need_extended_hours_data: bool = False, missing_data_ranges: Optional[List[Tuple[datetime, datetime]]] = None) -> List[dict]:
        """
        Fetches historical data for a given symbol from the TD Ameritrade API based on the specified granularity.

        Args:
            symbol (str): The stock or ETF symbol.
            start_date (datetime, optional): The start date for historical data.
            end_date (datetime, optional): The end date for historical data.
            frequency (Union[str, int]): The frequency of data points.
            frequency_type (str): The type of frequency for data points.
            need_extended_hours_data (bool, optional): Whether to include extended hours data. Defaults to False.
            missing_data_ranges (list[tuple[datetime, datetime]], optional): List of tuples representing the missing date ranges to fetch the historical data for.

        Returns:
            list[dict]: A list of historical data points for the specified symbol and parameters.
        """

        # If missing_data_ranges is not provided but start_date and end_date are, create a single range list
        if missing_data_ranges is None and start_datetime_utc is not None and end_datetime_utc is not None:
            missing_data_ranges = [(start_datetime_utc, end_datetime_utc)]

        result = []
        used_data_ranges = []
        timedelta_historical_data_special = self.get_timedelta_limit_for_self_get_historical_data_special_endpoint(frequency=frequency, frequency_type=frequency_type)
        timedelta_historical_data = self.get_timedelta_limit_for_self_get_historical_data(frequency_type=frequency_type)
        if missing_data_ranges:
            # Get optimized date_ranges
            grouped_missing_data_ranges = self.process_date_ranges_for_td_ameritrade_historical_data(missing_data_ranges, timedelta_historical_data_special)
            # For each date range in list of date ranges
            for missing_data_start_datetime_utc, missing_data_end_datetime_utc in grouped_missing_data_ranges:
                try:
                    # Try to get the data for the date range from the endpoint get_historical_data_special
                    chunk = self.get_historical_data_special(symbol, start_datetime_utc=missing_data_start_datetime_utc, end_datetime_utc=missing_data_end_datetime_utc, frequency=frequency, frequency_type=frequency_type, need_extended_hours_data=need_extended_hours_data)
                    # If we received data back
                    if chunk is not None:
                        data = chunk.json()  # extract data from response
                        if isinstance(data, list):
                            result.extend(data)
                            # add date range to list of data ranges we have used
                            used_data_ranges.append((missing_data_start_datetime_utc, missing_data_end_datetime_utc))
                        elif isinstance(data, dict) and 'candles' in data:
                            result.extend(data['candles'])
                            used_data_ranges.append((missing_data_start_datetime_utc, missing_data_end_datetime_utc))
                        else:
                            logger.error(f"Unexpected data format from get_historical_data_special: {type(data)}")
                # if the frequency is not valid for the endpoint
                except UnsupportedFrequencyError:
                    # Break out of this loop and go to the next endpoints loop
                    break
                # Catch any other exceptions and log them so the process can continue
                except Exception as e:
                    logger.error(f"Error fetching historical data using specialized endpoints for {symbol}: {e}")
                    log_exception(e)
            
            remaining_missing_data_ranges = []
            freq = self.td_frequency_to_pandas_frequency(frequency_type=frequency_type, frequency=frequency)
            # for each date range in missing_data_ranges
            for original_range_start_datetime_utc, original_range_end_datetime_utc in missing_data_ranges:
                # Get range of original variable provided as arg to method
                original_range = pd.date_range(start=original_range_start_datetime_utc, end=original_range_end_datetime_utc, freq=freq or "D")
                # for each used date range
                for used_range_start_datetime_utc, used_range_end_datetime_utc in used_data_ranges:
                    # Get range of dates that data had been provided to first endpoing because we dont need to request this data from the second endpoint
                    used_range = pd.date_range(start=used_range_start_datetime_utc, end=used_range_end_datetime_utc, freq=freq or "D")
                    # set original_range to be anything that was included in the original range that was not included in the used_range
                    original_range = original_range.difference(used_range)
                    # If there is a range of dates
                    if not original_range.empty:
                        # append it to remaining_missing_data_ranges which we rest just prior to beginning this loop
                        remaining_missing_data_ranges.append((min(original_range), max(original_range)))

            # We now finished looping thru the original missing_data_ranges
            # group the missing data into optimized date ranges as per the timedelta for the second endpoint
            grouped_remaining_missing_data_ranges = self.process_date_ranges_for_td_ameritrade_historical_data(remaining_missing_data_ranges, timedelta_historical_data)

            # for each remaining date window in grouped_remaining_missing_data_ranges
            for remaining_start_datetime_utc, remaining_end_datetime_utc in grouped_remaining_missing_data_ranges:
                try:
                    # request the data from the second endpoint
                    chunk = self.get_historical_data(symbol, start_datetime_utc=remaining_start_datetime_utc, end_datetime_utc=remaining_end_datetime_utc, frequency=frequency, frequency_type=frequency_type, need_extended_hours_data=need_extended_hours_data)
                    # If we received data back
                    if chunk is not None:
                        # add data to the variable 'result'
                        result.extend(chunk)
                # Catch any exceptions and log them so the process can continue
                except Exception as e:
                    logger.error(f"Error fetching historical data using specialized endpoints for {symbol}: {e}")
                    log_exception(e)

        # Remove duplicates by converting the result into a DataFrame, dropping duplicates, and then converting it back into a list
        df = pd.DataFrame(result)
        df = df.drop_duplicates()
        result = df.to_dict('records')

        return result
    
    
    def get_historical_data_special(self, symbol: str, start_datetime_utc: datetime = None, end_datetime_utc: datetime = None, frequency: int = None, frequency_type: str = None, need_extended_hours_data: bool = True) -> List:
        """
        Fetches historical data for a given symbol from the TD Ameritrade API using specialized endpoints based on the specified granularity.

        Parameters:
            symbol (str): The stock or ETF symbol.
            start_date (datetime, optional): The start date for historical data.
            end_date (datetime, optional): The end date for historical data.
            frequency (str or int, optional): The frequency of data points.
            frequency_type (str, optional): The type of frequency for data points.
            need_extended_hours_data (bool, optional): Whether to include extended hours data. Defaults to True.

        Returns:
            list: A list of historical data points for the specified symbol and parameters.
        """

        if frequency_type == 'minute':
            if frequency in [1, 5, 10, 15, 30, 60, 240]:
                method = self.td_client.get_price_history_every_minute
            else:
                raise UnsupportedFrequencyError(f"Frequency and frequency_type combination not supported by special endpoint: frequency_type={frequency_type}, frequency={frequency}")
        elif frequency_type == 'daily':
            method = self.td_client.get_price_history_every_day
        elif frequency_type in ['weekly', 'monthly'] and frequency == 1:
            method = self.td_client.get_price_history_every_week
        else:
            raise UnsupportedFrequencyError(f"Frequency and frequency_type combination not supported by special endpoint: frequency_type={frequency_type}, frequency={frequency}")
        return method(symbol, start_datetime=start_datetime_utc, end_datetime=end_datetime_utc, need_extended_hours_data=need_extended_hours_data)


    def get_historical_data(self, symbol: str, start_datetime_utc: Optional[datetime] = None, end_datetime_utc: Optional[datetime] = None, frequency: Optional[Union[str, int]] = None, frequency_type: Optional[str] = None, period: Optional[int] = None, period_type: Optional[str] = None, need_extended_hours_data: Optional[bool] = True) -> Optional[List[dict]]:
        """
        Fetches historical data for a given symbol from the TD Ameritrade API.

        Parameters:
            symbol (str): The stock or ETF symbol.

            frequency (str or int, optional): The frequency of data points, based on the Frequency enum. 
                For minute frequencies, use integers: 1, 5, 10, 15, or 30.
                For daily, weekly, and monthly frequencies, use 1.
            frequency_type (str, optional): The type of frequency for data points, based on the FrequencyType enum. 
                Options are: 'minute', 'daily', 'weekly', 'monthly'.
            period (int, optional): The number of periods to retrieve data for, based on the Period enum. 
                For daily periods: 1, 2, 3, 4, 5, 10.
                For monthly periods: 1, 2, 3, 6.
                For yearly periods: 1, 2, 3, 5, 10, 15, 20.
                For year-to-date periods: 1.
            period_type (str, optional): The type of period for data retrieval, based on the PeriodType enum. 
                Options are: 'day', 'month', 'year', 'ytd' (year-to-date).
            need_extended_hours_data (bool, optional): Whether to include extended hours data. Defaults to True.

        Returns:
            list: A list of historical data points for the specified symbol and parameters.
        """
        try:
            period_type = self.run_tda_enum('PeriodType', period_type) if period_type is not None else None
            period = self.run_tda_enum('Period', period) if period is not None else None
            frequency_type = self.run_tda_enum('FrequencyType', frequency_type) if frequency_type is not None else None
            frequency = self.run_tda_enum('Frequency', frequency) if frequency is not None else None

            response = self.td_client.get_price_history(
                symbol=symbol,
                period_type=period_type,
                period=period,
                frequency_type=frequency_type,
                frequency=frequency,
                start_datetime=start_datetime_utc,
                end_datetime=end_datetime_utc,
                need_extended_hours_data=need_extended_hours_data
            )

            if response.status_code == 200:
                historical_data = response.json()
                if historical_data.get('error'):
                    logger.error(f"Error fetching historical data for {symbol}: {historical_data['error']}")
                    return None
                return historical_data.get('candles', [])
            else:
                logger.error(f"Error fetching historical data for {symbol}: {response.status_code}")
                return None
        except Exception as e:
            logger.error(f"Error fetching historical data for {symbol}: {e}")
            log_exception(e)
            return None


    # Loop thru endpoint to gather data as per date ranges determined by matching get_timedelta_limit_for_{method_name}()
    def get_data_thru_self_get_historical_data_with_loop(self, symbol: str, start_datetime_utc: datetime, end_datetime_utc: datetime, frequency: int, frequency_type: str, need_extended_hours_data: bool) -> List[Dict[str, Union[str, int, float]]]:
        """
        Retrieves historical price data for a given symbol within a specified date range and frequency,
        using the `get_historical_data` method, with the date range split up into smaller chunks if necessary.
        
        Parameters:
            symbol (str): The symbol for which to retrieve historical data.
            start_date (datetime): The starting date of the historical data to retrieve.
            end_date (datetime): The ending date of the historical data to retrieve.
            frequency (int): The number of units of `frequency_type` between each data point.
            frequency_type (str): The type of frequency at which to retrieve the historical data
                                  (e.g. 'minute', 'daily', 'weekly', 'monthly').
            need_extended_hours_data (bool): Whether to retrieve data from extended trading hours.
        
        Returns:
            List[Dict[str, Union[str, int, float]]]: A list of dictionaries, with each dictionary representing a data point.
        """
        result = []
        current_start_datetime_utc = start_datetime_utc
        current_end_datetime_utc = end_datetime_utc

        timedelta_limit = self.get_timedelta_limit_for_self_get_historical_data(frequency_type)

        while current_start_datetime_utc < end_datetime_utc:
            current_end_date = min(current_start_datetime_utc + timedelta_limit, end_datetime_utc)

            try:
                chunk = self.get_historical_data(symbol, start_datetime_utc=current_start_datetime_utc, end_datetime_utc=current_end_datetime_utc, frequency=frequency, frequency_type=frequency_type, need_extended_hours_data=need_extended_hours_data)
                result.extend(chunk)
            except Exception as e:
                logger.error(f"Error fetching historical data for {symbol} between {current_start_datetime_utc} and {current_end_datetime_utc}: {e}")
                log_exception(e)

            current_start_datetime_utc += timedelta_limit

        return result
    

    # Determine how we can loop thru the data to get the data in the date range user species, and for locating missing data and 
    # finding the best way to merge missing data dates into fewer date windows for api request to fill missing data.
    def get_timedelta_limit_for_self_get_historical_data_special_endpoint(self, frequency: int, frequency_type: str) -> timedelta:
        """
        Determines the time delta limit to use for a given frequency and frequency type, assuming the data is being retrieved
        from a specialized endpoint.

        Parameters:
            frequency (int): The frequency of data points.
            frequency_type (str): The type of frequency for data points.

        Returns:
            timedelta: The time delta limit to use when retrieving data from the specialized endpoint.
        Raises:
            ValueError: If the input frequency or frequency_type is invalid.
        """
        if frequency_type == 'minute':
            if frequency in [1, 5, 10, 15, 30]:
                return timedelta(days=365)  # assuming 365 days of data for these minute frequencies
            elif frequency == 60:
                return timedelta(days=365 * 2)  # assuming 2 years of data for 60 minutes frequency
            elif frequency == 240:
                return timedelta(days=365 * 10)  # assuming 10 years of data for 240 minutes frequency
            else:
                raise ValueError(f"Invalid frequency {frequency} for frequency_type {frequency_type}")
        elif frequency_type == 'daily':
            return timedelta(days=365 * 20)  # assuming 20 years of data for daily frequency
        elif frequency_type in ['weekly', 'monthly'] and frequency == 1:
            return timedelta(days=365 * 50)  # assuming 50 years of data for weekly and monthly frequency
        else:
            raise ValueError(f"Invalid frequency_type {frequency_type}")


    # Determine how we can loop thru the data to get the data in the date range user species, and for locating missing data 
    # and finding the best way to merge missing data dates into fewer date windows for api request to fill missing data
    def get_timedelta_limit_for_self_get_historical_data(self, frequency_type: str) -> timedelta:
        """
        Determines the timedelta limit for the `get_data_thru_self_get_historical_data_with_loop` function.

        Parameters:
        frequency_type (str): The type of the frequency to retrieve.

        Returns:
        timedelta: The timedelta limit for the `get_data_thru_self_get_historical_data_with_loop` function.
        """
        if frequency_type == 'minute':
            return timedelta(days=10)
        elif frequency_type in ['daily', 'weekly', 'monthly']:
            return timedelta(days=365 * 10)
        else:
            raise ValueError(f"Invalid frequency_type: {frequency_type}. Please use one of the following: 'minute', 'daily', 'weekly', or 'monthly'.")

    @staticmethod
    def process_date_ranges_for_td_ameritrade_historical_data(date_ranges: Union[Tuple[datetime, datetime], List[Tuple[datetime, datetime]]], max_timedelta: timedelta) -> List[Tuple[datetime, datetime]]:
        """
        Process date ranges to combine them when possible and split them into smaller chunks when they exceed the
        maximum timedelta allowed for each API request.

        Parameters:
            date_ranges (List[Tuple[datetime, datetime]] or Tuple[datetime, datetime]): 
                A list of tuples representing the missing date ranges or a single tuple. Each tuple consists of two datetime objects:
                the start date and the end date. These datetime objects must be timezone-aware and in UTC. The date ranges should ideally 
                be sorted in ascending order by start date for the function to work correctly.
            max_timedelta (timedelta): 
                The maximum timedelta allowed for each API request.

        Returns:
            List[Tuple[datetime, datetime]]: 
                A list of processed date ranges. Each tuple consists of two datetime objects: the start date and the end date.
                The date ranges are combined when possible and split into smaller chunks when they exceed the maximum timedelta.

        Notes:
            1. The function is expecting datetime objects in UTC. It's not converting any timestamps to UTC, so make sure 
            that the data you pass to this function is already in UTC.
            2. The function does not handle timezone-naive datetime objects. If timezone-naive datetime objects are passed 
            to this function, it might lead to unexpected results. Ensure the datetime objects are timezone-aware 
            (i.e., they have timezone information associated with them).
            3. Ensure that the `date_ranges` passed to this function are in ascending order. The function is sorting the ranges 
            by start date, so if they are not in ascending order, the results may not be what you expect.
        """
        if not date_ranges:
            return []

        if isinstance(date_ranges, tuple):
            date_ranges = [date_ranges]

        # Sort date ranges by start_date
        date_ranges = sorted(date_ranges, key=lambda x: x[0])

        processed_date_ranges = []
        current_start_datetime_utc, current_end_datetime_utc = date_ranges[0]

        for start_datetime_utc, end_datetime_utc in date_ranges[1:]:
            if end_datetime_utc - current_start_datetime_utc <= max_timedelta and start_datetime_utc <= current_end_datetime_utc:
                # Combine date ranges if they can fit within max_timedelta
                current_end_datetime_utc = max(current_end_datetime_utc, end_datetime_utc)
            else:
                # Split the current date range into smaller chunks if it exceeds max_timedelta
                while current_end_datetime_utc - current_start_datetime_utc > max_timedelta:
                    processed_date_ranges.append((current_start_datetime_utc, current_start_datetime_utc + max_timedelta))
                    current_start_datetime_utc += max_timedelta

                # Add the current date range to the processed list and start a new date range
                processed_date_ranges.append((current_start_datetime_utc, current_end_datetime_utc))
                current_start_datetime_utc, current_end_datetime_utc = start_datetime_utc, end_datetime_utc

        # Split the last date range into smaller chunks if it exceeds max_timedelta
        while current_end_datetime_utc - current_start_datetime_utc > max_timedelta:
            processed_date_ranges.append((current_start_datetime_utc, current_start_datetime_utc + max_timedelta))
            current_start_datetime_utc += max_timedelta

        # Add the last date range
        processed_date_ranges.append((current_start_datetime_utc, current_end_datetime_utc))

        return processed_date_ranges


    # For use in get_historical_data_from_td_ameritrade so that pandas can determine which date windows are still needed between each endpoint attempt
    def td_frequency_to_pandas_frequency(self, frequency_type, frequency):
        frequency_type_map = {
            'minute': 'T',
            'daily': 'D',
            'weekly': 'W',
            'monthly': 'M'
        }
        
        pandas_frequency = frequency_type_map.get(frequency_type.lower(), None)
        if pandas_frequency is None:
            raise ValueError(f"Unsupported frequency_type: {frequency_type}")
        
        return f"{frequency}{pandas_frequency}"


    # Run an input value through the tda library enum logic.
    def run_tda_enum(self, enum_type: str, enum_value: str) -> Any:
        """
        Run an input value through the TD Ameritrade API enum logic.

        Parameters:
            enum_type (str): The name of the enum type.
            enum_value (str): The input value to be processed by the enum.

        Returns:
            Any: The processed value after being passed through the enum.

        Raises:
            ValueError: If an error occurs while processing the enum.
        """
        try:
            enum_class = getattr(self.td_client.PriceHistory, enum_type)
            enum_value = enum_class(enum_value)
            return enum_value
        except Exception as e:
            log_exception(e)
            logger.error(f"Error running input through TD Ameritrade API enum logic: {e}")
            return None
        

# Create custom exception for use in get_historical_data_special above
class UnsupportedFrequencyError(Exception):
    pass

