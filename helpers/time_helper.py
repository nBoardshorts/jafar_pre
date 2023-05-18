# helpers/time_helper.py
from typing import Union
from datetime import datetime, timezone, date, time
import pytz
from pytz import timezone

def get_current_datetime_utc(tz: str = 'US/Eastern') -> datetime:
    """
    Returns the current datetime as a timezone-aware datetime object. 
    The timezone defaults to 'US/Eastern' but can be specified.

    Args:
        tz (str, optional): The timezone for the datetime object. Defaults to 'US/Eastern'.

    Returns:
        datetime: The current datetime in the specified timezone.
    """
    tz = timezone(tz)
    time = datetime.now(tz)
    return time

def get_start_of_current_year(tz: str = 'US/Eastern') -> datetime:
    now = get_current_datetime_utc(tz=tz)
    start_of_year = datetime(now.year, 1, 1, tzinfo=now.tzinfo)
    return start_of_year

def timestamp_utc_ms_to_datetime_utc(timestamp_utc_ms: int) -> datetime:
    """
    Converts a UNIX timestamp in milliseconds to a UTC datetime object.

    Args:
        unix_timestamp_ms (int): The UNIX timestamp in milliseconds.

    Returns:
        datetime: The corresponding UTC datetime object.
    """
    timestamp_s = timestamp_utc_ms / 1000  # Convert to seconds
    datetime_utc = datetime.utcfromtimestamp(timestamp_s)  # Convert to UTC datetime

    return datetime_utc

def datetime_utc_to_timestamp_utc_ms(date: datetime) -> int:
    """
    Converts a datetime object to a UTC timestamp in milliseconds.
    
    Args:
        date (datetime): The datetime object to convert.

    Returns:
        int: The resulting UTC timestamp in milliseconds.
    """
    utc = pytz.UTC
    if date.tzinfo is None or date.tzinfo.utcoffset(date) is None:
        date = utc.localize(date)
    timestamp_seconds = date.timestamp()
    return int(timestamp_seconds * 1000)  # Convert to milliseconds


def if_date_date_to_datetime(date):
    """
    Convert a date object to a datetime object with timezone.

    Args:
        date (date): The date object to convert.

    Returns:
        datetime: The converted datetime object in UTC if the input was a date.
    """
    if isinstance(date, datetime.date):
        date = datetime.combine(date, datetime.min.time())
        # Assume the date is in the 'US/Eastern' timezone
        et = timezone('US/Eastern')
        date = et.localize(date)
        # Convert to UTC
        utc = timezone('UTC')
        date = date.astimezone(utc)
    return date


def date_or_date_str_to_datetime_utc(date_string: Union[str, datetime, date], timezone='US/Eastern') -> datetime:
    """
    Converts a date string, datetime object, or date object into a datetime object in UTC.
    If the input is a datetime object without timezone information or a date string, it is
    assumed to represent a time in the US/Eastern timezone. If the input is a datetime object
    with timezone information, it is converted to UTC as is. If the input is a date object,
    it is assumed to represent a date in UTC.
    
    Args:
        date_string (Union[str, datetime, date]): The date string, datetime object, or date object to convert.
        timezone (str): The timezone of the date_string. Should be a string from the tz (IANA) database 
                        like 'US/Eastern', 'Europe/London', etc. Defaults to 'US/Eastern'.

    Returns:
        datetime: The resulting datetime object in UTC.
    """
    utc = pytz.UTC
    tz = pytz.timezone(timezone)

    # Check if date_string is already a datetime object
    if isinstance(date_string, datetime):
        if date_string.tzinfo is None:
            # If datetime object has no tzinfo, assume it's in the specified timezone
            date_string = tz.localize(date_string)
        return date_string.astimezone(utc)
    # Check if date_string is a date object and convert it to datetime
    elif isinstance(date_string, date):
        return datetime.combine(date_string, time(), tzinfo=utc)

    formats = ['%Y-%m-%d', '%m-%d-%Y', '%m/%d/%Y', '%d-%m-%Y', '%d/%m/%Y']
    for fmt in formats:
        try:
            # Assume date string is in the specified timezone
            date = datetime.strptime(date_string, fmt)
            date = tz.localize(date)
            return date.astimezone(utc)
        except ValueError:
            pass
    raise ValueError("No valid date format found for '{}'".format(date_string))
