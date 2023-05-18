# helpers/db_query_helper.py
from sqlalchemy import select, Table
from support.base import Base
import json
from datetime import datetime, time
from models import Symbol_TD_Ameritrade
from models import Exchange_EODHistoricalData
from models import Symbol_EODHistoricalData
from models import Entity
from support.db import DB
from helpers.data_helper import single_column_list_to_csv
from helpers.logging_helper import configure_logging, logger

configure_logging()
db = DB()

def get_market_hours(symbol):
    # Get entity associated with the symbol
    with db.session_scope() as session:
        entity = session.query(Entity).filter(Entity.code == symbol).one_or_none()

    # Get associated exchange
    exchange = entity.exchange_data

    # Parse trading hours
    trading_hours = json.loads(exchange.trading_hours)
    open_time_utc = datetime.strptime(trading_hours["OpenUTC"], "%H:%M:%S").time()
    close_time_utc = datetime.strptime(trading_hours["CloseUTC"], "%H:%M:%S").time()

    return open_time_utc, close_time_utc


# Entity Table in database trade_house
def get_gics_sector(entity_id):
    with db.session_scope() as session:
        entity = session.query(Entity).filter(Entity.id == entity_id).one_or_none()
        return entity.gics_sector if entity else None

def get_symbols_by_gics_sector(gics_sector):
    with db.session_scope() as session:
        symbols = session.query(Entity.symbol).filter(Entity.gics_sector == gics_sector).all()
        # Flatten the list of tuples returned by the query
        symbols = [symbol[0] for symbol in symbols]
        return symbols

def get_entity_id_from_symbol(symbol):
    with db.session_scope() as session:
        entity_id = session.query(Entity.id).filter(Entity.code == symbol).one_or_none()
        return entity_id

def get_exchange_for_symbol(symbol):
    with db.session_scope() as session:
        entity = session.query(Entity).filter(Entity.code == symbol).one_or_none()
        if entity:
            exchange = entity.exchange
            return exchange
        else:
            return None
        
def get_table(table_name):
        """Example Usage: partition_table = get_table(partition_name) """
        table = Table(table_name, Base.metadata, autoload_with=db.engine, extend_existing=True)
        return table

def get_entity_type_from_entity_id(entity_id):
    with db.session_scope() as session:
        entity = session.query(Entity).filter(Entity.id == entity_id).one_or_none()
    return entity.type if entity else None


# working with symbols between td_ameritrade and eodhistoricaldata
def get_all_symbols_for_td_ameritrade_and_eodhistoricaldata() -> list:
    """
    Retrieve a list of US-based stock symbols that are present in both the Symbol_EODHistoricalData and 
    Symbol_TD_Ameritrade tables.

    Returns:
        list: A list of strings, where each string represents a US-based stock symbol in the format "<symbol>.US".

    Raises:
        ValueError: If no US-based symbols are found in either table.
    """
    try:
        eod_us_symbols = get_all_us_symbols_eodhistoricaldata()
        td_symbols_set = get_all_symbols_td_ameritrade_set()

        # Create new list by iterating through each tuple in a list of US-based stock symbols from 
        # Symbol_EODHistoricalData and checking whether the symbol name is also present in a set of
        # symbols retrieved from Symbol_TD_Ameritrade
        filtered_symbols = [symbol[0] for symbol in eod_us_symbols if symbol[0] in td_symbols_set]
        if not filtered_symbols:
            raise ValueError("No US-based symbols found in either the Symbol_EODHistoricalData or Symbol_TD_Ameritrade tables")
        request_symbols = [get_eod_symbol_code(symbol) for symbol in filtered_symbols]
        # Save list for use later
        single_column_list_to_csv(request_symbols, 'data/symbol_lists/master_td_eod_symbol_list_4_24_23.csv')
        return request_symbols
    except Exception as e:
        print(f"An error occurred: {e}")
        raise

def get_all_us_based_symbols_for_td_ameritrade_and_eodhistoricaldata() -> list:
    """
    Retrieve a list of US-based stock symbols that are present in both the Symbol_EODHistoricalData and 
    Symbol_TD_Ameritrade tables.

    Returns:
        list: A list of strings, where each string represents a US-based stock symbol in the format "<symbol>.US".

    Raises:
        ValueError: If no US-based symbols are found in either table.
    """
    try:
        eod_us_symbols = get_all_us_symbols_eodhistoricaldata()
        td_symbols_set = get_all_symbols_td_ameritrade_set()

        # Create new list by iterating through each tuple in a list of US-based stock symbols from 
        # Symbol_EODHistoricalData and checking whether the symbol name is also present in a set of
        # symbols retrieved from Symbol_TD_Ameritrade
        filtered_symbols = [symbol[0] for symbol in eod_us_symbols if symbol[0] in td_symbols_set]
        if not filtered_symbols:
            raise ValueError("No US-based symbols found in either the Symbol_EODHistoricalData or Symbol_TD_Ameritrade tables")

        request_symbols = [f"{symbol}.US" for symbol in filtered_symbols]
        return request_symbols
    except Exception as e:
        print(f"An error occurred: {e}")
        raise

# symbol_td_ameritrade queries
def get_symbol_cusip(symbol: str) -> str:
    """
    Retrieves the cusip for a given symbol from the Symbol_TD_Ameritrade table.

    Args:
        symbol (str): Symbol to query for.

    Returns:
        str: CUSIP value for the given symbol.

    Raises:
        ValueError: If no symbol is found with the given name.
    """
    with db.session_scope() as session:
        try:
            symbol = symbol.strip().upper()
            result = session.execute(
                select(Symbol_TD_Ameritrade.cusip).where(Symbol_TD_Ameritrade.symbol == symbol)
            ).first()
            if result is None:
                logger.error(f"No symbol found with name '{symbol}'")
                raise ValueError(f"No symbol found with name '{symbol}'")
            return result[0]
        except Exception as e:
            logger.exception(f"An error occurred: {e}")
            session.rollback()
            raise

def get_all_symbols_td_ameritrade_set() -> set:
    """
    Retrieve all symbols from the Symbol_TD_Ameritrade table.

    Args:
        

    Returns:
        set: Set of all symbols.

    Raises:
        ValueError: If no symbols are found in the table.
        
    """
    with db.session_scope() as session:
        try:
            symbols = session.query(Symbol_TD_Ameritrade.symbol).all()
            #print(symbols)
            if not symbols:
                logger.error(f"No symbols found in Symbol_TD_Ameritrade table")
                raise ValueError(f"No symbols found in Symbol_TD_Ameritrade table")
            logger.info("Retrieved all symbols from TD Ameritrade")
            return set(symbol[0] for symbol in symbols)
        except Exception as e:
            logger.exception(f"An error occurred: {e}")
            session.rollback()
            raise

def get_all_symbols_td_ameritrade_list() -> list:
    """
    Retrieve all symbols from the Symbol_TD_Ameritrade table.

    Args:

    Returns:
        list: List of all symbols.

    Raises:
        ValueError: If no symbols are found in the table.
    """
    with db.session_scope() as session:
        try:
            symbols = session.query(Symbol_TD_Ameritrade.symbol).all()
            if not symbols:
                logger.error("No symbols found in Symbol_TD_Ameritrade table")
                raise ValueError("No symbols found in Symbol_TD_Ameritrade table")
            return [symbol[0] for symbol in symbols]
        except Exception as e:
            logger.exception(f"An error occurred: {e}")
            session.rollback()
            raise

    
#symbols_eodhistoricaldata queries
def get_all_symbols_eodhistoricaldata() -> list:
    """
    Retrieve all symbols from the Symbol_EODHistoricalData table.

    Args:
        None

    Returns:
        list: List of tuples, where each tuple contains a single string representing a US-based symbol.

    Raises:
        None
    """
    with db.session_scope() as session:
        try:
            symbols = session.query(Symbol_EODHistoricalData.symbol).all()
            return symbols
        except Exception as e:
            logger.exception(f"An error occurred: {e}")
            session.rollback()
            raise


def get_all_us_symbols_eodhistoricaldata() -> list:
    """
    Retrieve all symbols from the Symbol_EODHistoricalData table for US-based companies.

    Args:

    Returns:
        list: List of tuples, where each tuple contains a single string representing a US-based symbol.

    Raises:

    """
    with db.session_scope() as session:
        try:
            symbols = session.query(Symbol_EODHistoricalData.symbol).filter_by(country='USA').all()
            return symbols
        except Exception as e:
            logger.exception(f"An error occurred: {e}")
            session.rollback()
            raise


def get_symbol_object_from_symbols_eodhistoricaldata(symbol_name: str, session) -> Symbol_EODHistoricalData:
    """
    Queries the Symbol_EODHistoricalData table for a symbol with the given name.

    Args:
        symbol_name (str): Name of the symbol to query for.
        session (Session): SQLAlchemy session object.

    Returns:
        Symbol_EODHistoricalData: Symbol object with the given name.

    Raises:
        ValueError: If no symbol is found with the given name.
        ValueError: If the session argument is None.
    """
    if session is None:
        logger.error("Session argument cannot be None")
        raise ValueError("Session argument cannot be None")

    try:
        symbol = session.query(Symbol_EODHistoricalData).filter_by(symbol=symbol_name).first()
        if not symbol:
            logger.error(f"No symbol found with name '{symbol_name}'")
            raise ValueError(f"No symbol found with name '{symbol_name}'")
        return symbol
    except Exception as e:
        logger.exception(f"An error occurred: {e}")
        session.rollback()
        raise

    

#exchanges_eodhistoricaldata queries   
def get_all_exchange_codes_from_exchanges_eodhistoricaldata() -> list:
    """
    Queries the Exchange_EODHistoricalData table for all exchange codes.

    Args:

    Returns:
        list: List of all exchange codes.

    Raises:
        ValueError: If no exchanges are found in the table.
    """
    with db.session_scope() as session:
        try:
            exchange_codes = session.query(Exchange_EODHistoricalData.Code).all()
            if not exchange_codes:
                logger.error(f"No exchanges found in Exchange_EODHistoricalData table")
                raise ValueError(f"No exchanges found in Exchange_EODHistoricalData table")
            return exchange_codes
        except Exception as e:
            logger.exception(f"An error occurred: {e}")
            session.rollback()
            raise


def get_eod_symbol_code(symbol):
        with db.session_scope() as session:
            symbols_eodhistoricaldata = session.query(Symbol_EODHistoricalData).filter_by(symbol=symbol).first()
            if symbols_eodhistoricaldata is None:
                raise ValueError(f"No data found for symbol: {symbol}")
            
            if symbols_eodhistoricaldata.country == "USA":
                return f"{symbol}.US"
            else:
                exchange_code = symbols_eodhistoricaldata.exchange
                return f"{symbol}.{exchange_code}"
            

def get_exchange_code_by_country_eodhistoricaldata(country: str) -> str:
    """
    Queries the Exchange_EODHistoricalData table for the exchange code with the given country.

    Args:
        country (str): Country to query for.

    Returns:
        str: Exchange code with the given country.

    Raises:
        ValueError: If no exchange is found with the given name.
    """
    with db.session_scope as session:

        try:
            exchange_code = session.query(Exchange_EODHistoricalData.Code).filter_by(Country=country).first()
            if not exchange_code:
                logger.error(f"No exchange found with country '{country}'")
                raise ValueError(f"No exchange found with country '{country}'")
            return exchange_code
        except Exception as e:
            logger.exception(f"An error occurred: {e}")
            session.rollback()
            raise

"""
Useful SQL commands:

    #unique items only
        SELECT DISTINCT "Code" FROM exchanges_eodhistoricaldata; # I had to put in quotes because capitalized first letter, the quotes must be double quotes or it takes it as a string literal instead of a column name
    
    #unique items return first row of each unique item encountered
        SELECT "Code", MIN("Name") as First_Name
        FROM exchanges_eodhistoricaldata
        GROUP BY "Code";

"""