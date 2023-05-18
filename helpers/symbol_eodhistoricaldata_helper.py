# helpers/symbol_eodhistoricaldata_helper.py
from support.db import DB
from models import Symbol_EODHistoricalData
from helpers.data_helper import remove_unknowns_and_blank_remnants_to_none, remove_unknowns_and_blank_remnants_to_none_pd_series
from helpers.db_query_helper import get_exchange_code_by_country_eodhistoricaldata, get_symbol_object_from_symbols_eodhistoricaldata


db = DB()

def get_formatted_symbol(symbol_name):
    """
    Returns the formatted symbol in the format {SYMBOL_NAME}.{EXCHANGE_CODE} by querying the 
    Symbol_EODHistoricalData table for the given symbol name and the Exchange_EODHistoricalData 
    table for the exchange code by country.

    Notes: Be certain to query the database symbols by country or exchange as to 

    Args:
        symbol_name (str): The symbol name to format.

    Returns:
        str: The formatted symbol in the format {SYMBOL_NAME}.{EXCHANGE_CODE}.

    Raises:
        ValueError: If no symbol or exchange is found in the database with the given name.
    """
    with db.session_scope() as session:
    # Get the symbol object from the database
        symbol = get_symbol_object_from_symbols_eodhistoricaldata(symbol_name, session)
        # Get the exchange object from the database using the name of the exchange
        country = symbol.country
        exchange = get_exchange_code_by_country_eodhistoricaldata(country, session)
        if not exchange:
            raise ValueError(f"No exchange_code found with name '{country}'")
        
        # Get the exchange code from the exchange object
        exchange_code = exchange.Code
        
        # Return the formatted symbol
        return f"{symbol_name}.{exchange_code}"

def extract_symbol(symbol_code):
    return symbol_code.split('.')[0]

def clean_symbols_from_eodhistorical(eodhistorical_data, source, last_updated=None, updated_by=None):
    """
    Cleans the EODHistoricalData API response for a symbol and returns a new Symbol_EODHistoricalData instance.

    Args:
        eodhistorical_data (dict): The API response data for a symbol.
        source (str): The source of the data.
        last_updated (datetime.datetime, optional): The timestamp of the last update for the data. Defaults to None.
        updated_by (str, optional): The username of the last user who updated the data. Defaults to None.

    Returns:
        models.Symbol_EODHistoricalData: A new instance of the Symbol_EODHistoricalData model with the cleaned data.

    """
    # Create a dictionary to hold the cleaned data
    cleaned_data = remove_unknowns_and_blank_remnants_to_none_pd_series(eodhistorical_data)


    # Set exchange to None if it's an empty string or None, and set status to 'inactive' if exchange is None
    exchange = cleaned_data.get('exchange')
    if not exchange:
        cleaned_data['status'] = 'inactive'

    # Create a new Symbol instance
        symbol_instance = Symbol_EODHistoricalData(
            symbol=cleaned_data.get('Code'),
            name=cleaned_data.get('Name'),
            country=cleaned_data.get('Country'),
            exchange=cleaned_data.get('Exchange'),
            currency=cleaned_data.get('Currency'),
            assetType=cleaned_data.get('Type'),
            isin=cleaned_data.get('Isin'),
            source=source,
            last_updated=last_updated,
            updated_by=updated_by,
        )
        
        return symbol_instance


def main():
    symbol_code = get_formatted_symbol('TSLA')
    print(symbol_code)

if __name__ == '__main__':
    main()