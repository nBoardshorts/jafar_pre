#helpers/td_ameritrade_fundamentals.py
from models import Symbol_Fundamentals_TD_Ameritrade
from support.td_ameritrade_symbols import TD_Ameritrade_Symbols, get_symbol_cusip, 
from support.td_client_wrapper import TD_Client_Wrapper
from helpers.logging_helper import configure_logging, logger
from helpers.db_query_helper import get_all_symbols_td_ameritrade_list
from support.db import DB

from datetime import datetime
from pprint import pprint
import requests
import json


configure_logging()

class TD_Ameritrade_Symbol_Fundamentals:
    def __init__(self, user='TD_Ameritrade_Symbol_Fundamentals'):
        self.td_client = TD_Client_Wrapper.get_instance().get_client()
        self.source = "TD Ameritrade" # Hardcoded source
        self.updater_name = user
        self.db = DB()

    def get_fundamentals_of_symbol(self, symbol):
        """
        Get an instrument by symbol.
        """
        with self.db.session_scope() as session:
            cusip = get_symbol_cusip(symbol, session)
            if cusip is None:
                logger.error(f"Error fetching instrument data for {symbol}: no cusip found")
                return None
            return TD_Ameritrade_Symbols.search_instruments(symbol, projection='fundamental')
        

    def save_fundamentals_to_db(self, fundamentals_data):
        with self.db.session_scope() as session:
            fundamentals_data = fundamentals_data['fundamental']
            fundamentals = Symbol_Fundamentals_TD_Ameritrade(timestamp=datetime.now(), **fundamentals_data)
            session.add(fundamentals)

def main():
    fundamentals_td = TD_Ameritrade_Symbol_Fundamentals()
   
    symbols = get_all_symbols_td_ameritrade_list()
    for symbol in symbols:
        fundamentals = fundamentals_td.get_fundamentals_of_symbol(symbol)
        if fundamentals:
            #pprint(f"Fundamentals of {symbol}: {fundamentals}")
            fundamentals_td.save_fundamentals_to_db(fundamentals)