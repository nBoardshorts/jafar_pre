# main.py
import time
from support.updater import Updater
from helpers.logging_helper import configure_logging, logger
from datetime import datetime
from support.td_ameritrade_historical import TD_Ameritrade_Historical
from support.td_ameritrade_symbols import TD_Ameritrade_Symbols
from helpers.data_helper import dict_list_to_csv

# The Updater class will automatically start the update loop, checking for updates and updating the database as needed.
# Keep in mind that the update loop runs in a separate thread, so it won't block the rest of your application. It sleeps for one hour after each update check to minimize the potential impact on your system resources.
#    def main():
#        updater = Updater()
#
#            exit_commands = {"exit", "quit", "\\q"}
#            while True:
#                user_input = input("Enter 'exit', 'quit', or '\\q' to stop the script: ").strip().lower()
#                if user_input in exit_commands:
#                    print("Stopping the script...")
#                    updater.stop()
#                    break
#
#            while not updater.is_stopped():  # Wait for the updater to stop
#                print("Waiting for the updater to finish...")
#                time.sleep(1)
#
#            print("Updater stopped. Exiting the script.")

configure_logging()
# Updater handles initialization of database
#updater = Updater()
# Instantiate the TD_Ameritrade_Historical class
td_symbols = TD_Ameritrade_Symbols()
td_hist = TD_Ameritrade_Historical()


def get_data():
    symbol = 'TSLA'
    start_date = datetime(2011, 1, 1)
    end_date = datetime.now()
    frequency = 1
    frequency_type = 'minute'
    #frequency = td_hist.td_client.PriceHistory.FrequencyType('daily')
    data = td_hist.get_historical_data(symbol, start_date, end_date, frequency, frequency_type)
    if data:
        logger.info(f"Fetched {len(data)} historical data points for {symbol}")
        dict_list_to_csv(data, 'tsla_hist_data_test.csv')
    else:
        logger.warning(f"No historical data found for {symbol}")

symbols = ['TSLA', 'TLT']

def get_instrument_info_symbol_search(symbols):
    
    instrument_info = td_symbols.search_instruments(symbols)
    return instrument_info


def main():
    updater = Updater()

    exit_commands = {"exit", "quit", "\\q"}
    while True:
        user_input = input("Enter 'exit', 'quit', or '\\q' to stop the script: ").strip().lower()
        if user_input in exit_commands:
            print("Stopping the script...")
            updater.stop()
            break

    while not updater.is_stopped():  # Wait for the updater to stop
        print("Waiting for the updater to finish...")
        time.sleep(1)

    print("Updater stopped. Exiting the script.")
    print("hello world")
    #instrument_info = get_instrument_info_symbol_search(symbols)
    #print(instrument_info)
    #info = td_symbols.get_instrument_by_symbol('tsla')
    #print(info)
    #fundamentals = td_symbols.get_fundamentals_of_symbol('tsla')
    #print(fundamentals)
    #get_data()
    

if __name__ == "__main__":
    main()