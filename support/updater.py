# support/updater.py
import time
import json
from datetime import timedelta
import threading
from support.db import DB
from helpers.time_helper import get_current_datetime_utc
from helpers.data_helper import read_symbols_from_csv
from support.td_ameritrade_symbols import TD_Ameritrade_Symbols
from support.eodhistoricaldata_exchanges import EODHistoricalData_Exchanges
from support.eodhistoricaldata_symbols import EODHistoricalData_Symbols
from support.eodhistoricaldata_fundamentals import EODHistoricalData_Fundamentals
from support.get_area_codes import write_all_area_codes_to_database
from support.get_cpi_codes import cpi_code_csvs_to_db_tables
from models import Update_Tracking
from helpers.logging_helper import configure_logging, log_exception, logger


# Purpose: The Updater class is responsible for coordinating the update process for various data sources. It handles the scheduling and tracking of updates by interacting with the Update_Tracking table. The Update_Handler class is responsible for managing updates for different tables.
#
# Workflow:
#
# 1. Initialize the Updater with the TD_Client_Wrapper instance.
# 2. The Updater initializes the UpdateHandler instance to handle updates for different tables.
# 3. The Updater sets the update frequency information in the Update_Tracking table.
# 4. The Updater starts the update thread to regularly call the update_all() method.
# 5. update_all() checks if updates are needed for each table and calls the UpdateHandler to update the table if needed.
#
# Criteria:
#
# 1. Updater should manage the update frequency information in the Update_Tracking table.
# 2. Updater should call the UpdateHandler to perform updates for the appropriate tables.
# 3. Updater should reference the latest_update field in the Update_Tracking table to determine when to update.
# 4. Updater should set the 'last_updated' field with the current date.
#
# Usage Instructions:
#
# Ensure that all necessary packages and modules are installed and available in your project.
# Configure the gitignore.config file with the necessary settings (e.g., DATABASE_URL).
# Make sure the Updater class is properly configured with the correct update frequencies for the required tables in the update_frequencies variable.
# Run the script as a standalone module (e.g., python updater.py) or import the Updater class and create an instance of it in your project.
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


                    #days, hours, seconds, microseconds, milliseconds, weeks
update_frequencies = {'trade_house_whole_database_backup': {'value': 1, 'unit': 'days'},
                      'symbols_td_ameritrade': {'value': 90, 'unit': 'days'},
                      'exchanges_eodhistoricaldata': {'value': 90, 'unit': 'days'},
                      'symbols_eodhistoricaldata': {'value': 90, 'unit': 'days'},
                      'entities.general': {'value': 90, 'unit': 'days',}
                      # TODO add logic for only conducting certain updates on weekends for api request count 
                      } 
                    #### The below were used to populate the database initially but are not needed for updates at this time
                    #'area_codes': {'value': 1, 'unit': 'milliseconds'},
                    #'cpi_codes': {'value':1, 'unit': 'milliseconds'},

configure_logging() 


class Updater:
    """
    The Updater class is responsible for coordinating the update process for various data sources.
    """
    def __init__(self):
        """
        Initializes the Updater instance with the TD Ameritrade API client and database instance.
        """
        self.db = DB()
        self.update_handler = Update_Handler(self)
        self.initialize_update_tracking()
        self.user = 'Updater'
        #self.start_update_thread() # Comment out self.update_loop() if wish to employ threading again, turned off for testing may forget to turn on until threading is issue
        self.update_loop()

    def initialize_update_tracking(self):
        """
        Initializes the update tracking table with the update frequencies for each table.
        """
        with self.db.session_scope() as session:
            for table_name, frequency_data in update_frequencies.items():
                frequency_dict = {'value': frequency_data['value'], 'unit': frequency_data['unit']}
                frequency_json = json.dumps(frequency_dict)
                update = session.query(Update_Tracking).filter_by(table_name=table_name).first()
                if update is None:
                    update = Update_Tracking(table_name=table_name, frequency=frequency_json)
                    session.add(update)
                elif update.frequency != frequency_json:
                    update.frequency = frequency_json
                    session.add(update)
    
    def update_last_updated_time(self, table_name):
        """
        Updates the 'last_updated' field of the specified table in the update tracking table.
        """
        with self.db.session_scope() as session:
            update = session.query(Update_Tracking).filter_by(table_name=table_name).first()
            if update is not None:
                update.last_updated = get_current_datetime_utc()
            else:
                new_update = Update_Tracking(table_name=table_name, last_updated=get_current_datetime_utc())
                session.add(new_update)
        logger.info(f"Successfully updated last_updated time for table: {table_name}")

    
    def update_all(self, check_for_changes=True):
        """
        Updates all tables in the database if needed based on the update tracking table.
        """
        with self.db.session_scope() as session:
            tables = session.query(Update_Tracking).all()

            for table in tables:
                frequency_dict = json.loads(table.frequency)
                update_interval = timedelta(**{frequency_dict['unit']: frequency_dict['value']})
                logger.info(f'Checking if {table.table_name} needs update...')
                if (table.last_updated is None or (table.last_updated is not None and get_current_datetime_utc() - table.last_updated > update_interval)):
                    self.db.backup_table(table.table_name)
                    logger.info(f'Starting update for {table.table_name}') 
                    self.update_handler.update(table.table_name)
                else:
                    logger.info(f'No update needed for {table.table_name}')


    def start_update_thread(self):
        """
        Starts a new thread to run the update loop.
        """
        self.stop_event = threading.Event()
        self.stopped_event = threading.Event()
        # Create a new thread to run the update loop
        update_thread = threading.Thread(target=self.update_loop)
        update_thread.daemon = True # Set the thread to run in the background
        update_thread.start()

    def stop(self):  # Modify this method to clear the stopped event
        self.stop_event.set()
        self.stopped_event.clear()

    def is_stopped(self):  # Add this method to check if the updater has stopped
        return self.stopped_event.is_set()
    
    # Main update loop
    def update_loop(self):
        """
        The main update loop that periodically checks for updates and updates the database.
        """
        # Uncomment the below if need to turn event tracking back on for background updating
        #while not self.stop_event.is_set():
        #    self.stopped_event.clear() # Clear the stopped event at the beginning of the loop
        try:
            logger.info(f'Beginning updater tasks {get_current_datetime_utc()}')
            self.update_all()
        except Exception as e:
            log_exception(e)
            logger.error(f'Error while saving data to database')
        logger.info(f"update_all completed by main at {get_current_datetime_utc()}")
        logger.info(f"sleeping for one hour before checking for updates again. Sleep started at {get_current_datetime_utc()}")
        # Uncomment the below if need to turn event tracking back on for background updating
        #self.stopped_event.set()
        time.sleep(60 * 60)  # Sleep for an hour before checking for updates again

class Update_Handler():
    """
    The UpdateHandler class is responsible for managing updates for different tables.
    """
    def __init__(self, updater):
        """
        Initializes the UpdateHandler instance with the given TD Ameritrade API client wrapper and database instance.
        """
        self.updater = updater
        self.user = 'Update_Handler'
        self.db = DB()


    def update(self, table):
        """
        Calls the appropriate update method for the specified table.
        """
        if table == 'trade_house_whole_database_backup':
            try:
                self.db.double_backup_database()
            except Exception as e:
                logger.exception(f'Exception while backing up database: {str(e)}')
        if table == 'symbols_td_ameritrade':
            self.update_symbols_td_ameritrade()
        if table == 'exchanges_eodhistoricaldata':
            self.update_exchanges_eodhistoricaldata()
        if table == 'symbols_eodhistoricaldata':
            self.update_symbols_eodhistoricaldata()
        if table == 'entities.general':
              self.update_entities_general_section()
        if table == 'area_codes':
            self.update_area_codes()
        if table == 'cpi_codes':
            self.update_cpi_codes()

    def run_database_backup(self):
        self.db.double_backup_database()
        self.updater.update_last_updated_time('trade_house_whole_database_backup')

    def update_cpi_codes(self):
        cpi_code_csvs_to_db_tables()
        self.updater.update_last_updated_time('cpi_codes')
        logger.info(f'Updated cpi_codes table')

    def update_area_codes(self):
        """
        Updates the area codes geodata tables from data in data folder.
        """
        write_all_area_codes_to_database()
        self.updater.update_last_updated_time('area_codes')

    def update_symbols_td_ameritrade(self):
        """
        Updates the symbols table using the TD Ameritrade API client wrapper.
        """
        td_ameritrade_symbols = TD_Ameritrade_Symbols(user=self.user)
        update_time = get_current_datetime_utc()
        td_ameritrade_symbols.update_symbols(update_time=update_time)
        self.updater.update_last_updated_time('symbols_td_ameritrade')
        logger.info(f'Updated symbols_td_ameritrade at {update_time}')

    def update_exchanges_eodhistoricaldata(self):
        """
        Updates the exchanges_eodhistoricaldata table using the eodhistoricaldata.com API client
        """
        eodhistoricaldata_exchanges = EODHistoricalData_Exchanges(user=self.user)
        update_time = get_current_datetime_utc()
        #eodhistoricaldata_exchanges.get_all_exchanges()
        eodhistoricaldata_exchanges.get_exchange_details_for_all()
        self.updater.update_last_updated_time('exchanges_eodhistoricaldata')
        logger.info(f'Updated exchanges_eodhistorical at {update_time}')

    def update_symbols_eodhistoricaldata(self):
        """
        Updates the symbols_eodhistorcaldata table.
        """
        eodhistoricaldata_symbols = EODHistoricalData_Symbols(user=self.user)
        update_time = get_current_datetime_utc()
        eodhistoricaldata_symbols.update_symbols(update_time=update_time)
        self.updater.update_last_updated_time('symbols_eodhistoricaldata')
        logger.info(f'Updated symbols_eodhistoricaldata table.')

    def update_entities_general_section(self):
        """
        Updates the entites table.
        """
        eodhistoricaldata_fundamentals = EODHistoricalData_Fundamentals(user=self.user)
        update_time = get_current_datetime_utc()
        #symbol_list = get_all_symbols_for_td_ameritrade_and_eodhistoricaldata()
        symbol_list = read_symbols_from_csv('C:\\Users\\mitch\\OneDrive\\io\\git\\backtests\\data\\symbol_lists\\master_td_eod_symbol_list_4_24_23.csv')
        eodhistoricaldata_fundamentals.update_general_section_for_each_symbol_in_list(symbol_list, update_time=update_time)
        self.updater.update_last_updated_time('entites')
        logger.info(f'Updated entites table.')

if __name__ == "__main__":

    updater = Updater()