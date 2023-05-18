
from datetime import datetime, timedelta
from tda.streaming import StreamClient
from gitignore.config import CLIENT_ID, TD_ACCOUNT
from helpers.logging_helper import configure_logging, logger
from support.db import DB
import requests
import os
import json

from support.td_client_wrapper import TD_Client_Wrapper


class Scans_TD_Ameritrade:
    def __init__(self, user='TD Ameritrade Symbols Class'):
        configure_logging()
        self.td = TD_Client_Wrapper()
        self.td_client = self.td.get_instance().get_client()
        self.source = "TD Ameritrade"  # Hardcoded source
        self.updater_name = user
        self.db = DB()


    def download_watchlist(self, watchlist_name):
        if not watchlist_name:
            raise ValueError("Watchlist name not provided")

        watchlists = self.td_client.get_watchlists(account_id=None).json()

        for watchlist in watchlists:
            if watchlist['name'] == watchlist_name:
                watchlist_id = watchlist['watchlistId']
                break
        else:
            raise ValueError("Watchlist not found")

        watchlist_items = self.td_client.get_watchlist(watchlist_id).json()

        symbols_list = [item['instrument']['symbol'] for item in watchlist_items]
        return symbols_list
    
    def get_alerts(self, account='', access_token=''):
        if account == '':
             account = TD_ACCOUNT
        if access_token == '':
             access_token = self.td.get_access_token()
             print(access_token)
        
        endpoint = f'https://api.tdameritrade.com/v1/accounts/{account}/alerts'

        headers = {
            'Authorization': f'Bearer {access_token}'
        }

        response = requests.get(endpoint, headers=headers)

        if response.status_code == 200:
            alerts = response.json()
            return alerts
        else:
            logger.error(f"Error {response.status_code}: {response.text}")

    def get_td_access_token(self):
        try:
            # Load the contents of the JSON file into a Python object
            with open('gitignore/td_credentials.json', 'r') as f:
                td_credentials = json.load(f)

            # Access the access_token value
            access_token = td_credentials['token']['access_token']
            return access_token
        except Exception as e:
             logger.exception("Error retrieving access token from gitignore/td_credentials.json If you need to change file location you need to in support/scans_td_ameritrade inside the class method get_td_access_token")

"""
    def earnings_happened_last_seven_days(self):
        # Define the start and end dates for the scanner
        start_date = (datetime.today() - timedelta(days=7)).strftime('%Y-%m-%d')
        end_date = datetime.today().strftime('%Y-%m-%d')

        # Make the API call to retrieve the earnings data
        response = self.td_client.get_historical_earnings(start_date, end_date)

        # Extract the list of symbols from the response data
        earnings_data = response.json()
        symbols_list = [result['symbol'] for result in earnings_data]

        # Create a ScanDetails object
        scan_date = datetime.today().strftime('%Y-%m-%d')
        scan_name = "Earnings in Last 7 Days"
        scan_description = "Symbols with earnings reported in the last 7 days"
        scan_details = ScanDetails(scan_date, scan_name, scan_description, symbols_list)
        return scan_details

        # Store the scan_details in the database
        self.db.insert_scan_details(scan_details)

        # Print the list of symbols
        print(symbols_list)
"""

def main():
    td_scans = Scans_TD_Ameritrade()
    if td_scans.td.refresh_access_token():    
        alerts = td_scans.get_alerts()
        if alerts:
            json_file_path = f'./data/alerts/test_response.json'
            # Check if the directory exists
            if not os.path.exists(os.path.dirname(json_file_path)):
                os.makedirs(os.path.dirname(json_file_path))
            # Open the file in write mode  
            with open(json_file_path, 'w') as json_file:
                json.dump(alerts, json_file)
    else:
        logger.error("Failed to refresh access token")


if __name__ == '__main__':
    main()
