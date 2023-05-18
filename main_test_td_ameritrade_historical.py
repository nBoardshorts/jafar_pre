import os
from datetime import datetime
from support.td_ameritrade_historical import TD_Ameritrade_Historical
from helpers.data_helper import dict_list_to_csv

def main():
    # Instantiate TD_Ameritrade_Historical
    td_ameritrade_historical = TD_Ameritrade_Historical()

    # Define parameters for the get_historical_data_by_granularity method
    symbol = 'AAPL'
    start_date = datetime(2020, 1, 1)
    end_date = datetime(2020, 12, 31)
    frequency = 1
    frequency_type = 'daily'

    # Call the method and get the results
    result = td_ameritrade_historical.get_historical_data_by_granularity(symbol, start_date, end_date, frequency, frequency_type)

    # Save the results to a CSV file
    csv_filename = f"{symbol}_historical_data.csv"
    dict_list_to_csv(result, csv_filename)

    print(f"Saved historical data to {csv_filename}")

if __name__ == "__main__":
    main()
