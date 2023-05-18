#support/bls_cpi_data.py
import os
import requests
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base


from models.cpi_data import CpiData
from models.area_code_geo_info import AreaGeoInfo
from gitignore.config import BLS_GOV_API_KEY

CPI_REPORTS_FOLDER = 'data/bls/cpi_reports/'


def get_cpi_data(start_year=None, end_year=None):
    api_key = BLS_GOV_API_KEY

    # Get the most recent year available if no end_year is specified
    if end_year is None:
        end_year = datetime.now().year - 1

    # Get the earliest year available if no start_year is specified
    if start_year is None:
        start_year = 1913

    series_ids = ['CUUR0000SA0']
    headers = {
        'Content-type': 'application/json',
    }

    data = {
        "seriesid": series_ids,
        "startyear": str(start_year),
        "endyear": str(end_year),
        "registrationkey": api_key
    }

    response = requests.post('https://api.bls.gov/publicAPI/v2/timeseries/data/', json=data, headers=headers)
    response_json = response.json()

    if response_json['status'] == 'REQUEST_SUCCEEDED':
        data_list = response_json['Results']['series'][0]['data']
        cpi_data_df = pd.DataFrame(data_list)
        cpi_data_df['Date'] = pd.to_datetime(cpi_data_df['year'] + cpi_data_df['period'].str[1:], format='%Y%m')

        cpi_data_df['CPI'] = cpi_data_df['value'].astype(float)
        cpi_data_df = cpi_data_df[['Date', 'CPI']]
        return cpi_data_df
    else:
        raise ValueError('Failed to retrieve CPI data. Error message: {}'.format(response_json['message']))


def main():
    configure_logging()

    # Read CSV file
    file_path = os.path(CPI_REPORTS_FOLDER)
    df = read_csv(csv_file_path)

    # TODO: Add code to fetch CPI data using BLS API, store CPI data in the `cpi_data` table,
    # and store the geographical information in the `area_geoinfo` table.

if __name__ == '__main__':
    main()
