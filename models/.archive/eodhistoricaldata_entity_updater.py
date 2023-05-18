
# support/eodhistoricaldata_entity_updater
import json
import requests
from models import Entity
from support.db import DB
from datetime import datetime
from eodhd import APIClient
from gitignore.config import EOD_HISTORICAL_DATA_API_KEY
from helpers.logging_helper import configure_logging, logger
from support.eodhistoricaldata_fundamentals import eodhistorical_fundamentals

configure_logging()


class Entity_Updater:
    def __init__(self, user='Entity_Updater Class'):
        self.eod_client = APIClient(EOD_HISTORICAL_DATA_API_KEY)
        self.source = 'eodhistoricaldata.com'
        self.updater_name = user
        self.db = DB()

    def update_entities(self, symbols_list, source=''):
        logger.info('Starting update on enties table.')
        for symbol in symbols_list:
            try:
                # Fetch data from the API or local JSON file for a given symbol
                # Uncomment the line below to use the API
                # json_data = self.fetch_data_from_api(symbol)

                # Uncomment the line below to use the local JSON file
                json_data = self.fetch_data_from_local_file(symbol)

                # Extract 'General' data from the JSON response
                general_data = json_data.get("General", {})

                # Clean and format the 'General' data
                entity_data = self.clean_general_data(general_data)

                # Store the 'General' data in the database
                self.store_entity_data(entity_data)
            except Exception as e:
                print(f"Error updating entity data for symbol '{symbol}': {e}")

    def fetch_data_from_api(self, symbol):
        # Replace with your actual API URL and access key
        response = response = requests.get(url)
        response.raise_for_status()
        return response.json()

    def fetch_data_from_local_file(self, symbol):
        with open("TSLA.US.fundamentals_from_eod_historical.json") as file:
            return json.load(file)

    def clean_general_data(self, general_data):
        return {
            "symbol": general_data.get("Code"),
            "isin": general_data.get("ISIN"),
            "cusip": general_data.get("CUSIP"),
            "name": general_data.get("Name"),
            "exchange": general_data.get("Exchange"),
            "currency": general_data.get("CurrencyCode"),
            "country": general_data.get("CountryName"),
            "type": general_data.get("Type"),
        }

    def store_entity_data(self, entity_data):
        with self.db.session_scope() as session:
            # Check if the entity with the given symbol already exists in the database
            existing_entity = session.query(Entity).filter_by(symbol=entity_data["symbol"]).first()

            if existing_entity:
                # Update the existing entity with the new data
                existing_entity.isin = entity_data["isin"]
                existing_entity.cusip = entity_data["cusip"]
                existing_entity.name = entity_data["name"]
                existing_entity.exchange = entity_data["exchange"]
                existing_entity.currency = entity_data["currency"]
                existing_entity.country = entity_data["country"]
                existing_entity.type = entity_data["type"]
                existing_entity.updated_at = datetime.utcnow()
            else:
                # Create a new entity with the given data and add it to the session
                new_entity = Entity(**entity_data, updated_at=datetime.utcnow())
                session.add(new_entity)
