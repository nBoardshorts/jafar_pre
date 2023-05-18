#support/get_area_codes.py
import os
import codecs
import pandas as pd
from models.area_codes_centroid_point import Area_Codes_Centroid_Point
from models.cities_by_area_code import Cities_By_Area_Code
from helpers.logging_helper import configure_logging, log_exception, logger
from support.db import DB
from helpers.data_helper import to_ascii_csv_file

"""
If this ever needs to be ran, the current configuration requires you to move this file from support up to the parent project folder. When done you can put this back in the support folder
as to not have the main project folder cluttered. (Or be my guest and figure out the locations, etc for the imports)

Purpose:
Read area code data from a CSV files within data\area_codes\Area-Code-Geolocation-Database-master
there are 4 files total that will need to be read through that cover the US and Canada.
Here's a brief overview of the script's components:
CSV_DIRECTORY: A constant that specifies the directory containing the CSV files.
save_area_codes_to_database(area_codes_df, centroid=False): A function that saves area codes and their geographical information to the database. It takes an optional 'centroid' parameter to indicate whether the current CSV file contains centroid data for area codes.
save_cities_by_area_code_to_database(area_codes_df): A function that saves cities and their associated area codes to the database.
read_area_code_csv_files(file_path): A function that reads CSV files containing area codes and geographical information, returning a pandas DataFrame.
main(): The main function that reads all CSV files in the specified directory, initializes the database, and iterates through all CSV files to save area codes and their geographical information to the database.
The script also imports and uses helper functions and models from other files:
helpers/db_helper.py: Contains functions for initializing the database, creating tables, and managing database sessions.
models/area_codes_centroid_point.py: Defines the Area_Codes_Centroid_Point model, representing the area codes and their centroid points in the database.
models/cities_by_area_code.py: Defines the Cities_By_Area_Code model, representing the cities associated with area codes in the database.
The script starts by calling the main() function, which reads the CSV files, initializes the database, and saves the area codes and their geographical information to the database.
Save area codes and geographical information to the database
Flow:
Read CSV file containing area codes and geographical information
Initialize the database (create tables if they don't exist)
Save area codes and their geographical information to the database or save cities and their associated area codes to the database based on the file name
The script now handles the nuance of processing different CSV files with different information (centroid data vs city data) by calling the appropriate function in the main() function based on the file name. The main function checks the file name to determine if the CSV file contains centroid data or city data and calls the corresponding function: save_area_codes_to_database() or save_cities_by_area_code_to_database().
"""

CSV_DIRECTORY = 'data/area_codes/Area-Code-Geolocation-Database-master'

# Configure logging
configure_logging()


# Initialize the database
db = DB()


def save_area_codes_to_database(area_codes_df):

    # Iterate through each row in the DataFrame to handle area codes
    try:
        with db.session_scope() as session:
            for _, row in area_codes_df.iterrows():
                #print(row['Area Code'])
                #print(type(row['Area Code']))
                area_code = str(row['Area Code'])
                #print(area_code)
                lat = row['Latitude']
                lon = row['Longitude']

                # Check if the area code already exists in the database
                area_code_record = session.query(Area_Codes_Centroid_Point).filter_by(area_code=area_code).first()

                # If it doesn't exist, create a new record and add it to the database
                if not area_code_record:
                    area_code_record = Area_Codes_Centroid_Point(
                        area_code=area_code,
                        centroid_lat=lat,
                        centroid_lon=lon,
                    )
                    #print(area_code)
                    #print(area_code_record.area_code)
                    session.add(area_code_record)
                    session.flush()
                    #print(f"Area Code (stored): {area_code_record.area_code}")
                # If the area code exists, update the record with new values
                else:
                    area_code_record.centroid_lat = lat
                    area_code_record.centroid_lon = lon

    except Exception as e:
        logger.exception(f"Error while reading csv and saving area codes to database: {e}")
        

def save_cities_by_area_code_to_database(area_codes_df):
    # Iterate through each row in the DataFrame to handle area codes
    try:
        with db.session_scope() as session:
            for _, row in area_codes_df.iterrows():
                #print(f"Area Code (raw): {row['Area Code']}, type: {type(row['Area Code'])}")
                area_code = str(row['Area Code'])
                #print(area_code)
                city = row.get('City', None)
                if isinstance(city, str) and city.startswith('"') and city.endswith('"'):
                    city = city.strip('"')
                state_province = row['State/Province']
                country = row['Country']
                lat = row['Latitude']
                lon = row['Longitude']

                # Fetch area_code_record for foreign key reference
                #print(f"area code is", {area_code})
                area_code_record = session.query(Area_Codes_Centroid_Point).filter_by(area_code=area_code).first()
                #print(f"Area Code (retrieved): {area_code_record.area_code}")
                #print(f"area_code_record.area_code: {area_code_record.area_code}, type: {type(area_code_record.area_code)}")
                #print(area_code_record.area_code)

                # Check if the city already exists in the database
                city_record = session.query(Cities_By_Area_Code).filter_by(city=city, state=state_province, lat=lat, lon=lon).first()

                # If it doesn't exist and city information is available, create a new record and add it to the database
                if not city_record and city:
                    city_record = Cities_By_Area_Code(
                        city=city,
                        state=state_province,
                        country=country,
                        lat=lat,
                        lon=lon,
                        area_code=area_code,
                        area_code_id=area_code_record.id
                    )
                    session.add(city_record)

    except Exception as e:
        logger.exception(f"Error while reading csv and saving city records to database: {e}")



def read_area_code_csv_files(file_path):
    # Check the file extension
    if file_path.endswith('us-area-code-cities.csv') or file_path.endswith('ca-area-code-cities.csv'):
        # For the us-area-code-cities.csv and ca-area-code-cities.csv files,
        # set the column names to 'Area Code', 'City', 'State/Province', 'Country', 'Latitude', 'Longitude'
        with codecs.open(file_path, 'r', encoding='utf-8') as f:
            return pd.read_csv(file_path, header=None, names=['Area Code', 'City', 'State/Province', 'Country', 'Latitude', 'Longitude'], dtype={'Area Code': str}) # had to add dtype-{'Area Code': str} because it was typecasting my ints to doubles.
    elif file_path.endswith('us-area-code-geo.csv') or file_path.endswith('ca-area-code-geo.csv'):
        # For the us-area-code-geo.csv and ca-area-code-geo.csv files,
        # set the column names to 'Area Code', 'Latitude', 'Longitude'
        with codecs.open(file_path, 'r', encoding='utf-8') as f:
            return pd.read_csv(file_path, header=None, names=['Area Code', 'Latitude', 'Longitude'], dtype={'Area Code': str}) # had to add dtype-{'Area Code': str} because it was typecasting my ints to doubles.
    else:
        # For other CSV files, use the default settings for read_csv()
        return pd.read_csv(file_path)
    

def write_all_area_codes_to_database():
    # Read all CSV files in the specified directory
    file_list = os.listdir(CSV_DIRECTORY)

    # Sort the file_list to ensure centroid data is processed before city data
    file_list.sort()
    for file_name in file_list:
        if file_name.endswith('us-area-code-cities.csv'):
            file_path = os.path.join(CSV_DIRECTORY, file_name)
            to_ascii_csv_file(file_path)

    # Populate the area_codes_centroid_point table first
    for file_name in file_list:
        if file_name.endswith('us-area-code-geo.csv') or file_name.endswith('ca-area-code-geo.csv'):
            file_path = os.path.join(CSV_DIRECTORY, file_name)
            area_codes_df = read_area_code_csv_files(file_path)
            save_area_codes_to_database(area_codes_df)

    # Populate the cities_by_area_code second in order to ensure the relationship is set up properly
    for file_name in file_list:

        if file_name.endswith('us-area-code-cities.csv') or file_name.endswith('ca-area-code-cities.csv'):
            file_path = os.path.join(CSV_DIRECTORY, file_name)
            area_codes_df = read_area_code_csv_files(file_path)
            save_cities_by_area_code_to_database(area_codes_df)


if __name__=='__main__':
    write_all_area_codes_to_database()