# helpers/data_helper.py
import os
import csv
import re
import json
import pandas as pd
from helpers.time_helper import get_current_datetime_utc
from helpers.logging_helper import logger, configure_logging

configure_logging()


def update_last_processed_symbol(filename, symbol):
    with open(filename, 'w') as f:
        f.write(symbol)

def read_last_processed_symbol(filename):
    os.makedirs(os.path.dirname(os.path.abspath(filename)), exist_ok=True)
    if not os.path.isfile(filename):
        with open(filename, 'w') as f:
            f.write('')
    with open(filename, 'r') as f:
        symbol = f.readline().strip()
        return symbol if symbol else None


def add_source_and_updated_info(data, source, last_updated, updated_by):
    for record in data:
        record['source'] = source
        record['last_updated'] = last_updated
        record['updated_by'] = updated_by

def camel_to_snake_case(name):
    """
    A function to convert camelCase or PascalCase to snake_case.
    """
    # Cover nuances from eodhistoricaldata
    if name == 'GicSector':
        return 'gics_sector'
    if name == 'GicGroup':
        return 'gics_group'
    if name == 'GicIndustry':
        return 'gics_industry'
    if name == 'GicSubIndustry':
        return 'gics_sub_industry'
    
    # Cover nuance of all caps letters
    # If the name is all uppercase, simply return the lowercase version of the name
    if name.isupper():
        return name.lower()

    # Insert an underscore before consecutive uppercase letters
    name = re.sub(r'([A-Z]{2,})', r'_\1', name)

    # Split words with underscores
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()


def read_symbols_from_csv(file_path):
    with open(file_path, newline='', encoding='utf-8') as csvfile:
        reader = csv.reader(csvfile)
        next(reader)  # skip header
        symbols = [row[0] for row in reader]
    return symbols

#file_name = f"cpi_data_{current_time}.csv"
def list_to_csv(data: list, filename: str):
    """
    Writes a list of tuples to a CSV file.

    Args:
        data (list): A list of tuples.
        filename (str): The name of the file to write to.

    Returns:
        None

    Raises:
        ValueError: If the data argument is not a list of tuples.
        ValueError: If the filename argument is not a string.
    """
    if not isinstance(data, list) or not all(isinstance(item, tuple) for item in data):
        logger.error("The data argument must be a list of tuples")
        raise ValueError("The data argument must be a list of tuples")

    if not isinstance(filename, str):
        logger.error("The filename argument must be a string")
        raise ValueError("The filename argument must be a string")

    try:
        with open(filename, mode='w', newline='') as file:
            writer = csv.writer(file)
            writer.writerows(data)
    except Exception as e:
        logger.exception(f"An error occurred while writing to file {filename}: {e}")
        raise

def single_column_list_to_csv(data: list, filename: str):
    """
    Writes a single column list to a CSV file.

    Args:
        data (list): A list of items.
        filename (str): The name of the file to write to.

    Returns:
        None

    Raises:
        ValueError: If the data argument is not a list.
        ValueError: If the filename argument is not a string.
    """
    if not isinstance(data, list):
        logger.error("The data argument must be a list")
        raise ValueError("The data argument must be a list")

    if not isinstance(filename, str):
        logger.error("The filename argument must be a string")
        raise ValueError("The filename argument must be a string")

    try:
        os.makedirs(os.path.dirname(filename), exist_ok=True)  # create directory if it does not exist
        with open(filename, mode='w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(["data"])
            for item in data:
                writer.writerow([item])
    except Exception as e:
        logger.exception(f"An error occurred while writing to file {filename}: {e}")
        raise


def json_to_df(json_dict):
    """
    Converts a json dict response to a Pandas DataFrame.

    Args:
        json_dict: JSON response object.

    Returns:
        Pandas DataFrame object.

    Raises:
        ValueError: If the response object is empty.
    """
    if not json_dict:
        logger.error("Argument for json_to_df() was empty")
        raise ValueError('Response is empty')
        
    try:
        # Convert dictionary to JSON string
        json_str = json.dumps(json_dict)
        # Load JSON string to pandas DataFrame
        df = pd.read_json(json_str)
        return df
    except Exception as e:
        logger.exception(f"An error occurred: {e}")
        raise


def relative_path_to_file_path(relative_path):
    """
    Converts a relative file path to an absolute file path.

    Args:
        relative_path (str): A string representing the relative file path.

    Returns:
        str: A string representing the absolute file path.

    Raises:
        ValueError: If the relative_path argument is not a string.
    """
    try:
        if not isinstance(relative_path, str):
            raise ValueError("The relative_path argument must be a string") 

        current_dir = os.getcwd()
        file_path = os.path.join(current_dir, *relative_path.split('/'))
        return file_path
    except Exception as e:
        logger.error(f"Error in relative_path_to_file_path() with relative_path={relative_path}: {e}")
        raise



def dict_list_to_csv(dict_list, csv_file_name):
    """
    Writes a list of dictionaries to a CSV file.

    Args:
        dict_list (list): A list of dictionaries.
        csv_file_name (str): The name of the CSV file to write to.

    Returns:
        None

    Raises:
        ValueError: If the dict_list argument is not a list of dictionaries.
        ValueError: If the csv_file_name argument is not a string.
    """
    if not isinstance(dict_list, list) or not all(isinstance(item, dict) for item in dict_list):
        raise ValueError("The dict_list argument must be a list of dictionaries")

    if not isinstance(csv_file_name, str):
        raise ValueError("The csv_file_name argument must be a string")

    try:
        with open(csv_file_name, 'w', newline='') as csvfile:
            fieldnames = list(dict_list[0].keys())
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            for row in dict_list:
                writer.writerow(row)
            #logger.info(f'Saved {dict_list} to {csv_file_name}')
    except Exception as e:
        logger.error(f'Error while saving {dict_list} to {csv_file_name}: {e}')
        raise

def save_data_to_csv(data_df, file_name: str, output_dir: str):
    """
    Saves a Pandas DataFrame to a CSV file.

    Args:
        data_df (DataFrame): Pandas DataFrame to be saved.
        file_name (str): The name of the CSV file to be saved.
        output_dir (str): The path to the directory where the CSV file should be saved.

    Returns:
        None

    Raises:
        ValueError: If the data_df argument is not a Pandas DataFrame.
        ValueError: If the file_name argument is not a string.
        ValueError: If the output_dir argument is not a string.
        FileNotFoundError: If the specified output directory does not exist and could not be created.
    """
    if not isinstance(data_df, pd.DataFrame):
        raise ValueError("The data_df argument must be a Pandas DataFrame")

    if not isinstance(file_name, str):
        raise ValueError("The file_name argument must be a string")

    if not isinstance(output_dir, str):
        raise ValueError("The output_dir argument must be a string")

    try:
        current_time = get_current_datetime_utc().strftime("%Y%m%d_%H%M%S")
        file_path = os.path.join(output_dir, file_name)
        os.makedirs(output_dir, exist_ok=True)
        data_df.to_csv(file_path, index=False)
    except FileNotFoundError as e:
        logger.exception(f"An error occurred: {e}")
        raise



def read_csv(file_path: str) -> pd.DataFrame:
    """
    Reads a CSV file and returns a pandas DataFrame.

    Args:
        file_path (str): Path to the CSV file.

    Returns:
        pd.DataFrame: Pandas DataFrame containing the CSV data.

    Raises:
        ValueError: If the provided file path does not end with the '.csv' extension.
        pd.errors.EmptyDataError: If the CSV file is empty.
        pd.errors.ParserError: If there is an error parsing the CSV file.
    """
    if not file_path.endswith('.csv'):
        raise ValueError("The provided file path is not a CSV file.")

    try:
        data = pd.read_csv(file_path)
        if data.empty:
            raise pd.errors.EmptyDataError("The CSV file is empty")
        return data
    except pd.errors.EmptyDataError as e:
        raise
    except pd.errors.ParserError as e:
        raise
    except Exception as e:
        print(f"An error occurred: {e}")
        raise


def read_multiple_csv(data_dir: str, index_column: str = None) -> pd.DataFrame:
    """
    Reads multiple CSV files from a directory and returns a single DataFrame.

    Args:
        data_dir (str): The path of the directory containing the CSV files.
        index_column (str): The name of the column to use as the index. Defaults to 'Date'.

    Returns:
        pandas.DataFrame: A DataFrame containing the combined data from all CSV files.

    Raises:
        ValueError: If the data_dir argument is not a string.
        FileNotFoundError: If the data_dir argument does not exist or is not a directory.
        ValueError: If no CSV files are found in the data_dir directory.
    """
    if not isinstance(data_dir, str):
        raise ValueError("The data_dir argument must be a string")

    if not os.path.exists(data_dir) or not os.path.isdir(data_dir):
        raise FileNotFoundError(f"The specified data directory '{data_dir}' does not exist or is not a directory")

    file_paths = [os.path.join(data_dir, f) for f in os.listdir(data_dir) if f.endswith('.csv')]

    if not file_paths:
        raise ValueError(f"No CSV files found in the specified data directory '{data_dir}'")

    data_frames = []

    for file_path in file_paths:
        df = pd.read_csv(file_path)
        if index_column is not None:
            df[index_column] = pd.to_datetime(df[index_column])
            df.set_index(index_column, inplace=True)
        data_frames.append(df)

    combined_df = pd.concat(data_frames)
    combined_df.sort_index(inplace=True)
    return combined_df

# Remove non-ASCII Characters from a csv file
def to_ascii_csv_file(file_path: str) -> None:
    """
    Reads a CSV file and removes any non-ASCII characters from its content. The modified content is then 
    written back to the same file.

    Args:
        file_path (str): The path to the CSV file to be modified.

    Returns:
        None

    Raises:
        ValueError: If the file_path argument is not a string.
        IOError: If there is an error reading from or writing to the file.
    """
    if not isinstance(file_path, str):
        raise ValueError("The file_path argument must be a string")

    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()

        # Remove non-ASCII characters
        content_clean = re.sub(r'[^\x00-\x7F]+', '', content)

        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(content_clean)

    except Exception as e:
        error_msg = f"An error occurred while processing the file: {e}"
        logger.exception(error_msg)
        raise IOError(error_msg)

# Remove remnants of strings from dataframe and blanks along with Unkown and converts to None for clean data injection
def remove_unknowns_and_blank_remnants_to_none(data: dict) -> dict:
    """
    Replaces all NaN values, "Unknown" strings and blank strings in a dictionary with None.

    Args:
        data (dict): Dictionary to clean.

    Returns:
        dict: Cleaned dictionary.

    Raises:
        ValueError: If the data argument is not a dictionary.
    """
    if not isinstance(data, dict):
        raise ValueError("The data argument must be a dictionary")

    cleaned_data = {}
    for key, value in data.items():
        if pd.isna(value):
            cleaned_data[key] = None
        elif isinstance(value, str) and (value.lower() == "unknown" or value.strip() == ""):
            cleaned_data[key] = None
        else:
            cleaned_data[key] = value

    return cleaned_data

def remove_unknowns_and_blank_remnants_to_none_pd_series(data: pd.Series) -> dict:
    """
    Replaces all NaN values, "Unknown" strings and blank strings in a Pandas Series with None,
    and returns the result as a dictionary.

    Args:
        data (pd.Series): Pandas Series to clean.

    Returns:
        dict: Cleaned data as a dictionary.
    """
    if not isinstance(data, pd.Series):
        raise ValueError("The data argument must be a Pandas Series")

    cleaned_data = {}
    for key, value in data.items():
        if pd.isna(value):
            cleaned_data[key] = None
        elif isinstance(value, str) and (value.lower() == "unknown" or value.strip() == ""):
            cleaned_data[key] = None
        else:
            cleaned_data[key] = value

    return cleaned_data
    # Debug: Print the original data if the 'symbol' key in cleaned_data is None
    #if cleaned_data.get('Code') is None:
    #    print("Problematic row:", data)
    #
    #return cleaned_data

