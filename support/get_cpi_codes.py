import pandas as pd
import os
from helpers.logging_helper import configure_logging, log_exception, logger
from helpers.data_helper import relative_path_to_file_path
from support.db import DB

# Configure logging
configure_logging()
# Get database spun up and ready for use, pass engine to functions
db = DB()

"""
FROM BLS

Section 4
=================================================================================
File Structure and Format: The following represents the file format used to define 
cu.series. Note that the Field Numbers are for reference only; they do not exist 
in the database.  Data files are in ASCII text format.  Data elements are separated 
by spaces; the first record of each file contains the column headers for the data
elements stored in each field.  Each record ends with a new line character. 

Field #/Data Element		Length		Value(Example)		

1.  series_id		  	17		CUSR0000SA0

2.  area_code		   	4		0400

3.  item_code		   	8		SA0E

4.  seasonal		   	1		S or U		
						
5.  periodicity_code	   	1		R	
						
6.  base_code		   	1		S

7.  base_period		   	20		1982-84=100		
				
8.  begin_year		   	4		1947		

9.  begin_period   		3		M01		

10. end_year			4		2002

11. end_period			3		M02
				

The series_id (CUSR0000SA0) can be broken out into:

Code					Value

survey abbreviation	=		CU
seasonal(code)		=		S
periodicity_code	=		R
area_code		=		0000
item_code		=		SA0
==================================================================================
Section 5
==================================================================================
File Structure and Format: The following represents the file format used to define
each data file. Note that the field numbers are for reference only; they do not 
exist in the database.  Data files are in ASCII text format.  Data elements are 
separated by spaces; the first record of each file contains the column headers for 
the data elements stored in each field. Each record ends with a new line character.   

The cu.data file is partitioned into a number of separate files:  See Section 2

All of the above-referenced data files have the following format:

Field #/Data Element	Length		Value(Example)		

1. series_id		  17		CUUR0400AA0

2. year			   4		1966	

3. period		   3		M12		

4. value		  12      	53.3	
				 
5. footnote_codes	  10		It varies
				

The series_id (CUUR0400AA0) can be broken out into:

Code					Value

survey abbreviation	=		CU
seasonal(code)		=		U
periodicity_code	=		R
area_code		=		0400
item_code		=		AA0
"""

# Write csv files to tables in database  
def cpi_code_csvs_to_db_tables():
    """
    Write a set of CSV files to corresponding tables in a database.

    The function reads in CSV files from the specified relative file path and writes the data to corresponding tables in the
    database specified by the provided engine. The function loops through a dictionary of file paths and table names,
    writing each file to its corresponding table. If an error is encountered while opening or writing to a CSV file, the
    function will skip that file and move on to the next one.

    Args:
        None

    Returns:
        None
    """
    # Define CSV files and corresponding table names
    files = {
        'data/bls/cpi/info/item_codes/cu.item.csv': 'bls_cpi_series_id_item_codes',
        'data/bls/cpi/info/area_codes/cu.area.csv': 'bls_cpi_series_id_area_codes',
        'data/bls/cpi/info/base_codes/cu.base.csv': 'bls_cpi_series_id_base_codes',
        'data/bls/cpi/info/period_codes/cu.period.csv': 'bls_cpi_series_id_period_codes',
        'data/bls/cpi/info/periodicity/cu.periodicity.csv': 'bls_cpi_series_id_periodicity',
        'data/bls/cpi/info/seasonal_codes/cu.seasonal.csv': 'bls_cpi_series_id_seasonal_codes'
    }
    # Loop over files and write data to tables
    for relative_file_path, table_name in files.items():
        logger.info(f'Writing {relative_file_path} to table {table_name}')
        success = db.csv_to_db_table(relative_file_path, table_name)
        if not success:
            logger.warning(f'Skipping table {table_name} due to error encountered with CSV file.')
        logger.info(f'Finished writing {relative_file_path} to table {table_name}')

# Retrieve item codes from txt file retrieved from bls website at https://download.bls.gov/pub/time.series/
def get_cpi_codes_from_txt_files():
    """
    Retrieves CPI codes data from txt files obtained from the BLS website and stores them as CSV files.

    The function reads text files containing CPI codes data from the BLS website and stores them as CSV files in the appropriate directory. The files to be processed are specified by their relative paths, which are defined in the function. The CSV files are named using the same file name as the corresponding text file, with the extension changed to '.csv'. The resulting CSV files can be used to populate tables in a database using the `csv_to_db_table()` function.

    Returns:
        None

    Raises:
        None

    """

    item_codes_rel_path = 'data/bls/cpi/info/item_codes/cu.item.txt'
    area_codes_rel_path = 'data/bls/cpi/info/area_codes/cu.area.txt'
    base_codes_rel_path = 'data/bls/cpi/info/base_codes/cu.base.txt'
    period_codes_rel_path = 'data/bls/cpi/info/period_codes/cu.period.txt'
    periodicity_rel_path = 'data/bls/cpi/info/periodicity/cu.periodicity.txt'
    season_codes_rel_path = 'data/bls/cpi/info/seasonal_codes/cu.seasonal.txt'

    files = [item_codes_rel_path, area_codes_rel_path, base_codes_rel_path, period_codes_rel_path, periodicity_rel_path, season_codes_rel_path]

    for file in files:
        # Build file_path
        file_path = relative_path_to_file_path(file)
        if not os.path.exists(file_path):
            logger.warning(f"File {file_path} not found, skipping.")
            continue
        logger.info(f"Processing {file_path}...")
        print(f"Processing {file_path}...")
        # Read the text file into a pandas DataFrame
        try:
            with open(file_path, "r") as file:
                content = file.readlines()
                # Clean up lines by removing newline characters
                content = [line.strip() for line in content]

            # Extract column names from the first line
            column_names = content[0].split("\t")

            # Create a DataFrame with the remaining lines
            data = [line.split("\t") for line in content[1:]]
            df = pd.DataFrame(data, columns=column_names)

            # Save the DataFrame as a CSV file
            csv_path = os.path.splitext(file_path)[0] + '.csv'
            logger.info(f"Saving {file_path} to csv file in {csv_path},")
            df.to_csv(csv_path, index=False)
            logger.info(f"Successfully saved [csv_path]")
        except Exception as e:
            logger.error(f"Error processing {file_path}: {str(e)}")
            log_exception(logger)


def main():
    # Run get_cpi_codes_from_txt_files() first, this is commented out because it was completed while script was being written
    get_cpi_codes_from_txt_files()
    # The files received were scrubbed with openrefine.exe to ensure utf8 compliant, remove leading/trailing spaces and blank rows before running the next function cpi_code_csvs_to_db_tables()
    cpi_code_csvs_to_db_tables()

if __name__ == '__main__':
    main()