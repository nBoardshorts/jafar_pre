# support/db.py

import subprocess
import os

import pandas as pd

from datetime import datetime, timedelta
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.orm import sessionmaker
from contextlib import contextmanager

from gitignore.config import POSTGRES_HOST, POSTGRES_PORT, POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_DB, DATABASE_URL, BACKUP_DIR
from helpers.time_helper import get_current_datetime_utc
from helpers.logging_helper import configure_logging, logger
from helpers.backup_helper import create_backup
from helpers.data_helper import relative_path_to_file_path
from support.base import Base
from models import *


# Purpose
# The purpose of the DB class in support/db.py is to provide a set of methods for working with a PostgreSQL database using SQLAlchemy. These methods include initializing a database connection, creating tables if they do not exist, backing up tables, setting up PostGIS extensions, and writing CSV data to a SQL table.
#
# Workflow
# The workflow for using the DB class is as follows:
#
# Instantiate the DB class.
# Use the session_scope method to perform database operations within a transactional scope.
# Use the other methods of the DB class as needed to work with the database.
# Criteria List
# The criteria list for the DB class in support/db.py includes the following:

# The class should provide a way to initialize a connection to a PostgreSQL database using SQLAlchemy.
# The class should provide a context manager (session_scope) for performing database operations within a transactional scope.
# The class should provide a method for creating specified tables in the database if they do not exist.
# The class should provide a method for backing up a table in the database to a timestamped .sql file in the specified backup directory.
# The class should provide a method for setting up the PostGIS database and enabling necessary extensions.
# The class should provide a method for writing the content of a CSV file to a SQL table.
# Instantiate the Database class
#db = Database()

# And then you can use it like this:
#with db.session_scope() as session:
    # Perform database operations

#BACKUP_FOLDER1 = f"C:\\Program Files\\PostgreSQL\\15\\data\\backups\\{database}\\"
BACKUP_FOLDER2 = f'D:\\Database Backups\\'


class DB:
    """
    A class for working with a PostgreSQL database using SQLAlchemy.
    Methods:
        initialize_database: Initialize a database connection.
        session_scope: Provide a transactional scope around a series of operations.
        get_engine: Create and return an SQLAlchemy engine for the PostgreSQL database.
        create_tables_if_not_exists: Create specified tables in the PostgreSQL database if they do not exist.
        backup_table: Backup a table in the database to a timestamped .sql file in the specified backup directory.
        setup_postgis_db: Set up the PostGIS database and enable necessary extensions.
        csv_to_db_table: Write the content of a CSV file to a SQL table.
    """
    # Creating a singleton class
    _instance= None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance.initialize_database()
        return cls._instance
    # Only instantiate the class if an instance has not been initialized
    def __init__(self, url = None):
        self.url = url

    def initialize_database(self):
        """
        This method is responsible for setting up the database connection and creating the 
        SQLAlchemy engine, which is later used for interacting with the PostgreSQL database. 
        It also sets up the PostGIS database and creates tables if they don't exist.
        """
        configure_logging()
        self.Base = Base
        self.engine = self.get_engine()
        self.SessionMaker = sessionmaker(bind=self.engine)
        self.setup_postgis_db()
        self.create_tables_if_not_exists()
        return self.engine

    @contextmanager
    def session_scope(self):
        """
        This is a context manager that provides a transactional scope around a series of operations. 
        It ensures that any changes made within the context are either committed if all operations
        are successful, or rolled back if any operation fails.

        Usage:
        with db.session_scope() as session:
            # Perform database operations
        """
        session = self.SessionMaker()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def get_engine(self):
        """
        Creates and returns an SQLAlchemy engine for the PostgreSQL database.

        :return: SQLAlchemy engine
        """
        if self.url:
            return create_engine(self.url)
        else:
            return create_engine(f'postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}', client_encoding='utf8')


    def create_tables_if_not_exists(self):
        """
        Creates the specified tables in the PostgreSQL database if they do not exist.

        :param base: SQLAlchemy declarative base containing the table definitions
        """
        
        inspector = inspect(self.engine)
        table_names = inspector.get_table_names()

        for table in self.Base.metadata.sorted_tables:
            if table.name not in table_names:
                self.Base.metadata.create_all(self.engine, tables=[table])

    def backup_database(self, database, backup_file_path):
        backup_command = f'pg_dump -U postgres -d {database} -f \"{backup_file_path}\" --no-password'

        try:
            result = subprocess.run(backup_command, shell=True, check=True, capture_output=True, text=True)
        except subprocess.CalledProcessError as e:
            logger.exception(f"Database backup failed with error: {e}\nOutput: {e.output}\nError Output: {e.stderr}")



    # PG Pass file is at c:\\users\mitch\   path has to be stored in environment variables
    def double_backup_database(self, database='trade_house'):
        """
        This method creates two backups of the database and stores them in different locations.
        If there are more than one backup files, it removes the oldest one.
        """
        # Define the backup folder and file name prefix
        BACKUP_FOLDER1 = f"C:\\Program Files\\PostgreSQL\\15\\data\\backups\\{database}\\"
        BACKUP_FOLDER2 = f'D:\\Database Backups\\{database}\\'
        backup_prefix = f'full_database_backup_{database}_'

        os.makedirs(os.path.dirname(BACKUP_FOLDER1), exist_ok=True)
        os.makedirs(os.path.dirname(BACKUP_FOLDER2), exist_ok=True)
        # Get a list of all files with the specific backup format in the primary backup folder
        backup_files1 = [f for f in os.listdir(BACKUP_FOLDER1) if f.startswith(backup_prefix)]

        # Get a list of all files with the specific backup format in the secondary backup folder
        backup_files2 = [f for f in os.listdir(BACKUP_FOLDER2) if f.startswith(backup_prefix)]

        # Combine the two lists of backup files
        backup_files = backup_files1 + backup_files2

        # If there are more than one backup files, sort by date and delete the oldest one from each folder
        if len(backup_files) > 1:
            backup_files.sort(reverse=True)
            latest_backup = backup_files[0]
            backup_datetime_str = latest_backup.replace(backup_prefix, "").replace(".sql", "")
            backup_datetime = datetime.strptime(backup_datetime_str, "%Y%m%d_%H%M%S")

            # Check if the latest backup is older than 20 hours
            time_since_last_backup = get_current_datetime_utc() - backup_datetime
            if time_since_last_backup > timedelta(hours=20):
                oldest_backup = backup_files[-1]
                if oldest_backup in backup_files1:
                    os.remove(os.path.join(BACKUP_FOLDER1, oldest_backup))
                elif oldest_backup in backup_files2:
                    os.remove(os.path.join(BACKUP_FOLDER2, oldest_backup))
            else:
                logger.info("Latest backup is less than 20 hours old. Assuming backup happened more frequently for a purpose, skipping deletion of older file.")

        backup1_date = get_current_datetime_utc()
        timestamp = backup1_date.strftime("%Y-%m-%d_%H-%M-%S_%f")
        backup1_path = os.path.join(BACKUP_FOLDER1, f"{backup_prefix}{timestamp}.sql")
        try:
            self.backup_database(database, backup1_path)
            logger.info("Primary database backup completed successfully.")
        except Exception as e:
            logger.exception(f"Primary database backup failed with error: {str(e)}")

        backup2_date = get_current_datetime_utc()
        timestamp = backup2_date.strftime("%Y-%m-%d_%H-%M-%S_%f")
        backup2_path = os.path.join(BACKUP_FOLDER2, f"{backup_prefix}{timestamp}.sql")
        try:
            self.backup_database(database, backup2_path)
            logger.info("Secondary database backup completed successfully.")
        except Exception as e:
            logger.exception(f"Secondary database backup failed with error: {str(e)}")
    
        self.update_backup_info(name=f'{database}_whole_database_backup', backup1_date=backup1_date, backup1_path=backup1_path,
                                backup2_date=backup2_date, backup2_path=backup2_path)


    def update_backup_info(self, name, backup1_date, backup1_path, backup2_date, backup2_path):
        """
        This method updates the backup information in the Update_Tracking table. If a row doesn't exist for a table, it creates a new one.
        """
        with self.session_scope() as session:
            # Check if a row exists in the update_tracking table
            row_exists = session.query(Update_Tracking.table_name == name).count() > 0

            # If a row exists, update the backup information
            if row_exists:
                session.query(Update_Tracking).filter(Update_Tracking.table_name == name).update({
                    'backup1_date': backup1_date,
                    'backup1_path': backup1_path,
                    'backup2_date': backup2_date,
                    'backup2_path': backup2_path
                })

            # If no row exists, insert a new row with the backup information
            else:
                new_tracking = Update_Tracking(table_name=name, backup1_date=backup1_date, backup1_path=backup1_path,
                                            backup2_date=backup2_date, backup2_path=backup2_path)
                session.add(new_tracking)


    def backup_table(self, table_name):
        """
        Backup a table in the database to a timestamped .sql file in the specified backup directory.

        Args:
            table_name (str): The name of the table to backup.

        Returns:
            str: The file path to the created backup file.

        Raises:
            CalledProcessError: If the pg_dump command fails.
        """
        # handle nuances
        if table_name == 'trade_house_whole_database_backup':
            return
        
        actual_table_name = table_name.split('.')[0]  # Split the table name and use only the first part
        os.makedirs(BACKUP_DIR, exist_ok=True)

        temp_backup_file = os.path.join(BACKUP_DIR, f"{actual_table_name}_temp.sql").replace("\\", "/")
        command = f'pg_dump -t {actual_table_name} --file="{temp_backup_file}" "{DATABASE_URL}"'
        subprocess.run(command, shell=True, check=True)

        backup_path = create_backup(temp_backup_file, BACKUP_DIR, f"{actual_table_name}")
        logger.info(f'Successfully backed up {actual_table_name}')
        os.remove(temp_backup_file)

        with self.session_scope() as session:
            update_tracking = session.query(Update_Tracking).filter_by(table_name=table_name).first()

            if update_tracking:
                current_backup1_path = getattr(update_tracking, 'backup1_path', '')
                current_backup2_path = getattr(update_tracking, 'backup2_path', '')

                current_backup1_date = getattr(update_tracking, 'backup1_date', None)
                current_backup2_date = getattr(update_tracking, 'backup2_date', None)

                older_backup_path = (
                    current_backup2_path
                    if current_backup1_path and current_backup2_date and current_backup1_date > current_backup2_date
                    else current_backup1_path 
                )
                if update_tracking.backup1_date is None:
                    update_tracking.backup1_date = get_current_datetime_utc()
                    update_tracking.backup1_path = backup_path
                elif update_tracking.backup2_date is None:
                    update_tracking.backup2_date = get_current_datetime_utc()
                    update_tracking.backup2_path = backup_path
                else:
                    if update_tracking.backup1_date < update_tracking.backup2_date:
                        update_tracking.backup1_date = get_current_datetime_utc()
                        update_tracking.backup1_path = backup_path
                    else:
                        update_tracking.backup2_date = get_current_datetime_utc()
                        update_tracking.backup2_path = backup_path

                if older_backup_path and os.path.exists(older_backup_path):
                    os.remove(older_backup_path)

                session.add(update_tracking)

            else:
                logger.warning(f'No update tracking entry found for {table_name}')

        return backup_path

    def setup_postgis_db(self):
        """
        Set up the PostGIS database and enable the necessary extensions.
        """
        with self.session_scope() as session:
            session.execute(text("CREATE EXTENSION IF NOT EXISTS postgis;"))
            session.execute(text("CREATE EXTENSION IF NOT EXISTS postgis_topology;"))
            session.execute(text("CREATE EXTENSION IF NOT EXISTS fuzzystrmatch;"))
            session.execute(text("CREATE EXTENSION IF NOT EXISTS postgis_tiger_geocoder;"))

    def csv_to_db_table(self, relative_file_path, table_name):
        """
        Writes the content of a CSV file to a SQL table.

        Args:
            relative_file_path (str): The relative path of the CSV file to be written to the database.
            table_name (str): The name of the SQL table to which the CSV data will be written.
            engine (sqlalchemy.engine.base.Engine): The SQLAlchemy engine object that will be used to connect to the database.

        Returns:
            bool: True if the CSV data was successfully written to the SQL table, False otherwise.

        """
        try:
            file_path = relative_path_to_file_path(relative_file_path)
        except Exception as e:
            logger.error(f'Error constructing filepath out of relative path (in csv_to_db_table function).')
            return False
        try:
            df = pd.read_csv(file_path)
            if df.empty:
                logger.warning(f'Empty dataframe loaded from {file_path}, skipping.')
                return False
        except Exception as e:
            logger.error(f'Error encountered while trying to open csv in helpers/dbhelper.py at csv_to_db_table: {e}')
            return False
        try:
            df.to_sql(table_name, self.engine, if_exists='replace')
            logger.info(f'Successfully wrote {file_path} to table {table_name}')
            return True
        except Exception as e:
            logger.error(f'Error encountered while trying to write datafile to sql table: {e}')
            return False


def main():
    db = DB()
    db.double_backup_database()


if __name__ == '__main__':
    main()