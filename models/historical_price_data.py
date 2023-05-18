from sqlalchemy import Column, String, Float, Integer, Boolean, DateTime, ForeignKey, BigInteger
from sqlalchemy.orm import relationship, declared_attr
from sqlalchemy.ext.declarative import DeclarativeMeta
from sqlalchemy.sql.ddl import DDL
from sqlalchemy import Table, ForeignKeyConstraint, CheckConstraint, inspect, PrimaryKeyConstraint
from helpers.time_helper import timestamp_utc_ms_to_datetime_utc
from helpers.logging_helper import configure_logging, logger
from typing import Tuple, List, Union, Optional, Dict
from datetime import datetime, timedelta
from support.base import Base


"""
This module implements a table partitioning system for storing historical price data in a PostgreSQL database. 



Note: This partitioning system is designed to work with SQLAlchemy and a PostgreSQL database. If you are using a different 
database system, you may need to modify the code to work with that system.

Also, please note that due to the structure of the codebase, `db` (the database session object) must be passed as a parameter
to some methods to avoid circular imports. A circular import occurs when two modules depend on each other, either directly 
or indirectly, which can lead to problems in Python's import system.

In partition_ranges, you specify the duration of each partition for a given frequency_type. Then, in create_partition, you use the
 start time and the frequency_type to calculate the end time of the partition. This effectively partitions the data both by timestamp 
 (i.e., the range of each partition) and frequency_type (i.e., the size of each partition).

"""

configure_logging()

class Historical_Price_Data_Mixin:
    """
    This mixin provides the common functionality for handling the historical price data. 
    This includes the creation, management, and interaction with partitioned tables.
    """
    __table_args__ = (
        PrimaryKeyConstraint('entity_id', 'timestamp'),
    )
    @declared_attr
    def entity_id(cls):
        return Column(Integer, ForeignKey('entities.id', onupdate="CASCADE", ondelete="CASCADE"), nullable=False)
    @declared_attr
    def timestamp(cls):
        return Column(BigInteger, nullable=False)
    @declared_attr
    def open(cls):
        return Column(Float, nullable=False)
    @declared_attr
    def high(cls):
        return Column(Float, nullable=False)
    @declared_attr
    def low(cls):
        return Column(Float, nullable=False)
    @declared_attr
    def close(cls):
        return Column(Float, nullable=True)
    @declared_attr
    def adjusted_close(cls):
        return Column(Float, nullable=True)
    @declared_attr
    def volume(cls):
        return Column(Float, nullable=False)
    @declared_attr
    def is_regular_trading_hours(cls):
        return Column(Boolean, nullable=False, default=True)
    @declared_attr
    def source(cls):
        return Column(String, nullable=False)
    @declared_attr
    def last_updated(cls):
        return Column(DateTime, default=datetime.utcnow, nullable=False)
    @declared_attr
    def updated_by(cls):
        return Column(String, nullable=False)
    @declared_attr
    def entity(cls):
        return relationship("Entity", back_populates="historical_price_data")

   


# Define the columns for the 'historical_price_data' table and the 
class Partition_By_Time_Range_Meta(DeclarativeMeta):
    """
    This metaclass is used to extend SQLAlchemy's DeclarativeMeta. It provides the 
    capability to create partitioned tables based on a specified time range.
    """

    def __new__(cls, clsname, bases, attrs, *, partition_by):
        """
        This special method is responsible for instantiating a new object. In the context of 
        the Partition_By_Time_Range_Meta metaclass, it is used to add attributes and methods 
        related to partitioning to the classes that use it as their metaclass.
        """
        attrs.update(
            {
                '__table_args__': attrs.get('__table_args__', ())
                + (dict(postgresql_partition_by=f'RANGE({partition_by})'),),
                'partitions': {},
                'partition_by': partition_by,
            }
        )

        return super().__new__(cls, clsname, bases, attrs)

    
class Historical_Price_Data(Historical_Price_Data_Mixin, Base, metaclass=Partition_By_Time_Range_Meta, partition_by='timestamp'): #The partition_by keyword in the metaclass creation is only telling the metaclass which column in the database should be used to determine the partition boundaries. In this case, it's the 'timestamp' column. However, the frequency_type is also taken into account when determining the size of each partition. This is handled by the partition_ranges dictionary and the create_partition method in the Historical_Price_Data class.
    """
    This class represents the historical_price_data table in the database. It inherits 
    from the Historical_Price_Data_Mixin and uses the Partition_By_Time_Range_Meta 
    metaclass to provide partitioning functionality based on the 'timestamp' column.
    """

    __tablename__ = 'historical_price_data'

    partition_ranges = {
        """The partition_ranges dictionary plays a crucial role in the creation and management of the partitions. It defines 
        the duration of each partition based on the frequency_type. So, it should be used in the create_partition method to 
        calculate the end date of the partition."""
        
        "1_min": timedelta(days=1),
        "5_min": timedelta(days=7),
        "15_min": timedelta(days=14),
        "1_hour": timedelta(days=30),
        "4_hour": timedelta(days=120),
        "1_day": timedelta(days=365),
        "1_week": timedelta(days=365*2),
        "1_month": timedelta(days=365*10),
    }


    @classmethod
    def create_partition_table_class(cls, name):
        """
        This method generates a new partition table class that inherits from the parent table. 
        The partition table is defined by a unique name and a constraint expression.

        Args:
            name (str): The name of the partition.

        Returns:
            Partition_Table: A class representing the partition.
        """

        class Partition_Table(Historical_Price_Data):
            __tablename__ = name
            __table_args__ = {
                'inherits': (Historical_Price_Data,),
                'postgresql_partition_by': f'RANGE({cls.partition_by})',
                'tablespace': 'historical_data', 
            }
        return Partition_Table
         

    @staticmethod
    def determine_partition_type(frequency: Union[str, int], frequency_type: Optional[str]) -> str:
        """
        This method determines the type of partition to be used based on the provided 
        frequency and frequency_type. The partition type is crucial for the partitioning 
        strategy.

        Args:
            frequency (Union[str, int]): The frequency of the data.
            frequency_type (str): The type of the frequency.

        Returns:
            str: The type of the partition.
        """

        frequency_mapping = {
            (1, 'min'): '1_min',
            (5, 'min'): '5_min',
            (15, 'min'): '15_min',
            (1, 'hour'): '1_hour',
            (4, 'hour'): '4_hour',
            (1, 'day'): '1_day',
            (1, 'week'): '1_week',
            (1, 'month'): '1_month',
        }
        partition_type = frequency_mapping.get((frequency, frequency_type))
        if partition_type is None:
            raise ValueError(f"Invalid frequency and frequency_type combination: {frequency}, {frequency_type}")
        return partition_type


    @classmethod # Must pass db because will have circular import error otherwise
    def bulk_write_to_partition(cls, instances: List['Historical_Price_Data'], db, start_datetime_utc: datetime, frequency_type: str) -> None:
        """
        This method writes a list of instances to a specific partition. The partition is 
        determined based on the start_datetime_utc and frequency_type parameters. If the 
        required partition doesn't exist, it is created.
        """

        if not instances:
            return

        PartitionTable = cls.create_partition((start_datetime_utc, frequency_type))

        with db.session_scope() as session:
            if not db.engine.dialect.has_table(db.engine, PartitionTable.__table__.name):
                PartitionTable.__table__.create(bind=db.engine)

            for instance in instances:
                if cls.validate_data(instance.__dict__):
                    session.add(PartitionTable(**instance.__dict__))
                else:
                    logger.error(f"Skipped adding instance due to missing data: {instance.__dict__}")


    @classmethod
    def create_partition(cls_, start_datetime_utc: datetime, frequency_type: str):
        """
        This method creates a new partition of the historical price data table, based on the 
        start_datetime_utc and frequency_type. The partition's duration is determined using 
        the partition_ranges dictionary.
        """

        partition_range = cls_.partition_ranges[frequency_type]
        partition_end = start_datetime_utc + partition_range
        partition_name = f'{cls_.__tablename__}_{start_datetime_utc.strftime("%Y%m%d%H%M%S")}_{frequency_type}'

        if partition_name not in cls_.partitions:
            constraint_expr = (
                f"{cls_.timestamp} >= '{start_datetime_utc.isoformat()}' AND {cls_.timestamp} < '{partition_end.isoformat()}'"
            )
            PartitionTable = cls_.create_partition_table_class(partition_name)
            cls_.partitions[partition_name] = PartitionTable

        return cls_.partitions[partition_name]


    @classmethod
    def from_td_ameritrade(cls, td_ameritrade_data, entity_id, source, last_updated, updated_by, is_regular_trading_hours):
        # Create a new Historical_Price_Data instance
        historical_price_data_instance = cls(
            entity_id=entity_id,
            timestamp=td_ameritrade_data.get('datetime'),
            open=td_ameritrade_data.get('open'),
            high=td_ameritrade_data.get('high'),
            low=td_ameritrade_data.get('low'),
            close=td_ameritrade_data.get('close'),
            adjusted_close=td_ameritrade_data.get('adjusted_close'),
            volume=td_ameritrade_data.get('volume'),
            is_regular_trading_hours=is_regular_trading_hours,
            source=source,
            last_updated=last_updated,
            updated_by=updated_by,
        )
        return historical_price_data_instance

    @classmethod
    def from_eod_historical(cls, eod_historical_data, entity_id, source, last_updated, updated_by, is_regular_trading_hours):
        # Create a new Historical_Price_Data instance
        historical_price_data_instance = cls(
            entity_id=entity_id,
            timestamp=datetime.fromtimestamp(eod_historical_data.get('timestamp')),
            open=eod_historical_data.get('open'),
            high=eod_historical_data.get('high'),
            low=eod_historical_data.get('low'),
            close=eod_historical_data.get('close'),
            adjusted_close=eod_historical_data.get('adjusted_close'),
            volume=eod_historical_data.get('volume'),
            is_regular_trading_hours=is_regular_trading_hours,
            source=source,
            last_updated=last_updated,
            updated_by=updated_by,
        )
        return historical_price_data_instance
    
    @staticmethod
    def validate_data(data):
        required_fields = ['entity_id', 'timestamp', 'open', 'high', 'low', 'volume', 'is_regular_trading_hours', 'source', 'last_updated', 'updated_by']
        for field in required_fields:
            if field not in data or data[field] is None:
                logger.error(f"Missing required field: {field}")
                return False
        return True


    @staticmethod
    def dates_to_timestamp_ms(data: List[Dict[str, Union[str, datetime]]]) -> List[Dict[str, Union[str, int]]]:
        """
        Converts 'date' in each record of data to a timestamp in milliseconds.

        Args:
            data (List[Dict[str, Union[str, datetime]]]): A list of records, each containing a 'date' key.

        Returns:
            List[Dict[str, Union[str, int]]]: The data list with 'date' replaced by 'timestamp' in milliseconds.
        """
        for record in data:
            record['timestamp'] = int(datetime.strptime(record['date'], '%Y-%m-%d').timestamp() * 1000)
        return data


