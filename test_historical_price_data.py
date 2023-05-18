import pytest
from datetime import datetime, timedelta
from models.historical_price_data import Historical_Price_Data
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import IntegrityError
from support.db import DB
from helpers.time_helper import datetime_utc_to_timestamp_utc_ms
from helpers.logging_helper import configure_logging, logger
from support.base import Base
from tests.factories import Historical_Price_Data_Factory, Entity_Factory

from gitignore.config import TEST_DATABASE_URL as url


class Test_Historical_Price_Data:
    
    @pytest.fixture(scope='module', autouse=True)
    def setup_class(self):
        configure_logging()
        self.db = DB(url=url)
        Base.metadata.drop_all()
        yield
        Base.metadata.drop_all(bind=self.db.engine)
        
    @pytest.fixture(scope='function')
    def db_session(self):
        session = self.db.Session()
        yield session
        session.rollback()
        session.close()

    # Test the create_partition method
    def test_create_partition(self, db_session):
        start_datetime_utc = datetime.utcnow()
        frequency_type = "1_min"
        partition = Historical_Price_Data.create_partition(start_datetime_utc, frequency_type)
        assert partition in Historical_Price_Data.partitions.values()

    # Test the bulk_write_to_partition method
    def test_bulk_write_to_partition(self, db_session):
        # Prepare data
        entity = Entity_Factory.create()
        db_session.add(entity)
        db_session.commit()

        start_datetime_utc = datetime.utcnow()
        frequency_type = "1_min"
        instances = [
            Historical_Price_Data_Factory.build(
                entity_id=entity.id,
                timestamp=datetime_utc_to_timestamp_utc_ms(start_datetime_utc + timedelta(minutes=i))
            ) for i in range(10)
        ]

        # Write data to partition
        Historical_Price_Data.bulk_write_to_partition(instances, self.db, start_datetime_utc, frequency_type)

        # Query data from partition
        partition = Historical_Price_Data.create_partition(start_datetime_utc, frequency_type)
        data = db_session.query(partition).all()

        # Assert that data was correctly written to partition
        assert len(data) == 10

        for i, record in enumerate(data):
            assert record.timestamp == datetime_utc_to_timestamp_utc_ms(start_datetime_utc + timedelta(minutes=i))
