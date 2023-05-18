import factory
from factory.faker import Faker
from sqlalchemy.orm import Session
from models.entity import Entity
from models.exchange_eodhistoricaldata import Exchange_EODHistoricalData
from models.historical_price_data import Historical_Price_Data

class Entity_Factory(factory.alchemy.SQLAlchemyModelFactory):
    class Meta:
        model = Entity
        sqlalchemy_session = Session

    id = factory.Sequence(lambda n: n)
    code = Faker('pystr_format', string_format='??????')  # Produces a random string of 6 characters
    type = Faker('random_element', elements=('Common Stock', 'Preferred Stock', 'Warrant'))
    name = Faker('company')
    exchange = Faker('random_element', elements=('NASDAQ', 'NYSE', 'TSX', 'LSE', 'HKSE'))
    exchange_id = factory.SubFactory(Exchange_EODHistoricalData)  # Assuming you've defined an ExchangeFactory
    currency_code = Faker('currency_code')
    currency_name = Faker('currency_name')
    currency_symbol = Faker('currency_symbol')
    country_name = Faker('country')
    country_iso = Faker('country_code')
    isin = Faker('pystr_format', string_format='??########')  # Produces a random string in the format of an ISIN
    lei = Faker('pystr_format', string_format='###############')  # Produces a random string in the format of an LEI
    primary_ticker = Faker('pystr_format', string_format='??????.??')  # Produces a random string in the format of a ticker
    cusip = Faker('pystr_format', string_format='#########')  # Produces a random string in the format of a CUSIP
    cik = Faker('random_number', digits=7)  # Produces a random 7-digit number for CIK
    employer_id_number = Faker('pystr_format', string_format='##-#######')  # Produces a random string in the format of an EIN
    fiscal_year_end = Faker('month_name')
    ipo_date = Faker('past_date')  # You can specify the start date and end date as well
    international_domestic = Faker('random_element', elements=('International', 'Domestic'))
    sector = Faker('job')  # This is a bit of a hack, but Faker doesn't have a built-in provider for sectors
    industry = Faker('job')  # Same as above
    gics_sector = Faker('job')  # Same as above
    gics_group = Faker('job')  # Same as above
    gics_industry = Faker('job')  # Same as above
    gics_sub_industry = Faker('job')  # Same as above
    home_category = Faker('random_element', elements=('Domestic', 'International'))
    is_delisted = Faker('boolean', chance_of_getting_true=50)
    description = Faker('paragraph')
    address = Faker('address')
    address_data = {}  # You can use the pydantic model to generate a dictionary for this field
    listings = {}  # You can use the pydantic model to generate a dictionary for this field
    officers = {}  # You can use the pydantic model to generate a dictionary for this field
    phone = Faker('phone_number')
    web_url = Faker('uri')
    logo_url = Faker('image_url')
    full_time_employees = Faker('random_int', min=50, max=200000)
    updated_by_eod_historical_at = Faker('date_time')
    source = 'Faker'
    last_updated = factory

class Exchange_EODHistoricalData_Factory(factory.alchemy.SQLAlchemyModelFactory):
    class Meta:
        model = Exchange_EODHistoricalData
        sqlalchemy_session = Session

    id = factory.Sequence(lambda n: n)
    Name = Faker('company')
    Code = Faker('pystr_format', string_format='??')  # Produces a random string of 6 characters
    OperatingMIC = Faker('pystr_format', string_format='??????')  # Produces a random string of 6 characters
    Country = Faker('country')
    Currency = Faker('pystr_format', string_format='???')  # Produces a random string of 6 characters
    CountryISO2 = Faker('pystr_format', string_format='??')  # Produces a random string of 6 characters
    CountryISO3 = Faker('pystr_format', string_format='???')  # Produces a random string of 6 characters
    Timezone = Faker('timezone')
    trading_hours = Faker('pydict', max_items=10)
    holidays = Faker('pydict', max_items=10)
    entities = factory.RelatedFactoryList(
        'Entity_Factory',  # Note the related factory here
        factory_related_name='exchange_id',  # Assuming this is the name of the relationship field on Entity
        size=5  # Number of related entities to create
    )

class Historical_Price_Data_Factory(factory.alchemy.SQLAlchemyModelFactory):
    class Meta:
        model = Historical_Price_Data

    entity_id = factory.SubFactory(Entity_Factory)
    timestamp = timestamp = Faker('past_datetime', start_date="-30d")  # Generates a datetime within the last 30 days
    open = factory.Float()
    high = factory.Float()
    low = factory.Float()
    close = factory.Float()
    adjusted_close = factory.Float()
    volume = factory.Float()
    is_regular_trading_hours = factory.Boolean()
    source = factory.Faker('company')
    last_updated = factory.Faker('date_time')
    updated_by = factory.Faker('name')
    entity = factory.SubFactory(Entity_Factory)
