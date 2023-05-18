#models/entity.py
from support.base import Base
from sqlalchemy import Column, Integer, String, ForeignKey, JSON, DateTime
from sqlalchemy.orm import relationship
from datetime import datetime

# From the general section of the fundamentals from eodhistoricaldata.com api, this 
# section will not need to be updated as frequently as other sections may need to.
# This should be the first section we worry about because it will enable us to determine 
# the partition by sectors for the historic data when we download and save historic data


class Entity(Base):
    __tablename__ = 'entities'

    id = Column(Integer, primary_key=True)
    #symbol
    code = Column(String(50), unique=True) #General Section 'Code' is symbol (e.g. 'TSLA')
    type = Column(String(50)) #General Section 'Type' (e.g. 'Common Stock')
    name = Column(String(255)) # General Section 'Name' (e.g. 'Tesla Inc')
    exchange = Column(String, nullable=True) # General Section 'Exchange' (e.g. 'NASDAQ')
    exchange_id = Column(Integer, ForeignKey('exchanges_eodhistoricaldata.id'))
    currency_code = Column(String, nullable=True) # General Section 'CurrencyCode' (e.g. 'USD')
    currency_name = Column(String, nullable=True) # General Section 'CurrencyName' (e.g. 'US Dollar')
    currency_symbol = Column(String, nullable=True) # General Section 'CurrencySymbol' (e.g. '$')
    country_name = Column(String, nullable=True) # General Section 'CountryName' (e.g. 'USA')
    country_iso = Column(String, nullable=True) # General Section 'CoutryISO' (e.g. 'US')
    isin = Column(String, nullable=True) # General Section 'ISIN' (e.g. US88160R1014')
    lei = Column(String, nullable=True) # General Section 'LEI' (e.g. '54930043XZGB27CT0V49')
    primary_ticker = Column(String, nullable=True) # General Section 'PrimaryTicker' (e.g. 'TSLA.US')
    cusip = Column(String, nullable=True) # General Section 'LEI' (e.g. '88160R101')
    # might be an int
    cik = Column(String, nullable=True) # General Section 'CIK' (e.g. '1318605')
    employer_id_number = (Column(String, nullable=True)) # General Section 'EmployerIdNumber' (e.g. "91-2197729")
    fiscal_year_end = (Column(String, nullable=True)) # General Section 'FiscalYearEnd' (e.g. 'December')
    ipo_date = (Column(String, nullable=True)) # General Section 'IPODate' (e.g. '2010-06-29')
    international_domestic = Column(String, nullable=True) # General Section 'InternationalDomestic' (e.g. 'International/Domestic')
    sector = Column(String, nullable=True) # General Section 'Sector' (e.g. 'Consumer Cyclical')
    industry = Column(String, nullable=True) # General Section 'Industry' (e.g. 'Auto Manufacturers')
    gics_sector = Column(String, nullable=True) # General Section 'GicSector' (e.g. 'Consumer Discretionary')
    gics_group = Column(String, nullable=True) # General Section 'GicGroup' (e.g. 'Automobiles & Components')
    gics_industry = Column(String, nullable=True) # General Section 'GicIndustry' (e.g. 'Automobiles')
    gics_sub_industry = Column(String, nullable=True) # General Section 'GicSubIndustry' (e.g. 'Automobile Manufacturers')
    home_category = Column(String, nullable=True) # General Section 'HomeCategory' (e.g. 'Domestic')
    is_delisted = Column(String, nullable=True) # General Section 'IsDelisted' (e.g. false) yes it was not in quotes and it was lowercase
    description = Column(String, nullable=True) # General Section 'Description' (e.g. ' Tesla, Inc. designs, develops, manufactures, leases, and sells electric vehicls,  and energy generation and storage systems...) May need to trim down in size, but would prefer not to.
    address = Column(String, nullable=True) # General Section 'Address' (e.g. 13101 Tesla Road, Austin, TX, United States, 78725)
    # dictionary
    address_data = Column(JSON, nullable=True) # General Section 'AddressData' (e.g. {'Street': 13101 Tesla Road', 'City': 'Austin', 'State': 'TX', 'Country': 'United States', 'ZIP': '78725'})
    # dictionary of dictionaries
    listings = Column(JSON, nullable=True) # General Section 'Listings' (e.g. {'0': {'Code': TSLA34', 'Exchange': 'SA', 'Name': 'Tesla Inc'}}  )
    # dictionary of dictionaries
    officers = Column(JSON, nullable=True) # General Section 'Officers' (e.g. {"0": {"Name": "Mr. Elon R. Musk", "Title": "Technoking of Tesla, CEO & Director", "YearBorn": "1972"}, "1": {"Name": "Mr. Zachary John Planell Kirkhorn", "Title": "Master of Coin & CFO", "YearBorn": "1985"}, "2": {"Name": "Mr. Andrew D. Baglino", "Title": "Sr. VP of Powertrain & Energy Engineering", "YearBorn": "1981"}, "3": {"Name": "Mr. Vaibhav  Taneja", "Title": "Corp. Controller & Chief Accounting Officer", "YearBorn": "1978"}, "4": {"Name": "Mr. Martin  Viecha", "Title": "Sr. Director for Investor Relations", "YearBorn": "NA"}, "5": {"Name": "Mr. Alan  Prescott", "Title": "VP of Legal", "YearBorn": "1979"}, "6": {"Name": "Mr. Dave  Arnold", "Title": "Sr. Director of Global Communications", "YearBorn": "NA"}, "7": {"Name": "Brian  Scelfo", "Title": "Sr. Director of Corp. Devel.", "YearBorn": "NA"}, "8": {"Name": "Mr. Jeffrey B. Straubel", "Title": "Sr. Advisor", "YearBorn": "1976"}, "9": {"Name": "Mr. Franz  von Holzhausen", "Title": "Chief Designer", "YearBorn": "NA"}})
    phone = Column(String, nullable=True) # General Section 'Phone' (e.g. '(512) 516-8177')
    web_url = Column(String, nullable=True) # General Section 'WebURL' (e.g. "https://www.tesla.com")
    logo_url = Column(String, nullable=True) # General Section 'LogoURL' (e.g. "/img/logos/US/TSLA.png")
    full_time_employees = Column(Integer, nullable=True) # General Section 'FullTimeEmployees' (e.g. 127855)
    updated_by_eod_historical_at = Column(String, nullable=True) # General Section 'UpdatedAt' (e.g. '2023-04-21')
    source = Column(String, nullable=False)
    last_updated = Column(DateTime, nullable=False)
    updated_by = Column(String, nullable=True)
    # Relationships
    exchange_data = relationship("Exchange_EODHistoricalData", back_populates="entities")
    historical_price_data = relationship("Historical_Price_Data", back_populates="entity")
    symbols_eodhistoricaldata = relationship("Symbol_EODHistoricalData", back_populates="entity")
    symbols_td_ameritrade = relationship("Symbol_TD_Ameritrade", back_populates="entity")