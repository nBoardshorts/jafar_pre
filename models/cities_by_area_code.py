# models/cities_by_area_code.py

from sqlalchemy.orm import relationship
from sqlalchemy import Column, Integer, String, Float, ForeignKey
from geoalchemy2 import Geometry
from support.base import Base

class Cities_By_Area_Code(Base):
    __tablename__ = 'cities_by_area_code'
    id = Column(Integer, primary_key=True)
    city = Column(String, nullable=False)
    state = Column(String, nullable=False)
    country = Column(String, nullable=False)
    lat = Column(Float, nullable=False)
    lon = Column(Float, nullable=False)
    geom = Column(Geometry(geometry_type='POINT', srid=4326, nullable=False))
    area_code = Column(String, nullable=False)
    area_code_id = Column(Integer, ForeignKey('area_codes_centroid_point.id'))
    area_code_data = relationship('Area_Codes_Centroid_Point', back_populates='city_data')