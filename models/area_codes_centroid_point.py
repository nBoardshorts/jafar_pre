# models/area_codes_centroid_point.py

from sqlalchemy.orm import relationship
from sqlalchemy import Column, Integer, String, Float, ForeignKey
from geoalchemy2 import Geometry
from support.base import Base

class Area_Codes_Centroid_Point(Base):
    __tablename__ = 'area_codes_centroid_point'
    id = Column(Integer, primary_key=True)
    area_code = Column(String, nullable=False, unique=True)
    centroid_lat = Column(Float, nullable=False)
    centroid_lon = Column(Float, nullable=False)
    geom = Column(Geometry(geometry_type='POINT', srid=4326, nullable=False))
    city_data = relationship('Cities_By_Area_Code', back_populates='area_code_data')