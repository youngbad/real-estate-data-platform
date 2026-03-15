from sqlalchemy import (
    Column, Integer, String, Float, Boolean, Date, DateTime, 
    Numeric, ForeignKey, UniqueConstraint
)
from sqlalchemy.orm import declarative_base, relationship

Base = declarative_base()

class DimDate(Base):
    __tablename__ = 'dim_date'
    
    date_id = Column(Integer, primary_key=True)
    full_date = Column(Date, nullable=False)
    year = Column(Integer, nullable=False)
    month = Column(Integer, nullable=False)
    month_name = Column(String(20), nullable=False)
    day_of_month = Column(Integer, nullable=False)
    day_of_week = Column(Integer, nullable=False)
    day_name = Column(String(20), nullable=False)
    quarter = Column(Integer, nullable=False)
    is_weekend = Column(Boolean, nullable=False)

class DimLocation(Base):
    __tablename__ = 'dim_location'
    __table_args__ = (UniqueConstraint('city', 'district'),)
    
    location_id = Column(Integer, primary_key=True, autoincrement=True)
    city = Column(String(100), nullable=False)
    district = Column(String(100), nullable=False)

class DimProperty(Base):
    __tablename__ = 'dim_property'
    __table_args__ = (UniqueConstraint('rooms', 'building_year', 'area_bucket'),)
    
    property_id = Column(Integer, primary_key=True, autoincrement=True)
    rooms = Column(Integer, nullable=True)
    building_year = Column(Integer, nullable=True)
    area_bucket = Column(String(20), nullable=False)

class DimSource(Base):
    __tablename__ = 'dim_source'
    
    source_id = Column(Integer, primary_key=True, autoincrement=True)
    source_name = Column(String(50), nullable=False, unique=True)

class FactListing(Base):
    __tablename__ = 'fact_listings'
    
    listing_sk = Column(Integer, primary_key=True, autoincrement=True)
    listing_natural_key = Column(String(100), nullable=False, unique=True)
    
    date_id = Column(Integer, ForeignKey('dim_date.date_id'), nullable=False, index=True)
    location_id = Column(Integer, ForeignKey('dim_location.location_id'), nullable=False, index=True)
    property_id = Column(Integer, ForeignKey('dim_property.property_id'), nullable=False, index=True)
    source_id = Column(Integer, ForeignKey('dim_source.source_id'), nullable=False)
    
    date_scraped = Column(DateTime, nullable=False)
    
    price = Column(Numeric(15, 2), nullable=False)
    area = Column(Numeric(10, 2), nullable=False)
    price_per_m2 = Column(Numeric(15, 2), nullable=False)
    
    url = Column(String(500), nullable=True)
    
    # Relationships
    date = relationship("DimDate")
    location = relationship("DimLocation")
    property = relationship("DimProperty")
    source = relationship("DimSource")
