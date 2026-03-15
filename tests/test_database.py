import os
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))

from database.models import DimDate, DimLocation, FactListing

def test_models_have_table_names():
    """Test that all SQLAlchemy models have the correct __tablename__ defined."""
    assert DimDate.__tablename__ == 'dim_date'
    assert DimLocation.__tablename__ == 'dim_location'
    assert FactListing.__tablename__ == 'fact_listings'

def test_dim_date_columns():
    """Ensure dim_date has the essential primary_key column and expected fields."""
    columns = [c.name for c in DimDate.__table__.columns]
    
    assert 'date_id' in columns
    assert 'year' in columns
    assert 'month' in columns
    assert DimDate.__table__.columns['date_id'].primary_key is True

def test_fact_listings_relationships():
    """Ensure fact_listings relates properly via foreign keys."""
    fks = [fk.column.name for fk in FactListing.__table__.foreign_keys]
    
    assert 'date_id' in fks
    assert 'location_id' in fks
    assert 'property_id' in fks
    assert 'source_id' in fks
