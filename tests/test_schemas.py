import os
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))

from schemas.listing import ListingSchema

def test_listing_schema_valid_data():
    """Test that valid listing data is correctly parsed into a ListingSchema object."""
    valid_data = {
        "listing_id": "olx_123456",
        "city": "Warsaw",
        "district": "Mokotow",
        "price": 850000.0,
        "price_per_m2": 17000.0,
        "area": 50.0,
        "rooms": 2,
        "floor": 3,
        "building_year": 2018,
        "url": "https://olx.pl/oferta/123456",
        "date_scraped": "2026-03-15"
    }
    
    listing = ListingSchema(**valid_data)
    
    assert listing.listing_id == "olx_123456"
    assert listing.city == "Warsaw"
    assert listing.price == 850000.0
    assert listing.area == 50.0

def test_listing_schema_missing_optional_fields():
    """Test that listing schema works even if optional fields are missing."""
    partial_data = {
        "listing_id": "gus_789",
        "city": "Krakow",
        "district": "Stare Miasto",
        "date_scraped": "2026-03-15"
    }
    
    listing = ListingSchema(**partial_data)
    
    assert listing.listing_id == "gus_789"
    assert listing.city == "Krakow"
    assert listing.price is None
    assert listing.rooms is None
