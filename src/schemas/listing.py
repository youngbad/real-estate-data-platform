from typing import Optional
from pydantic import BaseModel

class ListingSchema(BaseModel):
    listing_id: str
    city: str
    district: str
    price: Optional[float]
    price_per_m2: Optional[float]
    area: Optional[float]
    rooms: Optional[int]
    floor: Optional[int]
    building_year: Optional[int]
    url: Optional[str]
    date_scraped: str
