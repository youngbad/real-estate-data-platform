from typing import Optional
from pydantic import BaseModel

class ListingSchema(BaseModel):
    listing_id: str
    city: str
    district: str
    price: Optional[float] = None
    price_per_m2: Optional[float] = None
    area: Optional[float] = None
    rooms: Optional[int] = None
    floor: Optional[int] = None
    building_year: Optional[int] = None
    url: Optional[str] = None
    date_scraped: str
