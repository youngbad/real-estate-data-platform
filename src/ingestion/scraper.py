import json
import logging
import os
import random
import uuid
from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

import requests

# Configure basic logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class BaseScraper(ABC):
    """Abstract base scraper for real estate data providers."""

    def __init__(self, base_url: str, output_dir: str):
        self.base_url = base_url
        self.output_dir = output_dir
        self.session = requests.Session()

        self.session.headers.update(
            {
                "User-Agent": os.getenv(
                    "SCRAPER_USER_AGENT",
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
                )
            }
        )

        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)
            logger.info(f"Created output directory: {self.output_dir}")

    def fetch_page(self, url: str) -> Optional[str]:
        try:
            logger.info(f"Fetching: {url}")
            response = self.session.get(url, timeout=15)
            response.raise_for_status()
            return response.text
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching {url}: {e}")
            return None

    @abstractmethod
    def parse_data(self, content: str) -> List[Dict[str, Any]]:
        """
        Parses raw HTML/JSON into the standardized schema:
        listing_id, city, district, price, price_per_m2, area, rooms, floor, building_year, date_scraped
        """
        pass

    def save_data(self, data: List[Dict[str, Any]], source_name: str) -> str:
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        filename = f"{source_name}_{timestamp}.json"
        filepath = os.path.join(self.output_dir, filename)

        try:
            with open(filepath, "w", encoding="utf-8") as f:
                for record in data:
                    f.write(json.dumps(record) + "\n")
            logger.info(f"Successfully saved {len(data)} records to {filepath}")
            return filepath
        except IOError as e:
            logger.error(f"Failed to save data to {filepath}: {e}")
            raise

    def run(self, source_name: str) -> None:
        logger.info(f"Starting scraper for {source_name}")
        content = self.fetch_page(self.base_url)

        if content:
            data = self.parse_data(content)
            if data:
                self.save_data(data, source_name)
            else:
                logger.warning(f"No data parsed for {source_name}.")
        else:
            logger.error(f"Failed to retrieve content for {source_name}.")


def generate_mock_data(source_name: str, count: int) -> List[Dict[str, Any]]:
    """Helper function to generate a large volume of mock data for 2025."""
    cities = ["Warsaw", "Krakow", "Gdansk", "Wroclaw", "Poznan", "Lodz"]
    districts = [
        "Centrum",
        "Mokotow",
        "Praga",
        "Wola",
        "Ochota",
        "Wrzeszcz",
        "Oliwa",
        "Stare Miasto",
        "Krzyki",
        "Srodmiescie",
    ]

    records = []
    for _ in range(count):
        area = round(random.uniform(25.0, 150.0), 1)
        price = round(random.uniform(300000.0, 3000000.0), 2)
        price_per_m2 = round(price / area, 2)

        # Spread dates throughout 2025
        base_date = datetime(2025, 1, 1)
        random_date = base_date + timedelta(
            days=random.randint(0, 364),
            hours=random.randint(0, 23),
            minutes=random.randint(0, 59),
        )

        is_gus = source_name == "gus"
        records.append(
            {
                "listing_id": f"{source_name}_{uuid.uuid4().hex[:8]}",
                "city": random.choice(cities),
                "district": random.choice(districts),
                "price": None if is_gus else price,
                "price_per_m2": price_per_m2,
                "area": None if is_gus else area,
                "rooms": None if is_gus else random.randint(1, 5),
                "floor": None if is_gus else random.randint(0, 12),
                "building_year": None if is_gus else random.randint(1950, 2025),
                "date_scraped": random_date.isoformat() + "Z",
            }
        )
    return records


class OtodomScraper(BaseScraper):
    def parse_data(self, content: str) -> List[Dict[str, Any]]:
        logger.info(
            "Extracting Otodom listings... (Generating 500 mock records for 2025)"
        )
        return generate_mock_data("otodom", count=500)


class OlxScraper(BaseScraper):
    def parse_data(self, content: str) -> List[Dict[str, Any]]:
        logger.info("Extracting OLX listings... (Generating 400 mock records for 2025)")
        return generate_mock_data("olx", count=400)


class GusDataFetcher(BaseScraper):
    def fetch_page(self, url: str) -> Optional[str]:
        logger.info(f"Fetching dataset from GUS API: {url}")
        return '{"mock_api_response": true}'

    def parse_data(self, content: str) -> List[Dict[str, Any]]:
        logger.info(
            "Extracting GUS public data... (Generating 50 mock records for 2025)"
        )
        return generate_mock_data("gus", count=50)


if __name__ == "__main__":
    # Use a persistent local data directory folder instead of /tmp
    OUTPUT_DIRECTORY = os.getenv(
        "RAW_DATA_DIR", os.path.join(os.getcwd(), "data", "raw", "json")
    )

    otodom = OtodomScraper(
        base_url="https://www.otodom.pl/pl/oferty/sprzedaz/mieszkanie",
        output_dir=OUTPUT_DIRECTORY,
    )
    otodom.run("otodom")

    olx = OlxScraper(
        base_url="https://www.olx.pl/nieruchomosci/mieszkania/sprzedaz/",
        output_dir=OUTPUT_DIRECTORY,
    )
    olx.run("olx")

    gus = GusDataFetcher(
        base_url="https://api.stat.gov.pl/mock_endpoint", output_dir=OUTPUT_DIRECTORY
    )
    gus.run("gus")
