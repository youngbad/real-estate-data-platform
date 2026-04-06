import os
import json
from unittest.mock import patch
from src.ingestion.scraper import BaseScraper, OtodomScraper

class DummyScraper(BaseScraper):
    def parse_data(self, content):
        return [{"mock": "data"}]

def test_base_scraper_fetch_page():
    scraper = DummyScraper(base_url="http://test.com", output_dir="/tmp/test_dir")
    
    with patch("requests.Session.get") as mock_get:
        mock_get.return_value.status_code = 200
        mock_get.return_value.text = "<html>Test</html>"
        
        result = scraper.fetch_page("http://test.com")
        
        assert result == "<html>Test</html>"
        mock_get.assert_called_once_with("http://test.com", timeout=15)

def test_base_scraper_save_data(tmp_path):
    scraper = DummyScraper(base_url="http://test.com", output_dir=str(tmp_path))
    data = [{"listing_id": "test_1", "price": 100}]
    
    saved_path = scraper.save_data(data, "test_source")
    
    assert os.path.exists(saved_path)
    with open(saved_path, "r") as f:
        saved_data = json.loads(f.readline())
        assert saved_data["listing_id"] == "test_1"

def test_otodom_scraper_parse():
    scraper = OtodomScraper(base_url="http://test.com", output_dir="/tmp")
    data = scraper.parse_data("<html>")
    # Generates mock data inside parse_data
    assert len(data) > 0
    assert data[0]["listing_id"].startswith("otodom_")
