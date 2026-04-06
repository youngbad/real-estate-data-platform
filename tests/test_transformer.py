from pyspark.sql import Row
from src.processing.transformer import RealEstateTransformer

def test_clean_listings(spark):
    transformer = RealEstateTransformer()
    
    # Create mock data
    data = [
        # Valid row
        Row(listing_id="olx_1", price=500000.0, area=50.0, rooms=2),
        # Null price & area (should be dropped because it's not GUS)
        Row(listing_id="otodom_1", price=None, area=None, rooms=2),
        # GUS record with nulls (should be kept)
        Row(listing_id="gus_1", price=None, area=None, rooms=None),
        # Negative price (should be dropped)
        Row(listing_id="olx_2", price=-10000.0, area=40.0, rooms=1),
        # Unrealistic area (should be dropped)
        Row(listing_id="olx_3", price=200000.0, area=2.0, rooms=1),
        # Unrealistic rooms (should be dropped)
        Row(listing_id="olx_4", price=1000000.0, area=100.0, rooms=100)
    ]
    
    df = spark.createDataFrame(data)
    cleaned_df = transformer.clean_listings(df)
    
    results = [row.listing_id for row in cleaned_df.collect()]
    
    assert "olx_1" in results
    assert "gus_1" in results
    assert "otodom_1" not in results
    assert "olx_2" not in results
    assert "olx_3" not in results
    assert "olx_4" not in results
    assert len(results) == 2

def test_normalize_columns(spark):
    transformer = RealEstateTransformer()
    
    data = [
        # Should normalize city/district, calculate price_per_m2, and set area_bucket
        Row(listing_id="id_1", city="WARSAW  ", district="  mokotow", price=500000.0, area=50.0, price_per_m2=None, building_year=2010),
        # Should drop duplicate
        Row(listing_id="id_1", city="WARSAW  ", district="  mokotow", price=500000.0, area=50.0, price_per_m2=None, building_year=2010),
        # test area bucket correctly
        Row(listing_id="id_2", city="Krakow", district="Centrum", price=800000.0, area=65.0, price_per_m2=None, building_year=None)
    ]
    
    schema = "listing_id STRING, city STRING, district STRING, price DOUBLE, area DOUBLE, price_per_m2 DOUBLE, building_year INT"
    df = spark.createDataFrame(data, schema=schema)
    norm_df = transformer.normalize_columns(df)
    
    results = {row.listing_id: row for row in norm_df.collect()}
    
    # Duplicate removed
    assert len(results) == 2
    
    # Normalization checks
    assert results["id_1"].city == "Warsaw"
    assert results["id_1"].district == "Mokotow"
    assert results["id_1"].price_per_m2 == 10000.0
    assert results["id_1"].area_bucket == "40-60"
    
    assert results["id_2"].area_bucket == "60-80"
    assert results["id_2"].price_per_m2 == 12307.69  # 800000 / 65
