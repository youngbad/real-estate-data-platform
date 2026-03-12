import logging
import os
import pandas as pd
from sqlalchemy import create_engine
from typing import Optional

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

class PostgresDataLoader:
    """Loads curated Parquet data into the PostgreSQL Star Schema."""
    
    def __init__(self):
        user = os.getenv("DB_USER", "postgres")
        password = os.getenv("DB_PASSWORD", "postgres")
        host = os.getenv("DB_HOST", "localhost")
        port = os.getenv("DB_PORT", "5432")
        db_name = os.getenv("DB_NAME", "real_estate_db")
        
        conn_str = f"postgresql://{user}:{password}@{host}:{port}/{db_name}"
        self.engine = create_engine(conn_str)
        
    def load_data(self, input_dir: str):
        logger.info(f"Loading Parquet files from {input_dir}")
        try:
            # Reconstruct the DataFrame from parquet
            df = pd.read_parquet(input_dir)
            logger.info(f"Loaded {len(df)} records into memory.")
        except Exception as e:
            logger.error(f"Error reading Parquet files: {e}")
            return
            
        # Ensure date_scraped is datetime
        df['date_scraped'] = pd.to_datetime(df['date_scraped'])
        
        # 1. Load dim_date
        logger.info("Processing dim_date...")
        df['date_id'] = df['date_scraped'].dt.strftime('%Y%m%d').astype(int)
        
        dim_date = df[['date_scraped', 'date_id']].drop_duplicates().copy()
        dim_date['full_date'] = dim_date['date_scraped'].dt.date
        dim_date['year'] = dim_date['date_scraped'].dt.year
        dim_date['month'] = dim_date['date_scraped'].dt.month
        dim_date['month_name'] = dim_date['date_scraped'].dt.month_name()
        dim_date['day_of_month'] = dim_date['date_scraped'].dt.day
        dim_date['day_of_week'] = dim_date['date_scraped'].dt.dayofweek + 1
        dim_date['day_name'] = dim_date['date_scraped'].dt.day_name()
        dim_date['quarter'] = dim_date['date_scraped'].dt.quarter
        dim_date['is_weekend'] = dim_date['day_of_week'].isin([6, 7])
        
        dim_date = dim_date.drop(columns=['date_scraped']).drop_duplicates(subset=['date_id'])
        
        dim_date.to_sql('dim_date', self.engine, if_exists='append', index=False, method='multi')
        
        # 2. Load dim_location
        logger.info("Processing dim_location...")
        dim_location = df[['city', 'district']].drop_duplicates().copy()
        try:
            dim_location.to_sql('dim_location', self.engine, if_exists='append', index=False, method='multi')
        except Exception as e:
            logger.warning(f"dim_location might already exist: {e}")
            
        # Re-fetch location limits
        db_locations = pd.read_sql("SELECT location_id, city, district FROM dim_location", self.engine)
        df = df.merge(db_locations, on=['city', 'district'], how='left')
        
        # 3. Load dim_property
        logger.info("Processing dim_property...")
        dim_property = df[['rooms', 'building_year', 'area_bucket']].drop_duplicates().copy()
        try:
            dim_property.to_sql('dim_property', self.engine, if_exists='append', index=False, method='multi')
        except Exception as e:
            logger.warning(f"dim_property might already exist: {e}")
            
        db_properties = pd.read_sql("SELECT property_id, rooms, building_year, area_bucket FROM dim_property", self.engine)
        # Fix NaNs for safe merge
        df_merge_prop = df.fillna({'rooms': -1, 'building_year': -1})
        db_properties_merge = db_properties.fillna({'rooms': -1, 'building_year': -1})
        
        df = df_merge_prop.merge(db_properties_merge, on=['rooms', 'building_year', 'area_bucket'], how='left')
        
        # 4. Load dim_source
        logger.info("Processing dim_source...")
        df['source_name'] = df['listing_id'].apply(lambda x: x.split('_')[0])
        dim_source = df[['source_name']].drop_duplicates().copy()
        try:
            dim_source.to_sql('dim_source', self.engine, if_exists='append', index=False, method='multi')
        except Exception as e:
            logger.warning("dim_source might already exist")
            
        db_sources = pd.read_sql("SELECT source_id, source_name FROM dim_source", self.engine)
        df = df.merge(db_sources, on=['source_name'], how='left')
        
        # 5. Load fact_listings
        logger.info("Processing fact_listings...")
        fact_listings = df[[
            'listing_id', 'date_id', 'location_id', 'property_id', 'source_id',
            'date_scraped', 'price', 'area', 'price_per_m2'
        ]].copy()
        
        fact_listings = fact_listings.rename(columns={'listing_id': 'listing_natural_key'})
        
        try:
            fact_listings.to_sql('fact_listings', self.engine, if_exists='append', index=False, method='multi')
            logger.info("Successfully loaded data into PostgreSQL!")
        except Exception as e:
            logger.error(f"Failed to insert into fact_listings: {e}")

if __name__ == "__main__":
    INPUT_DIR = os.getenv("INPUT_PARQUET_DIR", os.path.join(os.getcwd(), "data", "curated", "parquet"))
    loader = PostgresDataLoader()
    loader.load_data(INPUT_DIR)
