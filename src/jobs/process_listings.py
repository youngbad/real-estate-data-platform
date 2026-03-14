import logging
import os
import sys

from pyspark.sql import SparkSession

# Adjust path so we can import our modules locally without fully packaging the app yet
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

import ingestion.json_loader
import processing.transformer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

def main():
    # 1. Initialize Spark Session
    logger.info("Initializing Spark Session...")
    spark = SparkSession.builder \
        .appName("RealEstatePlatform-Local-ETL") \
        .master("local[*]") \
        .getOrCreate()
        
    spark.sparkContext.setLogLevel("ERROR")

    # 2. Extract: Load the JSON files
    raw_data_dir = os.getenv("RAW_DATA_DIR", os.path.join(os.getcwd(), "data", "raw", "json"))
    loader = ingestion.json_loader.RawListingLoader(spark)
    
    try:
        logger.info("Loading raw data...")
        df_raw = loader.load_json_directory(raw_data_dir)
        
        logger.info("Raw Data Preview:")
        df_raw.show(truncate=False)
        
    except Exception as e:
        logger.error(f"ETL failed during extraction: {e}")
        spark.stop()
        return

    # 3. Transform: Clean and calculate fields
    transformer = processing.transformer.RealEstateTransformer()
    logger.info("Transforming data...")
    df_transformed = transformer.transform(df_raw)
    
    logger.info("Transformed Data Preview:")
    df_transformed.show(truncate=False)

    # 4. Load: Save cleaned data as Parquet directly to curated zone
    curated_data_dir = os.getenv("CURATED_DATA_DIR", os.path.join(os.getcwd(), "data", "curated", "parquet"))
    logger.info(f"Saving transformed data as Parquet to: {curated_data_dir}")
    try:
        df_transformed.write.mode("overwrite").parquet(curated_data_dir)
        logger.info("Successfully saved curated data to Parquet format.")
    except Exception as e:
        logger.error(f"Failed to save Parquet data: {e}")

    # Note: We'll implement Step 5 (Load to PostgreSQL) next!
    logger.info("Transformation complete. Spark job finished successfully.")
    
    spark.stop()


if __name__ == "__main__":
    main()
