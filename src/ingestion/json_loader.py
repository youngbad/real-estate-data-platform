import logging
import os

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    IntegerType,
    TimestampType,
)

# Configure basic logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class RawListingLoader:
    """
    Loads raw real estate listing JSON files into PySpark DataFrames
    with strict schema validation.
    """

    def __init__(self, spark: SparkSession):
        self.spark = spark

    @staticmethod
    def get_listing_schema() -> StructType:
        """
        Defines the updated expected schema for raw real estate listings,
        matching Otodom, OLX, and GUS scrapper output.
        """
        return StructType(
            [
                StructField("listing_id", StringType(), False),
                StructField("city", StringType(), True),
                StructField("district", StringType(), True),
                StructField("price", DoubleType(), True),
                StructField("price_per_m2", DoubleType(), True),
                StructField("area", DoubleType(), True),
                StructField("rooms", IntegerType(), True),
                StructField("floor", IntegerType(), True),
                StructField("building_year", IntegerType(), True),
                StructField("date_scraped", TimestampType(), True),
            ]
        )

    def load_json_directory(self, input_dir: str) -> DataFrame:
        if not os.path.exists(input_dir):
            logger.error(f"Input directory does not exist: {input_dir}")
            raise FileNotFoundError(f"Input directory not found: {input_dir}")

        logger.info(f"Attempting to load raw JSON listings from: {input_dir}")
        schema = self.get_listing_schema()

        try:
            df = (
                self.spark.read.option("mode", "FAILFAST")
                .schema(schema)
                .json(input_dir)
            )

            record_count = df.count()
            logger.info(f"Successfully loaded {record_count} records from {input_dir}")

            return df

        except Exception as e:
            logger.error(f"Failed to load JSON data from {input_dir}. Error: {str(e)}")
            raise
