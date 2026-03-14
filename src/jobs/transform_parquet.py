import logging
import os

from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F

# Configure basic logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class ParquetListingsTransformer:
    """
    Reads a real estate Parquet dataset, applies business rules and cleaning,
    and writes the curated output back to Parquet.
    """

    def __init__(self, spark: SparkSession):
        self.spark = spark

    def transform(self, df: DataFrame) -> DataFrame:
        """
        Applies the required transformation rules.

        Tasks:
        - remove duplicates by listing_id
        - filter invalid rows (price <= 0, area <= 10)
        - compute price_per_m2
        - create area_bucket (0-40, 40-60, 60-80, 80+)
        """
        logger.info(f"Input record count: {df.count()}")

        # 1. Remove duplicates by listing_id
        df_transformed = df.dropDuplicates(["listing_id"])

        # 2. Filter invalid rows (keep rows where price > 0 AND area > 10)
        df_transformed = df_transformed.filter(
            (F.col("price") > 0) & (F.col("area") > 10)
        )

        # 3. Compute price_per_m2
        df_transformed = df_transformed.withColumn(
            "price_per_m2", F.round(F.col("price") / F.col("area"), 2)
        )

        # 4. Create area_bucket (0-40, 40-60, 60-80, 80+)
        df_transformed = df_transformed.withColumn(
            "area_bucket",
            F.when(F.col("area") <= 40, "0-40")
            .when((F.col("area") > 40) & (F.col("area") <= 60), "40-60")
            .when((F.col("area") > 60) & (F.col("area") <= 80), "60-80")
            .otherwise("80+"),
        )

        logger.info(
            f"Output record count after transformations: {df_transformed.count()}"
        )
        return df_transformed

    def run(self, input_path: str, output_path: str) -> None:
        """
        Executes the PySpark job: Read -> Transform -> Write.
        """
        logger.info(f"Reading Parquet dataset from: {input_path}")
        try:
            df = self.spark.read.parquet(input_path)

            # Show schema and preview before transform
            logger.info("Input Schema:")
            df.printSchema()

            df_clean = self.transform(df)

            # Show preview
            logger.info("Transformed Data Preview:")
            df_clean.show(5, truncate=False)

            logger.info(f"Writing cleaned Parquet dataset to: {output_path}")
            df_clean.write.mode("overwrite").parquet(output_path)

            logger.info("Transformation job completed successfully.")

        except Exception as e:
            logger.error(f"Failed to process Parquet data: {str(e)}")
            raise


if __name__ == "__main__":
    # Initialize Spark
    spark_session = (
        SparkSession.builder.appName("CurateRealEstateParquet")
        .config("spark.sql.parquet.vint.backwardCompatibility", "true")
        .getOrCreate()
    )

    spark_session.sparkContext.setLogLevel("ERROR")

    # Define directories (defaults to the output of our previous process_listings job)
    INPUT_DIR = os.getenv(
        "INPUT_PARQUET_DIR", os.path.join(os.getcwd(), "data", "processed", "parquet")
    )
    OUTPUT_DIR = os.getenv(
        "OUTPUT_PARQUET_DIR", os.path.join(os.getcwd(), "data", "curated", "parquet")
    )

    # Run the job
    job = ParquetListingsTransformer(spark_session)
    job.run(input_path=INPUT_DIR, output_path=OUTPUT_DIR)

    spark_session.stop()
