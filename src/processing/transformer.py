import logging
from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from pyspark.sql.types import DoubleType

logger = logging.getLogger(__name__)


class RealEstateTransformer:
    """
    Handles data cleaning, normalization, and business logic transformations
    for real estate listings PySpark DataFrames.
    """

    def clean_listings(self, df: DataFrame) -> DataFrame:
        """
        Applies cleaning rules to raw listings data.

        Args:
            df (DataFrame): The raw data loaded from JSON.

        Returns:
            DataFrame: A cleaned DataFrame without invalid records.
        """
        logger.info("Starting data cleaning process...")

        # 1. Drop rows where both price and area are missing (unusable records)
        df_cleaned = df.dropna(subset=["price", "area"], how="all")

        # 2. Filter out unrealistic outliers (e.g., negative prices or area, absurdly high rooms)
        df_cleaned = df_cleaned.filter(
            (F.col("price").isNull() | (F.col("price") > 0))
            & (F.col("area").isNull() | (F.col("area") > 5))  # Minimum 5 sqm
            & (F.col("rooms").isNull() | ((F.col("rooms") > 0) & (F.col("rooms") < 50)))
        )

        return df_cleaned

    def normalize_columns(self, df: DataFrame) -> DataFrame:
        """
        Standardizes string formats and calculates missing derived metrics.

        Args:
            df (DataFrame): Cleaned DataFrame.

        Returns:
            DataFrame: DataFrame with normalized columns.
        """
        logger.info("Applying column normalization and derivations...")

        # 1. Standardize city and district strings (Title Case, trim whitespace)
        df_norm = df.withColumn("city", F.initcap(F.trim(F.col("city")))).withColumn(
            "district", F.initcap(F.trim(F.col("district")))
        )

        # 2. Backfill missing price_per_m2 if price and area are available
        df_norm = df_norm.withColumn(
            "price_per_m2",
            F.when(
                F.col("price_per_m2").isNull()
                & F.col("price").isNotNull()
                & F.col("area").isNotNull(),
                (F.col("price") / F.col("area")).cast(DoubleType()),
            ).otherwise(F.col("price_per_m2")),
        )

        # 3. Handle data staleness or fill simple nulls
        # If building_year is missing, we might leave it as null, but ensure it's an integer
        # (covered by schema, but good practice to explicitly state business rules).

        return df_norm

    def transform(self, df: DataFrame) -> DataFrame:
        """
        Executes the full transformation pipeline.

        Args:
            df (DataFrame): Raw input DataFrame.

        Returns:
            DataFrame: Fully transformed DataFrame ready for insertion into staging.
        """
        logger.info(f"Initial record count: {df.count()}")

        df = self.clean_listings(df)
        df = self.normalize_columns(df)

        logger.info(f"Transformed record count: {df.count()}")
        return df
