import logging
import os

import pandas as pd

from database.connection import create_sql_server_engine

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def prepare_frame_for_sql_server(frame: pd.DataFrame) -> pd.DataFrame:
    prepared = frame.copy()

    if "is_weekend" in prepared.columns:
        prepared["is_weekend"] = prepared["is_weekend"].astype(int)

    return prepared.where(pd.notna(prepared), None)


class SQLServerDataLoader:
    """Loads curated Parquet data into the SQL Server star schema."""

    def __init__(self):
        self.engine = create_sql_server_engine()

    def load_data(self, input_dir: str):
        logger.info("Loading Parquet files from %s", input_dir)
        try:
            df = pd.read_parquet(input_dir)
            logger.info("Loaded %s records into memory.", len(df))
        except Exception as exc:
            logger.error("Error reading Parquet files: %s", exc)
            return

        df["date_scraped"] = pd.to_datetime(df["date_scraped"])
        df["source_name"] = df["listing_id"].apply(lambda value: value.split("_")[0])

        logger.info("Processing dim_date...")
        df["date_id"] = df["date_scraped"].dt.strftime("%Y%m%d").astype(int)

        dim_date = df[["date_scraped", "date_id"]].drop_duplicates().copy()
        dim_date["full_date"] = dim_date["date_scraped"].dt.date
        dim_date["year"] = dim_date["date_scraped"].dt.year
        dim_date["month"] = dim_date["date_scraped"].dt.month
        dim_date["month_name"] = dim_date["date_scraped"].dt.month_name()
        dim_date["day_of_month"] = dim_date["date_scraped"].dt.day
        dim_date["day_of_week"] = dim_date["date_scraped"].dt.dayofweek + 1
        dim_date["day_name"] = dim_date["date_scraped"].dt.day_name()
        dim_date["quarter"] = dim_date["date_scraped"].dt.quarter
        dim_date["is_weekend"] = dim_date["day_of_week"].isin([6, 7])
        dim_date = dim_date.drop(columns=["date_scraped"]).drop_duplicates(subset=["date_id"])
        dim_date = prepare_frame_for_sql_server(dim_date)

        dim_date.to_sql("dim_date", self.engine, if_exists="append", index=False, chunksize=200)

        logger.info("Processing dim_location...")
        dim_location = df[["city", "district"]].drop_duplicates().copy()
        dim_location = prepare_frame_for_sql_server(dim_location)
        try:
            dim_location.to_sql(
                "dim_location",
                self.engine,
                if_exists="append",
                index=False,
                chunksize=200,
            )
        except Exception as exc:
            logger.warning("dim_location might already exist: %s", exc)

        db_locations = pd.read_sql(
            "SELECT location_id, city, district FROM dim_location", self.engine
        )
        df = df.merge(db_locations, on=["city", "district"], how="left")

        logger.info("Processing dim_property...")
        df_listings = df[df["source_name"] != "gus"].copy()
        dim_property = df_listings[["rooms", "building_year", "area_bucket"]].drop_duplicates().copy()
        dim_property = prepare_frame_for_sql_server(dim_property)
        try:
            dim_property.to_sql(
                "dim_property",
                self.engine,
                if_exists="append",
                index=False,
                chunksize=200,
            )
        except Exception as exc:
            logger.warning("dim_property might already exist: %s", exc)

        db_properties = pd.read_sql(
            "SELECT property_id, rooms, building_year, area_bucket FROM dim_property",
            self.engine,
        )
        df_merge_prop = df.fillna({"rooms": -1, "building_year": -1})
        db_properties_merge = db_properties.fillna({"rooms": -1, "building_year": -1})
        df = df_merge_prop.merge(
            db_properties_merge,
            on=["rooms", "building_year", "area_bucket"],
            how="left",
        )

        logger.info("Processing dim_source...")
        dim_source = df[["source_name"]].drop_duplicates().copy()
        dim_source = prepare_frame_for_sql_server(dim_source)
        try:
            dim_source.to_sql(
                "dim_source",
                self.engine,
                if_exists="append",
                index=False,
                chunksize=200,
            )
        except Exception as exc:
            logger.warning("dim_source might already exist: %s", exc)

        db_sources = pd.read_sql(
            "SELECT source_id, source_name FROM dim_source", self.engine
        )
        df = df.merge(db_sources, on=["source_name"], how="left")

        logger.info("Processing fact_listings...")
        df_listings = df[df["source_name"] != "gus"].copy()
        fact_listings = df_listings[
            [
                "listing_id",
                "date_id",
                "location_id",
                "property_id",
                "source_id",
                "date_scraped",
                "price",
                "area",
                "price_per_m2",
                "url",
            ]
        ].copy()
        fact_listings = fact_listings.rename(columns={"listing_id": "listing_natural_key"})
        fact_listings = prepare_frame_for_sql_server(fact_listings)

        try:
            fact_listings.to_sql(
                "fact_listings",
                self.engine,
                if_exists="append",
                index=False,
                chunksize=200,
            )
            logger.info("Successfully loaded data into fact_listings.")
        except Exception as exc:
            logger.error("Failed to insert into fact_listings: %s", exc)

        logger.info("Processing fact_macro_indicators...")
        df_macro = df[df["source_name"] == "gus"].copy()
        if not df_macro.empty:
            fact_macro = df_macro[
                [
                    "listing_id",
                    "date_id",
                    "location_id",
                    "source_id",
                    "date_scraped",
                    "price_per_m2",
                ]
            ].copy()
            fact_macro = fact_macro.rename(
                columns={
                    "listing_id": "indicator_natural_key",
                    "date_scraped": "date_recorded",
                    "price_per_m2": "average_price_per_m2",
                }
            )
            fact_macro = prepare_frame_for_sql_server(fact_macro)
            try:
                fact_macro.to_sql(
                    "fact_macro_indicators",
                    self.engine,
                    if_exists="append",
                    index=False,
                    chunksize=200,
                )
                logger.info("Successfully loaded data into fact_macro_indicators.")
            except Exception as exc:
                logger.error("Failed to insert into fact_macro_indicators: %s", exc)


if __name__ == "__main__":
    input_dir = os.getenv(
        "INPUT_PARQUET_DIR", os.path.join(os.getcwd(), "data", "curated", "parquet")
    )
    loader = SQLServerDataLoader()
    loader.load_data(input_dir)