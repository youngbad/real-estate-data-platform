import os

import pandas as pd
from sqlalchemy import create_engine

from src.database.connection import create_sql_server_engine
from src.database.models import Base

SOURCE_TABLES = [
    "dim_date",
    "dim_location",
    "dim_property",
    "dim_source",
    "fact_listings",
    "fact_macro_indicators",
]

IDENTITY_TABLES = {
    "dim_location",
    "dim_property",
    "dim_source",
    "fact_listings",
    "fact_macro_indicators",
}

BOOLEAN_COLUMNS = {
    "dim_date": ["is_weekend"],
}


def prepare_frame_for_sql_server(table_name: str, frame: pd.DataFrame) -> pd.DataFrame:
    prepared = frame.copy()

    for column_name in BOOLEAN_COLUMNS.get(table_name, []):
        if column_name in prepared.columns:
            prepared[column_name] = prepared[column_name].astype(int)

    return prepared.where(pd.notna(prepared), None)


def build_postgres_source_url(prefix: str = "SOURCE_DB") -> str:
    user = os.getenv(f"{prefix}_USER", "postgres")
    password = os.getenv(f"{prefix}_PASSWORD", "postgres")
    host = os.getenv(f"{prefix}_HOST", "localhost")
    port = os.getenv(f"{prefix}_PORT", "5432")
    database = os.getenv(f"{prefix}_NAME", "real_estate_db")
    return f"postgresql://{user}:{password}@{host}:{port}/{database}"


def clear_target_tables(target_engine):
    with target_engine.begin() as conn:
        for table_name in reversed(SOURCE_TABLES):
            conn.exec_driver_sql(f"DELETE FROM {table_name}")


def migrate_data():
    source_engine = create_engine(build_postgres_source_url(), pool_pre_ping=True)
    target_engine = create_sql_server_engine(prefix="TARGET_DB")

    Base.metadata.drop_all(target_engine)
    Base.metadata.create_all(target_engine)

    for table_name in SOURCE_TABLES:
        frame = pd.read_sql(f"SELECT * FROM {table_name}", source_engine)
        if frame.empty:
            print(f"Skipping {table_name}: no rows found.")
            continue

        frame = prepare_frame_for_sql_server(table_name, frame)

        with target_engine.begin() as conn:
            if table_name in IDENTITY_TABLES:
                conn.exec_driver_sql(f"SET IDENTITY_INSERT {table_name} ON")

            frame.to_sql(
                table_name,
                conn,
                if_exists="append",
                index=False,
                chunksize=200,
            )

            if table_name in IDENTITY_TABLES:
                conn.exec_driver_sql(f"SET IDENTITY_INSERT {table_name} OFF")

        print(f"Migrated {len(frame)} rows into {table_name}.")


if __name__ == "__main__":
    migrate_data()