import os

from jobs.load_to_sqlserver import SQLServerDataLoader

PostgresDataLoader = SQLServerDataLoader


if __name__ == "__main__":
    input_dir = os.getenv(
        "INPUT_PARQUET_DIR", os.path.join(os.getcwd(), "data", "curated", "parquet")
    )
    loader = SQLServerDataLoader()
    loader.load_data(input_dir)
