import pytest
from pyspark.sql import SparkSession

@pytest.fixture(scope="session")
def spark():
    """Provides a session-scoped Spark session for testing."""
    spark_session = (
        SparkSession.builder
        .master("local[1]")
        .appName("real-estate-test")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield spark_session
    spark_session.stop()
