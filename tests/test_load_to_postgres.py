import os
import sys
from unittest.mock import patch

# Add src folder to Python path to import modules in tests
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))

from jobs.load_to_postgres import PostgresDataLoader

@patch('jobs.load_to_postgres.create_engine')
def test_postgres_data_loader_init(mock_create_engine):
    """Test proper initialization of the PostgreSQL data loader class."""
    # Set up test environment variables
    os.environ['DB_USER'] = 'test_user'
    os.environ['DB_PASSWORD'] = 'test_pass'
    os.environ['DB_HOST'] = 'localhost'
    os.environ['DB_PORT'] = '5432'
    os.environ['DB_NAME'] = 'test_db'
    
    # loader is assigned, and we assert that create_engine was called during its __init__
    _loader = PostgresDataLoader()
    
    # Check if SQLAlchemy Engine was initialized correctly with the environment variables
    mock_create_engine.assert_called_once_with('postgresql://test_user:test_pass@localhost:5432/test_db')


@patch('jobs.load_to_postgres.pd.read_parquet')
@patch('jobs.load_to_postgres.create_engine')
def test_load_data_handles_missing_file(mock_create_engine, mock_read_parquet, caplog):
    """Test whether the system gracefully fails when the Parquet file is missing."""
    mock_read_parquet.side_effect = Exception("File missing!")
    
    loader = PostgresDataLoader()
    loader.load_data("dummy/invalid/path")
    
    # Ensure error log occurred without crashing the app (graceful fail)
    assert "Error reading Parquet files: File missing!" in caplog.text