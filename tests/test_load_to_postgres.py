import os
import sys
from unittest.mock import patch

# Add src folder to Python path to import modules in tests
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))

from jobs.load_to_sqlserver import SQLServerDataLoader

@patch('jobs.load_to_sqlserver.create_sql_server_engine')
def test_sql_server_data_loader_init(mock_create_sql_server_engine):
    """Test proper initialization of the SQL Server data loader class."""
    # Set up test environment variables
    os.environ['DB_USER'] = 'test_user'
    os.environ['DB_PASSWORD'] = 'test_pass'
    os.environ['DB_HOST'] = 'localhost'
    os.environ['DB_PORT'] = '1433'
    os.environ['DB_NAME'] = 'test_db'
    
    # loader is assigned, and we assert that the engine factory was called during init
    _loader = SQLServerDataLoader()
    
    mock_create_sql_server_engine.assert_called_once_with()


@patch('jobs.load_to_sqlserver.pd.read_parquet')
@patch('jobs.load_to_sqlserver.create_sql_server_engine')
def test_load_data_handles_missing_file(_mock_create_sql_server_engine, mock_read_parquet, caplog):
    """Test whether the system gracefully fails when the Parquet file is missing."""
    mock_read_parquet.side_effect = Exception("File missing!")
    
    loader = SQLServerDataLoader()
    loader.load_data("dummy/invalid/path")
    
    # Ensure error log occurred without crashing the app (graceful fail)
    assert "Error reading Parquet files: File missing!" in caplog.text