import os
import sys
import pandas as pd
import pytest
from unittest.mock import patch, MagicMock

# Dodajemy folder src do ścieżki Pythona, aby móc importować nasze moduły w testach
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))

from jobs.load_to_postgres import PostgresDataLoader

@patch('jobs.load_to_postgres.create_engine')
def test_postgres_data_loader_init(mock_create_engine):
    """Testujemy poprawne zainicjalizowanie klasy ładującej do bazy PostgreSQL."""
    # Ustawiamy środowisko testowe
    os.environ['DB_USER'] = 'test_user'
    os.environ['DB_PASSWORD'] = 'test_pass'
    
    loader = PostgresDataLoader()
    
    # Sprawdzamy czy SQLAlchemy Engine został zainicjalizowany poprawnie
    mock_create_engine.assert_called_once_with('postgresql://test_user:test_pass@localhost:5432/real_estate_db')

@patch('jobs.load_to_postgres.pd.read_parquet')
@patch('jobs.load_to_postgres.create_engine')
def test_load_data_handles_missing_file(mock_create_engine, mock_read_parquet, caplog):
    """Testujemy czy system nie wysypuje ze błędem, gdy brakuje pliku Parquet."""
    mock_read_parquet.side_effect = Exception("File missing!")
    
    loader = PostgresDataLoader()
    loader.load_data("dummy/invalid/path")
    
    # Upewniamy się, że wystąpił odpowiedni log błędu bez zatrzymywania aplikacji (graceful fail)
    assert "Error reading Parquet files: File missing!" in caplog.text
