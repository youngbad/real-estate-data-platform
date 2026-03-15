import os
import sys
import pandas as pd
import streamlit as st
from unittest.mock import patch, MagicMock

sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))

from dashboard.app import get_engine, require_data

@patch('dashboard.app.create_engine')
def test_dashboard_get_engine(mock_create_engine):
    """Test if Streamlit dashboard initializes the database connection properly."""
    # st.cache_resource requires resetting or clearing in testing
    st.cache_resource.clear()
    engine = get_engine()
    
    mock_create_engine.assert_called_once()
    assert engine == mock_create_engine.return_value

@patch('dashboard.app.pd.read_sql')
@patch('dashboard.app.get_engine')
def test_require_data_success(mock_get_engine, mock_read_sql):
    """Test requiring data from the database returns a DataFrame."""
    # Setup mock
    mock_df = pd.DataFrame({"total_listings": [100]})
    mock_read_sql.return_value = mock_df
    
    result = require_data("SELECT COUNT(*) FROM fact_listings")
    
    assert not result.empty
    assert result.iloc[0]["total_listings"] == 100
    mock_read_sql.assert_called_once()

@patch('dashboard.app.pd.read_sql')
@patch('dashboard.app.get_engine')
@patch('dashboard.app.st.error')
@patch('dashboard.app.st.stop')
def test_require_data_failure(mock_st_stop, mock_st_error, mock_get_engine, mock_read_sql):
    """Test that failed queries via require_data properly throw Streamlit errors."""
    # Setup mock to raise OperationalError
    from sqlalchemy.exc import OperationalError
    mock_read_sql.side_effect = OperationalError("statement", "params", "orig")
    
    result = require_data("SELECT * FROM invalid_table")
    
    # st.stop() will cause require_data to return whatever it does (None if it doesn't return anything else before st.stop raises an exception)
    assert result is None or (isinstance(result, pd.DataFrame) and result.empty)
    mock_st_error.assert_called_once()
    mock_st_stop.assert_called_once()
