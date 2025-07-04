"""Tests for Databricks service."""

import os
import tempfile
from unittest.mock import MagicMock, Mock, patch

import pandas as pd
import pytest

from src.services.databricks_service import (DatabricksService,
                                             DatabricksServiceError)


@pytest.fixture
def mock_connection():
    """Mock Databricks SQL connection."""
    connection = MagicMock()
    cursor = MagicMock()

    # Configure cursor behavior
    cursor.description = [("id",), ("name",), ("value",)]
    cursor.fetchall.return_value = [(1, "test", 100), (2, "example", 200)]

    connection.cursor.return_value.__enter__.return_value = cursor
    return connection


@patch("src.services.databricks_service.sql.connect")
def test_databricks_service_initialization(mock_connect, test_config):
    """Test Databricks service initialization."""
    service = DatabricksService(test_config)
    assert service.config == test_config
    assert os.path.exists(test_config.temp_dir)


@patch("src.services.databricks_service.sql.connect")
def test_execute_query_success(mock_connect, test_config, mock_connection):
    """Test successful query execution."""
    mock_connect.return_value.__enter__.return_value = mock_connection

    service = DatabricksService(test_config)
    result = service.execute_query("SELECT * FROM test_table")

    assert result["success"] is True
    assert result["row_count"] == 2
    assert result["columns"] == ["id", "name", "value"]
    assert len(result["data"]) == 2
    assert result["data"][0]["id"] == 1


@patch("src.services.databricks_service.sql.connect")
def test_execute_query_connection_failure(mock_connect, test_config):
    """Test query execution with connection failure."""
    mock_connect.side_effect = Exception("Connection failed")

    service = DatabricksService(test_config)

    with pytest.raises(DatabricksServiceError) as exc_info:
        service.execute_query("SELECT * FROM test_table")

    assert "Failed to connect to Databricks" in str(exc_info.value)


@patch("src.services.databricks_service.sql.connect")
def test_execute_query_empty_result(mock_connect, test_config):
    """Test query execution with empty result."""
    connection = MagicMock()
    cursor = MagicMock()
    cursor.description = None
    cursor.fetchall.return_value = []

    connection.cursor.return_value.__enter__.return_value = cursor
    mock_connect.return_value.__enter__.return_value = connection

    service = DatabricksService(test_config)
    result = service.execute_query("SELECT * FROM empty_table")

    assert result["success"] is True
    assert result["row_count"] == 0
    assert result["columns"] == []
    assert result["data"] == []


@patch("src.services.databricks_service.sql.connect")
def test_get_table_data_success(mock_connect, test_config, mock_connection):
    """Test successful table data retrieval."""
    mock_connect.return_value.__enter__.return_value = mock_connection

    service = DatabricksService(test_config)
    csv_path = service.get_table_data("test_table")

    assert csv_path.endswith("test_table.csv")
    assert os.path.exists(csv_path)

    # Verify CSV content
    df = pd.read_csv(csv_path)
    assert len(df) == 2
    assert list(df.columns) == ["id", "name", "value"]


@patch("src.services.databricks_service.sql.connect")
def test_get_table_data_no_data(mock_connect, test_config):
    """Test table data retrieval with no data."""
    connection = MagicMock()
    cursor = MagicMock()
    cursor.description = None
    cursor.fetchall.return_value = []

    connection.cursor.return_value.__enter__.return_value = cursor
    mock_connect.return_value.__enter__.return_value = connection

    service = DatabricksService(test_config)

    with pytest.raises(DatabricksServiceError) as exc_info:
        service.get_table_data("empty_table")

    assert "No data found in table" in str(exc_info.value)


@patch("src.services.databricks_service.sql.connect")
def test_get_table_info_success(mock_connect, test_config):
    """Test successful table info retrieval."""
    connection = MagicMock()
    cursor = MagicMock()

    # Create separate cursors for different queries
    describe_cursor = MagicMock()
    count_cursor = MagicMock()

    # Mock describe table response
    describe_cursor.description = [("col_name",), ("data_type",), ("comment",)]
    describe_cursor.fetchall.return_value = [
        ("id", "int", ""),
        ("name", "string", ""),
        ("value", "int", ""),
    ]

    # Mock count query response
    count_cursor.description = [("row_count",)]
    count_cursor.fetchall.return_value = [(1000,)]

    # Return different cursors for different queries
    cursors = [describe_cursor, count_cursor]
    connection.cursor.return_value.__enter__.side_effect = cursors
    mock_connect.return_value.__enter__.return_value = connection

    service = DatabricksService(test_config)
    result = service.get_table_info("test_table")

    assert result["table_name"] == "test_catalog.test_schema.test_table"
    assert result["row_count"] == 1000
    assert len(result["columns"]) == 3


def test_get_table_info_with_custom_catalog_schema(test_config):
    """Test table info with custom catalog and schema."""
    with patch("src.services.databricks_service.sql.connect") as mock_connect:
        connection = MagicMock()

        # Create separate cursors for different queries
        describe_cursor = MagicMock()
        count_cursor = MagicMock()

        # Mock describe table response (empty result)
        describe_cursor.description = [("col_name",), ("data_type",), ("comment",)]
        describe_cursor.fetchall.return_value = []

        # Mock count query response
        count_cursor.description = [("row_count",)]
        count_cursor.fetchall.return_value = [(0,)]

        # Return different cursors for different queries
        cursors = [describe_cursor, count_cursor]
        connection.cursor.return_value.__enter__.side_effect = cursors
        mock_connect.return_value.__enter__.return_value = connection

        service = DatabricksService(test_config)
        result = service.get_table_info("test_table", "custom_catalog", "custom_schema")

        assert result["table_name"] == "custom_catalog.custom_schema.test_table"
