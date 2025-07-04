"""Pytest configuration and fixtures for Databricks MCP Server tests."""

import os
import tempfile
from unittest.mock import AsyncMock, Mock, patch

import pytest
from fastmcp import Context

from src.config import DatabricksConfig
from src.services.databricks_service import DatabricksService


@pytest.fixture
def test_config():
    """Test configuration fixture."""
    return DatabricksConfig(
        server_hostname="test.databricks.com",
        http_path="/sql/1.0/warehouses/test",
        access_token="test_token",
        catalog="test_catalog",
        schema="test_schema",
        temp_dir="/tmp/test_databricks",
    )


@pytest.fixture
def mock_databricks_service(test_config):
    """Mock Databricks service fixture."""
    service = Mock(spec=DatabricksService)
    service.config = test_config
    service.execute_query = Mock()
    service.get_table_data = Mock()
    service.get_table_info = Mock()
    return service


@pytest.fixture
def mock_context():
    """Mock FastMCP context fixture."""
    context = Mock(spec=Context)
    context.info = AsyncMock()
    context.error = AsyncMock()
    context.warning = AsyncMock()
    return context


@pytest.fixture
def temp_csv_files():
    """Create temporary CSV files for testing."""
    with tempfile.TemporaryDirectory() as temp_dir:
        file1 = os.path.join(temp_dir, "table1.csv")
        file2 = os.path.join(temp_dir, "table2.csv")

        # Create identical files
        with open(file1, "w") as f:
            f.write("id,name,value\n1,test,100\n2,example,200\n")

        with open(file2, "w") as f:
            f.write("id,name,value\n1,test,100\n2,example,200\n")

        yield file1, file2


@pytest.fixture
def temp_different_csv_files():
    """Create temporary CSV files with differences for testing."""
    with tempfile.TemporaryDirectory() as temp_dir:
        file1 = os.path.join(temp_dir, "table1.csv")
        file2 = os.path.join(temp_dir, "table2.csv")

        # Create different files
        with open(file1, "w") as f:
            f.write("id,name,value\n1,test,100\n2,example,200\n")

        with open(file2, "w") as f:
            f.write("id,name,value\n1,test,150\n2,example,250\n")

        yield file1, file2


@pytest.fixture
def sample_query_result():
    """Sample query result for testing."""
    return {
        "success": True,
        "row_count": 2,
        "columns": ["id", "name", "value"],
        "data": [
            {"id": 1, "name": "test", "value": 100},
            {"id": 2, "name": "example", "value": 200},
        ],
        "query": "SELECT * FROM test_table",
    }


@pytest.fixture
def sample_table_info():
    """Sample table info for testing."""
    return {
        "table_name": "test_catalog.test_schema.test_table",
        "columns": [
            {"col_name": "id", "data_type": "int"},
            {"col_name": "name", "data_type": "string"},
            {"col_name": "value", "data_type": "int"},
        ],
        "row_count": 1000,
    }


@pytest.fixture(autouse=True)
def mock_env_vars():
    """Mock environment variables for tests."""
    with patch.dict(
        os.environ,
        {
            "DATABRICKS_SERVER_HOSTNAME": "test.databricks.com",
            "DATABRICKS_HTTP_PATH": "/sql/1.0/warehouses/test",
            "DATABRICKS_ACCESS_TOKEN": "test_token",
            "DATABRICKS_CATALOG": "test_catalog",
            "DATABRICKS_SCHEMA": "test_schema",
        },
    ):
        yield
