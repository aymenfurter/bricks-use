"""Tests for Databricks configuration module."""

import os
from unittest.mock import patch

import pytest

from src.config import DatabricksConfig


def test_config_initialization():
    """Test configuration initialization with all parameters."""
    config = DatabricksConfig(
        server_hostname="test.databricks.com",
        http_path="/sql/1.0/warehouses/test",
        access_token="test_token",
        catalog="test_catalog",
        schema="test_schema",
        temp_dir="/tmp/test",
    )

    assert config.server_hostname == "test.databricks.com"
    assert config.http_path == "/sql/1.0/warehouses/test"
    assert config.access_token == "test_token"
    assert config.catalog == "test_catalog"
    assert config.schema == "test_schema"
    assert config.temp_dir == "/tmp/test"


def test_config_load_from_env():
    """Test loading configuration from environment variables."""
    config = DatabricksConfig.load_from_env()

    assert config.server_hostname == "test.databricks.com"
    assert config.http_path == "/sql/1.0/warehouses/test"
    assert config.access_token == "test_token"
    assert config.catalog == "test_catalog"
    assert config.schema == "test_schema"


def test_config_load_from_env_missing_required():
    """Test configuration failure when required environment variables are missing."""
    with patch.dict(os.environ, {}, clear=True):
        with pytest.raises(ValueError) as exc_info:
            DatabricksConfig.load_from_env()

        assert "Missing Databricks configuration" in str(exc_info.value)


def test_config_load_from_env_with_defaults():
    """Test configuration with default values."""
    with patch.dict(
        os.environ,
        {
            "DATABRICKS_SERVER_HOSTNAME": "test.databricks.com",
            "DATABRICKS_HTTP_PATH": "/sql/1.0/warehouses/test",
            "DATABRICKS_ACCESS_TOKEN": "test_token",
        },
        clear=True,
    ):
        config = DatabricksConfig.load_from_env()

        assert config.catalog == "main"
        assert config.schema == "default"
        assert config.temp_dir == "/tmp/databricks_mcp"


@pytest.mark.parametrize(
    "missing_var",
    ["DATABRICKS_SERVER_HOSTNAME", "DATABRICKS_HTTP_PATH", "DATABRICKS_ACCESS_TOKEN"],
)
def test_config_missing_individual_vars(missing_var):
    """Test configuration failure for each required variable."""
    env_vars = {
        "DATABRICKS_SERVER_HOSTNAME": "test.databricks.com",
        "DATABRICKS_HTTP_PATH": "/sql/1.0/warehouses/test",
        "DATABRICKS_ACCESS_TOKEN": "test_token",
    }

    # Remove one required variable
    del env_vars[missing_var]

    with patch.dict(os.environ, env_vars, clear=True):
        with pytest.raises(ValueError):
            DatabricksConfig.load_from_env()
