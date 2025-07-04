"""Tests for Databricks MCP server."""

from unittest.mock import Mock, patch

import pytest

from src.config import DatabricksConfig
from src.server import DatabricksMCPServer


def test_server_initialization(test_config):
    """Test server initialization with config."""
    with patch("src.server.DatabricksService"), patch("src.server.QueryTool"), patch(
        "src.server.TableCompareTool"
    ), patch.object(DatabricksMCPServer, "_register_tools"):

        server = DatabricksMCPServer()
        assert server.initialize(test_config) is True
        assert server.config == test_config
        assert server.databricks_service is not None
        assert server.query_tool is not None
        assert server.table_compare_tool is not None


def test_server_initialization_failure(test_config):
    """Test server initialization failure handling."""
    with patch("src.server.DatabricksService", side_effect=Exception("Init failed")):
        server = DatabricksMCPServer()
        assert server.initialize(test_config) is False
        assert server.databricks_service is None


def test_server_reset_services():
    """Test resetting server services."""
    server = DatabricksMCPServer()
    server._reset_services()
    assert server.databricks_service is None
    assert server.query_tool is None
    assert server.table_compare_tool is None


def test_server_register_tools():
    """Test tool registration."""
    server = DatabricksMCPServer()

    # Create mock methods that are actual functions (not Mock objects)
    async def mock_execute_query(context, query: str):
        pass

    async def mock_get_table_info(context, table_name: str):
        pass

    async def mock_compare_tables(context, table1: str, table2: str):
        pass

    async def mock_quick_compare_tables(context, table1: str, table2: str):
        pass

    # Create mock objects with real methods
    server.query_tool = Mock()
    server.query_tool.execute_query = mock_execute_query
    server.query_tool.get_table_info = mock_get_table_info

    server.table_compare_tool = Mock()
    server.table_compare_tool.compare_tables = mock_compare_tables
    server.table_compare_tool.quick_compare_tables = mock_quick_compare_tables

    # This should not raise an exception
    server._register_tools()

    # Just verify the method completed without errors
    # The actual tool registration is handled by FastMCP internally


@patch("src.server.DatabricksService")
@patch("src.server.QueryTool")
@patch("src.server.TableCompareTool")
def test_server_run(mock_table_tool, mock_query_tool, mock_service, test_config):
    """Test server run method."""
    server = DatabricksMCPServer()
    server.initialize(test_config)

    with patch.object(server.mcp, "run") as mock_run:
        server.run()
        mock_run.assert_called_once()
