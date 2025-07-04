"""Tests for query tool."""

from unittest.mock import Mock

import pytest

from src.services.databricks_service import DatabricksServiceError
from src.tools.query_tool import QueryTool


@pytest.mark.asyncio
async def test_execute_query_success(
    mock_context, mock_databricks_service, sample_query_result
):
    """Test successful query execution."""
    mock_databricks_service.execute_query.return_value = sample_query_result

    tool = QueryTool(mock_databricks_service)
    result = await tool.execute_query(mock_context, query="SELECT * FROM test_table")

    assert result["success"] is True
    assert result["row_count"] == 2
    assert result["columns"] == ["id", "name", "value"]
    assert len(result["data"]) == 2
    mock_context.info.assert_called()


@pytest.mark.asyncio
async def test_execute_query_with_limit(
    mock_context, mock_databricks_service, sample_query_result
):
    """Test query execution with limit."""
    mock_databricks_service.execute_query.return_value = sample_query_result

    tool = QueryTool(mock_databricks_service)
    await tool.execute_query(mock_context, query="SELECT * FROM test_table", limit=500)

    # Verify the query was modified to include LIMIT
    call_args = mock_databricks_service.execute_query.call_args[0][0]
    assert "LIMIT 500" in call_args


@pytest.mark.asyncio
async def test_execute_query_existing_limit(
    mock_context, mock_databricks_service, sample_query_result
):
    """Test query execution with existing LIMIT clause."""
    mock_databricks_service.execute_query.return_value = sample_query_result

    tool = QueryTool(mock_databricks_service)
    query_with_limit = "SELECT * FROM test_table LIMIT 100"
    await tool.execute_query(mock_context, query=query_with_limit, limit=500)

    # Verify the original query was not modified
    call_args = mock_databricks_service.execute_query.call_args[0][0]
    assert call_args == query_with_limit


@pytest.mark.asyncio
async def test_execute_query_non_select(
    mock_context, mock_databricks_service, sample_query_result
):
    """Test query execution with non-SELECT statement."""
    mock_databricks_service.execute_query.return_value = sample_query_result

    tool = QueryTool(mock_databricks_service)
    update_query = "UPDATE test_table SET value = 100"
    await tool.execute_query(mock_context, query=update_query, limit=500)

    # Verify LIMIT was not added to non-SELECT query
    call_args = mock_databricks_service.execute_query.call_args[0][0]
    assert "LIMIT" not in call_args
    assert call_args == update_query


@pytest.mark.asyncio
async def test_execute_query_service_error(mock_context, mock_databricks_service):
    """Test query execution with service error."""
    mock_databricks_service.execute_query.side_effect = DatabricksServiceError(
        "Query failed"
    )

    tool = QueryTool(mock_databricks_service)
    result = await tool.execute_query(mock_context, query="SELECT * FROM test_table")

    assert result["success"] is False
    assert "Query failed" in result["error"]
    mock_context.error.assert_called()


@pytest.mark.asyncio
async def test_execute_query_unexpected_error(mock_context, mock_databricks_service):
    """Test query execution with unexpected error."""
    mock_databricks_service.execute_query.side_effect = Exception("Unexpected error")

    tool = QueryTool(mock_databricks_service)
    result = await tool.execute_query(mock_context, query="SELECT * FROM test_table")

    assert result["success"] is False
    assert "Unexpected error" in result["error"]
    mock_context.error.assert_called()


@pytest.mark.asyncio
async def test_get_table_info_success(
    mock_context, mock_databricks_service, sample_table_info
):
    """Test successful table info retrieval."""
    mock_databricks_service.get_table_info.return_value = sample_table_info

    tool = QueryTool(mock_databricks_service)
    result = await tool.get_table_info(mock_context, table_name="test_table")

    assert result["success"] is True
    assert result["table_info"]["table_name"] == "test_catalog.test_schema.test_table"
    assert result["table_info"]["row_count"] == 1000
    mock_context.info.assert_called()


@pytest.mark.asyncio
async def test_get_table_info_with_custom_catalog_schema(
    mock_context, mock_databricks_service, sample_table_info
):
    """Test table info with custom catalog and schema."""
    mock_databricks_service.get_table_info.return_value = sample_table_info

    tool = QueryTool(mock_databricks_service)
    await tool.get_table_info(
        mock_context,
        table_name="test_table",
        catalog="custom_catalog",
        schema="custom_schema",
    )

    mock_databricks_service.get_table_info.assert_called_with(
        "test_table", "custom_catalog", "custom_schema"
    )


@pytest.mark.asyncio
async def test_get_table_info_service_error(mock_context, mock_databricks_service):
    """Test table info retrieval with service error."""
    mock_databricks_service.get_table_info.side_effect = DatabricksServiceError(
        "Table not found"
    )

    tool = QueryTool(mock_databricks_service)
    result = await tool.get_table_info(mock_context, table_name="nonexistent_table")

    assert result["success"] is False
    assert "Table not found" in result["error"]
    mock_context.error.assert_called()


def test_add_limit_if_needed():
    """Test the _add_limit_if_needed method."""
    tool = QueryTool(Mock())

    # Test adding limit to SELECT query
    query = "SELECT * FROM test_table"
    result = tool._add_limit_if_needed(query, 100)
    assert result == "SELECT * FROM test_table LIMIT 100"

    # Test not adding limit to query with existing LIMIT
    query_with_limit = "SELECT * FROM test_table LIMIT 50"
    result = tool._add_limit_if_needed(query_with_limit, 100)
    assert result == query_with_limit

    # Test not adding limit to non-SELECT query
    update_query = "UPDATE test_table SET value = 1"
    result = tool._add_limit_if_needed(update_query, 100)
    assert result == update_query

    # Test removing semicolon before adding limit
    query_with_semicolon = "SELECT * FROM test_table;"
    result = tool._add_limit_if_needed(query_with_semicolon, 100)
    assert result == "SELECT * FROM test_table LIMIT 100"
