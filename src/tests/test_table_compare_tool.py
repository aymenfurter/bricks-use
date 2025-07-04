"""Tests for table compare tool."""

from unittest.mock import Mock, patch

import pytest

from src.services.databricks_service import DatabricksServiceError
from src.tools.table_compare_tool import TableCompareTool


@pytest.mark.asyncio
async def test_compare_tables_success(
    mock_context, mock_databricks_service, temp_csv_files
):
    """Test successful table comparison."""
    file1, file2 = temp_csv_files
    mock_databricks_service.get_table_data.side_effect = [file1, file2]

    tool = TableCompareTool(mock_databricks_service)
    result = await tool.compare_tables(mock_context, table1="table1", table2="table2")

    assert result["success"] is True
    assert result["table1"] == "table1"
    assert result["table2"] == "table2"
    assert result["files_identical"] is True
    assert "Files are identical" in result["diff_output"]
    mock_context.info.assert_called()


@pytest.mark.asyncio
async def test_compare_tables_with_differences(
    mock_context, mock_databricks_service, temp_different_csv_files
):
    """Test table comparison with differences."""
    file1, file2 = temp_different_csv_files
    mock_databricks_service.get_table_data.side_effect = [file1, file2]

    tool = TableCompareTool(mock_databricks_service)
    result = await tool.compare_tables(
        mock_context, table1="table1", table2="table2", diff_lines=5
    )

    assert result["success"] is True
    assert result["files_identical"] is False
    assert "100" in result["diff_output"]  # Should show the difference
    assert "150" in result["diff_output"]


@pytest.mark.asyncio
async def test_compare_tables_with_catalogs_schemas(
    mock_context, mock_databricks_service, temp_csv_files
):
    """Test table comparison with specific catalogs and schemas."""
    file1, file2 = temp_csv_files
    mock_databricks_service.get_table_data.side_effect = [file1, file2]

    tool = TableCompareTool(mock_databricks_service)
    await tool.compare_tables(
        mock_context,
        table1="table1",
        table2="table2",
        catalog1="cat1",
        schema1="schema1",
        catalog2="cat2",
        schema2="schema2",
    )

    # Verify service was called with correct parameters
    calls = mock_databricks_service.get_table_data.call_args_list
    assert calls[0][0] == ("table1", "cat1", "schema1")
    assert calls[1][0] == ("table2", "cat2", "schema2")


@pytest.mark.asyncio
async def test_compare_tables_service_error(mock_context, mock_databricks_service):
    """Test table comparison with service error."""
    mock_databricks_service.get_table_data.side_effect = DatabricksServiceError(
        "Table not found"
    )

    tool = TableCompareTool(mock_databricks_service)
    result = await tool.compare_tables(mock_context, table1="table1", table2="table2")

    assert result["success"] is False
    assert "Table not found" in result["error"]
    mock_context.error.assert_called()


@pytest.mark.asyncio
async def test_compare_tables_unexpected_error(mock_context, mock_databricks_service):
    """Test table comparison with unexpected error."""
    mock_databricks_service.get_table_data.side_effect = Exception("Unexpected error")

    tool = TableCompareTool(mock_databricks_service)
    result = await tool.compare_tables(mock_context, table1="table1", table2="table2")

    assert result["success"] is False
    assert "Unexpected error" in result["error"]
    mock_context.error.assert_called()


@pytest.mark.asyncio
async def test_quick_compare_tables_success(
    mock_context, mock_databricks_service, sample_table_info
):
    """Test successful quick table comparison."""
    table_info_1 = sample_table_info.copy()
    table_info_2 = sample_table_info.copy()
    table_info_2["row_count"] = 1200  # Different row count

    mock_databricks_service.get_table_info.side_effect = [table_info_1, table_info_2]

    tool = TableCompareTool(mock_databricks_service)
    result = await tool.quick_compare_tables(
        mock_context, table1="table1", table2="table2"
    )

    assert result["success"] is True
    assert result["row_count_difference"] == -200  # 1000 - 1200
    assert result["column_count_difference"] == 0
    assert result["columns_match"] is True
    assert result["columns_missing_in_table1"] == []
    assert result["columns_missing_in_table2"] == []


@pytest.mark.asyncio
async def test_quick_compare_tables_different_columns(
    mock_context, mock_databricks_service, sample_table_info
):
    """Test quick comparison with different columns."""
    table_info_1 = sample_table_info.copy()
    table_info_2 = sample_table_info.copy()

    # Modify columns in table_info_2
    table_info_2["columns"] = [
        {"col_name": "id", "data_type": "int"},
        {"col_name": "name", "data_type": "string"},
        {"col_name": "email", "data_type": "string"},  # Different column
    ]

    mock_databricks_service.get_table_info.side_effect = [table_info_1, table_info_2]

    tool = TableCompareTool(mock_databricks_service)
    result = await tool.quick_compare_tables(
        mock_context, table1="table1", table2="table2"
    )

    assert result["success"] is True
    assert result["columns_match"] is False
    assert "value" in result["columns_missing_in_table2"]
    assert "email" in result["columns_missing_in_table1"]


def test_run_diff_command_identical_files(temp_csv_files):
    """Test diff command with identical files."""
    file1, file2 = temp_csv_files

    tool = TableCompareTool(Mock())
    result = tool._run_diff_command(file1, file2, 5)

    assert result["identical"] is True
    expected_output = "Files are identical (verified through progressive sampling)"
    assert result["output"] == expected_output
    assert result["return_code"] == 0


def test_run_diff_command_different_files(temp_different_csv_files):
    """Test diff command with different files."""
    file1, file2 = temp_different_csv_files

    tool = TableCompareTool(Mock())
    result = tool._run_diff_command(file1, file2, 5)

    assert result["identical"] is False
    assert "100" in result["output"]
    assert "150" in result["output"]
    assert result["return_code"] == 1


@patch("subprocess.run")
def test_run_diff_command_timeout(mock_subprocess_run):
    """Test diff command timeout."""
    from subprocess import TimeoutExpired

    mock_subprocess_run.side_effect = TimeoutExpired("diff", 300)

    tool = TableCompareTool(Mock())

    with pytest.raises(Exception) as exc_info:
        tool._run_diff_command("file1", "file2", 5)

    assert "timed out" in str(exc_info.value)


@patch("subprocess.run")
def test_run_diff_command_error(mock_subprocess_run):
    """Test diff command with error."""
    from subprocess import CalledProcessError

    mock_subprocess_run.side_effect = CalledProcessError(2, "diff", "Error message")

    tool = TableCompareTool(Mock())

    with pytest.raises(Exception) as exc_info:
        tool._run_diff_command("file1", "file2", 5)

    assert "Diff command failed" in str(exc_info.value)
