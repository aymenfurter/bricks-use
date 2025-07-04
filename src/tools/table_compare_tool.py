"""Tool for comparing two Databricks tables."""

import logging
import os
import subprocess
from typing import Any, Dict, List, Optional, Tuple

from fastmcp import Context

from ..services.databricks_service import (DatabricksService,
                                           DatabricksServiceError)

logger = logging.getLogger(__name__)

# Constants
DEFAULT_DIFF_LINES = 10
MAX_DIFF_OUTPUT_LINES = 10  # Maximum lines to show in diff output
DIFF_TIMEOUT_SECONDS = 300  # 5 minutes
DIFF_SUCCESS_CODE = 0
DIFF_DIFFERENT_CODE = 1
UNIFIED_DIFF_FORMAT = "-u"

# Smart diff strategy constants
INITIAL_SAMPLE_SIZE = 5
INCREMENTAL_SAMPLE_SIZES = [5, 25, 100, 500]  # Progressive sample sizes
MAX_SAMPLE_SIZE = 1000


class TableCompareTool:
    """Tool for comparing data between two Databricks tables."""

    def __init__(self, databricks_service: DatabricksService):
        """Initialize the table compare tool.

        Args:
            databricks_service: Service for Databricks operations.
        """
        self.databricks_service = databricks_service

    async def compare_tables(
        self,
        ctx: Context,
        *,
        table1: str,
        table2: str,
        catalog1: Optional[str] = None,
        schema1: Optional[str] = None,
        catalog2: Optional[str] = None,
        schema2: Optional[str] = None,
        diff_lines: int = DEFAULT_DIFF_LINES,
    ) -> Dict[str, Any]:
        """Compare data between two Databricks tables.

        Args:
            ctx: The FastMCP context
            table1: Name of the first table
            table2: Name of the second table
            catalog1: Optional catalog for table1
            schema1: Optional schema for table1
            catalog2: Optional catalog for table2
            schema2: Optional schema for table2
            diff_lines: Number of diff lines to show (default 10)

        Returns:
            Dictionary containing comparison results
        """
        await ctx.info(f"Comparing tables: {table1} vs {table2}")

        try:
            # Get data from both tables
            csv_path1, csv_path2 = await self._retrieve_table_data(
                ctx, table1, table2, catalog1, schema1, catalog2, schema2
            )

            # Run diff comparison
            await ctx.info("Running diff comparison...")
            diff_result = self._run_diff_command(csv_path1, csv_path2, diff_lines)

            # Get additional file information
            file_info = self._get_file_comparison_info(csv_path1, csv_path2)

            await ctx.info("Table comparison completed")

            return self._create_comparison_result(
                table1, table2, csv_path1, csv_path2, diff_result, file_info
            )

        except DatabricksServiceError as e:
            return await self._handle_databricks_error(ctx, e)
        except Exception as e:
            return await self._handle_unexpected_error(ctx, e)

    async def _retrieve_table_data(
        self,
        ctx: Context,
        table1: str,
        table2: str,
        catalog1: Optional[str],
        schema1: Optional[str],
        catalog2: Optional[str],
        schema2: Optional[str],
    ) -> Tuple[str, str]:
        """Retrieve data from both tables and return CSV paths.

        Args:
            ctx: FastMCP context.
            table1: First table name.
            table2: Second table name.
            catalog1: Optional catalog for table1.
            schema1: Optional schema for table1.
            catalog2: Optional catalog for table2.
            schema2: Optional schema for table2.

        Returns:
            Tuple of (csv_path1, csv_path2).
        """
        await ctx.info("Retrieving data from first table...")
        csv_path1 = self.databricks_service.get_table_data(table1, catalog1, schema1)

        await ctx.info("Retrieving data from second table...")
        csv_path2 = self.databricks_service.get_table_data(table2, catalog2, schema2)

        return csv_path1, csv_path2

    def _get_file_comparison_info(
        self, csv_path1: str, csv_path2: str
    ) -> Dict[str, int]:
        """Get file size information for comparison.

        Args:
            csv_path1: Path to first CSV file.
            csv_path2: Path to second CSV file.

        Returns:
            Dictionary with file size information.
        """
        return {
            "size1": os.path.getsize(csv_path1),
            "size2": os.path.getsize(csv_path2),
        }

    def _create_comparison_result(
        self,
        table1: str,
        table2: str,
        csv_path1: str,
        csv_path2: str,
        diff_result: Dict[str, Any],
        file_info: Dict[str, int],
    ) -> Dict[str, Any]:
        """Create the final comparison result dictionary.

        Args:
            table1: First table name.
            table2: Second table name.
            csv_path1: Path to first CSV file.
            csv_path2: Path to second CSV file.
            diff_result: Result from diff command.
            file_info: File size information.

        Returns:
            Complete comparison result.
        """
        return {
            "success": True,
            "table1": table1,
            "table2": table2,
            "csv_path1": csv_path1,
            "csv_path2": csv_path2,
            "file_size1": file_info["size1"],
            "file_size2": file_info["size2"],
            "diff_output": diff_result["output"],
            "files_identical": diff_result["identical"],
            "diff_command": diff_result["command"],
        }

    async def _handle_databricks_error(
        self, ctx: Context, error: DatabricksServiceError
    ) -> Dict[str, Any]:
        """Handle Databricks service errors.

        Args:
            ctx: FastMCP context.
            error: The Databricks service error.

        Returns:
            Error response dictionary.
        """
        error_msg = str(error)
        await ctx.error(error_msg)
        return {"success": False, "error": error_msg}

    async def _handle_unexpected_error(
        self, ctx: Context, error: Exception
    ) -> Dict[str, Any]:
        """Handle unexpected errors.

        Args:
            ctx: FastMCP context.
            error: The unexpected error.

        Returns:
            Error response dictionary.
        """
        error_msg = f"Unexpected error comparing tables: {error}"
        logger.exception(error_msg)
        await ctx.error(error_msg)
        return {"success": False, "error": str(error)}

    def _run_diff_command(
        self, file1: str, file2: str, context_lines: int
    ) -> Dict[str, Any]:
        """Run smart diff command to compare two CSV files.

        Uses progressive sampling strategy: starts with small samples and increases
        if no differences found, but limits output to MAX_DIFF_OUTPUT_LINES.

        Args:
            file1: Path to first CSV file
            file2: Path to second CSV file
            context_lines: Number of context lines for diff (max applied)

        Returns:
            Dictionary containing diff results

        Raises:
            Exception: If diff command fails or times out.
        """
        try:
            # Limit context lines to maximum
            actual_context_lines = min(context_lines, MAX_DIFF_OUTPUT_LINES)

            # Try smart diff with progressive sampling
            return self._smart_diff_with_sampling(file1, file2, actual_context_lines)

        except subprocess.TimeoutExpired:
            raise Exception(
                f"Diff command timed out after {DIFF_TIMEOUT_SECONDS // 60} minutes"
            )
        except subprocess.CalledProcessError as e:
            raise Exception(f"Diff command failed: {e}")
        except Exception as e:
            raise Exception(f"Error running diff: {e}")

    def _smart_diff_with_sampling(
        self, file1: str, file2: str, context_lines: int
    ) -> Dict[str, Any]:
        """Perform smart diff using progressive sampling strategy.

        Args:
            file1: Path to first CSV file.
            file2: Path to second CSV file.
            context_lines: Number of context lines for diff.

        Returns:
            Dictionary containing diff results.
        """
        for sample_size in INCREMENTAL_SAMPLE_SIZES:
            logger.debug(f"Trying diff with sample size: {sample_size}")

            # Create temporary sample files
            sample_file1 = self._create_sample_file(
                file1, sample_size, suffix="_1"
            )
            sample_file2 = self._create_sample_file(
                file2, sample_size, suffix="_2"
            )

            try:
                # Run diff on samples
                cmd = self._build_diff_command(
                    sample_file1, sample_file2, context_lines
                )
                result = self._execute_diff_command(cmd)

                # If files are different, return the result
                if result.returncode == DIFF_DIFFERENT_CODE:
                    diff_result = self._process_diff_result(cmd, result)
                    # Update command to show original files for user clarity
                    command_text = f"diff (sample {sample_size} lines) {file1} {file2}"
                    diff_result["command"] = command_text
                    diff_result["sample_size"] = sample_size
                    return diff_result
                elif result.returncode != DIFF_SUCCESS_CODE:
                    # Error case - process and return
                    return self._process_diff_result(cmd, result)

                # Files are identical for this sample size, try larger sample
                logger.debug(f"No differences found in {sample_size} line sample")

            finally:
                # Clean up temporary sample files
                self._cleanup_sample_files(sample_file1, sample_file2)

        # If we've tried all sample sizes and found no differences,
        # the files are likely identical
        max_sample = INCREMENTAL_SAMPLE_SIZES[-1]
        return {
            "command": f"diff (smart sampling up to {max_sample} lines)",
            "output": "Files are identical (verified through progressive sampling)",
            "identical": True,
            "return_code": DIFF_SUCCESS_CODE,
            "sample_size": INCREMENTAL_SAMPLE_SIZES[-1]
        }

    def _create_sample_file(
        self, original_file: str, sample_size: int, suffix: str = ""
    ) -> str:
        """Create a temporary file with first N lines of the original file.

        Args:
            original_file: Path to original file.
            sample_size: Number of lines to include.
            suffix: Suffix for temp file name.

        Returns:
            Path to temporary sample file.
        """
        # Create temporary file in same directory as original
        temp_dir = os.path.dirname(original_file)
        base_name = os.path.basename(original_file)
        temp_file = os.path.join(
            temp_dir, f"sample_{sample_size}{suffix}_{base_name}"
        )

        # Use head command to get first N lines
        cmd = ["head", "-n", str(sample_size), original_file]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)

        if result.returncode != 0:
            raise Exception(f"Failed to create sample file: {result.stderr}")

        # Write sample to temporary file
        with open(temp_file, 'w') as f:
            f.write(result.stdout)

        return temp_file

    def _cleanup_sample_files(self, *file_paths: str) -> None:
        """Clean up temporary sample files.

        Args:
            file_paths: Paths to files to remove.
        """
        for file_path in file_paths:
            try:
                if os.path.exists(file_path):
                    os.remove(file_path)
            except OSError as e:
                logger.warning(f"Failed to remove sample file {file_path}: {e}")

    def _build_diff_command(
        self, file1: str, file2: str, context_lines: int
    ) -> List[str]:
        """Build the diff command with parameters.

        Args:
            file1: Path to first file.
            file2: Path to second file.
            context_lines: Number of context lines.

        Returns:
            Command as list of strings.
        """
        return ["diff", f"{UNIFIED_DIFF_FORMAT}{context_lines}", file1, file2]

    def _execute_diff_command(self, cmd: List[str]) -> subprocess.CompletedProcess:
        """Execute the diff command.

        Args:
            cmd: Command to execute.

        Returns:
            Completed process result.
        """
        return subprocess.run(
            cmd, capture_output=True, text=True, timeout=DIFF_TIMEOUT_SECONDS
        )

    def _process_diff_result(
        self, cmd: List[str], result: subprocess.CompletedProcess
    ) -> Dict[str, Any]:
        """Process the diff command result.

        Args:
            cmd: Original command.
            result: Process result.

        Returns:
            Processed diff result.
        """
        if result.returncode == DIFF_SUCCESS_CODE:
            return {
                "command": " ".join(cmd),
                "output": "Files are identical",
                "identical": True,
                "return_code": result.returncode,
            }
        elif result.returncode == DIFF_DIFFERENT_CODE:
            # Limit the output to MAX_DIFF_OUTPUT_LINES
            limited_output = self._limit_diff_output(result.stdout)
            return {
                "command": " ".join(cmd),
                "output": limited_output,
                "identical": False,
                "return_code": result.returncode,
            }
        else:
            # Error occurred
            error_msg = result.stderr or "Unknown diff error"
            raise subprocess.CalledProcessError(result.returncode, cmd, error_msg)

    def _limit_diff_output(self, diff_output: str) -> str:
        """Limit diff output to maximum number of lines.

        Args:
            diff_output: Original diff output.

        Returns:
            Limited diff output with truncation message if needed.
        """
        lines = diff_output.split('\n')

        if len(lines) <= MAX_DIFF_OUTPUT_LINES:
            return diff_output

        # Take first MAX_DIFF_OUTPUT_LINES and add truncation message
        limited_lines = lines[:MAX_DIFF_OUTPUT_LINES]
        total_lines = len(lines)
        truncated_count = total_lines - MAX_DIFF_OUTPUT_LINES

        truncation_msg = (
            f"\n... (truncated {truncated_count} more lines, "
            f"showing first {MAX_DIFF_OUTPUT_LINES} lines)"
        )
        limited_lines.append(truncation_msg)

        return '\n'.join(limited_lines)

    async def quick_compare_tables(
        self,
        ctx: Context,
        *,
        table1: str,
        table2: str,
        catalog1: Optional[str] = None,
        schema1: Optional[str] = None,
        catalog2: Optional[str] = None,
        schema2: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Quick comparison of table metadata without downloading full data.

        Args:
            ctx: The FastMCP context
            table1: Name of the first table
            table2: Name of the second table
            catalog1: Optional catalog for table1
            schema1: Optional schema for table1
            catalog2: Optional catalog for table2
            schema2: Optional schema for table2

        Returns:
            Dictionary containing quick comparison results
        """
        await ctx.info(f"Quick comparing tables: {table1} vs {table2}")

        try:
            # Get table info for both tables
            info1, info2 = await self._get_both_table_infos(
                table1, table2, catalog1, schema1, catalog2, schema2
            )

            # Perform comparisons
            comparison_metrics = self._calculate_comparison_metrics(info1, info2)
            column_comparison = self._compare_columns(
                info1["columns"], info2["columns"]
            )

            await ctx.info("Quick table comparison completed")

            return self._create_quick_comparison_result(
                info1, info2, comparison_metrics, column_comparison
            )

        except DatabricksServiceError as e:
            return await self._handle_databricks_error(ctx, e)
        except Exception as e:
            return await self._handle_quick_comparison_error(ctx, e)

    async def _get_both_table_infos(
        self,
        table1: str,
        table2: str,
        catalog1: Optional[str],
        schema1: Optional[str],
        catalog2: Optional[str],
        schema2: Optional[str],
    ) -> Tuple[Dict[str, Any], Dict[str, Any]]:
        """Get info for both tables.

        Args:
            table1: First table name.
            table2: Second table name.
            catalog1: Optional catalog for table1.
            schema1: Optional schema for table1.
            catalog2: Optional catalog for table2.
            schema2: Optional schema for table2.

        Returns:
            Tuple of (info1, info2).
        """
        info1 = self.databricks_service.get_table_info(table1, catalog1, schema1)
        info2 = self.databricks_service.get_table_info(table2, catalog2, schema2)
        return info1, info2

    def _calculate_comparison_metrics(
        self, info1: Dict[str, Any], info2: Dict[str, Any]
    ) -> Dict[str, int]:
        """Calculate basic comparison metrics.

        Args:
            info1: First table info.
            info2: Second table info.

        Returns:
            Dictionary with comparison metrics.
        """
        return {
            "row_count_difference": info1["row_count"] - info2["row_count"],
            "column_count_difference": len(info1["columns"]) - len(info2["columns"]),
        }

    def _compare_columns(self, cols1: List[Dict], cols2: List[Dict]) -> Dict[str, Any]:
        """Compare column information between tables.

        Args:
            cols1: Columns from first table.
            cols2: Columns from second table.

        Returns:
            Column comparison results.
        """
        col_names1 = {col["col_name"] for col in cols1}
        col_names2 = {col["col_name"] for col in cols2}

        return {
            "columns_match": col_names1 == col_names2,
            "columns_missing_in_table2": list(col_names1 - col_names2),
            "columns_missing_in_table1": list(col_names2 - col_names1),
        }

    def _create_quick_comparison_result(
        self,
        info1: Dict[str, Any],
        info2: Dict[str, Any],
        metrics: Dict[str, int],
        column_comparison: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Create the quick comparison result dictionary.

        Args:
            info1: First table info.
            info2: Second table info.
            metrics: Comparison metrics.
            column_comparison: Column comparison results.

        Returns:
            Complete quick comparison result.
        """
        result = {"success": True, "table1_info": info1, "table2_info": info2}
        result.update(metrics)
        result.update(column_comparison)
        return result

    async def _handle_quick_comparison_error(
        self, ctx: Context, error: Exception
    ) -> Dict[str, Any]:
        """Handle quick comparison errors.

        Args:
            ctx: FastMCP context.
            error: The error that occurred.

        Returns:
            Error response dictionary.
        """
        error_msg = f"Unexpected error in quick comparison: {error}"
        logger.exception(error_msg)
        await ctx.error(error_msg)
        return {"success": False, "error": str(error)}
