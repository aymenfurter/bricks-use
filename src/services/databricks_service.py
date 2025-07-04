"""Service for interacting with Databricks."""

import logging
import os
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
from databricks import sql

from ..config import DatabricksConfig

logger = logging.getLogger(__name__)

# Constants
DEFAULT_QUERY_LOG_LENGTH = 100
CSV_FILE_EXTENSION = ".csv"


class DatabricksServiceError(Exception):
    """Custom exception for Databricks service operations."""

    pass


class DatabricksService:
    """Service for Databricks SQL operations."""

    def __init__(self, config: DatabricksConfig):
        """Initialize the Databricks service.

        Args:
            config: The Databricks configuration.

        Raises:
            DatabricksServiceError: If connection initialization fails.
        """
        self.config = config
        self._ensure_temp_dir()

    def _ensure_temp_dir(self) -> None:
        """Ensure temporary directory exists."""
        try:
            os.makedirs(self.config.temp_dir, exist_ok=True)
        except OSError as e:
            raise DatabricksServiceError(f"Failed to create temp directory: {e}")

    def _get_connection(self):
        """Get a Databricks SQL connection.

        Returns:
            A Databricks SQL connection context manager.

        Raises:
            DatabricksServiceError: If connection fails.
        """
        try:
            return sql.connect(
                server_hostname=self.config.server_hostname,
                http_path=self.config.http_path,
                access_token=self.config.access_token,
            )
        except Exception as e:
            raise DatabricksServiceError(f"Failed to connect to Databricks: {e}")

    def _execute_sql_query(self, query: str) -> Tuple[List[str], List[Any]]:
        """Execute SQL query and return column names and rows.

        Args:
            query: SQL query to execute.

        Returns:
            Tuple of (column_names, rows).

        Raises:
            DatabricksServiceError: If query execution fails.
        """
        with self._get_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(query)

                # Get column names
                columns = (
                    [desc[0] for desc in cursor.description]
                    if cursor.description
                    else []
                )

                # Fetch all results
                rows = cursor.fetchall()

                return columns, rows

    def _create_result_dict(
        self, query: str, columns: List[str], rows: List[Any]
    ) -> Dict[str, Any]:
        """Create standardized result dictionary.

        Args:
            query: The executed query.
            columns: Column names.
            rows: Query result rows.

        Returns:
            Standardized result dictionary.
        """
        if rows and columns:
            df = pd.DataFrame(rows, columns=columns)
            return {
                "success": True,
                "row_count": len(df),
                "columns": columns,
                "data": df.to_dict("records"),
                "query": query,
            }
        else:
            return {
                "success": True,
                "row_count": 0,
                "columns": [],
                "data": [],
                "query": query,
            }

    def execute_query(self, query: str) -> Dict[str, Any]:
        """Execute a SQL query and return results.

        Args:
            query: SQL query to execute

        Returns:
            Dictionary containing query results and metadata

        Raises:
            DatabricksServiceError: If query execution fails
        """
        logger.info(f"Executing query: {query[:DEFAULT_QUERY_LOG_LENGTH]}...")

        try:
            columns, rows = self._execute_sql_query(query)
            result = self._create_result_dict(query, columns, rows)
            logger.info(
                f"Query executed successfully, returned {result['row_count']} rows"
            )
            return result

        except Exception as e:
            error_msg = f"Query execution failed: {e}"
            logger.error(error_msg)
            raise DatabricksServiceError(error_msg)

    def _build_full_table_name(
        self,
        table_name: str,
        catalog: Optional[str] = None,
        schema: Optional[str] = None,
    ) -> str:
        """Build full table name with catalog and schema.

        Args:
            table_name: Base table name.
            catalog: Optional catalog name (uses config default if not provided).
            schema: Optional schema name (uses config default if not provided).

        Returns:
            Full table name in format: catalog.schema.table_name
        """
        catalog = catalog or self.config.catalog
        schema = schema or self.config.schema
        return f"{catalog}.{schema}.{table_name}"

    def _save_dataframe_to_csv(self, df: pd.DataFrame, table_name: str) -> str:
        """Save DataFrame to CSV file.

        Args:
            df: DataFrame to save.
            table_name: Table name for CSV filename.

        Returns:
            Path to saved CSV file.
        """
        csv_path = os.path.join(
            self.config.temp_dir, f"{table_name}{CSV_FILE_EXTENSION}"
        )
        df.to_csv(csv_path, index=False)
        return csv_path

    def get_table_data(
        self,
        table_name: str,
        catalog: Optional[str] = None,
        schema: Optional[str] = None,
    ) -> str:
        """Get full data from a table and save to CSV.

        Args:
            table_name: Name of the table
            catalog: Optional catalog name (uses config default if not provided)
            schema: Optional schema name (uses config default if not provided)

        Returns:
            Path to the saved CSV file

        Raises:
            DatabricksServiceError: If table retrieval fails
        """
        full_table_name = self._build_full_table_name(table_name, catalog, schema)

        logger.info(f"Retrieving data from table: {full_table_name}")

        try:
            query = f"SELECT * FROM {full_table_name}"
            result = self.execute_query(query)

            if not result["success"] or not result["data"]:
                raise DatabricksServiceError(
                    f"No data found in table {full_table_name}"
                )

            # Convert to DataFrame and save as CSV
            df = pd.DataFrame(result["data"])
            csv_path = self._save_dataframe_to_csv(df, table_name)

            logger.info(f"Saved {result['row_count']} rows to {csv_path}")
            return csv_path

        except DatabricksServiceError:
            raise
        except Exception as e:
            error_msg = f"Failed to retrieve table data: {e}"
            logger.error(error_msg)
            raise DatabricksServiceError(error_msg)

    def get_table_info(
        self,
        table_name: str,
        catalog: Optional[str] = None,
        schema: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Get information about a table.

        Args:
            table_name: Name of the table
            catalog: Optional catalog name
            schema: Optional schema name

        Returns:
            Dictionary containing table information

        Raises:
            DatabricksServiceError: If table info retrieval fails
        """
        full_table_name = self._build_full_table_name(table_name, catalog, schema)

        try:
            # Get table description
            desc_result = self._get_table_description(full_table_name)

            # Get row count
            row_count = self._get_table_row_count(full_table_name)

            return {
                "table_name": full_table_name,
                "columns": desc_result["data"],
                "row_count": row_count,
            }

        except DatabricksServiceError:
            raise
        except Exception as e:
            error_msg = f"Failed to get table info: {e}"
            logger.error(error_msg)
            raise DatabricksServiceError(error_msg)

    def _get_table_description(self, full_table_name: str) -> Dict[str, Any]:
        """Get table description using DESCRIBE TABLE.

        Args:
            full_table_name: Full table name including catalog and schema.

        Returns:
            Query result containing table description.
        """
        desc_query = f"DESCRIBE TABLE {full_table_name}"
        return self.execute_query(desc_query)

    def _get_table_row_count(self, full_table_name: str) -> int:
        """Get table row count.

        Args:
            full_table_name: Full table name including catalog and schema.

        Returns:
            Number of rows in the table.
        """
        count_query = f"SELECT COUNT(*) as row_count FROM {full_table_name}"
        count_result = self.execute_query(count_query)
        return count_result["data"][0]["row_count"] if count_result["data"] else 0
