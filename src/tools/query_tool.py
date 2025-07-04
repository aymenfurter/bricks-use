"""Tool for executing Databricks SQL queries."""

import logging
from typing import Any, Dict, Optional

from fastmcp import Context

from ..services.databricks_service import (DatabricksService,
                                           DatabricksServiceError)

logger = logging.getLogger(__name__)

# Constants
DEFAULT_QUERY_LIMIT = 1000
SELECT_KEYWORD = "select"
LIMIT_KEYWORD = "limit"


class QueryTool:
    """Tool for executing SQL queries on Databricks."""

    def __init__(self, databricks_service: DatabricksService):
        """Initialize the query tool.

        Args:
            databricks_service: Service for Databricks operations.
        """
        self.databricks_service = databricks_service

    async def execute_query(
        self, ctx: Context, *, query: str, limit: int = DEFAULT_QUERY_LIMIT
    ) -> Dict[str, Any]:
        """Execute a SQL query on Databricks.

        Args:
            ctx: The FastMCP context
            query: SQL query to execute
            limit: Maximum number of rows to return (default 1000)

        Returns:
            Dictionary containing query results
        """
        await ctx.info("Executing Databricks query")

        try:
            # Add LIMIT clause if not present and limit is specified
            limited_query = self._add_limit_if_needed(query, limit)

            result = self.databricks_service.execute_query(limited_query)

            await ctx.info(
                f"Query executed successfully, returned {result['row_count']} rows"
            )

            return self._create_success_response(limited_query, result)

        except DatabricksServiceError as e:
            return await self._handle_databricks_error(ctx, e, query)
        except Exception as e:
            return await self._handle_unexpected_error(ctx, e, query)

    def _create_success_response(
        self, query: str, result: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Create a successful response dictionary.

        Args:
            query: The executed query.
            result: Query execution result.

        Returns:
            Formatted success response.
        """
        return {
            "success": True,
            "query": query,
            "row_count": result["row_count"],
            "columns": result["columns"],
            "data": result["data"],
        }

    async def _handle_databricks_error(
        self, ctx: Context, error: DatabricksServiceError, query: str
    ) -> Dict[str, Any]:
        """Handle Databricks service errors.

        Args:
            ctx: FastMCP context.
            error: The Databricks service error.
            query: Original query.

        Returns:
            Error response dictionary.
        """
        error_msg = str(error)
        await ctx.error(error_msg)
        return {"success": False, "error": error_msg, "query": query}

    async def _handle_unexpected_error(
        self, ctx: Context, error: Exception, query: str
    ) -> Dict[str, Any]:
        """Handle unexpected errors.

        Args:
            ctx: FastMCP context.
            error: The unexpected error.
            query: Original query.

        Returns:
            Error response dictionary.
        """
        error_msg = f"Unexpected error executing query: {error}"
        logger.exception(error_msg)
        await ctx.error(error_msg)
        return {"success": False, "error": str(error), "query": query}

    def _add_limit_if_needed(self, query: str, limit: int) -> str:
        """Add LIMIT clause to query if not present.

        Args:
            query: Original SQL query.
            limit: Row limit to apply.

        Returns:
            Query with LIMIT clause if needed.
        """
        query_lower = query.lower().strip()

        # Don't add limit if already present or if it's not a SELECT statement
        if self._has_limit_clause(query_lower) or not self._is_select_query(
            query_lower
        ):
            return query

        return f"{query.rstrip(';')} LIMIT {limit}"

    def _has_limit_clause(self, query_lower: str) -> bool:
        """Check if query already has a LIMIT clause.

        Args:
            query_lower: Lowercase query string.

        Returns:
            True if LIMIT clause is present.
        """
        return LIMIT_KEYWORD in query_lower

    def _is_select_query(self, query_lower: str) -> bool:
        """Check if query is a SELECT statement.

        Args:
            query_lower: Lowercase query string.

        Returns:
            True if query is a SELECT statement.
        """
        return query_lower.startswith(SELECT_KEYWORD)

    async def get_table_info(
        self,
        ctx: Context,
        *,
        table_name: str,
        catalog: Optional[str] = None,
        schema: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Get information about a Databricks table.

        Args:
            ctx: The FastMCP context
            table_name: Name of the table
            catalog: Optional catalog name
            schema: Optional schema name

        Returns:
            Dictionary containing table information
        """
        await ctx.info(f"Getting info for table: {table_name}")

        try:
            table_info = self.databricks_service.get_table_info(
                table_name, catalog, schema
            )

            await ctx.info(f"Retrieved info for table {table_info['table_name']}")

            return {"success": True, "table_info": table_info}

        except DatabricksServiceError as e:
            return await self._handle_databricks_error(ctx, e, table_name)
        except Exception as e:
            return await self._handle_table_info_error(ctx, e)

    async def _handle_table_info_error(
        self, ctx: Context, error: Exception
    ) -> Dict[str, Any]:
        """Handle table info retrieval errors.

        Args:
            ctx: FastMCP context.
            error: The error that occurred.

        Returns:
            Error response dictionary.
        """
        error_msg = f"Unexpected error getting table info: {error}"
        logger.exception(error_msg)
        await ctx.error(error_msg)
        return {"success": False, "error": str(error)}
