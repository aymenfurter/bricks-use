"""FastMCP server implementation for Databricks operations."""

import logging
from typing import Optional

from fastmcp import FastMCP

from .config import DatabricksConfig
from .services import DatabricksService
from .tools import QueryTool, TableCompareTool

logger = logging.getLogger(__name__)

# Constants
SERVER_NAME = "DatabricksOperations"
SERVER_INSTRUCTIONS = (
    "Provides tools to execute Databricks queries and compare table data."
)


class DatabricksMCPServer:
    """
    FastMCP server for Databricks operations.
    Manages initialization, configuration, and server lifecycle.
    """

    def __init__(self):
        """Initialize server components."""
        self.mcp = FastMCP(name=SERVER_NAME, instructions=SERVER_INSTRUCTIONS)
        self.config: Optional[DatabricksConfig] = None
        self._reset_services()

    def _reset_services(self) -> None:
        """Reset all service instances to None."""
        self.databricks_service: Optional[DatabricksService] = None
        self.query_tool: Optional[QueryTool] = None
        self.table_compare_tool: Optional[TableCompareTool] = None

    def initialize(self, config: DatabricksConfig) -> bool:
        """
        Initialize server with configuration.

        Args:
            config: Databricks configuration.

        Returns:
            True if initialization successful, False otherwise.
        """
        self.config = config
        try:
            self._initialize_services()
            self._register_tools()
            logger.info("DatabricksMCPServer initialized successfully.")
            return True
        except Exception as e:
            return self._handle_initialization_error(e)

    def _handle_initialization_error(self, error: Exception) -> bool:
        """Handle server initialization errors.

        Args:
            error: The initialization error.

        Returns:
            False to indicate initialization failure.
        """
        logger.critical(f"Failed to initialize server: {error}", exc_info=True)
        self._reset_services()
        return False

    def _initialize_services(self) -> None:
        """Initialize all services.

        Raises:
            Exception: If service initialization fails.
        """
        try:
            self.databricks_service = DatabricksService(self.config)
            self.query_tool = QueryTool(self.databricks_service)
            self.table_compare_tool = TableCompareTool(self.databricks_service)
        except Exception as e:
            logger.error(f"Failed to initialize services: {e}")
            raise

    def _register_tools(self) -> None:
        """Register all tools with the MCP.

        Raises:
            Exception: If tool registration fails.
        """
        try:
            self._register_query_tools()
            self._register_comparison_tools()
        except Exception as e:
            logger.error(f"Failed to register tools: {e}")
            raise

    def _register_query_tools(self) -> None:
        """Register query-related tools."""
        self.mcp.tool()(self.query_tool.execute_query)
        self.mcp.tool()(self.query_tool.get_table_info)

    def _register_comparison_tools(self) -> None:
        """Register table comparison tools."""
        self.mcp.tool()(self.table_compare_tool.compare_tables)
        self.mcp.tool()(self.table_compare_tool.quick_compare_tables)

    def run(self) -> None:
        """Run the MCP server."""
        server_name = self.mcp.name
        logger.info(f"Starting FastMCP server '{server_name}'...")

        try:
            self.mcp.run()
        except KeyboardInterrupt:
            logger.info("Server interrupted by user")
        except Exception as e:
            logger.critical(
                f"FastMCP server '{server_name}' encountered a fatal error: {e}",
                exc_info=True,
            )
        finally:
            logger.info(f"FastMCP server '{server_name}' stopped.")

    def is_initialized(self) -> bool:
        """Check if server is properly initialized.

        Returns:
            True if all components are initialized.
        """
        return all(
            [
                self.config is not None,
                self.databricks_service is not None,
                self.query_tool is not None,
                self.table_compare_tool is not None,
            ]
        )
