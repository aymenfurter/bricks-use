"""Databricks MCP Server."""

from .config import DatabricksConfig
from .server import DatabricksMCPServer

__version__ = "1.0.0"
__all__ = ["DatabricksConfig", "DatabricksMCPServer"]
